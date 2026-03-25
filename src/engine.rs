use crate::config::{Config, Preset};
use crate::hard_drive::{disk_info, disk_layout, dump_detection, format_disk_layout};
use crate::tui::{start_tui, DupEntry, DupSelection, LiveStats};
use crate::util::{format_bytes_binary, format_bytes_binary_u128};
use crate::web::{best_ui_url, open_http_ui, serve_http};
use anyhow::{Context, Result};
use atty::Stream;
use blake3::Hasher;
use env_logger::Builder;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::LevelFilter;
use rayon::prelude::*;
use rusqlite::{params, Connection};
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc, Arc, Mutex, Once,
};
use std::time::{Duration, Instant, UNIX_EPOCH};
use time::{macros::format_description, OffsetDateTime};
use walkdir::WalkDir;

fn init_logging(force: bool) {
    static LOGGER_INIT: Once = Once::new();
    LOGGER_INIT.call_once(|| {
        let mut builder = Builder::from_default_env();
        if force {
            builder.filter_level(LevelFilter::Trace);
        }
        let _ = builder.try_init();
    });
}

/// Try to pick a filesystem block size hint (bytes).
fn detect_fs_block_size(path: &Path) -> usize {
    #[cfg(unix)]
    {
        let meta = std::fs::metadata(path).or_else(|_| std::fs::metadata(".")); // fallback to CWD
        if let Ok(m) = meta {
            let bs = m.blksize() as usize;
            if bs > 0 {
                return bs;
            }
        }
        4096
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        4096
    }
}

fn align_block_size(target: usize, fs_block: usize) -> usize {
    let blk = fs_block.max(4096);
    ((target + blk - 1) / blk).max(1) * blk
}

#[derive(Debug, Serialize, Clone)]
pub struct Stats {
    pub total_files: usize,
    pub total_candidates: usize,
    pub duplicate_sets: usize,
    pub duplicate_files: usize,
    pub wasted_bytes: u128,
    pub duration_seconds: f64,
    pub error_log: Option<PathBuf>,
}

impl Default for Stats {
    fn default() -> Self {
        Stats {
            total_files: 0,
            total_candidates: 0,
            duplicate_sets: 0,
            duplicate_files: 0,
            wasted_bytes: 0,
            duration_seconds: 0.0,
            error_log: None,
        }
    }
}

#[derive(Clone)]
struct FileRecord {
    path: PathBuf,
    size: u64,
    mtime: i64,
}

#[derive(Clone, Debug)]
struct CacheEntry {
    size: u64,
    mtime: i64,
    partial: Option<String>,
    full: Option<String>,
}

const MAX_DUP_PATHS: usize = 5;

type FullGroupMap = HashMap<String, (u64, Vec<PathBuf>)>;

fn record_full_group(full_groups: &Arc<Mutex<FullGroupMap>>, hash: &str, size: u64, path: &Path) {
    if let Ok(mut map) = full_groups.lock() {
        let entry = map
            .entry(hash.to_string())
            .or_insert_with(|| (size, Vec::new()));
        entry.0 = size;
        if !entry.1.iter().any(|p| p == path) {
            entry.1.push(path.to_path_buf());
        }
    }
}

fn refresh_from_full_groups(
    full_groups: &Arc<Mutex<FullGroupMap>>,
    entries: &Arc<Mutex<Vec<DupEntry>>>,
    update_count: &Arc<AtomicU64>,
    dup_stage: &Arc<AtomicU64>,
) {
    let update_idx = update_count.fetch_add(1, Ordering::Relaxed);
    if update_idx % 200 != 0 && update_idx >= 20 {
        return;
    }
    let snapshot = if let Ok(map) = full_groups.lock() {
        let mut out: Vec<DupEntry> = map
            .iter()
            .filter(|(_, (size, paths))| *size > 0 && paths.len() > 1)
            .map(|(hash, (size, paths))| {
                let mut p = Vec::new();
                for path in paths.iter().take(MAX_DUP_PATHS) {
                    p.push(path.to_string_lossy().to_string());
                }
                DupEntry {
                    hash: hash.clone(),
                    gain: *size as u128 * (paths.len() as u128 - 1),
                    count: paths.len(),
                    size: *size,
                    paths: p,
                }
            })
            .collect();
        out.sort_by(|a, b| b.gain.cmp(&a.gain));
        out.truncate(200);
        out
    } else {
        Vec::new()
    };
    if !snapshot.is_empty() {
        dup_stage.store(0, Ordering::Relaxed);
    }
    if let Ok(mut lock) = entries.lock() {
        *lock = snapshot;
    }
}

enum ReportEvent {
    File {
        hash: String,
        path: PathBuf,
        size: u64,
    },
    Group {
        hash: String,
        paths: Vec<PathBuf>,
        size: u64,
    },
    Summary {
        stats: Stats,
    },
}

fn start_report_writer(
    path: &Path,
) -> Result<(mpsc::Sender<ReportEvent>, std::thread::JoinHandle<()>)> {
    let output = File::create(path)
        .with_context(|| format!("cannot create output file {}", path.display()))?;
    let (tx, rx) = mpsc::channel::<ReportEvent>();
    let handle = std::thread::spawn(move || {
        let mut out = BufWriter::new(output);
        for event in rx {
            let line = match event {
                ReportEvent::File { hash, path, size } => serde_json::json!({
                    "type": "file",
                    "hash": hash,
                    "path": path,
                    "size": size,
                })
                .to_string(),
                ReportEvent::Group { hash, paths, size } => serde_json::json!({
                    "type": "group",
                    "hash": hash,
                    "paths": paths,
                    "size": size,
                })
                .to_string(),
                ReportEvent::Summary { stats } => serde_json::json!({
                    "type": "summary",
                    "stats": stats,
                })
                .to_string(),
            };
            if writeln!(&mut out, "{}", line).is_ok() {
                let _ = out.flush();
            }
        }
    });
    Ok((tx, handle))
}

const ERROR_BUFFER_LIMIT: usize = 8;

fn log_error(
    err_sink: &Arc<Mutex<File>>,
    err_buffer: &Arc<Mutex<VecDeque<String>>>,
    err_count: &Arc<AtomicU64>,
    msg: impl AsRef<str>,
) {
    let msg = msg.as_ref().to_string();
    if let Ok(mut lock) = err_sink.lock() {
        let _ = writeln!(lock, "{}", msg);
    }
    err_count.fetch_add(1, Ordering::Relaxed);
    if let Ok(mut buf) = err_buffer.lock() {
        if buf.len() >= ERROR_BUFFER_LIMIT {
            buf.pop_front();
        }
        buf.push_back(msg);
    }
}

fn file_mtime(path: &Path) -> Result<i64> {
    let md = path.metadata()?;
    let m = md.modified()?;
    let dur = m.duration_since(UNIX_EPOCH).unwrap_or_default();
    Ok(dur.as_secs() as i64)
}

fn collect_files(
    root: &Path,
    err_sink: &Arc<Mutex<File>>,
    err_buffer: &Arc<Mutex<VecDeque<String>>>,
    err_count: &Arc<AtomicU64>,
    scan_pb: Option<&ProgressBar>,
    scan_log: bool,
) -> Result<(Vec<FileRecord>, HashMap<PathBuf, (u64, i64)>)> {
    let mut files = Vec::new();
    let mut map = HashMap::new();
    let mut scanned = 0u64;
    let mut last_path = String::new();
    for entry in WalkDir::new(root).into_iter() {
        scanned += 1;
        if let Some(pb) = scan_pb {
            let last_display = if last_path.is_empty() {
                "<n/a>"
            } else {
                last_path.as_str()
            };
            pb.set_message(format!(
                "Scanning... entries: {} files: {} last: {}",
                scanned,
                files.len(),
                last_display
            ));
            pb.tick();
        } else if scan_log {
            let last_display = if last_path.is_empty() {
                "<n/a>"
            } else {
                last_path.as_str()
            };
            print!(
                "\r\x1b[2KScanning... entries: {} files: {} last: {}",
                scanned,
                files.len(),
                last_display
            );
            let _ = std::io::stdout().flush();
        }
        match entry {
            Ok(e) => {
                last_path = e.path().to_string_lossy().to_string();
                if e.file_type().is_file() {
                    match e.metadata() {
                        Ok(md) => {
                            let size = md.len();
                            let mtime = md
                                .modified()
                                .ok()
                                .and_then(|m| m.duration_since(UNIX_EPOCH).ok())
                                .map(|d| d.as_secs() as i64)
                                .unwrap_or(0);
                            let path = e.into_path();
                            map.insert(path.clone(), (size, mtime));
                            files.push(FileRecord { path, size, mtime });
                        }
                        Err(err) => log_error(
                            err_sink,
                            err_buffer,
                            err_count,
                            format!("metadata error: {}", err),
                        ),
                    }
                }
            }
            Err(err) => log_error(
                err_sink,
                err_buffer,
                err_count,
                format!("walk error: {}", err),
            ),
        }
    }
    if let Some(pb) = scan_pb {
        pb.set_message(format!("Scanning files ({} found)", files.len()));
        pb.finish_and_clear();
    } else if scan_log {
        println!();
    }
    Ok((files, map))
}

fn hash_prefix(path: &Path, limit: usize, buffer_size: usize) -> Result<String> {
    let file =
        File::open(path).with_context(|| format!("failed opening file {}", path.display()))?;
    let mut reader = BufReader::with_capacity(buffer_size, file).take(limit as u64);
    let mut buf = vec![0u8; buffer_size];
    let mut hasher = Hasher::new();
    loop {
        let read = reader.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn hash_full(path: &Path, buffer_size: usize) -> Result<String> {
    let file =
        File::open(path).with_context(|| format!("failed opening file {}", path.display()))?;
    let mut reader = BufReader::with_capacity(buffer_size, file);
    let mut buf = vec![0u8; buffer_size];
    let mut hasher = Hasher::new();
    loop {
        let read = reader.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

type PartialOutput = (Option<String>, Option<(u64, Option<String>)>, PathBuf);

struct PartialContext {
    finished: Arc<AtomicBool>,
    cache_map: Arc<HashMap<PathBuf, CacheEntry>>,
    partial_bytes: usize,
    block_size: usize,
    thread_paths: Arc<Mutex<Vec<String>>>,
    last_path: Arc<Mutex<String>>,
    partial_bytes_read: Arc<AtomicU64>,
    files_processed: Arc<AtomicU64>,
    partial_done: Arc<AtomicU64>,
    err_sink: Arc<Mutex<File>>,
    err_buffer: Arc<Mutex<VecDeque<String>>>,
    err_count: Arc<AtomicU64>,
    pb_partial: Option<ProgressBar>,
    report_tx: Option<mpsc::Sender<ReportEvent>>,
    dup_entries: Arc<Mutex<Vec<DupEntry>>>,
    dup_update_count: Arc<AtomicU64>,
    dup_stage: Arc<AtomicU64>,
    full_groups: Arc<Mutex<FullGroupMap>>,
}

fn update_current_path(
    thread_paths: &Arc<Mutex<Vec<String>>>,
    last_path: &Arc<Mutex<String>>,
    path: &Path,
) {
    let display = path.to_string_lossy().to_string();
    if let Ok(mut lp) = last_path.lock() {
        *lp = display.clone();
    }
    if let Ok(mut paths) = thread_paths.lock() {
        if paths.is_empty() {
            return;
        }
        let idx = rayon::current_thread_index().unwrap_or(0);
        let slot = idx.min(paths.len() - 1);
        paths[slot] = display;
    }
}

fn send_file_record(tx: &Option<mpsc::Sender<ReportEvent>>, hash: &str, path: &Path, size: u64) {
    if let Some(tx) = tx {
        let _ = tx.send(ReportEvent::File {
            hash: hash.to_string(),
            path: path.to_path_buf(),
            size,
        });
    }
}

fn process_partial_candidate(rec: &FileRecord, ctx: &PartialContext) -> Option<PartialOutput> {
    if ctx.finished.load(Ordering::Relaxed) {
        return None;
    }
    update_current_path(&ctx.thread_paths, &ctx.last_path, &rec.path);
    let cache_hit = ctx
        .cache_map
        .get(&rec.path)
        .filter(|entry| entry.size == rec.size && entry.mtime == rec.mtime);
    if let Some(hit) = cache_hit {
        if let Some(full) = &hit.full {
            send_file_record(&ctx.report_tx, full, &rec.path, rec.size);
            record_full_group(&ctx.full_groups, full, rec.size, &rec.path);
            refresh_from_full_groups(
                &ctx.full_groups,
                &ctx.dup_entries,
                &ctx.dup_update_count,
                &ctx.dup_stage,
            );
            if let Some(pb) = &ctx.pb_partial {
                pb.inc(1);
            }
            ctx.partial_done.fetch_add(1, Ordering::Relaxed);
            return Some((
                Some(full.clone()),
                Some((rec.size, hit.partial.clone())),
                rec.path.clone(),
            ));
        }
        if let Some(partial) = &hit.partial {
            if let Some(pb) = &ctx.pb_partial {
                pb.inc(1);
            }
            ctx.partial_done.fetch_add(1, Ordering::Relaxed);
            return Some((
                None,
                Some((rec.size, Some(partial.clone()))),
                rec.path.clone(),
            ));
        }
    }
    if ctx.partial_bytes == 0 {
        if let Some(pb) = &ctx.pb_partial {
            pb.inc(1);
        }
        ctx.partial_done.fetch_add(1, Ordering::Relaxed);
        return Some((None, Some((rec.size, None)), rec.path.clone()));
    }
    match hash_prefix(&rec.path, ctx.partial_bytes, ctx.block_size) {
        Ok(h) => {
            let read = std::cmp::min(rec.size as usize, ctx.partial_bytes) as u64;
            ctx.partial_bytes_read.fetch_add(read, Ordering::Relaxed);
            ctx.files_processed.fetch_add(1, Ordering::Relaxed);
            ctx.partial_done.fetch_add(1, Ordering::Relaxed);
            if let Some(pb) = &ctx.pb_partial {
                pb.inc(1);
            }
            Some((None, Some((rec.size, Some(h))), rec.path.clone()))
        }
        Err(err) => {
            log_error(
                &ctx.err_sink,
                &ctx.err_buffer,
                &ctx.err_count,
                format!("partial hash failed {}: {}", rec.path.display(), err),
            );
            ctx.partial_done.fetch_add(1, Ordering::Relaxed);
            if let Some(pb) = &ctx.pb_partial {
                pb.inc(1);
            }
            None
        }
    }
}

fn progress_bar(len: u64, label: &str) -> ProgressBar {
    let pb = ProgressBar::new(len);
    pb.set_style(
        ProgressStyle::with_template(
            "{prefix:10} {wide_bar} {pos}/{len} [{elapsed_precise} -> {eta_precise}]",
        )
        .unwrap()
        .progress_chars("█▉▊▋▌▍▎▏  "),
    );
    pb.set_prefix(label.to_string());
    pb
}

fn default_cache_path(root: &Path) -> PathBuf {
    root.join(".dupdup-cache.sqlite")
}

fn open_cache(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS files (
            path TEXT PRIMARY KEY,
            size INTEGER NOT NULL,
            mtime INTEGER NOT NULL,
            partial TEXT,
            full TEXT
        )",
        [],
    )?;
    conn.execute("CREATE INDEX IF NOT EXISTS files_mtime ON files(mtime)", [])?;
    Ok(conn)
}

fn load_cache(conn: &Connection) -> Result<HashMap<PathBuf, CacheEntry>> {
    let mut stmt = conn.prepare("SELECT path,size,mtime,partial,full FROM files")?;
    let rows = stmt.query_map([], |row| {
        let path: String = row.get(0)?;
        let size: u64 = row.get(1)?;
        let mtime: i64 = row.get(2)?;
        let partial: Option<String> = row.get(3)?;
        let full: Option<String> = row.get(4)?;
        Ok((
            PathBuf::from(path),
            CacheEntry {
                size,
                mtime,
                partial,
                full,
            },
        ))
    })?;
    let mut map = HashMap::new();
    for r in rows {
        let (p, e) = r?;
        map.insert(p, e);
    }
    Ok(map)
}

pub fn run(mut cfg: Config) -> Result<Stats> {
    init_logging(cfg.dump_disk_info);
    if cfg.dump_disk_info {
        dump_detection(&cfg.path);
        return Ok(Stats::default());
    }

    let disk = disk_info(&cfg.path);
    let auto_preset = cfg.preset == Preset::Auto;
    let mut preset = cfg.preset.clone();
    if auto_preset {
        match disk.as_ref().and_then(|d| d.rotational) {
            Some(true) => {
                println!("Auto-preset: detected rotational disk -> enabling --preset hdd");
                preset = Preset::Hdd;
            }
            Some(false) => {
                println!("Auto-preset: detected SSD -> enabling --preset ssd");
                preset = Preset::Ssd;
            }
            None => {
                println!("Auto-preset: could not detect disk type, defaulting to hdd");
                preset = Preset::Hdd;
            }
        }
    }

    // Apply presets if requested
    if preset == Preset::Ssd {
        if cfg.threads == 0 {
            cfg.threads = num_cpus::get().saturating_mul(2);
        }
        cfg.block_size = cfg.block_size.max(1 * 1024 * 1024);
        cfg.partial_bytes = 0; // full-pass hashing
    }
    if preset == Preset::Hdd {
        // Favor sequential I/O on spinny disks: single thread unless user insisted on 1+.
        if cfg.threads == 0 {
            cfg.threads = 1;
        } else {
            cfg.threads = cfg.threads.min(1);
        }
        cfg.ordered = true;
        cfg.block_size = cfg.block_size.min(128 * 1024).max(64 * 1024);
        if cfg.partial_bytes == 0 {
            cfg.partial_bytes = 4096;
        }
    }

    // Align block size to filesystem block for better throughput/cache fit
    let fs_block = detect_fs_block_size(&cfg.path);
    let disk_block = disk.as_ref().map(|d| d.block_size).unwrap_or(0);
    let align_block = if disk_block > 0 { disk_block } else { fs_block };
    if preset == Preset::Hdd {
        let mut bs = cfg.block_size.clamp(64 * 1024, 128 * 1024);
        bs = align_block_size(bs, align_block);
        cfg.block_size = bs.clamp(64 * 1024, 128 * 1024);
    } else {
        cfg.block_size = align_block_size(cfg.block_size, align_block);
    }

    let start = Instant::now();
    let stamp = OffsetDateTime::now_utc()
        .format(&format_description!(
            "[year]-[month]-[day]_[hour][minute][second]"
        ))
        .unwrap_or_else(|_| "timestamp".to_string());
    let error_path = cfg
        .error
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("error-{}.log", stamp)));
    let err_sink = Arc::new(Mutex::new(File::create(&error_path)?));
    let (report_tx, report_handle) = start_report_writer(&cfg.output)?;
    let err_buffer = Arc::new(Mutex::new(VecDeque::with_capacity(ERROR_BUFFER_LIMIT)));
    let err_count = Arc::new(AtomicU64::new(0));
    let thread_paths = Arc::new(Mutex::new(Vec::new()));
    let status_line = Arc::new(Mutex::new(String::new()));
    let dup_entries = Arc::new(Mutex::new(Vec::new()));
    let dup_update_count = Arc::new(AtomicU64::new(0));
    let dup_scroll = Arc::new(AtomicU64::new(0));
    let dup_stage = Arc::new(AtomicU64::new(0));
    let dup_selected = Arc::new(Mutex::new(DupSelection {
        hash: None,
        path_idx: None,
    }));
    let dup_expanded = Arc::new(Mutex::new(HashSet::new()));
    let full_groups: Arc<Mutex<FullGroupMap>> = Arc::new(Mutex::new(HashMap::new()));

    if let Some(layout) = disk_layout(&cfg.path) {
        let line = format_disk_layout(&layout);
        println!("Disk: {}", line);
        if let Ok(mut status) = status_line.lock() {
            if status.is_empty() {
                *status = format!("Disk: {}", line);
            } else {
                *status = format!("{} | Disk: {}", status, line);
            }
        }
    }

    let use_tui = if cfg.no_tui {
        false
    } else if cfg.tui {
        true
    } else {
        atty::is(Stream::Stdout)
    };
    let auto_serve = !cfg.serve && !cfg.no_open_ui;
    let mut server_handle: Option<std::thread::JoinHandle<()>> = None;
    if cfg.serve || auto_serve {
        server_handle = serve_http(cfg.output.clone(), cfg.port);
        if server_handle.is_some() {
            let ui_url = best_ui_url(cfg.port);
            if let Ok(mut status) = status_line.lock() {
                if status.is_empty() {
                    *status = format!("UI: {}", ui_url);
                } else {
                    *status = format!("{} | UI: {}", status, ui_url);
                }
            }
            if !cfg.no_open_ui {
                open_http_ui(&ui_url);
            }
            println!("UI: {}", ui_url);
            println!("HTTP server running. Press Ctrl+C to stop.");
        }
    }
    let scan_pb = if atty::is(Stream::Stdout) {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::with_template("{spinner} {msg}")
                .unwrap()
                .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ "),
        );
        pb.set_message("Scanning files...");
        pb.enable_steady_tick(Duration::from_millis(120));
        Some(pb)
    } else {
        None
    };

    let scan_log = !atty::is(Stream::Stdout);
    let (mut files, size_map) = collect_files(
        &cfg.path,
        &err_sink,
        &err_buffer,
        &err_count,
        scan_pb.as_ref(),
        scan_log,
    )?;
    if cfg.ordered {
        files.sort_by(|a, b| a.path.cmp(&b.path));
    }
    let total_files = files.len();
    println!("Discovered {} files", total_files);

    let mut by_size: HashMap<u64, Vec<FileRecord>> = HashMap::new();
    for rec in &files {
        by_size.entry(rec.size).or_default().push(rec.clone());
    }

    {
        let mut snapshot: Vec<DupEntry> = by_size
            .values()
            .filter(|v| v.len() > 1)
            .map(|v| {
                let mut paths = Vec::new();
                for rec in v.iter().take(MAX_DUP_PATHS) {
                    paths.push(rec.path.to_string_lossy().to_string());
                }
                DupEntry {
                    hash: format!("size:{}", v[0].size),
                    gain: v[0].size as u128 * (v.len() as u128 - 1),
                    count: v.len(),
                    size: v[0].size,
                    paths,
                }
            })
            .collect();
        snapshot.sort_by(|a, b| b.gain.cmp(&a.gain));
        snapshot.truncate(200);
        if !snapshot.is_empty() {
            if let Ok(mut lock) = dup_entries.lock() {
                *lock = snapshot;
            }
            dup_stage.store(2, Ordering::Relaxed);
        }
    }

    let potential_waste = Arc::new(Mutex::new(0u128));
    let mut candidates: Vec<FileRecord> = Vec::new();
    for v in by_size.values() {
        if v.len() > 1 {
            candidates.extend(v.clone());
        }
    }
    {
        let size_only_waste: u128 = by_size
            .values()
            .map(|v| {
                if v.len() > 1 {
                    v[0].size as u128 * (v.len() as u128 - 1)
                } else {
                    0
                }
            })
            .sum();
        if let Ok(mut w) = potential_waste.lock() {
            *w = size_only_waste;
        }
    }
    if cfg.ordered {
        candidates.sort_by(|a, b| a.path.cmp(&b.path));
    }
    let total_candidates = candidates.len();

    let thread_count = if cfg.ordered {
        1
    } else if cfg.threads == 0 {
        num_cpus::get()
    } else {
        cfg.threads
    };
    if let Ok(mut paths) = thread_paths.lock() {
        *paths = vec![String::from("<idle>"); thread_count.max(1)];
    }
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build()?;

    // Cache setup
    let cache_path = cfg
        .cache
        .clone()
        .unwrap_or_else(|| default_cache_path(&cfg.path));
    let auto_cache = preset == Preset::Hdd;
    let use_cache = cfg.resume || cfg.cache.is_some() || auto_cache;
    let mut cache_conn = if use_cache {
        Some(open_cache(&cache_path)?)
    } else {
        None
    };
    let cache_map = if let Some(ref conn) = cache_conn {
        load_cache(conn)?
    } else {
        HashMap::new()
    };

    let size_lookup = Arc::new(size_map);
    let partial_bytes = cfg.partial_bytes;
    let block_size = cfg.block_size;
    let cache_map_arc = Arc::new(cache_map);

    let partial_bytes_read = Arc::new(AtomicU64::new(0));
    let full_bytes_read = Arc::new(AtomicU64::new(0));
    let files_processed = Arc::new(AtomicU64::new(0));
    let partial_done = Arc::new(AtomicU64::new(0));
    let full_done = Arc::new(AtomicU64::new(0));
    let finished = Arc::new(AtomicBool::new(false));
    let aborting_flag = Arc::new(AtomicBool::new(false));
    let last_path = Arc::new(Mutex::new(String::from("starting...")));

    let preset_label = match preset {
        Preset::Auto => "auto",
        Preset::Ssd => "ssd",
        Preset::Hdd => "hdd",
    };
    let cache_status = if use_cache {
        let mode = if cfg.resume || cfg.cache.is_some() {
            "on"
        } else if auto_cache {
            "auto"
        } else {
            "on"
        };
        format!("{} ({})", mode, cache_path.display())
    } else {
        "off".to_string()
    };
    let partial_status = if cfg.partial_bytes == 0 {
        "partial: off (full hash)".to_string()
    } else {
        format!("partial: {}", format_bytes_binary(cfg.partial_bytes as u64))
    };
    let settings_line = format!(
        "preset:{} auto:{} ordered:{} threads:{} block:{} fs:{} {} cache:{}",
        preset_label,
        if auto_preset { "on" } else { "off" },
        cfg.ordered,
        thread_count,
        format_bytes_binary(cfg.block_size as u64),
        format_bytes_binary(fs_block as u64),
        partial_status,
        cache_status
    );

    // Ctrl+C: first press sets aborting flag, second exits immediately.
    {
        let aborting = aborting_flag.clone();
        let finished_flag = finished.clone();
        ctrlc::set_handler(move || {
            if finished_flag.load(Ordering::SeqCst) {
                // Already done; honor Ctrl+C as a no-op exit 0
                std::process::exit(0);
            }
            if aborting.fetch_or(true, Ordering::SeqCst) {
                // second Ctrl+C
                std::process::exit(130);
            } else {
                eprintln!("Ctrl+C received: finishing current tasks (press again to force quit)…");
                finished_flag.store(true, Ordering::SeqCst);
            }
        })
        .ok();
    }

    let live_stats = LiveStats {
        total_files,
        total_candidates,
        partial_total: total_candidates as u64,
        full_total: Arc::new(AtomicU64::new(0)),
        partial_done: partial_done.clone(),
        full_done: full_done.clone(),
        partial_bytes: partial_bytes_read.clone(),
        full_bytes: full_bytes_read.clone(),
        files_done: files_processed.clone(),
        finished: finished.clone(),
        aborting: aborting_flag.clone(),
        last_path: last_path.clone(),
        thread_paths: thread_paths.clone(),
        settings: settings_line.clone(),
        status_line: status_line.clone(),
        potential_waste: potential_waste.clone(),
        errors: err_buffer.clone(),
        error_count: err_count.clone(),
        dup_entries: dup_entries.clone(),
        dup_scroll: dup_scroll.clone(),
        dup_stage: dup_stage.clone(),
        dup_selected: dup_selected.clone(),
        dup_expanded: dup_expanded.clone(),
        full_groups: full_groups.clone(),
        start,
    };
    let mp = if use_tui {
        None
    } else {
        Some(Arc::new(MultiProgress::new()))
    };
    let pb_partial = mp
        .as_ref()
        .map(|m| m.add(progress_bar(total_candidates as u64, "partial")));
    let pb_full = mp.as_ref().map(|m| m.add(progress_bar(0, "full")));
    let tui_handle = if use_tui {
        start_tui(live_stats.clone())
    } else {
        None
    };

    let mut full_pairs: Vec<(String, PathBuf)> = Vec::new();
    let mut partial_results: Vec<(u64, String, PathBuf)> = Vec::new();

    let partial_ctx = PartialContext {
        finished: finished.clone(),
        cache_map: cache_map_arc.clone(),
        partial_bytes,
        block_size,
        thread_paths: thread_paths.clone(),
        last_path: last_path.clone(),
        partial_bytes_read: partial_bytes_read.clone(),
        files_processed: files_processed.clone(),
        partial_done: partial_done.clone(),
        err_sink: err_sink.clone(),
        err_buffer: err_buffer.clone(),
        err_count: err_count.clone(),
        pb_partial: pb_partial.clone(),
        report_tx: Some(report_tx.clone()),
        dup_entries: dup_entries.clone(),
        dup_update_count: dup_update_count.clone(),
        dup_stage: dup_stage.clone(),
        full_groups: full_groups.clone(),
    };
    let partial_output: Vec<PartialOutput> = if cfg.ordered {
        let mut out = Vec::with_capacity(candidates.len());
        for rec in candidates.iter() {
            if finished.load(Ordering::Relaxed) {
                break;
            }
            if let Some(result) = process_partial_candidate(rec, &partial_ctx) {
                out.push(result);
            }
        }
        out
    } else {
        pool.install(|| {
            candidates
                .par_iter()
                .filter_map(|rec| process_partial_candidate(rec, &partial_ctx))
                .collect::<Vec<_>>()
        })
    };
    if let Some(pb) = pb_partial.as_ref() {
        pb.finish_and_clear();
    }

    for (full_opt, partial_opt, path) in partial_output {
        if let Some((size, partial_val)) = partial_opt {
            if let Some(full_hash) = full_opt {
                full_pairs.push((full_hash, path));
            } else {
                partial_results.push((size, partial_val.unwrap_or_default(), path));
            }
        }
    }

    // Group by (size, partial_hash)
    let mut partial_groups: HashMap<(u64, String), Vec<PathBuf>> = HashMap::new();
    for (size, hash, path) in partial_results {
        partial_groups.entry((size, hash)).or_default().push(path);
    }
    let partial_dup_sets = partial_groups.values().filter(|v| v.len() > 1).count();
    let partial_dup_files: usize = partial_groups
        .values()
        .filter(|v| v.len() > 1)
        .map(|v| v.len())
        .sum();
    // Estimate upper-bound savings from partial groups (or size groups if no partials)
    let upper_waste: u128 = if partial_bytes == 0 {
        by_size
            .values()
            .map(|v| {
                if v.len() > 1 {
                    v[0].size as u128 * (v.len() as u128 - 1)
                } else {
                    0
                }
            })
            .sum()
    } else {
        partial_groups
            .values()
            .map(|v| {
                if v.len() > 1 {
                    let sz = size_lookup.get(&v[0]).map(|(s, _)| *s).unwrap_or(0) as u128;
                    sz * (v.len() as u128 - 1)
                } else {
                    0
                }
            })
            .sum()
    };
    if let Ok(mut w) = potential_waste.lock() {
        *w = upper_waste;
    }
    if partial_bytes != 0 {
        let msg = format!(
            "Partial pass: {} candidate sets, {} files, potential waste up to {}",
            partial_dup_sets,
            partial_dup_files,
            format_bytes_binary_u128(upper_waste)
        );
        if use_tui {
            if let Ok(mut status) = status_line.lock() {
                *status = msg;
            }
        } else {
            println!("{}", msg);
        }
        let mut snapshot: Vec<DupEntry> = partial_groups
            .iter()
            .filter(|(_, v)| v.len() > 1)
            .map(|(key, v)| {
                let size = size_lookup.get(&v[0]).map(|(s, _)| *s).unwrap_or(0);
                let mut paths = Vec::new();
                for p in v.iter().take(MAX_DUP_PATHS) {
                    paths.push(p.to_string_lossy().to_string());
                }
                DupEntry {
                    hash: format!("partial:{}:{}", key.0, key.1),
                    gain: size as u128 * (v.len() as u128 - 1),
                    count: v.len(),
                    size,
                    paths,
                }
            })
            .collect();
        snapshot.sort_by(|a, b| b.gain.cmp(&a.gain));
        snapshot.truncate(200);
        if !snapshot.is_empty() {
            if let Ok(mut lock) = dup_entries.lock() {
                *lock = snapshot;
            }
            dup_stage.store(1, Ordering::Relaxed);
        }
    }

    let mut full_candidates: Vec<PathBuf> = if partial_bytes == 0 {
        candidates.into_iter().map(|r| r.path).collect()
    } else {
        partial_groups
            .iter()
            .filter(|(_, v)| v.len() > 1)
            .flat_map(|(_, v)| v.clone())
            .collect()
    };
    if cfg.ordered {
        full_candidates.sort();
    }

    if let Some(pb) = pb_full.as_ref() {
        pb.set_length(full_candidates.len() as u64);
    }
    live_stats
        .full_total
        .store(full_candidates.len() as u64, Ordering::Relaxed);

    let cache_map_arc_full = cache_map_arc.clone();
    let err_clone = err_sink.clone();
    let thread_paths_full = thread_paths.clone();
    let last_path_full = last_path.clone();
    let report_tx_full = Some(report_tx.clone());
    let size_lookup_full = size_lookup.clone();
    let full_groups_full = full_groups.clone();
    let dup_entries_full = dup_entries.clone();
    let dup_update_count_full = dup_update_count.clone();
    let dup_stage_full = dup_stage.clone();

    let full_output = if cfg.ordered {
        let mut out = Vec::with_capacity(full_candidates.len());
        for path in full_candidates.iter() {
            if finished.load(Ordering::Relaxed) {
                break;
            }
            update_current_path(&thread_paths_full, &last_path_full, path);
            if let Some(entry) = cache_map_arc_full.get(path) {
                let meta_ok = file_mtime(path)
                    .ok()
                    .map(|m| m == entry.mtime)
                    .unwrap_or(false)
                    && path
                        .metadata()
                        .map(|m| m.len() == entry.size)
                        .unwrap_or(false);
                if meta_ok {
                    if let Some(full) = &entry.full {
                        if let Some(pb) = pb_full.as_ref() {
                            pb.inc(1);
                        }
                        full_done.fetch_add(1, Ordering::Relaxed);
                        files_processed.fetch_add(1, Ordering::Relaxed);
                        let size = size_lookup_full.get(path).map(|(s, _)| *s).unwrap_or(0);
                        send_file_record(&report_tx_full, full, path, size);
                        record_full_group(&full_groups_full, full, size, path);
                        refresh_from_full_groups(
                            &full_groups_full,
                            &dup_entries_full,
                            &dup_update_count_full,
                            &dup_stage_full,
                        );
                        out.push((full.clone(), path.clone(), Some(entry.partial.clone())));
                        continue;
                    }
                }
            }
            match hash_full(path, block_size) {
                Ok(h) => {
                    full_bytes_read.fetch_add(
                        path.metadata().map(|m| m.len()).unwrap_or(0),
                        Ordering::Relaxed,
                    );
                    full_done.fetch_add(1, Ordering::Relaxed);
                    files_processed.fetch_add(1, Ordering::Relaxed);
                    if let Some(pb) = pb_full.as_ref() {
                        pb.inc(1);
                    }
                    let size = size_lookup_full.get(path).map(|(s, _)| *s).unwrap_or(0);
                    send_file_record(&report_tx_full, &h, path, size);
                    record_full_group(&full_groups_full, &h, size, path);
                    refresh_from_full_groups(
                        &full_groups_full,
                        &dup_entries_full,
                        &dup_update_count_full,
                        &dup_stage_full,
                    );
                    out.push((h, path.clone(), None));
                }
                Err(err) => {
                    log_error(
                        &err_clone,
                        &err_buffer,
                        &err_count,
                        format!("full hash failed {}: {}", path.display(), err),
                    );
                    if let Some(pb) = pb_full.as_ref() {
                        pb.inc(1);
                    }
                }
            }
        }
        out
    } else {
        pool.install(|| {
            full_candidates
                .par_iter()
                .filter_map(|path| {
                    if finished.load(Ordering::Relaxed) {
                        return None;
                    }
                    update_current_path(&thread_paths_full, &last_path_full, path);
                    if let Some(entry) = cache_map_arc_full.get(path) {
                        let meta_ok = file_mtime(path)
                            .ok()
                            .map(|m| m == entry.mtime)
                            .unwrap_or(false)
                            && path
                                .metadata()
                                .map(|m| m.len() == entry.size)
                                .unwrap_or(false);
                        if meta_ok {
                            if let Some(full) = &entry.full {
                                if let Some(pb) = pb_full.as_ref() {
                                    pb.inc(1);
                                }
                                full_done.fetch_add(1, Ordering::Relaxed);
                                files_processed.fetch_add(1, Ordering::Relaxed);
                                let size = size_lookup_full.get(path).map(|(s, _)| *s).unwrap_or(0);
                                send_file_record(&report_tx_full, full, path, size);
                                record_full_group(&full_groups_full, full, size, path);
                                refresh_from_full_groups(
                                    &full_groups_full,
                                    &dup_entries_full,
                                    &dup_update_count_full,
                                    &dup_stage_full,
                                );
                                return Some((
                                    full.clone(),
                                    path.clone(),
                                    Some(entry.partial.clone()),
                                ));
                            }
                        }
                    }
                    match hash_full(path, block_size) {
                        Ok(h) => {
                            full_bytes_read.fetch_add(
                                path.metadata().map(|m| m.len()).unwrap_or(0),
                                Ordering::Relaxed,
                            );
                            full_done.fetch_add(1, Ordering::Relaxed);
                            files_processed.fetch_add(1, Ordering::Relaxed);
                            if let Some(pb) = pb_full.as_ref() {
                                pb.inc(1);
                            }
                            let size = size_lookup_full.get(path).map(|(s, _)| *s).unwrap_or(0);
                            send_file_record(&report_tx_full, &h, path, size);
                            record_full_group(&full_groups_full, &h, size, path);
                            refresh_from_full_groups(
                                &full_groups_full,
                                &dup_entries_full,
                                &dup_update_count_full,
                                &dup_stage_full,
                            );
                            Some((h, path.clone(), None))
                        }
                        Err(err) => {
                            log_error(
                                &err_clone,
                                &err_buffer,
                                &err_count,
                                format!("full hash failed {}: {}", path.display(), err),
                            );
                            if let Some(pb) = pb_full.as_ref() {
                                pb.inc(1);
                            }
                            None
                        }
                    }
                })
                .collect::<Vec<_>>()
        })
    };
    if let Some(pb) = pb_full.as_ref() {
        pb.finish_and_clear();
    }

    for (full_hash, path, _maybe_partial) in full_output {
        full_pairs.push((full_hash, path));
    }

    // Group by full hash
    let mut full_groups: HashMap<String, Vec<PathBuf>> = HashMap::new();
    for (hash, path) in &full_pairs {
        full_groups
            .entry(hash.clone())
            .or_default()
            .push(path.clone());
    }

    let mut filtered: HashMap<String, (Vec<PathBuf>, u64)> = HashMap::new();
    let mut duplicate_files = 0usize;
    let mut wasted_bytes: u128 = 0;
    for (hash, paths) in full_groups {
        if paths.len() > 1 {
            duplicate_files += paths.len();
            let size = paths
                .get(0)
                .and_then(|p| size_lookup.get(p).map(|(s, _)| *s))
                .unwrap_or(0);
            wasted_bytes += size as u128 * (paths.len() as u128 - 1);
            filtered.insert(hash, (paths, size));
        }
    }
    if let Ok(mut w) = potential_waste.lock() {
        *w = wasted_bytes;
    }
    dup_stage.store(0, Ordering::Relaxed);
    {
        let mut snapshot: Vec<DupEntry> = filtered
            .iter()
            .filter(|(_, (paths, size))| !paths.is_empty() && *size > 0)
            .map(|(hash, (paths, size))| {
                let mut p = Vec::new();
                for path in paths.iter().take(MAX_DUP_PATHS) {
                    p.push(path.to_string_lossy().to_string());
                }
                DupEntry {
                    hash: hash.clone(),
                    gain: *size as u128 * (paths.len() as u128 - 1),
                    count: paths.len(),
                    size: *size,
                    paths: p,
                }
            })
            .collect();
        snapshot.sort_by(|a, b| b.gain.cmp(&a.gain));
        snapshot.truncate(200);
        if let Ok(mut lock) = dup_entries.lock() {
            *lock = snapshot;
        }
    }

    for (hash, (paths, size)) in &filtered {
        let _ = report_tx.send(ReportEvent::Group {
            hash: hash.clone(),
            paths: paths.clone(),
            size: *size,
        });
    }

    // Persist cache updates
    if let Some(conn) = cache_conn.as_mut() {
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO files(path,size,mtime,partial,full) VALUES (?1,?2,?3,?4,?5)
                 ON CONFLICT(path) DO UPDATE SET size=excluded.size, mtime=excluded.mtime, partial=excluded.partial, full=excluded.full",
            )?;
            for (hash, path) in &full_pairs {
                let md = path.metadata();
                if let Ok(md) = md {
                    let size = md.len() as i64;
                    let mtime = md
                        .modified()
                        .ok()
                        .and_then(|m| m.duration_since(UNIX_EPOCH).ok())
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0);
                    stmt.execute(params![
                        path.to_string_lossy(),
                        size,
                        mtime,
                        None::<String>,
                        hash
                    ])?;
                }
            }
        }
        tx.commit()?;
    }

    let duration_seconds = start.elapsed().as_secs_f64();
    let total_bytes_hashed =
        partial_bytes_read.load(Ordering::Relaxed) + full_bytes_read.load(Ordering::Relaxed);
    let files_done = files_processed.load(Ordering::Relaxed);
    let bytes_per_sec = if duration_seconds > 0.0 {
        total_bytes_hashed as f64 / duration_seconds
    } else {
        0.0
    };
    let files_per_sec = if duration_seconds > 0.0 {
        files_done as f64 / duration_seconds
    } else {
        0.0
    };
    finished.store(true, Ordering::Relaxed);

    println!(
        "Done in {:.2}s. Duplicate sets: {} ({} files), reclaimable ~{} bytes",
        duration_seconds,
        filtered.len(),
        duplicate_files,
        wasted_bytes
    );
    println!(
        "Hashed: {:.2} MiB ({:.2} MiB/s), files: {} ({:.1}/s)",
        total_bytes_hashed as f64 / (1024.0 * 1024.0),
        bytes_per_sec / (1024.0 * 1024.0),
        files_done,
        files_per_sec
    );
    finished.store(true, Ordering::Relaxed);
    if let Some(handle) = tui_handle {
        let _ = handle.join();
    }

    if aborting_flag.load(Ordering::SeqCst) {
        std::process::exit(130);
    }

    let stats = Stats {
        total_files,
        total_candidates,
        duplicate_sets: filtered.len(),
        duplicate_files,
        wasted_bytes,
        duration_seconds,
        error_log: Some(error_path),
    };
    let _ = report_tx.send(ReportEvent::Summary {
        stats: stats.clone(),
    });
    drop(report_tx);
    let _ = report_handle.join();

    if server_handle.is_some() {
        // Keep main thread alive so the server thread isn't torn down immediately
        loop {
            std::thread::park();
        }
    }

    Ok(stats)
}
