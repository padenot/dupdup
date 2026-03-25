use crate::util::{format_bytes_binary, format_bytes_binary_u128};
use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    widgets::{Block, Borders, Clear, Gauge, Paragraph, Sparkline},
    Terminal,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::stdout;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use trash;

const MAX_DUP_PATHS: usize = 5;
const ERROR_BUFFER_LIMIT: usize = 8;

#[derive(Clone)]
pub(crate) struct LiveStats {
    pub(crate) total_files: usize,
    pub(crate) total_candidates: usize,
    pub(crate) partial_total: u64,
    pub(crate) full_total: Arc<AtomicU64>,
    pub(crate) partial_done: Arc<AtomicU64>,
    pub(crate) full_done: Arc<AtomicU64>,
    pub(crate) partial_bytes: Arc<AtomicU64>,
    pub(crate) full_bytes: Arc<AtomicU64>,
    pub(crate) files_done: Arc<AtomicU64>,
    pub(crate) finished: Arc<AtomicBool>,
    pub(crate) aborting: Arc<AtomicBool>,
    pub(crate) last_path: Arc<Mutex<String>>,
    pub(crate) thread_paths: Arc<Mutex<Vec<String>>>,
    pub(crate) settings: String,
    pub(crate) status_line: Arc<Mutex<String>>,
    pub(crate) potential_waste: Arc<Mutex<u128>>,
    pub(crate) errors: Arc<Mutex<VecDeque<String>>>,
    pub(crate) error_count: Arc<AtomicU64>,
    pub(crate) dup_entries: Arc<Mutex<Vec<DupEntry>>>,
    pub(crate) dup_scroll: Arc<AtomicU64>,
    pub(crate) dup_stage: Arc<AtomicU64>,
    pub(crate) dup_selected: Arc<Mutex<DupSelection>>,
    pub(crate) dup_expanded: Arc<Mutex<HashSet<String>>>,
    pub(crate) full_groups: Arc<Mutex<HashMap<String, (u64, Vec<PathBuf>)>>>,
    pub(crate) start: Instant,
}

#[derive(Clone)]
pub(crate) struct DupEntry {
    pub(crate) hash: String,
    pub(crate) gain: u128,
    pub(crate) count: usize,
    pub(crate) size: u64,
    pub(crate) paths: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DupSelection {
    pub(crate) hash: Option<String>,
    pub(crate) path_idx: Option<usize>,
}

#[derive(Clone, Copy, Debug)]
enum DupSort {
    Gain,
    Count,
    Size,
}

enum ConfirmAction {
    Delete {
        hash: String,
        targets: Vec<PathBuf>,
        permanent: bool,
    },
    Keep {
        hash: String,
        keep: PathBuf,
        delete: Vec<PathBuf>,
        permanent: bool,
    },
}

struct ConfirmState {
    title: String,
    body: Vec<String>,
    action: ConfirmAction,
}

struct DeleteResult {
    deleted: Vec<PathBuf>,
    failed: Vec<String>,
}

fn rebuild_entries_from_groups(map: &HashMap<String, (u64, Vec<PathBuf>)>) -> Vec<DupEntry> {
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
}

fn delete_paths(paths: &[PathBuf], permanent: bool) -> DeleteResult {
    let mut deleted = Vec::new();
    let mut failed = Vec::new();
    for p in paths {
        if permanent {
            match std::fs::remove_file(p) {
                Ok(_) => deleted.push(p.clone()),
                Err(e) => failed.push(format!("{}: {}", p.display(), e)),
            }
        } else {
            match trash::delete(p) {
                Ok(_) => deleted.push(p.clone()),
                Err(e) => failed.push(format!("{}: {}", p.display(), e)),
            }
        }
    }
    DeleteResult { deleted, failed }
}

fn pad_left(s: &str, width: usize) -> String {
    if s.len() >= width {
        s.to_string()
    } else {
        format!("{:>width$}", s, width = width)
    }
}

fn centered_rect_fixed(width: u16, height: u16, r: Rect) -> Rect {
    let w = width.min(r.width);
    let h = height.min(r.height);
    let x = r.x + (r.width.saturating_sub(w)) / 2;
    let y = r.y + (r.height.saturating_sub(h)) / 2;
    Rect {
        x,
        y,
        width: w,
        height: h,
    }
}

fn recompute_potential_waste(map: &HashMap<String, (u64, Vec<PathBuf>)>) -> u128 {
    map.values()
        .map(|(size, paths)| {
            if paths.len() > 1 {
                *size as u128 * (paths.len() as u128 - 1)
            } else {
                0
            }
        })
        .sum()
}

pub(crate) fn start_tui(stats: LiveStats) -> Option<JoinHandle<()>> {
    let terminal_result: std::io::Result<JoinHandle<()>> = (|| {
        enable_raw_mode()?;
        let mut out = stdout();
        execute!(out, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(out);
        let mut terminal = Terminal::new(backend)?;
        terminal.clear()?;
        let handle = thread::spawn(move || {
            let mut last_bytes = 0u64;
            let mut last_files = 0u64;
            let mut last_draw_time = Instant::now();
            let mut speed_series: VecDeque<u64> = VecDeque::with_capacity(120);
            let mut last_dup_height = 6usize;
            let mut dup_sort = DupSort::Gain;
            let mut confirm_state: Option<ConfirmState> = None;
            let mut confirm_skip = false;
            loop {
                let elapsed = stats.start.elapsed().as_secs_f64().max(0.0001);
                let partial_done = stats.partial_done.load(Ordering::Relaxed);
                let partial_total = stats.partial_total;
                let full_done = stats.full_done.load(Ordering::Relaxed);
                let full_total = stats.full_total.load(Ordering::Relaxed).max(1);
                let bytes = stats.partial_bytes.load(Ordering::Relaxed)
                    + stats.full_bytes.load(Ordering::Relaxed);
                let files = stats.files_done.load(Ordering::Relaxed);
                let bps = bytes as f64 / elapsed;
                let _fps = files as f64 / elapsed;

                let partial_ratio = if partial_total > 0 {
                    (partial_done as f64 / partial_total as f64).clamp(0.0, 1.0)
                } else {
                    1.0
                };
                let full_ratio = (full_done as f64 / full_total as f64).clamp(0.0, 1.0);

                let _delta_bytes = bytes.saturating_sub(last_bytes);
                last_bytes = bytes;
                // compute instantaneous files/sec since last draw; store with one decimal resolution
                let delta_files = files.saturating_sub(last_files);
                let now = Instant::now();
                let dt = now.duration_since(last_draw_time).as_secs_f64().max(0.001);
                last_files = files;
                last_draw_time = now;
                let inst_fps = ((delta_files as f64 / dt) * 10.0).round() as u64; // files/sec *10
                speed_series.push_back(inst_fps);
                if speed_series.len() > 60 {
                    speed_series.pop_front();
                }

                let inst_fps_display = if let Some(last) = speed_series.back() {
                    *last as f64 / 10.0
                } else {
                    0.0
                };
                let potential_bytes = stats.potential_waste.lock().map(|v| *v).unwrap_or(0);
                let info = format!(
                    "files {:>6}/{:>6} | dup-cands {:>6} | bytes {:.2} MiB | avg {:.2} MiB/s | inst {:.1} files/s | potential {}",
                    files,
                    stats.total_files,
                    stats.total_candidates,
                    bytes as f64 / (1024.0 * 1024.0),
                    bps / (1024.0 * 1024.0),
                    inst_fps_display,
                    format_bytes_binary_u128(potential_bytes)
                );
                let savings_bytes = stats.potential_waste.lock().map(|v| *v).unwrap_or(0);
                let settings_line = format!(
                    "{} savings:{}",
                    stats.settings,
                    format_bytes_binary(savings_bytes as u64)
                );
                let status_line = stats
                    .status_line
                    .lock()
                    .map(|s| s.clone())
                    .unwrap_or_default();

                let current_path = stats
                    .last_path
                    .lock()
                    .map(|s| s.clone())
                    .unwrap_or_else(|_| "<path unavailable>".to_string());
                let mut path_lines = Vec::new();
                if let Ok(paths) = stats.thread_paths.lock() {
                    for (idx, path) in paths.iter().enumerate() {
                        let label = if path.is_empty() {
                            "<idle>"
                        } else {
                            path.as_str()
                        };
                        path_lines.push(format!("{:>2}: {}", idx + 1, label));
                    }
                }
                if path_lines.is_empty() {
                    path_lines.push(current_path.clone());
                }
                let path_text = path_lines.join("\n");
                let error_count = stats.error_count.load(Ordering::Relaxed);
                let mut error_lines = Vec::new();
                error_lines.push(format!("errors: {}", error_count));
                if let Ok(buf) = stats.errors.lock() {
                    let mut recent: Vec<String> = buf.iter().rev().take(3).cloned().collect();
                    recent.reverse();
                    error_lines.extend(recent);
                }
                let error_text = error_lines.join("\n");

                let mut dup_entries = stats
                    .dup_entries
                    .lock()
                    .map(|v| v.clone())
                    .unwrap_or_default();
                match dup_sort {
                    DupSort::Gain => dup_entries.sort_by(|a, b| b.gain.cmp(&a.gain)),
                    DupSort::Count => dup_entries.sort_by(|a, b| b.count.cmp(&a.count)),
                    DupSort::Size => dup_entries.sort_by(|a, b| b.size.cmp(&a.size)),
                }
                let dup_expanded = stats
                    .dup_expanded
                    .lock()
                    .map(|s| s.clone())
                    .unwrap_or_default();
                let mut current_selection =
                    stats
                        .dup_selected
                        .lock()
                        .map(|s| s.clone())
                        .unwrap_or(DupSelection {
                            hash: None,
                            path_idx: None,
                        });
                let full_groups_guard = stats.full_groups.lock().ok();

                if current_selection.hash.is_some() {
                    let hash = current_selection.hash.clone().unwrap_or_default();
                    let has_entry = dup_entries.iter().any(|e| e.hash == hash);
                    if !has_entry {
                        current_selection = if let Some(first) = dup_entries.first() {
                            DupSelection {
                                hash: Some(first.hash.clone()),
                                path_idx: None,
                            }
                        } else {
                            DupSelection {
                                hash: None,
                                path_idx: None,
                            }
                        };
                    } else if let Some(idx) = current_selection.path_idx {
                        let expanded = dup_expanded.contains(&hash);
                        let path_len = full_groups_guard
                            .as_ref()
                            .and_then(|m| m.get(&hash))
                            .map(|(_, paths)| paths.len())
                            .unwrap_or_else(|| {
                                dup_entries
                                    .iter()
                                    .find(|e| e.hash == hash)
                                    .map(|e| e.paths.len())
                                    .unwrap_or(0)
                            });
                        if !expanded || idx >= path_len {
                            current_selection.path_idx = None;
                        }
                    }
                } else if let Some(first) = dup_entries.first() {
                    current_selection = DupSelection {
                        hash: Some(first.hash.clone()),
                        path_idx: None,
                    };
                }

                if let Ok(mut sel_lock) = stats.dup_selected.lock() {
                    *sel_lock = current_selection.clone();
                }

                let (gain_label, count_label, size_label) = match dup_sort {
                    DupSort::Gain => ("GAIN*", "CNT", "SIZE"),
                    DupSort::Count => ("GAIN", "CNT*", "SIZE"),
                    DupSort::Size => ("GAIN", "CNT", "SIZE*"),
                };

                let mut dup_lines = Vec::new();
                let mut dup_line_map: Vec<Option<DupSelection>> = Vec::new();
                dup_lines.push(format!(
                    "  {}  {}  {}  {}",
                    pad_left(gain_label, 10),
                    pad_left(count_label, 4),
                    pad_left(size_label, 10),
                    "PATH"
                ));
                dup_line_map.push(None);

                for entry in dup_entries.iter() {
                    let gain_s = format_bytes_binary_u128(entry.gain);
                    let count_s = entry.count.to_string();
                    let size_s = format_bytes_binary(entry.size);
                    let expanded = dup_expanded.contains(&entry.hash);
                    let (paths_full, group_count) = if let Some(map) = full_groups_guard.as_ref() {
                        if let Some((_, paths)) = map.get(&entry.hash) {
                            (Some(paths), paths.len())
                        } else {
                            (None, entry.count)
                        }
                    } else {
                        (None, entry.count)
                    };
                    let first_path = if let Some(paths) = paths_full {
                        paths
                            .get(0)
                            .map(|p| p.to_string_lossy().to_string())
                            .unwrap_or_else(|| "<unknown>".to_string())
                    } else {
                        entry
                            .paths
                            .get(0)
                            .cloned()
                            .unwrap_or_else(|| "<unknown>".to_string())
                    };
                    let extra = if group_count > 1 {
                        format!(" (+{})", group_count - 1)
                    } else {
                        String::new()
                    };
                    let path_display = format!("{}{}", first_path, extra);
                    let selected = if current_selection.hash.as_ref() == Some(&entry.hash)
                        && current_selection.path_idx.is_none()
                    {
                        ">"
                    } else {
                        " "
                    };
                    let toggle = if expanded { "-" } else { "+" };
                    dup_lines.push(format!(
                        "{}{} {}  {}  {}  {}",
                        selected,
                        toggle,
                        pad_left(&gain_s, 10),
                        pad_left(&count_s, 4),
                        pad_left(&size_s, 10),
                        path_display
                    ));
                    dup_line_map.push(Some(DupSelection {
                        hash: Some(entry.hash.clone()),
                        path_idx: None,
                    }));

                    if expanded {
                        let path_list: Vec<String> = if let Some(paths) = paths_full {
                            paths
                                .iter()
                                .map(|p| p.to_string_lossy().to_string())
                                .collect()
                        } else {
                            entry.paths.clone()
                        };
                        for (p_idx, p) in path_list.iter().enumerate() {
                            let selected = if current_selection.hash.as_ref() == Some(&entry.hash)
                                && current_selection.path_idx == Some(p_idx)
                            {
                                ">"
                            } else {
                                " "
                            };
                            dup_lines.push(format!("{}   - {}", selected, p));
                            dup_line_map.push(Some(DupSelection {
                                hash: Some(entry.hash.clone()),
                                path_idx: Some(p_idx),
                            }));
                        }
                        if entry.count > path_list.len() {
                            dup_lines
                                .push(format!("    ... +{} more", entry.count - path_list.len()));
                            dup_line_map.push(None);
                        }
                    }
                }

                let mut selected_line_idx = dup_line_map
                    .iter()
                    .position(|v| v.as_ref() == Some(&current_selection));
                if selected_line_idx.is_none() {
                    if let Some(hash) = current_selection.hash.as_ref() {
                        selected_line_idx = dup_line_map.iter().position(|v| {
                            v.as_ref()
                                .map(|s| s.hash.as_ref() == Some(hash) && s.path_idx.is_none())
                                .unwrap_or(false)
                        });
                    }
                    if selected_line_idx.is_none() {
                        selected_line_idx = dup_line_map.iter().position(|v| v.is_some());
                    }
                    if let Some(idx) = selected_line_idx {
                        if let Some(sel) = dup_line_map.get(idx).and_then(|v| v.clone()) {
                            if let Ok(mut sel_lock) = stats.dup_selected.lock() {
                                *sel_lock = sel;
                            }
                        }
                    }
                }
                let selected_line_idx = selected_line_idx.unwrap_or(0);
                let selectable_indices: Vec<usize> = dup_line_map
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, sel)| sel.as_ref().map(|_| idx))
                    .collect();
                let selected_pos = selectable_indices
                    .iter()
                    .position(|idx| *idx == selected_line_idx)
                    .unwrap_or(0);
                drop(full_groups_guard);

                let stage = stats.dup_stage.load(Ordering::Relaxed);
                let sort_label = match dup_sort {
                    DupSort::Gain => "gain",
                    DupSort::Count => "count",
                    DupSort::Size => "size",
                };
                let base_title = match stage {
                    2 => format!("Top candidates (size, sort: {})", sort_label),
                    1 => format!("Top candidates (partial, sort: {})", sort_label),
                    _ => format!("Top duplicates (sort: {})", sort_label),
                };

                let _ = terminal.draw(|f| {
                    let size = f.size();
                    let chunks = Layout::default()
                        .direction(Direction::Vertical)
                        .constraints([
                            Constraint::Length(3),
                            Constraint::Length(3),
                            Constraint::Length(3),
                            Constraint::Length(6),
                            Constraint::Length(3),
                            Constraint::Length(3),
                            Constraint::Length(5),
                            Constraint::Length(4),
                            Constraint::Min(6),
                        ])
                        .split(size);

                    let partial = Gauge::default()
                        .block(Block::default().title("Partial pass").borders(Borders::ALL))
                        .gauge_style(Style::default().fg(Color::Cyan))
                        .ratio(partial_ratio);
                    f.render_widget(partial, chunks[0]);

                    let full = Gauge::default()
                        .block(Block::default().title("Full pass").borders(Borders::ALL))
                        .gauge_style(Style::default().fg(Color::Green))
                        .ratio(full_ratio);
                    f.render_widget(full, chunks[1]);

                    let speed = Paragraph::new(info.clone())
                        .block(Block::default().borders(Borders::ALL).title("Speed"));
                    f.render_widget(speed, chunks[2]);

                    let spark_data: Vec<u64> = speed_series.clone().into_iter().collect::<Vec<_>>();
                    let max_speed = spark_data.iter().cloned().max().unwrap_or(1).max(1);
                    let spark_top = max_speed as f64 / 10.0;
                    let spark = Sparkline::default()
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .title(format!("Files/s (~6s) top {:.1}", spark_top)),
                        )
                        .style(Style::default().fg(Color::Magenta))
                        .max(max_speed)
                        .data(&spark_data);
                    f.render_widget(spark, chunks[3]);

                    let settings_p = Paragraph::new(settings_line.clone())
                        .block(Block::default().borders(Borders::ALL).title("Settings"));
                    f.render_widget(settings_p, chunks[4]);

                    let status_p = Paragraph::new(status_line.clone())
                        .block(Block::default().borders(Borders::ALL).title("Status"));
                    f.render_widget(status_p, chunks[5]);

                    let errors_p = Paragraph::new(error_text.clone())
                        .block(Block::default().borders(Borders::ALL).title("Errors"));
                    f.render_widget(errors_p, chunks[6]);

                    let path_p = Paragraph::new(path_text.clone()).block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title("Current files (press q to hide)"),
                    );
                    f.render_widget(path_p, chunks[7]);

                    let dup_height = chunks[8].height.saturating_sub(2).max(1) as usize;
                    last_dup_height = dup_height;
                    let mut dup_scroll = stats.dup_scroll.load(Ordering::Relaxed) as usize;
                    let max_scroll = dup_lines.len().saturating_sub(dup_height);
                    if selected_line_idx < dup_scroll {
                        dup_scroll = selected_line_idx;
                    } else if selected_line_idx >= dup_scroll + dup_height {
                        dup_scroll = selected_line_idx.saturating_sub(dup_height - 1);
                    }
                    dup_scroll = dup_scroll.min(max_scroll);
                    stats.dup_scroll.store(dup_scroll as u64, Ordering::Relaxed);
                    let shown = dup_lines
                        .iter()
                        .skip(dup_scroll)
                        .take(dup_height)
                        .cloned()
                        .collect::<Vec<_>>();

                    let dup_title = if dup_entries.is_empty() {
                        base_title.to_string()
                    } else {
                        format!(
                            "{} ({} shown, scroll {}/{})",
                            base_title,
                            shown.len(),
                            dup_scroll,
                            max_scroll
                        )
                    };
                    let dup_p = Paragraph::new(shown.join("\n"))
                        .block(Block::default().borders(Borders::ALL).title(dup_title));
                    f.render_widget(dup_p, chunks[8]);

                    if let Some(confirm) = &confirm_state {
                        let confirm_height =
                            (confirm.body.len() + 6).min(size.height as usize) as u16;
                        let confirm_width = size.width.saturating_sub(6).min(84);
                        let area = centered_rect_fixed(confirm_width, confirm_height, size);
                        let mut lines = Vec::new();
                        lines.push(confirm.title.clone());
                        lines.push(String::new());
                        lines.extend(confirm.body.clone());
                        lines.push(String::new());
                        lines.push("[y]es  [n]o  [a]lways (don't ask again)".to_string());
                        let panel = Paragraph::new(lines.join("\n"))
                            .block(Block::default().borders(Borders::ALL).title("Confirm"));
                        f.render_widget(Clear, area);
                        f.render_widget(panel, area);
                    }
                });

                if stats.finished.load(Ordering::Relaxed) || stats.aborting.load(Ordering::Relaxed)
                {
                    break;
                }
                let apply_action =
                    |action: ConfirmAction| {
                        let (hash, deleted_paths, failed, msg_prefix) = match action {
                            ConfirmAction::Delete {
                                hash,
                                targets,
                                permanent,
                            } => {
                                let res = delete_paths(&targets, permanent);
                                let mode = if permanent { "permanent" } else { "trash" };
                                let msg = format!(
                                    "Deleted {}/{} file ({})",
                                    res.deleted.len(),
                                    targets.len(),
                                    mode
                                );
                                (hash, res.deleted, res.failed, msg)
                            }
                            ConfirmAction::Keep {
                                hash,
                                keep,
                                delete,
                                permanent,
                            } => {
                                let res = delete_paths(&delete, permanent);
                                let mode = if permanent { "permanent" } else { "trash" };
                                let msg = format!(
                                    "Kept {}, deleted {}/{} files ({})",
                                    keep.display(),
                                    res.deleted.len(),
                                    delete.len(),
                                    mode
                                );
                                (hash, res.deleted, res.failed, msg)
                            }
                        };

                        if let Ok(mut groups) = stats.full_groups.lock() {
                            if let Some((_, paths)) = groups.get_mut(&hash) {
                                if !deleted_paths.is_empty() {
                                    let deleted_set: HashSet<PathBuf> =
                                        deleted_paths.iter().cloned().collect();
                                    paths.retain(|p| !deleted_set.contains(p));
                                }
                                if paths.len() <= 1 {
                                    groups.remove(&hash);
                                }
                            }
                            let rebuilt = rebuild_entries_from_groups(&groups);
                            if let Ok(mut lock) = stats.dup_entries.lock() {
                                *lock = rebuilt.clone();
                            }
                            if let Ok(mut expanded) = stats.dup_expanded.lock() {
                                if !groups.contains_key(&hash) {
                                    expanded.remove(&hash);
                                }
                            }
                            if let Ok(mut waste) = stats.potential_waste.lock() {
                                *waste = recompute_potential_waste(&groups);
                            }
                            let current_sel =
                                stats.dup_selected.lock().map(|s| s.clone()).unwrap_or(
                                    DupSelection {
                                        hash: None,
                                        path_idx: None,
                                    },
                                );
                            let mut new_sel = current_sel.clone();
                            if let Some(sel_hash) = current_sel.hash.clone() {
                                if let Some((_, paths)) = groups.get(&sel_hash) {
                                    if let Some(idx) = current_sel.path_idx {
                                        if idx >= paths.len() {
                                            new_sel = DupSelection {
                                                hash: Some(sel_hash),
                                                path_idx: None,
                                            };
                                        }
                                    }
                                } else {
                                    new_sel = DupSelection {
                                        hash: None,
                                        path_idx: None,
                                    };
                                }
                            }
                            if new_sel.hash.is_none() {
                                if let Some(first) = rebuilt.first() {
                                    new_sel = DupSelection {
                                        hash: Some(first.hash.clone()),
                                        path_idx: None,
                                    };
                                }
                            }
                            if let Ok(mut sel_lock) = stats.dup_selected.lock() {
                                *sel_lock = new_sel;
                            }
                        }

                        if !failed.is_empty() {
                            stats
                                .error_count
                                .fetch_add(failed.len() as u64, Ordering::Relaxed);
                            if let Ok(mut buf) = stats.errors.lock() {
                                for msg in failed.iter().take(5) {
                                    if buf.len() == ERROR_BUFFER_LIMIT {
                                        buf.pop_front();
                                    }
                                    buf.push_back(msg.clone());
                                }
                            }
                        }

                        let mut status = msg_prefix;
                        if !failed.is_empty() {
                            status = format!("{} ({} failed)", status, failed.len());
                        }
                        if let Ok(mut status_line) = stats.status_line.lock() {
                            *status_line = status;
                        }
                    };

                if event::poll(Duration::from_millis(100)).unwrap_or(false) {
                    if let Ok(Event::Key(k)) = event::read() {
                        if let Some(state) = confirm_state.take() {
                            match k.code {
                                KeyCode::Char('y') => {
                                    apply_action(state.action);
                                }
                                KeyCode::Char('a') => {
                                    confirm_skip = true;
                                    apply_action(state.action);
                                }
                                KeyCode::Char('n') | KeyCode::Esc => {}
                                _ => {
                                    confirm_state = Some(state);
                                }
                            }
                            continue;
                        }

                        if k.code == KeyCode::Char('q') {
                            break;
                        }
                        let scroll_amt = last_dup_height.max(1);
                        if k.code == KeyCode::Char('c')
                            && k.modifiers.contains(KeyModifiers::CONTROL)
                        {
                            if stats.aborting.fetch_or(true, Ordering::SeqCst) {
                                break;
                            } else {
                                // mirror signal handler behavior
                                stats.finished.store(true, Ordering::SeqCst);
                            }
                        } else if k.code == KeyCode::Char('s') {
                            dup_sort = match dup_sort {
                                DupSort::Gain => DupSort::Count,
                                DupSort::Count => DupSort::Size,
                                DupSort::Size => DupSort::Gain,
                            };
                        } else if k.code == KeyCode::Up {
                            if !selectable_indices.is_empty() {
                                let new_pos = selected_pos.saturating_sub(1);
                                if let Some(sel) = dup_line_map
                                    .get(selectable_indices[new_pos])
                                    .and_then(|v| v.clone())
                                {
                                    if let Ok(mut sel_lock) = stats.dup_selected.lock() {
                                        *sel_lock = sel;
                                    }
                                }
                            }
                        } else if k.code == KeyCode::Down {
                            if !selectable_indices.is_empty() {
                                let new_pos = (selected_pos + 1).min(selectable_indices.len() - 1);
                                if let Some(sel) = dup_line_map
                                    .get(selectable_indices[new_pos])
                                    .and_then(|v| v.clone())
                                {
                                    if let Ok(mut sel_lock) = stats.dup_selected.lock() {
                                        *sel_lock = sel;
                                    }
                                }
                            }
                        } else if k.code == KeyCode::PageUp {
                            if !selectable_indices.is_empty() {
                                let new_pos = selected_pos.saturating_sub(scroll_amt);
                                if let Some(sel) = dup_line_map
                                    .get(selectable_indices[new_pos])
                                    .and_then(|v| v.clone())
                                {
                                    if let Ok(mut sel_lock) = stats.dup_selected.lock() {
                                        *sel_lock = sel;
                                    }
                                }
                            }
                        } else if k.code == KeyCode::PageDown {
                            if !selectable_indices.is_empty() {
                                let new_pos =
                                    (selected_pos + scroll_amt).min(selectable_indices.len() - 1);
                                if let Some(sel) = dup_line_map
                                    .get(selectable_indices[new_pos])
                                    .and_then(|v| v.clone())
                                {
                                    if let Ok(mut sel_lock) = stats.dup_selected.lock() {
                                        *sel_lock = sel;
                                    }
                                }
                            }
                        } else if k.code == KeyCode::Home {
                            if let Some(sel) = selectable_indices
                                .first()
                                .and_then(|idx| dup_line_map.get(*idx))
                                .and_then(|v| v.clone())
                            {
                                if let Ok(mut sel_lock) = stats.dup_selected.lock() {
                                    *sel_lock = sel;
                                }
                            }
                        } else if k.code == KeyCode::End {
                            if let Some(sel) = selectable_indices
                                .last()
                                .and_then(|idx| dup_line_map.get(*idx))
                                .and_then(|v| v.clone())
                            {
                                if let Ok(mut sel_lock) = stats.dup_selected.lock() {
                                    *sel_lock = sel;
                                }
                            }
                        } else if k.code == KeyCode::Enter || k.code == KeyCode::Char(' ') {
                            let selection = stats.dup_selected.lock().map(|s| s.clone()).unwrap_or(
                                DupSelection {
                                    hash: None,
                                    path_idx: None,
                                },
                            );
                            if let Some(hash) = selection.hash {
                                if let Ok(mut expanded) = stats.dup_expanded.lock() {
                                    if expanded.contains(&hash) {
                                        expanded.remove(&hash);
                                    } else {
                                        expanded.insert(hash);
                                    }
                                }
                            }
                        } else if matches!(k.code, KeyCode::Char('d') | KeyCode::Char('D'))
                            || matches!(k.code, KeyCode::Char('k') | KeyCode::Char('K'))
                        {
                            let stage = stats.dup_stage.load(Ordering::Relaxed);
                            if stage != 0 {
                                if let Ok(mut status) = stats.status_line.lock() {
                                    *status =
                                        "Delete only available for full duplicates".to_string();
                                }
                                continue;
                            }
                            let selection = stats.dup_selected.lock().map(|s| s.clone()).unwrap_or(
                                DupSelection {
                                    hash: None,
                                    path_idx: None,
                                },
                            );
                            let Some(hash) = selection.hash.clone() else {
                                if let Ok(mut status) = stats.status_line.lock() {
                                    *status = "No duplicate selected".to_string();
                                }
                                continue;
                            };
                            let paths = stats
                                .full_groups
                                .lock()
                                .ok()
                                .and_then(|m| m.get(&hash).map(|(_, p)| p.clone()))
                                .unwrap_or_default();
                            if paths.len() < 2 {
                                if let Ok(mut status) = stats.status_line.lock() {
                                    *status = "Need at least 2 copies to delete".to_string();
                                }
                                continue;
                            }
                            let mut target_idx = selection.path_idx.unwrap_or(0);
                            if target_idx >= paths.len() {
                                target_idx = 0;
                            }
                            let target = paths.get(target_idx).cloned();
                            let Some(target) = target else {
                                if let Ok(mut status) = stats.status_line.lock() {
                                    *status = "Selected file not found".to_string();
                                }
                                continue;
                            };
                            let permanent =
                                matches!(k.code, KeyCode::Char('D') | KeyCode::Char('K'));
                            let action =
                                if matches!(k.code, KeyCode::Char('d') | KeyCode::Char('D')) {
                                    ConfirmAction::Delete {
                                        hash: hash.clone(),
                                        targets: vec![target.clone()],
                                        permanent,
                                    }
                                } else {
                                    let mut delete = paths.clone();
                                    delete.retain(|p| *p != target);
                                    if delete.is_empty() {
                                        if let Ok(mut status) = stats.status_line.lock() {
                                            *status = "No other copies to delete".to_string();
                                        }
                                        continue;
                                    }
                                    ConfirmAction::Keep {
                                        hash: hash.clone(),
                                        keep: target.clone(),
                                        delete,
                                        permanent,
                                    }
                                };
                            if confirm_skip {
                                apply_action(action);
                            } else {
                                let (title, mut body) = match &action {
                                    ConfirmAction::Delete {
                                        targets, permanent, ..
                                    } => {
                                        let title = if *permanent {
                                            "Delete file permanently?".to_string()
                                        } else {
                                            "Move file to trash?".to_string()
                                        };
                                        let mut body = Vec::new();
                                        if let Some(path) = targets.first() {
                                            body.push(format!("Target: {}", path.display()));
                                        }
                                        body.push(format!(
                                            "Mode: {}",
                                            if *permanent { "permanent" } else { "trash" }
                                        ));
                                        (title, body)
                                    }
                                    ConfirmAction::Keep {
                                        keep,
                                        delete,
                                        permanent,
                                        ..
                                    } => {
                                        let title = if *permanent {
                                            "Keep one, delete others permanently?".to_string()
                                        } else {
                                            "Keep one, move others to trash?".to_string()
                                        };
                                        let mut body = Vec::new();
                                        body.push(format!("Keep: {}", keep.display()));
                                        body.push(format!("Delete: {} files", delete.len()));
                                        for p in delete.iter().take(5) {
                                            body.push(format!(" - {}", p.display()));
                                        }
                                        if delete.len() > 5 {
                                            body.push(format!(" ... +{} more", delete.len() - 5));
                                        }
                                        body.push(format!(
                                            "Mode: {}",
                                            if *permanent { "permanent" } else { "trash" }
                                        ));
                                        (title, body)
                                    }
                                };
                                if body.is_empty() {
                                    body.push("Proceed with deletion?".to_string());
                                }
                                confirm_state = Some(ConfirmState {
                                    title,
                                    body,
                                    action,
                                });
                            }
                        }
                    }
                }
            }
            let _ = disable_raw_mode();
            let mut out = stdout();
            let _ = execute!(out, LeaveAlternateScreen);
        });
        Ok(handle)
    })();

    match terminal_result {
        Ok(h) => Some(h),
        Err(_) => None,
    }
}
