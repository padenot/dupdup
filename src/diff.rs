use crate::config::DiffConfig;
use crate::diagnostics::ErrorLog;
use crate::file_tools::{collect_inventory, HashCache, InventoryEntryKind};
use crate::util::format_bytes_binary;
use anyhow::{Context, Result};
use atty::Stream;
use rayon::prelude::*;
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Once};
use std::time::Instant;
use time::{macros::format_description, OffsetDateTime};
use tracing::info;
use tracing_subscriber::fmt::time::OffsetTime;
use tracing_subscriber::EnvFilter;

const DIFF_REPORT_FORMAT: &str = "dupdup-tree-diff/v1";
const DIFF_SCHEMA_ID: &str =
    "https://github.com/padenot/dupdup/blob/main/schemas/tree-diff-v1.schema.json";

#[derive(Debug, Serialize, Clone, Default)]
pub struct DiffStats {
    pub only_in_a: usize,
    pub only_in_b: usize,
    pub same_path_same_content: usize,
    pub same_path_different_content: usize,
    pub same_path_different_metadata: usize,
    pub type_mismatch: usize,
    pub relocation_groups: usize,
    pub relocated_paths_a: usize,
    pub relocated_paths_b: usize,
    pub comparison_errors: usize,
    pub duration_seconds: f64,
    pub error_log: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum EntryKind {
    File,
    Directory,
    Symlink,
    Other,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "kebab-case")]
enum PathDiffStatus {
    OnlyInA,
    OnlyInB,
    SamePathSameContent,
    SamePathDifferentContent,
    SamePathDifferentMetadata,
    TypeMismatch,
    ComparisonError,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "kebab-case")]
enum ComparisonBasis {
    Size,
    PartialHash,
    FullHash,
    LinkTarget,
    EntryType,
    ReadError,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum ReportRecord {
    Meta {
        format: &'static str,
        schema_id: &'static str,
        root_a: PathBuf,
        root_b: PathBuf,
        partial_bytes: usize,
        block_size: usize,
    },
    PathDiff {
        path: String,
        status: PathDiffStatus,
        entry_type: Option<EntryKind>,
        type_a: Option<EntryKind>,
        type_b: Option<EntryKind>,
        size_a: Option<u64>,
        size_b: Option<u64>,
        mtime_a: Option<i64>,
        mtime_b: Option<i64>,
        link_target_a: Option<String>,
        link_target_b: Option<String>,
        comparison_basis: Option<ComparisonBasis>,
        partial_hash_a: Option<String>,
        partial_hash_b: Option<String>,
        hash_a: Option<String>,
        hash_b: Option<String>,
        metadata_differences: Vec<String>,
        note: Option<String>,
    },
    Relocation {
        status: &'static str,
        entry_type: EntryKind,
        content_hash: String,
        size: u64,
        a_paths: Vec<String>,
        b_paths: Vec<String>,
        comparison_basis: ComparisonBasis,
    },
    Summary {
        stats: DiffStats,
    },
}

#[derive(Debug, Clone)]
struct TreeEntry {
    rel_path: String,
    abs_path: PathBuf,
    kind: EntryKind,
    size: Option<u64>,
    mtime: Option<i64>,
    link_target: Option<String>,
}

#[derive(Debug, Clone)]
struct FileCandidate {
    path: String,
    a: TreeEntry,
    b: TreeEntry,
}

#[derive(Debug, Clone)]
struct UnmatchedFile {
    rel_path: String,
    abs_path: PathBuf,
    size: u64,
    mtime: i64,
}

#[derive(Debug)]
enum SamePathOutcome {
    SameContent(ReportRecord),
    MetadataDiff(ReportRecord),
    ContentDiff(ReportRecord),
    ComparisonError(ReportRecord),
}

fn init_tracing() {
    static LOGGER_INIT: Once = Once::new();
    LOGGER_INIT.call_once(|| {
        let timer = OffsetTime::new(
            time::UtcOffset::UTC,
            time::format_description::well_known::Rfc3339,
        );
        let default_directive = if atty::is(Stream::Stderr) {
            "dupdup=info"
        } else {
            "dupdup=warn"
        };
        let filter = EnvFilter::try_from_default_env()
            .or_else(|_| EnvFilter::try_new(default_directive))
            .unwrap_or_else(|_| EnvFilter::new("info"));
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_timer(timer)
            .with_target(false)
            .with_ansi(atty::is(Stream::Stderr))
            .try_init();
    });
}

fn entry_kind(kind: InventoryEntryKind) -> EntryKind {
    match kind {
        InventoryEntryKind::File => EntryKind::File,
        InventoryEntryKind::Directory => EntryKind::Directory,
        InventoryEntryKind::Symlink => EntryKind::Symlink,
        InventoryEntryKind::Other => EntryKind::Other,
    }
}

fn collect_tree(root: &Path, errors: &Arc<ErrorLog>) -> Result<HashMap<String, TreeEntry>> {
    let inventory = collect_inventory(
        root,
        1,
        false,
        |_scanned, _files, _last| {},
        |msg| errors.log("diff", msg),
    );
    let mut entries = HashMap::new();
    for entry in inventory {
        let kind = entry_kind(entry.kind);
        entries.insert(
            entry.rel_path.clone(),
            TreeEntry {
                rel_path: entry.rel_path,
                abs_path: entry.abs_path,
                kind,
                size: entry.size,
                mtime: entry.mtime,
                link_target: entry.link_target,
            },
        );
    }
    Ok(entries)
}

fn cached_partial_hash(
    cache: &HashCache,
    path: &Path,
    partial_bytes: usize,
    block_size: usize,
) -> Result<String> {
    cache.partial_hash(path, partial_bytes, block_size)
}

fn cached_full_hash(cache: &HashCache, path: &Path, block_size: usize) -> Result<String> {
    cache.full_hash(path, block_size)
}

fn metadata_diff_fields(a: &TreeEntry, b: &TreeEntry) -> Vec<String> {
    let mut fields = Vec::new();
    if a.mtime != b.mtime {
        fields.push("mtime".to_string());
    }
    if a.link_target != b.link_target {
        fields.push("link-target".to_string());
    }
    fields
}

fn make_path_diff_record(
    path: String,
    status: PathDiffStatus,
    entry_type: Option<EntryKind>,
    type_a: Option<EntryKind>,
    type_b: Option<EntryKind>,
    size_a: Option<u64>,
    size_b: Option<u64>,
    mtime_a: Option<i64>,
    mtime_b: Option<i64>,
    link_target_a: Option<String>,
    link_target_b: Option<String>,
    comparison_basis: Option<ComparisonBasis>,
    partial_hash_a: Option<String>,
    partial_hash_b: Option<String>,
    hash_a: Option<String>,
    hash_b: Option<String>,
    metadata_differences: Vec<String>,
    note: Option<String>,
) -> ReportRecord {
    ReportRecord::PathDiff {
        path,
        status,
        entry_type,
        type_a,
        type_b,
        size_a,
        size_b,
        mtime_a,
        mtime_b,
        link_target_a,
        link_target_b,
        comparison_basis,
        partial_hash_a,
        partial_hash_b,
        hash_a,
        hash_b,
        metadata_differences,
        note,
    }
}

fn evaluate_same_path_candidate(
    candidate: &FileCandidate,
    partial_bytes: usize,
    block_size: usize,
    cache: &HashCache,
    errors: &Arc<ErrorLog>,
) -> SamePathOutcome {
    let size_a = candidate.a.size.unwrap_or(0);
    let size_b = candidate.b.size.unwrap_or(0);
    if size_a != size_b {
        return SamePathOutcome::ContentDiff(make_path_diff_record(
            candidate.path.clone(),
            PathDiffStatus::SamePathDifferentContent,
            Some(EntryKind::File),
            None,
            None,
            Some(size_a),
            Some(size_b),
            candidate.a.mtime,
            candidate.b.mtime,
            None,
            None,
            Some(ComparisonBasis::Size),
            None,
            None,
            None,
            None,
            Vec::new(),
            None,
        ));
    }

    let mut partial_a = None;
    let mut partial_b = None;
    if partial_bytes > 0 {
        match cached_partial_hash(cache, &candidate.a.abs_path, partial_bytes, block_size) {
            Ok(value) => partial_a = Some(value),
            Err(err) => {
                let msg = format!(
                    "partial hash failed for {}: {}",
                    candidate.a.abs_path.display(),
                    err
                );
                errors.log("diff", &msg);
                return SamePathOutcome::ComparisonError(make_path_diff_record(
                    candidate.path.clone(),
                    PathDiffStatus::ComparisonError,
                    Some(EntryKind::File),
                    None,
                    None,
                    Some(size_a),
                    Some(size_b),
                    candidate.a.mtime,
                    candidate.b.mtime,
                    None,
                    None,
                    Some(ComparisonBasis::ReadError),
                    None,
                    None,
                    None,
                    None,
                    Vec::new(),
                    Some(msg),
                ));
            }
        }
        match cached_partial_hash(cache, &candidate.b.abs_path, partial_bytes, block_size) {
            Ok(value) => partial_b = Some(value),
            Err(err) => {
                let msg = format!(
                    "partial hash failed for {}: {}",
                    candidate.b.abs_path.display(),
                    err
                );
                errors.log("diff", &msg);
                return SamePathOutcome::ComparisonError(make_path_diff_record(
                    candidate.path.clone(),
                    PathDiffStatus::ComparisonError,
                    Some(EntryKind::File),
                    None,
                    None,
                    Some(size_a),
                    Some(size_b),
                    candidate.a.mtime,
                    candidate.b.mtime,
                    None,
                    None,
                    Some(ComparisonBasis::ReadError),
                    partial_a,
                    None,
                    None,
                    None,
                    Vec::new(),
                    Some(msg),
                ));
            }
        }
        if partial_a != partial_b {
            return SamePathOutcome::ContentDiff(make_path_diff_record(
                candidate.path.clone(),
                PathDiffStatus::SamePathDifferentContent,
                Some(EntryKind::File),
                None,
                None,
                Some(size_a),
                Some(size_b),
                candidate.a.mtime,
                candidate.b.mtime,
                None,
                None,
                Some(ComparisonBasis::PartialHash),
                partial_a,
                partial_b,
                None,
                None,
                Vec::new(),
                None,
            ));
        }
    }

    let hash_a = match cached_full_hash(cache, &candidate.a.abs_path, block_size) {
        Ok(value) => value,
        Err(err) => {
            let msg = format!(
                "full hash failed for {}: {}",
                candidate.a.abs_path.display(),
                err
            );
            errors.log("diff", &msg);
            return SamePathOutcome::ComparisonError(make_path_diff_record(
                candidate.path.clone(),
                PathDiffStatus::ComparisonError,
                Some(EntryKind::File),
                None,
                None,
                Some(size_a),
                Some(size_b),
                candidate.a.mtime,
                candidate.b.mtime,
                None,
                None,
                Some(ComparisonBasis::ReadError),
                partial_a,
                partial_b,
                None,
                None,
                Vec::new(),
                Some(msg),
            ));
        }
    };
    let hash_b = match cached_full_hash(cache, &candidate.b.abs_path, block_size) {
        Ok(value) => value,
        Err(err) => {
            let msg = format!(
                "full hash failed for {}: {}",
                candidate.b.abs_path.display(),
                err
            );
            errors.log("diff", &msg);
            return SamePathOutcome::ComparisonError(make_path_diff_record(
                candidate.path.clone(),
                PathDiffStatus::ComparisonError,
                Some(EntryKind::File),
                None,
                None,
                Some(size_a),
                Some(size_b),
                candidate.a.mtime,
                candidate.b.mtime,
                None,
                None,
                Some(ComparisonBasis::ReadError),
                partial_a,
                partial_b,
                Some(hash_a),
                None,
                Vec::new(),
                Some(msg),
            ));
        }
    };

    if hash_a != hash_b {
        return SamePathOutcome::ContentDiff(make_path_diff_record(
            candidate.path.clone(),
            PathDiffStatus::SamePathDifferentContent,
            Some(EntryKind::File),
            None,
            None,
            Some(size_a),
            Some(size_b),
            candidate.a.mtime,
            candidate.b.mtime,
            None,
            None,
            Some(ComparisonBasis::FullHash),
            partial_a,
            partial_b,
            Some(hash_a),
            Some(hash_b),
            Vec::new(),
            None,
        ));
    }

    let metadata_differences = metadata_diff_fields(&candidate.a, &candidate.b);
    if metadata_differences.is_empty() {
        SamePathOutcome::SameContent(make_path_diff_record(
            candidate.path.clone(),
            PathDiffStatus::SamePathSameContent,
            Some(EntryKind::File),
            None,
            None,
            Some(size_a),
            Some(size_b),
            candidate.a.mtime,
            candidate.b.mtime,
            None,
            None,
            Some(ComparisonBasis::FullHash),
            partial_a,
            partial_b,
            Some(hash_a),
            Some(hash_b),
            Vec::new(),
            None,
        ))
    } else {
        SamePathOutcome::MetadataDiff(make_path_diff_record(
            candidate.path.clone(),
            PathDiffStatus::SamePathDifferentMetadata,
            Some(EntryKind::File),
            None,
            None,
            Some(size_a),
            Some(size_b),
            candidate.a.mtime,
            candidate.b.mtime,
            None,
            None,
            Some(ComparisonBasis::FullHash),
            partial_a,
            partial_b,
            Some(hash_a),
            Some(hash_b),
            metadata_differences,
            None,
        ))
    }
}

fn group_unmatched_by_size(files: &[UnmatchedFile]) -> HashMap<u64, Vec<UnmatchedFile>> {
    let mut map = HashMap::new();
    for file in files {
        map.entry(file.size)
            .or_insert_with(Vec::new)
            .push(file.clone());
    }
    map
}

pub fn run_diff(cfg: DiffConfig) -> Result<DiffStats> {
    init_tracing();
    let start = Instant::now();

    let thread_count = if cfg.threads == 0 {
        num_cpus::get()
    } else {
        cfg.threads
    };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(thread_count.max(1))
        .build()?;

    let stamp = OffsetDateTime::now_utc()
        .format(&format_description!(
            "[year]-[month]-[day]_[hour][minute][second]"
        ))
        .unwrap_or_else(|_| "timestamp".to_string());
    let error_path = cfg
        .error
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("diff-error-{}.log", stamp)));
    let errors = Arc::new(ErrorLog::new(&error_path)?);

    info!(
        root_a = %cfg.a.display(),
        root_b = %cfg.b.display(),
        output = %cfg.output.display(),
        partial = cfg.partial_bytes,
        block = cfg.block_size,
        threads = thread_count,
        "starting tree diff"
    );

    let tree_a = collect_tree(&cfg.a, &errors)?;
    let tree_b = collect_tree(&cfg.b, &errors)?;
    info!(
        entries_a = tree_a.len(),
        entries_b = tree_b.len(),
        "tree inventory complete"
    );

    let mut keys = BTreeSet::new();
    keys.extend(tree_a.keys().cloned());
    keys.extend(tree_b.keys().cloned());

    let mut records = Vec::new();
    records.push(ReportRecord::Meta {
        format: DIFF_REPORT_FORMAT,
        schema_id: DIFF_SCHEMA_ID,
        root_a: cfg.a.clone(),
        root_b: cfg.b.clone(),
        partial_bytes: cfg.partial_bytes,
        block_size: cfg.block_size,
    });

    let mut stats = DiffStats {
        error_log: Some(error_path.clone()),
        ..DiffStats::default()
    };

    let mut same_path_candidates = Vec::new();
    let mut unmatched_files_a = Vec::new();
    let mut unmatched_files_b = Vec::new();

    for key in keys {
        match (tree_a.get(&key), tree_b.get(&key)) {
            (Some(a), None) => {
                if a.kind == EntryKind::File {
                    unmatched_files_a.push(UnmatchedFile {
                        rel_path: a.rel_path.clone(),
                        abs_path: a.abs_path.clone(),
                        size: a.size.unwrap_or(0),
                        mtime: a.mtime.unwrap_or(0),
                    });
                } else {
                    records.push(make_path_diff_record(
                        key,
                        PathDiffStatus::OnlyInA,
                        Some(a.kind),
                        Some(a.kind),
                        None,
                        a.size,
                        None,
                        a.mtime,
                        None,
                        a.link_target.clone(),
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        Vec::new(),
                        None,
                    ));
                    stats.only_in_a += 1;
                }
            }
            (None, Some(b)) => {
                if b.kind == EntryKind::File {
                    unmatched_files_b.push(UnmatchedFile {
                        rel_path: b.rel_path.clone(),
                        abs_path: b.abs_path.clone(),
                        size: b.size.unwrap_or(0),
                        mtime: b.mtime.unwrap_or(0),
                    });
                } else {
                    records.push(make_path_diff_record(
                        key,
                        PathDiffStatus::OnlyInB,
                        Some(b.kind),
                        None,
                        Some(b.kind),
                        None,
                        b.size,
                        None,
                        b.mtime,
                        None,
                        b.link_target.clone(),
                        None,
                        None,
                        None,
                        None,
                        None,
                        Vec::new(),
                        None,
                    ));
                    stats.only_in_b += 1;
                }
            }
            (Some(a), Some(b)) => {
                if a.kind != b.kind {
                    records.push(make_path_diff_record(
                        key,
                        PathDiffStatus::TypeMismatch,
                        None,
                        Some(a.kind),
                        Some(b.kind),
                        a.size,
                        b.size,
                        a.mtime,
                        b.mtime,
                        a.link_target.clone(),
                        b.link_target.clone(),
                        Some(ComparisonBasis::EntryType),
                        None,
                        None,
                        None,
                        None,
                        Vec::new(),
                        None,
                    ));
                    stats.type_mismatch += 1;
                    continue;
                }

                match a.kind {
                    EntryKind::Directory => {
                        // Directories are considered equal by path/kind; descendants carry the diff.
                    }
                    EntryKind::Symlink | EntryKind::Other => {
                        if a.link_target != b.link_target {
                            records.push(make_path_diff_record(
                                key,
                                PathDiffStatus::SamePathDifferentContent,
                                Some(a.kind),
                                None,
                                None,
                                a.size,
                                b.size,
                                a.mtime,
                                b.mtime,
                                a.link_target.clone(),
                                b.link_target.clone(),
                                Some(ComparisonBasis::LinkTarget),
                                None,
                                None,
                                None,
                                None,
                                Vec::new(),
                                None,
                            ));
                            stats.same_path_different_content += 1;
                        } else {
                            let metadata_differences = metadata_diff_fields(a, b);
                            if metadata_differences.is_empty() {
                                records.push(make_path_diff_record(
                                    key,
                                    PathDiffStatus::SamePathSameContent,
                                    Some(a.kind),
                                    None,
                                    None,
                                    a.size,
                                    b.size,
                                    a.mtime,
                                    b.mtime,
                                    a.link_target.clone(),
                                    b.link_target.clone(),
                                    Some(ComparisonBasis::LinkTarget),
                                    None,
                                    None,
                                    None,
                                    None,
                                    Vec::new(),
                                    None,
                                ));
                                stats.same_path_same_content += 1;
                            } else {
                                records.push(make_path_diff_record(
                                    key,
                                    PathDiffStatus::SamePathDifferentMetadata,
                                    Some(a.kind),
                                    None,
                                    None,
                                    a.size,
                                    b.size,
                                    a.mtime,
                                    b.mtime,
                                    a.link_target.clone(),
                                    b.link_target.clone(),
                                    Some(ComparisonBasis::LinkTarget),
                                    None,
                                    None,
                                    None,
                                    None,
                                    metadata_differences,
                                    None,
                                ));
                                stats.same_path_different_metadata += 1;
                            }
                        }
                    }
                    EntryKind::File => {
                        same_path_candidates.push(FileCandidate {
                            path: key,
                            a: a.clone(),
                            b: b.clone(),
                        });
                    }
                }
            }
            (None, None) => {}
        }
    }

    let hash_cache = Arc::new(HashCache::default());
    let same_path_outcomes: Vec<SamePathOutcome> = pool.install(|| {
        same_path_candidates
            .par_iter()
            .map(|candidate| {
                evaluate_same_path_candidate(
                    candidate,
                    cfg.partial_bytes,
                    cfg.block_size,
                    &hash_cache,
                    &errors,
                )
            })
            .collect()
    });

    for outcome in same_path_outcomes {
        match outcome {
            SamePathOutcome::SameContent(record) => {
                records.push(record);
                stats.same_path_same_content += 1;
            }
            SamePathOutcome::MetadataDiff(record) => {
                records.push(record);
                stats.same_path_different_metadata += 1;
            }
            SamePathOutcome::ContentDiff(record) => {
                records.push(record);
                stats.same_path_different_content += 1;
            }
            SamePathOutcome::ComparisonError(record) => {
                records.push(record);
                stats.comparison_errors += 1;
            }
        }
    }

    let by_size_a = group_unmatched_by_size(&unmatched_files_a);
    let by_size_b = group_unmatched_by_size(&unmatched_files_b);
    let mut relocated_a = BTreeSet::new();
    let mut relocated_b = BTreeSet::new();

    let common_sizes: Vec<u64> = by_size_a
        .keys()
        .filter(|size| by_size_b.contains_key(size))
        .copied()
        .collect();

    for size in common_sizes {
        let Some(group_a) = by_size_a.get(&size) else {
            continue;
        };
        let Some(group_b) = by_size_b.get(&size) else {
            continue;
        };

        let partial_pairs: Vec<(UnmatchedFile, String, bool)> = pool.install(|| {
            group_a
                .par_iter()
                .map(|file| {
                    let key = if cfg.partial_bytes == 0 {
                        format!("size-only:{}", file.size)
                    } else {
                        match cached_partial_hash(
                            &hash_cache,
                            &file.abs_path,
                            cfg.partial_bytes,
                            cfg.block_size,
                        ) {
                            Ok(value) => value,
                            Err(err) => {
                                errors.log(
                                    "diff",
                                    format!(
                                        "partial hash failed for {}: {}",
                                        file.abs_path.display(),
                                        err
                                    ),
                                );
                                String::new()
                            }
                        }
                    };
                    (file.clone(), key, true)
                })
                .chain(group_b.par_iter().map(|file| {
                    let key = if cfg.partial_bytes == 0 {
                        format!("size-only:{}", file.size)
                    } else {
                        match cached_partial_hash(
                            &hash_cache,
                            &file.abs_path,
                            cfg.partial_bytes,
                            cfg.block_size,
                        ) {
                            Ok(value) => value,
                            Err(err) => {
                                errors.log(
                                    "diff",
                                    format!(
                                        "partial hash failed for {}: {}",
                                        file.abs_path.display(),
                                        err
                                    ),
                                );
                                String::new()
                            }
                        }
                    };
                    (file.clone(), key, false)
                }))
                .collect()
        });

        let mut partial_map_a: HashMap<String, Vec<UnmatchedFile>> = HashMap::new();
        let mut partial_map_b: HashMap<String, Vec<UnmatchedFile>> = HashMap::new();
        for (file, partial, is_a) in partial_pairs {
            if partial.is_empty() {
                stats.comparison_errors += 1;
                continue;
            }
            if is_a {
                partial_map_a.entry(partial).or_default().push(file);
            } else {
                partial_map_b.entry(partial).or_default().push(file);
            }
        }

        for (partial, partial_group_a) in partial_map_a {
            let Some(partial_group_b) = partial_map_b.get(&partial) else {
                continue;
            };

            let full_pairs: Vec<(UnmatchedFile, String, bool)> = pool.install(|| {
                partial_group_a
                    .par_iter()
                    .map(|file| {
                        let hash =
                            match cached_full_hash(&hash_cache, &file.abs_path, cfg.block_size) {
                                Ok(hash) => hash,
                                Err(err) => {
                                    errors.log(
                                        "diff",
                                        format!(
                                            "full hash failed for {}: {}",
                                            file.abs_path.display(),
                                            err
                                        ),
                                    );
                                    String::new()
                                }
                            };
                        (file.clone(), hash, true)
                    })
                    .chain(partial_group_b.par_iter().map(|file| {
                        let hash =
                            match cached_full_hash(&hash_cache, &file.abs_path, cfg.block_size) {
                                Ok(hash) => hash,
                                Err(err) => {
                                    errors.log(
                                        "diff",
                                        format!(
                                            "full hash failed for {}: {}",
                                            file.abs_path.display(),
                                            err
                                        ),
                                    );
                                    String::new()
                                }
                            };
                        (file.clone(), hash, false)
                    }))
                    .collect()
            });

            let mut full_map_a: HashMap<String, Vec<UnmatchedFile>> = HashMap::new();
            let mut full_map_b: HashMap<String, Vec<UnmatchedFile>> = HashMap::new();
            for (file, full, is_a) in full_pairs {
                if full.is_empty() {
                    stats.comparison_errors += 1;
                    continue;
                }
                if is_a {
                    full_map_a.entry(full).or_default().push(file);
                } else {
                    full_map_b.entry(full).or_default().push(file);
                }
            }

            for (content_hash, files_a) in full_map_a {
                let Some(files_b) = full_map_b.get(&content_hash) else {
                    continue;
                };
                let mut a_paths = Vec::new();
                let mut b_paths = Vec::new();
                for file in &files_a {
                    relocated_a.insert(file.rel_path.clone());
                    a_paths.push(file.rel_path.clone());
                }
                for file in files_b {
                    relocated_b.insert(file.rel_path.clone());
                    b_paths.push(file.rel_path.clone());
                }
                a_paths.sort();
                b_paths.sort();
                records.push(ReportRecord::Relocation {
                    status: "same-content-different-path",
                    entry_type: EntryKind::File,
                    content_hash,
                    size,
                    a_paths: a_paths.clone(),
                    b_paths: b_paths.clone(),
                    comparison_basis: ComparisonBasis::FullHash,
                });
                stats.relocation_groups += 1;
                stats.relocated_paths_a += a_paths.len();
                stats.relocated_paths_b += b_paths.len();
            }
        }
    }

    let mut remaining_only_a: Vec<_> = unmatched_files_a
        .into_iter()
        .filter(|file| !relocated_a.contains(&file.rel_path))
        .collect();
    let mut remaining_only_b: Vec<_> = unmatched_files_b
        .into_iter()
        .filter(|file| !relocated_b.contains(&file.rel_path))
        .collect();
    remaining_only_a.sort_by(|left, right| left.rel_path.cmp(&right.rel_path));
    remaining_only_b.sort_by(|left, right| left.rel_path.cmp(&right.rel_path));

    for file in remaining_only_a {
        records.push(make_path_diff_record(
            file.rel_path,
            PathDiffStatus::OnlyInA,
            Some(EntryKind::File),
            Some(EntryKind::File),
            None,
            Some(file.size),
            None,
            Some(file.mtime),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
            None,
        ));
        stats.only_in_a += 1;
    }
    for file in remaining_only_b {
        records.push(make_path_diff_record(
            file.rel_path,
            PathDiffStatus::OnlyInB,
            Some(EntryKind::File),
            None,
            Some(EntryKind::File),
            None,
            Some(file.size),
            None,
            Some(file.mtime),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
            None,
        ));
        stats.only_in_b += 1;
    }

    stats.duration_seconds = start.elapsed().as_secs_f64();
    records.push(ReportRecord::Summary {
        stats: stats.clone(),
    });

    let output = File::create(&cfg.output)
        .with_context(|| format!("cannot create output file {}", cfg.output.display()))?;
    let mut writer = BufWriter::new(output);
    for record in records {
        serde_json::to_writer(&mut writer, &record)?;
        writeln!(&mut writer)?;
    }
    writer.flush()?;

    info!(
        output = %cfg.output.display(),
        partial = format_bytes_binary(cfg.partial_bytes as u64),
        block = format_bytes_binary(cfg.block_size as u64),
        only_in_a = stats.only_in_a,
        only_in_b = stats.only_in_b,
        same_path_different_content = stats.same_path_different_content,
        same_path_different_metadata = stats.same_path_different_metadata,
        relocation_groups = stats.relocation_groups,
        comparison_errors = stats.comparison_errors,
        duration_seconds = stats.duration_seconds,
        logged_errors = errors.count(),
        "tree diff complete"
    );

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::{run_diff, DiffConfig};
    use anyhow::Result;
    use jsonschema::{meta, validator_for};
    use serde_json::Value;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(name: &str) -> Result<PathBuf> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!("dupdup-{}-{}", name, nanos));
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    fn write_file(path: &Path, contents: &str) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, contents)?;
        Ok(())
    }

    fn validate_report_against_schema(report_path: &Path) -> Result<()> {
        let schema_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("schemas/tree-diff-v1.schema.json");
        let schema_text = fs::read_to_string(&schema_path)?;
        let schema: Value = serde_json::from_str(&schema_text)?;
        if !meta::is_valid(&schema) {
            anyhow::bail!("tree diff schema is not a valid JSON Schema document");
        }
        let validator = validator_for(&schema)?;

        let report = fs::read_to_string(report_path)?;
        for (line_number, line) in report.lines().enumerate() {
            let value: Value = serde_json::from_str(line)?;
            validator.validate(&value).map_err(|err| {
                anyhow::anyhow!(
                    "schema validation failed on line {}: {}",
                    line_number + 1,
                    err
                )
            })?;
        }
        Ok(())
    }

    #[test]
    fn diff_reports_expected_buckets() -> Result<()> {
        let root = unique_temp_dir("tree-diff")?;
        let a = root.join("a");
        let b = root.join("b");
        let output = root.join("report.jsonl");
        let error = root.join("error.log");
        fs::create_dir_all(&a)?;
        fs::create_dir_all(&b)?;

        write_file(&a.join("same.txt"), "same")?;
        write_file(&b.join("same.txt"), "same")?;
        write_file(&a.join("only-a.txt"), "only a")?;
        write_file(&b.join("only-b.txt"), "only b")?;
        write_file(&a.join("changed.txt"), "left")?;
        write_file(&b.join("changed.txt"), "right")?;
        write_file(&a.join("renamed/old-name.txt"), "move me")?;
        write_file(&b.join("renamed/new-name.txt"), "move me")?;
        fs::create_dir_all(a.join("dir-only-a"))?;
        fs::create_dir_all(b.join("dir-only-b"))?;

        let stats = run_diff(DiffConfig {
            a: a.clone(),
            b: b.clone(),
            output: output.clone(),
            error: Some(error),
            partial_bytes: 4,
            block_size: 4096,
            threads: 2,
        })?;

        assert_eq!(stats.only_in_a, 2);
        assert_eq!(stats.only_in_b, 2);
        assert_eq!(stats.same_path_same_content, 1);
        assert_eq!(stats.same_path_different_content, 1);
        assert_eq!(stats.relocation_groups, 1);
        validate_report_against_schema(&output)?;

        let report = fs::read_to_string(&output)?;
        let mut seen_only_a = false;
        let mut seen_only_b = false;
        let mut seen_same = false;
        let mut seen_changed = false;
        let mut seen_relocation = false;
        let mut seen_summary = false;

        for line in report.lines() {
            let value: Value = serde_json::from_str(line)?;
            let record_type = value.get("type").and_then(Value::as_str).unwrap_or("");
            match record_type {
                "path-diff" => {
                    let path = value.get("path").and_then(Value::as_str).unwrap_or("");
                    let status = value.get("status").and_then(Value::as_str).unwrap_or("");
                    if path == "only-a.txt" && status == "only-in-a" {
                        seen_only_a = true;
                    }
                    if path == "only-b.txt" && status == "only-in-b" {
                        seen_only_b = true;
                    }
                    if path == "same.txt" && status == "same-path-same-content" {
                        seen_same = true;
                    }
                    if path == "changed.txt" && status == "same-path-different-content" {
                        seen_changed = true;
                    }
                }
                "relocation" => {
                    let status = value.get("status").and_then(Value::as_str).unwrap_or("");
                    let a_paths = value
                        .get("a_paths")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    let b_paths = value
                        .get("b_paths")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    if status == "same-content-different-path"
                        && a_paths.iter().any(|entry| entry == "renamed/old-name.txt")
                        && b_paths.iter().any(|entry| entry == "renamed/new-name.txt")
                    {
                        seen_relocation = true;
                    }
                }
                "summary" => {
                    seen_summary = true;
                }
                _ => {}
            }
        }

        assert!(seen_only_a);
        assert!(seen_only_b);
        assert!(seen_same);
        assert!(seen_changed);
        assert!(seen_relocation);
        assert!(seen_summary);

        let _ = fs::remove_dir_all(root);
        Ok(())
    }

    #[test]
    fn diff_reports_type_mismatch() -> Result<()> {
        let root = unique_temp_dir("tree-diff-type-mismatch")?;
        let a = root.join("a");
        let b = root.join("b");
        let output = root.join("report.jsonl");
        let error = root.join("error.log");
        fs::create_dir_all(&a)?;
        fs::create_dir_all(&b)?;

        write_file(&a.join("conflict"), "file here")?;
        fs::create_dir_all(b.join("conflict"))?;

        let stats = run_diff(DiffConfig {
            a: a.clone(),
            b: b.clone(),
            output: output.clone(),
            error: Some(error),
            partial_bytes: 4,
            block_size: 4096,
            threads: 1,
        })?;

        assert_eq!(stats.type_mismatch, 1);
        validate_report_against_schema(&output)?;

        let report = fs::read_to_string(&output)?;
        let mut seen_type_mismatch = false;
        for line in report.lines() {
            let value: Value = serde_json::from_str(line)?;
            let record_type = value.get("type").and_then(Value::as_str).unwrap_or("");
            if record_type != "path-diff" {
                continue;
            }
            let path = value.get("path").and_then(Value::as_str).unwrap_or("");
            let status = value.get("status").and_then(Value::as_str).unwrap_or("");
            if path == "conflict" && status == "type-mismatch" {
                seen_type_mismatch = true;
            }
        }

        assert!(seen_type_mismatch);

        let _ = fs::remove_dir_all(root);
        Ok(())
    }

    #[test]
    fn diff_uses_full_hash_after_matching_partial_hash() -> Result<()> {
        let root = unique_temp_dir("tree-diff-full-hash")?;
        let a = root.join("a");
        let b = root.join("b");
        let output = root.join("report.jsonl");
        let error = root.join("error.log");
        fs::create_dir_all(&a)?;
        fs::create_dir_all(&b)?;

        write_file(&a.join("same-prefix.bin"), "ABCD-left-payload")?;
        write_file(&b.join("same-prefix.bin"), "ABCD-rightpayload")?;

        let stats = run_diff(DiffConfig {
            a: a.clone(),
            b: b.clone(),
            output: output.clone(),
            error: Some(error),
            partial_bytes: 4,
            block_size: 4096,
            threads: 1,
        })?;

        assert_eq!(stats.same_path_different_content, 1);
        validate_report_against_schema(&output)?;

        let report = fs::read_to_string(&output)?;
        let mut saw_full_hash_decision = false;
        for line in report.lines() {
            let value: Value = serde_json::from_str(line)?;
            let record_type = value.get("type").and_then(Value::as_str).unwrap_or("");
            if record_type != "path-diff" {
                continue;
            }
            let path = value.get("path").and_then(Value::as_str).unwrap_or("");
            let status = value.get("status").and_then(Value::as_str).unwrap_or("");
            let basis = value
                .get("comparison_basis")
                .and_then(Value::as_str)
                .unwrap_or("");
            let partial_a = value
                .get("partial_hash_a")
                .and_then(Value::as_str)
                .unwrap_or("");
            let partial_b = value
                .get("partial_hash_b")
                .and_then(Value::as_str)
                .unwrap_or("");
            let hash_a = value.get("hash_a").and_then(Value::as_str).unwrap_or("");
            let hash_b = value.get("hash_b").and_then(Value::as_str).unwrap_or("");
            if path == "same-prefix.bin"
                && status == "same-path-different-content"
                && basis == "full-hash"
                && !partial_a.is_empty()
                && partial_a == partial_b
                && !hash_a.is_empty()
                && !hash_b.is_empty()
                && hash_a != hash_b
            {
                saw_full_hash_decision = true;
            }
        }

        assert!(saw_full_hash_decision);

        let _ = fs::remove_dir_all(root);
        Ok(())
    }
}
