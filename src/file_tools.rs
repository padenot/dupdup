use anyhow::{Context, Result};
use blake3::Hasher;
use std::fs::{self, File, FileType};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::UNIX_EPOCH;
use walkdir::WalkDir;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InventoryEntryKind {
    File,
    Directory,
    Symlink,
    Other,
}

#[derive(Debug, Clone)]
pub(crate) struct InventoryEntry {
    pub abs_path: PathBuf,
    pub rel_path: String,
    pub kind: InventoryEntryKind,
    pub size: Option<u64>,
    pub mtime: Option<i64>,
    pub link_target: Option<String>,
}

pub(crate) fn normalize_rel_path(path: &Path) -> String {
    let mut parts = Vec::new();
    for component in path.components() {
        parts.push(component.as_os_str().to_string_lossy().to_string());
    }
    parts.join("/")
}

pub(crate) fn file_mtime_secs(path: &Path) -> Result<i64> {
    let metadata = path.metadata()?;
    let modified = metadata.modified()?;
    let duration = modified.duration_since(UNIX_EPOCH).unwrap_or_default();
    Ok(duration.as_secs() as i64)
}

pub(crate) fn inventory_entry_kind(file_type: &FileType) -> InventoryEntryKind {
    if file_type.is_file() {
        InventoryEntryKind::File
    } else if file_type.is_dir() {
        InventoryEntryKind::Directory
    } else if file_type.is_symlink() {
        InventoryEntryKind::Symlink
    } else {
        InventoryEntryKind::Other
    }
}

pub(crate) fn collect_inventory<F, E>(
    root: &Path,
    min_depth: usize,
    follow_links: bool,
    mut on_progress: F,
    mut on_error: E,
) -> Vec<InventoryEntry>
where
    F: FnMut(u64, usize, &str),
    E: FnMut(String),
{
    let mut entries = Vec::new();
    let mut scanned = 0u64;
    let mut files_seen = 0usize;
    let mut last_path = String::new();

    for item in WalkDir::new(root)
        .min_depth(min_depth)
        .follow_links(follow_links)
    {
        scanned += 1;
        let display = if last_path.is_empty() {
            "<n/a>"
        } else {
            last_path.as_str()
        };
        on_progress(scanned, files_seen, display);
        match item {
            Ok(entry) => {
                last_path = entry.path().to_string_lossy().to_string();
                let rel_path = match entry.path().strip_prefix(root) {
                    Ok(path) => normalize_rel_path(path),
                    Err(err) => {
                        on_error(format!(
                            "strip_prefix failed for {}: {}",
                            entry.path().display(),
                            err
                        ));
                        continue;
                    }
                };
                let kind = inventory_entry_kind(&entry.file_type());
                let link_target = if kind == InventoryEntryKind::Symlink {
                    match fs::read_link(entry.path()) {
                        Ok(target) => Some(target.to_string_lossy().to_string()),
                        Err(err) => {
                            on_error(format!(
                                "read_link failed for {}: {}",
                                entry.path().display(),
                                err
                            ));
                            None
                        }
                    }
                } else {
                    None
                };
                let (size, mtime) = match entry.metadata() {
                    Ok(metadata) => {
                        let size = if kind == InventoryEntryKind::File {
                            Some(metadata.len())
                        } else {
                            None
                        };
                        let mtime = metadata
                            .modified()
                            .ok()
                            .and_then(|mtime| mtime.duration_since(UNIX_EPOCH).ok())
                            .map(|duration| duration.as_secs() as i64);
                        (size, mtime)
                    }
                    Err(err) => {
                        on_error(format!(
                            "metadata failed for {}: {}",
                            entry.path().display(),
                            err
                        ));
                        (None, None)
                    }
                };
                if kind == InventoryEntryKind::File {
                    files_seen += 1;
                }
                entries.push(InventoryEntry {
                    abs_path: entry.into_path(),
                    rel_path,
                    kind,
                    size,
                    mtime,
                    link_target,
                });
            }
            Err(err) => on_error(format!("walk error under {}: {}", root.display(), err)),
        }
    }

    entries
}

pub(crate) fn hash_prefix(path: &Path, limit: usize, buffer_size: usize) -> Result<String> {
    let file =
        File::open(path).with_context(|| format!("failed opening file {}", path.display()))?;
    let mut reader = BufReader::with_capacity(buffer_size, file).take(limit as u64);
    let mut buffer = vec![0u8; buffer_size];
    let mut hasher = Hasher::new();
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

pub(crate) fn hash_full(path: &Path, buffer_size: usize) -> Result<String> {
    let file =
        File::open(path).with_context(|| format!("failed opening file {}", path.display()))?;
    let mut reader = BufReader::with_capacity(buffer_size, file);
    let mut buffer = vec![0u8; buffer_size];
    let mut hasher = Hasher::new();
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

#[derive(Default)]
pub(crate) struct HashCache {
    partial: Mutex<std::collections::HashMap<PathBuf, String>>,
    full: Mutex<std::collections::HashMap<PathBuf, String>>,
}

impl HashCache {
    pub(crate) fn partial_hash(
        &self,
        path: &Path,
        partial_bytes: usize,
        block_size: usize,
    ) -> Result<String> {
        if let Ok(guard) = self.partial.lock() {
            if let Some(value) = guard.get(path) {
                return Ok(value.clone());
            }
        }
        let value = hash_prefix(path, partial_bytes, block_size)?;
        if let Ok(mut guard) = self.partial.lock() {
            guard.insert(path.to_path_buf(), value.clone());
        }
        Ok(value)
    }

    pub(crate) fn full_hash(&self, path: &Path, block_size: usize) -> Result<String> {
        if let Ok(guard) = self.full.lock() {
            if let Some(value) = guard.get(path) {
                return Ok(value.clone());
            }
        }
        let value = hash_full(path, block_size)?;
        if let Ok(mut guard) = self.full.lock() {
            guard.insert(path.to_path_buf(), value.clone());
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::{hash_full, hash_prefix, normalize_rel_path, HashCache};
    use anyhow::Result;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(name: &str) -> Result<PathBuf> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!("dupdup-file-tools-{}-{}", name, nanos));
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    #[test]
    fn normalize_rel_path_uses_forward_slashes() {
        let path = Path::new("foo").join("bar").join("baz.txt");
        assert_eq!(normalize_rel_path(&path), "foo/bar/baz.txt");
    }

    #[test]
    fn hash_cache_matches_direct_hashes() -> Result<()> {
        let root = unique_temp_dir("hash-cache")?;
        let path = root.join("sample.bin");
        fs::write(&path, b"abcdefgh12345678")?;

        let direct_partial = hash_prefix(&path, 4, 1024)?;
        let direct_full = hash_full(&path, 1024)?;

        let cache = HashCache::default();
        assert_eq!(cache.partial_hash(&path, 4, 1024)?, direct_partial);
        assert_eq!(cache.partial_hash(&path, 4, 1024)?, direct_partial);
        assert_eq!(cache.full_hash(&path, 1024)?, direct_full);
        assert_eq!(cache.full_hash(&path, 1024)?, direct_full);

        let _ = fs::remove_dir_all(root);
        Ok(())
    }
}
