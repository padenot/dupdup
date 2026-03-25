use log::{debug, info, trace, warn};
#[cfg(target_os = "linux")]
use std::collections::HashSet;
#[cfg(target_os = "macos")]
use std::io::Write;
#[cfg(target_os = "linux")]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;

#[cfg(target_os = "macos")]
use serde_json::Value;
#[cfg(target_os = "macos")]
use std::ffi::{CStr, CString};
#[cfg(target_os = "macos")]
use std::process::Stdio;

/// Best-effort detection of rotational disks.
#[derive(Debug, Default)]
pub struct DiskInfo {
    pub block_size: usize,
    pub rotational: Option<bool>,
}

/// Describes the filesystem and underlying leaf disks for a path.
#[derive(Debug, Default)]
pub struct DiskLayout {
    pub mount_point: PathBuf,
    pub fs_type: String,
    pub source: String,
    pub leaf_devices: Vec<String>,
}

/// Returns Some(true) if likely rotational, Some(false) if likely SSD, None if unknown.
pub fn disk_info(path: &Path) -> Option<DiskInfo> {
    trace!("probing disk info for {}", path.display());
    #[cfg(target_os = "linux")]
    {
        if let Some(info) = disk_info_from_metadata(path) {
            return Some(info);
        }
        if let Some(layout) = disk_layout(path) {
            for dev_path in layout
                .leaf_devices
                .iter()
                .chain(std::iter::once(&layout.source))
            {
                if dev_path.is_empty() {
                    continue;
                }
                let path = Path::new(dev_path);
                if let Some(info) = disk_info_from_device_path(path) {
                    return Some(info);
                }
            }
        }
        trace!("falling back to lsblk for {}", path.display());
        if let Some(info) = disk_info_from_lsblk(path) {
            return Some(info);
        }
        None
    }
    #[cfg(target_os = "macos")]
    {
        let mut statfs = std::mem::MaybeUninit::<libc::statfs>::uninit();
        let cpath = CString::new(path.to_string_lossy().as_bytes()).ok()?;
        if unsafe { libc::statfs(cpath.as_ptr(), statfs.as_mut_ptr()) } != 0 {
            warn!("statfs failed for {}", path.display());
            return None;
        }
        let statfs = unsafe { statfs.assume_init() };
        let fs_block = statfs.f_bsize as usize;
        let dev = unsafe { CStr::from_ptr(statfs.f_mntfromname.as_ptr()) }
            .to_string_lossy()
            .to_string();
        let mut info = DiskInfo {
            block_size: fs_block,
            rotational: None,
        };
        if dev.is_empty() {
            info!("macos disk info: fast path missing device name");
            return Some(info);
        }

        let out = Command::new("diskutil")
            .args(["info", "-plist", &dev])
            .output()
            .ok()?;
        if !out.status.success() {
            warn!("diskutil info failed for {}", dev);
            return Some(info);
        }
        let txt = String::from_utf8_lossy(&out.stdout);
        if let Some(ssd) = plist_bool(&txt, "SolidState") {
            info.rotational = Some(!ssd);
        }
        if let Some(bs) = plist_int(&txt, "PhysicalBlockSize") {
            info.block_size = bs as usize;
        }
        trace!(
            "macos diskutil info carrier={} block={} rotational={:?}",
            dev,
            info.block_size,
            info.rotational
        );
        Some(info)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        trace!("disk detection not implemented for this platform");
        None
    }
}

#[cfg(target_os = "linux")]
fn disk_info_from_metadata(path: &Path) -> Option<DiskInfo> {
    if let Ok(meta) = path.metadata() {
        let dev = meta.dev();
        let major = linux_dev_major(dev);
        let minor = linux_dev_minor(dev);
        let sys_path = PathBuf::from(format!("/sys/dev/block/{}:{}", major, minor));
        trace!("linux sysfs path: {}", sys_path.display());
        if let Ok(target) = std::fs::read_link(&sys_path) {
            if let Some(base) = linux_sysfs_block_name(&target) {
                if let Some(info) = read_disk_info_from_block_name(&base) {
                    return Some(info);
                }
            }
        }
        if let Some(info) = read_disk_info_for_dev(major, minor) {
            return Some(info);
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn disk_info_from_device_path(dev_path: &Path) -> Option<DiskInfo> {
    let base_name = std::fs::canonicalize(dev_path).ok().and_then(|canonical| {
        canonical
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
    });
    if let Some(name) = base_name {
        if let Some(info) = read_disk_info_from_block_name(&name) {
            return Some(info);
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn disk_info_from_lsblk(path: &Path) -> Option<DiskInfo> {
    let out = Command::new("lsblk")
        .args(["-ndo", "ROTA,PHY-SEC", path.to_string_lossy().as_ref()])
        .output()
        .ok()?;
    if !out.status.success() {
        warn!("lsblk failed for {}", path.display());
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout);
    trace!("lsblk output: {}", s);
    let mut parts = s.split_whitespace();
    let rotational = match parts.next() {
        Some("1") => Some(true),
        Some("0") => Some(false),
        _ => None,
    };
    let block_size = parts
        .next()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    if rotational.is_some() || block_size > 0 {
        return Some(DiskInfo {
            block_size,
            rotational,
        });
    }
    None
}

#[cfg(target_os = "linux")]
fn read_disk_info_from_block_name(name: &str) -> Option<DiskInfo> {
    for leaf in collect_leaf_block_names(name) {
        let class_path = Path::new("/sys/class/block").join(&leaf);
        if let Some(info) = read_disk_info_from_sysfs(&class_path) {
            return Some(info);
        }
        let block_path = Path::new("/sys/block").join(&leaf);
        if let Some(info) = read_disk_info_from_sysfs(&block_path) {
            return Some(info);
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn read_disk_info_from_sysfs(block_dir: &Path) -> Option<DiskInfo> {
    let rotational = std::fs::read_to_string(block_dir.join("queue/rotational"))
        .ok()
        .map(|v| v.trim().starts_with('1'));
    let block_size = std::fs::read_to_string(block_dir.join("queue/physical_block_size"))
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0);
    trace!(
        "linux sysfs info base={} rotational={:?} block={}",
        block_dir.display(),
        rotational,
        block_size
    );
    if rotational.is_some() || block_size > 0 {
        Some(DiskInfo {
            block_size,
            rotational,
        })
    } else {
        None
    }
}

#[cfg(target_os = "linux")]
fn read_disk_info_for_dev(major: u64, minor: u64) -> Option<DiskInfo> {
    let dev_str = format!("{}:{}", major, minor);
    trace!("searching /sys/class/block for dev {}", dev_str);
    let entries = std::fs::read_dir("/sys/class/block").ok()?;
    for entry in entries.flatten() {
        let dev_path = entry.path().join("dev");
        if std::fs::read_to_string(&dev_path)
            .ok()
            .map(|s| s.trim() == dev_str)
            .unwrap_or(false)
        {
            let block_dir = entry.path();
            trace!("matched {} for dev {}", block_dir.display(), dev_str);
            if let Some(info) = read_disk_info_from_sysfs(&block_dir) {
                return Some(info);
            }
            if let Ok(target) = std::fs::read_link(&block_dir) {
                if let Some(base) = linux_sysfs_block_name(&target) {
                    let sys_block = Path::new("/sys/block").join(base);
                    if let Some(info) = read_disk_info_from_sysfs(&sys_block) {
                        return Some(info);
                    }
                }
            }
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn collect_leaf_block_names(name: &str) -> Vec<String> {
    let mut set = HashSet::new();
    linux_collect_leaf_devices(name, &mut set);
    if set.is_empty() {
        return vec![name.to_string()];
    }
    let mut leaves: Vec<String> = set.into_iter().collect();
    leaves.sort();
    leaves
}

#[cfg(target_os = "linux")]
fn linux_dev_major(dev: u64) -> u64 {
    (dev >> 8) & 0xfff
}

#[cfg(target_os = "linux")]
fn linux_dev_minor(dev: u64) -> u64 {
    (dev & 0xff) | ((dev >> 12) & 0xfff00)
}

#[cfg(target_os = "linux")]
fn linux_sysfs_block_name(link: &Path) -> Option<String> {
    let mut comps = link.components();
    while let Some(comp) = comps.next() {
        if comp.as_os_str() == "block" {
            if let Some(next) = comps.next() {
                return Some(next.as_os_str().to_string_lossy().to_string());
            }
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn linux_base_block_name(name: &str) -> String {
    let sys = PathBuf::from(format!("/sys/class/block/{}", name));
    if let Ok(link) = std::fs::read_link(sys) {
        if let Some(base) = linux_sysfs_block_name(&link) {
            return base;
        }
    }
    name.to_string()
}

#[cfg(target_os = "linux")]
fn linux_collect_leaf_devices(name: &str, out: &mut HashSet<String>) {
    let base = linux_base_block_name(name);
    let slaves_dir = Path::new("/sys/block").join(&base).join("slaves");
    if let Ok(entries) = std::fs::read_dir(slaves_dir) {
        let mut has_slaves = false;
        for entry in entries.flatten() {
            if let Some(child) = entry.file_name().to_str() {
                has_slaves = true;
                linux_collect_leaf_devices(child, out);
            }
        }
        if has_slaves {
            return;
        }
    }
    out.insert(base);
}

#[cfg(target_os = "linux")]
fn unescape_mountinfo(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    let mut chars = src.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            let mut code = String::new();
            for _ in 0..3 {
                if let Some(next) = chars.peek() {
                    if next.is_ascii_digit() {
                        code.push(*next);
                        chars.next();
                    }
                }
            }
            if code.len() == 3 && code.chars().all(|c| c.is_digit(8)) {
                if let Ok(v) = u8::from_str_radix(&code, 8) {
                    out.push(v as char);
                    continue;
                }
            }
            out.push('\\');
            out.push_str(&code);
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(target_os = "linux")]
fn linux_disk_layout(path: &Path) -> Option<DiskLayout> {
    trace!("computing linux disk layout for {}", path.display());
    let canon = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let path_s = canon.to_string_lossy();
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    let mut best: Option<(String, String, String, String)> = None;
    for line in mountinfo.lines() {
        let (pre, post) = match line.split_once(" - ") {
            Some(v) => v,
            None => continue,
        };
        let pre_fields: Vec<&str> = pre.split_whitespace().collect();
        if pre_fields.len() < 5 {
            continue;
        }
        let mount_point = unescape_mountinfo(pre_fields[4]);
        if !(path_s == mount_point || path_s.starts_with(&(mount_point.clone() + "/"))) {
            continue;
        }
        let post_fields: Vec<&str> = post.split_whitespace().collect();
        if post_fields.len() < 2 {
            continue;
        }
        let major_minor = pre_fields[2].to_string();
        let fs_type = post_fields[0].to_string();
        let source = unescape_mountinfo(post_fields[1]);
        let replace = best
            .as_ref()
            .map(|(mp, _, _, _)| mount_point.len() > mp.len())
            .unwrap_or(true);
        if replace {
            best = Some((mount_point, fs_type, source, major_minor));
        }
    }
    let (mount_point, fs_type, source, major_minor) = best?;
    trace!(
        "disk layout match mount={} fs={} source={} dev={} ",
        mount_point,
        fs_type,
        source,
        major_minor
    );
    let mut leaf_set: HashSet<String> = HashSet::new();
    let sys_path = PathBuf::from(format!("/sys/dev/block/{}", major_minor));
    if let Ok(target) = std::fs::read_link(&sys_path) {
        if let Some(base) = linux_sysfs_block_name(&target) {
            linux_collect_leaf_devices(&base, &mut leaf_set);
        }
    }
    let mut leaf_devices: Vec<String> = leaf_set
        .into_iter()
        .map(|d| format!("/dev/{}", d))
        .collect();
    leaf_devices.sort();
    if leaf_devices.is_empty() && !source.is_empty() {
        leaf_devices.push(source.clone());
    }
    Some(DiskLayout {
        mount_point: PathBuf::from(mount_point),
        fs_type,
        source,
        leaf_devices,
    })
}

#[cfg(target_os = "macos")]
fn diskutil_plist_json(args: &[&str]) -> Option<Value> {
    let out = Command::new("diskutil").args(args).output().ok()?;
    if !out.status.success() {
        warn!("diskutil {:?} failed", args);
        return None;
    }
    let mut child = Command::new("plutil")
        .args(["-convert", "json", "-o", "-", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .ok()?;
    if let Some(stdin) = child.stdin.as_mut() {
        let _ = stdin.write_all(&out.stdout);
    }
    let output = child.wait_with_output().ok()?;
    if !output.status.success() {
        warn!("plutil conversion failed for {:?}", args);
        return None;
    }
    serde_json::from_slice(&output.stdout).ok()
}

#[cfg(target_os = "macos")]
fn mac_apfs_physical_stores(apfs: &Value, container: &str) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(containers) = apfs.get("Containers").and_then(|v| v.as_array()) {
        for cont in containers {
            let cref = cont
                .get("ContainerReference")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if cref != container {
                continue;
            }
            if let Some(stores) = cont.get("PhysicalStores").and_then(|v| v.as_array()) {
                for store in stores {
                    if let Some(id) = store.get("DeviceIdentifier").and_then(|v| v.as_str()) {
                        out.push(id.to_string());
                    }
                }
            }
        }
    }
    out
}

#[cfg(target_os = "macos")]
fn mac_disk_layout(path: &Path) -> Option<DiskLayout> {
    let mut statfs = std::mem::MaybeUninit::<libc::statfs>::uninit();
    let cpath = CString::new(path.to_string_lossy().as_bytes()).ok()?;
    if unsafe { libc::statfs(cpath.as_ptr(), statfs.as_mut_ptr()) } != 0 {
        warn!("statfs failed for {}", path.display());
        return None;
    }
    let statfs = unsafe { statfs.assume_init() };
    let mount_point = unsafe { CStr::from_ptr(statfs.f_mntonname.as_ptr()) }
        .to_string_lossy()
        .to_string();
    let source = unsafe { CStr::from_ptr(statfs.f_mntfromname.as_ptr()) }
        .to_string_lossy()
        .to_string();
    let fs_type = unsafe { CStr::from_ptr(statfs.f_fstypename.as_ptr()) }
        .to_string_lossy()
        .to_string();
    let info = diskutil_plist_json(&["info", "-plist", &source]);
    let parent = info
        .as_ref()
        .and_then(|v| v.get("ParentWholeDisk"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let fs_type = info
        .as_ref()
        .and_then(|v| v.get("FilesystemType"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or(fs_type);
    let container = info
        .as_ref()
        .and_then(|v| v.get("APFSContainerReference"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let mut leaf_devices: Vec<String> = Vec::new();
    if let Some(container) = container {
        if let Some(apfs) = diskutil_plist_json(&["apfs", "list", "-plist"]) {
            let stores = mac_apfs_physical_stores(&apfs, &container);
            if !stores.is_empty() {
                for store in stores {
                    let dev = format!("/dev/{}", store);
                    if let Some(info) = diskutil_plist_json(&["info", "-plist", &dev]) {
                        if let Some(parent) = info.get("ParentWholeDisk").and_then(|v| v.as_str()) {
                            leaf_devices.push(format!("/dev/{}", parent));
                            continue;
                        }
                    }
                    leaf_devices.push(dev);
                }
            }
        }
    }
    if leaf_devices.is_empty() {
        if let Some(parent) = parent {
            leaf_devices.push(format!("/dev/{}", parent));
        } else if !source.is_empty() {
            leaf_devices.push(source.clone());
        }
    }
    leaf_devices.sort();
    leaf_devices.dedup();
    Some(DiskLayout {
        mount_point: PathBuf::from(mount_point),
        fs_type,
        source,
        leaf_devices,
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn disk_layout(path: &Path) -> Option<DiskLayout> {
    #[cfg(target_os = "linux")]
    {
        linux_disk_layout(path)
    }
    #[cfg(target_os = "macos")]
    {
        mac_disk_layout(path)
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn disk_layout(_path: &Path) -> Option<DiskLayout> {
    warn!("disk layout detection not implemented on this platform");
    None
}

pub fn format_disk_layout(layout: &DiskLayout) -> String {
    let mut parts = Vec::new();
    if !layout.fs_type.is_empty() {
        parts.push(format!("fs:{}", layout.fs_type));
    }
    if !layout.source.is_empty() {
        parts.push(format!("src:{}", layout.source));
    }
    if !layout.leaf_devices.is_empty() {
        parts.push(format!("disks:{}", layout.leaf_devices.join(",")));
    }
    if layout.mount_point.as_os_str().is_empty() {
        parts.join(" ")
    } else {
        format!("{} mount:{}", parts.join(" "), layout.mount_point.display())
    }
}

#[cfg(target_os = "macos")]
fn plist_bool(text: &str, key: &str) -> Option<bool> {
    let needle = format!("<key>{}</key>", key);
    let mut found = false;
    for line in text.lines() {
        let line = line.trim();
        if found {
            if line.starts_with("<true") {
                return Some(true);
            }
            if line.starts_with("<false") {
                return Some(false);
            }
        }
        if line.contains(&needle) {
            found = true;
        }
    }
    None
}

#[cfg(target_os = "macos")]
fn plist_int(text: &str, key: &str) -> Option<u64> {
    let needle = format!("<key>{}</key>", key);
    let mut found = false;
    for line in text.lines() {
        let line = line.trim();
        if found {
            if let Some(rest) = line.strip_prefix("<integer>") {
                if let Some(val) = rest.strip_suffix("</integer>") {
                    return val.trim().parse::<u64>().ok();
                }
            }
        }
        if line.contains(&needle) {
            found = true;
        }
    }
    None
}

/// Print and log everything we can learn about the disk backing `path`.
pub fn dump_detection(path: &Path) {
    println!("=== Hard drive diagnostics ===");
    println!("Target: {}", path.display());
    info!("dumping disk detection for {}", path.display());

    if let Some(disk) = disk_info(path) {
        info!(
            "detected disk info: block={} rotational={:?}",
            disk.block_size, disk.rotational
        );
        debug!("disk info detail: {:?}", disk);
        println!(
            "Disk info: block={} rotational={:?}",
            disk.block_size, disk.rotational
        );
    } else {
        warn!("disk info unavailable for {}", path.display());
        println!("Disk info: unavailable");
    }

    if let Some(layout) = disk_layout(path) {
        let formatted = format_disk_layout(&layout);
        info!("disk layout summary: {}", formatted);
        debug!("disk layout detail: {:?}", layout);
        println!("Disk layout: {}", formatted);
        println!("Leaf devices: {:?}", layout.leaf_devices);
    } else {
        warn!("disk layout unavailable for {}", path.display());
        println!("Disk layout: unavailable");
    }

    println!("=== End diagnostics ===");
}
