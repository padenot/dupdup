use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let Ok(manifest_dir) = env::var("CARGO_MANIFEST_DIR").map(PathBuf::from) else {
        return;
    };
    let target = env::var("TARGET").unwrap_or_default();

    let cross_chromaprint_dir = manifest_dir
        .join(".tools")
        .join("chromaprint")
        .join(&target)
        .join("lib");
    if cross_chromaprint_dir.exists() {
        println!(
            "cargo:rustc-link-search=native={}",
            cross_chromaprint_dir.display()
        );
    }

    if target == "x86_64-unknown-linux-musl" {
        let zig_cache = manifest_dir.join(".tools").join("zig-cache").join("global");
        for lib_name in ["libc++.a", "libc++abi.a", "libunwind.a"] {
            if let Some(lib_dir) = find_library_dir(&zig_cache, lib_name) {
                println!("cargo:rustc-link-search=native={}", lib_dir.display());
            }
        }
        println!("cargo:rustc-link-lib=static=c++");
        println!("cargo:rustc-link-lib=static=c++abi");
    }

    #[cfg(target_os = "macos")]
    {
        use std::path::Path;

        for dir in ["/opt/homebrew/lib", "/usr/local/lib"] {
            let dylib = Path::new(dir).join("libchromaprint.dylib");
            let static_lib = Path::new(dir).join("libchromaprint.a");
            if dylib.exists() || static_lib.exists() {
                println!("cargo:rustc-link-search=native={}", dir);
            }
        }
    }
}

fn find_library_dir(root: &PathBuf, lib_name: &str) -> Option<PathBuf> {
    let entries = fs::read_dir(root).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let candidate = path.join(lib_name);
            if candidate.exists() {
                return Some(path);
            }
            if let Some(found) = find_library_dir(&path, lib_name) {
                return Some(found);
            }
        }
    }
    None
}
