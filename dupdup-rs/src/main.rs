extern crate md5;
extern crate serde_json;
extern crate walkdir;
#[macro_use]
extern crate clap;
extern crate bytesize;
extern crate chrono;
extern crate time;

use chrono::Utc;
use clap::{App, Arg};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Read;
use walkdir::WalkDir;

enum HashComputationType {
    Partial(usize),
    Full,
}

fn process_file(
    path: &str,
    results: &mut HashMap<String, Vec<String>>,
    buf: &mut [u8],
    partial: HashComputationType,
) -> Result<usize, String> {
    let file = File::open(path);

    let bytes_to_read = match partial {
        HashComputationType::Partial(count) => count,
        HashComputationType::Full => std::usize::MAX,
    };

    if file.is_err() {
        return Err(format!("{} could not open file\n", path));
    }
    let mut f = file.unwrap();
    let mut hash = md5::Context::new();
    let mut total_bytes_read = 0;
    loop {
        match f.read(buf) {
            Ok(bytes_read) => {
                total_bytes_read += bytes_read;
                if bytes_read < buf.len() || total_bytes_read >= bytes_to_read {
                    hash.consume(&buf[0..bytes_read]);
                    break;
                }
                hash.consume(&buf[0..bytes_read]);
            }
            Err(e) => {
                return Err(format!("{} {}", path, e.to_string()));
            }
        }
    }
    let digest = hash.compute();
    let strrepr = format!("{:x}", digest);
    let mut dup = false;
    results
        .entry(strrepr)
        .and_modify(|e| {
            dup = true;
            e.push(path.to_string());
        })
        .or_insert_with(|| vec![path.to_string()]);
    if dup {
        Ok(total_bytes_read)
    } else {
        Ok(0)
    }
}

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn main() -> std::io::Result<()> {
    let matches = App::new("dupdup-rs")
        .version(VERSION)
        .author("Paul Adenot <paul@paul.cx>")
        .about("Find duplicate files")
        .arg(Arg::with_name("path").help("path to analyse").index(1))
        .arg(
            Arg::with_name("output")
                .short("o")
                .long("output")
                .help("output file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("error")
                .short("e")
                .long("error")
                .help("error log file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("interval")
                .short("i")
                .long("interval")
                .help("interval between print")
                .takes_value(true),
        )
        .get_matches();

    let search_path = matches.value_of("path").unwrap_or(".");
    let output_file = matches.value_of("output").unwrap_or("results.json");
    let interval = value_t!(matches.value_of("interval"), f32).unwrap_or(1.0);
    let mut got_error = false;

    let metadata = fs::metadata(output_file);

    match metadata {
        Ok(_) => {
            println!("File {} already exists.", output_file);
            return Err(Error::new(ErrorKind::Other, "Output file already exists"));
        }
        Err(e) => {
            println!("{:?}", e);
        }
    }

    println!(
        "Looking for duplicates in {}, will output report in {}",
        search_path, output_file
    );

    let wd = WalkDir::new(search_path);

    let error_log_file = match matches.value_of("error") {
        Some(v) => v.to_string(),
        None => format!("error-{}.log", Utc::now().format("%F-%T")),
    };

    let mut error_file = File::create(error_log_file.clone())?;

    let mut partial_results: HashMap<String, Vec<String>> = HashMap::new();

    let mut buf: [u8; 16 * 1024] = [0; 16 * 1024];

    let mut last_time = time::precise_time_s();
    let mut count = 0;
    for entry in wd {
        if entry.is_err() {
            writeln!(error_file, "{} enumeration error", entry.unwrap_err())?;
            got_error = true;
            continue;
        }
        let f = entry.unwrap();
        if f.file_type().is_dir() {
            continue;
        }
        count += 1;
    }

    println!("{} total files to consider...", count);

    let wd = WalkDir::new(search_path);

    let mut small_buf = buf.split_at_mut(4 * 1024).0;
    let mut current = 0;
    let mut wasted: usize = 0;
    for entry in wd {
        if entry.is_err() {
            writeln!(error_file, "Could not enumerate an entry")?;
            got_error = true;
            continue;
        }
        let f = entry.unwrap();
        if f.file_type().is_dir() {
            continue;
        }
        let path = f.path();
        let path_str = match path.to_str() {
            Some(p) => p,
            None => {
                writeln!(error_file, "could not convert {} to string", path.display())?;
                got_error = true;
                continue;
            }
        };
        match process_file(
            &path_str,
            &mut partial_results,
            &mut small_buf,
            HashComputationType::Partial(4 * 1024),
        ) {
            Err(e) => {
                writeln!(error_file, "{}", e)?;
                got_error = true;
            }
            Ok(w) => {
                if w != 0 {
                    let size = fs::metadata(path_str)?.len() as usize;
                    wasted += size;
                }
            }
        }
        if (time::precise_time_s() - last_time) as f32 > interval {
            print!("\r");
            print!(
                "[{}/{}][{}] {path:<width$}",
                current,
                count,
                bytesize::to_string(wasted as u64, false),
                path = path.display(),
                width = 100
            );
            if io::stdout().flush().is_err() {
                print!("Could not flush stdio ?!");
            }
            last_time = time::precise_time_s();
        }
        current += 1;
    }

    let mut count = 0;
    let initial_dups: HashMap<String, Vec<String>> = partial_results
        .into_iter()
        .filter(|(_, v)| {
            if v.len() > 1 {
                count += v.len();
                return true;
            }
            false
        })
        .collect();

    print!("\r");
    println!(
        "First pass finished, potentially wasted {} in {} duplicated files.",
        bytesize::to_string(wasted as u64, false),
        count
    );

    let mut results: HashMap<String, Vec<String>> = HashMap::new();
    current = 0;
    wasted = 0;
    for (_, v) in initial_dups.iter() {
        for candidate in v {
            match process_file(
                &candidate,
                &mut results,
                &mut buf,
                HashComputationType::Full,
            ) {
                Err(e) => {
                    writeln!(error_file, "{}", e)?;
                    got_error = true;
                }
                Ok(w) => {
                    wasted += w;
                }
            }
            if (time::precise_time_s() - last_time) as f32 > interval {
                print!("\r");
                print!(
                    "[{}/{}][{}] {path:<width$}",
                    current,
                    count,
                    bytesize::to_string(wasted as u64, false),
                    path = candidate,
                    width = 100
                );
                if io::stdout().flush().is_err() {
                    print!("Could not flush stdio ?!");
                }
                last_time = time::precise_time_s();
            }
            current += 1;
        }
    }

    print!("\r");

    let filtered: HashMap<String, Vec<String>> = results
        .into_iter()
        .filter(|&(_, ref v)| v.len() != 1)
        .collect();

    println!(
        "Second pass finished, wasted {} in {} duplicated files.",
        bytesize::to_string(wasted as u64, false),
        filtered.len()
    );

    let json_text = serde_json::to_string(&filtered).unwrap();

    let mut file = File::create(output_file).unwrap();
    let rv = file.write_all(json_text.as_bytes());
    if rv.is_err() {
        writeln!(error_file, "could not write json report to {}", output_file)?;
        got_error = true;
    } else {
        println!("JSON report written in {}", output_file);
    }
    if got_error {
        println!(
            "Duplication search completed, with reported errors, see {}.",
            error_log_file
        );
    } else {
        println!("Duplication search completed without errors.");
        std::fs::remove_file(error_log_file)?;
    }
    Ok(())
}
