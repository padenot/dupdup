extern crate md5;
extern crate serde_json;
extern crate walkdir;
#[macro_use]
extern crate clap;
extern crate chrono;
extern crate time;
extern crate bytesize;

use chrono::Utc;
use clap::{App, Arg};
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::prelude::*;
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
        .or_insert_with(|| { vec![path.to_string()] });
    if dup {
        Ok(total_bytes_read)
    } else {
        Ok(0)
    }
}

fn main() -> std::io::Result<()> {
    let matches = App::new("dupdup-rs")
        .version("0.1")
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

    println!(
        "Looking for duplicates in {}, will output report in {}",
        search_path, output_file
    );

    let wd = WalkDir::new(search_path);

    let error_log_file = match matches.value_of("error") {
        Some(v) => v.to_string(),
        None => format!("error-{}.log", Utc::now().format("%F-%T")),
    };

    println!("{}", error_log_file);

    let mut error_file = File::create(error_log_file.clone())?;

    let mut partial_results: HashMap<String, Vec<String>> = HashMap::new();

    let mut buf: [u8; 16 * 1024] = [0; 16 * 1024];

    let mut last_time = time::precise_time_s();
    let mut count = 0;
    for entry in wd {
        if entry.is_err() {
            write!(error_file, "{} enumeration error\n", entry.unwrap_err())?;
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
    let mut wasted = 0;
    for entry in wd {
        if entry.is_err() {
            write!(error_file, "Could not enumerate an entry")?;
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
                write!(error_file, "could not convert {} to string", path.display())?;
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
                write!(error_file, "{}", e)?;
                got_error = true;
            }
            Ok(w) => {
                wasted += w;
            }
        }
        if (time::precise_time_s() - last_time) as f32 > interval {
            print!("\r");
            print!(
                "[{}/{}] {path:<width$}",
                current,
                count,
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

    print!("\r");
    println!("First pass finished, potentially wasted {}.", bytesize::to_string(wasted as u64, false));

    let mut results: HashMap<String, Vec<String>> = HashMap::new();
    current = 0;
    wasted = 0;
    let count = partial_results.len();
    for (_, v) in partial_results.iter() {
        for candidate in v {
            match process_file(
                &candidate,
                &mut results,
                &mut buf,
                HashComputationType::Full,
            ) {
                Err(e) => {
                    write!(error_file, "{}", e)?;
                    got_error = true;
                }
                Ok(w) => {
                    wasted += w;
                }
            }
            if (time::precise_time_s() - last_time) as f32 > interval {
                print!("\r");
                print!(
                    "[{}/{}] {path:<width$}",
                    current,
                    count,
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

    println!("Second pass finished, potentially wasted {}.", bytesize::to_string(wasted as u64, false));

    let filtered: HashMap<String, Vec<String>> = results
        .into_iter()
        .filter(|&(_, ref v)| v.len() != 1)
        .collect();

    let json_text = serde_json::to_string(&filtered).unwrap();

    let mut file = File::create(output_file).unwrap();
    let rv = file.write_all(json_text.as_bytes());
    if rv.is_err() {
        write!(error_file, "could write json report to {}\n", output_file)?;
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
        println!("Duplication search completed with errors.");
        std::fs::remove_file(error_log_file)?;
    }
    Ok(())
}
