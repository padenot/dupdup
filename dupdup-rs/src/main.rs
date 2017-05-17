extern crate walkdir;
extern crate md5;
extern crate serde_json;
#[macro_use]
extern crate clap;
extern crate time;

use std::io;
use std::io::Read;
use walkdir::WalkDir;
use std::fs::File;
use std::collections::HashMap;
use std::io::prelude::*;
use clap::{App, Arg};

fn main() {
    let matches = App::new("dupdup-rs")
        .version("0.1")
        .author("Paul Adenot <paul@paul.cx>")
        .about("Find duplicate files")
        .arg(Arg::with_name("path").help("path to analyse").index(1))
        .arg(Arg::with_name("output")
                 .short("o")
                 .long("output")
                 .value_name("OUTPUT")
                 .help("output file")
                 .takes_value(true))
        .arg(Arg::with_name("interval")
                 .short("i")
                 .long("interval")
                 .value_name("INTERVAL")
                 .help("interval between print")
                 .takes_value(true))
        .get_matches();

    let search_path = matches.value_of("PATH").unwrap_or(".");
    let output_file = matches.value_of("OUTPUT").unwrap_or("results.json");
    let interval = value_t!(matches.value_of("INTERVAL"), f32).unwrap_or(1.0);

    let wd = WalkDir::new(search_path);

    let mut results: HashMap<String, Vec<String>> = HashMap::new();

    let mut buf: [u8; 16 * 1024] = [0; 16 * 1024];

    let mut last_time = time::precise_time_s();

    for entry in wd {
        if entry.is_err() {
            println!("{}", entry.unwrap_err());
            continue;
        }
        let f = entry.unwrap();
        if f.file_type().is_dir() {
            continue;
        }
        let path = f.path();
        let file = File::open(path);
        if file.is_err() {
            println!("Could not open {}.", path.display());
            continue;
        }
        if (time::precise_time_s() - last_time) as f32 > interval {
            print!("\r");
            print!("{path:<width$}", path = path.display(), width = 100);
            if io::stdout().flush().is_err() {
                print!("Could not flush stdio ?!");
            }
            last_time = time::precise_time_s();
        }
        let mut f = file.unwrap();
        let mut hash = md5::Context::new();
        loop {
            let rv = f.read(&mut buf);
            let bytes_read = rv.unwrap();
            if bytes_read == 0 {
                break;
            }
            hash.consume(&buf[0..bytes_read]);
        }
        let digest = hash.compute();
        let strrepr = format!("{:x}", digest);
        let path_string = String::from(path.to_str().unwrap());
        results.entry(strrepr).or_insert(Vec::new()).push(path_string);
    }

    print!("\n");
    let filtered: HashMap<String, Vec<String>> =
        results.into_iter().filter(|&(_, ref v)| v.len() != 1).collect();

    let json_text = serde_json::to_string(&filtered).unwrap();

    let mut file = File::create(output_file).unwrap();
    let rv = file.write_all(json_text.as_bytes());
    if rv.is_err() {
        println!("Could not write json report in {}", output_file);
    } else {
        println!("JSON report written in {}", output_file);
    }
}
