use clap::{ArgAction, Args, Parser, Subcommand, ValueHint};
use serde::Serialize;
use std::path::PathBuf;
use std::str::FromStr;

/// Parse human-friendly byte sizes (e.g., "256K", "1M", "4m"), falling back to plain bytes.
pub(crate) fn parse_byte_size(src: &str) -> std::result::Result<usize, String> {
    byte_unit::Byte::from_str(src)
        .map_err(|e| e.to_string())
        .map(|b| b.as_u64() as usize)
}

pub(crate) fn parse_positive_byte_size(src: &str) -> std::result::Result<usize, String> {
    let value = parse_byte_size(src)?;
    if value == 0 {
        return Err("value must be greater than 0".to_string());
    }
    Ok(value)
}

#[derive(clap::ValueEnum, Clone, Default, Debug, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum Preset {
    #[default]
    Auto,
    Ssd,
    Hdd,
}

#[derive(clap::ValueEnum, Clone, Default, Debug, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum RunMode {
    #[default]
    Ui,
    Serve,
    Headless,
    Diagnostic,
}

#[derive(Args, Debug, Clone)]
pub struct ScanConfig {
    /// Path to analyze
    #[arg(value_hint = ValueHint::DirPath, default_value = ".")]
    pub path: PathBuf,

    /// Output file (JSON Lines)
    #[arg(short, long, default_value = "results.jsonl")]
    pub output: PathBuf,

    /// Error log file
    #[arg(long, default_value = None)]
    pub error: Option<PathBuf>,

    /// Bytes to read in the candidate-hash stage
    #[arg(long, default_value = "4096", value_parser = parse_positive_byte_size)]
    pub partial_bytes: usize,

    /// Block size used when hashing files
    #[arg(long, default_value = "1M", value_parser = parse_byte_size)]
    pub block_size: usize,

    /// Number of worker threads (0 = num_cpus)
    #[arg(long, default_value_t = 0)]
    pub threads: usize,

    /// Run mode: ui starts the HTTP UI and opens it, serve starts the HTTP UI without opening it,
    /// headless disables the HTTP UI, diagnostic disables both HTTP UI and TUI and enables tracing.
    #[arg(long, default_value_t, value_enum)]
    pub mode: RunMode,

    /// Force-enable the terminal TUI during the scan
    #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
    pub tui: bool,

    /// Path to on-disk cache (SQLite). Omit to disable caching.
    #[arg(long)]
    pub cache: Option<PathBuf>,

    /// Reuse cached hashes from --cache
    #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
    pub resume: bool,

    /// Process files in deterministic path order (single-thread, HDD-friendly)
    #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
    pub ordered: bool,

    /// Preset to use: auto, ssd, hdd
    #[clap(short, long, default_value_t, value_enum)]
    pub preset: Preset,

    /// Dump hard drive detection info and exit
    #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
    pub dump_disk_info: bool,

    /// Port for the HTTP UI
    #[arg(long, default_value_t = 3030)]
    pub port: u16,

    /// Disable the terminal TUI during the scan
    #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
    pub no_tui: bool,
}

#[derive(Args, Debug, Clone)]
pub struct DiffConfig {
    /// Left-hand directory tree
    #[arg(value_hint = ValueHint::DirPath)]
    pub a: PathBuf,

    /// Right-hand directory tree
    #[arg(value_hint = ValueHint::DirPath)]
    pub b: PathBuf,

    /// Output file (JSON Lines)
    #[arg(short, long, default_value = "tree-diff.jsonl")]
    pub output: PathBuf,

    /// Error log file
    #[arg(long, default_value = None)]
    pub error: Option<PathBuf>,

    /// Bytes to read in the candidate-hash stage
    #[arg(long, default_value = "4096", value_parser = parse_positive_byte_size)]
    pub partial_bytes: usize,

    /// Block size used when hashing files
    #[arg(long, default_value = "1M", value_parser = parse_byte_size)]
    pub block_size: usize,

    /// Number of worker threads (0 = num_cpus)
    #[arg(long, default_value_t = 0)]
    pub threads: usize,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    /// Diff two directory trees and emit a structured JSONL report
    Diff(DiffConfig),
}

#[derive(Parser, Debug, Clone)]
#[command(
    name = "dupdup",
    version,
    about = "Find duplicate files, audio-equivalent tracks, and diff similar directory trees"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    #[command(flatten)]
    pub scan: ScanConfig,
}

pub type Config = ScanConfig;
