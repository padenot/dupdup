use clap::{ArgAction, Parser, ValueHint};
use serde::Serialize;
use std::path::PathBuf;
use std::str::FromStr;

/// Parse human-friendly byte sizes (e.g., "256K", "1M", "4m"), falling back to plain bytes.
pub(crate) fn parse_byte_size(src: &str) -> std::result::Result<usize, String> {
    byte_unit::Byte::from_str(src)
        .map_err(|e| e.to_string())
        .map(|b| b.as_u64() as usize)
}

#[derive(clap::ValueEnum, Clone, Default, Debug, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum Preset {
    #[default]
    Auto,
    Ssd,
    Hdd,
}

#[derive(Parser, Debug, Clone)]
#[command(
    name = "dupdup",
    version,
    about = "Find duplicate files with fast hashes"
)]
pub struct Config {
    /// Path to analyze
    #[arg(value_hint = ValueHint::DirPath, default_value = ".")]
    pub path: PathBuf,

    /// Output file (JSON Lines)
    #[arg(short, long, default_value = "results.jsonl")]
    pub output: PathBuf,

    /// Error log file
    #[arg(long, default_value = None)]
    pub error: Option<PathBuf>,

    /// Bytes to read in the partial pass (set 0 to skip partial pass)
    #[arg(long, default_value = "4096", value_parser = parse_byte_size)]
    pub partial_bytes: usize,

    /// Block size used when hashing files
    #[arg(long, default_value = "1M", value_parser = parse_byte_size)]
    pub block_size: usize,

    /// Number of worker threads (0 = num_cpus)
    #[arg(long, default_value_t = 0)]
    pub threads: usize,

    /// Disable auto-opening the web UI
    #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
    pub no_open_ui: bool,
    /// Enable terminal TUI (ratatui) during scan
    #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
    pub tui: bool,

    /// Path to on-disk cache (SQLite). Omit to disable caching.
    #[arg(long)]
    pub cache: Option<PathBuf>,

    /// Reuse cached hashes when available
    #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
    pub resume: bool,

    /// Process files in deterministic path order (single-thread, HDD-friendly)
    #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
    pub ordered: bool,

    /// Preset to use: auto, ssd, hdd
    #[clap(short, long, default_value_t, value_enum)]
    pub preset: Preset,

    /// Dump hard drive detection info and exit (for diagnostics)
    #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
    pub dump_disk_info: bool,

    /// Serve a tiny HTTP UI (index.html + report)
    #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
    pub serve: bool,

    /// Port for the HTTP UI
    #[arg(long, default_value_t = 3030)]
    pub port: u16,

    /// Disable terminal TUI (ratatui) during scan
    #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
    pub no_tui: bool,
}
