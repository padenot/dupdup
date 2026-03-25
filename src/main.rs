use clap::Parser;
use dupdup::{run, Config};

fn main() -> anyhow::Result<()> {
    let cfg = Config::parse();
    run(cfg)?;
    Ok(())
}
