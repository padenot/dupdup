use clap::Parser;
use dupdup::{run, run_diff, Cli, Command};

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Some(Command::Diff(cfg)) => {
            run_diff(cfg)?;
        }
        None => {
            run(cli.scan)?;
        }
    }
    Ok(())
}
