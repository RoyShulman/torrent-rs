mod modules;
use clap::Parser;

/// Simple torrent client
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Directory to store files
    #[arg(short, long)]
    directory: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    tracing::info!("Initializaing!");

    tracing::info!("Done. cya later :)");
}
