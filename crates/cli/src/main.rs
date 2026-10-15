use opal_core::Task;
use clap::{Parser, Subcommand};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli {
        Cli::Init => {println!("Inititing");},
        Cli::Run => {println!("{:#?}", Task { name: "Test".to_string(), depends: vec![], steps: vec![] });},
    }
}

#[derive(Parser)]
enum Cli {
    Init,
    Run,
}