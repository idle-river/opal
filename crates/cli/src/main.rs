use std::process;

use clap::Parser;

const CONFIG_FILE: &str = ".opal.yaml";

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli {
        Cli::Init => {
            println!("Inititing");
        }
        Cli::Run { file } => {
            use opal_core::config::Program;

            let prog = if let Some(f) = file {
                Program::from_file(&f).unwrap_or_else(|e| {
                    eprintln!("Failed to read from {} with an error of: {}", f, e);
                    process::exit(1)
                })
            } else {
                Program::from_file(CONFIG_FILE).unwrap_or_else(|e| {
                    eprintln!(
                        "Failed to read from {} with an error of: {}",
                        CONFIG_FILE, e
                    );
                    process::exit(1)
                })
            };

            println!("{:#?}", prog);
        }
    }
}

#[derive(Parser)]
enum Cli {
    Init,
    Run {
        #[arg(long)]
        file: Option<String>,
    },
}
