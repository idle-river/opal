use std::process;
use std::sync::Arc;

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
            use opal_core::engine::{ExecutionEngine, LogStream};

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

            let mut engine = ExecutionEngine::default();

            let callback = Arc::new(|event: opal_core::engine::LogEvent| {
                let message = format_event(&event);
                if event.stream == LogStream::Stderr {
                    eprintln!("{message}");
                } else {
                    println!("{message}");
                }
            });

            if let Err(error) = engine.init_program_streaming(prog, callback).await {
                eprintln!("Program execution failed: {error}");
                process::exit(1);
            }
        }
    }
}

fn format_event(event: &opal_core::engine::LogEvent) -> String {
    format!(
        "[{}] [{}] [{}] {}",
        event.program_name, event.task_name, event.step_name, event.message
    )
}

#[cfg(test)]
mod tests {
    use super::format_event;
    use opal_core::engine::{LogEvent, LogStream};

    #[test]
    fn formats_streaming_event_with_program_task_and_step() {
        let event = LogEvent {
            program_name: "build".to_owned(),
            task_name: "unit_tests".to_owned(),
            step_name: "test".to_owned(),
            stream: LogStream::Stdout,
            message: "finished".to_owned(),
        };

        assert_eq!(format_event(&event), "[build] [unit_tests] [test] finished");
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
