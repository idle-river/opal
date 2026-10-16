use clap::Parser;
use colored::*;
use opal_core::config::Program;
use std::process;
use std::sync::Arc;

const CONFIG_FILE: &str = ".opal.yaml";
const SAMPLE_YAML: &str = include_str!("../../../sample.yaml");

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli {
        Cli::Init => {
            use std::fs;
            let mut opal_yaml_file = std::env::current_dir().unwrap_or_else(|_| {
                eprintln!(
                    "{}",
                    "Failed to get the current directory; exiting...".red()
                );
                process::exit(1);
            });

            opal_yaml_file.push(CONFIG_FILE);

            fs::write(opal_yaml_file, SAMPLE_YAML).unwrap_or_else(|e| {
                eprintln!(
                    "{}",
                    format!("Failed to write to {} with this error: {}", CONFIG_FILE, e).red()
                );
                process::exit(1);
            });
        }
        Cli::Run { file } => {
            use opal_core::engine::{ExecutionEngine, LogStream};

            let prog = extract_program(file);

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
        Cli::Check { file } => {
            use std::fs;
            use std::path::PathBuf;

            let path = PathBuf::from(file.clone().unwrap_or(".opal.yaml".to_string()));

            if !fs::exists(&path).unwrap_or(false) {
                println!("{} is not accessable; cannot check", path.display());
                process::exit(1);
            }

            if check_program(file.as_ref()) {
                println!(
                    "Config in {} is valid",
                    file.unwrap_or(CONFIG_FILE.to_string())
                )
            } else {
                println!(
                    "Config in {} is NOT VALID",
                    file.unwrap_or(CONFIG_FILE.to_string())
                )
            }
        }
    }
}

fn format_event(event: &opal_core::engine::LogEvent) -> String {
    let program_palette = [
        Color::Blue,
        Color::Green,
        Color::Magenta,
        Color::Cyan,
        Color::BrightBlue,
        Color::BrightGreen,
        Color::BrightMagenta,
        Color::BrightCyan,
        Color::BrightYellow,
    ];

    let color_index = opal_core::engine::task_color_index(&event.task_name, program_palette.len());

    format!(
        "[{}] [{}] [{}] {}",
        event.program_name, event.task_name, event.step_name, event.message
    )
    .color(program_palette[color_index])
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::format_event;
    use opal_core::engine::{LogEvent, LogStream};

    #[test]
    fn formats_streaming_event_with_program_task_and_step() {
        colored::control::set_override(false);

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

fn extract_program(file: Option<String>) -> Program {
    if let Some(f) = file {
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
    }
}

fn check_program(file: Option<&String>) -> bool {
    if let Some(f) = file {
        Program::from_file(f)
    } else {
        Program::from_file(CONFIG_FILE)
    }
    .is_ok()
}

#[derive(Parser)]
enum Cli {
    Init,
    Run {
        #[arg(long)]
        file: Option<String>,
    },
    Check {
        #[arg(long)]
        file: Option<String>,
    },
}
