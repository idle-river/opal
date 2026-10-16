use crate::{Error, Step, Task, config::Program};
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::task::JoinSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogStream {
    Lifecycle,
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEvent {
    pub program_name: String,
    pub task_name: String,
    pub step_name: String,
    pub stream: LogStream,
    pub message: String,
}

pub fn program_color_seed(program_name: &str) -> u64 {
    let mut hash = 14695981039346656037_u64;
    for byte in program_name.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(1099511628211_u64);
    }
    hash
}

pub fn program_color_index(program_name: &str, palette_size: usize) -> usize {
    if palette_size == 0 {
        return 0;
    }

    (program_color_seed(program_name) % palette_size as u64) as usize
}

pub fn task_color_index(task_name: &str, palette_size: usize) -> usize {
    program_color_index(task_name, palette_size)
}

pub type LogCallback = Arc<dyn Fn(LogEvent) + Send + Sync + 'static>;

pub enum ExecState {
    Pending,
    Queued,
    Running,
    Retrying,
    Success,
    Failed,
    Cancelled,
    TimedOut,
}

struct RunningTask {
    program_name: String,
    task: Task,
    state: ExecState,
    callback: Option<LogCallback>,
}

pub struct ExecutionEngine {
    max_concurrent_jobs: usize,
}

impl Default for ExecutionEngine {
    fn default() -> Self {
        Self::new(4, 100) // Default to 4 concurrent jobs and a buffer size of 100
    }
}

impl ExecutionEngine {
    pub fn new(max_concurrent_jobs: usize, buffer_size: usize) -> Self {
        let _ = buffer_size;
        ExecutionEngine {
            max_concurrent_jobs: max_concurrent_jobs.max(1),
        }
    }

    pub async fn init_program(
        &mut self,
        prog: Program,
        specific_task: Option<String>,
    ) -> Result<(), Error> {
        self.run_program(prog, None, specific_task).await
    }

    pub async fn init_program_streaming(
        &mut self,
        prog: Program,
        callback: LogCallback,
        specific_task: Option<String>,
    ) -> Result<(), Error> {
        self.run_program(prog, Some(callback), specific_task).await
    }

    async fn run_program(
        &self,
        prog: Program,
        callback: Option<LogCallback>,
        task: Option<String>,
    ) -> Result<(), Error> {
        use std::collections::HashMap;
        let mut pending: HashMap<String, Task> = HashMap::new();

        let jobs = prog.jobs.iter().filter(|job| {
            if let Some(t) = &task {
                &job.name == t
            } else {
                true
            }
        });

        for job in jobs {
            if pending.insert(job.name.clone(), job.clone()).is_some() {
                return Err(Box::new(std::io::Error::other("job names must be unique")));
            }
        }
        for task in pending.values() {
            for dependency in &task.depends {
                if !pending.contains_key(dependency) {
                    return Err(Box::new(std::io::Error::other(format!(
                        "job '{}' depends on unknown job '{}'",
                        task.name, dependency
                    ))));
                }
            }
        }

        let mut completed = HashMap::new();
        let mut running = JoinSet::new();
        let mut first_error = None;

        while !pending.is_empty() || !running.is_empty() {
            let ready = pending
                .values()
                .filter(|task| task.depends.iter().all(|name| completed.contains_key(name)))
                .map(|task| task.name.clone())
                .collect::<Vec<_>>();

            for name in ready {
                let task = pending.remove(&name).expect("ready job must be pending");
                if let Some(error) =
                    task.depends
                        .iter()
                        .find_map(|dependency| match completed.get(dependency) {
                            Some(Err(error)) => Some(error),
                            _ => None,
                        })
                {
                    let message = format!("skipped because dependency failed: {error}");
                    completed.insert(name, Err(message.clone()));
                    first_error.get_or_insert(message);
                } else if running.len() < self.max_concurrent_jobs {
                    let program_name = prog.name.clone();
                    let task_name = task.name.clone();
                    let task_callback = callback.clone();
                    running.spawn(async move {
                        (
                            task_name,
                            execute_job(RunningTask {
                                program_name,
                                task,
                                state: ExecState::Pending,
                                callback: task_callback,
                            })
                            .await,
                        )
                    });
                } else {
                    pending.insert(name, task);
                }
            }

            if let Some(result) = running.join_next().await {
                let (name, result) = result.map_err(|error| {
                    Box::new(std::io::Error::other(format!(
                        "job executor failed: {error}"
                    ))) as Error
                })?;
                if let Err(error) = &result {
                    first_error.get_or_insert(error.clone());
                }
                completed.insert(name, result);
            } else if !pending.is_empty() {
                return Err(Box::new(std::io::Error::other(
                    "job dependencies contain a cycle",
                )));
            }
        }

        first_error.map_or(Ok(()), |error| Err(Box::new(std::io::Error::other(error))))
    }
}

async fn execute_job(mut job: RunningTask) -> Result<(), String> {
    job.state = ExecState::Running;

    for step in &job.task.steps {
        let mut cmd = match command_for_step(step, job.task.envs.as_ref()) {
            Ok(command) => command,
            Err(message) => {
                emit(&job, step, LogStream::Stderr, message.clone());
                job.state = ExecState::Failed;
                return Err(message);
            }
        };
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        emit(
            &job,
            step,
            LogStream::Lifecycle,
            format!("executing command: {:?}", cmd),
        );

        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => {
                let message = format!("failed to execute with error: {e}");
                emit(&job, step, LogStream::Stderr, message.clone());
                job.state = ExecState::Failed;
                return Err(message);
            }
        };

        let stdout = child.stdout.take().expect("stdout was configured as piped");
        let stderr = child.stderr.take().expect("stderr was configured as piped");
        let stdout_task = stream_output(
            stdout,
            job.callback.clone(),
            job.program_name.clone(),
            job.task.name.clone(),
            step.name.clone(),
            LogStream::Stdout,
        );
        let stderr_task = stream_output(
            stderr,
            job.callback.clone(),
            job.program_name.clone(),
            job.task.name.clone(),
            step.name.clone(),
            LogStream::Stderr,
        );
        let (status, _, _) = tokio::join!(child.wait(), stdout_task, stderr_task);

        match status {
            Ok(status) if status.success() => {}
            Ok(status) => {
                let message = format!("failed with status: {status}");
                emit(&job, step, LogStream::Stderr, message.clone());
                job.state = ExecState::Failed;
                return Err(message);
            }
            Err(e) => {
                let message = format!("failed to wait for process: {e}");
                emit(&job, step, LogStream::Stderr, message.clone());
                job.state = ExecState::Failed;
                return Err(message);
            }
        }
    }

    job.state = ExecState::Success;
    Ok(())
}

async fn stream_output<R>(
    reader: R,
    callback: Option<LogCallback>,
    program_name: String,
    task_name: String,
    step_name: String,
    stream: LogStream,
) where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break,
            Ok(_) => {
                let message = line.trim_end_matches(['\r', '\n']).to_owned();
                if let Some(callback) = &callback {
                    callback(LogEvent {
                        program_name: program_name.clone(),
                        task_name: task_name.clone(),
                        step_name: step_name.clone(),
                        stream,
                        message,
                    });
                } else if stream == LogStream::Stderr {
                    eprintln!("{line}");
                } else {
                    print!("{line}");
                }
            }
            Err(error) => {
                let message = format!("failed to read {stream:?}: {error}");
                if let Some(callback) = &callback {
                    callback(LogEvent {
                        program_name: program_name.clone(),
                        task_name: task_name.clone(),
                        step_name: step_name.clone(),
                        stream: LogStream::Stderr,
                        message,
                    });
                } else {
                    eprintln!("{message}");
                }
                break;
            }
        }
    }
}

fn emit(job: &RunningTask, step: &Step, stream: LogStream, message: String) {
    if let Some(callback) = &job.callback {
        callback(LogEvent {
            program_name: job.program_name.clone(),
            task_name: job.task.name.clone(),
            step_name: step.name.clone(),
            stream,
            message,
        });
    } else if stream == LogStream::Stderr {
        eprintln!(
            "Program: {}, Task: {}, Step: {} {message}",
            job.program_name, job.task.name, step.name
        );
    } else {
        println!(
            "Program: {}, Task: {}, Step: {} {message}",
            job.program_name, job.task.name, step.name
        );
    }
}

fn command_for_step(
    step: &Step,
    env_vars: Option<&HashMap<String, String>>,
) -> Result<tokio::process::Command, String> {
    let (executable, args) = command_parts(step)?;
    let mut command = tokio::process::Command::new(executable);
    command.args(args);

    if let Some(envs) = env_vars {
        for (key, val) in envs {
            command.env(key, val);
        }
    }

    Ok(command)
}

fn command_parts(step: &Step) -> Result<(String, Vec<String>), String> {
    let mut parts = shell_words::split(&step.command)
        .map_err(|error| format!("invalid command '{}': {error}", step.command))?;

    let executable = parts
        .drain(..1)
        .next()
        .ok_or_else(|| "command cannot be empty".to_owned())?;

    let mut args = parts;
    if let Some(explicit_args) = &step.args {
        args.extend(explicit_args.iter().cloned());
    }

    Ok((executable, args))
}

#[cfg(test)]
mod tests {
    use super::{ExecutionEngine, LogEvent, command_parts, program_color_index};
    use crate::{Step, Task, config::Program};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    fn task(name: &str, depends: &[&str], command: &str) -> Task {
        Task {
            name: name.to_owned(),
            envs: None,
            depends: depends
                .iter()
                .map(|dependency| (*dependency).to_owned())
                .collect(),
            steps: vec![Step {
                name: "step".to_owned(),
                command: command.to_owned(),
                args: None,
            }],
        }
    }

    #[test]
    fn parses_executable_and_inline_arguments() {
        let step = Step {
            name: "build".to_owned(),
            command: "cargo build --release".to_owned(),
            args: None,
        };

        let (executable, args) = command_parts(&step).unwrap();
        assert_eq!(executable, "cargo");
        assert_eq!(args, ["build", "--release"]);
    }

    #[test]
    fn appends_explicit_arguments_to_inline_arguments() {
        let step = Step {
            name: "lint".to_owned(),
            command: "cargo clippy".to_owned(),
            args: Some(vec![
                "--".to_owned(),
                "-D".to_owned(),
                "warnings".to_owned(),
            ]),
        };

        let (executable, args) = command_parts(&step).unwrap();
        assert_eq!(executable, "cargo");
        assert_eq!(args, ["clippy", "--", "-D", "warnings"]);
    }

    #[test]
    fn preserves_quoted_shell_command_as_single_argument() {
        let step = Step {
            name: "echo env".to_owned(),
            command: "bash -c \"echo $API_KEY\"".to_owned(),
            args: None,
        };

        let (executable, args) = command_parts(&step).unwrap();
        assert_eq!(executable, "bash");
        assert_eq!(args, ["-c", "echo $API_KEY"]);
    }

    #[tokio::test]
    async fn streams_child_output_to_callback() {
        let events: Arc<Mutex<Vec<LogEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let received_events = Arc::clone(&events);
        let callback = Arc::new(move |event| {
            received_events.lock().unwrap().push(event);
        });
        let program = Program {
            name: "program".to_owned(),
            jobs: vec![Task {
                name: "task".to_owned(),
                envs: None,
                depends: Vec::new(),
                steps: vec![Step {
                    name: "step".to_owned(),
                    command: "printf streamed-output".to_owned(),
                    args: None,
                }],
            }],
        };

        ExecutionEngine::default()
            .init_program_streaming(program, callback, None)
            .await
            .unwrap();

        let events = events.lock().unwrap();
        assert!(events.iter().any(|event| {
            event.stream == super::LogStream::Stdout && event.message == "streamed-output"
        }));
    }

    #[tokio::test]
    async fn returns_command_failure_after_streaming_error_event() {
        let events: Arc<Mutex<Vec<LogEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let received_events = Arc::clone(&events);
        let callback = Arc::new(move |event| {
            received_events.lock().unwrap().push(event);
        });
        let program = Program {
            name: "program".to_owned(),
            jobs: vec![Task {
                name: "task".to_owned(),
                envs: None,
                depends: Vec::new(),
                steps: vec![Step {
                    name: "step".to_owned(),
                    command: "false".to_owned(),
                    args: None,
                }],
            }],
        };

        let result = ExecutionEngine::default()
            .init_program_streaming(program, callback, None)
            .await;

        assert!(result.is_err());
        assert!(events.lock().unwrap().iter().any(|event| {
            event.stream == super::LogStream::Stderr && event.message.contains("failed with status")
        }));
    }

    #[tokio::test]
    async fn runs_independent_jobs_concurrently() {
        let program = Program {
            name: "program".to_owned(),
            jobs: vec![
                task("first", &[], "sleep 0.2"),
                task("second", &[], "sleep 0.2"),
            ],
        };
        let started = Instant::now();

        ExecutionEngine::new(2, 1)
            .init_program(program, None)
            .await
            .unwrap();

        assert!(
            started.elapsed() < Duration::from_millis(350),
            "independent jobs did not run concurrently"
        );
    }

    #[tokio::test]
    async fn waits_for_dependencies_before_running_a_job() {
        let marker = std::env::temp_dir().join(format!("opal-dependency-{}", std::process::id()));
        let _ = std::fs::remove_file(&marker);
        let marker_path = marker.to_string_lossy();
        let program = Program {
            name: "program".to_owned(),
            jobs: vec![
                task("first", &[], &format!("touch {marker_path}")),
                task("second", &["first"], &format!("test -f {marker_path}")),
            ],
        };

        ExecutionEngine::new(2, 1)
            .init_program(program, None)
            .await
            .unwrap();

        assert!(marker.exists());
        let _ = std::fs::remove_file(marker);
    }

    #[tokio::test]
    async fn does_not_run_job_when_a_dependency_fails() {
        let events: Arc<Mutex<Vec<LogEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let received_events = Arc::clone(&events);
        let callback = Arc::new(move |event| {
            received_events.lock().unwrap().push(event);
        });
        let program = Program {
            name: "program".to_owned(),
            jobs: vec![
                task("first", &[], "false"),
                task("second", &["first"], "printf should-not-run"),
            ],
        };

        assert!(
            ExecutionEngine::new(2, 1)
                .init_program_streaming(program, callback, None)
                .await
                .is_err()
        );
        assert!(
            !events
                .lock()
                .unwrap()
                .iter()
                .any(|event| event.message == "should-not-run")
        );
    }

    #[test]
    fn program_color_index_is_deterministic_and_bounded() {
        let first = program_color_index("build", 8);
        let second = program_color_index("build", 8);

        assert_eq!(first, second);
        assert!(first < 8);
        assert_eq!(program_color_index("build", 0), 0);
    }
}
