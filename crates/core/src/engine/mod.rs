use crate::{Error, Step, Task, config::Program};
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{Semaphore, mpsc, oneshot};

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
    completion: Option<oneshot::Sender<Result<(), String>>>,
    callback: Option<LogCallback>,
}

pub struct ExecutionEngine {
    sender: mpsc::Sender<RunningTask>,
}

impl Default for ExecutionEngine {
    fn default() -> Self {
        Self::new(4, 100) // Default to 4 concurrent jobs and a buffer size of 100
    }
}

impl ExecutionEngine {
    pub fn new(max_concurrent_jobs: usize, buffer_size: usize) -> Self {
        let (tx, mut rx) = mpsc::channel::<RunningTask>(buffer_size);
        let semaphore = Arc::new(Semaphore::new(max_concurrent_jobs));

        // Background dispatcher task
        tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                // Acquire permit before spawning worker task
                let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();

                tokio::spawn(async move {
                    let result = execute_job(job).await;
                    if let Some(completion) = result.1 {
                        let _ = completion.send(result.0);
                    }
                    drop(permit); // Release slot when done
                });
            }
        });

        ExecutionEngine { sender: tx }
    }

    pub async fn init_program(&mut self, prog: Program) -> Result<(), Error> {
        let mut completions = Vec::with_capacity(prog.jobs.len());

        for task in prog.jobs {
            let (completion, result) = oneshot::channel();
            let running_task = RunningTask {
                program_name: prog.name.clone(),
                task,
                state: ExecState::Pending,
                completion: Some(completion),
                callback: None,
            };

            // Enqueue the task for execution
            self.enqueue(running_task).await?;
            completions.push(result);
        }

        for completion in completions {
            let result = completion.await.map_err(|_| {
                Box::new(std::io::Error::other("job executor stopped unexpectedly")) as Error
            })?;

            if let Err(message) = result {
                return Err(Box::new(std::io::Error::other(message)));
            }
        }

        Ok(())
    }

    pub async fn init_program_streaming(
        &mut self,
        prog: Program,
        callback: LogCallback,
    ) -> Result<(), Error> {
        let mut completions = Vec::with_capacity(prog.jobs.len());

        for task in prog.jobs {
            let (completion, result) = oneshot::channel();
            let running_task = RunningTask {
                program_name: prog.name.clone(),
                task,
                state: ExecState::Pending,
                completion: Some(completion),
                callback: Some(Arc::clone(&callback)),
            };

            self.enqueue(running_task).await?;
            completions.push(result);
        }

        for completion in completions {
            let result = completion.await.map_err(|_| {
                Box::new(std::io::Error::other("job executor stopped unexpectedly")) as Error
            })?;

            if let Err(message) = result {
                return Err(Box::new(std::io::Error::other(message)));
            }
        }

        Ok(())
    }

    async fn enqueue(&mut self, job: RunningTask) -> Result<(), Error> {
        self.sender.send(job).await.map_err(|e| {
            Box::new(std::io::Error::other(format!("failed to enqueue job: {e}"))) as Error
        })
    }
}

async fn execute_job(
    mut job: RunningTask,
) -> (
    Result<(), String>,
    Option<oneshot::Sender<Result<(), String>>>,
) {
    let completion = job.completion.take();
    job.state = ExecState::Running;

    for step in &job.task.steps {
        let mut cmd = command_for_step(&step);
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        emit(
            &job,
            &step,
            LogStream::Lifecycle,
            format!("executing command: {:?}", cmd),
        );

        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => {
                let message = format!("failed to execute with error: {e}");
                emit(&job, &step, LogStream::Stderr, message.clone());
                job.state = ExecState::Failed;
                return (Err(message), completion);
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
                emit(&job, &step, LogStream::Stderr, message.clone());
                job.state = ExecState::Failed;
                return (Err(message), completion);
            }
            Err(e) => {
                let message = format!("failed to wait for process: {e}");
                emit(&job, &step, LogStream::Stderr, message.clone());
                job.state = ExecState::Failed;
                return (Err(message), completion);
            }
        }
    }

    job.state = ExecState::Success;
    (Ok(()), completion)
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

fn command_for_step(step: &Step) -> tokio::process::Command {
    let (executable, args) = command_parts(step);
    let mut command = tokio::process::Command::new(executable);
    command.args(args);

    command
}

fn command_parts(step: &Step) -> (&str, Vec<&str>) {
    let mut parts = step.command.split_whitespace();
    let executable = parts.next().unwrap_or_default();
    let mut args = parts.collect::<Vec<_>>();

    if let Some(explicit_args) = &step.args {
        args.extend(explicit_args.iter().map(String::as_str));
    }

    (executable, args)
}

#[cfg(test)]
mod tests {
    use super::{ExecutionEngine, LogEvent, command_parts};
    use crate::{Step, Task, config::Program};
    use std::sync::{Arc, Mutex};

    #[test]
    fn parses_executable_and_inline_arguments() {
        let step = Step {
            name: "build".to_owned(),
            command: "cargo build --release".to_owned(),
            args: None,
        };

        let (executable, args) = command_parts(&step);
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

        let (executable, args) = command_parts(&step);
        assert_eq!(executable, "cargo");
        assert_eq!(args, ["clippy", "--", "-D", "warnings"]);
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
            depends: Vec::new(),
            jobs: vec![Task {
                name: "task".to_owned(),
                steps: vec![Step {
                    name: "step".to_owned(),
                    command: "printf streamed-output".to_owned(),
                    args: None,
                }],
            }],
        };

        ExecutionEngine::default()
            .init_program_streaming(program, callback)
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
            depends: Vec::new(),
            jobs: vec![Task {
                name: "task".to_owned(),
                steps: vec![Step {
                    name: "step".to_owned(),
                    command: "false".to_owned(),
                    args: None,
                }],
            }],
        };

        let result = ExecutionEngine::default()
            .init_program_streaming(program, callback)
            .await;

        assert!(result.is_err());
        assert!(events.lock().unwrap().iter().any(|event| {
            event.stream == super::LogStream::Stderr && event.message.contains("failed with status")
        }));
    }
}
