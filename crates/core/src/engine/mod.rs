use crate::{Error, Step, Task, config::Program};
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc, oneshot};

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

    for step in job.task.steps {
        let mut cmd = command_for_step(&step);

        println!(
            "Executing Program: {}, Task: {}, Step: {} with command: {:?}",
            job.program_name, job.task.name, step.name, cmd
        );

        match cmd.output().await {
            Ok(output) => {
                if !output.status.success() {
                    eprintln!(
                        "Program: {}, Task: {}, Step: {} failed with status: {}",
                        job.program_name, job.task.name, step.name, output.status
                    );
                    job.state = ExecState::Failed;
                    return (
                        Err(format!(
                            "Program: {}, Task: {}, Step: {} failed with status: {}",
                            job.program_name, job.task.name, step.name, output.status
                        )),
                        completion,
                    );
                }
            }
            Err(e) => {
                eprintln!(
                    "Program: {}, Task: {}, Step: {} failed to execute with error: {}",
                    job.program_name, job.task.name, step.name, e
                );
                job.state = ExecState::Failed;
                return (
                    Err(format!(
                        "Program: {}, Task: {}, Step: {} failed to execute with error: {}",
                        job.program_name, job.task.name, step.name, e
                    )),
                    completion,
                );
            }
        }
    }

    job.state = ExecState::Success;
    (Ok(()), completion)
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
    use super::command_parts;
    use crate::Step;

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
}
