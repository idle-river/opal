use crate::{Error, Task, config::Program};
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc};

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
                    execute_job(job).await;
                    drop(permit); // Release slot when done
                });
            }
        });

        ExecutionEngine { sender: tx }
    }

    pub async fn init_program(&mut self, prog: Program) -> Result<(), Error> {
        for task in prog.jobs {
            let running_task = RunningTask {
                program_name: prog.name.clone(),
                task,
                state: ExecState::Pending,
            };

            // Enqueue the task for execution
            self.enqueue(running_task).await?;
        }
        Ok(())
    }

    async fn enqueue(&mut self, job: RunningTask) -> Result<(), Error> {
        self.sender.send(job).await.map_err(|e| {
            Box::new(std::io::Error::other(format!("failed to enqueue job: {e}"))) as Error
        })
    }
}

async fn execute_job(mut job: RunningTask) {
    use tokio::process::Command;
    job.state = ExecState::Running;

    for step in job.task.steps {
        let mut cmd = Command::new(&step.command);
        cmd.args(&step.args);

        match cmd.output().await {
            Ok(output) => {
                if output.status.success() {
                    println!(
                        "Program: {}, Task: {}, Step: {} executed successfully.",
                        job.program_name, job.task.name, step.name
                    );
                } else {
                    eprintln!(
                        "Program: {}, Task: {}, Step: {} failed with status: {}",
                        job.program_name, job.task.name, step.name, output.status
                    );
                    job.state = ExecState::Failed;
                    return;
                }
            }
            Err(e) => {
                eprintln!(
                    "Program: {}, Task: {}, Step: {} failed to execute with error: {}",
                    job.program_name, job.task.name, step.name, e
                );
                job.state = ExecState::Failed;
                return;
            }
        }
    }
}
