use crate::{Task, config::Program};
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

struct RunningTask<'a> {
    task: &'a Task,
    state: ExecState,
}

pub struct ExecutionEngine<'a> {
    sender: mpsc::Sender<RunningTask<'a>>,
}

impl<'a> ExecutionEngine<'a> {
    pub fn new(max_concurrent_jobs: usize, buffer_size: usize) -> Self {
        let (tx, mut rx) = mpsc::channel::<RunningTask>(buffer_size);
        let semaphore = Arc::new(Semaphore::new(max_concurrent_jobs));
        let engine = ExecutionEngine { sender: tx };

        // Background dispatcher task
        tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                // Acquire permit before spawning worker task
                let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();

                tokio::spawn(async move {
                    engine.execute_job(job).await;
                    drop(permit); // Release slot when done
                });
            }
        });

        engine
    }

    pub fn init_program(&mut self, prog: Program) {
        unimplemented!()
    }

    fn enqueue(&mut self) {
        unimplemented!()
    }

    async fn execute_job(job: &<_>RunningTask) {
        unimplemented!()
    }
}
