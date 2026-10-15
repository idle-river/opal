# Opal

Opal is a small, asynchronous task runner written in Rust. Define a program
as YAML jobs and steps, then let Opal execute independent jobs concurrently
while respecting their dependencies. Output can be printed directly or
consumed as structured streaming events through the core library.

## Features

- YAML-based program and job definitions
- Dependency-aware execution for jobs
- Concurrent execution of independent jobs
- Configurable maximum number of concurrent jobs
- Streaming lifecycle, stdout, and stderr events

## Requirements

- Rust and Cargo with support for the Rust 2024 edition

## Getting started

Create an `.opal.yaml` file in the project directory:

```yaml
name: my_program
jobs:
  - name: compile
    steps:
      - name: build
        command: cargo build --release

  - name: test
    depends:
      - compile
    steps:
      - name: test
        command: cargo test --all --all-features

  - name: lint
    steps:
      - name: clippy
        command: cargo clippy
```

Run it with the CLI:

```bash
opal run
```

To use a different configuration file:

```bash
opal run --file path/to/program.yaml
```

The release binary can be built with:

```bash
cargo build --release -p cli
```

The resulting executable is `target/release/opal`.

## Configuration

A program has a name and a list of jobs. Each job has a unique name, an
optional list of job names it depends on, and one or more steps:

```yaml
name: example
jobs:
  - name: prepare
    steps:
      - name: create-output
        command: mkdir -p build
        args:
          - --verbose

  - name: package
    depends: [prepare]
    steps:
      - name: archive
        command: tar
        args:
          - -czf
          - build/package.tar.gz
          - src
```

The first whitespace-separated word in `command` is used as the executable.
The remaining words are passed as inline arguments, followed by any values in
`args`.

Job names must be unique, and dependencies must refer to jobs in the same
program. A dependency cycle is reported as an error.

## Concurrent execution

Jobs without unfinished dependencies are eligible to run at the same time.
Opal limits the number of active jobs with a concurrency setting. The default
`ExecutionEngine` runs up to four jobs concurrently:

```rust
let mut engine = opal_core::engine::ExecutionEngine::default();
```

Library callers can choose a different limit:

```rust
let mut engine = opal_core::engine::ExecutionEngine::new(8, 100);
```

The first argument is the maximum number of concurrent jobs. The second
argument is reserved for the engine's output buffer configuration. A job only
becomes eligible after all of its dependencies succeed. If a dependency
fails, dependent jobs are skipped and the program returns an error.

## Streaming output

The engine can deliver structured events as a program runs. Events identify
the program, job, and step, and classify the message as a lifecycle, standard
output, or standard error event:

```rust
use std::sync::Arc;

use opal_core::config::Program;
use opal_core::engine::{ExecutionEngine, LogEvent, LogStream};

let program = Program::from_file(".opal.yaml")?;
let callback = Arc::new(|event: LogEvent| {
    match event.stream {
        LogStream::Stderr => eprintln!(
            "[{}] [{}] [{}] {}",
            event.program_name, event.task_name, event.step_name, event.message
        ),
        _ => println!(
            "[{}] [{}] [{}] {}",
            event.program_name, event.task_name, event.step_name, event.message
        ),
    }
});

let mut engine = ExecutionEngine::default();
engine.init_program_streaming(program, callback).await?;
```

Use `init_program` instead when structured callbacks are not needed. Without a
callback, Opal writes standard output and lifecycle messages to stdout and
standard error messages to stderr.

## Using `opal-core`

`opal-core` is split into feature-gated capabilities:

| Feature | Enables |
| --- | --- |
| `config` | `Program::from_file` and YAML deserialization |
| `engine` | Asynchronous program execution; also enables `config` |
| `streaming` | Streaming execution support; also enables `engine` |

Add the library to a Cargo project with the features you need:

```toml
[dependencies]
opal-core = { path = "path/to/opal/crates/core", features = ["streaming"] }
tokio = { version = "1.53.3", features = ["macros", "rt-multi-thread"] }
```

Then run a program from an async function:

```rust
use opal_core::config::Program;
use opal_core::engine::ExecutionEngine;

async fn run() -> Result<(), opal_core::Error> {
    let program = Program::from_file(".opal.yaml")?;
    ExecutionEngine::default().init_program(program).await?;
    Ok(())
}
```

The CLI is a thin consumer of these same library APIs. It loads the YAML
program, configures a streaming callback, and formats each `LogEvent` for the
terminal.

## Contributing

Contributions are welcome. Before opening a pull request, format and test the
workspace:

```bash
cargo fmt --all
cargo test --all --all-features
```

Please keep changes focused, include tests for behavior changes, and describe
the motivation in the pull request.
