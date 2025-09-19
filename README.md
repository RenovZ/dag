# dag

An asynchronous Directed Acyclic Graph (DAG) task runner for Rust.
This crate lets you define tasks as vertices in a DAG, declare dependencies between them, and run everything concurrently with Tokio.

## Features

- Define tasks as async functions (futures).
- Add edges between tasks to express dependencies.
- Automatic cycle detection.
- Runs tasks concurrently once their dependencies are satisfied.
- Stops execution on the first error.

## Example

```rust
use anyhow::Result;
use dag::Dag;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<()> {
    let mut dag = Dag::default();
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    // Add vertices (async tasks).
    dag.add_vertex("one", {
        let tx = tx.clone();
        || async move {
            tx.send("one".to_string()).unwrap();
            Ok(())
        }
    });

    dag.add_vertex("two", {
        let tx = tx.clone();
        || async move {
            tx.send("two".to_string()).unwrap();
            Ok(())
        }
    });

    dag.add_vertex("three", {
        let tx = tx.clone();
        || async move {
            tx.send("three".to_string()).unwrap();
            Ok(())
        }
    });

    // Define dependencies
    dag.add_edge("one", "two");   // two depends on one
    dag.add_edge("two", "three"); // three depends on two

    // Run the DAG
    dag.run().await?;

    // Collect results
    while let Some(msg) = rx.recv().await {
        println!("Task finished: {}", msg);
    }

    Ok(())
}
```
