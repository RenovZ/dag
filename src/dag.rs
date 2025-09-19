use anyhow::Result;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio::task;

type DynAsyncFn = dyn Fn() -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync;

#[derive(Default)]
pub struct Dag {
    fns: HashMap<String, Arc<DynAsyncFn>>,
    graph: HashMap<String, Vec<String>>,
}

impl Dag {
    pub fn add_vertex<F, Fut>(&mut self, name: &str, f: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.fns.insert(
            name.to_string(),
            Arc::new(move || Box::pin(f()) as Pin<Box<dyn Future<Output = Result<()>> + Send>>),
        );
    }

    pub fn add_edge(&mut self, from: &str, to: &str) {
        self.graph
            .entry(from.to_string())
            .or_default()
            .push(to.to_string());
    }

    fn detect_cycles(&self) -> bool {
        fn dfs(
            v: &str,
            graph: &HashMap<String, Vec<String>>,
            visited: &mut HashMap<String, bool>,
            stack: &mut HashMap<String, bool>,
        ) -> bool {
            visited.insert(v.to_string(), true);
            stack.insert(v.to_string(), true);

            if let Some(neighbors) = graph.get(v) {
                for n in neighbors {
                    if !*visited.get(n).unwrap_or(&false) {
                        if dfs(n, graph, visited, stack) {
                            return true;
                        }
                    } else if *stack.get(n).unwrap_or(&false) {
                        return true;
                    }
                }
            }

            stack.insert(v.to_string(), false);
            false
        }

        let mut visited = HashMap::new();
        let mut stack = HashMap::new();
        for v in self.graph.keys() {
            if !*visited.get(v).unwrap_or(&false) && dfs(v, &self.graph, &mut visited, &mut stack) {
                return true;
            }
        }
        false
    }

    pub async fn run(&self) -> Result<()> {
        if self.fns.is_empty() {
            return Ok(());
        }

        let mut deps: HashMap<String, usize> = HashMap::new();
        for (from, tos) in &self.graph {
            if !self.fns.contains_key(from) {
                anyhow::bail!("missing vertex");
            }
            for to in tos {
                if !self.fns.contains_key(to) {
                    anyhow::bail!("missing vertex");
                }
                *deps.entry(to.clone()).or_default() += 1;
            }
        }

        if self.detect_cycles() {
            anyhow::bail!("dependency cycle detected");
        }

        let (tx, mut rx) = mpsc::unbounded_channel::<(String, Result<()>)>();
        let deps = Arc::new(Mutex::new(deps));
        let mut running = 0usize;
        let mut err: Option<anyhow::Error> = None;

        for name in self.fns.keys() {
            if *deps.lock().await.get(name).unwrap_or(&0) == 0 {
                running += 1;
                Self::start(
                    name.clone(),
                    Arc::clone(self.fns.get(name).unwrap()),
                    tx.clone(),
                )
                .await;
            }
        }

        while running > 0 {
            let (name, res) = rx.recv().await.unwrap();
            running -= 1;

            if res.is_err() && err.is_none() {
                err = res.err();
            }

            if err.is_some() {
                continue;
            }

            if let Some(nexts) = self.graph.get(&name) {
                for n in nexts {
                    let mut deps_lock = deps.lock().await;
                    let entry = deps_lock.entry(n.clone()).or_default();
                    *entry -= 1;
                    if *entry == 0 {
                        running += 1;
                        Self::start(n.clone(), Arc::clone(self.fns.get(n).unwrap()), tx.clone())
                            .await;
                    }
                }
            }
        }

        if let Some(e) = err { Err(e) } else { Ok(()) }
    }

    async fn start(
        name: String,
        f: Arc<DynAsyncFn>,
        tx: mpsc::UnboundedSender<(String, Result<()>)>,
    ) {
        task::spawn(async move {
            let r = f().await;
            let _ = tx.send((name, r));
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::anyhow;
    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn test_zero() {
        let dag = Dag::default();
        let res = timeout(Duration::from_millis(100), dag.run()).await;
        assert!(res.is_ok());
        assert!(res.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_one() {
        let mut dag = Dag::default();
        dag.add_vertex("one", || async { Err(anyhow!("error")) });

        let res = timeout(Duration::from_millis(100), dag.run())
            .await
            .unwrap();
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "error");
    }

    #[tokio::test]
    async fn test_many_no_deps() {
        let mut dag = Dag::default();
        dag.add_vertex("one", || async { Err(anyhow!("error")) });
        dag.add_vertex("two", || async { Ok(()) });
        dag.add_vertex("three", || async { Ok(()) });
        dag.add_vertex("four", || async { Ok(()) });

        let res = timeout(Duration::from_millis(100), dag.run())
            .await
            .unwrap();
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "error");
    }

    #[tokio::test]
    async fn test_many_with_cycle() {
        let mut dag = Dag::default();
        dag.add_vertex("one", || async { Ok(()) });
        dag.add_vertex("two", || async { Ok(()) });
        dag.add_vertex("three", || async { Ok(()) });
        dag.add_vertex("four", || async { Ok(()) });
        dag.add_edge("one", "two");
        dag.add_edge("two", "three");
        dag.add_edge("three", "four");
        dag.add_edge("three", "one"); // cycle

        let res = timeout(Duration::from_millis(100), dag.run())
            .await
            .unwrap();
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "dependency cycle detected");
    }

    #[tokio::test]
    async fn test_invalid_to_vertex() {
        let mut dag = Dag::default();
        dag.add_vertex("one", || async { Ok(()) });
        dag.add_vertex("two", || async { Ok(()) });
        dag.add_vertex("three", || async { Ok(()) });
        dag.add_vertex("four", || async { Ok(()) });
        dag.add_edge("one", "two");
        dag.add_edge("two", "three");
        dag.add_edge("three", "four");
        dag.add_edge("three", "definitely-not-a-valid-vertex");

        let res = timeout(Duration::from_millis(100), dag.run())
            .await
            .unwrap();
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "missing vertex");
    }

    #[tokio::test]
    async fn test_invalid_from_vertex() {
        let mut dag = Dag::default();
        dag.add_vertex("one", || async { Ok(()) });
        dag.add_vertex("two", || async { Ok(()) });
        dag.add_vertex("three", || async { Ok(()) });
        dag.add_vertex("four", || async { Ok(()) });
        dag.add_edge("one", "two");
        dag.add_edge("two", "three");
        dag.add_edge("three", "four");
        dag.add_edge("definitely-not-a-valid-vertex", "three");

        let res = timeout(Duration::from_millis(100), dag.run())
            .await
            .unwrap();
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "missing vertex");
    }

    #[tokio::test]
    async fn test_many_with_deps_success() {
        let mut dag = Dag::default();
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();

        let tasks = vec![
            ("one", tx.clone()),
            ("two", tx.clone()),
            ("three", tx.clone()),
            ("four", tx.clone()),
            ("five", tx.clone()),
            ("six", tx.clone()),
            ("seven", tx.clone()),
        ];

        for (name, tx) in tasks {
            dag.add_vertex(name, move || {
                let tx = tx.clone();
                let name = name.to_string();
                async move {
                    let _ = tx.send(name);
                    Ok(())
                }
            });
        }

        dag.add_edge("one", "two");
        dag.add_edge("one", "three");
        dag.add_edge("two", "four");
        dag.add_edge("two", "seven");
        dag.add_edge("five", "six");

        let res = timeout(Duration::from_millis(100), dag.run())
            .await
            .unwrap();
        assert!(res.is_ok());

        let mut results = Vec::new();
        for _ in 0..7 {
            let val = timeout(Duration::from_millis(100), rx.recv())
                .await
                .unwrap();
            results.push(val.unwrap());
        }

        fn check_order(from: &str, to: &str, results: &[String]) {
            let from_index = results.iter().position(|x| x == from).unwrap();
            let to_index = results.iter().position(|x| x == to).unwrap();
            assert!(
                from_index <= to_index,
                "from vertex: {} came after to vertex: {}",
                from,
                to
            );
        }

        check_order("one", "two", &results);
        check_order("one", "three", &results);
        check_order("two", "four", &results);
        check_order("two", "seven", &results);
        check_order("five", "six", &results);
    }
}
