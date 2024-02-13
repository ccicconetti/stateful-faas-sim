#[derive(Debug)]
pub struct Vertex {
    /// CPU requested to execute this task, every 100 unit means 1 core
    cpu_request: usize,
    /// Size of the internal state of this task, in MB
    state_size: usize,
}

impl Vertex {
    pub fn new(cpu_request: usize, state_size: usize) -> Self {
        Self {
            cpu_request,
            state_size,
        }
    }
}

impl std::fmt::Display for Vertex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(cpu = {}, state = {})",
            self.cpu_request, self.state_size
        )
    }
}

#[derive(Debug)]
pub struct Edge {
    /// Size of the invocation arguments, in MB
    arg_size: usize,
}

impl std::fmt::Display for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(arg = {})", self.arg_size)
    }
}

pub struct Job {
    graph: petgraph::Graph<Vertex, Edge>,
}

impl Job {
    pub fn new(vertices: Vec<Vertex>) -> Self {
        let mut graph = petgraph::Graph::<Vertex, Edge>::new();
        for vertex in vertices {
            graph.add_node(vertex);
        }
        Self { graph }
    }

    pub fn to_dot(&self) {
        println!(
            "{}",
            petgraph::dot::Dot::with_config(&self.graph, &[petgraph::dot::Config::EdgeNoLabel])
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_ctor() {
        let job = Job::new(vec![
            Vertex::new(100, 10),
            Vertex::new(200, 20),
            Vertex::new(300, 30),
            Vertex::new(400, 40),
        ]);
        job.to_dot();
    }
}
