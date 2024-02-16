use crate::rv_histo;
use rand::seq::SliceRandom;
use rand::SeedableRng;

#[derive(Debug, Clone)]
pub struct Vertex {
    /// CPU requested to execute this task, every 100 unit means 1 core
    pub cpu_request: usize,
    /// Size of the internal state of this task, in MB
    pub state_size: usize,
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

#[derive(Debug, Clone)]
pub struct Edge {
    /// Size of the invocation arguments, in MB
    pub arg_size: usize,
}

impl Edge {
    pub fn new(arg_size: usize) -> Self {
        Self { arg_size }
    }
}

impl std::fmt::Display for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(arg = {})", self.arg_size)
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    pub graph: petgraph::Graph<Vertex, Edge>,
}

impl Job {
    pub fn new(vertices: Vec<Vertex>, edges: Vec<(u32, u32, Edge)>) -> Self {
        let mut graph = petgraph::Graph::<Vertex, Edge>::new();
        for vertex in vertices {
            graph.add_node(vertex);
        }
        for (u, v, weight) in edges {
            graph.update_edge(u.into(), v.into(), weight);
        }
        Self { graph }
    }

    pub fn total_cpu(&self) -> usize {
        self.graph.node_weights().map(|x| x.cpu_request).sum()
    }

    pub fn total_state_size(&self) -> usize {
        self.graph.node_weights().map(|x| x.state_size).sum()
    }

    pub fn total_arg_size(&self) -> usize {
        self.graph.edge_weights().map(|x| x.arg_size).sum()
    }

    pub fn print_to_dot(&self) {
        println!("{}", petgraph::dot::Dot::new(&self.graph))
    }
}

impl std::fmt::Display for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} tasks (tot cpu {}, tot state {}), {} invocations (tot args {})",
            self.graph.node_count(),
            self.total_cpu(),
            self.total_state_size(),
            self.total_arg_size(),
            self.graph.edge_count()
        )
    }
}

pub struct JobFactory {
    /// Number of tasks in this DAG
    num_rv: rv_histo::RvHisto,
    /// Critical path length, for a given number of tasks (saturates to 35)
    cpl_rv: std::collections::HashMap<u32, rv_histo::RvHisto>,
    /// Number of siblings per level, for a given cpl (saturates to 20)
    lvl_rv: std::collections::HashMap<u32, rv_histo::RvHisto>,
    /// Task CPU requested, every 100 unit means 1 core
    cpu_rv: rv_histo::RvHisto,
    /// Task memory requested, the fraction of 100 unit
    mem_rv: rv_histo::RvHisto,
    /// RNG to select random edges
    edge_rng: rand::rngs::StdRng,
    /// Multiplier to be applied to mem samples to obtain the state size of a task
    state_mul: f64,
    /// Multiplier to be applied to mem samples to obtain the argument size of an edge
    arg_mul: f64,
}

impl JobFactory {
    /// Create a factor of jobs initialized with the given pseudo-random number generator seed.
    pub fn new(seed: u64, state_mul: f64, arg_mul: f64) -> anyhow::Result<Self> {
        let mut seed_cnt = 0_u64;
        let mut next_seed = || {
            seed_cnt += 1;
            seed + 1000000 * seed_cnt
        };
        let num_rv = rv_histo::RvHisto::from_file(next_seed(), "data/task_num_dist.dat")?;
        let mut cpl_rv = std::collections::HashMap::new();
        for i in 2..=35 {
            cpl_rv.insert(
                i,
                rv_histo::RvHisto::from_file(
                    next_seed(),
                    format!("data/cpl_dist-{}.dat", i).as_str(),
                )?,
            );
        }
        let mut lvl_rv = std::collections::HashMap::new();
        for i in 1..=20 {
            lvl_rv.insert(
                i,
                rv_histo::RvHisto::from_file(
                    next_seed(),
                    format!("data/level_dist-{}.dat", i).as_str(),
                )?,
            );
        }
        let cpu_rv = rv_histo::RvHisto::from_file(next_seed(), "data/task_cpu_dist.dat")?;
        let mem_rv = rv_histo::RvHisto::from_file(next_seed(), "data/task_mem_dist.dat")?;

        Ok(Self {
            num_rv,
            cpl_rv,
            lvl_rv,
            cpu_rv,
            mem_rv,
            edge_rng: rand::rngs::StdRng::seed_from_u64(next_seed()),
            state_mul,
            arg_mul,
        })
    }

    /// Create a new random job.
    pub fn make(&mut self) -> Job {
        // draw the number of tasks and assign them random characteristics
        let num: u32 = self.num_rv.sample() as u32;
        assert!(
            num > 0,
            "invalid task_num_dist.dat file: cannot have 0 number of tasks"
        );
        let mut vertices = vec![];
        for _ in 0..num {
            vertices.push(Vertex::new(
                self.cpu_rv.sample() as usize,
                (self.mem_rv.sample() * self.state_mul) as usize,
            ));
        }

        // draw the number of tasks in the critical path (cpl = critical path length)
        let saturate = |x| std::cmp::min(x, 35);
        let cpl = std::cmp::min(
            num,
            match num {
                1 => 1,
                val => self.cpl_rv.get_mut(&saturate(val)).unwrap().sample() as u32,
            },
        );

        // assign a level (with 1-based index) to each task
        // - tasks in the critical path form a chain
        // - all other tasks are assigned as siblings of one of the tasks in the critical path
        let saturate = |x| std::cmp::max(std::cmp::min(x, 20), 1);
        let mut level = std::collections::HashMap::new();
        for i in 0..cpl {
            level.insert(i + 1, vec![i + 1]);
        }
        for i in cpl..num {
            loop {
                let lvl = self.lvl_rv.get_mut(&saturate(cpl)).unwrap().sample() as u32;
                assert!(
                    lvl > 0,
                    "wrong level_dist-X file: levels have 1-based indices"
                );
                if lvl <= cpl {
                    level.get_mut(&lvl).unwrap().push(i + 1);
                    break;
                }
            }
        }

        // create the critical path
        let mut edges = vec![];
        for i in 0..cpl - 1 {
            edges.push((
                i,
                i + 1,
                Edge::new((self.mem_rv.sample() * self.arg_mul) as usize),
            ));
        }

        // draw random edges
        for (lvl, tasks) in level.iter() {
            assert!(!tasks.is_empty());
            assert!(tasks[0] == *lvl);
            let next_lvl_tasks = match level.get(&(lvl + 1)) {
                Some(tasks) => tasks,
                None => continue,
            };
            // if we are here, then this is not the last level
            let num_edges_per_task = std::cmp::min(1, next_lvl_tasks.len() / tasks.len());
            for task in tasks {
                for other_task in
                    next_lvl_tasks.choose_multiple(&mut self.edge_rng, num_edges_per_task)
                {
                    if *other_task > cpl || (*task + 1) != *other_task {
                        edges.push((
                            task - 1,
                            other_task - 1,
                            Edge::new((self.mem_rv.sample() * self.arg_mul) as usize),
                        ));
                    }
                }
            }
        }

        // make sure that all edges exist
        let num_vertices = vertices.len() as u32;
        for (u, v, _) in &edges {
            if *v >= num_vertices {
                println!(
                    "{} {} {} {:?}\n{:?}\n{:?}",
                    num,
                    cpl,
                    vertices.len(),
                    level,
                    vertices,
                    edges
                );
            }
            assert!(*u < num_vertices);
            assert!(*v < num_vertices);
        }

        Job::new(vertices, edges)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_ctor() {
        let job = Job::new(
            vec![
                Vertex::new(100, 1),
                Vertex::new(200, 2),
                Vertex::new(300, 3),
                Vertex::new(400, 4),
            ],
            vec![
                (0, 1, Edge::new(10)),
                (0, 2, Edge::new(20)),
                (1, 3, Edge::new(30)),
                (2, 3, Edge::new(40)),
            ],
        );
        job.print_to_dot();

        assert_eq!(1000, job.total_cpu());
        assert_eq!(10, job.total_state_size());
        assert_eq!(100, job.total_arg_size());
    }

    #[test]
    fn test_job_factory() -> anyhow::Result<()> {
        let mut jf = JobFactory::new(42, 10000.0, 100.0)?;
        for _ in 0..10000 {
            let job = jf.make();
            let n = job.graph.node_count();
            let e = job.graph.edge_count();
            assert!(n >= 1 && n <= 199);
            assert!(n != 1 || e == 0);
            for task in job.graph.node_weights() {
                assert!(task.cpu_request >= 50 && task.cpu_request <= 800);
                assert!(task.state_size >= 2 * 100 && task.state_size <= 303 * 100);
            }
            for edge in job.graph.edge_weights() {
                assert!(edge.arg_size >= 2 && edge.arg_size <= 303);
            }
        }
        Ok(())
    }
}
