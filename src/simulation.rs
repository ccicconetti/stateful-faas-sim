use petgraph::visit::{EdgeRef, IntoNodeReferences};
use rand::{distributions::Distribution, seq::SliceRandom, SeedableRng};

#[derive(Debug, Clone)]
pub enum Policy {
    /// Fluid model: each task is assigned a fraction of a node.
    /// Only the minimum number of nodes are kept active to match the requests.
    StatelessMinNodes,
    /// Same as `Policy::StatelessMinNodes` but all the required nodes
    /// always remain active.
    StatelessMaxBalancing,
    /// When allocating the task of a job:
    /// - if there is another task of the same job that depends on this one
    ///   in a node with sufficient residual capacity, use that node
    /// - otherwise, allocate the task to the node that minimizes the
    ///   residual capacity, if any available (if not: add a new node)
    StatefulBestFit,
    /// Allocate task job by job, each assigned to a random node
    /// among those with sufficient residual capacity, otherwise
    /// a new node is added.
    StatefulRandom,
}

impl Policy {
    pub fn from(policy: &str) -> anyhow::Result<Self> {
        match policy {
            "stateless-min-nodes" => Ok(Policy::StatelessMinNodes),
            "stateless-max-balancing" => Ok(Policy::StatelessMaxBalancing),
            "stateful-best-fit" => Ok(Policy::StatefulBestFit),
            "stateful-random" => Ok(Policy::StatefulRandom),
            _ => Err(anyhow::anyhow!("unknown policy: {}", policy)),
        }
    }

    pub fn all() -> Vec<Policy> {
        vec![
            Policy::StatelessMinNodes,
            Policy::StatelessMaxBalancing,
            Policy::StatefulBestFit,
            Policy::StatefulRandom,
        ]
    }
}

impl std::fmt::Display for Policy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Policy::StatelessMinNodes => "stateless-min-nodes",
                Policy::StatelessMaxBalancing => "stateless-max-balancing",
                Policy::StatefulBestFit => "stateful-best-fit",
                Policy::StatefulRandom => "stateful-random",
            }
        )
    }
}

#[derive(PartialEq, Eq)]
enum Event {
    /// A new job arrives.
    /// 0: Event time.
    JobStart(u64),
    /// An active job ends.
    /// 0: Event time.
    /// 1: Job ID.
    JobEnd(u64, u64),
    /// The simulation ends.
    /// 0: Event time.
    ExperimentEnd(u64),
    /// Defragmentation occurs.
    /// 0: Event time.
    Defragmentation(u64),
}

impl Event {
    fn time(&self) -> u64 {
        match self {
            Self::JobStart(t)
            | Self::JobEnd(t, _)
            | Self::ExperimentEnd(t)
            | Self::Defragmentation(t) => *t,
        }
    }
}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other.time().partial_cmp(&self.time())
    }
}

impl Ord for Event {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[derive(Debug)]
pub struct Output {
    pub seed: u64,
    pub avg_busy_nodes: f64,
    pub total_traffic: f64,
    pub migration_rate: f64,
    pub execution_time: f64,
}

impl Output {
    pub fn header() -> &'static str {
        "seed,avg-busy-nodes,total-traffic,migration-rate,execution-time"
    }
}

impl std::fmt::Display for Output {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},{},{},{},{}",
            self.seed,
            self.avg_busy_nodes,
            self.total_traffic,
            self.migration_rate,
            self.execution_time
        )
    }
}

#[derive(Debug)]
pub struct Config {
    /// The duration of the simulation, in s.
    pub duration: u64,
    /// The average lifetime of a job, in s.
    pub job_lifetime: f64,
    /// The average interval between two jobs, in s.
    pub job_interarrival: f64,
    /// The rate at which the job is executed within its lifetime, in Hz.
    pub job_invocation_rate: f64,
    /// The capacity of each processing node, every 100 unit means 1 core
    pub node_capacity: usize,
    /// The periodic interval at which defragmentation occures, in s.
    pub defragmentation_interval: u64,
    /// The task allocation policy.
    pub policy: Policy,
    /// The state size multiplier applied to the task memory size.
    pub state_mul: f64,
    /// The argument size multiplier applied to the task memory size.
    pub arg_mul: f64,
    /// The seed to initialize pseudo-random number generators.
    pub seed: u64,
}

#[derive(Debug)]
struct Node {
    pub jobs: Vec<(u64, u32)>, // job ID, task ID within the job
}

impl Node {
    fn is_active(&self) -> bool {
        !self.jobs.is_empty()
    }
}

pub struct Simulation {
    job_factory: crate::job::JobFactory,
    job_interarrival_rng: rand::rngs::StdRng,
    job_lifetime_rng: rand::rngs::StdRng,
    active_jobs: std::collections::HashMap<u64, crate::job::Job>,

    // internal data structures used only with stateful policies
    nodes: Vec<Node>,
    allocations: std::collections::HashMap<u64, usize>, // key: hash of job ID and task ID; value: node ID
    allocate_rng: rand::rngs::StdRng,

    // configuration
    config: Config,
}

impl Simulation {
    pub fn new(config: Config) -> anyhow::Result<Self> {
        anyhow::ensure!(config.duration > 0, "vanishing duration");
        anyhow::ensure!(
            config.job_interarrival > 0.0,
            "vanishing avg job interarrival time"
        );
        anyhow::ensure!(config.job_lifetime > 0.0, "vanishing avg job lifetime");
        anyhow::ensure!(
            config.defragmentation_interval > 0,
            "vanishing defragmentation interval"
        );

        Ok(Self {
            job_factory: crate::job::JobFactory::new(
                config.seed,
                config.state_mul,
                config.arg_mul,
            )?,
            job_interarrival_rng: rand::rngs::StdRng::seed_from_u64(config.seed),
            job_lifetime_rng: rand::rngs::StdRng::seed_from_u64(config.seed + 1000000),
            active_jobs: std::collections::HashMap::new(),
            nodes: vec![],
            allocations: std::collections::HashMap::new(),
            allocate_rng: rand::rngs::StdRng::seed_from_u64(config.seed + 1100000),
            config,
        })
    }

    /// Run a simulation.
    pub fn run(&mut self) -> Output {
        // create the event queue and push initial events
        let mut events = std::collections::BinaryHeap::new();
        events.push(Event::JobStart(0));
        events.push(Event::ExperimentEnd(self.config.duration));
        events.push(Event::Defragmentation(self.config.defragmentation_interval));

        // initialize simulated time and ID of the first job
        let mut now = 0;
        let mut job_id = 0;

        // configure random variables for workload generation
        let job_interarrival_rv = rand_distr::Exp::new(1.0 / self.config.job_interarrival).unwrap();
        let job_duration_rv = rand_distr::Exp::new(1.0 / self.config.job_lifetime).unwrap();

        // initialize metric counters
        let mut avg_busy_nodes = 0.0;
        let mut max_busy_nodes = 0;
        let mut total_traffic = 0.0;
        let mut migration_rate = 0;

        // simulation loop
        let real_now = std::time::Instant::now();
        'main_loop: loop {
            if let Some(event) = events.pop() {
                let stat_interval = (event.time() - now) as f64;
                now = event.time();
                let (busy_nodes, traffic) = self.compute_stats(self.config.node_capacity);
                avg_busy_nodes += busy_nodes as f64 * stat_interval; // unit: s
                max_busy_nodes = usize::max(max_busy_nodes, busy_nodes);
                total_traffic += traffic * self.config.job_invocation_rate * stat_interval; // unit: bits
                match event {
                    Event::JobStart(_) => {
                        // create a new job and draw randomly its lifetime
                        let job = self.job_factory.make();
                        let job_lifetime =
                            job_duration_rv.sample(&mut self.job_lifetime_rng).ceil() as u64;
                        log::debug!(
                            "A {} job ID {} (lifetime {} s) {}",
                            now,
                            job_id,
                            job_lifetime,
                            job
                        );

                        // add it to the set of active jobs
                        let _insert_ret = self.active_jobs.insert(job_id, job.clone());
                        assert!(_insert_ret.is_none());

                        // allocate the tasks of a job to processing nodes
                        self.allocate(job_id, &job);

                        // schedule the end of this job
                        events.push(Event::JobEnd(now + job_lifetime, job_id));

                        // schedule a new job
                        job_id += 1;
                        events.push(Event::JobStart(
                            now + job_interarrival_rv
                                .sample(&mut self.job_interarrival_rng)
                                .ceil() as u64,
                        ));
                    }
                    Event::JobEnd(_, id) => {
                        log::debug!("T {} job ID {}", now, id);
                        self.deallocate(id);
                    }
                    Event::ExperimentEnd(_) => {
                        log::debug!("E {}", now);
                        break 'main_loop;
                    }
                    Event::Defragmentation(_) => {
                        log::debug!("D {}", now);

                        // perform optimization of the current active jobs
                        let (migration_traffic, num_migrations) = self.defragment();
                        total_traffic += migration_traffic;
                        migration_rate += num_migrations;

                        // schedule the next defragmentation
                        events.push(Event::Defragmentation(
                            now + self.config.defragmentation_interval,
                        ));
                    }
                }
            }
        }
        let execution_time = real_now.elapsed().as_secs_f64();

        // adapt the busy node metric to the different policies
        avg_busy_nodes = match self.config.policy {
            Policy::StatelessMinNodes | Policy::StatefulBestFit | Policy::StatefulRandom => {
                avg_busy_nodes / self.config.duration as f64
            }
            Policy::StatelessMaxBalancing => max_busy_nodes as f64,
        };

        // return the simulation output
        Output {
            avg_busy_nodes,
            total_traffic,
            seed: self.config.seed,
            migration_rate: migration_rate as f64 / self.config.duration as f64,
            execution_time,
        }
    }

    fn job_task_hash(job_id: u64, task_id: u32) -> u64 {
        assert!(task_id < 1000);
        job_id * 1000 + task_id as u64
    }

    fn allocate(&mut self, job_id: u64, job: &crate::job::Job) {
        match self.config.policy {
            Policy::StatelessMinNodes | Policy::StatelessMaxBalancing => {}
            Policy::StatefulBestFit => {
                'allocation_loop: for (index, weight) in job.graph.node_references() {
                    let task_id = index.index() as u32;
                    let cpu = weight.cpu_request;
                    assert!(cpu <= self.config.node_capacity);

                    // if there is a node hosting a task which is a predecessor of this
                    // node with enough residual capacity to host this task too, then
                    // use it
                    for pred_task_id in job.graph.neighbors_directed(index, petgraph::Incoming) {
                        match self.allocations.get(&Simulation::job_task_hash(
                            job_id,
                            pred_task_id.index() as u32,
                        )) {
                            Some(pred_node_id) => {
                                let pred_node = &self.nodes[*pred_node_id];
                                if let Some(_) = self.capacity_residual(pred_node, cpu) {
                                    self.add_job(job_id, task_id, *pred_node_id);
                                    continue 'allocation_loop;
                                }
                            }
                            None => {}
                        }
                    }

                    // find the active node that would leaves the smallest residual
                    // if this task is assigned to it
                    let mut candidates = vec![];
                    match self
                        .nodes
                        .iter()
                        .filter_map(|x| self.capacity_residual(x, cpu))
                        .min()
                    {
                        None => {
                            // there is no node (active or inactive) where this new task would fit
                        }
                        Some(min_residual) => {
                            // there is at least one node where the new task would fit, leaving
                            // min_residual as residual capacity, including both active and
                            // inactive nodes
                            //
                            // note that an inactive node will be selected below only if there
                            // are no active nodes that could fulfill the request, with no need
                            // of filtering on this condition explicitly, because we pick the
                            // node that leaves the smallest residual
                            for (node_id, node) in self.nodes.iter().enumerate() {
                                if let Some(residual) = self.capacity_residual(node, cpu) {
                                    if residual == min_residual {
                                        candidates.push(node_id);
                                    }
                                }
                            }
                        }
                    }
                    match candidates.choose(&mut self.allocate_rng) {
                        Some(node_id) => {
                            self.add_job(job_id, task_id, *node_id);
                        }
                        None => {
                            self.nodes.push(Node { jobs: vec![] });
                            self.add_job(job_id, task_id, self.nodes.len() - 1);
                        }
                    }
                }
            }
            Policy::StatefulRandom => {
                for (index, weight) in job.graph.node_references() {
                    let task_id = index.index() as u32;
                    let cpu = weight.cpu_request;
                    assert!(cpu <= self.config.node_capacity);
                    let mut candidates = vec![];
                    for (node_id, node) in self.nodes.iter().enumerate() {
                        if let Some(_) = self.capacity_residual(node, cpu) {
                            candidates.push(node_id);
                        }
                    }
                    match candidates.choose(&mut self.allocate_rng) {
                        Some(node_id) => {
                            self.add_job(job_id, task_id, *node_id);
                        }
                        None => match self
                            .nodes
                            .iter()
                            .enumerate()
                            .find(|(_node_id, node)| !node.is_active())
                        {
                            Some((node_id, _node)) => {
                                self.add_job(job_id, task_id, node_id);
                            }
                            None => {
                                self.nodes.push(Node { jobs: vec![] });
                                self.add_job(job_id, task_id, self.nodes.len() - 1);
                            }
                        },
                    }
                }
            }
        };
    }

    fn deallocate(&mut self, job_id: u64) {
        match self.config.policy {
            Policy::StatelessMinNodes | Policy::StatelessMaxBalancing => {}
            Policy::StatefulRandom | Policy::StatefulBestFit => {
                self.active_jobs
                    .get(&job_id)
                    .unwrap()
                    .graph
                    .node_indices()
                    .for_each(|x| self.del_job(job_id, x.index() as u32));
            }
        };
        let _remove_ret = self.active_jobs.remove(&job_id);
        assert!(_remove_ret.is_some());
    }

    fn add_job(&mut self, job_id: u64, task_id: u32, node_id: usize) {
        log::debug!("add job {}, task {}, to node {}", job_id, task_id, node_id);
        self.nodes[node_id].jobs.push((job_id, task_id));
        self.allocations
            .insert(Simulation::job_task_hash(job_id, task_id), node_id);
    }

    fn del_job(&mut self, job_id: u64, task_id: u32) {
        let node_id = self
            .allocations
            .remove(&Simulation::job_task_hash(job_id, task_id))
            .unwrap();
        log::debug!(
            "del job {}, task {}, from node {}",
            job_id,
            task_id,
            node_id
        );
        self.nodes[node_id]
            .jobs
            .retain(|(cur_job_id, cur_task_id)| *cur_job_id != job_id || *cur_task_id != task_id);
    }

    fn capacity_used(&self, node: &Node) -> usize {
        node.jobs
            .iter()
            .map(|(job_id, task_id)| {
                self.active_jobs
                    .get(job_id)
                    .unwrap()
                    .graph
                    .node_weight((*task_id).into())
                    .unwrap()
                    .cpu_request
            })
            .sum::<usize>()
    }

    /// Return the capacity residual if this node was allocated
    /// a new task with given capacity, or `None` if the new
    /// task would not fit into the node.
    fn capacity_residual(&self, node: &Node, new_capacity: usize) -> Option<usize> {
        let new_capacity_used = self.capacity_used(node) + new_capacity;
        if self.config.node_capacity >= new_capacity_used {
            Some(self.config.node_capacity - new_capacity_used)
        } else {
            None
        }
    }

    fn defragment(&mut self) -> (f64, u64) {
        match self.config.policy {
            Policy::StatelessMinNodes | Policy::StatelessMaxBalancing => (0.0, 0),
            Policy::StatefulBestFit => {
                let mut new_nodes = std::mem::take(&mut self.nodes);
                let mut new_allocations = std::mem::take(&mut self.allocations);
                assert!(self.nodes.is_empty());
                assert!(self.allocations.is_empty());
                for (job_id, job) in self.active_jobs.clone().into_iter() {
                    self.allocate(job_id, &job);
                }
                let mut migration_traffic = 0.0;
                let mut num_migrations = 0;
                // XXX
                (migration_traffic, num_migrations)
            }
            Policy::StatefulRandom => (0.0, 0),
        }
    }

    /// Return the statistics computed at this time: (number of busy nodes, total traffic).
    fn compute_stats(&mut self, node_capacity: usize) -> (usize, f64) {
        let busy_nodes = |x: &std::collections::HashMap<u64, crate::job::Job>| {
            (x.values().map(|x| x.total_cpu()).sum::<usize>() as f64 / node_capacity as f64).ceil()
                as usize
        };
        let tot_size = |x: &std::collections::HashMap<u64, crate::job::Job>| {
            x.values().map(|x| x.total_state_size()).sum::<usize>() as f64
                + x.values().map(|x| x.total_arg_size()).sum::<usize>() as f64
        };

        match self.config.policy {
            Policy::StatelessMinNodes | Policy::StatelessMaxBalancing => {
                (busy_nodes(&self.active_jobs), tot_size(&self.active_jobs))
            }
            Policy::StatefulBestFit | Policy::StatefulRandom => (
                self.nodes.iter().filter(|x| x.is_active()).count(),
                self.active_jobs
                    .iter()
                    .map(|(job_id, job)| {
                        let mut cnt = 0;
                        for node_ndx in job.graph.node_indices() {
                            for edge in job.graph.edges(node_ndx) {
                                let u = self
                                    .allocations
                                    .get(&Simulation::job_task_hash(
                                        *job_id,
                                        edge.source().index() as u32,
                                    ))
                                    .unwrap();
                                let v = self
                                    .allocations
                                    .get(&Simulation::job_task_hash(
                                        *job_id,
                                        edge.target().index() as u32,
                                    ))
                                    .unwrap();
                                if u != v {
                                    cnt += edge.weight().arg_size;
                                }
                            }
                        }
                        cnt
                    })
                    .sum::<usize>() as f64,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simulation_run() -> anyhow::Result<()> {
        for policy in Policy::all() {
            let _ = env_logger::try_init();
            let mut out = vec![];
            for i in 1..4 {
                let mut sim = Simulation::new(Config {
                    duration: 3600 * i,
                    job_lifetime: 10.0,
                    job_interarrival: 1.0,
                    job_invocation_rate: 5.0,
                    node_capacity: 1000,
                    defragmentation_interval: 300,
                    policy: policy.clone(),
                    state_mul: 100.0,
                    arg_mul: 100.0,
                    seed: 42,
                })?;
                out.push(sim.run());
            }
            println!("{} {:?}", policy, out);
            for i in 1..3 {
                assert!(out[i].avg_busy_nodes * 0.5 < out[0].avg_busy_nodes);
                assert!(out[i].avg_busy_nodes * 1.5 > out[0].avg_busy_nodes);
                assert!(out[i].total_traffic > ((1 + i) as f64 - 0.5) * out[0].total_traffic);
                assert!(out[i].total_traffic < ((1 + i) as f64 + 0.5) * out[0].total_traffic);
            }
        }

        Ok(())
    }
}
