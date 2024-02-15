use rand::{distributions::Distribution, SeedableRng};

#[derive(Debug)]
pub enum Policy {
    StatelessMinNodes,
    StatelessMaxBalancing,
    StatefulBestFit,
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
            Self::JobStart(t) => *t,
            Self::JobEnd(t, _) => *t,
            Self::ExperimentEnd(t) => *t,
            Self::Defragmentation(t) => *t,
        }
    }
}

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
}

impl Output {
    pub fn header() -> &'static str {
        "seed,avg-busy-nodes,total-traffic,migration-rate"
    }
}

impl std::fmt::Display for Output {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},{},{},{}",
            self.seed, self.avg_busy_nodes, self.total_traffic, self.migration_rate
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

pub struct Simulation {
    job_factory: crate::job::JobFactory,
    job_interarrival_rng: rand::rngs::StdRng,
    job_lifetime_rng: rand::rngs::StdRng,
    active_jobs: std::collections::HashMap<u64, crate::job::Job>,
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
            job_interarrival_rng: rand::rngs::StdRng::seed_from_u64(config.seed + 1000000),
            job_lifetime_rng: rand::rngs::StdRng::seed_from_u64(config.seed),
            active_jobs: std::collections::HashMap::new(),
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
        let mut total_traffic = 0.0;
        let mut migration_rate = 0.0;

        // simulation loop
        'main_loop: loop {
            if let Some(event) = events.pop() {
                let stat_interval = (event.time() - now) as f64;
                now = event.time();
                let (busy_nodes, traffic) = self.compute_stats(self.config.node_capacity);
                avg_busy_nodes += busy_nodes * stat_interval; // unit: s
                total_traffic += traffic * self.config.job_invocation_rate * stat_interval; // unit: bits
                match event {
                    Event::JobStart(_) => {
                        // create a job and allocate it
                        let job = self.job_factory.make();
                        self.allocate(job_id, job);

                        // schedule the end of this job
                        let job_lifetime =
                            job_duration_rv.sample(&mut self.job_lifetime_rng).ceil() as u64;
                        log::debug!("A {} job ID {} (lifetime {} s)", now, job_id, job_lifetime);
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
                        let _remove_ret = self.active_jobs.remove(&id);
                        assert!(_remove_ret.is_some());
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

        // return the simulation output
        avg_busy_nodes /= self.config.duration as f64;
        migration_rate /= self.config.duration as f64;
        Output {
            avg_busy_nodes,
            total_traffic,
            seed: self.config.seed,
            migration_rate,
        }
    }

    fn allocate(&mut self, job_id: u64, job: crate::job::Job) {
        match self.config.policy {
            Policy::StatelessMinNodes => {}
            Policy::StatelessMaxBalancing => {}
            Policy::StatefulBestFit => panic!("not implemented"),
            Policy::StatefulRandom => panic!("not implemented"),
        };

        let _insert_ret = self.active_jobs.insert(job_id, job);
        assert!(_insert_ret.is_none());
    }

    fn defragment(&mut self) -> (f64, f64) {
        match self.config.policy {
            Policy::StatelessMinNodes => (0.0, 0.0),
            Policy::StatelessMaxBalancing => (0.0, 0.0),
            Policy::StatefulBestFit => panic!("not implemented"),
            Policy::StatefulRandom => panic!("not implemented"),
        }
    }

    /// Return the statistics computed at this time: (number of busy nodes, total traffic).
    fn compute_stats(&mut self, node_capacity: usize) -> (f64, f64) {
        match self.config.policy {
            Policy::StatelessMinNodes => (
                (self
                    .active_jobs
                    .values()
                    .map(|x| x.total_cpu())
                    .sum::<usize>() as f64
                    / node_capacity as f64)
                    .ceil(),
                self.active_jobs
                    .values()
                    .map(|x| x.total_state_size())
                    .sum::<usize>() as f64
                    + self
                        .active_jobs
                        .values()
                        .map(|x| x.total_arg_size())
                        .sum::<usize>() as f64,
            ),
            Policy::StatelessMaxBalancing => panic!("not implemented"),
            Policy::StatefulBestFit => panic!("not implemented"),
            Policy::StatefulRandom => panic!("not implemented"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simulation_run() -> anyhow::Result<()> {
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
                policy: Policy::StatelessMinNodes,
                state_mul: 100.0,
                arg_mul: 100.0,
                seed: 42,
            })?;
            out.push(sim.run());
        }
        for i in 1..3 {
            assert!(out[i].avg_busy_nodes * 0.5 < out[0].avg_busy_nodes);
            assert!(out[i].avg_busy_nodes * 1.5 > out[0].avg_busy_nodes);
            assert!(out[i].total_traffic > ((1 + i) as f64 - 0.5) * out[0].total_traffic);
            assert!(out[i].total_traffic < ((1 + i) as f64 + 0.5) * out[0].total_traffic);
        }

        Ok(())
    }
}
