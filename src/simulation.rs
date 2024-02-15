use rand::{distributions::Distribution, SeedableRng};

pub enum Policy {
    StatelessMinNodes,
    StatelessMaxBalancing,
    StatefulBestFit,
    StatefulRandom,
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

pub struct Simulation {
    policy: Policy,
    job_factory: crate::job::JobFactory,
    job_interarrival_rng: rand::rngs::StdRng,
    job_lifetime_rng: rand::rngs::StdRng,
    active_jobs: std::collections::HashMap<u64, crate::job::Job>,
}

impl Simulation {
    pub fn new(policy: Policy, seed: u64, state_mul: f64, arg_mul: f64) -> anyhow::Result<Self> {
        Ok(Self {
            policy,
            job_factory: crate::job::JobFactory::new(seed, state_mul, arg_mul)?,
            job_interarrival_rng: rand::rngs::StdRng::seed_from_u64(seed + 1000000),
            job_lifetime_rng: rand::rngs::StdRng::seed_from_u64(seed),
            active_jobs: std::collections::HashMap::new(),
        })
    }

    /// Run a simulation
    ///
    /// * `duration` - The duration of the simulation, in s.
    /// * `avg_interarrival` - The average interval between two jobs, in s.
    /// * `avg_lifetime` - The average lifetime of a job, in s.
    /// * `invocation_rate` - The rate at which the job is executed within its lifetime, in Hz.
    /// * `node_capacity` - Capacity of each processing node, every 100 unit means 1 core
    /// * `defragmentation_interval` - Periodic interval at which defragmentation occures, in s.
    ///
    pub fn run(
        &mut self,
        duration: u64,
        avg_interarrival: f64,
        avg_lifetime: f64,
        invocation_rate: f64,
        node_capacity: usize,
        defragmentation_interval: u64,
    ) -> (f64, f64) {
        assert!(duration > 0);
        assert!(avg_interarrival > 0.0);
        assert!(avg_lifetime > 0.0);
        assert!(invocation_rate > 0.0);
        assert!(defragmentation_interval > 0);

        let mut events = std::collections::BinaryHeap::new();
        events.push(Event::JobStart(0));
        events.push(Event::ExperimentEnd(duration));
        events.push(Event::Defragmentation(defragmentation_interval));
        let mut now = 0;
        let mut job_id = 0;
        let job_interarrival_rv = rand_distr::Exp::new(1.0 / avg_interarrival).unwrap();
        let job_duration_rv = rand_distr::Exp::new(1.0 / avg_lifetime).unwrap();
        let mut avg_busy_nodes = 0.0;
        let mut tot_traffic = 0.0;
        'main_loop: loop {
            if let Some(event) = events.pop() {
                let stat_interval = (event.time() - now) as f64;
                now = event.time();
                let (busy_nodes, traffic) = self.compute_stats(node_capacity);
                avg_busy_nodes += busy_nodes * stat_interval; // unit: s
                tot_traffic += traffic * invocation_rate * stat_interval; // unit: bits
                match event {
                    Event::JobStart(_) => {
                        // create a job and allocate it
                        let job = self.job_factory.make();
                        self.allocate(job_id, job);

                        // schedule the end of this job
                        let job_lifetime =
                            job_duration_rv.sample(&mut self.job_lifetime_rng).ceil() as u64;
                        log::info!("A {} job ID {} (lifetime {} s)", now, job_id, job_lifetime);
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
                        log::info!("T {} job ID {}", now, id);
                        let _remove_ret = self.active_jobs.remove(&id);
                        assert!(_remove_ret.is_some());
                    }
                    Event::ExperimentEnd(_) => {
                        log::info!("E {}", now);
                        break 'main_loop;
                    }
                    Event::Defragmentation(_) => {
                        log::info!("D {}", now);

                        // perform optimization of the current active jobs
                        tot_traffic += self.defragment();

                        // schedule the next defragmentation
                        events.push(Event::Defragmentation(now + defragmentation_interval));
                    }
                }
            }
        }
        (avg_busy_nodes / duration as f64, tot_traffic as f64)
    }

    fn allocate(&mut self, job_id: u64, job: crate::job::Job) {
        match self.policy {
            Policy::StatelessMinNodes => {}
            Policy::StatelessMaxBalancing => {}
            Policy::StatefulBestFit => panic!("not implemented"),
            Policy::StatefulRandom => panic!("not implemented"),
        };

        let _insert_ret = self.active_jobs.insert(job_id, job);
        assert!(_insert_ret.is_none());
    }

    fn defragment(&mut self) -> f64 {
        match self.policy {
            Policy::StatelessMinNodes => 0.0,
            Policy::StatelessMaxBalancing => 0.0,
            Policy::StatefulBestFit => panic!("not implemented"),
            Policy::StatefulRandom => panic!("not implemented"),
        }
    }

    /// Return the statistics computed at this time: (number of busy nodes, total traffic).
    fn compute_stats(&mut self, node_capacity: usize) -> (f64, f64) {
        match self.policy {
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
            let mut sim = Simulation::new(Policy::StatelessMinNodes, 42, 100.0, 100.0)?;
            let (busy_nodes, traffic) = sim.run(3600 * i, 1.0, 10.0, 5.0, 1000, 300);
            out.push((busy_nodes, traffic));
        }
        for i in 1..3 {
            assert!(out[i].0 * 0.5 < out[0].0);
            assert!(out[i].0 * 1.5 > out[0].0);
            assert!(out[i].1 > ((1 + i) as f64 - 0.5) * out[0].1);
            assert!(out[i].1 < ((1 + i) as f64 + 0.5) * out[0].1);
        }

        Ok(())
    }
}
