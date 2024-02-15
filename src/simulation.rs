use rand::{distributions::Distribution, SeedableRng};

enum Policy {
    StatelessMinNodes,
    StatelessMaxBalancing,
    StatefulBestFit,
    StatefulRandom,
}

#[derive(PartialEq, Eq)]
enum Event {
    /// 0: Event time.
    JobStart(u64),
    /// 0: Event time.
    /// 1: Job ID.
    JobEnd(u64, u64),
    /// 0: Event time.
    ExperimentEnd(u64),
}

impl Event {
    fn time(&self) -> u64 {
        match self {
            Self::JobStart(t) => *t,
            Self::JobEnd(t, _) => *t,
            Self::ExperimentEnd(t) => *t,
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
    job_factory: crate::job::JobFactory,
    job_interarrival_rng: rand::rngs::StdRng,
    job_lifetime_rng: rand::rngs::StdRng,
}

impl Simulation {
    pub fn new(seed: u64, state_mul: f64, arg_mul: f64) -> anyhow::Result<Self> {
        Ok(Self {
            job_factory: crate::job::JobFactory::new(seed, state_mul, arg_mul)?,
            job_interarrival_rng: rand::rngs::StdRng::seed_from_u64(seed + 1000000),
            job_lifetime_rng: rand::rngs::StdRng::seed_from_u64(seed),
        })
    }

    /// Run a simulation
    ///
    /// * `duration` - The duration of the simulation, in s.
    /// * `avg_interarrival` - The average interval between two jobs, in s.
    /// * `avg_lifetime` - The average lifetime of a job, in s.
    pub fn run(&mut self, duration: u64, avg_interarrival: f64, avg_lifetime: f64) {
        let mut events = std::collections::BinaryHeap::new();
        events.push(Event::JobStart(0));
        events.push(Event::ExperimentEnd(duration));
        let mut job_id = 0;
        let job_interarrival_rv = rand_distr::Exp::new(1.0 / avg_interarrival).unwrap();
        let job_duration_rv = rand_distr::Exp::new(1.0 / avg_lifetime).unwrap();
        'main_loop: loop {
            if let Some(event) = events.pop() {
                let now = event.time();
                match event {
                    Event::JobStart(_) => {
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
                        log::info!("D {} job ID {}", now, id);
                    }
                    Event::ExperimentEnd(_) => {
                        log::info!("E {}", now);
                        break 'main_loop;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simulation_run() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let mut sim = Simulation::new(42, 100.0, 100.0)?;
        sim.run(3600, 1.0, 10.0);

        Ok(())
    }
}
