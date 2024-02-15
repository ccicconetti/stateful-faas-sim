use clap::Parser;

#[derive(Debug, clap::Parser)]
#[command(long_about = None)]
struct Args {
    /// Duration of the simulation experiment, in s
    #[arg(long, default_value_t = 3600)]
    duration: u64,
    /// Average lifetime duration of a job, in s
    #[arg(long, default_value_t = 10.0)]
    job_lifetime: f64,
    /// Average inter-arrival between consecutive jobs, in s
    #[arg(long, default_value_t = 1.0)]
    job_interarrival: f64,
    /// Invocation rate of a job in its lifetime, in Hz
    #[arg(long, default_value_t = 5.0)]
    job_invocation_rate: f64,
    /// Node capacity, every 100 unit means 1 core
    #[arg(long, default_value_t = 1000)]
    node_capacity: usize,
    /// Defragmentation interval, ins
    #[arg(long, default_value_t = 300)]
    defragmentation_interval: usize,
    /// Initial seed to initialize the pseudo-random number generators
    #[arg(long, default_value_t = 0)]
    seed_init: u64,
    /// Final seed to initialize the pseudo-random number generators
    #[arg(long, default_value_t = 10)]
    seed_end: u64,
    /// Number of parallel workers
    #[arg(long, default_value_t = std::thread::available_parallelism().unwrap().get())]
    concurrency: usize,
    /// Allocation policy to use
    #[arg(long, default_value_t = String::from("StatelessMinNodes"))]
    policy: String,
    /// Name of the CSV output file where to save the metrics collected.
    #[arg(long, default_value_t = String::from("out.csv"))]
    output: String,
    /// Append to the output file.
    #[arg(long, default_value_t = false)]
    append: bool,
    /// Additional fields recorded in the CSV output file.
    #[arg(long, default_value_t = String::from(""))]
    additional_fields: String,
    /// Header of additional fields recorded in the CSV output file.
    #[arg(long, default_value_t = String::from(""))]
    additional_header: String,
}

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();

    // let mut sim = Simulation::new(Policy::StatelessMinNodes, 42, 100.0, 100.0)?;
    // let (busy_nodes, traffic) = sim.run(3600 * i, 1.0, 10.0, 5.0, 1000, 300);

    Ok(())
}
