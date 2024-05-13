use clap::Parser;
use std::io::Write;

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
    defragmentation_interval: u64,
    /// State size multiplier applied to the task memory size.
    #[arg(long, default_value_t = 100.0)]
    state_mul: f64,
    /// Argument size multiplier applied to the task memory size.
    #[arg(long, default_value_t = 100.0)]
    arg_mul: f64,
    /// Initial seed to initialize the pseudo-random number generators
    #[arg(long, default_value_t = 0)]
    seed_init: u64,
    /// Final seed to initialize the pseudo-random number generators
    #[arg(long, default_value_t = 10)]
    seed_end: u64,
    /// Number of parallel workers
    #[arg(long, default_value_t = std::thread::available_parallelism().unwrap().get())]
    concurrency: usize,
    /// Allocation policy to use, use 'list' to get a list of policies
    #[arg(long, default_value_t = String::from("stateless-min-nodes"))]
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();

    anyhow::ensure!(
        args.additional_fields.matches(',').count() == args.additional_header.matches(',').count(),
        "--additional_fields and --additional_header have a different number of commas"
    );

    if args.policy == "list" {
        println!(
            "available policies: {}",
            stateful_faas_sim::simulation::Policy::all()
                .iter()
                .map(|x| format!("{}", x))
                .collect::<Vec<String>>()
                .join(", ")
        );
        return Ok(());
    }
    let policy = stateful_faas_sim::simulation::Policy::from(&args.policy)?;

    // create the configurations of all the experiments
    let configurations = std::sync::Arc::new(std::sync::Mutex::new(vec![]));
    for seed in args.seed_init..args.seed_end {
        configurations
            .lock()
            .unwrap()
            .push(stateful_faas_sim::simulation::Config {
                duration: args.duration,
                job_lifetime: args.job_lifetime,
                job_interarrival: args.job_interarrival,
                job_invocation_rate: args.job_invocation_rate,
                node_capacity: args.node_capacity,
                defragmentation_interval: args.defragmentation_interval,
                policy: policy.clone(),
                state_mul: args.state_mul,
                arg_mul: args.arg_mul,
                seed,
            });
    }

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    for i in 0..args.concurrency {
        let tx = tx.clone();
        let configurations = configurations.clone();
        tokio::spawn(async move {
            log::info!("spawned worker #{}", i);
            loop {
                let config;
                {
                    if let Some(val) = configurations.lock().unwrap().pop() {
                        config = Some(val);
                    } else {
                        break;
                    }
                }
                match stateful_faas_sim::simulation::Simulation::new(config.unwrap()) {
                    Ok(mut sim) => tx.send(sim.run()).unwrap(),
                    Err(err) => log::error!("error when running simulation: {}", err),
                };
            }
            log::info!("terminated worker #{}", i);
        });
    }
    let _ = || tx;

    // wait until all the simulations have been done
    let mut outputs = vec![];
    while let Some(output) = rx.recv().await {
        outputs.push(output);
    }

    // save output to file
    let header = !args.append
        || match std::fs::metadata(&args.output) {
            Ok(metadata) => metadata.len() == 0,
            Err(_) => true,
        };
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .append(args.append)
        .create(true)
        .truncate(!args.append)
        .open(args.output)?;

    if header {
        writeln!(
            &mut f,
            "{}{}",
            args.additional_header,
            stateful_faas_sim::simulation::Output::header()
        )?;
    }

    for output in outputs {
        writeln!(&mut f, "{}{}", args.additional_fields, output)?;
    }

    Ok(())
}
