use rand::{distributions::Distribution, SeedableRng};
use std::io::prelude::*;

struct SampleFiles {
    /// Key: filename. Value: vector of (weight, value)
    samples: std::collections::HashMap<String, (Vec<usize>, Vec<f64>)>,
}

static SAMPLE_FILES: std::sync::OnceLock<std::sync::Mutex<SampleFiles>> =
    std::sync::OnceLock::new();

pub struct RvHisto {
    rng: rand::rngs::StdRng,
    values: Vec<f64>,
    rv: rand_distr::weighted_alias::WeightedAliasIndex<usize>,
    stats: incr_stats::incr::Stats,
    stats_w: incr_stats::incr::Stats,
}

impl RvHisto {
    #[cfg(test)]
    fn from_vector(seed: u64, values: Vec<f64>, weights: Vec<usize>) -> Self {
        assert!(values.len() == weights.len());
        assert!(!weights.is_empty());
        let (stats, stats_w) = RvHisto::vec_to_stats(values.as_slice(), weights.as_slice());
        Self {
            rng: rand::rngs::StdRng::seed_from_u64(seed),
            values,
            rv: rand_distr::weighted_alias::WeightedAliasIndex::new(weights).unwrap(),
            stats,
            stats_w,
        }
    }

    pub fn from_file(seed: u64, filename: &str) -> anyhow::Result<Self> {
        // initialize the database of samples files, if not yet done
        let _ = SAMPLE_FILES.set(std::sync::Mutex::new(SampleFiles {
            samples: std::collections::HashMap::new(),
        }));

        // read from file only if the values are not already cached
        let sample_files = SAMPLE_FILES.get().unwrap().lock().unwrap();
        let samples = match sample_files.samples.get(filename) {
            Some(val) => val.clone(),
            None => {
                let infile = std::fs::File::open(filename)?;
                let reader = std::io::BufReader::new(infile);

                let mut values = vec![];
                let mut weights = vec![];
                for (i, line) in reader.lines().enumerate() {
                    let line = line?;
                    let tokens = line.split(' ').collect::<Vec<&str>>();
                    anyhow::ensure!(tokens.len() == 2, format!("invalid line {}", i));
                    weights.push(tokens[0].parse::<f64>()? as usize);
                    values.push(tokens[1].parse::<f64>()?);
                }
                (weights, values)
            }
        };

        let (stats, stats_w) = RvHisto::vec_to_stats(samples.1.as_slice(), samples.0.as_slice());

        Ok(Self {
            rng: rand::rngs::StdRng::seed_from_u64(seed),
            values: samples.1,
            rv: rand_distr::weighted_alias::WeightedAliasIndex::new(samples.0)?,
            stats,
            stats_w,
        })
    }

    fn vec_to_stats(
        values: &[f64],
        weights: &[usize],
    ) -> (incr_stats::incr::Stats, incr_stats::incr::Stats) {
        let mut stats = incr_stats::incr::Stats::new();
        let _ = stats.array_update(values);
        let mut stats_w = incr_stats::incr::Stats::new();
        let multiplier = stats.count() as f64 / weights.iter().map(|x| *x as f64).sum::<f64>();
        let _ = stats_w.array_update(
            (0..values.len())
                .map(|x| values[x] * weights[x] as f64 * multiplier)
                .collect::<Vec<f64>>()
                .as_slice(),
        );
        (stats, stats_w)
    }

    pub fn sample(&mut self) -> f64 {
        self.values[self.rv.sample(&mut self.rng)]
    }

    pub fn min(&self) -> f64 {
        self.stats.min().unwrap_or_default()
    }

    pub fn mean(&self) -> f64 {
        self.stats_w.mean().unwrap_or_default()
    }

    pub fn max(&self) -> f64 {
        self.stats.max().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rv_histo_single_value() {
        let rvh = RvHisto::from_vector(42, (&[42.0]).to_vec(), (&[1]).to_vec());
        assert!(rvh.min() == 42.0);
        assert!(rvh.mean() == 42.0);
        assert!(rvh.max() == 42.0);
    }

    #[test]
    fn test_rv_histo_vector_ctor() {
        let v = vec![0.0_f64, 1_f64, 2_f64, 3_f64];
        let w = vec![1, 10, 1, 10];
        let mut counts = vec![0; v.len()];
        let mut rvh = RvHisto::from_vector(42, v, w);
        assert!(rvh.min() == 0.0);
        assert!(rvh.mean() == 42.0 / 22.0);
        assert!(rvh.max() == 3.0);
        for _ in 0..100000 {
            counts[rvh.sample() as usize] += 1;
        }
        assert_eq!((counts[1] as f64 / counts[0] as f64).round() as u64, 10);
        assert_eq!((counts[3] as f64 / counts[2] as f64).round() as u64, 10);
    }

    #[test]
    fn test_rv_histo_file_ctor() {
        let mut rvh = RvHisto::from_file(42, "data/task_mem_dist.dat")
            .expect("could not create a RvHisto from file");
        let mut counts = std::collections::HashMap::new();
        for _ in 0..100000 {
            let key = (100.0 * rvh.sample()) as u64;
            match counts.get_mut(&key) {
                Some(val) => *val += 1,
                None => {
                    let _ = counts.insert(key, 1_usize);
                }
            };
        }
        assert_eq!(0, counts.iter().filter(|x| *x.0 < 2 || *x.0 > 303).count());
        assert_eq!(100000_usize, counts.iter().map(|x| x.1).sum());
    }

    #[test]
    fn test_rv_histo_file_repeatable() {
        let filename = "data/task_mem_dist.dat";
        let mut rvh1 =
            RvHisto::from_file(42, filename).expect("could not create a RvHisto from file");
        let mut rvh2 =
            RvHisto::from_file(42, filename).expect("could not create a RvHisto from file");
        let mut rvh3 =
            RvHisto::from_file(43, filename).expect("could not create a RvHisto from file");
        let mut count_same = 0;
        for _ in 0..1000 {
            let s1 = rvh1.sample();
            let s2 = rvh2.sample();
            let s3 = rvh3.sample();
            assert!(s1 == s2);
            if s1 == s3 {
                count_same += 1;
            }
        }
        assert!(count_same < 500);
    }

    #[test]
    fn test_rv_histo_all_files_stats() {
        let mut files = vec![
            String::from("instance_cpu_dist.dat"),
            String::from("instance_duration_dist.dat"),
            String::from("instance_mem_dist.dat"),
            String::from("instance_num_dist.dat"),
            String::from("job_interval_dist.dat"),
            String::from("task_cpu_dist.dat"),
            String::from("task_duration_dist.dat"),
            String::from("task_mem_dist.dat"),
            String::from("task_num_dist.dat"),
        ];
        for i in 1..=20 {
            files.push(format!("level_dist-{}.dat", i));
        }
        for i in 2..=35 {
            files.push(format!("cpl_dist-{}.dat", i));
        }

        for file in files {
            let rvh = RvHisto::from_file(42, format!("data/{}", file).as_str())
                .expect(format!("could not create a RvHisto file: {}", file).as_str());
            assert!(rvh.min() <= rvh.mean());
            assert!(rvh.mean() <= rvh.max());
            println!("{}: {}, {}, {}", file, rvh.min(), rvh.mean(), rvh.max());
        }
    }
}
