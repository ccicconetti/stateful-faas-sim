use rand::{distributions::Distribution, SeedableRng};
use std::io::prelude::*;

struct RvHisto {
    rng: rand::rngs::StdRng,
    values: Vec<f64>,
    rv: rand_distr::weighted_alias::WeightedAliasIndex<usize>,
}

impl RvHisto {
    fn from_vector(seed: u64, values: Vec<f64>, weights: Vec<usize>) -> Self {
        Self {
            rng: rand::rngs::StdRng::seed_from_u64(seed),
            values,
            rv: rand_distr::weighted_alias::WeightedAliasIndex::new(weights).unwrap(),
        }
    }

    fn from_file(seed: u64, filename: &str) -> anyhow::Result<Self> {
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

        Ok(Self {
            rng: rand::rngs::StdRng::seed_from_u64(seed),
            values,
            rv: rand_distr::weighted_alias::WeightedAliasIndex::new(weights).unwrap(),
        })
    }

    fn sample(&mut self) -> f64 {
        self.values[self.rv.sample(&mut self.rng)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rv_histo_vector_ctor() {
        let v = vec![0.0_f64, 1_f64, 2_f64, 3_f64];
        let w = vec![1, 10, 1, 10];
        let mut counts = vec![0; v.len()];
        let mut rvh = RvHisto::from_vector(42, v, w);
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
}
