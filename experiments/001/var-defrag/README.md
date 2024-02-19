# Experiment 001 - Sub-experiment var-defrag

## Run

To see which experiments will run:

```bash
DRY_RUN=1 ./run.sh
```

Run the experiments with:

```bash
./run.sh
```

You can use the following environment variables to influence the execution:

- `SEED_INIT`: initial seed of experiments
- `SEED_END`: final seed of experiments; the total number of experiments is `$SEED_END-$SEED_INIT`
- `CONCURRENCY`: number of concurrent threads used

## Post-processing

Post-process `output.csv` database of output metrics with:

```bash
python3 post.py
```

## Visualization

Gnuplot scripts are included in the `graph/` directory, try:

```bash
cd graphs ; for i in *.plt ; do gnuplot -persist $i ; done ; cd -
```

## Artifacts

You can download the artifacts of the experiments with:

```bash
../../../scripts/download-artifacts.sh
```
