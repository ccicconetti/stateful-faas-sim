#!/usr/bin/env python3

import pandas as pd
import numpy as np

df = pd.read_csv("output.csv")

metrics = ["avg-busy-nodes", "total-traffic", "migration-rate", "execution-time"]

for state_mul in df["state-mul"].unique():
    with open(f"{state_mul}.dat", "w", encoding="utf8") as outfile:
        for defrag in sorted(list(df["defragmentation-interval"].unique())):

            line = f"{defrag}"
            df_filtered = df.loc[
                (
                    (df["defragmentation-interval"] == defrag)
                    & (df["state-mul"] == state_mul)
                )
            ]

            for metric in metrics:
                df_metric = df_filtered[metric]
                mean = df_metric.values.mean()
                p025 = np.quantile(df_metric.values, 0.025)
                p975 = np.quantile(df_metric.values, 0.975)
                line += f" {mean} {p025} {p975}"

            outfile.write(f"{line}\n")
