#!/usr/bin/env python3

import pandas as pd
import numpy as np

df = pd.read_csv("output.csv")

P_N = 100.0  # Watt
E_B_values = [0.05, 5]  # uW/b/s

for E_B in E_B_values:
    for policy in df["policy"].unique():
        with open(f"{policy}-{E_B}.dat", "w", encoding="utf8") as outfile:
            for job_lifetime in sorted(list(df["job-lifetime"].unique())):

                df_filtered = df.loc[
                    ((df["policy"] == policy) & (df["job-lifetime"] == job_lifetime))
                ]

                # in kWh
                df_metric = (
                    df_filtered["avg-busy-nodes"] * P_N * 86400
                    + df_filtered["total-traffic"] * 8000 * E_B * 1e-6
                ) / 3600000
                mean = df_metric.values.mean()
                p025 = np.quantile(df_metric.values, 0.025)
                p975 = np.quantile(df_metric.values, 0.975)
                line = f"{job_lifetime} {mean} {p025} {p975}"

                outfile.write(f"{line}\n")
