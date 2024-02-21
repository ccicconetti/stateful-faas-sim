#!/usr/bin/env python3

import pandas as pd
import numpy as np

df = pd.read_csv("output.csv")

P_N = 100.0  # Watt

for policy in df["policy"].unique():
    for defragmentation_interval in df["defragmentation-interval"].unique():
        output = ""
        for node_capacity in sorted(list(df["node-capacity"].unique())):
            df_filtered = df.loc[
                (
                    (df["policy"] == policy)
                    & (df["node-capacity"] == node_capacity)
                    & (df["defragmentation-interval"] == defragmentation_interval)
                )
            ]

            if df_filtered.empty:
                continue

            # in kWh
            df_metric = (df_filtered["avg-busy-nodes"] * P_N * 86400) / 3600000
            mean = df_metric.values.mean()
            p025 = np.quantile(df_metric.values, 0.025)
            p975 = np.quantile(df_metric.values, 0.975)

            df_metric = df_filtered["avg-busy-nodes"]
            output += (
                f"{node_capacity} {mean} {p025} {p975} {df_metric.values.mean()}\n"
            )

        if output != "":
            with open(
                f"{policy}-{defragmentation_interval}.dat", "w", encoding="utf8"
            ) as outfile:
                outfile.write(output)
