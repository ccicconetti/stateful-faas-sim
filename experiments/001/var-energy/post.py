#!/usr/bin/env python3

import pandas as pd
import numpy as np

df = pd.read_csv("output.csv")

P_N = 50.0  # Watt
E_B_min = 0.05  # uW/b/s
E_B_max = 5

E_B = E_B_min
E_B_values = []
while E_B < E_B_max:
    E_B_values.append(E_B)
    E_B += 0.05

for policy in df["policy"].unique():
    with open(f"{policy}.dat", "w", encoding="utf8") as outfile:
        df_filtered = df.loc[(df["policy"] == policy)]

        for E_B in E_B_values:
            # in kWh
            df_metric = (
                df_filtered["avg-busy-nodes"] * P_N * 86400
                + df_filtered["total-traffic"] * 8000 * E_B * 1e-6
            ) / 3600000
            mean = df_metric.values.mean()
            p025 = np.quantile(df_metric.values, 0.025)
            p975 = np.quantile(df_metric.values, 0.975)
            line = f"{E_B} {mean} {p025} {p975}"
            print(f"{policy} {line}")

            outfile.write(f"{line}\n")
