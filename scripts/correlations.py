#!/usr/bin/env python3

"""
IDENTIFY FREQUENCY OF CONFLICTS BETWEEN METRICS THAT PUPORT TO MEASURE THE DEGREE TO WHICH
A PLAN FAVORS ONE PARTY OR ANOTHER.
"""

from typing import List, Dict, Any, Set

import os
import pandas as pd
from collections import defaultdict

from rdapy import DISTRICTS_BY_STATE
from rdametrics import *


scores_path: str = "~/local/beta-ensembles/dataframe/contents/scores_df.parquet"

df = pd.read_parquet(os.path.expanduser(scores_path))
ensembles = [e for e in ensembles if e not in ["A1", "A2", "A3", "A4", "Rev*"]]

ignore: List[str] = [
    "efficiency_gap_wasted_votes",
    "efficiency_gap_statewide",
    "mean_median_average_district",
    "turnout_bias",
]

for category, all_metrics in metrics_by_category.items():
    if category != "proportionality":  # TODO
        continue

    D = dict()  # dictionary mapping (state, chamber) to the correlation table
    metrics: List[str] = [c for c in all_metrics if c not in ignore]

    for xx in states:
        for chamber in chambers:
            subset_df = df[
                (df["state"] == xx)
                & (df["chamber"] == chamber)
                # & (df["ensemble"] == "ensemble")
            ][metrics + ["ensemble"]]

            D[(xx, chamber)] = subset_df.corr(numeric_only=True)

    sum_corr = pd.DataFrame(0.0, columns=metrics, index=metrics)
    count_corr = pd.DataFrame(0, columns=metrics, index=metrics)

    for combo in combos:
        df = D[combo]
        mask = df.notna()
        sum_corr += df.fillna(0)
        count_corr += mask.astype(int)

    avg_corr = sum_corr / count_corr
    avg_corr = avg_corr.round(2)

    print(f"Category: {category}")
    print(avg_corr)

pass

### END ###
