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
subset_ensembles = [e for e in ensembles if e not in ["A1", "A2", "A3", "A4", "Rev*"]]

ignore_by_category: Dict[str, List[str]] = {
    "general": [],
    "proportionality": [
        "estimated_seats",
        "pr_deviation",
        "mean_median_average_district",
        "turnout_bias",
    ],
    "competitiveness": [],
    "minority": [],
    "compactness": [],
    "splitting": [],
}


for category, all_metrics in metrics_by_category.items():
    if category == "general":
        continue

    D = dict()  # dictionary mapping (state, chamber) to the correlation table

    # Subset the metrics to look at
    subset_metrics: List[str] = [
        m for m in all_metrics if m not in ignore_by_category[category]
    ]

    # Find the correlation table for each state-chamber combo for the 11 ensembles
    for xx in states:
        for chamber in chambers:
            subset_df = df[
                (df["state"] == xx)
                & (df["chamber"] == chamber)
                & (df["ensemble"].isin(subset_ensembles))
            ][subset_metrics + ["ensemble"]]

            D[(xx, chamber)] = subset_df.corr(numeric_only=True)

    # Average the correlation tables over the state-chamber combinations
    # (we need some extra code so the average for each cell is over the non-Nan values)
    sum_corr = pd.DataFrame(0.0, columns=subset_metrics, index=subset_metrics)
    count_corr = pd.DataFrame(0, columns=subset_metrics, index=subset_metrics)

    # Accumulate sum and count of non-NaN entries
    for combo in combos:
        df = D[combo]
        mask = df.notna()
        sum_corr += df.fillna(0)
        count_corr += mask.astype(int)

    # Compute average of non-NaN values
    avg_corr = sum_corr / count_corr
    avg_corr = avg_corr.round(2)

    print(f"Category correlations: {category}")
    print(avg_corr)

pass

### END ###
