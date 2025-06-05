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
from rdametrics import states, chambers, ensembles, metrics_by_category


scores_path: str = "~/local/beta-ensembles/dataframe/contents/scores_df.parquet"

df = pd.read_parquet(os.path.expanduser(scores_path))
ensembles = [e for e in ensembles if e not in ["A1", "A2", "A3", "A4", "Rev*"]]

ignore: List[str] = [
    "efficiency_gap_wasted_votes",
    "efficiency_gap_statewide",
    "mean_median_average_district",
    "turnout_bias",
]

D = dict()  # dictionary mapping (state, chamber) to the correlation table

for xx in states:
    for chamber in chambers:
        for category, metrics in metrics_by_category.items():
            if category != "proportionality":  # TODO
                continue

            subset_cols: List[str] = [c for c in metrics if c not in ignore]
            subset_df = df[
                (df["state"] == xx)
                & (df["chamber"] == chamber)
                & (df["ensemble"] == "A0")
            ][subset_cols]

            D[(xx, chamber)] = subset_df.corr(numeric_only=True)

    #         break  # for debugging
    #     break  # for debugging
    # break  # for debugging

print(D[("WI", "congress")])

pass

### END ###
