#!/usr/bin/env python3

"""
SELECT STATISTICS FOR SELECT METRICS FOR THE R75 ENSEMBLE
"""

from typing import List, Dict

import os
import pandas as pd

from rdapy import DISTRICTS_BY_STATE
from rdametrics import states, chambers, ensembles


scores_path: str = "~/local/beta-ensembles/prepackaged/scores/scores.parquet"

df = pd.read_parquet(os.path.expanduser(scores_path))

metrics: List[str] = [
    "estimated_seats",
    "competitive_districts",
    "opportunity_districts",
    "coalition_districts",
    "polsby_popper",
    "reock",
    "county_splits",
    "county_splitting",
    "district_splitting",
]

# Subset to R75 ensemble
df_r75 = df[df["ensemble"] == "R75"]

# Initialize results dictionary
results = {}

# Compute statistics for each metric
for metric in metrics:
    results[metric] = {
        "min": round(df_r75[metric].min(), 4),
        "max": round(df_r75[metric].max(), 4),
        "mean": round(df_r75[metric].mean(), 4),
        "median": round(df_r75[metric].median(), 4),
    }

print(results)

pass  # for debugging

### END ###
