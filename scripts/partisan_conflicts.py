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
from rdametrics import states, chambers, ensembles


scores_path: str = "~/local/beta-ensembles/dataframe/contents/scores_df.parquet"

df = pd.read_parquet(os.path.expanduser(scores_path))
ensembles = [e for e in ensembles if e not in ["A1", "A2", "A3", "A4", "Rev*"]]

partisan_metrics: List[str] = [
    "disproportionality",
    "efficiency_gap",
    "seats_bias",
    "votes_bias",
    "geometric_seats_bias",
    "declination",
    "mean_median_statewide",
    "lopsided_outcomes",
]


def same_sign(a, b):
    return (a >= 0) == (b >= 0)


counters: Dict[str, Dict[str, Any]] = dict()
for m in partisan_metrics[1:]:
    counters[m] = {"state": set(), "chamber": set(), "ensemble": set(), "conflicts": 0}

total_plans: int = len(df)
for index, row in df.iterrows():
    xx: str = row["state"]
    chamber: str = row["chamber"]
    ensemble: str = row["ensemble"]

    for m in partisan_metrics[1:]:
        if same_sign(row[m], row["disproportionality"]):
            continue
        else:
            counters[m]["state"].add(xx)
            counters[m]["chamber"].add(chamber)
            counters[m]["ensemble"].add(ensemble)
            counters[m]["conflicts"] += 1

print("Partisan Conflicts* Summary:")
for m in partisan_metrics[1:]:
    conflicts: int = counters[m]["conflicts"]
    print(
        f"- {m}: {conflicts:,} of {total_plans:,} conflicts ({conflicts / total_plans:.1%}). "
    )
print(
    f"* When the sign of a metric is the opposite of the sign for simple 'disproportionality'."
)

pass

### END ###
