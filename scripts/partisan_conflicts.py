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
    if a == 0.0 or b == 0.0:
        return True

    return a * b > 0


counters: Dict[str, Dict[str, Any]] = dict()
for m in partisan_metrics[1:]:
    counters[m] = {
        "combinations": set(),
        "conflicts": 0,
        "example": None,
        "value": None,
        "disproportionality": None,
    }

total_plans: int = len(df)
for index, row in df.iterrows():
    xx: str = row["state"]
    chamber: str = row["chamber"]
    ensemble: str = row["ensemble"]

    if ensemble not in ensembles:
        continue

    for m in partisan_metrics[1:]:
        if same_sign(row[m], row["disproportionality"]):
            continue
        else:
            counters[m]["combinations"].add((xx, chamber, ensemble))
            counters[m]["conflicts"] += 1

            if counters[m]["example"] is None or counters[m]["value"] < 0.005:
                counters[m]["example"] = row["map"]
                counters[m]["value"] = row[m]
                counters[m]["disproportionality"] = row["disproportionality"]

print("Partisan Conflicts Summary:")
for m in partisan_metrics[1:]:
    conflicts: int = counters[m]["conflicts"]
    combos: int = len(counters[m]["combinations"])
    name: str = counters[m]["example"]
    sample: float = counters[m]["value"]
    disp: float = counters[m]["disproportionality"]
    print(
        f"- {m}: {conflicts:,} of {total_plans:,} conflicts ({conflicts / total_plans:.1%})"
    )
    print(
        f"  across {combos} of 231 = 7 x 3 x 11 state, chamber, and ensemble combinations."
    )
    print(f"  Example: Map {name} has {sample:.4f} vs. disproportionality {disp:.2%}.")
print()
print(
    f"Where a 'conflict' is when the sign of the metric is the *opposite* of the sign for simple 'disproportionality'."
)

pass

### END ###
