#!/usr/bin/env python3

"""
IDENTIFY FREQUENCY OF CONFLICTS BETWEEN METRICS THAT PUPORT TO MEASURE THE DEGREE TO WHICH
A PLAN FAVORS ONE PARTY OR ANOTHER.
"""

from typing import List, Dict

import os
import pandas as pd
from collections import defaultdict

from rdapy import DISTRICTS_BY_STATE
from rdametrics import states, chambers, ensembles


scores_path: str = "~/local/beta-ensembles/dataframe/contents/scores_df.parquet"

df = pd.read_parquet(os.path.expanduser(scores_path))
ensembles = [e for e in ensembles if e not in ["A1", "A2", "A3", "A4", "Rev*"]]

partisan_metrics: List[str] = [
    "estimated_seats",
    "pr_deviation",
    "disproportionality",
    "fptp_seats",
    "efficiency_gap_wasted_votes",
    "efficiency_gap_statewide",
    "efficiency_gap",
    "seats_bias",
    "votes_bias",
    "geometric_seats_bias",
    "declination",
    "mean_median_statewide",
    "mean_median_average_district",
    "turnout_bias",
    "lopsided_outcomes",
    "proportionality",
]

for index, row in df.iterrows():
    print(row)

    if index == 10:
        break

pass

### END ###
