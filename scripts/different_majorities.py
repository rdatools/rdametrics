#!/usr/bin/env python3

"""
DO ANY COMBOS HAVE DIFFERENT MAJORITIES IN DIFFERENT ENSEMBLES?
"""

from typing import List, Dict

import os
import pandas as pd
from collections import defaultdict

from rdapy import DISTRICTS_BY_STATE
from rdametrics import states, chambers, ensembles

df = pd.read_parquet(
    os.path.expanduser("~/local/beta-ensembles/dataframe/contents/scores_df.parquet")
)

# NOTE -- "Dem seats" is 'estimated_seats' in the dataframe

combos: List[str] = list()
majorities_by_combo: Dict[str, Dict[str, int]] = dict()

for xx in states:
    for chamber in chambers:
        combo: str = f"{xx}/{chamber}"
        combos.append(combo)

        n_districts: int = DISTRICTS_BY_STATE[xx][chamber]
        majority: int = n_districts // 2 + 1

        majorities: Dict[str, int] = defaultdict(int)

        for ensemble in ensembles:
            mean_seats = df[
                (df["state"] == xx)
                & (df["chamber"] == chamber)
                & (df["ensemble"] == ensemble)
            ]["estimated_seats"].mean()

            if mean_seats > majority:
                majorities["over"] += 1
            else:
                majorities["under"] += 1

        majorities_by_combo[combo] = majorities

for combo in combos:
    print(
        f"{combo:12} -- over: {majorities_by_combo[combo]["over"]:>2}, under: {majorities_by_combo[combo]["under"]:>2}"
    )
pass  # for debugging


### END ###
