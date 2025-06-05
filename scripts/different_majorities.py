#!/usr/bin/env python3

"""
DO ANY COMBOS HAVE DIFFERENT MAJORITIES IN DIFFERENT ENSEMBLES?
"""

from typing import Dict

import os
import pandas as pd
from collections import defaultdict

from rdapy import DISTRICTS_BY_STATE
from rdametrics import states, chambers, ensembles


scores_path: str = "~/local/beta-ensembles/dataframe/contents/scores_df.parquet"

df = pd.read_parquet(os.path.expanduser(scores_path))
ensembles = [e for e in ensembles if e not in ["A1", "A2", "A3", "A4", "Rev*"]]

for xx in states:
    subset_df = df[
        (df["state"] == xx) & (df["chamber"] == "congress") & (df["ensemble"] == "A0")
    ]
    first_row = subset_df.iloc[0]
    Vf: float = subset_df.iloc[0][
        "estimated_vote_pct"
    ]  # Same for all chambers & ensembles

    for chamber in chambers:
        combo: str = f"{xx}/{chamber}"

        n_districts: int = DISTRICTS_BY_STATE[xx][chamber]
        majority: int = n_districts // 2 + 1
        proportional: int = round(Vf * n_districts)

        majorities: Dict[str, int] = defaultdict(int)

        for ensemble in ensembles:
            mean_seats = df[
                (df["state"] == xx)
                & (df["chamber"] == chamber)
                & (df["ensemble"] == ensemble)
            ]["estimated_seats"].mean()

            if mean_seats > majority:
                majorities["more"] += 1
            else:
                majorities["less"] += 1

        print(
            f"{combo:12} -- districts: {n_districts:>3}, majority: {majority:>2}, D vote share: {Vf:>.2%}, proportional: {proportional:>2d}, # more: {majorities["more"]:>2}, # less: {majorities["less"]:>2}"
        )


### END ###
