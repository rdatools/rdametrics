#!/usr/bin/env python3

"""
DEM SEATS BY STATE/CHAMBER COMBO AND ENSEMBLE TYPE
(the values behind Table 6)
"""

from typing import List, Dict

import os
import pandas as pd

from rdapy import DISTRICTS_BY_STATE

df = pd.read_parquet(
    os.path.expanduser("~/local/beta-ensembles/dataframe/contents/scores_df.parquet")
)

states: List[str] = sorted(list(df["state"].unique()))
chambers: List[str] = ["congress", "upper", "lower"]
ensembles: List[str] = list(df["ensemble"].unique())
table_6: List[str] = [
    "A0",  # Added this baseline
    "A1",
    "Pop-",  # Special hex character for the minus sign!
    "Pop+",
    "B",
    "Rev",
    "C",
    "D",
    "R25",
    "R50",
    "R75",
    "R100",
]

# NOTE -- "Dem seats" is 'estimated_seats' in the dataframe

d_seats: Dict[str, Dict[str, float]] = dict()
combos: List[str] = list()
for xx in states:
    for chamber in chambers:
        n_districts: int = DISTRICTS_BY_STATE[xx][chamber]
        combo: str = f"{xx} {n_districts}"
        combos.append(combo)
        d_seats[combo] = dict()

        for ensemble in ensembles:
            if ensemble not in table_6:
                continue

            # Compute the mean 'estimated_seats'
            mean_seats = df[
                (df["state"] == xx)
                & (df["chamber"] == chamber)
                & (df["ensemble"] == ensemble)
            ]["estimated_seats"].mean()
            d_seats[combo][ensemble] = float(mean_seats)

header: str = ",".join(v for v in table_6)  # In the proper order

print(f",{header}")
for combo in combos:
    values: str = ",".join([f"{d_seats[combo][e]:.2f}" for e in table_6])
    print(f"{combo},{values}")

pass  # for debugging


### END ###
