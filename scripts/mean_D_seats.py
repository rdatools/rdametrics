#!/usr/bin/env python3

"""
DEM SEATS BY STATE/CHAMBER COMBO AND ENSEMBLE TYPE
(the values behind Table 6)
"""

from typing import List, Dict

import os
import pandas as pd

from rdapy import DISTRICTS_BY_STATE
from rdametrics import states, chambers, ensembles


scores_path: str = "~/local/beta-ensembles/dataframe/contents/scores_df.parquet"

df = pd.read_parquet(os.path.expanduser(scores_path))

table_6: List[str] = ["A0", "A1"] + ensembles[1:]  # Insert 'A1' after 'A0'

# NOTE -- "Dem seats" is 'fptp_seats' in the dataframe

d_seats: Dict[str, Dict[str, float]] = dict()
combos: List[str] = list()
for xx in states:
    for chamber in chambers:
        n_districts: int = DISTRICTS_BY_STATE[xx][chamber]
        combo: str = f"{xx} {n_districts}"
        combos.append(combo)
        d_seats[combo] = dict()

        for ensemble in table_6:
            mean_seats = df[
                (df["state"] == xx)
                & (df["chamber"] == chamber)
                & (df["ensemble"] == ensemble)
            ]["fptp_seats"].mean()
            d_seats[combo][ensemble] = float(mean_seats)

header: str = ",".join(v for v in table_6)  # In the proper order

print(f",{header}")
for combo in combos:
    values: str = ",".join([f"{d_seats[combo][e]:.3f}" for e in table_6])
    print(f"{combo},{values}")

pass  # for debugging


### END ###
