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

for xx in states:
    for chamber in chambers:
        subset_A0 = df[
            (df["state"] == xx) & (df["chamber"] == chamber) & (df["ensemble"] == "A0")
        ]["fptp_seats"]

        subset_Pop = df[
            (df["state"] == xx)
            & (df["chamber"] == chamber)
            & (df["ensemble"] == "Pop+")
        ]["fptp_seats"]

        are_all_equal = subset_A0.equals(subset_Pop)
        print(
            f"For {xx}/{chamber} and A0 and Pop+, all values are equal: {are_all_equal}"
        )

        # # Or element-wise comparison (if lengths might differ)
        # if len(subset_A0) == len(subset_Pop):
        #     element_wise_equal = (subset_A0.values == subset_Pop.values).all()
        #     print(f"Element-wise comparison: {element_wise_equal}")
        # else:
        #     print(
        #         f"Different lengths: A0 has {len(subset_A0)} rows, Pop+ has {len(subset_Pop)} rows"
        #     )

# for index, row in df.iterrows():
#     xx: str = row["state"]
#     chamber: str = row["chamber"]
#     ensemble: str = row["ensemble"]

#     print(f"{xx:>2} {chamber:>10} {ensemble:>3} -- {row}")

#     if index == 10:
#         break

pass

### END ###
