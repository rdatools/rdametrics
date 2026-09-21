#!/usr/bin/env python3

"""
INSPECT POPULATION COMPACTNESS BY STATE / CHAMBER / ENSEMBLE
"""


from typing import List, Dict

import os
import pandas as pd
import numpy as np

from rdametrics import *

##########

scores_path: str = "~/local/beta-ensembles/prepackaged/scores/scores.parquet"

##########

scores_df = pd.read_parquet(os.path.expanduser(scores_path))

###

density_by_combo: Dict[Tuple[str, str, str], List[float]] = dict()
for xx in scores_df["state"].unique():
    for chamber in scores_df["chamber"].unique():
        for ensemble in scores_df["ensemble"].unique():
            density_by_combo[(xx, chamber, ensemble)] = list()
            scores = scores_df[
                (scores_df["state"] == xx) 
                & (scores_df["chamber"] == chamber) 
                & (scores_df["ensemble"] == ensemble)]["population_compactness"]
            density_by_combo[(xx, chamber, ensemble)] = scores.tolist()

            arr = np.array(density_by_combo[(xx, chamber, ensemble)])
            print(f"{xx}, {chamber:<8}, {ensemble:<4}: "
                f"min={arr.min():>13,.2f}  max={arr.max():>13,.2f}  "
                f"mean={arr.mean():>13,.2f}  stdev={arr.std():>13,.2f}")

            pass # for debugging
        pass # for debugging
    pass # for debugging

pass # for debugging

# stats_by_combo = {}
# for key, values in density_by_combo.items():
#     arr = np.array(values)
#     stats_by_combo[key] = {
#         'min': arr.min(),
#         'max': arr.max(),
#         'mean': arr.mean(),
#         'median': np.median(arr),
#         'stdev': arr.std(),
#         'q25': np.percentile(arr, 25),
#         'q75': np.percentile(arr, 75),
#     }

pass # for debugging

### END ###
