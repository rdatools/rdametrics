#!/usr/bin/env python3

"""
TEST HARNESS TO EXERCISE LOADING SCORES & AGGREGATES
"""

from typing import Dict, List, Any

import numpy as np
import pandas as pd

from constants import *
from helpers import load_scores, arr_from_scores, load_aggregates, arr_from_aggregates


##########

scores_path: str = "~/local/beta-ensembles/prepackaged/scores/scores.parquet"
zip_dir: str = "~/local/beta-ensembles/zipped"

##########

# Verify scores

# print()
# print("Loading scores from:", scores_path)
# scores_df: pd.DataFrame = load_scores(scores_path)

# print("Exercising scores DataFrame:")
# for xx in states:
#     for chamber in chambers:
#         for ensemble in ensembles:
#             for i, metric in enumerate(metrics):
#                 arr: np.ndarray = arr_from_scores(
#                     xx, chamber, ensemble, metric, scores_df
#                 )
#                 assert (
#                     arr.size > 0
#                 ), f"Empty array for {xx}, {chamber}, {ensemble}, {metric}"
#                 print(
#                     f"  Fetching scores array for {xx}, {chamber}, {ensemble}, {i+1:2d}: {metric} ..."
#                 )
#             break
#         break
#     break

# Verify aggregates

print()
print("Exercising aggregates:")

for xx in states:
    for chamber in chambers:
        for ensemble in ensembles:
            for category, aggs in aggregates_by_category.items():
                for agg in aggs:
                    print(
                        f"  Fetching aggregate {agg} for {xx}, {chamber}, {ensemble} ..."
                    )

            # loaded_aggregates: List[Dict[str, Any]] = load_aggregates(
            #     xx, chamber, ensemble, category, zip_dir
            # )

            # arr: np.ndarray = arr_from_aggregates("dem_by_district", loaded_aggregates)

            break
        break
    break

pass

### END ###
