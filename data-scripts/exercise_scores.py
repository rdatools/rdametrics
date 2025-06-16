#!/usr/bin/env python3

"""
TEST HARNESS TO EXERCISE LOADING & FETCHING SCORES
"""

import numpy as np
import pandas as pd

from data import states, chambers, ensembles, metrics, load_scores, arr_from_scores


##########

scores_path: str = "~/local/beta-ensembles/prepackaged/scores/scores.parquet"

##########

# Verify scores

print()
print("Loading scores from:", scores_path)
scores_df: pd.DataFrame = load_scores(scores_path)

print("Exercising scores DataFrame:")
for xx in states:
    for chamber in chambers:
        for ensemble in ensembles:
            for i, metric in enumerate(metrics):
                print(
                    f"  Fetching scores array for {xx}, {chamber}, {ensemble}: {metric} ..."
                )
                arr: np.ndarray = arr_from_scores(
                    xx, chamber, ensemble, metric, scores_df
                )
                assert (
                    arr.size > 0
                ), f"Empty array for {xx}, {chamber}, {ensemble}, {metric}"

print("All scores loaded successfully.")

pass

### END ###
