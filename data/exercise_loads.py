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

# Verify aggregates

print()
print("Exercising aggregates:")

for xx in states:
    for chamber in chambers:
        for ensemble in ensembles:
            for category, aggs in aggregates_by_category.items():
                loaded: List[List[Dict[str, Any]]] = [
                    load_aggregates(xx, chamber, ensemble, category, zip_dir)
                ]
                if category == "minority":
                    loaded.append(
                        load_aggregates(
                            xx,
                            chamber,
                            ensemble,
                            category,
                            zip_dir,
                            minority_dataset="cvap",
                        )
                    )

                for i, loaded_aggregates in enumerate(loaded):
                    vap_flavor: str = ""
                    if category == "minority":
                        vap_flavor = " (VAP)"
                        if i == 1:
                            vap_flavor = " (CVAP)"
                    for agg in aggs:
                        print(
                            f"  Fetching aggregate {agg}{vap_flavor} for {xx}, {chamber}, {ensemble} ..."
                        )
                        arr: np.ndarray = arr_from_aggregates(agg, loaded_aggregates)
                        assert (
                            arr.size > 0
                        ), f"Empty array for {xx}, {chamber}, {ensemble}, {agg}"

print("All aggregates loaded successfully.")

pass

### END ###
