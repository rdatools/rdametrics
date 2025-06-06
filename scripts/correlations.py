#!/usr/bin/env python3

"""
FIND CORRELATIONS BETWEEN METRICS W/IN EACH CATEGORY
ACROSS STATES, CHAMBERS, AND THE 11 ENSEMBLES
"""

from typing import List, Dict

import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

import os
import pandas as pd
import openpyxl

from rdapy import DISTRICTS_BY_STATE
from rdametrics import *


##########

scores_path: str = "~/local/beta-ensembles/dataframe/contents/scores_df.parquet"
output_dir: str = "analysis/correlations"

##########

scores_df = pd.read_parquet(os.path.expanduser(scores_path))
subset_ensembles = [e for e in ensembles if e not in ["A1", "A2", "A3", "A4", "Rev*"]]

ignore_by_category: Dict[str, List[str]] = {
    "general": [],
    "proportionality": [
        "estimated_seats",
        "pr_deviation",
        "mean_median_average_district",
        "turnout_bias",
    ],
    "competitiveness": [],
    "minority": [
        "proportional_opportunities",
        "proportional_coalitions",
    ],
    "compactness": [],
    "splitting": [],
}


for category, all_metrics in metrics_by_category.items():
    if category == "general":
        continue

    print(f"Processing {category} ...")

    # Subset the metrics to look at
    subset_metrics: List[str] = [
        m for m in all_metrics if m not in ignore_by_category[category]
    ]

    print(
        "  Find the correlation table for each state-chamber combo and 11 ensembles ..."
    )
    D = dict()
    for xx in states:
        for chamber in chambers:
            subset_df = scores_df[
                (scores_df["state"] == xx)
                & (scores_df["chamber"] == chamber)
                & (scores_df["ensemble"].isin(subset_ensembles))
            ][subset_metrics + ["ensemble"]]

            D[(xx, chamber)] = subset_df.corr(numeric_only=True)

    print("  Average the correlation tables over the state-chamber combinations ...")
    # (Need some extra code so the average for each cell is over the non-Nan values)
    sum_corr = pd.DataFrame(0.0, columns=subset_metrics, index=subset_metrics)
    count_corr = pd.DataFrame(0, columns=subset_metrics, index=subset_metrics)

    print("  Accumulate sum and count of non-NaN entries ...")
    for combo in combos:
        combo_df = D[combo]
        mask = combo_df.notna()
        sum_corr += combo_df.fillna(0)
        count_corr += mask.astype(int)

    print("  Compute average of non-NaN values ...")
    avg_corr = sum_corr / count_corr
    avg_corr = avg_corr.round(2)

    print(
        "Mark score pairs for which the sign of the correlation is consistent across all combos ..."
    )
    avg_corr_marked = avg_corr.copy().round(2)
    for score1 in subset_metrics:
        for score2 in subset_metrics:
            num_pos = len(
                [
                    1
                    for state_chamber in combos
                    if D[state_chamber].loc[score1, score2] > 0
                ]
            )
            num_neg = len(
                [
                    1
                    for state_chamber in combos
                    if D[state_chamber].loc[score1, score2] < 0
                ]
            )
            consistent_sign = 1 if num_neg == 0 else -1 if num_pos == 0 else 0
            if consistent_sign != 0:
                avg_corr_marked.loc[score1, score2] = (
                    f"*{avg_corr_marked.loc[score1, score2]}"
                )

    print("  Save the average correlation table to Excel ...")
    avg_corr_marked.to_excel(f"{output_dir}/{category}_avg_corr.xlsx")

    # print(f"Category correlations: {category}")
    # print(avg_corr)

    pass  # for debugging

pass

### END ###
