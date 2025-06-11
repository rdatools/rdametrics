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

# import re

from rdametrics import (
    states,
    chambers,
    combos,
    correlations_ensembles,
    metrics_by_category,
    ignore_by_category,
    stack_string,
    make_correlation_tables,
    average_correlation_tables,
)


##########

scores_path: str = "~/local/beta-ensembles/dataframe/contents/scores_df.parquet"
# output_dir: str = "analysis/correlations"
output_dir: str = "temp"

##########

scores_df = pd.read_parquet(os.path.expanduser(scores_path))

for category, all_metrics in metrics_by_category.items():
    if category == "general":
        continue

    subset_metrics: List[str] = [
        m for m in all_metrics if m not in ignore_by_category[category]
    ]

    D = make_correlation_tables(states, chambers, scores_df, subset_metrics)
    avg_corr = average_correlation_tables(D, subset_metrics, combos)

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

    print("  Generate LaTex for the table ...")
    col_dict = {score: stack_string(score) for score in subset_metrics}

    avg_corr_marked.rename(columns=col_dict, inplace=True)
    avg_corr_marked.to_latex(f"{output_dir}/{category}_avg_corr.tex", escape=False)

    pass  # for debugging

pass

### END ###
