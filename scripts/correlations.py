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

from rdametrics import (
    states,
    chambers,
    combos,
    metrics_by_category,
    subset_metrics,
    make_correlation_tables,
    average_correlation_tables,
    mark_consistent_signs,
    stack_string,
)


##########

scores_path: str = "~/local/beta-ensembles/prepackaged/scores/scores.parquet"
output_dir: str = "analysis/correlations"

##########

scores_df = pd.read_parquet(os.path.expanduser(scores_path))

for category, all_metrics in metrics_by_category.items():
    if category == "general":
        continue

    metrics: List[str] = subset_metrics(all_metrics, category)

    D = make_correlation_tables(states, chambers, scores_df, metrics)

    avg_corr = average_correlation_tables(D, metrics, combos)

    avg_corr_marked = mark_consistent_signs(avg_corr, D, metrics, combos)

    avg_corr_marked.to_excel(f"{output_dir}/{category}_avg_corr.xlsx")

    col_dict = {score: stack_string(score) for score in metrics}
    avg_corr_marked.rename(columns=col_dict, inplace=True)
    avg_corr_marked.to_latex(f"{output_dir}/{category}_avg_corr.tex", escape=False)

    pass  # for debugging

pass

### END ###
