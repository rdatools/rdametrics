"""
CORRELATION TABLE HELPER CODE
"""

from typing import Any, Dict, List, Tuple

import pandas as pd

from .constants import ensembles

correlations_ensembles = [
    e for e in ensembles if e not in ["A1", "A2", "A3", "A4", "Rev*"]
]

ignore_by_category: Dict[str, List[str]] = {
    "general": [],
    "proportionality": [
        # "estimated_seats",
        "pr_deviation",
        "mean_median_statewide",
        # "mean_median_average_district",
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


def subset_metrics(all_metrics: List[str], category: str) -> List[str]:
    subset: List[str] = [
        m for m in all_metrics if m not in ignore_by_category[category]
    ]

    return subset


def make_correlation_tables(
    states: List[str], chambers: List[str], scores_df, metrics: List[str]
) -> Dict:
    """Make correlation tables for all state-chamber combinations."""

    D = dict()
    for xx in states:
        for chamber in chambers:
            D[(xx, chamber)] = make_correlation_table(xx, chamber, scores_df, metrics)

    return D


def make_correlation_table(
    xx: str, chamber: str, scores_df, metrics: List[str]
) -> pd.DataFrame:
    """Create a correlation table for a state-chamber combo and its ensembles."""
    subset_df = scores_df[
        (scores_df["state"] == xx)
        & (scores_df["chamber"] == chamber)
        & (scores_df["ensemble"].isin(correlations_ensembles))
    ][metrics + ["ensemble"]]

    table = subset_df.corr(numeric_only=True)

    return table


def average_correlation_tables(
    D: Dict, subset_metrics: List[str], combos: List[Tuple[str, str]]
) -> pd.DataFrame:
    """Average the correlation tables over the state-chamber combinations."""

    # Average the correlation tables over the state-chamber combinations
    # (Need some extra code so the average for each cell is over the non-Nan values)
    sum_corr = pd.DataFrame(0.0, columns=subset_metrics, index=subset_metrics)
    count_corr = pd.DataFrame(0, columns=subset_metrics, index=subset_metrics)

    # Accumulate sum and count of non-NaN entries

    for combo in combos:
        combo_df = D[combo]
        mask = combo_df.notna()
        sum_corr += combo_df.fillna(0)
        count_corr += mask.astype(int)

    # Compute average of non-NaN values

    avg_corr = sum_corr / count_corr
    avg_corr = avg_corr.round(2)

    return avg_corr


def mark_consistent_signs(
    avg_corr: pd.DataFrame, D: Dict, metrics: List[str], combos: List[Tuple[str, str]]
) -> pd.DataFrame:
    """Mark score pairs for which the sign of the correlation is consistent across all combos"""

    avg_corr_marked = avg_corr.copy().round(2)

    for score1 in metrics:
        for score2 in metrics:
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

    return avg_corr_marked


### END ###
