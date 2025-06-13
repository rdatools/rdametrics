"""
HELPERS
"""

import os
import numpy as np
import pandas as pd


def load_scores(scores_path: str) -> pd.DataFrame:
    """Read the scores .parquet file into a DataFrame."""

    df: pd.DataFrame = pd.read_parquet(os.path.expanduser(scores_path))

    return df


def arr_from_scores(
    xx: str, chamber: str, ensemble: str, metric: str, scores_df: pd.DataFrame
) -> np.ndarray:
    """Extract a metric for a state, chamber, and ensemble combination from the scores DataFrame into a numpy array."""

    arr: np.ndarray = scores_df[
        (scores_df["state"] == xx)
        & (scores_df["chamber"] == chamber)
        & (scores_df["ensemble"] == ensemble)
    ][metric].to_numpy()

    return arr


def df_from_scores(
    xx: str, chamber: str, ensemble: str, scores_df: pd.DataFrame
) -> pd.DataFrame:
    """Subset the scores DataFrame for a state, chamber, and ensemble combination."""

    subset_df: pd.DataFrame = scores_df[
        (scores_df["state"] == xx)
        & (scores_df["chamber"] == chamber)
        & (scores_df["ensemble"] == ensemble)
    ]

    return subset_df


### END ###
