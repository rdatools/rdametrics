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


def array_from_scores(
    xx: str, chamber: str, ensemble: str, metric: str, df: pd.DataFrame
) -> np.ndarray:
    """Extract a specific metric from the scores DataFrame into a numpy array."""

    a: np.ndarray = df[
        (df["state"] == xx) & (df["chamber"] == chamber) & (df["ensemble"] == ensemble)
    ][metric].to_numpy()

    return a


### END ###
