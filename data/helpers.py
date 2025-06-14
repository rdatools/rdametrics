"""
HELPERS
"""

from typing import List, Dict, Any

import os, json
import numpy as np
import pandas as pd

from rdapy import smart_read
from constants import states, chambers, ensembles, aggregates


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


def arr_from_aggregates(
    xx: str,
    chamber: str,
    ensemble: str,
    aggregate: str,
    *,
    include_statewide: bool = False,
    file_path: str,
) -> np.ndarray:
    """
    Scan the aggregates file and return a by-district aggregrate for a given state, chamber, and ensemble.
    By default, the statewide values are not included.
    Returns a 2D numpy array where each row corresponds to a plan and each column corresponds to a district.
    """

    assert xx in states, f"Invalid state: {xx}"
    assert chamber in chambers, f"Invalid chamber: {chamber}"
    assert ensemble in ensembles, f"Invalid ensemble: {ensemble}"
    assert aggregate in aggregates, f"Invalid aggregate: {aggregate}"

    result: List = list()
    index: int = 0 if include_statewide else 1
    in_range: bool = False

    with smart_read(os.path.expanduser(file_path)) as input_stream:
        for i, line in enumerate(input_stream):
            r: Dict[str, Any] = json.loads(line)

            if (
                r["state"] == xx
                and r["chamber"] == chamber
                and r["ensemble"] == ensemble
            ):
                in_range = True
                agg: List[Any] = r["aggregates"][aggregate][index:]
                result.append(agg)
            elif in_range:
                # JSONL lines for states, chambers, and ensemble are in order by riding plan numbers,
                # so if records *stop* matching, stop processing.
                break

    return np.array(result)


### END ###
