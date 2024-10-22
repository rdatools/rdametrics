#!/usr/bin/env python3

"""
COMPARE DRA RATINGS (Y) to OTHER METRICS (X)
"""

from typing import List, Dict, Callable

import warnings

warnings.warn = lambda *args, **kwargs: None

from csv import DictReader
import matplotlib.pyplot as plt
import pandas as pd

from rdaensemble import ratings_dimensions


def is_realistic(ratings: List[int | float]) -> bool:
    """
    Do a set of ratings meet DRA's 'realistic' thresholds?

    See 'Realistic' @ https://medium.com/dra-2020/notable-maps-66d744933a48
    """

    thresholds: List[int] = [20, 10, 0, 20, 20]

    return all(r >= t for r, t in zip(ratings, thresholds))


def filter_scores(
    scores: Dict[str, str],
    *,
    roughly_equal: float = 0.01,
) -> bool:
    """Filter out maps that don't have 'roughly equal' population or are 'unrealistic'."""

    if "population_deviation" in scores:
        population_deviation: float = float(scores["population_deviation"])
        if population_deviation > roughly_equal:  # was (roughly_equal * 2):
            return False

    ratings: List[int | float] = [int(scores[d]) for d in ratings_dimensions]
    if not is_realistic(ratings):
        return False

    return True


def scores_to_df(
    scores_csv: str,
    fieldnames: List[str],
    fieldtypes: List[Callable],
    *,
    filter=False,
    roughly_equal: float = 0.01,
    verbose=False,
) -> pd.DataFrame:
    """Convert ratings in a scores CSV file into a Pandas dataframe."""

    scores: List[Dict[str, str]] = []
    total: int = 0
    filtered: int = 0
    with open(scores_csv, "r", encoding="utf-8-sig") as f:
        reader: DictReader[str] = DictReader(
            f, fieldnames=None, restkey=None, restval=None, dialect="excel"
        )
        for row in reader:
            total += 1
            if filter:
                if filter_scores(row, roughly_equal=roughly_equal):
                    filtered += 1
                    scores.append(row)
            else:
                scores.append(row)

    if verbose and filter:
        print()
        print(
            f"Note: Only {filtered} of {total} plans had 'roughly equal' population and were 'realistic'."  # per the DRA Notable Maps criteria.
        )
        print()

    data: List[List[str | int | float]] = []
    for score in scores:
        data.append([fieldtypes[i](score[f]) for i, f in enumerate(fieldnames)])

    df: pd.DataFrame = pd.DataFrame(data, columns=fieldnames)

    return df


# Setup

scores_csv = "../../iCloud/fileout/tradeoffs/NC/ensembles/NC20C_scores.csv"

y_metrics: List[str] = [
    "proportionality",
    "competitiveness",
    "minority",
    "compactness",
    "splitting",
]
y_types: List[type] = [int] * 5

x_inputs: List[str] = [
    "D",
    "C",
    "efficiency_gap",
    "average_margin",
    "proportional_opportunities",
    "alt_opportunity_districts",
    "defined_opportunity_districts",
    "polsby_popper",
    "counties_split",
    "county_splits",
]
x_inputs_types: List[type] = [
    int,
    int,
    float,
    float,
    int,
    float,
    int,
    float,
    int,
    int,
]
x_metrics: List[str] = [
    "efficiency_gap",
    "average_margin",
    "defined_opportunity_pct",
    "polsby_popper",
    "county_splits_ratio",
]

# Read the scores from a CSV file into a Pandas dataframe

fieldnames: List[str] = y_metrics + x_inputs
fieldtypes: List[Callable] = y_types + x_inputs_types

scores: pd.DataFrame = scores_to_df(
    scores_csv,
    fieldnames,
    fieldtypes,
)

# Compute derived metrics

scores["defined_opportunity_pct"] = (
    scores["defined_opportunity_districts"] / scores["proportional_opportunities"]
)
scores["county_splits_ratio"] = scores["county_splits"] / scores["D"]

# Plot each pair of metrics

for y_metric, x_metric in zip(y_metrics, x_metrics):

    y = scores[y_metric]
    x = scores[x_metric]

    # Create the scatter plot
    plt.figure(figsize=(10, 6))
    plt.scatter(x, y, color="blue", alpha=0.6)

    # Add labels and title
    plt.xlabel(x_metric)
    plt.ylabel(y_metric)
    # plt.title('2D Scatter Plot')

    # Add a grid
    plt.grid(True, linestyle="--", alpha=0.7)

    # Show the plot
    plt.show()

    pass

pass

### END ###
