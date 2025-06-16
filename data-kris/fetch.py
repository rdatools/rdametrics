#!/usr/bin/env python3

"""
'THUNKING' BETWEEN SCORES & AGGREGATES HELPERS AND KRIS' ANALYSIS CODE
"""

from typing import List, Dict

import os
import json
import pandas as pd
import numpy as np

from rdapy import DISTRICTS_BY_STATE
from data import (
    states,
    load_scores,
    arr_from_scores,
    load_aggregates,
    arr_from_aggregates,
)

debug: bool = False  # Set to True to run the test code at the end of this file

########## SUPPORT CODE ##########

# Get the scores.parquet and zip files locations

# NOTE - Might not need the next two lines if the config.json is already in the same directory as this script.
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, "config.json")
with open(config_path, "r") as file:
    config = json.load(file)
scores_path = config["scores-path"]
zip_dir = config["zip-dir"]


def _extract_num_seats(districts_by_state):
    """Extract num_seats_dict from DISTRICTS_BY_STATE."""

    num_seats_dict = {}

    for state, state_data in districts_by_state.items():
        for chamber, seats in state_data.items():
            if seats is not None:  # Skip None values
                num_seats_dict[(state, chamber)] = seats

    return num_seats_dict


def _calc_d_vote_share(xx: str, chamber: str, ensemble: str) -> np.ndarray:
    """Calculate the two-party Democratic vote share"""

    aggregates_subset = load_aggregates(xx, chamber, ensemble, "partisan", zip_dir)

    arrays: List = list()
    for aggregate in ["dem_by_district", "tot_by_district"]:
        arr = arr_from_aggregates(aggregate, aggregates_subset)  # No statewide values
        arrays.append(arr)

    to_return = []
    # No metadata record
    for i, (dem_counts, tot_counts) in enumerate(zip(arrays[0], arrays[1])):
        dem_portions = [
            dem_counts[i] / tot_counts[i] for i in range(dem_counts.shape[0])
        ]
        dem_portions.sort()
        to_return.append(np.array(dem_portions))

    return np.array(to_return)


_score_mapping: Dict[str, str] = {
    "Reock": "reock",
    "Polsby-Popper": "polsby_popper",
    "cut edges": "cut_score",
    "Dem seats": "fptp_seats",
    "efficiency gap": "efficiency_gap_wasted_votes",
    "mean-median": "mean_median_average_district",
    "seat bias": "geometric_seats_bias",
    "competitive districts": "competitive_district_count",
    "average margin": "average_margin",
    "MMD black": "mmd_black",
    "MMD hispanic": "mmd_hispanic",
    "county splits": "county_splits",
    "counties split": "counties_split",
    # Extend this mapping, to support more scores
}


def _map_score_name(user_name: str) -> str:
    """Map Kris' score names to the names in the scores dataframe."""

    if user_name == "by_district":
        return "by_district"

    if user_name in _score_mapping:
        return _score_mapping[user_name]
    else:
        raise ValueError(f"Unknown user score: {user_name}. Please check the mapping.")


_ensemble_mapping: Dict[str, str] = {
    "base0": "A0",
    "base1": "A1",
    "base2": "A2",
    "base3": "A3",
    "base4": "A4",
    "pop_minus": "Pop-",
    "pop_plus": "Pop+",
    "ust": "C",
    "distpair": "B",
    "distpair_ust": "D",
    "reversible": "Rev",
    "county25": "R25",
    "county50": "R50",
    "county75": "R75",
    "county100": "R100",
}


def _map_ensemble_name(user_name: str) -> str:
    """Map Kris' ensemble names to the names in the helpers."""

    if user_name in _ensemble_mapping:
        return _ensemble_mapping[user_name]
    else:
        raise ValueError(
            f"Unknown user ensemble: {user_name}. Please check the mapping."
        )


########## MODIFIED CODE ##########

state_list = states
num_seats_dict = _extract_num_seats(DISTRICTS_BY_STATE)

# Tally the Dem voteshare for each state
scores_df: pd.DataFrame = load_scores(scores_path)
state_to_dem_voteshare = dict()
for state in state_list:
    a = arr_from_scores(state, "congress", "A0", "estimated_vote_pct", scores_df)[0]
    state_to_dem_voteshare[state] = a

##########


def fetch_score_array(state, chamber, ensemble_type, score):
    """
    Fetches the score array for the given state, chamber and ensemble_type.
    If score is 'reock', 'polsby_popper', etc.,
    then it returns 1D array containing the scores of the maps in the ensemble.
    If score == 'by_district', then it returns a 2D array containing,
    for each map in the ensemble, an ordered array recording the dem_portions of the districts of the map.
    """

    # preface a partisan score with 'maj ' to make it with respect to the majority party
    if score[:3] == "maj":
        a = fetch_score_array(state, chamber, ensemble_type, score[4:])
        if state_to_dem_voteshare[state] > 0.5:
            return a
        else:
            return num_seats_dict[(state, chamber)] - a

    # the total should include all three MMD scores
    if score == "MMD coalition":
        return (
            fetch_score_array(state, chamber, ensemble_type, "mmd_black")
            + fetch_score_array(state, chamber, ensemble_type, "mmd_hispanic")
            + fetch_score_array(state, chamber, ensemble_type, "mmd_coalition")
        )

    # Either a simple plan-level score or the D vote share

    ensemble = _map_ensemble_name(ensemble_type)
    col_name: str = _map_score_name(score)

    if score == "by_district":
        return _calc_d_vote_share(state, chamber, ensemble)
    else:  # a non-by-district score
        return arr_from_scores(state, chamber, ensemble, col_name, scores_df)


########## TEST CODE ##########

if debug:
    xx = "NC"
    chamber = "congress"

    print("Exercise fetch_score_array with all scores:")

    ensemble = "base0"
    for score in _score_mapping.keys():
        print(f"  Fetching score: {score} ...")
        result = fetch_score_array(state, chamber, ensemble, score)

    print("Exercise fetch_score_array for all ensembles")

    score = "Dem seats"
    for ensemble in _ensemble_mapping.keys():
        print(f"  Fetching score: {ensemble}")
        result = fetch_score_array(state, chamber, ensemble, score)

    print("Exercise fetch_score_array for D vote share")

    print("  Fetching D vote share (by_district)")
    score = "by_district"
    result = fetch_score_array(state, chamber, ensemble, score)

    print("Done.")

pass  # for debugging

### END ###
