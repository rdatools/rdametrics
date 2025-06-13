"""
fetch_score_array() from Kris' analysis code
"""

import json
import pandas as pd
import numpy as np


def fetch_score_array(state, chamber, ensemble_type, score):
    """
    Fetches the score array for the given state, chamber and ensemble_type.
    If score is 'reock', 'polsby_popper', etc.,
    then it returns 1D array containing the scores of the maps in the ensemble.
    If score == 'by_district', then it returns a 2D array containing,
    for each map in the ensemble, an ordered array recording the dem_portions of the districts of the map.
    """

    if (
        score[:3] == "maj"
    ):  # preface a partisan score with 'maj ' to make it with respect to the majority party
        a = fetch_score_array(state, chamber, ensemble_type, score[4:])
        if state_to_dem_voteshare[state] > 0.5:
            return a
        else:
            return num_seats_dict[(state, chamber)] - a

    if score == "MMD coalition":  # the total should include all three MMD scores
        return (
            fetch_score_array(state, chamber, ensemble_type, "mmd_black")
            + fetch_score_array(state, chamber, ensemble_type, "mmd_hispanic")
            + fetch_score_array(state, chamber, ensemble_type, "mmd_coalition")
        )

    if chamber == "congress":
        pop0 = "0.01"
        pop_minus = "0.005"
        pop_plus = "0.015"
    else:
        pop0 = "0.05"
        pop_minus = "0.025"
        pop_plus = "0.075"

    type0 = "cut-edges-rmst"
    type1 = "cut-edges-region-aware"
    county0 = "0.0"

    ensemble_dict = {
        "base0": f"T{pop0}_S{county0}_R0_V{type0}",
        "base1": f"T{pop0}_S{county0}_R1_V{type0}",
        "base2": f"T{pop0}_S{county0}_R2_V{type0}",
        "base3": f"T{pop0}_S{county0}_R3_V{type0}",
        "base4": f"T{pop0}_S{county0}_R4_V{type0}",
        "pop_minus": f"T{pop_minus}_S{county0}_R0_V{type0}",
        "pop_plus": f"T{pop_plus}_S{county0}_R0_V{type0}",
        "ust": f"T{pop0}_S{county0}_R0_Vcut-edges-ust",
        "distpair": f"T{pop0}_S{county0}_R0_Vdistrict-pairs-rmst",
        "distpair_ust": f"T{pop0}_S{county0}_R0_Vdistrict-pairs-ust",
        "reversible": f"T{pop0}_S{county0}_R0_Vreversible",
        "county25": f"T{pop0}_S{0.25}_R0_V{type1}",
        "county50": f"T{pop0}_S{0.5}_R0_V{type1}",
        "county75": f"T{pop0}_S{0.75}_R0_V{type1}",
        "county100": f"T{pop0}_S{1.0}_R0_V{type1}",
    }
    snipet = ensemble_dict[ensemble_type]

    score_sheet, col_name = (
        score_to_spreadsheet_info[score]
        if score != "by_district"
        else ("partisan_bydistrict.jsonl", None)
    )
    filename = f"{local_folder}/{state}_{chamber}/{state}_{chamber}/{state}_{chamber}_{snipet}/{state}_{chamber}_{snipet}_{score_sheet}"

    if score == "by_district":
        with open(filename, "r", encoding="utf-8") as f:
            data = [json.loads(line) for line in f]
        to_return = []
        for i in range(1, len(data)):
            dem_counts = data[i]["by-district"]["election"]["C16GCO"]["dem_by_district"]
            tot_counts = data[i]["by-district"]["election"]["C16GCO"]["tot_by_district"]
            dem_portions = [
                dem_counts[i] / tot_counts[i] for i in range(1, len(dem_counts))
            ]
            dem_portions.sort()
            to_return.append(np.array(dem_portions))
        return np.array(to_return)
    else:  # a non-by-district score
        df = pd.read_csv(filename)
        return df[col_name].to_numpy()


### END ###
