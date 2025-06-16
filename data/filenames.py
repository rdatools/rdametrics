"""
ENSEMBLE ID TO FILE NAME MAPPING
"""

from typing import Dict

ensemble_id_to_long_name: Dict[str, str] = {
    "A0": "S0.0_R0_Vcut-edges-rmst",
    "A1": "S0.0_R1_Vcut-edges-rmst",
    "A2": "S0.0_R2_Vcut-edges-rmst",
    "A3": "S0.0_R3_Vcut-edges-rmst",
    "A4": "S0.0_R4_Vcut-edges-rmst",
    "Pop-": "S0.0_R0_Vcut-edges-rmst",
    "Pop+": "S0.0_R0_Vcut-edges-rmst",
    "B": "S0.0_R0_Vdistrict-pairs-rmst",
    "C": "S0.0_R0_Vcut-edges-ust",
    "D": "S0.0_R0_Vdistrict-pairs-ust",
    "Rev*": "S0.0_R0_Vreversible",  # The original 50M sampled every 2.5K ensembles
    "Rev": "S0.0_R0_Vreversible",  # The revised 1B sampled every 50K ensembles
    "R25": "S0.25_R0_Vcut-edges-region-aware",
    "R50": "S0.5_R0_Vcut-edges-region-aware",
    "R75": "S0.75_R0_Vcut-edges-region-aware",
    "R100": "S1.0_R0_Vcut-edges-region-aware",
}


def get_ensemble_name(xx: str, chamber: str, variant: str) -> str:
    """Construct an ensemble directory name."""

    T: str
    if chamber == "congress":
        if variant == "Pop-":
            T = "T0.005"
        elif variant == "Pop+":
            T = "T0.015"
        else:
            T = "T0.01"
    else:
        if variant == "Pop-":
            T = "T0.025"
        elif variant == "Pop+":
            T = "T0.075"
        else:
            T = "T0.05"

    ensemble_dir: str = f"{xx}_{chamber}_{T}_{ensemble_id_to_long_name[variant]}"

    return ensemble_dir


### ENDS ###
