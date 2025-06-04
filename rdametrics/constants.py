"""
CONSTANTS
"""

from typing import List, Dict

states: List[str] = ["FL", "IL", "MI", "NC", "NY", "OH", "WI"]
chambers: List[str] = ["congress", "upper", "lower"]
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
ensembles: List[str] = list(ensemble_id_to_long_name.keys())
categories: List[str] = [
    "general",
    "partisan",
    "minority",
    "compactness",
    "splitting",
    "mmd",
]

### END ###
