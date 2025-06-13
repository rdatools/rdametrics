"""
CONSTANTS
"""

from typing import Dict, List

states: List[str] = ["FL", "IL", "MI", "NC", "NY", "OH", "WI"]

chambers: List[str] = ["congress", "upper", "lower"]

ensembles: List[str] = [
    "A0",
    "A1",
    "A2",
    "A3",
    "A4",
    "Pop-",
    "Pop+",
    "B",
    "C",
    "D",
    "Rev*",  # The original 50M sampled every 2.5K ensembles
    "Rev",  # The revised 1B sampled every 50K ensembles
    "R25",
    "R50",
    "R75",
    "R100",
]

metrics_by_category: Dict[str, List[str]] = {
    "general": [
        "population_deviation",
        "estimated_vote_pct",
    ],
    "proportionality": [
        "estimated_seats",
        "pr_deviation",
        "disproportionality",
        "fptp_seats",
        "efficiency_gap_wasted_votes",
        "efficiency_gap_statewide",
        "efficiency_gap",
        "seats_bias",
        "votes_bias",
        "geometric_seats_bias",
        "declination",
        "mean_median_statewide",
        "mean_median_average_district",
        "turnout_bias",
        "lopsided_outcomes",
        "proportionality",
    ],
    "competitiveness": [
        "competitive_district_count",
        "competitive_districts",
        "average_margin",
        "responsiveness",
        "responsive_districts",
        "overall_responsiveness",
        "competitiveness",
    ],
    "minority": [
        "mmd_black",
        "mmd_hispanic",
        "mmd_coalition",
        "opportunity_districts",
        "proportional_opportunities",
        "coalition_districts",
        "proportional_coalitions",
        "minority",
    ],
    "compactness": [
        "cut_score",
        "reock",
        "polsby_popper",
        "population_compactness",
        "compactness",
    ],
    "splitting": [
        "counties_split",
        "county_splits",
        "county_splitting",
        "district_splitting",
        "splitting",
    ],
}
metrics: List[str] = [
    metric for category in metrics_by_category.values() for metric in category
]

aggregates_by_dataset = {
    "census": ["pop_by_district", "district_splitting"],
    "vap": [
        "asian_vap",
        "black_vap",
        "hispanic_vap",
        "minority_vap",
        "native_vap",
        "pacific_vap",
        "total_vap",
        "white_vap",
    ],
    "cvap": [
        "asian_cvap",
        "black_cvap",
        "hispanic_cvap",
        "minority_cvap",
        "native_cvap",
        "pacific_cvap",
        "total_cvap",
        "white_cvap",
    ],
    "election": ["dem_by_district", "tot_by_district"],
    "shapes": ["area", "diameter", "perimeter", "polsby_popper", "reock"],
}
aggregates: List[str] = [
    agg for category in aggregates_by_dataset.values() for agg in category
]

### END ###
