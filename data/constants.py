"""
CONSTANTS FOR WORKING WITH SCORES AND BY-DISTRICT AGGREGATES
"""

from typing import Dict, List

# The states in the study
states: List[str] = ["FL", "IL", "MI", "NC", "NY", "OH", "WI"]

# The chambers in the study
chambers: List[str] = ["congress", "upper", "lower"]

# The identifiers used in the paper for the ensembles in the study
ensembles: List[str] = [
    "A0",  # Cut edges, minimum spanning tree
    "A1",
    "A2",
    "A3",
    "A4",
    "Pop-",
    "Pop+",
    "B",  # District pairs, minimum spanning tree
    "C",  # Cut edges, uniform spanning tree
    "D",  # District pairs, uniform spanning tree
    "Rev*",  # The original 50M sampled every 2.5K ensembles
    "Rev",  # The revised 1B sampled every 50K ensembles
    "R25",
    "R50",
    "R75",
    "R100",
]

### METRICS ###

# The plan-level metrics computed for each ensemble, by category and individually.
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
metric_categories: List[str] = list(metrics_by_category.keys())
metrics: List[str] = [
    metric for category in metrics_by_category.values() for metric in category
]

### AGGREGATES ###

# The by-district aggregates computed for each ensemble, by category and individually.
aggregates_by_category = {
    "general": ["pop_by_district"],
    "partisan": ["dem_by_district", "tot_by_district"],
    "minority": [
        "asian_vap",
        "black_vap",
        "hispanic_vap",
        "minority_vap",
        "native_vap",
        "pacific_vap",
        "total_vap",
        "white_vap",
        "asian_cvap",
        "black_cvap",
        "hispanic_cvap",
        "minority_cvap",
        "native_cvap",
        "pacific_cvap",
        "total_cvap",
        "white_cvap",
    ],
    "compactness": ["area", "diameter", "perimeter", "polsby_popper", "reock"],
    "splitting": ["district_splitting"],
}
aggregate_categories: List[str] = list(aggregates_by_category.keys())
aggregates: List[str] = [
    agg for category in aggregates_by_category.values() for agg in category
]

datasets_by_aggregate_category: Dict[str, List[str]] = {
    "general": ["census"],
    "partisan": ["election"],
    "minority": ["vap", "cvap"],
    "compactness": ["shapes"],
    "splitting": ["census"],
}

### END ###
