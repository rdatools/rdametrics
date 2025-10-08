#!/usr/bin/env python3

"""
Take pairs of tables of:
- average measures of partisan bias by state/chamber, and
- the % of plans less than zero (unbiased) for the same cross products

and invert the second table to show % that favor the party favored in the first table.

"""

import json
from typing import List, Dict

import os
import pandas as pd

from rdapy import read_json

# from rdametrics import ensembles

# ensemble_filenames: List[str] = [
#     "base0",  # Cut edges, minimum spanning tree
#     "base1",
#     "base2",
#     "base3",
#     "base4",
#     "pop_minus",
#     "pop_plus",
#     "distpair",  # District pairs, minimum spanning tree
#     "ust",  # Cut edges, uniform spanning tree
#     "distpair_ust",  # District pairs, uniform spanning tree
#     "reversible_original",  # The original 50M sampled every 2.5K ensembles
#     "reversible",  # The revised 1B sampled every 50K ensembles
#     "county25",
#     "county50",
#     "county75",
#     "county100",
# ]

# ensemble_mapping: Dict[str, str] = dict(zip(ensembles, ensemble_filenames))


table_dir: str = (
    "~/Documents/work/Ensembles/partisan-bias-of-ensembles/tables/intermediate"
)

### Code for the ensemble variant tables ###

table_files = ("bias_tables.json", "zero_tables.json")

table1_path = os.path.expanduser(os.path.join(table_dir, table_files[0]))
table2_path = os.path.expanduser(os.path.join(table_dir, table_files[1]))

table1: dict = read_json(table1_path)
table2: dict = read_json(table2_path)

for variant, _data in table1.items():
    table2_prime: dict = dict()

    for combo, _measures in _data.items():
        table2_prime[combo] = dict()
        for m, value in _measures.items():
            z = (
                table2[variant][combo][m]
                if value < 0
                else 1 - table2[variant][combo][m]
            )
            table2_prime[combo][m] = z

    outfile = os.path.expanduser(
        os.path.join(table_dir, f"zero_table_{variant}-INVERTED.json")
    )
    print(json.dumps(table2_prime, indent=2), file=open(outfile, "w"))

pass  # for debugging

### Code for the all-together table ###

# table_files = ("bias_table_all-DERIVED.json", "zero_table_all-DERIVED.json")

# table1_path = os.path.expanduser(os.path.join(table_dir, table_files[0]))
# table2_path = os.path.expanduser(os.path.join(table_dir, table_files[1]))

# table1: dict = read_json(table1_path)
# table2: dict = read_json(table2_path)

# table2_prime: dict = dict()

# for combo, _measures in table1.items():
#     table2_prime[combo] = dict()
#     for m, value in _measures.items():
#         z = table2[combo][m] if value < 0 else 1 - table2[combo][m]
#         table2_prime[combo][m] = z

#     pass  # for debugging

# print(json.dumps(table2_prime, indent=2))

# pass  # for debugging

### END ###
