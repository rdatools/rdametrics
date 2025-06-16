#!/usr/bin/env python3

"""
TEST HARNESS TO EXERCISE LOADING & FETCHING AGGREGATES
"""

from typing import Dict, List, Any

import numpy as np
import fnmatch

from data import (
    states,
    chambers,
    ensembles,
    aggregates_by_category,
    load_aggregates,
    arr_from_aggregates,
)


##########

zip_dir: str = "~/local/beta-ensembles/zipped"

##########

print()
print("Exercising aggregates:")

for xx in states:
    for chamber in chambers:
        for ensemble in ensembles:
            for category, aggs in aggregates_by_category.items():
                loaded: List[List[Dict[str, Any]]] = [
                    load_aggregates(xx, chamber, ensemble, category, zip_dir)
                ]
                if category == "minority":
                    loaded.append(
                        load_aggregates(
                            xx,
                            chamber,
                            ensemble,
                            category,
                            zip_dir,
                            minority_dataset="cvap",
                        )
                    )

                for i, loaded_aggregates in enumerate(loaded):
                    todo: List[str] = list(aggs)

                    vap_flavor: str = ""
                    if category == "minority":
                        vap_flavor = " (VAP)" if i == 0 else " (CVAP)"
                        vap_pattern: str = "*_vap" if i == 0 else "*_cvap"
                        todo = [
                            name for name in aggs if fnmatch.fnmatch(name, vap_pattern)
                        ]

                    for agg in todo:
                        print(
                            f"  Fetching aggregate {agg}{vap_flavor} for {xx}, {chamber}, {ensemble} ..."
                        )
                        arr: np.ndarray = arr_from_aggregates(agg, loaded_aggregates)
                        assert (
                            arr.size > 0
                        ), f"Empty array for {xx}, {chamber}, {ensemble}, {agg}"

print("All aggregates loaded successfully.")

pass

### END ###
