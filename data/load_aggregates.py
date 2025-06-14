#!/usr/bin/env python3

"""
TEST HARNESS TO LOAD THE AGGREGATES FOR A STATE, CHAMBER, ENSEMBLE, AND AGGREGATE-TYPE COMBINATION
"""

from typing import Dict, List, Any

import numpy as np

from constants import aggregates
from helpers import load_aggregates, arr_from_aggregates

zip_dir: str = "~/local/beta-ensembles/zipped"
xx: str = "NC"
chamber: str = "congress"
ensemble: str = "A0"
category: str = "partisan"

loaded_aggregates: List[Dict[str, Any]] = load_aggregates(
    xx, chamber, ensemble, category, zip_dir
)

arr: np.ndarray = arr_from_aggregates("dem_by_district", loaded_aggregates)


pass

### END ###
