#!/usr/bin/env python3

"""
TEST HARNESS TO LOAD THE AGGREGATES FOR A STATE, CHAMBER, ENSEMBLE, AND AGGREGATE-TYPE COMBINATION
"""

from typing import Dict, List, Set, Any

from helpers import load_aggregates

zip_dir: str = "~/local/beta-ensembles/zipped"
xx: str = "NC"
chamber: str = "congress"
ensemble: str = "A0"
category: str = "partisan"

aggs: List[Dict[str, Any]] = load_aggregates(xx, chamber, ensemble, category, zip_dir)

pass

### END ###
