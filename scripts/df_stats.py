#!/usr/bin/env python3

"""
SUMMARY STATS ON SCORES_DF.PARQUET
"""

from typing import List, Dict

import os
import pandas as pd
from collections import defaultdict

from rdapy import DISTRICTS_BY_STATE
from rdametrics import states, chambers, ensembles


scores_path: str = "~/local/beta-ensembles/dataframe/contents/scores_df.parquet"

df = pd.read_parquet(os.path.expanduser(scores_path))

N = 5
print(f"Columns {len(df.columns.tolist())}:")
for col in df.columns.tolist():
    print(f"  {col}")
print(f"\nFirst {N} rows:")
print(df.head(N))

pass

### END ###
