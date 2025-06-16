#!/usr/bin/env python3

"""
SUMMARY STATS ON SCORES.PARQUET
"""
import os
import pandas as pd


scores_path: str = "~/local/beta-ensembles/prepackaged/scores/scores.parquet"

df = pd.read_parquet(os.path.expanduser(scores_path))

N = 5
print(f"Columns {len(df.columns.tolist())}:")
for col in df.columns.tolist():
    print(f"  {col}")
print(f"\nFirst {N} rows:")
print(df.head(N))

pass

### END ###
