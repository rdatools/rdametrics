# Using Scores and By-District Aggregates

There are 2,016 scores CSV files in our study containing 48 scores for 6.7 million plans!

- 7 states
- 3 chambers
- 16 ensembles  
- 6 categories of scores 
- 48 scores per plan
- 20,000 plans per ensemble

Each scores CSV is easy to import into a spreadsheet, but if you want to work with the data, this surface area is daunting.
The by-district aggregates have a similar surface area.

So we created a single, integrated `pandas` dataframe that contains all of the scores: `scores.parquet` and
a set of helpers to make working with it and the by-district aggregates easier.

## Using `scores.parquet` Natively

The information below describes how to load the dataframe from disk and how filter for specific scores.

### Installing Dependencies

To use `scores.parquet`, you need to have `pandas` and `pyarrow` installed. You can install them using pip:

```bash
pip install pandas
pip install pyarrow
```

### Loading the Dataframe

Then the Python is simple:

```python
import os
import pandas as pd

all_scores = pd.read_parquet(os.path.expanduser("/path/to/scores.parquet"))
```

In this example, `all_scores` is a `pandas` dataframe.
See "Helpers" below for more information.

### Index Columns

Each set of scores is indexed with three columns:

- `state` -- the state name
- `chamber` -- the chamber name
- `ensemble` -- the ensemble id

You can use them to filter the dataframe.
For a specific state, chamber, and ensemble combination, all 6 categories of scores 
&mdash;general, partisan, minority, compactness, splitting, and majority-minority (MMD)&mdash;
are together.

There are 7 states : `FL`, `IL`, `MI`, `NC`, `NY`, `OH`, and `WI`.
There are 3 chambers: `congress` and `upper` and `lower` states houses.
There are 16 ensembles, the 15 ensembles reported in the paper plus a second reversible ensemble:
`A0`, `A1`, `A2`, `A3`, `A4`, `Pop-`, `Pop+`, `B`, `C`, `D`, `Rev*`, `Rev`, `R25`, `R50`, `R75`, and `R100`.
The `Rev` ensemble corresponds to what is reported in the paper.
It has a chain length of 1 billion and a subsampling rate of every 50,000th plan.
The `Rev*` is the original reversible ensemble that we produced using the same chain length (50 million) 
and subsampling rate (every 2,500) as the other non-reversible ensembles.

## Using the Helpers to Work with Scores and By-District Aggregates

The Python code the `data/` folder makes it easier to work with `scores.parquet` and 
all the by-district aggregates contained in the state/chamber zip files. 
To use it, put the contents&mdash;including the `__init__.py` file&mdash;in the root of your project. 
Make sure you've installed the dependencies as described above.

Then, to work with the scores dataframe, load 'panda' dataframe from disk:

```python
scores_df = load_scores("/path/to/scores.parquet")
```

You only need to do this once per session, i.e., it loads *all* the
plan-level scores (metrics) into a single 'pandas' dataframe.

You can use this as with vanilla dataframe operations. Alternatively,
you can filter the dataframe to the subset for a state, chamber, and
ensemble:

```python
xx = "NC"
chamber = "congress"
ensemble = "A0"
subset_df = df_from_scores(xx, chamber, ensemble, scores_df)
```

You can also fetch an individual metric for for a state, chamber, and
ensemble:

```python
metric = "estimated_seats"
arr = arr_from_scores(xx, chamber, ensemble, metric, scores_df)
```

This returns a 1D 'numpy' array.

The companion file constants.py defines many helpful constants, e.g.,
if you want to iterate over states, chambers, ensembles, metrics, and
aggregates, etc.

To work with by-district aggregates, first load the desired aggregates
from disk:

```python
xx = "NC"
chamber = "congress"
ensemble = "A0"
category = "partisan"
zip_dir = "/path/to/dir-with-zip-files"
aggregates_subset = load_aggregates(xx, chamber, ensemble, category, zip_dir)
```

You need to do this for each different combination of those parameters, where
the aggregate categories (defined in constants.py) are 'general', 'partisan',
'minority', 'compactness', and 'splitting'.

By default, this will load the 'vap' aggregates for the 'minority' category.
If you want the 'cvap' aggregates instead, modify the call:

```python
category = "minority"
aggregates_subset = load_aggregates(xx, chamber, ensemble, category, zip_dir, minority_dataset="cvap")
```

You can, of course, work with this directly in Python. It is a list of dictionaries,
where each item in the list corresponds to a plan, and each dictionary contains the
by-district aggregates for that plan. The keys are the names of the aggregates, and
the values are lists of values for the districts in the plan.

Note: The first value in each list is a statewide aggregate. The other 1-N values
correspond to the districts in the plan.

Alternatively, you can extract an individual aggregate from the loaded subset:

```python
aggregate = "dem_by_district"
arr = arr_from_aggregates(aggregate, aggregates_subset)
```

This returns a 2D 'numpy' array where each row corresponds to a plan, and the "column"
contains the list of values for the districts in the plan. By default, this excludes
the statewide aggregate. If you want those included, you can set the `include_statewide`
parameter to `True`:

You can also fetch multiple aggregates from the same category in succession:

```python
arrays = {}
for aggregate in ["dem_by_district", "tot_by_district"]:
    arr = arr_from_aggregates(aggregate, aggregates_subset)
    arrays[aggregate] = arr
```