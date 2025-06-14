"""
HELPERS FOR WORKING WITH SCORES AND BY-DISTRICT AGGREGATES

These functions make it easier to work with the all-up scores 'pandas' dataframe
(scores.parquet) and all the by-district aggregates contained in the state/chamber
zip files. To use them, import this file into your Python. Put the
companion file, constants.py, along side it.

To work with the scores dataframe, load 'panda' dataframe from disk:

    scores_df = load_scores("/path/to/scores.parquet")

You only need to do this once per session, i.e., it loads *all* the
plan-level scores (metrics) into a single 'pandas' dataframe.

You can use this as with vanilla dataframe operations. Alternatively,
you can filter the dataframe to the subset for a state, chamber, and
ensemble:

    xx = "NC"
    chamber = "congress"
    ensemble = "A0"
    subset_df = df_from_scores(xx, chamber, ensemble, scores_df)

You can also fetch an individual metric for for a state, chamber, and
ensemble:

    metric = "estimated_seats"
    arr = arr_from_scores(xx, chamber, ensemble, metric, scores_df)

This returns a 1D 'numpy' array.

The companion file constants.py defines many helpful constants, e.g.,
if you want to iterate over states, chambers, ensembles, metrics, and
aggregates, etc.

To work with by-district aggregates, first load the desired aggregates
from disk:

    xx = "NC"
    chamber = "congress"
    ensemble = "A0"
    category = "partisan"
    zip_dir = "/path/to/dir-with-zip-files"
    aggregates_subset = load_aggregates(xx, chamber, ensemble, category)

You need to do this for each different combination of those parameters, where
the aggregate categories (defined in constants.py) are 'general', 'partisan',
'minority', 'compactness', and 'splitting'.

By default, this will load the 'vap' aggregates for the 'minority' category.
If you want the 'cvap' aggregates instead, modify the call:

    category = "minority"
    aggregates_subset = load_aggregates(xx, chamber, ensemble, category, minority_dataset="cvap")

You can, of course, work with this directly in Python. It is a list of dictionaries,
where each item in the list corresponds to a plan, and each dictionary contains the
by-district aggregates for that plan. The keys are the names of the aggregates, and
the values are lists of values for the districts in the plan.

Note: The first value in each list is a statewide aggregate. The other 1-N values
correspond to the districts in the plan.

Alternatively, you can extract an individual aggregate from the loaded subset:

    aggregate = "dem_by_district"
    arr = arr_from_aggregates(aggregate, aggregates_subset)

This returns a 2D 'numpy' array where each row corresponds to a plan, and the "column"
contains the list of values for the districts in the plan. By default, this excludes
the statewide aggregate. If you want those included, you can set the `include_statewide`
parameter to `True`:

You can also fetch multiple aggregates from the same category in succession:

    arrays = {}
    for aggregate in ["dem_by_district", "tot_by_district"]:
        arr = arr_from_aggregates(aggregate, aggregates_subset)
        arrays[aggregate] = arr

"""

from typing import List, Dict, Any

import os, json
import numpy as np
import pandas as pd
import zipfile
import fnmatch
import lzma
import tempfile

from rdapy import smart_read
from constants import (
    states,
    chambers,
    ensembles,
    metrics,
    aggregates,
    aggregate_categories,
    datasets_by_aggregate_category,
)
from filenames import get_ensemble_name

### SCORES ###


def load_scores(scores_path: str) -> pd.DataFrame:
    """Read the scores .parquet file into a DataFrame."""

    df: pd.DataFrame = pd.read_parquet(os.path.expanduser(scores_path))

    return df


def df_from_scores(
    xx: str, chamber: str, ensemble: str, scores: pd.DataFrame
) -> pd.DataFrame:
    """Subset the scores DataFrame for a state, chamber, and ensemble combination."""

    assert xx in states, f"Invalid state: {xx}"
    assert chamber in chambers, f"Invalid chamber: {chamber}"
    assert ensemble in ensembles, f"Invalid ensemble: {ensemble}"

    subset_df: pd.DataFrame = scores[
        (scores["state"] == xx)
        & (scores["chamber"] == chamber)
        & (scores["ensemble"] == ensemble)
    ]

    return subset_df


def arr_from_scores(
    xx: str, chamber: str, ensemble: str, metric: str, scores: pd.DataFrame
) -> np.ndarray:
    """Extract a metric for a state, chamber, and ensemble combination from the scores DataFrame into a numpy array."""

    assert xx in states, f"Invalid state: {xx}"
    assert chamber in chambers, f"Invalid chamber: {chamber}"
    assert ensemble in ensembles, f"Invalid ensemble: {ensemble}"
    assert metric in metrics, f"Invalid metric: {metric}"

    arr: np.ndarray = scores[
        (scores["state"] == xx)
        & (scores["chamber"] == chamber)
        & (scores["ensemble"] == ensemble)
    ][metric].to_numpy()

    return arr


### BY-DISTRICT AGGREGATES ###


def load_aggregates(
    xx: str,
    chamber: str,
    ensemble: str,
    category: str,
    zip_dir: str,
    *,
    minority_dataset: str = "vap",
) -> List[Dict[str, Any]]:

    assert xx in states, f"Invalid state: {xx}"
    assert chamber in chambers, f"Invalid chamber: {chamber}"
    assert ensemble in ensembles, f"Invalid ensemble: {ensemble}"
    assert category in aggregate_categories, f"Invalid aggregates category: {category}"

    zip_path: str = (
        f"{zip_dir}/{xx}_{chamber}.zip"
        if ensemble != "Rev"
        else f"{zip_dir}/reversible.long.zip"
    )
    zip_path = os.path.expanduser(zip_path)

    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(zip_path) as zf:

            ensemble_name: str = get_ensemble_name(xx, chamber, ensemble)

            aggregates_pattern: str = f"*_{category}_bydistrict.jsonl"
            if ensemble != "Rev":
                aggregates_pattern = f"{xx}_{chamber}/{ensemble_name}/{xx}_{chamber}_{aggregates_pattern}.xz"
            else:
                aggregates_pattern = f"reversible.long/{xx}/{xx}_{chamber}/{ensemble_name}/{xx}_{chamber}_{aggregates_pattern}"

            zipped_files: List[str] = zf.namelist()
            zipped_files = [
                f for f in zipped_files if fnmatch.fnmatch(f, aggregates_pattern)
            ]
            assert (
                len(zipped_files) == 1
            ), f"Expected 1 {category} bydistrict file, found {len(zipped_files)}"
            aggs_file: str = zipped_files[0]

            if ensemble != "Rev":
                xz_data = zf.read(aggs_file)
                agg_data = lzma.decompress(xz_data)
            else:
                agg_data = zf.read(aggs_file)

            json_objects: List[Dict[str, Any]] = _decode_bytes(agg_data)

            aggregate_data: List[Dict[str, Any]] = _extract_aggregates(
                json_objects, category, minority_dataset
            )

    return aggregate_data


def arr_from_aggregates(
    aggregate: str,
    loaded_aggregates: List[Dict[str, Any]],
    *,
    include_statewide: bool = False,
) -> np.ndarray:
    """
    Extract an aggregrate the loaded aggregates for a state, chamber, ensemble, and aggregate category.
    Returns a 2D numpy array where each row corresponds to a plan and each column corresponds to a district.
    """

    assert aggregate in aggregates, f"Invalid aggregate: {aggregate}"
    assert (
        aggregate in loaded_aggregates[0]
    ), f"Aggregate {aggregate} not found in loaded aggregates"

    index: int = 0 if include_statewide else 1
    result: List = [r[aggregate][index:] for r in loaded_aggregates]

    return np.array(result)


### HELPERS ###


def _decode_bytes(bytes: bytes) -> List[Dict[str, Any]]:
    """Decode the bytes from a zipped by-district JSONL file."""

    json_objects = [
        json.loads(line) for line in bytes.decode("utf-8").strip().split("\n") if line
    ]

    return json_objects


def _extract_aggregates(
    data, category: str, minority_dataset: str = "vap"
) -> List[Dict[str, Any]]:
    """Extract the by-district aggregates from the raw data. Ignore datasets & dataset types. Assume one dataset per type."""

    aggregates: List[Dict[str, Any]] = list()

    for record in data:
        assert "_tag_" in record, "Record does not contain '_tag_' key"

        if record["_tag_"] == "metadata":
            continue

        assert (
            record["_tag_"] == "by-district"
        ), f"Record does not contain '_tag_' key with value 'by-district': {record}"

        collected_aggregates: Dict[str, Any] = dict()
        collected_aggregates["name"] = record["name"]

        dataset: str = (
            datasets_by_aggregate_category[category][0]
            if category != "minority"
            else minority_dataset  # VAP or CVAP
        )

        # Skip over the dataset type and dataset name
        aggs_list: List[Dict[str, List[Any]]] = record["by-district"][dataset].values()
        # Make the aggregates a single dictionary again
        aggs_dict: Dict[str, List[Any]] = {
            k: v for agg in aggs_list for k, v in agg.items()
        }
        collected_aggregates.update(aggs_dict)

        aggregates.append(collected_aggregates)

    return aggregates


### END ###
