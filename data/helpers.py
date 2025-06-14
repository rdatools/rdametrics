"""
HELPERS FOR WORKING WITH SCORES AND BY-DISTRICT AGGREGATES
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
    aggregate_categories,
    datasets_by_aggregate_category,
)
from filenames import get_ensemble_name

### SCORES ###


def load_scores(scores_path: str) -> pd.DataFrame:
    """Read the scores .parquet file into a DataFrame."""

    df: pd.DataFrame = pd.read_parquet(os.path.expanduser(scores_path))

    return df


def arr_from_scores(
    xx: str, chamber: str, ensemble: str, metric: str, scores_df: pd.DataFrame
) -> np.ndarray:
    """Extract a metric for a state, chamber, and ensemble combination from the scores DataFrame into a numpy array."""

    arr: np.ndarray = scores_df[
        (scores_df["state"] == xx)
        & (scores_df["chamber"] == chamber)
        & (scores_df["ensemble"] == ensemble)
    ][metric].to_numpy()

    return arr


def df_from_scores(
    xx: str, chamber: str, ensemble: str, scores_df: pd.DataFrame
) -> pd.DataFrame:
    """Subset the scores DataFrame for a state, chamber, and ensemble combination."""

    subset_df: pd.DataFrame = scores_df[
        (scores_df["state"] == xx)
        & (scores_df["chamber"] == chamber)
        & (scores_df["ensemble"] == ensemble)
    ]

    return subset_df


### BY-DISTRICT AGGREGATES ###


def load_aggregates(
    xx: str,
    chamber: str,
    ensemble: str,
    category: str,
    zip_dir: str,
    *,
    minority_dataset: str = "VAP",
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

            json_objects: List[Dict[str, Any]] = decode_bytes(agg_data)

            aggregate_data: List[Dict[str, Any]] = extract_aggregates(
                json_objects, category
            )

    return aggregate_data


# TODO - Add arr_from_aggregates function to extract a specific aggregate from the by-district aggregates data.

### HELPERS ###


def decode_bytes(bytes: bytes) -> List[Dict[str, Any]]:
    """Decode the bytes from a zipped by-district JSONL file."""

    json_objects = [
        json.loads(line) for line in bytes.decode("utf-8").strip().split("\n") if line
    ]

    return json_objects


def extract_aggregates(
    data, category: str, minority_dataset: str = "VAP"
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
