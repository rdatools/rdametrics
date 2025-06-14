#!/usr/bin/env python3

"""
TEST HARNESS TO LOAD THE AGGREGATES FOR A STATE, CHAMBER, ENSEMBLE, AND AGGREGATE-TYPE COMBINATION
"""

from typing import Dict, List, Set, Any

import argparse
from argparse import ArgumentParser, Namespace

import os
import json
import zipfile
import fnmatch
import lzma
import tempfile

from rdapy import smart_write, write_record

from constants import states, chambers, ensembles, aggregates
from filenames import get_ensemble_name

# ARGS

zip_dir: str = "~/local/beta-ensembles/zipped"
xx: str = "NC"
chamber: str = "congress"
ensemble: str = "A0"
category: str = "partisan"

aggregate: str = "dem_by_district"

# TODO - Move these into constants.py

datasets_by_aggregate_category: Dict[str, List[str]] = {
    "general": ["census"],
    "partisan": ["election"],
    "minority": ["vap", "cvap"],
    "compactness": ["shapes"],
    "splitting": ["census"],
}
aggregate_categories: List[str] = list(datasets_by_aggregate_category.keys())

# TODO - Helpers


def decode_bytes(bytes: bytes) -> List[Dict[str, Any]]:
    """Decode the bytes from a zipped by-district JSONL file."""

    json_objects = [
        json.loads(line) for line in bytes.decode("utf-8").strip().split("\n") if line
    ]

    return json_objects


def extract_aggregates(data, agg_type: str) -> Dict[str, Dict[str, Any]]:
    """Extract the by-district aggregates from the raw data. Ignore datasets & dataset types. Assume one dataset per type."""

    aggregates_by_name: Dict[str, Dict[str, Any]] = dict()

    for i, record in enumerate(data):
        assert "_tag_" in record, "Record does not contain '_tag_' key"

        if record["_tag_"] == "metadata":
            continue

        assert (
            record["_tag_"] == "by-district"
        ), f"Record does not contain '_tag_' key with value 'by-district': {record}"

        name: str = record["name"]

        collection: Dict[str, Any] = dict()
        for dataset in datasets_by_aggregate_category[agg_type]:
            # Skip over the dataset type and dataset name
            aggs_list: List[Dict[str, List[Any]]] = record["by-district"][
                dataset
            ].values()
            # Make the aggregates a single dictionary again
            aggs_dict: Dict[str, List[Any]] = {
                k: v for agg in aggs_list for k, v in agg.items()
            }
            collection.update(aggs_dict)

        aggregates_by_name[name] = collection

    return aggregates_by_name


def merge_aggregates(
    separate_aggs: List[Dict[str, Dict[str, Any]]],
) -> Dict[str, Dict[str, Any]]:
    """Merge the separate aggregates together."""

    all_names = set().union(*separate_aggs)
    merged = {name: {} for name in sorted(all_names)}

    for d in separate_aggs:
        for name, info in d.items():
            merged[name].update(info)

    return merged


# DEFINE A FUNCTION

assert xx in states, f"Invalid state: {xx}"
assert chamber in chambers, f"Invalid chamber: {chamber}"
assert ensemble in ensembles, f"Invalid ensemble: {ensemble}"
assert category in aggregate_categories, f"Invalid aggregates category: {category}"
# assert aggregate in aggregates, f"Invalid aggregate: {aggregate}"

for zip_type in [
    "xx_chamber",  # The 21 xx_chamber zips
    "reversible.long",  # The reversible.long zip
]:
    bydistrict_files: Set[str] = set()  # Ensure each is processed only once

    for xx in states:
        for chamber in chambers:
            for e_id in ensembles:

                # Toggle between the two types of zips

                if zip_type == "xx_chamber" and e_id == "Rev":
                    continue
                if zip_type == "reversible.long" and e_id != "Rev":
                    continue

                zip_path: str
                if zip_type == "xx_chamber":
                    zip_path = os.path.expanduser(f"{args.input}/{xx}_{chamber}.zip")
                else:
                    zip_path = os.path.expanduser(f"{args.input}/reversible.long.zip")

                # Read the bydistrict JSONL files from the zips.
                # To keep this nested xx / chamber / e__id looping simple,
                # it cracks the same {xx}_{chamber} zip file repeatedly.

                with tempfile.TemporaryDirectory() as temp_dir:
                    with zipfile.ZipFile(zip_path) as zf:

                        # Get the by-district file names

                        ensemble_name: str = get_ensemble_name(xx, chamber, e_id)
                        assert (
                            ensemble_name not in bydistrict_files
                        ), f"Duplicate ensemble name {ensemble_name} found in {zip_path}"
                        bydistrict_files.add(ensemble_name)

                        aggregates_pattern: str = "*_bydistrict.jsonl"
                        if e_id != "Rev":
                            aggregates_pattern = f"{xx}_{chamber}/{ensemble_name}/{xx}_{chamber}_{aggregates_pattern}.xz"
                        else:
                            aggregates_pattern = f"reversible.long/{xx}/{xx}_{chamber}/{ensemble_name}/{xx}_{chamber}_{aggregates_pattern}"

                        zipped_files: List[str] = zf.namelist()
                        zipped_files = [
                            f
                            for f in zipped_files
                            if fnmatch.fnmatch(f, aggregates_pattern)
                        ]
                        assert (
                            len(zipped_files) == 5
                        ), f"Expected 5 bydistrict files, found {len(zipped_files)}"

                        # Read each file & collect the aggregates

                        aggregate_pieces: List[Dict[str, Dict[str, Any]]] = list()
                        for j, aggs_file in enumerate(zipped_files):
                            agg_type = next(
                                (s for s in aggregate_categories if s in aggs_file),
                                None,
                            )
                            assert (
                                agg_type is not None
                            ), f"Unknown aggregate type in {aggs_file}"
                            print(f"  Loading {agg_type} aggregates: {aggs_file} ...")

                            if e_id != "Rev":
                                xz_data = zf.read(aggs_file)
                                agg_data = lzma.decompress(xz_data)
                            else:
                                agg_data = zf.read(aggs_file)

                            json_objects: List[Dict[str, Any]] = decode_bytes(agg_data)

                            agg_partial: Dict[str, Dict[str, Any]] = extract_aggregates(
                                json_objects, agg_type
                            )
                            aggregate_pieces.append(agg_partial)

                        # Merge the aggregates from all files

                        aggs_for_plans: Dict[str, Dict[str, Any]] = merge_aggregates(
                            aggregate_pieces
                        )

                        # Write the combined aggregates to the output stream

                        for name, aggs in aggs_for_plans.items():
                            record: Dict[str, Any] = {
                                "_tag_": "by-district",
                                "name": name,
                                "state": xx,
                                "chamber": chamber,
                                "ensemble": e_id,
                                "aggregates": aggs,
                            }
                            write_record(record, aggregates_stream)

pass

### END ###
