#!/usr/bin/env python3

"""
MAKE A SINGLE, INTEGRATED AGGREGATES JSONL FILE

To use this script:

(1) Put all 22 zip files in a directory (7 x 3 = 21 xx_chamber zips + 1 reversible.long zip).

(2) Run the script:
    ```
    data/collect_aggregates.py \
    --input /path/to/zip/directory \
    --output /path/to/aggregates.jsonl
    ```

(3) Zip the README.md together with the .jsonl file and upload it to the download server.

NOTE - This isn't a good solution: The file size is way too big (~40GB) making the access time very slow.
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

from constants import *
from filenames import get_ensemble_name

import numpy as np
from rdapy import smart_read


def main() -> None:
    """Load all the by-district JSONL files into a single unified JSON file."""

    args = parse_arguments()

    i: int = 0
    with smart_write(args.output) as aggregates_stream:
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
                            zip_path = os.path.expanduser(
                                f"{args.input}/{xx}_{chamber}.zip"
                            )
                        else:
                            zip_path = os.path.expanduser(
                                f"{args.input}/reversible.long.zip"
                            )

                        # Read the bydistrict JSONL files from the zips.
                        # To keep this nested xx / chamber / e__id looping simple,
                        # it cracks the same {xx}_{chamber} zip file repeatedly.

                        with tempfile.TemporaryDirectory() as temp_dir:
                            with zipfile.ZipFile(zip_path) as zf:

                                # Get the by-district file names

                                ensemble_name: str = get_ensemble_name(
                                    xx, chamber, e_id
                                )
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

                                aggregate_pieces: List[Dict[str, Dict[str, Any]]] = (
                                    list()
                                )
                                for j, aggs_file in enumerate(zipped_files):
                                    agg_type = next(
                                        (s for s in agg_types if s in aggs_file),
                                        None,
                                    )
                                    assert (
                                        agg_type is not None
                                    ), f"Unknown aggregate type in {aggs_file}"
                                    print(
                                        f"  Loading {agg_type} aggregates: {aggs_file} ..."
                                    )

                                    if e_id != "Rev":
                                        xz_data = zf.read(aggs_file)
                                        agg_data = lzma.decompress(xz_data)
                                    else:
                                        agg_data = zf.read(aggs_file)

                                    json_objects: List[Dict[str, Any]] = decode_bytes(
                                        agg_data
                                    )

                                    agg_partial: Dict[str, Dict[str, Any]] = (
                                        extract_aggregates(json_objects, agg_type)
                                    )
                                    aggregate_pieces.append(agg_partial)

                                    i += 1

                                # Merge the aggregates from all files

                                aggs_for_plans: Dict[str, Dict[str, Any]] = (
                                    merge_aggregates(aggregate_pieces)
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

    print(f"Collected all {i} bydistrict files ...")  # s.b. 1,680


def arr_from_aggregates(
    xx: str,
    chamber: str,
    ensemble: str,
    aggregate: str,
    *,
    include_statewide: bool = False,
    file_path: str,
) -> np.ndarray:
    """
    Scan the aggregates file and return a by-district aggregrate for a given state, chamber, and ensemble.
    By default, the statewide values are not included.
    Returns a 2D numpy array where each row corresponds to a plan and each column corresponds to a district.
    """

    assert xx in states, f"Invalid state: {xx}"
    assert chamber in chambers, f"Invalid chamber: {chamber}"
    assert ensemble in ensembles, f"Invalid ensemble: {ensemble}"
    assert aggregate in aggregates, f"Invalid aggregate: {aggregate}"

    result: List = list()
    index: int = 0 if include_statewide else 1
    in_range: bool = False

    with smart_read(os.path.expanduser(file_path)) as input_stream:
        for i, line in enumerate(input_stream):
            r: Dict[str, Any] = json.loads(line)

            if (
                r["state"] == xx
                and r["chamber"] == chamber
                and r["ensemble"] == ensemble
            ):
                in_range = True
                agg: List[Any] = r["aggregates"][aggregate][index:]
                result.append(agg)
            elif in_range:
                # JSONL lines for states, chambers, and ensemble are in order by riding plan numbers,
                # so if records *stop* matching, stop processing.
                break

    return np.array(result)


### HELPERS ###

agg_types: List[str] = [
    "general",
    "partisan",
    "minority",
    "compactness",
    "splitting",
]
datasets_by_type: Dict[str, List[str]] = {
    "general": ["census"],
    "partisan": ["election"],
    "minority": ["vap", "cvap"],
    "compactness": ["shapes"],
    "splitting": ["census"],
}


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
        for dataset in datasets_by_type[agg_type]:
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


def parse_arguments():
    """Parse command line arguments."""

    parser: ArgumentParser = argparse.ArgumentParser(
        description="Parse command line arguments."
    )

    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="The directory containing the input .zip files",
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="The path to the output .parquet file",
    )

    parser.add_argument("--debug", dest="debug", action="store_true", help="Debug mode")
    parser.add_argument(
        "-v", "--verbose", dest="verbose", action="store_true", help="Verbose mode"
    )

    args: Namespace = parser.parse_args()

    return args


if __name__ == "__main__":
    main()

### END ###
