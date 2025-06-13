#!/usr/bin/env python3

"""
TODO - Make by-district info ...

"""

from typing import Dict, List, Set, Any

import argparse
from argparse import ArgumentParser, Namespace

import os
import zipfile
import fnmatch
import lzma
import tempfile
import pandas as pd
from pathlib import Path

from constants import *
from filenames import get_ensemble_name


def main() -> None:
    """Load all the by-district JSONL files into a single unified JSON file."""

    args = parse_arguments()

    i: int = 0
    all_aggregates: Dict[str, Any] = dict()

    for zip_type in [
        "xx_chamber",  # The 21 xx_chamber zips
        "reversible.long",  # The reversible.long zip
    ]:
        bydistrict_files: Set[str] = set()
        for xx in states:
            all_aggregates[xx] = dict()

            for chamber in chambers:
                all_aggregates[xx][chamber] = dict()

                for e_id in ensembles:
                    # The long run reversible is not in the xx_chamber zips
                    if zip_type == "xx_chamber" and e_id == "Rev":
                        continue
                    # The reversible.long zip only contains the long run reversible
                    if zip_type == "reversible.long" and e_id != "Rev":
                        continue

                    all_aggregates[xx][chamber][e_id] = dict()

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
                            ensemble_name: str = get_ensemble_name(xx, chamber, e_id)
                            assert (
                                ensemble_name not in bydistrict_files
                            ), f"Duplicate ensemble name {ensemble_name} found in {zip_path}"
                            bydistrict_files.add(ensemble_name)

                            # The file names w/in the zip files
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
                                len(zipped_files) == 6
                            ), f"Expected 5 bydistrict files, found {len(zipped_files)}"

                            for aggs_file in zipped_files:
                                print(f"      Loading {aggs_file} ...")

                                agg_filename: str
                                if e_id != "Rev":
                                    agg_filename = Path(aggs_file).stem
                                    xz_data = zf.read(aggs_file)
                                    agg_data = lzma.decompress(xz_data)
                                else:
                                    agg_filename = Path(aggs_file).name
                                    agg_data = zf.read(aggs_file)

                                # agg_path = Path(temp_dir) / agg_filename
                                # with open(agg_path, "wb") as agg_file:
                                #     agg_file.write(agg_data)

                                # df = pd.read_csv(agg_path)
                                # category_dfs.append(df)

                                i += 1

                            combined_df = category_dfs[0]
                            for df in category_dfs[1:]:
                                combined_df = combined_df.merge(
                                    df, on="map", how="outer"
                                )

                            combined_df["state"] = xx
                            combined_df["chamber"] = chamber
                            combined_df["ensemble"] = e_id

                            all_aggregates.append(combined_df)

    print(f"Concatenating all {i} scores files ...")  # s.b. 2,106
    unified_df = pd.concat(all_aggregates, ignore_index=True)

    print(f"Saving unified dataframe to {args.output} ...")
    unified_df.to_parquet(args.output)

    print(
        f"The integrated dataframe has {len(unified_df):,} rows and {len(unified_df.columns)} columns"
    )

    pass


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
