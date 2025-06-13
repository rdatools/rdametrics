#!/usr/bin/env python3

"""
MAKE A SINGLE, INTEGRATED SCORES DATAFRAME

To use this script:

(1) Put all 22 zip files in a directory (7 x 3 = 21 xx_chamber zips + 1 reversible.long zip).

(2) Install dependencies:
    `pip install pandas`
    `pip install pyarrow`

(3) Run the script:
    ```
    data/make_scores_df.py \
    --input /path/to/zip/directory \
    --output /path/to/scores.parquet
    ```

(4) Zip the README.md together with the .parquet file and upload it to the download server.

"""

from typing import List, Set

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
    """Load all the scores CSV files into a single unified pandas dataframe."""

    args = parse_arguments()

    i: int = 0
    all_scores = []

    for zip_type in [
        "xx_chamber",  # The 21 xx_chamber zips
        "reversible.long",  # The reversible.long zip
    ]:
        scores_files: Set[str] = set()
        for xx in states:
            for chamber in chambers:
                for e_id in ensembles:
                    # The long run reversible is not in the xx_chamber zips
                    if zip_type == "xx_chamber" and e_id == "Rev":
                        continue
                    # The reversible.long zip only contains the long run reversible
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

                    # Read the scores CSVs from the zip files.
                    # To keep this nested xx / chamber / e__id looping simple,
                    # it cracks the same {xx}_{chamber} zip file repeatedly.

                    with tempfile.TemporaryDirectory() as temp_dir:
                        with zipfile.ZipFile(zip_path) as zf:
                            ensemble_name: str = get_ensemble_name(xx, chamber, e_id)
                            assert (
                                ensemble_name not in scores_files
                            ), f"Duplicate ensemble name {ensemble_name} found in {zip_path}"
                            scores_files.add(ensemble_name)

                            # The file names w/in the zip files
                            scores_pattern: str = "*_scores.csv"
                            if e_id != "Rev":
                                scores_pattern = f"{xx}_{chamber}/{ensemble_name}/{xx}_{chamber}_{scores_pattern}.xz"
                            else:
                                scores_pattern = f"reversible.long/{xx}/{xx}_{chamber}/{ensemble_name}/{xx}_{chamber}_{scores_pattern}"

                            zipped_files: List[str] = zf.namelist()
                            zipped_files = [
                                f
                                for f in zipped_files
                                if fnmatch.fnmatch(f, scores_pattern)
                            ]
                            assert (
                                len(zipped_files) == 6
                            ), f"Expected 6 scores files, found {len(zipped_files)}"

                            category_dfs = []
                            for scores_file in zipped_files:
                                print(f"      Loading {scores_file} ...")

                                csv_filename: str
                                if e_id != "Rev":
                                    csv_filename = Path(scores_file).stem
                                    xz_data = zf.read(scores_file)
                                    csv_data = lzma.decompress(xz_data)
                                else:
                                    csv_filename = Path(scores_file).name
                                    csv_data = zf.read(scores_file)

                                csv_path = Path(temp_dir) / csv_filename
                                with open(csv_path, "wb") as csv_file:
                                    csv_file.write(csv_data)

                                df = pd.read_csv(csv_path)
                                category_dfs.append(df)
                                i += 1

                            combined_df = category_dfs[0]
                            for df in category_dfs[1:]:
                                combined_df = combined_df.merge(
                                    df, on="map", how="outer"
                                )

                            combined_df["state"] = xx
                            combined_df["chamber"] = chamber
                            combined_df["ensemble"] = e_id

                            all_scores.append(combined_df)

    print(f"Concatenating all {i} scores files ...")  # s.b. 2,106
    unified_df = pd.concat(all_scores, ignore_index=True)

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
