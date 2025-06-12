#!/usr/bin/env python3

"""
TODO - Make by-district info ...

"""

from typing import List, Dict, Set

import os
import zipfile
import fnmatch
import lzma
import tempfile
import pandas as pd
from pathlib import Path

######### Change these values #########

zipped_root: str = "~/local/beta-ensembles/zipped"
output_path: str = "~/local/beta-ensembles/dataframe/contents/scores_df.parquet"

#######################################

states: List[str] = ["FL", "IL", "MI", "NC", "NY", "OH", "WI"]
chambers: List[str] = ["congress", "upper", "lower"]
ensemble_id_to_long_name: Dict[str, str] = {
    "A0": "S0.0_R0_Vcut-edges-rmst",
    "A1": "S0.0_R1_Vcut-edges-rmst",
    "A2": "S0.0_R2_Vcut-edges-rmst",
    "A3": "S0.0_R3_Vcut-edges-rmst",
    "A4": "S0.0_R4_Vcut-edges-rmst",
    "Pop-": "S0.0_R0_Vcut-edges-rmst",
    "Pop+": "S0.0_R0_Vcut-edges-rmst",
    "B": "S0.0_R0_Vdistrict-pairs-rmst",
    "C": "S0.0_R0_Vcut-edges-ust",
    "D": "S0.0_R0_Vdistrict-pairs-ust",
    "Rev*": "S0.0_R0_Vreversible",  # The original 50M sampled every 2.5K ensembles
    "Rev": "S0.0_R0_Vreversible",  # The revised 1B sampled every 50K ensembles
    "R25": "S0.25_R0_Vcut-edges-region-aware",
    "R50": "S0.5_R0_Vcut-edges-region-aware",
    "R75": "S0.75_R0_Vcut-edges-region-aware",
    "R100": "S1.0_R0_Vcut-edges-region-aware",
}
ensembles: List[str] = list(ensemble_id_to_long_name.keys())
categories: List[str] = [
    "general",
    "partisan",
    "minority",
    "compactness",
    "splitting",
    "mmd",
]


def main() -> None:
    """Load all the CSV files into a single unified pandas dataframe."""

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
                            f"{zipped_root}/{xx}_{chamber}.zip"
                        )
                    else:
                        zip_path = os.path.expanduser(
                            f"{zipped_root}/reversible.long.zip"
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

    print(f"Saving unified dataframe to {output_path} ...")
    unified_df.to_parquet(output_path)

    print(
        f"The integrated dataframe has {len(unified_df):,} rows and {len(unified_df.columns)} columns"
    )

    pass


# Helpers to construct ensemble and scores file names, e.g.:
# NC_lower_T0.05_S0.0_R0_Vcut-edges-rmst_*_scores.csv.xz


def get_ensemble_name(xx: str, chamber: str, variant: str) -> str:
    """Construct an ensemble directory name."""

    T: str
    if chamber == "congress":
        if variant == "Pop-":
            T = "T0.005"
        elif variant == "Pop+":
            T = "T0.015"
        else:
            T = "T0.01"
    else:
        if variant == "Pop-":
            T = "T0.025"
        elif variant == "Pop+":
            T = "T0.075"
        else:
            T = "T0.05"

    ensemble_dir: str = f"{xx}_{chamber}_{T}_{ensemble_id_to_long_name[variant]}"

    return ensemble_dir


def get_scores_file_name(xx: str, chamber: str, variant: str, scores: str) -> str:
    """Construct a scores file name."""

    scores_file: str = get_ensemble_name(xx, chamber, variant) + f"_{scores}_scores.csv"

    return scores_file


"""
# This is the code I originally used to read the scores CSVs from the 22 zip files
# that were all unzipped in same place.

unzipped_root: str = "~/local/beta-ensembles/unzipped"

ensemble_path: str
if e_id != "Rev":
    ensemble_path = (
        os.path.expanduser(unzipped_root)
        + f"/{xx}_{chamber}/"
    )
else:
    ensemble_path = (
        os.path.expanduser(unzipped_root)
        + f"/reversible.long/{xx}"  # Extra level of hierarchy in the zip file
        + f"/{xx}_{chamber}/"
    )
ensemble_path += get_ensemble_name(xx, chamber, e_id)

for category in categories:
    scores_file: str = (
        ensemble_path
        + "/"
        + get_scores_file_name(xx, chamber, e_id, category)
    )
    print(f"      Loading {scores_file} ...")

    df = pd.read_csv(scores_file)
    df["state"] = xx
    df["chamber"] = chamber
    df["ensemble"] = e_id
    # df["category"] = category

    all_scores.append(df)

"""


if __name__ == "__main__":
    main()

### END ###
