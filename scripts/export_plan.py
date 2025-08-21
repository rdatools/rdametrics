#!/usr/bin/env python3

"""
EXTRACT A PLAN FROM AN ENSEMBLE TO ASSIGNMENT FILE CSV
"""

import argparse
from argparse import ArgumentParser, Namespace

from typing import Any, List, Dict, TextIO, Tuple, Generator

import os, json, zipfile, lzma, tempfile, subprocess
from pathlib import Path

from rdapy import smart_read, read_record

from data.constants import states, chambers, ensembles
from data.filenames import get_ensemble_name
from data.helpers import _decode_bytes


def main() -> None:
    """Extract a plan from an ensemble"""

    args: argparse.Namespace = parse_args()

    assert args.xx in states, f"Invalid state: {args.xx}"
    assert args.chamber in chambers, f"Invalid chamber: {args.chamber}"
    assert args.ensemble in ensembles, f"Invalid ensemble: {args.ensemble}"

    # Load the ensemble from the xz file w/in the zip file

    zip_path: str = f"{args.input_dir}/{args.xx}_{args.chamber}.zip"
    zip_path = os.path.expanduser(zip_path)

    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(zip_path) as zf:
            ensemble_name: str = get_ensemble_name(args.xx, args.chamber, args.ensemble)
            ensemble_path: str = (
                f"{args.xx}_{args.chamber}/{ensemble_name}/{ensemble_name}_ensemble.jsonl.xz"
            )

            if args.ensemble != "Rev":
                xz_data = zf.read(ensemble_path)
                ensemble_data = lzma.decompress(xz_data)
            else:
                ensemble_data = zf.read(ensemble_path)

            json_objects: List[Dict[str, Any]] = _decode_bytes(ensemble_data)

    # Serialize the JSON objects to a temp file

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False
    ) as input_file:
        for obj in json_objects:
            json.dump(obj, input_file)
            input_file.write("\n")
        input_file_path = input_file.name

    # Decompress the ensemble using distill

    script_dir = Path(__file__).parent
    distill_path = script_dir / "distill"

    output_file = tempfile.NamedTemporaryFile(delete=False, suffix=".out")
    output_file_path = output_file.name
    output_file.close()

    try:
        result = subprocess.run(
            [
                str(distill_path),
                "--from",
                "compress",
                "--to",
                "assignment",
                "--output",
                output_file_path,
                input_file_path,
            ],
            check=True,
            capture_output=True,
            text=True,
        )

        Path(input_file_path).unlink()
        output_path = Path(output_file_path)

    except subprocess.CalledProcessError as e:
        Path(input_file_path).unlink(missing_ok=True)
        Path(output_file_path).unlink(missing_ok=True)
        raise RuntimeError(f"distill command failed: {e.stderr}") from e

    # Scan the plans in the ensemble, find the specified plan, and export it as a CSV

    try:
        plan_found: bool = False
        with open(output_path, "r") as ensemble_stream:
            for name, plan in ensemble_plans(ensemble_stream):
                if name == args.plan:
                    plan_found = True
                    with open(args.output, "w") as output_file:
                        print("GEOID20,District", file=output_file)
                        for geoid, district in plan.items():
                            print(f"{geoid},{district}", file=output_file)
                    print(f"Plan {name} written to {args.output}")

    finally:
        output_path.unlink(missing_ok=True)
        if not plan_found:
            print(
                f"Plan {args.plan} not found in {args.xx}/{args.chamber}/{args.ensemble}."
            )

    pass


def ensemble_plans(
    ensemble_stream: TextIO,
) -> Generator[Tuple[str, Dict[str, int]], None, None]:
    """Return plans (assignments) one at a time from an ensemble"""

    for i, line in enumerate(ensemble_stream):
        try:
            # Skip the metadata and ReCom graph records
            in_record: Dict[str, Any] = read_record(line)
            if "_tag_" not in in_record:
                continue
            if in_record["_tag_"] == "metadata":
                continue

            # Plan records

            assert in_record["_tag_"] == "plan"

            name: str = (
                f"{int(in_record['name']):09d}"
                if in_record["name"].isdigit()
                else in_record["name"]
            )

            plan: Dict[str, int] = in_record["plan"]
            yield (name, plan)

        except Exception as e:
            raise Exception(f"Reading ensemble plan {i}: {e}")


def parse_args():
    parser: ArgumentParser = argparse.ArgumentParser(
        description="Parse command line arguments"
    )

    parser.add_argument(
        "--input-dir",
        type=str,
        default="~/local/beta-ensembles/zipped",
        help="The input directory containing the zipped ensembles",
    )

    parser.add_argument(
        "--state",
        type=str,
        dest="xx",
        default="NC",
        help="The state for the ensemble",
    )
    parser.add_argument(
        "--plan-type",
        type=str,
        dest="chamber",
        default="congress",
        help="The plan type of the ensemble",
    )
    parser.add_argument(
        "--ensemble",
        type=str,
        default="A0",
        help="The variant id of the ensemble",
    )
    parser.add_argument(
        "--plan",
        type=str,
        default="7460000",
        help="The plan name",
    )

    parser.add_argument(
        "--output",
        type=str,
        default="temp/TEST.csv",
        help="The output precinct-assignment CSV file",
    )

    parser.add_argument(
        "-v", "--verbose", dest="verbose", action="store_true", help="Verbose mode"
    )

    args: Namespace = parser.parse_args()

    return args


if __name__ == "__main__":
    main()

### END ###
