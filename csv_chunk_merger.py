#!/usr/bin/env python3
import argparse
import re
from pathlib import Path
from collections import defaultdict
import pandas as pd

CHUNK_PATTERN = re.compile(r"^(?P<base>.+?)(\((?P<idx>\d+)\))?\.csv$", re.IGNORECASE)

def parse_args():
    parser = argparse.ArgumentParser(
        description="Merge chunked CSV files split by row limits."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input directory containing chunked CSV files"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output directory for merged CSV files"
    )
    return parser.parse_args()

def collect_csv_chunks(input_dir: Path):
    groups = defaultdict(list)

    for file in input_dir.rglob("*.csv"):
        if not file.is_file():
            continue

        match = CHUNK_PATTERN.match(file.name)
        if not match:
            continue

        base_name = match.group("base").strip()
        idx = match.group("idx")
        order = int(idx) if idx is not None else 0

        groups[base_name].append((order, file))

    return groups

def merge_csv_group(files, output_path: Path):
    files = sorted(files, key=lambda x: x[0])
    dataframes = []

    for _, file in files:
        df = pd.read_csv(file)
        dataframes.append(df)

    merged_df = pd.concat(dataframes, ignore_index=True)
    merged_df.to_csv(output_path, index=False)

def main():
    args = parse_args()
    input_dir = Path(args.input)
    output_dir = Path(args.output)

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)

    groups = collect_csv_chunks(input_dir)

    if not groups:
        print("No chunked CSV files found.")
        return

    for base_name, files in groups.items():
        output_file = output_dir / f"{base_name}.csv"
        merge_csv_group(files, output_file)
        print(f"Merged {len(files)} files -> {output_file.name}")

if __name__ == "__main__":
    main()
