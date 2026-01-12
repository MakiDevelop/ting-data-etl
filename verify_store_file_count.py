#!/usr/bin/env python3
import argparse
import os
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Verify file count under each store directory."
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Output directory containing storeId subdirectories"
    )
    parser.add_argument(
        "--limit-file-count",
        type=int,
        required=True,
        help="Maximum allowed file count under each store directory"
    )
    return parser.parse_args()


def verify_store_dirs(input_dir: Path, limit: int) -> int:
    error_count = 0

    for item in sorted(input_dir.rglob("*")):
        if not item.is_dir():
            continue

        files = [f for f in item.iterdir() if f.is_file()]
        if not files:
            continue

        file_count = len(files)

        if file_count > limit:
            print(f"{item} exceeded limit")
            error_count += 1

    return error_count


def main():
    args = parse_args()
    input_dir = Path(args.input_dir)

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    error_count = verify_store_dirs(input_dir, args.limit_file_count)

    if error_count > 0:
        print(f"\nVerification failed: {error_count} store(s) exceeded limit.")
        exit(1)

    print("Verification passed: all store directories are within file count limit.")


if __name__ == "__main__":
    main()
