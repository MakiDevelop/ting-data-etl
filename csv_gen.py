#!/usr/bin/env python3
import argparse
import csv
import os
import random
from datetime import date, timedelta

DATA_TYPES = ["string", "int", "float", "date", "bool"]


def build_store_ids(count: int):
    width = max(3, len(str(count)))
    return [f"store_{i:0{width}d}" for i in range(1, count + 1)]


def random_column_name(rng: random.Random, used: set):
    while True:
        name = f"col_{rng.randint(1000, 9999)}"
        if name not in used:
            used.add(name)
            return name


def random_date(rng: random.Random):
    start = date(2018, 1, 1)
    end = date(2024, 12, 31)
    delta_days = (end - start).days
    return (start + timedelta(days=rng.randint(0, delta_days))).isoformat()


def random_value(rng: random.Random, dtype: str):
    if dtype == "string":
        return f"val_{rng.randint(10000, 99999)}"
    if dtype == "int":
        return rng.randint(-100000, 100000)
    if dtype == "float":
        return f"{rng.uniform(-10000, 10000):.4f}"
    if dtype == "date":
        return random_date(rng)
    if dtype == "bool":
        return rng.choice(["true", "false"])
    return ""


def generate_schema(rng: random.Random, min_cols: int, max_cols: int):
    # storeId + random other columns
    col_count = rng.randint(min_cols, max_cols)
    used = {"storeId"}
    columns = ["storeId"]
    types = ["string"]  # storeId is always string in this generator

    for _ in range(col_count - 1):
        columns.append(random_column_name(rng, used))
        types.append(rng.choice(DATA_TYPES))

    return columns, types


def write_csv(path, rows, store_ids, rng, min_cols, max_cols):
    columns, types = generate_schema(rng, min_cols, max_cols)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)

        for _ in range(rows):
            store_id = rng.choice(store_ids)
            row = [store_id]
            for dtype in types[1:]:
                row.append(random_value(rng, dtype))
            writer.writerow(row)


def parse_args():
    parser = argparse.ArgumentParser(
        description="CSV fake data generator for inconsistent schemas."
    )
    parser.add_argument("--csv-count", type=int, default=60)
    parser.add_argument("--store-count", type=int, default=150)
    parser.add_argument("--min-rows", type=int, default=1000)
    parser.add_argument("--max-rows", type=int, default=10000)
    parser.add_argument("--output-dir", type=str, default="./generated_data")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--min-cols", type=int, default=3)
    parser.add_argument("--max-cols", type=int, default=10)
    return parser.parse_args()


def main():
    args = parse_args()
    if args.min_rows > args.max_rows:
        raise ValueError("min-rows must be <= max-rows")
    if args.min_cols < 2:
        raise ValueError("min-cols must be >= 2 (including storeId)")
    if args.min_cols > args.max_cols:
        raise ValueError("min-cols must be <= max-cols")

    os.makedirs(args.output_dir, exist_ok=True)
    rng = random.Random(args.seed)

    store_ids = build_store_ids(args.store_count)

    for i in range(1, args.csv_count + 1):
        rows = rng.randint(args.min_rows, args.max_rows)
        filename = f"data_{i:02d}.csv"
        path = os.path.join(args.output_dir, filename)
        write_csv(
            path,
            rows=rows,
            store_ids=store_ids,
            rng=rng,
            min_cols=args.min_cols,
            max_cols=args.max_cols,
        )


if __name__ == "__main__":
    main()
