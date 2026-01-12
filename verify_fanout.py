#!/usr/bin/env python3
import argparse
import csv
import os
from typing import Dict, List, Set, Tuple


def list_input_files(input_dir: str) -> Set[str]:
    return {
        entry.name
        for entry in os.scandir(input_dir)
        if entry.is_file() and entry.name.lower().endswith(".csv")
    }


def list_store_dirs(output_dir: str) -> List[str]:
    return [
        entry.name
        for entry in os.scandir(output_dir)
        if entry.is_dir()
    ]


def list_store_files(store_dir: str) -> Set[str]:
    return {
        entry.name
        for entry in os.scandir(store_dir)
        if entry.is_file() and entry.name.lower().endswith(".csv")
    }


def check_file_sets(
    input_files: Set[str], output_dir: str, store_dirs: List[str]
) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]]]:
    missing_by_store: Dict[str, Set[str]] = {}
    extra_by_store: Dict[str, Set[str]] = {}

    for store in store_dirs:
        store_path = os.path.join(output_dir, store)
        store_files = list_store_files(store_path)
        missing = input_files - store_files
        extra = store_files - input_files
        if missing:
            missing_by_store[store] = missing
        if extra:
            extra_by_store[store] = extra

    return missing_by_store, extra_by_store


def check_store_no_values(
    output_dir: str, store_dirs: List[str], encoding: str
) -> Tuple[int, int, int, Dict[Tuple[str, str], List[Tuple[int, str]]]]:
    checked_files = 0
    violated_files = 0
    total_violations = 0
    violations: Dict[Tuple[str, str], List[Tuple[int, str]]] = {}

    for store in store_dirs:
        store_path = os.path.join(output_dir, store)
        for entry in os.scandir(store_path):
            if not (entry.is_file() and entry.name.lower().endswith(".csv")):
                continue
            checked_files += 1

            file_violations: List[Tuple[int, str]] = []
            with open(entry.path, "r", newline="", encoding=encoding) as f:
                reader = csv.reader(f)
                header = None
                store_idx = None
                header_row_num = 0

                for row in reader:
                    header_row_num += 1
                    if "商店序號" in row:
                        header = row
                        store_idx = row.index("商店序號")
                        break

                if header is None:
                    file_violations.append((1, "<missing 商店序號 header>"))
                    total_violations += 1
                    violations[(store, entry.name)] = file_violations
                    violated_files += 1
                    continue

                row_num = header_row_num
                for row in reader:
                    row_num += 1
                    if store_idx >= len(row):
                        actual = ""
                    else:
                        actual = row[store_idx]
                    if actual.strip() != store:
                        total_violations += 1
                        if len(file_violations) < 5:
                            file_violations.append((row_num, actual))
                if file_violations:
                    violations[(store, entry.name)] = file_violations
                    violated_files += 1

    return checked_files, violated_files, total_violations, violations


def print_set_report(
    input_files: Set[str],
    store_dirs: List[str],
    missing_by_store: Dict[str, Set[str]],
    extra_by_store: Dict[str, Set[str]],
):
    total_stores = len(store_dirs)
    missing_count = len(missing_by_store)
    extra_count = len(extra_by_store)
    ok_count = total_stores - len(
        set(missing_by_store.keys()) | set(extra_by_store.keys())
    )

    print("== File Set Check ==")
    print(f"stores: {total_stores}")
    print(f"stores ok: {ok_count}")
    print(f"stores missing files: {missing_count}")
    print(f"stores extra files: {extra_count}")

    if missing_by_store:
        print("-- Missing files (up to 10 stores) --")
        for store in sorted(missing_by_store.keys())[:10]:
            missing_list = sorted(missing_by_store[store])
            print(f"{store}: {', '.join(missing_list)}")
    if extra_by_store:
        print("-- Extra files (up to 10 stores) --")
        for store in sorted(extra_by_store.keys())[:10]:
            extra_list = sorted(extra_by_store[store])
            print(f"{store}: {', '.join(extra_list)}")

    print(f"input files: {len(input_files)}")


def print_content_report(
    checked_files: int,
    violated_files: int,
    total_violations: int,
    violations: Dict[Tuple[str, str], List[Tuple[int, str]]],
):
    print("== Content Check ==")
    print(f"checked files: {checked_files}")
    print(f"violated files: {violated_files}")
    print(f"violation rows (total): {total_violations}")

    if violations:
        print("-- Violations (up to 5 rows per file) --")
        for (store, filename), rows in sorted(violations.items()):
            for row_num, actual in rows:
                print(
                    f"{store}/{filename} line {row_num}: 商店序號={actual}"
                )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Verify fan-out output by 商店序號."
    )
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--encoding", default="utf-8")
    return parser.parse_args()


def main():
    args = parse_args()
    input_files = list_input_files(args.input_dir)
    store_dirs = list_store_dirs(args.output_dir)

    missing_by_store, extra_by_store = check_file_sets(
        input_files, args.output_dir, store_dirs
    )
    print_set_report(
        input_files, store_dirs, missing_by_store, extra_by_store
    )

    checked_files, violated_files, total_violations, violations = (
        check_store_no_values(args.output_dir, store_dirs, args.encoding)
    )
    print_content_report(
        checked_files, violated_files, total_violations, violations
    )

    has_errors = bool(missing_by_store or extra_by_store or total_violations)
    raise SystemExit(1 if has_errors else 0)


if __name__ == "__main__":
    main()
