STORE_ID_HEADERS = ["商店序號", "shopId", "Shop 商店序號", "ShopId"]
#!/usr/bin/env python3
import argparse
import csv
import os


def iter_csv_files(input_dir: str):
    for entry in os.scandir(input_dir):
        if entry.is_file() and entry.name.lower().endswith(".csv"):
            yield entry.path, entry.name


def split_csv_file(path: str, name: str, output_dir: str, encoding: str):
    with open(path, "r", newline="", encoding=encoding) as f:
        reader = csv.reader(f)
        prefix_rows = []
        header = None
        store_idx = None

        for row in reader:
            found_idx = None
            for header_name in STORE_ID_HEADERS:
                if header_name in row:
                    found_idx = row.index(header_name)
                    break
            if found_idx is not None:
                header = row
                store_idx = found_idx
                break
            else:
                prefix_rows.append(row)

        if header is None:
            print(f"[warn] missing store id header, skipped: {path}")
            return

        for row in reader:
            if store_idx >= len(row):
                continue
            raw_store_id = row[store_idx].strip()

            # normalize storeId: treat as string ID, never numeric
            # e.g. "40316.0", "40316.00" -> "40316"
            if raw_store_id.endswith(".0"):
                store_id = raw_store_id.split(".")[0]
            else:
                store_id = raw_store_id
            if store_id == "":
                continue
            store_dir = os.path.join(output_dir, store_id)
            os.makedirs(store_dir, exist_ok=True)
            out_path = os.path.join(store_dir, name)
            needs_header = not os.path.exists(out_path) or os.path.getsize(out_path) == 0
            with open(out_path, "a", newline="", encoding=encoding) as out_f:
                writer = csv.writer(out_f)
                if needs_header:
                    for r in prefix_rows:
                        writer.writerow(r)
                    writer.writerow(header)
                writer.writerow(row)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fan-out CSV rows into per-商店序號 directories."
    )
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--encoding", default="utf-8")
    return parser.parse_args()


def main():
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    for path, name in iter_csv_files(args.input_dir):
        print(f"[info] processing: {path}")
        split_csv_file(path, name, args.output_dir, args.encoding)


if __name__ == "__main__":
    main()
