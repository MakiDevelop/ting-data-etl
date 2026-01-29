#!/usr/bin/env python3
"""
Looker Downloader - 整合版
支援三種下載模式：custom、stable、slow

模式說明：
- custom: 建立自訂 query（需指定日期範圍）
- stable: 使用現有 query ID（支援明確排序）
- slow:   使用現有 query ID（含重複偵測）

使用範例：
  # custom 模式 - 建立新 query
  python3 looker_downloader.py custom --from 2025/01/01 --to 2025/12/31

  # stable 模式 - 使用現有 query
  python3 looker_downloader.py stable --query-id Japtc2w4jUKaJGmWrRRBzj

  # slow 模式 - 含重複偵測
  python3 looker_downloader.py slow --query-id Japtc2w4jUKaJGmWrRRBzj
"""

import argparse
import csv
import os
import requests
import time
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Looker Configuration
LOOKER_BASE_URL = os.getenv("LOOKER_BASE_URL", "https://analytics-platform.91app.com")
LOOKER_CLIENT_ID = os.getenv("LOOKER_CLIENT_ID")
LOOKER_CLIENT_SECRET = os.getenv("LOOKER_CLIENT_SECRET")

if not LOOKER_CLIENT_ID or not LOOKER_CLIENT_SECRET:
    raise ValueError("Missing LOOKER_CLIENT_ID or LOOKER_CLIENT_SECRET in environment")


class LookerDownloader:
    """整合版 Looker 下載器"""

    def __init__(
        self,
        mode: str,
        query_id: str = None,
        date_from: str = None,
        date_to: str = None,
        sort_field: str = None,
        batch_size: int = 50000,
        output_prefix: str = None,
        detect_duplicates: bool = False,
    ):
        """
        初始化下載器

        Args:
            mode: 下載模式 (custom, stable, slow)
            query_id: Looker Query ID (stable/slow 模式必填)
            date_from: 開始日期 (custom 模式必填，格式: "2025/01/01")
            date_to: 結束日期 (custom 模式必填)
            sort_field: 排序欄位 (stable 模式可選)
            batch_size: 每批次筆數
            output_prefix: 輸出檔案前綴
            detect_duplicates: 是否偵測重複 (slow 模式自動開啟)
        """
        self.mode = mode
        self.query_id = query_id
        self.date_from = date_from
        self.date_to = date_to
        self.sort_field = sort_field
        self.batch_size = batch_size
        self.detect_duplicates = detect_duplicates or (mode == "slow")

        # 自動生成 output_prefix
        if output_prefix:
            self.output_prefix = output_prefix
        elif mode == "custom" and date_from and date_to:
            date_label = date_from.replace("/", "") + "_" + date_to.replace("/", "")
            self.output_prefix = f"looker_{date_label}"
        else:
            self.output_prefix = f"looker_{mode}"

        self.access_token = None
        self.batch_files = []
        self.total_downloaded = 0

        # 重複偵測用
        self.seen_hashes = set() if self.detect_duplicates else None
        self.total_unique = 0

    def authenticate(self):
        """認證"""
        print("認證中...")
        auth_url = f"{LOOKER_BASE_URL}/api/4.0/login"
        payload = {
            "client_id": LOOKER_CLIENT_ID,
            "client_secret": LOOKER_CLIENT_SECRET,
        }

        response = requests.post(auth_url, data=payload, verify=True)
        response.raise_for_status()
        self.access_token = response.json()["access_token"]
        print("✓ 認證成功")

    def create_query(self):
        """建立自訂 query (custom 模式)"""
        print(f"\n{'='*60}")
        print("建立 Query")
        print(f"{'='*60}")

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        create_url = f"{LOOKER_BASE_URL}/api/4.0/queries"

        query_body = {
            "model": "prod_tw_external_general",
            "view": "member",
            "fields": [
                "member_personal_info.phone",
                "member_personal_info.phone_e164",
                "member_personal_info.email",
                "member_personal_info.email_raw",
            ],
            "filters": {
                "member.state": "Enabled",
                "member.join_at_date": f"{self.date_from} to {self.date_to}",
            },
            "sorts": ["member_personal_info.phone"],
            "limit": "500",
        }

        print(f"日期範圍: {self.date_from} to {self.date_to}")
        print("排序: member_personal_info.phone")

        response = requests.post(create_url, headers=headers, json=query_body, verify=True)

        if response.status_code != 200:
            print(f"✗ 建立失敗: {response.status_code}")
            print(f"  {response.text}")
            raise Exception("Query 建立失敗")

        query_info = response.json()
        self.query_id = query_info.get("id") or query_info.get("client_id")

        print("✓ Query 建立成功")
        print(f"  Query ID: {self.query_id}")
        return query_info

    def get_query_info(self):
        """取得 query 資訊"""
        headers = {"Authorization": f"Bearer {self.access_token}"}
        query_url = f"{LOOKER_BASE_URL}/api/4.0/queries/{self.query_id}"

        response = requests.get(query_url, headers=headers, verify=True)
        return response.json()

    def download_batch(self, offset: int):
        """
        下載一個批次

        Returns:
            (csv_data, row_count, unique_count, is_duplicate)
        """
        headers = {"Authorization": f"Bearer {self.access_token}"}
        run_url = f"{LOOKER_BASE_URL}/api/4.0/queries/{self.query_id}/run/csv"

        params = {
            "limit": str(self.batch_size),
            "offset": str(offset),
        }

        # stable 模式：可指定排序欄位
        sort_info = ""
        if self.sort_field:
            params["sorts"] = self.sort_field
            sort_info = f", sorts={self.sort_field}"

        print(f"\n下載批次：offset={offset:,}, limit={self.batch_size:,}{sort_info}")
        start_time = time.time()

        try:
            response = requests.get(
                run_url,
                headers=headers,
                params=params,
                verify=True,
                timeout=600,
            )

            if response.status_code != 200:
                print(f"  ✗ HTTP {response.status_code}: {response.text[:200]}")
                return None, 0, 0, False

            csv_data = response.text
            elapsed = time.time() - start_time

            lines = csv_data.strip().split("\n")
            if len(lines) <= 1:
                print("  ✗ 無資料")
                return None, 0, 0, False

            row_count = len(lines) - 1

            # 重複偵測 (slow 模式)
            unique_count = row_count
            is_duplicate = False

            if self.detect_duplicates and self.seen_hashes is not None:
                reader = csv.reader(lines[1:])
                batch_hashes = set()

                for row in reader:
                    if row:
                        hash_value = "|".join([cell for cell in row if cell.strip()])
                        if hash_value:
                            batch_hashes.add(hash_value)

                new_hashes = batch_hashes - self.seen_hashes
                unique_count = len(new_hashes)
                duplicate_rate = (
                    (len(batch_hashes) - unique_count) / len(batch_hashes) * 100
                    if batch_hashes
                    else 0
                )
                is_duplicate = duplicate_rate > 90

                print(f"  ✓ 收到 {row_count:,} 筆 (耗時 {elapsed:.1f} 秒)")
                print(f"    新增 unique: {unique_count:,} 筆, 重複率: {duplicate_rate:.1f}%")

                if not is_duplicate:
                    self.seen_hashes.update(new_hashes)
                    self.total_unique += unique_count
            else:
                print(f"  ✓ 收到 {row_count:,} 筆 (耗時 {elapsed:.1f} 秒)")

            return csv_data, row_count, unique_count, is_duplicate

        except requests.exceptions.Timeout:
            print("  ✗ 超時 (>10 分鐘)")
            return None, 0, 0, False
        except Exception as e:
            print(f"  ✗ 錯誤：{e}")
            return None, 0, 0, False

    def save_batch(self, batch_num: int, csv_data: str):
        """儲存批次檔案"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.output_prefix}_batch_{batch_num:03d}_{timestamp}.csv"

        with open(filename, "w", encoding="utf-8") as f:
            f.write(csv_data)

        self.batch_files.append(filename)
        print(f"  ✓ 儲存到：{filename}")
        return filename

    def merge_batches(self, output_file: str):
        """合併所有批次"""
        if not self.batch_files:
            print("⚠️  沒有批次檔案可合併")
            return

        print(f"\n{'='*60}")
        print(f"合併 {len(self.batch_files)} 個批次到：{output_file}")
        print(f"{'='*60}")

        with open(output_file, "w", encoding="utf-8") as outfile:
            for i, batch_file in enumerate(self.batch_files, 1):
                print(f"  合併 {i}/{len(self.batch_files)}: {batch_file}")
                with open(batch_file, "r", encoding="utf-8") as infile:
                    if i == 1:
                        outfile.write(infile.read())
                    else:
                        lines = infile.readlines()
                        if len(lines) > 1:
                            outfile.writelines(lines[1:])

        print("✓ 合併完成")

        with open(output_file, "r") as f:
            final_count = sum(1 for _ in f) - 1

        print("\n最終統計：")
        print(f"  總下載筆數：{self.total_downloaded:,}")
        if self.detect_duplicates:
            print(f"  Unique 筆數：{self.total_unique:,}")
        print(f"  合併檔案筆數：{final_count:,}")

    def download(self, max_batches: int = None):
        """開始下載"""
        self.authenticate()

        # custom 模式：先建立 query
        if self.mode == "custom":
            self.create_query()

        # 取得 query 資訊
        query_info = self.get_query_info()

        print(f"\n{'='*60}")
        print(f"開始下載 [{self.mode.upper()} 模式]")
        print(f"{'='*60}")
        print(f"Query ID: {self.query_id}")
        print(f"Model: {query_info.get('model', 'N/A')}")
        print(f"View: {query_info.get('view', 'N/A')}")
        print(f"批次大小: {self.batch_size:,} 筆")

        if self.sort_field:
            print(f"排序欄位: {self.sort_field}")
        if self.detect_duplicates:
            print("重複偵測: 開啟")
        if max_batches:
            print(f"最大批次數: {max_batches}")

        offset = 0
        batch_num = 1
        consecutive_issues = 0
        start_time = datetime.now()

        while True:
            if max_batches and batch_num > max_batches:
                print(f"\n✓ 達到最大批次數 ({max_batches})")
                break

            print(f"\n{'='*60}")
            print(f"批次 {batch_num}")
            print(f"{'='*60}")

            if self.detect_duplicates:
                print(f"已下載：{self.total_downloaded:,} 筆（unique: {self.total_unique:,}）")
            else:
                print(f"已下載：{self.total_downloaded:,} 筆")

            elapsed = (datetime.now() - start_time).total_seconds()
            print(f"已執行：{elapsed/60:.1f} 分鐘")

            # 下載批次
            csv_data, row_count, unique_count, is_duplicate = self.download_batch(offset)

            if csv_data is None:
                consecutive_issues += 1
                print(f"  ⚠️  下載失敗（連續 {consecutive_issues} 次）")

                if consecutive_issues >= 2:
                    print(f"\n✗ 連續 {consecutive_issues} 次失敗，停止下載")
                    break

                print("  等待 5 秒後重試...")
                time.sleep(5)
                continue

            if row_count == 0:
                print("\n✓ 無更多資料")
                break

            # 重複偵測
            if is_duplicate:
                consecutive_issues += 1
                print(f"  ⚠️  重複批次！（連續 {consecutive_issues} 次）")

                if consecutive_issues >= 2:
                    print(f"\n✗ 連續 {consecutive_issues} 個批次重複，停止下載")
                    break
            else:
                consecutive_issues = 0

            # 儲存批次
            self.save_batch(batch_num, csv_data)
            self.total_downloaded += row_count

            if row_count < self.batch_size:
                print(f"\n✓ 到達資料末端（本批次只有 {row_count:,} 筆）")
                break

            offset += self.batch_size
            batch_num += 1

            print("  休息 3 秒...")
            time.sleep(3)

        # 最終統計
        total_time = (datetime.now() - start_time).total_seconds()
        print(f"\n{'='*60}")
        print("下載完成")
        print(f"{'='*60}")
        print(f"總執行時間：{total_time/60:.1f} 分鐘")
        print(f"總批次數：{len(self.batch_files)}")
        print(f"總下載筆數：{self.total_downloaded:,}")

        if self.detect_duplicates:
            print(f"Unique 筆數：{self.total_unique:,}")

        # 合併檔案
        if self.batch_files:
            output_file = f"{self.output_prefix}_merged_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            self.merge_batches(output_file)
            print(f"\n✓ 最終檔案：{output_file}")
            return output_file

        return None


def main():
    parser = argparse.ArgumentParser(
        description="Looker 資料下載工具（整合版）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例：
  # custom 模式 - 建立新 query（需指定日期範圍）
  python3 looker_downloader.py custom --from 2025/01/01 --to 2025/12/31
  python3 looker_downloader.py custom --from 2025/12/01 --to 2025/12/31 --batch-size 30000

  # stable 模式 - 使用現有 query（可指定排序）
  python3 looker_downloader.py stable --query-id Japtc2w4jUKaJGmWrRRBzj
  python3 looker_downloader.py stable --query-id Japtc2w4jUKaJGmWrRRBzj --sort member.id

  # slow 模式 - 含重複偵測
  python3 looker_downloader.py slow --query-id Japtc2w4jUKaJGmWrRRBzj --max-batches 5
        """,
    )

    parser.add_argument(
        "mode",
        choices=["custom", "stable", "slow"],
        help="下載模式：custom(建立新query)、stable(使用現有query)、slow(含重複偵測)",
    )
    parser.add_argument(
        "--query-id", "-q",
        help="Query ID (stable/slow 模式必填)",
    )
    parser.add_argument(
        "--from", dest="date_from",
        help="開始日期 (custom 模式必填，格式: 2025/01/01)",
    )
    parser.add_argument(
        "--to", dest="date_to",
        help="結束日期 (custom 模式必填)",
    )
    parser.add_argument(
        "--sort", "-s",
        help="排序欄位 (stable 模式可選，如: member.id)",
    )
    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=50000,
        help="每批次筆數 (預設: 50000)",
    )
    parser.add_argument(
        "--max-batches", "-m",
        type=int,
        help="最大批次數 (用於測試)",
    )
    parser.add_argument(
        "--output-prefix", "-o",
        help="輸出檔案前綴",
    )

    args = parser.parse_args()

    # 驗證參數
    if args.mode == "custom":
        if not args.date_from or not args.date_to:
            parser.error("custom 模式需要 --from 和 --to 參數")
    else:
        if not args.query_id:
            parser.error(f"{args.mode} 模式需要 --query-id 參數")

    downloader = LookerDownloader(
        mode=args.mode,
        query_id=args.query_id,
        date_from=args.date_from,
        date_to=args.date_to,
        sort_field=args.sort,
        batch_size=args.batch_size,
        output_prefix=args.output_prefix,
    )

    downloader.download(max_batches=args.max_batches)


if __name__ == "__main__":
    main()
