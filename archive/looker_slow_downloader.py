#!/usr/bin/env python3
"""
Looker Slow Downloader
慢慢下載 Looker 資料，帶重複驗證機制
"""

import requests
import time
import csv
import os
from datetime import datetime
import hashlib
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Looker Configuration
LOOKER_BASE_URL = os.getenv("LOOKER_BASE_URL", "https://analytics-platform.91app.com")
LOOKER_CLIENT_ID = os.getenv("LOOKER_CLIENT_ID")
LOOKER_CLIENT_SECRET = os.getenv("LOOKER_CLIENT_SECRET")

if not LOOKER_CLIENT_ID or not LOOKER_CLIENT_SECRET:
    raise ValueError("Missing LOOKER_CLIENT_ID or LOOKER_CLIENT_SECRET in environment")


class SlowDownloader:
    def __init__(self, query_id, batch_size=50000, output_prefix="looker_slow"):
        """
        初始化下載器

        Args:
            query_id: Looker Query ID
            batch_size: 每批次筆數（建議 10K-50K）
            output_prefix: 輸出檔案前綴
        """
        self.query_id = query_id
        self.batch_size = batch_size
        self.output_prefix = output_prefix
        self.access_token = None

        # 追蹤已下載的 unique hashes（用於偵測重複）
        self.seen_hashes = set()
        self.batch_files = []
        self.total_downloaded = 0
        self.total_unique = 0

    def authenticate(self):
        """認證"""
        print("認證中...")
        auth_url = f"{LOOKER_BASE_URL}/api/4.0/login"
        payload = {
            "client_id": LOOKER_CLIENT_ID,
            "client_secret": LOOKER_CLIENT_SECRET
        }

        response = requests.post(auth_url, data=payload, verify=True)
        response.raise_for_status()
        self.access_token = response.json()["access_token"]
        print("✓ 認證成功")

    def get_query_info(self):
        """取得 query 資訊"""
        headers = {"Authorization": f"Bearer {self.access_token}"}
        query_url = f"{LOOKER_BASE_URL}/api/4.0/queries/{self.query_id}"

        response = requests.get(query_url, headers=headers, verify=True)
        return response.json()

    def download_batch(self, offset):
        """
        下載一個批次

        Returns:
            (csv_data, row_count, unique_count, is_duplicate)
        """
        headers = {"Authorization": f"Bearer {self.access_token}"}
        run_url = f"{LOOKER_BASE_URL}/api/4.0/queries/{self.query_id}/run/csv"

        params = {
            "limit": str(self.batch_size),
            "offset": str(offset)
        }

        print(f"\n下載批次：offset={offset:,}, limit={self.batch_size:,}")
        start_time = time.time()

        try:
            response = requests.get(
                run_url,
                headers=headers,
                params=params,
                verify=True,
                timeout=600  # 10 分鐘超時
            )

            if response.status_code != 200:
                print(f"  ✗ HTTP {response.status_code}")
                return None, 0, 0, False

            csv_data = response.text
            elapsed = time.time() - start_time

            # 解析資料
            lines = csv_data.strip().split('\n')
            if len(lines) <= 1:
                print(f"  ✗ 無資料")
                return None, 0, 0, False

            row_count = len(lines) - 1  # 扣除 header

            # 檢查 unique hashes（假設第一個資料欄位是 hash）
            reader = csv.reader(lines[1:])  # Skip header
            batch_hashes = set()

            for row in reader:
                if row and len(row) > 0:
                    # 取第一個非空欄位作為識別
                    hash_value = '|'.join([cell for cell in row if cell.strip()])
                    if hash_value:
                        batch_hashes.add(hash_value)

            # 檢查與之前批次的重複度
            new_hashes = batch_hashes - self.seen_hashes
            unique_count = len(new_hashes)
            duplicate_rate = (len(batch_hashes) - unique_count) / len(batch_hashes) * 100 if batch_hashes else 0

            is_duplicate = duplicate_rate > 90  # 超過 90% 重複視為重複批次

            print(f"  ✓ 收到 {row_count:,} 筆 (耗時 {elapsed:.1f} 秒)")
            print(f"    新增 unique: {unique_count:,} 筆")
            print(f"    重複率: {duplicate_rate:.1f}%")

            if not is_duplicate:
                self.seen_hashes.update(new_hashes)
                self.total_unique += unique_count

            return csv_data, row_count, unique_count, is_duplicate

        except requests.exceptions.Timeout:
            print(f"  ✗ 超時 (>10 分鐘)")
            return None, 0, 0, False

        except Exception as e:
            print(f"  ✗ 錯誤：{e}")
            return None, 0, 0, False

    def save_batch(self, batch_num, csv_data):
        """儲存批次檔案"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.output_prefix}_batch_{batch_num:03d}_{timestamp}.csv"

        with open(filename, 'w', encoding='utf-8') as f:
            f.write(csv_data)

        self.batch_files.append(filename)
        print(f"  ✓ 儲存到：{filename}")
        return filename

    def merge_batches(self, output_file):
        """合併所有批次"""
        if not self.batch_files:
            print("⚠️  沒有批次檔案可合併")
            return

        print(f"\n{'='*60}")
        print(f"合併 {len(self.batch_files)} 個批次到：{output_file}")
        print(f"{'='*60}")

        with open(output_file, 'w', encoding='utf-8') as outfile:
            for i, batch_file in enumerate(self.batch_files, 1):
                print(f"  合併 {i}/{len(self.batch_files)}: {batch_file}")
                with open(batch_file, 'r', encoding='utf-8') as infile:
                    if i == 1:
                        # 第一個檔案：保留 header
                        outfile.write(infile.read())
                    else:
                        # 其他檔案：跳過 header
                        lines = infile.readlines()
                        if len(lines) > 1:
                            outfile.writelines(lines[1:])

        print(f"✓ 合併完成")

        # 統計最終行數
        with open(output_file, 'r') as f:
            final_count = sum(1 for _ in f) - 1  # 扣除 header

        print(f"\n最終統計：")
        print(f"  總下載筆數：{self.total_downloaded:,}")
        print(f"  Unique 筆數：{self.total_unique:,}")
        print(f"  合併檔案筆數：{final_count:,}")
        print(f"  去重率：{(1 - final_count/self.total_downloaded)*100:.1f}%")

    def download(self, max_batches=None):
        """
        開始下載

        Args:
            max_batches: 最大批次數（None = 無限制）
        """
        self.authenticate()

        # 取得 query 資訊
        query_info = self.get_query_info()
        print(f"\n{'='*60}")
        print(f"開始慢慢下載")
        print(f"{'='*60}")
        print(f"Query ID: {self.query_id}")
        print(f"Model: {query_info.get('model', 'N/A')}")
        print(f"View: {query_info.get('view', 'N/A')}")
        print(f"Fields: {query_info.get('fields', [])}")
        print(f"Filters: {query_info.get('filters', {})}")
        print(f"批次大小: {self.batch_size:,} 筆")

        if max_batches:
            print(f"最大批次數: {max_batches}")

        offset = 0
        batch_num = 1
        consecutive_duplicates = 0
        start_time = datetime.now()

        while True:
            if max_batches and batch_num > max_batches:
                print(f"\n✓ 達到最大批次數 ({max_batches})")
                break

            print(f"\n{'='*60}")
            print(f"批次 {batch_num}")
            print(f"{'='*60}")
            print(f"已下載：{self.total_downloaded:,} 筆（unique: {self.total_unique:,}）")
            elapsed = (datetime.now() - start_time).total_seconds()
            print(f"已執行：{elapsed/60:.1f} 分鐘")

            # 下載批次
            csv_data, row_count, unique_count, is_duplicate = self.download_batch(offset)

            if csv_data is None:
                print("\n✗ 下載失敗，停止")
                break

            if row_count == 0:
                print("\n✓ 無更多資料")
                break

            # 檢查重複
            if is_duplicate:
                consecutive_duplicates += 1
                print(f"  ⚠️  重複批次！（連續 {consecutive_duplicates} 次）")

                if consecutive_duplicates >= 2:
                    print(f"\n✗ 連續 {consecutive_duplicates} 個批次重複，停止下載")
                    print(f"   原因：Looker API 的 offset 參數無效")
                    break
            else:
                consecutive_duplicates = 0

            # 儲存批次
            self.save_batch(batch_num, csv_data)
            self.total_downloaded += row_count

            # 如果收到的筆數少於請求數，表示到底了
            if row_count < self.batch_size:
                print(f"\n✓ 到達資料末端（本批次只有 {row_count:,} 筆）")
                break

            # 下一個批次
            offset += self.batch_size
            batch_num += 1

            # 休息一下，避免太頻繁
            print(f"  休息 3 秒...")
            time.sleep(3)

        # 輸出最終統計
        total_time = (datetime.now() - start_time).total_seconds()
        print(f"\n{'='*60}")
        print(f"下載完成")
        print(f"{'='*60}")
        print(f"總執行時間：{total_time/60:.1f} 分鐘")
        print(f"總批次數：{len(self.batch_files)}")
        print(f"總下載筆數：{self.total_downloaded:,}")
        print(f"Unique 筆數：{self.total_unique:,}")

        # 合併檔案
        if self.batch_files:
            output_file = f"{self.output_prefix}_merged_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            self.merge_batches(output_file)
            print(f"\n✓ 最終檔案：{output_file}")
            return output_file

        return None


def main():
    import sys

    if len(sys.argv) < 2:
        print("使用方法:")
        print(f"  python3 {sys.argv[0]} <query_id> [batch_size] [max_batches]")
        print("\n範例:")
        print(f"  python3 {sys.argv[0]} Japtc2w4jUKaJGmWrRRBzj 50000")
        print(f"  python3 {sys.argv[0]} Japtc2w4jUKaJGmWrRRBzj 50000 10  # 只下載 10 個批次測試")
        sys.exit(1)

    query_id = sys.argv[1]
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 50000
    max_batches = int(sys.argv[3]) if len(sys.argv) > 3 else None

    downloader = SlowDownloader(query_id, batch_size=batch_size)
    downloader.download(max_batches=max_batches)


if __name__ == "__main__":
    main()
