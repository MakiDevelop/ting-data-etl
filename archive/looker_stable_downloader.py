#!/usr/bin/env python3
"""
Looker Stable Downloader (with explicit sorting)
使用明確排序條件的穩定分頁下載

關鍵改進：加上 sorts 參數，確保 offset pagination 穩定
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


class StableDownloader:
    def __init__(self, query_id, sort_field=None, batch_size=50000, output_prefix="looker_stable"):
        """
        初始化下載器

        Args:
            query_id: Looker Query ID
            sort_field: 排序欄位（例如："member.id" 或 "member.join_time"）
                       如果為 None，則使用 query 自己的排序設定
            batch_size: 每批次筆數（建議 10K-50K）
            output_prefix: 輸出檔案前綴
        """
        self.query_id = query_id
        self.sort_field = sort_field
        self.batch_size = batch_size
        self.output_prefix = output_prefix
        self.access_token = None

        self.batch_files = []
        self.total_downloaded = 0

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
        下載一個批次（使用明確排序）

        Returns:
            (csv_data, row_count)
        """
        headers = {"Authorization": f"Bearer {self.access_token}"}
        run_url = f"{LOOKER_BASE_URL}/api/4.0/queries/{self.query_id}/run/csv"

        # 建立參數
        params = {
            "limit": str(self.batch_size),
            "offset": str(offset)
        }

        # 如果有指定排序欄位，才加上 sorts 參數
        if self.sort_field:
            params["sorts"] = self.sort_field
            print(f"\n下載批次：offset={offset:,}, limit={self.batch_size:,}, sorts={self.sort_field}")
        else:
            print(f"\n下載批次：offset={offset:,}, limit={self.batch_size:,}, sorts=(使用 query 預設)")
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
                print(f"  ✗ HTTP {response.status_code}: {response.text[:200]}")
                return None, 0

            csv_data = response.text
            elapsed = time.time() - start_time

            # 解析資料
            lines = csv_data.strip().split('\n')
            if len(lines) <= 1:
                print(f"  ✗ 無資料")
                return None, 0

            row_count = len(lines) - 1  # 扣除 header

            print(f"  ✓ 收到 {row_count:,} 筆 (耗時 {elapsed:.1f} 秒)")

            return csv_data, row_count

        except requests.exceptions.Timeout:
            print(f"  ✗ 超時 (>10 分鐘)")
            return None, 0

        except Exception as e:
            print(f"  ✗ 錯誤：{e}")
            return None, 0

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
        print(f"  合併檔案筆數：{final_count:,}")

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
        print(f"開始穩定下載")
        print(f"{'='*60}")
        print(f"Query ID: {self.query_id}")
        print(f"Model: {query_info.get('model', 'N/A')}")
        print(f"View: {query_info.get('view', 'N/A')}")
        print(f"Fields: {query_info.get('fields', [])}")
        print(f"Filters: {query_info.get('filters', {})}")

        if self.sort_field:
            print(f"排序欄位: {self.sort_field} (明確指定)")
        else:
            existing_sorts = query_info.get('sorts', [])
            if existing_sorts:
                print(f"排序欄位: {existing_sorts} (使用 query 預設)")
            else:
                print(f"排序欄位: (無) ⚠️  可能導致 non-deterministic ordering")

        print(f"批次大小: {self.batch_size:,} 筆")

        if max_batches:
            print(f"最大批次數: {max_batches}")

        offset = 0
        batch_num = 1
        start_time = datetime.now()

        while True:
            if max_batches and batch_num > max_batches:
                print(f"\n✓ 達到最大批次數 ({max_batches})")
                break

            print(f"\n{'='*60}")
            print(f"批次 {batch_num}")
            print(f"{'='*60}")
            print(f"已下載：{self.total_downloaded:,} 筆")
            elapsed = (datetime.now() - start_time).total_seconds()
            print(f"已執行：{elapsed/60:.1f} 分鐘")

            # 下載批次
            csv_data, row_count = self.download_batch(offset)

            if csv_data is None:
                print("\n✗ 下載失敗，停止")
                break

            if row_count == 0:
                print("\n✓ 無更多資料")
                break

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
        print(f"  python3 {sys.argv[0]} <query_id> [sort_field] [batch_size] [max_batches]")
        print("\n範例:")
        print(f"  # 使用 query 預設排序")
        print(f"  python3 {sys.argv[0]} Japtc2w4jUKaJGmWrRRBzj")
        print(f"  python3 {sys.argv[0]} Japtc2w4jUKaJGmWrRRBzj - 50000 2  # 測試 2 批次")
        print(f"\n  # 明確指定排序欄位")
        print(f"  python3 {sys.argv[0]} Japtc2w4jUKaJGmWrRRBzj member.id 50000")
        print(f"  python3 {sys.argv[0]} Japtc2w4jUKaJGmWrRRBzj member.join_time 50000")
        print("\n常用排序欄位:")
        print("  - member.id (會員 ID)")
        print("  - member.join_time (加入時間)")
        print("  - member_personal_info.phone (手機號碼)")
        print("\n⚠️  重要：如果 query 有 pivots，請使用 query 預設排序（不傳 sort_field）")
        sys.exit(1)

    query_id = sys.argv[1]
    sort_field = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] != '-' else None
    batch_size = int(sys.argv[3]) if len(sys.argv) > 3 else 50000
    max_batches = int(sys.argv[4]) if len(sys.argv) > 4 else None

    downloader = StableDownloader(query_id, sort_field, batch_size=batch_size)
    downloader.download(max_batches=max_batches)


if __name__ == "__main__":
    main()
