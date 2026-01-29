#!/usr/bin/env python3
"""
Looker Custom Downloader
用 API 創建自定義 query 並分批下載

可以完全控制：
- 日期範圍
- 排序條件
- 批次大小
"""

import requests
import time
import csv
import os
from datetime import datetime
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Looker Configuration
LOOKER_BASE_URL = os.getenv("LOOKER_BASE_URL", "https://analytics-platform.91app.com")
LOOKER_CLIENT_ID = os.getenv("LOOKER_CLIENT_ID")
LOOKER_CLIENT_SECRET = os.getenv("LOOKER_CLIENT_SECRET")

if not LOOKER_CLIENT_ID or not LOOKER_CLIENT_SECRET:
    raise ValueError("Missing LOOKER_CLIENT_ID or LOOKER_CLIENT_SECRET in environment")


class CustomDownloader:
    def __init__(self, date_from, date_to, batch_size=50000, output_prefix="looker_custom"):
        """
        初始化下載器

        Args:
            date_from: 開始日期 (格式: "2025/01/01")
            date_to: 結束日期 (格式: "2025/12/31")
            batch_size: 每批次筆數
            output_prefix: 輸出檔案前綴
        """
        self.date_from = date_from
        self.date_to = date_to
        self.batch_size = batch_size
        self.output_prefix = output_prefix
        self.access_token = None
        self.query_id = None

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

    def create_query(self):
        """創建自定義 query"""
        print(f"\n{'='*60}")
        print(f"創建 Query")
        print(f"{'='*60}")

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        create_url = f"{LOOKER_BASE_URL}/api/4.0/queries"

        # Query 定義
        query_body = {
            "model": "prod_tw_external_general",
            "view": "member",
            "fields": [
                "member_personal_info.phone",
                "member_personal_info.phone_e164",
                "member_personal_info.email",
                "member_personal_info.email_raw"
            ],
            "filters": {
                "member.state": "Enabled",
                "member.join_at_date": f"{self.date_from} to {self.date_to}"
            },
            "sorts": [
                "member_personal_info.phone"  # 使用 phone hash 排序（穩定且唯一）
            ],
            "limit": "500"  # 這個會被 run/csv 的 limit 參數覆蓋
        }

        print(f"日期範圍: {self.date_from} to {self.date_to}")
        print(f"排序: member_personal_info.phone")

        response = requests.post(create_url, headers=headers, json=query_body, verify=True)

        if response.status_code != 200:
            print(f"✗ 創建失敗: {response.status_code}")
            print(f"  {response.text}")
            raise Exception("Query 創建失敗")

        query_info = response.json()
        self.query_id = query_info.get("id") or query_info.get("client_id")

        print(f"✓ Query 創建成功")
        print(f"  Query ID: {self.query_id}")
        print(f"  Model: {query_info.get('model')}")
        print(f"  View: {query_info.get('view')}")
        print(f"  Fields: {len(query_info.get('fields', []))} 個欄位")

        return query_info

    def download_batch(self, offset):
        """
        下載一個批次

        Returns:
            (csv_data, row_count)
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

            # 檢查前 3 筆資料（用於驗證是否重複）
            if row_count > 0:
                sample_lines = lines[1:min(4, len(lines))]
                print(f"  樣本資料前 3 筆:")
                for i, line in enumerate(sample_lines, 1):
                    cols = line.split(',')
                    if cols:
                        # 只顯示前幾個字元
                        preview = cols[0][:16] + "..." if len(cols[0]) > 16 else cols[0]
                        print(f"    {i}. {preview}")

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
        self.create_query()

        print(f"\n{'='*60}")
        print(f"開始下載")
        print(f"{'='*60}")
        print(f"日期範圍: {self.date_from} to {self.date_to}")
        print(f"批次大小: {self.batch_size:,} 筆")

        if max_batches:
            print(f"最大批次數: {max_batches}")

        offset = 0
        batch_num = 1
        consecutive_empty = 0
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
                consecutive_empty += 1
                print(f"  ⚠️  下載失敗或無資料 (連續 {consecutive_empty} 次)")

                if consecutive_empty >= 2:
                    print(f"\n✗ 連續 {consecutive_empty} 次無資料，停止下載")
                    break

                # 重試
                print(f"  等待 5 秒後重試...")
                time.sleep(5)
                continue

            if row_count == 0:
                print("\n✓ 無更多資料")
                break

            consecutive_empty = 0

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

    if len(sys.argv) < 3:
        print("使用方法:")
        print(f"  python3 {sys.argv[0]} <date_from> <date_to> [batch_size] [max_batches]")
        print("\n範例:")
        print(f"  # 下載 2025 全年")
        print(f"  python3 {sys.argv[0]} 2025/01/01 2025/12/31")
        print(f"\n  # 下載 2025/12 一個月")
        print(f"  python3 {sys.argv[0]} 2025/12/01 2025/12/31")
        print(f"\n  # 測試：下載 2025 Q1，只要前 2 批次")
        print(f"  python3 {sys.argv[0]} 2025/01/01 2025/03/31 50000 2")
        print(f"\n  # 下載 2024 全年")
        print(f"  python3 {sys.argv[0]} 2024/01/01 2024/12/31")
        sys.exit(1)

    date_from = sys.argv[1]
    date_to = sys.argv[2]
    batch_size = int(sys.argv[3]) if len(sys.argv) > 3 else 50000
    max_batches = int(sys.argv[4]) if len(sys.argv) > 4 else None

    # 生成有意義的 output prefix
    date_label = date_from.replace('/', '') + '_' + date_to.replace('/', '')
    output_prefix = f"looker_{date_label}"

    downloader = CustomDownloader(date_from, date_to, batch_size=batch_size, output_prefix=output_prefix)
    downloader.download(max_batches=max_batches)


if __name__ == "__main__":
    main()
