#!/usr/bin/env python3
"""
Looker Hash Mapper - Merge Tool
將新的 Looker 資料合併到現有的 combined database

使用方法:
  python3 looker_hash_mapper_merge.py <existing_db> <new_looker_file.csv> [month_label]

範例:
  python3 looker_hash_mapper_merge.py hash_mapping_combined.db looker_2025_12.csv 2025dec
"""

import csv
import sqlite3
import os
import sys
from datetime import datetime


class LookerHashMapperMerge:
    def __init__(self, existing_db, new_looker_file, month_label=""):
        """
        初始化 merge tool

        Args:
            existing_db: 現有的 SQLite 資料庫路徑
            new_looker_file: 新的 Looker CSV 檔案路徑
            month_label: 月份標籤 (例如 "2025dec")
        """
        self.existing_db = existing_db
        self.new_looker_file = new_looker_file
        self.month_label = month_label or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.conn = None

    def connect_database(self):
        """連接到現有資料庫"""
        print(f"\n{'='*60}")
        print(f"連接資料庫: {self.existing_db}")
        print(f"{'='*60}")

        if not os.path.exists(self.existing_db):
            raise FileNotFoundError(f"找不到資料庫: {self.existing_db}")

        self.conn = sqlite3.connect(self.existing_db)
        cursor = self.conn.cursor()

        # 檢查現有資料量
        cursor.execute("SELECT COUNT(*) FROM phone_mapping")
        existing_phones = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM email_mapping")
        existing_emails = cursor.fetchone()[0]

        print(f"✓ 資料庫連接成功")
        print(f"  現有 phone mappings: {existing_phones:,}")
        print(f"  現有 email mappings: {existing_emails:,}")
        print(f"  總計: {existing_phones + existing_emails:,}")

        return existing_phones, existing_emails

    def merge_looker_data(self):
        """合併新的 Looker 資料"""
        print(f"\n{'='*60}")
        print(f"合併新資料: {os.path.basename(self.new_looker_file)}")
        print(f"{'='*60}")

        if not os.path.exists(self.new_looker_file):
            raise FileNotFoundError(f"找不到 Looker 檔案: {self.new_looker_file}")

        cursor = self.conn.cursor()
        phone_count = 0
        email_count = 0
        processed = 0

        start_time = datetime.now()

        with open(self.new_looker_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            phone_batch = []
            email_batch = []
            batch_size = 10000

            for row in reader:
                processed += 1
                if processed % 100000 == 0:
                    print(f"  已處理 {processed:,} 筆...")

                phone_hash = row.get('Member Personal Info 手機號碼', '').strip()
                phone_e164 = row.get('Member Personal Info 手機號碼(E.164)', '').strip()
                email_hash = row.get('Member Personal Info 聯絡 Email', '').strip()
                email_raw = row.get('Member Personal Info 聯絡 Email (raw data)', '').strip()

                if phone_hash and phone_e164:
                    phone_batch.append((phone_hash, phone_e164))
                    phone_count += 1

                    if len(phone_batch) >= batch_size:
                        cursor.executemany('INSERT OR REPLACE INTO phone_mapping VALUES (?, ?)', phone_batch)
                        phone_batch = []

                if email_hash and email_raw:
                    email_batch.append((email_hash, email_raw))
                    email_count += 1

                    if len(email_batch) >= batch_size:
                        cursor.executemany('INSERT OR REPLACE INTO email_mapping VALUES (?, ?)', email_batch)
                        email_batch = []

            # 插入剩餘的記錄
            if phone_batch:
                cursor.executemany('INSERT OR REPLACE INTO phone_mapping VALUES (?, ?)', phone_batch)
            if email_batch:
                cursor.executemany('INSERT OR REPLACE INTO email_mapping VALUES (?, ?)', email_batch)

        self.conn.commit()

        merge_time = (datetime.now() - start_time).total_seconds()

        print(f"✓ 合併完成 (耗時 {merge_time:.1f} 秒)")
        print(f"  處理了 {processed:,} 筆新資料")
        print(f"  新增/更新 phone mappings: {phone_count:,}")
        print(f"  新增/更新 email mappings: {email_count:,}")

        return phone_count, email_count

    def show_statistics(self, before_phones, before_emails):
        """顯示合併後統計"""
        print(f"\n{'='*60}")
        print(f"合併後統計")
        print(f"{'='*60}")

        cursor = self.conn.cursor()

        # 合併後的數量
        cursor.execute("SELECT COUNT(*) FROM phone_mapping")
        after_phones = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM email_mapping")
        after_emails = cursor.fetchone()[0]

        new_phones = after_phones - before_phones
        new_emails = after_emails - before_emails

        print(f"\nPhone Mappings:")
        print(f"  合併前: {before_phones:,}")
        print(f"  合併後: {after_phones:,}")
        print(f"  新增 unique: {new_phones:,} (+{new_phones/before_phones*100:.2f}%)")

        print(f"\nEmail Mappings:")
        print(f"  合併前: {before_emails:,}")
        print(f"  合併後: {after_emails:,}")
        print(f"  新增 unique: {new_emails:,} (+{new_emails/before_emails*100:.2f}%)" if before_emails > 0 else f"  新增 unique: {new_emails:,}")

        print(f"\n總計:")
        print(f"  合併前: {before_phones + before_emails:,}")
        print(f"  合併後: {after_phones + after_emails:,}")
        print(f"  新增 unique: {new_phones + new_emails:,}")

        return after_phones, after_emails, new_phones, new_emails

    def close(self):
        """關閉資料庫連線"""
        if self.conn:
            self.conn.close()
            print(f"\n✓ 資料庫已更新: {self.existing_db}")


def main():
    if len(sys.argv) < 3:
        print("使用方法:")
        print(f"  python3 {sys.argv[0]} <existing_db> <new_looker_file.csv> [month_label]")
        print("\n範例:")
        print(f"  python3 {sys.argv[0]} hash_mapping_combined.db looker_2025_12.csv 2025dec")
        sys.exit(1)

    existing_db = sys.argv[1]
    new_looker_file = sys.argv[2]
    month_label = sys.argv[3] if len(sys.argv) > 3 else ""

    print("=" * 60)
    print("Looker Hash Mapper - Merge Tool")
    print("=" * 60)
    print(f"現有資料庫: {existing_db}")
    print(f"新 Looker 檔案: {new_looker_file}")
    print(f"月份標籤: {month_label or '(auto)'}")

    # 建立 merger 並執行
    merger = LookerHashMapperMerge(existing_db, new_looker_file, month_label)

    try:
        before_phones, before_emails = merger.connect_database()
        merger.merge_looker_data()
        merger.show_statistics(before_phones, before_emails)
    finally:
        merger.close()

    print("\n✓ 完成！")
    print("\n下一步:")
    print(f"  python3 looker_hash_mapper.py {existing_db} /Users/maki/Documents/CMoney combined")
    print("  (使用 .db 作為輸入時會直接使用現有資料庫，不會重建)")


if __name__ == "__main__":
    main()
