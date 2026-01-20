#!/usr/bin/env python3
"""
Looker Hash Mapper
將 CMoney 的非 E.164 hash 轉換為 E.164 格式

使用方法:
  python3 looker_hash_mapper.py <looker_file.csv> <cmoney_dir>

範例:
  python3 looker_hash_mapper.py looker_report_7000000.csv /Users/maki/Downloads/CMoney
"""

import csv
import sqlite3
import glob
import os
import sys
from datetime import datetime


class LookerHashMapper:
    def __init__(self, looker_file, output_suffix=""):
        """
        初始化 mapper

        Args:
            looker_file: Looker 匯出的 CSV 檔案路徑
            output_suffix: 輸出目錄的後綴 (例如 "7m", "14m")
        """
        self.looker_file = looker_file
        self.output_suffix = output_suffix or datetime.now().strftime("%Y%m%d_%H%M%S")

        # 建立暫時的 SQLite 資料庫
        self.db_file = f"hash_mapping_{self.output_suffix}.db"
        self.conn = None

    def create_database(self):
        """建立 SQLite 資料庫和索引"""
        print(f"\n{'='*60}")
        print(f"建立資料庫: {self.db_file}")
        print(f"{'='*60}")

        # 如果資料庫已存在，刪除它
        if os.path.exists(self.db_file):
            os.remove(self.db_file)
            print(f"✓ 已刪除舊資料庫")

        self.conn = sqlite3.connect(self.db_file)
        cursor = self.conn.cursor()

        # 建立 phone mapping 表
        cursor.execute("""
            CREATE TABLE phone_mapping (
                phone_hash TEXT PRIMARY KEY,
                phone_e164 TEXT NOT NULL
            )
        """)

        # 建立 email mapping 表
        cursor.execute("""
            CREATE TABLE email_mapping (
                email_hash TEXT PRIMARY KEY,
                email_raw TEXT NOT NULL
            )
        """)

        # 建立索引
        cursor.execute("CREATE INDEX idx_phone_hash ON phone_mapping(phone_hash)")
        cursor.execute("CREATE INDEX idx_email_hash ON email_mapping(email_hash)")

        self.conn.commit()
        print("✓ 資料庫表格和索引建立完成")

    def load_looker_data(self):
        """載入 Looker 資料到資料庫"""
        print(f"\n載入 Looker 資料: {os.path.basename(self.looker_file)}")

        if not os.path.exists(self.looker_file):
            raise FileNotFoundError(f"找不到 Looker 檔案: {self.looker_file}")

        cursor = self.conn.cursor()
        phone_count = 0
        email_count = 0
        processed = 0

        start_time = datetime.now()

        with open(self.looker_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            phone_batch = []
            email_batch = []
            batch_size = 10000

            for row in reader:
                processed += 1
                if processed % 1000000 == 0:
                    print(f"  已處理 {processed/1000000:.0f}M 筆...")

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

        # 統計 unique 數量
        cursor.execute("SELECT COUNT(*) FROM phone_mapping")
        unique_phones = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM email_mapping")
        unique_emails = cursor.fetchone()[0]

        load_time = (datetime.now() - start_time).total_seconds()

        print(f"✓ 載入完成 (耗時 {load_time:.1f} 秒)")
        print(f"  Phone mappings: {phone_count:,} 筆 → {unique_phones:,} unique")
        print(f"  Email mappings: {email_count:,} 筆 → {unique_emails:,} unique")
        print(f"  總 unique mappings: {unique_phones + unique_emails:,}")

        return unique_phones, unique_emails

    def map_cmoney_files(self, cmoney_dir):
        """
        Mapping CMoney 檔案

        Args:
            cmoney_dir: CMoney CSV 檔案所在目錄
        """
        print(f"\n{'='*60}")
        print(f"開始 mapping CMoney 檔案")
        print(f"{'='*60}")

        # 建立輸出目錄
        output_dir = f"output/mapped_cmoney_{self.output_suffix}"
        os.makedirs(output_dir, exist_ok=True)
        print(f"輸出目錄: {output_dir}")

        # 尋找所有 CMoney CSV 檔案
        cmoney_files = glob.glob(f"{cmoney_dir}/*.csv")

        if not cmoney_files:
            print(f"⚠️  在 {cmoney_dir} 中找不到 CSV 檔案")
            return

        print(f"找到 {len(cmoney_files)} 個檔案")

        cursor = self.conn.cursor()
        total_records = 0
        total_matched_phone = 0
        total_matched_email = 0

        # 處理每個 CMoney 檔案
        for cmoney_file in sorted(cmoney_files):
            filename = os.path.basename(cmoney_file)
            print(f"\nProcessing: {filename}")

            matched_phone = 0
            matched_email = 0
            total = 0
            unmatched = 0

            output_file = os.path.join(output_dir, f"mapped_{filename}")

            with open(cmoney_file, 'r', encoding='utf-8') as fin:
                with open(output_file, 'w', encoding='utf-8', newline='') as fout:
                    reader = csv.reader(fin)
                    writer = csv.writer(fout)

                    for row in reader:
                        if not row or not row[0]:
                            continue

                        hash_value = row[0].strip()
                        total += 1

                        # 嘗試 phone mapping
                        cursor.execute("SELECT phone_e164 FROM phone_mapping WHERE phone_hash = ?", (hash_value,))
                        result = cursor.fetchone()

                        if result:
                            writer.writerow([result[0]])
                            matched_phone += 1
                        else:
                            # 嘗試 email mapping
                            cursor.execute("SELECT email_raw FROM email_mapping WHERE email_hash = ?", (hash_value,))
                            result = cursor.fetchone()

                            if result:
                                writer.writerow([result[0]])
                                matched_email += 1
                            else:
                                writer.writerow([hash_value])
                                unmatched += 1

            match_rate = (matched_phone + matched_email) / total * 100 if total > 0 else 0
            print(f"  Total records: {total:,}")
            print(f"  Matched (phone): {matched_phone:,}")
            print(f"  Matched (email): {matched_email:,}")
            print(f"  Unmatched: {unmatched:,}")
            print(f"  Match rate: {match_rate:.2f}%")

            total_records += total
            total_matched_phone += matched_phone
            total_matched_email += matched_email

        # 輸出總結
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        overall_match_rate = (total_matched_phone + total_matched_email) / total_records * 100 if total_records > 0 else 0
        print(f"Total records processed: {total_records:,}")
        print(f"Matched as phone (E.164): {total_matched_phone:,}")
        print(f"Matched as email (raw data): {total_matched_email:,}")
        print(f"Unmatched (kept original): {total_records - total_matched_phone - total_matched_email:,}")
        print(f"\n🎯 Overall match rate: {overall_match_rate:.2f}%")
        print(f"\nOutput directory: {output_dir}")

        return overall_match_rate

    def close(self):
        """關閉資料庫連線"""
        if self.conn:
            self.conn.close()
            print(f"\n✓ Database saved to: {self.db_file}")


def main():
    if len(sys.argv) < 3:
        print("使用方法:")
        print(f"  python3 {sys.argv[0]} <looker_file.csv> <cmoney_dir> [output_suffix]")
        print("\n範例:")
        print(f"  python3 {sys.argv[0]} looker_report_7000000.csv /Users/maki/Downloads/CMoney 7m")
        sys.exit(1)

    looker_file = sys.argv[1]
    cmoney_dir = sys.argv[2]
    output_suffix = sys.argv[3] if len(sys.argv) > 3 else ""

    print("=" * 60)
    print("Looker Hash Mapper")
    print("=" * 60)
    print(f"Looker file: {looker_file}")
    print(f"CMoney directory: {cmoney_dir}")
    print(f"Output suffix: {output_suffix or '(auto)'}")

    # 建立 mapper 並執行
    mapper = LookerHashMapper(looker_file, output_suffix)

    try:
        mapper.create_database()
        mapper.load_looker_data()
        mapper.map_cmoney_files(cmoney_dir)
    finally:
        mapper.close()

    print("\n✓ 完成！")


if __name__ == "__main__":
    main()
