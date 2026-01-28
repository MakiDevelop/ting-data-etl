"""
Hash Converter Service
將非 E.164 格式的 hash 轉換為 E.164 格式

支援：
- phone_hash → phone_e164
- email_hash → email_e164 (如有需要)
"""

import sqlite3
from pathlib import Path
from typing import Iterator, Callable, Optional
import csv
import io


class HashConverter:
    """Hash 轉換器，使用批次查詢優化效能"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.cursor = None

    def connect(self):
        """建立資料庫連線（唯讀模式）"""
        # 使用 URI 格式開啟唯讀連線
        db_uri = f"file:{self.db_path}?mode=ro"
        self.conn = sqlite3.connect(db_uri, uri=True, check_same_thread=False)
        self.cursor = self.conn.cursor()
        # 設定較大的 cache 以提升讀取效能
        self.cursor.execute("PRAGMA cache_size=-64000;")  # 64MB cache

    def close(self):
        """關閉連線"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_db_stats(self) -> dict:
        """取得資料庫統計資訊"""
        if not self.cursor:
            self.connect()

        stats = {}

        # Phone mapping 數量
        self.cursor.execute("SELECT COUNT(*) FROM phone_mapping")
        stats['phone_count'] = self.cursor.fetchone()[0]

        # Email mapping 數量
        try:
            self.cursor.execute("SELECT COUNT(*) FROM email_mapping")
            stats['email_count'] = self.cursor.fetchone()[0]
        except sqlite3.OperationalError:
            stats['email_count'] = 0

        return stats

    def batch_lookup_phone(self, hashes: list[str]) -> dict[str, str]:
        """
        批次查詢 phone hash → E.164

        Args:
            hashes: 要查詢的 hash 列表

        Returns:
            dict: {原始hash: E.164 hash}
        """
        if not hashes:
            return {}

        if not self.cursor:
            self.connect()

        # 使用 IN 子句批次查詢
        placeholders = ','.join(['?' for _ in hashes])
        query = f"""
            SELECT phone_hash, phone_e164
            FROM phone_mapping
            WHERE phone_hash IN ({placeholders})
        """

        self.cursor.execute(query, hashes)
        results = self.cursor.fetchall()

        return {row[0]: row[1] for row in results}

    def batch_lookup_email(self, hashes: list[str]) -> dict[str, str]:
        """
        批次查詢 email hash → E.164
        """
        if not hashes:
            return {}

        if not self.cursor:
            self.connect()

        placeholders = ','.join(['?' for _ in hashes])
        query = f"""
            SELECT email_hash, email_e164
            FROM email_mapping
            WHERE email_hash IN ({placeholders})
        """

        try:
            self.cursor.execute(query, hashes)
            results = self.cursor.fetchall()
            return {row[0]: row[1] for row in results}
        except sqlite3.OperationalError:
            return {}

    def process_csv(
        self,
        input_file,
        hash_column: str,
        hash_type: str = 'phone',  # 'phone' or 'email'
        batch_size: int = 5000,
        progress_callback: Optional[Callable[[float, int, int], None]] = None
    ) -> tuple[io.BytesIO, dict]:
        """
        處理 CSV 檔案，將指定欄位的 hash 轉換為 E.164

        Args:
            input_file: 上傳的檔案物件
            hash_column: 包含 hash 的欄位名稱
            hash_type: 'phone' 或 'email'
            batch_size: 批次查詢大小
            progress_callback: 進度回調函數 (progress, processed, total)

        Returns:
            (output_buffer, stats)
        """
        if not self.cursor:
            self.connect()

        # 讀取 CSV 內容
        content = input_file.read()
        if isinstance(content, bytes):
            content = content.decode('utf-8-sig')

        lines = content.strip().split('\n')
        reader = csv.DictReader(lines)

        # 確認欄位存在
        if hash_column not in reader.fieldnames:
            raise ValueError(f"找不到欄位: {hash_column}，可用欄位: {reader.fieldnames}")

        # 準備輸出
        output = io.StringIO()

        # 輸出欄位：原始欄位 + e164 欄位
        output_fieldnames = list(reader.fieldnames) + [f'{hash_column}_e164']
        writer = csv.DictWriter(output, fieldnames=output_fieldnames)
        writer.writeheader()

        # 統計
        total_rows = len(lines) - 1  # 扣除 header
        processed = 0
        matched = 0
        unmatched = 0

        # 批次處理
        batch = []
        batch_rows = []

        lookup_func = self.batch_lookup_phone if hash_type == 'phone' else self.batch_lookup_email

        for row in reader:
            hash_value = row.get(hash_column, '').strip()
            batch.append(hash_value)
            batch_rows.append(row)

            if len(batch) >= batch_size:
                # 執行批次查詢
                mapping = lookup_func(batch)

                # 寫入結果
                for r, h in zip(batch_rows, batch):
                    e164 = mapping.get(h, '')
                    r[f'{hash_column}_e164'] = e164
                    writer.writerow(r)

                    if e164:
                        matched += 1
                    else:
                        unmatched += 1

                processed += len(batch)

                # 回報進度
                if progress_callback:
                    progress_callback(processed / total_rows, processed, total_rows)

                batch = []
                batch_rows = []

        # 處理剩餘的資料
        if batch:
            mapping = lookup_func(batch)

            for r, h in zip(batch_rows, batch):
                e164 = mapping.get(h, '')
                r[f'{hash_column}_e164'] = e164
                writer.writerow(r)

                if e164:
                    matched += 1
                else:
                    unmatched += 1

            processed += len(batch)

            if progress_callback:
                progress_callback(1.0, processed, total_rows)

        # 轉換為 bytes
        output_bytes = io.BytesIO(output.getvalue().encode('utf-8-sig'))

        stats = {
            'total': total_rows,
            'matched': matched,
            'unmatched': unmatched,
            'match_rate': matched / total_rows * 100 if total_rows > 0 else 0
        }

        return output_bytes, stats


def detect_hash_column(fieldnames: list[str]) -> tuple[Optional[str], str]:
    """
    自動偵測 hash 欄位

    Returns:
        (欄位名稱, hash_type) 或 (None, '')
    """
    phone_keywords = ['phone', 'mobile', '手機', '電話', 'hashedphone']
    email_keywords = ['email', 'mail', '信箱', 'hashedemail']

    fieldnames_lower = [f.lower() for f in fieldnames]

    # 優先找 phone
    for i, fname in enumerate(fieldnames_lower):
        for kw in phone_keywords:
            if kw in fname:
                return fieldnames[i], 'phone'

    # 再找 email
    for i, fname in enumerate(fieldnames_lower):
        for kw in email_keywords:
            if kw in fname:
                return fieldnames[i], 'email'

    # 都找不到，檢查是否只有一個欄位（可能是純 hash 列表）
    if len(fieldnames) == 1:
        return fieldnames[0], 'phone'  # 預設為 phone

    return None, ''
