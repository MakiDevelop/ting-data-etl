#!/usr/bin/env python3
"""XGB 原始檔轉換工具

產出三個檔案：
1. 中繼1_匯進名單管理.csv — MemberCode 後 10 碼
2. 中繼2_投廣名單.csv — 清洗後 ph (E.164 無+) + 驗證後 em
3. 中繼2_投廣名單_SHA256.csv — 中繼2 的 ph/em 各自 SHA256
"""

import csv
import hashlib
import re
import sys
from pathlib import Path

# ---------- config ----------
INPUT_FILE = Path.home() / "Downloads" / "XGB 原始檔.csv"
OUTPUT_DIR = Path(__file__).parent / "xgb-output"

EMAIL_RE = re.compile(
    r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
)


def clean_phone(raw: str) -> str | None:
    """清洗電話號碼為 E.164（無 + 號）格式，回傳 None 表示無效。"""
    s = raw.strip()
    s = s.replace("+", "").replace("-", "").replace(" ", "")
    # 去掉國碼後的前導 0：886 09xx -> 886 9xx
    if s.startswith("886") and len(s) > 3 and s[3] == "0":
        s = s[:3] + s[4:]
    if len(s) == 12 and s.startswith("886") and s[3] == "9":
        return s
    return None


def validate_email(raw: str) -> str:
    """驗證 email，無效則回傳空字串。"""
    s = raw.strip()
    if EMAIL_RE.match(s):
        return s
    return ""


def sha256(value: str) -> str:
    if not value:
        return ""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def main():
    if not INPUT_FILE.exists():
        print(f"找不到來源檔案：{INPUT_FILE}", file=sys.stderr)
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    out1_path = OUTPUT_DIR / "中繼1_匯進名單管理.csv"
    out2_path = OUTPUT_DIR / "中繼2_投廣名單.csv"
    out3_path = OUTPUT_DIR / "中繼2_投廣名單_SHA256.csv"

    stats = {
        "total": 0,
        "phone_invalid": 0,
        "email_invalid": 0,
    }

    with (
        open(INPUT_FILE, newline="", encoding="utf-8") as fin,
        open(out1_path, "w", newline="", encoding="utf-8") as f1,
        open(out2_path, "w", newline="", encoding="utf-8") as f2,
        open(out3_path, "w", newline="", encoding="utf-8") as f3,
    ):
        reader = csv.DictReader(fin)
        w1 = csv.writer(f1)
        w2 = csv.writer(f2)
        w3 = csv.writer(f3)

        for row in reader:
            stats["total"] += 1

            # --- 中繼1：MemberCode 後 10 碼 ---
            member_code = row["MemberCode"].strip()
            tail = member_code.split("-", 1)[-1] if "-" in member_code else member_code
            tail = tail[-10:]
            w1.writerow([tail])

            # --- 中繼2：清洗 ph + 驗證 em ---
            phone = clean_phone(row["ph"])
            if phone is None:
                stats["phone_invalid"] += 1
                phone = ""
            email = validate_email(row["em"])
            if not email:
                stats["email_invalid"] += 1
            w2.writerow([phone, email])

            # --- 中繼2 SHA256 ---
            w3.writerow([sha256(phone), sha256(email)])

    print(f"處理完成 — 共 {stats['total']:,} 筆")
    print(f"  電話無效：{stats['phone_invalid']:,} 筆")
    print(f"  Email 無效：{stats['email_invalid']:,} 筆")
    print(f"\n產出目錄：{OUTPUT_DIR}")
    print(f"  {out1_path.name}")
    print(f"  {out2_path.name}")
    print(f"  {out3_path.name}")


if __name__ == "__main__":
    main()
