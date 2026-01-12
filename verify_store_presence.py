import pandas as pd
from pathlib import Path
import argparse

AGG_DIR = Path("input/aggregate")

FILES = {
    "23-1 / 23-2 / 24-1 / 24-2（區間推薦人綁定）": {
        "file": "區間綁定推薦人人數.csv",
        "store_col": "商店序號",
    },
    "23-1 / 24-1（累計推薦人綁定）": {
        "file": "累計至今綁定推薦人人數.csv",
        "store_col": "商店序號",
    },
    "23-1 / 24-1（會員總數）": {
        "file": "14-1.會員成長趨勢_新增註冊會員數卡片.csv",
        "store_col": "商店序號",
    },
    "24-2（門市首購人數－月份）": {
        "file": "門市首購人數_月份.csv",
        "store_col": "商店序號",
    },
    "25-1 / 25-2（門市首購人數－門市）": {
        "file": "門市首購人數_門市.csv",
        "store_col": "商店序號",
    },
    "25-1 / 25-2（各門市累計綁定）": {
        "file": "各門市累計綁定人數.csv",
        "store_col": "商店序號",
    },
}


def check_store(store_id: str):
    print(f"\n🔍 檢查商店序號：{store_id}\n")

    for desc, cfg in FILES.items():
        path = AGG_DIR / cfg["file"]

        if not path.exists():
            print(f"❌ {desc}：找不到檔案 {cfg['file']}")
            continue

        try:
            df = pd.read_csv(path, dtype=str)
        except Exception as e:
            print(f"❌ {desc}：讀取失敗 ({e})")
            continue

        col = cfg["store_col"]
        if col not in df.columns:
            print(f"⚠️  {desc}：找不到欄位 {col}")
            continue

        count = df[df[col].astype(str) == store_id].shape[0]

        if count > 0:
            print(f"✅ {desc}：有資料（{count} 列）")
        else:
            print(f"⛔ {desc}：沒有任何資料")

    print("\n--- 檢查完成 ---\n")


def main():
    parser = argparse.ArgumentParser(description="Verify store presence in raw datasets")
    parser.add_argument("--store", required=True, help="商店序號，例如 1194")
    args = parser.parse_args()

    check_store(str(args.store))


if __name__ == "__main__":
    main()
    
# python verify_store_presence.py --store 1194
# python verify_store_presence.py --store 40316