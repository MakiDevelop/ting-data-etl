import argparse
import pandas as pd
from pathlib import Path

# ===============================
# Global Paths
# ===============================
INPUT_DIR = Path("./input")
AGGREGATE_INPUT_DIR = INPUT_DIR / "aggregate"
OUTPUT_DIR = Path("./output")

# ===============================
# Configs: 六個需求（以編號作為 key）
# ===============================
CONFIGS = {
    # 23-1：指定月份業績加總 (final, fixed-year logic and outputs)
    "23-1": {
        "input_subdir": "aggregate",
        "year_current": "2025",
        "year_prev": "2024",
        "months": ["01","02","03","04","05","06","07","08","09","10","11","12"],

        "sources": {
            "interval_bind": {
                "file": "區間綁定推薦人人數.csv",
                "store_col": "商店序號",
                "year_col": "年度",
                "month_col": "月份",
                "value_col": "總綁定",
            },
            "cumulative_bind": {
                "file": "累計至今綁定推薦人人數.csv",
                "store_col": "商店序號",
                "value_col": "累計至今推薦人綁定人數",
            },
            "member_total": {
                "file": "14-1.會員成長趨勢_新增註冊會員數卡片.csv",
                "store_col": "商店序號",
                "value_col": "總會員數",
            },
        },
    },

    # 23-2：全年拜訪次數加總
    "23-2": {
        "input_subdir": "aggregate",
        "input_file": "門市首購人數_門市.csv",
        "store_col": "store_id",
        "month_col": "yyyymm",
        "target_col": "visit_count",
        "months": [
            "202401","202402","202403","202404","202405","202406",
            "202407","202408","202409","202410","202411","202412"
        ],
    },

    # 24-1：指定區間首購人數加總
    "24-1": {
        "input_subdir": "aggregate",
        "input_file": "14-1.會員成長趨勢_新增註冊會員數卡片.csv",
        "store_col": "store_id",
        "month_col": "yyyymm",
        "target_col": "first_purchase_cnt",
        "months": ["202405", "202406", "202407"],
    },

    # 24-2：推薦人綁定數加總
    "24-2": {
        "input_subdir": "aggregate",
        "input_file": "區間綁定推薦人人數.csv",
        "store_col": "store_id",
        "month_col": "yyyymm",
        "target_col": "referral_bind_cnt",
        "months": ["202401", "202402", "202403", "202404"],
    },

    # 25-1：年度成交金額加總
    "25-1": {
        "input_subdir": "aggregate",
        "input_file": "累計至今綁定推薦人人數.csv",
        "store_col": "store_id",
        "month_col": "yyyymm",
        "target_col": "gmv",
        "months": [
            "202501","202502","202503","202504","202505","202506",
            "202507","202508","202509","202510","202511","202512"
        ],
    },

    # 25-2：指定月份成交筆數加總
    "25-2": {
        "input_subdir": "aggregate",
        "input_file": "各門市累計綁定人數.csv",
        "store_col": "store_id",
        "month_col": "yyyymm",
        "target_col": "order_cnt",
        "months": ["202507"],
    },
}


def run_aggregation(config_key: str):
    if config_key not in CONFIGS:
        raise ValueError(f"Config '{config_key}' not found")

    cfg = CONFIGS[config_key]

    # ===== 23-1 KPI card (fixed full-year logic) =====
    if config_key == "23-1":
        base_dir = INPUT_DIR / cfg["input_subdir"]

        def _to_number(series: pd.Series) -> pd.Series:
            # Normalize common human-formatted numbers like "12,345", " 123 ", "1,234.0", "45%"
            s = series.astype(str).str.strip()
            s = s.str.replace(",", "", regex=False)
            s = s.str.replace("%", "", regex=False)
            # Treat empty or "nan" as NaN for proper fillna downstream
            s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
            return pd.to_numeric(s, errors="coerce")

        # --- B：區間推薦人綁定人數（今年） ---
        src = cfg["sources"]["interval_bind"]
        df = pd.read_csv(base_dir / src["file"], dtype=str)
        df[src["store_col"]] = df[src["store_col"]].astype(str).str.strip()
        # Normalize month to int 1–12 to avoid zero-padding mismatch
        df[src["month_col"]] = (
            df[src["month_col"]]
            .astype(str)
            .str.strip()
            .str.replace(r"[^0-9]", "", regex=True)
        )
        df[src["month_col"]] = pd.to_numeric(df[src["month_col"]], errors="coerce")
        df = df[df[src["month_col"]].between(1, 12)]

        df[src["value_col"]] = _to_number(df[src["value_col"]]).fillna(0)

        df_cur = df[
            (df[src["year_col"]] == cfg["year_current"]) &
            (df[src["month_col"]].isin(range(1, 13)))
        ]

        df_prev = df[
            (df[src["year_col"]] == cfg["year_prev"]) &
            (df[src["month_col"]].isin(range(1, 13)))
        ]

        cur_sum = (
            df_cur.groupby(src["store_col"], as_index=False)[src["value_col"]]
            .sum()
            .rename(columns={
                src["store_col"]: "商店序號",
                src["value_col"]: "區間推薦人綁定人數"
            })
        )
        prev_sum = (
            df_prev.groupby(src["store_col"], as_index=False)[src["value_col"]]
            .sum()
            .rename(columns={
                src["store_col"]: "商店序號",
                src["value_col"]: "prev_value"
            })
        )
        cur_sum["商店序號"] = cur_sum["商店序號"].astype(str).str.strip()
        prev_sum["商店序號"] = prev_sum["商店序號"].astype(str).str.strip()

        result = cur_sum.merge(prev_sum, on="商店序號", how="left")

        result["區間推薦人綁定人數 YoY"] = (
            (result["區間推薦人綁定人數"] - result["prev_value"]) / result["prev_value"]
        ).where(result["prev_value"] != 0)

        result.drop(columns=["prev_value"], inplace=True)

        # --- D：推薦人綁定率 ---
        src_cum = cfg["sources"]["cumulative_bind"]
        df_cum = pd.read_csv(base_dir / src_cum["file"], dtype=str)
        df_cum[src_cum["store_col"]] = df_cum[src_cum["store_col"]].astype(str).str.strip()
        df_cum[src_cum["value_col"]] = _to_number(df_cum[src_cum["value_col"]]).fillna(0)

        src_mem = cfg["sources"]["member_total"]
        df_mem = pd.read_csv(base_dir / src_mem["file"], dtype=str)
        df_mem[src_mem["store_col"]] = df_mem[src_mem["store_col"]].astype(str).str.strip()
        df_mem[src_mem["value_col"]] = _to_number(df_mem[src_mem["value_col"]]).fillna(0)

        result = (
            result
            .merge(
                df_cum[[src_cum["store_col"], src_cum["value_col"]]]
                .rename(columns={src_cum["store_col"]: "商店序號"}),
                on="商店序號",
                how="left"
            )
            .merge(
                df_mem[[src_mem["store_col"], src_mem["value_col"]]]
                .rename(columns={src_mem["store_col"]: "商店序號"}),
                on="商店序號",
                how="left"
            )
        )

        result[src_cum["value_col"]] = result[src_cum["value_col"]].fillna(0)
        result[src_mem["value_col"]] = result[src_mem["value_col"]].fillna(0)

        result["推薦人綁定率"] = (
            result[src_cum["value_col"]] / result[src_mem["value_col"]]
        ).where(result[src_mem["value_col"]] != 0)

        # --- Format percentage fields ---
        def _fmt_pct(x):
            if pd.isna(x):
                return ""
            return f"{x * 100:.2f}%"

        result["區間推薦人綁定人數 YoY"] = result["區間推薦人綁定人數 YoY"].apply(_fmt_pct)
        result["推薦人綁定率"] = result["推薦人綁定率"].apply(_fmt_pct)

        result = result[[
            "商店序號",
            "區間推薦人綁定人數",
            "區間推薦人綁定人數 YoY",
            "推薦人綁定率",
        ]]

        # --- Output ---
        for _, row in result.iterrows():
            store_id = row["商店序號"]
            store_dir = OUTPUT_DIR / str(store_id)
            store_dir.mkdir(parents=True, exist_ok=True)

            output_path = store_dir / "23-1.csv"
            pd.DataFrame([row]).to_csv(
                output_path, index=False, encoding="utf-8-sig"
            )

        print(f"[OK] config=23-1, stores={len(result)}")
        return

    # ===== 23-2 Monthly YoY table =====
    if config_key == "23-2":
        base_dir = INPUT_DIR / cfg["input_subdir"]

        def _to_number(series: pd.Series) -> pd.Series:
            s = series.astype(str).str.strip()
            s = s.str.replace(",", "", regex=False)
            s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
            return pd.to_numeric(s, errors="coerce")

        def _fmt_pct(x):
            if pd.isna(x):
                return ""
            return f"{x * 100:.2f}%"

        src_file = "區間綁定推薦人人數.csv"
        store_col = "商店序號"
        year_col = "年度"
        month_col = "月份"
        value_col = "總綁定"

        df = pd.read_csv(base_dir / src_file, dtype=str)
        df[store_col] = df[store_col].astype(str).str.strip()
        df[month_col] = _to_number(df[month_col]).astype("Int64")
        df[value_col] = _to_number(df[value_col]).fillna(0)

        # Only years we care about
        df = df[df[year_col].isin(["2024", "2025"])]

        # Aggregate per store, month, year
        agg = (
            df.groupby([store_col, month_col, year_col], as_index=False)[value_col]
            .sum()
        )

        # Pivot to get 2024 / 2025 columns
        pivot = (
            agg.pivot_table(
                index=[store_col, month_col],
                columns=year_col,
                values=value_col,
                aggfunc="sum",
                fill_value=0,
            )
            .reset_index()
        )

        # Ensure all months 1–12 exist per store
        stores = pivot[store_col].unique()
        full = []
        for sid in stores:
            tmp = pivot[pivot[store_col] == sid].set_index(month_col)
            tmp = tmp.reindex(range(1, 13), fill_value=0).reset_index()
            tmp[store_col] = sid
            full.append(tmp)
        result = pd.concat(full, ignore_index=True)

        # Rename year columns
        result = result.rename(columns={
            "2024": "2024年",
            "2025": "2025年",
            month_col: "月份"
        })

        # YoY calculation
        result["推薦人新綁定數 YoY"] = (
            (result["2025年"] - result["2024年"]) / result["2024年"]
        ).where(result["2024年"] != 0)

        result["推薦人新綁定數 YoY"] = result["推薦人新綁定數 YoY"].apply(_fmt_pct)

        # Output per store
        for sid, g in result.groupby(store_col):
            out = g[[store_col, "月份", "2024年", "2025年", "推薦人新綁定數 YoY"]].sort_values("月份")
            store_dir = OUTPUT_DIR / str(sid)
            store_dir.mkdir(parents=True, exist_ok=True)
            out.to_csv(store_dir / "23-2.csv", index=False, encoding="utf-8-sig")

        print(f"[OK] config=23-2, stores={result[store_col].nunique()}")
        return

    # ===== 24-1 KPI card (referral performance summary) =====
    if config_key == "24-1":
        base_dir = INPUT_DIR / cfg["input_subdir"]

        def _to_number(series: pd.Series) -> pd.Series:
            s = series.astype(str).str.strip()
            s = s.str.replace(",", "", regex=False)
            s = s.str.replace("%", "", regex=False)
            s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
            return pd.to_numeric(s, errors="coerce")

        def _fmt_pct(x):
            if pd.isna(x):
                return ""
            return f"{x * 100:.2f}%"

        # --- Interval sum & YoY ---
        src_file = "區間綁定推薦人人數.csv"
        store_col = "商店序號"
        year_col = "年度"
        month_col = "月份"
        value_col = "總綁定"

        df = pd.read_csv(base_dir / src_file, dtype=str)
        df[store_col] = df[store_col].astype(str).str.strip()
        df[value_col] = _to_number(df[value_col]).fillna(0)

        df_2025 = df[df[year_col] == "2025"]
        df_2024 = df[df[year_col] == "2024"]

        sum_2025 = (
            df_2025.groupby(store_col, as_index=False)[value_col]
            .sum()
            .rename(columns={value_col: "區間推薦人綁定人數"})
        )

        sum_2024 = (
            df_2024.groupby(store_col, as_index=False)[value_col]
            .sum()
            .rename(columns={value_col: "prev_value"})
        )

        result = sum_2025.merge(sum_2024, on=store_col, how="left")

        result["區間推薦人綁定人數 YoY"] = (
            (result["區間推薦人綁定人數"] - result["prev_value"]) / result["prev_value"]
        ).where(result["prev_value"] != 0)

        # --- Referral rate ---
        df_cum = pd.read_csv(base_dir / "累計至今綁定推薦人人數.csv", dtype=str)
        df_cum[store_col] = df_cum[store_col].astype(str).str.strip()
        df_cum["累計至今推薦人綁定人數"] = _to_number(
            df_cum["累計至今推薦人綁定人數"]
        ).fillna(0)

        df_mem = pd.read_csv(base_dir / "14-1.會員成長趨勢_新增註冊會員數卡片.csv", dtype=str)
        df_mem[store_col] = df_mem[store_col].astype(str).str.strip()
        df_mem["總會員數"] = _to_number(df_mem["總會員數"]).fillna(0)

        result = (
            result
            .merge(
                df_cum[[store_col, "累計至今推薦人綁定人數"]],
                on=store_col,
                how="left",
            )
            .merge(
                df_mem[[store_col, "總會員數"]],
                on=store_col,
                how="left",
            )
        )

        result["推薦人綁定率"] = (
            result["累計至今推薦人綁定人數"] / result["總會員數"]
        ).where(result["總會員數"] != 0)

        # --- Format outputs ---
        result["區間推薦人綁定人數 YoY"] = result["區間推薦人綁定人數 YoY"].apply(_fmt_pct)
        result["推薦人綁定率"] = result["推薦人綁定率"].apply(_fmt_pct)

        result = result[[
            store_col,
            "推薦人綁定率",
            "區間推薦人綁定人數",
            "區間推薦人綁定人數 YoY",
        ]]

        # --- Output ---
        for _, row in result.iterrows():
            sid = row[store_col]
            store_dir = OUTPUT_DIR / str(sid)
            store_dir.mkdir(parents=True, exist_ok=True)
            row.to_frame().T.to_csv(
                store_dir / "24-1.csv",
                index=False,
                encoding="utf-8-sig",
            )

        print(f"[OK] config=24-1, stores={len(result)}")
        return

    # ===== 24-2 Monthly referral conversion rate =====
    if config_key == "24-2":
        base_dir = INPUT_DIR / "aggregate"

        def _to_number(series: pd.Series) -> pd.Series:
            s = series.astype(str).str.strip()
            s = s.str.replace(",", "", regex=False)
            s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
            return pd.to_numeric(s, errors="coerce")

        def _fmt_pct(x):
            if pd.isna(x):
                return ""
            return f"{x * 100:.2f}%"

        def _parse_month(series: pd.Series) -> pd.Series:
            # Accept formats like "01", "1", "202501", "2025-01", "2025/01", "2025-1"
            s = series.astype(str).str.strip()
            # Keep digits only
            digits = s.str.replace(r"[^0-9]", "", regex=True)
            # If YYYYMM (len>=6), take last 2 digits; otherwise take the full digits
            m = digits.where(digits.str.len() < 6, digits.str[-2:])
            # Convert to int and keep only 1..12
            m_num = pd.to_numeric(m, errors="coerce")
            m_num = m_num.where(m_num.between(1, 12))
            return m_num.astype("Int64")

        store_col = "商店序號"
        month_col = "月份"

        # Helper to ensure/rename month column
        def _ensure_month_col(df: pd.DataFrame, preferred: str = "月份") -> pd.DataFrame:
            if preferred in df.columns:
                return df
            # common variants
            candidates = ["月", "月份(數字)", "month", "Month", "MONTH", "Established At Month"]
            for c in candidates:
                if c in df.columns:
                    return df.rename(columns={c: preferred})
            # try case-insensitive match
            lowered = {str(c).lower(): c for c in df.columns}
            if preferred.lower() in lowered:
                return df.rename(columns={lowered[preferred.lower()]: preferred})
            raise KeyError(f"Cannot find month column. Available columns: {list(df.columns) }")

        # --- 門市首購人數 ---
        df_fp = pd.read_csv(base_dir / "門市首購人數_月份.csv", dtype=str)
        df_fp.columns = df_fp.columns.astype(str).str.strip()
        df_fp = _ensure_month_col(df_fp, month_col)
        df_fp[store_col] = df_fp[store_col].astype(str).str.strip()
        df_fp[month_col] = _parse_month(df_fp[month_col])
        df_fp = df_fp[df_fp[month_col].notna()]
        df_fp["門市首購人數"] = _to_number(df_fp["門市首購人數"]).fillna(0)

        # --- 推薦人綁定數（2025） ---
        df_ref = pd.read_csv(base_dir / "區間綁定推薦人人數.csv", dtype=str)
        df_ref.columns = df_ref.columns.astype(str).str.strip()
        df_ref = _ensure_month_col(df_ref, month_col)
        df_ref[store_col] = df_ref[store_col].astype(str).str.strip()
        df_ref[month_col] = _parse_month(df_ref[month_col])
        df_ref = df_ref[df_ref[month_col].notna()]
        df_ref["總綁定"] = _to_number(df_ref["總綁定"]).fillna(0)
        df_ref = df_ref[df_ref["年度"] == "2025"]

        ref_monthly = (
            df_ref.groupby([store_col, month_col], as_index=False)["總綁定"]
            .sum()
            .rename(columns={"總綁定": "推薦人綁定數"})
        )

        # --- Merge ---
        result = df_fp.merge(ref_monthly, on=[store_col, month_col], how="left")
        result["推薦人綁定數"] = result["推薦人綁定數"].fillna(0)

        # --- 推薦人綁定率 ---
        result["推薦人綁定率"] = (
            result["推薦人綁定數"] / result["門市首購人數"]
        ).where(result["門市首購人數"] != 0)

        result["推薦人綁定率"] = result["推薦人綁定率"].apply(_fmt_pct)

        # --- Output per store ---
        for sid, g in result.groupby(store_col):
            out = g[[store_col, month_col, "門市首購人數", "推薦人綁定數", "推薦人綁定率"]].sort_values(month_col)
            store_dir = OUTPUT_DIR / str(sid)
            store_dir.mkdir(parents=True, exist_ok=True)
            out.to_csv(store_dir / "24-2.csv", index=False, encoding="utf-8-sig")

        print(f"[OK] config=24-2, stores={result[store_col].nunique()}")
        return

    # ===== 25-1 Store structure (Top 5 by referral ratio) =====
    if config_key == "25-1":
        base_dir = INPUT_DIR / "aggregate"

        def _to_number(series: pd.Series) -> pd.Series:
            s = series.astype(str).str.strip()
            s = s.str.replace(",", "", regex=False)
            s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
            return pd.to_numeric(s, errors="coerce")

        def _fmt_pct_2(x):
            if pd.isna(x):
                return ""
            return f"{x * 100:.2f}%"

        store_col = "商店序號"
        store_name_col = "門市名稱"

        def _ensure_store_name_col(df: pd.DataFrame, preferred: str = "門市名稱") -> pd.DataFrame:
            if preferred in df.columns:
                return df
            # common variants observed in files
            candidates = [
                "門市",
                "門市名稱 ",
                "Store Name",
                "store_name",
                "Name",
                "門市名稱(中文)",
                "門市名稱（中文）",
            ]
            for c in candidates:
                if c in df.columns:
                    return df.rename(columns={c: preferred})
            # case-insensitive fallback
            lowered = {str(c).lower(): c for c in df.columns}
            if preferred.lower() in lowered:
                return df.rename(columns={lowered[preferred.lower()]: preferred})
            raise KeyError(f"Cannot find store name column. Available columns: {list(df.columns)}")

        # --- 門市首購人數 ---
        df_fp = pd.read_csv(base_dir / "門市首購人數_門市.csv", dtype=str)
        df_fp.columns = df_fp.columns.astype(str).str.strip()
        df_fp = _ensure_store_name_col(df_fp, store_name_col)
        df_fp[store_col] = df_fp[store_col].astype(str).str.strip()
        df_fp[store_name_col] = df_fp[store_name_col].astype(str).str.strip()
        df_fp = df_fp[df_fp[store_name_col].notna()]
        df_fp["門市首購人數"] = _to_number(df_fp["門市首購人數"]).fillna(0)

        # --- 推薦人綁定人數（2025, per store name） ---
        df_ref = pd.read_csv(base_dir / "各門市累計綁定人數.csv", dtype=str)
        df_ref.columns = df_ref.columns.astype(str).str.strip()
        df_ref = _ensure_store_name_col(df_ref, store_name_col)
        df_ref[store_col] = df_ref[store_col].astype(str).str.strip()
        df_ref[store_name_col] = df_ref[store_name_col].astype(str).str.strip()
        df_ref = df_ref[df_ref[store_name_col].notna()]
        df_ref = df_ref[df_ref["年度"] == "2025"]
        df_ref["總綁定數"] = _to_number(df_ref["總綁定數"]).fillna(0)

        ref_sum = (
            df_ref
            .groupby([store_col, store_name_col], as_index=False)["總綁定數"]
            .sum()
            .rename(columns={"總綁定數": "推薦人綁定人數"})
        )

        # --- Merge ---
        result = df_fp.merge(
            ref_sum,
            on=[store_col, store_name_col],
            how="left"
        )
        result["推薦人綁定人數"] = result["推薦人綁定人數"].fillna(0)

        # --- 佔比 ---
        result["佔比"] = (
            result["推薦人綁定人數"] / result["門市首購人數"]
        ).where(result["門市首購人數"] != 0)

        # --- Output per store (Top 5 per 商店序號) ---
        store_count = 0
        for sid, g in result.groupby(store_col):
            g = g.sort_values("佔比", ascending=False).head(5)
            g["佔比"] = g["佔比"].apply(_fmt_pct_2)

            out = g[[
                store_col,
                store_name_col,
                "門市首購人數",
                "推薦人綁定人數",
                "佔比",
            ]]

            store_dir = OUTPUT_DIR / str(sid)
            store_dir.mkdir(parents=True, exist_ok=True)
            out.to_csv(store_dir / "25-1.csv", index=False, encoding="utf-8-sig")
            store_count += 1

        print(f"[OK] config=25-1, stores={store_count}")
        return

    # ===== 25-2 Store structure (Bottom 5 by referral ratio) =====
    if config_key == "25-2":
        base_dir = INPUT_DIR / "aggregate"

        def _to_number(series: pd.Series) -> pd.Series:
            s = series.astype(str).str.strip()
            s = s.str.replace(",", "", regex=False)
            s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
            return pd.to_numeric(s, errors="coerce")

        def _fmt_pct_2(x):
            if pd.isna(x):
                return ""
            return f"{x * 100:.2f}%"

        store_col = "商店序號"
        store_name_col = "門市名稱"

        def _ensure_store_name_col(df: pd.DataFrame, preferred: str = "門市名稱") -> pd.DataFrame:
            if preferred in df.columns:
                return df
            candidates = [
                "門市",
                "Store Name",
                "store_name",
                "Name",
            ]
            for c in candidates:
                if c in df.columns:
                    return df.rename(columns={c: preferred})
            lowered = {str(c).lower(): c for c in df.columns}
            if preferred.lower() in lowered:
                return df.rename(columns={lowered[preferred.lower()]: preferred})
            raise KeyError(f"Cannot find store name column. Available columns: {list(df.columns)}")

        # --- 門市首購人數 ---
        df_fp = pd.read_csv(base_dir / "門市首購人數_門市.csv", dtype=str)
        df_fp.columns = df_fp.columns.astype(str).str.strip()
        df_fp = _ensure_store_name_col(df_fp, store_name_col)
        df_fp[store_col] = df_fp[store_col].astype(str).str.strip()
        df_fp[store_name_col] = (
            df_fp[store_name_col]
            .astype(str)
            .str.strip()
            .replace(
                ["", "nan", "NaN", "NULL", "None"],
                pd.NA
            )
        )
        df_fp = df_fp[df_fp[store_name_col].notna()]
        df_fp["門市首購人數"] = _to_number(df_fp["門市首購人數"]).fillna(0)

        # --- 推薦人綁定人數（2025） ---
        df_ref = pd.read_csv(base_dir / "各門市累計綁定人數.csv", dtype=str)
        df_ref.columns = df_ref.columns.astype(str).str.strip()
        df_ref = _ensure_store_name_col(df_ref, store_name_col)
        df_ref[store_col] = df_ref[store_col].astype(str).str.strip()
        df_ref[store_name_col] = (
            df_ref[store_name_col]
            .astype(str)
            .str.strip()
            .replace(
                ["", "nan", "NaN", "NULL", "None"],
                pd.NA
            )
        )
        df_ref = df_ref[df_ref[store_name_col].notna()]
        df_ref = df_ref[df_ref["年度"] == "2025"]
        df_ref["總綁定數"] = _to_number(df_ref["總綁定數"]).fillna(0)

        ref_sum = (
            df_ref
            .groupby([store_col, store_name_col], as_index=False)["總綁定數"]
            .sum()
            .rename(columns={"總綁定數": "推薦人綁定人數"})
        )

        # --- Merge ---
        result = df_fp.merge(
            ref_sum,
            on=[store_col, store_name_col],
            how="left"
        )
        result["推薦人綁定人數"] = result["推薦人綁定人數"].fillna(0)

        # --- 佔比 ---
        result["佔比"] = (
            result["推薦人綁定人數"] / result["門市首購人數"]
        ).where(result["門市首購人數"] != 0)

        # --- Output per store (Bottom 5 per 商店序號) ---
        store_count = 0
        for sid, g in result.groupby(store_col):
            g = g.sort_values("佔比", ascending=True).head(5)
            g["佔比"] = g["佔比"].apply(_fmt_pct_2)

            out = g[[
                store_col,
                store_name_col,
                "門市首購人數",
                "推薦人綁定人數",
                "佔比",
            ]]

            store_dir = OUTPUT_DIR / str(sid)
            store_dir.mkdir(parents=True, exist_ok=True)
            out.to_csv(store_dir / "25-2.csv", index=False, encoding="utf-8-sig")
            store_count += 1

        print(f"[OK] config=25-2, stores={store_count}")
        return

    input_base = INPUT_DIR
    if cfg.get("input_subdir"):
        input_base = INPUT_DIR / cfg["input_subdir"]

    input_path = input_base / cfg["input_file"]
    if not input_path.exists():
        raise FileNotFoundError(input_path)

    df = pd.read_csv(input_path, dtype=str)

    # 數值欄位轉型
    df[cfg["target_col"]] = (
        pd.to_numeric(df[cfg["target_col"]], errors="coerce")
        .fillna(0)
    )

    # 篩選月份
    df = df[df[cfg["month_col"]].isin(cfg["months"])]

    # 依商店彙總
    result = (
        df.groupby(cfg["store_col"], as_index=False)[cfg["target_col"]]
        .sum()
        .rename(columns={cfg["target_col"]: "total"})
    )

    # 依商店序號輸出檔案
    for _, row in result.iterrows():
        store_id = row[cfg["store_col"]]
        store_dir = OUTPUT_DIR / str(store_id)
        store_dir.mkdir(parents=True, exist_ok=True)

        output_path = store_dir / cfg["input_file"]
        pd.DataFrame([row]).to_csv(
            output_path, index=False, encoding="utf-8-sig"
        )

    print(f"[OK] config={config_key}, stores={len(result)}")


def main():
    parser = argparse.ArgumentParser(description="Aggregate by store with config key")
    parser.add_argument("--config", required=True, help="config key, e.g. 23-1")
    args = parser.parse_args()

    run_aggregation(args.config)


if __name__ == "__main__":
    main()
