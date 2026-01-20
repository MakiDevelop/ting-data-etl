import argparse
import pandas as pd
from pathlib import Path
import sys


# ===============================
# Global Paths
# ===============================
INPUT_DIR = Path("./input")
AGGREGATE_INPUT_DIR = INPUT_DIR / "aggregate"
# Default output directory
OUTPUT_DIR = Path("./output")



# ===============================
# Store Helper Functions
# ===============================
def get_all_store_ids(output_dir: Path):
    """
    Define the store universe based on existing output directories.
    """
    return sorted(
        d.name for d in output_dir.iterdir()
        if d.is_dir()
    )


# ===============================
# Helper: Read per-store input CSV from output/{store_id}/{filename}
# ===============================
def read_store_input_csv(
    output_dir: Path,
    store_id: str,
    filename: str,
) -> pd.DataFrame | None:
    """
    Read per-store CSV from output/{store_id}/{filename}.
    Return None if file does not exist.
    """
    path = output_dir / str(store_id) / filename
    if not path.exists():
        return None
    return pd.read_csv(path, dtype=str)


import re

def write_store_csv_with_fill(store_id, output_dir, filename, df, columns):
    """
    Write per-store CSV with header fill.
    FINAL safeguard: remove pandas duplicate column suffixes like '.1', '.2', ...
    """
    store_dir = output_dir / str(store_id)
    store_dir.mkdir(parents=True, exist_ok=True)

    output_path = store_dir / filename

    def _normalize_headers(cols):
        normalized = []
        seen = {}

        for c in cols:
            c = str(c)
            base = re.sub(r"\.\d+$", "", c)

            # 第一次出現：直接使用 base
            if base not in seen:
                seen[base] = 1
                normalized.append(base)
            else:
                # 已出現過：仍保留欄位數量，但強制回到 base
                # （避免 pandas Length mismatch，也避免 .1 外洩）
                normalized.append(base)

        return normalized

    # ===== FINAL HEADER NORMALIZATION (出口層，唯一保證點) =====
    if df is not None:
        df = df.copy()
        df.columns = _normalize_headers(list(df.columns))

    if columns is not None:
        columns = _normalize_headers(list(columns))

    if df is None or df.empty:
        empty_df = pd.DataFrame(columns=columns)
        empty_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    else:
        df.to_csv(output_path, index=False, encoding="utf-8-sig")

def add_group_ratio(
    df: pd.DataFrame,
    group_col: str,
    value_col: str,
    target_col: str,
    *,
    fmt: str | None = None,
) -> pd.DataFrame:
    """
    Compute ratio per group within a single DataFrame.
    ratio = sum(value_col per group) / sum(value_col total)
    """
    if df is None or df.empty:
        df[target_col] = ""
        return df

    values = pd.to_numeric(df[value_col], errors="coerce").fillna(0)
    total = values.sum()

    if total == 0:
        df[target_col] = ""
        return df

    group_sum = (
        df.groupby(group_col)[value_col]
        .transform("sum")
        .astype(float)
    )

    ratio = group_sum / total

    if fmt == "percent":
        df[target_col] = ratio.apply(
            lambda x: "" if pd.isna(x) else f"{x * 100:.2f}%"
        )
    else:
        df[target_col] = ratio

    return df

# ===============================
# Helper: Add ratio column for a single DataFrame (for 7-1~9-2, 27)
# ===============================
def add_column_ratio(
    df: pd.DataFrame,
    source_col: str,
    target_col: str,
    *,
    fmt: str | None = None,
) -> pd.DataFrame:
    """
    Add a ratio column for a source column in a single DataFrame.
    ratio = value / total
    """
    if df is None or df.empty:
        df[target_col] = ""
        return df
    values = pd.to_numeric(df[source_col], errors="coerce").fillna(0)
    total = values.sum()
    if total == 0:
        df[target_col] = ""
        return df
    ratio = values / total
    if fmt == "percent":
        df[target_col] = ratio.apply(lambda x: "" if pd.isna(x) else f"{x * 100:.2f}%")
    elif fmt == "percent_int":
        df[target_col] = ratio.apply(lambda x: "" if pd.isna(x) else f"{x * 100:.0f}%")
    else:
        df[target_col] = ratio
    return df

# ===============================
# Configs: 六個需求（以編號作為 key）
# ===============================
CONFIGS = {
    "27": {
        "input_file": "27.OMO會員貢獻.csv",
        "output_file": "27.OMO會員貢獻.csv",
        "ratios": [
            {"source_col": "購買會員數", "target_col": "購買會員數.1", "fmt": "percent_int", "rename_to": "購買會員數"},
            {"source_col": "總業績", "target_col": "總業績.1", "fmt": "percent_int", "rename_to": "總業績"},
        ],
    },
    "4": {
        "input_subdir": "aggregate",
        "input_file": "4.跨裝置appvsweb概況.csv",
        "store_col": "商店序號",
        "value_col_amount": "訂單金額",
        "ratio_target_col_amount": "業績佔比",
        "value_col_session": "工作階段數",
        "ratio_target_col_session": "流量佔比",
        "output_file": "4.跨裝置appvsweb概況.csv",
    },
    # 7-1～9-2: 直欄佔比
    "7-1": {
        "input_file": "7-1.流量渠道總覽.csv",
        "output_file": "7-1.流量渠道總覽.csv",
        "ratios": [
            {"source_col": "工作階段數", "target_col": "工作階段數佔比"},
            {"source_col": "線上訂單金額", "target_col": "線上訂單金額佔比"},
            {"source_col": "線上註冊數", "target_col": "線上註冊數佔比"},
        ],
    },
    "7-2": {
        "input_file": "7-2.流量渠道總覽.csv",
        "output_file": "7-2.流量渠道總覽.csv",
        "ratios": [
            {"source_col": "工作階段數", "target_col": "工作階段數佔比"},
            {"source_col": "線上訂單金額", "target_col": "線上訂單金額佔比"},
            {"source_col": "門市訂單金額", "target_col": "門市訂單金額佔比"},
            {"source_col": "線上註冊數", "target_col": "線上註冊數佔比"},  # 覆蓋原有欄位（不帶空格）
        ],
    },
    "8-1": {
        "input_file": "8-1.廣告渠道分析.csv",
        "output_file": "8-1.廣告渠道分析.csv",
        "ratios": [
            {"source_col": "工作階段數", "target_col": "工作階段數佔比"},
            {"source_col": "線上訂單金額", "target_col": "線上訂單金額佔比"},
            {"source_col": "註冊會員數", "target_col": "線上註冊數佔比"},
        ],
    },
    "9-1": {
        "input_file": "9-1.自媒體渠道分析.csv",
        "output_file": "9-1.自媒體渠道分析.csv",
        "ratios": [
            {"source_col": "工作階段數", "target_col": "工作階段數佔比"},
            {"source_col": "線上訂單金額", "target_col": "線上訂單金額佔比"},
            {"source_col": "線上註冊會員數", "target_col": "線上註冊數佔比"},
        ],
    },
    "8-2": {
        "input_file": "8-2.廣告渠道分析.csv",
        "output_file": "8-2.廣告渠道分析.csv",
        "ratios": [
            {"source_col": "工作階段數", "target_col": "工作階段數佔比"},
            {"source_col": "線上訂單金額", "target_col": "線上訂單金額佔比"},
            {"source_col": "[指定區間] 門市訂單金額", "target_col": "門市訂單金額佔比"},
            {"source_col": "線上註冊會員數", "target_col": "線上註冊會員數佔比"},
        ],
    },
    "9-2": {
        "input_file": "9-2.自媒體渠道分析.csv",
        "output_file": "9-2.自媒體渠道分析.csv",
        "ratios": [
            {"source_col": "工作階段數", "target_col": "工作階段數佔比"},
            {"source_col": "線上訂單金額", "target_col": "線上訂單金額佔比"},
            {"source_col": "[指定區間] 門市訂單金額", "target_col": "門市訂單金額佔比"},
            {"source_col": "線上註冊會員數", "target_col": "線上註冊會員數佔比"},
        ],
    },
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


def run_aggregation(config_key: str, ting_test_mode: bool = False, output_dir_name: str = "output/ting-test"):
    if config_key not in CONFIGS:
        raise ValueError(f"Config '{config_key}' not found")

    cfg = CONFIGS[config_key]
    # ===== 4: 跨裝置 app vs web 概況（雙欄位直欄佔比）=====
    if config_key == "4":
        # 當 ting-test 模式啟用時，輸入從 output/{store_id}/ 讀取，輸出到指定目錄
        if ting_test_mode:
            input_dir = Path("./output")
            output_dir = Path(f"./{output_dir_name}")
        else:
            input_dir = OUTPUT_DIR
            output_dir = OUTPUT_DIR

        all_store_ids = get_all_store_ids(input_dir)

        expected_columns = None

        for sid in all_store_ids:
            g = read_store_input_csv(
                output_dir=input_dir,
                store_id=sid,
                filename=cfg["input_file"],
            )

            if g is not None and not g.empty:
                g = g.copy()
                g.columns = g.columns.astype(str).str.strip()

                # 訂單金額 → 業績佔比
                if cfg["value_col_amount"] in g.columns:
                    g[cfg["value_col_amount"]] = (
                        g[cfg["value_col_amount"]]
                        .astype(str)
                        .str.replace(",", "", regex=False)
                        .str.strip()
                    )
                    g = add_column_ratio(
                        g,
                        source_col=cfg["value_col_amount"],
                        target_col=cfg["ratio_target_col_amount"],
                        fmt="percent",
                    )

                # 工作階段數 → 流量佔比
                if cfg["value_col_session"] in g.columns:
                    g[cfg["value_col_session"]] = (
                        g[cfg["value_col_session"]]
                        .astype(str)
                        .str.replace(",", "", regex=False)
                        .str.strip()
                    )
                    g = add_column_ratio(
                        g,
                        source_col=cfg["value_col_session"],
                        target_col=cfg["ratio_target_col_session"],
                        fmt="percent",
                    )

                if expected_columns is None:
                    expected_columns = list(g.columns)

                out_df = g
            else:
                if expected_columns is None:
                    expected_columns = [
                        cfg["ratio_target_col_amount"],
                        cfg["ratio_target_col_session"],
                    ]
                out_df = pd.DataFrame(columns=expected_columns)

            # 寫入輸出檔案
            write_store_csv_with_fill(
                store_id=sid,
                output_dir=output_dir,
                filename=cfg["output_file"],
                df=out_df,
                columns=expected_columns,
            )

        print(f"[OK] config=4, stores={len(all_store_ids)}")
        return

    # ===== 7-1, 7-2, 8-1, 8-2, 9-1, 9-2, 27: 多欄位佔比 =====
    if config_key in ["7-1", "7-2", "8-1", "8-2", "9-1", "9-2", "27"]:
        if ting_test_mode:
            input_dir = Path("./output")
            output_dir = Path(f"./{output_dir_name}")
        else:
            input_dir = OUTPUT_DIR
            output_dir = OUTPUT_DIR

        all_store_ids = get_all_store_ids(input_dir)
        expected_columns = None

        for sid in all_store_ids:
            g = read_store_input_csv(
                output_dir=input_dir,
                store_id=sid,
                filename=cfg["input_file"],
            )

            if g is not None and not g.empty:
                g = g.copy()
                g.columns = g.columns.astype(str).str.strip()

                # 處理多個佔比欄位
                for ratio_cfg in cfg["ratios"]:
                    source_col = ratio_cfg["source_col"]
                    target_col = ratio_cfg["target_col"]

                    if source_col in g.columns:
                        # 清理數據（移除千分位、百分號等）
                        g[source_col] = (
                            g[source_col]
                            .astype(str)
                            .str.lstrip("'")
                            .str.replace(",", "", regex=False)
                            .str.replace("%", "", regex=False)
                            .str.strip()
                        )
                        # 計算佔比
                        fmt = ratio_cfg.get("fmt", "percent")  # 從配置中取得格式，默認為 "percent"
                        g = add_column_ratio(
                            g,
                            source_col=source_col,
                            target_col=target_col,
                            fmt=fmt,
                        )

                # 處理欄位重命名（for config 27）
                if config_key == "27":
                    rename_map = {}
                    for ratio_cfg in cfg["ratios"]:
                        if "rename_to" in ratio_cfg:
                            rename_map[ratio_cfg["target_col"]] = ratio_cfg["rename_to"]
                    if rename_map:
                        g = g.rename(columns=rename_map)

                if expected_columns is None:
                    expected_columns = list(g.columns)

                out_df = g
            else:
                if expected_columns is None:
                    # 對於 config 27，使用 rename_to 作為欄位名稱
                    if config_key == "27":
                        expected_columns = [r.get("rename_to", r["target_col"]) for r in cfg["ratios"]]
                    else:
                        expected_columns = [r["target_col"] for r in cfg["ratios"]]
                out_df = pd.DataFrame(columns=expected_columns)

            # 對於 config 27，直接用 pandas 寫入（允許重複欄位名稱）
            if config_key == "27":
                store_dir = output_dir / str(sid)
                store_dir.mkdir(parents=True, exist_ok=True)
                output_path = store_dir / cfg["output_file"]
                if out_df is None or out_df.empty:
                    empty_df = pd.DataFrame(columns=expected_columns)
                    empty_df.to_csv(output_path, index=False, encoding="utf-8-sig")
                else:
                    out_df.to_csv(output_path, index=False, encoding="utf-8-sig")
            else:
                write_store_csv_with_fill(
                    store_id=sid,
                    output_dir=output_dir,
                    filename=cfg["output_file"],
                    df=out_df,
                    columns=expected_columns,
                )

        print(f"[OK] config={config_key}, stores={len(all_store_ids)}")
        return

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

        # Rename the two raw fields to required output names
        result = result.rename(
            columns={
                src_cum["value_col"]: "累計至今推薦人綁定人數",
                src_mem["value_col"]: "總會員數",
            }
        )

        # Select columns in the specified order (updated 2024-06: move 綁定率 before 累計/會員數)
        result = result[
            [
                "商店序號",
                "區間推薦人綁定人數",
                "區間推薦人綁定人數 YoY",
                "推薦人綁定率",
                "累計至今推薦人綁定人數",
                "總會員數",
            ]
        ]

        # --- Output (structure-first, fill header if no data for store) ---
        all_store_ids = get_all_store_ids(OUTPUT_DIR)

        expected_columns = list(result.columns)
        grouped = {sid: g for sid, g in result.groupby("商店序號")}

        for sid in all_store_ids:
            g = grouped.get(sid)
            out = g if g is not None else pd.DataFrame(columns=expected_columns)

            write_store_csv_with_fill(
                store_id=sid,
                output_dir=OUTPUT_DIR,
                filename="23-1.區間推薦人綁定人數_門市企業方案.csv",
                df=out,
                columns=expected_columns,
            )

        print(f"[OK] config=23-1, stores={len(all_store_ids)}")
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

        # Output per store (structure-first, fill header if no data for store)
        all_store_ids = get_all_store_ids(OUTPUT_DIR)

        expected_columns = [
            store_col,
            "月份",
            "2024年",
            "2025年",
            "推薦人新綁定數 YoY",
        ]

        grouped = {sid: g for sid, g in result.groupby(store_col)}

        for sid in all_store_ids:
            g = grouped.get(sid)
            out = g[expected_columns].sort_values("月份") if g is not None else pd.DataFrame(columns=expected_columns)

            write_store_csv_with_fill(
                store_id=sid,
                output_dir=OUTPUT_DIR,
                filename="23-2.推薦人新綁定數.csv",
                df=out,
                columns=expected_columns,
            )

        print(f"[OK] config=23-2, stores={len(all_store_ids)}")
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

        # Output columns: 商店序號, 推薦人綁定率, 區間推薦人綁定人數, 區間推薦人綁定人數 YoY, 累計至今推薦人綁定人數, 總會員數
        result = result[
            [
                store_col,
                "推薦人綁定率",
                "區間推薦人綁定人數",
                "區間推薦人綁定人數 YoY",
                "累計至今推薦人綁定人數",
                "總會員數",
            ]
        ]

        # --- Output (structure-first, fill header if no data for store) ---
        all_store_ids = get_all_store_ids(OUTPUT_DIR)

        expected_columns = list(result.columns)
        grouped = {sid: g for sid, g in result.groupby(store_col)}

        for sid in all_store_ids:
            g = grouped.get(sid)
            out = g if g is not None else pd.DataFrame(columns=expected_columns)

            write_store_csv_with_fill(
                store_id=sid,
                output_dir=OUTPUT_DIR,
                filename="24-1.區間推薦人綁定人數_有線下交易資料品牌適用.csv",
                df=out,
                columns=expected_columns,
            )

        print(f"[OK] config=24-1, stores={len(all_store_ids)}")
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

        # --- 推薦人綁定佔門市首購佔比 ---
        result["推薦人綁定佔門市首購佔比"] = (
            result["推薦人綁定數"] / result["門市首購人數"]
        ).where(result["門市首購人數"] != 0)

        result["推薦人綁定佔門市首購佔比"] = result["推薦人綁定佔門市首購佔比"].apply(_fmt_pct)

        # --- Output per store (using store universe and header fill) ---
        all_store_ids = get_all_store_ids(OUTPUT_DIR)

        expected_columns = [
            store_col,
            month_col,
            "門市首購人數",
            "推薦人綁定數",
            "推薦人綁定佔門市首購佔比",
        ]

        grouped = {sid: g for sid, g in result.groupby(store_col)}

        for sid in all_store_ids:
            g = grouped.get(sid)
            if g is not None:
                out = g[expected_columns].sort_values(month_col)
            else:
                out = pd.DataFrame(columns=expected_columns)

            write_store_csv_with_fill(
                store_id=sid,
                output_dir=OUTPUT_DIR,
                filename="24-2.門市推動推薦人綁定.csv",
                df=out,
                columns=expected_columns,
            )

        print(f"[OK] config=24-2, stores={len(all_store_ids)}")
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

        # --- 推薦人綁定佔門市首購佔比 ---
        result["推薦人綁定佔門市首購佔比"] = (
            result["推薦人綁定人數"] / result["門市首購人數"]
        ).where(result["門市首購人數"] != 0)

        # --- Output per store (Top 5 per 商店序號, structure-first, fill header if no data for store) ---
        all_store_ids = get_all_store_ids(OUTPUT_DIR)

        expected_columns = [
            store_col,
            store_name_col,
            "門市首購人數",
            "推薦人綁定人數",
            "推薦人綁定佔門市首購佔比",
        ]

        grouped = {}
        for sid, g in result.groupby(store_col):
            g = g.sort_values("推薦人綁定佔門市首購佔比", ascending=False).head(5)
            g["推薦人綁定佔門市首購佔比"] = g["推薦人綁定佔門市首購佔比"].apply(_fmt_pct_2)
            grouped[sid] = g[expected_columns]

        for sid in all_store_ids:
            out = grouped.get(sid, pd.DataFrame(columns=expected_columns))

            write_store_csv_with_fill(
                store_id=sid,
                output_dir=OUTPUT_DIR,
                filename="25-1.TOP5績優門市.csv",
                df=out,
                columns=expected_columns,
            )

        print(f"[OK] config=25-1, stores={len(all_store_ids)}")
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

        # --- 推薦人綁定佔門市首購佔比 ---
        result["推薦人綁定佔門市首購佔比"] = (
            result["推薦人綁定人數"] / result["門市首購人數"]
        ).where(result["門市首購人數"] != 0)

        # --- Output per store (Bottom 5 per 商店序號, structure-first, fill header if no data for store) ---
        all_store_ids = get_all_store_ids(OUTPUT_DIR)

        expected_columns = [
            store_col,
            store_name_col,
            "門市首購人數",
            "推薦人綁定人數",
            "推薦人綁定佔門市首購佔比",
        ]

        grouped = {}
        for sid, g in result.groupby(store_col):
            g = g.sort_values("推薦人綁定佔門市首購佔比", ascending=True).head(5)
            g["推薦人綁定佔門市首購佔比"] = g["推薦人綁定佔門市首購佔比"].apply(_fmt_pct_2)
            grouped[sid] = g[expected_columns]

        for sid in all_store_ids:
            out = grouped.get(sid, pd.DataFrame(columns=expected_columns))

            write_store_csv_with_fill(
                store_id=sid,
                output_dir=OUTPUT_DIR,
                filename="25-2.BOTTOM5待加強門市.csv",
                df=out,
                columns=expected_columns,
            )

        print(f"[OK] config=25-2, stores={len(all_store_ids)}")
        return

    # ===== 7-1～9-2, 27: Per-store calculation from output/{store_id}/{filename} =====
    if config_key in {"7-1", "7-2", "8-1", "9-1", "9-2", "27"}:
        # Only use OUTPUT_DIR and per-store file, do not use input/aggregate
        all_store_ids = get_all_store_ids(OUTPUT_DIR)

        expected_columns = None

        for sid in all_store_ids:
            g = read_store_input_csv(
                output_dir=OUTPUT_DIR,
                store_id=sid,
                filename=cfg["input_file"],
            )

            if g is not None:
                g.columns = g.columns.astype(str).str.strip()

                # 僅針對 7-1～9-2 清洗直欄佔比來源欄位（避免千分位或字串導致 total=0）
                if config_key in {"7-1", "7-2", "8-1", "9-1", "9-2"}:
                    # Resolve source column for 流量佔比（優先本期，其次舊欄位）
                    src = cfg["ratio_source_col"]
                    if src not in g.columns:
                        fallback_cols = ["工作階段數", "Sessions", "session_count"]
                        for c in fallback_cols:
                            if c in g.columns:
                                src = c
                                break

                    if src in g.columns:
                        g[src] = (
                            g[src]
                            .astype(str)
                            .str.lstrip("'")             # 移除 Excel 常見的前置單引號
                            .str.replace(",", "", regex=False)
                            .str.replace("%", "", regex=False)
                            .str.strip()
                        )
                else:
                    src = cfg["ratio_source_col"]

                g = add_column_ratio(
                    g,
                    source_col=src,
                    target_col=cfg["ratio_target_col"],
                    fmt="percent",
                )

                if expected_columns is None:
                    expected_columns = list(g.columns)

                out_df = g
            else:
                if expected_columns is None:
                    expected_columns = [cfg["ratio_target_col"]]
                out_df = pd.DataFrame(columns=expected_columns)

            write_store_csv_with_fill(
                store_id=sid,
                output_dir=OUTPUT_DIR,
                filename=cfg["output_file"],
                df=out_df,
                columns=expected_columns,
            )
        print(f"[OK] config={config_key}, stores={len(all_store_ids)}")
        return



    # default: legacy for 23-25
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
    parser.add_argument(
        "--ting-test",
        action="store_true",
        help="Enable test mode: read from output/{store_id}/, write to specified output directory",
    )
    parser.add_argument(
        "--output-dir",
        default="output/ting-test",
        help="Output directory path (default: output/ting-test)",
    )
    args = parser.parse_args()

    global OUTPUT_DIR
    if args.ting_test:
        OUTPUT_DIR = Path(f"./{args.output_dir}")

    run_aggregation(args.config, ting_test_mode=args.ting_test, output_dir_name=args.output_dir)


if __name__ == "__main__":
    main()