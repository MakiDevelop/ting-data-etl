# 泛用型 CSV 處理服務 - 技術規格書

> 文件版本：v1.0
> 日期：2026-01-22
> 來源專案：91app
> 目標專案：LookerMCP / ABD WebTools

---

## 1. 專案背景

### 1.1 問題陳述

目前有多個 Python 資料轉換腳本以 CLI 方式運作：

| 腳本 | 用途 | 行數 |
|------|------|------|
| `fan_out_by_storeid.py` | 依 storeId 拆分 CSV | 111 |
| `aggregate_by_store.py` | 依店家彙總計算（27 種 config） | 1343 |
| `looker_hash_mapper.py` | Looker hash 對應 | 282 |
| `looker_hash_mapper_merge.py` | Hash mapper 合併 | 203 |
| `verify_store_file_count.py` | 驗證店家檔案數量 | 84 |

### 1.2 使用者痛點

1. **情境 A - 單一大檔**：用戶知道要怎麼計算，但 CSV 太大（500MB+）Excel 打不開，無法自行運算
2. **情境 B - 批次多檔**：CSV 數量太多，需要批次處理，像 fan_out 有多種計算方式

### 1.3 專案目標

建立一個 **泛用型 CSV 處理 Web 服務**，具備：
- 視覺化操作介面
- 自然語言條件輸入（如「篩選金額大於1000」）
- 大檔案處理能力
- 可組合的 Pipeline 流程

---

## 2. 技術架構

### 2.1 整體架構

```
┌─────────────────────────────────────────────────────────────┐
│                    泛用型 CSV 處理服務                        │
├─────────────────────────────────────────────────────────────┤
│  UI 層：Streamlit                                            │
│    - Phase 1: 表單式「新增步驟」介面                          │
│    - Phase 2: Mito（Excel 風格）或 React Flow（拖曳節點）     │
├─────────────────────────────────────────────────────────────┤
│  自然語言層                                                   │
│    - 簡易版：規則式 Pattern Matching（無需外部 API）          │
│    - 進階版：LLM → DuckDB SQL                                │
├─────────────────────────────────────────────────────────────┤
│  運算核心：DuckDB（大檔）+ Pandas（小檔）                      │
│    - 100MB 以下：Pandas 直接處理                              │
│    - 100MB 以上：自動切換 DuckDB                              │
│    - 直接對 CSV 執行 SQL，不需載入記憶體                       │
├─────────────────────────────────────────────────────────────┤
│  模板層：現有腳本                                             │
│    - 91app 店家分流模板                                       │
│    - 91app 店家彙總模板                                       │
│    - 使用者可選用、可修改                                     │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 技術選型

| 層級 | 技術 | 理由 |
|------|------|------|
| **UI** | Streamlit | Python 原生、快速開發、內部工具首選 |
| **大檔運算** | DuckDB | 可直接對 CSV 執行 SQL，不載入記憶體，速度快 |
| **小檔運算** | Pandas | 生態成熟、彈性高 |
| **自然語言** | 規則式 + LLM fallback | 簡單指令用規則，複雜指令用 LLM |
| **視覺化拖曳**（Phase 3） | streamlit-flow 或 React Flow | ETL 節點風格 |

### 2.3 模組結構

```
csv_processor/
├── core/
│   ├── pipeline.py          # Pipeline 執行引擎
│   ├── operations.py         # 基礎 Operation 介面
│   └── registry.py           # Operation 註冊與發現
├── operations/
│   ├── io/                   # 讀寫操作
│   │   ├── csv_reader.py
│   │   ├── csv_writer.py
│   │   └── chunked_reader.py # 大檔處理
│   ├── transform/            # 轉換操作
│   │   ├── filter.py
│   │   ├── select_columns.py
│   │   ├── rename.py
│   │   ├── compute.py        # 新增欄位運算
│   │   └── type_cast.py
│   ├── aggregate/            # 聚合操作
│   │   ├── groupby.py
│   │   ├── pivot.py
│   │   └── merge.py
│   └── fan_out/              # 分流操作
│       ├── split_by_column.py
│       └── batch_process.py
├── nlp/
│   └── condition_parser.py   # 自然語言轉條件
├── ui/
│   ├── app.py                # Streamlit 主程式
│   ├── pipeline_builder.py   # Pipeline 建構器
│   └── data_preview.py       # 即時預覽
└── templates/
    ├── fan_out_template.py   # 91app 分流模板
    └── aggregate_template.py # 91app 彙總模板
```

---

## 3. 核心設計

### 3.1 Operation 介面

每個操作實作統一介面，確保可組合性：

```python
from abc import ABC, abstractmethod
from typing import Dict, Any, Iterator
import pandas as pd

class Operation(ABC):
    name: str                  # UI 顯示名稱
    category: str              # 分類（io/transform/aggregate/fan_out）
    params_schema: dict        # 參數定義（用於動態表單生成）

    @abstractmethod
    def validate(self, input_schema: dict) -> dict:
        """驗證此 Operation 是否可接受輸入

        Returns:
            {"valid": bool, "errors": list[str]}
        """
        pass

    @abstractmethod
    def execute(self, df: pd.DataFrame, params: dict) -> pd.DataFrame:
        """執行操作"""
        pass

    @abstractmethod
    def get_output_schema(self, input_schema: dict) -> dict:
        """預測輸出 schema（用於 UI 預覽下一步可用欄位）"""
        pass
```

### 3.2 大檔案處理策略

```python
class ChunkedCSVProcessor:
    """處理大型 CSV 檔案（500MB+）"""

    def __init__(self, path: str, chunk_size: int = 50000):
        self.path = path
        self.chunk_size = chunk_size

    def read_schema(self) -> dict:
        """只讀取 header，不載入資料"""
        df = pd.read_csv(self.path, nrows=0)
        return {"columns": list(df.columns), "dtypes": df.dtypes.to_dict()}

    def preview(self, n: int = 100) -> pd.DataFrame:
        """預覽前 N 筆"""
        return pd.read_csv(self.path, nrows=n)

    def iter_chunks(self) -> Iterator[pd.DataFrame]:
        """分塊迭代處理"""
        for chunk in pd.read_csv(self.path, chunksize=self.chunk_size):
            yield chunk

    def process_with_duckdb(self, sql: str) -> pd.DataFrame:
        """使用 DuckDB 直接查詢 CSV（最省記憶體）"""
        import duckdb
        return duckdb.query(sql).df()
```

### 3.3 自然語言解析（Phase 2）

```python
import re
from typing import Optional

PATTERNS = {
    r"篩選\s*(.+?)\s*(大於|小於|等於|不等於|包含)\s*(.+)": "filter",
    r"排序\s*(.+?)\s*(遞增|遞減|升冪|降冪)?": "sort",
    r"選取\s*(.+)": "select",
    r"刪除\s*(.+)\s*欄": "drop",
    r"按\s*(.+?)\s*分組.*(計算|加總|平均|最大|最小)\s*(.+)": "groupby",
    r"合併\s*(.+)": "merge",
}

OPERATOR_MAP = {
    "大於": ">", "小於": "<", "等於": "==",
    "不等於": "!=", "包含": "contains"
}

def parse_natural_language(text: str, available_columns: list) -> Optional[dict]:
    """
    解析自然語言指令

    範例：
        輸入：「篩選金額大於1000」
        輸出：{"operation": "filter", "column": "金額", "operator": ">", "value": 1000}

        輸入：「按店家分組，計算金額的總和」
        輸出：{"operation": "groupby", "by": "店家", "agg": {"金額": "sum"}}
    """
    for pattern, op_type in PATTERNS.items():
        match = re.match(pattern, text.strip())
        if match:
            return _build_operation(op_type, match, available_columns)
    return None
```

---

## 4. 開發階段規劃

### 4.1 Phase 1 MVP（3 週）

**目標**：可用的內部工具，解決「大檔打不開」問題

| 功能 | 說明 | 優先級 |
|------|------|--------|
| CSV 上傳與預覽 | 支援大檔（分頁載入前 100 筆） | P0 |
| 基礎操作面板 | 篩選、選欄、排序、聚合 | P0 |
| 操作串接 | 線性 Pipeline（表單式新增步驟） | P0 |
| 即時預覽 | 每步驟顯示前 100 筆結果 | P0 |
| 結果下載 | 單檔 CSV / Excel 輸出 | P0 |
| 大檔支援 | 100MB+ 自動切換 DuckDB | P0 |
| 91app 模板 | 整合 fan_out + aggregate 作為一鍵執行 | P1 |

**不含（延後）**：
- 視覺化拖曳 UI
- 自然語言輸入
- 批次多檔處理
- Pipeline 儲存/載入

**工時細項**：

| 任務 | 工時 |
|------|------|
| 專案骨架 + Operation 介面 | 1 天 |
| CSV Reader（含 DuckDB 大檔） | 1.5 天 |
| 基礎 Operations（filter/select/sort/groupby） | 2 天 |
| Streamlit UI 主框架 | 1.5 天 |
| Pipeline Builder UI（表單式） | 2 天 |
| 即時預覽組件 | 1 天 |
| 91app 模板整合 | 2 天 |
| 測試 + 修正 | 2 天 |
| **小計** | **13 天** |

### 4.2 Phase 2（2 週）

**目標**：降低使用門檻

| 功能 | 說明 |
|------|------|
| 自然語言條件 | 「篩選金額大於1000」→ filter 條件 |
| 批次多檔上傳 | 支援 ZIP 或多檔選擇 |
| Fan-out 輸出 | 按指定欄位分割成多檔（ZIP 下載） |
| Pipeline 儲存 | 儲存為 JSON，可重複載入使用 |

### 4.3 Phase 3（2.5 週）

**目標**：完整 UX

| 功能 | 說明 |
|------|------|
| 視覺化拖曳 | 使用 streamlit-flow 實現節點拖曳 |
| 進階聚合 | Pivot table、多檔 merge |
| 排程執行 | 定時跑 Pipeline（可選） |

---

## 5. 技術挑戰與解決方案

### 5.1 大檔案處理（500MB+）

| 挑戰 | 解決方案 |
|------|----------|
| 記憶體溢出 | DuckDB 直接查詢 CSV，不載入記憶體 |
| 上傳限制 | 設定 `.streamlit/config.toml`: `server.maxUploadSize = 1000` |
| 前端卡頓 | 永遠只顯示 `head(100)`，不傳大量資料到前端 |
| 長時間處理 | 顯示進度條，支援取消 |

### 5.2 視覺化拖曳

| 方案 | 優點 | 缺點 | 階段 |
|------|------|------|------|
| 表單式 + 上下排序 | 穩定、易開發 | UX 較傳統 | Phase 1 |
| streamlit-flow | 節點連線、ETL 風格 | 文件較少 | Phase 3 |
| Mito | Excel 風格、自動生成 code | 進階功能付費 | 可選 |

### 5.3 自然語言解析

| 方案 | 優點 | 缺點 | 階段 |
|------|------|------|------|
| 規則式 Pattern | 無需外部 API、可離線 | 支援語法有限 | Phase 2 |
| LLM + DuckDB SQL | 彈性高、支援複雜查詢 | 需 API、有幻覺風險 | 可選 |

---

## 6. 與現有腳本的整合

### 6.1 整合策略：作為「預設模板」

```
┌─────────────────────────────────────────────────────┐
│                   Template 層                       │
├─────────────────────────────────────────────────────┤
│  「91app 店家分流」模板                              │
│   = 讀取 CSV                                        │
│   + 偵測商店序號欄位（多種欄位名稱相容）              │
│   + 按商店序號 Fan-out                              │
│   + 每店一個資料夾輸出                               │
├─────────────────────────────────────────────────────┤
│  「91app 店家彙總」模板                              │
│   = 選擇 config key（下拉選單）                     │
│   + 套用預設參數                                     │
│   + 輸出至 output/{store_id}/                       │
└─────────────────────────────────────────────────────┘
        ↓ 使用者可以「基於模板修改」或「從頭建立」
┌─────────────────────────────────────────────────────┐
│                   Pipeline Builder                  │
│  [讀取] → [篩選] → [欄位運算] → [分組] → [輸出]      │
└─────────────────────────────────────────────────────┘
```

### 6.2 腳本抽象化對應

| 原腳本 | 抽象為 Operation | 說明 |
|--------|------------------|------|
| `fan_out_by_storeid.py` | `SplitByColumn` | 按指定欄位分割成多檔 |
| `aggregate_by_store.py` | `GroupByAggregate` + `ComputeColumn` | 分組聚合 + 欄位運算 |
| `looker_hash_mapper.py` | `HashLookup` | 對照表查詢 |
| `verify_store_file_count.py` | `ValidateOutput` | 驗證輸出完整性 |

---

## 7. 現成工具參考

| 工具 | 類型 | 優點 | 缺點 | 參考價值 |
|------|------|------|------|----------|
| **Mito** | Spreadsheet UI | Excel 風格、自動生成 Python code | 進階功能付費 | UI 設計參考 |
| **PyGWalker** | BI / Viz | 類似 Tableau 拖曳繪圖 | 專注視覺化，非 ETL | 繪圖整合 |
| **Pandas GUI** | Desktop | 功能完整 | 不適合 Web | 功能參考 |
| **streamlit-flow** | Flow Chart | 節點拖曳連線 | 文件少 | Phase 3 使用 |

---

## 8. 決策記錄

以下決策已由 Human 確認（2026-01-22）：

| # | 決策點 | 決策 | 理由 |
|---|--------|------|------|
| 1 | 目標用戶 | 內部團隊 | 不需外部 API 整合 |
| 2 | 技術棧 | Streamlit | Python 原生、快速開發 |
| 3 | MVP 起點 | 表單式 UI | 穩定優先，降低風險 |
| 4 | 現有腳本 | 作為可選模板 | 保留彈性、漸進式遷移 |
| 5 | 大檔支援 | Phase 1 就做 | 核心痛點 |
| 6 | 運算引擎 | DuckDB | 大檔效能優勢明顯 |

---

## 9. 下一步行動

1. **建立專案骨架**：在 LookerMCP 或 ABD 專案下建立 `csv_processor/` 目錄
2. **實作 Operation 介面**：定義基礎抽象類別
3. **實作 CSV Reader**：含 DuckDB 大檔支援
4. **Streamlit 主框架**：上傳 → 預覽 → 操作 → 下載

---

## 附錄 A：Streamlit 配置範例

`.streamlit/config.toml`

```toml
[server]
maxUploadSize = 1000  # 1GB
maxMessageSize = 1000

[browser]
gatherUsageStats = false
```

---

## 附錄 B：DuckDB 查詢範例

```python
import duckdb

# 直接查詢 CSV，不載入記憶體
result = duckdb.query("""
    SELECT store_id, SUM(amount) as total
    FROM 'large_file.csv'
    WHERE amount > 1000
    GROUP BY store_id
    ORDER BY total DESC
""").df()
```

---

*文件結束*
