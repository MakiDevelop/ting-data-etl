# 依 storeId fan-out CSV 的資料分桶程式（splitter）— Codex Prompt

請你用 **Python** 撰寫一支「依 storeId fan-out CSV 的資料分桶程式（splitter）」。

## 背景說明

目前有一批 CSV 檔案：

- CSV 檔數量很多（例如 60 個）
- 每個 CSV 的 schema 不一致，欄位名稱與數量都不同
- 所有 CSV 都一定包含 `storeId` 欄位
- 同一個 CSV 檔中，可能出現多個不同的 `storeId`
- CSV 資料量可能很大，不適合一次載入記憶體

## 核心目標

依照 `storeId` 將資料列 fan-out 成多個目錄與 CSV 檔：

- 每一個 `storeId` 建立一個目錄，例如：
  ```
  output/
    store_001/
    store_002/
    store_003/
    ...
  ```

- 在每個 `storeId` 目錄底下：
  - 依「原始 CSV 檔名」輸出對應的 CSV
  - 只包含該 `storeId` 的資料列
  - header 必須與原始 CSV 完全一致

範例輸出結構：

```
output/
  store_081/
    data_01.csv
    data_02.csv
  store_133/
    data_01.csv
  store_084/
    data_01.csv
    data_03.csv
```

## 功能需求

### 1. 輸入

- 一個目錄，內含多個 CSV 檔
- 不預設檔名規則，只處理副檔名為 `.csv` 的檔案

### 2. 處理方式

- 逐檔、逐列讀取 CSV（streaming）
- 動態找到 `storeId` 欄位 index
- 對每一列資料：
  - 依 `storeId` 分流到對應輸出檔
- 不允許先把整個檔案或整個 `storeId` group 存進記憶體

### 3. 輸出規則

- `output/{storeId}/{原始檔名}.csv`
- 若輸出檔第一次被寫入：
  - 需先寫 header
- 之後同 `storeId`、同來源檔名直接 append 資料列

### 4. 穩定性要求

- 不假設 `storeId` 有排序
- 不假設 `storeId` 連續
- 若 CSV 缺少 `storeId` 欄位：
  - 印出 warning
  - 跳過該檔案
- 程式中途 crash，不應留下破壞性狀態（append 是可接受的）

### 5. 實作限制

- 使用 Python 內建 `csv` module
- 不使用 pandas
- 程式結構清楚，可維護
- 不需要多執行緒或 async

### 6. CLI 介面

請提供可執行入口，支援以下參數：

- `--input-dir`：來源 CSV 目錄
- `--output-dir`：fan-out 輸出目錄
- `--encoding`（選填，預設 `utf-8`）

## 加分但非必要

- 適當 logging（例如 print 或 logging module）
- 註解清楚說明設計假設
- 將「取得 writer / 檔案 handler」獨立成小函式

## 不需要做的事

- 不需要合併 CSV
- 不需要排序
- 不需要驗證資料正確性
- 不需要單元測試

請直接輸出到 fan_out_by_storeid.py **完整、可執行的 Python 程式碼**。
