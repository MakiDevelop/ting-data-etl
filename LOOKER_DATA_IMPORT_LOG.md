# Looker 資料匯入記錄

## 當前方案（2026-01-15）

**資料來源：** Looker UI 手動匯出

**重要提醒：**
1. ⚠️ **下載時務必固定排序條件**
   - 在 Looker UI 中明確指定排序欄位（建議使用主鍵或時間欄位）
   - 避免「今天跟明天下載結果不一樣」的問題
   - 分析型資料庫的查詢結果順序是 non-deterministic

2. 📝 **階段性方案說明**
   - 目前資料由 Looker UI 匯出（一次性/低頻更新）
   - 待需求穩定再評估自動化方案

## 技術發現

### Looker API 分頁問題
- **問題：** Offset-based pagination 在分析型資料庫上不可靠
- **原因：** Non-deterministic ordering（無固定排序時，每次查詢順序可能不同）
- **結果：** 使用 offset 分頁會取到重複資料
- **解決方案：**
  - 方案 A：Looker UI 下載（簡單直接）✅
  - 方案 B：API 查詢時加上明確的 sorts 參數（複雜）

### 目前資料狀態
- **檔案：** `looker_report_7000000_20260115_124938.csv` (1.3GB)
- **記錄數：** 7,000,000 筆
- **Unique phone hashes：** 3,648,581
- **Unique email hashes：** 3,573,022

### CMoney Mapping 結果
- **Match Rate：** 19.24% (392,677 / 2,041,186)
- **輸出目錄：** `/Users/maki/91app/output/mapped_cmoney_7m/`

## 未來自動化考量

如需要定期自動更新：
1. 使用 Looker API 時必須加上明確的排序條件
2. 或使用 cursor-based pagination（如果 Looker 支援）
3. 評估是否改用 Looker Scheduled Delivery 自動送檔案

## 相關檔案
- Looker 原始資料：`/Users/maki/91app/looker_report_*.csv`
- Mapping 輸出：`/Users/maki/91app/output/mapped_cmoney_*/`
- SQLite DB：`/Users/maki/91app/hash_mapping_*.db`

---
最後更新：2026-01-15
負責人：Data Team
