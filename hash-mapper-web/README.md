# Hash Mapper Web Service

將非 E.164 格式的 Hash 轉換為 E.164 格式的 Web 服務。

## 功能

- 上傳 CSV 檔案（支援 HashedPhone / HashedEmail）
- 自動偵測 Hash 欄位
- 批次查詢轉換（效能優化）
- 即時顯示轉換進度
- 下載轉換結果 CSV

## 快速啟動

```bash
# 在 hash-mapper-web 目錄下
docker-compose up -d

# 開啟瀏覽器
open http://localhost:8088
```

## 資料庫

服務使用 SQLite 資料庫進行 Hash 對照查詢：

- **位置**: `../hash_mapping_combined.db`（相對於此目錄）
- **大小**: ~6.4 GB
- **內容**:
  - `phone_mapping`: 18,301,404 筆
  - `email_mapping`: 3,573,022 筆

資料庫以唯讀方式掛載，確保資料安全。

## 目錄結構

```
hash-mapper-web/
├── app.py                 # Streamlit 主程式
├── services/
│   ├── __init__.py
│   └── hash_converter.py  # 轉換核心邏輯
├── .streamlit/
│   └── config.toml        # Streamlit 配置
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## 效能

- 批次查詢：每次 5000 筆
- 200 萬筆資料約需 2-3 分鐘
- SQLite WAL 模式，支援併發讀取

## 更新資料庫

1. 從 Looker 下載最新會員資料
2. 使用 `looker_hash_mapper_merge.py` 合併到 DB
3. 重啟 Docker 容器

```bash
# 停止服務
docker-compose down

# 更新資料庫（在上層目錄）
python3 ../looker_hash_mapper_merge.py ../hash_mapping_combined.db new_data.csv

# 重啟服務
docker-compose up -d
```

## 環境變數

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `DB_PATH` | `/app/data/hash_mapping_combined.db` | 資料庫路徑 |

## Port

- **對外**: 8088
- **容器內部**: 8501（Streamlit 預設）

## 版本

- v1.0.0 (2026-01-22)
