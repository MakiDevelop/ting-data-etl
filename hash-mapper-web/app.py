"""
Hash Mapper Web Application
將非 E.164 格式的 hash 轉換為 E.164 格式

功能：
- 上傳 CSV（支援 HashedPhone / HashedEmail）
- 自動偵測 hash 欄位
- 批次查詢轉換（效能優化）
- 下載轉換結果
"""

import streamlit as st
import pandas as pd
import csv
import io
import os
from pathlib import Path
from datetime import datetime

from services.hash_converter import HashConverter, detect_hash_column

# 設定
DB_PATH = os.environ.get('DB_PATH', '/app/data/hash_mapping_combined.db')

# 頁面配置
st.set_page_config(
    page_title="Hash to E.164 Converter",
    page_icon="🔄",
    layout="wide"
)

# 標題
st.title("🔄 Hash to E.164 Converter")
st.markdown("將非 E.164 格式的 Hash 轉換為 E.164 格式")

# 側邊欄：資料庫狀態
with st.sidebar:
    st.header("📊 資料庫狀態")

    if os.path.exists(DB_PATH):
        try:
            with HashConverter(DB_PATH) as converter:
                stats = converter.get_db_stats()
                st.success("✅ 資料庫連線正常")
                st.metric("Phone Mappings", f"{stats['phone_count']:,}")
                st.metric("Email Mappings", f"{stats['email_count']:,}")

                # DB 檔案資訊
                db_size = os.path.getsize(DB_PATH) / (1024 * 1024 * 1024)
                st.caption(f"DB 大小: {db_size:.2f} GB")
        except Exception as e:
            st.error(f"❌ 資料庫錯誤: {e}")
    else:
        st.error(f"❌ 找不到資料庫: {DB_PATH}")
        st.info("請確認 Docker volume 掛載正確")

    st.divider()
    st.header("📖 使用說明")
    st.markdown("""
    1. 上傳包含 Hash 的 CSV 檔案
    2. 選擇或確認 Hash 欄位
    3. 點擊「開始轉換」
    4. 下載轉換結果
    """)

    st.divider()
    st.caption("版本: 1.0.0")
    st.caption(f"更新時間: {datetime.now().strftime('%Y-%m-%d')}")

# 主要區域
col1, col2 = st.columns([2, 1])

with col1:
    st.header("📁 上傳 CSV 檔案")

    uploaded_file = st.file_uploader(
        "選擇 CSV 檔案",
        type=['csv'],
        help="支援包含 HashedPhone 或 HashedEmail 欄位的 CSV 檔案"
    )

    if uploaded_file:
        # 讀取預覽
        try:
            # 重置檔案指標
            uploaded_file.seek(0)
            content = uploaded_file.read().decode('utf-8-sig')
            uploaded_file.seek(0)

            lines = content.strip().split('\n')
            reader = csv.DictReader(lines)
            fieldnames = reader.fieldnames

            st.success(f"✅ 檔案載入成功: {uploaded_file.name}")
            st.caption(f"檔案大小: {len(content) / 1024 / 1024:.2f} MB | 總行數: {len(lines) - 1:,}")

            # 預覽前 5 筆
            preview_df = pd.read_csv(io.StringIO(content), nrows=5)
            st.subheader("📋 資料預覽（前 5 筆）")
            st.dataframe(preview_df, use_container_width=True)

            # 自動偵測 hash 欄位
            detected_col, detected_type = detect_hash_column(fieldnames)

            st.subheader("⚙️ 轉換設定")

            col_a, col_b = st.columns(2)

            with col_a:
                # 選擇 hash 欄位
                default_idx = fieldnames.index(detected_col) if detected_col else 0
                hash_column = st.selectbox(
                    "選擇 Hash 欄位",
                    options=fieldnames,
                    index=default_idx,
                    help="選擇包含要轉換的 Hash 的欄位"
                )

            with col_b:
                # 選擇 hash 類型
                hash_type = st.radio(
                    "Hash 類型",
                    options=['phone', 'email'],
                    index=0 if detected_type != 'email' else 1,
                    horizontal=True,
                    help="選擇 Hash 的類型"
                )

            # 顯示偵測結果
            if detected_col:
                st.info(f"💡 自動偵測到 Hash 欄位: **{detected_col}** (類型: {detected_type})")

            # 轉換按鈕
            st.divider()

            if st.button("🚀 開始轉換", type="primary", use_container_width=True):
                if not os.path.exists(DB_PATH):
                    st.error("❌ 資料庫不存在，無法進行轉換")
                else:
                    # 進度條
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    stats_placeholder = st.empty()

                    def update_progress(progress: float, processed: int, total: int):
                        progress_bar.progress(progress)
                        status_text.text(f"處理中... {processed:,} / {total:,} 筆 ({progress*100:.1f}%)")

                    try:
                        with HashConverter(DB_PATH) as converter:
                            # 重置檔案指標
                            uploaded_file.seek(0)

                            # 執行轉換
                            start_time = datetime.now()
                            output_buffer, result_stats = converter.process_csv(
                                input_file=uploaded_file,
                                hash_column=hash_column,
                                hash_type=hash_type,
                                batch_size=5000,
                                progress_callback=update_progress
                            )
                            end_time = datetime.now()

                            # 完成
                            progress_bar.progress(1.0)
                            status_text.text("✅ 轉換完成！")

                            # 顯示統計
                            elapsed = (end_time - start_time).total_seconds()

                            st.success(f"🎉 轉換完成！耗時 {elapsed:.1f} 秒")

                            # 統計卡片
                            stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
                            with stat_col1:
                                st.metric("總筆數", f"{result_stats['total']:,}")
                            with stat_col2:
                                st.metric("成功匹配", f"{result_stats['matched']:,}")
                            with stat_col3:
                                st.metric("未匹配", f"{result_stats['unmatched']:,}")
                            with stat_col4:
                                st.metric("匹配率", f"{result_stats['match_rate']:.2f}%")

                            # 下載按鈕
                            output_filename = f"mapped_{uploaded_file.name}"
                            st.download_button(
                                label="📥 下載轉換結果",
                                data=output_buffer.getvalue(),
                                file_name=output_filename,
                                mime="text/csv",
                                type="primary",
                                use_container_width=True
                            )

                    except ValueError as e:
                        st.error(f"❌ 欄位錯誤: {e}")
                    except Exception as e:
                        st.error(f"❌ 轉換失敗: {e}")
                        st.exception(e)

        except Exception as e:
            st.error(f"❌ 檔案讀取失敗: {e}")

with col2:
    st.header("ℹ️ 說明")

    with st.expander("支援的 Hash 類型", expanded=True):
        st.markdown("""
        **Phone Hash (手機)**
        - 非 E.164 格式的手機號碼 SHA-256 hash
        - 轉換為 E.164 格式（+886...）的 hash

        **Email Hash (信箱)**
        - 原始 email 的 SHA-256 hash
        - 轉換為標準化格式的 hash
        """)

    with st.expander("輸出格式"):
        st.markdown("""
        輸出 CSV 會保留原始所有欄位，並新增一個 `{欄位名}_e164` 欄位。

        例如輸入有 `HashedPhone` 欄位，輸出會多一個 `HashedPhone_e164` 欄位。

        未匹配的資料，e164 欄位會是空值。
        """)

    with st.expander("效能說明"):
        st.markdown("""
        - 使用批次查詢優化，每次查詢 5000 筆
        - 200 萬筆資料約需 2-3 分鐘
        - 資料庫已建立索引，查詢效率高
        """)

    with st.expander("常見問題"):
        st.markdown("""
        **Q: 為什麼有些資料未匹配？**

        A: 可能原因：
        - 該手機/信箱不在會員資料庫中
        - 資料來源的 hash 演算法不同
        - 測試帳號或已刪除帳號

        **Q: 如何更新資料庫？**

        A: 聯繫管理員，從 Looker 下載最新資料後更新
        """)
