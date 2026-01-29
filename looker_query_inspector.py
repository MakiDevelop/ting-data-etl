#!/usr/bin/env python3
"""
Looker Query Inspector
檢查 Looker query 的詳細資訊，包括排序設定
"""

import requests
import json
import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Looker Configuration
LOOKER_BASE_URL = os.getenv("LOOKER_BASE_URL", "https://analytics-platform.91app.com")
LOOKER_CLIENT_ID = os.getenv("LOOKER_CLIENT_ID")
LOOKER_CLIENT_SECRET = os.getenv("LOOKER_CLIENT_SECRET")

if not LOOKER_CLIENT_ID or not LOOKER_CLIENT_SECRET:
    raise ValueError("Missing LOOKER_CLIENT_ID or LOOKER_CLIENT_SECRET in environment")


def authenticate():
    """認證"""
    print("認證中...")
    auth_url = f"{LOOKER_BASE_URL}/api/4.0/login"
    payload = {
        "client_id": LOOKER_CLIENT_ID,
        "client_secret": LOOKER_CLIENT_SECRET
    }

    response = requests.post(auth_url, data=payload, verify=True)
    response.raise_for_status()
    access_token = response.json()["access_token"]
    print("✓ 認證成功\n")
    return access_token


def inspect_query(query_id, access_token):
    """檢查 query 資訊"""
    headers = {"Authorization": f"Bearer {access_token}"}
    query_url = f"{LOOKER_BASE_URL}/api/4.0/queries/{query_id}"

    print(f"{'='*60}")
    print(f"Query ID: {query_id}")
    print(f"{'='*60}\n")

    response = requests.get(query_url, headers=headers, verify=True)
    response.raise_for_status()
    query_info = response.json()

    # 顯示關鍵資訊
    print(f"Model: {query_info.get('model', 'N/A')}")
    print(f"View: {query_info.get('view', 'N/A')}")
    print(f"\nFields:")
    for field in query_info.get('fields', []):
        print(f"  - {field}")

    print(f"\nFilters:")
    filters = query_info.get('filters', {})
    if filters:
        for key, value in filters.items():
            print(f"  - {key}: {value}")
    else:
        print("  (none)")

    print(f"\nSorts:")
    sorts = query_info.get('sorts', [])
    if sorts:
        for sort in sorts:
            print(f"  - {sort}")
    else:
        print("  (none - 可能導致 non-deterministic ordering!)")

    print(f"\nPivots:")
    pivots = query_info.get('pivots', [])
    if pivots:
        for pivot in pivots:
            print(f"  - {pivot}")
        print("\n⚠️  此 query 使用了 pivots，可能限制排序選項")
    else:
        print("  (none)")

    print(f"\nLimit: {query_info.get('limit', 'N/A')}")

    print(f"\n{'='*60}")
    print("完整 JSON:")
    print(f"{'='*60}")
    print(json.dumps(query_info, indent=2, ensure_ascii=False))

    return query_info


def main():
    if len(sys.argv) < 2:
        print("使用方法:")
        print(f"  python3 {sys.argv[0]} <query_id>")
        print("\n範例:")
        print(f"  python3 {sys.argv[0]} Japtc2w4jUKaJGmWrRRBzj")
        sys.exit(1)

    query_id = sys.argv[1]

    access_token = authenticate()
    inspect_query(query_id, access_token)


if __name__ == "__main__":
    main()
