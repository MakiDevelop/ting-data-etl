"""
寶雅錢包交易資料分析
日期：2026-01-21
需求來源：Joe

分析目標：計算九個指標
- 會員分類定義：
  1. OMO會員：同一會員在同一年內，既有「門市」交易也有「線上交易」
  2. 純線下會員：同一會員在同一年內，只有「門市」交易，沒有「線上交易」
  3. 純線上會員：同一會員在同一年內，只有「線上交易」，沒有「門市」交易

- 計算指標：
  1-3. 三種會員類型的會員數
  4-6. 三種會員類型的年均消費金額（交易金額 / 消費會員數）
  7-9. 三種會員類型的年均消費次數（訂單數 / 消費會員數）
"""

import csv
from collections import defaultdict

# 讀取 CSV，統計每個會員的交易門市類型、消費金額、訂單數
member_data = defaultdict(lambda: {'online': False, 'offline': False, 'total_amount': 0.0, 'order_count': 0})

with open('2025年 寶雅錢包交易資料_0121撈.csv', 'r', encoding='utf-8-sig') as f:
    reader = csv.reader(f)
    header = next(reader)  # 跳過標頭

    for row in reader:
        if len(row) < 5:
            continue

        member_id = row[0].strip("'")
        amount = float(row[3].strip("'"))
        store_name = row[4].strip("'")

        member_data[member_id]['total_amount'] += amount
        member_data[member_id]['order_count'] += 1

        if store_name == '線上交易':
            member_data[member_id]['online'] = True
        else:
            member_data[member_id]['offline'] = True

# 計算三種會員數和消費金額
omo_count = 0       # 線上+線下都有
offline_only = 0    # 純線下
online_only = 0     # 純線上

omo_amount = 0.0
offline_only_amount = 0.0
online_only_amount = 0.0

omo_orders = 0
offline_only_orders = 0
online_only_orders = 0

for member_id, data in member_data.items():
    if data['online'] and data['offline']:
        omo_count += 1
        omo_amount += data['total_amount']
        omo_orders += data['order_count']
    elif data['offline'] and not data['online']:
        offline_only += 1
        offline_only_amount += data['total_amount']
        offline_only_orders += data['order_count']
    elif data['online'] and not data['offline']:
        online_only += 1
        online_only_amount += data['total_amount']
        online_only_orders += data['order_count']

print(f"總會員數: {len(member_data):,}")
print(f"---")
print(f"1. OMO會員數（線上+線下）: {omo_count:,}")
print(f"2. 純線下會員數: {offline_only:,}")
print(f"3. 純線上會員數: {online_only:,}")
print(f"---")
print(f"驗證總和: {omo_count + offline_only + online_only:,}")

print(f"\n=== 年均消費金額（交易金額 / 消費會員數）===")
print(f"4. OMO會員年均消費金額: {omo_amount / omo_count:,.2f}")
print(f"5. 純線下會員年均消費金額: {offline_only_amount / offline_only:,.2f}")
print(f"6. 純線上會員年均消費金額: {online_only_amount / online_only:,.2f}")

print(f"\n=== 年均消費次數（訂單數 / 消費會員數）===")
print(f"7. OMO會員年均消費次數: {omo_orders / omo_count:,.2f}")
print(f"8. 純線下會員年均消費次數: {offline_only_orders / offline_only:,.2f}")
print(f"9. 純線上會員年均消費次數: {online_only_orders / online_only:,.2f}")
