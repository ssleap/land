# main.py
from landapi import (
    fetch_total_apt_list_v3,
    fetch_apt_basic_info_v3,
    fetch_apt_detail_info_v3,
    fetch_apt_trade_detail_v3
)
from sqlitehandler import save_to_db, reset_table_schema
import time
import pandas as pd

# === 설정 ===
service_key = "Q7uO9oY26rqekbCW6lMPeBjSLkFlIGRWJr8Px/Kwd1OspHoWhYGIbUN1SgH7l8ic1swKPDoYqN/mYAPTuL6vnA=="
deal_ymd = "202503"  # 예시: 2025년 3월
lawd_cd = "41110"     # 예시: 수원시

# === 1단계: 전국 단지 목록 수집 ===
apt_list_df = fetch_total_apt_list_v3(service_key)
reset_table_schema(apt_list_df, table_name="apt_list")

# === 2단계: 각 단지별 기본정보 수집 ===
basic_list = []
for idx, row in apt_list_df.iterrows():
    try:
        basic_df = fetch_apt_basic_info_v3(service_key, row['kaptCode'])
        if not basic_df.empty:
            basic_list.append(basic_df)
        time.sleep(0.2)
    except Exception as e:
        print(f"❌ 기본정보 실패: {row['kaptCode']} - {e}")

if basic_list:
    all_basic_df = pd.concat(basic_list, ignore_index=True)
    reset_table_schema(all_basic_df, table_name="apt_basic")

# === 3단계: 각 단지별 상세정보 수집 ===
detail_list = []
for idx, row in apt_list_df.iterrows():
    try:
        detail_df = fetch_apt_detail_info_v3(service_key, row['kaptCode'])
        if not detail_df.empty:
            detail_list.append(detail_df)
        time.sleep(0.2)
    except Exception as e:
        print(f"❌ 상세정보 실패: {row['kaptCode']} - {e}")

if detail_list:
    all_detail_df = pd.concat(detail_list, ignore_index=True)
    reset_table_schema(all_detail_df, table_name="apt_detail")

# === 4단계: 실거래가 수집 (법정동 코드 기반) ===
try:
    trade_df = fetch_apt_trade_detail_v3(service_key, lawd_cd, deal_ymd)
    reset_table_schema(trade_df, table_name="apt_trade")
except Exception as e:
    print(f"❌ 실거래가 수집 실패: {e}")
