from landapi import (
    fetch_all_total_apt_list,
    fetch_apt_basic_info_v3,
    fetch_apt_detail_info_v3,
    fetch_apt_trade_detail_v3,
    fetch_apt_rent_data_v3,
    fetch_apt_list_by_sigungu_v3,
)
from sqlitehandler import (
    save_to_db, 
    reset_table_schema, 
    load_from_db,
)

import time
import pandas as pd

# === 설정 ===
service_key = "Q7uO9oY26rqekbCW6lMPeBjSLkFlIGRWJr8Px/Kwd1OspHoWhYGIbUN1SgH7l8ic1swKPDoYqN/mYAPTuL6vnA=="
deal_ymd = "202503"  # 예시: 2025년 3월
lawd_cd = "41110"     # 예시: 수원시
apt_list_df = load_from_db(db_path="apt_data.db", table_name="apt_list")

seoul_lawd = [11110, 11140, 11170, 11200, 11215, 11230, 11260, 11290,
              11305, 11320, 11350, 11380, 11410, 11440, 11470, 11500,
              11530, 11545, 11560, 11590, 11620, 11650, 11680, 11710, 11740]

gyeonggi_lawd = [41111, 41113, 41115, 41117, 41131, 41133, 41135, 41150,
                 41171, 41173, 41190, 41210, 41220, 41250, 41271, 41273,
                 41281, 41285, 41287, 41290, 41310, 41360, 41370, 41390,
                 41410, 41430, 41450, 41461, 41463, 41465, 41480, 41500,
                 41550, 41570, 41590, 41610, 41630, 41650, 41670, 41800,
                 41820, 41830]

incheon_lawd = [28110, 28140, 28177, 28185, 28200, 28237, 28245, 28260,
                28710, 28720]

lawd_central = [11, 41, 28]

sale_mon = [202501, 202502, 202503]

# === 1단계: 전국 단지 목록 수집 ===
def create_and_save_apt_list() :
    print("📥 단지 목록 수집 시작...")
    apt_list_df = fetch_all_total_apt_list(service_key)
    reset_table_schema(apt_list_df, table_name="apt_list")
    print("✅ 단지 목록 저장 완료")

# === 4단계: 실거래가 수집 (법정동 코드 기반) ===
def create_and_save_trade_detail(lawd_cd, deal_ymd):
    try:
        print(f"📌 [매매] {lawd_cd} - {deal_ymd} 수집 중...")
        trade_df = fetch_apt_trade_detail_v3(service_key, lawd_cd, deal_ymd)
        save_to_db(trade_df, table_name="apt_trade")
        print(f"✅ [매매] {lawd_cd} - {deal_ymd} 저장 완료")
    except Exception as e:
        print(f"❌ 실거래가 수집 실패: {lawd_cd}, {deal_ymd}, 오류: {e}")

# 5단계 : 전월세 데이터 수집
def create_and_save_rent_trade_detail(lawd_cd, deal_ymd):
    try :
        print(f"📌 [전월세] {lawd_cd} - {deal_ymd} 수집 중...")
        rent_df = fetch_apt_rent_data_v3(service_key, lawd_cd, deal_ymd)
        save_to_db(rent_df, table_name="apt_rent")
        print(f"✅ [전월세] {lawd_cd} - {deal_ymd} 저장 완료")
    except Exception as e:
        print(f"❌ 전월세 상세정보 실패: {lawd_cd}, {deal_ymd}, 오류: {e}")

# === 2단계: 각 단지별 기본정보 수집 ===
def create_and_save_apt_basic_info():
    print("📥 기본정보 수집 시작...")
    basic_list = []
    for idx, row in apt_list_df.iterrows():
        try:
            if idx % 1000 == 0:
                print(f"🔄 기본정보 진행: {idx} / {len(apt_list_df)}")
            basic_df = fetch_apt_basic_info_v3(service_key, row['kaptCode'])
            if not basic_df.empty:
                basic_list.append(basic_df)
            # time.sleep(0.2)
        except Exception as e:
            print(f"❌ 기본정보 실패: {row['kaptCode']}, 오류: {e}")

    if basic_list:
        all_basic_df = pd.concat(basic_list, ignore_index=True)
        save_to_db(all_basic_df, table_name="apt_basic")
        print("✅ 기본정보 저장 완료")

# === 3단계: 각 단지별 상세정보 수집 ===
def create_and_save_apt_complex_dtl():
    print("📥 상세정보 수집 시작...")
    detail_list = []
    apt_list_df = load_from_db(db_path="apt_data.db", table_name="apt_list")
    for idx, row in apt_list_df.iterrows():
        try:
            if idx % 1000 == 0:
                print(f"🔄 상세정보 진행: {idx} / {len(apt_list_df)}")
            detail_df = fetch_apt_detail_info_v3(service_key, row['kaptCode'])
            if not detail_df.empty:
                detail_list.append(detail_df)
            time.sleep(0.2)
        except Exception as e:
            print(f"❌ 상세정보 실패: {row['kaptCode']}, 오류: {e}")

    if detail_list:
        all_detail_df = pd.concat(detail_list, ignore_index=True)
        save_to_db(all_detail_df, table_name="apt_detail")
        print("✅ 상세정보 저장 완료")

def run_trade():
    for deal_ym in sale_mon:
        print(f"====================== {deal_ym}월 수집 시작 ======================")
        for lawd in seoul_lawd:
            create_and_save_trade_detail(lawd_cd=lawd, deal_ymd=deal_ym)
            create_and_save_rent_trade_detail(lawd_cd=lawd, deal_ymd=deal_ym)
        for lawd in gyeonggi_lawd:
            create_and_save_trade_detail(lawd_cd=lawd, deal_ymd=deal_ym)
            create_and_save_rent_trade_detail(lawd_cd=lawd, deal_ymd=deal_ym)
        for lawd in incheon_lawd:
            create_and_save_trade_detail(lawd_cd=lawd, deal_ymd=deal_ym)
            create_and_save_rent_trade_detail(lawd_cd=lawd, deal_ymd=deal_ym)
        print(f"====================== {deal_ym}월 수집 완료 ======================")


if __name__ == "__main__":
    create_and_save_apt_basic_info()