from landapi import fetch_apt_trade_data, fetch_apt_detail_info
from sqlitehandler import load_from_db, save_to_db, save_detail_to_db

# === 설정 ===
service_key = "Q7uO9oY26rqekbCW6lMPeBjSLkFlIGRWJr8Px/Kwd1OspHoWhYGIbUN1SgH7l8ic1swKPDoYqN/mYAPTuL6vnA=="  # URL 디코딩된 형태로
lawd_cd = "11110"     # 서울특별시 종로구
deal_ym = "202503"    # 2025년 3월
kaptCode = "A00023456"  # 예: 단지 코드 (기본정보 API로 미리 얻어야 함)

# === 매매 실거래가 수집 및 저장 ===
try:
    trade_df = fetch_apt_trade_data(service_key, lawd_cd, deal_ym)
    print(f"[매매] 수집된 건수: {len(trade_df)}")
    if not trade_df.empty:
        save_to_db(trade_df)
        print("[매매] DB 저장 완료")
        print(load_from_db().head())
    else:
        print("[매매] 데이터가 비어 있음")
except Exception as e:
    print("❌ 매매 데이터 처리 중 오류 발생:", e)

# === 단지 상세정보 수집 및 저장 ===
try:
    detail_df = fetch_apt_detail_info(service_key, kaptCode)
    print(f"[단지상세] 수집 결과:\n{detail_df}")
    if not detail_df.empty:
        save_detail_to_db(detail_df)
        print("[단지상세] DB 저장 완료")
except Exception as e:
    print("❌ 단지 상세정보 처리 중 오류 발생:", e)

    """
    1. 요일
    2. 시간
    3. 장소
    4. 홀이 얼마나 이쁜지
    5. 우월감
    6. 밥
    """