# sqlitehandler.py
import sqlite3
import pandas as pd
import sqlite3
import pandas as pd
from rapidfuzz import process, fuzz
import os

def merge_and_make_table_to_csv():
# DB 연결
    conn = sqlite3.connect("apt_data.db")  # .db 파일 경로 조정

    # 테이블 불러오기
    apt_list = pd.read_sql("SELECT kaptCode, kaptName, bjdCode FROM apt_list", conn)
    apt_basic = pd.read_sql("SELECT kaptCode, kaptdaCnt FROM apt_basic", conn)
    rent = pd.read_sql("SELECT aptNm, sggCd, excluUseAr, REPLACE(deposit, ',', '') AS deposit FROM apt_rent WHERE monthlyRent = '0'", conn)
    trade = pd.read_sql("SELECT aptNm, sggCd, excluUseAr, REPLACE(dealAmount, ',', '') AS dealAmount FROM apt_trade WHERE aptNm IS NOT NULL", conn)

    # 아파트 이름 전처리
    def normalize(name):
        return name.lower().replace(" ", "").replace("(", "").replace(")", "") if name else ""

    apt_list["apt_key"] = apt_list["kaptName"].apply(normalize)
    rent["apt_key_raw"] = rent["aptNm"].apply(normalize)
    trade["apt_key_raw"] = trade["aptNm"].apply(normalize)

    # RapidFuzz 매칭 함수
    def map_kaptcode(df, sgg_col):
        kapt_codes, bjd_codes = [], []
        for idx, row in df.iterrows():
            sgg = str(row[sgg_col])[:5]
            name = row["apt_key_raw"]
            local_apts = apt_list[apt_list["bjdCode"].str.startswith(sgg)]
            match = process.extractOne(name, local_apts["apt_key"], scorer=fuzz.token_sort_ratio, score_cutoff=80)
            if match:
                matched = local_apts[local_apts["apt_key"] == match[0]].iloc[0]
                kapt_codes.append(matched["kaptCode"])
                bjd_codes.append(matched["bjdCode"])
            else:
                kapt_codes.append(None)
                bjd_codes.append(None)
        df["kaptCode"] = kapt_codes
        df["bjdCode"] = bjd_codes
        return df.dropna(subset=["kaptCode", "bjdCode"])

    # 매칭 실행
    rent = map_kaptcode(rent, "sggCd")
    trade = map_kaptcode(trade, "sggCd")

    # 숫자형 변환 및 전용면적 반올림
    rent["deposit"] = pd.to_numeric(rent["deposit"], errors="coerce")
    trade["dealAmount"] = pd.to_numeric(trade["dealAmount"], errors="coerce")
    rent["excluUseAr"] = pd.to_numeric(rent["excluUseAr"], errors="coerce")
    trade["excluUseAr"] = pd.to_numeric(trade["excluUseAr"], errors="coerce")
    rent["rounded_area"] = rent["excluUseAr"].round(1)
    trade["rounded_area"] = trade["excluUseAr"].round(1)

    # 전용면적이 겹치는 항목만 남김
    common_area = pd.merge(
        rent[["kaptCode", "bjdCode", "rounded_area"]],
        trade[["kaptCode", "bjdCode", "rounded_area"]],
        on=["kaptCode", "bjdCode", "rounded_area"]
    ).drop_duplicates()

    rent = pd.merge(rent, common_area, on=["kaptCode", "bjdCode", "rounded_area"])
    trade = pd.merge(trade, common_area, on=["kaptCode", "bjdCode", "rounded_area"])

    # 집계
    merged = pd.merge(trade, rent, on=["kaptCode", "bjdCode", "rounded_area"], suffixes=("_trade", "_rent"))
    grouped = merged.groupby(["kaptCode", "bjdCode"]).agg({
        "dealAmount": "mean",
        "deposit": "mean",
        "excluUseAr_trade": "mean"
    }).reset_index()

    grouped = grouped.rename(columns={
        "dealAmount": "평균_매매가",
        "deposit": "평균_전세가",
        "excluUseAr_trade": "전용면적"
    })
    grouped["갭차이"] = grouped["평균_매매가"] - grouped["평균_전세가"]

    # 세대수 붙이기
    final = pd.merge(grouped, apt_basic, on="kaptCode", how="left")

    # 정렬 및 상위 50개 출력
    # final = final.dropna().sort_values(by="갭차이").head(50)

    # 결과 확인
    pd.set_option('display.float_format', '{:,.0f}'.format)
    print(final)
    final.to_csv("전세-매매갭.csv", index=False, encoding="utf-8-sig")




def export_all_tables_to_csv(db_path="apt_data.db", output_dir="csv_output"):
    # 출력 폴더 생성
    os.makedirs(output_dir, exist_ok=True)

    # DB 연결
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 모든 테이블 목록 가져오기
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()

    print(f"📦 변환할 테이블 수: {len(tables)}")

    for table_name_tuple in tables:
        table_name = table_name_tuple[0]
        print(f"🔄 변환 중: {table_name}")
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        file_path = os.path.join(output_dir, f"{table_name}.csv")
        df.to_csv(file_path, index=False, encoding="utf-8-sig")

    conn.close()
    print(f"\n✅ 변환 완료! 폴더 경로: {os.path.abspath(output_dir)}")

# 실행
export_all_tables_to_csv("apt_data.db")

def save_to_db(df, db_path="apt_data.db", table_name="apt_data"):
    if df.empty:
        print(f"⛔ [{table_name}] 저장할 데이터가 없습니다.")
        return

    conn = sqlite3.connect(db_path)
    df.columns = df.columns.str.strip().str.replace(" ", "_").str.replace(r"[^\w]", "", regex=True)
    df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()
    print(f"✅ [{table_name}] {len(df)}건 저장 완료")


def load_from_db(db_path="apt_data.db", table_name="apt_data"):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df


def reset_table_schema(df, db_path="apt_data.db", table_name="apt_data"):
    if df.empty:
        print(f"⛔ [{table_name}] 초기화할 데이터가 없습니다.")
        return

    conn = sqlite3.connect(db_path)
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()
    df.columns = df.columns.str.strip().str.replace(" ", "_").str.replace(r"[^\w]", "", regex=True)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    print(f"🔄 [{table_name}] 테이블 초기화 및 저장 완료")
