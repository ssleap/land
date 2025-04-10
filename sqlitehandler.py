# sqlitehandler.py
import sqlite3
import pandas as pd


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
