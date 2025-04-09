import sqlite3
import pandas as pd

def save_to_db(df, db_path="apt_trade.db", table_name="apt_trades"):
    if df.empty:
        print("⛔ 저장할 데이터가 없습니다. 테이블 생성을 생략합니다.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()

    df.columns = df.columns.str.strip().str.replace(" ", "_").str.replace(r"[^\w]", "", regex=True)

    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()



def load_from_db(db_path="apt_trade.db", table_name="apt_trades"):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df



def save_detail_to_db(df, db_path="apt_complex.db", table_name="apt_detail"):
    if df.empty:
        print("⛔ 저장할 데이터가 없습니다.")
        return

    conn = sqlite3.connect(db_path)
    df.columns = df.columns.str.strip().str.replace(" ", "_").str.replace(r"[^\w]", "", regex=True)
    df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()