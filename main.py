from landapi import fetch_apt_trade_data
from sqlitehandler import load_from_db, save_to_db

# 예시 실행
# Q7uO9oY26rqekbCW6lMPeBjSLkFlIGRWJr8Px%2FKwd1OspHoWhYGIbUN1SgH7l8ic1swKPDoYqN%2FmYAPTuL6vnA%3D%3D
# Q7uO9oY26rqekbCW6lMPeBjSLkFlIGRWJr8Px/Kwd1OspHoWhYGIbUN1SgH7l8ic1swKPDoYqN/mYAPTuL6vnA==
service_key = "Q7uO9oY26rqekbCW6lMPeBjSLkFlIGRWJr8Px/Kwd1OspHoWhYGIbUN1SgH7l8ic1swKPDoYqN/mYAPTuL6vnA=="
df = fetch_apt_trade_data(service_key, "11110", "202503")
print(f"데이터 건수: {len(df)}")
print(df.head())

save_to_db(df)
result_df = load_from_db()
print(result_df.head())