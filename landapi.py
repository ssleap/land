# landapi.py (V3 JSON 기반 + returnType=JSON 추가)
import requests
import pandas as pd

# 1. 공동주택 기본 정보 (getAphusBassInfoV3)
def fetch_apt_basic_info_v3(service_key, kaptCode):
    url = "https://apis.data.go.kr/1611000/AptBasisInfoServiceV3/getAphusBassInfoV3"
    params = {
        "serviceKey": service_key,
        "kaptCode": kaptCode,
        "returnType": "JSON"
    }
    response = requests.get(url, params=params, verify=False)
    data = response.json()
    item = data.get("body", {}).get("item")
    if not item:
        return pd.DataFrame()
    return pd.DataFrame([item])


# 2. 공동주택 상세 정보 (getAphusDtlInfoV3)
def fetch_apt_detail_info_v3(service_key, kaptCode):
    url = "https://apis.data.go.kr/1611000/AptBasisInfoServiceV3/getAphusDtlInfoV3"
    params = {
        "serviceKey": service_key,
        "kaptCode": kaptCode,
        "returnType": "JSON"
    }
    response = requests.get(url, params=params, verify=False)
    data = response.json()
    item = data.get("body", {}).get("item")
    if not item:
        return pd.DataFrame()
    return pd.DataFrame([item])


# 3. 아파트 매매 실거래가 상세 자료 (getRTMSDataSvcAptTradeDev)
def fetch_apt_trade_detail_v3(service_key, lawd_cd, deal_ymd):
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    params = {
        "serviceKey": service_key,
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ymd,
        "returnType": "XML"
    }
    response = requests.get(url, params=params, verify=False)
    data = response.json()
    items = data.get("body", {}).get("items", {}).get("item")
    if not items:
        return pd.DataFrame()
    if isinstance(items, dict):
        items = [items]
    return pd.DataFrame(items)


# 4. 단지 목록 조회 (getSigunguAptList3)
def fetch_apt_list_by_sigungu_v3(service_key, sigunguCd, bjdongCd):
    url = "https://apis.data.go.kr/1611000/AptListService3/getSigunguAptList3"
    params = {
        "serviceKey": service_key,
        "sigunguCd": sigunguCd,
        "bjdongCd": bjdongCd,
        "returnType": "JSON"
    }
    response = requests.get(url, params=params, verify=False)
    data = response.json()
    items = data.get("body", {}).get("items")
    if not items:
        return pd.DataFrame()
    return pd.DataFrame(items)


# 5. 전국 단지 목록 전체 조회 (getTotalAptList3)
def fetch_total_apt_list_v3(service_key):
    url = "https://apis.data.go.kr/1611000/AptListService3/getTotalAptList3"
    params = {
        "serviceKey": service_key,
        "returnType": "JSON"
    }
    response = requests.get(url, params=params, verify=False)
    data = response.json()
    items = data.get("body", {}).get("items")
    if not items:
        return pd.DataFrame()
    return pd.DataFrame(items)