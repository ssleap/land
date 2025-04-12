# landapi.py (V3 JSON 기반 + returnType=JSON 추가)
import requests
import pandas as pd
import xml.etree.ElementTree as ET

# 아파트 매매 실거래가 상세 자료
def fetch_apt_trade_detail_v3(service_key, lawd_cd, deal_ymd):
    url = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    params = {
        "serviceKey": service_key,
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ymd,
        "returnType": "XML"
    }

    response = requests.get(url, params=params, verify=False)
    
    try:
        tree = ET.fromstring(response.content)
        items = tree.findall(".//item")
        rows = []

        for item in items:
            row = {
                "sggCd": item.findtext("sggCd"),
                "umdCd": item.findtext("umdCd"),
                "landCd": item.findtext("landCd"),
                "bonbun": item.findtext("bonbun"),
                "bubun": item.findtext("bubun"),
                "roadNm": item.findtext("roadNm"),
                "roadNmSggCd": item.findtext("roadNmSggCd"),
                "roadNmCd": item.findtext("roadNmCd"),
                "roadNmSeq": item.findtext("roadNmSeq"),
                "roadNmbCd": item.findtext("roadNmbCd"),
                "roadNmBonbun": item.findtext("roadNmBonbun"),
                "roadNmBubun": item.findtext("roadNmBubun"),
                "umdNm": item.findtext("umdNm"),
                "aptNm": item.findtext("aptNm"),
                "jibun": item.findtext("jibun"),
                "excluUseAr": item.findtext("excluUseAr"),
                "dealYear": item.findtext("dealYear"),
                "dealMonth": item.findtext("dealMonth"),
                "dealDay": item.findtext("dealDay"),
                "dealAmount": item.findtext("dealAmount"),
                "floor": item.findtext("floor"),
                "buildYear": item.findtext("buildYear"),
                "aptSeq": item.findtext("aptSeq"),
                "cdealType": item.findtext("cdealType"),
                "cdealDay": item.findtext("cdealDay"),
                "dealingGbn": item.findtext("dealingGbn"),
                "estateAgentSggNm": item.findtext("estateAgentSggNm"),
                "rgstDate": item.findtext("rgstDate"),
                "aptDong": item.findtext("aptDong"),
                "slerGbn": item.findtext("slerGbn"),
                "buyerGbn": item.findtext("buyerGbn"),
                "landLeaseholdGbn": item.findtext("landLeaseholdGbn"),
            }
            rows.append(row)

        return pd.DataFrame(rows)
    
    except ET.ParseError as e:
        print("❌ XML 파싱 오류:", e)
        return pd.DataFrame()


# 5. 전국 단지 목록 전체 조회 (getTotalAptList3)
def fetch_all_total_apt_list(service_key):
    all_rows = []
    page = 1

    while True:
        url = "http://apis.data.go.kr/1613000/AptListService3/getTotalAptList3"
        params = {
            "serviceKey": service_key,
            "returnType": "JSON",
            "pageNo": page,
            "numOfRows": 100
        }
        response = requests.get(url, params=params, verify=False)
        data = response.json()
        body = data.get("response", {}).get("body", {})
        items = body.get("items", [])
        
        if not items:
            break

        all_rows.extend(items)
        page += 1

    return pd.DataFrame(all_rows)

# 전월세 실거래가 데이터
def fetch_apt_rent_data_v3(service_key, lawd_cd, deal_ymd):
    url = "http://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"
    params = {
        "serviceKey": service_key,
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ymd,
        "returnType": "XML"
    }

    response = requests.get(url, params=params, verify=False)
    try:
        tree = ET.fromstring(response.content)
        items = tree.findall(".//item")
        rows = []

        for item in items:
            row = {
                "aptNm": item.findtext("aptNm"),
                "buildYear": item.findtext("buildYear"),
                "contractTerm": item.findtext("contractTerm"),
                "contractType": item.findtext("contractType"),
                "dealYear": item.findtext("dealYear"),
                "dealMonth": item.findtext("dealMonth"),
                "dealDay": item.findtext("dealDay"),
                "deposit": item.findtext("deposit"),
                "monthlyRent": item.findtext("monthlyRent"),
                "preDeposit": item.findtext("preDeposit"),
                "preMonthlyRent": item.findtext("preMonthlyRent"),
                "excluUseAr": item.findtext("excluUseAr"),
                "floor": item.findtext("floor"),
                "jibun": item.findtext("jibun"),
                "sggCd": item.findtext("sggCd"),
                "umdNm": item.findtext("umdNm"),
                "useRRRight": item.findtext("useRRRight")
            }
            rows.append(row)

        return pd.DataFrame(rows)

    except ET.ParseError as e:
        print("❌ XML 파싱 오류:", e)
        return pd.DataFrame()


################## 이 이하는 아직 쓸모없는 코드 #####################
# 1. 공동주택 기본 정보 (getAphusBassInfoV3)
def fetch_apt_basic_info_v3(service_key, kaptCode):
    url = "http://apis.data.go.kr/1611000/AptBasisInfoServiceV3/getAphusBassInfoV3"
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
    url = "http://apis.data.go.kr/1611000/AptBasisInfoServiceV3/getAphusDtlInfoV3"
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




# 4. 단지 목록 조회 (getSigunguAptList3)
def fetch_apt_list_by_sigungu_v3(service_key, sigunguCd, bjdongCd):
    url = "http://apis.data.go.kr/1611000/AptListService3/getSigunguAptList3"
    params = {
        "serviceKey": service_key,
        "sigunguCd": sigunguCd,
        "bjdongCd": bjdongCd,
        "returnType": "JSON",
        "pageNo": 1,
        "numOfRows": 100
    }
    response = requests.get(url, params=params, verify=False)
    
    try:
        data = response.json()
        items = data.get("response", {}).get("body", {}).get("items", [])
        if not items:
            print(f"📭 {sigunguCd}-{bjdongCd} 에 대한 아파트 목록이 없습니다.")
            return pd.DataFrame()
        return pd.DataFrame(items)
    
    except Exception as e:
        print("❌ JSON 파싱 오류:", e)
        return pd.DataFrame()






