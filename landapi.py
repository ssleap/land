import requests
import xml.etree.ElementTree as ET
import pandas as pd

def fetch_apt_trade_data(service_key, lawd_cd, deal_ym):
    url = "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade"

    params = {
        'LAWD_CD': lawd_cd,         # 지역코드: 서울=11110, 수원=41110 등
        'DEAL_YMD': deal_ym,        # 년월: 202503 (2025년 3월)
        'serviceKey': service_key   # 본인의 인증키
    }

    response = requests.get(url, params=params, verify=False)
    tree = ET.fromstring(response.content)

    rows = []
    for item in tree.iter("item"):
        row = {
            '거래금액': item.findtext("거래금액").strip().replace(",", ""),
            '건축년도': item.findtext("건축년도"),
            '년': item.findtext("년"),
            '월': item.findtext("월"),
            '일': item.findtext("일"),
            '법정동': item.findtext("법정동"),
            '아파트': item.findtext("아파트"),
            '전용면적': item.findtext("전용면적"),
            '지번': item.findtext("지번"),
            '층': item.findtext("층"),
            '지역코드': lawd_cd,
        }
        rows.append(row)

    return pd.DataFrame(rows)



def fetch_apt_detail_info(service_key, kaptCode):
    url = "https://apis.data.go.kr/1611000/AptListService2/getAphusDtlInfo"

    params = {
        "serviceKey": service_key,
        "kaptCode": kaptCode
    }

    response = requests.get(url, params=params)
    tree = ET.fromstring(response.content)

    item = tree.find(".//item")
    if item is None:
        return pd.DataFrame()

    row = {
        "단지코드": kaptCode,
        "전기방식": item.findtext("elecType"),
        "가스방식": item.findtext("gasType"),
        "난방방식": item.findtext("heatType"),
        "구조": item.findtext("kaptUsedStrctCd"),
        "주차대수": item.findtext("kaptdPcnt"),
        "층수정보": item.findtext("kaptFloor"),
        "엘리베이터유무": item.findtext("elvtYn")
    }

    return pd.DataFrame([row])
