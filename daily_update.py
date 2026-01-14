import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import time

# --- 설정 ---
API_KEY = os.environ.get('DATA_GO_KR_API_KEY')
API_URL = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'

def get_gs_client():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    creds_dict = json.loads(auth_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def main():
    # 1. 어제 날짜 설정
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    keywords = ['CCTV', '통합관제', '주차관리', '영상감시장치', '영상정보처리기기']
    new_rows = []

    # 2. 키워드별 수집
    for kw in keywords:
        params = {
            'serviceKey': API_KEY, 'pageNo': '1', 'numOfRows': '999',
            'inqryDiv': '1', 'type': 'xml', 'inqryBgnDate': yesterday, 'inqryEndDate': yesterday, 'cntrctNm': kw
        }
        try:
            res = requests.get(API_URL, params=params, timeout=60)
            root = ET.fromstring(res.content)
            for item in root.findall('.//item'):
                raw = {child.tag: child.text for child in item}
                # 가공 필드 생성 (기존 수집 형식 유지)
                processed = {
                    '★가공_계약일': f"{yesterday[:4]}-{yesterday[4:6]}-{yesterday[6:]}",
                    '★가공_착수일': raw.get('stDate', '-'),
                    '★가공_만료일': raw.get('ttalScmpltDate') or raw.get('thtmScmpltDate') or '-',
                    '★가공_수요기관': raw.get('dminsttList', ''),
                    '★가공_계약명': raw.get('cntrctNm', ''),
                    '★가공_업체명': raw.get('corpList', ''),
                    '★가공_계약금액': int(raw.get('totCntrctAmt', 0))
                }
                processed.update(raw)
                new_rows.append(processed)
        except: continue

    # 3. 구글 시트 하단에 추가 (Append)
    if new_rows:
        sh = get_gs_client().open("나라장터_용역계약내역")
        ws = sh.get_worksheet(0)
        ws.append_rows(pd.DataFrame(new_rows).values.tolist(), value_input_option='RAW')
        print(f"✅ {yesterday} 데이터 {len(new_rows)}건 추가 완료")

if __name__ == "__main__":
    main()
