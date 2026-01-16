import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import time
from pytimekr import pytimekr  # 공휴일 체크를 위해 추가

# --- 설정 ---
API_KEY = os.environ.get('DATA_GO_KR_API_KEY')
API_URL = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'
"""
def get_target_date():
    
    now = datetime.utcnow() + timedelta(hours=9)
    target = now - timedelta(days=1)
    
    holidays = pytimekr.holidays(year=target.year)
    
    while target.weekday() >= 5 or target.date() in holidays:
        target -= timedelta(days=1)
        
    return target
"""
def get_gs_client():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    creds_dict = json.loads(auth_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def main():
    # 1. 수집 대상 날짜 계산
    #target_dt = get_target_date()
    #target_str = "2025-01-01"
    #display_str = "2025-06-30"
    
    # 키워드 리스트: '방위' 추가
    keywords = ['국방', '부대', '작전', '경계', '방위', '군사', '무인화', '사령부', '군대']
    all_fetched_rows = []

    # 2. 키워드별 수집
    for kw in keywords:
        params = {
            'serviceKey': API_KEY, 'pageNo': '1', 'numOfRows': '999',
            'inqryDiv': '1', 'type': 'xml', 
            'inqryBgnDate': '20250101', 'inqryEndDate': '20250603', 
            'cntrctNm': kw
        }
        try:
            res = requests.get(API_URL, params=params, timeout=60)
            if res.status_code == 200:
                root = ET.fromstring(res.content)
                for item in root.findall('.//item'):
                    raw = {child.tag: child.text for child in item}
                    
                    # 수요기관 및 업체명 정제
                    raw_demand = raw.get('dminsttList', '')
                    demand_parts = raw_demand.replace('[', '').replace(']', '').split('^')
                    clean_demand = demand_parts[2] if len(demand_parts) > 2 else raw_demand
                    
                    raw_corp = raw.get('corpList', '')
                    corp_parts = raw_corp.replace('[', '').replace(']', '').split('^')
                    clean_corp = corp_parts[3] if len(corp_parts) > 3 else raw_corp

                    processed = {
                        '★가공_계약일': '',
                        '★가공_착수일': raw.get('stDate', '-'),
                        '★가공_만료일': raw.get('ttalScmpltDate') or raw.get('thtmScmpltDate') or '-',
                        '★가공_수요기관': clean_demand,
                        '★가공_계약명': raw.get('cntrctNm', ''),
                        '★가공_업체명': clean_corp,
                        '★가공_계약금액': int(raw.get('totCntrctAmt', 0))
                    }
                    processed.update(raw)
                    all_fetched_rows.append(processed)
        except Exception as e:
            print(f"❌ {kw} 수집 중 오류: {e}")
            continue
        time.sleep(0.5)

    # 3. 데이터 중복 제거 및 필터링
    if all_fetched_rows:
        df = pd.DataFrame(all_fetched_rows)
        
        # 계약번호(cntrctNo)가 있다면 중복 제거 (없을 경우 전체 컬럼 기준)
        if 'cntrctNo' in df.columns:
            df = df.drop_duplicates(subset=['cntrctNo'])
        else:
            df = df.drop_duplicates()

        # 4. 구글 시트 저장
            try:
                sh = get_gs_client().open("나라장터_용역계약내역")
                ws = sh.get_worksheet(0)
                
                ws.append_rows(df.values.tolist(), value_input_option='RAW')
                print(f"✅ 'a' 데이터 {len(df)}건(중복제외) 추가 완료")
            except Exception as e:
                print(f"❌ 시트 저장 중 오류: {e}")
    else:
        print(f"ℹ️ {display_str}에 해당하는 수집 데이터가 없습니다.")

if __name__ == "__main__":
    main()
