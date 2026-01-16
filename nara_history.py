import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import time

# --- 설정 (기존과 동일) ---
API_KEY = os.environ.get('DATA_GO_KR_API_KEY')
API_URL = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'

def get_gs_client():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    creds_dict = json.loads(auth_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def main():
    # 1. 수집 대상 기간 설정 (2025년 1월 1일 ~ 오늘)
    start_dt = datetime(2025, 1, 1)
    end_dt = datetime.now()
    
    # [사용자 요청 키워드]
    keywords = ['국방', '부대', '작전', '경계', '방위', '군사', '무인화', '사령부', '군대']
    all_fetched_rows = []

    # 2. 날짜별 순회 수집 (기존 코드의 루프화)
    current_dt = start_dt
    while current_dt <= end_dt:
        target_str = current_dt.strftime("%Y%m%d")
        display_str = current_dt.strftime("%Y-%m-%d")
        print(f"📡 {display_str} 데이터 조회 중...")

        for kw in keywords:
            params = {
                'serviceKey': API_KEY, 
                'pageNo': '1', 
                'numOfRows': '999',
                'inqryDiv': '1', 
                'type': 'xml',  # 기존 코드와 동일한 XML 방식
                'inqryBgnDate': target_str, 
                'inqryEndDate': target_str, 
                'cntrctNm': kw
            }
            try:
                res = requests.get(API_URL, params=params, timeout=60)
                if res.status_code == 200:
                    root = ET.fromstring(res.content)
                    for item in root.findall('.//item'):
                        raw = {child.tag: child.text for child in item}
                        
                        # 수요기관 및 업체명 정제 (사용자님 로직 100% 동일)
                        raw_demand = raw.get('dminsttList', '')
                        demand_parts = raw_demand.replace('[', '').replace(']', '').split('^')
                        clean_demand = demand_parts[2] if len(demand_parts) > 2 else raw_demand
                        
                        raw_corp = raw.get('corpList', '')
                        corp_parts = raw_corp.replace('[', '').replace(']', '').split('^')
                        clean_corp = corp_parts[3] if len(corp_parts) > 3 else raw_corp

                        processed = {
                            '★가공_계약일': display_str,
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
                # 에러 발생 시 로그만 찍고 다음으로 넘어감
                print(f"❌ {display_str} [{kw}] 오류: {e}")
                continue
        
        # 날짜 변경 및 서버 부하 방지용 짧은 휴식
        current_dt += timedelta(days=1)
        time.sleep(0.1)

    # 3. 데이터 중복 제거 및 시트 저장
    if all_fetched_rows:
        df = pd.DataFrame(all_fetched_rows)
        # 중복 제거 (계약번호 기준)
        if 'cntrctNo' in df.columns:
            df = df.drop_duplicates(subset=['cntrctNo'])
        else:
            df = df.drop_duplicates()

        try:
            client = get_gs_client()
            sh = client.open("나라장터_용역계약내역")
            ws = sh.get_worksheet(0)
            
            # 리스트로 변환하여 시트 하단에 추가
            ws.append_rows(df.values.tolist(), value_input_option='RAW')
            print(f"✅ 2025년 데이터 총 {len(df)}건 수집 및 시트 축적 완료!")
        except Exception as e:
            print(f"❌ 시트 저장 오류: {e}")
    else:
        print(f"ℹ️ {start_dt.strftime('%Y%m%d')} ~ {end_dt.strftime('%Y%m%d')} 사이 데이터가 없습니다.")

if __name__ == "__main__":
    main()
