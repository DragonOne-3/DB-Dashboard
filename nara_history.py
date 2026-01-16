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
    # 1. 수집 기간 설정 (2025년 1월 1일 ~ 현재)
    start_date = "20250101"
    end_date = datetime.now().strftime("%Y%m%d")
    
    # [사용자 요청] 계약명 기준 키워드만 사용
    contract_keywords = ['작전', '경계', '무인화', '국방', '군사', '부대']
    
    all_fetched_rows = []
    print(f"🚀 계약명 키워드 기준 수집 시작: {start_date} ~ {end_date}")

    # 2. 키워드별 수집 진행
    for kw in contract_keywords:
        params = {
            'serviceKey': API_KEY, 'pageNo': '1', 'numOfRows': '999',
            'inqryDiv': '1', 'type': 'xml', 
            'inqryBgnDate': start_date, 'inqryEndDate': end_date, 
            'cntrctNm': kw # 계약명 검색 파라미터
        }
        
        try:
            res = requests.get(API_URL, params=params, timeout=60)
            if res.status_code == 200:
                root = ET.fromstring(res.content)
                items = root.findall('.//item')
                print(f"📡 키워드 [{kw}] 검색 결과: {len(items)}건 발견")
                
                for item in items:
                    raw = {child.tag: child.text for child in item}
                    
                    cntrct_name = raw.get('cntrctNm', '')
                    
                    # 상수도 제외 로직 유지
                    if '상수도' in cntrct_name:
                        continue

                    # 수요기관명 정제
                    raw_demand = raw.get('dminsttList', '')
                    demand_parts = raw_demand.replace('[', '').replace(']', '').split('^')
                    clean_demand = demand_parts[2] if len(demand_parts) > 2 else raw_demand
                    
                    # 업체명 정제
                    raw_corp = raw.get('corpList', '')
                    corp_parts = raw_corp.replace('[', '').replace(']', '').split('^')
                    clean_corp = corp_parts[3] if len(corp_parts) > 3 else raw_corp

                    processed = {
                        '★가공_수요기관': clean_demand,
                        '★가공_계약명': cntrct_name,
                        '★가공_업체명': clean_corp,
                        '★가공_계약금액': int(raw.get('totCntrctAmt', 0)),
                        '계약일자': raw.get('cntrctDate', ''),
                        '착수일자': raw.get('stDate', ''),
                        '계약기간': raw.get('cntrctPrdNm', ''),
                        '총완수일자': raw.get('ttalScmpltDate') or raw.get('thtmScmpltDate') or '',
                        '계약상세정보URL': f"https://www.g2b.go.kr:8067/co/common/moveCntrctDetail.do?cntrctNo={raw.get('cntrctNo')}&cntrctOrdNo={raw.get('cntrctOrdNo')}"
                    }
                    all_fetched_rows.append(processed)
            
            # API 호출 간격 조절
            time.sleep(1)
            
        except Exception as e:
            print(f"❌ {kw} 수집 중 오류: {e}")

    # 3. 데이터 중복 제거 및 구글 시트 전송
    if all_fetched_rows:
        df = pd.DataFrame(all_fetched_rows)
        # 중복 제거
        df = df.drop_duplicates(subset=['★가공_수요기관', '★가공_계약명', '★가공_업체명'])

        try:
            sh = get_gs_client().open("나라장터_용역계약내역")
            ws = sh.get_worksheet(0)
            
            # 시트에 추가
            ws.append_rows(df.values.tolist(), value_input_option='USER_ENTERED')
            print(f"✨ 성공! 계약명 기준 데이터 총 {len(df)}건 추가 완료")
        except Exception as e:
            print(f"❌ 시트 저장 오류: {e}")
    else:
        print("ℹ️ 해당 기간 내 계약명 키워드에 맞는 데이터가 여전히 없습니다.")

if __name__ == "__main__":
    main()
