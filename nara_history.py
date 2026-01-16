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
    
    # [사용자 지정 키워드]
    agency_keywords = ['국방', '군대', '부대', '사령부', '방위']
    contract_keywords = ['작전', '경계', '무인화', '국방', '군사', '부대']
    
    # API 검색용 통합 키워드 리스트 (중복 제거)
    search_keywords = list(set(agency_keywords + contract_keywords))

    all_fetched_rows = []
    print(f"🚀 요청하신 키워드로 수집 시작: {start_date} ~ {end_date}")

    # 2. 키워드별 수집 진행
    for kw in search_keywords:
        params = {
            'serviceKey': API_KEY, 'pageNo': '1', 'numOfRows': '999',
            'inqryDiv': '1', 'type': 'xml', 
            'inqryBgnDate': start_date, 'inqryEndDate': end_date, 
            'cntrctNm': kw
        }
        
        try:
            res = requests.get(API_URL, params=params, timeout=60)
            if res.status_code == 200:
                root = ET.fromstring(res.content)
                for item in root.findall('.//item'):
                    raw = {child.tag: child.text for child in item}
                    
                    # 수요기관명 정제
                    raw_demand = raw.get('dminsttList', '')
                    demand_parts = raw_demand.replace('[', '').replace(']', '').split('^')
                    clean_demand = demand_parts[2] if len(demand_parts) > 2 else raw_demand
                    
                    cntrct_name = raw.get('cntrctNm', '')
                    
                    # [사용자 요청 필터링 로직]
                    is_target_agency = any(k in clean_demand for k in agency_keywords)
                    is_target_contract = any(k in cntrct_name for k in contract_keywords)
                    is_excluded = '상수도' in cntrct_name

                    if (is_target_agency or is_target_contract) and not is_excluded:
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
            time.sleep(0.5)
        except Exception as e:
            print(f"❌ {kw} 수집 중 오류: {e}")

    # 3. 데이터 중복 제거 및 구글 시트 전송
    if all_fetched_rows:
        df = pd.DataFrame(all_fetched_rows)
        # 중복 제거 (수요기관, 계약명, 업체명이 모두 같은 경우)
        df = df.drop_duplicates(subset=['★가공_수요기관', '★가공_계약명', '★가공_업체명'])

        try:
            sh = get_gs_client().open("나라장터_용역계약내역")
            ws = sh.get_worksheet(0)
            
            # 리스트로 변환하여 시트 추가
            ws.append_rows(df.values.tolist(), value_input_option='USER_ENTERED')
            print(f"✅ 요청하신 키워드 데이터 총 {len(df)}건 추가 완료")
        except Exception as e:
            print(f"❌ 시트 저장 오류: {e}")
    else:
        print("ℹ️ 해당 키워드에 맞는 데이터가 발견되지 않았습니다.")

if __name__ == "__main__":
    main()
