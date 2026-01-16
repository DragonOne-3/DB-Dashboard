import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import time

# --- 환경 변수 설정 ---
API_KEY = os.environ.get('DATA_GO_KR_API_KEY')
# 용역계약 목록 조회 API (XML 전용)
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
    
    # 요청하신 국방/군사 및 기존 키워드 통합
    # '유지'는 필수로 포함하고 나머지는 OR 조건이 되도록 구성
    keywords = ['국방 유지', '군 부대 유지', '작전 유지', '경계 유지', '방위 유지', 'CCTV 유지', '통합관제 유지']
    
    all_fetched_rows = []

    print(f"🚀 데이터 수집 시작: {start_date} ~ {end_date}")

    # 2. 키워드별 수집 (차수 계약 대응을 위해 차례대로 호출)
    for kw in keywords:
        params = {
            'serviceKey': API_KEY,
            'pageNo': '1',
            'numOfRows': '999',
            'inqryDiv': '1',  # 계약일자 기준
            'type': 'xml', 
            'inqryBgnDate': start_date,
            'inqryEndDate': end_date, 
            'cntrctNm': kw
        }
        
        try:
            print(f"📡 키워드 [{kw}] 수집 중...")
            res = requests.get(API_URL, params=params, timeout=60)
            if res.status_code == 200:
                root = ET.fromstring(res.content)
                items = root.findall('.//item')
                
                for item in items:
                    raw = {child.tag: child.text for child in item}
                    
                    # 수요기관 및 업체명 정제 (사용자 제공 로직)
                    raw_demand = raw.get('dminsttList', '')
                    demand_parts = raw_demand.replace('[', '').replace(']', '').split('^')
                    clean_demand = demand_parts[2] if len(demand_parts) > 2 else raw_demand
                    
                    raw_corp = raw.get('corpList', '')
                    corp_parts = raw_corp.replace('[', '').replace(']', '').split('^')
                    clean_corp = corp_parts[3] if len(corp_parts) > 3 else raw_corp

                    # 대시보드 시트 컬럼 구조에 맞게 가공
                    processed = {
                        '★가공_수요기관': clean_demand,
                        '★가공_계약명': raw.get('cntrctNm', ''),
                        '★가공_업체명': clean_corp,
                        '★가공_계약금액': int(raw.get('totCntrctAmt', 0)),
                        '계약일자': raw.get('cntrctDate', ''),
                        '착수일자': raw.get('stDate', ''),
                        '계약기간': raw.get('cntrctPrdNm', ''), # 기간 텍스트
                        '총완수일자': raw.get('ttalScmpltDate') or raw.get('thtmScmpltDate') or '',
                        '계약상세정보URL': f"https://www.g2b.go.kr:8067/co/common/moveCntrctDetail.do?cntrctNo={raw.get('cntrctNo')}&cntrctOrdNo={raw.get('cntrctOrdNo')}"
                    }
                    all_fetched_rows.append(processed)
            
            time.sleep(1) # API 부하 방지
            
        except Exception as e:
            print(f"❌ {kw} 수집 중 오류: {e}")
            continue

    # 3. 데이터 중복 제거 및 시트 저장
    if all_fetched_rows:
        df = pd.DataFrame(all_fetched_rows)
        # 중복 계약 제거
        df = df.drop_duplicates(subset=['★가공_수요기관', '★가공_계약명', '★가공_업체명'])

        try:
            client = get_gs_client()
            sh = client.open("나라장터_용역계약내역")
            ws = sh.get_worksheet(0)
            
            # 시트의 기존 헤더 순서를 알고 있다면 그에 맞춰 values 리스트 생성 필요
            # 여기서는 API에서 뽑은 순서대로 리스트화하여 추가함
            data_list = df.values.tolist()
            ws.append_rows(data_list, value_input_option='USER_ENTERED')
            
            print(f"✅ 총 {len(df)}건의 데이터가 시트에 추가되었습니다.")
        except Exception as e:
            print(f"❌ 시트 저장 오류: {e}")
    else:
        print("ℹ️ 조건에 맞는 수집 데이터가 없습니다.")

if __name__ == "__main__":
    main()
