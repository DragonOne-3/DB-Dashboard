import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import time

# --- 1. 설정 및 인증 ---
API_KEY = os.environ.get('DATA_GO_KR_API_KEY')
API_URL = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'

def get_gs_client():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    creds_dict = json.loads(auth_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def main():
    # 수집 시작일(2025-01-01)부터 오늘까지 설정
    start_date = datetime(2025, 1, 1)
    end_date = datetime.now()
    
    # [사용자 지정 키워드] 국방/군사 관련만 집중
    keywords = ['국방', '부대', '작전', '경계', '방위', '군사', '무인화', '사령부', '군대']
    all_fetched_rows = []

    # 날짜 구간을 15일 단위로 끊어서 안정적으로 수집 (나라장터 API 부하 방지)
    current_start = start_date
    while current_start < end_date:
        current_end = current_start + timedelta(days=15)
        if current_end > end_date:
            current_end = end_date
        
        s_str = current_start.strftime("%Y%m%d")
        e_str = current_end.strftime("%Y%m%d")
        
        print(f"📅 구간 조회 중: {s_str} ~ {e_str}")

        # 키워드별 순회
        for kw in keywords:
            params = {
                'serviceKey': API_KEY,
                'pageNo': '1',
                'numOfRows': '999',
                'inqryDiv': '1',  # 계약일자 기준
                'type': 'xml',    # 사용자님 코드 방식 그대로 XML 사용
                'inqryBgnDate': s_str,
                'inqryEndDate': e_str,
                'cntrctNm': kw
            }
            
            try:
                res = requests.get(API_URL, params=params, timeout=60)
                if res.status_code == 200:
                    root = ET.fromstring(res.content)
                    items = root.findall('.//item')
                    
                    for item in items:
                        raw = {child.tag: child.text for child in item}
                        
                        # [사용자님 코드의 정제 로직 그대로 적용]
                        raw_demand = raw.get('dminsttList', '')
                        demand_parts = raw_demand.replace('[', '').replace(']', '').split('^')
                        clean_demand = demand_parts[2] if len(demand_parts) > 2 else raw_demand
                        
                        raw_corp = raw.get('corpList', '')
                        corp_parts = raw_corp.replace('[', '').replace(']', '').split('^')
                        clean_corp = corp_parts[3] if len(corp_parts) > 3 else raw_corp

                        # 대시보드 시트 컬럼 구조에 맞춘 리스트 생성
                        processed = [
                            clean_demand,                                # ★가공_수요기관
                            raw.get('cntrctNm', ''),                     # ★가공_계약명
                            clean_corp,                                  # ★가공_업체명
                            int(raw.get('totCntrctAmt', 0)),             # ★가공_계약금액
                            raw.get('cntrctDate', ''),                   # 계약일자
                            raw.get('stDate', '-'),                      # 착수일자
                            raw.get('cntrctPrdNm', '-'),                 # 계약기간
                            raw.get('ttalScmpltDate') or raw.get('thtmScmpltDate') or '-', # 총완수일자
                            f"https://www.g2b.go.kr:8067/co/common/moveCntrctDetail.do?cntrctNo={raw.get('cntrctNo')}&cntrctOrdNo={raw.get('cntrctOrdNo', '00')}"
                        ]
                        all_fetched_rows.append(processed)
                
                time.sleep(0.5) # API 매너 타임
                
            except Exception as e:
                print(f"❌ {kw} 수집 중 오류: {e}")
                continue
        
        # 다음 15일 구간으로 이동
        current_start = current_end + timedelta(days=1)

    # 4. 데이터 중복 제거 및 구글 시트 저장
    if all_fetched_rows:
        df = pd.DataFrame(all_fetched_rows)
        # 동일 계약 중복 제거
        df = df.drop_duplicates()
        
        try:
            sh = get_gs_client().open("나라장터_용역계약내역")
            ws = sh.get_worksheet(0)
            
            # 리스트로 변환하여 시트 맨 아래에 추가
            ws.append_rows(df.values.tolist(), value_input_option='USER_ENTERED')
            print(f"✨ 완료! 2025년 국방 데이터 총 {len(df)}건을 시트에 추가했습니다.")
        except Exception as e:
            print(f"❌ 시트 저장 오류: {e}")
    else:
        print("ℹ️ 조건에 맞는 데이터가 없습니다.")

if __name__ == "__main__":
    main()
