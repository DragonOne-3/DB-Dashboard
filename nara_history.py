import requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import time

# --- 설정 ---
API_KEY = os.environ.get('DATA_GO_KR_API_KEY')
API_URL = 'http://apis.data.go.kr/1230000/Service_7/getServcCntrctInfoService01'

def get_gs_client():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    creds_dict = json.loads(auth_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def main():
    # 1. 수집 기간 설정 (2025-01-01 ~ 현재)
    start_dt = datetime(2025, 1, 1)
    end_dt = datetime.now()
    
    # [사용자 지정 키워드]
    contract_keywords = ['작전', '경계', '무인화', '국방', '군사', '부대']
    
    all_fetched_rows = []
    
    # 2. 날짜를 3개월 단위로 쪼개기
    current_start = start_dt
    while current_start < end_dt:
        # 3개월 뒤 계산 (약 90일)
        current_end = current_start + timedelta(days=90)
        if current_end > end_dt:
            current_end = end_dt
            
        s_str = current_start.strftime("%Y%m%d")
        e_str = current_end.strftime("%Y%m%d")
        
        print(f"📅 구간 수집 시작: {s_str} ~ {e_str}")
        
        # 해당 구간에서 페이지별로 수집
        for page in range(1, 11): # 구간별 최대 약 1만건까지 확인
            params = {
                'serviceKey': API_KEY,
                'type': 'json',
                'numOfRows': '999',
                'pageNo': str(page),
                'inqryBgnDt': s_str,
                'inqryEndDt': e_str,
                'inqryDiv': '1'
            }
            
            try:
                res = requests.get(API_URL, params=params, timeout=60)
                res_data = res.json()
                items = res_data.get('response', {}).get('body', {}).get('items', [])
                
                if not items:
                    break
                    
                for item in items:
                    cntrct_name = item.get('cntrctNm', '')
                    
                    # 키워드 필터링 (상수도 제외 및 사용자 키워드 포함)
                    if any(kw in cntrct_name for kw in contract_keywords) and '상수도' not in cntrct_name:
                        processed = [
                            item.get('orderInsttNm', ''), # ★가공_수요기관
                            cntrct_name,                   # ★가공_계약명
                            item.get('mainEntrpsNm', '-'), # ★가공_업체명
                            int(item.get('cntrctAmt', 0)), # ★가공_계약금액
                            item.get('cntrctDate', ''),    # 계약일자
                            item.get('strtDate', '-'),     # 착수일자
                            item.get('cntrctPrdNm', '-'),  # 계약기간
                            item.get('totScmpltDate', '') or item.get('endDate', ''), # 총완수일자
                            f"https://www.g2b.go.kr:8067/co/common/moveCntrctDetail.do?cntrctNo={item.get('cntrctNo')}&cntrctOrdNo={item.get('cntrctOrdNo', '00')}"
                        ]
                        all_fetched_rows.append(processed)
                
                time.sleep(0.5) # API 매너 타임
            except Exception as e:
                print(f"❌ {s_str} 구간 처리 중 오류: {e}")
                break
        
        # 다음 구간으로 이동 (이전 끝 날짜의 다음 날부터)
        current_start = current_end + timedelta(days=1)

    # 3. 데이터 중복 제거 및 구글 시트 저장
    if all_fetched_rows:
        # 중복 제거 (리스트를 튜플로 변환하여 set으로 중복 체크 후 다시 리스트로)
        unique_rows = list(map(list, set(map(tuple, all_fetched_rows))))
        
        try:
            sh = get_gs_client().open("나라장터_용역계약내역")
            ws = sh.get_worksheet(0)
            ws.append_rows(unique_rows, value_input_option='USER_ENTERED')
            print(f"✨ 전체 수집 완료! 총 {len(unique_rows)}건의 데이터가 추가되었습니다.")
        except Exception as e:
            print(f"❌ 시트 저장 중 오류: {e}")
    else:
        print("ℹ️ 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    main()
