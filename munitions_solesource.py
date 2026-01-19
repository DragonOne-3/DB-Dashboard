import os
import json
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import time
from concurrent.futures import ThreadPoolExecutor

# 환경 변수 로드
SERVICE_KEY = os.environ['DATA_GO_KR_API_KEY']
GOOGLE_AUTH_JSON = os.environ['GOOGLE_AUTH_JSON']

def fetch_period(date_range):
    """특정 7일 구간의 공개수의공고 데이터를 수집"""
    start_date, end_date = date_range
    url = 'http://openapi.d2b.go.kr/openapi/service/BidPblancInfoService/getDmstcOthbcVltrnNtatPlanList'
    all_items = []
    page_no = 1
    
    for attempt in range(3): # 재시도 3회
        try:
            while True:
                params = {
                    'serviceKey': SERVICE_KEY,
                    'orntCode': 'EGC',
                    'prqudoPresentnClosDateBegin': start_date, # 신청마감일 시작
                    'prqudoPresentnClosDateEnd': end_date,     # 신청마감일 종료
                    'numOfRows': '500',
                    'pageNo': str(page_no)
                }
                
                response = requests.get(url, params=params, timeout=60)
                if response.status_code != 200: break

                root = ET.fromstring(response.content)
                items = root.findall('.//item')
                
                if not items: break
                    
                for item in items:
                    all_items.append({child.tag: child.text for child in item})
                
                total_element = root.find('.//totalCount')
                if total_element is not None:
                    total_count = int(total_element.text)
                    if len(all_items) >= total_count: break
                    page_no += 1
                else:
                    break
            
            if all_items or page_no == 1: break
                
        except Exception as e:
            print(f"  [재시도 {attempt+1}] {start_date} 구간 오류: {e}")
            time.sleep(2)
            
    return all_items

def run_process():
    # 1. 구글 인증 및 시트 열기
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    # 구글 드라이브에 '군수품조달_국내_공개수의공고' 시트가 있어야 함
    spreadsheet = client.open("군수품조달_국내_공개수의공고")
    sheet = spreadsheet.get_worksheet(0)

    # 2. 날짜 구간 생성 (2024-01-01 ~ 오늘)
    start_dt = datetime(2024, 1, 1)
    end_dt = datetime.now()
    date_ranges = []
    
    temp_dt = start_dt
    while temp_dt <= end_dt:
        d_start = temp_dt.strftime('%Y%m%d')
        d_end_dt = temp_dt + timedelta(days=6)
        if d_end_dt > end_dt: d_end_dt = end_dt
        date_ranges.append((d_start, d_end_dt.strftime('%Y%m%d')))
        temp_dt = d_end_dt + timedelta(days=1)

    # 3. 병렬 수집
    print(f">>> 국내 공개수의공고 {len(date_ranges)}개 구간 수집 시작...")
    final_results = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(fetch_period, date_ranges))
        for r in results:
            final_results.extend(r)

    # 4. 데이터 저장 (누적)
    if final_results:
        df = pd.DataFrame(final_results)
        
        # 제목행 확인
        first_row = sheet.row_values(1)
        if not first_row:
            sheet.insert_row(df.columns.tolist(), 1)
        
        values = df.fillna('').values.tolist()
        batch_size = 3000
        print(f">>> 총 {len(values)}건 데이터 전송 중...")
        for i in range(0, len(values), batch_size):
            sheet.append_rows(values[i:i + batch_size])
            time.sleep(1)
        print(">>> 업데이트 완료.")
    else:
        print(">>> 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    run_process()
