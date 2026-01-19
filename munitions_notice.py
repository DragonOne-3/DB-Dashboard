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
    """특정 7일 구간의 데이터를 수집 (실패 시 3회 재시도)"""
    start_date, end_date = date_range
    url = 'http://openapi.d2b.go.kr/openapi/service/BidPblancInfoService/getDmstcCmpetBidPblancList'
    all_items = []
    page_no = 1
    
    for attempt in range(3): # 재시도 로직
        try:
            while True:
                params = {
                    'serviceKey': SERVICE_KEY,
                    'anmtDateBegin': start_date,
                    'anmtDateEnd': end_date,
                    'numOfRows': '500',
                    'pageNo': str(page_no)
                }
                
                response = requests.get(url, params=params, timeout=60)
                if response.status_code != 200:
                    break

                root = ET.fromstring(response.content)
                items = root.findall('.//item')
                
                if not items:
                    break
                    
                for item in items:
                    all_items.append({child.tag: child.text for child in item})
                
                # totalCount 확인
                total_element = root.find('.//totalCount')
                if total_element is not None:
                    total_count = int(total_element.text)
                    if len(all_items) >= total_count:
                        break
                    page_no += 1
                else:
                    break
            
            if all_items or page_no == 1: # 데이터가 있거나 조회를 마쳤으면 종료
                break
                
        except Exception as e:
            print(f"  [재시도 {attempt+1}] {start_date} 구간 오류: {e}")
            time.sleep(2)
            
    print(f"  [수집완료] {start_date} ~ {end_date}: {len(all_items)}건")
    return all_items

def run_process():
    # 1. 구글 인증 및 시트 준비
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open("군수품조달_국내_입찰공고")
    sheet = spreadsheet.get_worksheet(0)

    # 2. 날짜 구간 생성 (안전하게 7일 단위로 생성)
    start_dt = datetime(2023, 1, 1)
    end_dt = datetime.now()
    date_ranges = []
    
    temp_dt = start_dt
    while temp_dt <= end_dt:
        chunk_start = temp_dt.strftime('%Y%m%d')
        chunk_end_dt = temp_dt + timedelta(days=6) # 7일 단위
        if chunk_end_dt > end_dt: chunk_end_dt = end_dt
        date_ranges.append((chunk_start, chunk_end_dt.strftime('%Y%m%d')))
        temp_dt = chunk_end_dt + timedelta(days=1)

    # 3. 병렬 수집 (서버 차단 방지를 위해 동시 작업수 조정)
    print(f">>> 총 {len(date_ranges)}개 구간(7일 단위) 수집 시작...")
    final_results = []
    with ThreadPoolExecutor(max_workers=3) as executor: # max_workers를 3으로 낮춤 (안전성)
        results = list(executor.map(fetch_period, date_ranges))
        for r in results:
            final_results.extend(r)

    # 4. 데이터 저장
    if final_results:
        df = pd.DataFrame(final_results)
        
        # 제목행 및 데이터 추가
        first_row = sheet.row_values(1)
        if not first_row:
            header = df.columns.tolist()
            sheet.insert_row(header, 1)
        
        values = df.fillna('').values.tolist()
        # 시트에 추가 (데이터가 많으므로 2000줄씩 안전하게 전송)
        batch_size = 2000
        print(f">>> 시트에 {len(values)}건 전송 시작...")
        for i in range(0, len(values), batch_size):
            sheet.append_rows(values[i:i + batch_size])
            time.sleep(1.5)
        print(">>> 모든 과거 데이터 업데이트 완료.")
    else:
        print(">>> 수집된 데이터가 없습니다. API 인증키나 서버 상태를 확인하세요.")

if __name__ == "__main__":
    run_process()
