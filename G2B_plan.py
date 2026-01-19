import os
import json
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import time
from concurrent.futures import ThreadPoolExecutor

# 환경 변수 로드
SERVICE_KEY = os.environ['DATA_GO_KR_API_KEY']
GOOGLE_AUTH_JSON = os.environ['GOOGLE_AUTH_JSON']

def fetch_g2b_period(date_range):
    """특정 기간의 나라장터 발주계획 수집"""
    start_dt, end_dt = date_range
    url = 'https://apis.data.go.kr/1230000/ao/OrderPlanSttusService/getOrderPlanSttusListServcPPSSrch'
    all_items = []
    page_no = 1
    
    while True:
        params = {
            'serviceKey': SERVICE_KEY,
            'pageNo': str(page_no),
            'numOfRows': '500',
            'type': 'json',
            'inqryBgnDt': start_dt + '0000', # YYYYMMDDHHMM
            'inqryEndDt': end_dt + '2359'
        }
        
        try:
            response = requests.get(url, params=params, timeout=60)
            if response.status_code != 200:
                break
            
            data = response.json()
            # 응답 구조 확인 (나라장터 API 표준 구조)
            items = data.get('response', {}).get('body', {}).get('items', [])
            
            if not items:
                break
            
            all_items.extend(items)
            
            total_count = int(data.get('response', {}).get('body', {}).get('totalCount', 0))
            if len(all_items) >= total_count:
                break
                
            page_no += 1
            time.sleep(0.2)
        except Exception as e:
            print(f"  [오류] {start_dt} 구간: {e}")
            break
            
    return all_items

def run_process():
    # 1. 구글 인증 및 시트 준비
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    # 구글 드라이브에 '나라장터_용역_발주계획' 시트가 미리 생성되어 있어야 함
    spreadsheet = client.open("나라장터_용역_발주계획")
    sheet = spreadsheet.get_worksheet(0)

    # 2. 날짜 구간 생성 (2024-01-01 ~ 오늘)
    start_date = datetime(2024, 1, 1)
    end_date = datetime.now()
    date_ranges = []
    
    temp_dt = start_date
    while temp_dt <= end_date:
        chunk_start = temp_dt.strftime('%Y%m%d')
        chunk_end_dt = temp_dt + timedelta(days=14)
        if chunk_end_dt > end_date: chunk_end_dt = end_date
        date_ranges.append((chunk_start, chunk_end_dt.strftime('%Y%m%d')))
        temp_dt = chunk_end_dt + timedelta(days=1)

    # 3. 병렬 수집
    print(f">>> 나라장터 용역 발주계획 {len(date_ranges)}개 구간 수집 시작...")
    final_results = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(fetch_g2b_period, date_ranges))
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
