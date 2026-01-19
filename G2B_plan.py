import os
import json
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import time

# 환경 변수 로드
SERVICE_KEY = os.environ['DATA_GO_KR_API_KEY']
GOOGLE_AUTH_JSON = os.environ['GOOGLE_AUTH_JSON']

def fetch_g2b_by_date_range(start_date, end_date):
    """5일 단위의 짧은 기간 내의 모든 데이터를 수집"""
    url = 'http://apis.data.go.kr/1230000/ao/OrderPlanSttusService/getOrderPlanSttusListServcPPSSrch'
    all_items = []
    page_no = 1
    
    while True:
        params = {
            'serviceKey': SERVICE_KEY,
            'pageNo': str(page_no),
            'numOfRows': '900',
            'type': 'json',
            'inqryBgnDt': start_date + '0000',
            'inqryEndDt': end_date + '2359'
        }
        
        try:
            response = requests.get(url, params=params, timeout=45)
            if response.status_code != 200: break

            data = response.json()
            body = data.get('response', {}).get('body', {})
            items = body.get('items', [])
            total_count = int(body.get('totalCount', 0))
            
            if not items: break
            if isinstance(items, dict): items = [items]
            
            all_items.extend(items)
            print(f"    [진행] {start_date}~{end_date}: {page_no}페이지 수집 중... ({len(all_items)}/{total_count})")
            
            if len(all_items) >= total_count: break
            page_no += 1
            time.sleep(0.5)
            
        except Exception as e:
            print(f"    [예외] {start_date} 구간 오류: {e}")
            break
            
    return all_items

def run_process():
    # 1. 구글 시트 연결
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open("나라장터_용역_발주계획")
    sheet = spreadsheet.get_worksheet(0)

    # 2. 날짜 구간 설정 (2025-01-01 ~ 오늘)
    current_dt = datetime(2025, 1, 1)
    end_dt = datetime.now()
    
    print(f"====================================")
    print(f">>> 2025년 1월부터 정밀 수집 시작")
    print(f"====================================")
    
    while current_dt <= end_dt:
        chunk_start = current_dt.strftime('%Y%m%d')
        chunk_end_dt = current_dt + timedelta(days=4) # 5일 단위
        if chunk_end_dt > end_dt: chunk_end_dt = end_dt
        chunk_end = chunk_end_dt.strftime('%Y%m%d')
        
        print(f"  > {chunk_start} ~ {chunk_end} 데이터 수집 중...")
        items = fetch_g2b_by_date_range(chunk_start, chunk_end)
        
        if items:
            df = pd.DataFrame(items)
            
            # 제목행이 없으면 생성
            if not sheet.row_values(1):
                sheet.insert_row(df.columns.tolist(), 1)
            
            # 데이터 추가 (누적)
            values = df.fillna('').values.tolist()
            for i in range(0, len(values), 3000):
                sheet.append_rows(values[i:i+3000])
            print(f"  > [완료] {chunk_start} 구간 저장 완료 ({len(items)}건)")
        
        current_dt = chunk_end_dt + timedelta(days=1)
        time.sleep(1) # 서버 보호를 위한 대기

if __name__ == "__main__":
    run_process()
