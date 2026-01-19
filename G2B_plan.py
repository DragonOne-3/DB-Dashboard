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
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    today = datetime.now().strftime('%Y%m%d')
    
    print(f">>> {yesterday} 발주계획 데이터 수집 시작...")
    items = fetch_g2b_by_date_range(yesterday, today) # fetch_g2b_by_date_range 함수 사용
    
    if items:
        df = pd.DataFrame(items)
        # 데이터 추가
        values = df.fillna('').values.tolist()
        sheet.append_rows(values, value_input_option='RAW')
        print(f"✅ {yesterday} 데이터 {len(items)}건 추가 완료")
    else:
        print(f"ℹ️ {yesterday}에 등록된 데이터가 없습니다.")

if __name__ == "__main__":
    run_process()
