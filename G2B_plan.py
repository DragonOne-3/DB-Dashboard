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

def fetch_g2b_by_order_month(target_month):
    """발주예정년월(orderBgnYm)을 기준으로 데이터 수집"""
    url = 'http://apis.data.go.kr/1230000/ao/OrderPlanSttusService/getOrderPlanSttusListServcPPSSrch'
    all_items = []
    page_no = 1
    
    while True:
        params = {
            'serviceKey': SERVICE_KEY,
            'pageNo': str(page_no),
            'numOfRows': '900',
            'type': 'json',
            'orderBgnYm': target_month, # 발주시작년월 (예: 202401)
            'orderEndYm': target_month  # 발주종료년월
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
            print(f"    [진행] {target_month}: {page_no}페이지 수집 중... ({len(all_items)}/{total_count})")
            
            if len(all_items) >= total_count: break
            page_no += 1
            time.sleep(0.3)
            
        except Exception as e:
            print(f"    [예외] {target_month} 수집 중 오류: {e}")
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

    # 2. 수집할 월 리스트 생성 (202401 ~ 현재월)
    start_year = 2024
    now = datetime.now()
    target_months = []
    
    for year in range(start_year, now.year + 1):
        m_start = 1
        m_end = now.month if year == now.year else 12
        for month in range(m_start, m_end + 1):
            target_months.append(f"{year}{month:02d}")

    # 3. 월별 전수 수집 및 저장
    print(f">>> 총 {len(target_months)}개 월 데이터 전수 수집 시작...")
    
    for month in target_months:
        print(f"  > {month} 발주 예정 데이터 수집 중...")
        items = fetch_g2b_by_order_month(month)
        
        if items:
            df = pd.DataFrame(items)
            # 1행 제목 작성
            if not sheet.row_values(1):
                sheet.insert_row(df.columns.tolist(), 1)
            
            # 시트 저장
            values = df.fillna('').values.tolist()
            for i in range(0, len(values), 3000):
                sheet.append_rows(values[i:i+3000])
            print(f"  > [완료] {month} 시트 저장 완료 ({len(items)}건)")
        
        time.sleep(0.5)

if __name__ == "__main__":
    run_process()
