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
    """특정 구간(start, end)의 모든 데이터를 가져오는 스레드용 함수"""
    start_date, end_date = date_range
    url = 'http://openapi.d2b.go.kr/openapi/service/BidPblancInfoService/getDmstcCmpetBidPblancList'
    all_items = []
    page_no = 1
    
    while True:
        params = {
            'serviceKey': SERVICE_KEY,
            'anmtDateBegin': start_date,
            'anmtDateEnd': end_date,
            'numOfRows': '500',
            'pageNo': str(page_no)
        }
        try:
            # 타임아웃을 넉넉히 주되 병렬로 여러 개를 찌릅니다.
            response = requests.get(url, params=params, timeout=60)
            if response.status_code != 200: break
            
            root = ET.fromstring(response.content)
            items = root.findall('.//item')
            if not items: break
            
            for item in items:
                all_items.append({child.tag: child.text for child in item})
            
            total_count = int(root.find('.//totalCount').text or 0)
            if len(all_items) >= total_count: break
            page_no += 1
        except Exception as e:
            print(f"  [오류] {start_date} 구간 에러: {e}")
            break
    return all_items

def run_process():
    # 1. 구글 인증 및 시트 준비
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open("군수품조달_국내_입찰공고")
    sheet = spreadsheet.get_worksheet(0)

    # 2. 수집할 날짜 구간 생성 (15일 단위로 쪼개서 병렬 처리)
    start_dt = datetime(2024, 1, 1)
    end_dt = datetime(2025, 12, 31)
    date_ranges = []
    
    temp_dt = start_dt
    while temp_dt <= end_dt:
        chunk_start = temp_dt.strftime('%Y%m%d')
        chunk_end_dt = temp_dt + timedelta(days=14) # 15일 단위
        if chunk_end_dt > end_dt: chunk_end_dt = end_dt
        date_ranges.append((chunk_start, chunk_end_dt.strftime('%Y%m%d')))
        temp_dt = chunk_end_dt + timedelta(days=1)

    # 3. 병렬 수집 실행 (동시 5개 구간씩 호출)
    print(f">>> 총 {len(date_ranges)}개 구간 병렬 수집 시작...")
    final_results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(fetch_period, date_ranges))
        for r in results:
            final_results.extend(r)

    # 4. 데이터 저장 처리
    if final_results:
        df = pd.DataFrame(final_results)
        # 중복 제거 (입찰공고번호 기준 등 - 필요시 활성화)
        # df = df.drop_duplicates() 

        # 시트 상태 확인 및 제목행 처리
        first_row = sheet.row_values(1)
        if not first_row:
            header = df.columns.tolist()
            sheet.insert_row(header, 1)
            print("  [알림] 제목행 생성 완료.")
        
        # 구글 시트에 대량 업로드 (5000줄씩 쪼개서 전송하여 속도 향상)
        values = df.fillna('').values.tolist()
        batch_size = 5000
        print(f">>> 총 {len(values)}건 시트 전송 시작 (Batch size: {batch_size})...")
        for i in range(0, len(values), batch_size):
            sheet.append_rows(values[i:i + batch_size])
            print(f"  [전송] {i + len(values[i:i + batch_size])} / {len(values)} 완료")
            time.sleep(1) # 시트 API 제한 방지
    else:
        print(">>> 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    run_process()
