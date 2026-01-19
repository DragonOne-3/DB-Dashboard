import os
import json
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import xml.etree.ElementTree as ET
import time
from concurrent.futures import ThreadPoolExecutor

# 환경 변수 로드
SERVICE_KEY = os.environ['DATA_GO_KR_API_KEY']
GOOGLE_AUTH_JSON = os.environ['GOOGLE_AUTH_JSON']

def fetch_monthly_plan(target_month):
    """특정 월(YYYYMM)의 모든 발주계획 데이터를 수집 (재시도 포함)"""
    url = 'http://openapi.d2b.go.kr/openapi/service/PrcurePlanInfoService/getDmstcPrcurePlanList'
    all_items = []
    page_no = 1
    
    for attempt in range(3):
        try:
            while True:
                params = {
                    'serviceKey': SERVICE_KEY,
                    'orderPrearngeMtBegin': target_month,
                    'orderPrearngeMtEnd': target_month,
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
                
                total_element = root.find('.//totalCount')
                if total_element is not None:
                    total_count = int(total_element.text)
                    if len(all_items) >= total_count:
                        break
                    page_no += 1
                else:
                    break
            
            if all_items or page_no == 1:
                break
        except Exception as e:
            print(f"  [재시도] {target_month} 에러: {e}")
            time.sleep(2)
            
    print(f"  [수집완료] {target_month}: {len(all_items)}건")
    return all_items

def run_process():
    # 1. 구글 인증 및 시트 열기
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open("군수품조달_국내_발주계획")
    sheet = spreadsheet.get_worksheet(0)

    # 2. 수집할 월 목록 생성 (202301 ~ 현재월)
    start_year = 2023
    now = datetime.now()
    months = []
    for year in range(start_year, now.year + 1):
        m_start = 1
        m_end = now.month if year == now.year else 12
        for month in range(m_start, m_end + 1):
            months.append(f"{year}{month:02d}")

    # 3. 병렬 수집 실행
    print(f">>> 총 {len(months)}개 월 발주계획 병렬 수집 시작...")
    final_results = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(fetch_monthly_plan, months))
        for r in results:
            final_results.extend(r)

    # 4. 데이터 저장 (누적 방식)
    if final_results:
        df = pd.DataFrame(final_results)
        
        # 1행 제목 확인 및 추가
        first_row = sheet.row_values(1)
        if not first_row:
            header = df.columns.tolist()
            sheet.insert_row(header, 1)
            print("  [알림] 제목행을 생성했습니다.")
        
        # 마지막 데이터 다음에 추가
        values = df.fillna('').values.tolist()
        batch_size = 3000
        print(f">>> 시트에 {len(values)}건 전송 시작...")
        for i in range(0, len(values), batch_size):
            sheet.append_rows(values[i:i + batch_size])
            time.sleep(1.5)
        print(">>> 발주계획 업데이트 완료.")
    else:
        print(">>> 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    run_process()
