import os
import json
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import xml.etree.ElementTree as ET
import time

# 환경 변수 로드
SERVICE_KEY = os.environ['DATA_GO_KR_API_KEY']
GOOGLE_AUTH_JSON = os.environ['GOOGLE_AUTH_JSON']

def fetch_monthly_plan(target_month):
    """특정 월(YYYYMM)의 모든 발주계획 데이터를 수집"""
    url = 'http://openapi.d2b.go.kr/openapi/service/PrcurePlanInfoService/getDmstcPrcurePlanList'
    all_items = []
    page_no = 1
    
    while True:
        params = {
            'serviceKey': SERVICE_KEY,
            'orderPrearngeMtBegin': '2026-01-19', #target_month,
            'orderPrearngeMtEnd': '2026-01-23', #target_month,
            'numOfRows': '500',
            'pageNo': str(page_no)
        }
        
        try:
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
                time.sleep(0.5)
            else:
                break
        except Exception as e:
            print(f"  [오류] {target_month} 수집 중 에러: {e}")
            break
            
    return all_items

def run_process():
    # 1. 구글 인증 및 시트 열기
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open("군수품조달_국내_발주계획")
    sheet = spreadsheet.get_worksheet(0)

    # 2. 이번 달 날짜 설정 (YYYYMM)
    # 발주계획은 수시로 업데이트되므로, 매일 실행 시 이번 달 전체를 다시 확인하여 누적합니다.
    current_month = datetime.now().strftime('%Y%m')
    
    print(f"====================================")
    print(f">>> {current_month} 발주계획 업데이트 시작")
    print(f"====================================")

    # 3. 데이터 수집 (병렬 제거, 하루치/한달치는 순차가 안전)
    items = fetch_monthly_plan(current_month)

    # 4. 데이터 저장 (누적)
    if items:
        df = pd.DataFrame(items)
        
        # 제목행 확인
        first_row = sheet.row_values(1)
        if not first_row:
            header = df.columns.tolist()
            sheet.insert_row(header, 1)
        
        # 데이터 추가
        values = df.fillna('').values.tolist()
        sheet.append_rows(values, value_input_option='RAW')
        print(f"✅ {current_month} 데이터 {len(items)}건 누적 완료.")
    else:
        print(f"ℹ️ {current_month}에 해당하는 발주계획 데이터가 없습니다.")

if __name__ == "__main__":
    run_process()
