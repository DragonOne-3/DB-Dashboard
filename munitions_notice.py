import os
import json
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import time

# 환경 변수 로드
SERVICE_KEY = os.environ['DATA_GO_KR_API_KEY']
GOOGLE_AUTH_JSON = os.environ['GOOGLE_AUTH_JSON']

def fetch_daily_data(target_date):
    """특정 날짜(하루)의 입찰공고 데이터를 수집"""
    url = 'http://openapi.d2b.go.kr/openapi/service/BidPblancInfoService/getDmstcCmpetBidPblancList'
    all_items = []
    page_no = 1
    
    while True:
        params = {
            'serviceKey': SERVICE_KEY,
            'anmtDateBegin': '20260119' #target_date, # 공고일자 시작 (어제)
            'anmtDateEnd': '20260123' #target_date,   # 공고일자 종료 (어제)
            'numOfRows': '500',
            'pageNo': str(page_no)
        }
        
        try:
            response = requests.get(url, params=params, timeout=60)
            if response.status_code != 200:
                print(f"    [오류] HTTP {response.status_code}")
                break

            root = ET.fromstring(response.content)
            items = root.findall('.//item')
            
            if not items:
                break
                
            for item in items:
                all_items.append({child.tag: child.text for child in item})
            
            # totalCount 확인하여 다음 페이지 여부 결정
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
            print(f"    [예외] {target_date} 수집 중 오류: {e}")
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

    # 2. 어제 날짜 설정 (한국 시간 기준)
    # GitHub Actions의 TZ: Asia/Seoul 설정을 활용합니다.
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    
    print(f"====================================")
    print(f">>> {yesterday} 입찰공고 수집 시작")
    print(f"====================================")

    # 3. 데이터 수집
    items = fetch_daily_data(yesterday)

    # 4. 데이터 저장 (누적)
    if items:
        df = pd.DataFrame(items)
        
        # 제목행이 없으면 생성
        first_row = sheet.row_values(1)
        if not first_row:
            header = df.columns.tolist()
            sheet.insert_row(header, 1)
        
        # 데이터 추가 (append_rows를 사용하여 기존 데이터 아래에 누적)
        values = df.fillna('').values.tolist()
        sheet.append_rows(values, value_input_option='RAW')
        print(f"✅ {yesterday} 데이터 {len(items)}건 누적 완료.")
    else:
        print(f"ℹ️ {yesterday}에 등록된 새로운 입찰공고가 없습니다.")

if __name__ == "__main__":
    run_process()
