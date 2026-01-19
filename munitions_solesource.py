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

def fetch_period(start_date, end_date):
    """특정 구간의 공개수의공고 데이터를 수집"""
    url = 'http://openapi.d2b.go.kr/openapi/service/BidPblancInfoService/getDmstcOthbcVltrnNtatPlanList'
    all_items = []
    page_no = 1
    
    while True:
        params = {
            'serviceKey': SERVICE_KEY,
            # 특정 기관 코드(EGC)를 빼고 전체 조회 시도 (데이터 확보를 위해)
            'prqudoPresentnClosDateBegin': start_date,
            'prqudoPresentnClosDateEnd': end_date,
            'numOfRows': '500',
            'pageNo': str(page_no)
        }
        
        try:
            response = requests.get(url, params=params, timeout=45)
            if response.status_code != 200:
                print(f"    [오류] HTTP {response.status_code}")
                break

            # 응답이 XML 에러인지 확인
            if response.text.strip().startswith('<CmmnMsg>'):
                print(f"    [API 에러] {response.text}")
                break

            root = ET.fromstring(response.content)
            items = root.findall('.//item')
            
            if not items:
                break
                
            for item in items:
                all_items.append({child.tag: child.text for child in item})
            
            total_count = int(root.find('.//totalCount').text or 0)
            if len(all_items) >= total_count:
                break
                
            page_no += 1
            time.sleep(0.3)
            
        except Exception as e:
            print(f"    [예외] {start_date} 구간 에러: {e}")
            break
            
    return all_items

def run_process():
    # 1. 구글 시트 연결
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open("군수품조달_국내_공개수의공고")
    sheet = spreadsheet.get_worksheet(0)

    # 2. 날짜 구간 (15일 단위로 더 넓게 쪼개서 안정성 확보)
    start_dt = datetime(2024, 1, 1)
    end_dt = datetime.now()
    
    print(f">>> 2024년부터 공개수의공고 수집 시작...")
    
    current_dt = start_dt
    while current_dt <= end_dt:
        d_start = current_dt.strftime('%Y%m%d')
        d_end_dt = current_dt + timedelta(days=14)
        if d_end_dt > end_dt: d_end_dt = end_dt
        d_end = d_end_dt.strftime('%Y%m%d')
        
        print(f"  > {d_start} ~ {d_end} 조회 중...")
        items = fetch_period(d_start, d_end)
        
        if items:
            df = pd.DataFrame(items)
            # 제목행 처리
            if not sheet.row_values(1):
                sheet.insert_row(df.columns.tolist(), 1)
            
            # 데이터 추가
            values = df.fillna('').values.tolist()
            sheet.append_rows(values)
            print(f"    [성공] {len(items)}건 저장 완료")
        
        current_dt = d_end_dt + timedelta(days=1)
        time.sleep(1)

if __name__ == "__main__":
    run_process()
