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

def get_all_pages_data(start_date, end_date):
    """특정 기간 동안의 모든 페이지 입찰공고 데이터를 수집"""
    url = 'http://openapi.d2b.go.kr/openapi/service/BidPblancInfoService/getDmstcCmpetBidPblancList'
    all_items = []
    page_no = 1
    
    while True:
        params = {
            'serviceKey': SERVICE_KEY,
            'anmtDateBegin': start_date, # 공고시작일
            'anmtDateEnd': end_date,     # 공고종료일
            'numOfRows': '500',          # 페이지당 건수 극대화
            'pageNo': str(page_no)
        }
        
        try:
            # 빠른 속도를 위해 타임아웃은 적절히 설정
            response = requests.get(url, params=params, timeout=45)
            if response.status_code != 200:
                break

            root = ET.fromstring(response.content)
            items = root.findall('.//item')
            
            if not items:
                break
                
            for item in items:
                row = {child.tag: child.text for child in item}
                all_items.append(row)
            
            # totalCount 확인하여 루프 종료 판단
            total_element = root.find('.//totalCount')
            if total_element is not None:
                total_count = int(total_element.text)
                if len(all_items) >= total_count:
                    break
            else:
                break
                
            page_no += 1
            time.sleep(0.1) # 속도를 위해 대기시간 최소화
            
        except Exception as e:
            print(f"  [오류] {start_date}~{end_date} 페이지 {page_no} 호출 중 에러: {e}")
            break
            
    return all_items

def run_process():
    # 1. 구글 인증 및 시트 열기
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open("군수품조달_국내_입찰공고")
    sheet = spreadsheet.get_worksheet(0)

    # 초기화: 시트 비우기
    sheet.clear()
    headers_initialized = False

    # 2. 날짜 설정 (2023-01-01부터 오늘까지)
    start_dt = datetime(2025, 1, 1)
    end_dt = datetime.now()
    
    current_dt = start_dt
    while current_dt <= end_dt:
        chunk_start = current_dt.strftime('%Y%m%d')
        # 30일 단위로 끊어서 수집 (속도와 안정성)
        chunk_end_dt = current_dt + timedelta(days=29)
        if chunk_end_dt > end_dt:
            chunk_end_dt = end_dt
        chunk_end = chunk_end_dt.strftime('%Y%m%d')
        
        print(f">>> {chunk_start} ~ {chunk_end} 입찰공고 수집 중...")
        
        notice_items = get_all_pages_data(chunk_start, chunk_end)
        
        if notice_items:
            df = pd.DataFrame(notice_items)
            
            # 첫 데이터 입력 시 헤더 작성
            if not headers_initialized:
                header = df.columns.tolist()
                sheet.append_row(header)
                headers_initialized = True
            
            # 구글 시트에 즉시 추가
            values = df.fillna('').values.tolist()
            sheet.append_rows(values)
            print(f"  [성공] {len(notice_items)}건 시트 업데이트 완료.")
        else:
            print(f"  [알림] 해당 기간 데이터 없음.")
            
        # 다음 30일 구간으로 이동
        current_dt = chunk_end_dt + timedelta(days=1)
        time.sleep(0.5)

if __name__ == "__main__":
    run_process()
