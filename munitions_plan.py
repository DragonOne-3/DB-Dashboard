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

def get_all_pages_data(target_month):
    """특정 월(YYYYMM)의 모든 페이지 데이터를 수집"""
    url = 'http://openapi.d2b.go.kr/openapi/service/PrcurePlanInfoService/getDmstcPrcurePlanList'
    all_items = []
    page_no = 1
    
    while True:
        params = {
            'serviceKey': SERVICE_KEY,
            'orderPrearngeMtBegin': target_month,
            'orderPrearngeMtEnd': target_month,
            'numOfRows': '500',
            'pageNo': str(page_no)
        }

        print(f"조회 중: {target_month} (페이지: {page_no})")
        
        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code != 200:
                break

            root = ET.fromstring(response.content)
            items = root.findall('.//item')
            
            if not items:
                break
                
            for item in items:
                row = {child.tag: child.text for child in item}
                all_items.append(row)
            
            # 전체 개수 확인 후 루프 종료 조건 체크
            total_element = root.find('.//totalCount')
            if total_element is not None:
                total_count = int(total_element.text)
                if len(all_items) >= total_count:
                    break
            else:
                break
                
            page_no += 1
            time.sleep(0.5) # 서버 부하 방지
            
        except Exception as e:
            print(f"오류 발생: {e}")
            break
            
    return all_items

def fetch_historical_data():
    """2023년 1월부터 현재까지 월별로 수집"""
    start_year = 2023
    start_month = 1
    
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    
    final_data = []
    
    # 2023년 01월부터 현재까지 루프
    for year in range(start_year, current_year + 1):
        m_start = start_month if year == start_year else 1
        m_end = current_month if year == current_year else 12
        
        for month in range(m_start, m_end + 1):
            target_yyyymm = f"{year}{month:02d}"
            month_items = get_all_pages_data(target_yyyymm)
            final_data.extend(month_items)
            
    return pd.DataFrame(final_data)

def update_google_sheet(df):
    if df.empty:
        print("수집된 데이터가 없습니다.")
        return

    # 중복 제거 (결정번호 'dcsNo' 등이 있다면 기준 설정 가능, 여기선 전체 기준)
    df = df.drop_duplicates()

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    # 구글 시트 파일명: 군수품조달_국내_발주계획
    spreadsheet = client.open("군수품조달_국내_발주계획")
    sheet = spreadsheet.get_worksheet(0)
    
    sheet.clear()
    header = df.columns.tolist()
    values = df.fillna('').values.tolist()
    
    # 2000행씩 분할 업로드
    rows_to_upload = [header] + values
    for i in range(0, len(rows_to_upload), 2000):
        chunk = rows_to_upload[i:i + 2000]
        sheet.append_rows(chunk)
        print(f"{i}행부터 데이터 전송 중...")
        time.sleep(1)
        
    print(f"총 {len(df)}건 업데이트 완료.")

if __name__ == "__main__":
    df_result = fetch_historical_data()
    update_google_sheet(df_result)
