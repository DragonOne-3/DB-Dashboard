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

def get_all_pages_data(start_dt, end_dt):
    """특정 기간 동안의 모든 페이지 데이터를 수집"""
    url = 'http://openapi.d2b.go.kr/openapi/service/CntrctInfoService/getDmstcCntrctInfoList'
    all_items = []
    page_no = 1
    
    while True:
        params = {
            'serviceKey': SERVICE_KEY,
            'cntrctDateBegin': start_dt.strftime('%Y%m%d'),
            'cntrctDateEnd': end_dt.strftime('%Y%m%d'),
            'numOfRows': '500', # 안정성을 위해 500건씩 분할
            'pageNo': str(page_no)
        }

        print(f"조회 중: {params['cntrctDateBegin']} ~ {params['cntrctDateEnd']} (페이지: {page_no})")
        
        try:
            response = requests.get(url, params=params, timeout=30)
            root = ET.fromstring(response.content)
            
            # 데이터 추출
            items = root.findall('.//item')
            if not items: # 더 이상 가져올 데이터가 없으면 중단
                break
                
            for item in items:
                row = {child.tag: child.text for child in item}
                all_items.append(row)
            
            # 전체 개수 확인 (종료 조건)
            total_count = int(root.find('.//totalCount').text)
            if len(all_items) >= total_count:
                break
                
            page_no += 1
            time.sleep(0.5) # 서버 부하 방지
            
        except Exception as e:
            print(f"오류 발생: {e}")
            break
            
    return all_items

def get_year_data_stably():
    """최근 1년을 1개월 단위로 쪼개고, 각 월별로 모든 페이지 수집"""
    final_data = []
    today = datetime.now()
    
    # 최근 1년 (12개월 반복)
    for i in range(12):
        # 한 달 단위 계산
        end_dt = today - timedelta(days=i*30)
        start_dt = today - timedelta(days=(i+1)*30)
        
        month_data = get_all_pages_data(start_dt, end_dt)
        final_data.extend(month_data)
        
    return pd.DataFrame(final_data)

def update_google_sheet(df):
    if df.empty:
        print("수집된 데이터가 없습니다.")
        return

    # 중복 제거 (계약번호 'cntrctNo' 기준)
    if 'cntrctNo' in df.columns:
        df = df.drop_duplicates(subset=['cntrctNo'])

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    spreadsheet = client.open("군수품조달_국내_계약정보")
    sheet = spreadsheet.get_worksheet(0)
    
    # 전체 갱신
    sheet.clear()
    header = df.columns.tolist()
    values = df.fillna('').values.tolist()
    
    # 데이터가 너무 많을 경우 분할 업로드
    def divide_chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    # 구글 시트 한 번 업데이트 제한(약 5-10MB)을 고려하여 2000줄씩 나눠서 업데이트
    rows = [header] + values
    for chunk in divide_chunks(rows, 2000):
        sheet.append_rows(chunk)
        time.sleep(1)
        
    print(f"최종 {len(df)}건 업데이트 완료 (중복 제외)")

if __name__ == "__main__":
    df_result = get_year_data_stably()
    update_google_sheet(df_result)
