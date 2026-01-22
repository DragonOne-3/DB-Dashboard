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
SERVICE_KEY = os.environ.get('DATA_GO_KR_API_KEY')
GOOGLE_AUTH_JSON = os.environ.get('GOOGLE_AUTH_JSON')

def get_data_chunk(session, start_date, end_date):
    """특정 구간(최대 7일)의 데이터를 가져오는 함수"""
    url = 'http://openapi.d2b.go.kr/openapi/service/CntrctInfoService/getDmstcCntrctInfoList'
    
    params = {
        'serviceKey': SERVICE_KEY,
        'cntrctDateBegin': start_date,
        'cntrctDateEnd': end_date,
        'numOfRows': '50000', 
        'pageNo': '1'
    }

    try:
        # Session을 사용하여 연결 재사용 (속도 향상)
        response = session.get(url, params=params, timeout=30)
        root = ET.fromstring(response.content)
        items = root.findall('.//item')
        
        data_list = []
        for item in items:
            row = {child.tag: child.text for child in item}
            data_list.append(row)
        
        return data_list
    except Exception as e:
        print(f"오류 발생 ({start_date} ~ {end_date}): {e}")
        return []

def update_google_sheet(data_list):
    """수집된 리스트를 구글 시트에 추가"""
    if not data_list:
        return

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    spreadsheet = client.open("군수품조달_국내_계약정보")
    sheet = spreadsheet.get_worksheet(0)
    
    df = pd.DataFrame(data_list)
    values = df.fillna('').values.tolist()
    sheet.append_rows(values)
    print(f"데이터 {len(values)}건 추가 완료.")

if __name__ == "__main__":
    # 1. 수집 기간 설정 (예: 2024년 1월 1일부터 오늘까지)
    total_start_date = datetime.strptime("20240101", "%Y%m%d")
    total_end_date = datetime.now()
    
    current_start = total_start_date
    session = requests.Session() # 속도 최적화를 위한 세션 객체 생성

    while current_start <= total_end_date:
        # 2. 7일 단위로 종료일 계산
        current_end = current_start + timedelta(days=6)
        if current_end > total_end_date:
            current_end = total_end_date
            
        str_start = current_start.strftime('%Y%m%d')
        str_end = current_end.strftime('%Y%m%d')
        
        print(f"수집 중: {str_start} ~ {str_end}...")
        
        # 3. 데이터 가져오기 및 저장
        chunk_data = get_data_chunk(session, str_start, str_end)
        update_google_sheet(chunk_data)
        
        # 4. 차단 방지를 위한 아주 짧은 휴식 (속도를 위해 0.5초만 설정)
        time.sleep(0.5)
        
        # 다음 7일 구간으로 이동
        current_start = current_end + timedelta(days=1)

    print("모든 구간 데이터 수집 및 업데이트가 완료되었습니다.")
