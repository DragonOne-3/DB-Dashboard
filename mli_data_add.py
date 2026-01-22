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
# 깃허브 액션에서 입력받은 날짜 로드
INPUT_START = os.environ.get('START_DATE', '20240101')
INPUT_END = os.environ.get('END_DATE', '20240107')

def get_data_chunk(session, start_date, end_date):
    """특정 구간의 데이터를 가져오는 함수"""
    url = 'http://openapi.d2b.go.kr/openapi/service/CntrctInfoService/getDmstcCntrctInfoList'
    
    params = {
        'serviceKey': SERVICE_KEY,
        'cntrctDateBegin': start_date,
        'cntrctDateEnd': end_date,
        'numOfRows': '50000', 
        'pageNo': '1'
    }

    try:
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
    """수집된 데이터를 구글 시트에 누적 추가"""
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
    print(f"-> {len(values)}건 추가 완료.")

if __name__ == "__main__":
    # 입력받은 문자열을 datetime 객체로 변환
    total_start = datetime.strptime(INPUT_START, "%Y%m%d")
    total_end = datetime.strptime(INPUT_END, "%Y%m%d")
    
    current_start = total_start
    session = requests.Session() # 속도 최적화

    print(f"전체 수집 기간: {INPUT_START} ~ {INPUT_END}")

    while current_start <= total_end:
        # 7일 단위로 끊기
        current_end = current_start + timedelta(days=6)
        if current_end > total_end:
            current_end = total_end
            
        str_start = current_start.strftime('%Y%m%d')
        str_end = current_end.strftime('%Y%m%d')
        
        print(f"진행 중: {str_start} ~ {str_end}...", end=" ")
        
        chunk_data = get_data_chunk(session, str_start, str_end)
        update_google_sheet(chunk_data)
        
        # 다음 구간으로 이동하기 전 아주 짧은 대기 (서버 부하 방지)
        time.sleep(0.3)
        current_start = current_end + timedelta(days=1)

    print("✅ 모든 작업이 완료되었습니다.")
