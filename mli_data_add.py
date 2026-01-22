import os
import json
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 환경 변수 로드
SERVICE_KEY = os.environ.get('DATA_GO_KR_API_KEY')
GOOGLE_AUTH_JSON = os.environ.get('GOOGLE_AUTH_JSON')
INPUT_START = os.environ.get('START_DATE', '20190101')
INPUT_END = os.environ.get('END_DATE', '20190331')

def get_session():
    """재시도 로직이 포함된 세션 생성"""
    session = requests.Session()
    retry = Retry(
        total=3, # 최대 3번 재시도
        backoff_factor=2, # 재시도 간격 지수적 증가 (2초, 4초, 8초...)
        status_forcelist=[500, 502, 503, 504] # 해당 에러 발생 시 재시도
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get_data_chunk(session, start_date, end_date):
    url = 'http://openapi.d2b.go.kr/openapi/service/CntrctInfoService/getDmstcCntrctInfoList'
    
    params = {
        'serviceKey': SERVICE_KEY,
        'cntrctDateBegin': start_date,
        'cntrctDateEnd': end_date,
        'numOfRows': '5000',  # 50000에서 5000으로 하향 조정 (안정성 확보)
        'pageNo': '1'
    }

    try:
        # timeout을 60초로 연장
        response = session.get(url, params=params, timeout=60)
        response.raise_for_status()
        
        root = ET.fromstring(response.content)
        items = root.findall('.//item')
        
        data_list = []
        for item in items:
            row = {child.tag: child.text for child in item}
            data_list.append(row)
        
        return data_list
    except Exception as e:
        print(f"\n❌ 오류 발생 ({start_date} ~ {end_date}): {e}")
        return None # 오류 발생 시 None 반환

def update_google_sheet(data_list):
    if not data_list:
        print("-> 추가할 데이터가 없습니다.")
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
    total_start = datetime.strptime(INPUT_START, "%Y%m%d")
    total_end = datetime.strptime(INPUT_END, "%Y%m%d")
    
    current_start = total_start
    session = get_session()

    print(f"🚀 수집 시작: {INPUT_START} ~ {INPUT_END}")

    while current_start <= total_end:
        current_end = current_start + timedelta(days=6)
        if current_end > total_end:
            current_end = total_end
            
        str_start = current_start.strftime('%Y%m%d')
        str_end = current_end.strftime('%Y%m%d')
        
        print(f"📅 구간 수집: {str_start} ~ {str_end}", end=" ", flush=True)
        
        chunk_data = get_data_chunk(session, str_start, str_end)
        
        if chunk_data is not None:
            update_google_sheet(chunk_data)
            # 서버 부하 방지를 위해 1.5초 대기
            time.sleep(1.5)
        else:
            print("-> 스킵합니다 (서버 응답 없음)")
            time.sleep(5) # 에러 시에는 좀 더 길게 대기
            
        current_start = current_end + timedelta(days=1)

    print("✅ 모든 작업 완료!")
