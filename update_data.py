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
from requests.packages.urllib3.util.retry import Retry

# 1. 환경 변수 로드
SERVICE_KEY = os.environ['DATA_GO_KR_API_KEY']
GOOGLE_AUTH_JSON = os.environ['GOOGLE_AUTH_JSON']

def requests_retry_session(retries=3, backoff_factor=0.3):
    """네트워크 오류 시 자동 재시도를 위한 세션 생성"""
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(500, 502, 504),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get_contract_data_by_period(start_dt, end_dt):
    url = 'http://openapi.d2b.go.kr/openapi/service/CntrctInfoService/getDmstcCntrctInfoList'
    
    # 서버 부담을 줄이기 위해 한 번에 500개씩만 요청 (필요시 더 늘리세요)
    params = {
        'serviceKey': SERVICE_KEY,
        'cntrctDateBegin': start_dt.strftime('%Y%m%d'),
        'cntrctDateEnd': end_dt.strftime('%Y%m%d'),
        'numOfRows': '500', 
        'pageNo': '1'
    }

    print(f"조회 중: {params['cntrctDateBegin']} ~ {params['cntrctDateEnd']}")
    
    try:
        session = requests_retry_session()
        # 타임아웃을 60초로 늘림
        response = session.get(url, params=params, timeout=60)
        
        if response.status_code != 200:
            print(f"API 응답 에러: {response.status_code}")
            return []

        root = ET.fromstring(response.content)
        items = root.findall('.//item')
        
        temp_list = []
        for item in items:
            row = {child.tag: child.text for child in item}
            temp_list.append(row)
        return temp_list

    except Exception as e:
        print(f"데이터 호출 중 오류 발생: {e}")
        return []

def get_year_data_in_chunks():
    all_data = []
    today = datetime.now()
    # 3개월씩 끊으면 양이 많을 수 있어 1개월(30일) 단위로 더 쪼개서 안정성 확보
    chunk_days = 30 
    
    current_start = today - timedelta(days=365)
    while current_start < today:
        current_end = current_start + timedelta(days=chunk_days)
        if current_end > today:
            current_end = today
            
        chunk_data = get_contract_data_by_period(current_start, current_end)
        all_data.extend(chunk_data)
        
        # 호출 간격을 2초로 늘려 서버 차단 방지
        time.sleep(2)
        current_start = current_end + timedelta(days=1)
        
    return pd.DataFrame(all_data)

def update_google_sheet(df):
    if df.empty:
        print("수집된 데이터가 전혀 없습니다. 시트를 업데이트하지 않습니다.")
        return

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    spreadsheet = client.open("군수품조달_국내_계약정보")
    sheet = spreadsheet.get_worksheet(0)
    
    sheet.clear()
    header = df.columns.tolist()
    values = df.fillna('').values.tolist()
    
    # 1행 제목 + 데이터
    sheet.update([header] + values)
    print(f"최종 총 {len(df)}건 업데이트 완료.")

if __name__ == "__main__":
    df_year = get_year_data_in_chunks()
    update_google_sheet(df_year)
