import os
import json
import requests
import pandas as pd
import gspread
import time
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

# 환경 변수 로드
SERVICE_KEY = os.environ['DATA_GO_KR_API_KEY']
GOOGLE_AUTH_JSON = os.environ['GOOGLE_AUTH_JSON']

def get_monthly_data(start_date, end_date):
    url = 'http://openapi.d2b.go.kr/openapi/service/CntrctInfoService/getDmstcCntrctInfoList'
    all_data = []
    page_no = 1
    
    while True:
        print(f"{start_date} ~ {end_date} 구간 | {page_no}페이지 수집 중...")
        params = {
            'serviceKey': SERVICE_KEY,
            'cntrctDateBegin': start_date,
            'cntrctDateEnd': end_date,
            'numOfRows': '1000',  # 안전하게 1000건씩 끊어서 호출
            'pageNo': str(page_no)
        }

        try:
            response = requests.get(url, params=params, timeout=60)
            root = ET.fromstring(response.content)
            
            # 전체 결과 수 확인
            total_count = int(root.find('.//totalCount').text if root.find('.//totalCount') is not None else 0)
            items = root.findall('.//item')
            
            if not items:
                break
                
            for item in items:
                row = {child.tag: child.text for child in item}
                all_data.append(row)
            
            # 다음 페이지 여부 확인
            if len(all_data) >= total_count:
                break
            page_no += 1
            time.sleep(0.5) # API 서버 부하 방지
            
        except Exception as e:
            print(f"오류 발생: {e}")
            time.sleep(5) # 오류 시 잠시 대기
            continue

    return pd.DataFrame(all_data)

def run_history_collect():
    # 수집 구간 설정 (2021-01-01 ~ 2023-12-31)
    # 한 번에 3년치를 돌리면 타임아웃 날 수 있으니 월별로 루프
    date_range = pd.date_range(start='2021-01-01', end='2023-12-31', freq='MS')
    
    # 구글 인증 (한 번만 실행)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open("군수품조달_국내_계약정보")
    sheet = spreadsheet.get_worksheet(0)

    for start_month in date_range:
        end_month = start_month + pd.offsets.MonthEnd(0)
        s_str = start_month.strftime('%Y%m%d')
        e_str = end_month.strftime('%Y%m%d')
        
        print(f"\n--- {s_str} 수집 시작 ---")
        df = get_monthly_data(s_str, e_str)
        
        if not df.empty:
            values = df.fillna('').values.tolist()
            sheet.append_rows(values)
            print(f"{s_str} 구간 {len(df)}건 전송 완료.")
        else:
            print(f"{s_str} 구간 데이터 없음.")

if __name__ == "__main__":
    run_history_collect()
