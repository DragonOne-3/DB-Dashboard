import os
import json
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

# 환경 변수 로드
SERVICE_KEY = os.environ['DATA_GO_KR_API_KEY']
GOOGLE_AUTH_JSON = os.environ['GOOGLE_AUTH_JSON']

def get_yesterday_data():
    url = 'http://openapi.d2b.go.kr/openapi/service/CntrctInfoService/getDmstcCntrctInfoList'
    
    # 어제 날짜 계산
    yesterday = '20260119' #(datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    to_date = '20260123'
    print(f"{yesterday} 데이터를 수집합니다.")
    
    params = {
        'serviceKey': SERVICE_KEY,
        'cntrctDateBegin': yesterday,
        'cntrctDateEnd': to_date #yesterday,
        'numOfRows': '20000', # 하루치 데이터는 5000건이면 충분함
        'pageNo': '1'
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        root = ET.fromstring(response.content)
        items = root.findall('.//item')
        
        data_list = []
        for item in items:
            row = {child.tag: child.text for child in item}
            data_list.append(row)
        
        return pd.DataFrame(data_list)
    except Exception as e:
        print(f"API 호출 오류: {e}")
        return pd.DataFrame()

def update_google_sheet(df):
    if df.empty:
        print("수집된 어제 데이터가 없습니다.")
        return

    # 구글 서비스 계정 인증
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    spreadsheet = client.open("군수품조달_국내_계약정보")
    sheet = spreadsheet.get_worksheet(0)
    
    # 데이터 변환 (결측치 제거 및 리스트화)
    values = df.fillna('').values.tolist()
    
    # 방법 A: 기존 데이터 아래에 추가하고 싶을 때 (추천)
    sheet.append_rows(values)
    
    # 방법 B: 만약 매일 시트를 싹 비우고 어제 데이터만 넣고 싶다면 아래 주석 해제
    # sheet.clear()
    # header = df.columns.tolist()
    # sheet.update([header] + values)
    
    print(f"총 {len(df)}건의 데이터 업데이트 완료.")

if __name__ == "__main__":
    df_new = get_yesterday_data()
    update_google_sheet(df_new)
