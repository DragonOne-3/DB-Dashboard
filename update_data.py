import os
import json
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

# 1. 환경 변수에서 시크릿 로드
SERVICE_KEY = os.environ['DATA_GO_KR_API_KEY']
GOOGLE_AUTH_JSON = os.environ['GOOGLE_AUTH_JSON']

def get_contract_data():
    url = 'http://openapi.d2b.go.kr/openapi/service/CntrctInfoService/getDmstcCntrctInfoList'
    
    # 최근 1년 날짜 계산 (오늘부터 365일 전까지)
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
    
    # API 파라미터 설정
    params = {
        'serviceKey': SERVICE_KEY,
        'cntrctDateBegin': start_date,
        'cntrctDateEnd': end_date,
        'numOfRows': '3000',  # 1년치 데이터 양에 따라 조절하세요
        'pageNo': '1'
    }

    print(f"{start_date}부터 {end_date}까지의 데이터를 요청합니다...")
    response = requests.get(url, params=params)
    
    # XML 데이터 파싱
    root = ET.fromstring(response.content)
    items = root.findall('.//item')
    
    data_list = []
    for item in items:
        # 각 item 내부의 모든 태그와 텍스트를 추출
        row = {child.tag: child.text for child in item}
        data_list.append(row)
    
    return pd.DataFrame(data_list)

def update_google_sheet(df):
    # 구글 서비스 계정 인증
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    # 구글 시트 열기 (파일명: 군수품조달_국내_계약정보)
    # 반드시 해당 시트를 서비스 계정 이메일에 공유해두어야 합니다.
    spreadsheet = client.open("군수품조달_국내_계약정보")
    sheet = spreadsheet.get_worksheet(0) # 첫 번째 시트 선택
    
    # 시트 비우기
    sheet.clear()
    
    # 데이터가 비어있지 않은 경우에만 업데이트
    if not df.empty:
        # 1행에는 컬럼명(영문), 2행부터는 데이터를 리스트 형태로 변환하여 전송
        header = df.columns.tolist()
        values = df.fillna('').values.tolist() # 결측치는 빈 문자열로 처리
        
        sheet.update([header] + values)
        print(f"총 {len(df)}건의 데이터를 1행 제목과 함께 업데이트 완료했습니다.")
    else:
        print("업데이트할 데이터가 없습니다.")

if __name__ == "__main__":
    try:
        df_result = get_contract_data()
        update_google_sheet(df_result)
    except Exception as e:
        print(f"실행 중 오류 발생: {e}")
