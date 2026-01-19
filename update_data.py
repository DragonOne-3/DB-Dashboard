import os
import json
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import time

# 1. 환경 변수 로드
SERVICE_KEY = os.environ['DATA_GO_KR_API_KEY']
GOOGLE_AUTH_JSON = os.environ['GOOGLE_AUTH_JSON']

def get_contract_data_by_period(start_dt, end_dt):
    """특정 기간 동안의 데이터를 가져오는 함수"""
    url = 'http://openapi.d2b.go.kr/openapi/service/CntrctInfoService/getDmstcCntrctInfoList'
    
    params = {
        'serviceKey': SERVICE_KEY,
        'cntrctDateBegin': start_dt.strftime('%Y%m%d'),
        'cntrctDateEnd': end_dt.strftime('%Y%m%d'),
        'numOfRows': '5000',  # 넉넉하게 설정
        'pageNo': '1'
    }

    print(f"기간 조회: {params['cntrctDateBegin']} ~ {params['cntrctDateEnd']}")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        root = ET.fromstring(response.content)
        items = root.findall('.//item')
        
        temp_list = []
        for item in items:
            row = {child.tag: child.text for child in item}
            temp_list.append(row)
        return temp_list
    except Exception as e:
        print(f"해당 기간 데이터 호출 중 오류 발생: {e}")
        return []

def get_year_data_in_chunks():
    """최근 1년 데이터를 3개월 단위(약 90일)로 나누어 수집"""
    all_data = []
    today = datetime.now()
    one_year_ago = today - timedelta(days=365)
    
    current_start = one_year_ago
    while current_start < today:
        # 3개월(90일) 뒤를 종료일로 설정 (오늘 날짜를 넘지 않도록)
        current_end = current_start + timedelta(days=90)
        if current_end > today:
            current_end = today
            
        # 데이터 수집
        chunk_data = get_contract_data_by_period(current_start, current_end)
        all_data.extend(chunk_data)
        
        # API 과부하 방지를 위한 짧은 휴식
        time.sleep(1)
        
        # 다음 구간 시작일을 이전 구간 종료일 + 1일로 설정
        current_start = current_end + timedelta(days=1)
        
    return pd.DataFrame(all_data)

def update_google_sheet(df):
    if df.empty:
        print("수집된 데이터가 없어 시트를 업데이트하지 않습니다.")
        return

    # 구글 서비스 계정 인증
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    # 시트 열기
    spreadsheet = client.open("군수품조달_국내_계약정보")
    sheet = spreadsheet.get_worksheet(0)
    
    # 데이터 정리: 중복 제거 (계약번호 등 고유 키가 있다면 기준 설정 가능)
    # df = df.drop_duplicates(subset=['cntrctNo']) # 필요 시 활성화
    
    # 시트 비우기 및 헤더 포함 데이터 업로드
    sheet.clear()
    header = df.columns.tolist()
    values = df.fillna('').values.tolist()
    
    # 데이터 양이 많을 경우를 대비해 분할 업로드 혹은 전체 업로드
    sheet.update([header] + values)
    print(f"최종 총 {len(df)}건의 데이터를 업데이트 완료했습니다.")

if __name__ == "__main__":
    try:
        df_year = get_year_data_in_chunks()
        update_google_sheet(df_year)
    except Exception as e:
        print(f"전체 실행 과정 중 오류 발생: {e}")
