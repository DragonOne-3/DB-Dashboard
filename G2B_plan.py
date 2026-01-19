import os
import json
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import time
import xml.etree.ElementTree as ET

# 환경 변수 로드
SERVICE_KEY = os.environ['DATA_GO_KR_API_KEY']
GOOGLE_AUTH_JSON = os.environ['GOOGLE_AUTH_JSON']

def fetch_g2b_period(date_range):
    start_dt, end_dt = date_range
    # 나라장터 용역 발주계획 API 엔드포인트
    url = 'http://apis.data.go.kr/1230000/ao/OrderPlanSttusService/getOrderPlanSttusListServcPPSSrch'
    
    params = {
        'serviceKey': SERVICE_KEY,
        'pageNo': '1',
        'numOfRows': '999', # 한 번에 최대한 많이
        'type': 'json',
        'inqryBgnDt': start_dt + '0000',
        'inqryEndDt': end_dt + '2359'
    }
    
    try:
        # 인증키 인코딩 문제 방지를 위해 params를 직접 string으로 넘기지 않고 requests에 맡김
        response = requests.get(url, params=params, timeout=30)
        
        # 만약 XML 에러가 왔을 경우 파싱해서 메시지 확인
        if response.text.strip().startswith('<'):
            print(f"    [확인] {start_dt} 구간: XML 응답 수신 (인증키 확인 필요)")
            try:
                root = ET.fromstring(response.text)
                msg = root.find('.//returnAuthMsg')
                if msg is not None:
                    print(f"    [API 메시지]: {msg.text}")
                else:
                    print(f"    [응답내용 일부]: {response.text[:100]}")
            except:
                pass
            return []

        # 정상적인 JSON 응답 처리
        data = response.json()
        body = data.get('response', {}).get('body', {})
        items = body.get('items', [])
        
        # items가 리스트가 아닌 경우 처리 (데이터가 1건이면 딕셔너리로 올 때가 있음)
        if isinstance(items, dict):
            items = [items]
            
        if items:
            print(f"    [성공] {start_dt} ~ {end_dt}: {len(items)}건 수집")
            return items
        return []
            
    except Exception as e:
        print(f"    [오류] {start_dt} 구간: {str(e)}")
        return []

def run_process():
    # 1. 구글 시트 연결
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open("나라장터_용역_발주계획")
    sheet = spreadsheet.get_worksheet(0)

    # 2. 날짜 구간 생성 (1개월 단위로 조회 - 나라장터는 구간이 너무 길면 에러날 수 있음)
    start_date = datetime(2024, 1, 1)
    end_date = datetime.now()
    date_ranges = []
    temp_dt = start_date
    while temp_dt <= end_date:
        d_start = temp_dt.strftime('%Y%m%d')
        # 약 30일 단위로 끊기
        d_end_dt = temp_dt + timedelta(days=30)
        if d_end_dt > end_date: d_end_dt = end_date
        date_ranges.append((d_start, d_end_dt.strftime('%Y%m%d')))
        temp_dt = d_end_dt + timedelta(days=1)

    # 3. 순차 수집 (병렬보다는 순차가 에러 확인에 용이함)
    all_data = []
    print(f">>> {len(date_ranges)}개 구간 수집 시작...")
    for dr in date_ranges:
        res = fetch_g2b_period(dr)
        all_data.extend(res)
        time.sleep(0.8) # 서버 부하 방지

    # 4. 저장
    if all_data:
        df = pd.DataFrame(all_data)
        # 제목행
        if not sheet.row_values(1):
            sheet.insert_row(df.columns.tolist(), 1)
        
        # 기존 데이터 유지하며 추가
        values = df.fillna('').values.tolist()
        sheet.append_rows(values)
        print(f">>> 총 {len(all_data)}건 업데이트 완료")
    else:
        print(">>> 수집된 데이터가 없습니다. 서비스 키의 권한을 확인하세요.")

if __name__ == "__main__":
    run_process()
