import os
import json
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import time
from concurrent.futures import ThreadPoolExecutor

# 환경 변수 로드
try:
    SERVICE_KEY = os.environ['DATA_GO_KR_API_KEY']
    GOOGLE_AUTH_JSON = os.environ['GOOGLE_AUTH_JSON']
    print(">>> [확인] 환경 변수 로드 성공")
except KeyError as e:
    print(f">>> [에러] 환경 변수가 설정되지 않았습니다: {e}")

def fetch_g2b_period(date_range):
    start_dt, end_dt = date_range
    url = 'https://apis.data.go.kr/1230000/ao/OrderPlanSttusService/getOrderPlanSttusListServcPPSSrch'
    all_items = []
    
    params = {
        'serviceKey': SERVICE_KEY,
        'pageNo': '1',
        'numOfRows': '500',
        'type': 'json',
        'inqryBgnDt': start_dt + '0000',
        'inqryEndDt': end_dt + '2359'
    }
    
    try:
        print(f"    [시도] {start_dt} ~ {end_dt} 요청...")
        response = requests.get(url, params=params, timeout=30)
        
        # XML로 응답이 올 경우(주로 키 에러) 대응
        if response.text.startswith('<'):
            print(f"    [경고] {start_dt} 구간: JSON이 아닌 XML 응답이 왔습니다. 인증키를 확인하세요.")
            print(f"    [응답내용]: {response.text[:200]}") # 에러 메시지 앞부분 출력
            return []

        data = response.json()
        items = data.get('response', {}).get('body', {}).get('items', [])
        
        if items:
            print(f"    [성공] {start_dt} 구간: {len(items)}건 수집")
            return items
        else:
            print(f"    [알림] {start_dt} 구간: 데이터가 없습니다.")
            return []
            
    except Exception as e:
        print(f"    [오류] {start_dt} 구간 호출 실패: {e}")
        return []

def run_process():
    print(">>> [1단계] 구글 시트 연결 중...")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds_dict = json.loads(GOOGLE_AUTH_JSON)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open("나라장터_용역_발주계획")
        sheet = spreadsheet.get_worksheet(0)
        print(">>> [확인] 구글 시트 연결 성공")
    except Exception as e:
        print(f">>> [에러] 구글 시트 연결 실패: {e}")
        return

    print(">>> [2단계] 날짜 구간 생성 중...")
    start_date = datetime(2024, 1, 1)
    end_date = datetime.now()
    date_ranges = []
    temp_dt = start_date
    while temp_dt <= end_date:
        d_start = temp_dt.strftime('%Y%m%d')
        d_end_dt = temp_dt + timedelta(days=14)
        if d_end_dt > end_date: d_end_dt = end_date
        date_ranges.append((d_start, d_end_dt.strftime('%Y%m%d')))
        temp_dt = d_end_dt + timedelta(days=1)

    print(f">>> [3단계] 병렬 수집 시작 (구간 개수: {len(date_ranges)})...")
    final_results = []
    # 데이터가 안 들어올 때는 max_workers를 1로 해서 순차적으로 확인하는 게 좋습니다.
    for dr in date_ranges:
        res = fetch_g2b_period(dr)
        final_results.extend(res)
        time.sleep(0.5)

    if final_results:
        print(f">>> [4단계] 데이터 저장 중... (총 {len(final_results)}건)")
        df = pd.DataFrame(final_results)
        
        # 제목행 처리
        if not sheet.row_values(1):
            sheet.insert_row(df.columns.tolist(), 1)
        
        values = df.fillna('').values.tolist()
        sheet.append_rows(values)
        print(">>> [완료] 모든 데이터가 업데이트되었습니다.")
    else:
        print(">>> [실패] 수집된 데이터가 최종적으로 0건입니다.")

if __name__ == "__main__":
    print("====================================")
    print("  G2B 발주계획 수집 스크립트 시작")
    print(f"  실행 시간: {datetime.now()}")
    print("====================================")
    run_process()
