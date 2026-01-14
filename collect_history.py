import os, json, time, datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests

# --- 설정 (직접 입력하거나 환경변수 사용) ---
AUTH_JSON_STR = os.environ.get('GOOGLE_AUTH_JSON')
API_KEY = os.environ.get('DATA_GO_KR_API_KEY')

def get_gspread_client():
    creds_dict = json.loads(AUTH_JSON_STR)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
    return gspread.authorize(creds)

def get_or_create_sheet(client, year, month):
    quarter = (month - 1) // 3 + 1
    file_name = f"조달청_납품내역_{year}_{quarter}분기"
    sheet_name = f"{year}_{month}월"
    
    try:
        sh = client.open(file_name)
    except gspread.SpreadsheetNotFound:
        sh = client.create(file_name)
        print(f"🆕 새 파일 생성: {file_name}")
    
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        # 헤더 포함하여 탭 생성 (열 개수는 기존 데이터와 맞춰 40개로 넉넉히 설정)
        ws = sh.add_worksheet(title=sheet_name, rows="1000", cols="40")
        print(f"  └ 📑 새 탭 생성: {sheet_name}")
    return ws

def fetch_data(start_date, end_date):
    """한 달치 데이터를 페이징하며 모두 가져오기"""
    all_data = []
    page = 1
    while True:
        url = "http://apis.data.go.kr/1230000/IndstPrdct_Prdctn_01/getIndstPrdct_Prdctn_01" # 납품내역 API
        params = {
            'serviceKey': API_KEY,
            'type': 'json',
            'inqryBgnDate': start_date,
            'inqryEndDate': end_date,
            'numOfRows': '999',
            'pageNo': str(page)
        }
        try:
            res = requests.get(url, params=params, timeout=30)
            items = res.json().get('response', {}).get('body', {}).get('items', [])
            if not items: break
            
            # 딕셔너리를 리스트(행) 형태로 변환 (인덱스 순서 유지)
            # 여기서는 API에서 내려오는 전체 필드를 그대로 리스트화합니다.
            for item in items:
                all_data.append(list(item.values()))
            
            print(f"    - {start_date}~{end_date} : {page}페이지 수집 완료 ({len(items)}건)")
            if len(items) < 999: break # 마지막 페이지 확인
            page += 1
            time.sleep(0.3)
        except Exception as e:
            print(f"    ❌ 에러 발생: {e}")
            break
    return all_data

def main():
    client = get_gspread_client()
    
    # 2021년부터 2025년까지 반복
    for year in range(2021, 2023):
        for month in range(1, 13):
            start_dt = f"{year}{month:02d}01"
            # 해당 월의 마지막 날 계산
            if month == 12:
                end_dt = f"{year}1231"
            else:
                end_dt = (datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)).strftime("%Y%m%d")
            
            print(f"🚀 {year}년 {month}월 수집 시작...")
            
            # 1. API 데이터 수집
            monthly_rows = fetch_data(start_dt, end_dt)
            
            if monthly_rows:
                # 2. 구글 시트 연결
                ws = get_or_create_sheet(client, year, month)
                # 3. 데이터 일괄 저장 (append_rows는 리스트의 리스트를 받습니다)
                ws.append_rows(monthly_rows)
                print(f"✅ {year}년 {month}월 저장 성공! (총 {len(monthly_rows)}건)")
            else:
                print(f"➖ {year}년 {month}월 데이터가 없습니다.")
            
            time.sleep(1) # API/시트 제한 방지

if __name__ == "__main__":
    main()
