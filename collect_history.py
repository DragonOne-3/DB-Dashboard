import os, json, time, datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests

AUTH_JSON_STR = os.environ.get('GOOGLE_AUTH_JSON')
API_KEY = os.environ.get('DATA_GO_KR_API_KEY')
START_YEAR = int(os.environ.get('START_YEAR', 2021))
END_YEAR = int(os.environ.get('END_YEAR', 2025))

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
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows="1000", cols="40")
    return ws

def fetch_data(start_date, end_date):
    all_data = []
    page = 1
    while True:
        url = "http://apis.data.go.kr/1230000/IndstPrdct_Prdctn_01/getIndstPrdct_Prdctn_01"
        params = {
            'serviceKey': API_KEY, 'type': 'json',
            'inqryBgnDate': start_date, 'inqryEndDate': end_date,
            'numOfRows': '999', 'pageNo': str(page)
        }
        try:
            res = requests.get(url, params=params, timeout=30)
            items = res.json().get('response', {}).get('body', {}).get('items', [])
            if not items: break
            for item in items:
                all_data.append(list(item.values()))
            if len(items) < 999: break
            page += 1
            time.sleep(0.5)
        except Exception as e:
            print(f"Error: {e}")
            break
    return all_data

def main():
    client = get_gspread_client()
    for year in range(START_YEAR, END_YEAR + 1):
        for month in range(1, 13):
            start_dt = f"{year}{month:02d}01"
            if month == 12: end_dt = f"{year}1231"
            else: end_dt = (datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)).strftime("%Y%m%d")
            
            print(f"🚀 {year}년 {month}월 수집 중...")
            rows = fetch_data(start_dt, end_dt)
            if rows:
                ws = get_or_create_sheet(client, year, month)
                ws.append_rows(rows)
                print(f"✅ {year}-{month} 완료 ({len(rows)}건)")
            time.sleep(1)

if __name__ == "__main__":
    main()
