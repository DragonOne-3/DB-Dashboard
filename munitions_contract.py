import json
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET

import gspread
import requests
from oauth2client.service_account import ServiceAccountCredentials

SERVICE_KEY = os.environ["DATA_GO_KR_API_KEY"]
GOOGLE_AUTH_JSON = os.environ["GOOGLE_AUTH_JSON"]
LOOKBACK_DAYS = int(os.environ.get("PROCUREMENT_LOOKBACK_DAYS", "7"))
PAGE_SIZE = int(os.environ.get("PROCUREMENT_PAGE_SIZE", "500"))
REQUEST_TIMEOUT = int(os.environ.get("PROCUREMENT_REQUEST_TIMEOUT", "90"))
SEOUL = ZoneInfo("Asia/Seoul")
SPREADSHEET_NAME = "군수품조달_국내_계약정보"
API_URL = "https://apis.data.go.kr/1690000/CntrctInfoService/getDmstcCntrctInfoList"


def google_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(GOOGLE_AUTH_JSON), scope)
    return gspread.authorize(creds)


def request_xml(params, max_retries=4):
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(API_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            code = root.findtext(".//resultCode")
            if code and code not in {"00", "0"}:
                raise RuntimeError(f"API {code}: {root.findtext('.//resultMsg') or '오류'}")
            return root
        except Exception as exc:
            last_error = exc
            if attempt < max_retries:
                wait = min(30, attempt * 5)
                print(f"  재시도 {attempt}/{max_retries}: {exc} ({wait}초 후)")
                time.sleep(wait)
    raise RuntimeError(f"API 호출 최종 실패: {last_error}")


def fetch_date(target_date):
    items, page = [], 1
    while True:
        root = request_xml({
            "serviceKey": SERVICE_KEY,
            "cntrctDateBegin": target_date,
            "cntrctDateEnd": target_date,
            "numOfRows": str(PAGE_SIZE),
            "pageNo": str(page),
        })
        page_items = root.findall(".//item")
        if not page_items:
            break
        items.extend({child.tag: child.text or "" for child in item} for item in page_items)
        total = int(root.findtext(".//totalCount") or len(items))
        if len(items) >= total:
            break
        page += 1
        time.sleep(0.4)
    return items


def headers_for(items):
    headers, seen = [], set()
    for item in items:
        for key in item:
            if key not in seen:
                seen.add(key)
                headers.append(key)
    return headers


def overwrite_tab(spreadsheet, title, items):
    headers = headers_for(items)
    rows = [[item.get(h, "") for h in headers] for item in items]
    if not headers:
        headers, rows = ["_조회결과"], [["0건"]]
    try:
        sheet = spreadsheet.worksheet(title)
        sheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=title, rows=max(1000, len(rows) + 1), cols=max(10, len(headers) + 2))
    if sheet.row_count < len(rows) + 1 or sheet.col_count < len(headers):
        sheet.resize(rows=max(sheet.row_count, len(rows) + 1), cols=max(sheet.col_count, len(headers)))
    sheet.update(range_name="A1", values=[headers] + rows, value_input_option="RAW")


def main():
    now = datetime.now(SEOUL)
    spreadsheet = google_client().open(SPREADSHEET_NAME)
    targets = [(now - timedelta(days=offset)).strftime("%Y%m%d") for offset in range(LOOKBACK_DAYS, 0, -1)]
    for target in targets:
        print(f"=== 계약정보 {target} 수집 ===")
        items = fetch_date(target)
        overwrite_tab(spreadsheet, target, items)
        print(f"완료: {len(items):,}건")


if __name__ == "__main__":
    main()
