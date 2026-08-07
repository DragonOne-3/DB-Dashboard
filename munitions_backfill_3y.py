import os
import json
import time
import calendar
from datetime import datetime
import xml.etree.ElementTree as ET

import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ========================================================
# 환경 변수
# ========================================================
SERVICE_KEY = os.environ['DATA_GO_KR_API_KEY']
GOOGLE_AUTH_JSON = os.environ['GOOGLE_AUTH_JSON']

# 워크플로우 입력값으로 넘어오는 기간/대상 서비스
START_MONTH = os.environ['BACKFILL_START_MONTH']   # YYYYMM, 예: "202301"
END_MONTH = os.environ['BACKFILL_END_MONTH']        # YYYYMM, 예: "202412"

# "plan,contract,bid" 형태의 콤마 구분 문자열 (workflow에서 boolean 입력값 조합해서 만들어 넘겨줌)
SELECTED_SERVICE_KEYS = {
    key.strip() for key in os.environ.get('BACKFILL_SERVICES', 'plan,contract,bid').split(',')
    if key.strip()
}

BACKFILL_SHEET_NAME = "백필"  # 데이터가 쌓이는 탭 이름

# 서비스 3개 정의 (URL, 파라미터명, 날짜 형식만 다르고 로직은 공통 함수로 처리)
SERVICES = [
    {
        "key": "plan",
        "label": "발주계획",
        "spreadsheet": "군수품조달_국내_발주계획",
        "url": "https://apis.data.go.kr/1690000/PrcurePlanInfoService/getDmstcPrcurePlanList",
        "begin_param": "orderPrearngeMtBegin",
        "end_param": "orderPrearngeMtEnd",
        "date_mode": "month",       # YYYYMM 그대로 사용
    },
    {
        "key": "contract",
        "label": "계약정보",
        "spreadsheet": "군수품조달_국내_계약정보",
        "url": "https://apis.data.go.kr/1690000/CntrctInfoService/getDmstcCntrctInfoList",
        "begin_param": "cntrctDateBegin",
        "end_param": "cntrctDateEnd",
        "date_mode": "day_range",   # 해당 월의 1일 ~ 말일
    },
    {
        "key": "bid",
        "label": "입찰공고",
        "spreadsheet": "군수품조달_국내_입찰공고",
        "url": "https://apis.data.go.kr/1690000/BidPblancInfoService/getDmstcCmpetBidPblancList",
        "begin_param": "anmtDateBegin",
        "end_param": "anmtDateEnd",
        "date_mode": "day_range",
    },
]


# ========================================================
# 유틸: 월 범위 계산
# ========================================================
def build_month_list(start_yyyymm, end_yyyymm):
    """start_yyyymm ~ end_yyyymm 사이 월(YYYYMM) 리스트를 과거 -> 현재 순서로 생성"""
    start_year, start_month = int(start_yyyymm[:4]), int(start_yyyymm[4:6])
    end_year, end_month = int(end_yyyymm[:4]), int(end_yyyymm[4:6])

    start_idx = start_year * 12 + (start_month - 1)
    end_idx = end_year * 12 + (end_month - 1)

    if start_idx > end_idx:
        raise ValueError(f"시작월({start_yyyymm})이 종료월({end_yyyymm})보다 뒤에 있습니다.")

    months = []
    idx = start_idx
    while idx <= end_idx:
        y, m = idx // 12, idx % 12 + 1
        months.append(f"{y}{m:02d}")
        idx += 1
    return months


def month_to_range(yyyymm, date_mode):
    """date_mode에 따라 API에 넘길 (begin, end) 문자열 계산"""
    year = int(yyyymm[:4])
    month = int(yyyymm[4:6])
    if date_mode == "month":
        return yyyymm, yyyymm
    # day_range: 해당 월 1일 ~ 말일
    last_day = calendar.monthrange(year, month)[1]
    begin = f"{yyyymm}01"
    end = f"{yyyymm}{last_day:02d}"
    return begin, end


# ========================================================
# API 호출 (페이지네이션 + 타임아웃 재시도 공통 처리)
# ========================================================
def fetch_range_data(service, begin, end, max_retries=3):
    all_items = []
    page_no = 1

    while True:
        params = {
            'serviceKey': SERVICE_KEY,
            service['begin_param']: begin,
            service['end_param']: end,
            'numOfRows': '500',
            'pageNo': str(page_no),
        }

        root = None
        for attempt in range(max_retries):
            try:
                response = requests.get(service['url'], params=params, timeout=90)
                if response.status_code != 200:
                    print(f"    [오류] HTTP {response.status_code}")
                    return all_items
                root = ET.fromstring(response.content)
                break
            except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout):
                wait = (attempt + 1) * 10
                print(f"    [타임아웃] {attempt + 1}/{max_retries}회차, {wait}초 후 재시도...")
                time.sleep(wait)
            except Exception as e:
                print(f"    [예외] {begin}~{end} 수집 중 오류: {e}")
                return all_items

        if root is None:
            print(f"    [실패] {begin}~{end} 최종 타임아웃, 이번 월은 건너뜁니다.")
            return all_items

        items = root.findall('.//item')
        if not items:
            break

        for item in items:
            all_items.append({child.tag: child.text for child in item})

        total_element = root.find('.//totalCount')
        if total_element is not None:
            total_count = int(total_element.text)
            if len(all_items) >= total_count:
                break
            page_no += 1
            time.sleep(0.5)
        else:
            break

    return all_items


# ========================================================
# 구글 시트 관련
# ========================================================
def get_or_create_worksheet(spreadsheet, title, rows="2000", cols="30"):
    try:
        return spreadsheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


def append_backfill_rows(spreadsheet, items):
    if not items:
        return
    df = pd.DataFrame(items)
    data_sheet = get_or_create_worksheet(
        spreadsheet, BACKFILL_SHEET_NAME, rows="20000", cols=str(len(df.columns) + 5)
    )

    first_row = data_sheet.row_values(1)
    if not first_row:
        header = df.columns.tolist()
        data_sheet.append_row(header, value_input_option='RAW')

    values = df.fillna('').values.tolist()
    data_sheet.append_rows(values, value_input_option='RAW')


# ========================================================
# 메인 로직
# ========================================================
def process_service(client, service, month_list):
    spreadsheet = client.open(service['spreadsheet'])

    print(f"\n>>> [{service['label']}] {month_list[0]} ~ {month_list[-1]} 수집 시작 (총 {len(month_list)}개월)")

    for yyyymm in month_list:
        begin, end = month_to_range(yyyymm, service['date_mode'])
        print(f"  - {yyyymm} ({begin}~{end}) 수집 중...")
        items = fetch_range_data(service, begin, end)
        append_backfill_rows(spreadsheet, items)
        print(f"    -> {len(items)}건 수집 및 저장 완료")
        time.sleep(1)

    print(f">>> [{service['label']}] 수집 완료!")


def run_backfill():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    month_list = build_month_list(START_MONTH, END_MONTH)
    target_services = [s for s in SERVICES if s['key'] in SELECTED_SERVICE_KEYS]

    if not target_services:
        print("선택된 서비스가 없습니다. (plan/contract/bid 중 최소 1개는 선택해야 합니다)")
        return

    print(f"수집 대상 기간: {month_list[0]} ~ {month_list[-1]} (총 {len(month_list)}개월)")
    print(f"수집 대상 서비스: {', '.join(s['label'] for s in target_services)}")

    for service in target_services:
        process_service(client, service, month_list)

    print("\n🎉 선택한 기간·서비스 수집이 모두 완료되었습니다!")


if __name__ == "__main__":
    run_backfill()
