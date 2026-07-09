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

# ========================================================
# 설정
# ========================================================
BACKFILL_TOTAL_MONTHS = 36          # 3년치 (36개월)
BACKFILL_SHEET_NAME = "백필"         # 데이터가 쌓이는 탭 이름
PROGRESS_SHEET_NAME = "백필_진행상황"  # 진행상황을 저장하는 탭 이름

# 실행 1회당 서비스별로 처리할 "월" 개수. 기본값 1 = 한 번 실행할 때 딱 1개월만 처리.
# 쿼터 여유가 있어서 더 빠르게 돌리고 싶으면 환경변수로 늘릴 수 있음 (예: BACKFILL_STEPS_PER_RUN=3)
STEPS_PER_RUN = int(os.environ.get('BACKFILL_STEPS_PER_RUN', '1'))

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
# 유틸: 월 계산
# ========================================================
def shift_month(year, month, offset):
    """year, month(1~12)에서 offset개월만큼 이동한 (year, month) 반환"""
    idx = (year * 12 + (month - 1)) + offset
    return idx // 12, idx % 12 + 1


def build_month_list():
    """3년(36개월) 치 대상 월(YYYYMM) 리스트를 과거 -> 현재 순서로 생성"""
    now = datetime.now()
    months = []
    for offset in range(-(BACKFILL_TOTAL_MONTHS - 1), 1):
        y, m = shift_month(now.year, now.month, offset)
        months.append(f"{y}{m:02d}")
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


def read_progress(spreadsheet):
    """진행상황 탭에서 마지막으로 완료된 월(YYYYMM) 또는 'DONE'을 읽어옴. 없으면 None."""
    sheet = get_or_create_worksheet(spreadsheet, PROGRESS_SHEET_NAME, rows="10", cols="5")
    values = sheet.get_all_values()
    if len(values) < 1 or len(values[0]) < 2:
        return sheet, None
    last = values[0][1].strip()
    return sheet, (last if last else None)


def write_progress(progress_sheet, last_completed):
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    progress_sheet.update('A1:B2', [
        ['last_completed_month', last_completed],
        ['updated_at', now_str],
    ])


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
def process_one_service_step(client, service, month_list):
    """
    이 서비스에서 아직 처리 안 된 월을 STEPS_PER_RUN개만큼 처리.
    이번 실행에서 뭔가 처리했으면 True, 이미 다 끝나 있었으면 False 반환.
    """
    spreadsheet = client.open(service['spreadsheet'])
    progress_sheet, last_completed = read_progress(spreadsheet)

    if last_completed == "DONE":
        return False  # 이 서비스는 이미 완료됨

    if last_completed is None:
        start_idx = 0
    else:
        try:
            start_idx = month_list.index(last_completed) + 1
        except ValueError:
            # 진행상황 값이 이상하면 안전하게 처음부터 다시 시작
            start_idx = 0

    if start_idx >= len(month_list):
        write_progress(progress_sheet, "DONE")
        return False

    end_idx = min(start_idx + STEPS_PER_RUN, len(month_list))
    target_months = month_list[start_idx:end_idx]

    print(f"\n>>> [{service['label']}] {target_months[0]} ~ {target_months[-1]} 수집 시작 "
          f"({start_idx + 1}~{end_idx} / {len(month_list)}개월)")

    for yyyymm in target_months:
        begin, end = month_to_range(yyyymm, service['date_mode'])
        print(f"  - {yyyymm} ({begin}~{end}) 수집 중...")
        items = fetch_range_data(service, begin, end)
        append_backfill_rows(spreadsheet, items)
        print(f"    -> {len(items)}건 수집 및 저장 완료")

        # 월 단위로 진행상황을 즉시 저장 -> 중간에 끊겨도 다음 실행에서 이어서 진행
        write_progress(progress_sheet, yyyymm)
        time.sleep(1)

    if end_idx >= len(month_list):
        write_progress(progress_sheet, "DONE")
        print(f">>> [{service['label']}] 3년치 백필 완료!")

    return True


def run_backfill():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    month_list = build_month_list()
    print(f"백필 대상 기간: {month_list[0]} ~ {month_list[-1]} (총 {len(month_list)}개월)")
    print(f"이번 실행에서는 서비스당 최대 {STEPS_PER_RUN}개월씩만 처리합니다.")

    for service in SERVICES:
        did_work = process_one_service_step(client, service, month_list)
        if did_work:
            # 여러 서비스를 한 번에 몰아서 호출하면 쿼터 초과 위험이 있으므로,
            # 이번 실행에서는 아직 안 끝난 첫 번째 서비스만 처리하고 종료.
            print("\n이번 실행은 여기까지 처리했습니다. 스크립트를 다시 실행하면 이어서 진행됩니다.")
            return
        else:
            print(f"[{service['label']}] 이미 백필 완료 상태 -> 다음 서비스 확인")

    print("\n🎉 3개 서비스 모두 3년치 백필이 완료되었습니다!")


if __name__ == "__main__":
    run_backfill()
