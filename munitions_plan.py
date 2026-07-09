import os
import json
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import xml.etree.ElementTree as ET
import time

# 환경 변수 로드
SERVICE_KEY = os.environ['DATA_GO_KR_API_KEY']
GOOGLE_AUTH_JSON = os.environ['GOOGLE_AUTH_JSON']


def fetch_monthly_plan(target_month):
    """특정 월(YYYYMM)의 모든 발주계획 데이터를 수집"""
    url = 'https://apis.data.go.kr/1690000/PrcurePlanInfoService/getDmstcPrcurePlanList'
    all_items = []
    page_no = 1

    while True:
        params = {
            'serviceKey': SERVICE_KEY,
            'orderPrearngeMtBegin': target_month,
            'orderPrearngeMtEnd': target_month,
            'numOfRows': '500',
            'pageNo': str(page_no)
        }

        try:
            response = requests.get(url, params=params, timeout=60)
            if response.status_code != 200:
                print(f"  [오류] HTTP {response.status_code} 응답")
                break
            root = ET.fromstring(response.content)
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
        except Exception as e:
            print(f"  [오류] {target_month} 수집 중 에러: {e}")
            break

    return all_items


def get_or_create_monthly_sheet(spreadsheet, target_month, header):
    """
    이번 달 이름의 워크시트를 가져오거나, 없으면 새로 생성.
    이미 있으면 내용을 전부 지우고(clear) 헤더만 다시 씀.
    """
    sheet_name = target_month  # 예: '202607'

    try:
        sheet = spreadsheet.worksheet(sheet_name)
        # 기존 탭이 있으면 통째로 비움 (헤더 포함 전체 삭제 후 재작성)
        sheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        # 없으면 새로 생성
        sheet = spreadsheet.add_worksheet(
            title=sheet_name, rows="1000", cols=str(len(header) + 5)
        )

    # 헤더 다시 씀
    sheet.append_row(header, value_input_option='RAW')
    return sheet


def run_process():
    # 1. 구글 인증 및 시트 열기
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open("군수품조달_국내_발주계획")

    # 2. 이번 달 날짜 설정 (YYYYMM)
    # 발주계획은 수시로 업데이트되므로, 매일 실행 시 이번 달 전체를 다시 확인하여
    # "덮어쓰기" 방식으로 항상 최신 상태를 유지합니다. (중복 누적 방지)
    current_month = datetime.now().strftime('%Y%m')

    print(f"====================================")
    print(f">>> {current_month} 발주계획 업데이트 시작")
    print(f"====================================")

    # 3. 데이터 수집
    items = fetch_monthly_plan(current_month)

    # 4. 데이터 저장 (해당 월 탭을 지우고 최신 데이터로 통째로 재작성)
    if items:
        df = pd.DataFrame(items)
        header = df.columns.tolist()
        values = df.fillna('').values.tolist()

        sheet = get_or_create_monthly_sheet(spreadsheet, current_month, header)
        sheet.append_rows(values, value_input_option='RAW')

        print(f"✅ {current_month} 탭 갱신 완료. 총 {len(items)}건 (최신 상태로 덮어쓰기).")
    else:
        print(f"ℹ️ {current_month}에 해당하는 발주계획 데이터가 없습니다.")


if __name__ == "__main__":
    run_process()
