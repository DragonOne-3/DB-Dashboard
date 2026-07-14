from __future__ import annotations

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from procurement_common import (
    digits, fetch_paged, first_value, google_client, merge_rows,
    read_existing, rewrite_single_sheet, stable_fallback_key,
)

API_URL = "https://apis.data.go.kr/1690000/PrcurePlanInfoService/getDmstcPrcurePlanList"
SPREADSHEET_NAME = "군수품조달_국내_발주계획"
RETENTION_DAYS = int(os.environ.get("PROCUREMENT_RETENTION_DAYS", "365"))
FULL_REFRESH = os.environ.get("PROCUREMENT_FULL_REFRESH", "false").lower() == "true"
SEOUL = ZoneInfo("Asia/Seoul")

ID_COLUMNS = ["dcsNo", "판단번호", "prcurePlanNo", "발주계획번호", "id"]
DATE_COLUMNS = ["orderPrearngeMt", "발주예정월", "demandYear", "요구년도"]
TITLE_COLUMNS = ["reprsntPrdlstNm", "대표품목명", "사업명", "품명"]
AGENCY_COLUMNS = ["ornt", "발주기관", "orntCode", "발주기관코드"]
AMOUNT_COLUMNS = ["budgetAmount", "예산금액", "예정가격"]


def row_month(row: dict) -> str:
    value = digits(first_value(row, DATE_COLUMNS))
    if len(value) >= 6:
        return value[:6]
    return value[:4] + "01" if len(value) >= 4 else ""


def row_key(row: dict) -> str:
    identifier = first_value(row, ID_COLUMNS)
    if identifier:
        return f"id:{identifier}"
    return stable_fallback_key([
        first_value(row, TITLE_COLUMNS), first_value(row, AGENCY_COLUMNS),
        row_month(row), digits(first_value(row, AMOUNT_COLUMNS)),
    ])


def month_shift(year: int, month: int, delta: int) -> str:
    index = year * 12 + month - 1 + delta
    return f"{index // 12:04d}{index % 12 + 1:02d}"


def target_months(now, full: bool) -> list[str]:
    count = 13 if full else 2
    return [month_shift(now.year, now.month, -offset) for offset in range(count - 1, -1, -1)]


def fetch_month(month: str) -> list[dict]:
    return fetch_paged(API_URL, {"orderPrearngeMtBegin": month, "orderPrearngeMtEnd": month})


def main() -> None:
    today = datetime.now(SEOUL).date()
    client = google_client()
    sheet = client.open(SPREADSHEET_NAME).get_worksheet(0)
    existing = read_existing(sheet)
    full = FULL_REFRESH or not existing

    fresh = []
    print(f"모드: {'최근 1년 전체 재수집' if full else '현재월·직전월 증분 갱신'}")
    for month in target_months(today, full):
        batch = fetch_month(month)
        fresh.extend(batch)
        print(f"=== 발주계획 {month}: {len(batch):,}건 ===")

    cutoff = (today - timedelta(days=RETENTION_DAYS)).strftime("%Y%m")
    final_rows = merge_rows(
        [] if full else existing,
        fresh,
        row_key,
        lambda row: not row_month(row) or row_month(row) >= cutoff,
    )
    rewrite_single_sheet(sheet, final_rows, row_month)
    print(f"완료: API {len(fresh):,}건 / 중복 제거 후 {len(final_rows):,}건")


if __name__ == "__main__":
    main()
