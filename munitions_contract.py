from __future__ import annotations

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from procurement_common import (
    date_chunks, digits, fetch_paged, first_value, google_client,
    merge_rows, parallel_collect, read_existing, rewrite_single_sheet,
    stable_fallback_key,
)

API_URL = "https://apis.data.go.kr/1690000/CntrctInfoService/getDmstcCntrctInfoList"
SPREADSHEET_NAME = "군수품조달_국내_계약정보"
LOOKBACK_DAYS = int(os.environ.get("PROCUREMENT_LOOKBACK_DAYS", "7"))
RETENTION_DAYS = int(os.environ.get("PROCUREMENT_RETENTION_DAYS", "365"))
FULL_REFRESH = os.environ.get("PROCUREMENT_FULL_REFRESH", "false").lower() == "true"
SEOUL = ZoneInfo("Asia/Seoul")

ID_COLUMNS = ["cntrctNo", "contractNo", "계약번호", "cntrctInfoId", "id", "dcsNo", "판단번호"]
DATE_COLUMNS = ["cntrctDate", "계약일자", "계약일", "contractDate"]
TITLE_COLUMNS = ["cntrctNm", "계약명", "prdlstNm", "품명", "사업명"]
AGENCY_COLUMNS = ["ornt", "발주기관", "dminsttNm", "수요기관"]
AMOUNT_COLUMNS = ["cntrctAmount", "계약금액", "totCntrctAmt", "총계약금액"]


def row_date(row: dict) -> str:
    value = digits(first_value(row, DATE_COLUMNS))
    return value[:8] if len(value) >= 8 else ""


def row_key(row: dict) -> str:
    identifier = first_value(row, ID_COLUMNS)
    if identifier:
        return f"id:{identifier}"
    return stable_fallback_key([
        first_value(row, TITLE_COLUMNS), first_value(row, AGENCY_COLUMNS),
        row_date(row), digits(first_value(row, AMOUNT_COLUMNS)),
    ])


def fetch_chunk(start, end):
    return fetch_paged(API_URL, {
        "cntrctDateBegin": start.strftime("%Y%m%d"),
        "cntrctDateEnd": end.strftime("%Y%m%d"),
    })


def main() -> None:
    today = datetime.now(SEOUL).date()
    end = today - timedelta(days=1)
    client = google_client()
    sheet = client.open(SPREADSHEET_NAME).get_worksheet(0)
    existing = read_existing(sheet)
    full = FULL_REFRESH or not existing
    start = today - timedelta(days=RETENTION_DAYS if full else LOOKBACK_DAYS)
    cutoff = (today - timedelta(days=RETENTION_DAYS)).strftime("%Y%m%d")

    print(f"모드: {'최근 1년 전체 재수집' if full else f'최근 {LOOKBACK_DAYS}일 증분 갱신'}")
    fresh = parallel_collect(date_chunks(start, end, 30), fetch_chunk, "계약정보")
    final_rows = merge_rows(
        [] if full else existing,
        fresh,
        row_key,
        lambda row: not row_date(row) or row_date(row) >= cutoff,
    )
    rewrite_single_sheet(sheet, final_rows, row_date)
    print(f"완료: API {len(fresh):,}건 / 중복 제거 후 {len(final_rows):,}건")


if __name__ == "__main__":
    main()
