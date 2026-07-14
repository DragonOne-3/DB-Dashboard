from __future__ import annotations

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from procurement_common import (
    date_chunks,
    digits,
    fetch_paged,
    first_value,
    google_client,
    merge_rows,
    parallel_collect,
    read_existing,
    rewrite_single_sheet,
    stable_fallback_key,
)

API_URL = "https://apis.data.go.kr/1690000/BidPblancInfoService/getDmstcCmpetBidPblancList"
SPREADSHEET_NAME = "군수품조달_국내_입찰공고"
LOOKBACK_DAYS = int(os.environ.get("PROCUREMENT_LOOKBACK_DAYS", "7"))
RETENTION_DAYS = int(os.environ.get("PROCUREMENT_RETENTION_DAYS", "365"))
FULL_REFRESH = os.environ.get("PROCUREMENT_FULL_REFRESH", "false").lower() == "true"
SEOUL = ZoneInfo("Asia/Seoul")

# 실제 방위사업청 국내 경쟁입찰공고 응답 필드와 기존 한글 변환 필드를 함께 지원한다.
G2B_ID_COLUMNS = ["g2bPblancNo", "G2B공고번호"]
NOTICE_ID_COLUMNS = [
    "pblancNo",
    "공고번호",
    "bidPblancNo",
    "bidNtceNo",
    "입찰공고번호",
]
DECISION_ID_COLUMNS = ["dcsNo", "판단번호"]
ORDER_COLUMNS = ["pblancOdr", "공고차수", "g2bPblancOdr", "G2B공고차수"]
DATE_COLUMNS = [
    "pblancDate",
    "공고일자",
    "anmtDate",
    "bidNtceDate",
    "공고일",
]
TITLE_COLUMNS = [
    "bidNm",
    "입찰명",
    "bidPblancNm",
    "bidNtceNm",
    "공고명",
    "입찰공고명",
    "사업명",
]
AGENCY_COLUMNS = [
    "orntCode",
    "발주기관코드",
    "ornt",
    "발주기관",
    "dminsttNm",
    "수요기관",
]
AMOUNT_COLUMNS = [
    "bsisPrdprc",
    "기초예가",
    "asignBdgtAmt",
    "배정예산",
    "presmptPrce",
    "추정가격",
    "기초예비가격",
]
YEAR_COLUMNS = ["demandYear", "요구년도"]


def row_date(row: dict) -> str:
    """공고일자를 YYYYMMDD 문자열로 반환한다."""
    value = digits(first_value(row, DATE_COLUMNS))
    return value[:8] if len(value) >= 8 else ""


def row_key(row: dict) -> str:
    """
    동일 공고를 안정적으로 식별한다.

    우선순위:
    1. G2B 공고번호 + 공고차수
    2. 발주기관코드 + 자체 공고번호 + 공고차수
    3. 발주기관코드 + 요구연도 + 판단번호 + 공고차수
    4. 제목·기관·공고일·금액·차수 조합 해시

    공고차수를 포함하므로 정정·재공고가 서로 잘못 합쳐지는 것을 방지한다.
    """
    order = first_value(row, ORDER_COLUMNS).strip()

    g2b_no = first_value(row, G2B_ID_COLUMNS).strip()
    if g2b_no:
        return f"g2b:{g2b_no}:{order}"

    agency_code = first_value(row, ["orntCode", "발주기관코드"]).strip()
    notice_no = first_value(row, NOTICE_ID_COLUMNS).strip()
    if notice_no:
        return f"notice:{agency_code}:{notice_no}:{order}"

    decision_no = first_value(row, DECISION_ID_COLUMNS).strip()
    demand_year = first_value(row, YEAR_COLUMNS).strip()
    if decision_no:
        return f"decision:{agency_code}:{demand_year}:{decision_no}:{order}"

    return stable_fallback_key([
        first_value(row, TITLE_COLUMNS),
        first_value(row, AGENCY_COLUMNS),
        row_date(row),
        digits(first_value(row, AMOUNT_COLUMNS)),
        order,
    ])


def fetch_chunk(start, end):
    # 이 서비스의 조회 조건은 공고일자 기준 anmtDateBegin/anmtDateEnd이다.
    return fetch_paged(
        API_URL,
        {
            "anmtDateBegin": start.strftime("%Y%m%d"),
            "anmtDateEnd": end.strftime("%Y%m%d"),
        },
    )


def main() -> None:
    today = datetime.now(SEOUL).date()
    end = today - timedelta(days=1)

    client = google_client()
    spreadsheet = client.open(SPREADSHEET_NAME)
    sheet = spreadsheet.get_worksheet(0)

    existing = read_existing(sheet)
    full = FULL_REFRESH or not existing

    start = today - timedelta(days=RETENTION_DAYS if full else LOOKBACK_DAYS)
    cutoff = (today - timedelta(days=RETENTION_DAYS)).strftime("%Y%m%d")

    print(f"모드: {'최근 1년 전체 재수집' if full else f'최근 {LOOKBACK_DAYS}일 증분 갱신'}")

    fresh = parallel_collect(
        date_chunks(start, end, 14),
        fetch_chunk,
        "입찰공고",
    )

    final_rows = merge_rows(
        [] if full else existing,
        fresh,
        row_key,
        lambda row: not row_date(row) or row_date(row) >= cutoff,
    )

    rewrite_single_sheet(sheet, final_rows, row_date)

    print(
        f"완료: API {len(fresh):,}건 / "
        f"식별번호·공고차수 기준 중복 제거 후 {len(final_rows):,}건"
    )


if __name__ == "__main__":
    main()
