#!/usr/bin/env python3
"""나라장터 '음식물' 계약내역 수집기.

- 공사/물품/용역 계약을 하나의 Google Spreadsheet에 통합 저장
- 최초 1년치 백필: --backfill-year
- 매일 전일분 수집: --daily
- 명시적 기간: --start YYYYMMDD --end YYYYMMDD

필수 환경변수
- DATA_GO_KR_API_KEY
- GOOGLE_AUTH_JSON

선택 환경변수
- GOV_CONTRACT_SPREADSHEET_ID : 지정 시 ID로 열기
- GOV_CONTRACT_SPREADSHEET_NAME : 기본값 '나라장터_음식물_계약내역'
- GOV_CONTRACT_WORKSHEET_NAME : 기본값 '계약내역'
- GOV_CONTRACT_KEYWORD : 기본값 '음식물'
"""

from __future__ import annotations

import argparse
import json
import math
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound

KST = ZoneInfo("Asia/Seoul")
BASE_URL = "https://apis.data.go.kr/1230000/ao/CntrctInfoService"

API_MAP = {
    "공사": f"{BASE_URL}/getCntrctInfoListCnstwkPPSSrch",
    "물품": f"{BASE_URL}/getCntrctInfoListThngPPSSrch",
    "용역": f"{BASE_URL}/getCntrctInfoListServcPPSSrch",
}

API_KEY = os.environ.get("DATA_GO_KR_API_KEY", "").strip()
AUTH_JSON = os.environ.get("GOOGLE_AUTH_JSON", "").strip()
SPREADSHEET_ID = os.environ.get("GOV_CONTRACT_SPREADSHEET_ID", "").strip()
SPREADSHEET_NAME = os.environ.get(
    "GOV_CONTRACT_SPREADSHEET_NAME", "나라장터_음식물_계약내역"
).strip()
WORKSHEET_NAME = os.environ.get("GOV_CONTRACT_WORKSHEET_NAME", "계약내역").strip()
KEYWORD = os.environ.get("GOV_CONTRACT_KEYWORD", "음식물").strip()

ROWS_PER_PAGE = 999
CHUNK_DAYS = 15
REQUEST_TIMEOUT = 60
MAX_RETRIES = 4

# 화면에서 바로 활용하기 좋은 컬럼을 앞에 두고, 나머지 원본 필드는 뒤에 보존합니다.
FRONT_COLUMNS = [
    "업무구분",
    "검색키워드",
    "계약체결일",
    "계약번호",
    "통합계약번호",
    "계약명",
    "수요기관명",
    "계약기관명",
    "업체명",
    "총계약금액",
    "계약방법",
    "착수일",
    "완료예정일",
    "계약상세URL",
    "수집일시",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="나라장터 음식물 계약내역 수집")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--daily", action="store_true", help="한국시간 기준 전일분 수집")
    mode.add_argument("--backfill-year", action="store_true", help="오늘 기준 최근 365일 수집")
    mode.add_argument("--range", action="store_true", help="--start/--end 기간 수집")
    parser.add_argument("--start", help="시작일 YYYYMMDD")
    parser.add_argument("--end", help="종료일 YYYYMMDD")
    return parser.parse_args()


def resolve_date_range(args: argparse.Namespace) -> Tuple[datetime, datetime]:
    today = datetime.now(KST).date()
    if args.daily:
        target = today - timedelta(days=1)
        return (
            datetime.combine(target, datetime.min.time(), tzinfo=KST),
            datetime.combine(target, datetime.min.time(), tzinfo=KST),
        )
    if args.backfill_year:
        end = today - timedelta(days=1)
        start = end - timedelta(days=364)
        return (
            datetime.combine(start, datetime.min.time(), tzinfo=KST),
            datetime.combine(end, datetime.min.time(), tzinfo=KST),
        )
    if not args.start or not args.end:
        raise ValueError("--range 사용 시 --start와 --end가 필요합니다.")
    start = datetime.strptime(args.start, "%Y%m%d").replace(tzinfo=KST)
    end = datetime.strptime(args.end, "%Y%m%d").replace(tzinfo=KST)
    if start > end:
        raise ValueError("시작일은 종료일보다 늦을 수 없습니다.")
    return start, end


def date_chunks(start: datetime, end: datetime, days: int = CHUNK_DAYS) -> Iterable[Tuple[datetime, datetime]]:
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=days - 1), end)
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "Innodep-G2B-Food-Contract-Collector/1.0"})
    return session


def safe_get(item: Dict[str, Any], *keys: str, default: str = "") -> str:
    for key in keys:
        value = item.get(key)
        if value not in (None, "", "null"):
            return str(value)
    return default


def parse_embedded_name(raw: str, preferred_index: int) -> str:
    """dminsttList/corpList의 ^ 구분 문자열에서 표시명을 꺼냅니다."""
    if not raw:
        return ""
    cleaned = str(raw).replace("[", "").replace("]", "")
    first = cleaned.split(",")[0]
    parts = first.split("^")
    if len(parts) > preferred_index and parts[preferred_index].strip():
        return parts[preferred_index].strip()
    return cleaned.strip()


def to_number(value: Any) -> Any:
    if value in (None, "", "-"):
        return ""
    try:
        return int(float(str(value).replace(",", "")))
    except (ValueError, TypeError):
        return value


def normalize_item(category: str, raw: Dict[str, Any]) -> Dict[str, Any]:
    demand_raw = safe_get(raw, "dminsttList", "dmndInsttList")
    corp_raw = safe_get(raw, "corpList", "cntrctCorpList")

    contract_name = safe_get(raw, "cntrctNm", "contractNm")
    demand_name = safe_get(raw, "dminsttNm", "dmndInsttNm") or parse_embedded_name(demand_raw, 2)
    contract_org = safe_get(raw, "cntrctInsttNm", "cntrctInsttName", "orderInsttNm")
    company_name = safe_get(raw, "corpNm", "cntrctCorpNm") or parse_embedded_name(corp_raw, 3)

    processed: Dict[str, Any] = {
        "업무구분": category,
        "검색키워드": KEYWORD,
        "계약체결일": safe_get(raw, "cntrctDate", "cntrctCnclsDate", "contractDate"),
        "계약번호": safe_get(raw, "cntrctNo", "contractNo"),
        "통합계약번호": safe_get(raw, "untyCntrctNo", "unifiedCntrctNo"),
        "계약명": contract_name,
        "수요기관명": demand_name,
        "계약기관명": contract_org,
        "업체명": company_name,
        "총계약금액": to_number(safe_get(raw, "totCntrctAmt", "totalCntrctAmt")),
        "계약방법": safe_get(raw, "cntrctMthdNm", "cntrctMthd", "contractMethodNm"),
        "착수일": safe_get(raw, "stDate", "startDate"),
        "완료예정일": safe_get(raw, "ttalScmpltDate", "thtmScmpltDate", "cmpltDate"),
        "계약상세URL": safe_get(raw, "cntrctDtlInfoUrl", "contractDtlInfoUrl"),
        "수집일시": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
    }

    # 원본 필드도 모두 저장해 향후 대시보드 가공에 사용할 수 있게 합니다.
    for key, value in raw.items():
        if key not in processed:
            processed[key] = "" if value is None else value
    return processed


def extract_items(payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int]:
    body = payload.get("response", {}).get("body", {})
    items = body.get("items", [])
    if isinstance(items, dict):
        items = items.get("item", [])
    if isinstance(items, dict):
        items = [items]
    if not isinstance(items, list):
        items = []
    total = int(body.get("totalCount") or 0)
    return items, total


def request_page(
    session: requests.Session,
    category: str,
    url: str,
    start: datetime,
    end: datetime,
    page: int,
) -> Tuple[List[Dict[str, Any]], int]:
    params = {
        "serviceKey": API_KEY,
        "pageNo": str(page),
        "numOfRows": str(ROWS_PER_PAGE),
        "inqryDiv": "1",
        "type": "json",
        "inqryBgnDate": start.strftime("%Y%m%d"),
        "inqryEndDate": end.strftime("%Y%m%d"),
        "cntrctNm": KEYWORD,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            payload = response.json()
            result_code = str(payload.get("response", {}).get("header", {}).get("resultCode", "00"))
            if result_code not in {"00", "0"}:
                message = payload.get("response", {}).get("header", {}).get("resultMsg", "API 오류")
                raise RuntimeError(f"{result_code}: {message}")
            return extract_items(payload)
        except Exception as exc:
            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"[{category}] {start:%Y%m%d}~{end:%Y%m%d} {page}페이지 실패: {exc}"
                ) from exc
            wait = attempt * 3
            print(f"[{category}] {page}p 오류, {wait}초 후 재시도 ({attempt}/{MAX_RETRIES}): {exc}")
            time.sleep(wait)
    return [], 0


def collect_category_range(
    session: requests.Session,
    category: str,
    url: str,
    start: datetime,
    end: datetime,
) -> List[Dict[str, Any]]:
    first_items, total = request_page(session, category, url, start, end, 1)
    page_count = max(1, math.ceil(total / ROWS_PER_PAGE)) if total else 1
    print(f"[{category}] {start:%Y%m%d} ~ {end:%Y%m%d} | API 검색결과 {total:,}건 | {page_count}p")

    raw_items = list(first_items)
    for page in range(2, page_count + 1):
        items, _ = request_page(session, category, url, start, end, page)
        raw_items.extend(items)

    # 서버 검색조건이 무시되는 상황에 대비해 계약명에 음식물이 실제 포함됐는지 재검증합니다.
    matched = []
    for raw in raw_items:
        contract_name = safe_get(raw, "cntrctNm", "contractNm")
        if KEYWORD.lower() in contract_name.lower():
            matched.append(normalize_item(category, raw))

    print(f"[{category}] 최종 키워드 일치 {len(matched):,}건")
    return matched


def collect_all(start: datetime, end: datetime) -> pd.DataFrame:
    session = build_session()
    rows: List[Dict[str, Any]] = []
    chunks = list(date_chunks(start, end))

    for index, (chunk_start, chunk_end) in enumerate(chunks, start=1):
        print(f"\n=== 기간 {index}/{len(chunks)}: {chunk_start:%Y%m%d} ~ {chunk_end:%Y%m%d} ===")
        for category, url in API_MAP.items():
            try:
                rows.extend(collect_category_range(session, category, url, chunk_start, chunk_end))
            except Exception as exc:
                print(f"❌ {exc}")
            time.sleep(0.2)

    if not rows:
        return pd.DataFrame(columns=FRONT_COLUMNS)

    df = pd.DataFrame(rows)
    return deduplicate(df)


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # 통합계약번호가 가장 안정적이며, 없으면 계약번호+업무구분, 그것도 없으면 핵심값 조합을 사용합니다.
    def make_key(row: pd.Series) -> str:
        unified = str(row.get("통합계약번호", "") or "").strip()
        contract_no = str(row.get("계약번호", "") or "").strip()
        category = str(row.get("업무구분", "") or "").strip()
        if unified and unified.lower() != "nan":
            return f"U|{unified}"
        if contract_no and contract_no.lower() != "nan":
            return f"C|{category}|{contract_no}"
        return "F|{}|{}|{}|{}|{}".format(
            category,
            row.get("계약체결일", ""),
            row.get("계약명", ""),
            row.get("수요기관명", ""),
            row.get("업체명", ""),
        )

    result = df.copy()
    result["__dedupe_key"] = result.apply(make_key, axis=1)
    result = result.drop_duplicates(subset=["__dedupe_key"], keep="last")
    return result.drop(columns=["__dedupe_key"])


def get_gspread_client() -> gspread.Client:
    info = json.loads(AUTH_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(credentials)


def open_target_worksheet(client: gspread.Client) -> gspread.Worksheet:
    if SPREADSHEET_ID:
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
    else:
        try:
            spreadsheet = client.open(SPREADSHEET_NAME)
        except SpreadsheetNotFound:
            spreadsheet = client.create(SPREADSHEET_NAME)
            print(
                "⚠️ 스프레드시트를 서비스계정 소유로 새로 만들었습니다. "
                "사용자 Drive에서 보려면 서비스계정에서 공유 설정이 필요합니다."
            )

    try:
        return spreadsheet.worksheet(WORKSHEET_NAME)
    except WorksheetNotFound:
        first = spreadsheet.get_worksheet(0)
        if first and not first.get_all_values():
            first.update_title(WORKSHEET_NAME)
            return first
        return spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=100)


def read_existing(ws: gspread.Worksheet) -> pd.DataFrame:
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header = values[0]
    if not any(header):
        return pd.DataFrame()
    width = len(header)
    rows = [(row + [""] * width)[:width] for row in values[1:]]
    return pd.DataFrame(rows, columns=header)


def prepare_columns(existing: pd.DataFrame, new: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    existing_cols = list(existing.columns) if not existing.empty else []
    new_cols = list(new.columns) if not new.empty else []
    raw_cols = sorted(set(existing_cols + new_cols) - set(FRONT_COLUMNS))
    columns = [c for c in FRONT_COLUMNS if c in set(existing_cols + new_cols + FRONT_COLUMNS)] + raw_cols
    combined = pd.concat([existing, new], ignore_index=True, sort=False)
    for col in columns:
        if col not in combined.columns:
            combined[col] = ""
    combined = combined[columns].fillna("")
    return combined, columns


def write_sheet(ws: gspread.Worksheet, new_df: pd.DataFrame) -> None:
    existing_df = read_existing(ws)
    before_count = len(existing_df)
    combined, columns = prepare_columns(existing_df, new_df)
    combined = deduplicate(combined)

    if "계약체결일" in combined.columns:
        combined = combined.sort_values(
            by=["계약체결일", "업무구분", "계약명"],
            ascending=[False, True, True],
            kind="stable",
        )

    output = [columns] + combined.astype(object).values.tolist()
    required_rows = max(len(output), 2)
    required_cols = max(len(columns), 1)
    if ws.row_count < required_rows or ws.col_count < required_cols:
        ws.resize(rows=max(ws.row_count, required_rows), cols=max(ws.col_count, required_cols))

    ws.clear()
    ws.update(range_name="A1", values=output, value_input_option="RAW")
    ws.freeze(rows=1)
    ws.set_basic_filter(1, 1, len(output), len(columns))

    added = len(combined) - before_count
    print(
        f"✅ [{SPREADSHEET_NAME}/{WORKSHEET_NAME}] 저장 완료 | "
        f"기존 {before_count:,}건 + 신규순증 {max(added, 0):,}건 = 총 {len(combined):,}건"
    )


def validate_environment() -> None:
    missing = []
    if not API_KEY:
        missing.append("DATA_GO_KR_API_KEY")
    if not AUTH_JSON:
        missing.append("GOOGLE_AUTH_JSON")
    if missing:
        raise EnvironmentError(f"필수 환경변수가 없습니다: {', '.join(missing)}")


def main() -> None:
    validate_environment()
    args = parse_args()
    start, end = resolve_date_range(args)
    print(f"수집 범위: {start:%Y-%m-%d} ~ {end:%Y-%m-%d} / 키워드: {KEYWORD}")

    new_df = collect_all(start, end)
    print(f"이번 실행에서 수집한 고유 계약: {len(new_df):,}건")

    client = get_gspread_client()
    worksheet = open_target_worksheet(client)
    write_sheet(worksheet, new_df)


if __name__ == "__main__":
    main()
