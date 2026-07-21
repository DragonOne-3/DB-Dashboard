import os
import sys
import json
import datetime
import time
import gc
import threading
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build


def log(message: str) -> None:
    print(message, flush=True)


SAVE_LOCK = threading.Lock()

SERVICE_KEY = os.environ.get("DATA_GO_KR_API_KEY")
AUTH_JSON_STR = os.environ.get("GOOGLE_AUTH_JSON")
KEYWORD = os.environ.get("GOV_BID_KEYWORD", "음식물").strip()

SPREADSHEET_IDS = {
    "공사": os.environ.get("GOV_BID_SHEET_ID_CONSTRUCTION", "1gaErGoHJzMrk_tD6PEYAqGg8OC3IxxDHO4KqysJeoh4").strip(),
    "물품": os.environ.get("GOV_BID_SHEET_ID_GOODS", "1LZOCzbL4juIxmhY015hgokOsUbyewiLcLmHkzLiIqJk").strip(),
    "용역": os.environ.get("GOV_BID_SHEET_ID_SERVICE", "13RdFWDXDt0S7V2mYrkKYzVFdX01MZd8AIxyvLyU9qmE").strip(),
}

SPREADSHEET_NAMES = {
    "공사": "나라장터_음식물_공사_공고",
    "물품": "나라장터_음식물_물품_공고",
    "용역": "나라장터_음식물_용역_공고",
}

WORKSHEET_NAME = os.environ.get("GOV_BID_WORKSHEET_NAME", "공고").strip()

API_MAP = {
    "공사": "https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwkPPSSrch",
    "물품": "https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThngPPSSrch",
    "용역": "https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoServcPPSSrch",
}


# 카테고리별 저장 대상 필드입니다.
# 첫 행은 API 원본 필드명을 유지하여 향후 대시보드 JSON 생성 시 안정적으로 사용합니다.
CATEGORY_FIELDS = {
    "공사": [
        "bidNtceNo", "bidNtceOrd", "reNtceYn", "rgstTyNm", "ntceKindNm",
        "bidNtceDt", "refNo", "bidNtceNm", "ntceInsttNm", "dminsttNm",
        "bidMethdNm", "cntrctCnclsMthdNm", "ntceInsttOfclNm",
        "ntceInsttOfclTelNo", "bidQlfctRgstDt", "bidBeginDt", "bidClseDt",
        "opengDt", "bdgtAmt", "presmptPrce", "govsplyAmt", "bidNtceDtlUrl",
    ],
    "물품": [
        "bidNtceNo", "bidNtceOrd", "reNtceYn", "rgstTyNm", "ntceKindNm",
        "bidNtceDt", "refNo", "bidNtceNm", "ntceInsttNm", "dminsttNm",
        "bidMethdNm", "cntrctCnclsMthdNm", "ntceInsttOfclNm",
        "ntceInsttOfclTelNo", "bidQlfctRgstDt", "bidBeginDt", "bidClseDt",
        "opengDt", "asignBdgtAmt", "presmptPrce", "bidNtceDtlUrl",
        "dtilPrdctClsfcNo", "dtilPrdctClsfcNoNm", "prdctQty",
    ],
    "용역": [
        "bidNtceNo", "bidNtceOrd", "reNtceYn", "rgstTyNm", "ntceKindNm",
        "bidNtceDt", "refNo", "bidNtceNm", "ntceInsttNm", "dminsttNm",
        "bidMethdNm", "cntrctCnclsMthdNm", "ntceInsttOfclNm",
        "ntceInsttOfclTelNo", "bidQlfctRgstDt", "bidBeginDt", "bidClseDt",
        "opengDt", "asignBdgtAmt", "presmptPrce", "bidNtceDtlUrl",
        "untyNtceNo", "sucsfbidLwltRate",
    ],
}

FIELD_LABELS = {
    "bidNtceNo": "입찰공고번호",
    "bidNtceOrd": "입찰공고차수",
    "reNtceYn": "재공고여부",
    "rgstTyNm": "등록유형명",
    "ntceKindNm": "공고종류명",
    "bidNtceDt": "입찰공고일시",
    "refNo": "참조번호",
    "bidNtceNm": "입찰공고명",
    "ntceInsttNm": "공고기관명",
    "dminsttNm": "수요기관명",
    "bidMethdNm": "입찰방식명",
    "cntrctCnclsMthdNm": "계약체결방법명",
    "ntceInsttOfclNm": "공고기관담당자명",
    "ntceInsttOfclTelNo": "공고기관담당자전화번호",
    "bidQlfctRgstDt": "입찰참가자격등록마감일시",
    "bidBeginDt": "입찰개시일시",
    "bidClseDt": "입찰마감일시",
    "opengDt": "개찰일시",
    "bdgtAmt": "예산금액",
    "asignBdgtAmt": "배정예산금액",
    "presmptPrce": "추정가격",
    "govsplyAmt": "관급금액",
    "bidNtceDtlUrl": "입찰공고상세URL",
    "dtilPrdctClsfcNo": "세부품명번호",
    "dtilPrdctClsfcNoNm": "세부품명",
    "prdctQty": "물품수량",
    "untyNtceNo": "통합공고번호",
    "sucsfbidLwltRate": "낙찰하한율",
}

TITLE_COLUMNS = [
    "bidNtceNm",
    "bidNtceName",
    "ntceNm",
    "bidPblancNm",
    "공고명",
    "입찰공고명",
]


def get_google_clients():
    if not AUTH_JSON_STR:
        raise RuntimeError("GOOGLE_AUTH_JSON 환경변수가 없습니다.")

    info = json.loads(AUTH_JSON_STR)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    gspread_client = gspread.authorize(creds)
    drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return gspread_client, drive_service


def open_target_spreadsheet(gspread_client, category: str):
    spreadsheet_id = SPREADSHEET_IDS.get(category, "").strip()
    if not spreadsheet_id:
        raise RuntimeError(f"[{category}] 대상 스프레드시트 ID가 없습니다.")

    log(
        f"📗 [{category}] 지정 스프레드시트 연결: "
        f"{SPREADSHEET_NAMES[category]} ({spreadsheet_id})"
    )
    return gspread_client.open_by_key(spreadsheet_id)


def fetch_data_chunk(category: str, url: str, start_date: str, end_date: str) -> pd.DataFrame:
    all_items = []
    page_no = 1

    with requests.Session() as session:
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        session.mount("https://", adapter)

        while True:
            params = {
                "serviceKey": SERVICE_KEY,
                "pageNo": str(page_no),
                "numOfRows": "999",
                "inqryDiv": "1",
                "type": "json",
                "inqryBgnDt": start_date + "0000",
                "inqryEndDt": end_date + "2359",
                # 나라장터 검색조건 API에 공고명 키워드를 직접 전달합니다.
                # 서버에서 먼저 '음식물' 공고만 추려 오므로 전체 공고를 전부 순회하지 않습니다.
                "bidNtceNm": KEYWORD,
            }

            log(f"   - [{category}] {start_date} ~ {end_date} | 키워드 '{KEYWORD}' | {page_no}p 요청")
            try:
                response = session.get(url, params=params, timeout=60)
                response.raise_for_status()
                payload = response.json()

                header = payload.get("response", {}).get("header", {})
                result_code = str(header.get("resultCode", ""))
                if result_code and result_code not in {"00", "0"}:
                    log(f"⚠️ [{category}] API 오류: {header}")
                    break

                body = payload.get("response", {}).get("body", {})
                items = body.get("items", [])
                if isinstance(items, dict):
                    items = items.get("item", items)
                if isinstance(items, dict):
                    items = [items]
                if not isinstance(items, list) or not items:
                    break

                all_items.extend(items)
                total_count = int(body.get("totalCount", 0) or 0)
                log(f"   - [{category}] 키워드 검색 진행: {len(all_items):,} / {total_count:,}")

                if len(all_items) >= total_count or len(items) < 999:
                    break

                page_no += 1
                time.sleep(0.25)

            except requests.RequestException as exc:
                log(f"⚠️ [{category}] HTTP 요청 실패: {exc}")
                break
            except (ValueError, TypeError, KeyError) as exc:
                log(f"⚠️ [{category}] 응답 처리 실패: {exc}")
                break

    return pd.DataFrame(all_items)


def filter_keyword_rows(df: pd.DataFrame, keyword: str) -> pd.DataFrame:
    if df.empty:
        return df

    title_columns = [column for column in TITLE_COLUMNS if column in df.columns]
    if not title_columns:
        log(f"⚠️ 공고명 컬럼 없음: {list(df.columns[:20])}")
        return pd.DataFrame(columns=df.columns)

    mask = pd.Series(False, index=df.index)
    matched_column = pd.Series("", index=df.index, dtype="object")

    for column in title_columns:
        current = df[column].astype(str).str.contains(
            keyword, case=False, na=False, regex=False
        )
        mask |= current
        matched_column.loc[current & matched_column.eq("")] = column

    filtered = df.loc[mask].copy()
    return filtered


def select_required_fields(df: pd.DataFrame, category: str, keyword: str) -> pd.DataFrame:
    """카테고리별 필수 필드만 남기고 관리용 컬럼을 앞에 추가합니다."""
    if df.empty:
        return df

    required = CATEGORY_FIELDS[category]
    selected = pd.DataFrame(index=df.index)

    # API 응답에 특정 필드가 없더라도 열 구조는 항상 동일하게 유지합니다.
    for column in required:
        selected[column] = df[column] if column in df.columns else ""

    selected.insert(0, "수집일시", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    selected.insert(0, "조달구분", category)
    selected.insert(0, "검색키워드", keyword)
    return selected.fillna("")


def deduplicate_notices(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    candidate_keys = [
        ["bidNtceNo", "bidNtceOrd"],
        ["bidNtceNo", "bidNtceOrd", "reNtceYn"],
        ["bidNtceNo"],
    ]

    for keys in candidate_keys:
        if all(key in df.columns for key in keys):
            valid = [key for key in keys if not df[key].replace("", pd.NA).isna().all()]
            if valid:
                return df.drop_duplicates(subset=valid, keep="last")

    return df.drop_duplicates(keep="last")


def get_or_create_worksheet(spreadsheet, title: str, cols: int):
    try:
        worksheet = spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=title, rows=1000, cols=max(cols, 20))
    return worksheet


def read_existing_sheet(worksheet) -> pd.DataFrame:
    values = worksheet.get_all_values()
    if len(values) < 2:
        return pd.DataFrame()

    headers = values[0]
    rows = values[1:]
    width = len(headers)
    normalized = [row[:width] + [""] * max(0, width - len(row)) for row in rows]
    return pd.DataFrame(normalized, columns=headers).fillna("")


def write_dataframe(worksheet, df: pd.DataFrame) -> None:
    df = df.fillna("").astype(str)
    values = [df.columns.tolist()] + df.values.tolist()

    worksheet.clear()
    worksheet.resize(rows=max(len(values), 2), cols=max(len(df.columns), 1))

    batch_size = 2000
    for start in range(0, len(values), batch_size):
        batch = values[start:start + batch_size]
        start_row = start + 1
        end_row = start_row + len(batch) - 1
        end_col = gspread.utils.rowcol_to_a1(1, len(df.columns)).replace("1", "")
        worksheet.update(
            range_name=f"A{start_row}:{end_col}{end_row}",
            values=batch,
            value_input_option="RAW",
        )
        time.sleep(0.4)

    worksheet.freeze(rows=1)


def save_to_spreadsheet(spreadsheet, category: str, new_df: pd.DataFrame) -> None:
    if new_df.empty:
        log(f"ℹ️ [{category}] '{KEYWORD}' 일치 공고 없음")
        return

    with SAVE_LOCK:
        try:
            worksheet = get_or_create_worksheet(
                spreadsheet, WORKSHEET_NAME, max(len(new_df.columns), 20)
            )
            old_df = read_existing_sheet(worksheet)

            if old_df.empty:
                merged = new_df.astype(str).copy()
            else:
                merged = pd.concat(
                    [old_df, new_df.astype(str)],
                    ignore_index=True,
                    sort=False,
                ).fillna("")

            before_count = len(merged)
            merged = deduplicate_notices(merged)
            after_count = len(merged)

            for date_column in ("bidNtceDt", "bidNtceDate", "rgstDt"):
                if date_column in merged.columns:
                    merged.sort_values(
                        date_column,
                        ascending=False,
                        inplace=True,
                        kind="stable",
                    )
                    break

            # Google Sheets는 셀 수 제한이 있으므로 너무 많은 빈 컬럼을 만들지 않습니다.
            nonempty_columns = [
                c for c in merged.columns
                if merged[c].replace("", pd.NA).notna().any()
            ]
            merged = merged[nonempty_columns]

            write_dataframe(worksheet, merged)
            log(
                f"✅ [{category}] 스프레드시트 저장 완료: "
                f"병합 {before_count:,}건 → 중복제거 {after_count:,}건"
            )

            del old_df, merged
            gc.collect()

        except Exception as exc:
            log(f"❌ [{category}] 스프레드시트 저장 오류: {exc}")


def process_category(category: str, url: str, date_chunks) -> None:
    gspread_client, _ = get_google_clients()
    spreadsheet = open_target_spreadsheet(gspread_client, category)

    # 새 파일의 기본 시트명을 통일합니다.
    try:
        sheet1 = spreadsheet.worksheet("Sheet1")
        if len(spreadsheet.worksheets()) == 1:
            sheet1.update_title(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        pass

    offset = {"공사": 0, "물품": 2, "용역": 4}[category]
    time.sleep(offset)

    collected_frames = []

    for start_date, end_date in date_chunks:
        log(f"\n🔄 [{category}] 구간 시작: {start_date} ~ {end_date}")
        full_df = fetch_data_chunk(category, url, start_date, end_date)

        if full_df.empty:
            log(f"ℹ️ [{category}] API 반환 데이터 없음")
            continue

        # API 서버 검색 결과를 다시 한 번 로컬에서 검증해 오탐을 제거합니다.
        keyword_df = filter_keyword_rows(full_df, KEYWORD)
        log(
            f"🔎 [{category}] API 키워드 검색 {len(full_df):,}건 중 "
            f"최종 일치 {len(keyword_df):,}건"
        )

        if not keyword_df.empty:
            selected_df = select_required_fields(keyword_df, category, KEYWORD)
            collected_frames.append(selected_df)

        del full_df, keyword_df
        gc.collect()
        time.sleep(0.5)

    # 장기간 수집 시 매 구간마다 시트 전체를 다시 쓰지 않고 카테고리별 1회만 저장합니다.
    if collected_frames:
        combined_df = pd.concat(collected_frames, ignore_index=True, sort=False).fillna("")
        combined_df = deduplicate_notices(combined_df)
        log(f"📦 [{category}] 수집 구간 통합: {len(combined_df):,}건")
        save_to_spreadsheet(spreadsheet, category, combined_df)
        del combined_df
    else:
        log(f"ℹ️ [{category}] 전체 기간에 '{KEYWORD}' 일치 공고 없음")

    del collected_frames
    gc.collect()


def build_date_chunks(start_date: datetime.datetime, end_date: datetime.datetime):
    chunks = []
    current = start_date
    while current <= end_date:
        chunk_end = min(current + datetime.timedelta(days=14), end_date)
        chunks.append((current.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")))
        current = chunk_end + datetime.timedelta(days=1)
    return chunks


def resolve_collection_period():
    """
    실행 모드:
      - 인자 없음: 한국시간 기준 전날 1일 수집
      - --daily: 한국시간 기준 전날 1일 수집
      - --backfill-year: 한국시간 기준 어제까지 최근 365일 수집
      - YYYYMMDD YYYYMMDD: 지정 기간 수집
    """
    now_kst = datetime.datetime.now(ZoneInfo("Asia/Seoul"))
    yesterday = (now_kst - datetime.timedelta(days=1)).date()

    if len(sys.argv) == 1 or sys.argv[1] == "--daily":
        start_day = end_day = yesterday
        mode = "daily"
    elif sys.argv[1] == "--backfill-year":
        start_day = yesterday - datetime.timedelta(days=364)
        end_day = yesterday
        mode = "backfill-year"
    elif len(sys.argv) >= 3:
        start_day = datetime.datetime.strptime(sys.argv[1], "%Y%m%d").date()
        end_day = datetime.datetime.strptime(sys.argv[2], "%Y%m%d").date()
        mode = "custom"
    else:
        raise SystemExit(
            "사용법: python gov_food_bid_collector_scheduled.py "
            "[--daily | --backfill-year | YYYYMMDD YYYYMMDD]"
        )

    if start_day > end_day:
        raise SystemExit("시작일이 종료일보다 늦습니다.")

    return (
        datetime.datetime.combine(start_day, datetime.time.min),
        datetime.datetime.combine(end_day, datetime.time.min),
        mode,
    )


def main() -> None:
    if not SERVICE_KEY:
        raise SystemExit("DATA_GO_KR_API_KEY 환경변수가 없습니다.")
    if not AUTH_JSON_STR:
        raise SystemExit("GOOGLE_AUTH_JSON 환경변수가 없습니다.")
    if not KEYWORD:
        raise SystemExit("GOV_BID_KEYWORD가 비어 있습니다.")

    start_date, end_date, mode = resolve_collection_period()
    start_text = start_date.strftime("%Y%m%d")
    end_text = end_date.strftime("%Y%m%d")

    date_chunks = build_date_chunks(start_date, end_date)
    log(f"⚙️ 실행 모드: {mode}")
    log(f"📊 나라장터 키워드 공고 수집 시작: {start_text} ~ {end_text}")
    log(f"🔑 검색 키워드: {KEYWORD}")
    for category, spreadsheet_name in SPREADSHEET_NAMES.items():
        log(f"📗 [{category}] 저장 파일: {spreadsheet_name}")

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(
                process_category,
                category,
                url,
                date_chunks,
            )
            for category, url in API_MAP.items()
        ]
        for future in as_completed(futures):
            future.result()

    log("\n🏁 모든 키워드 공고 수집 작업 종료")


if __name__ == "__main__":
    main()
