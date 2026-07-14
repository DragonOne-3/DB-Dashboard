"""공공데이터 API/Google Sheets 수집기 공통 유틸리티."""
from __future__ import annotations

import hashlib
import json
import os
import random
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Callable, Iterable

import gspread
import requests
from oauth2client.service_account import ServiceAccountCredentials

SERVICE_KEY = os.environ["DATA_GO_KR_API_KEY"]
GOOGLE_AUTH_JSON = os.environ["GOOGLE_AUTH_JSON"]
PAGE_SIZE = int(os.environ.get("PROCUREMENT_PAGE_SIZE", "500"))
REQUEST_TIMEOUT = int(os.environ.get("PROCUREMENT_REQUEST_TIMEOUT", "90"))
MAX_WORKERS = max(1, min(4, int(os.environ.get("PROCUREMENT_MAX_WORKERS", "3"))))


def google_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(GOOGLE_AUTH_JSON), scope)
    return gspread.authorize(creds)


def request_xml(url: str, params: dict, max_retries: int = 5) -> ET.Element:
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            code = root.findtext(".//resultCode")
            if code and code not in {"00", "0"}:
                raise RuntimeError(f"API {code}: {root.findtext('.//resultMsg') or '오류'}")
            return root
        except Exception as exc:  # 네트워크/XML/API 오류를 동일한 백오프로 처리
            last_error = exc
            if attempt == max_retries:
                break
            wait = min(45, (2 ** (attempt - 1)) + random.uniform(0.2, 1.0))
            print(f"  재시도 {attempt}/{max_retries}: {exc} ({wait:.1f}초 후)")
            time.sleep(wait)
    raise RuntimeError(f"API 호출 최종 실패: {last_error}")


def fetch_paged(url: str, base_params: dict) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    page = 1
    while True:
        params = {**base_params, "serviceKey": SERVICE_KEY, "numOfRows": str(PAGE_SIZE), "pageNo": str(page)}
        root = request_xml(url, params)
        page_items = root.findall(".//item")
        if not page_items:
            break
        items.extend({child.tag: child.text or "" for child in item} for item in page_items)
        total = int(root.findtext(".//totalCount") or len(items))
        if len(items) >= total:
            break
        page += 1
        time.sleep(0.25)
    return items


def first_value(row: dict, candidates: Iterable[str]) -> str:
    for key in candidates:
        value = row.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def digits(value) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def stable_fallback_key(parts: Iterable[str]) -> str:
    normalized = "|".join(str(v or "").strip().lower() for v in parts)
    return "fallback:" + hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def read_existing(sheet) -> list[dict[str, str]]:
    values = sheet.get_all_values()
    if len(values) < 2:
        return []
    headers = values[0]
    return [dict(zip(headers, row)) for row in values[1:] if any(row)]


def merge_rows(existing: list[dict], fresh: list[dict], key_fn: Callable[[dict], str], keep_fn: Callable[[dict], bool]) -> list[dict]:
    merged: dict[str, dict] = {}
    for row in existing:
        if keep_fn(row):
            merged[key_fn(row)] = row
    for row in fresh:  # 최신 API 값이 기존 시트 값을 덮어씀
        if keep_fn(row):
            merged[key_fn(row)] = row
    return list(merged.values())


def ordered_headers(rows: list[dict]) -> list[str]:
    headers: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                headers.append(key)
    return headers


def rewrite_single_sheet(sheet, rows: list[dict], sort_key: Callable[[dict], str]) -> None:
    headers = ordered_headers(rows)
    sheet.clear()
    if not headers:
        sheet.update(range_name="A1", values=[["_조회결과"], ["0건"]], value_input_option="RAW")
        return
    rows.sort(key=sort_key, reverse=True)
    matrix = [headers] + [[row.get(header, "") for header in headers] for row in rows]
    if sheet.row_count < len(matrix) or sheet.col_count < len(headers):
        sheet.resize(rows=max(sheet.row_count, len(matrix)), cols=max(sheet.col_count, len(headers)))
    # gspread가 큰 요청을 처리할 수 있도록 5천 행 단위로 작성
    sheet.update(range_name="A1", values=matrix[:5000], value_input_option="RAW")
    for start in range(5000, len(matrix), 5000):
        end = min(start + 5000, len(matrix))
        sheet.update(range_name=f"A{start + 1}", values=matrix[start:end], value_input_option="RAW")


def date_chunks(start: date, end: date, days: int) -> list[tuple[date, date]]:
    chunks = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=days - 1), end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def parallel_collect(chunks, fetch_fn: Callable, label: str) -> list[dict]:
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(chunks) or 1)) as executor:
        futures = {executor.submit(fetch_fn, start, end): (start, end) for start, end in chunks}
        for future in as_completed(futures):
            start, end = futures[future]
            batch = future.result()
            print(f"=== {label} {start:%Y%m%d}~{end:%Y%m%d}: {len(batch):,}건 ===")
            results.extend(batch)
    return results
