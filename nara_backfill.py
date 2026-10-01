"""
조달청 종합쇼핑몰 납품내역 백필 수집 (연도별 CSV -> 구글드라이브)
- main.py(매일 수집 코드)와 같은 폴더에 두고 실행합니다.
- 키워드/헤더/폴더ID/드라이브 인증은 main.py에서 그대로 가져옵니다.
- 기간을 월 단위로 처리하고, 월이 끝날 때마다 {연도}.csv 에 병합 + 중복제거 후 업로드합니다.
"""
import os
import io
import time
import calendar
import datetime
import requests
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

from main import (
    keywords,
    HEADER_KOR,
    SHOPPING_FOLDER_ID,
    MY_DIRECT_KEY,
    get_drive_service_for_script,
)

KST = datetime.timezone(datetime.timedelta(hours=9))
API_URL = "https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getSpcifyPrdlstPrcureInfoList"

# main.py의 중복제거 기준과 반드시 동일하게 유지 (main.py가 매일 전체 파일에 이 기준을 다시 적용함)
DEDUPE_KEY = ["계약납품요구일자", "수요기관명", "품명", "금액"]

CHUNK_DAYS = int(os.environ.get("CHUNK_DAYS") or 10)   # API 1회 조회 기간(일)
MAX_WORKERS = int(os.environ.get("MAX_WORKERS") or 3)  # main.py와 동일하게 3


# ---------------------------------------------------------------------------
# 수집
# ---------------------------------------------------------------------------
def fetch_keyword_all_pages(kw, bgn, end, retries=3):
    """키워드 1개를 999건 단위로 끝까지 페이징하며 수집"""
    n_cols = len(HEADER_KOR)
    rows, page = [], 1
    while True:
        params = {
            "numOfRows": "999",
            "pageNo": str(page),
            "ServiceKey": MY_DIRECT_KEY,
            "type": "xml",
            "inqryDiv": "1",
            "inqryPrdctDiv": "2",
            "inqryBgnDate": bgn,
            "inqryEndDate": end,
            "dtilPrdctClsfcNoNm": kw,
        }
        root = None
        for attempt in range(retries):
            try:
                res = requests.get(API_URL, params=params, timeout=60)
                if res.status_code == 200:
                    root = ET.fromstring(res.content)
                    break
            except Exception as e:
                print(f"[{kw}] 오류({attempt + 1}/{retries}): {e}")
            time.sleep((attempt + 1) * 5)

        if root is None:
            print(f"[{kw}] {bgn}~{end} p{page} 최종 실패")
            break

        items = root.findall(".//item")
        if not items:
            break
        for it in items:
            r = [el.text if el.text else "" for el in it]
            rows.append((r + [""] * n_cols)[:n_cols])

        total = int(root.findtext(".//totalCount") or 0)
        if len(rows) >= total:
            break
        page += 1
        time.sleep(0.3)
    return rows


def collect_period(bgn, end):
    rows = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_keyword_all_pages, kw, bgn, end): kw for kw in keywords}
        for fut in as_completed(futures):
            rows.extend(fut.result())
    return rows


# ---------------------------------------------------------------------------
# 구글드라이브 연도별 CSV 병합
# ---------------------------------------------------------------------------
def find_file(drive, name):
    res = drive.files().list(
        q=f"name='{name}' and '{SHOPPING_FOLDER_ID}' in parents and trashed=false",
        fields="files(id)",
    ).execute()
    items = res.get("files", [])
    return items[0]["id"] if items else None


def download_csv(drive, file_id):
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, drive.files().get_media(fileId=file_id))
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buf.seek(0)
    # 전부 문자열로 읽어야 새로 받은 데이터(문자열)와 중복 비교가 정확함
    return pd.read_csv(buf, encoding="utf-8-sig", dtype=str, keep_default_na=False)


def upload_csv(drive, file_id, name, df):
    data = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype="text/csv", resumable=True)
    if file_id:
        drive.files().update(fileId=file_id, media_body=media).execute()
    else:
        drive.files().create(
            body={"name": name, "parents": [SHOPPING_FOLDER_ID]},
            media_body=media,
        ).execute()


def merge_into_year_file(drive, year, new_rows):
    name = f"{year}.csv"
    new_df = pd.DataFrame(new_rows, columns=HEADER_KOR).astype(str)
    file_id = find_file(drive, name)

    if file_id:
        old_df = download_csv(drive, file_id)
        merged = pd.concat([old_df, new_df], ignore_index=True)
        old_cnt = len(old_df)
    else:
        merged = new_df
        old_cnt = 0

    before = len(merged)
    merged = merged.drop_duplicates(subset=DEDUPE_KEY, keep="last")
    upload_csv(drive, file_id, name, merged)
    print(f"✅ {name}: 기존 {old_cnt:,} + 신규 {len(new_df):,} -> 중복 {before - len(merged):,}건 제거 -> 최종 {len(merged):,}건")


# ---------------------------------------------------------------------------
# 기간 계산
# ---------------------------------------------------------------------------
def get_date_range():
    yesterday = datetime.datetime.now(KST).date() - datetime.timedelta(days=1)
    s = (os.environ.get("START_DATE") or "").strip() or "20260101"
    e = (os.environ.get("END_DATE") or "").strip() or yesterday.strftime("%Y%m%d")
    return (datetime.datetime.strptime(s, "%Y%m%d").date(),
            datetime.datetime.strptime(e, "%Y%m%d").date())


def month_ranges(s, e):
    cur = s
    while cur <= e:
        last = calendar.monthrange(cur.year, cur.month)[1]
        m_end = min(e, cur.replace(day=last))
        yield cur, m_end
        cur = m_end + datetime.timedelta(days=1)


def split_days(s, e, n):
    cur = s
    while cur <= e:
        c_end = min(e, cur + datetime.timedelta(days=n - 1))
        yield cur, c_end
        cur = c_end + datetime.timedelta(days=1)


# ---------------------------------------------------------------------------
def main():
    if not MY_DIRECT_KEY:
        raise SystemExit("DATA_GO_KR_API_KEY 가 없습니다.")

    s, e = get_date_range()
    print(f"📅 수집 기간: {s} ~ {e}")
    drive, _ = get_drive_service_for_script()

    for m_start, m_end in month_ranges(s, e):
        print(f"\n===== {m_start:%Y-%m} ({m_start} ~ {m_end}) =====")
        month_rows = []
        for c_start, c_end in split_days(m_start, m_end, CHUNK_DAYS):
            bgn, end = c_start.strftime("%Y%m%d"), c_end.strftime("%Y%m%d")
            rows = collect_period(bgn, end)
            print(f"   {bgn}~{end}: {len(rows):,}건")
            month_rows.extend(rows)

        if month_rows:
            merge_into_year_file(drive, m_start.year, month_rows)
        else:
            print("   수집된 데이터 없음 - 업로드 생략")


if __name__ == "__main__":
    main()
