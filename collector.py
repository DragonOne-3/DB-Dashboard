import os
import sys
import json
import datetime
import time
import requests
import pandas as pd
import gc
import threading

from googleapiclient.discovery import build
from google.oauth2 import service_account

# ================= 설정 =================
SERVICE_KEY = os.environ.get('DATA_GO_KR_API_KEY')
AUTH_JSON_STR = os.environ.get('GOOGLE_AUTH_JSON')

SPREADSHEET_ID = '1QCkfXZzKohTZot-Ap1kE_dqfpbnIhbkpdFiEvaBrcio'
SHEET_NAME = '나라장터_유지보수_공고'

API_URL = 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoServcPPSSrch'

KEYWORDS = ['통합관제', 'CCTV', '통합플랫폼', '스마트도시', '스마트시티', '주차', '출입']
REQUIRED_WORD = '유지'

COLUMNS = [
    'bidNtceNo',            # 입찰공고번호
    'rgstTyNm',             # 등록유형명
    'ntceKindNm',           # 공고종류명
    'bidNtceDt',            # 입찰공고일시
    'refNo',                # 참조번호
    'bidNtceNm',            # 입찰공고명
    'ntceInsttCd',          # 공고기관코드
    'ntceInsttNm',          # 공고기관명
    'dminsttCd',            # 수요기관코드
    'dminsttNm',            # 수요기관명
    'bidMethdNm',           # 입찰방식명
    'cntrctCnclsMthdNm',    # 계약체결방법명
    'ntceInsttOfclNm',      # 공고기관담당자명
    'ntceInsttOfclTelNo',   # 공고기관담당자전화번호
    'bidNtceDtlUrl',        # 입찰공고상세URL
    'bidBeginDt',           # 입찰개시일시
    'bidClseDt',            # 입찰마감일시
    'asignBdgtAmt',         # 배정예산금액
    'presmptPrce',          # 추정가격
    'sucsfbidMthdNm',       # 낙찰방법명
]

save_lock = threading.Lock()

def log(msg):
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

# ================= Google Sheets =================
def get_sheets_service():
    info = json.loads(AUTH_JSON_STR)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=[
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
    )
    return build('sheets', 'v4', credentials=creds, cache_discovery=False)

def ensure_sheet_exists(svc):
    meta = svc.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    names = [s['properties']['title'] for s in meta.get('sheets', [])]
    if SHEET_NAME not in names:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={'requests': [{'addSheet': {'properties': {'title': SHEET_NAME}}}]}
        ).execute()
        log(f"✅ 새 시트 생성: {SHEET_NAME}")

def read_sheet(svc):
    try:
        result = svc.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{SHEET_NAME}!A:Z'
        ).execute()
        values = result.get('values', [])
        if not values or len(values) < 2:
            return pd.DataFrame(columns=COLUMNS + ['수집일자'])
        headers = values[0]
        rows = [r + [''] * (len(headers) - len(r)) for r in values[1:]]
        return pd.DataFrame(rows, columns=headers)
    except Exception as e:
        log(f"⚠️ 시트 읽기 오류: {e}")
        return pd.DataFrame(columns=COLUMNS + ['수집일자'])

def write_sheet(svc, df):
    values = [df.columns.tolist()] + df.fillna('').values.tolist()
    values = [[str(c) for c in row] for row in values]
    svc.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:Z'
    ).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f'{SHEET_NAME}!A1',
        valueInputOption='RAW',
        body={'values': values}
    ).execute()
    log(f"✅ 시트 저장 완료: 총 {len(df)}행")

# ================= API 수집 =================
def fetch_data_chunk(s_dt, e_dt):
    all_data = []
    page = 1
    with requests.Session() as session:
        while True:
            params = {
                'serviceKey': SERVICE_KEY,
                'pageNo': str(page),
                'numOfRows': '999',
                'inqryDiv': '1',
                'type': 'json',
                'inqryBgnDt': s_dt + '0000',
                'inqryEndDt': e_dt + '2359',
            }
            log(f"  API 요청 - {s_dt}~{e_dt} | {page}p")
            try:
                res = session.get(API_URL, params=params, timeout=45)
                if res.status_code != 200:
                    log(f"  ⚠️ HTTP {res.status_code}")
                    break
                res_json = res.json()
                items = res_json.get('response', {}).get('body', {}).get('items', [])
                if not items:
                    break
                all_data.extend(items)
                total = int(res_json.get('response', {}).get('body', {}).get('totalCount', 0))
                log(f"  수집: {len(all_data)} / {total}")
                if len(all_data) >= total or len(items) < 999:
                    break
                page += 1
                time.sleep(0.3)
            except Exception as e:
                log(f"  ❌ 오류: {e}")
                break
    return pd.DataFrame(all_data)

def make_date_chunks(start_str, end_str):
    start = datetime.datetime.strptime(start_str, '%Y%m%d')
    end = datetime.datetime.strptime(end_str, '%Y%m%d')
    chunks, curr = [], start
    while curr <= end:
        chunk_e = min(curr + datetime.timedelta(days=13), end)
        chunks.append((curr.strftime('%Y%m%d'), chunk_e.strftime('%Y%m%d')))
        curr = chunk_e + datetime.timedelta(days=1)
    return chunks

# ================= 필터 =================
def filter_by_keywords(df):
    if df.empty or 'bidNtceNm' not in df.columns:
        return pd.DataFrame()
    kw = df['bidNtceNm'].str.contains('|'.join(KEYWORDS), na=False)
    req = df['bidNtceNm'].str.contains(REQUIRED_WORD, na=False)
    filtered = df[kw & req].copy()
    log(f"  필터: {len(df)}건 → {len(filtered)}건")
    return filtered

# ================= 1년 지난 데이터 삭제 =================
def remove_old_data(df):
    if 'bidNtceDt' not in df.columns or df.empty:
        return df
    cutoff = datetime.datetime.now() - datetime.timedelta(days=365)

    def parse_dt(val):
        try:
            val = str(val).strip()
            if '-' in val:
                return datetime.datetime.strptime(val[:10], '%Y-%m-%d')
            elif len(val) >= 8:
                return datetime.datetime.strptime(val[:8], '%Y%m%d')
        except:
            pass
        return None

    df = df.copy()
    df['_dt'] = df['bidNtceDt'].apply(parse_dt)
    before = len(df)
    df = df[df['_dt'].isna() | (df['_dt'] >= cutoff)].copy()
    df.drop(columns=['_dt'], inplace=True)
    if before - len(df) > 0:
        log(f"  🗑️ 1년 이상 지난 {before - len(df)}건 삭제")
    return df

# ================= 병합 저장 =================
def merge_and_save(svc, new_df):
    if new_df.empty:
        log("  ℹ️ 저장할 데이터 없음")
        return

    # 지정 컬럼만 추출 (없는 컬럼은 빈값)
    for col in COLUMNS:
        if col not in new_df.columns:
            new_df[col] = ''
    new_df = new_df[COLUMNS].copy()
    new_df['수집일자'] = datetime.datetime.now().strftime('%Y-%m-%d')

    with save_lock:
        existing = read_sheet(svc)
        if '수집일자' not in existing.columns:
            existing['수집일자'] = ''
        combined = pd.concat([existing, new_df], ignore_index=True)
        if 'bidNtceNo' in combined.columns:
            combined.drop_duplicates(subset=['bidNtceNo'], keep='last', inplace=True)
        combined = remove_old_data(combined)
        write_sheet(svc, combined)
        gc.collect()

# ================= 일일 수집 =================
def collect_daily():
    log("=" * 50)
    log("🚀 [일일 수집] 시작")
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    today = datetime.datetime.now().strftime('%Y%m%d')
    log(f"📅 {yesterday} ~ {today}")

    svc = get_sheets_service()
    ensure_sheet_exists(svc)
    raw = fetch_data_chunk(yesterday, today)
    merge_and_save(svc, filter_by_keywords(raw))
    log("🏁 [일일 수집] 완료")

# ================= 초기 수집 (최근 6개월) =================
def collect_initial():
    log("=" * 50)
    log("🚀 [초기 수집] 최근 6개월 시작")
    end = datetime.datetime.now()
    start = end - datetime.timedelta(days=180)
    chunks = make_date_chunks(start.strftime('%Y%m%d'), end.strftime('%Y%m%d'))
    log(f"📅 {start.strftime('%Y%m%d')} ~ {end.strftime('%Y%m%d')} | {len(chunks)}개 구간")

    svc = get_sheets_service()
    ensure_sheet_exists(svc)

    for i, (s, e) in enumerate(chunks, 1):
        log(f"\n🔄 [{i}/{len(chunks)}] {s} ~ {e}")
        raw = fetch_data_chunk(s, e)
        merge_and_save(svc, filter_by_keywords(raw))
        time.sleep(1)

    log("🏁 [초기 수집] 완료")

# ================= 진입점 =================
if __name__ == "__main__":
    """
    python collector.py init    → 최근 6개월치 초기 수집 (최초 1회)
    python collector.py daily   → 어제~오늘치 수집 (Actions 일일 자동 실행)
    """
    mode = sys.argv[1] if len(sys.argv) > 1 else 'daily'
    if mode == 'init':
        collect_initial()
    else:
        collect_daily()
