import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import sys
import time
from pytimekr import pytimekr  # 공휴일 체크를 위해 추가

# --- 설정 ---
API_KEY = os.environ.get('DATA_GO_KR_API_KEY')
API_URL = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'

EXCLUDE_KEYWORDS = [
    '감리', '데이터베이스', '교육', '작성', '예방', '발굴', 'ISP', '구조물', '관광', '가명', '익명', '검토', '의료', '귀농', '귀촌',
    '실시', '설계', '바이오', '콘텐츠', '측정', '조사', '검증', '거래', '탄소', '농수산물', '도매', '컨설팅', '가이드라인', '굿즈', '폐기물', '인사', '육아', '수산물', '목재', '주소',
    '하드웨어', '3차원', '3D', '유산', '문화', '대행'
]

KEYWORDS = ['CCTV', '통합관제', '영상감시장치', '영상정보처리기기', '국방', '부대', '작전', '경계', '방위','데이터','플랫폼','솔루션','군사', '무인화', '사령부', '군대','스마트시티','스마트도시','ITS','GIS','중요시설','주둔지','과학화','출입','주차','육군','해군','공군','해병',
            '통합', '안전센터', '스마트관제']

def get_target_date():
    """한국 시간 기준, 주말 및 공휴일을 제외한 최근 평일 계산"""
    now = datetime.utcnow() + timedelta(hours=9)
    target = now - timedelta(days=1)
    holidays = pytimekr.holidays(year=target.year)
    while target.weekday() >= 5 or target.date() in holidays:
        target -= timedelta(days=1)
    return target

def get_gs_client():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    creds_dict = json.loads(auth_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def make_date_chunks(start_str, end_str, chunk_days=13):
    start = datetime.strptime(start_str, '%Y%m%d')
    end = datetime.strptime(end_str, '%Y%m%d')
    chunks, curr = [], start
    while curr <= end:
        chunk_e = min(curr + timedelta(days=chunk_days), end)
        chunks.append((curr.strftime('%Y%m%d'), chunk_e.strftime('%Y%m%d')))
        curr = chunk_e + timedelta(days=1)
    return chunks

def fetch_for_range(bgn_str, end_str, display_str):
    """지정 구간(bgn~end)에 대해 키워드별로 수집"""
    all_fetched_rows = []
    for kw in KEYWORDS:
        params = {
            'serviceKey': API_KEY, 'pageNo': '1', 'numOfRows': '999',
            'inqryDiv': '1', 'type': 'xml',
            'inqryBgnDate': bgn_str, 'inqryEndDate': end_str,
            'cntrctNm': kw
        }
        try:
            res = requests.get(API_URL, params=params, timeout=60)
            if res.status_code == 200:
                root = ET.fromstring(res.content)
                for item in root.findall('.//item'):
                    raw = {child.tag: child.text for child in item}
                    cntrct_nm = raw.get('cntrctNm', '')
                    if any(ex_kw in cntrct_nm for ex_kw in EXCLUDE_KEYWORDS):
                        continue

                    raw_demand = raw.get('dminsttList', '')
                    demand_parts = raw_demand.replace('[', '').replace(']', '').split('^')
                    clean_demand = demand_parts[2] if len(demand_parts) > 2 else raw_demand

                    raw_corp = raw.get('corpList', '')
                    corp_parts = raw_corp.replace('[', '').replace(']', '').split('^')
                    clean_corp = corp_parts[3] if len(corp_parts) > 3 else raw_corp

                    processed = {
                        '★가공_계약일': display_str,
                        '★가공_착수일': raw.get('stDate', '-'),
                        '★가공_만료일': raw.get('ttalScmpltDate') or raw.get('thtmScmpltDate') or '-',
                        '★가공_수요기관': clean_demand,
                        '★가공_계약명': raw.get('cntrctNm', ''),
                        '★가공_업체명': clean_corp,
                        '★가공_계약금액': int(raw.get('totCntrctAmt', 0))
                    }
                    processed.update(raw)
                    all_fetched_rows.append(processed)
        except Exception as e:
            print(f"❌ {kw} 수집 중 오류: {e}")
            continue
        time.sleep(0.5)
    return all_fetched_rows

# ================= 일일 수집 (기존 로직) =================
def main():
    target_dt = get_target_date()
    target_str = target_dt.strftime("%Y%m%d")
    display_str = target_dt.strftime("%Y-%m-%d")

    all_fetched_rows = fetch_for_range(target_str, target_str, display_str)
    save_append_only(all_fetched_rows, display_str)

def save_append_only(rows, display_str):
    """기존 daily 방식: 단순 append + 이번 실행 내 중복만 제거"""
    if not rows:
        print(f"ℹ️ {display_str}에 해당하는 수집 데이터가 없습니다.")
        return
    df = pd.DataFrame(rows)
    if 'cntrctNo' in df.columns:
        df = df.drop_duplicates(subset=['cntrctNo'])
    else:
        df = df.drop_duplicates()
    try:
        sh = get_gs_client().open("나라장터_용역계약내역")
        ws = sh.get_worksheet(0)
        ws.append_rows(df.values.tolist(), value_input_option='RAW')
        print(f"✅ {display_str} 데이터 {len(df)}건(중복제외) 추가 완료")
    except Exception as e:
        print(f"❌ 시트 저장 중 오류: {e}")

# ================= 구간 백필 (신규) =================
def collect_backfill(start_str, end_str):
    print(f"🚀 [백필 수집] {start_str} ~ {end_str} 시작")
    chunks = make_date_chunks(start_str, end_str)
    print(f"📅 {len(chunks)}개 구간")

    all_rows = []
    for i, (s, e) in enumerate(chunks, 1):
        print(f"\n🔄 [{i}/{len(chunks)}] {s} ~ {e}")
        display_str = f"{s}~{e}"
        rows = fetch_for_range(s, e, display_str)
        print(f"  수집: {len(rows)}건")
        all_rows.extend(rows)

    if not all_rows:
        print("ℹ️ 백필 구간에 해당하는 데이터가 없습니다.")
        return

    new_df = pd.DataFrame(all_rows)
    if 'cntrctNo' in new_df.columns:
        new_df = new_df.drop_duplicates(subset=['cntrctNo'])
    else:
        new_df = new_df.drop_duplicates()

    try:
        sh = get_gs_client().open("나라장터_용역계약내역")
        ws = sh.get_worksheet(0)
        existing_values = ws.get_all_values()
        if len(existing_values) < 2:
            existing_df = pd.DataFrame(columns=new_df.columns)
        else:
            headers = existing_values[0]
            existing_df = pd.DataFrame(existing_values[1:], columns=headers)
            # 컬럼 맞추기
            for col in new_df.columns:
                if col not in existing_df.columns:
                    existing_df[col] = ''
            existing_df = existing_df[new_df.columns.tolist()]

        combined = pd.concat([existing_df, new_df], ignore_index=True)
        if 'cntrctNo' in combined.columns:
            combined = combined.drop_duplicates(subset=['cntrctNo'], keep='last')

        ws.clear()
        ws.update([combined.columns.tolist()] + combined.astype(str).values.tolist())
        print(f"🏁 [백필 수집] 완료 — 시트 총 {len(combined)}행 (신규 {len(new_df)}건 병합)")
    except Exception as e:
        print(f"❌ 시트 저장 중 오류: {e}")

# ================= 진입점 =================
if __name__ == "__main__":
    """
    python main.py                          → 일일 수집 (어제/직전평일, append)
    python main.py backfill YYYYMMDD YYYYMMDD → 지정 구간 백필 (병합+중복제거)
    """
    if len(sys.argv) > 1 and sys.argv[1] == 'backfill':
        if len(sys.argv) < 4:
            print("❌ 사용법: python main.py backfill 20250101 20250630")
            sys.exit(1)
        collect_backfill(sys.argv[2], sys.argv[3])
    else:
        main()
