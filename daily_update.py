import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_URL = 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'

session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=Retry(
    total=2, backoff_factor=2, status_forcelist=[500, 502, 503, 504])))

MAX_CONN_FAIL = 3  # 연속 연결 실패 시 즉시 중단


def fetch_for_range(bgn_str, end_str, display_str):
    all_fetched_rows = []
    conn_fail = 0
    for kw in KEYWORDS:
        params = {
            'serviceKey': API_KEY, 'pageNo': '1', 'numOfRows': '999',
            'inqryDiv': '1', 'type': 'xml',
            'inqryBgnDate': bgn_str, 'inqryEndDate': end_str,
            'cntrctNm': kw
        }
        try:
            res = session.get(API_URL, params=params, timeout=(10, 30))
            conn_fail = 0
            if res.status_code != 200:
                print(f"❌ {kw} HTTP {res.status_code}")
                continue
            root = ET.fromstring(res.content)
            for item in root.findall('.//item'):
                raw = {child.tag: child.text for child in item}
                cntrct_nm = raw.get('cntrctNm', '')
                if any(ex in cntrct_nm for ex in EXCLUDE_KEYWORDS):
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
                    '★가공_계약금액': int(raw.get('totCntrctAmt') or 0)
                }
                processed.update(raw)
                all_fetched_rows.append(processed)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            conn_fail += 1
            print(f"❌ {kw} 연결 오류: {e}")
            if conn_fail >= MAX_CONN_FAIL:
                raise RuntimeError("API 연결 불가 - 수집 중단 (시트는 변경하지 않음)")
        except Exception as e:
            print(f"❌ {kw} 수집 중 오류: {e}")
        time.sleep(0.5)
    return all_fetched_rows


def append_new_rows(rows, label):
    """기존 시트는 수정/삭제하지 않고, 시트에 없는 cntrctNo만 헤더 순서에 맞춰 append"""
    if not rows:
        print(f"ℹ️ {label}: 수집 데이터 없음")
        return
    df = pd.DataFrame(rows).drop_duplicates(subset=['cntrctNo'])

    ws = get_gs_client().open("나라장터_용역계약내역").get_worksheet(0)
    values = ws.get_all_values()

    if not values:  # 완전히 빈 시트일 때만 헤더 생성
        ws.append_rows([df.columns.tolist()], value_input_option='RAW')
        header, existing_ids = df.columns.tolist(), set()
    else:
        header = values[0]
        if 'cntrctNo' not in header:
            raise RuntimeError("시트 헤더에 cntrctNo가 없음 - 중단")
        idx = header.index('cntrctNo')
        existing_ids = {r[idx] for r in values[1:] if len(r) > idx}

    new_df = df[~df['cntrctNo'].astype(str).isin(existing_ids)]
    if new_df.empty:
        print(f"ℹ️ {label}: 신규 건 없음")
        return

    out = new_df.reindex(columns=header).fillna('')
    ws.append_rows(out.values.tolist(), value_input_option='RAW')
    print(f"✅ {label}: {len(out)}건 추가 (시트 기존 {len(existing_ids)}건 유지)")


def main():
    target_dt = get_target_date()
    display_str = target_dt.strftime("%Y-%m-%d")
    t = target_dt.strftime("%Y%m%d")
    rows = fetch_for_range(t, t, display_str)
    append_new_rows(rows, display_str)


def collect_backfill(start_str, end_str):
    all_rows = []
    for i, (s, e) in enumerate(make_date_chunks(start_str, end_str), 1):
        print(f"🔄 {s} ~ {e}")
        all_rows.extend(fetch_for_range(s, e, f"{s}~{e}"))
    append_new_rows(all_rows, f"백필 {start_str}~{end_str}")
