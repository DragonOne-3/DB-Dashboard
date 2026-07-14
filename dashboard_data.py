import os
import json
import re
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
from oauth2client.service_account import ServiceAccountCredentials

GOOGLE_AUTH_JSON = os.environ['GOOGLE_AUTH_JSON']
KEYWORD = os.environ.get('DASHBOARD_KEYWORD', '음식물')
OUTPUT_DIR = os.environ.get('DASHBOARD_OUTPUT_DIR', 'docs/data')
LEGACY_OUTPUT_PATH = os.environ.get('DASHBOARD_OUTPUT_PATH', 'docs/data.json')
SEOUL = ZoneInfo('Asia/Seoul')

SOURCE_SPREADSHEETS = {
    '발주계획': '군수품조달_국내_발주계획',
    '계약정보': '군수품조달_국내_계약정보',
    '입찰공고': '군수품조달_국내_입찰공고',
}
EXCLUDE_TABS = {'백필_진행상황'}

TITLE_COLUMNS = [
    '대표품목명', '공고명', '입찰공고명', '사업명', '품명', '계약명',
    '수요품명', '구매품명', '용역명', '건명'
]
STATUS_COLUMNS = ['진행상태', '계약상태', '공고구분']
AGENCY_COLUMNS = ['발주기관', '수요기관', '수요부대', '기관명']
COMPANY_COLUMNS = ['업체명', '계약업체명', '낙찰업체명', '상호']
BID_RATE_COLUMNS = ['낙찰률', '낙찰율']

RENTAL_KEYWORDS = ['임차', '렌탈', '리스', '대여']
PURCHASE_KEYWORDS = ['구매', '구입', '매입', '납품', '설치', '도입']
MAINTENANCE_KEYWORDS = ['정비', '유지보수', '수리', '보수', '위탁관리', '관리용역']
SERVICE_KEYWORDS = ['폐기물 처리', '수거', '운반', '위탁처리', '처리용역', '잔반처리']
ACCESSORY_KEYWORDS = ['처리대', '처리통', '쓰레기통', '잔반통', '종량제 봉투', '봉투', '보관판넬', '부수자재']
CORE_EQUIPMENT_KEYWORDS = [
    '처리기', '분쇄기', '건조기', '감량기', '감량기기', '처리기기',
    '자원화기기', '절단기', '탈수기'
]

CLOSED_STATUS_VALUES = {'계약완료', '취소공고', '유찰', '종료', '계약종료'}
KNOWN_STATUS_VALUES = {
    '계약완료', '공고중', '조달판단중', '수의협상중', '조달판단완료',
    '공고의뢰중', '계약준비중(낙찰단가처리중)', '정상공고', '긴급공고',
    '취소공고', '정정공고', '재공고', '유찰', '낙찰', '개찰대기',
    '접수중', '개찰완료', '계약체결'
}

AMOUNT_PRIORITY_BY_SOURCE = {
    '발주계획': ['예산금액', '추정금액', '사업금액', '예정금액'],
    '입찰공고': ['추정가격', '배정예산', '기초예가', '기초예비가격', '예정가격', '예산금액'],
    '계약정보': ['계약금액', '총계약금액', '낙찰금액', '계약단가', '예산금액'],
}



IDENTIFIER_COLUMNS = [
    '입찰공고번호', '공고번호', '계약번호', '판단번호', '사업번호',
    'bidNtceNo', 'bidPblancNo', 'cntrctNo', 'dcsNo', 'noticeNo'
]


def dedupe_key(row, source):
    identifier = first_value(row, IDENTIFIER_COLUMNS)
    if identifier:
        return (source, 'id', identifier)
    return (
        source,
        'fallback',
        extract_title(row),
        first_value(row, AGENCY_COLUMNS) or '',
        extract_date_sort_key(row),
        extract_amount(row, source),
    )

def first_value(row, candidates):
    for key in candidates:
        value = row.get(key)
        if value not in (None, ''):
            return str(value).strip()
    return None


def digits_only(value):
    return ''.join(ch for ch in str(value) if ch.isdigit())


def extract_title(row, keyword=KEYWORD):
    title = first_value(row, TITLE_COLUMNS)
    if title:
        return title
    for value in row.values():
        if value and keyword in str(value):
            return str(value).strip()
    for value in row.values():
        if value and str(value).strip():
            return str(value).strip()
    return '(제목 없음)'


def row_matches_keyword(value_row, keyword):
    return any(value and keyword in str(value) for value in value_row)


def classify_detail(title):
    if any(k in title for k in SERVICE_KEYWORDS):
        return '폐기물처리용역'
    if any(k in title for k in MAINTENANCE_KEYWORDS):
        return '유지보수'
    if any(k in title for k in ACCESSORY_KEYWORDS):
        return '부속·비품'
    if any(k in title for k in CORE_EQUIPMENT_KEYWORDS):
        if any(k in title for k in RENTAL_KEYWORDS):
            return '장비임차'
        if any(k in title for k in PURCHASE_KEYWORDS):
            return '장비구매'
        return '핵심장비'
    if any(k in title for k in RENTAL_KEYWORDS):
        return '장비임차'
    if any(k in title for k in PURCHASE_KEYWORDS):
        return '기타구매'
    return '기타'


def is_equipment_category(detail_category):
    return detail_category in {'핵심장비', '장비구매', '장비임차'}


def subtype_from_detail(detail_category):
    if detail_category == '장비임차':
        return '임차(렌탈)'
    if detail_category in {'핵심장비', '장비구매'}:
        return '구매'
    return '기타'


def extract_year(row):
    for key, value in row.items():
        if value and '연도' in key:
            digits = digits_only(value)
            if len(digits) >= 4:
                return digits[:4]
    for key, value in row.items():
        if value and any(tag in key for tag in ('월', '일자', '날짜', '일시')):
            digits = digits_only(value)
            if len(digits) >= 4:
                return digits[:4]
    return None


def extract_date_sort_key(row):
    preferred = ['공고일자', '계약일자', '계약일', '발주예정월', '등록일자', '작성일자']
    for key in preferred:
        value = row.get(key)
        digits = digits_only(value) if value else ''
        if len(digits) >= 8:
            return digits[:8]
        if len(digits) >= 6:
            return digits[:6]
    for key, value in row.items():
        if value and any(tag in key for tag in ('일자', '날짜', '일시')):
            digits = digits_only(value)
            if len(digits) >= 8:
                return digits[:8]
    for key, value in row.items():
        if value and '월' in key:
            digits = digits_only(value)
            if len(digits) >= 6:
                return digits[:6]
    return extract_year(row) or ''


def extract_amount(row, source):
    for key in AMOUNT_PRIORITY_BY_SOURCE.get(source, []):
        value = row.get(key)
        if value:
            digits = digits_only(value)
            if digits:
                return int(digits)
    for key, value in row.items():
        if value and ('금액' in key or '가격' in key or '예산' in key):
            digits = digits_only(value)
            if digits:
                return int(digits)
    return 0


def extract_status(row):
    for col in STATUS_COLUMNS:
        value = row.get(col)
        if value:
            status = str(value).strip()
            return status if status in KNOWN_STATUS_VALUES else '데이터확인필요'
    return '미상'


def is_open_opportunity(source, status):
    if source == '계약정보':
        return False
    if status in {'미상', '데이터확인필요'}:
        return False
    return status not in CLOSED_STATUS_VALUES


def extract_bid_rate(row):
    value = first_value(row, BID_RATE_COLUMNS)
    if not value:
        return None
    cleaned = re.sub(r'[^0-9.]', '', value)
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def opportunity_score(row):
    if not row.get('_is_open'):
        return 0
    amount = row.get('_amount', 0)
    score = 0
    if amount >= 1_000_000_000:
        score += 40
    elif amount >= 300_000_000:
        score += 34
    elif amount >= 100_000_000:
        score += 27
    elif amount >= 30_000_000:
        score += 19
    elif amount > 0:
        score += 10

    status_score = {
        '긴급공고': 25, '공고중': 23, '정상공고': 21, '재공고': 20,
        '개찰대기': 18, '공고의뢰중': 16, '조달판단완료': 14,
        '수의협상중': 12, '조달판단중': 10,
    }
    score += status_score.get(row.get('_status'), 7)

    key = str(row.get('_date_sort_key', ''))
    now = datetime.now(SEOUL)
    current_month = now.strftime('%Y%m')
    previous_month = f'{now.year - (1 if now.month == 1 else 0):04d}{(12 if now.month == 1 else now.month - 1):02d}'
    if key.startswith(current_month):
        score += 20
    elif key.startswith(previous_month):
        score += 13
    elif key[:4] == str(now.year):
        score += 7

    category_score = {
        '장비구매': 5, '장비임차': 5, '핵심장비': 5,
        '유지보수': 3, '폐기물처리용역': 2, '부속·비품': 1,
    }
    score += category_score.get(row.get('_detail_category'), 0)
    return min(score, 100)


def score_grade(score):
    if score >= 75:
        return 'A'
    if score >= 50:
        return 'B'
    return 'C'


def scan_spreadsheet(client, spreadsheet_name, source, keyword):
    spreadsheet = client.open(spreadsheet_name)
    all_headers, equipment_rows, other_rows = [], [], []
    seen = set()

    for ws in spreadsheet.worksheets():
        if ws.title in EXCLUDE_TABS:
            continue
        values = ws.get_all_values()
        if len(values) < 2:
            continue
        header = values[0]
        for h in header:
            if h and h not in all_headers:
                all_headers.append(h)

        for value_row in values[1:]:
            if not any(value_row) or not row_matches_keyword(value_row, keyword):
                continue
            row = dict(zip(header, value_row))
            key = dedupe_key(row, source)
            if key in seen:
                continue
            seen.add(key)

            title = extract_title(row, keyword)
            detail = classify_detail(title)
            status = extract_status(row)
            row.update({
                '_tab': ws.title,
                '_source': source,
                '_title': title,
                '_detail_category': detail,
                '_subtype': subtype_from_detail(detail),
                '_status': status,
                '_is_open': is_open_opportunity(source, status),
                '_date_sort_key': extract_date_sort_key(row),
                '_amount': extract_amount(row, source),
                '_agency': first_value(row, AGENCY_COLUMNS),
                '_company': first_value(row, COMPANY_COLUMNS),
                '_bid_rate': extract_bid_rate(row),
            })
            row['_score'] = opportunity_score(row)
            row['_grade'] = score_grade(row['_score']) if row['_is_open'] else '-'

            if is_equipment_category(detail):
                equipment_rows.append(row)
            else:
                other_rows.append(row)

    extras = [
        '_tab', '_source', '_title', '_detail_category', '_subtype', '_status',
        '_is_open', '_date_sort_key', '_amount', '_agency', '_company',
        '_bid_rate', '_score', '_grade'
    ]
    for col in extras:
        if col not in all_headers:
            all_headers.append(col)
    return all_headers, equipment_rows, other_rows


def build_yearly(rows):
    result = {}
    for row in rows:
        year = extract_year(row) or '미상'
        bucket = result.setdefault(year, {
            'equipment_sum': 0, 'equipment_count': 0,
            'other_sum': 0, 'other_count': 0,
            'purchase_sum': 0, 'purchase_count': 0,
            'rental_sum': 0, 'rental_count': 0,
            'service_sum': 0, 'service_count': 0,
        })
        amount = row.get('_amount', 0)
        if is_equipment_category(row['_detail_category']):
            bucket['equipment_count'] += 1
            bucket['equipment_sum'] += amount
        else:
            bucket['other_count'] += 1
            bucket['other_sum'] += amount
        if row['_detail_category'] in {'장비구매', '핵심장비'}:
            bucket['purchase_count'] += 1
            bucket['purchase_sum'] += amount
        elif row['_detail_category'] == '장비임차':
            bucket['rental_count'] += 1
            bucket['rental_sum'] += amount
        else:
            bucket['service_count'] += 1
            bucket['service_sum'] += amount
    return result


def build_monthly(rows, limit=24):
    monthly = defaultdict(lambda: {
        'count': 0, 'amount_sum': 0, 'open_count': 0, 'open_amount': 0,
        'purchase_count': 0, 'rental_count': 0, 'service_count': 0,
    })
    for row in rows:
        key = str(row.get('_date_sort_key', ''))[:6]
        if len(key) != 6 or not key.isdigit():
            continue
        bucket = monthly[key]
        bucket['count'] += 1
        bucket['amount_sum'] += row.get('_amount', 0)
        if row.get('_is_open'):
            bucket['open_count'] += 1
            bucket['open_amount'] += row.get('_amount', 0)
        detail = row.get('_detail_category')
        if detail in {'장비구매', '핵심장비'}:
            bucket['purchase_count'] += 1
        elif detail == '장비임차':
            bucket['rental_count'] += 1
        else:
            bucket['service_count'] += 1
    keys = sorted(monthly.keys())[-limit:]
    return [{'month': k, **monthly[k]} for k in keys]


def build_agency_ranking(rows, top_n=20):
    agg = {}
    for row in rows:
        agency = row.get('_agency')
        if not agency:
            continue
        bucket = agg.setdefault(agency, {
            'count': 0, 'amount_sum': 0, 'open_count': 0, 'open_amount': 0,
            'purchase_count': 0, 'rental_count': 0, 'service_count': 0,
            'latest_date': '', 'companies': defaultdict(int),
        })
        bucket['count'] += 1
        bucket['amount_sum'] += row.get('_amount', 0)
        if row.get('_is_open'):
            bucket['open_count'] += 1
            bucket['open_amount'] += row.get('_amount', 0)
        detail = row.get('_detail_category')
        if detail in {'장비구매', '핵심장비'}:
            bucket['purchase_count'] += 1
        elif detail == '장비임차':
            bucket['rental_count'] += 1
        else:
            bucket['service_count'] += 1
        bucket['latest_date'] = max(bucket['latest_date'], row.get('_date_sort_key', ''))
        if row.get('_company'):
            bucket['companies'][row['_company']] += 1

    result = []
    for agency, value in agg.items():
        major = max(
            [('구매', value['purchase_count']), ('임차', value['rental_count']), ('용역·기타', value['service_count'])],
            key=lambda x: x[1]
        )[0]
        top_company = max(value['companies'].items(), key=lambda x: x[1])[0] if value['companies'] else None
        value['major_type'] = major
        value['top_company'] = top_company
        value.pop('companies', None)
        result.append({'agency': agency, **value})
    result.sort(key=lambda x: (x['open_count'], x['amount_sum'], x['count']), reverse=True)
    return result[:top_n]


def build_competitor_ranking(rows, top_n=20):
    agg = {}
    for row in rows:
        company = row.get('_company')
        if not company:
            continue
        bucket = agg.setdefault(company, {
            'count': 0, 'amount_sum': 0, 'bid_rates': [], 'agencies': defaultdict(int),
            'purchase_count': 0, 'rental_count': 0, 'service_count': 0,
        })
        bucket['count'] += 1
        bucket['amount_sum'] += row.get('_amount', 0)
        if row.get('_bid_rate') is not None:
            bucket['bid_rates'].append(row['_bid_rate'])
        if row.get('_agency'):
            bucket['agencies'][row['_agency']] += 1
        detail = row.get('_detail_category')
        if detail in {'장비구매', '핵심장비'}:
            bucket['purchase_count'] += 1
        elif detail == '장비임차':
            bucket['rental_count'] += 1
        else:
            bucket['service_count'] += 1

    result = []
    for company, value in agg.items():
        avg_rate = round(sum(value['bid_rates']) / len(value['bid_rates']), 2) if value['bid_rates'] else None
        top_agency = max(value['agencies'].items(), key=lambda x: x[1])[0] if value['agencies'] else None
        major = max(
            [('구매', value['purchase_count']), ('임차', value['rental_count']), ('용역·기타', value['service_count'])],
            key=lambda x: x[1]
        )[0]
        result.append({
            'company': company, 'count': value['count'], 'amount_sum': value['amount_sum'],
            'avg_bid_rate': avg_rate, 'top_agency': top_agency, 'major_type': major,
        })
    result.sort(key=lambda x: (x['amount_sum'], x['count']), reverse=True)
    return result[:top_n]


def compact_lead(row):
    return {
        'source': row.get('_source'), 'title': row.get('_title'), 'agency': row.get('_agency'),
        'company': row.get('_company'), 'amount': row.get('_amount', 0),
        'status': row.get('_status'), 'date': row.get('_date_sort_key'),
        'detail_category': row.get('_detail_category'), 'subtype': row.get('_subtype'),
        'score': row.get('_score', 0), 'grade': row.get('_grade', '-'), 'tab': row.get('_tab'),
    }


def main():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(GOOGLE_AUTH_JSON), scope)
    client = gspread.authorize(creds)

    result = {
        'updated_at': datetime.now(SEOUL).strftime('%Y-%m-%d %H:%M:%S'),
        'keyword': KEYWORD,
        'sources': {}, 'summary': [], 'yearly': {}, 'monthly': {},
        'agencies': {}, 'competitors': {}, 'pipeline': {}, 'priority_leads': [],
        'kpi': {},
    }

    all_rows = []
    for source, spreadsheet_name in SOURCE_SPREADSHEETS.items():
        print(f'=== {source} ({spreadsheet_name}) 스캔 중... ===')
        headers, equipment_rows, other_rows = scan_spreadsheet(client, spreadsheet_name, source, KEYWORD)
        rows = equipment_rows + other_rows
        all_rows.extend(rows)

        result['sources'][source] = {'columns': headers, 'equipment': equipment_rows, 'other': other_rows}
        result['summary'].extend([
            {'source': source, 'category': '음식물처리기', 'count': len(equipment_rows)},
            {'source': source, 'category': '그외음식물', 'count': len(other_rows)},
        ])
        result['yearly'][source] = build_yearly(rows)
        result['monthly'][source] = build_monthly(rows)
        result['agencies'][source] = build_agency_ranking(rows)
        result['competitors'][source] = build_competitor_ranking(rows)
        result['pipeline'][source] = {
            'count': len(rows),
            'amount_sum': sum(r.get('_amount', 0) for r in rows),
            'open_count': sum(1 for r in rows if r.get('_is_open')),
            'open_amount': sum(r.get('_amount', 0) for r in rows if r.get('_is_open')),
        }

    open_rows = [r for r in all_rows if r.get('_is_open')]
    open_rows.sort(key=lambda r: (r.get('_score', 0), r.get('_amount', 0), r.get('_date_sort_key', '')), reverse=True)
    result['priority_leads'] = [compact_lead(r) for r in open_rows[:20]]

    current_month = datetime.now(SEOUL).strftime('%Y%m')
    contract_rows = [r for r in all_rows if r.get('_source') == '계약정보']
    bid_rates = [r['_bid_rate'] for r in contract_rows if r.get('_bid_rate') is not None]
    collection_summary_path = 'runtime/collection_summary.json'
    if os.path.exists(collection_summary_path):
        try:
            with open(collection_summary_path, encoding='utf-8') as summary_file:
                result['collection'] = json.load(summary_file)
        except (OSError, json.JSONDecodeError):
            result['collection'] = None
    else:
        result['collection'] = None

    result['kpi'] = {
        'total_count': len(all_rows),
        'total_amount': sum(r.get('_amount', 0) for r in all_rows),
        'open_count': len(open_rows),
        'open_amount': sum(r.get('_amount', 0) for r in open_rows),
        'current_month_count': sum(1 for r in all_rows if str(r.get('_date_sort_key', '')).startswith(current_month)),
        'current_month_open_count': sum(1 for r in open_rows if str(r.get('_date_sort_key', '')).startswith(current_month)),
        'purchase_count': sum(1 for r in all_rows if r.get('_detail_category') in {'장비구매', '핵심장비'}),
        'rental_count': sum(1 for r in all_rows if r.get('_detail_category') == '장비임차'),
        'company_count': len({r.get('_company') for r in contract_rows if r.get('_company')}),
        'avg_bid_rate': round(sum(bid_rates) / len(bid_rates), 2) if bid_rates else None,
    }

    # 화면별로 필요한 데이터만 분리해 초기 로딩 용량을 줄인다.
    summary_payload = {key: value for key, value in result.items() if key != 'sources'}
    leads_payload = {
        'updated_at': result['updated_at'],
        'keyword': result['keyword'],
        'sources': result['sources'],
        'kpi': result['kpi'],
    }
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_files = {
        os.path.join(OUTPUT_DIR, 'summary.json'): summary_payload,
        os.path.join(OUTPUT_DIR, 'leads.json'): leads_payload,
    }
    for path, payload in output_files.items():
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))
        print(f'✅ {path} 생성 완료 ({os.path.getsize(path):,} bytes)')

    # 기존 외부 링크와 호환되도록 data.json도 유지한다.
    os.makedirs(os.path.dirname(LEGACY_OUTPUT_PATH) or '.', exist_ok=True)
    with open(LEGACY_OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, separators=(',', ':'))
    print(f'✅ {LEGACY_OUTPUT_PATH} 호환 파일 생성 완료 ({os.path.getsize(LEGACY_OUTPUT_PATH):,} bytes)')


if __name__ == '__main__':
    main()
