import os, json, datetime, time, re, requests
import xml.etree.ElementTree as ET
import pandas as pd
import io
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseUpload
from concurrent.futures import ThreadPoolExecutor, as_completed

# =================================================================================
# 1. 설정 및 상수
# =================================================================================
MY_DIRECT_KEY   = os.environ.get('DATA_GO_KR_API_KEY')
AUTH_JSON_STR   = os.environ.get('GOOGLE_AUTH_JSON')

NOTICE_FOLDER_ID   = "1AsvVmayEmTtY92d1SfXxNi6bL0Zjw5mg"
SHOPPING_FOLDER_ID = "1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr"

HEADER_KOR = [
    '조달구분명', '계약구분명', '계약납품구분명', '계약납품요구일자', '계약납품요구번호',
    '변경차수', '최종변경차수여부', '수요기관명', '수요기관구분명', '수요기관지역명',
    '수요기관코드', '물품분류번호', '품명', '세부물품분류번호', '세부품명',
    '물품식별번호', '물품규격명', '단가', '수량', '단위', '금액', '업체명',
    '업체기업구분명', '계약명', '우수제품여부', '공사용자재직접구매대상여부',
    '다수공급자계약여부', '다수공급자계약2단계진행여부', '단가계약번호', '단가계약변경차수',
    '최초계약(납품요구)일자', '계약체결방법명', '증감수량', '증감금액', '납품장소명',
    '납품기한일자', '업체사업자등록번호', '인도조건명', '물품순번'
]

SHOPPING_DEDUP_COLS = ['계약납품요구번호', '물품순번']
NOTICE_DEDUP_COL = 'bidNtceNo'

CAT_KEYWORDS: dict[str, list[str]] = {
    '영상감시장치': ['CCTV', '통합관제', '영상감시장치', '영상정보처리기기'],
    '국방':        ['국방', '부대', '작전', '경계', '방위', '군사', '무인화', '사령부', '군대',
                    '중요시설', '주둔지', '과학화', '육군', '해군', '공군', '해병'],
    '솔루션':      ['데이터', '플랫폼', '솔루션', '주차', '출입', 'GIS'],
    '스마트도시':   ['ITS', '스마트시티', '스마트도시'],
}

CAT_PATTERNS: dict[str, re.Pattern] = {
    cat: re.compile('|'.join(map(re.escape, kws)))
    for cat, kws in CAT_KEYWORDS.items()
}
ALL_NOTICE_KEYWORDS = [kw for kws in CAT_KEYWORDS.values() for kw in kws]
ALL_NOTICE_PATTERN  = re.compile('|'.join(map(re.escape, ALL_NOTICE_KEYWORDS)))
EXCLUDE_ORG_PATTERN = re.compile(r'학교|민방위|교육청')

# 이노뎁 핵심 사업 키워드 (우선순위 정렬용)
INNODEP_PRIORITY_KEYWORDS = [
    'CCTV', '영상감시', '통합관제', '영상정보처리', '지능형영상',
    '과학화경계', '무인경계', '스마트도시', 'ITS', '교통관제',
    '주차관제', '출입통제', 'VMS', 'NVR', 'DVR'
]
INNODEP_PRIORITY_PATTERN = re.compile('|'.join(map(re.escape, INNODEP_PRIORITY_KEYWORDS)))

# 이노뎁 주요 경쟁사
COMPETITOR_KEYWORDS = [
    '한화비전', '한화테크윈', '아이디스', '씨앤비텍', '에이치디씨',
    '다후아', '하이크비전', '유니뷰', '보쉬', 'AXIS', 'Axis',
    '인텍플러스', '아이브스', '넥스위', '아이씨엔티', '모아시스',
    '에스원', 'KT텔레캅', 'ADT캡스', '케이티텔레캅',
]
COMPETITOR_PATTERN = re.compile('|'.join(map(re.escape, COMPETITOR_KEYWORDS)))

# 카테고리 메타 - 다크계열 배경 없이 라이트 파스텔 톤으로 통일
CAT_META = {
    '영상감시장치': {'icon': '📷', 'accent': '#1e5fa8', 'bg': '#eef4fb', 'border': '#b8d0ee', 'text': '#1e3a5f'},
    '국방':        {'icon': '🛡', 'accent': '#b91c1c', 'bg': '#fdf2f2', 'border': '#f5c0c0', 'text': '#7f1d1d'},
    '솔루션':      {'icon': '💡', 'accent': '#6d28d9', 'bg': '#f5f3ff', 'border': '#d4c9f7', 'text': '#3b1a8e'},
    '스마트도시':   {'icon': '🏙', 'accent': '#047857', 'bg': '#f0fdf6', 'border': '#a7d9c0', 'text': '#064e3b'},
}

KEYWORDS = sorted(set([
    '네트워크시스템장비용랙', '영상감시장치', 'PA용스피커', '안내판', '카메라브래킷', '액정모니터',
    '광송수신모듈', '전원공급장치', '광분배함', '컨버터', '컴퓨터서버', '하드디스크드라이브',
    '네트워크스위치', '광점퍼코드', '풀박스', '서지흡수기', '디지털비디오레코더',
    '스피커', '오디오앰프', '브래킷', 'UTP케이블', '정보통신공사', '영상정보디스플레이장치',
    '송신기', '난연전력케이블', '1종금속제가요전선관', '호온스피커', '누전차단기', '방송수신기',
    'LAP외피광케이블', '폴리에틸렌전선관', '리모트앰프', '랙캐비닛용패널', '베어본컴퓨터',
    '분배기', '결선보드유닛', '벨', '난연접지용비닐절연전선', '경광등', '데스크톱컴퓨터',
    '특수목적컴퓨터', '철근콘크리트공사', '토공사', '안내전광판', '접지봉', '카메라회전대',
    '무선랜액세스포인트', '컴퓨터망전환장치', '포장공사', '고주파동축케이블', '카메라하우징',
    '인터폰', '스위칭모드전원공급장치', '금속상자', '열선감지기', '태양전지조절기',
    '밀폐고정형납축전지', 'IP전화기', '디스크어레이', '그래픽용어댑터', '인터콤장비',
    '기억유닛', '컴퓨터지문인식장치', '랜접속카드', '접지판', '제어케이블', '비디오네트워킹장비',
    '레이스웨이', '콘솔익스텐더', '전자카드', '비대면방역감지장비', '온습도트랜스미터',
    '도난방지기', '융복합영상감시장치', '멀티스크린컴퓨터', '컴퓨터정맥인식장치',
    '카메라컨트롤러', 'SSD저장장치', '원격단말장치(RTU)', '융복합네트워크스위치',
    '융복합액정모니터', '융복합데스크톱컴퓨터', '융복합그래픽용어댑터', '융복합베어본컴퓨터',
    '융복합서지흡수기', '배선장치', '융복합배선장치', '융복합카메라브래킷',
    '융복합네트워크시스템장비용랙', '융복합UTP케이블', '테이프백업장치', '자기식테이프',
    '레이드저장장치', '광송수신기', '450/750V 유연성단심비닐절연전선', '솔내시스템',
    '450/750V유연성단심비닐절연전선', '카메라받침대', '텔레비전거치대', '광수신기',
    '무선통신장치', '동작분석기', '전력공급장치', '450/750V 일반용유연성단심비닐절연전선',
    '분전함', '비디오믹서', '절연전선및피복선', '레이더', '적외선방사기', '보안용카메라',
    '통신소프트웨어', '분석및과학용소프트웨어', '소프트웨어유지및지원서비스',
    '교통관제시스템', '산업관리소프트웨어', '시스템관리소프트웨어', '적외선카메라',
    '주차경보등', '주차관제주변기기', '주차권판독기', '주차안내판', '주차요금계산기',
    '주차주제어장치', '차량감지기', '차량인식기', '차량차단기',
    '패키지소프트웨어개발및도입서비스', '무선인식리더기', '바코드시스템', '출입통제시스템', '카드인쇄기',
]))

NOTICE_API_MAP = {
    '공사': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwkPPSSrch',
    '물품': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThngPPSSrch',
    '용역': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoServcPPSSrch',
}


# =================================================================================
# 2. 유틸리티
# =================================================================================

def get_drive_service():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(AUTH_JSON_STR),
        scopes=['https://www.googleapis.com/auth/drive']
    )
    return build('drive', 'v3', credentials=creds), creds


def get_target_date() -> datetime.datetime:
    now = datetime.datetime.now(datetime.UTC) + datetime.timedelta(hours=9)
    return now - datetime.timedelta(days=1)


def classify_text(text: str) -> str:
    for cat, pat in CAT_PATTERNS.items():
        if pat.search(str(text)):
            return cat
    return '기타'


def innodep_priority_score(item: dict) -> int:
    """이노뎁 핵심 사업 관련도 점수 (높을수록 우선 표출)"""
    text = str(item.get('nm', '')) + str(item.get('org', ''))
    return 1 if INNODEP_PRIORITY_PATTERN.search(text) else 0


def sort_items_by_priority(items: list[dict]) -> list[dict]:
    """이노뎁 핵심 키워드 포함 항목을 상위로 정렬, 그 다음 금액순"""
    def sort_key(item):
        priority = innodep_priority_score(item)
        try:
            amt = int(str(item.get('amt', '0')).replace(',', '').split('.')[0])
        except (ValueError, AttributeError):
            amt = 0
        return (-priority, -amt)
    return sorted(items, key=sort_key)


def fmt_amount(val) -> str:
    try:
        n = int(str(val).replace(',', '').split('.')[0])
        if n >= 100_000_000:
            return f"{n/100_000_000:.1f}억원"
        elif n >= 10_000:
            return f"{n/10_000:.0f}만원"
        return f"{n:,}원"
    except (ValueError, AttributeError):
        return str(val) if val else '별도공고'


def read_drive_csv(drive_creds, file_id: str) -> pd.DataFrame | None:
    try:
        resp = requests.get(
            f'https://www.googleapis.com/drive/v3/files/{file_id}?alt=media',
            headers={'Authorization': f'Bearer {drive_creds.token}'},
            timeout=30
        )
        resp.raise_for_status()
        return pd.read_csv(io.BytesIO(resp.content), encoding='utf-8-sig', low_memory=False)
    except Exception as e:
        print(f"  ⚠️  드라이브 CSV 읽기 실패 (id={file_id}): {e}")
        return None


def upload_drive_csv(drive_service, file_id: str | None, folder_id: str,
                     file_name: str, df: pd.DataFrame) -> None:
    csv_bytes = df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
    media = MediaIoBaseUpload(io.BytesIO(csv_bytes), mimetype='text/csv', resumable=False)
    if file_id:
        drive_service.files().update(fileId=file_id, media_body=media).execute()
    else:
        drive_service.files().create(
            body={'name': file_name, 'parents': [folder_id]},
            media_body=media
        ).execute()


def find_drive_file(drive_service, folder_id: str, file_name: str) -> str | None:
    res = drive_service.files().list(
        q=f"name='{file_name}' and '{folder_id}' in parents and trashed=false",
        fields='files(id)'
    ).execute()
    items = res.get('files', [])
    return items[0]['id'] if items else None


# =================================================================================
# 3. 데이터 수집
# =================================================================================

def fetch_shopping(kw: str, d_str: str, retries: int = 3) -> list:
    url = "https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getSpcifyPrdlstPrcureInfoList"
    params = {
        'numOfRows': '999', 'pageNo': '1', 'ServiceKey': MY_DIRECT_KEY,
        'type': 'xml', 'inqryDiv': '1', 'inqryPrdctDiv': '2',
        'inqryBgnDate': d_str, 'inqryEndDate': d_str, 'dtilPrdctClsfcNoNm': kw,
    }
    for attempt in range(retries):
        try:
            res = requests.get(url, params=params, timeout=60)
            if res.status_code == 200 and "<item>" in res.text:
                root = ET.fromstring(res.content)
                return [[e.text or '' for e in item] for item in root.findall('.//item')]
            return []
        except requests.exceptions.Timeout:
            wait = (attempt + 1) * 5
            print(f"  [{kw}] 타임아웃 ({attempt+1}/{retries}회), {wait}s 후 재시도...")
            time.sleep(wait)
        except Exception as e:
            print(f"  [{kw}] 수집 오류: {e}")
            return []
    print(f"  [{kw}] 최종 실패 — 스킵")
    return []


def fetch_notice(cat_api: str, api_url: str, d_str: str) -> pd.DataFrame:
    params = {
        'serviceKey': MY_DIRECT_KEY, 'pageNo': '1', 'numOfRows': '999',
        'inqryDiv': '1', 'type': 'json',
        'inqryBgnDt': d_str + "0000", 'inqryEndDt': d_str + "2359",
    }
    try:
        res = requests.get(api_url, params=params, timeout=15)
        if res.status_code == 200:
            items = res.json().get('response', {}).get('body', {}).get('items', [])
            return pd.DataFrame(items)
    except Exception as e:
        print(f"  [{cat_api}] 공고 수집 오류: {e}")
    return pd.DataFrame()


def fetch_contract(kw: str, d_str: str) -> list[dict]:
    url = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'
    params = {
        'serviceKey': MY_DIRECT_KEY, 'inqryDiv': '1', 'type': 'xml',
        'inqryBgnDate': d_str, 'inqryEndDate': d_str, 'cntrctNm': kw,
    }
    results = []
    try:
        res = requests.get(url, params=params, timeout=20)
        if res.status_code != 200:
            return results
        root = ET.fromstring(res.content)
        for item in root.findall('.//item'):
            detail_url = item.findtext('cntrctDtlInfoUrl') or "https://www.g2b.go.kr"
            raw_demand = item.findtext('dminsttList', '-')
            clean_org  = (raw_demand.replace('[','').replace(']','').split('^')[2]
                          if '^' in raw_demand else raw_demand)
            raw_corp   = item.findtext('corpList', '-')
            clean_corp = (raw_corp.replace('[','').replace(']','').split('^')[3]
                          if '^' in raw_corp else raw_corp)
            results.append({
                'org':  clean_org,
                'nm':   item.findtext('cntrctNm', '-'),
                'corp': clean_corp,
                'amt':  item.findtext('totCntrctAmt', '0'),
                'url':  detail_url,
            })
    except Exception as e:
        print(f"  [{kw}] 계약 수집 오류: {e}")
    return results


# =================================================================================
# 4. 드라이브 저장 (중복 제거 포함)
# =================================================================================

def save_shopping_to_drive(drive_service, drive_creds,
                            new_df: pd.DataFrame, year: int) -> dict:
    file_name = f"{year}.csv"
    file_id   = find_drive_file(drive_service, SHOPPING_FOLDER_ID, file_name)
    before_today = len(new_df)
    combined     = new_df.copy()
    if file_id:
        old_df = read_drive_csv(drive_creds, file_id)
        if old_df is not None and not old_df.empty:
            combined = pd.concat([old_df, new_df], ignore_index=True)
    if '변경차수' in combined.columns:
        combined['변경차수'] = pd.to_numeric(combined['변경차수'], errors='coerce').fillna(0)
        combined.sort_values('변경차수', ascending=True, inplace=True)
    dedup_cols = [c for c in SHOPPING_DEDUP_COLS if c in combined.columns]
    before_dedup = len(combined)
    if dedup_cols:
        combined.drop_duplicates(subset=dedup_cols, keep='last', inplace=True)
    dropped = before_dedup - len(combined)
    combined.reset_index(drop=True, inplace=True)
    upload_drive_csv(drive_service, file_id, SHOPPING_FOLDER_ID, file_name, combined)
    action = "갱신" if file_id else "신규 생성"
    print(f"  ✅ 쇼핑몰 {file_name} {action} | "
          f"오늘 {before_today:,}건 → 중복 {dropped:,}건 제거 → 누적 {len(combined):,}건")
    return {'before': before_today, 'after': len(combined), 'dropped': dropped}


def save_notice_to_drive(drive_service, drive_creds,
                          cat_name: str, new_df: pd.DataFrame, year: int) -> dict:
    file_name = f"나라장터_공고_{cat_name}_{year}년.csv"
    file_id   = find_drive_file(drive_service, NOTICE_FOLDER_ID, file_name)
    before_today = len(new_df)
    combined     = new_df.copy()
    if file_id:
        old_df = read_drive_csv(drive_creds, file_id)
        if old_df is not None and not old_df.empty:
            combined = pd.concat([old_df, new_df], ignore_index=True)
    before_dedup = len(combined)
    if NOTICE_DEDUP_COL in combined.columns:
        combined.drop_duplicates(subset=[NOTICE_DEDUP_COL], keep='last', inplace=True)
    dropped = before_dedup - len(combined)
    combined.reset_index(drop=True, inplace=True)
    upload_drive_csv(drive_service, file_id, NOTICE_FOLDER_ID, file_name, combined)
    action = "갱신" if file_id else "신규 생성"
    print(f"  ✅ [{cat_name}] {file_name} {action} | "
          f"오늘 {before_today:,}건 → 중복 {dropped:,}건 제거 → 누적 {len(combined):,}건")
    return {'before': before_today, 'after': len(combined), 'dropped': dropped}


# =================================================================================
# 5. HTML 리포트 생성
# =================================================================================

def _stat_card(label: str, value: str, sub: str = '', accent: str = '#1e5fa8', bg: str = '#eef4fb') -> str:
    return (
        f"<div style='background:{bg};border-radius:10px;padding:18px 14px;"
        f"text-align:center;border-left:4px solid {accent};'>"
        f"<div style='font-size:11px;color:#6b7280;font-weight:600;letter-spacing:0.5px;"
        f"text-transform:uppercase;margin-bottom:6px;'>{label}</div>"
        f"<div style='font-size:22px;font-weight:700;color:{accent};'>{value}</div>"
        + (f"<div style='font-size:10px;color:#9ca3af;margin-top:4px;'>{sub}</div>" if sub else "")
        + "</div>"
    )


def _section_header(title: str, count: int, meta: dict) -> str:
    accent, bg, border, text, icon = meta['accent'], meta['bg'], meta['border'], meta['text'], meta['icon']
    return (
        f"<div style='display:flex;align-items:center;gap:10px;padding:12px 16px;"
        f"background:{bg};border:1px solid {border};border-radius:8px;margin-bottom:12px;'>"
        f"<span style='font-size:18px;'>{icon}</span>"
        f"<span style='font-size:15px;font-weight:700;color:{text};'>{title}</span>"
        f"<span style='margin-left:auto;background:{accent};color:#fff;"
        f"border-radius:20px;padding:3px 12px;font-size:11px;font-weight:600;'>{count}건</span>"
        f"</div>"
    )


def _section_table(items: list[dict], title: str, cat: str) -> str:
    meta = CAT_META.get(cat, {'icon': '📋', 'accent': '#374151', 'bg': '#f9fafb', 'border': '#e5e7eb', 'text': '#374151'})
    accent, bg = meta['accent'], meta['bg']

    # 이노뎁 핵심 사업 우선 정렬
    sorted_items = sort_items_by_priority(items)

    html = _section_header(title, len(sorted_items), meta)

    if not sorted_items:
        return (html +
                f"<div style='background:#f9fafb;border:1px dashed #d1d5db;border-radius:8px;"
                f"padding:14px;color:#9ca3af;font-size:13px;text-align:center;margin-bottom:20px;'>"
                f"해당 내역이 없습니다.</div>")

    rows = ""
    for i, item in enumerate(sorted_items):
        is_innodep   = '이노뎁' in str(item.get('corp', ''))
        is_priority  = innodep_priority_score(item) == 1
        is_competitor = COMPETITOR_PATTERN.search(str(item.get('corp', ''))) is not None

        if is_innodep:
            row_bg = '#fffbeb'
            corp_style = f"color:{accent};font-weight:600;"
        elif is_competitor:
            row_bg = '#fdf2f2'
            corp_style = "color:#b91c1c;font-weight:500;"
        else:
            row_bg = '#ffffff' if i % 2 == 0 else '#f9fafb'
            corp_style = "color:#374151;"

        badge = ""
        if is_innodep:
            badge = f"<span style='font-size:9px;background:#fef3c7;color:#92400e;border:1px solid #fde68a;border-radius:4px;padding:1px 5px;margin-left:4px;'>★이노뎁</span>"
        elif is_priority and not is_innodep:
            badge = f"<span style='font-size:9px;background:{bg};color:{accent};border:1px solid {meta['border']};border-radius:4px;padding:1px 5px;margin-left:4px;'>핵심</span>"

        nm   = item.get('nm', '-')
        url  = item.get('url', '#')
        link_nm = (f"<a href='{url}' target='_blank' "
                   f"style='color:{accent};text-decoration:none;font-weight:500;'>{nm}</a>"
                   if url and url != '#' else f"<span style='color:#374151;'>{nm}</span>")

        rows += (
            f"<tr style='background:{row_bg};border-bottom:1px solid #e5e7eb;'>"
            f"<td style='padding:7px 10px;color:#374151;font-size:12px;'>{item.get('org','-')}</td>"
            f"<td style='padding:7px 10px;font-size:12px;'>{link_nm}</td>"
            f"<td style='padding:7px 10px;text-align:center;font-size:12px;{corp_style}'>"
            f"{item.get('corp','-')}{badge}</td>"
            f"<td style='padding:7px 10px;text-align:right;color:#374151;"
            f"font-size:12px;font-variant-numeric:tabular-nums;'>{fmt_amount(item.get('amt','0'))}</td>"
            f"</tr>"
        )

    html += (
        f"<div style='overflow-x:auto;margin-bottom:20px;'>"
        f"<table style='width:100%;border-collapse:collapse;font-size:12.5px;"
        f"border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;'>"
        f"<thead><tr style='background:{bg};'>"
        f"<th style='padding:9px 10px;text-align:left;width:22%;font-size:12px;font-weight:700;color:{meta['text']};border-bottom:2px solid {meta['border']};'>수요기관</th>"
        f"<th style='padding:9px 10px;text-align:left;font-size:12px;font-weight:700;color:{meta['text']};border-bottom:2px solid {meta['border']};'>사업명</th>"
        f"<th style='padding:9px 10px;text-align:center;width:16%;font-size:12px;font-weight:700;color:{meta['text']};border-bottom:2px solid {meta['border']};'>업체명</th>"
        f"<th style='padding:9px 10px;text-align:right;width:13%;font-size:12px;font-weight:700;color:{meta['text']};border-bottom:2px solid {meta['border']};'>금액</th>"
        f"</tr></thead><tbody>{rows}</tbody></table></div>"
    )
    return html


def _vendor_chart_js(vendor_data: dict, chart_id: str, title: str,
                     innodep_color: str = '#1e5fa8',
                     competitor_color: str = '#b91c1c',
                     other_color: str = '#94a3b8') -> str:
    """업체별 납품금액 수평 막대 차트 (경쟁사 빨강, 이노뎁 파랑, 기타 회색)"""
    if not vendor_data:
        return ""

    top20 = sorted(vendor_data.items(), key=lambda x: x[1], reverse=True)[:20]
    labels = [v[0] for v in top20]
    values = [v[1] for v in top20]
    colors = []
    for lbl in labels:
        if '이노뎁' in lbl:
            colors.append(innodep_color)
        elif COMPETITOR_PATTERN.search(lbl):
            colors.append(competitor_color)
        else:
            colors.append(other_color)

    labels_js = json.dumps(labels, ensure_ascii=False)
    values_js = json.dumps(values)
    colors_js = json.dumps(colors)
    bar_height = max(len(top20) * 32 + 80, 200)

    return f"""
<div style='margin-bottom:8px;'>
  <div style='font-size:13px;font-weight:700;color:#374151;margin-bottom:6px;'>{title}</div>
  <div style='display:flex;gap:12px;margin-bottom:8px;font-size:11px;'>
    <span style='display:flex;align-items:center;gap:4px;'>
      <span style='width:10px;height:10px;border-radius:2px;background:{innodep_color};display:inline-block;'></span>
      <span style='color:#374151;'>이노뎁</span>
    </span>
    <span style='display:flex;align-items:center;gap:4px;'>
      <span style='width:10px;height:10px;border-radius:2px;background:{competitor_color};display:inline-block;'></span>
      <span style='color:#374151;'>주요 경쟁사</span>
    </span>
    <span style='display:flex;align-items:center;gap:4px;'>
      <span style='width:10px;height:10px;border-radius:2px;background:{other_color};display:inline-block;'></span>
      <span style='color:#374151;'>기타</span>
    </span>
  </div>
  <div style='position:relative;width:100%;height:{bar_height}px;'>
    <canvas id='{chart_id}' role='img' aria-label='{title} 업체별 납품금액 차트'></canvas>
  </div>
</div>
<script>
(function(){{
  var ctx = document.getElementById('{chart_id}');
  if(!ctx) return;
  new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels: {labels_js},
      datasets: [{{
        label: '납품금액(원)',
        data: {values_js},
        backgroundColor: {colors_js},
        borderWidth: 0,
        borderRadius: 3,
      }}]
    }},
    options: {{
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          callbacks: {{
            label: function(ctx) {{
              var v = ctx.raw;
              if(v >= 100000000) return (v/100000000).toFixed(1) + '억원';
              if(v >= 10000) return Math.round(v/10000) + '만원';
              return v.toLocaleString() + '원';
            }}
          }}
        }}
      }},
      scales: {{
        x: {{
          grid: {{ color: '#f1f5f9' }},
          ticks: {{
            font: {{ size: 10 }},
            color: '#6b7280',
            callback: function(v) {{
              if(v >= 100000000) return (v/100000000).toFixed(0) + '억';
              if(v >= 10000) return Math.round(v/10000) + '만';
              return v;
            }}
          }}
        }},
        y: {{
          grid: {{ display: false }},
          ticks: {{ font: {{ size: 11 }}, color: '#374151' }}
        }}
      }}
    }}
  }});
}})();
</script>"""


def _org_chart_js(org_data: dict, chart_id: str, title: str, accent: str = '#1e5fa8', bg_light: str = '#eef4fb') -> str:
    """수요기관 상위 20 수평 막대 차트"""
    if not org_data:
        return ""

    top20 = sorted(org_data.items(), key=lambda x: x[1], reverse=True)[:20]
    labels = [v[0] for v in top20]
    values = [v[1] for v in top20]
    labels_js = json.dumps(labels, ensure_ascii=False)
    values_js = json.dumps(values)
    bar_height = max(len(top20) * 32 + 80, 200)

    return f"""
<div style='margin-bottom:8px;'>
  <div style='font-size:13px;font-weight:700;color:#374151;margin-bottom:10px;'>{title}</div>
  <div style='position:relative;width:100%;height:{bar_height}px;'>
    <canvas id='{chart_id}' role='img' aria-label='{title} 수요기관별 납품금액 차트'></canvas>
  </div>
</div>
<script>
(function(){{
  var ctx = document.getElementById('{chart_id}');
  if(!ctx) return;
  new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels: {labels_js},
      datasets: [{{
        label: '납품금액(원)',
        data: {values_js},
        backgroundColor: '{accent}',
        borderWidth: 0,
        borderRadius: 3,
      }}]
    }},
    options: {{
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          callbacks: {{
            label: function(ctx) {{
              var v = ctx.raw;
              if(v >= 100000000) return (v/100000000).toFixed(1) + '억원';
              if(v >= 10000) return Math.round(v/10000) + '만원';
              return v.toLocaleString() + '원';
            }}
          }}
        }}
      }},
      scales: {{
        x: {{
          grid: {{ color: '#f1f5f9' }},
          ticks: {{
            font: {{ size: 10 }},
            color: '#6b7280',
            callback: function(v) {{
              if(v >= 100000000) return (v/100000000).toFixed(0) + '억';
              if(v >= 10000) return Math.round(v/10000) + '만';
              return v;
            }}
          }}
        }},
        y: {{
          grid: {{ display: false }},
          ticks: {{ font: {{ size: 11 }}, color: '#374151' }}
        }}
      }}
    }}
  }});
}})();
</script>"""


def build_report_html(
    display_date: str, weekday_str: str,
    shopping_cnt: int, notice_cnt: int, contract_cnt: int,
    shopping_dedup: int, notice_dedup_total: int,
    school_stats: dict, innodep_dict: dict, innodep_total: int,
    notice_buckets: dict, contract_buckets: dict,
    vendor_stats: dict,   # 업체별 납품금액 집계 (전체)
    org_stats: dict,      # 수요기관별 납품금액 집계 (전체)
) -> str:

    # ── 학교 CCTV 테이블 ──────────────────────────────────────────────
    if school_stats:
        total_school_amt = sum(s['total_amt'] for s in school_stats.values())
        school_rows = "".join(
            f"<tr style='background:{'#ffffff' if i%2==0 else '#fffbeb'};border-bottom:1px solid #fde68a;'>"
            f"<td style='padding:7px 10px;color:#374151;font-size:12px;'>{sch}</td>"
            f"<td style='padding:7px 10px;color:#374151;font-size:12px;'>{info['main_vendor']}</td>"
            f"<td style='padding:7px 10px;text-align:right;color:#374151;font-size:12px;'>{info['total_amt']:,}원</td></tr>"
            for i, (sch, info) in enumerate(school_stats.items())
        )
        school_section = (
            f"<div style='overflow-x:auto;'>"
            f"<table style='width:100%;border-collapse:collapse;font-size:12.5px;"
            f"border:1px solid #fde68a;border-radius:8px;overflow:hidden;'>"
            f"<thead><tr style='background:#fef9ec;'>"
            f"<th style='padding:9px 10px;text-align:left;font-size:12px;font-weight:700;color:#92400e;border-bottom:2px solid #fde68a;'>학교명</th>"
            f"<th style='padding:9px 10px;text-align:left;font-size:12px;font-weight:700;color:#92400e;border-bottom:2px solid #fde68a;'>납품업체</th>"
            f"<th style='padding:9px 10px;text-align:right;font-size:12px;font-weight:700;color:#92400e;border-bottom:2px solid #fde68a;width:20%;'>납품금액</th>"
            f"</tr></thead><tbody>{school_rows}</tbody>"
            f"<tfoot><tr style='background:#fef3c7;'>"
            f"<td colspan='2' style='padding:9px 10px;font-size:12px;font-weight:700;color:#92400e;'>합계 ({len(school_stats)}개교)</td>"
            f"<td style='padding:9px 10px;text-align:right;font-size:12px;font-weight:700;color:#92400e;'>{total_school_amt:,}원</td>"
            f"</tr></tfoot></table></div>"
        )
    else:
        school_section = "<div style='color:#9ca3af;font-size:13px;padding:10px 0;'>해당 내역 없음</div>"

    # ── 이노뎁 납품 테이블 (사업명 포함) ─────────────────────────────
    if innodep_dict:
        innodep_rows = "".join(
            f"<tr style='background:{'#ffffff' if i%2==0 else '#f0f7ff'};border-bottom:1px solid #b8d0ee;'>"
            f"<td style='padding:7px 10px;color:#374151;font-size:12px;'>{org}</td>"
            f"<td style='padding:7px 10px;color:#1e3a5f;font-size:12px;'>{info.get('nm','-')}</td>"
            f"<td style='padding:7px 10px;text-align:right;color:#374151;font-size:12px;'>{info['amt']:,}원</td></tr>"
            for i, (org, info) in enumerate(innodep_dict.items())
        )
        innodep_section = (
            f"<div style='overflow-x:auto;'>"
            f"<table style='width:100%;border-collapse:collapse;font-size:12.5px;"
            f"border:1px solid #b8d0ee;border-radius:8px;overflow:hidden;'>"
            f"<thead><tr style='background:#eef4fb;'>"
            f"<th style='padding:9px 10px;text-align:left;font-size:12px;font-weight:700;color:#1e3a5f;border-bottom:2px solid #b8d0ee;'>수요기관</th>"
            f"<th style='padding:9px 10px;text-align:left;font-size:12px;font-weight:700;color:#1e3a5f;border-bottom:2px solid #b8d0ee;'>사업명</th>"
            f"<th style='padding:9px 10px;text-align:right;font-size:12px;font-weight:700;color:#1e3a5f;border-bottom:2px solid #b8d0ee;width:22%;'>납품금액</th>"
            f"</tr></thead><tbody>{innodep_rows}</tbody>"
            f"<tfoot><tr style='background:#dbeafe;'>"
            f"<td colspan='2' style='padding:9px 10px;font-size:12px;font-weight:700;color:#1e3a5f;'>합계 ({len(innodep_dict)}건)</td>"
            f"<td style='padding:9px 10px;text-align:right;font-size:12px;font-weight:700;color:#1e3a5f;'>{innodep_total:,}원</td>"
            f"</tr></tfoot></table></div>"
        )
    else:
        innodep_section = "<div style='color:#9ca3af;font-size:13px;padding:10px 0;'>해당 내역 없음</div>"

    notice_sections   = "".join(_section_table(notice_buckets[c],   f"{c} 입찰공고",   c) for c in CAT_KEYWORDS)
    contract_sections = "".join(_section_table(contract_buckets[c], f"{c} 계약내역", c) for c in CAT_KEYWORDS)

    # 요약 카드 4개
    stat_cards = (
        f"<div style='display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:20px;'>"
        + _stat_card('쇼핑몰 수집', f'{shopping_cnt:,}건',  f'중복 {shopping_dedup:,}건 제거', '#b45309', '#fef9ec')
        + _stat_card('공고 수집',   f'{notice_cnt:,}건',    f'중복 {notice_dedup_total:,}건 제거', '#1e5fa8', '#eef4fb')
        + _stat_card('계약 수집',   f'{contract_cnt:,}건',  '', '#6d28d9', '#f5f3ff')
        + _stat_card('이노뎁 납품', fmt_amount(innodep_total), f'{len(innodep_dict)}개 기관', '#047857', '#f0fdf6')
        + "</div>"
    )

    # 차트 JS
    vendor_chart = _vendor_chart_js(vendor_stats, 'vendorChart', '업체별 납품금액 순위 (상위 20개)')
    org_chart    = _org_chart_js(org_stats, 'orgChart', '수요기관별 납품금액 순위 (상위 20개)', '#1e5fa8', '#eef4fb')

    return f"""<!DOCTYPE html>
<html lang="ko"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; padding: 0;
    background: #f1f5f9;
    font-family: 'Apple SD Gothic Neo','Malgun Gothic','맑은 고딕',sans-serif;
    color: #1e293b;
  }}
  .wrap {{ max-width: 760px; margin: 0 auto; padding: 20px 12px; }}

  /* ── 섹션 래퍼 ── */
  .section {{
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 22px 24px;
    margin-bottom: 16px;
  }}

  /* ── 섹션 제목 ── */
  .section-title {{
    font-size: 17px;
    font-weight: 700;
    margin: 0 0 16px 0;
    padding-bottom: 10px;
    display: flex;
    align-items: center;
    gap: 8px;
  }}

  /* ── 서브 제목 ── */
  .sub-title {{
    font-size: 13px;
    font-weight: 700;
    color: #374151;
    margin: 18px 0 8px 0;
    display: flex;
    align-items: center;
    gap: 6px;
  }}
  .sub-title::before {{
    content: '';
    display: inline-block;
    width: 3px;
    height: 14px;
    border-radius: 2px;
  }}
</style>
</head>
<body>
<div class="wrap">

  <!-- ── 헤더 ── -->
  <div style="background:#1e3a5f;border-radius:12px;padding:24px 28px;
    color:#f0f7ff;margin-bottom:16px;">
    <div style="font-size:10px;letter-spacing:3px;color:#93c5fd;margin-bottom:6px;
      text-transform:uppercase;font-weight:600;">
      Innodep · Procurement Intelligence
    </div>
    <div style="font-size:22px;font-weight:700;letter-spacing:-0.5px;color:#f0f7ff;">
      조달청 데이터 수집 리포트
    </div>
    <div style="margin-top:8px;font-size:13px;color:#93c5fd;">
      {display_date}({weekday_str}요일) &nbsp;·&nbsp; 자동 수집 완료
    </div>
  </div>

  <!-- ── 요약 카드 ── -->
  <div class="section">
    <div class="section-title" style="color:#1e3a5f;border-bottom:2px solid #dbeafe;">
      <span style="font-size:16px;">📊</span> 오늘의 수집 요약
    </div>
    {stat_cards}
  </div>

  <!-- ── 종합쇼핑몰 섹션 ── -->
  <div class="section">
    <div class="section-title" style="color:#92400e;border-bottom:2px solid #fde68a;">
      <span style="font-size:16px;">🛒</span> 종합쇼핑몰 3자단가
    </div>

    <!-- 업체별 차트 -->
    <div class="sub-title" style="color:#92400e;">
      <span style="background:#b45309;"></span>
      업체별 납품금액 순위
    </div>
    {vendor_chart}

    <!-- 수요기관별 차트 -->
    <div class="sub-title" style="color:#1e3a5f;margin-top:24px;">
      <span style="background:#1e5fa8;"></span>
      수요기관별 납품금액 순위
    </div>
    {org_chart}

    <!-- 학교 CCTV -->
    <div class="sub-title" style="color:#92400e;margin-top:24px;">
      <span style="background:#b45309;"></span>
      학교 지능형 CCTV 납품 현황
    </div>
    {school_section}

    <!-- 이노뎁 실적 -->
    <div class="sub-title" style="color:#1e3a5f;margin-top:24px;">
      <span style="background:#1e5fa8;"></span>
      이노뎁 납품 실적 (사업명 포함)
    </div>
    {innodep_section}
  </div>

  <!-- ── 나라장터 입찰공고 ── -->
  <div class="section">
    <div class="section-title" style="color:#b91c1c;border-bottom:2px solid #fecaca;">
      <span style="font-size:16px;">📢</span> 나라장터 입찰공고
      <span style="font-size:11px;font-weight:500;color:#6b7280;margin-left:4px;">
        (★이노뎁 관련·핵심 사업 우선 표출)
      </span>
    </div>
    {notice_sections}
  </div>

  <!-- ── 나라장터 계약내역 ── -->
  <div class="section">
    <div class="section-title" style="color:#1e5fa8;border-bottom:2px solid #bfdbfe;">
      <span style="font-size:16px;">📝</span> 나라장터 계약내역
      <span style="font-size:11px;font-weight:500;color:#6b7280;margin-left:4px;">
        (★이노뎁 관련·핵심 사업 우선 표출)
      </span>
    </div>
    {contract_sections}
  </div>

  <!-- ── 푸터 ── -->
  <div style="text-align:center;padding:14px;color:#94a3b8;font-size:11px;">
    본 메일은 GitHub Actions 자동화 스크립트로 발송됩니다.&nbsp;·&nbsp;Innodep Procurement Bot
  </div>

</div>
</body></html>"""


# =================================================================================
# 6. 메인
# =================================================================================

def main():
    if not MY_DIRECT_KEY or not AUTH_JSON_STR:
        print("❌ 환경변수 누락: DATA_GO_KR_API_KEY 또는 GOOGLE_AUTH_JSON")
        return

    target_dt    = get_target_date()
    d_str        = target_dt.strftime("%Y%m%d")
    year         = target_dt.year
    display_date = target_dt.strftime("%Y년 %m월 %d일")
    weekday_str  = ["월","화","수","목","금","토","일"][target_dt.weekday()]

    drive_service, drive_creds = get_drive_service()
    print(f"\n{'='*60}\n🗓  수집 날짜: {display_date}({weekday_str})\n{'='*60}")

    # ─── PART 1: 종합쇼핑몰 ────────────────────────────────────────────
    print("\n[1/3] 종합쇼핑몰 3자단가 수집...")
    raw_shopping: list[list] = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_shopping, kw, d_str): kw for kw in KEYWORDS}
        for future in as_completed(futures):
            data = future.result()
            if data:
                raw_shopping.extend(data)

    school_stats:      dict = {}
    # 이노뎁 실적: org -> {amt, nm} 구조로 변경 (사업명 포함)
    innodep_today:     dict = {}
    innodep_total_amt: int  = 0
    shopping_dedup_cnt: int = 0
    vendor_stats:      dict = {}  # 업체별 집계
    org_stats:         dict = {}  # 수요기관별 집계

    if raw_shopping:
        new_df = pd.DataFrame(raw_shopping, columns=HEADER_KOR)
        stats  = save_shopping_to_drive(drive_service, drive_creds, new_df, year)
        shopping_dedup_cnt = stats['dropped']

        for row in raw_shopping:
            org    = str(row[7])
            comp   = str(row[21])
            cntrct = str(row[23])  # 계약명 (사업명)
            try:
                amt = int(str(row[20]).replace(',', '').split('.')[0])
            except ValueError:
                amt = 0

            # 업체별 집계
            vendor_stats[comp] = vendor_stats.get(comp, 0) + amt
            # 수요기관별 집계
            org_stats[org] = org_stats.get(org, 0) + amt

            # 학교 CCTV
            if '학교' in org and '지능형' in cntrct and 'CCTV' in cntrct:
                school_stats.setdefault(org, {'total_amt': 0, 'main_vendor': comp})
                school_stats[org]['total_amt'] += amt

            # 이노뎁 실적 (사업명 포함)
            if '이노뎁' in comp:
                if org not in innodep_today:
                    innodep_today[org] = {'amt': 0, 'nm': cntrct}
                innodep_today[org]['amt'] += amt
                innodep_total_amt += amt

    print(f"  → 쇼핑몰 {len(raw_shopping):,}건 | 이노뎁 {len(innodep_today)}건 ({innodep_total_amt:,}원)")

    # ─── PART 2: 나라장터 입찰공고 ─────────────────────────────────────
    print("\n[2/3] 나라장터 입찰공고 수집...")
    notice_buckets: dict[str, list] = {cat: [] for cat in CAT_KEYWORDS}
    all_notice_cnt:     int = 0
    notice_dedup_total: int = 0

    for cat_api, api_url in NOTICE_API_MAP.items():
        n_df = fetch_notice(cat_api, api_url, d_str)
        if n_df.empty:
            continue
        all_notice_cnt += len(n_df)
        stats = save_notice_to_drive(drive_service, drive_creds, cat_api, n_df, year)
        notice_dedup_total += stats['dropped']

        if NOTICE_DEDUP_COL in n_df.columns:
            filtered = n_df[n_df['bidNtceNm'].str.contains(ALL_NOTICE_PATTERN, na=False)]
            for _, row in filtered.iterrows():
                cat = classify_text(row['bidNtceNm'])
                if cat in notice_buckets:
                    notice_buckets[cat].append({
                        'org':  row.get('dminsttNm', '-'),
                        'nm':   row.get('bidNtceNm', '-'),
                        'amt':  row.get('presmptPrce', '별도공고'),
                        'url':  row.get('bidNtceDtlUrl', '#'),
                        'corp': '',
                    })

    print(f"  → 공고 전체 {all_notice_cnt:,}건 | 중복 제거 {notice_dedup_total:,}건")

    # ─── PART 3: 나라장터 용역계약 ─────────────────────────────────────
    print("\n[3/3] 나라장터 용역계약 수집...")
    contract_buckets: dict[str, list] = {cat: [] for cat in CAT_KEYWORDS}
    raw_contracts: list[dict] = []

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_contract, kw, d_str): kw for kw in ALL_NOTICE_KEYWORDS}
        for future in as_completed(futures):
            raw_contracts.extend(future.result())

    seen: dict[str, dict] = {}
    for c in raw_contracts:
        key = f"{c['org']}_{c['nm']}"
        seen[key] = c
    unique_contracts = list(seen.values())

    for c in unique_contracts:
        cat = classify_text(c['nm'])
        if cat in contract_buckets:
            contract_buckets[cat].append(c)

    # 국방 카테고리: 학교·민방위·교육청 제외
    for bucket in (notice_buckets, contract_buckets):
        bucket['국방'] = [i for i in bucket['국방'] if not EXCLUDE_ORG_PATTERN.search(i['org'])]

    print(f"  → 계약 {len(raw_contracts):,}건 수집 | 중복 제거 후 {len(unique_contracts):,}건")

    # ─── PART 4: 리포트 출력 ───────────────────────────────────────────
    report_html = build_report_html(
        display_date        = display_date,
        weekday_str         = weekday_str,
        shopping_cnt        = len(raw_shopping),
        notice_cnt          = all_notice_cnt,
        contract_cnt        = len(unique_contracts),
        shopping_dedup      = shopping_dedup_cnt,
        notice_dedup_total  = notice_dedup_total,
        school_stats        = school_stats,
        innodep_dict        = innodep_today,
        innodep_total       = innodep_total_amt,
        notice_buckets      = notice_buckets,
        contract_buckets    = contract_buckets,
        vendor_stats        = vendor_stats,
        org_stats           = org_stats,
    )

    print(f"\n✅ 완료\n{'='*60}")

    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:
            f.write(f"collect_date={d_str}\nfull_report<<EOF\n{report_html}\nEOF\n")


if __name__ == "__main__":
    main()
