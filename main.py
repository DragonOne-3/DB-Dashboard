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

INNODEP_PRIORITY_KEYWORDS = [
    'CCTV', '영상감시', '통합관제', '영상정보처리', '지능형영상',
    '과학화경계', '무인경계', '스마트도시', 'ITS', '교통관제',
    '주차관제', '출입통제', 'VMS', 'NVR', 'DVR'
]
INNODEP_PRIORITY_PATTERN = re.compile('|'.join(map(re.escape, INNODEP_PRIORITY_KEYWORDS)))

COMPETITOR_KEYWORDS = [
    '한화비전', '한화테크윈', '아이디스', '씨앤비텍', '에이치디씨',
    '다후아', '하이크비전', '유니뷰', '보쉬', 'AXIS', 'Axis',
    '인텍플러스', '아이브스', '넥스위', '아이씨엔티', '모아시스',
    '에스원', 'KT텔레캅', 'ADT캡스', '케이티텔레캅',
]
COMPETITOR_PATTERN = re.compile('|'.join(map(re.escape, COMPETITOR_KEYWORDS)))

# 카테고리별 색상·아이콘 메타
CAT_META = {
    '영상감시장치': {
        'icon': '&#128247;', 'accent': '#2d7dd2', 'bg': '#eff6ff',
        'border': '#bfdbfe', 'text': '#1e3a5f', 'badge_bg': '#2d7dd2',
    },
    '국방': {
        'icon': '&#128737;', 'accent': '#e03444', 'bg': '#fef2f2',
        'border': '#fecaca', 'text': '#7f1d1d', 'badge_bg': '#e03444',
    },
    '솔루션': {
        'icon': '&#128161;', 'accent': '#8b5cf6', 'bg': '#f5f3ff',
        'border': '#d4c9f7', 'text': '#4c1d95', 'badge_bg': '#8b5cf6',
    },
    '스마트도시': {
        'icon': '&#127751;', 'accent': '#10b981', 'bg': '#f0fdf4',
        'border': '#a7d9c0', 'text': '#064e3b', 'badge_bg': '#10b981',
    },
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
    text = str(item.get('nm', '')) + str(item.get('org', ''))
    return 1 if INNODEP_PRIORITY_PATTERN.search(text) else 0


def sort_items_by_priority(items: list[dict]) -> list[dict]:
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
# 5. HTML 리포트 생성 (이메일 클라이언트 호환 — JS/CSS 없음, 순수 테이블 레이아웃)
# =================================================================================

def _bar_row(rank: str, label: str, pct: int, amount_str: str,
             bar_color: str, bar_bg: str,
             label_color: str = '#374151', label_bold: bool = False) -> str:
    """JS 없이 table width(%)로 구현한 수평 바 차트 한 행"""
    pct = max(3, min(pct, 100))
    bold_style = 'font-weight:700;' if label_bold else ''
    return (
        f"<tr>"
        f"<td width='14' style='font-size:10px;color:#9ca3af;text-align:right;"
        f"padding:3px 4px;white-space:nowrap;'>{rank}</td>"
        f"<td width='88' style='font-size:11px;color:{label_color};{bold_style}"
        f"padding:3px 6px;white-space:nowrap;overflow:hidden;max-width:88px;'>{label}</td>"
        f"<td style='padding:3px 4px;'>"
        f"<table width='100%' cellpadding='0' cellspacing='0' border='0' "
        f"style='background-color:{bar_bg};border-radius:3px;height:8px;'><tr>"
        f"<td width='{pct}%' height='8' "
        f"style='background-color:{bar_color};border-radius:3px;font-size:0;line-height:0;'>"
        f"&nbsp;</td><td></td></tr></table>"
        f"</td>"
        f"<td width='46' style='font-size:10px;color:#6b7280;text-align:right;"
        f"padding:3px 4px;white-space:nowrap;'>{amount_str}</td>"
        f"</tr>"
    )


def _bar_chart_panel(title: str, subtitle: str,
                     items: list[tuple[str, int]],
                     color_fn,
                     bar_bg: str = '#e8f0fd',
                     legend_html: str = '') -> str:
    """items = [(label, raw_int), ...] 상위 10개 표시"""
    if not items:
        return (
            f"<p style='margin:0 0 6px;font-size:11px;font-weight:700;color:#374151;'>{title}</p>"
            f"<p style='margin:0 0 10px;font-size:10px;color:#9ca3af;'>{subtitle}</p>"
            "<p style='font-size:12px;color:#9ca3af;'>데이터 없음</p>"
        )
    top = items[:10]
    max_val = max(v for _, v in top) or 1
    rows = ""
    for i, (label, val) in enumerate(top):
        pct = int(val / max_val * 100)
        is_innodep = '이노뎁' in label
        is_comp    = COMPETITOR_PATTERN.search(label) is not None
        bar_color, lbl_color, lbl_bold, rank_str = color_fn(label, is_innodep, is_comp)
        rows += _bar_row(
            rank_str or str(i + 1), label, pct,
            fmt_amount(val), bar_color, bar_bg, lbl_color, lbl_bold
        )
    return (
        f"<p style='margin:0 0 4px;font-size:11px;font-weight:700;color:#374151;'>{title}</p>"
        f"<p style='margin:0 0 8px;font-size:10px;color:#9ca3af;'>{subtitle}</p>"
        f"{legend_html}"
        f"<table width='100%' cellpadding='0' cellspacing='2' border='0'>{rows}</table>"
    )


def _legend(colors: list[tuple[str, str]]) -> str:
    cells = "".join(
        f"<td style='padding-right:10px;'>"
        f"<table cellpadding='0' cellspacing='0' border='0'><tr>"
        f"<td width='8' height='8' style='background-color:{c};border-radius:2px;font-size:0;line-height:0;'>&nbsp;</td>"
        f"<td style='padding-left:4px;font-size:10px;color:#6b7280;'>{lbl}</td>"
        f"</tr></table></td>"
        for c, lbl in colors
    )
    return (
        f"<table cellpadding='0' cellspacing='0' border='0' style='margin-bottom:8px;'>"
        f"<tr>{cells}</tr></table>"
    )


def _cat_section_header(cat: str, count: int) -> str:
    meta = CAT_META.get(cat, {
        'icon': '&#128203;', 'accent': '#374151', 'bg': '#f9fafb',
        'border': '#e5e7eb', 'text': '#374151', 'badge_bg': '#374151',
    })
    m_bg     = meta['bg']
    m_accent = meta['accent']
    m_text   = meta['text']
    m_badge  = meta['badge_bg']
    m_icon   = meta['icon']
    return (
        f"<table width='100%' cellpadding='0' cellspacing='0' border='0' "
        f"style='background-color:{m_bg};border-radius:6px;"
        f"border-left:3px solid {m_accent};margin-bottom:10px;'>"
        f"<tr><td style='padding:9px 12px;'>"
        f"<table width='100%' cellpadding='0' cellspacing='0' border='0'><tr>"
        f"<td style='font-size:12px;font-weight:700;color:{m_text};'>"
        f"{m_icon} {cat}</td>"
        f"<td style='text-align:right;'>"
        f"<span style='font-size:10px;font-weight:700;color:#ffffff;"
        f"background-color:{m_badge};padding:2px 10px;border-radius:10px;'>"
        f"{count}건</span></td>"
        f"</tr></table></td></tr></table>"
    )


def _items_table(items: list[dict], cat: str) -> str:
    meta = CAT_META.get(cat, {
        'icon': '&#128203;', 'accent': '#374151', 'bg': '#f9fafb',
        'border': '#e5e7eb', 'text': '#374151', 'badge_bg': '#374151',
    })
    accent = meta['accent']
    bg     = meta['bg']
    border = meta['border']

    if not items:
        return (
            "<table width='100%' cellpadding='0' cellspacing='0' border='0' "
            "style='margin-bottom:16px;'><tr>"
            "<td style='padding:14px;text-align:center;font-size:12px;color:#9ca3af;"
            "background-color:#fafafa;border-radius:6px;border:1px dashed #e5e7eb;'>"
            "해당 내역이 없습니다.</td></tr></table>"
        )

    sorted_items = sort_items_by_priority(items)

    thead = (
        f"<tr style='background-color:{bg};'>"
        f"<th style='padding:7px 8px;font-size:10px;font-weight:700;color:#6b7280;"
        f"text-align:left;width:20%;border-bottom:1px solid {border};'>수요기관</th>"
        f"<th style='padding:7px 8px;font-size:10px;font-weight:700;color:#6b7280;"
        f"text-align:left;border-bottom:1px solid {border};'>사업명</th>"
        f"<th style='padding:7px 8px;font-size:10px;font-weight:700;color:#6b7280;"
        f"text-align:center;width:16%;border-bottom:1px solid {border};'>업체명</th>"
        f"<th style='padding:7px 8px;font-size:10px;font-weight:700;color:#6b7280;"
        f"text-align:right;width:12%;border-bottom:1px solid {border};'>금액</th>"
        f"</tr>"
    )

    tbody = ""
    for i, item in enumerate(sorted_items):
        is_innodep    = '이노뎁' in str(item.get('corp', ''))
        is_priority   = innodep_priority_score(item) == 1
        is_competitor = COMPETITOR_PATTERN.search(str(item.get('corp', ''))) is not None

        row_bg = '#fffbeb' if is_innodep else ('#ffffff' if i % 2 == 0 else '#fafafa')

        if is_innodep:
            corp_color, corp_bold = '#2d7dd2', 'font-weight:700;'
        elif is_competitor:
            corp_color, corp_bold = '#e03444', ''
        else:
            corp_color, corp_bold = '#374151', ''

        badge = ''
        if is_innodep:
            badge = (" <span style='font-size:9px;color:#92400e;background-color:#fef3c7;"
                     "border:1px solid #fde68a;padding:1px 4px;border-radius:3px;'>"
                     "&#9733;이노뎁</span>")
        elif is_priority:
            badge = (f" <span style='font-size:9px;color:{accent};background-color:{bg};"
                     f"border:1px solid {border};padding:1px 4px;border-radius:3px;'>"
                     f"핵심</span>")

        nm  = item.get('nm', '-')
        url = item.get('url', '#')
        nm_html = (
            f"<a href='{url}' target='_blank' "
            f"style='color:{accent};text-decoration:none;'>{nm}</a>{badge}"
            if url and url != '#'
            else f"<span style='color:#374151;'>{nm}</span>{badge}"
        )

        tbody += (
            f"<tr style='background-color:{row_bg};'>"
            f"<td style='padding:7px 8px;font-size:11px;color:#374151;"
            f"border-bottom:1px solid #f3f4f6;'>{item.get('org', '-')}</td>"
            f"<td style='padding:7px 8px;font-size:11px;border-bottom:1px solid #f3f4f6;'>"
            f"{nm_html}</td>"
            f"<td style='padding:7px 8px;font-size:11px;color:{corp_color};{corp_bold}"
            f"text-align:center;border-bottom:1px solid #f3f4f6;'>"
            f"{item.get('corp', '-')}</td>"
            f"<td style='padding:7px 8px;font-size:11px;color:#374151;"
            f"text-align:right;border-bottom:1px solid #f3f4f6;'>"
            f"{fmt_amount(item.get('amt', '0'))}</td>"
            f"</tr>"
        )

    return (
        f"<table width='100%' cellpadding='0' cellspacing='0' border='0' "
        f"style='margin-bottom:16px;'>"
        f"<thead>{thead}</thead><tbody>{tbody}</tbody></table>"
    )


def _cat_block(cat: str, items: list[dict]) -> str:
    return _cat_section_header(cat, len(items)) + _items_table(items, cat)


def build_report_html(
    display_date: str, weekday_str: str,
    shopping_cnt: int, notice_cnt: int, contract_cnt: int,
    shopping_dedup: int, notice_dedup_total: int,
    school_stats: dict, innodep_dict: dict, innodep_total: int,
    notice_buckets: dict, contract_buckets: dict,
    vendor_stats: dict,
    org_stats: dict,
) -> str:

    # ── 요약 카드 4개 ────────────────────────────────────────────────────
    def _card(color: str, label: str, value: str, sub: str = '') -> str:
        sub_html = f"<p style='margin:4px 0 0;font-size:10px;color:#9ca3af;'>{sub}</p>" if sub else ''
        return (
            f"<table width='100%' cellpadding='0' cellspacing='0' border='0' "
            f"style='background-color:#ffffff;border-radius:8px;border-top:3px solid {color};'>"
            f"<tr><td style='padding:16px 14px;'>"
            f"<p style='margin:0 0 8px 0;font-size:9px;color:#9ca3af;font-weight:700;"
            f"letter-spacing:1.5px;text-transform:uppercase;'>{label}</p>"
            f"<p style='margin:0;font-size:20px;font-weight:700;color:{color};'>{value}</p>"
            f"{sub_html}"
            f"</td></tr></table>"
        )

    stat_cards = (
        "<table width='100%' cellpadding='0' cellspacing='0' border='0'><tr>"
        f"<td width='25%' style='padding-right:6px;'>"
        f"{_card('#f59e0b', '쇼핑몰 수집', f'{shopping_cnt:,}건', f'중복 {shopping_dedup:,}건 제거')}"
        f"</td>"
        f"<td width='25%' style='padding-right:6px;padding-left:6px;'>"
        f"{_card('#2d7dd2', '공고 수집', f'{notice_cnt:,}건', f'중복 {notice_dedup_total:,}건 제거')}"
        f"</td>"
        f"<td width='25%' style='padding-right:6px;padding-left:6px;'>"
        f"{_card('#8b5cf6', '계약 수집', f'{contract_cnt:,}건')}"
        f"</td>"
        f"<td width='25%' style='padding-left:6px;'>"
        f"{_card('#10b981', '이노뎁 납품', fmt_amount(innodep_total), f'{len(innodep_dict)}개 기관')}"
        f"</td>"
        "</tr></table>"
    )

    # ── 업체별 바 차트 ────────────────────────────────────────────────────
    vendor_sorted = sorted(vendor_stats.items(), key=lambda x: x[1], reverse=True)

    def vendor_color_fn(lbl, is_innodep, is_comp):
        if is_innodep:
            return '#2d7dd2', '#2d7dd2', True, '★'
        if is_comp:
            return '#e03444', '#e03444', False, None
        return '#94a3b8', '#374151', False, None

    vendor_legend = _legend([('#2d7dd2', '이노뎁'), ('#e03444', '경쟁사'), ('#94a3b8', '기타')])
    vendor_chart  = _bar_chart_panel(
        '경쟁사 납품금액 TOP 10', '어제 기준 · 이노뎁 포함 비교',
        vendor_sorted, vendor_color_fn, bar_bg='#e8edf5', legend_html=vendor_legend
    )

    # ── 수요기관별 바 차트 ─────────────────────────────────────────────────
    org_sorted = sorted(org_stats.items(), key=lambda x: x[1], reverse=True)

    def org_color_fn(lbl, is_innodep, is_comp):
        return '#2d7dd2', '#374151', False, None

    org_chart = _bar_chart_panel(
        '수요기관 납품금액 TOP 10', '어제 기준 · 기관명별 합산',
        org_sorted, org_color_fn, bar_bg='#dce8f5'
    )

    charts_2col = (
        "<table width='100%' cellpadding='0' cellspacing='0' border='0' "
        "style='margin-bottom:12px;'><tr valign='top'>"
        # 왼쪽: 업체별
        "<td width='50%' style='padding-right:6px;'>"
        "<table width='100%' cellpadding='0' cellspacing='0' border='0' "
        "style='background-color:#ffffff;border-radius:8px;border:1px solid #e5e7eb;'>"
        "<tr><td style='padding:14px 16px 10px;border-bottom:1px solid #f3f4f6;'>"
        "<p style='margin:0;font-size:11px;font-weight:700;color:#374151;'>"
        "&#128202; 경쟁사 납품금액 TOP 10</p>"
        "<p style='margin:4px 0 0;font-size:10px;color:#9ca3af;'>어제 기준 · 이노뎁 포함 비교</p>"
        "</td></tr>"
        f"<tr><td style='padding:12px 16px 14px;'>{vendor_chart}</td></tr>"
        "</table></td>"
        # 오른쪽: 수요기관별
        "<td width='50%' style='padding-left:6px;'>"
        "<table width='100%' cellpadding='0' cellspacing='0' border='0' "
        "style='background-color:#ffffff;border-radius:8px;border:1px solid #e5e7eb;'>"
        "<tr><td style='padding:14px 16px 10px;border-bottom:1px solid #f3f4f6;'>"
        "<p style='margin:0;font-size:11px;font-weight:700;color:#374151;'>"
        "&#127963; 수요기관 납품금액 TOP 10</p>"
        "<p style='margin:4px 0 0;font-size:10px;color:#9ca3af;'>어제 기준 · 기관명별 합산</p>"
        "</td></tr>"
        f"<tr><td style='padding:12px 16px 14px;'>{org_chart}</td></tr>"
        "</table></td>"
        "</tr></table>"
    )

    # ── 학교 CCTV 테이블 ───────────────────────────────────────────────────
    if school_stats:
        total_school_amt = sum(s['total_amt'] for s in school_stats.values())
        school_rows = ""
        for i, (sch, info) in enumerate(school_stats.items()):
            row_bg = '#ffffff' if i % 2 == 0 else '#fffdf5'
            school_rows += (
                f"<tr style='background-color:{row_bg};'>"
                f"<td style='padding:7px 10px;font-size:11px;color:#374151;"
                f"border-bottom:1px solid #fef9e7;'>{sch}</td>"
                f"<td style='padding:7px 10px;font-size:11px;color:#374151;"
                f"border-bottom:1px solid #fef9e7;'>{info['main_vendor']}</td>"
                f"<td style='padding:7px 10px;font-size:11px;color:#374151;text-align:right;"
                f"border-bottom:1px solid #fef9e7;'>{info['total_amt']:,}원</td></tr>"
            )
        school_body = (
            "<table width='100%' cellpadding='0' cellspacing='0' border='0'>"
            "<thead><tr style='background-color:#fffbeb;'>"
            "<th style='padding:7px 10px;font-size:10px;font-weight:700;color:#92400e;"
            "text-align:left;border-bottom:1px solid #fde68a;'>학교명</th>"
            "<th style='padding:7px 10px;font-size:10px;font-weight:700;color:#92400e;"
            "text-align:left;border-bottom:1px solid #fde68a;'>납품업체</th>"
            "<th style='padding:7px 10px;font-size:10px;font-weight:700;color:#92400e;"
            "text-align:right;border-bottom:1px solid #fde68a;width:20%;'>납품금액</th>"
            f"</tr></thead><tbody>{school_rows}</tbody>"
            f"<tfoot><tr style='background-color:#fef3c7;'>"
            f"<td colspan='2' style='padding:9px 10px;font-size:12px;font-weight:700;"
            f"color:#92400e;'>합계 ({len(school_stats)}개교)</td>"
            f"<td style='padding:9px 10px;font-size:12px;font-weight:700;color:#92400e;"
            f"text-align:right;'>{total_school_amt:,}원</td>"
            f"</tr></tfoot></table>"
        )
    else:
        school_body = ("<p style='color:#9ca3af;font-size:13px;"
                       "padding:14px 16px;'>해당 내역 없음</p>")

    # ── 이노뎁 실적 테이블 ─────────────────────────────────────────────────
    if innodep_dict:
        innodep_rows = ""
        for i, (org, info) in enumerate(innodep_dict.items()):
            row_bg = '#ffffff' if i % 2 == 0 else '#f8faff'
            innodep_rows += (
                f"<tr style='background-color:{row_bg};'>"
                f"<td style='padding:7px 10px;font-size:11px;color:#374151;"
                f"border-bottom:1px solid #f0f4ff;'>{org}</td>"
                f"<td style='padding:7px 10px;font-size:11px;color:#2d7dd2;"
                f"border-bottom:1px solid #f0f4ff;'>{info.get('nm', '-')}</td>"
                f"<td style='padding:7px 10px;font-size:11px;color:#374151;text-align:right;"
                f"border-bottom:1px solid #f0f4ff;'>{info['amt']:,}원</td></tr>"
            )
        innodep_body = (
            "<table width='100%' cellpadding='0' cellspacing='0' border='0'>"
            "<thead><tr style='background-color:#eff6ff;'>"
            "<th style='padding:7px 10px;font-size:10px;font-weight:700;color:#1e3a5f;"
            "text-align:left;border-bottom:1px solid #bfdbfe;'>수요기관</th>"
            "<th style='padding:7px 10px;font-size:10px;font-weight:700;color:#1e3a5f;"
            "text-align:left;border-bottom:1px solid #bfdbfe;'>사업명</th>"
            "<th style='padding:7px 10px;font-size:10px;font-weight:700;color:#1e3a5f;"
            "text-align:right;border-bottom:1px solid #bfdbfe;width:22%;'>납품금액</th>"
            f"</tr></thead><tbody>{innodep_rows}</tbody>"
            f"<tfoot><tr style='background-color:#dbeafe;'>"
            f"<td colspan='2' style='padding:9px 10px;font-size:12px;font-weight:700;"
            f"color:#1e3a5f;'>합계 ({len(innodep_dict)}건)</td>"
            f"<td style='padding:9px 10px;font-size:12px;font-weight:700;color:#1e3a5f;"
            f"text-align:right;'>{innodep_total:,}원</td>"
            f"</tr></tfoot></table>"
        )
    else:
        innodep_body = ("<p style='color:#9ca3af;font-size:13px;"
                        "padding:14px 16px;'>해당 내역 없음</p>")

    school_innodep_2col = (
        "<table width='100%' cellpadding='0' cellspacing='0' border='0' "
        "style='margin-bottom:12px;'><tr valign='top'>"
        # 학교
        "<td width='50%' style='padding-right:6px;'>"
        "<table width='100%' cellpadding='0' cellspacing='0' border='0' "
        "style='background-color:#ffffff;border-radius:8px;border:1px solid #e5e7eb;'>"
        "<tr><td style='padding:12px 16px;background-color:#fffbeb;border-bottom:2px solid #fde68a;'>"
        "<p style='margin:0;font-size:11px;font-weight:700;color:#92400e;'>"
        "&#127979; 학교 지능형 CCTV 납품현황</p></td></tr>"
        f"<tr><td style='padding:0;'>{school_body}</td></tr>"
        "</table></td>"
        # 이노뎁
        "<td width='50%' style='padding-left:6px;'>"
        "<table width='100%' cellpadding='0' cellspacing='0' border='0' "
        "style='background-color:#ffffff;border-radius:8px;border:1px solid #e5e7eb;'>"
        "<tr><td style='padding:12px 16px;background-color:#eff6ff;border-bottom:2px solid #bfdbfe;'>"
        "<p style='margin:0;font-size:11px;font-weight:700;color:#1e3a5f;'>"
        "&#11088; 이노뎁 납품 실적</p></td></tr>"
        f"<tr><td style='padding:0;'>{innodep_body}</td></tr>"
        "</table></td>"
        "</tr></table>"
    )

    # ── 카테고리별 공고·계약 블록 ─────────────────────────────────────────
    notice_blocks   = "".join(_cat_block(cat, notice_buckets[cat])   for cat in CAT_KEYWORDS)
    contract_blocks = "".join(_cat_block(cat, contract_buckets[cat]) for cat in CAT_KEYWORDS)

    # ── 최종 HTML ─────────────────────────────────────────────────────────
    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<title>Innodep 조달청 데이터 수집 리포트</title>
</head>
<body style="margin:0;padding:0;background-color:#f0f4f8;
  font-family:'맑은 고딕','Apple SD Gothic Neo',Arial,sans-serif;
  -webkit-text-size-adjust:100%;mso-line-height-rule:exactly;">

<table width="100%" cellpadding="0" cellspacing="0" border="0"
  style="background-color:#f0f4f8;">
<tr><td align="center" style="padding:24px 12px;">

  <table width="680" cellpadding="0" cellspacing="0" border="0"
    style="max-width:680px;width:100%;">

    <!-- ══ HEADER ══ -->
    <tr><td style="padding-bottom:12px;">
      <table width="100%" cellpadding="0" cellspacing="0" border="0"
             style="background-color:#1e3a5f;border-radius:10px;overflow:hidden;">
        <tr>
          <td colspan="2" height="3"
            style="background-color:#2d7dd2;font-size:0;line-height:0;">&nbsp;</td>
        </tr>
        <tr>
          <td style="padding:20px 24px 18px;">
            <p style="margin:0 0 4px 0;font-size:10px;letter-spacing:3px;color:#7eb8f7;
              font-weight:700;text-transform:uppercase;">
              Innodep &middot; Procurement Intelligence
            </p>
            <p style="margin:0;font-size:20px;font-weight:700;color:#f0f7ff;
              letter-spacing:-0.5px;">
              조달청 데이터 수집 리포트
            </p>
          </td>
          <td style="padding:20px 24px 18px;text-align:right;vertical-align:middle;">
            <p style="margin:0 0 2px 0;font-size:10px;color:#5a87b8;letter-spacing:1px;">
              기준일 (어제)
            </p>
            <p style="margin:0 0 4px 0;font-size:15px;font-weight:700;color:#7eb8f7;">
              {display_date} ({weekday_str})
            </p>
            <p style="margin:0;font-size:10px;color:#4a9d6e;font-weight:700;
              letter-spacing:1px;">&#9679; AUTO COLLECTED</p>
          </td>
        </tr>
      </table>
    </td></tr>

    <!-- ══ 요약 카드 ══ -->
    <tr><td style="padding-bottom:12px;">
      {stat_cards}
    </td></tr>

    <!-- ══ 쇼핑몰 섹션 헤더 ══ -->
    <tr><td style="padding-bottom:8px;">
      <table width="100%" cellpadding="0" cellspacing="0" border="0"
             style="background-color:#fffbeb;border-radius:8px;
               border-left:4px solid #f59e0b;">
        <tr><td style="padding:12px 16px;">
          <p style="margin:0;font-size:14px;font-weight:700;color:#92400e;">
            &#128722; 종합쇼핑몰 3자단가
            <span style="font-size:11px;font-weight:400;color:#b45309;">
              &nbsp;— 어제 기준 일매출 집계
            </span>
          </p>
        </td></tr>
      </table>
    </td></tr>

    <!-- ══ 업체·수요기관 차트 (2열) ══ -->
    <tr><td style="padding-bottom:0;">
      {charts_2col}
    </td></tr>

    <!-- ══ 학교 CCTV + 이노뎁 실적 (2열) ══ -->
    <tr><td style="padding-bottom:12px;">
      {school_innodep_2col}
    </td></tr>

    <!-- ══ 입찰공고 헤더 ══ -->
    <tr><td style="padding-bottom:8px;">
      <table width="100%" cellpadding="0" cellspacing="0" border="0"
             style="background-color:#fef2f2;border-radius:8px;
               border-left:4px solid #e03444;">
        <tr><td style="padding:12px 16px;">
          <p style="margin:0;font-size:14px;font-weight:700;color:#991b1b;">
            &#128226; 나라장터 입찰공고
            <span style="font-size:11px;font-weight:400;color:#b91c1c;">
              &nbsp;— &#9733;이노뎁 관련 &middot; 핵심 사업 우선 표출
            </span>
          </p>
        </td></tr>
      </table>
    </td></tr>

    <!-- ══ 입찰공고 카드 ══ -->
    <tr><td style="padding-bottom:12px;">
      <table width="100%" cellpadding="0" cellspacing="0" border="0"
             style="background-color:#ffffff;border-radius:8px;
               border:1px solid #e5e7eb;">
        <tr><td style="padding:16px 16px 8px;">
          {notice_blocks}
        </td></tr>
      </table>
    </td></tr>

    <!-- ══ 계약내역 헤더 ══ -->
    <tr><td style="padding-bottom:8px;">
      <table width="100%" cellpadding="0" cellspacing="0" border="0"
             style="background-color:#eff6ff;border-radius:8px;
               border-left:4px solid #2d7dd2;">
        <tr><td style="padding:12px 16px;">
          <p style="margin:0;font-size:14px;font-weight:700;color:#1e3a5f;">
            &#128221; 나라장터 계약내역
            <span style="font-size:11px;font-weight:400;color:#2d7dd2;">
              &nbsp;— &#9733;이노뎁 관련 &middot; 핵심 사업 우선 표출
            </span>
          </p>
        </td></tr>
      </table>
    </td></tr>

    <!-- ══ 계약내역 카드 ══ -->
    <tr><td style="padding-bottom:12px;">
      <table width="100%" cellpadding="0" cellspacing="0" border="0"
             style="background-color:#ffffff;border-radius:8px;
               border:1px solid #e5e7eb;">
        <tr><td style="padding:16px 16px 8px;">
          {contract_blocks}
        </td></tr>
      </table>
    </td></tr>

    <!-- ══ FOOTER ══ -->
    <tr><td style="padding-top:8px;padding-bottom:8px;text-align:center;">
      <p style="margin:0;font-size:10px;color:#9ca3af;letter-spacing:0.5px;">
        본 메일은 GitHub Actions 자동화 스크립트로 발송됩니다
        &nbsp;&middot;&nbsp; Innodep Procurement Bot
      </p>
    </td></tr>

  </table>

</td></tr>
</table>
</body>
</html>"""


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

    school_stats:       dict = {}
    innodep_today:      dict = {}
    innodep_total_amt:  int  = 0
    shopping_dedup_cnt: int  = 0
    vendor_stats:       dict = {}
    org_stats:          dict = {}

    if raw_shopping:
        new_df = pd.DataFrame(raw_shopping, columns=HEADER_KOR)
        stats  = save_shopping_to_drive(drive_service, drive_creds, new_df, year)
        shopping_dedup_cnt = stats['dropped']

        for row in raw_shopping:
            org    = str(row[7])
            comp   = str(row[21])
            cntrct = str(row[23])
            try:
                amt = int(str(row[20]).replace(',', '').split('.')[0])
            except ValueError:
                amt = 0

            vendor_stats[comp] = vendor_stats.get(comp, 0) + amt
            org_stats[org]     = org_stats.get(org, 0) + amt

            if '학교' in org and '지능형' in cntrct and 'CCTV' in cntrct:
                school_stats.setdefault(org, {'total_amt': 0, 'main_vendor': comp})
                school_stats[org]['total_amt'] += amt

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
