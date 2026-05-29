"""
send_b.py  ─  필터링 리포트 발송 스크립트 (드라이브 데이터 재활용)
  • 쇼핑몰 데이터 : 구글 드라이브 {연도}.csv → 날짜 필터링
  • 공고 데이터   : 구글 드라이브 나라장터_공고_{카테고리}_{연도}년.csv → bidNtceDt 필터링
  • 계약 데이터   : API 재호출 (드라이브 미저장)
  • 학교 지능형 CCTV 섹션 제거
  • 국방 카테고리 제거
  • 수집 데이터 0건이면 sys.exit(1) → 메일 스텝 자동 스킵
  • 월요일 실행 시 금·토·일 3일치 합산
  • 경쟁사 목록: 스크립트와 같은 디렉토리의 companies.txt에서 로드
"""

import os
import sys
import io
import json
import time
import datetime
import requests
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account

# ── 환경변수 ───────────────────────────────────────────────────────────────────
MY_DIRECT_KEY = os.environ.get("DATA_GO_KR_API_KEY")
AUTH_JSON_STR  = os.environ.get("GOOGLE_AUTH_JSON")

SHOPPING_FOLDER_ID = "1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr"
NOTICE_FOLDER_ID   = "1AsvVmayEmTtY92d1SfXxNi6bL0Zjw5mg"

# ── 컬럼 정의 ──────────────────────────────────────────────────────────────────
HEADER_KOR = [
    "조달구분명", "계약구분명", "계약납품구분명", "계약납품요구일자", "계약납품요구번호",
    "변경차수", "최종변경차수여부", "수요기관명", "수요기관구분명", "수요기관지역명",
    "수요기관코드", "물품분류번호", "품명", "세부물품분류번호", "세부품명",
    "물품식별번호", "물품규격명", "단가", "수량", "단위", "금액", "업체명",
    "업체기업구분명", "계약명", "우수제품여부", "공사용자재직접구매대상여부",
    "다수공급자계약여부", "다수공급자계약2단계진행여부", "단가계약번호", "단가계약변경차수",
    "최초계약(납품요구)일자", "계약체결방법명", "증감수량", "증감금액", "납품장소명",
    "납품기한일자", "업체사업자등록번호", "인도조건명", "물품순번"
]

# ── 카테고리 (국방 포함 정의 — 수집/분류용. 메일은 MAIL_CATEGORIES만) ─────────
CAT_KEYWORDS = {
    "영상감시장치": ["CCTV", "통합관제", "영상감시장치", "영상정보처리기기"],
    "국방": ["국방", "부대", "작전", "경계", "방위", "군사", "무인화", "사령부", "군대", "중요시설", "주둔지", "과학화", "육군", "해군", "공군", "해병"],
    "솔루션": ["데이터", "플랫폼", "솔루션", "주차", "출입", "GIS"],
    "스마트도시": ["ITS", "스마트시티", "스마트도시", "K-AI", "AI시티"],
    "드론": ["드론", "무인기", "UAV", "UAS", "무인항공", "드론관제", "드론감시", "드론탐지"],
}
MAIL_CATEGORIES = ["영상감시장치", "솔루션", "스마트도시", "드론"]  # 국방 제외

CAT_META = {
    "영상감시장치": {"icon": "&#128247;", "accent": "#2563eb", "bg": "#eff6ff", "border": "#bfdbfe", "text": "#1e40af", "badge_bg": "#2563eb", "header_bg": "#dbeafe"},
    "솔루션":       {"icon": "&#128161;", "accent": "#7c3aed", "bg": "#f5f3ff", "border": "#ddd6fe", "text": "#5b21b6", "badge_bg": "#7c3aed", "header_bg": "#ede9fe"},
    "스마트도시":   {"icon": "&#127751;", "accent": "#059669", "bg": "#ecfdf5", "border": "#a7f3d0", "text": "#065f46", "badge_bg": "#059669", "header_bg": "#d1fae5"},
    "드론":         {"icon": "&#128641;", "accent": "#0284c7", "bg": "#f0f9ff", "border": "#bae6fd", "text": "#0c4a6e", "badge_bg": "#0284c7", "header_bg": "#e0f2fe"},
}

NOTICE_CATEGORY_NAMES = ["공사", "물품", "용역"]

keywords_notice_all = [kw for sublist in CAT_KEYWORDS.values() for kw in sublist]

keywords = sorted(list(set([
    "네트워크시스템장비용랙", "영상감시장치", "PA용스피커", "안내판", "카메라브래킷", "액정모니터",
    "광송수신모듈", "전원공급장치", "광분배함", "컨버터", "컴퓨터서버", "하드디스크드라이브",
    "네트워크스위치", "광점퍼코드", "풀박스", "서지흡수기", "디지털비디오레코더",
    "스피커", "오디오앰프", "브래킷", "UTP케이블", "정보통신공사", "영상정보디스플레이장치",
    "송신기", "난연전력케이블", "1종금속제가요전선관", "호온스피커", "누전차단기", "방송수신기",
    "LAP외피광케이블", "폴리에틸렌전선관", "리모트앰프", "랙캐비닛용패널", "베어본컴퓨터",
    "분배기", "결선보드유닛", "벨", "난연접지용비닐절연전선", "경광등", "데스크톱컴퓨터",
    "특수목적컴퓨터", "철근콘크리트공사", "토공사", "안내전광판", "접지봉", "카메라회전대",
    "무선랜액세스포인트", "컴퓨터망전환장치", "포장공사", "고주파동축케이블", "카메라하우징",
    "인터폰", "스위칭모드전원공급장치", "금속상자", "열선감지기", "태양전지조절기",
    "밀폐고정형납축전지", "IP전화기", "디스크어레이", "그래픽용어댑터", "인터콤장비",
    "기억유닛", "컴퓨터지문인식장치", "랜접속카드", "접지판", "제어케이블", "비디오네트워킹장비",
    "레이스웨이", "콘솔익스텐더", "전자카드", "비대면방역감지장비", "온습도트랜스미터",
    "도난방지기", "융복합영상감시장치", "멀티스크린컴퓨터", "컴퓨터정맥인식장치",
    "카메라컨트롤러", "SSD저장장치", "원격단말장치(RTU)", "융복합네트워크스위치",
    "융복합액정모니터", "융복합데스크톱컴퓨터", "융복합그래픽용어댑터", "융복합베어본컴퓨터",
    "융복합서지흡수기", "배선장치", "융복합배선장치", "융복합카메라브래킷",
    "융복합네트워크시스템장비용랙", "융복합UTP케이블", "테이프백업장치", "자기식테이프",
    "레이드저장장치", "광송수신기", "450/750V 유연성단심비닐절연전선", "솔내시스템",
    "450/750V유연성단심비닐절연전선", "카메라받침대", "텔레비전거치대", "광수신기",
    "무선통신장치", "동작분석기", "전력공급장치", "450/750V 일반용유연성단심비닐절연전선",
    "분전함", "비디오믹서", "절연전선및피복선", "레이더", "적외선방사기", "보안용카메라",
    "통신소프트웨어", "분석및과학용소프트웨어", "소프트웨어유지및지원서비스",
    "교통관제시스템", "산업관리소프트웨어", "시스템관리소프트웨어", "적외선카메라",
    "주차경보등", "주차관제주변기기", "주차권판독기", "주차안내판", "주차요금계산기",
    "주차주제어장치", "차량감지기", "차량인식기", "차량차단기",
    "패키지소프트웨어개발및도입서비스", "무선인식리더기", "바코드시스템", "출입통제시스템", "카드인쇄기"
])))


# =============================================================================
# 유틸리티
# =============================================================================

def get_kst_now():
    return datetime.datetime.now(datetime.UTC) + datetime.timedelta(hours=9)


def get_date_range():
    """
    KST 기준 요일에 따라 수집 날짜 목록 반환
      월요일 → 금·토·일 3일치
      화~금  → 전날 1일치
    반환: (date_list, is_weekly)
    """
    now = get_kst_now()
    weekday = now.weekday()  # 0=월 … 6=일
    if weekday == 0:
        dates = [
            (now - datetime.timedelta(days=3)).strftime("%Y%m%d"),  # 금
            (now - datetime.timedelta(days=2)).strftime("%Y%m%d"),  # 토
            (now - datetime.timedelta(days=1)).strftime("%Y%m%d"),  # 일
        ]
        return dates, True
    else:
        return [(now - datetime.timedelta(days=1)).strftime("%Y%m%d")], False


def fmt_amount(val):
    """
    금액 포맷 — 천단위 쉼표 포함 전체 표시
      1억 이상  → X억 X,XXX만원
      1만 이상  → X,XXX만원
      1만 미만  → X,XXX원
    별도공고/0/None → "별도공고"
    """
    try:
        n = int(str(val).replace(",", "").split(".")[0])
        if n == 0:
            return "별도공고"
        if n >= 100_000_000:
            eok  = n // 100_000_000
            man  = (n % 100_000_000) // 10_000
            if man:
                return f"{eok:,}억 {man:,}만원"
            return f"{eok:,}억원"
        if n >= 10_000:
            return f"{n // 10_000:,}만원"
        return f"{n:,}원"
    except Exception:
        return str(val) if val else "별도공고"


def classify_text(text):
    for cat, kws in CAT_KEYWORDS.items():
        if any(kw in str(text) for kw in kws):
            return cat
    return "기타"


# ── 경쟁사 목록 (main.py와 동일한 방식) ──────────────────────────────────────

def get_target_companies():
    """
    스크립트와 같은 디렉토리의 companies.txt에서 경쟁사 목록을 읽습니다.
    파일이 없으면 기본값(이노뎁)을 반환합니다.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "companies.txt")
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            companies = [line.strip() for line in f if line.strip()]
        print(f"📋 경쟁사 목록 로드: {len(companies)}개 ({file_path})")
        return companies
    print("⚠️ companies.txt 없음 — 기본값(이노뎁) 사용")
    return ["이노뎁(주)", "이노뎁"]


def normalize_company_name(name):
    """main.py와 동일한 정규화 함수"""
    return str(name).replace(" ", "").replace("(주)", "").replace("주식회사", "").upper()


# =============================================================================
# 구글 드라이브 헬퍼
# =============================================================================

def get_drive_service():
    info = json.loads(AUTH_JSON_STR)
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds), creds


def download_csv_from_drive(drive_service, creds, file_id) -> pd.DataFrame:
    resp = requests.get(
        f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media",
        headers={"Authorization": f"Bearer {creds.token}"},
    )
    resp.raise_for_status()
    return pd.read_csv(io.BytesIO(resp.content), encoding="utf-8-sig", low_memory=False)


def find_file_id(drive_service, folder_id, file_name):
    res = drive_service.files().list(
        q=f"name='{file_name}' and '{folder_id}' in parents and trashed=false",
        fields="files(id, name)",
    ).execute()
    items = res.get("files", [])
    return items[0]["id"] if items else None


# =============================================================================
# 데이터 수집
# =============================================================================

def load_shopping_from_drive(drive_service, creds, date_list):
    years    = sorted(set(d[:4] for d in date_list))
    all_rows = []
    for year in years:
        file_name = f"{year}.csv"
        file_id   = find_file_id(drive_service, SHOPPING_FOLDER_ID, file_name)
        if not file_id:
            print(f"⚠️ 쇼핑몰 파일 없음: {file_name}")
            continue
        df = download_csv_from_drive(drive_service, creds, file_id)
        print(f"📥 쇼핑몰 {file_name}: {len(df):,}행 로드")
        date_col = "계약납품요구일자"
        if date_col not in df.columns:
            print(f"⚠️ '{date_col}' 컬럼 없음 — 전체 사용")
            all_rows.extend(df.values.tolist())
            continue
        df[date_col] = df[date_col].astype(str).str[:8]
        filtered = df[df[date_col].isin(date_list)]
        print(f"   └ 날짜 필터 후: {len(filtered):,}행")
        all_rows.extend(filtered.values.tolist())
    return all_rows


def load_notice_from_drive(drive_service, creds, date_list):
    years    = sorted(set(d[:4] for d in date_list))
    date_set = {f"{d[:4]}-{d[4:6]}-{d[6:]}" for d in date_list}
    notice_buckets = {cat: [] for cat in MAIL_CATEGORIES}
    total_cnt = 0
    for year in years:
        for cat_name in NOTICE_CATEGORY_NAMES:
            file_name = f"나라장터_공고_{cat_name}_{year}년.csv"
            file_id   = find_file_id(drive_service, NOTICE_FOLDER_ID, file_name)
            if not file_id:
                print(f"⚠️ 공고 파일 없음: {file_name}")
                continue
            df = download_csv_from_drive(drive_service, creds, file_id)
            print(f"📥 공고 {file_name}: {len(df):,}행 로드")
            if "bidNtceDt" not in df.columns:
                print(f"   └ bidNtceDt 컬럼 없음 — 스킵")
                continue
            # "2026-05-28  11:31:23 PM" → 앞 10자리만 사용
            df["_date"] = df["bidNtceDt"].astype(str).str[:10]
            filtered  = df[df["_date"].isin(date_set)]
            print(f"   └ 날짜 필터 후: {len(filtered):,}행")
            total_cnt += len(filtered)
            pattern = "|".join(keywords_notice_all)
            matched = filtered[filtered["bidNtceNm"].str.contains(pattern, na=False, case=False)]
            for _, row in matched.iterrows():
                cat_found = classify_text(row["bidNtceNm"])
                if cat_found in notice_buckets:
                    notice_buckets[cat_found].append({
                        "org":  row.get("dminsttNm", "-"),
                        "nm":   row.get("bidNtceNm", "-"),
                        "amt":  row.get("presmptPrce", "0"),
                        "url":  row.get("bidNtceDtlUrl", "#"),
                        "corp": "-",
                        "date": row.get("_date", ""),
                    })
    return notice_buckets, total_cnt


def fetch_single_contract(kw_s, d_str):
    api_url = "http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch"
    results = []
    p = {
        "serviceKey": MY_DIRECT_KEY, "inqryDiv": "1", "type": "xml",
        "inqryBgnDate": d_str, "inqryEndDate": d_str, "cntrctNm": kw_s,
    }
    try:
        r = requests.get(api_url, params=p, timeout=20)
        if r.status_code == 200:
            root = ET.fromstring(r.content)
            for item in root.findall(".//item"):
                detail_url   = item.findtext("cntrctDtlInfoUrl") or "https://www.g2b.go.kr"
                raw_demand   = item.findtext("dminsttList", "-")
                clean_demand = raw_demand.replace("[", "").replace("]", "").split("^")[2] if "^" in raw_demand else raw_demand
                raw_corp     = item.findtext("corpList", "-")
                clean_corp   = raw_corp.replace("[", "").replace("]", "").split("^")[3] if "^" in raw_corp else raw_corp
                results.append({
                    "org":  clean_demand,
                    "nm":   item.findtext("cntrctNm", "-"),
                    "corp": clean_corp,
                    "amt":  item.findtext("totCntrctAmt", "0"),
                    "url":  detail_url,
                })
    except Exception as e:
        print(f"계약 API 오류 ({kw_s}): {e}")
    return results


def load_contracts_from_api(date_list):
    contract_buckets  = {cat: [] for cat in MAIL_CATEGORIES}
    exclude_keywords  = ["학교", "민방위", "교육청"]
    for d_str in date_list:
        collected = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(fetch_single_contract, kw_s, d_str): kw_s for kw_s in keywords_notice_all}
            for future in as_completed(futures):
                collected.extend(future.result())
        unique = list({f"{d['org']}_{d['nm']}": d for d in collected}.values())
        for s in unique:
            cat_found = classify_text(s["nm"])
            if cat_found not in contract_buckets:
                continue
            if any(kw in s["org"] for kw in exclude_keywords):
                continue
            contract_buckets[cat_found].append({**s, "date": d_str})
    return contract_buckets


# =============================================================================
# 통계 계산 — companies.txt 기반 경쟁사 필터 (main.py와 동일한 방식)
# =============================================================================

def calc_stats(all_shopping, companies=None):
    """
    companies 리스트가 있으면 정규화 비교 OR 이노뎁 직접 포함으로 vendor_stats 필터링.
    없으면 이노뎁만 포함 (기본 동작).
    main.py의 normalize_company_name + normalized_target_companies 방식과 동일.
    """
    vendor_stats        = {}
    org_stats           = {}
    innodep_org_summary = {}
    innodep_total_amt   = 0

    # 경쟁사 집합 정규화 (main.py와 동일)
    normalized_target = {normalize_company_name(c) for c in companies} if companies else set()

    for row in all_shopping:
        try:
            org     = str(row[7])
            comp    = str(row[21])
            amt_val = str(row[33])
            amt     = int(amt_val.replace(",", "").split(".")[0])
        except Exception:
            continue

        org_stats[org] = org_stats.get(org, 0) + amt

        # main.py와 동일 조건: 정규화 일치 OR 이노뎁 직접 포함
        normalized_comp = normalize_company_name(comp)
        if normalized_comp in normalized_target or "이노뎁" in comp:
            vendor_stats[comp] = vendor_stats.get(comp, 0) + amt

        if "이노뎁" in comp:
            innodep_org_summary[org] = innodep_org_summary.get(org, 0) + amt
            innodep_total_amt       += amt

    return vendor_stats, org_stats, innodep_org_summary, innodep_total_amt


# =============================================================================
# HTML 빌더 — 리디자인 버전
# =============================================================================

# ── 공통 스타일 상수 ──────────────────────────────────────────────────────────
_TH = "padding:8px 10px;font-size:11px;font-weight:700;letter-spacing:0.4px;color:#64748b;text-align:{align};border-bottom:2px solid #e2e8f0;background-color:#f8fafc;white-space:nowrap;"
_TD = "padding:8px 10px;font-size:13px;color:{color};border-bottom:1px solid #f1f5f9;{extra}"


def _th(label, align="left", width=""):
    w = f"width:{width};" if width else ""
    return f"<th style='{_TH.format(align=align)}{w}'>{label}</th>"


def _td(content, color="#1e293b", extra="", tag="td"):
    return f"<{tag} style='{_TD.format(color=color, extra=extra)}'>{content}</{tag}>"


def build_ranking_table(data, col_label, accent_color):
    """범용 순위 테이블 — (이름, 금액) 쌍 리스트"""
    if not data:
        return _empty_state("데이터 없음")
    rows = ""
    for i, (label, val) in enumerate(data):
        is_top   = i == 0
        is_i     = "이노뎁" in label
        rank_bg  = accent_color if is_top else ("#fff7ed" if is_i else "transparent")
        rank_txt = "★" if is_i else str(i + 1)
        rank_col = "#ffffff" if is_top else (accent_color if is_i else "#94a3b8")
        name_col = accent_color if is_i else ("#1e293b" if not is_top else "#1e293b")
        name_w   = "font-weight:700;" if is_i else ("font-weight:600;" if is_top else "")
        row_bg   = "#fafffe" if is_i else ("#fafafa" if i % 2 else "#ffffff")
        amt_col  = accent_color if is_i else "#374151"
        amt_w    = "font-weight:700;" if is_i else ""

        rows += (
            f"<tr style='background-color:{row_bg};'>"
            f"<td style='padding:9px 10px;width:36px;text-align:center;border-bottom:1px solid #f1f5f9;'>"
            f"<span style='display:inline-block;width:22px;height:22px;line-height:22px;border-radius:50%;"
            f"background-color:{rank_bg};color:{rank_col};font-size:11px;font-weight:700;text-align:center;'>{rank_txt}</span>"
            f"</td>"
            f"<td style='padding:9px 10px;font-size:13px;color:{name_col};{name_w}border-bottom:1px solid #f1f5f9;'>{label}</td>"
            f"<td style='padding:9px 10px;font-size:13px;color:{amt_col};{amt_w}text-align:right;border-bottom:1px solid #f1f5f9;white-space:nowrap;'>{fmt_amount(val)}</td>"
            f"</tr>"
        )
    return (
        f"<table width='100%' cellpadding='0' cellspacing='0' border='0'>"
        f"<tr style='background-color:#f8fafc;'>"
        f"{_th('', 'center', '36px')}"
        f"{_th(col_label)}"
        f"{_th('금액', 'right', '110px')}"
        f"</tr>{rows}</table>"
    )


def _empty_state(msg="해당 내역이 없습니다."):
    return (
        f"<table width='100%' cellpadding='0' cellspacing='0' border='0'>"
        f"<tr><td style='padding:20px;text-align:center;font-size:13px;color:#94a3b8;"
        f"background-color:#f8fafc;border-radius:6px;border:1px dashed #e2e8f0;'>{msg}</td></tr></table>"
    )


def build_vendor_chart(vendor_stats):
    top = sorted(vendor_stats.items(), key=lambda x: x[1], reverse=True)[:10]
    return build_ranking_table(top, "업체명", "#2563eb")


def build_org_chart(org_stats):
    top = sorted(org_stats.items(), key=lambda x: x[1], reverse=True)[:10]
    return build_ranking_table(top, "기관명", "#0284c7")


def build_innodep_table(innodep_org_summary, innodep_total_amt):
    if not innodep_org_summary:
        return _empty_state("납품 실적 없음")
    rows = ""
    sorted_items = sorted(innodep_org_summary.items(), key=lambda x: x[1], reverse=True)
    for i, (org, amt) in enumerate(sorted_items):
        bg = "#ffffff" if i % 2 == 0 else "#f8faff"
        rows += (
            f"<tr style='background-color:{bg};'>"
            f"<td style='padding:9px 12px;font-size:13px;color:#1e293b;border-bottom:1px solid #f1f5f9;'>{org}</td>"
            f"<td style='padding:9px 12px;font-size:13px;color:#1e40af;font-weight:600;text-align:right;"
            f"border-bottom:1px solid #f1f5f9;white-space:nowrap;'>{fmt_amount(amt)}</td>"
            f"</tr>"
        )
    total_row = (
        f"<tr style='background-color:#dbeafe;'>"
        f"<td style='padding:10px 12px;font-size:13px;font-weight:700;color:#1e40af;'>"
        f"합계 <span style='font-size:11px;font-weight:400;color:#3b82f6;'>({len(innodep_org_summary)}개 기관)</span></td>"
        f"<td style='padding:10px 12px;font-size:14px;font-weight:700;color:#1d4ed8;text-align:right;white-space:nowrap;'>{fmt_amount(innodep_total_amt)}</td>"
        f"</tr>"
    )
    return (
        f"<table width='100%' cellpadding='0' cellspacing='0' border='0'>"
        f"<tr style='background-color:#eff6ff;'>"
        f"{_th('수요기관')}{_th('금액', 'right', '120px')}"
        f"</tr>{rows}{total_row}</table>"
    )


def build_category_section(cat, items):
    meta = CAT_META.get(cat, {
        "icon": "&#128203;", "accent": "#475569", "bg": "#f8fafc",
        "border": "#e2e8f0", "text": "#334155", "badge_bg": "#475569", "header_bg": "#f1f5f9",
    })

    total_amt = 0
    for item in items:
        try:
            total_amt += int(str(item.get("amt", 0)).replace(",", "").split(".")[0])
        except Exception:
            pass
    amt_summary = f" &nbsp;<span style='font-size:11px;font-weight:400;color:{meta['text']};opacity:0.75;'>합계 {fmt_amount(total_amt)}</span>" if total_amt else ""

    # 섹션 헤더
    header = (
        f"<table width='100%' cellpadding='0' cellspacing='0' border='0' style='"
        f"background-color:{meta['header_bg']};border-radius:8px 8px 0 0;"
        f"border-left:4px solid {meta['accent']};margin-bottom:0;'>"
        f"<tr><td style='padding:10px 14px;'>"
        f"<table width='100%' cellpadding='0' cellspacing='0' border='0'><tr>"
        f"<td style='font-size:13px;font-weight:700;color:{meta['text']};vertical-align:middle;'>"
        f"{meta['icon']}&nbsp; {cat}{amt_summary}</td>"
        f"<td style='text-align:right;vertical-align:middle;'>"
        f"<span style='font-size:11px;font-weight:700;color:#ffffff;background-color:{meta['badge_bg']};"
        f"padding:3px 10px;border-radius:20px;'>{len(items)}건</span></td>"
        f"</tr></table></td></tr></table>"
    )

    if not items:
        empty = (
            f"<table width='100%' cellpadding='0' cellspacing='0' border='0' style='margin-bottom:16px;border:1px solid {meta['border']};border-top:none;border-radius:0 0 8px 8px;'>"
            f"<tr><td style='padding:16px;text-align:center;font-size:12px;color:#94a3b8;'>해당 내역 없음</td></tr></table>"
        )
        return header + empty

    thead = (
        f"<tr style='background-color:#f8fafc;'>"
        f"{_th('수요기관', width='22%')}"
        f"{_th('사업명')}"
        f"{_th('업체명', 'center', '14%')}"
        f"{_th('금액', 'right', '13%')}"
        f"</tr>"
    )

    tbody = ""
    for i, item in enumerate(items):
        is_i   = "이노뎁" in str(item.get("corp", ""))
        row_bg = "#fffbeb" if is_i else ("#ffffff" if i % 2 == 0 else "#fafafa")
        cn     = item.get("corp", "-")
        cc     = "#1d4ed8" if is_i else "#374151"
        cw     = "font-weight:700;" if is_i else ""
        badge  = (
            f"&nbsp;<span style='font-size:9px;color:#92400e;background-color:#fef3c7;"
            f"border:1px solid #fde68a;padding:1px 5px;border-radius:3px;vertical-align:middle;'>★이노뎁</span>"
        ) if is_i else ""
        date_b = (
            f"&nbsp;<span style='font-size:10px;color:#94a3b8;background-color:#f1f5f9;"
            f"padding:1px 5px;border-radius:3px;vertical-align:middle;'>{item.get('date','')}</span>"
        ) if item.get("date") else ""

        nm      = item.get("nm", "-")
        url     = item.get("url", "#")
        nm_html = (
            f"<a href='{url}' target='_blank' style='color:{meta['accent']};text-decoration:none;"
            f"font-weight:500;'>{nm}</a>{badge}{date_b}"
            if url and url != "#" else f"{nm}{badge}{date_b}"
        )
        amt_val = fmt_amount(item.get("amt", "0"))
        amt_col = "#1d4ed8" if is_i else "#374151"
        amt_w   = "font-weight:600;" if is_i else ""

        tbody += (
            f"<tr style='background-color:{row_bg};'>"
            f"<td style='padding:9px 10px;font-size:12px;color:#475569;border-bottom:1px solid #f1f5f9;vertical-align:top;'>{item.get('org', '-')}</td>"
            f"<td style='padding:9px 10px;font-size:13px;border-bottom:1px solid #f1f5f9;vertical-align:top;line-height:1.5;'>{nm_html}</td>"
            f"<td style='padding:9px 10px;font-size:12px;color:{cc};{cw}text-align:center;border-bottom:1px solid #f1f5f9;vertical-align:top;white-space:nowrap;'>{cn}</td>"
            f"<td style='padding:9px 10px;font-size:12px;color:{amt_col};{amt_w}text-align:right;border-bottom:1px solid #f1f5f9;vertical-align:top;white-space:nowrap;'>{amt_val}</td>"
            f"</tr>"
        )

    table = (
        f"<table width='100%' cellpadding='0' cellspacing='0' border='0' style='"
        f"border:1px solid {meta['border']};border-top:none;border-radius:0 0 8px 8px;margin-bottom:20px;'>"
        f"<thead>{thead}</thead><tbody>{tbody}</tbody></table>"
    )
    return header + table


def build_report_html_b(
    period_label, is_weekly,
    shopping_cnt, notice_cnt, contract_cnt,
    innodep_org_summary, innodep_total_amt,
    vendor_stats, org_stats,
    notice_mail_buckets, contract_mail_buckets,
):
    # ── 상단 요약 카드 ─────────────────────────────────────────────────────────
    def stat_card(color, icon, label, value, sub=""):
        sub_html = f"<p style='margin:6px 0 0;font-size:11px;color:#94a3b8;line-height:1.4;'>{sub}</p>" if sub else ""
        return (
            f"<table width='100%' cellpadding='0' cellspacing='0' border='0' style='"
            f"background-color:#ffffff;border-radius:10px;border-top:3px solid {color};"
            f"box-shadow:0 1px 3px rgba(0,0,0,0.06);'>"
            f"<tr><td style='padding:14px 16px;'>"
            f"<p style='margin:0 0 6px;font-size:10px;color:#94a3b8;font-weight:700;"
            f"letter-spacing:1px;text-transform:uppercase;'>{icon} {label}</p>"
            f"<p style='margin:0;font-size:22px;font-weight:800;color:{color};letter-spacing:-0.5px;'>{value}</p>"
            f"{sub_html}"
            f"</td></tr></table>"
        )

    stat_cards = (
        "<table width='100%' cellpadding='0' cellspacing='0' border='0'><tr>"
        f"<td width='25%' style='padding-right:5px;'>{stat_card('#f59e0b','&#128722;','쇼핑몰',f'{shopping_cnt:,}건')}</td>"
        f"<td width='25%' style='padding:0 5px;'>{stat_card('#2563eb','&#128226;','공고',f'{notice_cnt:,}건')}</td>"
        f"<td width='25%' style='padding:0 5px;'>{stat_card('#7c3aed','&#128221;','계약',f'{contract_cnt:,}건')}</td>"
        f"<td width='25%' style='padding-left:5px;'>{stat_card('#059669','&#11088;','이노뎁 납품',fmt_amount(innodep_total_amt))}</td>"
        "</tr></table>"
    )

    weekly_badge = (
        f"<span style='display:inline-block;font-size:10px;font-weight:700;color:#92400e;"
        f"background-color:#fef3c7;border:1px solid #fde68a;padding:2px 8px;"
        f"border-radius:12px;vertical-align:middle;margin-left:8px;'>&#128197; 주간 종합 금·토·일</span>"
    ) if is_weekly else ""

    notice_blocks   = "".join(build_category_section(cat, notice_mail_buckets[cat])   for cat in MAIL_CATEGORIES)
    contract_blocks = "".join(build_category_section(cat, contract_mail_buckets[cat]) for cat in MAIL_CATEGORIES)

    # ── 섹션 타이틀 헬퍼 ──────────────────────────────────────────────────────
    def section_title(icon, title, subtitle, color):
        return (
            f"<table width='100%' cellpadding='0' cellspacing='0' border='0' style='"
            f"border-radius:8px;overflow:hidden;margin-bottom:4px;'>"
            f"<tr><td style='padding:12px 16px;background-color:{color};'>"
            f"<table width='100%' cellpadding='0' cellspacing='0' border='0'><tr>"
            f"<td><p style='margin:0;font-size:14px;font-weight:700;color:#ffffff;'>{icon} {title}</p>"
            f"<p style='margin:3px 0 0;font-size:11px;color:rgba(255,255,255,0.7);'>{subtitle}</p></td>"
            f"</tr></table></td></tr></table>"
        )

    def panel(content):
        return (
            f"<table width='100%' cellpadding='0' cellspacing='0' border='0' style='"
            f"background-color:#ffffff;border-radius:10px;border:1px solid #e2e8f0;"
            f"overflow:hidden;margin-bottom:16px;'>"
            f"<tr><td style='padding:16px 16px 8px;'>{content}</td></tr></table>"
        )

    def panel_with_header(header_html, body_html):
        return (
            f"<table width='100%' cellpadding='0' cellspacing='0' border='0' style='"
            f"background-color:#ffffff;border-radius:10px;border:1px solid #e2e8f0;"
            f"overflow:hidden;margin-bottom:16px;'>"
            f"<tr><td style='padding:14px 16px 10px;border-bottom:1px solid #f1f5f9;'>{header_html}</td></tr>"
            f"<tr><td style='padding:12px 16px 14px;'>{body_html}</td></tr></table>"
        )

    def panel_header(title, sub):
        return (
            f"<p style='margin:0;font-size:14px;font-weight:700;color:#1e293b;'>{title}</p>"
            f"<p style='margin:3px 0 0;font-size:11px;color:#94a3b8;'>{sub}</p>"
        )

    dashboard_links = (
        f"<table width='100%' cellpadding='0' cellspacing='0' border='0' style='"
        f"background:linear-gradient(135deg,#1e3a5f 0%,#1e40af 100%);"
        f"border-radius:10px;margin-bottom:16px;'>"
        f"<tr><td style='padding:16px 20px;'>"
        f"<table width='100%' cellpadding='0' cellspacing='0' border='0'><tr>"
        f"<td><p style='margin:0;font-size:11px;color:rgba(255,255,255,0.6);letter-spacing:1px;'>DASHBOARD</p>"
        f"<p style='margin:4px 0 0;font-size:13px;font-weight:700;color:#ffffff;'>실시간 조달 현황 대시보드</p></td>"
        f"<td style='text-align:right;white-space:nowrap;'>"
        f"<a href='http://211.171.190.220:3001' target='_blank' style='"
        f"display:inline-block;background-color:#3b82f6;color:#ffffff;font-size:12px;"
        f"font-weight:700;text-decoration:none;padding:7px 14px;border-radius:6px;margin-left:8px;'>외부 접속</a>"
        f"<a href='http://dashboard.innodep.com:3001/' target='_blank' style='"
        f"display:inline-block;background-color:#10b981;color:#ffffff;font-size:12px;"
        f"font-weight:700;text-decoration:none;padding:7px 14px;border-radius:6px;margin-left:8px;'>내부 접속</a>"
        f"</td></tr></table></td></tr></table>"
    )

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Innodep 조달청 리포트</title>
</head>
<body style="margin:0;padding:0;background-color:#eef2f7;font-family:'맑은 고딕','Apple SD Gothic Neo',Arial,sans-serif;-webkit-text-size-adjust:100%;">

<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#eef2f7;">
<tr><td align="center" style="padding:20px 12px 28px;">
<table width="680" cellpadding="0" cellspacing="0" border="0" style="max-width:680px;width:100%;">

<!-- ══ 헤더 ══════════════════════════════════════════════════════════════ -->
<tr><td style="padding-bottom:16px;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0"
         style="background:linear-gradient(135deg,#0f172a 0%,#1e3a5f 60%,#1e40af 100%);border-radius:12px;overflow:hidden;">
    <tr><td colspan="2" height="4"
            style="background:linear-gradient(90deg,#3b82f6,#10b981,#f59e0b);font-size:0;line-height:0;">&nbsp;</td></tr>
    <tr>
      <td style="padding:22px 24px 20px;">
        <p style="margin:0 0 3px;font-size:10px;letter-spacing:3px;color:#60a5fa;font-weight:700;text-transform:uppercase;">
          Innodep &middot; Procurement Intelligence</p>
        <p style="margin:0 0 4px;font-size:20px;font-weight:800;color:#f0f9ff;letter-spacing:-0.5px;">
          조달청 데이터 리포트 {weekly_badge}</p>
        <p style="margin:0;font-size:11px;color:#60a5fa;opacity:0.8;">
          &#128683;&nbsp; 학교 CCTV &middot; 국방 분야 제외</p>
      </td>
      <td style="padding:22px 24px 20px;text-align:right;vertical-align:middle;white-space:nowrap;">
        <p style="margin:0 0 2px;font-size:10px;color:#60a5fa;letter-spacing:1px;text-transform:uppercase;">Period</p>
        <p style="margin:0 0 6px;font-size:13px;font-weight:700;color:#bfdbfe;">{period_label}</p>
        <p style="margin:0;font-size:10px;color:#34d399;font-weight:700;letter-spacing:1px;">&#9679;&nbsp;AUTO COLLECTED</p>
      </td>
    </tr>
  </table>
</td></tr>

<!-- ══ 통계 카드 ══════════════════════════════════════════════════════════ -->
<tr><td style="padding-bottom:16px;">{stat_cards}</td></tr>

<!-- ══ 경쟁사 납품 TOP 10 ════════════════════════════════════════════════ -->
<tr><td style="padding-bottom:0;">
  {panel_with_header(
      panel_header("&#128202;&nbsp; 경쟁사 납품금액 TOP 10",
                   f"{period_label} · 이노뎁 포함 / ★ 표시"),
      build_vendor_chart(vendor_stats)
  )}
</td></tr>

<!-- ══ 수요기관 TOP 10 ═══════════════════════════════════════════════════ -->
<tr><td style="padding-bottom:0;">
  {panel_with_header(
      panel_header("&#127963;&nbsp; 수요기관 납품금액 TOP 10",
                   f"{period_label} · 기관별 합산"),
      build_org_chart(org_stats)
  )}
</td></tr>

<!-- ══ 이노뎁 납품 실적 ══════════════════════════════════════════════════ -->
<tr><td style="padding-bottom:0;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#ffffff;border-radius:10px;border:1px solid #bfdbfe;overflow:hidden;margin-bottom:16px;">
    <tr><td style="padding:12px 16px 10px;background-color:#eff6ff;border-bottom:2px solid #bfdbfe;">
      <p style="margin:0;font-size:14px;font-weight:700;color:#1e40af;">&#11088;&nbsp; 이노뎁 납품 실적</p>
      <p style="margin:3px 0 0;font-size:11px;color:#60a5fa;">{period_label}</p>
    </td></tr>
    <tr><td>{build_innodep_table(innodep_org_summary, innodep_total_amt)}</td></tr>
  </table>
</td></tr>

<!-- ══ 대시보드 링크 ═════════════════════════════════════════════════════ -->
<tr><td style="padding-bottom:0;">{dashboard_links}</td></tr>

<!-- ══ 나라장터 입찰공고 ═════════════════════════════════════════════════ -->
<tr><td style="padding-bottom:6px;">
  {section_title("&#128226;", "나라장터 입찰공고", "키워드 필터링 · 국방 제외", "#dc2626")}
</td></tr>
<tr><td style="padding-bottom:4px;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#ffffff;border-radius:10px;border:1px solid #e2e8f0;overflow:hidden;margin-bottom:16px;">
    <tr><td style="padding:16px 16px 10px;">{notice_blocks}</td></tr>
  </table>
</td></tr>

<!-- ══ 나라장터 계약내역 ═════════════════════════════════════════════════ -->
<tr><td style="padding-bottom:6px;">
  {section_title("&#128221;", "나라장터 계약내역", "키워드 필터링 · 국방 제외", "#1d4ed8")}
</td></tr>
<tr><td style="padding-bottom:4px;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#ffffff;border-radius:10px;border:1px solid #e2e8f0;overflow:hidden;margin-bottom:16px;">
    <tr><td style="padding:16px 16px 10px;">{contract_blocks}</td></tr>
  </table>
</td></tr>

<!-- ══ 푸터 ══════════════════════════════════════════════════════════════ -->
<tr><td style="padding:12px 0 4px;text-align:center;border-top:1px solid #e2e8f0;">
  <p style="margin:0;font-size:11px;color:#94a3b8;letter-spacing:0.3px;">
    본 메일은 GitHub Actions 자동화 스크립트로 발송됩니다 &nbsp;&middot;&nbsp; Innodep Procurement Bot
  </p>
</td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""


# =============================================================================
# 메인
# =============================================================================

def main():
    if not MY_DIRECT_KEY or not AUTH_JSON_STR:
        print("❌ 환경변수 누락 (API 키 또는 Google Auth) — 종료")
        sys.exit(1)

    now     = get_kst_now()
    weekday = now.weekday()
    if weekday >= 5:
        print(f"⏭ 주말({['월','화','수','목','금','토','일'][weekday]}) — 발송 스킵")
        sys.exit(0)

    date_list, is_weekly = get_date_range()
    print(f"📅 수집 대상: {date_list} ({'주간 종합' if is_weekly else '전일'})")

    drive_service, drive_creds = get_drive_service()

    # ── 경쟁사 목록 로드 (main.py와 동일 방식) ──────────────────────────────
    target_companies = get_target_companies()

    print("📦 쇼핑몰 데이터 로드 중 (드라이브)...")
    all_shopping = load_shopping_from_drive(drive_service, drive_creds, date_list)
    print(f"   └ {len(all_shopping):,}행")

    print("📢 공고 데이터 로드 중 (드라이브)...")
    notice_buckets, total_notice_cnt = load_notice_from_drive(drive_service, drive_creds, date_list)
    print(f"   └ 공고 {total_notice_cnt:,}건")

    if not all_shopping and total_notice_cnt == 0:
        print("⚠️ 수집 데이터 없음 — main.py 미실행 또는 공휴일, 메일 발송 중단")
        sys.exit(1)

    print("📝 계약 데이터 수집 중 (API)...")
    contract_buckets = load_contracts_from_api(date_list)
    contract_cnt     = sum(len(v) for v in contract_buckets.values())
    print(f"   └ 계약 {contract_cnt:,}건")

    # ── 통계 계산 (경쟁사 목록 전달) ────────────────────────────────────────
    vendor_stats, org_stats, innodep_org_summary, innodep_total_amt = calc_stats(
        all_shopping, companies=target_companies
    )

    if is_weekly:
        period_label = (
            f"{date_list[0][:4]}.{date_list[0][4:6]}.{date_list[0][6:]} "
            f"~ {date_list[-1][:4]}.{date_list[-1][4:6]}.{date_list[-1][6:]}"
        )
    else:
        d = date_list[0]
        period_label = f"{d[:4]}.{d[4:6]}.{d[6:]} (전일)"

    report_html = build_report_html_b(
        period_label          = period_label,
        is_weekly             = is_weekly,
        shopping_cnt          = len(all_shopping),
        notice_cnt            = total_notice_cnt,
        contract_cnt          = contract_cnt,
        innodep_org_summary   = innodep_org_summary,
        innodep_total_amt     = innodep_total_amt,
        vendor_stats          = vendor_stats,
        org_stats             = org_stats,
        notice_mail_buckets   = notice_buckets,
        contract_mail_buckets = contract_buckets,
    )

    if "GITHUB_OUTPUT" in os.environ:
        tag         = "weekly" if is_weekly else date_list[0]
        report_path = f"/tmp/report_b_{tag}.html"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_html)
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:
            f.write(f"collect_period={period_label}\n")
            f.write(f"report_b_path={report_path}\n")
        print(f"✅ 리포트 저장 완료: {report_path}")


if __name__ == "__main__":
    main()
