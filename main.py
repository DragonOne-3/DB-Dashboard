import os
import io
import json
import time
import datetime
import requests
import threading
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
 
import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseUpload
 
 
# =================================================================================
# 1. 설정 및 환경 변수
# =================================================================================
MY_DIRECT_KEY = os.environ.get("DATA_GO_KR_API_KEY")
AUTH_JSON_STR = os.environ.get("GOOGLE_AUTH_JSON")
 
NOTICE_FOLDER_ID = "1AsvVmayEmTtY92d1SfXxNi6bL0Zjw5mg"
SHOPPING_FOLDER_ID = "1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr"
 
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
 
CAT_KEYWORDS = {
    "영상감시장치": ["CCTV", "통합관제", "영상감시장치", "영상정보처리기기"],
    "국방": ["국방", "부대", "작전", "경계", "방위", "군사", "무인화", "사령부", "군대", "중요시설", "주둔지", "과학화", "육군", "해군", "공군", "해병"],
    "솔루션": ["데이터", "플랫폼", "솔루션", "주차", "출입", "GIS"],
    "스마트도시": ["ITS", "스마트시티", "스마트도시","K-AI","AI시티"],
    "드론":["드론", "무인기", "UAV", "UAS", "무인항공", "드론관제", "드론감시", "드론탐지"],
}
 
CAT_META = {
    "영상감시장치": {
        "icon": "&#128247;", "accent": "#2d7dd2", "bg": "#eff6ff",
        "border": "#bfdbfe", "text": "#1e3a5f", "badge_bg": "#2d7dd2",
    },
    "국방": {
        "icon": "&#128737;", "accent": "#e03444", "bg": "#fef2f2",
        "border": "#fecaca", "text": "#7f1d1d", "badge_bg": "#e03444",
    },
    "솔루션": {
        "icon": "&#128161;", "accent": "#8b5cf6", "bg": "#f5f3ff",
        "border": "#d4c9f7", "text": "#4c1d95", "badge_bg": "#8b5cf6",
    },
    "스마트도시": {
        "icon": "&#127751;", "accent": "#10b981", "bg": "#f0fdf4",
        "border": "#a7d9c0", "text": "#064e3b", "badge_bg": "#10b981",
    },
    "드론": {
        "icon": "&#128641;",   # ✈ 비행기 이모지 (또는 🚁)
        "accent": "#0ea5e9",
        "bg": "#f0f9ff",
        "border": "#bae6fd",
        "text": "#0c4a6e",
        "badge_bg": "#0ea5e9",
    },
}
 
keywords = sorted(list(set([
    "1종금속제가요전선관",     "3차원프린터",    "450/750V 유연성단심비닐절연전선",    "450/750V 일반용유연성단심비닐절연전선",    "450/750V유연성단심비닐절연전선",    "AV스위쳐",    "CD녹음및플레이어",    "DVD드라이브",   "IP전화기",    "LAP외피광케이블",    "LED가로등기구",    "LED경관조명기구",
    "LED다운라이트",    "LED램프",    "LED보안등기구",    "LED실내조명등",    "LED터널용등기구",    "LED투광등기구",    "PA용스피커",    "SSD저장장치",    "UTP케이블",    "가로등자동점멸기",    "가로등주부속자재",    "견인용갈고리",    "결선보드유닛",    "경관조명기구",    "경광등",    "계장제어장치",
    "고주파동축케이블",    "광분배함",    "광송수신기",    "광송수신모듈",    "광수신기",    "광점퍼코드",    "교육용소프트웨어",    "교통관제시스템",    "교통신호등",    "교통신호제어기",    "구내단자함",    "구내방송장치",    "그래픽용어댑터",   "그레이팅덮개",    "금속기둥",    "금속상자",    "기상전광판",
    "기억유닛",    "난연전력케이블",    "난연접지용비닐절연전선",    "냉각팬",    "네트워크스위치",    "네트워크시스템장비용랙",    "네트워크회의용소프트웨어",    "논슬립",   "누전차단기",    "다목적승용차",    "대기오염측정기",    "데스크톱컴퓨터",    "데이터베이스관리소프트웨어",
    "도난방지기",    "도로안전표지판지주",    "도로표지병",    "도서관리시스템",    "동작분석기",    "등기구보강대",    "디바이더",    "디스크어레이",    "디지털비디오레코더",    "라디오튜너",    "랙캐비닛용패널",    "랜접속카드",    "레이더",    "레이드저장장치",    "레이드컨트롤러",    "레이스웨이",
    "리모트앰프",    "릴레이유닛",    "마그네틱카드판독기",    "마을무선방송장치",   "마이크로폰",    "마이크스탠드",    "매트릭스로직유닛",    "멀티미디어학습장치",    "멀티스크린컴퓨터",    "멀티탭",    "메가폰",    "무선랜액세스포인트",   "무선마이크장치",    "무선인식리더기",
    "무선통신장치",    "무인교통감시장치",    "무정전전원장치",    "밀폐고정형납축전지",    "바닥형보행신호등",    "바코드시스템",    "방송수신기",    "방화벽장치",    "배선장치",    "버스및차량정보안내장치",    "베어본컴퓨터",    "벨",    "보건용마스크",    "보관용선반",    "보안소프트웨어",
    "보안용카메라",    "보행매트",    "보행신호음성안내보조장치",    "보행자안전차단기",    "보행자작동신호기",    "볼라드",    "분배기",    "분석및과학용소프트웨어",    "분전반",    "분전함",    "브래킷",    "비대면방역감지장비",    "비디오네트워킹장비",    "비디오믹서",    "비디오프로젝터",
    "비상경보기",    "비상유닛",    "산업관리소프트웨어",    "서지흡수기",    "세탁물건조기",    "소방용방화복세탁기",    "소프트웨어유지및지원서비스",    "소형기기용충전기",    "솔내시스템",    "송신기",    "수업자동녹화시스템",    "수위조절기",    "스위치박스",    "스위칭모드전원공급장치",    "스테이플",
    "스테인리스가로등주",    "스피커",    "스피커선택유닛",    "스피커스탠드",    "시스템관리소프트웨어",    "식별용태그",    "안내전광판",    "안내판",    "액정모니터",    "엔코더",    "열선감지기",    "영사대",    "영사용스크린",    "영상감시장치",    "영상분배기",    "영상정보디스플레이장치",    "영상회의시스템",
    "오디오모니터",    "오디오믹서",    "오디오앰프",    "온습도트랜스미터",    "우산빗물제거기",    "운영체제",    "원격단말장치(RTU)",    "원격자동검침시스템",    "유틸리티소프트웨어",    "융복합UTP케이블",    "융복합그래픽용어댑터",    "융복합네트워크스위치",    "융복합네트워크시스템장비용랙",    "융복합대기오염측정기",
    "융복합데스크톱컴퓨터",    "융복합무선데이터통신장비",    "융복합배선장치",    "융복합베어본컴퓨터",    "융복합서지흡수기",    "융복합안내전광판",    "융복합액정모니터",    "융복합영상감시장치",    "융복합카메라브래킷",    "융복합화염감지기",    "응용과학용소프트웨어",    "의료용살충제",    "이퀄라이저",
    "인증관리시스템",    "인터랙티브화이트보드",    "인터콤장비",    "인터폰",    "자기식테이프",    "자동변속기",    "자동승강조명장치",    "장치제어보드",    "적외선방사기",    "적외선카메라",    "적외선탐지기",    "전동기제어반",     "전력공급장치",    "전원공급장치",    "전자카드",    "절연전선및피복선",    
    "접지봉",    "접지판",    "정보통신공사",    "정보화교육서비스",    "제어케이블",    "조명용제어장치",    "조명타워",    "종합폴",    "주차경보등",    "주차관제주변기기",    "주차권판독기",    "주차안내판",    "주차요금계산기",    "주차주제어장치",
    "주파수분할다중화장치",    "지도소프트웨어",    "차량감지기",    "차량검지기",    "차량번호판독기",    "차량인식기",    "차량지지용아우트리거",    "차량차단기",    "철근콘크리트공사",    "철제가로등주",    "출입통제시스템",    "카드락",    "카드인쇄기",    "카메라받침대",
    "카메라브래킷",    "카메라컨트롤러",    "카메라하우징",    "카메라회전대",    "캠코더",    "컨버터",    "컴바이너",    "컴퓨터망전환장치",    "컴퓨터및주변기기설치",    "컴퓨터서버",    "컴퓨터정맥인식장치",    "컴퓨터지문인식장치",    "케이블타이",    "콘솔익스텐더",    "콘텐츠관리소프트웨어",    "콤바인",
    "탐조등",    "태양광가로등",    "태양광발전장치",    "태양전지조절기",    "테이프백업장치",    "텔레비전",    "텔레비전거치대",    "토공사",    "통신소프트웨어",    "통신용변조기",    "통신케이블어셈블리",    "통합배선반",     "특수목적컴퓨터",    "패키지소프트웨어개발및도입서비스",    "패키지용품",
    "폐쇄형배전반",    "포장공사",    "폴리에틸렌전선관",    "풀박스",    "플러그용잭",    "피뢰탄기반",    "하드디스크드라이브",    "호온스피커"  
])))
 
NOTICE_API_MAP = {
    "공사": "https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwkPPSSrch",
    "물품": "https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThngPPSSrch",
    "용역": "https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoServcPPSSrch",
}
 
 
# =================================================================================
# 2. 유틸리티 함수
# =================================================================================
def get_drive_service_for_script():
    info = json.loads(AUTH_JSON_STR)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=creds), creds
 
 
def get_target_dates():
    """
    한국시간 기준 오늘 요일에 따라 수집 대상 날짜(들)를 반환.
    - 월요일: 금/토/일 (주말 포함 3일치 통합 리포트)
    - 화~금: 어제 하루
    - 토/일: 어제 하루만 수집(데이터는 저장하되 메일 발송은 skip)
    반환값은 datetime 객체 리스트(오름차순).
    """
    now = datetime.datetime.now(datetime.UTC) + datetime.timedelta(hours=9)
    weekday = now.weekday()  # 월=0 ... 일=6
    if weekday == 0:  # 월요일
        return [now - datetime.timedelta(days=d) for d in (3, 2, 1)]
    return [now - datetime.timedelta(days=1)]
 
 
def should_send_mail():
    """토요일/일요일에는 메일 발송을 skip한다."""
    now = datetime.datetime.now(datetime.UTC) + datetime.timedelta(hours=9)
    return now.weekday() not in (5, 6)  # 토=5, 일=6
 
 
def classify_text(text):
    for cat, kws in CAT_KEYWORDS.items():
        if any(kw in str(text) for kw in kws):
            return cat
    return "기타"
 
 
def get_target_companies():
    """
    경쟁사 리스트는 main.py와 같은 위치의 companies.txt에서 읽습니다.
    파일이 없으면 아래 기본값을 사용합니다.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "companies.txt")
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    return ["이노뎁(주)", "이노뎁"]
 
 
def normalize_company_name(name):
    return str(name).replace(" ", "").replace("(주)", "").replace("주식회사", "").upper()
 
 
def fmt_amount_short(val):
    try:
        n = int(str(val).replace(",", "").split(".")[0])
        if n >= 100_000_000:
            return f"{n / 100_000_000:.1f}억"
        if n >= 10_000:
            return f"{n / 10_000:.0f}만"
        return f"{n:,}원"
    except Exception:
        return str(val) if val else "-"
 
 
def fmt_amount_full(val):
    try:
        n = int(str(val).replace(",", "").split(".")[0])
        if n >= 100_000_000:
            return f"{n / 100_000_000:.1f}억원"
        if n >= 10_000:
            return f"{n / 10_000:.0f}만원"
        return f"{n:,}원"
    except Exception:
        return str(val) if val else "별도공고"
 
 
def format_html_table(data_list, title):
    html = f"<div style='margin-top:25px;'><h4 style='color:#2c3e50; border-bottom:2px solid #34495e; padding-bottom:8px;'>{title}</h4>"
    if not data_list:
        html += "<p style='color:#888; padding:12px;'>- 해당 내역이 없습니다.</p></div>"
        return html
 
    html += "<table border='1' style='border-collapse:collapse; width:100%; font-size:13px; line-height:1.8;'>"
    html += "<tr style='background-color:#f8f9fa;'><th>수요기관</th><th>명칭(링크)</th><th>업체명</th><th>금액</th></tr>"
 
    for item in data_list:
        corp_name = item.get("corp", "-")
        bg = "background-color:#FFF9C4;" if "이노뎁" in corp_name else ""
        amt_val = item.get("amt", "0")
        try:
            amt_str = f"{int(str(amt_val).replace(',', '').split('.')[0]):,}원"
        except Exception:
            amt_str = amt_val
        link_name = f"<a href='{item['url']}' target='_blank' style='color:#1a73e8; text-decoration:none;'>{item['nm']}</a>"
        html += f"<tr style='{bg}'><td style='padding:8px; text-align:center;'>{item['org']}</td>"
        html += f"<td style='padding:8px;'>{link_name}</td>"
        html += f"<td style='padding:8px; text-align:center;'>{corp_name}</td>"
        html += f"<td style='padding:8px; text-align:right;'>{amt_str}</td></tr>"
    html += "</table></div>"
    return html
 
 
def fetch_api_data_from_g2b(kw, d_str, retries=3):
    url = "https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getSpcifyPrdlstPrcureInfoList"
    params = {
        "numOfRows": "999",
        "pageNo": "1",
        "ServiceKey": MY_DIRECT_KEY,
        "type": "xml",
        "inqryDiv": "1",
        "inqryPrdctDiv": "2",
        "inqryBgnDate": d_str,
        "inqryEndDate": d_str,
        "dtilPrdctClsfcNoNm": kw,
    }
    for attempt in range(retries):
        try:
            res = requests.get(url, params=params, timeout=60)
            if res.status_code == 200 and "<item>" in res.text:
                root = ET.fromstring(res.content)
                return [[elem.text if elem.text else "" for elem in item] for item in root.findall(".//item")]
            return []
        except requests.exceptions.Timeout:
            wait = (attempt + 1) * 5
            print(f"[{kw}] 타임아웃 ({attempt + 1}/{retries}), {wait}초 후 재시도...")
            time.sleep(wait)
        except Exception as e:
            print(f"[{kw}] 오류: {e}")
            return []
    print(f"[{kw}] 최종 실패")
    return []
 
 
def fetch_notice_data(category, url, d_str):
    params = {
        "serviceKey": MY_DIRECT_KEY,
        "pageNo": "1",
        "numOfRows": "999",
        "inqryDiv": "1",
        "type": "json",
        "inqryBgnDt": d_str + "0000",
        "inqryEndDt": d_str + "2359",
    }
    try:
        res = requests.get(url, params=params, timeout=15)
        if res.status_code == 200:
            return pd.DataFrame(res.json().get("response", {}).get("body", {}).get("items", []))
    except Exception:
        pass
    return pd.DataFrame()
 
 
def fetch_single_contract(kw_s, d_str):
    api_url_servc = "https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch"
    results = []
    p = {
        "serviceKey": MY_DIRECT_KEY,
        "inqryDiv": "1",
        "type": "xml",
        "inqryBgnDate": d_str,
        "inqryEndDate": d_str,
        "cntrctNm": kw_s,
    }
    try:
        r = requests.get(api_url_servc, params=p, timeout=20)
        if r.status_code == 200:
            root = ET.fromstring(r.content)
            for item in root.findall(".//item"):
                detail_url = item.findtext("cntrctDtlInfoUrl") or "https://www.g2b.go.kr"
                raw_demand = item.findtext("dminsttList", "-")
                clean_demand = raw_demand.replace("[", "").replace("]", "").split("^")[2] if "^" in raw_demand else raw_demand
                raw_corp = item.findtext("corpList", "-")
                clean_corp = raw_corp.replace("[", "").replace("]", "").split("^")[3] if "^" in raw_corp else raw_corp
                results.append({
                    "org": clean_demand,
                    "nm": item.findtext("cntrctNm", "-"),
                    "corp": clean_corp,
                    "amt": item.findtext("totCntrctAmt", "0"),
                    "url": detail_url,
                })
    except Exception as e:
        print(f"계약 데이터 수집 오류 ({kw_s}): {e}")
    return results
 
 
def save_notice_by_year(drive_service, creds, cat_name, new_df, year):
    file_name = f"나라장터_공고_{cat_name}_{year}년.csv"
    res = drive_service.files().list(
        q=f"name='{file_name}' and '{NOTICE_FOLDER_ID}' in parents and trashed=false",
        fields="files(id)",
    ).execute()
    items = res.get("files", [])
    file_id = items[0]["id"] if items else None
 
    if file_id:
        resp = requests.get(
            f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media",
            headers={"Authorization": f"Bearer {creds.token}"},
        )
        try:
            old_df = pd.read_csv(io.BytesIO(resp.content), encoding="utf-8-sig", low_memory=False)
            new_df = pd.concat([old_df, new_df], ignore_index=True)
        except Exception as e:
            print(f"기존 파일 읽기 오류 ({file_name}): {e}")
 
    if "bidNtceNo" in new_df.columns:
        new_df.drop_duplicates(subset=["bidNtceNo"], keep="last", inplace=True)
 
    csv_bytes = new_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    media = MediaIoBaseUpload(io.BytesIO(csv_bytes), mimetype="text/csv")
 
    if file_id:
        drive_service.files().update(fileId=file_id, media_body=media).execute()
    else:
        drive_service.files().create(
            body={"name": file_name, "parents": [NOTICE_FOLDER_ID]},
            media_body=media,
        ).execute()
    print(f"✅ [{cat_name}] {file_name} 저장 완료 ({len(new_df):,}건)")
 
 
def _bar_row(rank, label, pct, amount_str, bar_color, bar_bg, label_color="#374151", label_bold=False):
    pct = max(3, min(pct, 100))
    bold = "font-weight:700;" if label_bold else ""
    return (
        f"<tr>"
        f"<td width='14' style='font-size:12px;color:#9ca3af;text-align:right;padding:3px 4px;white-space:nowrap;'>{rank}</td>"
        f"<td width='98' style='font-size:13px;color:{label_color};{bold}padding:3px 6px;white-space:nowrap;overflow:hidden;max-width:98px;'>{label}</td>"
        f"<td style='padding:3px 4px;'>"
        f"<table width='100%' cellpadding='0' cellspacing='0' border='0' style='background-color:{bar_bg};border-radius:3px;height:8px;'><tr>"
        f"<td width='{pct}%' height='8' style='background-color:{bar_color};border-radius:3px;font-size:0;line-height:0;'>&nbsp;</td><td></td></tr></table>"
        f"</td>"
        f"<td width='52' style='font-size:12px;color:#6b7280;text-align:right;padding:3px 4px;white-space:nowrap;'>{amount_str}</td>"
        f"</tr>"
    )
 
 
def build_vendor_chart(vendor_stats):
    top = sorted(vendor_stats.items(), key=lambda x: x[1], reverse=True)[:10]
    if not top:
        return "<p style='font-size:12px;color:#9ca3af;padding:12px 0;'>데이터 없음</p>"
 
    rows = ""
    for i, (label, val) in enumerate(top):
        is_innodep = "이노뎁" in label
 
        rank = "★" if is_innodep else str(i + 1)
        name_color = "#2d7dd2" if is_innodep else "#374151"
        name_weight = "font-weight:700;" if is_innodep else ""
        row_bg = "#eff6ff" if is_innodep else ("#ffffff" if i % 2 == 0 else "#fafafa")
 
        rows += (
            f"<tr style='background-color:{row_bg};'>"
            f"<td style='padding:7px 8px;font-size:11px;color:#9ca3af;text-align:center;border-bottom:1px solid #f3f4f6;width:12%;'>{rank}</td>"
            f"<td style='padding:7px 8px;font-size:12px;color:{name_color};{name_weight}border-bottom:1px solid #f3f4f6;width:53%;'>{label}</td>"
            f"<td style='padding:7px 8px;font-size:12px;color:#374151;text-align:right;border-bottom:1px solid #f3f4f6;width:35%;'>{fmt_amount_full(val)}</td>"
            f"</tr>"
        )
 
    return (
        "<table width='100%' cellpadding='0' cellspacing='0' border='0'>"
        "<tr style='background-color:#f8fafc;'>"
        "<th style='padding:7px 8px;font-size:10px;font-weight:700;color:#6b7280;text-align:center;border-bottom:1px solid #e5e7eb;'>순위</th>"
        "<th style='padding:7px 8px;font-size:10px;font-weight:700;color:#6b7280;text-align:left;border-bottom:1px solid #e5e7eb;'>업체명</th>"
        "<th style='padding:7px 8px;font-size:10px;font-weight:700;color:#6b7280;text-align:right;border-bottom:1px solid #e5e7eb;'>금액</th>"
        "</tr>"
        f"{rows}"
        "</table>"
    )
 
 
def build_org_chart(org_stats):
    top = sorted(org_stats.items(), key=lambda x: x[1], reverse=True)[:10]
    if not top:
        return "<p style='font-size:12px;color:#9ca3af;padding:12px 0;'>데이터 없음</p>"
 
    rows = ""
    for i, (label, val) in enumerate(top):
        row_bg = "#ffffff" if i % 2 == 0 else "#fafafa"
 
        rows += (
            f"<tr style='background-color:{row_bg};'>"
            f"<td style='padding:7px 8px;font-size:11px;color:#9ca3af;text-align:center;border-bottom:1px solid #f3f4f6;width:12%;'>{i + 1}</td>"
            f"<td style='padding:7px 8px;font-size:12px;color:#374151;border-bottom:1px solid #f3f4f6;width:53%;'>{label}</td>"
            f"<td style='padding:7px 8px;font-size:12px;color:#374151;text-align:right;border-bottom:1px solid #f3f4f6;width:35%;'>{fmt_amount_full(val)}</td>"
            f"</tr>"
        )
 
    return (
        "<table width='100%' cellpadding='0' cellspacing='0' border='0'>"
        "<tr style='background-color:#f8fafc;'>"
        "<th style='padding:7px 8px;font-size:10px;font-weight:700;color:#6b7280;text-align:center;border-bottom:1px solid #e5e7eb;'>순위</th>"
        "<th style='padding:7px 8px;font-size:10px;font-weight:700;color:#6b7280;text-align:left;border-bottom:1px solid #e5e7eb;'>기관명</th>"
        "<th style='padding:7px 8px;font-size:10px;font-weight:700;color:#6b7280;text-align:right;border-bottom:1px solid #e5e7eb;'>금액</th>"
        "</tr>"
        f"{rows}"
        "</table>"
    )
 
 
 
def build_category_section(cat, items):
    meta = CAT_META.get(cat, {
        "icon": "&#128203;", "accent": "#374151", "bg": "#f9fafb",
        "border": "#e5e7eb", "text": "#374151", "badge_bg": "#374151",
    })
 
    header = (
        f"<table width='100%' cellpadding='0' cellspacing='0' border='0' "
        f"style='background-color:{meta['bg']};border-radius:6px;border-left:3px solid {meta['accent']};margin-bottom:12px;'>"
        f"<tr><td style='padding:9px 12px;'>"
        f"<table width='100%' cellpadding='0' cellspacing='0' border='0'><tr>"
        f"<td style='font-size:12px;font-weight:700;color:{meta['text']};'>{meta['icon']} {cat}</td>"
        f"<td style='text-align:right;'><span style='font-size:12px;font-weight:700;color:#ffffff;background-color:{meta['badge_bg']};padding:2px 12px;border-radius:12px;'>{len(items)}건</span></td>"
        f"</tr></table></td></tr></table>"
    )
 
    if not items:
        return header + (
            "<table width='100%' cellpadding='0' cellspacing='0' border='0' style='margin-bottom:16px;'>"
            "<tr><td style='padding:14px;text-align:center;font-size:12px;color:#9ca3af;background-color:#fafafa;border-radius:6px;border:1px dashed #e5e7eb;'>"
            "해당 내역이 없습니다.</td></tr></table>"
        )
 
    thead = (
        f"<tr style='background-color:{meta['bg']};'>"
        f"<th style='padding:7px 8px;font-size:12px;font-weight:700;color:#6b7280;text-align:left;width:20%;border-bottom:1px solid {meta['border']};'>수요기관</th>"
        f"<th style='padding:7px 8px;font-size:12px;font-weight:700;color:#6b7280;text-align:left;border-bottom:1px solid {meta['border']};'>사업명</th>"
        f"<th style='padding:7px 8px;font-size:12px;font-weight:700;color:#6b7280;text-align:center;width:16%;border-bottom:1px solid {meta['border']};'>업체명</th>"
        f"<th style='padding:7px 8px;font-size:12px;font-weight:700;color:#6b7280;text-align:right;width:12%;border-bottom:1px solid {meta['border']};'>금액</th>"
        f"</tr>"
    )
 
    tbody = ""
    for i, item in enumerate(items):
        row_bg = "#fffbeb" if "이노뎁" in str(item.get("corp", "")) else ("#ffffff" if i % 2 == 0 else "#fafafa")
        corp_name = item.get("corp", "-")
        corp_color = "#2d7dd2" if "이노뎁" in corp_name else "#374151"
        corp_bold = "font-weight:700;" if "이노뎁" in corp_name else ""
        badge = ""
        if "이노뎁" in corp_name:
            badge = " <span style='font-size:9px;color:#92400e;background-color:#fef3c7;border:1px solid #fde68a;padding:1px 4px;border-radius:3px;'>&#9733;이노뎁</span>"
 
        nm = item.get("nm", "-")
        url = item.get("url", "#")
        nm_html = f"<a href='{url}' target='_blank' style='color:{meta['accent']};text-decoration:none;'>{nm}</a>{badge}" if url and url != "#" else f"{nm}{badge}"
 
        tbody += (
            f"<tr style='background-color:{row_bg};'>"
            f"<td style='padding:7px 8px;font-size:13px;color:#374151;border-bottom:1px solid #f3f4f6;'>{item.get('org', '-')}</td>"
            f"<td style='padding:7px 8px;font-size:13px;border-bottom:1px solid #f3f4f6;'>{nm_html}</td>"
            f"<td style='padding:7px 8px;font-size:13px;color:{corp_color};{corp_bold}text-align:center;border-bottom:1px solid #f3f4f6;'>{corp_name}</td>"
            f"<td style='padding:7px 8px;font-size:13px;color:#374151;text-align:right;border-bottom:1px solid #f3f4f6;'>{fmt_amount_full(item.get('amt', '0'))}</td>"
            f"</tr>"
        )
 
    return header + f"<table width='100%' cellpadding='0' cellspacing='0' border='0' style='margin-bottom:16px;'><thead>{thead}</thead><tbody>{tbody}</tbody></table>"
 
 
def build_report_html(
    display_date,
    weekday_str,
    shopping_cnt,
    notice_cnt,
    contract_cnt,
    school_stats,
    innodep_org_summary,
    innodep_detail_map,
    innodep_total_amt,
    vendor_stats,
    org_stats,
    notice_mail_buckets,
    contract_mail_buckets,
):
    school_total_amt = sum(item["total_amt"] for item in school_stats.values()) if school_stats else 0
    innodep_org_count = len(innodep_org_summary) if innodep_org_summary else 0
 
    def card(color, label, value, sub=""):
        sub_html = f"<p style='margin:4px 0 0;font-size:12px;color:#9ca3af;'>{sub}</p>" if sub else ""
        return (
            f"<table width='100%' cellpadding='0' cellspacing='0' border='0' style='background-color:#ffffff;border-radius:8px;border-top:3px solid {color};overflow:hidden;'>"
            f"<tr><td style='padding:16px 14px;'>"
            f"<p style='margin:0 0 8px 0;font-size:9px;color:#9ca3af;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;'>{label}</p>"
            f"<p style='margin:0;font-size:26px;font-weight:700;color:{color};'>{value}</p>"
            f"{sub_html}</td></tr></table>"
        )
 
    stat_cards = (
        "<table width='100%' cellpadding='0' cellspacing='0' border='0'><tr>"
        f"<td width='25%' style='padding-right:6px;'>{card('#f59e0b', '쇼핑몰 수집', f'{shopping_cnt:,}건')}</td>"
        f"<td width='25%' style='padding-right:6px;padding-left:6px;'>{card('#2d7dd2', '공고 수집', f'{notice_cnt:,}건')}</td>"
        f"<td width='25%' style='padding-right:6px;padding-left:6px;'>{card('#8b5cf6', '계약 수집', f'{contract_cnt:,}건')}</td>"
        f"<td width='25%' style='padding-left:6px;'>{card('#10b981', '이노뎁 납품', fmt_amount_full(innodep_total_amt))}</td>"
 
        "</tr></table>"
    )
 
    school_rows_html = ""
    if school_stats:
        for i, (school_name, info) in enumerate(sorted(school_stats.items(), key=lambda x: x[1]["total_amt"], reverse=True)):
            row_bg = "#ffffff" if i % 2 == 0 else "#fffdf5"
            vendor_color = "#2d7dd2" if "이노뎁" in info["main_vendor"] else "#e03444" if info["main_vendor"] != "-" else "#374151"
            vendor_weight = "font-weight:700;" if "이노뎁" in info["main_vendor"] else ""
            school_rows_html += (
                f"<tr style='background-color:{row_bg};'>"
                f"<td style='padding:7px 12px;font-size:13px;color:#374151;border-bottom:1px solid #fef9e7;'>{school_name}</td>"
                f"<td style='padding:7px 12px;font-size:13px;color:{vendor_color};{vendor_weight}border-bottom:1px solid #fef9e7;'>{info['main_vendor']}</td>"
                f"<td style='padding:7px 12px;font-size:13px;color:#374151;text-align:right;border-bottom:1px solid #fef9e7;'>{fmt_amount_full(info['total_amt'])}</td>"
                f"</tr>"
            )
        school_table = (
            "<table width='100%' cellpadding='0' cellspacing='0' border='0'>"
            "<tr style='background-color:#fffbeb;'>"
            "<th style='padding:7px 12px;font-size:12px;font-weight:700;color:#92400e;text-align:left;border-bottom:1px solid #fde68a;'>학교명</th>"
            "<th style='padding:7px 12px;font-size:12px;font-weight:700;color:#92400e;text-align:left;border-bottom:1px solid #fde68a;'>납품업체</th>"
            "<th style='padding:7px 12px;font-size:12px;font-weight:700;color:#92400e;text-align:right;border-bottom:1px solid #fde68a;'>금액</th>"
            "</tr>"
            f"{school_rows_html}"
            f"<tr style='background-color:#fef9ec;'>"
            f"<td colspan='2' style='padding:8px 12px;font-size:13px;font-weight:700;color:#92400e;'>합계 ({len(school_stats)}개교)</td>"
            f"<td style='padding:8px 12px;font-size:13px;font-weight:700;color:#92400e;text-align:right;'>{fmt_amount_full(school_total_amt)}</td>"
            f"</tr></table>"
        )
    else:
        school_table = "<p style='color:#9ca3af;font-size:13px;padding:14px 16px;'>해당 내역 없음</p>"
 
    innodep_rows_html = ""
    if innodep_org_summary:
        sorted_rows = sorted(innodep_org_summary.items(), key=lambda x: x[1], reverse=True)
        for i, (org, amt) in enumerate(sorted_rows):
            row_bg = "#ffffff" if i % 2 == 0 else "#f8faff"
            innodep_rows_html += (
                f"<tr style='background-color:{row_bg};'>"
                f"<td style='padding:7px 12px;font-size:13px;color:#374151;border-bottom:1px solid #f0f4ff;'>{org}</td>"
                f"<td style='padding:7px 12px;font-size:13px;color:#374151;text-align:right;border-bottom:1px solid #f0f4ff;'>{fmt_amount_full(amt)}</td>"
                f"</tr>"
            )
 
        innodep_table = (
            "<table width='100%' cellpadding='0' cellspacing='0' border='0'>"
            "<tr style='background-color:#eff6ff;'>"
            "<th style='padding:7px 12px;font-size:12px;font-weight:700;color:#1e3a5f;text-align:left;border-bottom:1px solid #bfdbfe;'>수요기관</th>"
            "<th style='padding:7px 12px;font-size:12px;font-weight:700;color:#1e3a5f;text-align:right;border-bottom:1px solid #bfdbfe;'>금액</th>"
            "</tr>"
            f"{innodep_rows_html}"
            f"<tr style='background-color:#dbeafe;'>"
            f"<td style='padding:8px 12px;font-size:13px;font-weight:700;color:#1e3a5f;'>합계 ({len(innodep_org_summary)}개 기관)</td>"
            f"<td style='padding:8px 12px;font-size:13px;font-weight:700;color:#1e3a5f;text-align:right;'>{fmt_amount_full(innodep_total_amt)}</td>"
            f"</tr></table>"
        )
    else:
        innodep_table = "<p style='color:#9ca3af;font-size:13px;padding:14px 16px;'>해당 내역 없음</p>"
 
 
    notice_blocks = "".join(build_category_section(cat, notice_mail_buckets[cat]) for cat in CAT_KEYWORDS)
    contract_blocks = "".join(build_category_section(cat, contract_mail_buckets[cat]) for cat in CAT_KEYWORDS)
 
    dashboard_links = """
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:12px;background-color:#ffffff;border-radius:8px;border:1px solid #e5e7eb;">
      <tr>
        <td align="center" style="padding:14px 16px;font-size:14pt;line-height:1.6;color:#1f2937;">
          <span style="font-weight:700;color:#1e3a5f;">대시보드 연결 :</span>
          <a href="http://211.171.190.220:3001" target="_blank"
             style="color:#2d7dd2;text-decoration:none;font-weight:700;">외부접속</a>
          <span style="color:#9ca3af;"> / </span>
          <a href="http://dashboard.innodep.com:3001/" target="_blank"
             style="color:#10b981;text-decoration:none;font-weight:700;">내부접속</a>
        </td>
      </tr>
    </table>
    """
 
 
    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<title>Innodep 조달청 데이터 수집 리포트</title>
</head>
<body style="margin:0;padding:0;background-color:#f0f4f8;font-family:'맑은 고딕','Apple SD Gothic Neo',Arial,sans-serif;-webkit-text-size-adjust:100%;mso-line-height-rule:exactly;">
 
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#f0f4f8;">
<tr><td align="center" style="padding:24px 12px;">
 
<table width="680" cellpadding="0" cellspacing="0" border="0" style="max-width:680px;width:100%;">
 
<tr>
  <td style="padding-bottom:12px;">
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#1e3a5f;border-radius:12px;overflow:hidden;">
      <tr><td colspan="2" height="3" style="background-color:#2d7dd2;font-size:0;line-height:0;">&nbsp;</td></tr>
      <tr>
        <td style="padding:24px 24px 18px;">
          <p style="margin:0 0 4px 0;font-size:12px;letter-spacing:3px;color:#7eb8f7;font-weight:700;text-transform:uppercase;">Innodep &middot; Procurement Intelligence</p>
          <p style="margin:0;font-size:24px;font-weight:700;color:#f0f7ff;letter-spacing:-0.5px;">조달청 데이터 수집 리포트</p>
        </td>
        <td style="padding:24px 24px 18px;text-align:right;vertical-align:middle;">
          <p style="margin:0 0 2px 0;font-size:12px;color:#5a87b8;letter-spacing:1px;">기준일 (어제)</p>
          <p style="margin:0 0 4px 0;font-size:15px;font-weight:700;color:#7eb8f7;">{display_date} ({weekday_str})</p>
          <p style="margin:0;font-size:12px;color:#4a9d6e;font-weight:700;letter-spacing:1px;">&#9679; AUTO COLLECTED</p>
        </td>
      </tr>
    </table>
  </td>
</tr>
 
<tr><td style="padding-bottom:12px;">{stat_cards}</td></tr>
 
<tr>
  <td style="padding-bottom:8px;">
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#fffbeb;border-radius:8px;border-left:4px solid #f59e0b;overflow:hidden;">
      <tr><td style="padding:12px 16px;">
        <p style="margin:0;font-size:14px;font-weight:700;color:#92400e;">
          &#128722; 종합쇼핑몰 3자단가
          <span style="font-size:13px;font-weight:400;color:#b45309;">&nbsp;— 어제 기준 일매출 집계</span>
        </p>
      </td></tr>
    </table>
  </td>
</tr>
 
<tr>
  <td style="padding-bottom:12px;">
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#ffffff;border-radius:8px;border:1px solid #e5e7eb;overflow:hidden;">
      <tr>
        <td style="padding:14px 16px 10px;border-bottom:1px solid #f3f4f6;">
          <p style="margin:0;font-size:14px;font-weight:700;color:#374151;">
            &#128202; 경쟁사 납품금액 TOP 10
          </p>
          <p style="margin:4px 0 0 0;font-size:12px;color:#9ca3af;">어제 기준 · 경쟁사 중 상위 10개</p>
        </td>
      </tr>
      <tr><td style="padding:12px 16px 14px;">{build_vendor_chart(vendor_stats)}</td></tr>
    </table>
  </td>
</tr>
 
<tr>
  <td style="padding-bottom:12px;">
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#ffffff;border-radius:8px;border:1px solid #e5e7eb;overflow:hidden;">
      <tr>
        <td style="padding:14px 16px 10px;border-bottom:1px solid #f3f4f6;">
          <p style="margin:0;font-size:14px;font-weight:700;color:#374151;">
            &#127963; 수요기관 납품금액 TOP 10
          </p>
          <p style="margin:4px 0 0 0;font-size:12px;color:#9ca3af;">어제 기준 · 기관별 합산 상위 10개</p>
        </td>
      </tr>
      <tr><td style="padding:12px 16px 14px;">{build_org_chart(org_stats)}</td></tr>
    </table>
  </td>
</tr>
 
 
<tr>
  <td style="padding-bottom:12px;">
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#ffffff;border-radius:8px;border:1px solid #e5e7eb;overflow:hidden;">
      <tr>
        <td style="padding:12px 16px;background-color:#fffbeb;border-bottom:2px solid #fde68a;">
          <p style="margin:0;font-size:14px;font-weight:700;color:#92400e;">&#127979; 학교 지능형 CCTV 납품현황</p>
        </td>
      </tr>
      <tr><td>{school_table}</td></tr>
    </table>
  </td>
</tr>
 
<tr>
  <td style="padding-bottom:12px;">
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#ffffff;border-radius:8px;border:1px solid #e5e7eb;overflow:hidden;">
      <tr>
        <td style="padding:12px 16px;background-color:#eff6ff;border-bottom:2px solid #bfdbfe;">
          <p style="margin:0;font-size:14px;font-weight:700;color:#1e3a5f;">&#11088; 이노뎁 납품 실적</p>
        </td>
      </tr>
      <tr><td>{innodep_table}</td></tr>
    </table>
  </td>
</tr>
 
<tr><td style="padding-bottom:12px;">{dashboard_links}</td></tr>
 
<tr>
  <td style="padding-bottom:8px;">
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#fef2f2;border-radius:8px;border-left:4px solid #e03444;overflow:hidden;">
      <tr><td style="padding:12px 16px;">
        <p style="margin:0;font-size:14px;font-weight:700;color:#991b1b;">
          &#128226; 나라장터 입찰공고
          <span style="font-size:13px;font-weight:400;color:#b91c1c;">&nbsp;— 핵심 사업 중심 요약</span>
        </p>
      </td></tr>
    </table>
  </td>
</tr>
 
<tr>
  <td style="padding-bottom:12px;">
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#ffffff;border-radius:8px;border:1px solid #e5e7eb;overflow:hidden;">
      <tr><td style="padding:16px 16px 8px;">{notice_blocks}</td></tr>
    </table>
  </td>
</tr>
 
<tr>
  <td style="padding-bottom:8px;">
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#eff6ff;border-radius:8px;border-left:4px solid #2d7dd2;overflow:hidden;">
      <tr><td style="padding:12px 16px;">
        <p style="margin:0;font-size:14px;font-weight:700;color:#1e3a5f;">
          &#128221; 나라장터 계약내역
          <span style="font-size:13px;font-weight:400;color:#2d7dd2;">&nbsp;— 핵심 사업 중심 요약</span>
        </p>
      </td></tr>
    </table>
  </td>
</tr>
 
<tr>
  <td style="padding-bottom:12px;">
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#ffffff;border-radius:8px;border:1px solid #e5e7eb;overflow:hidden;">
      <tr><td style="padding:16px 16px 8px;">{contract_blocks}</td></tr>
    </table>
  </td>
</tr>
 
<tr>
  <td style="padding-top:8px;padding-bottom:8px;text-align:center;">
    <p style="margin:0;font-size:12px;color:#9ca3af;letter-spacing:0.5px;">
      본 메일은 GitHub Actions 자동화 스크립트로 발송됩니다 &nbsp;&middot;&nbsp; Innodep Procurement Bot
    </p>
  </td>
</tr>
 
</table>
</td></tr>
</table>
</body>
</html>"""
 
 
# =================================================================================
# 3. 메인 로직
# =================================================================================
def main():
    if not MY_DIRECT_KEY or not AUTH_JSON_STR:
        return
 
    target_dates = get_target_dates()  # 오름차순 datetime 리스트 (월요일은 금/토/일 3개)
    send_mail = should_send_mail()
 
    d_strs = [dt.strftime("%Y%m%d") for dt in target_dates]
 
    if len(target_dates) == 1:
        display_date = target_dates[0].strftime("%Y.%m.%d")
        weekday_str = ["월", "화", "수", "목", "금", "토", "일"][target_dates[0].weekday()]
    else:
        first_d = target_dates[0].strftime("%Y.%m.%d")
        last_d = target_dates[-1].strftime("%Y.%m.%d")
        display_date = f"{first_d} ~ {last_d}"
        weekday_str = "금~일, 주말 포함 통합"
 
    drive_service, drive_creds = get_drive_service_for_script()
    keywords_notice_all = [kw for sublist in CAT_KEYWORDS.values() for kw in sublist]
 
    target_companies = get_target_companies()
    normalized_target_companies = {normalize_company_name(name) for name in target_companies}
 
    # ---------------- 날짜(들)에 걸쳐 누적할 집계 변수 ----------------
    final_data_all = []
    school_stats = {}
    innodep_org_summary = {}
    innodep_detail_map = {}
    innodep_total_amt = 0
    vendor_stats = {}
    org_stats = {}
    notice_mail_buckets = {cat: [] for cat in CAT_KEYWORDS}
    contract_mail_buckets = {cat: [] for cat in CAT_KEYWORDS}
    all_notice_count = 0
    unique_servc_list_all = []
 
    for d_str in d_strs:
        # -------------------------------------------------------------------
        # PART 1: 종합쇼핑몰 3자단가 수집
        # -------------------------------------------------------------------
        final_data = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(fetch_api_data_from_g2b, kw, d_str): kw for kw in keywords}
            for future in as_completed(futures):
                data = future.result()
                if data:
                    final_data.extend(data)
 
        if final_data:
            new_df = pd.DataFrame(final_data, columns=HEADER_KOR)
            d_year = d_str[:4]
 
            query = f"name='{d_year}.csv' and '{SHOPPING_FOLDER_ID}' in parents and trashed=false"
            res = drive_service.files().list(q=query, fields="files(id)").execute()
            items = res.get("files", [])
            f_id = items[0]["id"] if items else None
 
            if f_id:
                resp = requests.get(
                    f"https://www.googleapis.com/drive/v3/files/{f_id}?alt=media",
                    headers={"Authorization": f"Bearer {drive_creds.token}"},
                )
                old_df = pd.read_csv(io.BytesIO(resp.content), encoding="utf-8-sig", low_memory=False)
                df_to_upload = pd.concat([old_df, new_df], ignore_index=True).drop_duplicates(
                    subset=["계약납품요구일자", "수요기관명", "품명", "금액"],
                    keep="last",
                )
                media = MediaIoBaseUpload(
                    io.BytesIO(df_to_upload.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")),
                    mimetype="text/csv",
                )
                drive_service.files().update(fileId=f_id, media_body=media).execute()
            else:
                media = MediaIoBaseUpload(
                    io.BytesIO(new_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")),
                    mimetype="text/csv",
                )
                drive_service.files().create(
                    body={"name": f"{d_year}.csv", "parents": [SHOPPING_FOLDER_ID]},
                    media_body=media,
                ).execute()
                print(f"✅ 종합쇼핑몰 {d_year}.csv 신규 생성 완료")
 
            for row in final_data:
                org = str(row[7])
                comp = str(row[21])
                amt_val = str(row[33])
                item_nm = str(row[14])
                cntrct = str(row[23])
 
                try:
                    amt = int(amt_val.replace(",", "").split(".")[0])
                except Exception:
                    amt = 0
 
                org_stats[org] = org_stats.get(org, 0) + amt
 
                normalized_company = normalize_company_name(comp)
                if normalized_company in normalized_target_companies or "이노뎁" in comp:
                    vendor_stats[comp] = vendor_stats.get(comp, 0) + amt
 
                if "학교" in org and "지능형" in cntrct and "CCTV" in cntrct:
                    if org not in school_stats:
                        school_stats[org] = {"total_amt": 0, "main_vendor": comp}
                    school_stats[org]["total_amt"] += amt
 
                if "이노뎁" in comp:
                    innodep_org_summary[org] = innodep_org_summary.get(org, 0) + amt
 
                    if org not in innodep_detail_map:
                        innodep_detail_map[org] = []
 
                    innodep_detail_map[org].append({
                        "nm": cntrct if cntrct and cntrct != "nan" else item_nm,
                        "amt": amt,
                    })
 
                    innodep_total_amt += amt
 
        final_data_all.extend(final_data)
 
        # -------------------------------------------------------------------
        # PART 2: 나라장터 입찰 공고 수집
        # -------------------------------------------------------------------
        d_year_int = int(d_str[:4])
        for cat_api, api_url in NOTICE_API_MAP.items():
            n_df = fetch_notice_data(cat_api, api_url, d_str)
            if not n_df.empty:
                all_notice_count += len(n_df)
                save_notice_by_year(drive_service, drive_creds, cat_api, n_df, d_year_int)
 
                pattern = "|".join(keywords_notice_all)
                filtered = n_df[n_df["bidNtceNm"].str.contains(pattern, na=False, case=False)]
                for _, row in filtered.iterrows():
                    cat_found = classify_text(row["bidNtceNm"])
                    if cat_found in notice_mail_buckets:
                        notice_mail_buckets[cat_found].append({
                            "org": row.get("dminsttNm", "-"),
                            "nm": row.get("bidNtceNm", "-"),
                            "amt": row.get("presmptPrce", "별도공고"),
                            "url": row.get("bidNtceDtlUrl", "#"),
                            "corp": "이노뎁" if "이노뎁" in str(row.get("bidNtceNm", "")) else "-",
                        })
 
        # -------------------------------------------------------------------
        # PART 3: 나라장터 용역 계약 내역 수집
        # -------------------------------------------------------------------
        collected_servc = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(fetch_single_contract, kw_s, d_str): kw_s
                for kw_s in keywords_notice_all
            }
            for future in as_completed(futures):
                collected_servc.extend(future.result())
 
        unique_servc_list = list({f"{d['org']}_{d['nm']}": d for d in collected_servc}.values())
        for s in unique_servc_list:
            cat_found = classify_text(s["nm"])
            if cat_found in contract_mail_buckets:
                contract_mail_buckets[cat_found].append(s)
 
        unique_servc_list_all.extend(unique_servc_list)
 
    # -------------------------------------------------------------------
    # 국방 카테고리 제외 필터 (날짜 통합 후 1회 적용)
    # -------------------------------------------------------------------
    exclude_keywords = ["학교", "민방위", "교육청"]
 
    def is_valid_org(org_name):
        return not any(word in org_name for word in exclude_keywords)
 
    notice_mail_buckets["국방"] = [i for i in notice_mail_buckets["국방"] if is_valid_org(i["org"])]
    contract_mail_buckets["국방"] = [i for i in contract_mail_buckets["국방"] if is_valid_org(i["org"])]
 
    # -------------------------------------------------------------------------
    # PART 4: 최종 HTML 리포트 조립
    # -------------------------------------------------------------------------
    report_html = build_report_html(
        display_date=display_date,
        weekday_str=weekday_str,
        shopping_cnt=len(final_data_all),
        notice_cnt=all_notice_count,
        contract_cnt=len(unique_servc_list_all),
        school_stats=school_stats,
        innodep_org_summary=innodep_org_summary,
        innodep_detail_map=innodep_detail_map,
        innodep_total_amt=innodep_total_amt,
        vendor_stats=vendor_stats,
        org_stats=org_stats,
        notice_mail_buckets=notice_mail_buckets,
        contract_mail_buckets=contract_mail_buckets,
    )
 
    # -------------------------------------------------------------------------
    # PART 5: GitHub Actions output 기록
    # -------------------------------------------------------------------------
    if "GITHUB_OUTPUT" in os.environ:
        report_path = f"/tmp/report_{d_strs[-1]}.html"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_html)
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:
            f.write(f"collect_date={d_strs[-1]}\n")
            f.write(f"report_path={report_path}\n")
            f.write(f"send_mail={'true' if send_mail else 'false'}\n")
 
 
if __name__ == "__main__":
    main()
