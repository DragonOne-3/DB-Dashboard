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
    "스마트도시": ["ITS", "스마트시티", "스마트도시"],
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
}

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


def get_target_date():
    now = datetime.datetime.now(datetime.UTC) + datetime.timedelta(hours=9)
    return now - datetime.timedelta(days=1)


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
    api_url_servc = "http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch"
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

    target_dt = get_target_date()
    d_str = target_dt.strftime("%Y%m%d")
    year = target_dt.year
    display_date = target_dt.strftime("%Y.%m.%d")
    weekday_str = ["월", "화", "수", "목", "금", "토", "일"][target_dt.weekday()]

    drive_service, drive_creds = get_drive_service_for_script()
    keywords_notice_all = [kw for sublist in CAT_KEYWORDS.values() for kw in sublist]

    target_companies = get_target_companies()
    normalized_target_companies = {normalize_company_name(name) for name in target_companies}

    # -------------------------------------------------------------------------
    # PART 1: 종합쇼핑몰 3자단가 수집
    # -------------------------------------------------------------------------
    final_data = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_api_data_from_g2b, kw, d_str): kw for kw in keywords}
        for future in as_completed(futures):
            data = future.result()
            if data:
                final_data.extend(data)

    school_stats = {}
    innodep_org_summary = {}
    innodep_detail_map = {}
    innodep_total_amt = 0
    vendor_stats = {}
    org_stats = {}

    if final_data:
        new_df = pd.DataFrame(final_data, columns=HEADER_KOR)

        query = f"name='{target_dt.year}.csv' and '{SHOPPING_FOLDER_ID}' in parents and trashed=false"
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
                body={"name": f"{target_dt.year}.csv", "parents": [SHOPPING_FOLDER_ID]},
                media_body=media,
            ).execute()
            print(f"✅ 종합쇼핑몰 {target_dt.year}.csv 신규 생성 완료")

        for row in final_data:
            org = str(row[7])
            comp = str(row[21])
            amt_val = str(row[20])
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


    # -------------------------------------------------------------------------
    # PART 2: 나라장터 입찰 공고 수집
    # -------------------------------------------------------------------------
    notice_mail_buckets = {cat: [] for cat in CAT_KEYWORDS}
    all_notice_count = 0

    for cat_api, api_url in NOTICE_API_MAP.items():
        n_df = fetch_notice_data(cat_api, api_url, d_str)
        if not n_df.empty:
            all_notice_count += len(n_df)
            save_notice_by_year(drive_service, drive_creds, cat_api, n_df, year)

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

    # -------------------------------------------------------------------------
    # PART 3: 나라장터 용역 계약 내역 수집
    # -------------------------------------------------------------------------
    contract_mail_buckets = {cat: [] for cat in CAT_KEYWORDS}
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

    exclude_keywords = ["학교", "민방위", "교육청"]

    def is_valid_org(org_name):
        for word in exclude_keywords:
            if word in org_name:
                return False
        return True

    notice_mail_buckets["국방"] = [i for i in notice_mail_buckets["국방"] if is_valid_org(i["org"])]
    contract_mail_buckets["국방"] = [i for i in contract_mail_buckets["국방"] if is_valid_org(i["org"])]

    # -------------------------------------------------------------------------
    # PART 4: 최종 HTML 리포트 조립
    # -------------------------------------------------------------------------
    report_html = build_report_html(
        display_date=display_date,
        weekday_str=weekday_str,
        shopping_cnt=len(final_data),
        notice_cnt=all_notice_count,
        contract_cnt=len(unique_servc_list),
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
        report_path = f"/tmp/report_{d_str}.html"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_html)
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:
            f.write(f"collect_date={d_str}\n")
            f.write(f"report_path={report_path}\n")


if __name__ == "__main__":
    main()
