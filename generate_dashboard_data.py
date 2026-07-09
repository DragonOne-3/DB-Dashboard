import os
import json
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials

GOOGLE_AUTH_JSON = os.environ['GOOGLE_AUTH_JSON']

KEYWORD = os.environ.get('DASHBOARD_KEYWORD', '음식물')

SOURCE_SPREADSHEETS = {
    "발주계획": "군수품조달_국내_발주계획",
    "계약정보": "군수품조달_국내_계약정보",
    "입찰공고": "군수품조달_국내_입찰공고",
}

EXCLUDE_TABS = {"백필_진행상황"}

# ---- 영업 관점 분류 키워드 ----
EQUIPMENT_KEYWORDS = [
    "구매", "임차", "렌탈", "리스", "납품", "설치",
    "처리기", "분쇄기", "건조기", "감량기", "감량기기",
    "처리대", "처리통", "처리기기",
]

OUTPUT_PATH = os.environ.get('DASHBOARD_OUTPUT_PATH', 'docs/data.json')


def classify_row(value_row, keyword):
    """
    행 전체가 아니라, 실제로 keyword('음식물')가 들어있는 '제목성' 필드만 보고 분류.
    (집행유형=구매, 계약방법=총액제 같은 무관한 행정 필드에 우연히
     '구매'/'설치' 같은 단어가 들어있어서 오분류되는 것을 방지)
    """
    title_like = None
    for v in value_row:
        if v and keyword in str(v):
            title_like = str(v)
            break
    if title_like is None:
        title_like = " ".join(str(v) for v in value_row if v)
    if any(kw in title_like for kw in EQUIPMENT_KEYWORDS):
        return "EQUIPMENT"
    return "OTHER"


def row_matches_keyword(value_row, keyword):
    for v in value_row:
        if v and keyword in str(v):
            return True
    return False


def extract_year(row_dict):
    """필드명에 '연도'가 들어간 컬럼을 우선 사용, 없으면 '월'/'일자'/'날짜'가 들어간
    컬럼에서 앞 4자리 숫자를 연도로 추정."""
    for k, v in row_dict.items():
        if v and '연도' in k:
            digits = ''.join(ch for ch in str(v) if ch.isdigit())
            if len(digits) >= 4:
                return digits[:4]
    for k, v in row_dict.items():
        if v and any(tag in k for tag in ('월', '일자', '날짜')):
            digits = ''.join(ch for ch in str(v) if ch.isdigit())
            if len(digits) >= 4:
                return digits[:4]
    return None


def extract_amount(row_dict):
    """필드명에 '금액' 또는 '가격'이 들어간 첫 번째 컬럼 값을 숫자로 변환."""
    for k, v in row_dict.items():
        if v and ('금액' in k or '가격' in k):
            digits = ''.join(ch for ch in str(v) if ch.isdigit())
            if digits:
                try:
                    return int(digits)
                except ValueError:
                    continue
    return None


def scan_spreadsheet(client, spreadsheet_name, keyword):
    spreadsheet = client.open(spreadsheet_name)

    all_headers = []
    equipment_rows = []
    other_rows = []
    seen = set()

    for ws in spreadsheet.worksheets():
        if ws.title in EXCLUDE_TABS:
            continue

        values = ws.get_all_values()
        if not values or len(values) < 2:
            continue

        header_row = values[0]
        for h in header_row:
            if h and h not in all_headers:
                all_headers.append(h)

        for value_row in values[1:]:
            if not any(value_row):
                continue
            if not row_matches_keyword(value_row, keyword):
                continue

            key = tuple(value_row)
            if key in seen:
                continue
            seen.add(key)

            row_dict = dict(zip(header_row, value_row))
            row_dict["_tab"] = ws.title

            if classify_row(value_row, keyword) == "EQUIPMENT":
                equipment_rows.append(row_dict)
            else:
                other_rows.append(row_dict)

    if "_tab" not in all_headers:
        all_headers.append("_tab")

    return all_headers, equipment_rows, other_rows


def main():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    result = {
        "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "keyword": KEYWORD,
        "sources": {},
        "summary": [],
        "yearly": {},
    }

    for label, spreadsheet_name in SOURCE_SPREADSHEETS.items():
        print(f"=== {label} ({spreadsheet_name}) 스캔 중... ===")
        headers, equipment_rows, other_rows = scan_spreadsheet(client, spreadsheet_name, KEYWORD)
        print(f"  음식물처리기(장비): {len(equipment_rows)}건 / 그 외 음식물: {len(other_rows)}건")

        result["sources"][label] = {
            "columns": headers,
            "equipment": equipment_rows,
            "other": other_rows,
        }
        result["summary"].append({"source": label, "category": "음식물처리기", "count": len(equipment_rows)})
        result["summary"].append({"source": label, "category": "그외음식물", "count": len(other_rows)})

        # ---- 연도별 금액 집계 ----
        yearly = {}  # {year: {"equipment_sum":.., "equipment_count":.., "other_sum":.., "other_count":..}}

        def add_to_yearly(rows, category_key):
            for row in rows:
                year = extract_year(row)
                amount = extract_amount(row)
                if year is None:
                    year = "미상"
                bucket = yearly.setdefault(year, {
                    "equipment_sum": 0, "equipment_count": 0,
                    "other_sum": 0, "other_count": 0,
                })
                bucket[f"{category_key}_count"] += 1
                if amount:
                    bucket[f"{category_key}_sum"] += amount

        add_to_yearly(equipment_rows, "equipment")
        add_to_yearly(other_rows, "other")

        result["yearly"][label] = yearly

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n✅ {OUTPUT_PATH} 생성 완료.")


if __name__ == "__main__":
    main()
