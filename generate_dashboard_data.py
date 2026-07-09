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


def classify_row(value_row):
    joined = " ".join(str(v) for v in value_row if v)
    if any(kw in joined for kw in EQUIPMENT_KEYWORDS):
        return "EQUIPMENT"
    return "OTHER"


def row_matches_keyword(value_row, keyword):
    for v in value_row:
        if v and keyword in str(v):
            return True
    return False


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

            if classify_row(value_row) == "EQUIPMENT":
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

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n✅ {OUTPUT_PATH} 생성 완료.")


if __name__ == "__main__":
    main()
