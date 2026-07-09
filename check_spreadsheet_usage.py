import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

GOOGLE_AUTH_JSON = os.environ['GOOGLE_AUTH_JSON']

# 확인하고 싶은 스프레드시트 이름들
SPREADSHEET_NAMES = [
    "군수품조달_국내_발주계획",
    "군수품조달_국내_계약정보",
    "군수품조달_국내_입찰공고",
]


def main():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_AUTH_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    for name in SPREADSHEET_NAMES:
        print(f"\n================= {name} =================")
        try:
            spreadsheet = client.open(name)
        except Exception as e:
            print(f"  열기 실패: {e}")
            continue

        total_cells = 0
        for ws in spreadsheet.worksheets():
            cells = ws.row_count * ws.col_count
            total_cells += cells
            print(f"  - [{ws.title}] {ws.row_count}행 x {ws.col_count}열 = {cells:,}셀")

        print(f"  >>> 전체 합계: {total_cells:,} / 10,000,000 셀 "
              f"({total_cells / 10_000_000 * 100:.1f}%)")


if __name__ == "__main__":
    main()
