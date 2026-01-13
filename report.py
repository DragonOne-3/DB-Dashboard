import os, json, datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# 환경 변수 로드
AUTH_JSON_STR = os.environ.get('GOOGLE_AUTH_JSON')

def get_last_week_range():
    """지난주 월요일~일요일 날짜 계산"""
    today = datetime.date.today()
    # 실행일(월요일) 기준 지난주 월요일(-7) ~ 일요일(-1)
    last_monday = today - datetime.timedelta(days=today.weekday() + 7)
    last_sunday = last_monday + datetime.timedelta(days=6)
    return last_monday, last_sunday

def main():
    if not AUTH_JSON_STR:
        print("❌ 에러: GOOGLE_AUTH_JSON 환경변수가 없습니다.")
        return

    # 구글 인증
    creds_dict = json.loads(AUTH_JSON_STR)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        creds_dict, 
        ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    )
    client = gspread.authorize(creds)
    
    # 날짜 및 파일 설정
    last_mon, last_sun = get_last_week_range()
    year = last_mon.year
    quarter = (last_mon.month - 1) // 3 + 1
    file_name = f"조달청_납품내역_{year}_{quarter}분기"
    
    print(f"📅 분석 기간: {last_mon} ~ {last_sun}")
    
    try:
        sh = client.open(file_name)
        months = list(set([last_mon.month, last_sun.month]))
        all_data = []

        for m in months:
            sheet_name = f"{year}_{m}월"
            try:
                ws = sh.worksheet(sheet_name)
                all_data.extend(ws.get_all_records())
            except:
                print(f"⚠️ {sheet_name} 시트가 없습니다. 건너뜁니다.")

        # 기업별 매출 합산
        summary = {}
        for row in all_data:
            d_val = str(row.get('계약납품요구일자', ''))
            if len(d_val) == 8:
                try:
                    row_date = datetime.datetime.strptime(d_val, "%Y%m%d").date()
                    if last_mon <= row_date <= last_sun:
                        comp = row.get('업체명', '알수없음')
                        # 금액 콤마 제거 및 정수 변환
                        amt_raw = str(row.get('금액', 0)).replace(',', '').split('.')[0]
                        amt = int(amt_raw) if amt_raw else 0
                        summary[comp] = summary.get(comp, 0) + amt
                except: continue

        # 상위 10개 정렬
        sorted_list = sorted(summary.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # --- 메일 본문 생성 ---
        report_body = f"📊 지난주 조달청 매출 순위 리포트\n"
        report_body += f"기간: {last_mon} ~ {last_sun}\n"
        report_body += "="*40 + "\n\n"
        
        if not sorted_list:
            report_body += "해당 기간에 집계된 매출 데이터가 없습니다."
        else:
            for i, (name, val) in enumerate(sorted_list, 1):
                report_body += f"{i}위. {name}\n   - 매출액: {val:,}원\n"
        
        report_body += "\n" + "="*40 + "\n"
        report_body += "본 메일은 시스템에서 자동으로 발송되었습니다."

        # --- GitHub Actions를 위한 결과 전달 ---
        # 1. 메일 제목에 쓸 날짜 범위 전달
        if "GITHUB_OUTPUT" in os.environ:
            with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                f.write(f"report_range={last_mon}~{last_sun}\n")
        
        # 2. 메일 본문 파일 생성 (mail_body.txt)
        with open("mail_body.txt", "w", encoding="utf-8") as bf:
            bf.write(report_body)

        print("✅ 리포트 생성 및 파일 저장 완료")

    except Exception as e:
        print(f"🔥 치명적 에러: {e}")

if __name__ == "__main__":
    main()
