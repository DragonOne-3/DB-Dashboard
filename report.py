import os, json, datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# 환경 변수 로드
AUTH_JSON_STR = os.environ.get('GOOGLE_AUTH_JSON')

def get_last_week_range():
    """지난주 월요일~일요일 날짜 계산"""
    today = datetime.date.today()
    # 오늘(월) 기준 7일 전이 지난주 월요일
    last_monday = today - datetime.timedelta(days=today.weekday() + 7)
    last_sunday = last_monday + datetime.timedelta(days=6)
    return last_monday, last_sunday

def main():
    if not AUTH_JSON_STR:
        print("❌ 에러: GOOGLE_AUTH_JSON 환경변수가 설정되지 않았습니다.")
        return

    # 1. 구글 서비스 계정 인증
    creds_dict = json.loads(AUTH_JSON_STR)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        creds_dict, 
        ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    )
    client = gspread.authorize(creds)
    
    # 2. 분석 날짜 설정
    last_mon, last_sun = get_last_week_range()
    year = last_mon.year
    quarter = (last_mon.month - 1) // 3 + 1
    file_name = f"조달청_납품내역_{year}_{quarter}분기"
    
    print(f"📅 분석 범위: {last_mon} ~ {last_sun}")

    try:
        # 3. 데이터 로드
        sh = client.open(file_name)
        months = list(set([last_mon.month, last_sun.month]))
        all_data = []

        for m in months:
            sheet_name = f"{year}_{m}월"
            try:
                ws = sh.worksheet(sheet_name)
                all_data.extend(ws.get_all_records())
            except:
                print(f"⚠️ {sheet_name} 시트를 찾을 수 없어 건너뜁니다.")

        # 4. 업체명 기준 매출 합산
        summary = {}
        for row in all_data:
            date_val = str(row.get('계약납품요구일자', ''))
            if len(date_val) == 8:
                try:
                    row_date = datetime.datetime.strptime(date_val, "%Y%m%d").date()
                    if last_mon <= row_date <= last_sun:
                        comp = row.get('업체명', '알수없음')
                        # 금액 데이터 정제 (콤마 제거 등)
                        amt_raw = str(row.get('금액', 0)).replace(',', '').split('.')[0]
                        amt = int(amt_raw) if amt_raw else 0
                        summary[comp] = summary.get(comp, 0) + amt
                except:
                    continue

        # 5. 상위 10개 정렬
        sorted_list = sorted(summary.items(), key=lambda x: x[1], reverse=True)[:10]

        # 6. HTML 표 메일 본문 생성
        html_report = f"""
        <html>
        <body style="font-family: 'Malgun Gothic', sans-serif; line-height: 1.6;">
            <h2 style="color: #2E75B6; border-bottom: 2px solid #2E75B6; padding-bottom: 10px;">📊 주간 조달청 매출 순위 리포트</h2>
            <p>지난주 <b>{last_mon} ~ {last_sun}</b> 기간의 기업별 매출 분석 결과입니다.</p>
            <table border="1" style="border-collapse: collapse; width: 100%; max-width: 600px; margin-top: 20px;">
                <thead>
                    <tr style="background-color: #DDEBF7; text-align: center;">
                        <th style="padding: 12px; border: 1px solid #A5A5A5;">순위</th>
                        <th style="padding: 12px; border: 1px solid #A5A5A5;">업체명</th>
                        <th style="padding: 12px; border: 1px solid #A5A5A5;">매출 합계</th>
                    </tr>
                </thead>
                <tbody>
        """

        if not sorted_list:
            html_report += '<tr><td colspan="3" style="padding: 20px; text-align: center; border: 1px solid #A5A5A5;">해당 기간 데이터가 없습니다.</td></tr>'
        else:
            for i, (name, val) in enumerate(sorted_list, 1):
                html_report += f"""
                    <tr style="text-align: left;">
                        <td style="padding: 10px; text-align: center; border: 1px solid #A5A5A5;">{i}</td>
                        <td style="padding: 10px; border: 1px solid #A5A5A5;"><b>{name}</b></td>
                        <td style="padding: 10px; text-align: right; border: 1px solid #A5A5A5;">{val:,}원</td>
                    </tr>
                """

        html_report += """
                </tbody>
            </table>
            <p style="color: #7F7F7F; font-size: 11px; margin-top: 30px;">※ 본 리포트는 GitHub Actions 시스템에서 자동 발송되었습니다.</p>
        </body>
        </html>
        """

        # 7. GitHub Actions 변수 전달 및 파일 저장
        report_range = f"{last_mon} ~ {last_sun}"
        if "GITHUB_OUTPUT" in os.environ:
            with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                f.write(f"report_range={report_range}\n")

        with open("report.html", "w", encoding="utf-8") as f:
            f.write(html_report)

        print(f"✅ 리포트 생성 완료 ({len(sorted_list)}개 업체 집계)")

    except Exception as e:
        print(f"🔥 오류 발생: {e}")

if __name__ == "__main__":
    main()
