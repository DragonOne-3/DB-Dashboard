import os, json, datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# 환경 변수 로드
AUTH_JSON_STR = os.environ.get('GOOGLE_AUTH_JSON')

def get_target_companies():
    """companies.txt 파일에서 업체 리스트를 읽어옴"""
    file_path = "companies.txt"
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            # 줄바꿈 제거하고 빈 줄은 제외하고 리스트 생성
            return [line.strip() for line in f if line.strip()]
    else:
        # 파일이 없을 경우 기본값
        return ["이노뎁"]

def get_last_week_range():
    today = datetime.date.today()
    last_monday = today - datetime.timedelta(days=today.weekday() + 7)
    last_sunday = last_monday + datetime.timedelta(days=6)
    return last_monday, last_sunday

def main():
    if not AUTH_JSON_STR:
        print("❌ 에러: GOOGLE_AUTH_JSON 누락")
        return

    # 업체 리스트 불러오기
    target_companies = get_target_companies()
    
    creds_dict = json.loads(AUTH_JSON_STR)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    
    last_mon, last_sun = get_last_week_range()
    year, quarter = last_mon.year, (last_mon.month - 1) // 3 + 1
    file_name = f"조달청_납품내역_{year}_{quarter}분기"
    
    try:
        sh = client.open(file_name)
        months = list(set([last_mon.month, last_sun.month]))
        all_data = []

        for m in months:
            try:
                ws = sh.worksheet(f"{year}_{m}월")
                all_data.extend(ws.get_all_records())
            except: continue

        # 1. 필터링 및 합산
        summary = {comp: 0 for comp in target_companies} # 리스트에 있는 업체 0으로 초기화
        for row in all_data:
            comp = str(row.get('업체명', '')).strip()
            if comp in summary:
                date_val = str(row.get('계약납품요구일자', ''))
                if len(date_val) == 8:
                    row_date = datetime.datetime.strptime(date_val, "%Y%m%d").date()
                    if last_mon <= row_date <= last_sun:
                        amt = int(str(row.get('금액', 0)).replace(',', '').split('.')[0])
                        summary[comp] += amt

        # 2. 상위 20개 정렬
        sorted_list = sorted(summary.items(), key=lambda x: x[1], reverse=True)[:20]
        
        # 3. 이노뎁 강제 포함 로직
        top_names = [item[0] for item in sorted_list]
        if "이노뎁" not in top_names:
            # 전체 summary에서 이노뎁 값을 찾아 마지막에 추가
            sorted_list.append(("이노뎁", summary.get("이노뎁", 0)))

        # 4. HTML 생성 (상단 20위 강조 및 이노뎁 노란색 처리)
        html_report = f"""
        <html>
        <body style="font-family: 'Malgun Gothic', sans-serif;">
            <h2 style="color: #2E75B6;">📊 주요 업체 주간 매출 분석 (상위 20)</h2>
            <p>분석 기간: {last_mon} ~ {last_sun}</p>
            <table border="1" style="border-collapse: collapse; width: 100%; max-width: 600px;">
                <thead style="background-color: #DDEBF7;">
                    <tr>
                        <th style="padding: 10px;">순위</th>
                        <th style="padding: 10px;">업체명</th>
                        <th style="padding: 10px;">매출 합계</th>
                    </tr>
                </thead>
                <tbody>
        """
        for i, (name, val) in enumerate(sorted_list, 1):
            bg_style = 'style="background-color: #FFF2CC;"' if name == "이노뎁" else ""
            html_report += f"""
                <tr {bg_style}>
                    <td style="padding: 8px; text-align: center;">{i if i <= 20 else '-'}</td>
                    <td style="padding: 8px;"><b>{name}</b></td>
                    <td style="padding: 8px; text-align: right;">{val:,}원</td>
                </tr>
            """
        html_report += "</tbody></table></body></html>"

        if "GITHUB_OUTPUT" in os.environ:
            with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                f.write(f"report_range={last_mon}~{last_sun}\n")
        with open("report.html", "w", encoding="utf-8") as f:
            f.write(html_report)

        print(f"✅ 리포트 생성 완료 (대상 업체: {len(target_companies)}개)")

    except Exception as e:
        print(f"🔥 오류: {e}")

if __name__ == "__main__":
    main()
