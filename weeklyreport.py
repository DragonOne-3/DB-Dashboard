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
            return [line.strip() for line in f if line.strip()]
    return ["이노뎁(주)", "이노뎁"] # 기본값

def get_last_week_range():
    today = datetime.date.today()
    last_monday = today - datetime.timedelta(days=today.weekday() + 7)
    last_sunday = last_monday + datetime.timedelta(days=6)
    return last_monday, last_sunday

def main():
    if not AUTH_JSON_STR: return
    
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

        # 1. 집계 데이터 구조화
        summary = {comp: 0 for comp in target_companies}
        innodep_details = {} # 수요기관별 합산용

        for row in all_data:
            comp = str(row.get('업체명', '')).strip()
            date_val = str(row.get('계약납품요구일자', ''))
            
            if len(date_val) == 8:
                row_date = datetime.datetime.strptime(date_val, "%Y%m%d").date()
                if last_mon <= row_date <= last_sun:
                    amt = int(str(row.get('금액', 0)).replace(',', '').split('.')[0])
                    
                    # A. 지정 업체 매출 합산
                    if comp in summary:
                        summary[comp] += amt
                    
                    # B. 이노뎁 상세 내역 추출 (이노뎁 명칭이 포함된 경우 모두 포함)
                    if "이노뎁" in comp:
                        org = row.get('수요기관명', '기타')
                        innodep_details[org] = innodep_details.get(org, 0) + amt

        # 2. 순위 계산 및 이노뎁 정보 추출
        sorted_list = sorted(summary.items(), key=lambda x: x[1], reverse=True)
        
        innodep_rank = "순위권 밖"
        innodep_total = 0
        for idx, (name, val) in enumerate(sorted_list, 1):
            if "이노뎁" in name:
                innodep_rank = f"{idx}위"
                innodep_total = val
                break

        # 상위 20개만 리스트업 (이노뎁이 없으면 강제 추가)
        final_list = sorted_list[:20]
        if not any("이노뎁" in item[0] for item in final_list):
            final_list.append(("이노뎁(주)", innodep_total))

        # 3. HTML 생성
        html_report = f"""
        <html>
        <body style="font-family: 'Malgun Gothic', sans-serif; padding: 20px;">
            <h2 style="color: #2E75B6;">📊 주간 특정품목 납품내역 분석 리포트</h2>
            <p>📅 <b>분석 기간:</b> {last_mon} ~ {last_sun}</p>
            
            <div style="background-color: #F8F9FA; padding: 15px; border-radius: 8px; margin-bottom: 20px; border-left: 5px solid #2E75B6;">
                <h3 style="margin: 0;">🏢 이노뎁 요약</h3>
                <p style="margin: 5px 0;">현재 순위: <b>{innodep_rank}</b> / 총 매출: <b>{innodep_total:,}원</b></p>
            </div>

            <h3 style="color: #444;">1️⃣ 주요 업체별 순위 (Top 20)</h3>
            <table border="1" style="border-collapse: collapse; width: 100%; max-width: 600px; margin-bottom: 30px;">
                <thead style="background-color: #DDEBF7;">
                    <tr><th style="padding: 10px;">순위</th><th style="padding: 10px;">업체명</th><th style="padding: 10px;">매출액</th></tr>
                </thead>
                <tbody>
        """
        for i, (name, val) in enumerate(final_list, 1):
            is_innodep = "이노뎁" in name
            bg = 'style="background-color: #FFF2CC;"' if is_innodep else ""
            html_report += f"<tr {bg}><td style='padding: 8px; text-align: center;'>{i if i<=20 else '-'}</td>"
            html_report += f"<td style='padding: 8px;'>{'<b>' if is_innodep else ''}{name}{'</b>' if is_innodep else ''}</td>"
            html_report += f"<td style='padding: 8px; text-align: right;'>{val:,}원</td></tr>"
            
        html_report += """
                </tbody>
            </table>

            <h3 style="color: #444;">2️⃣ 이노뎁 수요기관별 납품 현황</h3>
            <table border="1" style="border-collapse: collapse; width: 100%; max-width: 600px;">
                <thead style="background-color: #E2EFDA;">
                    <tr><th style="padding: 10px;">수요기관명</th><th style="padding: 10px;">납품 금액</th></tr>
                </thead>
                <tbody>
        """
        
        if not innodep_details:
            html_report += "<tr><td colspan='2' style='padding: 10px; text-align: center;'>납품 내역이 없습니다.</td></tr>"
        else:
            # 금액 순으로 정렬하여 표시
            for org, amt in sorted(innodep_details.items(), key=lambda x: x[1], reverse=True):
                html_report += f"<tr><td style='padding: 8px;'>{org}</td><td style='padding: 8px; text-align: right;'>{amt:,}원</td></tr>"

        html_report += "</tbody></table></body></html>"

        # 결과 저장
        if "GITHUB_OUTPUT" in os.environ:
            with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                f.write(f"report_range={last_mon}~{last_sun}\n")
        with open("report.html", "w", encoding="utf-8") as f:
            f.write(html_report)

    except Exception as e:
        print(f"🔥 오류: {e}")

if __name__ == "__main__":
    main()
