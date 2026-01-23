import os, json, datetime, io, re
import pandas as pd
import requests
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
from datetime import timezone, timedelta

# [수정 필요 시점] GitHub Secrets에 저장된 환경 변수 이름이 바뀔 때만 수정하세요.
AUTH_JSON_STR = os.environ.get('GOOGLE_AUTH_JSON')

def get_target_companies():
    """
    [설명] 분석 대상인 경쟁 업체 리스트를 가져오는 함수입니다.
    [수정] 별도의 파일(companies.txt) 없이 코드에서 직접 수정하고 싶다면 return 뒤의 리스트를 수정하세요.
    """
    file_path = "companies.txt"
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    return ["이노뎁(주)", "이노뎁"]

def get_last_week_range():
    """
    [설명] 리포트의 분석 기간(지난주 월~일)을 계산합니다.
    [수정] 분석 기간을 '이번 달'이나 '어제' 등으로 바꾸고 싶을 때 날짜 계산 로직을 수정합니다.
    """
    now_utc = datetime.datetime.now(timezone.utc)
    today = (now_utc + timedelta(hours=9)).date()
    
    this_monday = today - datetime.timedelta(days=today.weekday())
    last_monday = this_monday - datetime.timedelta(days=7)
    last_sunday = last_monday + datetime.timedelta(days=6)
    
    return last_monday, last_sunday

def get_drive_service():
    """
    [설명] 구글 드라이브 API 연결을 위한 인증 서비스 세팅입니다.
    [수정] 드라이브 접근 권한 범위(scopes)를 변경해야 할 때 수정합니다.
    """
    info = json.loads(AUTH_JSON_STR)
    scopes = ['https://www.googleapis.com/auth/drive.readonly']
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    return build('drive', 'v3', credentials=creds)

def clean_company_name(name):
    """
    [설명] 업체명에서 (주), 주식회사 등을 제거하여 텍스트를 통일합니다.
    [수정] 제거하고 싶은 문구가 추가될 경우(예: '(사)', '유한회사') 정규식에 추가하세요.
    """
    return re.sub(r'\(주\)|주식회사|\(유\)', '', name).strip()

def main():
    if not AUTH_JSON_STR:
        print("❌ GOOGLE_AUTH_JSON 환경변수가 설정되지 않았습니다."); return
    
    try:
        target_companies = get_target_companies()
        target_map = {clean_company_name(c): c for c in target_companies}
        
        drive_service = get_drive_service()
        last_mon, last_sun = get_last_week_range()
        
        # [수정 중요!] 데이터가 저장되는 구글 드라이브 폴더 ID입니다. 폴더가 바뀌면 이 ID를 교체하세요.
        DRIVE_FOLDER_ID = '1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr'
        
        # [수정 중요!] 메인 스크립트(main.py)가 저장하는 파일명 규칙과 반드시 일치해야 합니다.
        FILE_NAME_FOR_YEAR = f"{last_mon.year}.csv"
        
        # 구글 드라이브에서 파일을 검색하는 쿼리문입니다.
        query = f"name='{FILE_NAME_FOR_YEAR}' and '{DRIVE_FOLDER_ID}' in parents and trashed=false"
        results = drive_service.files().list(q=query, fields='files(id)').execute()
        items = results.get('files', [])
        
        if not items:
            print(f"⚠️ 드라이브에 {FILE_NAME_FOR_YEAR} 파일이 없습니다."); return
            
        file_id = items[0]['id']
        
        # 드라이브의 파일을 메모리로 다운로드합니다.
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        fh.seek(0)
        # CSV 파일을 읽어옵니다. 한글 깨짐 방지를 위해 utf-8-sig를 사용합니다.
        df = pd.read_csv(fh, encoding='utf-8-sig', low_memory=False)
        
        # [설명] 날짜 데이터를 숫자로 변환하여 지난주 범위(월~일)만 필터링합니다.
        df['계약납품요구일자'] = pd.to_numeric(df['계약납품요구일자'], errors='coerce')
        start_date = int(last_mon.strftime("%Y%m%d"))
        end_date = int(last_sun.strftime("%Y%m%d"))
        
        weekly_df = df[(df['계약납품요구일자'] >= start_date) & (df['계약납품요구일자'] <= end_date)].copy()
        
        if weekly_df.empty:
            print(f"ℹ️ {last_mon} ~ {last_sun} 기간에 해당하는 데이터가 CSV에 없습니다."); return

        summary = {clean_company_name(comp): 0 for comp in target_companies}
        innodep_details = {}

        # [설명] 필터링된 데이터를 한 줄씩 읽으며 업체별로 금액을 합산합니다.
        for _, row in weekly_df.iterrows():
            raw_comp = str(row.get('업체명', '')).strip()
            comp = clean_company_name(raw_comp)
            
            # [수정] 데이터의 '금액' 컬럼 형식이 바뀔 경우(예: 소수점 포함 등) 이 부분을 수정합니다.
            amt = int(str(row.get('금액', 0)).replace(',', '').split('.')[0])
            
            if comp in summary:
                summary[comp] += amt
            
            if "이노뎁" in comp:
                org = row.get('수요기관명', '기타')
                innodep_details[org] = innodep_details.get(org, 0) + amt

        # 합산된 금액을 기준으로 순위를 정렬합니다.
        sorted_list = sorted(summary.items(), key=lambda x: x[1], reverse=True)
        innodep_rank = "순위권 밖"
        innodep_total = 0
        for idx, (name, val) in enumerate(sorted_list, 1):
            if "이노뎁" in name:
                innodep_rank = f"{idx}위"
                innodep_total = val
                break

        # 상위 20개 업체만 리포트 표에 노출합니다.
        final_list = sorted_list[:20]
        if not any("이노뎁" in item[0] for item in final_list):
            final_list.append(("이노뎁", innodep_total))

        # [수정] 리포트의 디자인(색상, 폰트, 문구 등)을 바꾸고 싶을 때 이 HTML 구간을 수정합니다.
        html_report = f"""
        <html>
        <body style="font-family: 'Malgun Gothic', sans-serif; padding: 20px; line-height: 1.6;">
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
            # 이노뎁 행일 경우 배경색을 노란색으로 강조합니다.
            bg = 'style="background-color: #FFF2CC;"' if "이노뎁" in name else ""
            html_report += f"<tr {bg}><td style='padding: 8px; text-align: center;'>{i if i<=20 else '-'}</td>"
            html_report += f"<td style='padding: 8px;'>{name}</td><td style='padding: 8px; text-align: right;'>{val:,}원</td></tr>"
            
        html_report += f"""
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
            # 이노뎁의 납품 현황을 금액이 높은 순으로 정렬하여 표시합니다.
            for org, amt in sorted(innodep_details.items(), key=lambda x: x[1], reverse=True):
                html_report += f"<tr><td style='padding: 8px;'>{org}</td><td style='padding: 8px; text-align: right;'>{amt:,}원</td></tr>"

        html_report += "</tbody></table></body></html>"

        # 최종 생성된 HTML을 report.html 파일로 저장합니다. 이 파일이 메일로 발송됩니다.
        with open("report.html", "w", encoding="utf-8") as f:
            f.write(html_report)
        
        # GitHub Actions 워크플로에서 사용할 수 있도록 실행 결과를 출력합니다.
        if "GITHUB_OUTPUT" in os.environ:
            with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                f.write(f"report_range={last_mon}~{last_sun}\n")

        print("✅ 주간 리포트(report.html) 생성 완료")

    except Exception as e:
        # 오류가 발생했을 때 멈추지 않고 에러 내용이 담긴 HTML을 생성하여 메일로 보냅니다.
        print(f"🔥 오류 발생: {e}")
        with open("report.html", "w", encoding="utf-8") as f:
            f.write(f"<html><body><h2>리포트 생성 오류</h2><p>{str(e)}</p></body></html>")

if __name__ == "__main__":
    main()
