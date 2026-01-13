import os, json, datetime, time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests

# 환경 변수 로드
AUTH_JSON_STR = os.environ.get('GOOGLE_AUTH_JSON')
TEAMS_WEBHOOK_URL = os.environ.get('TEAMS_WEBHOOK_URL')

def get_last_week_range():
    """지난주 월요일~일요일 날짜 계산"""
    today = datetime.date.today()
    # 실행 시점(월요일) 기준 7일 전이 지난주 월요일
    last_monday = today - datetime.timedelta(days=today.weekday() + 7)
    last_sunday = last_monday + datetime.timedelta(days=6)
    return last_monday, last_sunday

def send_teams_report(content):
    """최신 팀즈 워크플로 규격(Adaptive Cards)으로 전송"""
    payload = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "type": "AdaptiveCard",
                "body": [
                    {"type": "TextBlock", "text": "📊 주간 매출 요약 리포트 (전주 기준)", "weight": "Bolder", "size": "Medium", "color": "Accent"},
                    {"type": "TextBlock", "text": content, "wrap": True}
                ],
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "version": "1.4"
            }
        }]
    }
    requests.post(TEAMS_WEBHOOK_URL, json=payload)

def main():
    if not AUTH_JSON_STR or not TEAMS_WEBHOOK_URL:
        print("❌ 환경변수 설정 누락"); return

    # 구글 서비스 계정 인증
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        json.loads(AUTH_JSON_STR), 
        ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    )
    client = gspread.authorize(creds)
    
    last_mon, last_sun = get_last_week_range()
    
    # 지난주가 걸쳐있는 연도와 분기 계산
    year = last_mon.year
    quarter = (last_mon.month - 1) // 3 + 1
    file_name = f"조달청_납품내역_{year}_{quarter}분기"
    
    try:
        sh = client.open(file_name)
        # 지난주가 두 달에 걸쳐 있을 수 있으므로 월 리스트 생성
        months = list(set([last_mon.month, last_sun.month]))
        
        all_data = []
        for m in months:
            try:
                ws = sh.worksheet(f"{year}_{m}월")
                all_data.extend(ws.get_all_records())
            except: continue

        # 기업별 매출 합산
        summary = {}
        for row in all_data:
            date_str = str(row.get('계약납품요구일자', ''))
            if not date_str: continue
            
            row_date = datetime.datetime.strptime(date_str, "%Y%m%d").date()
            # 지난주 범위 내 데이터만 필터링
            if last_mon <= row_date <= last_sun:
                company = row.get('업체명', '알수없음')
                amount = int(row.get('금액', 0))
                summary[company] = summary.get(company, 0) + amount

        # 금액 기준 내림차순 정렬 후 상위 10개 추출
        sorted_summary = sorted(summary.items(), key=lambda x: x[1], reverse=True)[:10]
        
        if not sorted_summary:
            report_text = f"📅 **기간:** {last_mon} ~ {last_sun}\n\n결과: 지난주 매출 데이터가 없습니다."
        else:
            report_text = f"📅 **기간:** {last_mon} ~ {last_sun}\n\n"
            for i, (comp, amt) in enumerate(sorted_summary, 1):
                report_text += f"**{i}위. {comp}**\n   - 매출액: {amt:,}원\n"

        send_teams_report(report_text)
        print("✅ 주간 보고서 전송 완료")

    except Exception as e:
        print(f"❌ 에러 발생: {e}")

if __name__ == "__main__":
    main()
