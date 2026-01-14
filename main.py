import os, json, datetime, time, requests
import xml.etree.ElementTree as ET
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pytimekr import pytimekr
import re

# [보안 적용] 환경 변수에서 키 불러오기
MY_DIRECT_KEY = os.environ.get('DATA_GO_KR_API_KEY')
AUTH_JSON_STR = os.environ.get('GOOGLE_AUTH_JSON')

HEADER_KOR = ['조달구분명', '계약구분명', '계약납품구분명', '계약납품요구일자', '계약납품요구번호', '변경차수', '최종변경차수여부', '수요기관명', '수요기관구분명', '수요기관지역명', '수요기관코드', '물품분류번호', '품명', '세부물품분류번호', '세부품명', '물품식별번호', '물품규격명', '단가', '수량', '단위', '금액', '업체명', '업체기업구분명', '계약명', '우수제품여부', '공사용자재직접구매대상여부', '다수공급자계약여부', '다수공급자계약2단계진행여부', '단가계약번호', '단가계약변경차수', '최초계약(납품요구)일자', '계약체결방법명', '증감수량', '증감금액', '납품장소명', '납품기한일자', '업체사업자등록번호', '인도조건명', '물품순번']

keywords = [
    '영상감시장치','보안용카메라','소프트웨어유지및지원서비스' # ... (기존 키워드 유지)
]

def get_target_date():
    """한국 시간 기준, 공휴일 제외 최근 평일 계산"""
    now = datetime.datetime.utcnow() + datetime.timedelta(hours=9)
    target = now - datetime.timedelta(days=1)
    holidays = pytimekr.holidays(year=target.year)
    while target.weekday() >= 5 or target.date() in holidays:
        target -= datetime.timedelta(days=1)
    return target

def get_quarter(month):
    return (month - 1) // 3 + 1

def get_or_create_worksheet(client, target_dt):
    year, month = target_dt.year, target_dt.month
    quarter = get_quarter(month)
    file_name = f"조달청_납품내역_{year}_{quarter}분기"
    sheet_name = f"{year}_{month}월"
    try:
        sh = client.open(file_name)
    except gspread.exceptions.SpreadsheetNotFound:
        sh = client.create(file_name)
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows="5000", cols="44")
        ws.append_row(HEADER_KOR)
    return ws

def fetch_data(kw, d_str):
    url = "https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getSpcifyPrdlstPrcureInfoList"
    params = {'numOfRows': '999', 'pageNo': '1', 'ServiceKey': MY_DIRECT_KEY, 'Type_A': 'xml', 'inqryDiv': '1', 'inqryPrdctDiv': '2', 'inqryBgnDate': d_str, 'inqryEndDate': d_str, 'dtilPrdctClsfcNoNm': kw}
    try:
        res = requests.get(url, params=params, timeout=30)
        if res.status_code == 200 and "<item>" in res.text:
            root = ET.fromstring(res.content)
            return [[elem.text if elem.text else '' for elem in item] for item in root.findall('.//item')]
    except: pass
    return []

def fetch_and_generate_servc_html(target_dt):
    """메인에서 계산된 target_dt를 인자로 받아 날짜 일치시킴"""
    api_key = os.environ.get('DATA_GO_KR_API_KEY')
    api_url = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'
    
    target_date_str = target_dt.strftime("%Y%m%d")
    display_date_str = target_dt.strftime("%Y-%m-%d")
    
    keywords = ['통합관제', 'CCTV', '영상감시장치']
    collected_data = []

    for kw in keywords:
        params = {
            'serviceKey': api_key, 'pageNo': '1', 'numOfRows': '999', 'inqryDiv': '1',
            'type': 'xml', 'inqryBgnDate': target_date_str, 'inqryEndDate': target_date_str,
            'cntrctNm': kw
        }
        try:
            res = requests.get(api_url, params=params, timeout=30)
            if res.status_code == 200:
                root = ET.fromstring(res.content)
                items = root.findall('.//item')
                for item in items:
                    raw_demand = item.findtext('dminsttList', '-')
                    raw_corp = item.findtext('corpList', '-')
                    cntrct_date_raw = item.findtext('cntrctDate', '')
                    end_date_raw = item.findtext('ttalScmpltDate', '')
                    
                    demand_parts = raw_demand.replace('[', '').replace(']', '').split('^')
                    clean_demand = demand_parts[2] if len(demand_parts) > 2 else raw_demand
                    
                    corp_parts = raw_corp.replace('[', '').replace(']', '').split('^')
                    clean_corp = corp_parts[3] if len(corp_parts) > 3 else raw_corp

                    clean_cntrct_date = "-"
                    if cntrct_date_raw:
                        try: clean_cntrct_date = datetime.datetime.strptime(cntrct_date_raw, "%Y%m%d").strftime("%Y-%m-%d")
                        except: clean_cntrct_date = cntrct_date_raw

                    final_end_date = "-"
                    if end_date_raw and cntrct_date_raw:
                        if '일' in end_date_raw:
                            try:
                                days = int(re.sub(r'[^0-9]', '', end_date_raw))
                                start_dt = datetime.datetime.strptime(cntrct_date_raw, "%Y%m%d")
                                final_end_date = (start_dt + datetime.timedelta(days=days)).strftime("%Y-%m-%d")
                            except: final_end_date = end_date_raw
                        else:
                            try: final_end_date = datetime.datetime.strptime(end_date_raw, "%Y%m%d").strftime("%Y-%m-%d")
                            except: final_end_date = end_date_raw

                    collected_data.append({
                        'demand': clean_demand,
                        'name': item.findtext('cntrctNm', '-'),
                        'corp': clean_corp,
                        'amount': int(item.findtext('totCntrctAmt', '0')),
                        'date': clean_cntrct_date,
                        'end_date': final_end_date
                    })
        except Exception as e:
            print(f"❌ 용역 API 에러 ({kw}): {e}")

    # 중복 제거
    unique_data = {f"{d['demand']}_{d['name']}": d for d in collected_data}.values()

    html = f"<div style='margin-top: 20px;'><h4 style='color: #1a73e8; border-bottom: 2px solid #1a73e8; padding-bottom: 5px;'>🏛️ 나라장터 용역 계약 내역 ({display_date_str} 체결분)</h4>"
    if not unique_data:
        html += f"<p style='color: #666;'>- {display_date_str}에 체결된 해당 키워드 계약 내역이 없습니다.</p></div>"
        return html

    html += """
    <table border='1' style='border-collapse: collapse; width: 100%; font-size: 11px; border: 1px solid #ddd;'>
        <tr style='background-color: #f8f9fa; text-align: center;'>
            <th style='padding: 5px;'>계약일자</th>
            <th style='padding: 5px;'>수요기관명</th>
            <th style='padding: 5px;'>계약명</th>
            <th style='padding: 5px;'>업체명</th>
            <th style='padding: 5px;'>계약금액</th>
            <th style='padding: 5px;'>계약만료일</th>
        </tr>
    """
    for row in unique_data:
        bg_style = "style='background-color: #FFF9C4;'" if "이노뎁" in row['corp'] else ""
        html += f"""
        <tr {bg_style}>
            <td style='padding: 5px; text-align: center;'>{row['date']}</td>
            <td style='padding: 5px;'>{row['demand']}</td>
            <td style='padding: 5px;'>{row['name']}</td>
            <td style='padding: 5px;'>{row['corp']}</td>
            <td style='padding: 5px; text-align: right;'>{row['amount']:,}원</td>
            <td style='padding: 5px; text-align: center;'>{row['end_date']}</td>
        </tr>
        """
    html += "</table></div><br>"
    return html

def main():
    if not MY_DIRECT_KEY or not AUTH_JSON_STR:
        print("❌ 환경변수 설정 확인 필요"); return

    target_dt = get_target_date()
    d_str = target_dt.strftime("%Y%m%d")
    
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(AUTH_JSON_STR), ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    ws = get_or_create_worksheet(client, target_dt)
    
    final_data = []
    for kw in keywords:
        data = fetch_data(kw, d_str)
        if data: final_data.extend(data)
        time.sleep(0.5)
    
    if final_data:
        ws.append_rows(final_data)
        
        school_stats = {} 
        # [수정 1] 중복 제거를 위한 딕셔너리 구조 사용 (기관명 키값)
        innodep_today_dict = {}
        innodep_total_amt = 0

        for row in final_data:
            try:
                org_name = str(row[7])
                item_name = str(row[14])
                amt_val = str(row[20])
                comp_name = str(row[21])
                contract_name = str(row[23])
                amt_raw = amt_val.replace(',', '').split('.')[0]
                amt = int(amt_raw) if amt_raw else 0
            except (IndexError, ValueError): continue

            # 학교 분석
            if '학교' in org_name and '지능형' in contract_name and 'CCTV' in contract_name:
                if org_name not in school_stats:
                    school_stats[org_name] = {'total_amt': 0, 'main_vendor': '', 'vendor_priority': 3}
                school_stats[org_name]['total_amt'] += amt
                # 우선순위 결정
                priority = 3
                if '영상감시장치' in item_name: priority = 1
                elif '보안용카메라' in item_name: priority = 2
                if priority < school_stats[org_name]['vendor_priority']:
                    school_stats[org_name]['main_vendor'] = comp_name
                    school_stats[org_name]['vendor_priority'] = priority

            # [수정 1] 이노뎁 중복 제거: 동일 기관명이 이미 있으면 금액만 합산
            if '이노뎁' in comp_name:
                if org_name in innodep_today_dict:
                    innodep_today_dict[org_name] += amt
                else:
                    innodep_today_dict[org_name] = amt
                innodep_total_amt += amt

        summary_lines = []
        summary_lines.append("⭐ 오늘자 학교 지능형 CCTV 납품 현황:")
        if school_stats:
            for school, info in school_stats.items():
                summary_lines.append(f"- {school} [{info['main_vendor']}]: {info['total_amt']:,}원")
        else: summary_lines.append(" 0건")
        
        summary_lines.append(" ") 
        
        summary_lines.append("🏢 오늘자 이노뎁 실적:")
        if innodep_today_dict:
            for org, amt in innodep_today_dict.items():
                summary_lines.append(f"- {org}: {amt:,}원")
            summary_lines.append(f"** 총합계: {innodep_total_amt:,}원")
        else: summary_lines.append(" 0건")

        # [수정 2] 용역 데이터 날짜 불일치 해결: 메인 target_dt를 전달
        servc_html = fetch_and_generate_servc_html(target_dt)

        if "GITHUB_OUTPUT" in os.environ:
            with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:
                f.write(f"collect_date={d_str}\n")
                f.write(f"collect_count={len(final_data)}\n")
                f.write("school_info<<EOF\n")
                for line in summary_lines:
                    f.write(f"{line}<br>\n")
                f.write("EOF\n")
                f.write("servc_info<<EOF\n")
                f.write(f"{servc_html}\n")
                f.write("EOF\n")

if __name__ == "__main__":
    main()
