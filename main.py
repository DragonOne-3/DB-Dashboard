import os, json, datetime, time, requests
import xml.etree.ElementTree as ET
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pytimekr import pytimekr
import re

# [보안 적용] 환경 변수에서 키 불러오기
MY_DIRECT_KEY = os.environ.get('DATA_GO_KR_API_KEY')
AUTH_JSON_STR = os.environ.get('GOOGLE_AUTH_JSON')

# 국문 헤더 (43개 항목 전체 유지)
HEADER_KOR = ['조달구분명', '계약구분명', '계약납품구분명', '계약납품요구일자', '계약납품요구번호', '변경차수', '최종변경차수여부', '수요기관명', '수요기관구분명', '수요기관지역명', '수요기관코드', '물품분류번호', '품명', '세부물품분류번호', '세부품명', '물품식별번호', '물품규격명', '단가', '수량', '단위', '금액', '업체명', '업체기업구분명', '계약명', '우수제품여부', '공사용자재직접구매대상여부', '다수공급자계약여부', '다수공급자계약2단계진행여부', '단가계약번호', '단가계약변경차수', '최초계약(납품요구)일자', '계약체결방법명', '증감수량', '증감금액', '납품장소명', '납품기한일자', '업체사업자등록번호', '인도조건명', '물품순번']

# 요청하신 모든 키워드 풀 리스트
keywords = [
    '네트워크시스템장비용랙','영상감시장치','PA용스피커','안내판','카메라브래킷','액정모니터','광송수신모듈','전원공급장치','광분배함','컨버터','컴퓨터서버','하드디스크드라이브','네트워크스위치','광점퍼코드','풀박스','서지흡수기','디지털비디오레코더',
    '스피커','오디오앰프','브래킷','UTP케이블','정보통신공사','영상정보디스플레이장치','송신기','난연전력케이블','1종금속제가요전선관','호온스피커','누전차단기','방송수신기','LAP외피광케이블','폴리에틸렌전선관','리모트앰프',
    '랙캐비닛용패널','베어본컴퓨터','분배기','결선보드유닛','벨','난연접지용비닐절연전선','경광등','데스크톱컴퓨터','특수목적컴퓨터','철근콘크리트공사','토공사','안내전광판','접지봉','카메라회전대','무선랜액세스포인트','컴퓨터망전환장치',
    '포장공사','고주파동축케이블','카메라하우징','인터폰','스위칭모드전원공급장치','금속상자','열선감지기','태양전지조절기','밀폐고정형납축전지','IP전화기','디스크어레이','그래픽용어댑터','인터콤장비','기억유닛','컴퓨터지문인식장치','랜접속카드',
    '접지판','제어케이블','비디오네트워킹장비','레이스웨이','콘솔익스텐더','전자카드','비대면방역감지장비','온습도트랜스미터','도난방지기','융복합영상감시장치','멀티스크린컴퓨터','컴퓨터정맥인식장치','카메라컨트롤러','SSD저장장치','원격단말장치(RTU)',
    '융복합네트워크스위치','융복합액정모니터','융복합데스크톱컴퓨터','융복합그래픽용어댑터','융복합베어본컴퓨터','융복합서지흡수기','배선장치','융복합배선장치','융복합카메라브래킷','융복합네트워크시스템장비용랙','융복합UTP케이블','테이프백업장치',
    '자기식테이프','레이드저장장치','광송수신기','450/750V 유연성단심비닐절연전선','솔내시스템','450/750V유연성단심비닐절연전선','카메라받침대','텔레비전거치대','광수신기','무선통신장치','동작분석기','전력공급장치','450/750V 일반용유연성단심비닐절연전선','분전함',
    '비디오믹서','절연전선및피복선','레이더','적외선방사기', '보안용카메라', '통신소프트웨어','분석및과학용소프트웨어','소프트웨어유지및지원서비스'
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
    """용역 계약 내역 수집 및 HTML 생성"""
    api_key = os.environ.get('DATA_GO_KR_API_KEY')
    api_url = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'
    target_date_str = target_dt.strftime("%Y%m%d")
    display_date_str = target_dt.strftime("%Y-%m-%d")
    
    keywords_servc = ['통합관제', 'CCTV', '영상감시장치']
    collected_data = []

    for kw in keywords_servc:
        params = {'serviceKey': api_key, 'pageNo': '1', 'numOfRows': '999', 'inqryDiv': '1', 'type': 'xml', 'inqryBgnDate': target_date_str, 'inqryEndDate': target_date_str, 'cntrctNm': kw}
        try:
            res = requests.get(api_url, params=params, timeout=30)
            if res.status_code == 200:
                root = ET.fromstring(res.content)
                items = root.findall('.//item')
                for item in items:
                    raw_demand = item.findtext('dminsttList', '-')
                    raw_corp = item.findtext('corpList', '-')
                    demand_parts = raw_demand.replace('[', '').replace(']', '').split('^')
                    clean_demand = demand_parts[2] if len(demand_parts) > 2 else raw_demand
                    corp_parts = raw_corp.replace('[', '').replace(']', '').split('^')
                    clean_corp = corp_parts[3] if len(corp_parts) > 3 else raw_corp

                    collected_data.append({
                        'demand': clean_demand, 'name': item.findtext('cntrctNm', '-'), 'corp': clean_corp,
                        'amount': int(item.findtext('totCntrctAmt', '0')), 'date': target_dt.strftime("%Y-%m-%d"),
                        'end_date': item.findtext('ttalScmpltDate', '-')
                    })
        except: pass

    unique_servc = {f"{d['demand']}_{d['name']}": d for d in collected_data}.values()
    
    html = f"<div style='margin-top: 20px;'><h4 style='color: #1a73e8; border-bottom: 2px solid #1a73e8; padding-bottom: 5px;'>🏛️ 나라장터 용역 계약 내역 ({display_date_str})</h4>"
    if not unique_servc:
        html += f"<p style='color: #666;'>- {display_date_str}에 해당 키워드 내역이 없습니다.</p></div>"
        return html

    html += "<table border='1' style='border-collapse: collapse; width: 100%; font-size: 11px;'> <tr style='background-color: #f8f9fa;'><th>수요기관</th><th>계약명</th><th>업체명</th><th>금액</th></tr>"
    for row in unique_servc:
        bg = "background-color: #FFF9C4;" if "이노뎁" in row['corp'] else ""
        html += f"<tr style='{bg}'><td>{row['demand']}</td><td>{row['name']}</td><td>{row['corp']}</td><td style='text-align: right;'>{row['amount']:,}원</td></tr>"
    html += "</table></div>"
    return html

def main():
    if not MY_DIRECT_KEY or not AUTH_JSON_STR:
        print("❌ 환경변수 누락"); return

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
        # 1. 구글 시트에는 중복 여부 상관없이 모든 수집 데이터 저장
        ws.append_rows(final_data)
        print(f"✅ {d_str} 원본 데이터 {len(final_data)}건 시트 저장 완료")

        # 2. [분석 및 메인 본문용] 중복 제거 로직
        # 기준: 기관명(7), 업체명(21), 금액(20), 세부품명(14)
        unique_final_data = {}
        for row in final_data:
            try:
                # 데이터 인덱스 기반 키 생성 (4가지 기준)
                key = (str(row[7]), str(row[21]), str(row[20]), str(row[14]))
                if key not in unique_final_data:
                    unique_final_data[key] = row
            except IndexError: continue
        
        deduplicated_data = list(unique_final_data.values())

        school_stats = {} 
        innodep_today_dict = {} 
        innodep_total_amt = 0

        # 3. 중복 제거된 데이터를 바탕으로 요약 분석
        for row in deduplicated_data:
            try:
                org_name = str(row[7])
                item_name = str(row[14])
                amt_val = str(row[20])
                comp_name = str(row[21])
                contract_name = str(row[23])
                amt_raw = amt_val.replace(',', '').split('.')[0]
                amt = int(amt_raw) if amt_raw else 0
            except: continue

            # 학교 지능형 CCTV 분석
            if '학교' in org_name and '지능형' in contract_name and 'CCTV' in contract_name:
                if org_name not in school_stats:
                    school_stats[org_name] = {'total_amt': 0, 'main_vendor': '', 'vendor_priority': 3}
                school_stats[org_name]['total_amt'] += amt
                priority = 1 if '영상감시장치' in item_name else 2 if '보안용카메라' in item_name else 3
                if priority < school_stats[org_name]['vendor_priority']:
                    school_stats[org_name]['main_vendor'] = comp_name
                    school_stats[org_name]['vendor_priority'] = priority

            # 이노뎁 실적 합산
            if '이노뎁' in comp_name:
                if org_name in innodep_today_dict: innodep_today_dict[org_name] += amt
                else: innodep_today_dict[org_name] = amt
                innodep_total_amt += amt

        # 4. 메일 요약 텍스트 생성
        summary_lines = [f"⭐ {d_str} 학교 지능형 CCTV 납품 현황:"]
        if school_stats:
            for school, info in school_stats.items():
                summary_lines.append(f"- {school} [{info['main_vendor']}]: {info['total_amt']:,}원")
        else: summary_lines.append(" 0건")
        
        summary_lines.append(" ") 
        summary_lines.append(f"🏢 {d_str} 이노뎁 실적:")
        if innodep_today_dict:
            for org, amt in innodep_today_dict.items():
                summary_lines.append(f"- {org}: {amt:,}원")
            summary_lines.append(f"** 총합계: {innodep_total_amt:,}원")
        else: summary_lines.append(" 0건")

        # 5. 용역 계약 데이터 HTML 생성
        servc_html = fetch_and_generate_servc_html(target_dt)

        # 6. GitHub Actions로 데이터 전달
        if "GITHUB_OUTPUT" in os.environ:
            with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:
                f.write(f"collect_date={d_str}\n")
                f.write(f"collect_count={len(final_data)}\n")
                f.write("school_info<<EOF\n")
                for line in summary_lines: f.write(f"{line}<br>\n")
                f.write("EOF\n")
                f.write("servc_info<<EOF\n")
                f.write(f"{servc_html}\n")
                f.write("EOF\n")
    else:
        print(f"ℹ️ {d_str} 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    main()
