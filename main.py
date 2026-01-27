import os, json, datetime, time, requests
import xml.etree.ElementTree as ET

# 구글 드라이브 및 데이터 처리를 위한 모듈
import pandas as pd
import io
import threading
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseUpload 

# 기타 유틸리티
from pytimekr import pytimekr # 공휴일 계산
import re

# ================= 1. 설정 및 환경 변수 =================
MY_DIRECT_KEY = os.environ.get('DATA_GO_KR_API_KEY')
AUTH_JSON_STR = os.environ.get('GOOGLE_AUTH_JSON')
save_lock = threading.Lock() # 파일 쓰기 충돌 방지용 잠금 장치

# [기존 유지] 물품 납품 상세 내역용 헤더 (43개 전체)
HEADER_KOR = ['조달구분명', '계약구분명', '계약납품구분명', '계약납품요구일자', '계약납품요구번호', '변경차수', '최종변경차수여부', '수요기관명', '수요기관구분명', '수요기관지역명', '수요기관코드', '물품분류번호', '품명', '세부물품분류번호', '세부품명', '물품식별번호', '물품규격명', '단가', '수량', '단위', '금액', '업체명', '업체기업구분명', '계약명', '우수제품여부', '공사용자재직접구매대상여부', '다수공급자계약여부', '다수공급자계약2단계진행여부', '단가계약번호', '단가계약변경차수', '최초계약(납품요구)일자', '계약체결방법명', '증감수량', '증감금액', '납품장소명', '납품기한일자', '업체사업자등록번호', '인도조건명', '물품순번']

# 🚀 [추가/수정] 주차관제 및 출입통제 품목 통합 키워드 리스트 (중복 제거 및 정렬 완료)
keywords = sorted(list(set([
    '네트워크시스템장비용랙','영상감시장치','PA용스피커','안내판','카메라브래킷','액정모니터','광송수신모듈','전원공급장치','광분배함','컨버터','컴퓨터서버','하드디스크드라이브','네트워크스위치','광점퍼코드','풀박스','서지흡수기','디지털비디오레코더',
    '스피커','오디오앰프','브래킷','UTP케이블','정보통신공사','영상정보디스플레이장치','송신기','난연전력케이블','1종금속제가요전선관','호온스피커','누전차단기','방송수신기','LAP외피광케이블','폴리에틸렌전선관','리모트앰프',
    '랙캐비닛용패널','베어본컴퓨터','분배기','결선보드유닛','벨','난연접지용비닐절연전선','경광등','데스크톱컴퓨터','특수목적컴퓨터','철근콘크리트공사','토공사','안내전광판','접지봉','카메라회전대','무선랜액세스포인트','컴퓨터망전환장치',
    '포장공사','고주파동축케이블','카메라하우징','인터폰','스위칭모드전원공급장치','금속상자','열선감지기','태양전지조절기','밀폐고정형납축전지','IP전화기','디스크어레이','그래픽용어댑터','인터콤장비','기억유닛','컴퓨터지문인식장치','랜접속카드',
    '접지판','제어케이블','비디오네트워킹장비','레이스웨이','콘솔익스텐더','전자카드','비대면방역감지장비','온습도트랜스미터','도난방지기','융복합영상감시장치','멀티스크린컴퓨터','컴퓨터정맥인식장치','카메라컨트롤러','SSD저장장치','원격단말장치(RTU)',
    '융복합네트워크스위치','융복합액정모니터','융복합데스크톱컴퓨터','융복합그래픽용어댑터','융복합베어본컴퓨터','융복합서지흡수기','배선장치','융복합배선장치','융복합카메라브래킷','융복합네트워크시스템장비용랙','융복합UTP케이블','테이프백업장치',
    '자기식테이프','레이드저장장치','광송수신기','450/750V 유연성단심비닐절연전선','솔내시스템','450/750V유연성단심비닐절연전선','카메라받침대','텔레비전거치대','광수신기','무선통신장치','동작분석기','전력공급장치','450/750V 일반용유연성단심비닐절연전선','분전함',
    '비디오믹서','절연전선및피복선','레이더','적외선방사기', '보안용카메라', '통신소프트웨어','분석및과학용소프트웨어','소프트웨어유지및지원서비스',
    # --- 주차관제 및 출입통제 추가 품목 ---
    '교통관제시스템', '분석및과학용소프트웨어', '산업관리소프트웨어', '시스템관리소프트웨어', '온습도트랜스미터', '인터콤장비', '자기식테이프', '적외선카메라', 
    '주차경보등', '주차관제주변기기', '주차권판독기', '주차안내판', '주차요금계산기', '주차주제어장치', '차량감지기', '차량인식기', '차량차단기', 
    '패키지소프트웨어개발및도입서비스', '도난방지기', '무선인식리더기', '바코드시스템', '비대면방역감지장비', '출입통제시스템', '카드인쇄기', '컴퓨터정맥인식장치'
])))

# [추가] 입찰 공고 API 주소
NOTICE_API_MAP = {
    '공사': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwkPPSSrch',
    '물품': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThngPPSSrch',
    '용역': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoServcPPSSrch'
}

# ================= 2. 유틸리티 함수 =================
def get_drive_service_for_script():
    info = json.loads(AUTH_JSON_STR)
    creds = service_account.Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/drive'])
    return build('drive', 'v3', credentials=creds), creds

def get_target_date():
    now = datetime.datetime.utcnow() + datetime.timedelta(hours=9)
    target = now - datetime.timedelta(days=1)
    holidays = pytimekr.holidays(year=target.year)
    while target.weekday() >= 5 or target.date() in holidays:
        target -= datetime.timedelta(days=1)
    return target

def fetch_api_data_from_g2b(kw, d_str):
    url = "https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getSpcifyPrdlstPrcureInfoList"
    params = {'numOfRows': '999', 'pageNo': '1', 'ServiceKey': MY_DIRECT_KEY, 'Type_A': 'xml', 'inqryDiv': '1', 'inqryPrdctDiv': '2', 'inqryBgnDate': d_str, 'inqryEndDate': d_str, 'dtilPrdctClsfcNoNm': kw}
    try:
        res = requests.get(url, params=params, timeout=30)
        if res.status_code == 200 and "<item>" in res.text:
            root = ET.fromstring(res.content)
            return [[elem.text if elem.text else '' for elem in elem_item] for elem_item in root.findall('.//item')]
    except: pass
    return []

def fetch_and_generate_servc_html(target_dt):
    api_key = os.environ.get('DATA_GO_KR_API_KEY')
    api_url = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'
    target_date_str = target_dt.strftime("%Y%m%d")
    display_date_str = target_dt.strftime("%Y-%m-%d")
    keywords_servc = ['통합관제', 'CCTV', '영상감시장치','국방','경계','작전','부대','육군','공군','해군','무인']
    collected_data = []

    for kw in keywords_servc:
        params = {'serviceKey': api_key, 'pageNo': '1', 'numOfRows': '999', 'inqryDiv': '1', 'type': 'xml', 'inqryBgnDate': target_date_str, 'inqryEndDate': target_date_str, 'cntrctNm': kw}
        try:
            res = requests.get(api_url, params=params, timeout=30)
            if res.status_code == 200:
                root = ET.fromstring(res.content)
                for item in root.findall('.//item'):
                    raw_demand = item.findtext('dminsttList', '-')
                    raw_corp = item.findtext('corpList', '-')
                    demand_parts = raw_demand.replace('[', '').replace(']', '').split('^')
                    clean_demand = demand_parts[2] if len(demand_parts) > 2 else raw_demand
                    corp_parts = raw_corp.replace('[', '').replace(']', '').split('^')
                    clean_corp = corp_parts[3] if len(corp_parts) > 3 else raw_corp
                    collected_data.append({
                        'demand': clean_demand, 'name': item.findtext('cntrctNm', '-'), 'corp': clean_corp,
                        'amount': int(item.findtext('totCntrctAmt', '0')), 'date': display_date_str
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

def fetch_notice_data(category, url, d_str):
    params = {'serviceKey': MY_DIRECT_KEY, 'pageNo': '1', 'numOfRows': '999', 'inqryDiv': '1', 'type': 'json', 'inqryBgnDt': d_str + "0000", 'inqryEndDt': d_str + "2359"}
    try:
        res = requests.get(url, params=params, timeout=45)
        if res.status_code == 200:
            items = res.json().get('response', {}).get('body', {}).get('items', [])
            return pd.DataFrame(items)
    except: pass
    return pd.DataFrame()

# ================= 3. 메인 실행 로직 =================
def main():
    if not MY_DIRECT_KEY or not AUTH_JSON_STR: return

    target_dt = get_target_date()
    d_str = target_dt.strftime("%Y%m%d")
    drive_service, drive_creds = get_drive_service_for_script()
    
    # --- PART 1: 물품 납품 상세 내역 수집 (기존 기능) ---
    final_data = []
    for kw in keywords:
        data = fetch_api_data_from_g2b(kw, d_str)
        if data: final_data.extend(data)
        time.sleep(0.5)
    
    if final_data:
        new_df = pd.DataFrame(final_data, columns=HEADER_KOR)
        DRIVE_FOLDER_ID = '1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr'
        FILE_NAME_FOR_YEAR = f"{target_dt.year}.csv"

        # 드라이브 파일 검색 및 업데이트
        query = f"name='{FILE_NAME_FOR_YEAR}' and '{DRIVE_FOLDER_ID}' in parents and trashed=false and mimeType='text/csv'" 
        res = drive_service.files().list(q=query, spaces='drive', fields='files(id)').execute()
        items = res.get('files', [])

        if items:
            f_id = items[0]['id']
            d_url = f'https://www.googleapis.com/drive/v3/files/{f_id}?alt=media'
            resp = requests.get(d_url, headers={'Authorization': f'Bearer {drive_creds.token}'})
            if resp.status_code == 200:
                old_df = pd.read_csv(io.BytesIO(resp.content), encoding='utf-8-sig', low_memory=False)
                df_to_upload = pd.concat([old_df, new_df], ignore_index=True).drop_duplicates(subset=['계약납품요구일자', '수요기관명', '품명', '금액'], keep='last')
            else: df_to_upload = new_df
        else:
            f_id = None
            df_to_upload = new_df

        csv_bytes = df_to_upload.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
        media = MediaIoBaseUpload(io.BytesIO(csv_bytes), mimetype='text/csv', resumable=True)

        if f_id: drive_service.files().update(fileId=f_id, media_body=media).execute()
        else: drive_service.files().create(body={'name': FILE_NAME_FOR_YEAR, 'parents': [DRIVE_FOLDER_ID], 'mimeType': 'text/csv'}, media_body=media).execute()
        
        # 메일용 분석 로직
        school_stats, innodep_today_dict, innodep_total_amt = {}, {}, 0
        for row in final_data:
            try:
                org, comp, amt_val, item_name, contract = str(row[7]), str(row[21]), str(row[20]), str(row[14]), str(row[23])
                amt = int(amt_val.replace(',', '').split('.')[0])
                if '학교' in org and '지능형' in contract and 'CCTV' in contract:
                    if org not in school_stats: school_stats[org] = {'total_amt': 0, 'main_vendor': '', 'vendor_priority': 3}
                    school_stats[org]['total_amt'] += amt
                    priority = 1 if '영상감시장치' in item_name else 2 if '보안용카메라' in item_name else 3
                    if priority < school_stats[org]['vendor_priority']:
                        school_stats[org]['main_vendor'], school_stats[org]['vendor_priority'] = comp, priority
                if '이노뎁' in comp:
                    innodep_today_dict[org] = innodep_today_dict.get(org, 0) + amt
                    innodep_total_amt += amt
            except: continue

        summary_lines = [f"⭐ {d_str} 학교 지능형 CCTV 납품 현황:"]
        for s, info in school_stats.items(): summary_lines.append(f"- {s} [{info['main_vendor']}]: {info['total_amt']:,}원")
        if not school_stats: summary_lines.append(" 0건")
        
        summary_lines.extend([" ", f"🏢 {d_str} 이노뎁 실적:"])
        for o, a in innodep_today_dict.items(): summary_lines.append(f"- {o}: {a:,}원")
        summary_lines.append(f"** 총합계: {innodep_total_amt:,}원") if innodep_today_dict else summary_lines.append(" 0건")

        servc_html = fetch_and_generate_servc_html(target_dt)

        if "GITHUB_OUTPUT" in os.environ:
            with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:
                f.write(f"collect_date={d_str}\ncollect_count={len(final_data)}\nschool_info<<EOF\n")
                for line in summary_lines: f.write(f"{line}<br>\n")
                f.write(f"EOF\nservc_info<<EOF\n{servc_html}\nEOF\n")

    # --- PART 2: 입찰 공고 수집 (G2B_notice 통합 기능) ---
    print(f"🚀 입찰 공고 수집 시작 ({d_str})")
    for cat, url in NOTICE_API_MAP.items():
        n_df = fetch_notice_data(cat, url, d_str)
        if not n_df.empty:
            f_name = f"나라장터_공고_{cat}.csv"
            res = drive_service.files().list(q=f"name='{f_name}' and '1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr' in parents and trashed=false", fields='files(id)').execute()
            items = res.get('files', [])
            fid = items[0]['id'] if items else None
            
            if fid:
                resp = requests.get(f'https://www.googleapis.com/drive/v3/files/{fid}?alt=media', headers={'Authorization': f'Bearer {drive_creds.token}'})
                if resp.status_code == 200:
                    n_df = pd.concat([pd.read_csv(io.BytesIO(resp.content), encoding='utf-8-sig', low_memory=False), n_df], ignore_index=True)
            
            n_df.drop_duplicates(subset=['bidNtceNo'], keep='last', inplace=True)
            media = MediaIoBaseUpload(io.BytesIO(n_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')), mimetype='text/csv', resumable=True)
            if fid: drive_service.files().update(fileId=fid, media_body=media).execute()
            else: drive_service.files().create(body={'name': f_name, 'parents': ['1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr'], 'mimeType': 'text/csv'}, media_body=media).execute()
            print(f"✅ {cat} 공고 업데이트 완료")

if __name__ == "__main__":
    main()
