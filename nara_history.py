import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import time
from urllib.parse import unquote

# --- 설정 ---
API_KEY = unquote(os.environ.get('DATA_GO_KR_API_KEY', ''))
API_URL = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'

EXCLUDE_KEYWORDS = [
    '감리', '데이터베이스', '교육', 'ISP', '구조물', '관광', '가명', '익명', '검토', '의료', '귀농', '귀촌',
    '실시', '설계', '바이오', '콘텐츠', '거래', '탄소', '농수산물', '도매', '컨설팅', '가이드라인', '굿즈', '폐기물', '인사', '육아', '수산물', '목재', '주소',
    '하드웨어', '3차원', '3D', '유산', '문화', '대행'
]

def get_gs_client():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    creds_dict = json.loads(auth_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def main():
    # 1. 과거 데이터 수집 기간 설정 (2024년 1월 1일 ~ 현재)
    start_dt = datetime(2023, 1, 1)
    end_dt = datetime(2024, 1, 1)
    
    keywords = list(set([
        'CCTV', '통합관제', '주차관리', '영상감시장치', '영상정보처리기기', '국방', '부대', '작전', '경계', '방위',
        '데이터','플랫폼','솔루션','군사', '무인화', '사령부', '군대','스마트시티','스마트도시','ITS','GIS'
    ]))
    
    all_fetched_rows = []
    print(f"🚀 {start_dt.strftime('%Y-%m-%d')} ~ {end_dt.strftime('%Y-%m-%d')} 과거 데이터 수집 시작 (429 방지 로직 적용)")

    current_dt = start_dt
    while current_dt <= end_dt:
        chunk_start = current_dt.strftime("%Y%m%d")
        chunk_end_dt = current_dt + timedelta(days=29)
        if chunk_end_dt > end_dt: chunk_end_dt = end_dt
        chunk_end = chunk_end_dt.strftime("%Y%m%d")
        
        print(f"📅 구간: {chunk_start} ~ {chunk_end}")
        
        for kw in keywords:
            params = {
                'serviceKey': API_KEY, 'pageNo': '1', 'numOfRows': '999',
                'inqryDiv': '1', 'type': 'xml', 
                'inqryBgnDate': chunk_start, 'inqryEndDate': chunk_end, 'cntrctNm': kw
            }
            
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries:
                try:
                    res = requests.get(API_URL, params=params, timeout=60)
                    
                    if res.status_code == 200:
                        content = res.text.strip()
                        if content.startswith('<?xml') or content.startswith('<response'):
                            root = ET.fromstring(res.content)
                            result_code = root.find('.//resultCode')
                            
                            if result_code is not None and result_code.text == '00':
                                items = root.findall('.//item')
                                for item in items:
                                    raw = {child.tag: child.text for child in item}
                                    cntrct_nm = raw.get('cntrctNm', '')
                                    if any(ex_kw in cntrct_nm for ex_kw in EXCLUDE_KEYWORDS):
                                        continue
                                    
                                    # 데이터 정제
                                    raw_demand = raw.get('dminsttList', '') or ''
                                    demand_parts = raw_demand.replace('[', '').replace(']', '').split('^')
                                    clean_demand = demand_parts[2] if len(demand_parts) > 2 else raw_demand
                                    raw_corp = raw.get('corpList', '') or ''
                                    corp_parts = raw_corp.replace('[', '').replace(']', '').split('^')
                                    clean_corp = corp_parts[3] if len(corp_parts) > 3 else raw_corp

                                    processed = {
                                        '★가공_계약일': raw.get('cntrctDate', ''),
                                        '★가공_착수일': raw.get('stDate', '-'),
                                        '★가공_만료일': raw.get('ttalScmpltDate') or raw.get('thtmScmpltDate') or '-',
                                        '★가공_수요기관': clean_demand,
                                        '★가공_계약명': cntrct_nm,
                                        '★가공_업체명': clean_corp,
                                        '★가공_계약금액': int(raw.get('totCntrctAmt', 0)) if raw.get('totCntrctAmt') else 0
                                    }
                                    processed.update(raw)
                                    all_fetched_rows.append(processed)
                                
                                if len(items) > 0:
                                    print(f"   > '{kw}': {len(items)}건 처리")
                                break # 성공 시 retry 루프 탈출
                        
                    elif res.status_code == 429:
                        print(f"   ⚠️ 429 에러 발생 (트래픽 초과). 10초 대기 후 재시도... ({retry_count+1}/{max_retries})")
                        time.sleep(10)
                        retry_count += 1
                    else:
                        print(f"   ❌ {kw} HTTP 에러: {res.status_code}")
                        break
                
                except Exception as e:
                    print(f"   ❌ {kw} 오류: {e}")
                    break
                
                # 기본 API 간격 유지 (안전하게 1.5초)
                time.sleep(1.5)
            
        current_dt = chunk_end_dt + timedelta(days=1)

    # 3. 저장 로직
    if all_fetched_rows:
        df = pd.DataFrame(all_fetched_rows)
        df = df.drop_duplicates(subset=['cntrctNo']) if 'cntrctNo' in df.columns else df.drop_duplicates()
        try:
            sh = get_gs_client().open("나라장터_용역계약내역")
            ws = sh.get_worksheet(0)
            if not ws.row_values(1): ws.insert_row(df.columns.tolist(), 1)
            values = df.fillna('').values.tolist()
            for i in range(0, len(values), 2000):
                ws.append_rows(values[i:i+2000], value_input_option='RAW')
            print(f"✅ 총 {len(df)}건 업데이트 완료")
        except Exception as e:
            print(f"❌ 시트 저장 오류: {e}")
    else:
        print("ℹ️ 수집 데이터 없음")

if __name__ == "__main__":
    main()
