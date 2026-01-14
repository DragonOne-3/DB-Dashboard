import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import time
import re
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 설정 ---
API_KEY = os.environ.get('DATA_GO_KR_API_KEY')
API_URL = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'
MAX_WORKERS = 5 # 동시에 실행할 작업 수 (키워드 개수와 맞춤)

def get_gspread_client():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    creds_dict = json.loads(auth_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def clean_name(raw_text, index):
    if not raw_text or '^' not in raw_text: return raw_text
    parts = raw_text.replace('[', '').replace(']', '').split('^')
    return parts[index] if len(parts) > index else raw_text

def format_date(date_str):
    if not date_str or len(date_str) < 8: return "-"
    try: return datetime.strptime(date_str[:8], "%Y%m%d").strftime("%Y-%m-%d")
    except: return date_str

def fetch_single_keyword(kw, start_date, end_date):
    """특정 키워드 한 개에 대해 데이터를 수집하는 함수 (병렬 실행용)"""
    keyword_rows = []
    page_no = 1
    while True:
        params = {
            'serviceKey': API_KEY, 'pageNo': str(page_no), 'numOfRows': '999',
            'inqryDiv': '1', 'type': 'xml', 'inqryBgnDate': start_date, 'inqryEndDate': end_date, 'cntrctNm': kw
        }
        try:
            res = requests.get(API_URL, params=params, timeout=60)
            if not res.text.strip().endswith('</response>'): break
            root = ET.fromstring(res.content)
            items = root.findall('.//item')
            if not items: break
            
            for item in items:
                raw_dict = {child.tag: child.text for child in item}
                raw_c_date = raw_dict.get('cntrctDate') or raw_dict.get('cntrctCnclsDate') or ''
                raw_s_date = raw_dict.get('stDate', '') 
                raw_e_date = raw_dict.get('ttalScmpltDate') or raw_dict.get('thtmScmpltDate') or ''
                
                fmt_e_date = "-"
                if raw_e_date:
                    if '일' in raw_e_date and raw_c_date:
                        try:
                            days_val = int(re.sub(r'[^0-9]', '', raw_e_date))
                            fmt_e_date = (datetime.strptime(raw_c_date[:8], "%Y%m%d") + timedelta(days=days_val)).strftime("%Y-%m-%d")
                        except: fmt_e_date = raw_e_date
                    else: fmt_e_date = format_date(raw_e_date)

                processed_dict = {
                    '★가공_계약일': format_date(raw_c_date),
                    '★가공_착수일': format_date(raw_s_date),
                    '★가공_만료일': fmt_e_date,
                    '★가공_수요기관': clean_name(raw_dict.get('dminsttList', ''), 2),
                    '★가공_계약명': raw_dict.get('cntrctNm', ''),
                    '★가공_업체명': clean_name(raw_dict.get('corpList', ''), 3),
                    '★가공_계약금액': int(raw_dict.get('totCntrctAmt', '0'))
                }
                processed_dict.update(raw_dict)
                keyword_rows.append(processed_dict)
            
            total_count = int(root.find('.//totalCount').text)
            if page_no * 999 >= total_count: break
            page_no += 1
            time.sleep(0.3)
        except: break
    return keyword_rows

def main():
    try:
        client = get_gspread_client()
        sh = client.open("나라장터_용역계약내역")
        ws = sh.get_worksheet(0)
        
        keywords = ['CCTV', '통합관제', '주차관리', '영상감시장치', '영상정보처리기기']
        start_date = datetime(2024, 1, 1)
        end_date = datetime.now() - timedelta(days=1)
        
        # 제목줄 체크
        if not ws.acell('A1').value:
            sample = fetch_single_keyword(keywords[0], "20240101", "20240101")
            if sample: ws.update('A1', [list(sample[0].keys())])

        current_date = start_date
        while current_date <= end_date:
            # 기간 단위 (병렬 처리 시에는 14일 정도로 조금 더 넓게 잡아도 안전합니다)
            chunk_start = current_date.strftime("%Y%m%d")
            chunk_end_dt = current_date + timedelta(days=13)
            if chunk_end_dt > end_date: chunk_end_dt = end_date
            chunk_end = chunk_end_dt.strftime("%Y%m%d")
            
            print(f"🚀 병렬 수집 시작: {chunk_start} ~ {chunk_end}")
            
            all_period_data = []
            # --- 병렬 실행 구간 ---
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(fetch_single_keyword, kw, chunk_start, chunk_end): kw for kw in keywords}
                for future in as_completed(futures):
                    kw_result = future.result()
                    all_period_data.extend(kw_result)
            # --------------------

            if all_period_data:
                df = pd.DataFrame(all_period_data).fillna('')
                ws.append_rows(df.values.tolist(), value_input_option='RAW')
                print(f"   ✅ {len(df)}건 시트 저장 완료 (구간 합계)")
                time.sleep(2) # 구글 API 안정화
            
            current_date = chunk_end_dt + timedelta(days=1)

        print("🎊 병렬 수집 및 저장이 모두 완료되었습니다.")

    except Exception as e:
        traceback.print_exc()

if __name__ == "__main__":
    main()
