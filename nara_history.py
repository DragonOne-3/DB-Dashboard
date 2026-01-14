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
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 설정 ---
API_KEY = os.environ.get('DATA_GO_KR_API_KEY')
API_URL = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'
MAX_WORKERS = 5 

def get_gs_client():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    creds_dict = json.loads(auth_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def clean_name(raw_text, index):
    if not raw_text or '^' not in raw_text: return raw_text
    parts = raw_text.replace('[', '').replace(']', '').split('^')
    return parts[index] if len(parts) > index else raw_text

def fetch_kw_data(kw, start, end):
    rows = []
    params = {
        'serviceKey': API_KEY, 'pageNo': '1', 'numOfRows': '999',
        'inqryDiv': '1', 'type': 'xml', 'inqryBgnDate': start, 'inqryEndDate': end, 'cntrctNm': kw
    }
    try:
        res = requests.get(API_URL, params=params, timeout=60)
        if not res.text.strip().endswith('</response>'): return []
        root = ET.fromstring(res.content)
        for item in root.findall('.//item'):
            raw = {child.tag: child.text for child in item}
            c_date = raw.get('cntrctDate') or raw.get('cntrctCnclsDate') or ''
            processed = {
                '★가공_계약일': f"{c_date[:4]}-{c_date[4:6]}-{c_date[6:8]}" if len(c_date)>=8 else "-",
                '★가공_착수일': raw.get('stDate', '-'),
                '★가공_만료일': raw.get('ttalScmpltDate') or raw.get('thtmScmpltDate') or '-',
                '★가공_수요기관': raw.get('dminsttList', ''),
                '★가공_계약명': raw.get('cntrctNm', ''),
                '★가공_업체명': raw.get('corpList', ''),
                '★가공_계약금액': int(raw.get('totCntrctAmt', 0))
            }
            processed.update(raw)
            rows.append(processed)
    except: pass
    return rows

def main():
    sh = get_gs_client().open("나라장터_용역계약내역")
    ws = sh.get_worksheet(0)
    
    # 🚨 시작일을 2025년 5월 1일로 고정하여 복구 시작
    start_dt = datetime(2025, 5, 1)
    end_dt = datetime.now() - timedelta(days=1)
    
    keywords = ['CCTV', '통합관제', '주차관리', '영상감시장치', '영상정보처리기기']
    
    curr = start_dt
    while curr <= end_dt:
        # 기간을 3일 단위로 쪼개어 서버 부하 및 끊김 방지
        c_start = curr.strftime("%Y%m%d")
        c_end_dt = curr + timedelta(days=2)
        if c_end_dt > end_dt: c_end_dt = end_dt
        c_end = c_end_dt.strftime("%Y%m%d")
        
        print(f"🚀 복구 중: {c_start} ~ {c_end} ...")
        
        period_data = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(fetch_kw_data, kw, c_start, c_end) for kw in keywords]
            for f in as_completed(futures):
                period_data.extend(f.result())
        
        if period_data:
            ws.append_rows(pd.DataFrame(period_data).values.tolist(), value_input_option='RAW')
            print(f"   ✅ {len(period_data)}건 시트 추가 완료")
            time.sleep(2)
        
        curr = c_end_dt + timedelta(days=1)

if __name__ == "__main__":
    main()
