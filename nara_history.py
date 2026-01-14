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

# --- 설정 ---
API_KEY = os.environ.get('DATA_GO_KR_API_KEY')
API_URL = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'

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
    try:
        return datetime.strptime(date_str[:8], "%Y%m%d").strftime("%Y-%m-%d")
    except: return date_str

def fetch_g2b_data_by_period(start_date, end_date):
    keywords = ['CCTV', '통합관제', '주차관리', '영상감시장치', '영상정보처리기기']
    period_rows = []
    
    for kw in keywords:
        page_no = 1
        print(f"   - 키워드 '{kw}' 수집 중 ({start_date} ~ {end_date})...")
        while True:
            params = {
                'serviceKey': API_KEY, 'pageNo': str(page_no), 'numOfRows': '999',
                'inqryDiv': '1', 'type': 'xml', 'inqryBgnDate': start_date, 'inqryEndDate': end_date, 'cntrctNm': kw
            }
            try:
                res = requests.get(API_URL, params=params, timeout=90)
                if not res.text.strip().endswith('</response>'): break
                    
                root = ET.fromstring(res.content)
                items = root.findall('.//item')
                if not items: break
                
                for item in items:
                    raw_dict = {child.tag: child.text for child in item}
                    
                    # 날짜 데이터 추출
                    raw_c_date = raw_dict.get('cntrctDate') or raw_dict.get('cntrctCnclsDate') or ''
                    raw_s_date = raw_dict.get('stDate', '') 
                    raw_e_date = raw_dict.get('ttalScmpltDate') or raw_dict.get('thtmScmpltDate') or ''
                    
                    # 만료일 계산
                    fmt_e_date = "-"
                    if raw_e_date:
                        if '일' in raw_e_date and raw_c_date:
                            try:
                                days_val = int(re.sub(r'[^0-9]', '', raw_e_date))
                                start_dt = datetime.strptime(raw_c_date[:8], "%Y%m%d")
                                fmt_e_date = (start_dt + timedelta(days=days_val)).strftime("%Y-%m-%d")
                            except: fmt_e_date = raw_e_date
                        else:
                            fmt_e_date = format_date(raw_e_date)

                    # 가공 필드 생성
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
                    period_rows.append(processed_dict)
                
                if page_no * 999 >= int(root.find('.//totalCount').text): break
                page_no += 1
                time.sleep(0.5)
            except: break
    return period_rows

def remove_duplicates(ws):
    """지정한 4개 항목(계약번호, 수요기관, 업체명, 계약명)이 모두 일치하면 중복 제거"""
    print("🧹 데이터 정제 시작 (중복 제거 중...)")
    all_data = ws.get_all_records()
    if not all_data: return
    
    df = pd.DataFrame(all_data)
    
    # 중복 기준 설정: 계약번호, 가공_수요기관, 가공_업체명, 가공_계약명
    duplicate_columns = ['cntrctNo', '★가공_수요기관', '★가공_업체명', '★가공_계약명']
    
    # 존재하는 컬럼만 기준으로 삼기 (오류 방지)
    valid_cols = [c for c in duplicate_columns if c in df.columns]
    
    if valid_cols:
        before_cnt = len(df)
        # 모든 기준이 일치하는 경우 첫 번째 행만 남김
        df = df.drop_duplicates(subset=valid_cols, keep='first')
        after_cnt = len(df)
        
        if after_cnt < before_cnt:
            ws.clear()
            # 헤더와 데이터를 다시 쓰기
            ws.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option='RAW')
            print(f"✅ 중복 제거 완료: {before_cnt}건 -> {after_cnt}건")
        else:
            print("ℹ️ 중복된 데이터가 발견되지 않았습니다.")

def main():
    try:
        client = get_gspread_client()
        sh = client.open("나라장터_용역계약내역")
        ws = sh.get_worksheet(0)
        
        # 1. 제목줄 체크 및 생성
        first_cell = ws.acell('A1').value
        if not first_cell:
            print("📝 제목줄 생성 중...")
            sample = fetch_g2b_data_by_period("20240101", "20240101")
            if sample:
                ws.update('A1', [list(sample[0].keys())])
        
        # 2. 기간별 수집 (7일 단위)
        start_date = datetime(2024, 1, 1)
        end_date = datetime.now() - timedelta(days=1)
        curr = start_date
        
        while curr <= end_date:
            c_start = curr.strftime("%Y%m%d")
            c_end_dt = curr + timedelta(days=6)
            if c_end_dt > end_date: c_end_dt = end_date
            c_end = c_end_dt.strftime("%Y%m%d")
            
            print(f"🚀 {c_start} ~ {c_end} 구간 수집...")
            data_list = fetch_g2b_data_by_period(c_start, c_end)
            
            if data_list:
                df = pd.DataFrame(data_list).fillna('')
                ws.append_rows(df.values.tolist(), value_input_option='RAW')
                print(f"   ✅ {len(df)}건 저장 완료.")
                time.sleep(3)
            curr = c_end_dt + timedelta(days=1)

        # 3. 마지막 단계: 강화된 중복 제거 실행
        remove_duplicates(ws)
        print("🎊 모든 작업이 성공적으로 완료되었습니다.")

    except Exception as e:
        traceback.print_exc()

if __name__ == "__main__":
    main()
