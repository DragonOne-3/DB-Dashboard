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
                content = res.text.strip()
                if not content.endswith('</response>'):
                    print(f"      ⚠️ {kw}: 데이터 잘림 발생.")
                    break
                    
                root = ET.fromstring(res.content)
                items = root.findall('.//item')
                if not items: break
                
                for item in items:
                    raw_dict = {child.tag: child.text for child in item}
                    
                    cntrct_nm = raw_dict.get('cntrctNm', '')
                    raw_c_date = raw_dict.get('cntrctDate') or raw_dict.get('cntrctCnclsDate') or ''
                    raw_e_date = raw_dict.get('ttalScmpltDate', '')
                    
                    demand = clean_name(raw_dict.get('dminsttList', ''), 2)
                    corp = clean_name(raw_dict.get('corpList', ''), 3)
                    amt = int(raw_dict.get('totCntrctAmt', '0'))
                    
                    # 계약일자 가공
                    fmt_c_date = "-"
                    if len(raw_c_date) >= 8:
                        try:
                            fmt_c_date = datetime.strptime(raw_c_date[:8], "%Y%m%d").strftime("%Y-%m-%d")
                        except: fmt_c_date = raw_c_date

                    # 계약만료일 가공
                    fmt_e_date = "-"
                    if raw_e_date and raw_c_date:
                        try:
                            if '일' in raw_e_date:
                                days_val = int(re.sub(r'[^0-9]', '', raw_e_date))
                                start_dt = datetime.strptime(raw_c_date[:8], "%Y%m%d")
                                fmt_e_date = (start_dt + timedelta(days=days_val)).strftime("%Y-%m-%d")
                            elif len(raw_e_date) >= 8:
                                fmt_e_date = datetime.strptime(raw_e_date[:8], "%Y%m%d").strftime("%Y-%m-%d")
                            else:
                                fmt_e_date = raw_e_date
                        except: fmt_e_date = raw_e_date

                    processed_dict = {
                        '★가공_계약일자': fmt_c_date,
                        '★가공_수요기관': demand,
                        '★가공_계약명': cntrct_nm,
                        '★가공_업체명': corp,
                        '★가공_계약금액': amt,
                        '★가공_계약만료일': fmt_e_date
                    }
                    processed_dict.update(raw_dict)
                    period_rows.append(processed_dict)
                
                total_count_node = root.find('.//totalCount')
                if total_count_node is not None:
                    if page_no * 999 >= int(total_count_node.text): break
                else: break
                page_no += 1
                time.sleep(1)
            except Exception:
                break
    return period_rows

def main():
    try:
        print("🔗 구글 시트 연결 시도...")
        client = get_gspread_client()
        sh = client.open("나라장터_용역계약내역")
        ws = sh.get_worksheet(0)
        
        # --- [중요] 제목줄 체크 및 생성 ---
        existing_data = ws.get_all_values()
        header_exists = len(existing_data) > 0
        
        start_date = datetime(2024, 1, 1)
        end_date = datetime.now() - timedelta(days=1)
        
        current_date = start_date
        while current_date <= end_date:
            chunk_start = current_date.strftime("%Y%m%d")
            chunk_end_dt = current_date + timedelta(days=6)
            if chunk_end_dt > end_date: chunk_end_dt = end_date
            chunk_end = chunk_end_dt.strftime("%Y%m%d")
            
            print(f"🚀 {chunk_start} ~ {chunk_end} 구간 수집...")
            data_list = fetch_g2b_data_by_period(chunk_start, chunk_end)
            
            if data_list:
                df = pd.DataFrame(data_list).fillna('')
                
                # 제목줄이 없는 경우 처음에만 헤더를 포함하여 업데이트
                if not header_exists:
                    ws.update([df.columns.values.tolist()] + df.values.tolist())
                    header_exists = True # 이제 제목이 생겼음을 표시
                else:
                    # 제목이 이미 있으면 데이터만 밑에 추가
                    ws.append_rows(df.values.tolist())
                
                print(f"   ✅ {len(df)}건 저장 완료.")
                time.sleep(3)
            
            current_date = chunk_end_dt + timedelta(days=1)

        print("🎊 모든 작업이 완료되었습니다.")

    except Exception as e:
        traceback.print_exc()

if __name__ == "__main__":
    main()
