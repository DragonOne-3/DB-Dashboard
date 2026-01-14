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
                # 타임아웃을 충분히 주고, 스트리밍 방식으로 데이터를 받지 않도록 처리
                res = requests.get(API_URL, params=params, timeout=90)
                
                # XML이 불완전하게 끝나는지 체크 (가장 마지막 태그 확인)
                content = res.text.strip()
                if not content.endswith('</response>'):
                    print(f"      ⚠️ {kw}: 데이터 잘림 발생(페이지 {page_no}). 기간을 더 좁혀야 할 수 있습니다.")
                    break
                    
                root = ET.fromstring(res.content)
                items = root.findall('.//item')
                if not items: break
                
                for item in items:
                    raw_dict = {child.tag: child.text for child in item}
                    cntrct_nm = raw_dict.get('cntrctNm', '')
                    
                    demand = clean_name(raw_dict.get('dminsttList', ''), 2)
                    corp = clean_name(raw_dict.get('corpList', ''), 3)
                    c_date = raw_dict.get('cntrctDate') or raw_dict.get('cntrctCnclsDate') or '00000000'
                    e_date = raw_dict.get('ttalScmpltDate', '')
                    amt = int(raw_dict.get('totCntrctAmt', '0'))
                    
                    f_end = "-"
                    if e_date and c_date:
                        try:
                            if '일' in e_date:
                                days = int(re.sub(r'[^0-9]', '', e_date))
                                f_end = (datetime.strptime(c_date[:8], "%Y%m%d") + timedelta(days=days)).strftime("%Y-%m-%d")
                            else:
                                f_end = datetime.strptime(e_date[:8], "%Y%m%d").strftime("%Y-%m-%d")
                        except: f_end = e_date

                    processed_dict = {
                        '★수집일자': datetime.now().strftime("%Y-%m-%d"),
                        '★가공_계약일자': c_date[:8],
                        '★가공_수요기관': demand,
                        '★가공_업체명': corp,
                        '★가공_계약금액': amt,
                        '★가공_계약만료일': f_end,
                        '★가공_계약명': cntrct_nm
                    }
                    processed_dict.update(raw_dict)
                    period_rows.append(processed_dict)
                
                total_count_node = root.find('.//totalCount')
                if total_count_node is not None:
                    total_count = int(total_count_node.text)
                    if page_no * 999 >= total_count: break
                else: break
                
                page_no += 1
                time.sleep(1) # 서버 부하 방지
            except ET.ParseError:
                print(f"      ❌ {kw}: XML 파싱 에러 (데이터가 도중에 끊김)")
                break
            except Exception as e:
                print(f"      ❌ 오류: {e}")
                break
    return period_rows

def main():
    try:
        print("🔗 구글 시트 연결 시도...")
        client = get_gspread_client()
        sh = client.open("나라장터_용역계약내역")
        ws = sh.get_worksheet(0)
        
        # --- 기간을 '7일' 단위로 쪼개기 ---
        start_date = datetime(2024, 1, 1)
        end_date = datetime.now() - timedelta(days=1)
        
        current_date = start_date
        while current_date <= end_date:
            chunk_start = current_date.strftime("%Y%m%d")
            # 7일 후 날짜 계산
            chunk_end_dt = current_date + timedelta(days=6)
            
            if chunk_end_dt > end_date:
                chunk_end_dt = end_date
                
            chunk_end = chunk_end_dt.strftime("%Y%m%d")
            
            print(f"🚀 {chunk_start} ~ {chunk_end} 구간 수집 시작...")
            data_list = fetch_g2b_data_by_period(chunk_start, chunk_end)
            
            if data_list:
                df = pd.DataFrame(data_list).fillna('')
                existing_values = ws.get_all_values()
                if not existing_values:
                    ws.update([df.columns.values.tolist()] + df.values.tolist())
                else:
                    ws.append_rows(df.values.tolist())
                print(f"   ✅ {len(df)}건 시트 저장 완료.")
                time.sleep(3) # 구글 시트 API 할당량 관리 (매우 중요)
            
            # 다음 구간(7일 후)으로 이동
            current_date = chunk_end_dt + timedelta(days=1)

        print("🎊 모든 작업이 완료되었습니다.")

    except Exception as e:
        traceback.print_exc()

if __name__ == "__main__":
    main()
