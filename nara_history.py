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

# --- 1. 설정 및 API 정보 ---
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
    """특정 기간 동안 5개 키워드로 데이터 수집 (API 호출 핵심 로직)"""
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
                res = requests.get(API_URL, params=params, timeout=30)
                
                # 오류 원인 파악을 위한 체크 추가
                if res.status_code != 200:
                    print(f"❌ API 연결 실패: {res.status_code}")
                    break

                # XML 내용이 비었는지 확인
                if not res.text or not res.text.strip():
                    print(f"⚠️ {kw}: 응답 본문이 비어 있습니다.")
                    break

                if not res.text.strip().startswith('<?xml'):
                    print(f"⚠️ {kw}: XML 형식이 아닙니다. 응답내용: {res.text[:100]}")
                    break
                
                # 파싱 시도
                root = ET.fromstring(res.content)
                
                # 결과 코드 확인 (00이 아니면 에러)
                result_code = root.find('.//resultCode')
                if result_code is not None and result_code.text != '00':
                    msg = root.find('.//resultMsg').text if root.find('.//resultMsg') is not None else "알 수 없는 에러"
                    print(f"❌ API 서버 에러: {msg} (코드: {result_code.text})")
                    break

                items = root.findall('.//item')
                if not items:
                    break
                
                # ... (이하 수집 로직 동일) ...
                
                for item in items:
                    raw_dict = {child.tag: child.text for child in item}
                    cntrct_nm = raw_dict.get('cntrctNm', '')
                    
                    # 가공 데이터 생성
                    demand = clean_name(raw_dict.get('dminsttList', ''), 2)
                    corp = clean_name(raw_dict.get('corpList', ''), 3)
                    c_date = raw_dict.get('cntrctDate') or raw_dict.get('cntrctCnclsDate') or '00000000'
                    e_date = raw_dict.get('ttalScmpltDate', '')
                    amt = int(raw_dict.get('totCntrctAmt', '0'))
                    
                    # 만료일 계산
                    final_end_date = "-"
                    if e_date and c_date:
                        try:
                            if '일' in e_date:
                                days = int(re.sub(r'[^0-9]', '', e_date))
                                final_end_date = (datetime.strptime(c_date[:8], "%Y%m%d") + timedelta(days=days)).strftime("%Y-%m-%d")
                            else:
                                final_end_date = datetime.strptime(e_date[:8], "%Y%m%d").strftime("%Y-%m-%d")
                        except: final_end_date = e_date

                    processed_dict = {
                        '★수집일자': datetime.now().strftime("%Y-%m-%d"),
                        '★가공_계약일자': c_date[:8],
                        '★가공_수요기관': demand,
                        '★가공_업체명': corp,
                        '★가공_계약금액': amt,
                        '★가공_계약만료일': final_end_date,
                        '★가공_계약명': cntrct_nm
                    }
                    processed_dict.update(raw_dict)
                    period_rows.append(processed_dict)
                
                total_count = int(root.find('.//totalCount').text)
                if page_no * 999 >= total_count: break
                page_no += 1
                time.sleep(0.3)
            except Exception as e:
                print(f"❌ API 호출 중 오류: {e}")
                break
    return period_rows

def main():
    try:
        client = get_gspread_client()
        sh = client.open("나라장터_용역계약내역")
        ws = sh.get_worksheet(0)
        
        # --- 기간 분할 로직 (1년 단위로 리스트 생성) ---
        # 2024년 전체, 2025년 전체, 2026년 현재까지
        date_chunks = [
            ("20240101", "20241231"),
            ("20250101", "20251231"),
            ("20260101", (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"))
        ]
        
        for start, end in date_chunks:
            print(f"🚀 {start} ~ {end} 기간 데이터 수집 시작...")
            data_list = fetch_g2b_data_by_period(start, end)
            
            if data_list:
                df = pd.DataFrame(data_list).fillna('')
                existing_values = ws.get_all_values()
                
                if not existing_values:
                    ws.update([df.columns.values.tolist()] + df.values.tolist())
                else:
                    ws.append_rows(df.values.tolist())
                print(f"✅ {start}~{end} 기간 {len(df)}건 저장 완료.\n")
                time.sleep(1) # 시트 API 과부하 방지
            else:
                print(f"ℹ️ {start}~{end} 기간 데이터 없음.\n")

        print("🎊 모든 기간 데이터 축적이 완료되었습니다.")

    except Exception as e:
        print(f"❌ 시스템 오류: {e}")

if __name__ == "__main__":
    main()
