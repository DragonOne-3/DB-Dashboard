import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
import time

# --- 설정 ---
API_KEY = os.environ.get('DATA_GO_KR_API_KEY')
# 안정성을 위해 XML 기반 API 엔드포인트 사용
API_URL = 'http://apis.data.go.kr/1230000/Service_7/getServcCntrctInfoService01'

def get_gs_client():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    creds_dict = json.loads(auth_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def main():
    start_dt = datetime(2025, 1, 1)
    end_dt = datetime.now()
    
    contract_keywords = ['작전', '경계', '무인화', '국방', '군사', '부대']
    all_fetched_rows = []
    
    current_start = start_dt
    while current_start < end_dt:
        current_end = current_start + timedelta(days=90)
        if current_end > end_dt:
            current_end = end_dt
            
        s_str = current_start.strftime("%Y%m%d")
        e_str = current_end.strftime("%Y%m%d")
        
        print(f"📅 구간 조회 중: {s_str} ~ {e_str}")
        
        # 구간별 수집
        for page in range(1, 11): 
            params = {
                'serviceKey': API_KEY,
                'type': 'xml', # XML 형식이 나라장터 API에서 더 안정적입니다
                'numOfRows': '999',
                'pageNo': str(page),
                'inqryBgnDt': s_str,
                'inqryEndDt': e_str,
                'inqryDiv': '1'
            }
            
            try:
                res = requests.get(API_URL, params=params, timeout=60)
                if res.status_code != 200:
                    print(f"⚠️ API 서버 응답 이상 (Status: {res.status_code})")
                    break
                
                # XML 파싱
                root = ET.fromstring(res.content)
                items = root.findall('.//item')
                
                if not items:
                    break
                    
                for item in items:
                    # XML 태그에서 데이터 추출
                    cntrct_name = item.findtext('cntrctNm', '')
                    
                    if any(kw in cntrct_name for kw in contract_keywords) and '상수도' not in cntrct_name:
                        processed = [
                            item.findtext('orderInsttNm', ''),
                            cntrct_name,
                            item.findtext('mainEntrpsNm', '-'),
                            int(item.findtext('cntrctAmt', '0')),
                            item.findtext('cntrctDate', ''),
                            item.findtext('strtDate', '-'),
                            item.findtext('cntrctPrdNm', '-'),
                            item.findtext('totScmpltDate', '') or item.findtext('endDate', ''),
                            f"https://www.g2b.go.kr:8067/co/common/moveCntrctDetail.do?cntrctNo={item.findtext('cntrctNo')}&cntrctOrdNo={item.findtext('cntrctOrdNo', '00')}"
                        ]
                        all_fetched_rows.append(processed)
                
                time.sleep(1.0) # 서버 부하 방지를 위해 대기 시간 증가
            except Exception as e:
                print(f"❌ {s_str} 구간 {page}페이지 오류: {e}")
                continue # 오류 발생 시 중단하지 않고 다음 페이지/구간 시도
        
        current_start = current_end + timedelta(days=1)

    # 데이터 저장
    if all_fetched_rows:
        df = pd.DataFrame(all_fetched_rows)
        unique_rows = df.drop_duplicates().values.tolist()
        
        try:
            sh = get_gs_client().open("나라장터_용역계약내역")
            ws = sh.get_worksheet(0)
            ws.append_rows(unique_rows, value_input_option='USER_ENTERED')
            print(f"✨ 최종 완료! {len(unique_rows)}건 추가됨.")
        except Exception as e:
            print(f"❌ 시트 저장 오류: {e}")
    else:
        print("ℹ️ 수집된 데이터가 없습니다. 키워드나 API 상태를 확인하세요.")

if __name__ == "__main__":
    main()
