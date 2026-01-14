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

# --- 설정 ---
# 서비스키 인코딩 문제를 방지하기 위해 unquote 적용
API_KEY = requests.utils.unquote(os.environ.get('DATA_GO_KR_API_KEY'))
API_URL = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'

def get_gs_client():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    creds_dict = json.loads(auth_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def main():
    try:
        sh = get_gs_client().open("나라장터_용역계약내역")
        ws = sh.get_worksheet(0)
        
        # 🚨 시작일을 데이터가 끊긴 2025년 5월 1일로 설정
        curr = datetime(2025, 5, 1)
        end_dt = datetime.now()
        
        keywords = ['CCTV', '통합관제', '주차관리', '영상감시장치', '영상정보처리기기']
        
        print("🚑 데이터 긴급 복구를 시작합니다...")

        while curr <= end_dt:
            date_str = curr.strftime("%Y%m%d")
            day_data = []
            
            for kw in keywords:
                params = {
                    'serviceKey': API_KEY, 'pageNo': '1', 'numOfRows': '999',
                    'inqryDiv': '1', 'type': 'xml', 'inqryBgnDate': date_str, 'inqryEndDate': date_str, 'cntrctNm': kw
                }
                
                try:
                    # 서버 차단을 피하기 위해 요청 사이의 간격을 둡니다.
                    time.sleep(0.7)
                    res = requests.get(API_URL, params=params, timeout=30)
                    
                    # 서버 응답이 비정상일 경우 (HTML 에러 페이지 등)
                    if not res.text.strip().startswith('<'):
                        print(f"⚠️ {date_str} [{kw}] 서버 응답 이상. 10초간 휴식 후 재시도...")
                        time.sleep(10)
                        continue

                    root = ET.fromstring(res.content)
                    items = root.findall('.//item')
                    
                    for item in items:
                        raw = {child.tag: child.text for child in item}
                        processed = {
                            '★가공_계약일': f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}",
                            '★가공_수요기관': raw.get('dminsttList', ''),
                            '★가공_계약명': raw.get('cntrctNm', ''),
                            '★가공_업체명': raw.get('corpList', ''),
                            '★가공_계약금액': int(raw.get('totCntrctAmt', 0)) if raw.get('totCntrctAmt') else 0
                        }
                        processed.update(raw)
                        day_data.append(processed)
                        
                    if items:
                        print(f"   ✅ {date_str} [{kw}] : {len(items)}건 발견")
                        
                except Exception as e:
                    print(f"   ❌ {date_str} [{kw}] 에러: {e}")

            # 하루치 결과가 있으면 시트에 즉시 추가
            if day_data:
                ws.append_rows(pd.DataFrame(day_data).values.tolist(), value_input_option='RAW')
                print(f"💰 {date_str} 데이터 저장 완료! (누적 건수: {len(day_data)})")
            
            curr += timedelta(days=1)

    except Exception as e:
        print(f"🔥 복구 작업 중 치명적 오류: {e}")

if __name__ == "__main__":
    main()
