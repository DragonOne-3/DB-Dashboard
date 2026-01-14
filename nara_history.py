import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import time

# --- 설정 ---
API_KEY = os.environ.get('DATA_GO_KR_API_KEY')
API_URL = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'

def get_gs_client():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    creds_dict = json.loads(auth_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def main():
    try:
        client = get_gs_client()
        sh = client.open("나라장터_용역계약내역")
        ws = sh.get_worksheet(0)
        
        # 5월 1일부터 오늘까지
        start_dt = datetime(2025, 5, 1)
        end_dt = datetime.now()
        
        keywords = ['CCTV', '통합관제', '주차관리', '영상감시장치', '영상정보처리기기']
        
        curr = start_dt
        while curr <= end_dt:
            date_str = curr.strftime("%Y%m%d")
            print(f"\n📅 [조회 날짜: {date_str}] 수집 시도 중...")
            
            day_data = []
            for kw in keywords:
                # 아까 성공했을 때와 동일한 파라미터 구성
                params = {
                    'serviceKey': API_KEY,
                    'pageNo': '1',
                    'numOfRows': '999',
                    'inqryDiv': '1',
                    'type': 'xml',
                    'inqryBgnDate': date_str,
                    'inqryEndDate': date_str,
                    'cntrctNm': kw
                }
                
                try:
                    res = requests.get(API_URL, params=params, timeout=30)
                    root = ET.fromstring(res.content)
                    
                    # 검색된 총 건수 확인
                    total_node = root.find('.//totalCount')
                    total_count = int(total_node.text) if total_node is not None else 0
                    
                    if total_count > 0:
                        print(f"   ✅ '{kw}' 키워드: {total_count}건 발견!")
                        items = root.findall('.//item')
                        for item in items:
                            raw = {child.tag: child.text for child in item}
                            # 가공 데이터 생성
                            processed = {
                                '★가공_계약일': f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}",
                                '★가공_수요기관': raw.get('dminsttList', '').split('^')[-1].replace(']', '') if '^' in raw.get('dminsttList', '') else raw.get('dminsttList', ''),
                                '★가공_계약명': raw.get('cntrctNm', ''),
                                '★가공_업체명': raw.get('corpList', '').split('^')[-1].replace(']', '') if '^' in raw.get('corpList', '') else raw.get('corpList', ''),
                                '★가공_계약금액': int(raw.get('totCntrctAmt', 0)) if raw.get('totCntrctAmt') else 0
                            }
                            processed.update(raw)
                            day_data.append(processed)
                    else:
                        # 데이터가 없을 때 로그
                        pass 
                except Exception as e:
                    print(f"   ❌ '{kw}' 조회 중 오류: {e}")
                
                time.sleep(0.1) # 키워드 간 짧은 대기

            # 하루치 모아서 시트에 기록
            if day_data:
                df = pd.DataFrame(day_data).fillna('')
                ws.append_rows(df.values.tolist(), value_input_option='RAW')
                print(f"   💰 {date_str} 데이터 {len(day_data)}건 시트 저장 완료!")
            else:
                print(f"   ⚠️ {date_str}에는 검색된 데이터가 없습니다.")
            
            curr += timedelta(days=1)
            time.sleep(1) # 구글 시트 쓰기 제한 방지

    except Exception as e:
        print(f"🔥 치명적 오류 발생: {e}")

if __name__ == "__main__":
    main()
