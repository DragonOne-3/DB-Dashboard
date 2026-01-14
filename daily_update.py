import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import time
from pytimekr import pytimekr  # 공휴일 체크를 위해 추가

# --- 설정 ---
API_KEY = os.environ.get('DATA_GO_KR_API_KEY')
API_URL = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'

def get_target_date():
    """한국 시간 기준, 주말 및 공휴일을 제외한 최근 평일 계산"""
    # UTC 기준 시간을 한국 시간(+9)으로 변환
    now = datetime.utcnow() + timedelta(hours=9)
    # 기본적으로 어제 날짜부터 시작
    target = now - timedelta(days=1)
    
    # 해당 연도의 공휴일 리스트 가져오기
    holidays = pytimekr.holidays(year=target.year)
    
    # 주말(토:5, 일:6)이거나 공휴일인 경우 하루씩 뒤로 이동
    while target.weekday() >= 5 or target.date() in holidays:
        target -= timedelta(days=1)
        
    return target

def get_gs_client():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    creds_dict = json.loads(auth_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def main():
    # 1. 수집 대상 날짜 계산 (주말/공휴일 제외 최근 평일)
    target_dt = get_target_date()
    target_str = target_dt.strftime("%Y%m%d")
    display_str = target_dt.strftime("%Y-%m-%d")
    
    keywords = ['CCTV', '통합관제', '주차관리', '영상감시장치', '영상정보처리기기']
    new_rows = []

    # 2. 키워드별 수집
    for kw in keywords:
        params = {
            'serviceKey': API_KEY, 'pageNo': '1', 'numOfRows': '999',
            'inqryDiv': '1', 'type': 'xml', 
            'inqryBgnDate': target_str, 'inqryEndDate': target_str, 
            'cntrctNm': kw
        }
        try:
            res = requests.get(API_URL, params=params, timeout=60)
            if res.status_code == 200:
                root = ET.fromstring(res.content)
                for item in root.findall('.//item'):
                    raw = {child.tag: child.text for child in item}
                    
                    # 3. 데이터 가공 및 정제
                    # 수요기관 및 업체명에서 불필요한 기호 제거 로직 추가
                    raw_demand = raw.get('dminsttList', '')
                    demand_parts = raw_demand.replace('[', '').replace(']', '').split('^')
                    clean_demand = demand_parts[2] if len(demand_parts) > 2 else raw_demand
                    
                    raw_corp = raw.get('corpList', '')
                    corp_parts = raw_corp.replace('[', '').replace(']', '').split('^')
                    clean_corp = corp_parts[3] if len(corp_parts) > 3 else raw_corp

                    processed = {
                        '★가공_계약일': display_str,
                        '★가공_착수일': raw.get('stDate', '-'),
                        '★가공_만료일': raw.get('ttalScmpltDate') or raw.get('thtmScmpltDate') or '-',
                        '★가공_수요기관': clean_demand,
                        '★가공_계약명': raw.get('cntrctNm', ''),
                        '★가공_업체명': clean_corp,
                        '★가공_계약금액': int(raw.get('totCntrctAmt', 0))
                    }
                    processed.update(raw)
                    new_rows.append(processed)
        except Exception as e:
            print(f"❌ {kw} 수집 중 오류: {e}")
            continue
        time.sleep(0.5) # API 과부하 방지

    # 4. 구글 시트 하단에 추가 (Append)
    if new_rows:
        try:
            sh = get_gs_client().open("나라장터_용역계약내역")
            ws = sh.get_worksheet(0)
            
            # DataFrame으로 변환하여 시트에 추가
            df = pd.DataFrame(new_rows)
            ws.append_rows(df.values.tolist(), value_input_option='RAW')
            print(f"✅ {display_str} 데이터 {len(new_rows)}건 추가 완료")
        except Exception as e:
            print(f"❌ 시트 저장 중 오류: {e}")
    else:
        print(f"ℹ️ {display_str}에 해당하는 수집 데이터가 없습니다.")

if __name__ == "__main__":
    main()
