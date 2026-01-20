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
    # 1. 수집 기간 설정 (2024-01-01부터 오늘까지)
    start_dt = datetime(2024, 1, 1)
    end_dt = datetime.now()
    
    keywords = [CCTV', '통합관제', '주차관리', '영상감시장치', '영상정보처리기기', '국방', '부대', '작전', '경계', '방위','데이터','플랫폼','솔루션','국방', '부대', '작전', '경계', '방위', '군사', '무인화', '사령부', '군대']
    all_fetched_rows = []

    print(f"🚀 {start_dt.strftime('%Y%m%d')} ~ {end_dt.strftime('%Y%m%d')} 수집을 시작합니다.")

    # 2. 날짜를 30일 단위로 쪼개서 키워드별 수집 (데이터 누락 방지)
    current_dt = start_dt
    while current_dt <= end_dt:
        chunk_start = current_dt.strftime("%Y%m%d")
        chunk_end_dt = current_dt + timedelta(days=29)
        if chunk_end_dt > end_dt: chunk_end_dt = end_dt
        chunk_end = chunk_end_dt.strftime("%Y%m%d")
        
        for kw in keywords:
            params = {
                'serviceKey': API_KEY, 
                'pageNo': '1', 
                'numOfRows': '999',
                'inqryDiv': '1', 
                'type': 'xml', 
                'inqryBgnDate': chunk_start, 
                'inqryEndDate': chunk_end, 
                'cntrctNm': kw
            }
            
            try:
                res = requests.get(API_URL, params=params, timeout=60)
                if res.status_code == 200:
                    if res.text.startswith('<?xml'):
                        root = ET.fromstring(res.content)
                        items = root.findall('.//item')
                        print(f"  > [{chunk_start}~{chunk_end}] 키워드 '{kw}': {len(items)}건 발견")
                        
                        for item in items:
                            raw = {child.tag: child.text for child in item}
                            
                            # 수요기관 및 업체명 정제
                            raw_demand = raw.get('dminsttList', '')
                            demand_parts = raw_demand.replace('[', '').replace(']', '').split('^')
                            clean_demand = demand_parts[2] if len(demand_parts) > 2 else raw_demand
                            
                            raw_corp = raw.get('corpList', '')
                            corp_parts = raw_corp.replace('[', '').replace(']', '').split('^')
                            clean_corp = corp_parts[3] if len(corp_parts) > 3 else raw_corp

                            processed = {
                                '★가공_계약일': raw.get('cntrctDate', ''),
                                '★가공_착수일': raw.get('stDate', '-'),
                                '★가공_만료일': raw.get('ttalScmpltDate') or raw.get('thtmScmpltDate') or '-',
                                '★가공_수요기관': clean_demand,
                                '★가공_계약명': raw.get('cntrctNm', ''),
                                '★가공_업체명': clean_corp,
                                '★가공_계약금액': int(raw.get('totCntrctAmt', 0)) if raw.get('totCntrctAmt') else 0
                            }
                            processed.update(raw)
                            all_fetched_rows.append(processed)
                    else:
                        print(f"  ⚠️ {kw} 응답이 XML 형식이 아닙니다.")
                else:
                    print(f"  ❌ {kw} HTTP 에러: {res.status_code}")
            except Exception as e:
                print(f"  ❌ {kw} 호출 중 오류: {e}")
            
            time.sleep(0.3) # API 제한 방지
            
        current_dt = chunk_end_dt + timedelta(days=1)

    # 3. 데이터 중복 제거 및 필터링
    if all_fetched_rows:
        df = pd.DataFrame(all_fetched_rows)
        if 'cntrctNo' in df.columns:
            df = df.drop_duplicates(subset=['cntrctNo'])
        else:
            df = df.drop_duplicates()

        # 4. 구글 시트 저장
        try:
            sh = get_gs_client().open("나라장터_용역계약내역")
            ws = sh.get_worksheet(0)
            
            # 1행이 비어있는지 확인하여 제목행 추가
            if not ws.row_values(1):
                ws.insert_row(df.columns.tolist(), 1)
                print("✅ 제목행을 생성했습니다.")
            
            # 데이터 전송 (양이 많을 수 있어 3000줄씩 분할)
            values = df.fillna('').values.tolist()
            for i in range(0, len(values), 3000):
                ws.append_rows(values[i:i+3000], value_input_option='RAW')
            
            print(f"✅ 최종 {len(df)}건 데이터 누적 업데이트 완료")
        except Exception as e:
            print(f"❌ 시트 저장 중 오류: {e}")
    else:
        print(f"ℹ️ 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    main()
