import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import time
from urllib.parse import unquote

# --- 설정 ---
API_KEY = unquote(os.environ.get('DATA_GO_KR_API_KEY', ''))
API_URL = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'

# 제외 키워드 리스트
EXCLUDE_KEYWORDS = [
    '감리', '데이터베이스', '교육', 'ISP', '구조물', '관광', '가명', '익명', '검토', '의료', '귀농', '귀촌',
    '실시', '설계', '바이오', '콘텐츠', '거래', '탄소', '농수산물', '도매', '컨설팅', '가이드라인', '굿즈', '폐기물', '인사', '육아', '수산물', '목재', '주소',
    '하드웨어', '3차원', '3D', '유산', '문화', '대행'
]

def get_gs_client():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    creds_dict = json.loads(auth_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def main():
    # 1. 과거 데이터 수집 기간 설정 (2024년 1월 1일 ~ 현재)
    start_dt = datetime(2024, 1, 1)
    end_dt = datetime.now()
    
    keywords = list(set([
        'CCTV', '통합관제', '주차관리', '영상감시장치', '영상정보처리기기', '국방', '부대', '작전', '경계', '방위',
        '데이터','플랫폼','솔루션','군사', '무인화', '사령부', '군대','스마트시티','스마트도시','ITS','GIS'
    ]))
    
    all_fetched_rows = []

    print(f"🚀 {start_dt.strftime('%Y-%m-%d')} ~ {end_dt.strftime('%Y-%m-%d')} 과거 데이터 전체 수집 시작")

    # 2. 날짜를 30일 단위로 쪼개서 수집 (API 과부하 및 누락 방지)
    current_dt = start_dt
    while current_dt <= end_dt:
        chunk_start = current_dt.strftime("%Y%m%d")
        chunk_end_dt = current_dt + timedelta(days=29)
        if chunk_end_dt > end_dt: chunk_end_dt = end_dt
        chunk_end = chunk_end_dt.strftime("%Y%m%d")
        
        print(f"📅 구간 수집 중: {chunk_start} ~ {chunk_end}")
        
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
                    content = res.text.strip()
                    # 잘 작동했던 "XML 선언문 무시" 로직 적용
                    if content.startswith('<?xml') or content.startswith('<response'):
                        root = ET.fromstring(res.content)
                        
                        result_code = root.find('.//resultCode')
                        if result_code is not None and result_code.text == '00':
                            items = root.findall('.//item')
                            total_found = len(items)
                            saved_count = 0
                            
                            for item in items:
                                raw = {child.tag: child.text for child in item}
                                cntrct_nm = raw.get('cntrctNm', '')
                                
                                # 제외 키워드 체크
                                if any(ex_kw in cntrct_nm for ex_kw in EXCLUDE_KEYWORDS):
                                    continue
                                
                                # 데이터 정제
                                raw_demand = raw.get('dminsttList', '') or ''
                                demand_parts = raw_demand.replace('[', '').replace(']', '').split('^')
                                clean_demand = demand_parts[2] if len(demand_parts) > 2 else raw_demand
                                
                                raw_corp = raw.get('corpList', '') or ''
                                corp_parts = raw_corp.replace('[', '').replace(']', '').split('^')
                                clean_corp = corp_parts[3] if len(corp_parts) > 3 else raw_corp

                                processed = {
                                    '★가공_계약일': raw.get('cntrctDate', ''),
                                    '★가공_착수일': raw.get('stDate', '-'),
                                    '★가공_만료일': raw.get('ttalScmpltDate') or raw.get('thtmScmpltDate') or '-',
                                    '★가공_수요기관': clean_demand,
                                    '★가공_계약명': cntrct_nm,
                                    '★가공_업체명': clean_corp,
                                    '★가공_계약금액': int(raw.get('totCntrctAmt', 0)) if raw.get('totCntrctAmt') else 0
                                }
                                processed.update(raw)
                                all_fetched_rows.append(processed)
                                saved_count += 1
                            
                            if total_found > 0:
                                print(f"   > '{kw}': {total_found}건 발견 (저장: {saved_count}건)")
                        else:
                            print(f"   ❌ {kw} API 에러 코드: {result_code.text if result_code is not None else 'None'}")
                    else:
                        print(f"   ⚠️ {kw} 응답 형식이 올바르지 않음 (첫 50자): {content[:50]}")
                else:
                    print(f"   ❌ {kw} HTTP 에러: {res.status_code}")
            
            except Exception as e:
                print(f"   ❌ {kw} 호출 중 오류: {e}")
            
            time.sleep(0.3) # 과거 데이터는 요청이 많으므로 약간의 대기시간 유지
            
        current_dt = chunk_end_dt + timedelta(days=1)

    # 3. 중복 제거 및 구글 시트 저장
    if all_fetched_rows:
        df = pd.DataFrame(all_fetched_rows)
        if 'cntrctNo' in df.columns:
            df = df.drop_duplicates(subset=['cntrctNo'])
        else:
            df = df.drop_duplicates()

        try:
            sh = get_gs_client().open("나라장터_용역계약내역")
            ws = sh.get_worksheet(0)
            
            # 시트가 비어있으면 제목 행 삽입
            if not ws.row_values(1):
                ws.insert_row(df.columns.tolist(), 1)
            
            # 과거 데이터는 양이 많으므로 2000줄씩 끊어서 전송 (안정성)
            values = df.fillna('').values.tolist()
            for i in range(0, len(values), 2000):
                ws.append_rows(values[i:i+2000], value_input_option='RAW')
                print(f"📤 {i+len(values[i:i+2000])} / {len(values)} 줄 업로드 중...")
            
            print(f"✅ 과거 데이터 총 {len(df)}건 누적 업데이트 완료!")
        except Exception as e:
            print(f"❌ 시트 저장 중 오류: {e}")
    else:
        print("ℹ️ 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    main()
