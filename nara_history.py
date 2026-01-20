import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import time
from urllib.parse import unquote # 추가

# --- 설정 ---
# API 키는 일반공공데이터 포털에서 받은 '인코딩' 키를 그대로 넣거나, '디코딩' 키를 넣으셔도 됩니다.
# 여기서는 안전하게 unquote 처리를 한 번 거칩니다.
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
    # 1. 수집 기간 설정 (너무 길면 서버에서 차단하므로 확인용으로 짧게 테스트 후 늘리세요)
    start_dt = datetime(2025, 1, 1)
    end_dt = datetime.now()
    
    # 중복 키워드 제거
    keywords = list(set(['CCTV', '통합관제', '주차관리', '영상감시장치', '영상정보처리기기', '국방', '부대', '작전', '경계', '방위','데이터','플랫폼','솔루션','군사', '무인화', '사령부', '군대','스마트시티','스마트도시','ITS','GIS']))
    all_fetched_rows = []

    print(f"🚀 {start_dt.strftime('%Y%m%d')} ~ {end_dt.strftime('%Y%m%d')} 수집 시작")

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
                # params를 통해 보내면 requests가 자동 인코딩합니다.
                res = requests.get(API_URL, params=params, timeout=60)
                
                if res.status_code == 200:
                    if res.text.strip().startswith('<?xml'):
                        root = ET.fromstring(res.content)
                        items = root.findall('.//item')
                        
                        count = 0
                        for item in items:
                            raw = {child.tag: child.text for child in item}
                            cntrct_nm = raw.get('cntrctNm', '')
                            
                            if any(ex_kw in cntrct_nm for ex_kw in EXCLUDE_KEYWORDS):
                                continue
                            
                            # 데이터 정제 (기존 로직)
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
                                '★가공_계약명': cntrct_nm,
                                '★가공_업체명': clean_corp,
                                '★가공_계약금액': int(raw.get('totCntrctAmt', 0)) if raw.get('totCntrctAmt') else 0
                            }
                            processed.update(raw)
                            all_fetched_rows.append(processed)
                            count += 1
                        
                        print(f"  > [{chunk_start}~{chunk_end}] '{kw}': {count}건 저장완료")
                    else:
                        # 에러 원인 분석을 위해 실제 응답 앞부분 출력
                        print(f"  ⚠️ {kw} 응답 오류: {res.text[:100]}...")
                else:
                    print(f"  ❌ {kw} HTTP 에러: {res.status_code}")
            
            except Exception as e:
                print(f"  ❌ {kw} 호출 중 오류: {e}")
            
            # API 제한 방지를 위해 간격 유지 (트래픽 에러 발생 시 1초 이상으로 늘리세요)
            time.sleep(0.5) 
            
        current_dt = chunk_end_dt + timedelta(days=1)

    # 3. 데이터 중복 제거 및 시트 저장 로직 (기존과 동일)
    # ... (이하 생략) ...
