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

# --- 1. 설정 및 인증 ---
# 시크릿 키가 인코딩되어 있을 경우를 대비해 unquote 처리
RAW_KEY = os.environ.get('DATA_GO_KR_API_KEY')
API_KEY = unquote(RAW_KEY) if RAW_KEY else None
# 사용자 지정 URL
API_URL = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'

def get_gs_client():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    creds_dict = json.loads(auth_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def main():
    start_dt = datetime(2025, 1, 1)
    end_dt = datetime.now()
    
    # [사용자 지정 키워드]
    contract_keywords = ['작전', '경계', '무인화', '국방', '군사', '부대']
    all_fetched_rows = []
    
    current_start = start_dt
    while current_start < end_dt:
        current_end = current_start + timedelta(days=90)
        if current_end > end_dt: current_end = end_dt
            
        s_str = current_start.strftime("%Y%m%d")
        e_str = current_end.strftime("%Y%m%d")
        
        print(f"📅 구간 수집 시작: {s_str} ~ {e_str}")
        
        # 페이지별 수집 (해당 API는 XML 파싱이 가장 정확합니다)
        for page in range(1, 11):
            params = {
                'serviceKey': API_KEY,
                'pageNo': str(page),
                'numOfRows': '999',
                'inqryDiv': '1', # 계약일자 기준
                'inqryBgnDate': s_str,
                'inqryEndDate': e_str,
                'type': 'xml' # 확실하게 XML로 요청
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0'
            }

            try:
                response = requests.get(API_URL, params=params, headers=headers, timeout=60)
                
                # 1. 응답 내용이 비어있는지 확인
                if not response.content.strip():
                    print(f"⚠️ {s_str} 구간: 서버 응답이 비어있습니다.")
                    break

                # 2. XML로 직접 파싱 (JSON 에러 발생 원천 차단)
                root = ET.fromstring(response.content)
                
                # 결과 코드 확인
                res_code = root.findtext('.//resultCode', '')
                if res_code != '00':
                    print(f"❌ API 에러 코드: {res_code} ({root.findtext('.//resultMsg')})")
                    break

                items = root.findall('.//item')
                if not items:
                    print(f"   ㄴ {page}페이지: 데이터 없음")
                    break
                    
                for item in items:
                    cntrct_nm = item.findtext('cntrctNm', '')
                    
                    # 키워드 필터링 (계약명 기준, 상수도 제외)
                    if any(kw in cntrct_nm for kw in contract_keywords) and '상수도' not in cntrct_nm:
                        # 수요기관명 정제
                        raw_demand = item.findtext('dminsttList', '')
                        demand_parts = raw_demand.replace('[', '').replace(']', '').split('^')
                        clean_demand = demand_parts[2] if len(demand_parts) > 2 else raw_demand
                        
                        # 업체명 정제
                        raw_corp = item.findtext('corpList', '')
                        corp_parts = raw_corp.replace('[', '').replace(']', '').split('^')
                        clean_corp = corp_parts[3] if len(corp_parts) > 3 else raw_corp

                        processed = [
                            clean_demand,                                # ★가공_수요기관
                            cntrct_nm,                                   # ★가공_계약명
                            clean_corp,                                  # ★가공_업체명
                            int(item.findtext('totCntrctAmt', '0')),     # ★가공_계약금액
                            item.findtext('cntrctDate', ''),              # 계약일자
                            item.findtext('stDate', '-'),                 # 착수일자
                            item.findtext('cntrctPrdNm', '-'),            # 계약기간
                            item.findtext('ttalScmpltDate', '') or item.findtext('thtmScmpltDate', ''), # 총완수일자
                            f"https://www.g2b.go.kr:8067/co/common/moveCntrctDetail.do?cntrctNo={item.findtext('cntrctNo')}&cntrctOrdNo={item.findtext('cntrctOrdNo', '00')}"
                        ]
                        all_fetched_rows.append(processed)
                
                print(f"   ㄴ {page}페이지 완료")
                time.sleep(1.0) # 서버 매너 타임

            except Exception as e:
                print(f"❌ {s_str} 구간 처리 중 오류: {e}")
                break
        
        current_start = current_end + timedelta(days=1)

    # 3. 데이터 저장 (구글 시트)
    if all_fetched_rows:
        df = pd.DataFrame(all_fetched_rows)
        unique_rows = df.drop_duplicates().values.tolist()
        
        try:
            client = get_gs_client()
            sh = client.open("나라장터_용역계약내역")
            ws = sh.get_worksheet(0)
            ws.append_rows(unique_rows, value_input_option='USER_ENTERED')
            print(f"✨ 전체 성공! 총 {len(unique_rows)}건의 데이터를 시트에 추가했습니다.")
        except Exception as e:
            print(f"❌ 시트 저장 오류: {e}")
    else:
        print("ℹ️ 해당 조건에 맞는 데이터가 한 건도 없습니다.")

if __name__ == "__main__":
    main()
