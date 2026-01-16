import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import time

# --- 환경 설정 ---
# 공공데이터포털에서 받은 Decoding 또는 Encoding 키 둘 중 하나를 시도해보세요.
API_KEY = os.environ.get('DATA_GO_KR_API_KEY')
# 용역계약 목록 조회 API (XML 엔드포인트)
API_URL = 'http://apis.data.go.kr/1230000/Service_7/getServcCntrctInfoService01'

def get_gs_client():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    creds_dict = json.loads(auth_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def main():
    # 1. 날짜 설정 (2025년 1월 1일 ~ 현재)
    # API가 허용하는 최대 조회 기간인 1년 단위로 요청 횟수를 최소화합니다.
    start_str = "20250101"
    end_str = datetime.now().strftime("%Y%m%d")
    
    # [사용자 요청 키워드]
    contract_keywords = ['작전', '경계', '무인화', '국방', '군사', '부대']
    all_fetched_rows = []

    print(f"🚀 수집 시작: {start_str} ~ {end_str}")

    # 2. API 호출 (XML 방식)
    # 999건씩 10페이지까지 총 1만건을 훑습니다.
    for page in range(1, 11):
        params = {
            'serviceKey': API_KEY,
            'type': 'xml', # JSON 에러 방지를 위해 XML 사용
            'numOfRows': '999',
            'pageNo': str(page),
            'inqryBgnDt': start_str,
            'inqryEndDt': end_str,
            'inqryDiv': '1' # 계약일자 기준
        }
        
        # 봇 차단을 막기 위한 브라우저 흉내 헤더
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        try:
            print(f"📡 {page}페이지 요청 중...")
            response = requests.get(API_URL, params=params, headers=headers, timeout=60)
            
            # 응답이 비어있는지 확인
            if not response.content.strip():
                print(f"⚠️ {page}페이지 응답 내용이 비어있습니다. 종료합니다.")
                break

            # XML 파싱 시작
            root = ET.fromstring(response.content)
            
            # API 에러 코드 체크
            result_code = root.findtext('.//resultCode', '')
            if result_code != '00':
                print(f"❌ API 에러 발생! 코드: {result_code}, 메시지: {root.findtext('.//resultMsg')}")
                break

            items = root.findall('.//item')
            if not items:
                print(f"ℹ️ {page}페이지에 더 이상 데이터가 없습니다.")
                break

            for item in items:
                cntrct_nm = item.findtext('cntrctNm', '')
                
                # 키워드 필터링 (계약명에 키워드 포함 & 상수도 제외)
                if any(kw in cntrct_nm for kw in contract_keywords) and '상수도' not in cntrct_nm:
                    row = [
                        item.findtext('orderInsttNm', ''), # 수요기관
                        cntrct_nm,                         # 계약명
                        item.findtext('mainEntrpsNm', '-'),# 업체명
                        int(item.findtext('cntrctAmt', '0')), # 금액
                        item.findtext('cntrctDate', ''),    # 계약일
                        item.findtext('strtDate', '-'),     # 착수일
                        item.findtext('cntrctPrdNm', '-'),  # 기간
                        item.findtext('totScmpltDate', '') or item.findtext('endDate', ''), # 만료일
                        f"https://www.g2b.go.kr:8067/co/common/moveCntrctDetail.do?cntrctNo={item.findtext('cntrctNo')}&cntrctOrdNo={item.findtext('cntrctOrdNo', '00')}"
                    ]
                    all_fetched_rows.append(row)

            time.sleep(1.0) # 서버 부하 방지

        except Exception as e:
            print(f"❌ {page}페이지 처리 중 치명적 오류: {e}")
            break

    # 3. 구글 시트 저장
    if all_fetched_rows:
        df = pd.DataFrame(all_fetched_rows)
        unique_list = df.drop_duplicates().values.tolist()
        
        try:
            client = get_gs_client()
            sh = client.open("나라장터_용역계약내역")
            ws = sh.get_worksheet(0)
            ws.append_rows(unique_list, value_input_option='USER_ENTERED')
            print(f"✨ 성공! 총 {len(unique_list)}건의 데이터를 시트에 축적했습니다.")
        except Exception as e:
            print(f"❌ 시트 저장 오류: {e}")
    else:
        print("ℹ️ 조건에 맞는 데이터가 한 건도 없습니다. 키워드를 확인해주세요.")

if __name__ == "__main__":
    main()
