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
# 가장 안정적인 기본 엔드포인트
API_URL = 'http://apis.data.go.kr/1230000/Service_7/getServcCntrctInfoService01'

def get_gs_client():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    creds_dict = json.loads(auth_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def main():
    # 수집 기간 설정
    start_str = "20250101"
    end_str = datetime.now().strftime("%Y%m%d")
    
    contract_keywords = ['작전', '경계', '무인화', '국방', '군사', '부대']
    all_fetched_rows = []

    print(f"🚀 진단 및 수집 시작: {start_str} ~ {end_str}")

    # 페이지별 수집 (안정성을 위해 XML 사용)
    for page in range(1, 11):
        params = {
            'serviceKey': API_KEY, # 여기서 에러가 나면 Decoding 키로 교체해보세요
            'type': 'xml',         # JSON 에러를 피하기 위해 XML 사용
            'numOfRows': '999',
            'pageNo': str(page),
            'inqryBgnDt': start_str,
            'inqryEndDt': end_str,
            'inqryDiv': '1'
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        try:
            # 1. API 호출
            response = requests.get(API_URL, params=params, headers=headers, timeout=60)
            
            # 2. 서버가 준 날것의 데이터 확인 (에러 진단용)
            if response.status_code != 200:
                print(f"❌ 서버 응답 에러 (Status: {response.status_code})")
                print(f"내용: {response.text[:200]}") # 에러 메시지 앞부분 출력
                break

            # 3. XML 파싱
            root = ET.fromstring(response.content)
            
            # resultCode 확인 (00이 아니면 열쇠 문제)
            res_code = root.findtext('.//resultCode', '')
            res_msg = root.findtext('.//resultMsg', '')
            
            if res_code != '00':
                print(f"❌ API 에러 코드: {res_code} / 메시지: {res_msg}")
                print("💡 팁: API 키를 '디코딩(Decoding)' 키로 바꿔서 설정해보세요.")
                break

            items = root.findall('.//item')
            if not items:
                print(f"ℹ️ {page}페이지에 더 이상 데이터가 없습니다.")
                break

            for item in items:
                cntrct_nm = item.findtext('cntrctNm', '')
                # 키워드 필터링 및 상수도 제외
                if any(kw in cntrct_nm for kw in contract_keywords) and '상수도' not in cntrct_nm:
                    all_fetched_rows.append([
                        item.findtext('orderInsttNm', ''),
                        cntrct_nm,
                        item.findtext('mainEntrpsNm', '-'),
                        int(item.findtext('cntrctAmt', '0')),
                        item.findtext('cntrctDate', ''),
                        item.findtext('strtDate', '-'),
                        item.findtext('cntrctPrdNm', '-'),
                        item.findtext('totScmpltDate', '') or item.findtext('endDate', ''),
                        f"https://www.g2b.go.kr:8067/co/common/moveCntrctDetail.do?cntrctNo={item.findtext('cntrctNo')}&cntrctOrdNo={item.findtext('cntrctOrdNo', '00')}"
                    ])

            print(f"✅ {page}페이지 검색 완료 (누적 {len(all_fetched_rows)}건 발견)")
            time.sleep(1.0)

        except Exception as e:
            print(f"❌ 실행 중 오류 발생: {e}")
            break

    # 4. 시트 저장
    if all_fetched_rows:
        try:
            client = get_gs_client()
            sh = client.open("나라장터_용역계약내역")
            ws = sh.get_worksheet(0)
            ws.append_rows(all_fetched_rows, value_input_option='USER_ENTERED')
            print(f"✨ 성공! {len(all_fetched_rows)}건 시트 축적 완료.")
        except Exception as e:
            print(f"❌ 시트 저장 실패: {e}")
    else:
        print("ℹ️ 최종 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    main()
