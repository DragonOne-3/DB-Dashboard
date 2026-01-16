import pandas as pd
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
from datetime import datetime, timedelta

def get_gspread_client():
    # 깃허브 시크릿에 저장한 구글 인증 정보를 불러옵니다
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    creds_dict = json.loads(auth_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def fetch_g2b_data(api_key, start_date, end_date):
    url = "http://apis.data.go.kr/1230000/Service_7/getServcCntrctInfoService01"
    all_items = []
    for page in range(1, 11):
        params = {
            'serviceKey': api_key,
            'type': 'json',
            'numOfRows': '999',
            'pageNo': str(page),
            'inqryBgnDt': start_date,
            'inqryEndDt': end_date,
            'inqryDiv': '1'
        }
        try:
            response = requests.get(url, params=params, timeout=30)
            items = response.json().get('response', {}).get('body', {}).get('items', [])
            if not items: break
            all_items.extend(items)
        except: break
    return pd.DataFrame(all_items)

if __name__ == "__main__":
    api_key = os.environ.get('DATA_GO_KR_API_KEY')
    start_dt = "20250101"
    end_dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    
    raw_df = fetch_g2b_data(api_key, start_dt, end_dt)
    if not raw_df.empty:
        agency_keys = ['국방', '군대', '부대', '사령부', '방위']
        contract_keys = ['작전', '경계', '무인화', '국방', '군사', '부대']
        
        filtered_df = raw_df[
            raw_df['orderInsttNm'].str.contains('|'.join(agency_keys), na=False) |
            raw_df['cntrctNm'].str.contains('|'.join(contract_keys), na=False)
        ]
        
        if not filtered_df.empty:
            client = get_gspread_client()
            sh = client.open("나라장터_용역계약내역")
            ws = sh.get_worksheet(0)
            ws.append_rows(filtered_df.values.tolist())
            print(f"✅ {len(filtered_df)}건 추가 완료")
