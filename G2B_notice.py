import os
import sys
import json
import datetime
import time
import requests
import pandas as pd
import io
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseUpload
from concurrent.futures import ThreadPoolExecutor

# ================= 설정 =================
SERVICE_KEY = os.environ.get('DATA_GO_KR_API_KEY')
AUTH_JSON_STR = os.environ.get('GOOGLE_AUTH_JSON')

FILE_MAP = {
    '공사': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwkPPSSrch',
    '물품': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThngPPSSrch',
    '용역': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoServcPPSSrch'
}

def get_drive_service():
    info = json.loads(AUTH_JSON_STR)
    creds = service_account.Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/drive'])
    return build('drive', 'v3', credentials=creds), creds

def fetch_data_chunk(category, url, s_dt, e_dt):
    """지정된 기간(청크) 동안의 데이터를 수집"""
    all_data = []
    page = 1
    while True:
        params = {
            'serviceKey': SERVICE_KEY, 'pageNo': str(page), 'numOfRows': '999',
            'inqryDiv': '1', 'type': 'json',
            'inqryBgnDt': s_dt + "0000", 'inqryEndDt': e_dt + "2359"
        }
        try:
            res = requests.get(url, params=params, timeout=45)
            if res.status_code == 200:
                items = res.json().get('response', {}).get('body', {}).get('items', [])
                if not items: break
                all_data.extend(items)
                if len(items) < 999: break
                page += 1
                time.sleep(0.3)
            else: break
        except: break
    return pd.DataFrame(all_data)

def process_category(category, url, date_chunks):
    """카테고리별로 날짜 구간을 순차 수집하여 합침"""
    category_df = pd.DataFrame()
    for s, e in date_chunks:
        print(f"🚀 [{category}] {s} ~ {e} 수집 중...")
        chunk_df = fetch_data_chunk(category, url, s, e)
        category_df = pd.concat([category_df, chunk_df], ignore_index=True)
    return category, category_df

def update_drive(drive_service, creds, file_name, new_df):
    """최종 결과물을 구글 드라이브 최상단에 업데이트 (중복 제거 포함)"""
    if new_df.empty: return
    file_name_csv = f"나라장터_공고_{file_name}.csv"
    
    query = f"name='{file_name_csv}' and trashed=false"
    results = drive_service.files().list(q=query, fields='files(id)').execute()
    items = results.get('files', [])

    file_id = items[0]['id'] if items else None
    if file_id:
        resp = requests.get(f'https://www.googleapis.com/drive/v3/files/{file_id}?alt=media', 
                            headers={'Authorization': f'Bearer {creds.token}'})
        if resp.status_code == 200:
            old_df = pd.read_csv(io.BytesIO(resp.content), encoding='utf-8-sig', low_memory=False)
            new_df = pd.concat([old_df, new_df], ignore_index=True)
    
    # 공고번호 기준 중복 제거 및 저장
    new_df.drop_duplicates(subset=['bidNtceNo'], keep='last', inplace=True)
    csv_bytes = new_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
    media = MediaIoBaseUpload(io.BytesIO(csv_bytes), mimetype='text/csv', resumable=True)

    if file_id:
        drive_service.files().update(fileId=file_id, media_body=media).execute()
    else:
        drive_service.files().create(body={'name': file_name_csv}, media_body=media).execute()
    print(f"✅ {file_name_csv} 저장 완료")

def main():
    start_str, end_str = sys.argv[1], sys.argv[2]
    start_date = datetime.datetime.strptime(start_str, '%Y%m%d')
    end_date = datetime.datetime.strptime(end_str, '%Y%m%d')

    # 1. 수집 기간을 1개월 단위로 쪼개기 (안정적 처리를 위해)
    date_chunks = []
    curr = start_date
    while curr <= end_date:
        chunk_s = curr.strftime('%Y%m%d')
        # 이번 달의 마지막 날 계산
        if curr.month == 12: next_m = curr.replace(year=curr.year+1, month=1, day=1)
        else: next_m = curr.replace(month=curr.month+1, day=1)
        chunk_e_date = min(next_m - datetime.timedelta(days=1), end_date)
        date_chunks.append((chunk_s, chunk_e_date.strftime('%Y%m%d')))
        curr = next_m

    # 2. 병렬 처리 실행 (공사, 물품, 용역 3개를 동시에 시작)
    drive_service, creds = get_drive_service()
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(process_category, cat, url, date_chunks) for cat, url in FILE_MAP.items()]
        for future in futures:
            cat_name, final_df = future.result()
            update_drive(drive_service, creds, cat_name, final_df)

if __name__ == "__main__":
    main()
