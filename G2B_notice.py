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
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=['https://www.googleapis.com/auth/drive']
    )
    return build('drive', 'v3', credentials=creds, cache_discovery=False), creds

def fetch_data_chunk(category, url, s_dt, e_dt):
    """지정된 기간 동안의 데이터를 수집"""
    all_data = []
    page = 1
    with requests.Session() as session:
        while True:
            params = {
                'serviceKey': SERVICE_KEY, 'pageNo': str(page), 'numOfRows': '999',
                'inqryDiv': '1', 'type': 'json',
                'inqryBgnDt': s_dt + "0000", 'inqryEndDt': e_dt + "2359"
            }
            try:
                res = session.get(url, params=params, timeout=30)
                if res.status_code == 200:
                    res_json = res.json()
                    items = res_json.get('response', {}).get('body', {}).get('items', [])
                    if not items: break
                    all_data.extend(items)
                    
                    total_count = int(res_json.get('response', {}).get('body', {}).get('totalCount', 0))
                    if len(all_data) >= total_count or len(items) < 999: break
                    page += 1
                else: break
            except: break
    return pd.DataFrame(all_data)

def update_drive(drive_service, creds, cat_name, new_df):
    """즉시 드라이브에 반영 (기존 데이터와 합쳐서 저장)"""
    if new_df.empty: return
    file_name = f"나라장터_공고_{cat_name}.csv"
    
    query = f"name='{file_name}' and trashed=false"
    results = drive_service.files().list(q=query, fields='files(id)').execute()
    items = results.get('files', [])

    file_id = items[0]['id'] if items else None
    
    if file_id:
        download_url = f'https://www.googleapis.com/drive/v3/files/{file_id}?alt=media'
        resp = requests.get(download_url, headers={'Authorization': f'Bearer {creds.token}'})
        if resp.status_code == 200:
            old_df = pd.read_csv(io.BytesIO(resp.content), encoding='utf-8-sig', low_memory=False)
            new_df = pd.concat([old_df, new_df], ignore_index=True)

    new_df.drop_duplicates(subset=['bidNtceNo'], keep='last', inplace=True)
    csv_bytes = new_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
    media = MediaIoBaseUpload(io.BytesIO(csv_bytes), mimetype='text/csv', resumable=True)

    if file_id:
        drive_service.files().update(fileId=file_id, media_body=media).execute()
    else:
        drive_service.files().create(body={'name': file_name}, media_body=media).execute()

def process_category(category, url, date_chunks, drive_service, creds):
    """카테고리별로 루프를 돌며 구간마다 즉시 저장"""
    total_chunks = len(date_chunks)
    for idx, (s, e) in enumerate(date_chunks):
        print(f"🔄 [{category}] {idx+1}/{total_chunks} 구간 수집 중 ({s} ~ {e})...")
        chunk_df = fetch_data_chunk(category, url, s, e)
        
        if not chunk_df.empty:
            update_drive(drive_service, creds, category, chunk_df)
            print(f"✅ [{category}] {s} ~ {e} 구간 저장 완료! ({len(chunk_df)}건)")
        else:
            print(f"ℹ️ [{category}] {s} ~ {e} 데이터 없음")
        time.sleep(1)

def main():
    if len(sys.argv) < 3: return
    start_str, end_str = sys.argv[1], sys.argv[2]
    
    start_date = datetime.datetime.strptime(start_str, '%Y%m%d')
    end_date = datetime.datetime.strptime(end_str, '%Y%m%d')
    
    date_chunks = []
    curr = start_date
    while curr <= end_date:
        next_m = (curr.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
        chunk_e = min(next_m - datetime.timedelta(days=1), end_date)
        date_chunks.append((curr.strftime('%Y%m%d'), chunk_e.strftime('%Y%m%d')))
        curr = next_m

    drive_service, creds = get_drive_service()
    
    print(f"📊 총 {len(date_chunks)}개 구간으로 나누어 병렬 수집을 시작합니다.")

    with ThreadPoolExecutor(max_workers=3) as executor:
        for cat, url in FILE_MAP.items():
            executor.submit(process_category, cat, url, date_chunks, drive_service, creds)

if __name__ == "__main__":
    main()
