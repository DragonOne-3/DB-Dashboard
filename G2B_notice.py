import os
import sys
import json
import datetime
import time
import requests
import pandas as pd
import io
import gc
import threading
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseUpload
from google.auth.transport.requests import Request
from concurrent.futures import ThreadPoolExecutor, as_completed

def log(msg):
    print(msg, flush=True)

save_lock = threading.Lock()

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
    all_data = []
    page = 1
    with requests.Session() as session:
        while True:
            params = {
                'serviceKey': SERVICE_KEY, 'pageNo': str(page), 'numOfRows': '999',
                'inqryDiv': '1', 'type': 'json',
                'inqryBgnDt': s_dt + "0000", 'inqryEndDt': e_dt + "2359"
            }
            log(f"   - [{category}] {s_dt} ~ {e_dt} | {page}p 요청")
            try:
                res = session.get(url, params=params, timeout=45)
                if res.status_code == 200:
                    res_json = res.json()
                    items = res_json.get('response', {}).get('body', {}).get('items', [])
                    if not items: break
                    all_data.extend(items)
                    total_count = int(res_json.get('response', {}).get('body', {}).get('totalCount', 0))
                    log(f"   - [{category}] 진행: {len(all_data)} / {total_count}")
                    if len(all_data) >= total_count or len(items) < 999: break
                    page += 1
                else: break
            except: break
    return pd.DataFrame(all_data)

def update_drive_robust(drive_service, creds, cat_name, new_df):
    if new_df.empty: return
    file_name = f"나라장터_공고_{cat_name}.csv"
    
    with save_lock:
        try:
            query = f"name='{file_name}' and trashed=false"
            results = drive_service.files().list(q=query, fields='files(id)').execute()
            items = results.get('files', [])
            file_id = items[0]['id'] if items else None
            
            if file_id:
                if not creds.valid: creds.refresh(Request())
                download_url = f'https://www.googleapis.com/drive/v3/files/{file_id}?alt=media'
                resp = requests.get(download_url, headers={'Authorization': f'Bearer {creds.token}'}, timeout=60)
                
                if resp.status_code == 200:
                    try:
                        # 기존 1행 헤더를 유지하며 데이터 읽기
                        old_df = pd.read_csv(io.BytesIO(resp.content), encoding='utf-8-sig', low_memory=False)
                        # 컬럼 순서 일치 및 병합
                        new_df = pd.concat([old_df, new_df], ignore_index=True)
                        del old_df
                    except Exception as e:
                        log(f"⚠️ [{cat_name}] 기존 파일 읽기 오류(새로 생성 시도): {e}")

            # 중복 제거 (공고번호 기준)
            if 'bidNtceNo' in new_df.columns:
                new_df.drop_duplicates(subset=['bidNtceNo'], keep='last', inplace=True)
            
            # 🚀 저장 시 index=False를 사용하여 데이터만 넣고, 
            # 🚀 header=True(기본값)로 기존 컬럼명을 1행에 유지함
            csv_buffer = io.BytesIO()
            new_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            csv_buffer.seek(0)
            
            media = MediaIoBaseUpload(csv_buffer, mimetype='text/csv', resumable=True)
            if file_id:
                drive_service.files().update(fileId=file_id, media_body=media).execute()
            else:
                drive_service.files().create(body={'name': file_name}, media_body=media).execute()
            
            log(f"✅ [{cat_name}] 드라이브 저장 완료")
            del new_df
            gc.collect() 
            
        except Exception as e:
            log(f"❌ [{cat_name}] 드라이브 저장 오류: {e}")

def process_category(category, url, date_chunks, drive_service, creds):
    offset = {'공사': 0, '물품': 3, '용역': 6}[category]
    time.sleep(offset)
    
    for s, e in date_chunks:
        log(f"\n🔄 [{category}] 구간 시작: {s} ~ {e}")
        chunk_df = fetch_data_chunk(category, url, s, e)
        if not chunk_df.empty:
            update_drive_robust(drive_service, creds, category, chunk_df)
        time.sleep(1)

def main():
    if len(sys.argv) < 3: return
    start_str, end_str = sys.argv[1], sys.argv[2]
    
    start_date = datetime.datetime.strptime(start_str, '%Y%m%d')
    end_date = datetime.datetime.strptime(end_str, '%Y%m%d')
    
    date_chunks = []
    curr = start_date
    while curr <= end_date:
        chunk_e = min(curr + datetime.timedelta(days=14), end_date)
        date_chunks.append((curr.strftime('%Y%m%d'), chunk_e.strftime('%Y%m%d')))
        curr = chunk_e + datetime.timedelta(days=1)

    drive_service, creds = get_drive_service()
    log(f"📊 수집 시작: {start_str} ~ {end_str}")

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(process_category, cat, url, date_chunks, drive_service, creds) 
                   for cat, url in FILE_MAP.items()]
        for future in as_completed(futures):
            future.result()
    log("\n🏁 모든 수집 작업 종료")

if __name__ == "__main__":
    main()
