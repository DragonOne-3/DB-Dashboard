import os
import sys
import json
import datetime
import time
import requests
import pandas as pd
import io
import traceback
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseUpload
from google.auth.transport.requests import Request
from concurrent.futures import ThreadPoolExecutor, as_completed

def log(msg):
    print(msg, flush=True)

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
    # API 요청 속도 및 안정성을 위해 전용 세션 구축
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
    """대용량 파일 처리에 강한 업로드 로직"""
    if new_df.empty: return
    file_name = f"나라장터_공고_{cat_name}.csv"
    
    try:
        # 1. 파일 찾기
        query = f"name='{file_name}' and trashed=false"
        results = drive_service.files().list(q=query, fields='files(id)').execute()
        items = results.get('files', [])
        file_id = items[0]['id'] if items else None
        
        # 2. 기존 데이터 다운로드 및 병합
        if file_id:
            try:
                # 토큰 갱신
                if not creds.valid:
                    creds.refresh(Request())
                
                download_url = f'https://www.googleapis.com/drive/v3/files/{file_id}?alt=media'
                # 타임아웃 넉넉히 설정
                resp = requests.get(download_url, headers={'Authorization': f'Bearer {creds.token}'}, timeout=60)
                if resp.status_code == 200:
                    # 인코딩 대응
                    try:
                        old_df = pd.read_csv(io.BytesIO(resp.content), encoding='utf-8-sig', low_memory=False)
                    except:
                        old_df = pd.read_csv(io.BytesIO(resp.content), encoding='cp949', low_memory=False)
                    new_df = pd.concat([old_df, new_df], ignore_index=True)
            except Exception as e:
                log(f"⚠️ [{cat_name}] 기존 파일 다운로드 실패(병합 건너뜀): {e}")

        # 3. 중복 제거
        if 'bidNtceNo' in new_df.columns:
            new_df.drop_duplicates(subset=['bidNtceNo'], keep='last', inplace=True)
            
        # 4. 메모리 절약을 위해 스트림 방식으로 업로드 준비
        csv_buffer = io.BytesIO()
        new_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        csv_buffer.seek(0)
        
        media = MediaIoBaseUpload(csv_buffer, mimetype='text/csv', resumable=True)

        # 5. 업로드 수행
        if file_id:
            drive_service.files().update(fileId=file_id, media_body=media).execute()
        else:
            drive_service.files().create(body={'name': file_name}, media_body=media).execute()
        log(f"✅ [{cat_name}] 드라이브 저장 완료")
        
    except Exception as e:
        log(f"❌ [{cat_name}] 드라이브 최종 처리 실패: {e}")
        traceback.print_exc()

def process_category(category, url, date_chunks, drive_service, creds):
    # API 서버 부하 방지를 위해 카테고리별로 약간의 시차를 둠
    time.sleep({'공사': 0, '물품': 5, '용역': 10}[category])
    
    for s, e in date_chunks:
        log(f"\n🔄 [{category}] 구간 시작: {s} ~ {e}")
        chunk_df = fetch_data_chunk(category, url, s, e)
        if not chunk_df.empty:
            update_drive_robust(drive_service, creds, category, chunk_df)
        time.sleep(2)

def main():
    if len(sys.argv) < 3: return
    start_str, end_str = sys.argv[1], sys.argv[2]
    
    start_date = datetime.datetime.strptime(start_str, '%Y%m%d')
    end_date = datetime.datetime.strptime(end_str, '%Y%m%d')
    
    # 데이터가 많으므로 10일 단위로 쪼갬
    date_chunks = []
    curr = start_date
    while curr <= end_date:
        chunk_e = min(curr + datetime.timedelta(days=9), end_date)
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
