import streamlit as st
import pandas as pd
from datetime import datetime
import io
import json
import requests
from google.oauth2 import service_account
import google.auth.transport.requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- 1. 페이지 설정 ---
st.set_page_config(page_title="공공조달 DATA 통합검색", layout="wide")

# --- 2. 구글 인증 설정 (사용자님 제공 GOOGLE_AUTH_JSON 방식) ---
@st.cache_resource
def get_drive_service():
    try:
        # 시크릿에서 GOOGLE_AUTH_JSON 문자열을 가져와 JSON으로 파싱
        auth_json_str = st.secrets["GOOGLE_AUTH_JSON"]
        info = json.loads(auth_json_str)
        
        creds = service_account.Credentials.from_service_account_info(
            info, 
            scopes=['https://www.googleapis.com/auth/drive.readonly', 
                    'https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        return build('drive', 'v3', credentials=creds), creds
    except Exception as e:
        st.error(f"인증 초기화 실패: 시크릿 설정을 확인하세요. ({e})")
        st.stop()

drive_service, credentials = get_drive_service()

# --- 3. 데이터 소스 정보 ---
CSV_FOLDER_ID = '1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr' 
SHEET_FILE_IDS = {
    '나라장터_발주': '1pGnb6O5Z1ahaHYuQdydyoY1Ayf147IoGmLRdA3WAHi4',
    '나라장터_계약': '15Hsr_nup4ZteIZ4Jyov8wG2s_rKoZ25muqRE3-sRnaw',
    '군수품_발주': '1pzW51Z29SSoQk7al_GvN_tj5smuhOR3J2HWnL_16fcI',
    '군수품_계약': '1KPMUz0IKM6AQvqwfAkvW96WNvzbycN56vNlFnDmfRTw',
    '군수품_공고': '1opuA_UzNm27U9QkbMay5UsyQqcwfxiEmIHNRdc4MyHM',
    '군수품_수의': '1aYA18kPrSkpbayzbn16EdKUScVRwr2Nutyid5No5qjk',
    '종합쇼핑몰': 'FOLDER'
}
DATE_COL_MAP = {
    '군수품_발주': '발주예정월', '군수품_수의': '개찰일자', '군수품_계약': '계약일자',
    '군수품_공고': '공고일자', '나라장터_계약': '★가공_계약일', '나라장터_발주': '공고일자', '종합쇼핑몰': '계약납품요구일자'
}

# --- 4. 데이터 로드 함수 ---
@st.cache_data(ttl=3600, show_spinner=False)
def load_large_sheet(file_id):
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv"
    headers = {'Authorization': f'Bearer {credentials.token}'}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return pd.read_csv(io.BytesIO(response.content), low_memory=False)
    return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def load_csv_file(file_id):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return pd.read_csv(fh, low_memory=False)

# --- 5. 사이드바 검색 UI ---
with st.sidebar:
    st.header("🔍 검색 조건 설정")
    
    # 지자체 유지보수 사이트 이동 단추 추가
    st.link_button("🌐 지자체 유지보수 사이트 이동", "https://www.g2b.go.kr/", use_container_width=True)
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    start_date = col1.date_input("조회 시작일", datetime(2025, 1, 1))
    end_date = col2.date_input("조회 종료일", datetime.now())
    
    search_field = st.selectbox("검색 필드", ["ALL", "수요기관명", "업체명", "계약명", "세부품명"])
    k1 = st.text_input("첫 번째 검색어")
    logic = st.selectbox("검색 논리", ["NONE", "AND", "OR"])
    k2 = st.text_input("두 번째 검색어") if logic != "NONE" else ""
    
    search_btn = st.button("🚀 데이터 조회 실행", type="primary", use_container_width=True)

# --- 6. 메인 화면: 탭 구성 ---
st.title("🏛 공공조달 DATA 통합검색")

# 드롭박스 대신 탭(Tabs) 사용
tabs = st.tabs(list(SHEET_FILE_IDS.keys()))

for i, tab in enumerate(tabs):
    category_name = list(SHEET_FILE_IDS.keys())[i]
    
    with tab:
        if search_btn:
            with st.spinner(f"{category_name} 데이터 분석 중..."):
                try:
                    df = pd.DataFrame()
                    s_str = start_date.strftime('%Y%m%d')
                    e_str = end_date.strftime('%Y%m%d')

                    # 데이터 로드 로직
                    if category_name == '종합쇼핑몰':
                        results = drive_service.files().list(q=f"'{CSV_FOLDER_ID}' in parents and trashed = false").execute()
                        files = results.get('files', [])
                        relevant_dfs = []
                        for f in files:
                            if any(str(y) in f['name'] for y in range(start_date.year, end_date.year + 1)):
                                tmp = load_csv_file(f['id'])
                                if not tmp.empty:
                                    date_col = tmp.columns[3]
                                    tmp['compare_date'] = tmp[date_col].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8]
                                    relevant_dfs.append(tmp[(tmp['compare_date'] >= s_str) & (tmp['compare_date'] <= e_str)])
                        if relevant_dfs: df = pd.concat(relevant_dfs, ignore_index=True)
                    else:
                        df = load_large_sheet(SHEET_FILE_IDS[category_name])
                        date_col_name = DATE_COL_MAP.get(category_name)
                        if not df.empty and date_col_name in df.columns:
                            df['compare_date'] = df[date_col_name].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8]
                            df = df[(df['compare_date'] >= s_str) & (df['compare_date'] <= e_str)]

                    # 키워드 필터링
                    if not df.empty and k1:
                        mask = df.astype(str).apply(lambda x: x.str.contains(k1, case=False, na=False)).any(axis=1) if search_field=="ALL" else df[search_field].astype(str).str.contains(k1, case=False, na=False)
                        if logic == "AND" and k2:
                            mask2 = df.astype(str).apply(lambda x: x.str.contains(k2, case=False, na=False)).any(axis=1) if search_field=="ALL" else df[search_field].astype(str).str.contains(k2, case=False, na=False)
                            df = df[mask & mask2]
                        elif logic == "OR" and k2:
                            mask2 = df.astype(str).apply(lambda x: x.str.contains(k2, case=False, na=False)).any(axis=1) if search_field=="ALL" else df[search_field].astype(str).str.contains(k2, case=False, na=False)
                            df = df[mask | mask2]
                        else:
                            df = df[mask]

                    # 결과 표시
                    if not df.empty:
                        st.success(f"✅ {category_name}: {len(df):,}건 조회 완료")
                        st.dataframe(df.drop(columns=['compare_date'], errors='ignore'), use_container_width=True, height=500)
                        csv = df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                        st.download_button(f"📥 {category_name} 엑셀 다운로드", csv, f"{category_name}.csv", "text/csv")
                    else:
                        st.info(f"조회 기간 내에 {category_name} 결과가 없습니다.")
                except Exception as e:
                    st.error(f"오류 발생: {e}")
        else:
            st.write("사이드바에서 조건을 입력하고 **데이터 조회 실행** 버튼을 눌러주세요.")

st.caption("🏛 공공조달 DATA 통합검색 시스템 | 사용자 지정 인증 보안 모드")
