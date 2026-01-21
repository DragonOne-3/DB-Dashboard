import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import io
import json
import requests
from google.oauth2 import service_account
import google.auth.transport.requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- 1. 페이지 설정 및 디자인 (디자인 주석 아님, 실행 코드) ---
st.set_page_config(page_title="공공조달 DATA 통합검색 시스템", layout="wide")

st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        background-color: white; border: 1px solid #dee2e6;
        border-radius: 8px 8px 0 0; padding: 10px 15px; font-weight: bold;
    }
    .stTabs [aria-selected="true"] { background-color: #0d6efd !important; color: white !important; }
    .search-panel {
        background: white; padding: 20px; border-radius: 12px;
        border: 1px solid #dee2e6; margin-top: 10px; margin-bottom: 20px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.05);
    }
    .stDownloadButton button { width: 100%; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 구글 인증 서비스 ---
@st.cache_resource
def get_drive_service():
    try:
        # 시크릿에 저장된 GOOGLE_AUTH_JSON 사용
        auth_json_str = st.secrets["GOOGLE_AUTH_JSON"]
        info = json.loads(auth_json_str)
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=['https://www.googleapis.com/auth/drive.readonly', 'https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        return build('drive', 'v3', credentials=creds), creds
    except Exception as e:
        st.error(f"인증 초기화 실패: {e}")
        st.stop()

drive_service, credentials = get_drive_service()

# --- 3. 데이터 설정 ---
SHEET_FILE_IDS = {
    '나라장터_발주': '1pGnb6O5Z1ahaHYuQdydyoY1Ayf147IoGmLRdA3WAHi4',
    '나라장터_계약': '15Hsr_nup4ZteIZ4Jyov8wG2s_rKoZ25muqRE3-sRnaw',
    '군수품_발주': '1pzW51Z29SSoQk7al_GvN_tj5smuhOR3J2HWnL_16fcI',
    '군수품_계약': '1KPMUz0IKM6AQvqwfAkvW96WNvzbycN56vNlFnDmfRTw',
    '군수품_공고': '1opuA_UzNm27U9QkbMay5UsyQqcwfxiEmIHNRdc4MyHM',
    '군수품_수의': '1aYA18kPrSkpbayzbn16EdKUScVRwr2Nutyid5No5qjk',
    '종합쇼핑몰': '1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr'
}

# 탭별 날짜 컬럼 매핑
DATE_COL_MAP = {
    '군수품_발주': '발주예정월', '군수품_수의': '개찰일자', '군수품_계약': '계약일자',
    '군수품_공고': '공고일자', '나라장터_계약': '★가공_계약일', '종합쇼핑몰': '계약납품요구일자'
}

# 표출 컬럼 인덱스 (제공해주신 displayIndexMap 기준)
DISPLAY_COLS = {
    '군수품_계약': [7, 5, 3, 1, 12],
    '군수품_수의': [12, 10, 8, 3],
    '군수품_발주': [7, 8, 12, 2, 3], 
    '군수품_공고': [0, 17, 15, 22],
    '나라장터_발주': [9, 13, 20],
    '나라장터_계약': [0, 3, 4, 5, 6],
    '종합쇼핑몰': ["수요기관명", "계약납품요구일자", "세부품명", "계약명", "업체명", "수량", "금액"]
}

# --- 4. 데이터 로드 함수 ---
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_data(file_id, is_sheet=True):
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    if is_sheet:
        url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv"
        headers = {'Authorization': f'Bearer {credentials.token}'}
        res = requests.get(url, headers=headers)
        return pd.read_csv(io.BytesIO(res.content), low_memory=False)
    else: # 종합쇼핑몰 폴더 내 CSV 스캔
        results = drive_service.files().list(q=f"'{file_id}' in parents and trashed = false").execute()
        files = results.get('files', [])
        dfs = []
        for f in files:
            url = f"https://www.googleapis.com/drive/v3/files/{f['id']}?alt=media"
            headers = {'Authorization': f'Bearer {credentials.token}'}
            res = requests.get(url, headers=headers)
            dfs.append(pd.read_csv(io.BytesIO(res.content), low_memory=False))
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# --- 5. 상단 타이틀 및 링크 ---
h1, h2 = st.columns([3, 1])
with h1:
    st.title("🏛 공공조달 DATA 통합검색 시스템")
with h2:
    st.write("")
    st.link_button("🌐 지자체 유지보수 바로가기", "https://g2b-info.streamlit.app/", use_container_width=True)

# --- 6. 탭 구성 및 각 탭별 로직 ---
tabs = st.tabs(list(SHEET_FILE_IDS.keys()))

for i, tab in enumerate(tabs):
    cat = list(SHEET_FILE_IDS.keys())[i]
    with tab:
        # 각 탭별 독립 검색 패널
        st.markdown(f"#### 🔍 {cat} 검색 조건")
        with st.container():
            st.markdown('<div class="search-panel">', unsafe_allow_html=True)
            c1, c2, c3 = st.columns([2.5, 4, 1.5])
            with c1:
                date_range = st.date_input("조회 기간", [datetime(2025, 1, 1), datetime.now()], key=f"d_{cat}")
            with c2:
                field = st.selectbox("검색 필드", ["ALL", "수요기관명", "업체명", "계약명", "세부품명"], key=f"f_{cat}")
                k1 = st.text_input("검색어 입력", key=f"k_{cat}", placeholder="검색어를 입력하세요")
            with c3:
                p_size = st.selectbox("표시 개수", [50, 100, 150, 200], key=f"p_{cat}")
                s_btn = st.button("🚀 검색 실행", key=f"b_{cat}", type="primary", use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        if s_btn:
            with st.spinner(f"{cat} 데이터 분석 중..."):
                df = fetch_data(SHEET_FILE_IDS[cat], is_sheet=(cat != '종합쇼핑몰'))
                
                if not df.empty:
                    # 1. 날짜 필터링 로직
                    s_str = date_range[0].strftime('%Y%m%d')
                    e_str = date_range[1].strftime('%Y%m%d') if len(date_range) > 1 else s_str
                    
                    if cat == '나라장터_발주':
                        # E열(년도: index 4) + M열(월: index 12) 결합
                        y_col, m_col = df.columns[4], df.columns[12]
                        df['tmp_date'] = df[y_col].astype(str) + df[m_col].astype(str).str.zfill(2) + "01"
                    else:
                        d_col = DATE_COL_MAP.get(cat)
                        if d_col in df.columns:
                            df['tmp_date'] = df[d_col].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8]
                        else:
                            df['tmp_date'] = "00000000"

                    # 기간 비교 (월 단위 검색 허용 위해 s_str의 1일자부터 비교)
                    df = df[(df['tmp_date'] >= s_str[:6] + "01") & (df['tmp_date'] <= e_str)]

                    # 2. 키워드 필터링 (해당 카테고리만 적용)
                    if k1:
                        if field == "ALL":
                            df = df[df.astype(str).apply(lambda x: x.str.contains(k1, case=False, na=False)).any(axis=1)]
                        elif field in df.columns:
                            df = df[field].astype(str).str.contains(k1, case=False, na=False)

                    if not df.empty:
                        # 3. 로우데이터 다운로드 버튼
                        col_dl, _ = st.columns([1, 4])
                        csv_data = df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                        col_dl.download_button("📊 전체 로우데이터(CSV) 다운로드", csv_data, f"{cat}_raw_data.csv", "text/csv")
                        
                        # 4. 표출 컬럼 가공
                        s_cols = DISPLAY_COLS.get(cat)
                        f_cols = [df.columns[c] if isinstance(c, int) else c for c in s_cols if (isinstance(c, int) and c < len(df.columns)) or (isinstance(c, str) and c in df.columns)]
                        
                        st.success(f"총 {len(df):,}건의 데이터를 찾았습니다.")
                        # 5. 반응형 페이징 데이터프레임
                        st.dataframe(df[f_cols].head(p_size), use_container_width=True, height=600)
                    else:
                        st.warning("해당 조건의 검색 결과가 없습니다.")
                else:
                    st.error("데이터 소스 연결에 실패했습니다.")

st.caption("🏛 공공조달 DATA 통합검색 | Ver. 2026.01.")
