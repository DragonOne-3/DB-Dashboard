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
from dateutil.relativedelta import relativedelta

# --- 1. 페이지 설정 및 디자인 ---
st.set_page_config(page_title="공공조달 DATA 통합검색 시스템", layout="wide")

st.markdown("""
    <style>
    /* 전체 배경 및 폰트 축소 */
    .main { background-color: #f8f9fa; font-size: 13px !important; }
    
    /* 탭 메뉴 디자인 */
    .stTabs [data-baseweb="tab-list"] { gap: 5px; border-bottom: 2px solid #dee2e6; }
    .stTabs [data-baseweb="tab"] {
        background-color: #fff; border: 1px solid #dee2e6;
        border-radius: 6px 6px 0 0; padding: 8px 15px; font-weight: bold;
    }
    .stTabs [aria-selected="true"] { background-color: #0d6efd !important; color: white !important; }
    
    /* 검색 패널 한 줄 압축 */
    .search-panel {
        background: white; padding: 15px; border-radius: 10px;
        border: 1px solid #dee2e6; margin-bottom: 15px;
    }
    
    /* 입력창 간격 조정 */
    div[data-testid="stHorizontalBlock"] { gap: 10px !important; }
    .stButton button { width: 100%; font-weight: bold; margin-top: 24px; }
    .date-label { font-weight: bold; margin-bottom: 5px; font-size: 12px; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 구글 인증 서비스 ---
@st.cache_resource
def get_drive_service():
    auth_json_str = st.secrets["GOOGLE_AUTH_JSON"]
    info = json.loads(auth_json_str)
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=['https://www.googleapis.com/auth/drive.readonly', 'https://www.googleapis.com/auth/spreadsheets.readonly']
    )
    return build('drive', 'v3', credentials=creds), creds

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

DISPLAY_INDEX_MAP = {
    '군수품_계약': [7, 5, 3, 1, 12], '군수품_수의': [12, 10, 8, 3],
    '군수품_발주': [7, 8, 12, 2, 3], '군수품_공고': [0, 17, 15, 22],
    '나라장터_발주': [9, 13, 20], '나라장터_계약': [0, 3, 4, 5, 6],
    '종합쇼핑몰': ["수요기관명", "계약납품요구일자", "세부품명", "계약명", "업체명", "수량", "금액"]
}

DATE_COL_MAP = {
    '군수품_발주': '발주예정월', '군수품_수의': '개찰일자', '군수품_계약': '계약일자',
    '군수품_공고': '공고일자', '나라장터_계약': '★가공_계약일', '종합쇼핑몰': '계약납품요구일자'
}

# --- 4. 데이터 로드 함수 ---
def fetch_data(file_id, is_sheet=True):
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    headers = {'Authorization': f'Bearer {credentials.token}'}
    if is_sheet:
        url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv"
        return pd.read_csv(io.BytesIO(requests.get(url, headers=headers).content), low_memory=False)
    else:
        results = drive_service.files().list(q=f"'{file_id}' in parents and trashed = false").execute()
        dfs = [pd.read_csv(io.BytesIO(requests.get(f"https://www.googleapis.com/drive/v3/files/{f['id']}?alt=media", headers=headers).content), low_memory=False) for f in results.get('files', [])]
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# --- 5. 상단 헤더 ---
h_col1, h_col2 = st.columns([4, 1])
with h_col1:
    st.markdown("<h4 style='font-weight:bold; margin:0;'>🏛 공공조달 DATA 통합검색 시스템</h4>", unsafe_allow_html=True)
with h_col2:
    st.link_button("🌐 지자체 유지보수 바로가기", "https://g2b-info.streamlit.app/", use_container_width=True)

# --- 6. 탭 구성 ---
tabs = st.tabs(list(SHEET_FILE_IDS.keys()))

for i, tab in enumerate(tabs):
    cat = list(SHEET_FILE_IDS.keys())[i]
    with tab:
        # 날짜 상태 초기화
        if f"sd_{cat}" not in st.session_state: st.session_state[f"sd_{cat}"] = datetime(2025, 1, 1).date()
        if f"ed_{cat}" not in st.session_state: st.session_state[f"ed_{cat}"] = datetime.now().date()

        # 검색 패널 (한 줄 배치)
        with st.container():
            st.markdown('<div class="search-panel">', unsafe_allow_html=True)
            row = st.columns([2.2, 1.2, 1.5, 0.8, 1.5, 0.8, 1])
            
            with row[0]: # 날짜 및 퀵버튼
                st.markdown("<p class='date-label'>📅 기간 및 퀵버튼</p>", unsafe_allow_html=True)
                s_col, e_col = st.columns(2)
                sd = s_col.date_input("시작", key=f"s_in_{cat}", value=st.session_state[f"sd_{cat}"], label_visibility="collapsed")
                ed = e_col.date_input("종료", key=f"e_in_{cat}", value=st.session_state[f"ed_{cat}"], label_visibility="collapsed")
                q_cols = st.columns(6)
                if q_cols[0].button("오늘", key=f"t_{cat}"): st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] = datetime.now().date(); st.rerun()
                if q_cols[1].button("1M", key=f"1m_{cat}"): st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] - relativedelta(months=1); st.rerun()
                if q_cols[2].button("3M", key=f"3m_{cat}"): st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] - relativedelta(months=3); st.rerun()
                if q_cols[3].button("6M", key=f"6m_{cat}"): st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] - relativedelta(months=6); st.rerun()
                if q_cols[4].button("1Y", key=f"1y_{cat}"): st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] - relativedelta(years=1); st.rerun()
                if q_cols[5].button("2Y", key=f"2y_{cat}"): st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] - relativedelta(years=2); st.rerun()

            with row[1]: # 필드
                st.markdown("<p class='date-label'>🔍 필드</p>", unsafe_allow_html=True)
                field = st.selectbox("필드", ["ALL", "수요기관명", "업체명", "계약명", "세부품명"], key=f"f_{cat}", label_visibility="collapsed")
            with row[2]: # 검색어 1
                st.markdown("<p class='date-label'>✏️ 검색어 1</p>", unsafe_allow_html=True)
                k1 = st.text_input("검색어1", key=f"k1_{cat}", label_visibility="collapsed")
            with row[3]: # 논리 (AND/OR 부활)
                st.markdown("<p class='date-label'>⚖️ 조건</p>", unsafe_allow_html=True)
                logic = st.selectbox("논리", ["NONE", "AND", "OR"], key=f"l_{cat}", label_visibility="collapsed")
            with row[4]: # 검색어 2
                st.markdown("<p class='date-label'>✏️ 검색어 2</p>", unsafe_allow_html=True)
                k2 = st.text_input("검색어2", key=f"k2_{cat}", label_visibility="collapsed", disabled=(logic=="NONE"))
            with row[5]: # 페이지
                st.markdown("<p class='date-label'>📄 보기</p>", unsafe_allow_html=True)
                p_size = st.selectbox("개수", [50, 100, 150, 200], key=f"ps_{cat}", label_visibility="collapsed")
            with row[6]: # 버튼
                search_exe = st.button("검색 실행", key=f"btn_{cat}", type="primary")
            st.markdown('</div>', unsafe_allow_html=True)

        if search_exe:
            with st.spinner("데이터 분석 중..."):
                df = fetch_data(SHEET_FILE_IDS[cat], is_sheet=(cat != '종합쇼핑몰'))
                if not df.empty:
                    s_str, e_str = sd.strftime('%Y%m%d'), ed.strftime('%Y%m%d')
                    # 날짜 필터링
                    if cat == '나라장터_발주':
                        y_c, m_c = df.columns[4], df.columns[12]
                        df['tmp_dt'] = df[y_c].astype(str) + df[m_c].astype(str).str.zfill(2) + "01"
                    else:
                        d_col = DATE_COL_MAP.get(cat)
                        df['tmp_dt'] = df[d_col].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8] if d_col in df.columns else "00000000"
                    df = df[(df['tmp_dt'] >= s_str[:6]+"01") & (df['tmp_dt'] <= e_str)]

                    # [AND/OR 논리 필터링]
                    if k1:
                        def get_mask(k):
                            if field == "ALL": return df.astype(str).apply(lambda x: x.str.contains(k, case=False, na=False)).any(axis=1)
                            return df[field].astype(str).str.contains(k, case=False, na=False)
                        
                        mask1 = get_mask(k1)
                        if logic == "AND" and k2: df = df[mask1 & get_mask(k2)]
                        elif logic == "OR" and k2: df = df[mask1 | get_mask(k2)]
                        else: df = df[mask1]

                    if not df.empty:
                        d_col1, d_col2, _ = st.columns([1, 1, 5])
                        csv = df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                        d_col1.download_button("📑 CSV 받기", csv, f"{cat}.csv", "text/csv")
                        d_col2.download_button("📊 엑셀 받기", csv, f"{cat}.xlsx", "application/vnd.ms-excel")
                        
                        target_indices = DISPLAY_INDEX_MAP.get(cat, [])
                        show_cols = [df.columns[idx] if isinstance(idx, int) else idx for idx in target_indices if (isinstance(idx, int) and idx < len(df.columns)) or (isinstance(idx, str) and idx in df.columns)]
                        
                        st.info(f"✅ 검색 결과: {len(df):,}건")
                        st.dataframe(df[show_cols].head(p_size), use_container_width=True, height=550)
                    else: st.warning("결과가 없습니다.")
