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

# --- 1. 페이지 설정 및 커스텀 디자인 (첨부 이미지 스타일 반영) ---
st.set_page_config(page_title="공공조달 DATA 통합검색 시스템", layout="wide")

st.markdown("""
    <style>
    /* 전체 배경색 및 폰트 */
    .main { background-color: #f4f4f4; font-size: 13px; }
    
    /* 검색 테이블 박스 디자인 */
    .search-container {
        background-color: white;
        border: 1px solid #ccc;
        border-radius: 0px;
        margin-bottom: 20px;
    }
    
    /* 검색 테이블 행(Row) 구분선 */
    .search-row {
        display: flex;
        border-bottom: 1px solid #eee;
        align-items: center;
    }
    
    /* 검색 테이블 왼쪽 라벨(Label) 영역 */
    .search-label {
        background-color: #f9f9f9;
        width: 150px;
        padding: 10px;
        font-weight: bold;
        border-right: 1px solid #eee;
        text-align: center;
        flex-shrink: 0;
    }
    
    /* 검색 테이블 오른쪽 입력(Input) 영역 */
    .search-input {
        padding: 10px;
        flex-grow: 1;
        display: flex;
        gap: 10px;
        align-items: center;
    }

    /* 탭 디자인 */
    .stTabs [data-baseweb="tab-list"] { gap: 5px; }
    .stTabs [data-baseweb="tab"] {
        height: 40px; background-color: #fdfdfd; border: 1px solid #ddd;
        border-radius: 5px 5px 0 0; font-weight: bold;
    }
    .stTabs [aria-selected="true"] { background-color: #00b050 !important; color: white !important; }

    /* 버튼 스타일 */
    .stButton button { border-radius: 2px; }
    div[data-testid="column"] { padding: 0px !important; }
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

# --- 3. 데이터 설정 (동일) ---
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

# --- 5. 화면 상단 ---
st.markdown("<h3 style='margin-bottom:0px;'>🏛 공공조달 DATA 통합검색 시스템</h3>", unsafe_allow_html=True)
st.markdown("---")

# --- 6. 탭 구성 ---
tabs = st.tabs(list(SHEET_FILE_IDS.keys()))

for i, tab in enumerate(tabs):
    cat = list(SHEET_FILE_IDS.keys())[i]
    with tab:
        # 날짜 상태 관리
        if f"sd_{cat}" not in st.session_state: st.session_state[f"sd_{cat}"] = datetime(2025, 1, 1).date()
        if f"ed_{cat}" not in st.session_state: st.session_state[f"ed_{cat}"] = datetime.now().date()

        # [이미지 스타일 반영] 격자형 검색 패널
        st.markdown('<div class="search-container">', unsafe_allow_html=True)
        
        # 행 1: 검색조건 (필드 + 키워드 + 논리 + 키워드2)
        c_row1 = st.columns([1, 6])
        with c_row1[0]: st.markdown('<div class="search-label">검색조건</div>', unsafe_allow_html=True)
        with c_row1[1]:
            sc1, sc2, sc3, sc4 = st.columns([1.5, 3, 1, 3])
            field = sc1.selectbox("필드", ["ALL", "수요기관명", "업체명", "계약명", "세부품명"], key=f"f_{cat}", label_visibility="collapsed")
            k1 = sc2.text_input("검색어1", key=f"k1_{cat}", label_visibility="collapsed")
            logic = sc3.selectbox("논리", ["NONE", "AND", "OR"], key=f"l_{cat}", label_visibility="collapsed")
            k2 = sc4.text_input("검색어2", key=f"k2_{cat}", label_visibility="collapsed", disabled=(logic=="NONE"))

        # 행 2: 조회기간 (날짜 + 퀵버튼)
        c_row2 = st.columns([1, 6])
        with c_row2[0]: st.markdown('<div class="search-label">조회기간</div>', unsafe_allow_html=True)
        with c_row2[1]:
            d_c1, d_c2, d_c3 = st.columns([2, 2, 4])
            sd = d_c1.date_input("시작", key=f"s_in_{cat}", value=st.session_state[f"sd_{cat}"], label_visibility="collapsed")
            ed = d_c2.date_input("종료", key=f"e_in_{cat}", value=st.session_state[f"ed_{cat}"], label_visibility="collapsed")
            
            # 퀵버튼 한 줄 배치
            q_cols = d_c3.columns(7)
            if q_cols[0].button("오늘", key=f"t_{cat}"): st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] = datetime.now().date(); st.rerun()
            if q_cols[1].button("1주", key=f"1w_{cat}"): st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] - timedelta(weeks=1); st.rerun()
            if q_cols[2].button("15일", key=f"15d_{cat}"): st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] - timedelta(days=15); st.rerun()
            if q_cols[3].button("1개월", key=f"1m_{cat}"): st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] - relativedelta(months=1); st.rerun()
            if q_cols[4].button("3개월", key=f"3m_{cat}"): st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] - relativedelta(months=3); st.rerun()
            if q_cols[5].button("6개월", key=f"6m_{cat}"): st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] - relativedelta(months=6); st.rerun()
            if q_cols[6].button("1년", key=f"1y_{cat}"): st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] - relativedelta(years=1); st.rerun()

        # 행 3: 기타설정 (표시개수)
        c_row3 = st.columns([1, 6])
        with c_row3[0]: st.markdown('<div class="search-label">표시개수</div>', unsafe_allow_html=True)
        with c_row3[1]:
            p_size = st.selectbox("개수", [50, 100, 150, 200], key=f"ps_{cat}", label_visibility="collapsed")

        st.markdown('</div>', unsafe_allow_html=True)

        # 검색/초기화 버튼 (이미지처럼 중앙 하단 배치)
        b_c1, b_c2, b_c3, b_c4, b_c5 = st.columns([4, 1, 1, 1, 4])
        search_exe = b_c2.button("🔍 검색", key=f"btn_{cat}", type="primary", use_container_width=True)
        if b_c3.button("🔄 초기화", key=f"reset_{cat}", use_container_width=True):
            st.session_state[f"sd_{cat}"] = datetime(2025,1,1).date()
            st.session_state[f"ed_{cat}"] = datetime.now().date()
            st.rerun()
        b_c4.link_button("⛓️ 바로가기", "https://g2b-info.streamlit.app/", use_container_width=True)

        # --- 검색 결과 출력 ---
        if search_exe:
            with st.spinner("데이터 로딩 중..."):
                df = fetch_data(SHEET_FILE_IDS[cat], is_sheet=(cat != '종합쇼핑몰'))
                if not df.empty:
                    # 날짜 필터링 로직 (동일)
                    s_str, e_str = sd.strftime('%Y%m%d'), ed.strftime('%Y%m%d')
                    if cat == '나라장터_발주':
                        y_c, m_c = df.columns[4], df.columns[12]
                        df['tmp_dt'] = df[y_c].astype(str) + df[m_c].astype(str).str.zfill(2) + "01"
                    else:
                        d_col = DATE_COL_MAP.get(cat)
                        df['tmp_dt'] = df[d_col].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8] if d_col in df.columns else "00000000"
                    df = df[(df['tmp_dt'] >= s_str[:6]+"01") & (df['tmp_dt'] <= e_str)]

                    # 키워드 검색 (AND/OR 적용)
                    if k1:
                        def get_mask(k):
                            if field == "ALL": return df.astype(str).apply(lambda x: x.str.contains(k, case=False, na=False)).any(axis=1)
                            return df[field].astype(str).str.contains(k, case=False, na=False)
                        mask1 = get_mask(k1)
                        if logic == "AND" and k2: df = df[mask1 & get_mask(k2)]
                        elif logic == "OR" and k2: df = df[mask1 | get_mask(k2)]
                        else: df = df[mask1]

                    if not df.empty:
                        st.markdown("---")
                        # 엑셀 다운로드 버튼
                        csv = df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                        st.download_button("📊 전체 데이터(엑셀용) 다운로드", csv, f"{cat}.csv", "text/csv")
                        
                        # 표출 컬럼 제한
                        idx_list = DISPLAY_INDEX_MAP.get(cat, [])
                        show_cols = [df.columns[idx] if isinstance(idx, int) else idx for idx in idx_list if (isinstance(idx, int) and idx < len(df.columns)) or (isinstance(idx, str) and idx in df.columns)]
                        
                        st.success(f"조회 결과: {len(df):,}건")
                        st.dataframe(df[show_cols].head(p_size), use_container_width=True, height=600)
                    else:
                        st.warning("검색 결과가 없습니다.")
