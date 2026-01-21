import streamlit as st
import pandas as pd
from datetime import datetime
import io
import json
import requests
from google.oauth2 import service_account
import google.auth.transport.requests
from googleapiclient.discovery import build
from dateutil.relativedelta import relativedelta

# --- 1. 페이지 설정 및 디자인 ---
st.set_page_config(page_title="공공조달 DATA 통합검색 시스템", layout="wide")

st.markdown("""
    <style>
    .block-container { padding-top: 1rem !important; padding-bottom: 0rem !important; }
    .main { background-color: #f4f4f4; font-size: 30px !important; }
    .title-text { font-size: 24px !important; font-weight: bold; color: #333; margin: 0; padding: 0; }
    .search-container { background-color: white; border: 1px solid #ccc; margin-bottom: 10px; }
    .search-label { background-color: #f9f9f9; width: 120px; padding: 8px; font-weight: bold; border-right: 1px solid #eee; text-align: center; }
    .stTabs [aria-selected="true"] { background-color: #00b050 !important; color: white !important; }
    .stDataFrame { font-size: 12px !important; }
    /* 페이지네이션 버튼 스타일 */
    .page-btn-container { display: flex; justify-content: center; align-items: center; gap: 20px; margin-top: 10px; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 구글 인증 및 데이터 로드 ---
@st.cache_resource
def get_drive_service():
    auth_json_str = st.secrets["GOOGLE_AUTH_JSON"]
    info = json.loads(auth_json_str)
    creds = service_account.Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/drive.readonly', 'https://www.googleapis.com/auth/spreadsheets.readonly'])
    return build('drive', 'v3', credentials=creds), creds

drive_service, credentials = get_drive_service()

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

# --- 3. 설정 ---
SHEET_FILE_IDS = {'나라장터_발주': '1pGnb6O5Z1ahaHYuQdydyoY1Ayf147IoGmLRdA3WAHi4', '나라장터_계약': '15Hsr_nup4ZteIZ4Jyov8wG2s_rKoZ25muqRE3-sRnaw', '군수품_발주': '1pzW51Z29SSoQk7al_GvN_tj5smuhOR3J2HWnL_16fcI', '군수품_계약': '1KPMUz0IKM6AQvqwfAkvW96WNvzbycN56vNlFnDmfRTw', '군수품_공고': '1opuA_UzNm27U9QkbMay5UsyQqcwfxiEmIHNRdc4MyHM', '군수품_수의': '1aYA18kPrSkpbayzbn16EdKUScVRwr2Nutyid5No5qjk', '종합쇼핑몰': '1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr'}
DISPLAY_INDEX_MAP = {'군수품_계약': [7, 5, 3, 1, 12], '군수품_수의': [12, 10, 8, 3], '군수품_발주': [7, 8, 12, 2, 3], '군수품_공고': [0, 17, 15, 22], '나라장터_발주': [9, 13, 20], '나라장터_계약': [0, 3, 4, 5, 6], '종합쇼핑몰': ["수요기관명", "계약납품요구일자", "세부품명", "계약명", "업체명", "수량", "금액"]}
DATE_COL_MAP = {'군수품_발주': '발주예정월', '군수품_수의': '개찰일자', '군수품_계약': '계약일자', '군수품_공고': '공고일자', '나라장터_계약': '★가공_계약일', '종합쇼핑몰': '계약납품요구일자'}

# --- 4. 상단 헤더 ---
h1, h2 = st.columns([3, 1])
with h1: st.markdown('<p class="title-text">🏛 공공조달 DATA 통합검색 시스템</p>', unsafe_allow_html=True)
with h2: st.link_button("⛓️ 지자체 유지보수 내역", "https://g2b-info.streamlit.app/", use_container_width=True)
st.markdown("<hr style='margin: 5px 0px 10px 0px; border-top: 2px solid #333;'>", unsafe_allow_html=True)

# --- 5. 탭 구성 ---
tabs = st.tabs(list(SHEET_FILE_IDS.keys()))

for i, tab in enumerate(tabs):
    cat = list(SHEET_FILE_IDS.keys())[i]
    with tab:
        if f"result_{cat}" not in st.session_state: st.session_state[f"result_{cat}"] = None
        if f"sd_{cat}" not in st.session_state: st.session_state[f"sd_{cat}"] = datetime(2025, 1, 1).date()
        if f"ed_{cat}" not in st.session_state: st.session_state[f"ed_{cat}"] = datetime.now().date()
        if f"curr_page_{cat}" not in st.session_state: st.session_state[f"curr_page_{cat}"] = 1

        # [중앙 정렬 레이아웃] 양쪽 여백 1:8:1 비율로 배치
        _, center_area, _ = st.columns([1, 8, 1])
        
        with center_area:
            st.markdown('<div class="search-container">', unsafe_allow_html=True)
            # 1행: 검색조건
            r1_l, r1_r = st.columns([1, 8.5])
            with r1_l: st.markdown('<div class="search-label">검색조건</div>', unsafe_allow_html=True)
            with r1_r:
                sc1, sc2, sc3, sc4 = st.columns([1.5, 3, 1, 3])
                f_val = sc1.selectbox("필드", ["ALL", "수요기관명", "업체명", "계약명", "세부품명"], key=f"f_{cat}", label_visibility="collapsed")
                k1_val = sc2.text_input("검색어1", key=f"k1_{cat}", label_visibility="collapsed")
                l_val = sc3.selectbox("논리", ["NONE", "AND", "OR"], key=f"l_{cat}", label_visibility="collapsed")
                k2_val = sc4.text_input("검색어2", key=f"k2_{cat}", label_visibility="collapsed", disabled=(l_val=="NONE"))

            # 2행: 기간 + 퀵버튼 + 검색
            r2_l, r2_r = st.columns([1, 8.5])
            with r2_l: st.markdown('<div class="search-label" style="border-bottom:none;">조회기간</div>', unsafe_allow_html=True)
            with r2_r:
                d1, d2, d3, d4 = st.columns([1.5, 1.5, 5, 1.5])
                # 날짜 입력값 반영 로직 수정
                sd_val = d1.date_input("시작", key=f"s_in_{cat}", value=st.session_state[f"sd_{cat}"], label_visibility="collapsed")
                ed_val = d2.date_input("종료", key=f"e_in_{cat}", value=st.session_state[f"ed_{cat}"], label_visibility="collapsed")
                
                q_cols = d3.columns(6)
                def handle_quick_date(m=0, y=0):
                    st.session_state[f"ed_{cat}"] = datetime.now().date()
                    st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] - relativedelta(months=m, years=y)
                    st.rerun()

                if q_cols[0].button("1개월", key=f"m1_{cat}"): handle_quick_date(m=1)
                if q_cols[1].button("3개월", key=f"m3_{cat}"): handle_quick_date(m=3)
                if q_cols[2].button("6개월", key=f"m6_{cat}"): handle_quick_date(m=6)
                if q_cols[3].button("9개월", key=f"m9_{cat}"): handle_quick_date(m=9)
                if q_cols[4].button("1년", key=f"y1_{cat}"): handle_quick_date(y=1)
                if q_cols[5].button("2년", key=f"y2_{cat}"): handle_quick_date(y=2)
                
                search_exe = d4.button("🔍 검색실행", key=f"exe_{cat}", type="primary", use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        # --- 검색 처리 ---
        if search_exe:
            with st.spinner("데이터 로딩 중..."):
                df_raw = fetch_data(SHEET_FILE_IDS[cat], is_sheet=(cat != '종합쇼핑몰'))
                if not df_raw.empty:
                    s_s, e_s = sd_val.strftime('%Y%m%d'), ed_val.strftime('%Y%m%d')
                    if cat == '나라장터_발주':
                        df_raw['tmp_dt'] = df_raw.iloc[:,4].astype(str) + df_raw.iloc[:,12].astype(str).str.zfill(2) + "01"
                    else:
                        d_col = DATE_COL_MAP.get(cat)
                        df_raw['tmp_dt'] = df_raw[d_col].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8] if d_col in df_raw.columns else "00000000"
                    
                    df_filtered = df_raw[(df_raw['tmp_dt'] >= s_s[:6]+"01") & (df_raw['tmp_dt'] <= e_s)]
                    if k1_val:
                        def get_m(k): return df_filtered.astype(str).apply(lambda x: x.str.contains(k, case=False, na=False)).any(axis=1) if f_val == "ALL" else df_filtered[f_val].astype(str).str.contains(k, case=False, na=False)
                        if l_val == "AND" and k2_val: df_filtered = df_filtered[get_m(k1_val) & get_m(k2_val)]
                        elif l_val == "OR" and k2_val: df_filtered = df_filtered[get_m(k1_val) | get_m(k2_val)]
                        else: df_filtered = df_filtered[get_m(k1_val)]
                    
                    st.session_state[f"result_{cat}"] = df_filtered
                    st.session_state[f"curr_page_{cat}"] = 1 # 검색 시 첫페이지로

        # --- 결과 표시 ---
        res_df = st.session_state[f"result_{cat}"]
        if res_df is not None:
            st.markdown("<br>", unsafe_allow_html=True)
            ctrl_l, ctrl_r = st.columns([6, 4])
            with ctrl_r:
                c1, c2, c3 = st.columns([1.5, 1, 1])
                p_limit = c1.selectbox("표시개수", [50, 100, 150, 200], key=f"ps_{cat}", label_visibility="collapsed")
                csv_data = res_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                c2.download_button("📑 CSV", csv_data, f"{cat}.csv", "text/csv")
                c3.download_button("📊 Excel", csv_data, f"{cat}.xlsx", "application/vnd.ms-excel")
            with ctrl_l: st.markdown(f"**✅ 조회결과: {len(res_df):,}건**")

            # [페이지네이션 계산]
            total_rows = len(res_df)
            total_pages = max((total_rows - 1) // p_limit + 1, 1)
            curr_p = st.session_state[f"curr_page_{cat}"]
            
            # 현재 페이지 데이터만 슬라이싱
            start_idx = (curr_p - 1) * p_limit
            end_idx = start_idx + p_limit
            
            idx_list = DISPLAY_INDEX_MAP.get(cat, [])
            show_cols = [res_df.columns[idx] if isinstance(idx, int) else idx for idx in idx_list if (isinstance(idx, int) and idx < len(res_df.columns)) or (isinstance(idx, str) and idx in res_df.columns)]
            
            # 테이블 출력
            st.dataframe(res_df[show_cols].iloc[start_idx:end_idx], use_container_width=True, height=550)

            # [페이지 컨트롤러]
            st.markdown('<div class="page-btn-container">', unsafe_allow_html=True)
            p_col1, p_col2, p_col3, p_col4, p_col5 = st.columns([4, 1, 2, 1, 4])
            if p_col2.button("이전", key=f"prev_{cat}", disabled=(curr_p <= 1)):
                st.session_state[f"curr_page_{cat}"] -= 1
                st.rerun()
            p_col3.markdown(f"<p style='text-align:center;'><b>{curr_p} / {total_pages} 페이지</b></p>", unsafe_allow_html=True)
            if p_col4.button("다음", key=f"next_{cat}", disabled=(curr_p >= total_pages)):
                st.session_state[f"curr_page_{cat}"] += 1
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)
