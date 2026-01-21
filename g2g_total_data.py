import streamlit as st
import pandas as pd
from datetime import datetime, date
import io
import json
import requests
from google.oauth2 import service_account
import google.auth.transport.requests
from googleapiclient.discovery import build
from dateutil.relativedelta import relativedelta

# --- [1] 페이지 설정 및 UI 디자인 (네모칸 완벽 제거) ---
st.set_page_config(page_title="공공조달 DATA 통합검색 시스템", layout="wide")

st.markdown("""
    <style>
    /* 상단 여백 및 배경 */
    .block-container { padding-top: 4rem !important; padding-bottom: 0rem !important; }
    .main { background-color: #f4f4f4; }
    
    /* [수정] 검색조건 위 네모칸(공백 및 테두리) 제거 */
    div[data-testid="stVerticalBlock"] > div:has(div.search-container) { gap: 0rem !important; }
    .search-container { 
        background-color: white; 
        border: 1px solid #ccc; 
        padding: 10px; 
        margin-top: -10px; /* 위쪽 여백 강제 밀착 */
    }
    .search-label { background-color: #f9f9f9; width: 120px; padding: 8px; font-weight: bold; border-right: 1px solid #eee; text-align: center; }
    
    /* [수정] 결과 위 색칠된 네모(정보바) 투명화 및 선 정리 */
    .data-info-bar { 
        background-color: transparent !important; 
        border-top: 1px solid #ddd; 
        border-bottom: 1px solid #ddd;
        padding: 10px 0px; 
        margin-top: 5px;
    }
    
    /* 퀵버튼 밀착 및 수평 정렬 */
    div[data-testid="column"] { padding-left: 0.5px !important; padding-right: 0.5px !important; }
    .stButton > button { height: 35px !important; font-size: 12px !important; border-radius: 2px !important; }
    
    /* 페이지네이션 버튼 확대 */
    .page-ctrl-row button { height: 45px !important; min-width: 45px !important; font-size: 15px !important; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

# --- [2] 구글 데이터 연결 함수 ---
@st.cache_resource
def get_drive_service():
    auth_json_str = st.secrets["GOOGLE_AUTH_JSON"]
    info = json.loads(auth_json_str)
    creds = service_account.Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/drive.readonly', 'https://www.googleapis.com/auth/spreadsheets.readonly'])
    return build('drive', 'v3', credentials=creds), creds

def fetch_data(file_id, is_sheet=True):
    drive_service, credentials = get_drive_service()
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    headers = {'Authorization': f'Bearer {credentials.token}'}
    try:
        if is_sheet:
            url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv"
            return pd.read_csv(io.BytesIO(requests.get(url, headers=headers).content), low_memory=False)
        else:
            results = drive_service.files().list(q=f"'{file_id}' in parents and trashed = false").execute()
            dfs = [pd.read_csv(io.BytesIO(requests.get(f"https://www.googleapis.com/drive/v3/files/{f['id']}?alt=media", headers=headers).content), low_memory=False) for f in results.get('files', [])]
            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    except Exception as e:
        return pd.DataFrame()

# --- [3] 매핑 데이터 설정 ---
SHEET_FILE_IDS = {'나라장터_발주': '1pGnb6O5Z1ahaHYuQdydyoY1Ayf147IoGmLRdA3WAHi4', '나라장터_계약': '15Hsr_nup4ZteIZ4Jyov8wG2s_rKoZ25muqRE3-sRnaw', '군수품_발주': '1pzW51Z29SSoQk7al_GvN_tj5smuhOR3J2HWnL_16fcI', '군수품_계약': '1KPMUz0IKM6AQvqwfAkvW96WNvzbycN56vNlFnDmfRTw', '군수품_공고': '1opuA_UzNm27U9QkbMay5UsyQqcwfxiEmIHNRdc4MyHM', '군수품_수의': '1aYA18kPrSkpbayzbn16EdKUScVRwr2Nutyid5No5qjk', '종합쇼핑몰': '1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr'}
DISPLAY_INDEX_MAP = {'군수품_계약': [7, 5, 3, 1, 12], '군수품_수의': [12, 10, 8, 3], '군수품_발주': [7, 8, 12, 2, 3], '군수품_공고': [0, 17, 15, 22], '나라장터_발주': [9, 13, 20], '나라장터_계약': [0, 3, 4, 5, 6], '종합쇼핑몰': ["수요기관명", "계약납품요구일자", "세부품명", "계약명", "업체명", "수량", "금액"]}
DATE_COL_MAP = {'군수품_발주': '발주예정월', '군수품_수의': '개찰일자', '군수품_계약': '계약일자', '군수품_공고': '공고일자', '나라장터_계약': '★가공_계약일', '종합쇼핑몰': '계약납품요구일자'}

# --- [4] 상단 헤더 ---
h_col1, h_col2 = st.columns([3, 1])
with h_col1:
    st.markdown('<p class="title-text">🏛 공공조달 DATA 통합검색 시스템</p>', unsafe_allow_html=True)
with h_col2:
    st.link_button("⛓️ 지자체 유지보수 내역", "https://g2b-info.streamlit.app/", use_container_width=True)
st.markdown("<hr style='margin: 10px 0px; border-top: 2px solid #333;'>", unsafe_allow_html=True)

# --- [5] 결과 테이블 조각 (Fragment) - TypeError 방지 로직 보강 ---
@st.fragment
def show_result_table(cat, idx_list):
    df = st.session_state.get(f"df_{cat}")
    if df is None or df.empty:
        return

    # 정보바 (결과 수 | 정렬 | 개수 | 다운로드)
    st.markdown('<div class="data-info-bar">', unsafe_allow_html=True)
    res_col, sort_col1, sort_col2, sort_col3, limit_col, dl_col = st.columns([1.2, 1.8, 1.8, 0.7, 1.2, 2.3])
    
    res_col.markdown(f"**✅ 결과: {len(df):,}건**")
    
    # 보여줄 컬럼 리스트 안전하게 생성
    show_cols = []
    for idx in idx_list:
        if isinstance(idx, int) and idx < len(df.columns): show_cols.append(df.columns[idx])
        elif isinstance(idx, str) and idx in df.columns: show_cols.append(idx)
    
    sort_target = sort_col1.selectbox("정렬기준", ["날짜순"] + show_cols, key=f"st_{cat}", label_visibility="collapsed")
    sort_dir = sort_col2.selectbox("순서", ["내림차순", "오름차순"], key=f"sd_{cat}", label_visibility="collapsed")
    
    if sort_col3.button("정렬", key=f"sb_{cat}", use_container_width=True):
        ascending = (sort_dir == "오름차순")
        # 데이터 타입 에러 방지를 위해 문자열 변환 후 정렬하거나 원본 유지
        sort_key = 'tmp_dt' if sort_target == "날짜순" else sort_target
        st.session_state[f"df_{cat}"] = df.sort_values(by=sort_key, ascending=ascending)
        st.rerun()

    p_limit = limit_col.selectbox("개수", [50, 100, 150, 200], key=f"ps_{cat}", label_visibility="collapsed")

    with dl_col:
        d_csv, d_xl = st.columns(2)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
        d_csv.download_button("📑 CSV", df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig'), f"{cat}.csv", "text/csv")
        d_xl.download_button("📊 Excel", output.getvalue(), f"{cat}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    st.markdown('</div>', unsafe_allow_html=True)

    # 테이블 및 페이지네이션
    total_pages = max((len(df) - 1) // p_limit + 1, 1)
    curr_p = st.session_state.get(f"p_num_{cat}", 1)
    if curr_p > total_pages: curr_p = total_pages
    
    st.dataframe(df[show_cols].iloc[(curr_p-1)*p_limit : curr_p*p_limit], use_container_width=True, height=520)

    # 하단 페이지 버튼
    pg_cols = st.columns([1, 8, 1])
    with pg_cols[1]:
        st.markdown('<div class="page-ctrl-row">', unsafe_allow_html=True)
        btn_cols = st.columns(14)
        start_p, end_p = max(1, curr_p - 4), min(total_pages, max(1, curr_p - 4) + 9)
        if btn_cols[0].button("«", key=f"f10_{cat}"): st.session_state[f"p_num_{cat}"] = max(1, curr_p - 10); st.rerun()
        if btn_cols[1].button("‹", key=f"f1_{cat}"): st.session_state[f"p_num_{cat}"] = max(1, curr_p - 1); st.rerun()
        for i, p in enumerate(range(start_p, end_p + 1)):
            if btn_cols[i+2].button(str(p), key=f"pg_{cat}_{p}", type="primary" if p == curr_p else "secondary"):
                st.session_state[f"p_num_{cat}"] = p; st.rerun()
        if btn_cols[12].button("›", key=f"n1_{cat}"): st.session_state[f"p_num_{cat}"] = min(total_pages, curr_p + 1); st.rerun()
        if btn_cols[13].button("»", key=f"n10_{cat}"): st.session_state[f"p_num_{cat}"] = min(total_pages, curr_p + 10); st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

# --- [6] 메인 루프 ---
tabs = st.tabs(list(SHEET_FILE_IDS.keys()))

for i, tab in enumerate(tabs):
    cat = list(SHEET_FILE_IDS.keys())[i]
    with tab:
        # 날짜 초기화 (date 객체 보장)
        today = date.today()
        if f"sd_{cat}" not in st.session_state: st.session_state[f"sd_{cat}"] = today - relativedelta(months=6)
        if f"ed_{cat}" not in st.session_state: st.session_state[f"ed_{cat}"] = today
        if f"ver_{cat}" not in st.session_state: st.session_state[f"ver_{cat}"] = 0
        if f"df_{cat}" not in st.session_state: st.session_state[f"df_{cat}"] = None

        _, center_area, _ = st.columns([0.1, 9.8, 0.1])
        with center_area:
            st.markdown('<div class="search-container">', unsafe_allow_html=True)
            r1_l, r1_r = st.columns([1, 8.5])
            with r1_l: st.markdown('<div class="search-label">검색조건</div>', unsafe_allow_html=True)
            with r1_r:
                sc1, sc2, sc3, sc4 = st.columns([1.5, 3, 1, 3])
                f_val = sc1.selectbox("필드", ["ALL", "수요기관명", "업체명", "계약명", "세부품명"], key=f"f_{cat}", label_visibility="collapsed")
                k1_val = sc2.text_input("검색어1", key=f"k1_{cat}", label_visibility="collapsed", placeholder="검색어")
                l_val = sc3.selectbox("논리", ["NONE", "AND", "OR"], key=f"l_{cat}", label_visibility="collapsed")
                k2_val = sc4.text_input("검색어2", key=f"k2_{cat}", label_visibility="collapsed", disabled=(l_val=="NONE"), placeholder="검색어2")

            r2_l, r2_r = st.columns([1, 8.5])
            with r2_l: st.markdown('<div class="search-label" style="border-bottom:none;">조회기간</div>', unsafe_allow_html=True)
            with r2_r:
                d1, d2, d3, d4 = st.columns([1.3, 1.3, 5.8, 1.2])
                
                v_num = st.session_state[f"ver_{cat}"]
                sd_in = d1.date_input("시작", value=st.session_state[f"sd_{cat}"], key=f"sd_w_{cat}_{v_num}", label_visibility="collapsed")
                ed_in = d2.date_input("종료", value=st.session_state[f"ed_{cat}"], key=f"ed_w_{cat}_{v_num}", label_visibility="collapsed")
                st.session_state[f"sd_{cat}"], st.session_state[f"ed_{cat}"] = sd_in, ed_in

                with d3:
                    q_cols = st.columns(6)
                    def set_period(m=0, y=0):
                        cur = date.today()
                        st.session_state[f"sd_{cat}"] = cur - relativedelta(months=m, years=y)
                        st.session_state[f"ed_{cat}"] = cur
                        st.session_state[f"ver_{cat}"] += 1
                        st.rerun()

                    if q_cols[0].button("1개월", key=f"m1_{cat}"): set_period(m=1)
                    if q_cols[1].button("3개월", key=f"m3_{cat}"): set_period(m=3)
                    if q_cols[2].button("6개월", key=f"m6_{cat}"): set_period(m=6)
                    if q_cols[3].button("9개월", key=f"m9_{cat}"): set_period(m=9)
                    if q_cols[4].button("1년", key=f"y1_{cat}"): set_period(y=1)
                    if q_cols[5].button("2년", key=f"y2_{cat}"): set_period(y=2)
                
                with d4:
                    search_exe = st.button("🔍 검색실행", key=f"exe_{cat}", type="primary", use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        if search_exe:
            with st.spinner("데이터 분석 중..."):
                df_raw = fetch_data(SHEET_FILE_IDS[cat], is_sheet=(cat != '종합쇼핑몰'))
                if not df_raw.empty:
                    s_s, e_s = sd_in.strftime('%Y%m%d'), ed_in.strftime('%Y%m%d')
                    # 날짜 가공 (문자열로 통일하여 TypeError 방지)
                    if cat == '나라장터_발주':
                        df_raw['tmp_dt'] = df_raw.iloc[:,4].astype(str) + df_raw.iloc[:,12].astype(str).str.zfill(2) + "01"
                    else:
                        d_col = DATE_COL_MAP.get(cat)
                        df_raw['tmp_dt'] = df_raw[d_col].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8] if d_col in df_raw.columns else "0"
                    
                    df_filtered = df_raw[(df_raw['tmp_dt'] >= s_s[:6]+"01") & (df_raw['tmp_dt'] <= e_s)].copy()
                    if k1_val and k1_val.strip():
                        def get_mask(k): return df_filtered.astype(str).apply(lambda x: x.str.contains(k, case=False, na=False)).any(axis=1) if f_val == "ALL" else df_filtered[f_val].astype(str).str.contains(k, case=False, na=False)
                        if l_val == "AND" and k2_val: df_filtered = df_filtered[get_mask(k1_val) & get_mask(k2_val)]
                        elif l_val == "OR" and k2_val: df_filtered = df_filtered[get_mask(k1_val) | get_mask(k2_val)]
                        else: df_filtered = df_filtered[get_mask(k1_val)]
                    
                    st.session_state[f"df_{cat}"] = df_filtered.sort_values(by='tmp_dt', ascending=False)
                    st.session_state[f"p_num_{cat}"] = 1
        
        if st.session_state[f"df_{cat}"] is not None:
            show_result_table(cat, DISPLAY_INDEX_MAP.get(cat, []))
