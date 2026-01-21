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

# --- [1] 페이지 기본 설정 ---
st.set_page_config(page_title="공공조달 DATA 통합검색 시스템", layout="wide")

st.markdown("""
    <style>
    .block-container { padding-top: 3.5rem !important; padding-bottom: 0rem !important; }
    .main { background-color: #f4f4f4; font-size: 13px !important; }
    .title-text { font-size: 24px !important; font-weight: bold; color: #333; margin-bottom: 5px; }
    .search-container { background-color: white; border: 1px solid #ccc; margin-bottom: 10px; }
    .search-label { background-color: #f9f9f9; width: 120px; padding: 8px; font-weight: bold; border-right: 1px solid #eee; text-align: center; }
    .stTabs [aria-selected="true"] { background-color: #00b050 !important; color: white !important; }
    .stDataFrame { font-size: 12px !important; }
    /* 퀵버튼 전용 스타일: 작고 한 줄에 들어오게 */
    .q-btn button { height: 28px !important; font-size: 11px !important; padding: 0 !important; margin-bottom: 5px; }
    </style>
    """, unsafe_allow_html=True)

# --- [2] 데이터 연결 함수 및 설정 (기존과 동일) ---
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

SHEET_FILE_IDS = {'나라장터_발주': '1pGnb6O5Z1ahaHYuQdydyoY1Ayf147IoGmLRdA3WAHi4', '나라장터_계약': '15Hsr_nup4ZteIZ4Jyov8wG2s_rKoZ25muqRE3-sRnaw', '군수품_발주': '1pzW51Z29SSoQk7al_GvN_tj5smuhOR3J2HWnL_16fcI', '군수품_계약': '1KPMUz0IKM6AQvqwfAkvW96WNvzbycN56vNlFnDmfRTw', '군수품_공고': '1opuA_UzNm27U9QkbMay5UsyQqcwfxiEmIHNRdc4MyHM', '군수품_수의': '1aYA18kPrSkpbayzbn16EdKUScVRwr2Nutyid5No5qjk', '종합쇼핑몰': '1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr'}
DISPLAY_INDEX_MAP = {'군수품_계약': [7, 5, 3, 1, 12], '군수품_수의': [12, 10, 8, 3], '군수품_발주': [7, 8, 12, 2, 3], '군수품_공고': [0, 17, 15, 22], '나라장터_발주': [9, 13, 20], '나라장터_계약': [0, 3, 4, 5, 6], '종합쇼핑몰': ["수요기관명", "계약납품요구일자", "세부품명", "계약명", "업체명", "수량", "금액"]}
DATE_COL_MAP = {'군수품_발주': '발주예정월', '군수품_수의': '개찰일자', '군수품_계약': '계약일자', '군수품_공고': '공고일자', '나라장터_계약': '★가공_계약일', '종합쇼핑몰': '계약납품요구일자'}

# --- [3] 상단 헤더 ---
h1, h2 = st.columns([3, 1])
with h1: st.markdown('<p class="title-text">🏛 공공조달 DATA 통합검색 시스템</p>', unsafe_allow_html=True)
with h2: st.link_button("⛓️ 지자체 유지보수 내역", "https://g2b-info.streamlit.app/", use_container_width=True)
st.markdown("<hr style='margin: 0px 0px 10px 0px; border-top: 2px solid #333;'>", unsafe_allow_html=True)

# --- [4] 결과 테이블 조각 (Fragment) ---
@st.fragment
def show_result_table(cat, df, idx_list):
    st.markdown("<br>", unsafe_allow_html=True)
    ctrl_l, ctrl_r = st.columns([6, 4])
    with ctrl_r:
        c1, c2, c3 = st.columns([1.5, 1, 1])
        p_limit = c1.selectbox("표시개수", [50, 100, 150, 200], key=f"ps_sel_{cat}", label_visibility="collapsed")
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
        c2.download_button("📑 CSV", df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig'), f"{cat}.csv", "text/csv", key=f"dl_csv_{cat}")
        c3.download_button("📊 Excel", output.getvalue(), f"{cat}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"dl_xl_{cat}")
    
    with ctrl_l: st.markdown(f"**✅ 조회결과: {len(df):,}건**")

    total_pages = max((len(df) - 1) // p_limit + 1, 1)
    if f"p_num_{cat}" not in st.session_state: st.session_state[f"p_num_{cat}"] = 1
    curr_p = st.session_state[f"p_num_{cat}"]

    show_cols = [df.columns[idx] if isinstance(idx, int) else idx for idx in idx_list if (isinstance(idx, int) and idx < len(df.columns)) or (isinstance(idx, str) and idx in df.columns)]
    st.dataframe(df[show_cols].iloc[(curr_p-1)*p_limit : curr_p*p_limit], use_container_width=True, height=520)

    # 페이지네이션
    pg_cols = st.columns([1, 8, 1])
    with pg_cols[1]:
        start_p, end_p = max(1, curr_p - 4), min(total_pages, max(1, curr_p - 4) + 9)
        btn_cols = st.columns(14)
        if btn_cols[0].button("«", key=f"f10_{cat}"): st.session_state[f"p_num_{cat}"] = max(1, curr_p - 10); st.rerun()
        if btn_cols[1].button("‹", key=f"f1_{cat}"): st.session_state[f"p_num_{cat}"] = max(1, curr_p - 1); st.rerun()
        for i, p in enumerate(range(start_p, end_p + 1)):
            if btn_cols[i+2].button(str(p), key=f"pg_{cat}_{p}", type="primary" if p == curr_p else "secondary"):
                st.session_state[f"p_num_{cat}"] = p; st.rerun()
        if btn_cols[12].button("›", key=f"n1_{cat}"): st.session_state[f"p_num_{cat}"] = min(total_pages, curr_p + 1); st.rerun()
        if btn_cols[13].button("»", key=f"n10_{cat}"): st.session_state[f"p_num_{cat}"] = min(total_pages, curr_p + 10); st.rerun()

# --- [5] 메인 루프 ---
tabs = st.tabs(list(SHEET_FILE_IDS.keys()))

for i, tab in enumerate(tabs):
    cat = list(SHEET_FILE_IDS.keys())[i]
    with tab:
        # [핵심] 날짜 세션 초기화 및 버튼 처리 (입력창보다 위에 배치)
        if f"sd_{cat}" not in st.session_state: st.session_state[f"sd_{cat}"] = datetime.now().date() - relativedelta(months=6)
        if f"ed_{cat}" not in st.session_state: st.session_state[f"ed_{cat}"] = datetime.now().date()
        if f"df_{cat}" not in st.session_state: st.session_state[f"df_{cat}"] = None

        # [수정] 버튼 클릭 시 세션 값을 즉시 변경 (rerun 없이도 아래 date_input에 반영되도록 로직 구성)
        st.markdown('<div class="q-btn">', unsafe_allow_html=True)
        q_cols = st.columns([1.2, 1, 1, 1, 1, 1, 1, 4]) # 퀵버튼을 한 줄에 작게 배치
        with q_cols[0]: st.write("**기간선택:**")
        if q_cols[1].button("1개월", key=f"m1_{cat}"): st.session_state[f"sd_{cat}"] = datetime.now().date() - relativedelta(months=1); st.session_state[f"ed_{cat}"] = datetime.now().date()
        if q_cols[2].button("3개월", key=f"m3_{cat}"): st.session_state[f"sd_{cat}"] = datetime.now().date() - relativedelta(months=3); st.session_state[f"ed_{cat}"] = datetime.now().date()
        if q_cols[3].button("6개월", key=f"m6_{cat}"): st.session_state[f"sd_{cat}"] = datetime.now().date() - relativedelta(months=6); st.session_state[f"ed_{cat}"] = datetime.now().date()
        if q_cols[4].button("9개월", key=f"m9_{cat}"): st.session_state[f"sd_{cat}"] = datetime.now().date() - relativedelta(months=9); st.session_state[f"ed_{cat}"] = datetime.now().date()
        if q_cols[5].button("1년", key=f"y1_{cat}"): st.session_state[f"sd_{cat}"] = datetime.now().date() - relativedelta(years=1); st.session_state[f"ed_{cat}"] = datetime.now().date()
        if q_cols[6].button("2년", key=f"y2_{cat}"): st.session_state[f"sd_{cat}"] = datetime.now().date() - relativedelta(years=2); st.session_state[f"ed_{cat}"] = datetime.now().date()
        st.markdown('</div>', unsafe_allow_html=True)

        # [검색창 레이아웃]
        _, center_area, _ = st.columns([1, 8, 1])
        with center_area:
            st.markdown('<div class="search-container">', unsafe_allow_html=True)
            # 행1
            r1_l, r1_r = st.columns([1, 8.5])
            with r1_l: st.markdown('<div class="search-label">검색조건</div>', unsafe_allow_html=True)
            with r1_r:
                sc1, sc2, sc3, sc4 = st.columns([1.5, 3, 1, 3])
                f_val = sc1.selectbox("필드", ["ALL", "수요기관명", "업체명", "계약명", "세부품명"], key=f"f_{cat}", label_visibility="collapsed")
                k1_val = sc2.text_input("검색어1", key=f"k1_{cat}", label_visibility="collapsed")
                l_val = sc3.selectbox("논리", ["NONE", "AND", "OR"], key=f"l_{cat}", label_visibility="collapsed")
                k2_val = sc4.text_input("검색어2", key=f"k2_{cat}", label_visibility="collapsed", disabled=(l_val=="NONE"))

            # 행2 (이미 결정된 세션 값을 value로 사용)
            r2_l, r2_r = st.columns([1, 8.5])
            with r2_l: st.markdown('<div class="search-label" style="border-bottom:none;">조회기간</div>', unsafe_allow_html=True)
            with r2_r:
                d1, d2, d3, d4 = st.columns([2, 2, 4.5, 1.5])
                sd_in = d1.date_input("시작", value=st.session_state[f"sd_{cat}"], key=f"sd_in_{cat}", label_visibility="collapsed")
                ed_in = d2.date_input("종료", value=st.session_state[f"ed_{cat}"], key=f"ed_in_{cat}", label_visibility="collapsed")
                # 수동 변경 시 세션 업데이트
                st.session_state[f"sd_{cat}"], st.session_state[f"ed_{cat}"] = sd_in, ed_in
                
                search_exe = d4.button("🔍 검색실행", key=f"exe_{cat}", type="primary", use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        if search_exe:
            with st.spinner("조회 중..."):
                df_raw = fetch_data(SHEET_FILE_IDS[cat], is_sheet=(cat != '종합쇼핑몰'))
                if not df_raw.empty:
                    s_s, e_s = sd_in.strftime('%Y%m%d'), ed_in.strftime('%Y%m%d')
                    if cat == '나라장터_발주':
                        df_raw['tmp_dt'] = df_raw.iloc[:,4].astype(str) + df_raw.iloc[:,12].astype(str).str.zfill(2) + "01"
                    else:
                        d_col = DATE_COL_MAP.get(cat)
                        df_raw['tmp_dt'] = df_raw[d_col].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8] if d_col in df_raw.columns else "0"
                    
                    df_filtered = df_raw[(df_raw['tmp_dt'] >= s_s[:6]+"01") & (df_raw['tmp_dt'] <= e_s)]
                    if k1_val and k1_val.strip():
                        def get_mask(k): return df_filtered.astype(str).apply(lambda x: x.str.contains(k, case=False, na=False)).any(axis=1) if f_val == "ALL" else df_filtered[f_val].astype(str).str.contains(k, case=False, na=False)
                        if l_val == "AND" and k2_val: df_filtered = df_filtered[get_mask(k1_val) & get_mask(k2_val)]
                        elif l_val == "OR" and k2_val: df_filtered = df_filtered[get_mask(k1_val) | get_mask(k2_val)]
                        else: df_filtered = df_filtered[get_mask(k1_val)]
                    
                    # 기본 정렬: 최신순
                    st.session_state[f"df_{cat}"] = df_filtered.sort_values(by='tmp_dt', ascending=False)
                    st.session_state[f"p_num_{cat}"] = 1

        if st.session_state[f"df_{cat}"] is not None:
            show_result_table(cat, st.session_state[f"df_{cat}"], DISPLAY_INDEX_MAP.get(cat, []))
