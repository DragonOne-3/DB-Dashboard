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

# --- [1] 페이지 설정 및 디자인 디자인 (CSS) ---
# 웹 브라우저 탭에 표시될 제목과 화면 너비를 'wide'로 설정합니다.
st.set_page_config(page_title="공공조달 DATA 통합검색 시스템", layout="wide")

st.markdown("""
    <style>
    /* block-container: 전체 화면의 상단 여백을 3.5rem 주어 제목이 짤리지 않게 보호합니다. */
    .block-container { padding-top: 3.5rem !important; padding-bottom: 0rem !important; }
    
    /* main: 앱 전체의 배경색과 기본 폰트 크기를 설정합니다. */
    .main { background-color: #f4f4f4; font-size: 13px !important; }
    
    /* title-text: 상단 메인 타이틀의 폰트 크기와 굵기를 지정합니다. */
    .title-text { font-size: 24px !important; font-weight: bold; color: #333; margin-bottom: 5px; }
    
    /* search-container: 검색창 영역의 흰색 배경과 테두리를 만듭니다. */
    .search-container { background-color: white; border: 1px solid #ccc; margin-bottom: 10px; }
    
    /* search-label: 검색창 왼쪽의 회색 항목 이름(검색조건, 조회기간 등) 영역입니다. */
    .search-label { background-color: #f9f9f9; width: 120px; padding: 8px; font-weight: bold; border-right: 1px solid #eee; text-align: center; }
    
    /* stTabs: 현재 선택된 탭을 초록색으로 강조합니다. */
    .stTabs [aria-selected="true"] { background-color: #00b050 !important; color: white !important; }
    
    /* stColumn: 1개월/3개월 등 버튼 사이의 간격을 0.5px로 매우 좁게 조정합니다. */
    .stColumn > div { padding-left: 0.5px !important; padding-right: 0.5px !important; }
    
    /* q-btn-container: 퀵버튼의 높이와 폰트를 조정하고 입력창과 상단 라인을 맞춥니다. */
    .q-btn-container button { height: 32px !important; font-size: 11px !important; white-space: nowrap !important; margin-top: 0px !important; }
    
    /* page-ctrl-row: 하단 페이지 번호 버튼을 크게(45px) 만들어 클릭이 편하게 합니다. */
    .page-ctrl-row button { height: 45px !important; min-width: 45px !important; font-size: 15px !important; }
    </style>
    """, unsafe_allow_html=True)

# --- [2] 구글 데이터 연결 함수 ---
@st.cache_resource
def get_drive_service():
    """구글 드라이브/시트 API 권한 인증을 수행합니다."""
    auth_json_str = st.secrets["GOOGLE_AUTH_JSON"]
    info = json.loads(auth_json_str)
    creds = service_account.Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/drive.readonly', 'https://www.googleapis.com/auth/spreadsheets.readonly'])
    return build('drive', 'v3', credentials=creds), creds

def fetch_data(file_id, is_sheet=True):
    """실제 구글 서버에서 데이터를 CSV 형태로 읽어와 판다스 데이터프레임으로 변환합니다."""
    drive_service, credentials = get_drive_service()
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    headers = {'Authorization': f'Bearer {credentials.token}'}
    if is_sheet:
        url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv"
        return pd.read_csv(io.BytesIO(requests.get(url, headers=headers).content), low_memory=False)
    else:
        # 폴더 내 모든 파일을 합치는 로직 (종합쇼핑몰 등)
        results = drive_service.files().list(q=f"'{file_id}' in parents and trashed = false").execute()
        dfs = [pd.read_csv(io.BytesIO(requests.get(f"https://www.googleapis.com/drive/v3/files/{f['id']}?alt=media", headers=headers).content), low_memory=False) for f in results.get('files', [])]
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# --- [3] 데이터 소스 및 매핑 설정 ---
# SHEET_FILE_IDS: 각 카테고리별 구글 시트의 ID 주소입니다.
SHEET_FILE_IDS = {'나라장터_발주': '1pGnb6O5Z1ahaHYuQdydyoY1Ayf147IoGmLRdA3WAHi4', '나라장터_계약': '15Hsr_nup4ZteIZ4Jyov8wG2s_rKoZ25muqRE3-sRnaw', '군수품_발주': '1pzW51Z29SSoQk7al_GvN_tj5smuhOR3J2HWnL_16fcI', '군수품_계약': '1KPMUz0IKM6AQvqwfAkvW96WNvzbycN56vNlFnDmfRTw', '군수품_공고': '1opuA_UzNm27U9QkbMay5UsyQqcwfxiEmIHNRdc4MyHM', '군수품_수의': '1aYA18kPrSkpbayzbn16EdKUScVRwr2Nutyid5No5qjk', '종합쇼핑몰': '1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr'}

# DISPLAY_INDEX_MAP: 데이터 표에서 보여줄 열(Column)의 번호나 이름을 지정합니다.
DISPLAY_INDEX_MAP = {'군수품_계약': [7, 5, 3, 1, 12], '군수품_수의': [12, 10, 8, 3], '군수품_발주': [7, 8, 12, 2, 3], '군수품_공고': [0, 17, 15, 22], '나라장터_발주': [9, 13, 20], '나라장터_계약': [0, 3, 4, 5, 6], '종합쇼핑몰': ["수요기관명", "계약납품요구일자", "세부품명", "계약명", "업체명", "수량", "금액"]}

# DATE_COL_MAP: 날짜 필터링의 기준이 되는 컬럼명을 지정합니다.
DATE_COL_MAP = {'군수품_발주': '발주예정월', '군수품_수의': '개찰일자', '군수품_계약': '계약일자', '군수품_공고': '공고일자', '나라장터_계약': '★가공_계약일', '종합쇼핑몰': '계약납품요구일자'}

# --- [4] 결과 테이블 표시 조각 (Fragment) ---
@st.fragment
def show_result_table(cat, idx_list):
    """
    페이지 번호를 눌러도 화면 전체가 새로고침되지 않도록 표 영역만 분리했습니다.
    정렬, 엑셀 다운로드, 데이터 표 표시, 페이지네이션 버튼 출력을 담당합니다.
    """
    df = st.session_state[f"df_{cat}"]
    if df is None: return

    st.markdown("<hr style='margin: 15px 0px; border-top: 1px solid #eee;'>", unsafe_allow_html=True)
    
    # 📊 전체 정렬 기능: 선택한 항목을 기준으로 전체 데이터를 다시 정렬합니다.
    c_sort_label, c_sort_col, c_sort_order, c_sort_btn, c_empty = st.columns([1, 2, 2, 1, 4])
    c_sort_label.markdown("**📊 전체 정렬:**")
    show_cols = [df.columns[idx] if isinstance(idx, int) else idx for idx in idx_list if (isinstance(idx, int) and idx < len(df.columns)) or (isinstance(idx, str) and idx in df.columns)]
    
    sort_target = c_sort_col.selectbox("정렬 항목", ["기본(날짜순)"] + show_cols, key=f"s_target_{cat}", label_visibility="collapsed")
    sort_dir = c_sort_order.selectbox("정렬 순서", ["내림차순(최신/큼)", "오름차순(과거/작음)"], key=f"s_dir_{cat}", label_visibility="collapsed")
    
    if c_sort_btn.button("정렬 적용", key=f"s_btn_{cat}", use_container_width=True):
        ascending = (sort_dir == "오름차순(과거/작음)")
        if sort_target == "기본(날짜순)":
            st.session_state[f"df_{cat}"] = df.sort_values(by='tmp_dt', ascending=ascending)
        else:
            st.session_state[f"df_{cat}"] = df.sort_values(by=sort_target, ascending=ascending)
        st.session_state[f"p_num_{cat}"] = 1 # 정렬 시 1페이지로 리셋
        st.rerun()

    # 표시개수 및 다운로드 버튼
    ctrl_l, ctrl_r = st.columns([6, 4])
    with ctrl_r:
        c1, c2, c3 = st.columns([1.5, 1, 1])
        p_limit = c1.selectbox("표시개수", [50, 100, 150, 200], key=f"ps_sel_{cat}", label_visibility="collapsed")
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
        c2.download_button("📑 CSV", df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig'), f"{cat}.csv", "text/csv")
        c3.download_button("📊 Excel", output.getvalue(), f"{cat}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    
    with ctrl_l: st.markdown(f"**✅ 조회결과: {len(df):,}건**")

    # 데이터 표 슬라이싱: 전체 데이터에서 현재 페이지에 해당하는 행만 잘라 보여줍니다.
    total_pages = max((len(df) - 1) // p_limit + 1, 1)
    curr_p = st.session_state.get(f"p_num_{cat}", 1)
    st.dataframe(df[show_cols].iloc[(curr_p-1)*p_limit : curr_p*p_limit], use_container_width=True, height=520)

    # 하단 페이지네이션 숫자 버튼 (총 14개 컬럼: <<, <, 숫자10개, >, >>)
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

# --- [5] 메인 루프 (검색 필터 및 데이터 로딩) ---
tabs = st.tabs(list(SHEET_FILE_IDS.keys()))

for i, tab in enumerate(tabs):
    cat = list(SHEET_FILE_IDS.keys())[i]
    with tab:
        # 세션 상태 초기화: 앱이 처음 켜질 때 기본 날짜(최근 6개월)를 설정합니다.
        if f"sd_{cat}" not in st.session_state: st.session_state[f"sd_{cat}"] = datetime.now().date() - relativedelta(months=6)
        if f"ed_{cat}" not in st.session_state: st.session_state[f"ed_{cat}"] = datetime.now().date()
        if f"ver_{cat}" not in st.session_state: st.session_state[f"ver_{cat}"] = 0 # 날짜 위젯 강제 갱신용
        if f"df_{cat}" not in st.session_state: st.session_state[f"df_{cat}"] = None

        # 검색창 레이아웃 구성
        _, center_area, _ = st.columns([0.1, 9.8, 0.1])
        with center_area:
            st.markdown('<div class="search-container">', unsafe_allow_html=True)
            # 행1: 필드 선택 및 키워드 입력 (AND/OR 조건 포함)
            r1_l, r1_r = st.columns([1, 8.5])
            with r1_l: st.markdown('<div class="search-label">검색조건</div>', unsafe_allow_html=True)
            with r1_r:
                sc1, sc2, sc3, sc4 = st.columns([1.5, 3, 1, 3])
                f_val = sc1.selectbox("필드", ["ALL", "수요기관명", "업체명", "계약명", "세부품명"], key=f"f_{cat}", label_visibility="collapsed")
                k1_val = sc2.text_input("검색어1", key=f"k1_{cat}", label_visibility="collapsed", placeholder="검색어")
                l_val = sc3.selectbox("논리", ["NONE", "AND", "OR"], key=f"l_{cat}", label_visibility="collapsed")
                k2_val = sc4.text_input("검색어2", key=f"k2_{cat}", label_visibility="collapsed", disabled=(l_val=="NONE"), placeholder="검색어2")

            # 행2: 날짜 선택기 및 1개월~2년 퀵 버튼
            r2_l, r2_r = st.columns([1, 8.5])
            with r2_l: st.markdown('<div class="search-label" style="border-bottom:none;">조회기간</div>', unsafe_allow_html=True)
            with r2_r:
                d1, d2, d3, d4 = st.columns([1.3, 1.3, 5.8, 1.2])
                # 시작/종료일 입력 (ver_{cat}을 key에 넣어 버튼 클릭 시 입력창 날짜가 즉시 바뀜)
                sd_in = d1.date_input("시작", value=st.session_state[f"sd_{cat}"], key=f"sd_w_{cat}_{st.session_state[f'ver_{cat}']}", label_visibility="collapsed")
                ed_in = d2.date_input("종료", value=st.session_state[f"ed_{cat}"], key=f"ed_w_{cat}_{st.session_state[f'ver_{cat}']}", label_visibility="collapsed")
                st.session_state[f"sd_{cat}"], st.session_state[f"ed_{cat}"] = sd_in, ed_in

                # 퀵버튼 영역 (간격을 좁게 붙임)
                with d3:
                    st.markdown('<div class="q-btn-container">', unsafe_allow_html=True)
                    q_cols = st.columns(6)
                    def set_period(m=0, y=0):
                        """퀵버튼 클릭 시 시작 날짜를 오늘 기준으로 계산하여 세션을 업데이트합니다."""
                        st.session_state[f"sd_{cat}"] = datetime.now().date() - relativedelta(months=m, years=y)
                        st.session_state[f"ed_{cat}"] = datetime.now().date()
                        st.session_state[f"ver_{cat}"] += 1 # 버전 상승 -> 날짜 위젯 강제 갱신
                        st.rerun()
                    if q_cols[0].button("1개월", key=f"m1_{cat}"): set_period(m=1)
                    if q_cols[1].button("3개월", key=f"m3_{cat}"): set_period(m=3)
                    if q_cols[2].button("6개월", key=f"m6_{cat}"): set_period(m=6)
                    if q_cols[3].button("9개월", key=f"m9_{cat}"): set_period(m=9)
                    if q_cols[4].button("1년", key=f"y1_{cat}"): set_period(y=1)
                    if q_cols[5].button("2년", key=f"y2_{cat}"): set_period(y=2)
                    st.markdown('</div>', unsafe_allow_html=True)

                with d4:
                    # 🔍 검색실행 버튼
                    search_exe = st.button("🔍 검색실행", key=f"exe_{cat}", type="primary", use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        # 실제 데이터 조회 및 필터링 로직
        if search_exe:
            with st.spinner("조회 중..."):
                df_raw = fetch_data(SHEET_FILE_IDS[cat], is_sheet=(cat != '종합쇼핑몰'))
                if not df_raw.empty:
                    s_s, e_s = sd_in.strftime('%Y%m%d'), ed_in.strftime('%Y%m%d')
                    # tmp_dt: 각기 다른 날짜 형식을 'YYYYMMDD'로 통일하여 비교 가능하게 만듭니다.
                    if cat == '나라장터_발주':
                        df_raw['tmp_dt'] = df_raw.iloc[:,4].astype(str) + df_raw.iloc[:,12].astype(str).str.zfill(2) + "01"
                    else:
                        d_col = DATE_COL_MAP.get(cat)
                        df_raw['tmp_dt'] = df_raw[d_col].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8] if d_col in df_raw.columns else "0"
                    
                    # 1. 날짜 범위 필터링
                    df_filtered = df_raw[(df_raw['tmp_dt'] >= s_s[:6]+"01") & (df_raw['tmp_dt'] <= e_s)].copy()
                    
                    # 2. 키워드 필터링 (입력값이 있을 때만 수행)
                    if k1_val and k1_val.strip():
                        def get_m(k): return df_filtered.astype(str).apply(lambda x: x.str.contains(k, case=False, na=False)).any(axis=1) if f_val == "ALL" else df_filtered[f_val].astype(str).str.contains(k, case=False, na=False)
                        if l_val == "AND" and k2_val: df_filtered = df_filtered[get_m(k1_val) & get_m(k2_val)]
                        elif l_val == "OR" and k2_val: df_filtered = df_filtered[get_m(k1_val) | get_m(k2_val)]
                        else: df_filtered = df_filtered[get_m(k1_val)]
                    
                    # 검색 결과를 세션에 저장 (정렬은 기본 최신순)
                    st.session_state[f"df_{cat}"] = df_filtered.sort_values(by='tmp_dt', ascending=False)
                    st.session_state[f"p_num_{cat}"] = 1 # 검색 시 1페이지로 리셋

        # 결과가 있는 경우 표 출력
        if st.session_state[f"df_{cat}"] is not None:
            show_result_table(cat, DISPLAY_INDEX_MAP.get(cat, []))
