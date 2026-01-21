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

# --- [1] 페이지 기본 설정 및 제목 짤림 방지 여백 조정 ---
# layout="wide"는 화면을 넓게 사용하게 합니다.
st.set_page_config(page_title="공공조달 DATA 통합검색 시스템", layout="wide")

st.markdown("""
    <style>
    /* block-container의 padding-top을 3rem으로 높여 제목이 상단바에 짤리지 않게 함 */
    .block-container { padding-top: 3rem !important; padding-bottom: 0rem !important; }
    .main { background-color: #f4f4f4; font-size: 13px !important; }
    
    /* 제목 폰트 크기 및 여백 조정 (24pt) */
    .title-text { font-size: 24px !important; font-weight: bold; color: #333; margin-bottom: 5px; }
    
    /* 검색 패널 테두리 및 배경색 */
    .search-container { background-color: white; border: 1px solid #ccc; margin-bottom: 10px; }
    
    /* 검색 항목 왼쪽 회색 라벨 영역 */
    .search-label { background-color: #f9f9f9; width: 120px; padding: 8px; font-weight: bold; border-right: 1px solid #eee; text-align: center; }
    
    /* 탭 메뉴 디자인 (초록색 강조) */
    .stTabs [aria-selected="true"] { background-color: #00b050 !important; color: white !important; }
    
    /* 데이터 표 폰트 크기 조정 */
    .stDataFrame { font-size: 12px !important; }
    
    /* 페이지네이션 버튼 중앙 정렬용 */
    .page-btn-container { display: flex; justify-content: center; align-items: center; gap: 20px; margin-top: 10px; }
    </style>
    """, unsafe_allow_html=True)

# --- [2] 구글 인증 및 데이터 연결 함수 (수정 금지 영역) ---
@st.cache_resource
def get_drive_service():
    # Secrets에 저장된 GOOGLE_AUTH_JSON을 읽어 인증함
    auth_json_str = st.secrets["GOOGLE_AUTH_JSON"]
    info = json.loads(auth_json_str)
    creds = service_account.Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/drive.readonly', 'https://www.googleapis.com/auth/spreadsheets.readonly'])
    return build('drive', 'v3', credentials=creds), creds

drive_service, credentials = get_drive_service()

def fetch_data(file_id, is_sheet=True):
    # 구글 시트 또는 드라이브 폴더에서 데이터를 가져오는 로직
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

# --- [3] 데이터 소스 및 날짜 컬럼 매핑 (추가/변경 가능) ---
SHEET_FILE_IDS = {'나라장터_발주': '1pGnb6O5Z1ahaHYuQdydyoY1Ayf147IoGmLRdA3WAHi4', '나라장터_계약': '15Hsr_nup4ZteIZ4Jyov8wG2s_rKoZ25muqRE3-sRnaw', '군수품_발주': '1pzW51Z29SSoQk7al_GvN_tj5smuhOR3J2HWnL_16fcI', '군수품_계약': '1KPMUz0IKM6AQvqwfAkvW96WNvzbycN56vNlFnDmfRTw', '군수품_공고': '1opuA_UzNm27U9QkbMay5UsyQqcwfxiEmIHNRdc4MyHM', '군수품_수의': '1aYA18kPrSkpbayzbn16EdKUScVRwr2Nutyid5No5qjk', '종합쇼핑몰': '1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr'}
DISPLAY_INDEX_MAP = {'군수품_계약': [7, 5, 3, 1, 12], '군수품_수의': [12, 10, 8, 3], '군수품_발주': [7, 8, 12, 2, 3], '군수품_공고': [0, 17, 15, 22], '나라장터_발주': [9, 13, 20], '나라장터_계약': [0, 3, 4, 5, 6], '종합쇼핑몰': ["수요기관명", "계약납품요구일자", "세부품명", "계약명", "업체명", "수량", "금액"]}
DATE_COL_MAP = {'군수품_발주': '발주예정월', '군수품_수의': '개찰일자', '군수품_계약': '계약일자', '군수품_공고': '공고일자', '나라장터_계약': '★가공_계약일', '종합쇼핑몰': '계약납품요구일자'}

# --- [4] 상단 제목 및 우측 바로가기 버튼 ---
h1, h2 = st.columns([3, 1])
with h1: st.markdown('<p class="title-text">🏛 공공조달 DATA 통합검색 시스템</p>', unsafe_allow_html=True)
with h2: st.link_button("⛓️ 지자체 유지보수 내역", "https://g2b-info.streamlit.app/", use_container_width=True)
st.markdown("<hr style='margin: 0px 0px 10px 0px; border-top: 2px solid #333;'>", unsafe_allow_html=True)

# --- [5] 메인 탭 구성 및 검색 로직 ---
tabs = st.tabs(list(SHEET_FILE_IDS.keys()))

for i, tab in enumerate(tabs):
    cat = list(SHEET_FILE_IDS.keys())[i]
    with tab:
        # 각 탭별로 상태(날짜, 결과, 페이지)를 독립적으로 저장함
        if f"sd_{cat}" not in st.session_state: st.session_state[f"sd_{cat}"] = datetime(2025, 1, 1).date()
        if f"ed_{cat}" not in st.session_state: st.session_state[f"ed_{cat}"] = datetime.now().date()
        if f"result_{cat}" not in st.session_state: st.session_state[f"result_{cat}"] = None
        if f"page_{cat}" not in st.session_state: st.session_state[f"page_{cat}"] = 1

        # [디자인] 검색창 중앙 집중 정렬 (양옆 여백 1:8:1)
        _, center_area, _ = st.columns([1, 8, 1])
        
        with center_area:
            st.markdown('<div class="search-container">', unsafe_allow_html=True)
            # 행1: 필드 및 키워드 검색 조건
            r1_l, r1_r = st.columns([1, 8.5])
            with r1_l: st.markdown('<div class="search-label">검색조건</div>', unsafe_allow_html=True)
            with r1_r:
                sc1, sc2, sc3, sc4 = st.columns([1.5, 3, 1, 3])
                f_val = sc1.selectbox("필드", ["ALL", "수요기관명", "업체명", "계약명", "세부품명"], key=f"f_{cat}", label_visibility="collapsed")
                k1_val = sc2.text_input("검색어1", key=f"k1_{cat}", label_visibility="collapsed")
                l_val = sc3.selectbox("논리", ["NONE", "AND", "OR"], key=f"l_{cat}", label_visibility="collapsed")
                k2_val = sc4.text_input("검색어2", key=f"k2_{cat}", label_visibility="collapsed", disabled=(l_val=="NONE"))

            # 행2: 날짜 선택 및 퀵 버튼 (버튼 로직 수정 완료)
            r2_l, r2_r = st.columns([1, 8.5])
            with r2_l: st.markdown('<div class="search-label" style="border-bottom:none;">조회기간</div>', unsafe_allow_html=True)
            with r2_r:
                d1, d2, d3, d4 = st.columns([1.5, 1.5, 5, 1.5])
                # 버튼 클릭 시 session_state를 직접 바꿔서 즉시 반영되게 함
                sd_val = d1.date_input("시작", value=st.session_state[f"sd_{cat}"], key=f"sd_input_{cat}", label_visibility="collapsed")
                ed_val = d2.date_input("종료", value=st.session_state[f"ed_{cat}"], key=f"ed_input_{cat}", label_visibility="collapsed")
                
                q_cols = d3.columns(6)
                # 날짜 버튼을 누르면 시작일이 종료일 기준으로 계산되어 session_state에 저장됨
                if q_cols[0].button("1개월", key=f"m1_{cat}"): st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] - relativedelta(months=1); st.rerun()
                if q_cols[1].button("3개월", key=f"m3_{cat}"): st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] - relativedelta(months=3); st.rerun()
                if q_cols[2].button("6개월", key=f"m6_{cat}"): st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] - relativedelta(months=6); st.rerun()
                if q_cols[3].button("9개월", key=f"m9_{cat}"): st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] - relativedelta(months=9); st.rerun()
                if q_cols[4].button("1년", key=f"y1_{cat}"): st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] - relativedelta(years=1); st.rerun()
                if q_cols[5].button("2년", key=f"y2_{cat}"): st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] - relativedelta(years=2); st.rerun()
                
                # [검색 실행] 버튼 클릭 시 데이터 호출
                search_exe = d4.button("🔍 검색실행", key=f"exe_{cat}", type="primary", use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        # --- [6] 데이터 필터링 실행부 ---
        if search_exe:
            # 날짜 값을 세션에 동기화
            st.session_state[f"sd_{cat}"] = sd_val
            st.session_state[f"ed_{cat}"] = ed_val
            with st.spinner("데이터 로딩 중..."):
                df_raw = fetch_data(SHEET_FILE_IDS[cat], is_sheet=(cat != '종합쇼핑몰'))
                if not df_raw.empty:
                    s_s, e_s = sd_val.strftime('%Y%m%d'), ed_val.strftime('%Y%m%d')
                    # 나라장터 발주건은 년/월 컬럼 결합 처리
                    if cat == '나라장터_발주':
                        df_raw['tmp_dt'] = df_raw.iloc[:,4].astype(str) + df_raw.iloc[:,12].astype(str).str.zfill(2) + "01"
                    else:
                        d_col = DATE_COL_MAP.get(cat)
                        df_raw['tmp_dt'] = df_raw[d_col].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8] if d_col in df_raw.columns else "0"
                    
                    df_filtered = df_raw[(df_raw['tmp_dt'] >= s_s[:6]+"01") & (df_raw['tmp_dt'] <= e_s)]
                    # 키워드 필터링 (AND/OR 조건 포함)
                    if k1_val:
                        def get_m(k): return df_filtered.astype(str).apply(lambda x: x.str.contains(k, case=False, na=False)).any(axis=1) if f_val == "ALL" else df_filtered[f_val].astype(str).str.contains(k, case=False, na=False)
                        if l_val == "AND" and k2_val: df_filtered = df_filtered[get_m(k1_val) & get_m(k2_val)]
                        elif l_val == "OR" and k2_val: df_filtered = df_filtered[get_m(k1_val) | get_m(k2_val)]
                        else: df_filtered = df_filtered[get_m(k1_val)]
                    
                    st.session_state[f"result_{cat}"] = df_filtered
                    st.session_state[f"page_{cat}"] = 1 # 검색 시 1페이지로 강제 이동

        # --- [7] 결과 출력 및 페이지네이션 (페이지 넘기기 기능) ---
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

            # 현재 페이지에 해당하는 데이터만 잘라서 보여줌
            total_rows = len(res_df)
            total_pages = max((total_rows - 1) // p_limit + 1, 1)
            curr_p = st.session_state[f"page_{cat}"]
            start_idx = (curr_p - 1) * p_limit
            
            idx_list = DISPLAY_INDEX_MAP.get(cat, [])
            show_cols = [res_df.columns[idx] if isinstance(idx, int) else idx for idx in idx_list if (isinstance(idx, int) and idx < len(res_df.columns)) or (isinstance(idx, str) and idx in res_df.columns)]
            st.dataframe(res_df[show_cols].iloc[start_idx : start_idx + p_limit], use_container_width=True, height=550)

            # [페이지 컨트롤러] 하단에 이전/다음 버튼 생성
            st.markdown('<div class="page-btn-container">', unsafe_allow_html=True)
            p_col1, p_col2, p_col3, p_col4, p_col5 = st.columns([4, 1, 2, 1, 4])
            if p_col2.button("이전", key=f"prev_{cat}", disabled=(curr_p <= 1)):
                st.session_state[f"page_{cat}"] -= 1
                st.rerun()
            p_col3.markdown(f"<p style='text-align:center;'><b>{curr_p} / {total_pages} 페이지</b></p>", unsafe_allow_html=True)
            if p_col4.button("다음", key=f"next_{cat}", disabled=(curr_p >= total_pages)):
                st.session_state[f"page_{cat}"] += 1
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)
