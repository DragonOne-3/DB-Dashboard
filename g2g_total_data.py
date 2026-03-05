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

# =================================================================================
# [1] 페이지 설정 및 디자인
# =================================================================================
st.markdown("<br><br>", unsafe_allow_html=True)

st.set_page_config(page_title="공공조달 DATA 통합검색", layout="wide", page_icon="🏛")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap');

/* ══════════════════════════════════════════
   전체 기반 — 흰색 테마
══════════════════════════════════════════ */
html, body, [class*="css"] { font-family: 'Noto Sans KR', sans-serif !important; }
.stApp { background: #f5f7fa !important; color: #1e293b !important; }
.block-container { padding: 1.5rem 2rem 2rem 2rem !important; max-width: 100% !important; }

/* ══════════════════════════════════════════
   헤더
══════════════════════════════════════════ */
.main-header {
    display: flex; align-items: center; gap: 14px;
    padding: 0 0 18px 0;
    border-bottom: 1px solid #e2e8f0;
    margin-bottom: 20px;
}
.header-icon {
    width: 44px; height: 44px;
    background: linear-gradient(135deg, #1d4ed8, #3b82f6);
    border-radius: 12px;
    display: flex; align-items: center; justify-content: center;
    font-size: 21px;
    box-shadow: 0 4px 14px rgba(59,130,246,0.35);
    flex-shrink: 0;
}
.header-title { font-size: 21px; font-weight: 700; color: #0f172a; letter-spacing: -0.4px; margin: 0; }
.header-sub { font-size: 12px; color: #94a3b8; margin: 3px 0 0 0; font-weight: 300; }
.header-divider {
    height: 1px;
    background: linear-gradient(90deg, #3b82f6 0%, #e2e8f0 55%);
    margin: 0 0 22px 0;
}

/* ══════════════════════════════════════════
   탭 — 캡슐 pill 스타일
══════════════════════════════════════════ */
.stTabs [data-baseweb="tab-list"] {
    background: #eef2f7 !important;
    border: 1px solid #dde3ec !important;
    border-radius: 14px !important;
    padding: 5px 6px !important;
    gap: 3px !important;
    margin-bottom: 20px;
    overflow-x: auto;
    flex-wrap: nowrap;
}
.stTabs [data-baseweb="tab-highlight"] { display: none !important; }
.stTabs [data-baseweb="tab-border"]    { display: none !important; }

.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    color: #64748b !important;
    border: none !important;
    border-radius: 10px !important;
    padding: 7px 18px !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    font-family: 'Noto Sans KR', sans-serif !important;
    transition: all 0.18s ease !important;
    white-space: nowrap;
}
.stTabs [data-baseweb="tab"]:hover {
    background: #dce6f5 !important;
    color: #1d4ed8 !important;
}
.stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, #1d4ed8, #3b82f6) !important;
    color: #ffffff !important;
    font-weight: 600 !important;
    box-shadow: 0 2px 10px rgba(29,78,216,0.35), inset 0 1px 0 rgba(255,255,255,0.2) !important;
}
.stTabs [data-baseweb="tab-panel"] { padding-top: 4px !important; }

/* ══════════════════════════════════════════
   검색 패널
══════════════════════════════════════════ */
.search-panel {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 14px;
    padding: 20px 24px 18px 24px;
    margin-bottom: 18px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.06);
}
.search-section-label {
    font-size: 10.5px; font-weight: 600; color: #94a3b8;
    letter-spacing: 1.8px; text-transform: uppercase;
    margin-bottom: 10px;
    display: flex; align-items: center; gap: 8px;
}
.search-section-label::after { content: ''; flex: 1; height: 1px; background: #e2e8f0; }

/* ══════════════════════════════════════════
   인풋 & 셀렉트박스
══════════════════════════════════════════ */
.stTextInput > div > div > input {
    background: #f8fafc !important;
    border: 1px solid #cbd5e1 !important;
    border-radius: 9px !important;
    color: #1e293b !important;
    font-size: 13px !important;
    font-family: 'Noto Sans KR', sans-serif !important;
    padding: 8px 13px !important;
    transition: all 0.2s;
}
.stTextInput > div > div > input:focus {
    border-color: #3b82f6 !important;
    box-shadow: 0 0 0 3px rgba(59,130,246,0.15) !important;
    background: #ffffff !important;
}
.stTextInput > div > div > input::placeholder { color: #94a3b8 !important; }

.stSelectbox > div > div {
    background: #f8fafc !important;
    border: 1px solid #cbd5e1 !important;
    border-radius: 9px !important;
    color: #1e293b !important;
    font-size: 13px !important;
}
[data-baseweb="select"] > div  { background: #f8fafc !important; border-color: #cbd5e1 !important; }
[data-baseweb="popover"]       { background: #ffffff !important; border: 1px solid #e2e8f0 !important; box-shadow: 0 8px 24px rgba(0,0,0,0.12) !important; }
[data-baseweb="menu"]          { background: #ffffff !important; }
[data-baseweb="option"]        { background: #ffffff !important; color: #1e293b !important; font-size: 13px !important; }
[data-baseweb="option"]:hover  { background: #eff6ff !important; color: #1d4ed8 !important; }

.stDateInput > div > div > input {
    background: #f8fafc !important;
    border: 1px solid #cbd5e1 !important;
    border-radius: 9px !important;
    color: #1e293b !important;
    font-size: 13px !important;
}

/* ══════════════════════════════════════════
   일반 버튼 — pill 형태, 밝은 배경
══════════════════════════════════════════ */
.stButton > button {
    background: #ffffff !important;
    border: 1px solid #cbd5e1 !important;
    border-radius: 20px !important;
    color: #475569 !important;
    font-size: 12px !important;
    font-family: 'Noto Sans KR', sans-serif !important;
    font-weight: 500 !important;
    padding: 4px 14px !important;
    height: 30px !important;
    transition: all 0.16s ease !important;
    letter-spacing: 0.1px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
}
.stButton > button:hover {
    background: #eff6ff !important;
    border-color: #3b82f6 !important;
    color: #1d4ed8 !important;
    box-shadow: 0 2px 8px rgba(59,130,246,0.2) !important;
    transform: translateY(-1px) !important;
}
.stButton > button:active { transform: translateY(0) !important; box-shadow: none !important; }

/* ══════════════════════════════════════════
   검색실행 버튼 (primary)
══════════════════════════════════════════ */
button[kind="primary"] {
    background: linear-gradient(135deg, #1d4ed8 0%, #3b82f6 100%) !important;
    border: none !important;
    border-radius: 10px !important;
    color: #ffffff !important;
    font-weight: 700 !important;
    font-size: 13px !important;
    height: 38px !important;
    box-shadow: 0 3px 12px rgba(37,99,235,0.4), inset 0 1px 0 rgba(255,255,255,0.2) !important;
    letter-spacing: 0.3px;
    transition: all 0.18s ease !important;
}
button[kind="primary"]:hover {
    box-shadow: 0 5px 18px rgba(37,99,235,0.5) !important;
    transform: translateY(-2px) !important;
    filter: brightness(1.06) !important;
}
button[kind="primary"]:active { transform: translateY(0) !important; }

/* 페이지네이션 현재 페이지 */
[data-testid="stHorizontalBlock"] button[kind="primary"] {
    background: #1d4ed8 !important;
    border-radius: 20px !important;
    height: 30px !important;
    min-width: 30px !important;
    padding: 0 10px !important;
    font-size: 12px !important;
    font-weight: 700 !important;
    box-shadow: 0 2px 8px rgba(29,78,216,0.4) !important;
}

/* ══════════════════════════════════════════
   다운로드 버튼
══════════════════════════════════════════ */
.stDownloadButton > button {
    background: #f0f7ff !important;
    border: 1px solid #bfdbfe !important;
    border-radius: 9px !important;
    color: #1d4ed8 !important;
    font-size: 12px !important;
    font-weight: 500 !important;
    height: 32px !important;
    transition: all 0.15s !important;
}
.stDownloadButton > button:hover {
    background: #dbeafe !important;
    border-color: #3b82f6 !important;
    color: #1e40af !important;
    box-shadow: 0 2px 8px rgba(59,130,246,0.2) !important;
}

/* ══════════════════════════════════════════
   링크 버튼
══════════════════════════════════════════ */
.stLinkButton > a {
    background: #ffffff !important;
    border: 1px solid #cbd5e1 !important;
    border-radius: 9px !important;
    color: #64748b !important;
    font-size: 12px !important;
    text-decoration: none;
    padding: 6px 14px !important;
    transition: all 0.15s;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
}
.stLinkButton > a:hover {
    border-color: #3b82f6 !important;
    color: #1d4ed8 !important;
    background: #eff6ff !important;
    box-shadow: 0 2px 8px rgba(59,130,246,0.15) !important;
}

/* ══════════════════════════════════════════
   정보바 & 결과 뱃지
══════════════════════════════════════════ */
.info-bar {
    display: flex; align-items: center; gap: 12px;
    padding: 10px 16px;
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    margin-bottom: 12px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.05);
}
.result-badge {
    background: linear-gradient(135deg, #eff6ff, #dbeafe);
    border: 1px solid #93c5fd;
    border-radius: 8px;
    padding: 5px 14px;
    font-size: 13px; font-weight: 700;
    color: #1d4ed8;
    font-family: 'JetBrains Mono', monospace;
    white-space: nowrap;
}

/* ══════════════════════════════════════════
   데이터프레임
══════════════════════════════════════════ */
.stDataFrame { border-radius: 12px !important; overflow: hidden; border: 1px solid #e2e8f0 !important; box-shadow: 0 1px 6px rgba(0,0,0,0.06); }
[data-testid="stDataFrame"] > div { border-radius: 12px !important; }
.stDataFrame thead tr th {
    background: #f8fafc !important; color: #64748b !important;
    font-size: 12px !important; font-weight: 600 !important;
    border-bottom: 1px solid #e2e8f0 !important; letter-spacing: 0.4px;
}
.stDataFrame tbody tr:hover td { background: #f0f7ff !important; }
.stDataFrame tbody tr td { font-size: 13px !important; color: #334155 !important; border-color: #f1f5f9 !important; }

/* ══════════════════════════════════════════
   기타
══════════════════════════════════════════ */
.stTextInput label, .stSelectbox label, .stDateInput label { display: none !important; }
.stSpinner > div { border-top-color: #3b82f6 !important; }
::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: #f1f5f9; }
::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
hr { border-color: #e2e8f0 !important; margin: 12px 0 !important; }

.no-data-msg { text-align: center; padding: 50px; color: #94a3b8; font-size: 14px; }
.no-data-icon { font-size: 38px; margin-bottom: 10px; opacity: 0.4; }
</style>
""", unsafe_allow_html=True)


# =================================================================================
# [2] 구글 드라이브 연결 함수
#     - @st.cache_resource : 서비스 객체는 앱 전체에서 1번만 생성 (재인증 방지)
#     - @st.cache_data     : 데이터는 1시간 캐시 → 검색 버튼마다 재다운로드 방지
# =================================================================================
@st.cache_resource
def get_drive_service():
    auth_json_str = st.secrets["GOOGLE_AUTH_JSON"]
    info = json.loads(auth_json_str)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=[
            'https://www.googleapis.com/auth/drive.readonly',
            'https://www.googleapis.com/auth/spreadsheets.readonly'
        ]
    )
    return build('drive', 'v3', credentials=creds), creds


@st.cache_data(ttl=3600)  # ✅ 핵심 수정: 1시간 캐시로 매번 다운로드 방지
def fetch_data(file_id, is_sheet=True):
    drive_service, credentials = get_drive_service()
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    headers = {'Authorization': f'Bearer {credentials.token}'}

    if is_sheet:
        url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv"
        return pd.read_csv(
            io.BytesIO(requests.get(url, headers=headers).content),
            low_memory=False
        )
    else:
        results = drive_service.files().list(
            q=f"'{file_id}' in parents and trashed = false"
        ).execute()
        dfs = [
            pd.read_csv(
                io.BytesIO(requests.get(
                    f"https://www.googleapis.com/drive/v3/files/{f['id']}?alt=media",
                    headers=headers
                ).content),
                low_memory=False
            )
            for f in results.get('files', [])
        ]
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# =================================================================================
# [3] 매핑 데이터: 시트 ID, 출력 컬럼, 날짜 컬럼 지정
# =================================================================================
SHEET_FILE_IDS = {
    '나라장터_발주':  '1pGnb6O5Z1ahaHYuQdydyoY1Ayf147IoGmLRdA3WAHi4',
    '나라장터_계약':  '15Hsr_nup4ZteIZ4Jyov8wG2s_rKoZ25muqRE3-sRnaw',
    '군수품_발주':    '1pzW51Z29SSoQk7al_GvN_tj5smuhOR3J2HWnL_16fcI',
    '군수품_계약':    '1KPMUz0IKM6AQvqwfAkvW96WNvzbycN56vNlFnDmfRTw',
    '군수품_공고':    '1opuA_UzNm27U9QkbMay5UsyQqcwfxiEmIHNRdc4MyHM',
    '군수품_수의':    '1aYA18kPrSkpbayzbn16EdKUScVRwr2Nutyid5No5qjk',
    '종합쇼핑몰':    '1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr'
}

DISPLAY_INDEX_MAP = {
    '군수품_계약':  [7, 5, 3, 1, 12],
    '군수품_수의':  [12, 10, 8, 3],
    '군수품_발주':  [7, 8, 12, 2, 3],
    '군수품_공고':  [0, 16, 18, 19, 23],
    '나라장터_발주': [9, 13, 20],
    '나라장터_계약': [0, 3, 4, 5, 6, 33],
    '종합쇼핑몰':   ["수요기관명", "계약납품요구일자", "세부품명", "계약명", "업체명", "수량", "금액"]
}

DATE_COL_MAP = {
    '군수품_발주':  '발주예정월',
    '군수품_수의':  '개찰일자',
    '군수품_계약':  '계약일자',
    '군수품_공고':  '공고일자',
    '나라장터_계약': '★가공_계약일',
    '종합쇼핑몰':   '계약납품요구일자'
}

TAB_ICONS = {
    '나라장터_발주': '📋',
    '나라장터_계약': '📝',
    '군수품_발주':   '🛡',
    '군수품_계약':   '✅',
    '군수품_공고':   '📢',
    '군수품_수의':   '🤝',
    '종합쇼핑몰':   '🛒'
}


# =================================================================================
# [4] 헤더
# =================================================================================
col_title, col_link = st.columns([5, 1])
with col_title:
    st.markdown("""
    <div class="main-header">
        <div class="header-icon">🏛</div>
        <div>
            <p class="header-title">공공조달 DATA 통합검색 시스템</p>
            <p class="header-sub"><b>나라장터 · 군수품 · 종합쇼핑몰 통합 데이터 조회 플랫폼</p>
        </div>
    </div>
    """, unsafe_allow_html=True)
with col_link:
    st.markdown("<br>", unsafe_allow_html=True)
    st.link_button("⛓️ 지자체 유지보수 내역", "https://g2b-info.streamlit.app/", use_container_width=True)

st.markdown('<div class="header-divider"></div>', unsafe_allow_html=True)


# =================================================================================
# [5] 결과 테이블 Fragment (역할: 데이터 출력, 정렬, 페이지네이션)
# =================================================================================
@st.fragment
def show_result_table(cat, idx_list):
    df = st.session_state.get(f"df_{cat}")
    if df is None:
        return

    # 표시할 컬럼 추출
    show_cols = [
        df.columns[idx] if isinstance(idx, int) else idx
        for idx in idx_list
        if (isinstance(idx, int) and idx < len(df.columns))
        or (isinstance(idx, str) and idx in df.columns)
    ]

    # ── 정보바 ──
    st.markdown('<div class="info-bar">', unsafe_allow_html=True)
    c_result, c_sort1, c_sort2, c_sortbtn, c_limit, c_dl = st.columns([1.2, 2.0, 1.6, 0.7, 1.1, 2.2])

    c_result.markdown(f'<div class="result-badge">✅ {len(df):,} 건</div>', unsafe_allow_html=True)

    sort_target = c_sort1.selectbox(
        "정렬기준", ["날짜순"] + show_cols,
        key=f"st_{cat}", label_visibility="collapsed"
    )
    sort_dir = c_sort2.selectbox(
        "순서", ["내림차순", "오름차순"],
        key=f"sd_sort_{cat}", label_visibility="collapsed"
    )

    if c_sortbtn.button("↕", key=f"sb_{cat}", use_container_width=True):
        ascending = (sort_dir == "오름차순")
        sort_key = 'tmp_dt' if sort_target == "날짜순" else sort_target
        st.session_state[f"df_{cat}"] = df.sort_values(by=sort_key, ascending=ascending)
        st.rerun()

    p_limit = c_limit.selectbox(
        "개수", [50, 100, 150, 200],
        key=f"ps_{cat}", label_visibility="collapsed"
    )

    with c_dl:
        d_csv_col, d_xl_col = st.columns(2)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
        d_csv_col.download_button(
            "📑 CSV", df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig'),
            f"{cat}.csv", "text/csv", use_container_width=True
        )
        d_xl_col.download_button(
            "📊 Excel", output.getvalue(),
            f"{cat}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    st.markdown('</div>', unsafe_allow_html=True)

    # ── 테이블 ──
    total_pages = max((len(df) - 1) // p_limit + 1, 1)
    curr_p = st.session_state.get(f"p_num_{cat}", 1)

    link_col_config = {}
    if "계약상세정보URL" in show_cols:
        link_col_config["계약상세정보URL"] = st.column_config.LinkColumn("계약상세정보URL", display_text="바로가기")

    st.dataframe(
        df[show_cols].iloc[(curr_p - 1) * p_limit: curr_p * p_limit],
        use_container_width=True,
        height=500,
        column_config=link_col_config
    )

    # ── 페이지네이션 ──
    st.markdown('<div class="pagination-area">', unsafe_allow_html=True)
    pg_cols = st.columns([1, 10, 1])
    with pg_cols[1]:
        btn_cols = st.columns(14)
        start_p = max(1, curr_p - 4)
        end_p = min(total_pages, start_p + 9)

        if btn_cols[0].button("«", key=f"f10_{cat}"):
            st.session_state[f"p_num_{cat}"] = max(1, curr_p - 10); st.rerun()
        if btn_cols[1].button("‹", key=f"f1_{cat}"):
            st.session_state[f"p_num_{cat}"] = max(1, curr_p - 1); st.rerun()

        for i, p in enumerate(range(start_p, end_p + 1)):
            btn_type = "primary" if p == curr_p else "secondary"
            if btn_cols[i + 2].button(str(p), key=f"pg_{cat}_{p}", type=btn_type):
                st.session_state[f"p_num_{cat}"] = p; st.rerun()

        if btn_cols[12].button("›", key=f"n1_{cat}"):
            st.session_state[f"p_num_{cat}"] = min(total_pages, curr_p + 1); st.rerun()
        if btn_cols[13].button("»", key=f"n10_{cat}"):
            st.session_state[f"p_num_{cat}"] = min(total_pages, curr_p + 10); st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)


# =================================================================================
# [6] 메인 루프 (탭별 검색 조건 입력 및 데이터 필터링)
# =================================================================================
tab_labels = [f"{TAB_ICONS.get(k, '')} {k}" for k in SHEET_FILE_IDS.keys()]
tabs = st.tabs(tab_labels)

for i, tab in enumerate(tabs):
    cat = list(SHEET_FILE_IDS.keys())[i]

    with tab:
        today = date.today()

        # 세션 초기화
        if f"sd_{cat}" not in st.session_state or not isinstance(st.session_state[f"sd_{cat}"], date):
            st.session_state[f"sd_{cat}"] = today - relativedelta(months=6)
        if f"ed_{cat}" not in st.session_state or not isinstance(st.session_state[f"ed_{cat}"], date):
            st.session_state[f"ed_{cat}"] = today - relativedelta(days=1)
        if f"ver_{cat}" not in st.session_state:
            st.session_state[f"ver_{cat}"] = 0
        if f"df_{cat}" not in st.session_state:
            st.session_state[f"df_{cat}"] = None

        # ── 검색 패널 ──
        st.markdown('<div class="search-panel">', unsafe_allow_html=True)

        # 검색조건 행
        st.markdown('<div class="search-section-label">🔍 검색 조건</div>', unsafe_allow_html=True)
        sc1, sc2, sc3, sc4 = st.columns([1.4, 3.2, 1.0, 3.2])
        f_val  = sc1.selectbox("필드", ["ALL", "수요기관명", "업체명", "계약명", "세부품명"], key=f"f_{cat}", label_visibility="collapsed")
        k1_val = sc2.text_input("검색어1", key=f"k1_{cat}", label_visibility="collapsed", placeholder="🔎  검색어를 입력하세요")
        l_val  = sc3.selectbox("논리", ["NONE", "AND", "OR"], key=f"l_{cat}", label_visibility="collapsed")
        k2_val = sc4.text_input("검색어2", key=f"k2_{cat}", label_visibility="collapsed", disabled=(l_val == "NONE"), placeholder="🔎  검색어2 (AND/OR 선택 시)")

        st.markdown("<div style='margin-top:12px;'>", unsafe_allow_html=True)

        # 조회기간 행
        st.markdown('<div class="search-section-label">📅 조회 기간</div>', unsafe_allow_html=True)
        d1, d2, d3, d4 = st.columns([1.3, 1.3, 5.2, 1.5])

        v_num   = st.session_state[f"ver_{cat}"]
        s_val   = st.session_state[f"sd_{cat}"]
        e_val   = st.session_state[f"ed_{cat}"]
        if isinstance(s_val, datetime): s_val = s_val.date()
        if isinstance(e_val, datetime): e_val = e_val.date()

        sd_in = d1.date_input("시작", value=s_val if isinstance(s_val, date) else None,
                               key=f"sd_w_{cat}_{v_num}", label_visibility="collapsed")
        ed_in = d2.date_input("종료", value=e_val if isinstance(e_val, date) else None,
                               key=f"ed_w_{cat}_{v_num}", label_visibility="collapsed")

        # 퀵버튼
        with d3:
            def set_period(cat=cat, d=0, m=0, y=0):
                st.session_state[f"sd_{cat}"] = st.session_state[f"ed_{cat}"] - relativedelta(days=d, months=m, years=y)
                st.session_state[f"ver_{cat}"] += 1
                st.rerun()

            q_cols = st.columns(8)
            if q_cols[0].button("어제",   key=f"d1_{cat}"):
                st.session_state[f"ed_{cat}"] = today - relativedelta(days=1)
                st.session_state[f"sd_{cat}"] = today - relativedelta(days=1)
                st.session_state[f"ver_{cat}"] += 1; st.rerun()
            if q_cols[1].button("1주",    key=f"d7_{cat}"): set_period(d=7)
            if q_cols[2].button("1개월",  key=f"m1_{cat}"): set_period(m=1)
            if q_cols[3].button("3개월",  key=f"m3_{cat}"): set_period(m=3)
            if q_cols[4].button("6개월",  key=f"m6_{cat}"): set_period(m=6)
            if q_cols[5].button("9개월",  key=f"m9_{cat}"): set_period(m=9)
            if q_cols[6].button("1년",    key=f"y1_{cat}"): set_period(y=1)
            if q_cols[7].button("2년",    key=f"y2_{cat}"): set_period(y=2)

        with d4:
            search_exe = st.button("🔍  검색실행", key=f"exe_{cat}", type="primary", use_container_width=True)

        st.markdown("</div></div>", unsafe_allow_html=True)

        # ── 검색 실행 로직 ──
        if search_exe:
            with st.spinner("데이터를 불러오는 중..."):
                st.session_state[f"sd_{cat}"] = sd_in
                st.session_state[f"ed_{cat}"] = ed_in

                df_raw = fetch_data(SHEET_FILE_IDS[cat], is_sheet=(cat != '종합쇼핑몰'))

                if not df_raw.empty:
                    s_s = sd_in.strftime('%Y%m%d')
                    e_s = ed_in.strftime('%Y%m%d')
                    d_col = DATE_COL_MAP.get(cat)

                    # 날짜 컬럼 전처리
                    if cat == '나라장터_발주':
                        df_raw['tmp_dt'] = s_s
                    elif cat == '군수품_발주':
                        df_raw['tmp_dt'] = df_raw[d_col].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:6] + "01"
                        s_s = s_s[:6] + "01"
                    elif cat == '나라장터_계약':
                        df_raw['tmp_dt'] = df_raw[d_col].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8]
                    else:
                        df_raw['tmp_dt'] = (
                            df_raw[d_col].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8]
                            if d_col and d_col in df_raw.columns else "0"
                        )

                    # 날짜 필터
                    df_filtered = df_raw.copy() if cat == '나라장터_발주' else \
                        df_raw[(df_raw['tmp_dt'] >= s_s) & (df_raw['tmp_dt'] <= e_s)].copy()

                    # 키워드 필터
                    if k1_val and k1_val.strip():
                        def get_mask(k, df=df_filtered):
                            target_col = f_val
                            if f_val == "업체명":
                                for cand in ["업체명", "상호", "상호명", "계약상대자", "계약상대자명"]:
                                    if cand in df.columns: target_col = cand; break
                            elif f_val == "수요기관명":
                                for cand in ["수요기관명", "수요기관", "발주기관", "공고기관"]:
                                    if cand in df.columns: target_col = cand; break

                            if target_col in df.columns:
                                return df[target_col].astype(str).str.contains(k, case=False, na=False)
                            else:
                                return df.astype(str).apply(lambda x: x.str.contains(k, case=False, na=False)).any(axis=1)

                        if l_val == "AND" and k2_val:
                            df_filtered = df_filtered[get_mask(k1_val) & get_mask(k2_val)]
                        elif l_val == "OR" and k2_val:
                            df_filtered = df_filtered[get_mask(k1_val) | get_mask(k2_val)]
                        else:
                            df_filtered = df_filtered[get_mask(k1_val)]

                    st.session_state[f"df_{cat}"] = df_filtered.sort_values(by='tmp_dt', ascending=False)
                    st.session_state[f"p_num_{cat}"] = 1
                else:
                    st.session_state[f"df_{cat}"] = pd.DataFrame()

        # ── 결과 출력 ──
        if st.session_state[f"df_{cat}"] is not None:
            if len(st.session_state[f"df_{cat}"]) == 0:
                st.markdown("""
                <div class="no-data-msg">
                    <div class="no-data-icon">🔍</div>
                    검색 결과가 없습니다. 검색 조건을 변경해보세요.
                </div>
                """, unsafe_allow_html=True)
            else:
                show_result_table(cat, DISPLAY_INDEX_MAP.get(cat, []))
        else:
            st.markdown("""
            <div class="no-data-msg">
                <div class="no-data-icon">📂</div>
                검색 조건을 입력하고 <strong>검색실행</strong> 버튼을 눌러주세요.
            </div>
            """, unsafe_allow_html=True)
