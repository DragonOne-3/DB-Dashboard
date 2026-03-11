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


st.set_page_config(page_title="공공조달 DATA 통합검색", layout="wide", page_icon="🏛")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
html, body, [class*="css"] { font-family: 'Noto Sans KR', sans-serif !important; }
.stApp { background: #f5f7fa !important; color: #1e293b !important; }
.block-container { padding: 1.5rem 2rem 2rem 2rem !important; max-width: 100% !important; }
.main-header { display: flex; align-items: center; gap: 14px; padding: 0 0 18px 0; border-bottom: 1px solid #e2e8f0; margin-bottom: 20px; }
.header-icon { width: 44px; height: 44px; background: linear-gradient(135deg, #1d4ed8, #3b82f6); border-radius: 12px; display: flex; align-items: center; justify-content: center; font-size: 21px; box-shadow: 0 4px 14px rgba(59,130,246,0.35); flex-shrink: 0; }
.header-title { font-size: 21px; font-weight: 700; color: #0f172a; letter-spacing: -0.4px; margin: 0; }
.header-sub   { font-size: 12px; color: #94a3b8; margin: 3px 0 0 0; font-weight: 300; }
.header-divider { height: 1px; background: linear-gradient(90deg, #3b82f6 0%, #e2e8f0 55%); margin: 0 0 22px 0; }
.stTabs [data-baseweb="tab-list"] { background: #eef2f7 !important; border: 1px solid #dde3ec !important; border-radius: 14px !important; padding: 5px 6px !important; gap: 3px !important; margin-bottom: 20px; overflow-x: auto; flex-wrap: nowrap; }
.stTabs [data-baseweb="tab-highlight"] { display: none !important; }
.stTabs [data-baseweb="tab-border"]    { display: none !important; }
.stTabs [data-baseweb="tab"] { background: transparent !important; color: #64748b !important; border: none !important; border-radius: 10px !important; padding: 7px 18px !important; font-size: 13px !important; font-weight: 500 !important; font-family: 'Noto Sans KR', sans-serif !important; transition: all 0.18s ease !important; white-space: nowrap; }
.stTabs [data-baseweb="tab"]:hover { background: #dce6f5 !important; color: #1d4ed8 !important; }
.stTabs [aria-selected="true"] { background: linear-gradient(135deg, #1d4ed8, #3b82f6) !important; color: #ffffff !important; font-weight: 600 !important; box-shadow: 0 2px 10px rgba(29,78,216,0.35), inset 0 1px 0 rgba(255,255,255,0.2) !important; }
.stTabs [data-baseweb="tab-panel"] { padding-top: 4px !important; }
.search-panel { background: #ffffff; border: 1px solid #e2e8f0; border-radius: 14px; padding: 20px 24px 18px 24px; margin-bottom: 18px; box-shadow: 0 2px 12px rgba(0,0,0,0.06); }
.search-section-label { font-size: 14px; font-weight: 600; color: #94a3b8; letter-spacing: 1.8px; text-transform: uppercase; margin-bottom: 10px; display: flex; align-items: center; gap: 8px; }
.search-section-label::after { content: ''; flex: 1; height: 1px; background: #e2e8f0; }
.stTextInput > div > div > input { background: #f8fafc !important; border: 1px solid #cbd5e1 !important; border-radius: 9px !important; color: #1e293b !important; font-size: 13px !important; font-family: 'Noto Sans KR', sans-serif !important; padding: 8px 13px !important; }
.stTextInput > div > div > input:focus { border-color: #3b82f6 !important; box-shadow: 0 0 0 3px rgba(59,130,246,0.15) !important; background: #ffffff !important; }
.stTextInput > div > div > input::placeholder { color: #94a3b8 !important; }
.stSelectbox > div > div { background: #f8fafc !important; border: 1px solid #cbd5e1 !important; border-radius: 9px !important; color: #1e293b !important; font-size: 13px !important; }
[data-baseweb="select"] > div { background: #f8fafc !important; border-color: #cbd5e1 !important; }
[data-baseweb="popover"] { background: #ffffff !important; border: 1px solid #e2e8f0 !important; box-shadow: 0 8px 24px rgba(0,0,0,0.12) !important; }
[data-baseweb="menu"] { background: #ffffff !important; }
[data-baseweb="option"] { background: #ffffff !important; color: #1e293b !important; font-size: 13px !important; }
[data-baseweb="option"]:hover { background: #eff6ff !important; color: #1d4ed8 !important; }
.stButton > button { background: #ffffff !important; border: 1px solid #cbd5e1 !important; border-radius: 20px !important; color: #475569 !important; font-size: 12px !important; font-family: 'Noto Sans KR', sans-serif !important; font-weight: 500 !important; padding: 4px 14px !important; height: 30px !important; transition: all 0.16s ease !important; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
.stButton > button:hover { background: #eff6ff !important; border-color: #3b82f6 !important; color: #1d4ed8 !important; box-shadow: 0 2px 8px rgba(59,130,246,0.2) !important; transform: translateY(-1px) !important; }
.stButton > button:active { transform: translateY(0) !important; box-shadow: none !important; }
button[kind="primary"] { background: linear-gradient(135deg, #1d4ed8 0%, #3b82f6 100%) !important; border: none !important; border-radius: 10px !important; color: #ffffff !important; font-weight: 700 !important; font-size: 13px !important; height: 38px !important; box-shadow: 0 3px 12px rgba(37,99,235,0.4) !important; transition: all 0.18s ease !important; }
button[kind="primary"]:hover { box-shadow: 0 5px 18px rgba(37,99,235,0.5) !important; transform: translateY(-2px) !important; }
[data-testid="stHorizontalBlock"] button[kind="primary"] { background: #1d4ed8 !important; border-radius: 20px !important; height: 30px !important; min-width: 30px !important; padding: 0 10px !important; font-size: 12px !important; font-weight: 700 !important; }
.stDownloadButton > button { background: #f0f7ff !important; border: 1px solid #bfdbfe !important; border-radius: 9px !important; color: #1d4ed8 !important; font-size: 12px !important; font-weight: 500 !important; height: 32px !important; }
.stDownloadButton > button:hover { background: #dbeafe !important; border-color: #3b82f6 !important; }
.stLinkButton > a { background: #ffffff !important; border: 1px solid #cbd5e1 !important; border-radius: 9px !important; color: #64748b !important; font-size: 12px !important; text-decoration: none; padding: 6px 14px !important; }
.stLinkButton > a:hover { border-color: #3b82f6 !important; color: #1d4ed8 !important; background: #eff6ff !important; }
.info-bar { display: flex; align-items: center; gap: 12px; padding: 10px 16px; background: #ffffff; border: 1px solid #e2e8f0; border-radius: 12px; margin-bottom: 12px; }
.result-badge { background: linear-gradient(135deg, #eff6ff, #dbeafe); border: 1px solid #93c5fd; border-radius: 8px; padding: 5px 14px; font-size: 13px; font-weight: 700; color: #1d4ed8; font-family: 'JetBrains Mono', monospace; white-space: nowrap; }
.stDataFrame { border-radius: 12px !important; overflow: hidden; border: 1px solid #e2e8f0 !important; }
.stDataFrame thead tr th { background: #f8fafc !important; color: #64748b !important; font-size: 12px !important; font-weight: 600 !important; border-bottom: 1px solid #e2e8f0 !important; }
.stDataFrame tbody tr:hover td { background: #f0f7ff !important; }
.stDataFrame tbody tr td { font-size: 13px !important; color: #334155 !important; border-color: #f1f5f9 !important; }
.stTextInput label, .stSelectbox label { display: none !important; }
.stSpinner > div { border-top-color: #3b82f6 !important; }
::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 3px; }
.no-data-msg { text-align: center; padding: 50px; color: #94a3b8; font-size: 14px; }
.no-data-icon { font-size: 38px; margin-bottom: 10px; opacity: 0.4; }
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════
# 공유 캐시: 서비스 계정 인증 (1회)
# ═══════════════════════════════════════════════════════════
@st.cache_resource
def get_drive_service():
    info  = json.loads(st.secrets["GOOGLE_AUTH_JSON"])
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=[
            'https://www.googleapis.com/auth/drive.readonly',
            'https://www.googleapis.com/auth/spreadsheets.readonly'
        ]
    )
    return build('drive', 'v3', credentials=creds), creds


# ═══════════════════════════════════════════════════════════
# dtype 최적화 (메모리 절약)
# ═══════════════════════════════════════════════════════════
def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == 'object':
            if df[col].nunique() / max(len(df), 1) < 0.5:
                df[col] = df[col].astype('category')
        elif df[col].dtype == 'float64':
            df[col] = pd.to_numeric(df[col], downcast='float')
        elif df[col].dtype == 'int64':
            df[col] = pd.to_numeric(df[col], downcast='integer')
    return df


# ═══════════════════════════════════════════════════════════
# 공유 캐시: 전체 원본 데이터 (모든 유저 공유, 복사 없음)
# ═══════════════════════════════════════════════════════════
@st.cache_resource(ttl=3600)
def fetch_data_shared(file_id, is_sheet=True):
    svc, creds = get_drive_service()
    creds.refresh(google.auth.transport.requests.Request())
    hdrs = {'Authorization': f'Bearer {creds.token}'}

    if is_sheet:
        url     = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv"
        content = requests.get(url, headers=hdrs).content
        df = pd.read_csv(io.BytesIO(content), low_memory=True, dtype_backend='numpy_nullable')
        return optimize_dtypes(df)

    results = svc.files().list(
        q=f"'{file_id}' in parents and trashed=false"
    ).execute()
    dfs = []
    for f in results.get('files', []):
        content = requests.get(
            f"https://www.googleapis.com/drive/v3/files/{f['id']}?alt=media",
            headers=hdrs
        ).content
        dfs.append(optimize_dtypes(pd.read_csv(io.BytesIO(content), low_memory=True)))
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# ── 나라장터_공고 연도별 CSV ────────────────────────────────
NOTICE_FOLDER_ID  = "1AsvVmayEmTtY92d1SfXxNi6bL0Zjw5mg"
NOTICE_CATS       = ['공사', '물품', '용역']
NOTICE_COL_RENAME = {
    "dminsttNm":     "수요기관명",
    "bidNtceNm":     "공고명",
    "presmptPrce":   "추정가격",
    "bidClsDt":      "입찰마감일시",
    "bidNtceDtlUrl": "공고문",
}

@st.cache_resource(ttl=3600)
def fetch_notice_csv_by_year(cat_name, year):
    svc, creds = get_drive_service()
    creds.refresh(google.auth.transport.requests.Request())
    hdrs      = {'Authorization': f'Bearer {creds.token}'}
    file_name = f"나라장터_공고_{cat_name}_{year}년.csv"
    files = svc.files().list(
        q=f"name='{file_name}' and '{NOTICE_FOLDER_ID}' in parents and trashed=false",
        fields='files(id)'
    ).execute().get('files', [])
    if not files:
        return pd.DataFrame()
    resp = requests.get(
        f"https://www.googleapis.com/drive/v3/files/{files[0]['id']}?alt=media",
        headers=hdrs
    )
    try:
        df = pd.read_csv(io.BytesIO(resp.content), encoding='utf-8-sig', low_memory=True)
    except Exception:
        df = pd.read_csv(io.BytesIO(resp.content), encoding='cp949', low_memory=True)
    return optimize_dtypes(df)


def load_notice_data(cat_name, s_s, e_s):
    dfs = []
    for year in range(int(s_s[:4]), int(e_s[:4]) + 1):
        df_y = fetch_notice_csv_by_year(cat_name, year)
        if not df_y.empty:
            dfs.append(df_y)
    if not dfs:
        return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)
    df['tmp_dt'] = (
        df['bidNtceDt'].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8]
        if 'bidNtceDt' in df.columns else "0"
    )
    return df[(df['tmp_dt'] >= s_s) & (df['tmp_dt'] <= e_s)]


# ═══════════════════════════════════════════════════════════
# ★ 수정 1: apply_keyword — ALL 모드 최적화
#   기존: df.astype(str) → 모든 컬럼 문자열 변환 (메모리 폭발)
#   수정: object/category 컬럼만 검색 → 불필요한 변환 제거
# ═══════════════════════════════════════════════════════════
# 각 카테고리별로 검색 대상이 되는 텍스트 컬럼 후보
_TEXT_COL_CANDIDATES = ["수요기관명","수요기관","발주기관","공고기관",
                         "업체명","상호","상호명","계약상대자","계약상대자명",
                         "계약명","공고명","세부품명","품명"]

def apply_keyword(df: pd.DataFrame, keyword: str, field: str) -> pd.Series:
    # 특정 필드 검색
    target_col = None
    if field == "업체명":
        for c in ["업체명","상호","상호명","계약상대자","계약상대자명"]:
            if c in df.columns: target_col = c; break
    elif field == "수요기관명":
        for c in ["수요기관명","수요기관","발주기관","공고기관"]:
            if c in df.columns: target_col = c; break
    elif field == "계약명":
        for c in ["계약명","공고명"]:
            if c in df.columns: target_col = c; break
    elif field == "세부품명":
        if "세부품명" in df.columns: target_col = "세부품명"
    elif field != "ALL":
        if field in df.columns: target_col = field

    if target_col:
        return df[target_col].astype(str).str.contains(keyword, case=False, na=False)

    # ★ ALL 모드: object/category 컬럼만 검색 (숫자 컬럼 제외)
    text_cols = [
        c for c in df.columns
        if df[c].dtype in ('object', 'category', 'string')
        or c in _TEXT_COL_CANDIDATES
    ]
    if not text_cols:
        # fallback: 전체 (최후 수단)
        return df.astype(str).apply(
            lambda x: x.str.contains(keyword, case=False, na=False)
        ).any(axis=1)

    mask = pd.Series(False, index=df.index)
    for col in text_cols:
        mask |= df[col].astype(str).str.contains(keyword, case=False, na=False)
    return mask


# ═══════════════════════════════════════════════════════════
# ★ 수정 2: filter_data — .copy() 최소화
#   기존: 조건마다 .copy() 호출 → 메모리 중복
#   수정: boolean mask만 누적, 마지막에 1번만 .copy()
# ═══════════════════════════════════════════════════════════
DATE_COL_MAP = {
    '나라장터_발주': None,
    '나라장터_공고': 'bidNtceDt',
    '나라장터_계약': '★가공_계약일',
    '군수품_발주':   '발주예정월',
    '군수품_공고':   '공고일자',
    '군수품_계약':   '계약일자',
    '군수품_수의':   '개찰일자',
    '종합쇼핑몰':   '계약납품요구일자'
}

def filter_data(df_raw: pd.DataFrame, cat: str, s_s: str, e_s: str,
                k1_val: str, k2_val: str, f_val: str, l_val: str) -> pd.DataFrame:
    d_col = DATE_COL_MAP.get(cat)

    # ── 날짜 mask ──────────────────────────────────────────
    if cat == '나라장터_발주':
        date_mask = pd.Series(True, index=df_raw.index)
        tmp_dt    = pd.Series(s_s, index=df_raw.index)
    elif cat == '군수품_발주':
        tmp_dt = (
            df_raw[d_col].astype(str)
            .str.replace(r'[^0-9]', '', regex=True).str[:6] + "01"
        )
        date_mask = (tmp_dt >= s_s[:6] + "01") & (tmp_dt <= e_s[:6] + "01")
    else:
        if d_col and d_col in df_raw.columns:
            tmp_dt = df_raw[d_col].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8]
        else:
            tmp_dt = pd.Series("0", index=df_raw.index)
        date_mask = (tmp_dt >= s_s) & (tmp_dt <= e_s)

    # ── 키워드 mask ────────────────────────────────────────
    kw_mask = pd.Series(True, index=df_raw.index)
    if k1_val and k1_val.strip():
        m1 = apply_keyword(df_raw, k1_val.strip(), f_val)
        if l_val == "AND" and k2_val and k2_val.strip():
            kw_mask = m1 & apply_keyword(df_raw, k2_val.strip(), f_val)
        elif l_val == "OR" and k2_val and k2_val.strip():
            kw_mask = m1 | apply_keyword(df_raw, k2_val.strip(), f_val)
        else:
            kw_mask = m1

    # ★ 최종 1번만 .copy() — 필터된 결과만 메모리에 보관
    result = df_raw[date_mask & kw_mask].copy()
    result['tmp_dt'] = tmp_dt[date_mask & kw_mask]
    return result.sort_values('tmp_dt', ascending=False)


# ── 설정값 ────────────────────────────────────────────────
SHEET_FILE_IDS = {
    '나라장터_발주': '1pGnb6O5Z1ahaHYuQdydyoY1Ayf147IoGmLRdA3WAHi4',
    '나라장터_공고': None,
    '나라장터_계약': '15Hsr_nup4ZteIZ4Jyov8wG2s_rKoZ25muqRE3-sRnaw',
    '군수품_발주':   '1pzW51Z29SSoQk7al_GvN_tj5smuhOR3J2HWnL_16fcI',
    '군수품_공고':   '1opuA_UzNm27U9QkbMay5UsyQqcwfxiEmIHNRdc4MyHM',
    '군수품_계약':   '1KPMUz0IKM6AQvqwfAkvW96WNvzbycN56vNlFnDmfRTw',
    '군수품_수의':   '1aYA18kPrSkpbayzbn16EdKUScVRwr2Nutyid5No5qjk',
    '종합쇼핑몰':   '1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr'
}

DISPLAY_INDEX_MAP = {
    '나라장터_발주': [9, 13, 20],
    '나라장터_공고': ["수요기관명", "공고명", "추정가격", "입찰마감일시", "공고문", "공고유형"],
    '나라장터_계약': [0, 3, 4, 5, 6, 33],
    '군수품_발주':   [7, 8, 12, 2, 3],
    '군수품_공고':   [0, 16, 18, 19, 23],
    '군수품_계약':   [7, 5, 3, 1, 12],
    '군수품_수의':   [12, 10, 8, 3],
    '종합쇼핑몰':   ["수요기관명", "계약납품요구일자", "세부품명", "계약명", "업체명", "수량", "금액"]
}

TAB_ICONS = {
    '나라장터_발주': '📋', '나라장터_공고': '📢',
    '나라장터_계약': '📝', '군수품_발주':   '🛡',
    '군수품_공고':   '📣', '군수품_계약':   '✅',
    '군수품_수의':   '🤝', '종합쇼핑몰':   '🛒'
}


# ═══════════════════════════════════════════════════════════
# ★ 수정 3: Excel/CSV 생성을 검색 시점에 1번만 수행
#   기존: show_result_table(@st.fragment) 안에서 생성
#         → 페이지 버튼 클릭마다 Excel 재생성 (CPU 폭발)
#   수정: 검색 완료 시 session_state에 bytes로 저장
#         → 다운로드 버튼은 이미 만들어진 bytes만 전달
# ═══════════════════════════════════════════════════════════
def build_download_bytes(df: pd.DataFrame) -> tuple[bytes, bytes]:
    """CSV + Excel bytes를 검색 시 1번만 생성해 session_state에 저장"""
    csv_bytes = df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='xlsxwriter') as w:
        df.to_excel(w, index=False)
    return csv_bytes, out.getvalue()


# ─────────────────────────────────────────────────────────
# 결과 테이블 Fragment
# ─────────────────────────────────────────────────────────
@st.fragment
def show_result_table(cat, idx_list):
    df = st.session_state.get(f"df_{cat}")
    if df is None:
        return

    show_cols = [
        df.columns[idx] if isinstance(idx, int) else idx
        for idx in idx_list
        if (isinstance(idx, int) and idx < len(df.columns))
        or (isinstance(idx, str) and idx in df.columns)
    ] or list(df.columns[:10])

    st.markdown('<div class="info-bar">', unsafe_allow_html=True)
    c_res, c_s1, c_s2, c_sb, c_lim, c_dl = st.columns([1.2, 2.0, 1.6, 0.7, 1.1, 2.2])
    c_res.markdown(f'<div class="result-badge">✅ {len(df):,} 건</div>', unsafe_allow_html=True)
    sort_by  = c_s1.selectbox("정렬", ["날짜순"] + show_cols, key=f"st_{cat}", label_visibility="collapsed")
    sort_dir = c_s2.selectbox("순서", ["내림차순", "오름차순"], key=f"sd_sort_{cat}", label_visibility="collapsed")
    if c_sb.button("↕", key=f"sb_{cat}", use_container_width=True):
        st.session_state[f"df_{cat}"] = df.sort_values(
            by='tmp_dt' if sort_by == "날짜순" else sort_by,
            ascending=(sort_dir == "오름차순")
        )
        st.rerun()
    p_lim = c_lim.selectbox("개수", [50, 100, 150, 200], key=f"ps_{cat}", label_visibility="collapsed")

    # ★ 다운로드: 검색 시 저장된 bytes 그대로 전달 (재생성 없음)
    with c_dl:
        cc, cx = st.columns(2)
        csv_bytes  = st.session_state.get(f"csv_{cat}", b"")
        xlsx_bytes = st.session_state.get(f"xlsx_{cat}", b"")
        cc.download_button("📑 CSV",   csv_bytes,  f"{cat}.csv",  "text/csv",
                           use_container_width=True)
        cx.download_button("📊 Excel", xlsx_bytes, f"{cat}.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    total = max((len(df) - 1) // p_lim + 1, 1)
    curr  = st.session_state.get(f"p_num_{cat}", 1)

    lcc = {}
    if "계약상세정보URL" in show_cols:
        lcc["계약상세정보URL"] = st.column_config.LinkColumn("계약상세정보URL", display_text="바로가기")
    if "공고문" in show_cols:
        lcc["공고문"] = st.column_config.LinkColumn("공고문", display_text="바로가기")

    st.dataframe(
        df[show_cols].iloc[(curr - 1) * p_lim: curr * p_lim],
        use_container_width=True, height=500, column_config=lcc
    )

    pg = st.columns([1, 10, 1])
    with pg[1]:
        bc = st.columns(14)
        sp = max(1, curr - 4)
        ep = min(total, sp + 9)
        if bc[0].button("«",  key=f"f10_{cat}"): st.session_state[f"p_num_{cat}"] = max(1, curr - 10);  st.rerun()
        if bc[1].button("‹",  key=f"f1_{cat}"):  st.session_state[f"p_num_{cat}"] = max(1, curr - 1);   st.rerun()
        for j, p in enumerate(range(sp, ep + 1)):
            if bc[j + 2].button(str(p), key=f"pg_{cat}_{p}",
                                type="primary" if p == curr else "secondary"):
                st.session_state[f"p_num_{cat}"] = p
                st.rerun()
        if bc[12].button("›", key=f"n1_{cat}"):  st.session_state[f"p_num_{cat}"] = min(total, curr + 1);  st.rerun()
        if bc[13].button("»", key=f"n10_{cat}"): st.session_state[f"p_num_{cat}"] = min(total, curr + 10); st.rerun()


# ─────────────────────────────────────────────────────────
# 세션 초기화
# ─────────────────────────────────────────────────────────
_today = date.today()
_yed   = (_today - relativedelta(days=1)).strftime('%Y-%m-%d')
_6m    = (_today - relativedelta(months=6)).strftime('%Y-%m-%d')
for _cat in SHEET_FILE_IDS:
    for key, val in [
        (f"sd_in_{_cat}", _6m),
        (f"ed_in_{_cat}", _yed),
        (f"df_{_cat}",    None),
        (f"csv_{_cat}",   b""),
        (f"xlsx_{_cat}",  b""),
    ]:
        if key not in st.session_state:
            st.session_state[key] = val
if "nt_나라장터_공고" not in st.session_state:
    st.session_state["nt_나라장터_공고"] = "전체"


# ─────────────────────────────────────────────────────────
# 헤더
# ─────────────────────────────────────────────────────────
c1, c2 = st.columns([5, 1])
with c1:
    st.markdown("""
    <div class="main-header">
      <div class="header-icon"><br><br>🏛</div>
      <div>
        <br><br>
        <p class="header-title"><b>공공조달 DATA 통합검색 시스템</b></p>
        <p class="header-sub">나라장터 · 군수품 · 종합쇼핑몰 통합 데이터 조회 플랫폼</p>
      </div>
    </div>""", unsafe_allow_html=True)
with c2:
    st.markdown("<br>", unsafe_allow_html=True)
    st.link_button("⛓️ 지자체 유지보수 내역", "https://g2b-info.streamlit.app/", use_container_width=True)
st.markdown('<div class="header-divider"></div>', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────
# 메인 탭 루프
# ─────────────────────────────────────────────────────────
tab_labels = [f"{TAB_ICONS.get(k,'')} {k}" for k in SHEET_FILE_IDS]
tabs = st.tabs(tab_labels)

for i, tab in enumerate(tabs):
    cat = list(SHEET_FILE_IDS.keys())[i]
    with tab:
        today     = date.today()
        yesterday = today - relativedelta(days=1)
        yed_str   = yesterday.strftime('%Y-%m-%d')

        st.markdown('<div class="search-panel">', unsafe_allow_html=True)
        st.markdown('<div class="search-section-label">📅 조회 기간</div>', unsafe_allow_html=True)

        qb = st.columns([0.55, 0.55, 0.65, 0.65, 0.65, 0.65, 0.65, 0.65, 6])
        QUICK = [
            ("어제",  f"d1_{cat}",  yed_str,                                                  yed_str),
            ("1주",   f"d7_{cat}",  (yesterday - relativedelta(days=6)).strftime('%Y-%m-%d'),  yed_str),
            ("1개월", f"m1_{cat}",  (yesterday - relativedelta(months=1)).strftime('%Y-%m-%d'), yed_str),
            ("3개월", f"m3_{cat}",  (yesterday - relativedelta(months=3)).strftime('%Y-%m-%d'), yed_str),
            ("6개월", f"m6_{cat}",  (yesterday - relativedelta(months=6)).strftime('%Y-%m-%d'), yed_str),
            ("9개월", f"m9_{cat}",  (yesterday - relativedelta(months=9)).strftime('%Y-%m-%d'), yed_str),
            ("1년",   f"y1_{cat}",  (yesterday - relativedelta(years=1)).strftime('%Y-%m-%d'),  yed_str),
            ("2년",   f"y2_{cat}",  (yesterday - relativedelta(years=2)).strftime('%Y-%m-%d'),  yed_str),
        ]
        for idx_q, (label, key, sd_q, ed_q) in enumerate(QUICK):
            if qb[idx_q].button(label, key=key):
                st.session_state[f"sd_in_{cat}"] = sd_q
                st.session_state[f"ed_in_{cat}"] = ed_q
                st.rerun()

        st.markdown('<div class="search-section-label">🔍 검색 조건</div>', unsafe_allow_html=True)

        if cat == '나라장터_공고':
            fd1, fd2, sc0, sc1, sc2, sc3, sc4, sc5 = st.columns([1.0, 1.0, 0.8, 0.9, 2.3, 0.7, 2.3, 1.0])
            sd_in       = fd1.text_input("시작일", key=f"sd_in_{cat}", placeholder="YYYY-MM-DD", label_visibility="collapsed")
            ed_in       = fd2.text_input("종료일", key=f"ed_in_{cat}", placeholder="YYYY-MM-DD", label_visibility="collapsed")
            notice_type = sc0.selectbox("공고유형", ["전체", "공사", "물품", "용역"], key=f"nt_{cat}", label_visibility="collapsed")
            f_val       = sc1.selectbox("필드", ["ALL", "수요기관명", "공고명"], key=f"f_{cat}", label_visibility="collapsed")
            k1_val      = sc2.text_input("검색어1", key=f"k1_{cat}", label_visibility="collapsed", placeholder="🔎  검색어를 입력하세요")
            l_val       = sc3.selectbox("논리", ["NONE", "AND", "OR"], key=f"l_{cat}", label_visibility="collapsed")
            k2_val      = sc4.text_input("검색어2", key=f"k2_{cat}", label_visibility="collapsed", placeholder="🔎  검색어2 (AND/OR 선택 시)")
            search_exe  = sc5.button("🔍  검색실행", key=f"search_{cat}", use_container_width=True, type="primary")
        else:
            fd1, fd2, sc1, sc2, sc3, sc4, sc5 = st.columns([1.1, 1.1, 1.0, 2.6, 0.7, 2.6, 1.1])
            sd_in      = fd1.text_input("시작일", key=f"sd_in_{cat}", placeholder="YYYY-MM-DD", label_visibility="collapsed")
            ed_in      = fd2.text_input("종료일", key=f"ed_in_{cat}", placeholder="YYYY-MM-DD", label_visibility="collapsed")
            f_val      = sc1.selectbox("필드", ["ALL", "수요기관명", "업체명", "계약명", "세부품명"], key=f"f_{cat}", label_visibility="collapsed")
            k1_val     = sc2.text_input("검색어1", key=f"k1_{cat}", label_visibility="collapsed", placeholder="🔎  검색어를 입력하세요")
            l_val      = sc3.selectbox("논리", ["NONE", "AND", "OR"], key=f"l_{cat}", label_visibility="collapsed")
            k2_val     = sc4.text_input("검색어2", key=f"k2_{cat}", label_visibility="collapsed", placeholder="🔎  검색어2 (AND/OR 선택 시)")
            search_exe = sc5.button("🔍  검색실행", key=f"search_{cat}", use_container_width=True, type="primary")
            notice_type = "전체"

        st.markdown("</div>", unsafe_allow_html=True)

        # ── 검색 실행 ────────────────────────────────────────
        if search_exe:
            try:
                sd = datetime.strptime(sd_in.strip(), '%Y-%m-%d').date()
                ed = datetime.strptime(ed_in.strip(), '%Y-%m-%d').date()
            except ValueError:
                st.warning("날짜 형식이 올바르지 않습니다. YYYY-MM-DD 형식으로 입력해주세요.")
                st.stop()
            s_s = sd.strftime('%Y%m%d')
            e_s = ed.strftime('%Y%m%d')

            try:
                with st.spinner("데이터를 불러오는 중..."):
                    if cat == '나라장터_공고':
                        cats_to_load = NOTICE_CATS if notice_type == "전체" else [notice_type]
                        dfs = []
                        for nc in cats_to_load:
                            df_t = load_notice_data(nc, s_s, e_s)
                            if not df_t.empty:
                                df_t = df_t.copy()
                                df_t['공고유형'] = nc
                                dfs.append(df_t)
                        df_f = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
                        if not df_f.empty:
                            df_f = df_f.rename(columns=NOTICE_COL_RENAME)
                            if '추정가격' in df_f.columns:
                                def fmt_price(v):
                                    try: return f"{int(float(str(v).replace(',', ''))):,}"
                                    except: return v
                                df_f['추정가격'] = df_f['추정가격'].apply(fmt_price)
                            if k1_val and k1_val.strip():
                                m1 = apply_keyword(df_f, k1_val.strip(), f_val)
                                if l_val == "AND" and k2_val and k2_val.strip():
                                    df_f = df_f[m1 & apply_keyword(df_f, k2_val.strip(), f_val)]
                                elif l_val == "OR" and k2_val and k2_val.strip():
                                    df_f = df_f[m1 | apply_keyword(df_f, k2_val.strip(), f_val)]
                                else:
                                    df_f = df_f[m1]
                            st.session_state[f"df_{cat}"] = df_f.sort_values('tmp_dt', ascending=False)
                        else:
                            st.session_state[f"df_{cat}"] = pd.DataFrame()
                    else:
                        # 공유 캐시에서 원본 참조 (복사 없음)
                        df_raw = fetch_data_shared(
                            SHEET_FILE_IDS[cat],
                            is_sheet=(cat != '종합쇼핑몰')
                        )
                        df_f = filter_data(df_raw, cat, s_s, e_s, k1_val, k2_val, f_val, l_val) \
                               if not df_raw.empty else pd.DataFrame()
                        st.session_state[f"df_{cat}"] = df_f

                    # ★ 검색 완료 시 다운로드 bytes 1번만 생성
                    df_result = st.session_state[f"df_{cat}"]
                    if df_result is not None and not df_result.empty:
                        csv_b, xlsx_b = build_download_bytes(df_result)
                        st.session_state[f"csv_{cat}"]  = csv_b
                        st.session_state[f"xlsx_{cat}"] = xlsx_b
                    else:
                        st.session_state[f"csv_{cat}"]  = b""
                        st.session_state[f"xlsx_{cat}"] = b""

                    st.session_state[f"p_num_{cat}"] = 1

            except Exception as e:
                st.error(f"오류: {e}")
                st.session_state[f"df_{cat}"] = pd.DataFrame()

        # ── 결과 출력 ─────────────────────────────────────────
        if st.session_state[f"df_{cat}"] is not None:
            if len(st.session_state[f"df_{cat}"]) == 0:
                st.markdown('<div class="no-data-msg"><div class="no-data-icon">🔍</div>검색 결과가 없습니다.</div>', unsafe_allow_html=True)
            else:
                show_result_table(cat, DISPLAY_INDEX_MAP.get(cat, []))
        else:
            st.markdown('<div class="no-data-msg"><div class="no-data-icon">📂</div>검색 조건을 입력하고 <strong>검색실행</strong> 버튼을 눌러주세요.</div>', unsafe_allow_html=True)
