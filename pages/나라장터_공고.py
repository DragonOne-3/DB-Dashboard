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

st.set_page_config(page_title="나라장터 공고 검색", layout="wide", page_icon="📢")

# ── 진단 모드: 실제 에러 원인 파악 ──────────────────────────────────────────────
import traceback as _tb

def _diag():
    """각 단계별로 실행해서 어디서 터지는지 확인"""
    steps = []
    try:
        import json as _json
        _info = _json.loads(st.secrets["GOOGLE_AUTH_JSON"])
        steps.append("✅ secrets 로드 OK")
    except Exception as e:
        steps.append(f"❌ secrets 로드 실패: {e}")
        return steps

    try:
        from google.oauth2 import service_account as _sa
        from googleapiclient.discovery import build as _build
        _creds = _sa.Credentials.from_service_account_info(
            _info, scopes=['https://www.googleapis.com/auth/drive.readonly'])
        _svc = _build('drive', 'v3', credentials=_creds)
        steps.append("✅ Google Drive 인증 OK")
    except Exception as e:
        steps.append(f"❌ Google Drive 인증 실패: {e}")
        return steps

    try:
        import google.auth.transport.requests as _gatr
        _creds.refresh(_gatr.Request())
        _hdrs = {'Authorization': f'Bearer {_creds.token}'}
        steps.append("✅ 토큰 갱신 OK")
    except Exception as e:
        steps.append(f"❌ 토큰 갱신 실패: {e}")
        return steps

    for fname in ['나라장터_공고_공사.csv', '나라장터_공고_물품.csv', '나라장터_공고_용역.csv']:
        try:
            import requests as _req, pandas as _pd, io as _io
            _files = _svc.files().list(
                q=f"name='{fname}' and trashed=false", fields='files(id,name)'
            ).execute().get('files', [])
            if not _files:
                steps.append(f"⚠️ {fname}: 파일 없음")
                continue
            _resp = _req.get(
                f"https://www.googleapis.com/drive/v3/files/{_files[0]['id']}?alt=media",
                headers=_hdrs
            )
            steps.append(f"✅ {fname}: HTTP {_resp.status_code}, {len(_resp.content):,} bytes")
            try:
                _df = _pd.read_csv(_io.BytesIO(_resp.content), encoding='utf-8-sig', low_memory=False)
                steps.append(f"   └ CSV 파싱 OK: {len(_df):,}행 × {len(_df.columns)}열")
            except Exception as e2:
                steps.append(f"   └ ❌ CSV 파싱 실패: {e2}")
        except Exception as e:
            steps.append(f"❌ {fname} 다운로드 실패: {e}")
    return steps

with st.expander("🔍 진단 실행 (에러 원인 확인용 — 해결 후 삭제)", expanded=True):
    if st.button("진단 시작", key="diag_btn"):
        with st.spinner("진단 중..."):
            results = _diag()
        for r in results:
            st.write(r)
# ────────────────────────────────────────────────────────────────────────────────

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
.search-panel { background: #ffffff; border: 1px solid #e2e8f0; border-radius: 14px; padding: 20px 24px 18px 24px; margin-bottom: 18px; box-shadow: 0 2px 12px rgba(0,0,0,0.06); }
.search-section-label { font-size: 14px; font-weight: 600; color: #94a3b8; letter-spacing: 1.8px; text-transform: uppercase; margin-bottom: 10px; display: flex; align-items: center; gap: 8px; }
.search-section-label::after { content: ''; flex: 1; height: 1px; background: #e2e8f0; }
.stTextInput > div > div > input { background: #f8fafc !important; border: 1px solid #cbd5e1 !important; border-radius: 9px !important; color: #1e293b !important; font-size: 13px !important; padding: 8px 13px !important; }
.stTextInput > div > div > input:focus { border-color: #3b82f6 !important; box-shadow: 0 0 0 3px rgba(59,130,246,0.15) !important; background: #ffffff !important; }
.stTextInput > div > div > input::placeholder { color: #94a3b8 !important; }
.stSelectbox > div > div { background: #f8fafc !important; border: 1px solid #cbd5e1 !important; border-radius: 9px !important; color: #1e293b !important; font-size: 13px !important; }
[data-baseweb="select"] > div { background: #f8fafc !important; border-color: #cbd5e1 !important; }
[data-baseweb="popover"] { background: #ffffff !important; border: 1px solid #e2e8f0 !important; box-shadow: 0 8px 24px rgba(0,0,0,0.12) !important; }
[data-baseweb="menu"] { background: #ffffff !important; }
[data-baseweb="option"] { background: #ffffff !important; color: #1e293b !important; font-size: 13px !important; }
[data-baseweb="option"]:hover { background: #eff6ff !important; color: #1d4ed8 !important; }
.stButton > button { background: #ffffff !important; border: 1px solid #cbd5e1 !important; border-radius: 20px !important; color: #475569 !important; font-size: 12px !important; font-family: 'Noto Sans KR', sans-serif !important; font-weight: 500 !important; padding: 4px 14px !important; height: 30px !important; transition: all 0.16s ease !important; }
.stButton > button:hover { background: #eff6ff !important; border-color: #3b82f6 !important; color: #1d4ed8 !important; transform: translateY(-1px) !important; }
button[kind="primary"] { background: linear-gradient(135deg, #1d4ed8 0%, #3b82f6 100%) !important; border: none !important; border-radius: 10px !important; color: #ffffff !important; font-weight: 700 !important; font-size: 13px !important; height: 38px !important; }
button[kind="primary"]:hover { transform: translateY(-2px) !important; filter: brightness(1.06) !important; }
[data-testid="stHorizontalBlock"] button[kind="primary"] { background: #1d4ed8 !important; border-radius: 20px !important; height: 30px !important; min-width: 30px !important; padding: 0 10px !important; font-size: 12px !important; }
.stDownloadButton > button { background: #f0f7ff !important; border: 1px solid #bfdbfe !important; border-radius: 9px !important; color: #1d4ed8 !important; font-size: 12px !important; height: 32px !important; }
.stDownloadButton > button:hover { background: #dbeafe !important; }
.info-bar { display: flex; align-items: center; gap: 12px; padding: 10px 16px; background: #ffffff; border: 1px solid #e2e8f0; border-radius: 12px; margin-bottom: 12px; }
.result-badge { background: linear-gradient(135deg, #eff6ff, #dbeafe); border: 1px solid #93c5fd; border-radius: 8px; padding: 5px 14px; font-size: 13px; font-weight: 700; color: #1d4ed8; font-family: 'JetBrains Mono', monospace; white-space: nowrap; }
.stDataFrame { border-radius: 12px !important; overflow: hidden; border: 1px solid #e2e8f0 !important; }
.stDataFrame thead tr th { background: #f8fafc !important; color: #64748b !important; font-size: 12px !important; font-weight: 600 !important; border-bottom: 1px solid #e2e8f0 !important; }
.stDataFrame tbody tr:hover td { background: #f0f7ff !important; }
.stDataFrame tbody tr td { font-size: 13px !important; color: #334155 !important; }
.stTextInput label, .stSelectbox label { display: none !important; }
.stSpinner > div { border-top-color: #3b82f6 !important; }
::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 3px; }
.no-data-msg { text-align: center; padding: 50px; color: #94a3b8; font-size: 14px; }
.no-data-icon { font-size: 38px; margin-bottom: 10px; opacity: 0.4; }
</style>
""", unsafe_allow_html=True)


# ── Google Drive ──────────────────────────────────────────────────────────────────
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

@st.cache_data(ttl=3600)
def fetch_csv_by_name(file_name):
    svc, creds = get_drive_service()
    creds.refresh(google.auth.transport.requests.Request())
    hdrs  = {'Authorization': f'Bearer {creds.token}'}
    files = svc.files().list(
        q=f"name='{file_name}' and trashed=false", fields='files(id)'
    ).execute().get('files', [])
    if not files:
        return pd.DataFrame()
    resp = requests.get(
        f"https://www.googleapis.com/drive/v3/files/{files[0]['id']}?alt=media",
        headers=hdrs
    )
    return pd.read_csv(io.BytesIO(resp.content), encoding='utf-8-sig', low_memory=False)


NOTICE_CSV_MAP = {
    '공사': '나라장터_공고_공사.csv',
    '물품': '나라장터_공고_물품.csv',
    '용역': '나라장터_공고_용역.csv',
}
DISPLAY_COLS = ["dminsttNm", "bidNtceNm", "presmptPrce", "bidClsDt", "bidNtceDtlUrl"]
CAT = "나라장터_공고"


def apply_keyword(df, keyword, field):
    target = field
    if field == "수요기관명":
        for c in ["수요기관명","수요기관","발주기관","공고기관","dminsttNm"]:
            if c in df.columns: target = c; break
    elif field == "계약명":
        for c in ["계약명","공고명","bidNtceNm"]:
            if c in df.columns: target = c; break
    elif field == "업체명":
        for c in ["업체명","상호","상호명"]:
            if c in df.columns: target = c; break
    if target != "ALL" and target in df.columns:
        return df[target].astype(str).str.contains(keyword, case=False, na=False)
    return df.astype(str).apply(lambda x: x.str.contains(keyword, case=False, na=False)).any(axis=1)


# ── 헤더 ─────────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="main-header">
  <div class="header-icon">📢</div>
  <div>
    <p class="header-title"><b>나라장터 공고 검색</b></p>
    <p class="header-sub">공사 · 물품 · 용역 공고 통합 조회</p>
  </div>
</div>""", unsafe_allow_html=True)
st.markdown('<div class="header-divider"></div>', unsafe_allow_html=True)


# ── 세션 초기화 ───────────────────────────────────────────────────────────────────
today     = date.today()
yesterday = today - relativedelta(days=1)

if "ng_sd" not in st.session_state:
    st.session_state["ng_sd"] = (today - relativedelta(months=6)).strftime('%Y-%m-%d')
if "ng_ed" not in st.session_state:
    st.session_state["ng_ed"] = yesterday.strftime('%Y-%m-%d')
if "ng_df" not in st.session_state:
    st.session_state["ng_df"] = None


# ── 퀵버튼 ───────────────────────────────────────────────────────────────────────
st.markdown('<div class="search-panel">', unsafe_allow_html=True)
st.markdown('<div class="search-section-label">📅 조회 기간</div>', unsafe_allow_html=True)

yed_str = yesterday.strftime('%Y-%m-%d')
qb = st.columns([0.55, 0.55, 0.65, 0.65, 0.65, 0.65, 0.65, 0.65, 6])
QUICK = [
    ("어제",  "ng_d1",  yed_str,                                                    yed_str),
    ("1주",   "ng_d7",  (yesterday-relativedelta(days=6)).strftime('%Y-%m-%d'),     yed_str),
    ("1개월", "ng_m1",  (yesterday-relativedelta(months=1)).strftime('%Y-%m-%d'),   yed_str),
    ("3개월", "ng_m3",  (yesterday-relativedelta(months=3)).strftime('%Y-%m-%d'),   yed_str),
    ("6개월", "ng_m6",  (yesterday-relativedelta(months=6)).strftime('%Y-%m-%d'),   yed_str),
    ("9개월", "ng_m9",  (yesterday-relativedelta(months=9)).strftime('%Y-%m-%d'),   yed_str),
    ("1년",   "ng_y1",  (yesterday-relativedelta(years=1)).strftime('%Y-%m-%d'),    yed_str),
    ("2년",   "ng_y2",  (yesterday-relativedelta(years=2)).strftime('%Y-%m-%d'),    yed_str),
]
for idx_q, (label, key, sd_q, ed_q) in enumerate(QUICK):
    if qb[idx_q].button(label, key=key):
        st.session_state["ng_sd"] = sd_q
        st.session_state["ng_ed"] = ed_q
        st.rerun()


# ── 검색 조건 ─────────────────────────────────────────────────────────────────────
st.markdown('<div class="search-section-label">🔍 검색 조건</div>', unsafe_allow_html=True)

fd1, fd2, sc0, sc1, sc2, sc3, sc4, sc5 = st.columns([1.1, 1.1, 0.9, 1.0, 2.4, 0.7, 2.4, 1.1])

sd_in       = fd1.text_input("시작일", key="ng_sd", placeholder="YYYY-MM-DD", label_visibility="collapsed")
ed_in       = fd2.text_input("종료일", key="ng_ed", placeholder="YYYY-MM-DD", label_visibility="collapsed")
notice_type = sc0.selectbox("공고유형", ["전체","공사","물품","용역"], key="ng_nt", label_visibility="collapsed")
f_val       = sc1.selectbox("필드", ["ALL","수요기관명","업체명","계약명","세부품명"], key="ng_f", label_visibility="collapsed")
k1_val      = sc2.text_input("검색어1", key="ng_k1", placeholder="🔎  검색어를 입력하세요", label_visibility="collapsed")
l_val       = sc3.selectbox("논리", ["NONE","AND","OR"], key="ng_l", label_visibility="collapsed")
k2_val      = sc4.text_input("검색어2", key="ng_k2", placeholder="🔎  검색어2 (AND/OR 선택 시)", label_visibility="collapsed")
search_exe  = sc5.button("🔍  검색실행", key="ng_search", use_container_width=True, type="primary")

st.markdown("</div>", unsafe_allow_html=True)


# ── 검색 실행 ─────────────────────────────────────────────────────────────────────
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
            types_to_load = list(NOTICE_CSV_MAP.keys()) if notice_type == "전체" else [notice_type]
            dfs = []
            for t in types_to_load:
                df_t = fetch_csv_by_name(NOTICE_CSV_MAP[t])
                if not df_t.empty:
                    df_t['공고유형'] = t
                    dfs.append(df_t)
            df_raw = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

            if not df_raw.empty:
                d_col = 'bidNtceDt'
                if d_col in df_raw.columns:
                    df_raw['tmp_dt'] = df_raw[d_col].astype(str).str.replace(r'[^0-9]','',regex=True).str[:8]
                else:
                    df_raw['tmp_dt'] = "0"

                df_f = df_raw[(df_raw['tmp_dt'] >= s_s) & (df_raw['tmp_dt'] <= e_s)].copy()

                if k1_val and k1_val.strip():
                    m1 = apply_keyword(df_f, k1_val.strip(), f_val)
                    if l_val == "AND" and k2_val and k2_val.strip():
                        df_f = df_f[m1 & apply_keyword(df_f, k2_val.strip(), f_val)]
                    elif l_val == "OR" and k2_val and k2_val.strip():
                        df_f = df_f[m1 | apply_keyword(df_f, k2_val.strip(), f_val)]
                    else:
                        df_f = df_f[m1]

                st.session_state["ng_df"] = df_f.sort_values(by='tmp_dt', ascending=False)
                st.session_state["ng_pnum"] = 1
            else:
                st.session_state["ng_df"] = pd.DataFrame()

    except Exception as e:
        st.error(f"오류: {e}")
        st.session_state["ng_df"] = pd.DataFrame()


# ── 결과 출력 ─────────────────────────────────────────────────────────────────────
df = st.session_state.get("ng_df")

if df is None:
    st.markdown('<div class="no-data-msg"><div class="no-data-icon">📂</div>검색 조건을 입력하고 <strong>검색실행</strong> 버튼을 눌러주세요.</div>', unsafe_allow_html=True)
elif len(df) == 0:
    st.markdown('<div class="no-data-msg"><div class="no-data-icon">🔍</div>검색 결과가 없습니다.</div>', unsafe_allow_html=True)
else:
    show_cols = [c for c in DISPLAY_COLS if c in df.columns]
    if not show_cols:
        show_cols = list(df.columns[:10])

    st.markdown('<div class="info-bar">', unsafe_allow_html=True)
    c_res, c_s1, c_s2, c_sb, c_lim, c_dl = st.columns([1.2, 2.0, 1.6, 0.7, 1.1, 2.2])
    c_res.markdown(f'<div class="result-badge">✅ {len(df):,} 건</div>', unsafe_allow_html=True)
    sort_by  = c_s1.selectbox("정렬", ["날짜순"]+show_cols, key="ng_sort", label_visibility="collapsed")
    sort_dir = c_s2.selectbox("순서", ["내림차순","오름차순"], key="ng_sortdir", label_visibility="collapsed")
    if c_sb.button("↕", key="ng_sb", use_container_width=True):
        st.session_state["ng_df"] = df.sort_values(
            by='tmp_dt' if sort_by=="날짜순" else sort_by, ascending=(sort_dir=="오름차순"))
        st.rerun()
    p_lim = c_lim.selectbox("개수", [50,100,150,200], key="ng_plim", label_visibility="collapsed")
    with c_dl:
        cc, cx = st.columns(2)
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine='xlsxwriter') as w: df.to_excel(w, index=False)
        cc.download_button("📑 CSV", df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig'),
                           "나라장터_공고.csv", "text/csv", use_container_width=True)
        cx.download_button("📊 Excel", out.getvalue(), "나라장터_공고.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    total = max((len(df)-1)//p_lim+1, 1)
    curr  = st.session_state.get("ng_pnum", 1)
    st.dataframe(df[show_cols].iloc[(curr-1)*p_lim: curr*p_lim],
                 use_container_width=True, height=500)

    pg = st.columns([1,10,1])
    with pg[1]:
        bc = st.columns(14)
        sp = max(1, curr-4); ep = min(total, sp+9)
        if bc[0].button("«", key="ng_f10"): st.session_state["ng_pnum"]=max(1,curr-10); st.rerun()
        if bc[1].button("‹", key="ng_f1"):  st.session_state["ng_pnum"]=max(1,curr-1);  st.rerun()
        for j, p in enumerate(range(sp, ep+1)):
            if bc[j+2].button(str(p), key=f"ng_pg_{p}", type="primary" if p==curr else "secondary"):
                st.session_state["ng_pnum"]=p; st.rerun()
        if bc[12].button("›", key="ng_n1"):  st.session_state["ng_pnum"]=min(total,curr+1);  st.rerun()
        if bc[13].button("»", key="ng_n10"): st.session_state["ng_pnum"]=min(total,curr+10); st.rerun()
