import streamlit as st
import json
import requests
from google.oauth2 import service_account
import google.auth.transport.requests
from googleapiclient.discovery import build
import pandas as pd
import io
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

st.title("📢 나라장터 공고")

NOTICE_FOLDER_ID = "1AsvVmayEmTtY92d1SfXxNi6bL0Zjw5mg"
SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets.readonly'
]

# 영어 컬럼 → 한글 매핑
COL_RENAME = {
    "dminsttNm":    "수요기관명",
    "bidNtceNm":    "공고명",
    "presmptPrce":  "추정가격",
    "bidClsDt":     "입찰마감일시",
    "bidNtceDtlUrl":"공고문",
}
DISPLAY_COLS = list(COL_RENAME.values()) + ["공고유형"]

@st.cache_resource
def get_notice_drive_service():
    info  = json.loads(st.secrets["GOOGLE_AUTH_JSON"])
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build('drive', 'v3', credentials=creds), creds

@st.cache_data(ttl=3600)
def fetch_notice_csv_by_year(cat_name, year):
    svc, creds = get_notice_drive_service()
    creds.refresh(google.auth.transport.requests.Request())
    hdrs = {'Authorization': f'Bearer {creds.token}'}

    file_name = f"나라장터_공고_{cat_name}_{year}년.csv"
    files = svc.files().list(
        q=f"name='{file_name}' and '{NOTICE_FOLDER_ID}' in parents and trashed=false",
        fields='files(id, name)'
    ).execute().get('files', [])

    if not files:
        return pd.DataFrame()

    resp = requests.get(
        f"https://www.googleapis.com/drive/v3/files/{files[0]['id']}?alt=media",
        headers=hdrs
    )
    try:
        return pd.read_csv(io.BytesIO(resp.content), encoding='utf-8-sig', low_memory=False)
    except Exception:
        return pd.read_csv(io.BytesIO(resp.content), encoding='cp949', low_memory=False)

def load_notice_data(cat_name, s_s, e_s):
    start_year = int(s_s[:4])
    end_year   = int(e_s[:4])
    dfs = []
    for year in range(start_year, end_year + 1):
        df_y = fetch_notice_csv_by_year(cat_name, year)
        if not df_y.empty:
            dfs.append(df_y)
    if not dfs:
        return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)
    if 'bidNtceDt' in df.columns:
        df['tmp_dt'] = df['bidNtceDt'].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8]
    else:
        df['tmp_dt'] = "0"
    return df[(df['tmp_dt'] >= s_s) & (df['tmp_dt'] <= e_s)].copy()

NOTICE_CATS = ['공사', '물품', '용역']

# ── 세션 초기화 ──
today     = date.today()
yesterday = today - relativedelta(days=1)
if "ng_sd" not in st.session_state:
    st.session_state["ng_sd"] = (today - relativedelta(months=6)).strftime('%Y-%m-%d')
if "ng_ed" not in st.session_state:
    st.session_state["ng_ed"] = yesterday.strftime('%Y-%m-%d')
if "ng_df" not in st.session_state:
    st.session_state["ng_df"] = None

# ── 퀵버튼 ──
yed = yesterday.strftime('%Y-%m-%d')
qb  = st.columns([0.55,0.55,0.65,0.65,0.65,0.65,0.65,0.65,6])
QUICK = [
    ("어제",  "ng_d1", yed,                                                    yed),
    ("1주",   "ng_d7", (yesterday-relativedelta(days=6)).strftime('%Y-%m-%d'),  yed),
    ("1개월", "ng_m1", (yesterday-relativedelta(months=1)).strftime('%Y-%m-%d'),yed),
    ("3개월", "ng_m3", (yesterday-relativedelta(months=3)).strftime('%Y-%m-%d'),yed),
    ("6개월", "ng_m6", (yesterday-relativedelta(months=6)).strftime('%Y-%m-%d'),yed),
    ("9개월", "ng_m9", (yesterday-relativedelta(months=9)).strftime('%Y-%m-%d'),yed),
    ("1년",   "ng_y1", (yesterday-relativedelta(years=1)).strftime('%Y-%m-%d'), yed),
    ("2년",   "ng_y2", (yesterday-relativedelta(years=2)).strftime('%Y-%m-%d'), yed),
]
for idx, (label, key, sd_q, ed_q) in enumerate(QUICK):
    if qb[idx].button(label, key=key):
        st.session_state["ng_sd"] = sd_q
        st.session_state["ng_ed"] = ed_q
        st.rerun()

# ── 검색 조건 ──
c1,c2,c3,c4,c5,c6,c7,c8 = st.columns([1.1,1.1,0.9,1.0,2.4,0.7,2.4,1.1])
sd_in       = c1.text_input("시작일", key="ng_sd", placeholder="YYYY-MM-DD", label_visibility="collapsed")
ed_in       = c2.text_input("종료일", key="ng_ed", placeholder="YYYY-MM-DD", label_visibility="collapsed")
notice_type = c3.selectbox("공고유형", ["전체","공사","물품","용역"], key="ng_nt", label_visibility="collapsed")
f_val       = c4.selectbox("필드", ["ALL","수요기관명","공고명"], key="ng_f", label_visibility="collapsed")
k1_val      = c5.text_input("검색어1", key="ng_k1", placeholder="🔎 검색어", label_visibility="collapsed")
l_val       = c6.selectbox("논리", ["NONE","AND","OR"], key="ng_l", label_visibility="collapsed")
k2_val      = c7.text_input("검색어2", key="ng_k2", placeholder="🔎 검색어2", label_visibility="collapsed")
search_exe  = c8.button("🔍 검색실행", key="ng_search", use_container_width=True, type="primary")

# ── 검색 실행 ──
if search_exe:
    try:
        sd = datetime.strptime(sd_in.strip(), '%Y-%m-%d').date()
        ed = datetime.strptime(ed_in.strip(), '%Y-%m-%d').date()
    except ValueError:
        st.warning("날짜 형식: YYYY-MM-DD")
        st.stop()

    s_s = sd.strftime('%Y%m%d')
    e_s = ed.strftime('%Y%m%d')

    with st.spinner("데이터를 불러오는 중..."):
        try:
            cats_to_load = NOTICE_CATS if notice_type == "전체" else [notice_type]
            dfs = []
            for cat in cats_to_load:
                df_t = load_notice_data(cat, s_s, e_s)
                if not df_t.empty:
                    df_t['공고유형'] = cat
                    dfs.append(df_t)

            df_f = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

            if not df_f.empty:
                # ── 컬럼명 한글로 변환 ──
                df_f = df_f.rename(columns=COL_RENAME)

                # ── 추정가격 쉼표 포맷 (숫자만) ──
                if '추정가격' in df_f.columns:
                    def fmt_price(v):
                        try:
                            return f"{int(float(str(v).replace(',', ''))):,}"
                        except:
                            return v
                    df_f['추정가격'] = df_f['추정가격'].apply(fmt_price)

                # ── 키워드 검색 ──
                if k1_val and k1_val.strip():
                    def get_mask(df, kw, field):
                        if field != "ALL" and field in df.columns:
                            return df[field].astype(str).str.contains(kw, case=False, na=False)
                        return df.astype(str).apply(lambda x: x.str.contains(kw, case=False, na=False)).any(axis=1)

                    m1 = get_mask(df_f, k1_val.strip(), f_val)
                    if l_val == "AND" and k2_val.strip():
                        df_f = df_f[m1 & get_mask(df_f, k2_val.strip(), f_val)]
                    elif l_val == "OR" and k2_val.strip():
                        df_f = df_f[m1 | get_mask(df_f, k2_val.strip(), f_val)]
                    else:
                        df_f = df_f[m1]

            st.session_state["ng_df"] = df_f.sort_values('tmp_dt', ascending=False) if not df_f.empty else pd.DataFrame()
            st.session_state["ng_pnum"] = 1

        except Exception as e:
            st.error(f"오류: {e}")
            import traceback
            st.code(traceback.format_exc())
            st.session_state["ng_df"] = pd.DataFrame()

# ── 결과 출력 ──
df = st.session_state.get("ng_df")
if df is None:
    st.info("검색 조건을 입력하고 검색실행 버튼을 눌러주세요.")
elif len(df) == 0:
    st.warning("검색 결과가 없습니다.")
else:
    show_cols = [c for c in DISPLAY_COLS if c in df.columns]

    p_lim = st.selectbox("페이지당 행수", [50,100,150,200], key="ng_plim")
    total = max((len(df)-1)//p_lim+1, 1)
    curr  = st.session_state.get("ng_pnum", 1)

    st.caption(f"총 {len(df):,}건 / {total}페이지")

    # ── 공고문 컬럼을 링크로 ──
    col_cfg = {}
    if '공고문' in show_cols:
        col_cfg['공고문'] = st.column_config.LinkColumn(
            "공고문",
            display_text="바로가기"
        )

    st.dataframe(
        df[show_cols].iloc[(curr-1)*p_lim: curr*p_lim],
        use_container_width=True,
        height=500,
        column_config=col_cfg
    )

    # ── 페이지네이션 ──
    cols = st.columns(14)
    sp = max(1, curr-4); ep = min(total, sp+9)
    if cols[0].button("«", key="ng_f10"): st.session_state["ng_pnum"]=max(1,curr-10); st.rerun()
    if cols[1].button("‹",  key="ng_f1"):  st.session_state["ng_pnum"]=max(1,curr-1);  st.rerun()
    for j,p in enumerate(range(sp,ep+1)):
        if cols[j+2].button(str(p), key=f"ng_pg_{p}", type="primary" if p==curr else "secondary"):
            st.session_state["ng_pnum"]=p; st.rerun()
    if cols[12].button("›",  key="ng_n1"):  st.session_state["ng_pnum"]=min(total,curr+1);  st.rerun()
    if cols[13].button("»", key="ng_n10"): st.session_state["ng_pnum"]=min(total,curr+10); st.rerun()

    # ── 다운로드 ──
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='xlsxwriter') as w: df.to_excel(w, index=False)
    dc1, dc2 = st.columns(2)
    dc1.download_button("📑 CSV", df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig'),
                        "나라장터_공고.csv", "text/csv")
    dc2.download_button("📊 Excel", out.getvalue(), "나라장터_공고.xlsx",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
