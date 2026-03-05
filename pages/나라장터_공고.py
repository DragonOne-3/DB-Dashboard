import streamlit as st
import json
import google.auth.transport.requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
import requests
import pandas as pd
import io
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

st.title("📢 나라장터 공고 (용역 테스트)")

# ── Google Drive 연결 ──
@st.cache_resource
def get_drive_service():
    info  = json.loads(st.secrets["GOOGLE_AUTH_JSON"])
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=['https://www.googleapis.com/auth/drive.readonly',
                      'https://www.googleapis.com/auth/spreadsheets.readonly'])
    return build('drive', 'v3', credentials=creds), creds

def fetch_csv(file_name):
    svc, creds = get_drive_service()
    request = google.auth.transport.requests.Request()
    if not creds.valid:
        creds.refresh(request)
    hdrs  = {'Authorization': f'Bearer {creds.token}'}
    files = svc.files().list(
        q=f"name='{file_name}' and trashed=false", fields='files(id)'
    ).execute().get('files', [])
    if not files:
        st.warning(f"{file_name} 파일을 찾을 수 없습니다.")
        return pd.DataFrame()
    resp = requests.get(
        f"https://www.googleapis.com/drive/v3/files/{files[0]['id']}?alt=media",
        headers=hdrs)
    try:
        return pd.read_csv(io.BytesIO(resp.content), encoding='utf-8-sig', low_memory=False)
    except Exception:
        return pd.read_csv(io.BytesIO(resp.content), encoding='cp949', low_memory=False)

# ── 용역만 테스트 ──
DISPLAY_COLS = ["dminsttNm", "bidNtceNm", "presmptPrce", "bidClsDt", "bidNtceDtlUrl"]

today     = date.today()
yesterday = today - relativedelta(days=1)
if "ng_sd" not in st.session_state:
    st.session_state["ng_sd"] = (today - relativedelta(months=1)).strftime('%Y-%m-%d')
if "ng_ed" not in st.session_state:
    st.session_state["ng_ed"] = yesterday.strftime('%Y-%m-%d')
if "ng_df" not in st.session_state:
    st.session_state["ng_df"] = None

c1, c2, c3 = st.columns([1.5, 1.5, 1])
sd_in = c1.text_input("시작일", key="ng_sd", placeholder="YYYY-MM-DD", label_visibility="collapsed")
ed_in = c2.text_input("종료일", key="ng_ed", placeholder="YYYY-MM-DD", label_visibility="collapsed")
search_exe = c3.button("🔍 검색실행", key="ng_search", use_container_width=True, type="primary")

if search_exe:
    try:
        sd = datetime.strptime(sd_in.strip(), '%Y-%m-%d').date()
        ed = datetime.strptime(ed_in.strip(), '%Y-%m-%d').date()
    except ValueError:
        st.warning("날짜 형식: YYYY-MM-DD")
        st.stop()

    s_s = sd.strftime('%Y%m%d')
    e_s = ed.strftime('%Y%m%d')

    with st.spinner("용역 데이터를 불러오는 중..."):
        try:
            st.write("📂 파일 로딩 시작...")
            df_t = fetch_csv('나라장터_공고_용역.csv')
            st.write(f"📂 파일 로딩 완료: {len(df_t)}행")

            if not df_t.empty:
                df_t['공고유형'] = '용역'
                st.write(f"📋 컬럼 목록: {list(df_t.columns)}")

                if 'bidNtceDt' in df_t.columns:
                    df_t['tmp_dt'] = df_t['bidNtceDt'].astype(str).str.replace(r'[^0-9]','',regex=True).str[:8]
                else:
                    df_t['tmp_dt'] = "0"

                df_f = df_t[(df_t['tmp_dt'] >= s_s) & (df_t['tmp_dt'] <= e_s)].copy()
                st.write(f"📅 날짜 필터 후: {len(df_f)}행")

                st.session_state["ng_df"] = df_f.sort_values('tmp_dt', ascending=False)
                st.session_state["ng_pnum"] = 1
            else:
                st.session_state["ng_df"] = pd.DataFrame()

        except Exception as e:
            st.error(f"❌ 오류 발생: {e}")
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
    show_cols = [c for c in DISPLAY_COLS if c in df.columns] or list(df.columns[:10])
    st.caption(f"총 {len(df):,}건")
    st.dataframe(df[show_cols].head(50), use_container_width=True, height=500)
