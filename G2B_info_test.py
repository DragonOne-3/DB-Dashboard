import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re
import uuid
import threading
from difflib import SequenceMatcher

st.set_page_config(
    page_title="지자체 유지보수 계약 현황",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

RE_NONDIGIT     = re.compile(r"[^0-9]")
RE_MONTH_ONLY   = re.compile(r"^[0-3]개월")
RE_CONTRACT_NUM = re.compile(r"\d+차분?|\d+")

RE_NORMALIZE = re.compile(
    r"\d{4}[~\-～]\d{4}년?"
    r"|\d{4}년\s*\d{1,2}월"
    r"|\d{4}년"
    r"|\d{1,2}차분?"
    r"|장기계속\s*\d*차?"
    r"|\(.*?\)"
    r"|연장분|추가분|수정"
)

st.markdown("""
<style>
  html, body, [class*="css"] { font-size: 18px; }
  .stApp { background: #f4f6fb; }
  div[data-testid="stTabs"] > div:first-child { border-bottom: 2px solid #e2e8f0; margin-bottom: 1.5rem; }
  button[data-baseweb="tab"] { font-size: 1.1rem !important; font-weight: 600 !important; padding: 0.8rem 2rem !important; color: #64748b !important; }
  button[data-baseweb="tab"][aria-selected="true"] { color: #2563eb !important; border-bottom: 3px solid #2563eb !important; }
  .hero-blue { background: linear-gradient(135deg, #1e3a5f 0%, #2563eb 100%); border-radius: 16px; padding: 2.5rem 3rem; margin-bottom: 2rem; text-align: center; box-shadow: 0 4px 20px rgba(37,99,235,.25); }
  .hero-green { background: linear-gradient(135deg, #064e3b 0%, #059669 100%); border-radius: 16px; padding: 2.5rem 3rem; margin-bottom: 2rem; text-align: center; box-shadow: 0 4px 20px rgba(5,150,105,.25); }
  .hero-blue h1, .hero-green h1 { font-size: 2.3rem; font-weight: 800; color: #fff; margin: 0 0 .6rem; letter-spacing: -.5px; }
  .hero-blue p, .hero-green p { color: rgba(255,255,255,.85); font-size: 1.1rem; margin: 0; }
  .search-panel { background: #fff; border: 1px solid #e2e8f0; border-radius: 14px; padding: 2rem 2.5rem; margin-bottom: 1.5rem; box-shadow: 0 2px 8px rgba(0,0,0,.06); }
  .stat-card { background: #fff; border: 1px solid #e2e8f0; border-radius: 12px; padding: 1.5rem 1.8rem; text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,.05); }
  .stat-card-red { background: linear-gradient(135deg,#fff5f5,#fff); border: 1.5px solid #fca5a5; border-radius: 12px; padding: 1.5rem 1.8rem; text-align: center; box-shadow: 0 2px 12px rgba(220,38,38,.10); }
  .stat-num-blue  { font-size: 2.3rem; font-weight: 800; color: #2563eb; line-height: 1.1; }
  .stat-num-green { font-size: 2.3rem; font-weight: 800; color: #059669; line-height: 1.1; }
  .stat-num-red   { font-size: 2.3rem; font-weight: 800; color: #dc2626; line-height: 1.1; }
  .stat-label { font-size: 1rem; color: #64748b; margin-top: .5rem; font-weight: 500; letter-spacing: .3px; }
  .stat-label-red { font-size: 1rem; color: #dc2626; margin-top: .5rem; font-weight: 600; letter-spacing: .3px; }
  .section-title { font-size: 1.3rem; font-weight: 700; color: #1e293b; margin-bottom: 1rem; letter-spacing: -.2px; }
  div[data-testid="stButton"] > button { font-size: 1.1rem !important; height: auto !important; padding: 0.6rem 1rem !important; }
  div[data-testid="stButton"] > button[kind="primary"] { background: linear-gradient(135deg, #2563eb, #7c3aed) !important; border: none !important; color: #fff !important; font-weight: 700 !important; border-radius: 8px !important; box-shadow: 0 3px 10px rgba(37,99,235,.35) !important; }
  div[data-testid="stDownloadButton"] > button { background: #fff !important; border: 1.5px solid #2563eb !important; color: #2563eb !important; font-weight: 600 !important; border-radius: 8px !important; }
  .copy-notice { background: #ecfdf5; border: 1px solid #6ee7b7; border-radius: 8px; padding: .6rem 1rem; font-size: .9rem; color: #065f46; margin-top: .5rem; }
  hr { border-color: #e2e8f0; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# 로그 기록
# ─────────────────────────────────────────────
def _write_log(event_type: str, detail: str, session_id: str, ip: str):
    try:
        auth_json = os.environ.get("GOOGLE_AUTH_JSON")
        if not auth_json:
            return
        creds_dict = json.loads(auth_json)
        scope      = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds      = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client     = gspread.authorize(creds)
        ws         = client.open("나라장터_usage_log2").get_worksheet(0)
        now        = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ws.append_row([now, session_id, ip, event_type, detail])
    except Exception:
        pass

def _get_client_ip() -> str:
    try:
        from streamlit.web.server.websocket_headers import _get_websocket_headers
        headers = _get_websocket_headers()
        if headers:
            return (
                headers.get("X-Forwarded-For", "")
                or headers.get("X-Real-Ip", "")
                or "unknown"
            ).split(",")[0].strip()
    except Exception:
        pass
    return "unknown"

def log_event(event_type: str, detail: str = "-"):
    session_id = st.session_state.get("session_id", "unknown")
    ip         = _get_client_ip()
    threading.Thread(
        target=_write_log,
        args=(event_type, detail, session_id, ip),
        daemon=True
    ).start()

# ─────────────────────────────────────────────
# 상수
# ─────────────────────────────────────────────
FULL_DISTRICT_LIST = [
    "서울특별시", "서울특별시 종로구", "서울특별시 중구", "서울특별시 용산구", "서울특별시 성동구", "서울특별시 광진구", "서울특별시 동대문구", "서울특별시 중랑구", "서울특별시 성북구", "서울특별시 강북구", "서울특별시 도봉구", "서울특별시 노원구", "서울특별시 은평구", "서울특별시 서대문구", "서울특별시 마포구", "서울특별시 양천구", "서울특별시 강서구", "서울특별시 구로구", "서울특별시 금천구", "서울특별시 영등포구", "서울특별시 동작구", "서울특별시 관악구", "서울특별시 서초구", "서울특별시 강남구", "서울특별시 송파구", "서울특별시 강동구",
    "부산광역시", "부산광역시 중구", "부산광역시 서구", "부산광역시 동구", "부산광역시 영도구", "부산광역시 부산진구", "부산광역시 동래구", "부산광역시 남구", "부산광역시 북구", "부산광역시 해운대구", "부산광역시 사하구", "부산광역시 금정구", "부산광역시 강서구", "부산광역시 연제구", "부산광역시 수영구", "부산광역시 사상구", "부산광역시 기장군",
    "대구광역시", "대구광역시 중구", "대구광역시 동구", "대구광역시 서구", "대구광역시 남구", "대구광역시 북구", "대구광역시 수성구", "대구광역시 달서구", "대구광역시 달성군", "대구광역시 군위군",
    "인천광역시", "인천광역시 중구", "인천광역시 동구", "인천광역시 미추홀구", "인천광역시 연수구", "인천광역시 남동구", "인천광역시 부평구", "인천광역시 계양구", "인천광역시 서구", "인천광역시 강화군", "인천광역시 옹진군",
    "광주광역시", "광주광역시 동구", "광주광역시 서구", "광주광역시 남구", "광주광역시 북구", "광주광역시 광산구",
    "대전광역시", "대전광역시 동구", "대전광역시 중구", "대전광역시 서구", "대전광역시 유성구", "대전광역시 대덕구",
    "울산광역시", "울산광역시 중구", "울산광역시 남구", "울산광역시 동구", "울산광역시 북구", "울산광역시 울주군",
    "세종특별자치시",
    "경기도 수원시", "경기도 성남시", "경기도 의정부시", "경기도 안양시", "경기도 부천시", "경기도 광명시", "경기도 평택시", "경기도 동두천시", "경기도 안산시", "경기도 고양시", "경기도 과천시", "경기도 구리시", "경기도 남양주시", "경기도 오산시", "경기도 시흥시", "경기도 군포시", "경기도 의왕시", "경기도 하남시", "경기도 용인시", "경기도 파주시", "경기도 이천시", "경기도 안성시", "경기도 김포시", "경기도 화성시", "경기도 광주시", "경기도 양주시", "경기도 포천시", "경기도 여주시", "경기도 연천군", "경기도 가평군", "경기도 양평군",
    "강원특별자치도 춘천시", "강원특별자치도 원주시", "강원특별자치도 강릉시", "강원특별자치도 동해시", "강원특별자치도 태백시", "강원특별자치도 속초시", "강원특별자치도 삼척시", "강원특별자치도 홍천군", "강원특별자치도 횡성군", "강원특별자치도 영월군", "강원특별자치도 평창군", "강원특별자치도 정선군", "강원특별자치도 철원군", "강원특별자치도 화천군", "강원특별자치도 양구군", "강원특별자치도 인제군", "강원특별자치도 고성군", "강원특별자치도 양양군",
    "충청북도 청주시", "충청북도 충주시", "충청북도 제천시", "충청북도 보은군", "충청북도 옥천군", "충청북도 영동군", "충청북도 증평군", "충청북도 진천군", "충청북도 괴산군", "충청북도 음성군", "충청북도 단양군",
    "충청남도 천안시", "충청남도 공주시", "충청남도 보령시", "충청남도 아산시", "충청남도 서산시", "충청남도 논산시", "충청남도 계룡시", "충청남도 당진시", "충청남도 금산군", "충청남도 부여군", "충청남도 서천군", "충청남도 청양군", "충청남도 홍성군", "충청남도 예산군", "충청남도 태안군",
    "전북특별자치도 전주시", "전북특별자치도 군산시", "전북특별자치도 익산시", "전북특별자치도 정읍시", "전북특별자치도 남원시", "전북특별자치도 김제시", "전북특별자치도 완주군", "전북특별자치도 진안군", "전북특별자치도 무주군", "전북특별자치도 장수군", "전북특별자치도 임실군", "전북특별자치도 순창군", "전북특별자치도 고창군", "전북특별자치도 부안군",
    "전라남도 목포시", "전라남도 여수시", "전라남도 순천시", "전라남도 나주시", "전라남도 광양시", "전라남도 담양군", "전라남도 곡성군", "전라남도 구례군", "전라남도 고흥군", "전라남도 보성군", "전라남도 화순군", "전라남도 장흥군", "전라남도 강진군", "전라남도 해남군", "전라남도 영암군", "전라남도 무안군", "전라남도 함평군", "전라남도 영광군", "전라남도 장성군", "전라남도 완도군", "전라남도 진도군", "전라남도 신안군",
    "경상북도 포항시", "경상북도 경주시", "경상북도 김천시", "경상북도 안동시", "경상북도 구미시", "경상북도 영주시", "경상북도 상주시", "경상북도 문경시", "경상북도 경산시", "경상북도 의성군", "경상북도 청송군", "경상북도 영양군", "경상북도 영덕군", "경상북도 청도군", "경상북도 고령군", "경상북도 성주군", "경상북도 칠곡군", "경상북도 예천군", "경상북도 봉화군", "경상북도 울진군", "경상북도 울릉군",
    "경상남도 창원시", "경상남도 진주시", "경상남도 통영시", "경상남도 사천시", "경상남도 김해시", "경상남도 밀양시", "경상남도 거제시", "경상남도 양산시", "경상남도 의령군", "경상남도 함안군", "경상남도 창녕군", "경상남도 고성군", "경상남도 남해군", "경상남도 하동군", "경상남도 산청군", "경상남도 함양군", "경상남도 거창군", "경상남도 합천군",
    "제주특별자치도", "제주특별자치도 제주시", "제주특별자치도 서귀포시",
]

METRO_LIST = [
    "전국", "서울특별시", "부산광역시", "대구광역시", "인천광역시", "광주광역시",
    "대전광역시", "울산광역시", "세종특별자치시", "경기도", "강원특별자치도",
    "충청북도", "충청남도", "전북특별자치도", "전라남도", "경상북도", "경상남도", "제주특별자치도",
]

PAGE_SIZE = 50

# ─────────────────────────────────────────────
# session_state 초기화
# ─────────────────────────────────────────────
if "session_id" not in st.session_state:
    st.session_state["session_id"] = str(uuid.uuid4())[:8]
    log_event("접속")

defaults = {
    "search_done": False, "search_region": "전국", "radio_region": "전국", "page": 1,
    "plan_search_done": False, "plan_search_region": "전국", "plan_radio_region": "전국", "plan_page": 1,
    "gong_search_done": False, "gong_search_region": "전국", "gong_radio_region": "전국", "gong_page": 1,
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ─────────────────────────────────────────────
# 공통 함수
# ─────────────────────────────────────────────
def get_metro(a: str) -> str:
    for m in METRO_LIST[1:]:
        if str(a).startswith(m):
            return m
    return "기타"

def parse_date_series(s: pd.Series) -> pd.Series:
    cleaned = s.astype(str).str.replace(RE_NONDIGIT, "", regex=True).str[:8]
    return pd.to_datetime(cleaned, format="%Y%m%d", errors="coerce")

# ─────────────────────────────────────────────
# 계약 만료일 & 남은기간 계산
# ─────────────────────────────────────────────
def calculate_logic_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    cntrct_date  = parse_date_series(df["계약일자"])
    start_date   = parse_date_series(df["착수일자"])
    this_finish  = parse_date_series(df["금차완수일자"])
    total_finish = parse_date_series(df["총완수일자"])
    period_raw   = df["계약기간"].astype(str)

    this_vals  = period_raw.str.extract(r"금차\s*[:\s]?\s*(\d+)",              expand=False).astype(float).fillna(0)
    total_vals = period_raw.str.extract(r"(?:총차|총용역|총)\s*[:\s]?\s*(\d+)", expand=False).astype(float).fillna(0)

    base_date = start_date.combine_first(cntrct_date)
    expire    = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")

    cond1 = (this_vals != total_vals) & total_finish.notna()
    expire = expire.where(~cond1, total_finish)

    cond2 = expire.isna() & (total_vals > 0) & base_date.notna()
    if cond2.any():
        expire[cond2] = base_date[cond2] + pd.to_timedelta(total_vals[cond2].astype(int) - 1, unit="D")

    cond3 = expire.isna()
    if cond3.any():
        expire[cond3] = parse_date_series(period_raw[cond3])

    cond4 = expire.isna() & total_finish.notna()
    expire[cond4] = total_finish[cond4]

    day_diff           = (total_finish - this_finish).dt.days.abs()
    is_close           = this_finish.notna() & total_finish.notna() & (day_diff < 10)
    contract_to_finish = (this_finish - cntrct_date).dt.days
    is_short_term      = contract_to_finish.notna() & (contract_to_finish < 30)
    can_override       = is_close | (is_short_term & total_finish.notna())

    override   = total_finish.notna() & expire.notna() & (total_finish > expire) & can_override
    expire[override] = total_finish[override]
    only_total = total_finish.notna() & expire.isna() & can_override
    expire[only_total] = total_finish[only_total]

    today      = pd.Timestamp(datetime.now().date())
    expire_str = expire.dt.strftime("%Y-%m-%d").fillna("정보부족")
    remaining  = pd.Series("정보부족", index=df.index)

    is_expired = expire.notna() & (expire < today)
    is_active  = expire.notna() & (expire >= today)
    remaining[is_expired] = "만료됨"

    if is_active.any():
        delta     = (expire[is_active] - today).dt.days
        months    = (delta // 30).astype(int)
        days_left = (delta % 30).astype(int)
        remaining[is_active] = months.astype(str) + "개월 " + days_left.astype(str) + "일"

    return pd.DataFrame({"★가공_계약만료일": expire_str, "남은기간": remaining})

def clean_contract_name(name: str) -> str:
    return RE_CONTRACT_NUM.sub("", str(name).replace(" ", ""))

# ─────────────────────────────────────────────
# 반복수주 탐지
# ─────────────────────────────────────────────
def normalize_contract_name(name: str) -> str:
    s = RE_NORMALIZE.sub("", str(name))
    s = re.sub(r"\s+", "", s)
    return s.strip()

def name_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()

def detect_repeat_contracts(df: pd.DataFrame, threshold: float = 0.80) -> pd.DataFrame:
    needed = ["★가공_수요기관", "★가공_계약명", "★가공_업체명", "★가공_계약금액", "계약일자"]
    work = df[needed].copy()
    work["정규화명"] = work["★가공_계약명"].apply(normalize_contract_name)
    work["계약일자_dt"] = pd.to_datetime(
        work["계약일자"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8],
        format="%Y%m%d", errors="coerce"
    )
    work["계약년도"] = work["계약일자_dt"].dt.year

    results = []
    for agency, ag_df in work.groupby("★가공_수요기관"):
        names   = ag_df["정규화명"].tolist()
        indices = ag_df.index.tolist()
        visited = [False] * len(names)

        for i in range(len(names)):
            if visited[i] or not names[i]:
                continue
            cluster_idx = [i]
            visited[i] = True
            for j in range(i + 1, len(names)):
                if not visited[j] and name_similarity(names[i], names[j]) >= threshold:
                    cluster_idx.append(j)
                    visited[j] = True

            if len(cluster_idx) < 2:
                continue

            cluster_rows = ag_df.loc[[indices[k] for k in cluster_idx]]

            for company, co_df in cluster_rows.groupby("★가공_업체명"):
                if len(co_df) < 2:
                    continue
                if co_df["계약년도"].dropna().nunique() < 2:
                    continue

                amt   = pd.to_numeric(co_df["★가공_계약금액"], errors="coerce").fillna(0)
                dates = co_df["계약일자_dt"].dropna()

                results.append({
                    "수요기관":      agency,
                    "업체명":        company,
                    "수주횟수":      len(co_df),
                    "수주연도":      ", ".join(map(str, sorted(co_df["계약년도"].dropna().astype(int).unique().tolist()))),
                    "대표사업명":    co_df["★가공_계약명"].iloc[0],
                    "계약금액합계":  int(amt.sum()),
                    "최초계약일":    dates.min().strftime("%Y-%m-%d") if not dates.empty else "-",
                    "최근계약일":    dates.max().strftime("%Y-%m-%d") if not dates.empty else "-",
                    "계약목록":      " / ".join(co_df["★가공_계약명"].tolist()),
                })

    if not results:
        return pd.DataFrame()

    out = pd.DataFrame(results)
    out = out.sort_values(["수주횟수", "계약금액합계"], ascending=False).reset_index(drop=True)
    return out

# ─────────────────────────────────────────────
# 데이터 로드 — 계약내역
# ★ 변경 포인트 1: 반환값이 (화면표시용 df, 반복수주탐지용 df) 튜플
# ★ 변경 포인트 2: 만료 계약 포함 기간 1년 → 3년
# ★ 변경 포인트 3: 반복수주 탐지용은 만료 필터 없이 전체 보존
# ─────────────────────────────────────────────
@st.cache_resource
def get_processed_df():
    auth_json = os.environ.get("GOOGLE_AUTH_JSON")
    if not auth_json:
        st.error("❌ 'GOOGLE_AUTH_JSON' 환경 변수가 설정되지 않았습니다.")
        return pd.DataFrame(), pd.DataFrame()
    try:
        creds_dict = json.loads(auth_json)
        scope      = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds      = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client     = gspread.authorize(creds)
        ws         = client.open("나라장터_용역계약내역").get_worksheet(0)
        records    = ws.get_all_records(value_render_option="UNFORMATTED_VALUE")
    except Exception as e:
        st.error(f"❌ 시트 로드 오류: {e}")
        return pd.DataFrame(), pd.DataFrame()

    if not records:
        return pd.DataFrame(), pd.DataFrame()

    df         = pd.DataFrame(records)
    agency_col = df["★가공_수요기관"].astype(str).str.strip()
    mask       = agency_col.apply(lambda a: any(a.startswith(d) for d in FULL_DISTRICT_LIST))
    df         = df[mask & ~agency_col.str.contains("교육청", na=False)].copy()

    cn = df["★가공_계약명"].astype(str)
    df = df[
        cn.str.contains("유지", na=False) &
        cn.str.contains("통합관제|통합|CCTV", na=False) &
        ~cn.str.contains("상수도|청사|악취|미세먼지|상담실|보건소", na=False)
    ].copy()

    df[["★가공_계약만료일", "남은기간"]] = calculate_logic_vectorized(df).values
    df["temp_date"] = pd.to_datetime(
        df["계약일자"].astype(str).str.replace(RE_NONDIGIT, "", regex=True).str[:8],
        format="%Y%m%d", errors="coerce"
    )
    df["contract_group_key"] = df["★가공_계약명"].apply(clean_contract_name)
    df = df.sort_values(
        ["★가공_수요기관", "contract_group_key", "★가공_업체명", "temp_date"],
        ascending=[True, True, True, False],
    )

    today           = pd.Timestamp(datetime.now().date())
    three_years_ago = today - pd.DateOffset(years=3)   # ★ 3년으로 확장

    # ── 화면 표시용: 진행중 + 3년 이내 만료 ──
    active_df = df[df["남은기간"] != "만료됨"].drop_duplicates(
        ["★가공_수요기관", "contract_group_key", "★가공_업체명"], keep="first"
    )
    recent_expired_df = df[df["남은기간"] == "만료됨"].copy()
    expire_dt         = pd.to_datetime(recent_expired_df["★가공_계약만료일"], errors="coerce")
    recent_expired_df = recent_expired_df[expire_dt >= three_years_ago].drop_duplicates(
        ["★가공_수요기관", "contract_group_key", "★가공_업체명"], keep="first"
    )
    display_out = pd.concat([active_df, recent_expired_df], ignore_index=True)
    display_out["광역단위"] = display_out["★가공_수요기관"].astype(str).apply(get_metro)

    # ── 반복수주 탐지용: 만료 필터 없이 전체 (중복만 제거) ──
    repeat_base = df.drop_duplicates(
        ["★가공_수요기관", "contract_group_key", "★가공_업체명"], keep="first"
    ).copy()
    repeat_base["광역단위"] = repeat_base["★가공_수요기관"].astype(str).apply(get_metro)

    # 금액 보정 (두 df 모두)
    for target in [display_out, repeat_base]:
        total_amt = pd.to_numeric(target["★가공_계약금액"], errors="coerce").fillna(0)
        if "금차계약금액" in target.columns:
            sub_amt = pd.to_numeric(target["금차계약금액"], errors="coerce").fillna(0)
            target["★가공_계약금액"] = sub_amt.where(sub_amt != 0, total_amt).astype(int)
        else:
            target["★가공_계약금액"] = total_amt.astype(int)

    return display_out, repeat_base

# ─────────────────────────────────────────────
# 데이터 로드 — 발주계획
# ─────────────────────────────────────────────
def parse_baljoo_date(year_val, month_val) -> pd.Timestamp:
    try:
        year  = int(str(year_val).strip())
        month = int(re.sub(r"[^0-9]", "", str(month_val))[:2])
        if 1 <= month <= 12 and year >= 2000:
            return pd.Timestamp(year=year, month=month, day=1)
    except Exception:
        pass
    return pd.NaT

@st.cache_resource
def get_baljoo_df() -> pd.DataFrame:
    auth_json = os.environ.get("GOOGLE_AUTH_JSON")
    if not auth_json:
        return pd.DataFrame()
    try:
        creds_dict = json.loads(auth_json)
        scope      = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds      = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client     = gspread.authorize(creds)
        ws         = client.open("나라장터_용역_발주계획").get_worksheet(0)
        records    = ws.get_all_records(value_render_option="UNFORMATTED_VALUE")
    except Exception as e:
        st.error(f"❌ 발주계획 시트 로드 오류: {e}")
        return pd.DataFrame()

    if not records:
        return pd.DataFrame()

    df         = pd.DataFrame(records)
    agency_col = df["기관명"].astype(str).str.strip()
    mask       = agency_col.apply(lambda a: any(a.startswith(d) for d in FULL_DISTRICT_LIST))
    df         = df[mask & ~agency_col.str.contains("교육청", na=False)].copy()

    sn           = df["사업명"].astype(str)
    include_mask = sn.str.contains("유지", na=False) & sn.str.contains("통합관제|통합|CCTV", na=False)
    exclude_mask = sn.str.contains("청사|악취|미세먼지|상담실|보건소|홈페이지|발급기|공간정보|계측기|부동산", na=False)
    df           = df[include_mask & ~exclude_mask].copy()

    df["발주일자"] = df.apply(
        lambda r: parse_baljoo_date(r.get("발주년도", ""), r.get("발주월", "")), axis=1
    )

    today      = pd.Timestamp(datetime.now().date())
    six_months = today - relativedelta(months=6)
    df         = df[df["발주일자"].notna() & (df["발주일자"] >= six_months)].copy()

    df["광역단위"]    = df["기관명"].astype(str).apply(get_metro)
    df["합계발주금액"] = pd.to_numeric(df["합계발주금액"], errors="coerce").fillna(0).astype(int)
    df["발주월_표시"] = df["발주일자"].dt.strftime("%Y년 %m월").fillna("정보없음")

    return df

# ─────────────────────────────────────────────
# 데이터 로드 — 공고
# ─────────────────────────────────────────────
@st.cache_resource
def get_gong_df() -> pd.DataFrame:
    auth_json = os.environ.get("GOOGLE_AUTH_JSON")
    if not auth_json:
        return pd.DataFrame()
    try:
        creds_dict = json.loads(auth_json)
        scope      = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds      = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client     = gspread.authorize(creds)
        ws         = client.open("나라장터_유지보수_공고").get_worksheet(0)
        records    = ws.get_all_records(value_render_option="UNFORMATTED_VALUE")
    except Exception as e:
        st.error(f"❌ 공고 시트 로드 오류: {e}")
        return pd.DataFrame()

    if not records:
        return pd.DataFrame()

    df         = pd.DataFrame(records)
    agency_col = df["수요기관명"].astype(str).str.strip()
    mask       = agency_col.apply(lambda a: any(a.startswith(d) for d in FULL_DISTRICT_LIST))
    df         = df[mask & ~agency_col.str.contains("교육청", na=False)].copy()

    sn           = df["입찰공고명"].astype(str)
    include_mask = sn.str.contains("유지", na=False) & sn.str.contains("통합관제|통합|CCTV", na=False)
    exclude_mask = sn.str.contains("상수도|청사|악취|미세먼지|상담실|보건소", na=False)
    df           = df[include_mask & ~exclude_mask].copy()

    df["입찰개시일시_dt"] = pd.to_datetime(
        df["입찰개시일시"].astype(str).str.replace(RE_NONDIGIT, "", regex=True).str[:8],
        format="%Y%m%d", errors="coerce"
    )
    today      = pd.Timestamp(datetime.now().date())
    six_months = today - relativedelta(months=6)
    df         = df[df["입찰개시일시_dt"].notna() & (df["입찰개시일시_dt"] >= six_months)].copy()

    df["입찰마감일시_dt"] = pd.to_datetime(
        df["입찰마감일시"].astype(str).str.replace(RE_NONDIGIT, "", regex=True).str[:8],
        format="%Y%m%d", errors="coerce"
    )
    df["입찰개시일_표시"] = df["입찰개시일시_dt"].dt.strftime("%Y-%m-%d").fillna("-")
    df["입찰마감일_표시"] = df["입찰마감일시_dt"].dt.strftime("%Y-%m-%d").fillna("-")
    df["마감여부"]        = df["입찰마감일시_dt"].apply(
        lambda d: "마감" if pd.notna(d) and d < today else "진행중"
    )
    df["배정예산금액"] = pd.to_numeric(df["배정예산금액"], errors="coerce").fillna(0).astype(int)
    df["광역단위"]     = df["수요기관명"].astype(str).apply(get_metro)

    return df

# ─────────────────────────────────────────────
# HTML 테이블 — 계약내역
# ─────────────────────────────────────────────
def status_badge(val: str) -> str:
    if "만료됨" in val or "계약만료" in val:
        color, bg = "#b91c1c", "#fef2f2"
    elif RE_MONTH_ONLY.match(val):
        color, bg = "#c2410c", "#fff7ed"
    elif any(x in val for x in ("정보부족", "오류", "계산불가")):
        color, bg = "#64748b", "#f1f5f9"
    else:
        color, bg = "#15803d", "#f0fdf4"
    return (f'<span style="background:{bg};color:{color};padding:3px 10px;'
            f'border-radius:999px;font-size:.85rem;font-weight:600;white-space:nowrap;">{val}</span>')

def render_info_table(df: pd.DataFrame) -> str:
    COL_LABELS = {
        "수요기관": "수요기관", "계약명": "계약명", "업체명": "업체명",
        "계약금액": "계약금액(원)", "계약일자": "계약일자", "착수일자": "착수일자",
        "계약만료일": "계약만료일", "남은기간": "남은기간", "URL": "상세",
    }
    TH = ("background:#1e3a5f;color:#fff;padding:12px 14px;font-size:0.95rem;font-weight:700;"
          "white-space:nowrap;border-bottom:2px solid #2563eb;text-align:left;")
    TD = "padding:11px 14px;font-size:1rem;color:#1e293b;border-bottom:1px solid #e2e8f0;vertical-align:middle;"

    headers = "".join(f'<th style="{TH}">{COL_LABELS.get(c, c)}</th>' for c in df.columns)
    rows = []
    for i, (_, row) in enumerate(df.iterrows()):
        bg = "#fff" if i % 2 == 0 else "#f8fafc"
        cells = []
        for col in df.columns:
            val = row[col]
            if col == "URL":
                cell = f'<td style="{TD}text-align:center;"><a href="{val}" target="_blank" style="color:#2563eb;font-weight:600;text-decoration:none;">🔗 보기</a></td>'
            elif col == "남은기간":
                cell = f'<td style="{TD}">{status_badge(str(val))}</td>'
            elif col == "계약금액":
                try:   fmt = f"{int(val):,}"
                except: fmt = str(val)
                cell = f'<td style="{TD}text-align:right;font-variant-numeric:tabular-nums;">{fmt}</td>'
            elif col in ("수요기관", "계약명", "업체명"):
                cell = f'<td style="{TD}max-width:250px;word-break:keep-all;">{str(val)}</td>'
            else:
                cell = f'<td style="{TD}white-space:nowrap;">{str(val)}</td>'
            cells.append(cell)
        rows.append(
            f'<tr style="background:{bg};" '
            f'onmouseover="this.style.background=\'#eff6ff\'" '
            f'onmouseout="this.style.background=\'{bg}\'">'
            + "".join(cells) + "</tr>"
        )
    return (f'<div style="width:100%;overflow-x:auto;border-radius:12px;'
            f'box-shadow:0 2px 12px rgba(0,0,0,.08);margin-top:.5rem;">'
            f'<table style="width:100%;border-collapse:collapse;min-width:1000px;">'
            f'<thead><tr>{headers}</tr></thead>'
            f'<tbody>{"".join(rows)}</tbody>'
            f'</table></div>')

# ─────────────────────────────────────────────
# HTML 테이블 — 반복수주
# ─────────────────────────────────────────────
def render_repeat_table(df: pd.DataFrame) -> str:
    COL_LABELS = {
        "수요기관": "수요기관", "업체명": "업체명", "수주횟수": "수주횟수",
        "수주연도": "수주연도", "대표사업명": "대표사업명",
        "계약금액합계": "계약금액합계(원)", "최초계약일": "최초계약일",
        "최근계약일": "최근계약일", "계약목록": "계약목록",
    }
    TH_R = ("background:#7f1d1d;color:#fff;padding:11px 13px;"
            "font-size:0.92rem;font-weight:700;white-space:nowrap;"
            "border-bottom:2px solid #dc2626;text-align:left;")
    TD_R = ("padding:10px 13px;font-size:.95rem;color:#1e293b;"
            "border-bottom:1px solid #fee2e2;vertical-align:middle;")

    disp_cols = [c for c in COL_LABELS if c in df.columns]
    headers   = "".join(f'<th style="{TH_R}">{COL_LABELS[c]}</th>' for c in disp_cols)
    rows = []
    for i, (_, row) in enumerate(df.iterrows()):
        bg = "#fff" if i % 2 == 0 else "#fff5f5"
        cells = []
        for col in disp_cols:
            val = row[col]
            if col == "수주횟수":
                color = "#dc2626" if val >= 3 else "#c2410c"
                cell = (f'<td style="{TD_R}text-align:center;">'
                        f'<span style="background:#fef2f2;color:{color};padding:3px 12px;'
                        f'border-radius:999px;font-size:.9rem;font-weight:700;">{val}회</span></td>')
            elif col == "계약금액합계":
                try:   fmt = f"{int(val):,}"
                except: fmt = str(val)
                cell = f'<td style="{TD_R}text-align:right;font-variant-numeric:tabular-nums;">{fmt}</td>'
            elif col == "계약목록":
                items  = str(val).split(" / ")
                listed = "".join(
                    f'<li style="margin-bottom:3px;font-size:.85rem;color:#475569;">{it}</li>'
                    for it in items
                )
                cell = (f'<td style="{TD_R}max-width:320px;">'
                        f'<ul style="margin:0;padding-left:1.2rem;">{listed}</ul></td>')
            elif col in ("수요기관", "대표사업명", "업체명"):
                cell = f'<td style="{TD_R}max-width:220px;word-break:keep-all;">{str(val)}</td>'
            else:
                cell = f'<td style="{TD_R}white-space:nowrap;">{str(val)}</td>'
            cells.append(cell)
        rows.append(
            f'<tr style="background:{bg};" '
            f'onmouseover="this.style.background=\'#fee2e2\'" '
            f'onmouseout="this.style.background=\'{bg}\'">'
            + "".join(cells) + "</tr>"
        )
    return (f'<div style="width:100%;overflow-x:auto;border-radius:12px;'
            f'box-shadow:0 2px 12px rgba(220,38,38,.12);margin-top:.5rem;">'
            f'<table style="width:100%;border-collapse:collapse;min-width:1100px;">'
            f'<thead><tr>{headers}</tr></thead>'
            f'<tbody>{"".join(rows)}</tbody>'
            f'</table></div>')

# ─────────────────────────────────────────────
# HTML 테이블 — 발주계획
# ─────────────────────────────────────────────
def render_plan_table(df: pd.DataFrame) -> str:
    COL_LABELS = {
        "기관명": "기관명", "사업명": "사업명", "발주월_표시": "발주월",
        "합계발주금액": "발주금액(원)", "계약방법명": "계약방법", "조달방식": "조달방식",
        "담당자명": "담당자", "전화번호": "전화번호",
        "부서명": "부서명", "발주계획통합번호": "발주계획번호",
    }
    TH = ("background:#064e3b;color:#fff;padding:12px 14px;font-size:0.95rem;font-weight:700;"
          "white-space:nowrap;border-bottom:2px solid #059669;text-align:left;")
    TD = "padding:11px 14px;font-size:1rem;color:#1e293b;border-bottom:1px solid #e2e8f0;vertical-align:middle;"
    G2B_URL = "https://www.g2b.go.kr"

    headers = "".join(f'<th style="{TH}">{COL_LABELS.get(c, c)}</th>' for c in df.columns)
    rows = []
    for i, (_, row) in enumerate(df.iterrows()):
        bg = "#fff" if i % 2 == 0 else "#f8fafc"
        cells = []
        for col in df.columns:
            val = row[col]
            if col == "발주계획통합번호":
                num = str(val).strip() if val and str(val).strip() not in ("", "nan") else ""
                if num:
                    cell = (
                        f'<td style="{TD}text-align:center;">'
                        f'<span style="font-size:.8rem;color:#475569;display:block;margin-bottom:3px;">{num}</span>'
                        f'<a href="{G2B_URL}" target="_blank" '
                        f'style="background:#059669;color:#fff;padding:3px 10px;border-radius:6px;'
                        f'font-size:.82rem;font-weight:600;text-decoration:none;white-space:nowrap;">'
                        f'🔗 나라장터</a></td>'
                    )
                else:
                    cell = f'<td style="{TD}text-align:center;color:#94a3b8;">-</td>'
            elif col == "합계발주금액":
                try:   fmt = f"{int(val):,}"
                except: fmt = str(val)
                cell = f'<td style="{TD}text-align:right;font-variant-numeric:tabular-nums;">{fmt}</td>'
            elif col == "발주월_표시":
                cell = (f'<td style="{TD}text-align:center;">'
                        f'<span style="background:#ecfdf5;color:#065f46;padding:3px 10px;'
                        f'border-radius:999px;font-size:.85rem;font-weight:600;">{str(val)}</span></td>')
            elif col in ("기관명", "사업명"):
                cell = f'<td style="{TD}max-width:220px;word-break:keep-all;">{str(val)}</td>'
            elif col == "비고내용":
                cell = f'<td style="{TD}max-width:180px;font-size:.9rem;color:#475569;word-break:keep-all;">{str(val) if str(val) != "nan" else "-"}</td>'
            else:
                display_val = str(val) if str(val) != "nan" else "-"
                cell = f'<td style="{TD}white-space:nowrap;">{display_val}</td>'
            cells.append(cell)
        rows.append(
            f'<tr style="background:{bg};" '
            f'onmouseover="this.style.background=\'#ecfdf5\'" '
            f'onmouseout="this.style.background=\'{bg}\'">'
            + "".join(cells) + "</tr>"
        )
    return (f'<div style="width:100%;overflow-x:auto;border-radius:12px;'
            f'box-shadow:0 2px 12px rgba(0,0,0,.08);margin-top:.5rem;">'
            f'<table style="width:100%;border-collapse:collapse;min-width:1200px;">'
            f'<thead><tr>{headers}</tr></thead>'
            f'<tbody>{"".join(rows)}</tbody>'
            f'</table></div>')

# ─────────────────────────────────────────────
# 페이지네이션
# ─────────────────────────────────────────────
def render_pagination(total_pages: int, page_key: str) -> int:
    MAX_BTN  = 20
    btn_cols = st.columns(min(total_pages, MAX_BTN))
    for i in range(min(total_pages, MAX_BTN)):
        with btn_cols[i]:
            is_current = (st.session_state[page_key] == i + 1)
            label      = f"**{i+1}**" if is_current else str(i + 1)
            if st.button(label, key=f"{page_key}_btn_{i+1}", use_container_width=True):
                st.session_state[page_key] = i + 1
                st.rerun()
    return st.session_state[page_key]

# ─────────────────────────────────────────────
# ★ 반복수주 모달 다이얼로그 (st.dialog 사용)
# ─────────────────────────────────────────────
@st.dialog("🔁 반복 수주 의심 상세 현황", width="large")
def show_repeat_modal(repeat_df: pd.DataFrame, region: str):
    rep_total_amt = repeat_df["계약금액합계"].sum()
    rep_amt_str   = f"{rep_total_amt/100_000_000:.1f}억" if rep_total_amt >= 100_000_000 else f"{rep_total_amt:,}원"

    r1, r2, r3 = st.columns(3)
    with r1:
        st.markdown(f'<div class="stat-card" style="border-color:#fca5a5;"><div class="stat-num-red">{len(repeat_df):,}건</div><div class="stat-label" style="color:#dc2626;">반복수주 의심 건수</div></div>', unsafe_allow_html=True)
    with r2:
        st.markdown(f'<div class="stat-card" style="border-color:#fca5a5;"><div class="stat-num-red">{repeat_df["업체명"].nunique():,}개사</div><div class="stat-label" style="color:#dc2626;">해당 업체 수</div></div>', unsafe_allow_html=True)
    with r3:
        st.markdown(f'<div class="stat-card" style="border-color:#fca5a5;"><div class="stat-num-red">{rep_amt_str}</div><div class="stat-label" style="color:#dc2626;">계약금액 합계</div></div>', unsafe_allow_html=True)

    st.markdown(
        '<div style="font-size:.92rem;color:#64748b;margin:1rem 0 .5rem;">'
        '동일 기관·유사 사업명(80% 이상)·동일 업체 · <b>최근 3년치 계약 전체 기준</b>'
        '</div>',
        unsafe_allow_html=True
    )

    _, dl_col = st.columns([8, 2])
    with dl_col:
        export_df = repeat_df.copy()
        export_df["계약목록"] = export_df["계약목록"].str.replace(" / ", "\n")
        st.download_button(
            "📥 CSV",
            data=export_df.to_csv(
                index=False, encoding="utf-8-sig"
            ).encode("utf-8-sig"),
            file_name=f"반복수주의심_{region}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True,
            key="repeat_modal_download",
        )

    st.markdown(render_repeat_table(repeat_df), unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# 메인 UI
# ═══════════════════════════════════════════════════════════════
with st.spinner("📡 데이터 준비 중…"):
    processed_df, repeat_base_df = get_processed_df()
    baljoo_df                    = get_baljoo_df()
    gong_df                      = get_gong_df()

tab1, tab2, tab3 = st.tabs(["🏛️ 유지보수 계약 내역", "📋 유지보수 발주 계획", "📢 유지보수 공고"])

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 1
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab1:
    st.markdown("""
    <div class="hero-blue">
      <h1>🏛️ 전국 지자체 유지보수 계약 현황</h1>
      <p>나라장터 통합관제·CCTV 유지보수 계약 데이터 | 실시간 계약 만료 분석</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="search-panel">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">📍 지역 선택</div>', unsafe_allow_html=True)
    st.radio("", options=METRO_LIST, horizontal=True, key="radio_region", label_visibility="collapsed")

    cb1, cb2 = st.columns([1, 8])
    with cb1:
        info_search = st.button("🔍 검색", type="primary", use_container_width=True, key="info_search_btn")
    with cb2:
        if st.button("🔄 데이터 새로고침", key="info_refresh_btn"):
            st.cache_resource.clear()
            st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

    if info_search:
        log_event("검색_계약내역", st.session_state["radio_region"])
        st.session_state["search_done"]   = True
        st.session_state["search_region"] = st.session_state["radio_region"]
        st.session_state["page"]          = 1
        st.rerun()

    if not st.session_state["search_done"]:
        st.markdown("""
        <div style="text-align:center;padding:5rem 0;color:#94a3b8;">
          <div style="font-size:5rem;margin-bottom:1rem;">🔍</div>
          <div style="font-size:1.6rem;font-weight:700;color:#334155;margin-bottom:.5rem;">지역을 선택하고 검색 버튼을 눌러주세요</div>
          <div style="font-size:1.1rem;color:#64748b;">전국 또는 광역시도를 선택하면 계약 현황이 표시됩니다</div>
        </div>
        """, unsafe_allow_html=True)
    elif processed_df.empty:
        st.warning("⚠️ 데이터를 불러올 수 없습니다.")
    else:
        region_to_show = st.session_state["search_region"]

        # 화면 표시용 df
        display_df = (
            processed_df if region_to_show == "전국"
            else processed_df[processed_df["광역단위"] == region_to_show]
        )
        # 반복수주 탐지용 df (3년치 전체)
        repeat_display_df = (
            repeat_base_df if region_to_show == "전국"
            else repeat_base_df[repeat_base_df["광역단위"] == region_to_show]
        )

        # ── 계약 요약 stat 카드 4개 ──
        total_count   = len(display_df)
        active_count  = len(display_df[
            ~display_df["남은기간"].str.contains("만료", na=False) &
            (display_df["남은기간"] != "정보부족") &
            (display_df["남은기간"] != "계산불가")
        ])
        expiring_soon = len(display_df[display_df["남은기간"].str.match(r"^[0-3]개월", na=False)])
        total_amount  = display_df["★가공_계약금액"].sum()
        amount_str    = f"{total_amount/100_000_000:.1f}억" if total_amount >= 100_000_000 else f"{total_amount:,}원"

        c1, c2, c3, c4 = st.columns(4)
        with c1: st.markdown(f'<div class="stat-card"><div class="stat-num-blue">{total_count:,}</div><div class="stat-label">전체 계약 건수</div></div>', unsafe_allow_html=True)
        with c2: st.markdown(f'<div class="stat-card"><div class="stat-num-blue" style="color:#16a34a">{active_count:,}</div><div class="stat-label">진행중 계약</div></div>', unsafe_allow_html=True)
        with c3: st.markdown(f'<div class="stat-card"><div class="stat-num-blue" style="color:#ea580c">{expiring_soon:,}</div><div class="stat-label">3개월 내 만료 예정</div></div>', unsafe_allow_html=True)
        with c4: st.markdown(f'<div class="stat-card"><div class="stat-num-blue" style="color:#7c3aed">{amount_str}</div><div class="stat-label">총 계약금액</div></div>', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # ── ★ 반복수주 요약 배너 + 모달 버튼 (상단 고정) ──
        with st.spinner("🔍 반복수주 패턴 분석 중…"):
            repeat_df = detect_repeat_contracts(repeat_display_df, threshold=0.80)

        if not repeat_df.empty:
            rep_total_amt = repeat_df["계약금액합계"].sum()
            rep_amt_str   = f"{rep_total_amt/100_000_000:.1f}억" if rep_total_amt >= 100_000_000 else f"{rep_total_amt:,}원"

            # ✅ 수정 - 삼중 따옴표로 감싸기
            st.markdown(
                """
                <div style="background:linear-gradient(135deg,#fef2f2,#fff5f5);
                border:1.5px solid #fca5a5;border-radius:14px;
                padding:1rem 1.5rem;margin-bottom:1.5rem;
                box-shadow:0 2px 10px rgba(220,38,38,.08);
                display:flex;align-items:center;gap:1.5rem;flex-wrap:wrap;">
                <span style="font-size:1.5rem;">🔁</span>
                <span style="font-weight:700;color:#dc2626;font-size:1.05rem;">반복 수주 의심 현황 (최근 3년)</span>
                """,
                unsafe_allow_html=True
            )
            bk1, bk2, bk3, bk4 = st.columns([1, 1, 1, 1])
            with bk1:
                st.markdown(f'<div class="stat-card" style="border-color:#fca5a5;padding:1rem;"><div class="stat-num-red" style="font-size:1.8rem;">{len(repeat_df):,}건</div><div class="stat-label" style="color:#dc2626;">의심 건수</div></div>', unsafe_allow_html=True)
            with bk2:
                st.markdown(f'<div class="stat-card" style="border-color:#fca5a5;padding:1rem;"><div class="stat-num-red" style="font-size:1.8rem;">{repeat_df["업체명"].nunique():,}개사</div><div class="stat-label" style="color:#dc2626;">해당 업체</div></div>', unsafe_allow_html=True)
            with bk3:
                st.markdown(f'<div class="stat-card" style="border-color:#fca5a5;padding:1rem;"><div class="stat-num-red" style="font-size:1.8rem;">{rep_amt_str}</div><div class="stat-label" style="color:#dc2626;">계약금액 합계</div></div>', unsafe_allow_html=True)
            with bk4:
                st.markdown("<div style='padding-top:.5rem;'>", unsafe_allow_html=True)
                if st.button("📋 상세 보기 →", key="open_repeat_modal", use_container_width=True):
                    show_repeat_modal(repeat_df, region_to_show)
                st.markdown("</div>", unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # ── 세부 필터 ──
        with st.expander("🎛️ 결과 내 세부 필터", expanded=False):
            fc1, fc2, fc3 = st.columns(3)
            with fc1: sel_agency  = st.multiselect("수요기관",  sorted(display_df["★가공_수요기관"].dropna().unique()), placeholder="전체", key="info_f_agency")
            with fc2: sel_company = st.multiselect("업체명",    sorted(display_df["★가공_업체명"].dropna().unique()),   placeholder="전체", key="info_f_company")
            with fc3: sel_status  = st.multiselect("계약 상태", ["진행중", "3개월 내 만료", "만료됨"],                  placeholder="전체", key="info_f_status")
            kw = st.text_input("🔎 계약명 키워드 검색", placeholder="예: CCTV, 통합관제, 영상...", key="info_kw")

        filtered_df = display_df
        if sel_agency:  filtered_df = filtered_df[filtered_df["★가공_수요기관"].isin(sel_agency)]
        if sel_company: filtered_df = filtered_df[filtered_df["★가공_업체명"].isin(sel_company)]
        if kw:          filtered_df = filtered_df[filtered_df["★가공_계약명"].str.contains(kw, case=False, na=False)]
        if sel_status:
            conds = []
            if "진행중"        in sel_status: conds.append(~filtered_df["남은기간"].str.contains("만료|정보부족|오류", na=True))
            if "3개월 내 만료" in sel_status: conds.append(filtered_df["남은기간"].str.match(r"^[0-3]개월", na=False))
            if "만료됨"        in sel_status: conds.append(filtered_df["남은기간"] == "만료됨")
            if conds:
                combined = conds[0]
                for c in conds[1:]: combined |= c
                filtered_df = filtered_df[combined]

        COLS = ["★가공_수요기관", "★가공_계약명", "★가공_업체명", "★가공_계약금액",
                "계약일자", "착수일자", "★가공_계약만료일", "남은기간", "계약상세정보URL"]
        col_rename = {c: c.replace("★가공_", "").replace("계약상세정보URL", "URL") for c in COLS}

        st.divider()
        rc, dc = st.columns([6, 2])
        with rc:
            st.markdown(f'<div class="section-title" style="font-size:1.5rem;">📊 {region_to_show} 계약 현황 — {len(filtered_df):,}건</div>', unsafe_allow_html=True)
        with dc:
            exp_df = filtered_df[COLS].copy()
            exp_df.columns = [col_rename[c] for c in COLS]
            st.download_button(
                "📥 CSV 다운로드",
                data=exp_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                file_name=f"계약현황_{region_to_show}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv", use_container_width=True, key="info_download"
            )

        sort_map = {
            "수요기관": "★가공_수요기관",
            "계약금액 (높은순)": "★가공_계약금액",
            "계약만료일 (빠른순)": "★가공_계약만료일",
            "계약일자 (최신순)": "계약일자",
        }
        sort_choice = st.selectbox("정렬 기준", list(sort_map.keys()), index=3, label_visibility="collapsed", key="info_sort")
        sorted_df   = filtered_df.sort_values(sort_map[sort_choice], ascending=sort_choice not in ["계약금액 (높은순)", "계약일자 (최신순)"])

        total_rows  = len(sorted_df)
        total_pages = max(1, (total_rows + PAGE_SIZE - 1) // PAGE_SIZE)
        if st.session_state["page"] > total_pages:
            st.session_state["page"] = 1

        st.markdown(f'<div style="padding-top:4px;color:#64748b;font-size:1rem;margin-bottom:.5rem;">총 <b>{total_rows:,}건</b> · {total_pages}페이지</div>', unsafe_allow_html=True)
        page = render_pagination(total_pages, "page")

        paged_df = sorted_df[COLS].iloc[(page-1)*PAGE_SIZE : page*PAGE_SIZE].rename(columns=col_rename)
        st.markdown(render_info_table(paged_df), unsafe_allow_html=True)
        st.markdown(f'<div style="text-align:center;color:#94a3b8;font-size:0.95rem;margin-top:1rem;">{page} / {total_pages} 페이지 &nbsp;·&nbsp; {(page-1)*PAGE_SIZE+1}–{min(page*PAGE_SIZE, total_rows)}번째 항목</div>', unsafe_allow_html=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 2
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab2:
    st.markdown("""
    <div class="hero-green">
      <h1>📋 전국 지자체 유지보수 발주 계획</h1>
      <p>나라장터 통합관제·CCTV 유지보수 발주 계획 | 최근 6개월 기준</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="search-panel">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">📍 지역 선택</div>', unsafe_allow_html=True)
    st.radio("", options=METRO_LIST, horizontal=True, key="plan_radio_region", label_visibility="collapsed")

    pb1, pb2 = st.columns([1, 8])
    with pb1:
        plan_search = st.button("🔍 검색", type="primary", use_container_width=True, key="plan_search_btn")
    with pb2:
        if st.button("🔄 데이터 새로고침", key="plan_refresh_btn"):
            st.cache_resource.clear()
            st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

    if plan_search:
        log_event("검색_발주계획", st.session_state["plan_radio_region"])
        st.session_state["plan_search_done"]   = True
        st.session_state["plan_search_region"] = st.session_state["plan_radio_region"]
        st.session_state["plan_page"]          = 1

    if not st.session_state["plan_search_done"]:
        st.markdown("""
        <div style="text-align:center;padding:5rem 0;color:#94a3b8;">
          <div style="font-size:5rem;margin-bottom:1rem;">📋</div>
          <div style="font-size:1.6rem;font-weight:700;color:#334155;margin-bottom:.5rem;">지역을 선택하고 검색 버튼을 눌러주세요</div>
          <div style="font-size:1.1rem;color:#64748b;">최근 6개월 내 발주 계획이 표시됩니다</div>
        </div>
        """, unsafe_allow_html=True)
    elif baljoo_df.empty:
        st.warning("⚠️ 조건에 맞는 발주 계획 데이터가 없습니다.")
    else:
        plan_region  = st.session_state["plan_search_region"]
        plan_display = (
            baljoo_df if plan_region == "전국"
            else baljoo_df[baljoo_df["광역단위"] == plan_region]
        )

        today      = pd.Timestamp(datetime.now().date())
        next_month = today + relativedelta(months=1)
        total_count  = len(plan_display)
        agency_count = plan_display["기관명"].nunique()
        soon_count   = len(plan_display[plan_display["발주일자"] <= next_month])
        total_amount = plan_display["합계발주금액"].sum()
        amount_str   = f"{total_amount/100_000_000:.1f}억" if total_amount >= 100_000_000 else f"{total_amount:,}원"

        c1, c2, c3, c4 = st.columns(4)
        with c1: st.markdown(f'<div class="stat-card"><div class="stat-num-green">{total_count:,}</div><div class="stat-label">발주 계획 건수</div></div>', unsafe_allow_html=True)
        with c2: st.markdown(f'<div class="stat-card"><div class="stat-num-green" style="color:#0284c7">{agency_count:,}</div><div class="stat-label">발주 기관 수</div></div>', unsafe_allow_html=True)
        with c3: st.markdown(f'<div class="stat-card"><div class="stat-num-green" style="color:#ea580c">{soon_count:,}</div><div class="stat-label">이번달·다음달 발주</div></div>', unsafe_allow_html=True)
        with c4: st.markdown(f'<div class="stat-card"><div class="stat-num-green" style="color:#7c3aed">{amount_str}</div><div class="stat-label">합계 발주금액</div></div>', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        with st.expander("🎛️ 결과 내 세부 필터", expanded=False):
            fc1, fc2, fc3 = st.columns(3)
            with fc1: sel_agency  = st.multiselect("기관명",   sorted(plan_display["기관명"].dropna().unique()),     placeholder="전체", key="plan_f_agency")
            with fc2: sel_month   = st.multiselect("발주월",   sorted(plan_display["발주월_표시"].dropna().unique()), placeholder="전체", key="plan_f_month")
            with fc3: sel_method  = st.multiselect("계약방법", sorted(plan_display["계약방법명"].dropna().unique()),  placeholder="전체", key="plan_f_method")
            kw2 = st.text_input("🔎 사업명 키워드 검색", placeholder="예: CCTV, 통합관제, 영상...", key="plan_kw")

        filtered_plan = plan_display
        if sel_agency:  filtered_plan = filtered_plan[filtered_plan["기관명"].isin(sel_agency)]
        if sel_month:   filtered_plan = filtered_plan[filtered_plan["발주월_표시"].isin(sel_month)]
        if sel_method:  filtered_plan = filtered_plan[filtered_plan["계약방법명"].isin(sel_method)]
        if kw2:         filtered_plan = filtered_plan[filtered_plan["사업명"].str.contains(kw2, case=False, na=False)]

        st.divider()
        rc2, dc2 = st.columns([6, 2])
        with rc2:
            st.markdown(f'<div class="section-title" style="font-size:1.5rem;">📋 {plan_region} 발주 계획 — {len(filtered_plan):,}건</div>', unsafe_allow_html=True)
        with dc2:
            PLAN_COLS     = ["기관명", "사업명", "발주월_표시", "합계발주금액", "계약방법명",
                             "조달방식", "담당자명", "전화번호", "부서명", "발주계획통합번호"]
            plan_exp_cols = [c for c in PLAN_COLS if c in filtered_plan.columns]
            exp_plan      = filtered_plan[plan_exp_cols].copy()
            exp_plan.rename(columns={"발주월_표시": "발주월"}, inplace=True)
            st.download_button(
                "📥 CSV 다운로드",
                data=exp_plan.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                file_name=f"발주계획_{plan_region}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv", use_container_width=True, key="plan_download"
            )

        st.markdown("""
        <div class="copy-notice">
          💡 <b>나라장터 검색 방법</b> : "나라장터" 버튼 클릭 → 나라장터 접속 후 상단 검색창에 <b>발주계획번호</b>를 직접 입력해주세요.
        </div>
        """, unsafe_allow_html=True)

        plan_sort_map = {
            "기관명": "기관명",
            "발주월 (최신순)": "발주일자",
            "발주금액 (높은순)": "합계발주금액",
        }
        sort_choice2 = st.selectbox("정렬 기준", list(plan_sort_map.keys()), index=1, label_visibility="collapsed", key="plan_sort")
        sorted_plan  = filtered_plan.sort_values(plan_sort_map[sort_choice2], ascending=sort_choice2 == "기관명")

        total_rows2  = len(sorted_plan)
        total_pages2 = max(1, (total_rows2 + PAGE_SIZE - 1) // PAGE_SIZE)
        if st.session_state["plan_page"] > total_pages2:
            st.session_state["plan_page"] = 1

        st.markdown(f'<div style="padding-top:4px;color:#64748b;font-size:1rem;margin-bottom:.5rem;">총 <b>{total_rows2:,}건</b> · {total_pages2}페이지</div>', unsafe_allow_html=True)
        page2 = render_pagination(total_pages2, "plan_page")

        plan_table_cols = [c for c in PLAN_COLS if c in sorted_plan.columns]
        paged_plan      = sorted_plan[plan_table_cols].iloc[(page2-1)*PAGE_SIZE : page2*PAGE_SIZE].copy()
        st.markdown(render_plan_table(paged_plan), unsafe_allow_html=True)

        st.markdown("""
        <div class="copy-notice">
          💡 <b>나라장터 검색 방법</b> : "나라장터" 버튼 클릭 → 나라장터 접속 후 상단 검색창에 <b>발주계획번호</b>를 직접 입력해주세요.
        </div>
        """, unsafe_allow_html=True)
        st.markdown(f'<div style="text-align:center;color:#94a3b8;font-size:0.95rem;margin-top:1rem;">{page2} / {total_pages2} 페이지 &nbsp;·&nbsp; {(page2-1)*PAGE_SIZE+1}–{min(page2*PAGE_SIZE, total_rows2)}번째 항목</div>', unsafe_allow_html=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 3
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab3:
    st.markdown("""
    <div style="background:linear-gradient(135deg,#4c1d95 0%,#7c3aed 100%);
    border-radius:16px;padding:2.5rem 3rem;margin-bottom:2rem;text-align:center;
    box-shadow:0 4px 20px rgba(124,58,237,.25);">
      <h1 style="font-size:2.3rem;font-weight:800;color:#fff;margin:0 0 .6rem;letter-spacing:-.5px;">
        📢 전국 지자체 유지보수 공고
      </h1>
      <p style="color:rgba(255,255,255,.85);font-size:1.1rem;margin:0;">
        나라장터 통합관제·CCTV 유지보수 입찰 공고 | 최근 6개월 기준
      </p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="search-panel">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">📍 지역 선택</div>', unsafe_allow_html=True)
    st.radio("", options=METRO_LIST, horizontal=True, key="gong_radio_region", label_visibility="collapsed")

    gb1, gb2 = st.columns([1, 8])
    with gb1:
        gong_search = st.button("🔍 검색", type="primary", use_container_width=True, key="gong_search_btn")
    with gb2:
        if st.button("🔄 데이터 새로고침", key="gong_refresh_btn"):
            st.cache_resource.clear()
            st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

    if gong_search:
        log_event("검색_공고", st.session_state["gong_radio_region"])
        st.session_state["gong_search_done"]   = True
        st.session_state["gong_search_region"] = st.session_state["gong_radio_region"]
        st.session_state["gong_page"]          = 1

    if not st.session_state["gong_search_done"]:
        st.markdown("""
        <div style="text-align:center;padding:5rem 0;color:#94a3b8;">
          <div style="font-size:5rem;margin-bottom:1rem;">📢</div>
          <div style="font-size:1.6rem;font-weight:700;color:#334155;margin-bottom:.5rem;">
            지역을 선택하고 검색 버튼을 눌러주세요
          </div>
          <div style="font-size:1.1rem;color:#64748b;">최근 6개월 내 입찰 공고가 표시됩니다</div>
        </div>
        """, unsafe_allow_html=True)
    elif gong_df.empty:
        st.warning("⚠️ 조건에 맞는 공고 데이터가 없습니다.")
    else:
        gong_region  = st.session_state["gong_search_region"]
        gong_display = (
            gong_df if gong_region == "전국"
            else gong_df[gong_df["광역단위"] == gong_region]
        )

        today        = pd.Timestamp(datetime.now().date())
        total_count  = len(gong_display)
        active_count = len(gong_display[gong_display["마감여부"] == "진행중"])
        agency_count = gong_display["수요기관명"].nunique()
        total_amount = gong_display["배정예산금액"].sum()
        amount_str   = f"{total_amount/100_000_000:.1f}억" if total_amount >= 100_000_000 else f"{total_amount:,}원"

        c1, c2, c3, c4 = st.columns(4)
        with c1: st.markdown(f'<div class="stat-card"><div class="stat-num-blue" style="color:#7c3aed">{total_count:,}</div><div class="stat-label">공고 건수</div></div>', unsafe_allow_html=True)
        with c2: st.markdown(f'<div class="stat-card"><div class="stat-num-blue" style="color:#16a34a">{active_count:,}</div><div class="stat-label">진행중 공고</div></div>', unsafe_allow_html=True)
        with c3: st.markdown(f'<div class="stat-card"><div class="stat-num-blue" style="color:#0284c7">{agency_count:,}</div><div class="stat-label">공고 기관 수</div></div>', unsafe_allow_html=True)
        with c4: st.markdown(f'<div class="stat-card"><div class="stat-num-blue" style="color:#7c3aed">{amount_str}</div><div class="stat-label">배정예산 합계</div></div>', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        with st.expander("🎛️ 결과 내 세부 필터", expanded=False):
            fc1, fc2, fc3 = st.columns(3)
            with fc1: sel_gong_agency = st.multiselect("수요기관명", sorted(gong_display["수요기관명"].dropna().unique()), placeholder="전체", key="gong_f_agency")
            with fc2: sel_gong_status = st.multiselect("마감여부",   ["진행중", "마감"],                                   placeholder="전체", key="gong_f_status")
            with fc3: sel_gong_method = st.multiselect("계약방식",   sorted(gong_display["계약체결방법명"].dropna().unique()), placeholder="전체", key="gong_f_method") if "계약체결방법명" in gong_display.columns else None
            kw3 = st.text_input("🔎 공고명 키워드 검색", placeholder="예: CCTV, 통합관제, 영상...", key="gong_kw")

        filtered_gong = gong_display
        if sel_gong_agency: filtered_gong = filtered_gong[filtered_gong["수요기관명"].isin(sel_gong_agency)]
        if sel_gong_status: filtered_gong = filtered_gong[filtered_gong["마감여부"].isin(sel_gong_status)]
        if sel_gong_method and "계약체결방법명" in filtered_gong.columns:
            filtered_gong = filtered_gong[filtered_gong["계약체결방법명"].isin(sel_gong_method)]
        if kw3: filtered_gong = filtered_gong[filtered_gong["입찰공고명"].str.contains(kw3, case=False, na=False)]

        st.divider()
        rc3, dc3 = st.columns([6, 2])
        with rc3:
            st.markdown(f'<div class="section-title" style="font-size:1.5rem;">📢 {gong_region} 유지보수 공고 — {len(filtered_gong):,}건</div>', unsafe_allow_html=True)
        with dc3:
            GONG_EXP_COLS = ["입찰공고명", "공고기관명", "수요기관명", "입찰개시일_표시",
                             "입찰마감일_표시", "배정예산금액", "계약체결방법명", "마감여부", "입찰공고상세URL"]
            gong_exp_cols = [c for c in GONG_EXP_COLS if c in filtered_gong.columns]
            exp_gong      = filtered_gong[gong_exp_cols].copy()
            st.download_button(
                "📥 CSV 다운로드",
                data=exp_gong.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                file_name=f"유지보수공고_{gong_region}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv", use_container_width=True, key="gong_download"
            )

        gong_sort_map = {
            "입찰개시일 (최신순)": "입찰개시일시_dt",
            "입찰마감일 (빠른순)": "입찰마감일시_dt",
            "예산금액 (높은순)":   "배정예산금액",
            "수요기관명":          "수요기관명",
        }
        sort_choice3 = st.selectbox("정렬 기준", list(gong_sort_map.keys()), index=0, label_visibility="collapsed", key="gong_sort")
        sorted_gong  = filtered_gong.sort_values(
            gong_sort_map[sort_choice3],
            ascending=sort_choice3 == "수요기관명"
        )

        total_rows3  = len(sorted_gong)
        total_pages3 = max(1, (total_rows3 + PAGE_SIZE - 1) // PAGE_SIZE)
        if st.session_state["gong_page"] > total_pages3:
            st.session_state["gong_page"] = 1

        st.markdown(f'<div style="padding-top:4px;color:#64748b;font-size:1rem;margin-bottom:.5rem;">총 <b>{total_rows3:,}건</b> · {total_pages3}페이지</div>', unsafe_allow_html=True)
        page3 = render_pagination(total_pages3, "gong_page")

        GONG_TABLE_COLS = ["입찰공고명", "공고기관명", "수요기관명",
                           "입찰개시일_표시", "입찰마감일_표시",
                           "배정예산금액", "계약체결방법명", "마감여부", "입찰공고상세URL"]
        gong_table_cols = [c for c in GONG_TABLE_COLS if c in sorted_gong.columns]
        paged_gong      = sorted_gong[gong_table_cols].iloc[(page3-1)*PAGE_SIZE : page3*PAGE_SIZE].copy()

        GONG_COL_LABELS = {
            "입찰공고명":      "입찰공고명",
            "공고기관명":      "공고기관",
            "수요기관명":      "수요기관",
            "입찰개시일_표시": "입찰개시일",
            "입찰마감일_표시": "입찰마감일",
            "배정예산금액":    "배정예산(원)",
            "계약체결방법명":  "계약방식",
            "마감여부":        "상태",
            "입찰공고상세URL": "공고 상세",
        }
        TH_G = ("background:#4c1d95;color:#fff;padding:12px 14px;font-size:0.95rem;font-weight:700;"
                "white-space:nowrap;border-bottom:2px solid #7c3aed;text-align:left;")
        TD_G = "padding:11px 14px;font-size:1rem;color:#1e293b;border-bottom:1px solid #e2e8f0;vertical-align:middle;"

        headers_g = "".join(f'<th style="{TH_G}">{GONG_COL_LABELS.get(c,c)}</th>' for c in paged_gong.columns)
        rows_g = []
        for i, (_, row) in enumerate(paged_gong.iterrows()):
            bg = "#fff" if i % 2 == 0 else "#f8fafc"
            cells = []
            for col in paged_gong.columns:
                val = row[col]
                if col == "입찰공고상세URL":
                    url = str(val).strip()
                    if url and url != "nan":
                        cell = f'<td style="{TD_G}text-align:center;"><a href="{url}" target="_blank" style="background:#7c3aed;color:#fff;padding:4px 12px;border-radius:6px;font-size:.85rem;font-weight:600;text-decoration:none;white-space:nowrap;">🔗 바로가기</a></td>'
                    else:
                        cell = f'<td style="{TD_G}text-align:center;color:#94a3b8;">-</td>'
                elif col == "배정예산금액":
                    try:   fmt = f"{int(val):,}"
                    except: fmt = str(val)
                    cell = f'<td style="{TD_G}text-align:right;font-variant-numeric:tabular-nums;">{fmt}</td>'
                elif col == "계약체결방법명":
                    display = str(val) if str(val) not in ("", "nan") else "-"
                    if "수의" in display:
                        bg_c, fg_c = "#fff7ed", "#c2410c"
                    elif "제한" in display:
                        bg_c, fg_c = "#eff6ff", "#1d4ed8"
                    else:
                        bg_c, fg_c = "#f1f5f9", "#475569"
                    cell = (f'<td style="{TD_G}text-align:center;">'
                            f'<span style="background:{bg_c};color:{fg_c};padding:3px 10px;'
                            f'border-radius:999px;font-size:.85rem;font-weight:600;white-space:nowrap;">'
                            f'{display}</span></td>')
                elif col == "마감여부":
                    if val == "진행중":
                        badge = '<span style="background:#f0fdf4;color:#15803d;padding:3px 10px;border-radius:999px;font-size:.85rem;font-weight:600;">진행중</span>'
                    else:
                        badge = '<span style="background:#fef2f2;color:#b91c1c;padding:3px 10px;border-radius:999px;font-size:.85rem;font-weight:600;">마감</span>'
                    cell = f'<td style="{TD_G}">{badge}</td>'
                elif col == "입찰공고명":
                    cell = f'<td style="{TD_G}max-width:280px;word-break:keep-all;">{str(val)}</td>'
                else:
                    cell = f'<td style="{TD_G}white-space:nowrap;">{str(val) if str(val) != "nan" else "-"}</td>'
                cells.append(cell)
            rows_g.append(
                f'<tr style="background:{bg};" '
                f'onmouseover="this.style.background=\'#f5f3ff\'" '
                f'onmouseout="this.style.background=\'{bg}\'">'
                + "".join(cells) + "</tr>"
            )

        table_html = (
            f'<div style="width:100%;overflow-x:auto;border-radius:12px;'
            f'box-shadow:0 2px 12px rgba(0,0,0,.08);margin-top:.5rem;">'
            f'<table style="width:100%;border-collapse:collapse;min-width:1100px;">'
            f'<thead><tr>{headers_g}</tr></thead>'
            f'<tbody>{"".join(rows_g)}</tbody>'
            f'</table></div>'
        )
        st.markdown(table_html, unsafe_allow_html=True)
        st.markdown(f'<div style="text-align:center;color:#94a3b8;font-size:0.95rem;margin-top:1rem;">{page3} / {total_pages3} 페이지 &nbsp;·&nbsp; {(page3-1)*PAGE_SIZE+1}–{min(page3*PAGE_SIZE, total_rows3)}번째 항목</div>', unsafe_allow_html=True)
