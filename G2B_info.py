import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re

# ─────────────────────────────────────────────
# 페이지 설정
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="지자체 유지보수 계약 현황",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────
# 정규식 (모듈 레벨 1회 컴파일)
# ─────────────────────────────────────────────
RE_NONDIGIT     = re.compile(r"[^0-9]")
RE_MONTH_ONLY   = re.compile(r"^[0-3]개월")
RE_CONTRACT_NUM = re.compile(r"\d+차분?|\d+")

# ─────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────
st.markdown("""
<style>
  .stApp { background: #f4f6fb; }
  .hero {
    background: linear-gradient(135deg, #1e3a5f 0%, #2563eb 100%);
    border-radius: 16px; padding: 2.5rem 3rem; margin-bottom: 2rem;
    text-align: center; box-shadow: 0 4px 20px rgba(37,99,235,.25);
  }
  .hero h1 { font-size: 2rem; font-weight: 800; color: #fff; margin: 0 0 .5rem; letter-spacing: -.5px; }
  .hero p  { color: rgba(255,255,255,.75); font-size: 1rem; margin: 0; }
  .search-panel {
    background: #fff; border: 1px solid #e2e8f0; border-radius: 14px;
    padding: 1.5rem 2rem; margin-bottom: 1.5rem; box-shadow: 0 2px 8px rgba(0,0,0,.06);
  }
  .stat-card {
    background: #fff; border: 1px solid #e2e8f0; border-radius: 12px;
    padding: 1.3rem 1.5rem; text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,.05);
  }
  .stat-num   { font-size: 1.9rem; font-weight: 800; color: #2563eb; line-height: 1.1; }
  .stat-label { font-size: .78rem; color: #64748b; margin-top: .3rem; font-weight: 500; letter-spacing: .3px; }
  .section-title { font-size: 1rem; font-weight: 700; color: #1e293b; margin-bottom: .8rem; letter-spacing: -.2px; }
  div[data-testid="stButton"] > button[kind="primary"] {
    background: linear-gradient(135deg, #2563eb, #7c3aed) !important;
    border: none !important; color: #fff !important; font-weight: 700 !important;
    border-radius: 8px !important; padding: .55rem 2rem !important;
    font-size: .95rem !important; box-shadow: 0 3px 10px rgba(37,99,235,.35) !important;
    transition: opacity .18s, transform .18s;
  }
  div[data-testid="stButton"] > button[kind="primary"]:hover { opacity: .9; transform: translateY(-1px); }
  div[data-testid="stDownloadButton"] > button {
    background: #fff !important; border: 1.5px solid #2563eb !important;
    color: #2563eb !important; font-weight: 600 !important; border-radius: 8px !important;
  }
  hr { border-color: #e2e8f0; }
</style>
""", unsafe_allow_html=True)

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
    "강원특별자치도 춘천시", "강원특별자치도 원주시", "강원특별자치도 강릉시", "강원특별자치도 동해시", "강원특별자치도 태백시", "강원특별자치도 속초시", "강원특별자치도 삼척시", "강원특별자치도 홍천군", "강원특별자치도 횡성군", "강원특별자치도 영월군", "강원특별자치도 평창군", "강원특별자치도 정선군", "강원특별자치도 철원군", "강원특별자치도 화천군", "강원특별자치도 양구군", "강원특별자치도 인제군", "강원특별자치도 고성군", "강원특별자치도 양양군", "강원특별자치도 원주시 도시정보센터",
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

# ─────────────────────────────────────────────
# session_state 초기화
# ─────────────────────────────────────────────
for k, v in [("search_done", False), ("search_region", "전국"), ("radio_region", "전국")]:
    if k not in st.session_state:
        st.session_state[k] = v


# ─────────────────────────────────────────────
# 데이터 로드
# gspread 클라이언트는 cache_resource로 한 번만 인증
# raw 데이터는 cache_data(ttl=600)로 10분 캐싱
# ─────────────────────────────────────────────
@st.cache_resource
def _get_worksheet():
    auth_json = os.environ.get("GOOGLE_AUTH_JSON")
    if not auth_json:
        return None
    creds_dict = json.loads(auth_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client.open("나라장터_용역계약내역").get_worksheet(0)


@st.cache_data(ttl=600, show_spinner=False)
def load_raw_data() -> pd.DataFrame:
    try:
        ws = _get_worksheet()
        if ws is None:
            st.error("❌ 'GOOGLE_AUTH_JSON' 환경 변수가 설정되지 않았습니다.")
            return pd.DataFrame()
        records = ws.get_all_records(value_render_option="UNFORMATTED_VALUE")
        return pd.DataFrame(records) if records else pd.DataFrame()
    except Exception as e:
        st.error(f"❌ 시트 로드 오류: {e}")
        return pd.DataFrame()


# ─────────────────────────────────────────────
# 날짜 파싱 (벡터화)
# ─────────────────────────────────────────────
def parse_date_series(s: pd.Series) -> pd.Series:
    cleaned = s.astype(str).str.replace(RE_NONDIGIT, "", regex=True).str[:8]
    return pd.to_datetime(cleaned, format="%Y%m%d", errors="coerce")


# ─────────────────────────────────────────────
# 계약 만료일 & 남은기간 계산 (벡터화)
# ─────────────────────────────────────────────
def calculate_logic_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    cntrct_date  = parse_date_series(df["계약일자"])
    start_date   = parse_date_series(df["착수일자"])
    total_finish = parse_date_series(df["총완수일자"])
    period_raw   = df["계약기간"].astype(str)

    this_vals  = period_raw.str.extract(r"금차\s*[:\s]*(\d+)",         expand=False).astype(float).fillna(0)
    total_vals = period_raw.str.extract(r"(?:총차|총용역|총)\s*[:\s]*(\d+)", expand=False).astype(float).fillna(0)

    base_date = start_date.combine_first(cntrct_date)
    expire    = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")

    # 후보 1: 금차 ≠ 총차 + 총완수일자
    cond1 = (this_vals != total_vals) & total_finish.notna()
    expire = expire.where(~cond1, total_finish)

    # 후보 2: 총 개월수 × 30.44일
    cond2 = expire.isna() & (total_vals > 0) & base_date.notna()
    if cond2.any():
        expire[cond2] = base_date[cond2] + pd.to_timedelta((total_vals[cond2] * 30.44).round().astype(int), unit="D")

    # 후보 3: period_raw 안에 8자리 날짜
    cond3 = expire.isna()
    if cond3.any():
        expire[cond3] = parse_date_series(period_raw[cond3])

    # 후보 4: total_finish fallback
    cond4 = expire.isna() & total_finish.notna()
    expire[cond4] = total_finish[cond4]

    today      = pd.Timestamp(datetime.now().date())
    expire_str = expire.dt.strftime("%Y-%m-%d").fillna("정보부족")
    remaining  = pd.Series("정보부족", index=df.index)

    is_expired = expire.notna() & (expire < today)
    is_active  = expire.notna() & (expire >= today)

    remaining[is_expired] = "만료됨"

    if is_active.any():
        delta      = (expire[is_active] - today).dt.days
        months     = (delta // 30).astype(int)
        days_left  = (delta % 30).astype(int)
        remaining[is_active] = months.astype(str) + "개월 " + days_left.astype(str) + "일"

    return pd.DataFrame({"★가공_계약만료일": expire_str, "남은기간": remaining})


# ─────────────────────────────────────────────
# 전처리
# ─────────────────────────────────────────────
def clean_contract_name(name: str) -> str:
    name = str(name).replace(" ", "")
    return RE_CONTRACT_NUM.sub("", name)


def get_metro(a: str) -> str:
    for m in METRO_LIST[1:]:
        if a.startswith(m):
            return m
    return "기타"


@st.cache_data(ttl=600, show_spinner=False)
def build_processed_df(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()

    # 수요기관 필터
    agency_col = df["★가공_수요기관"].astype(str).str.strip()
    mask = agency_col.apply(lambda a: any(a.startswith(d) for d in FULL_DISTRICT_LIST))
    df = df[mask & ~agency_col.str.contains("교육청", na=False)].copy()

    # 계약명 필터
    cn = df["★가공_계약명"].astype(str)
    df = df[
        cn.str.contains("유지", na=False) &
        cn.str.contains("통합관제|통합|CCTV", na=False) &
        ~cn.str.contains("상수도|청사|악취|미세먼지", na=False)
    ].copy()

    # 만료일 & 남은기간
    df[["★가공_계약만료일", "남은기간"]] = calculate_logic_vectorized(df).values

    # 정렬용 컬럼
    df["temp_date"]          = pd.to_datetime(df["계약일자"].astype(str).str.replace(RE_NONDIGIT, "", regex=True).str[:8], format="%Y%m%d", errors="coerce")
    df["contract_group_key"] = df["★가공_계약명"].apply(clean_contract_name)

    df = df.sort_values(
        ["★가공_수요기관", "contract_group_key", "★가공_업체명", "temp_date"],
        ascending=[True, True, True, False],
    )

    active_df  = df[df["남은기간"] != "만료됨"].drop_duplicates(["★가공_수요기관", "contract_group_key", "★가공_업체명"], keep="first")
    expired_df = df[df["남은기간"] == "만료됨"].copy()

    active_agencies   = set(active_df["★가공_수요기관"])
    fallback_agencies = [a for a in df["★가공_수요기관"].unique() if a not in active_agencies]

    fallback_df = (
        expired_df[expired_df["★가공_수요기관"].isin(fallback_agencies)]
        .drop_duplicates("★가공_수요기관", keep="first")
        .copy()
    )
    fallback_df["남은기간"] = fallback_df["★가공_계약만료일"].apply(
        lambda d: f"{str(d)[:4]}년 계약만료" if d and len(str(d)) >= 4 else "계약만료"
    )

    out = pd.concat([active_df, fallback_df], ignore_index=True)
    out["광역단위"] = out["★가공_수요기관"].astype(str).apply(get_metro)

    # ── 계약금액: ★가공_계약금액 우선, 0이면 금차계약금액 fallback ──
    main_amt = pd.to_numeric(out["★가공_계약금액"], errors="coerce").fillna(0)
    if "금차계약금액" in out.columns:
        sub_amt = pd.to_numeric(out["금차계약금액"], errors="coerce").fillna(0)
        out["★가공_계약금액"] = main_amt.where(main_amt != 0, sub_amt).astype(int)
    else:
        out["★가공_계약금액"] = main_amt.astype(int)

    return out


# ─────────────────────────────────────────────
# 헤더
# ─────────────────────────────────────────────
st.markdown("""
<div class="hero">
  <h1>🏛️ 전국 지자체 유지보수 계약 현황</h1>
  <p>나라장터 통합관제·CCTV 유지보수 계약 데이터 | 실시간 계약 만료 분석</p>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# 검색 패널
# ─────────────────────────────────────────────
st.markdown('<div class="search-panel">', unsafe_allow_html=True)
st.markdown('<div class="section-title">📍 지역 선택</div>', unsafe_allow_html=True)

st.radio("", options=METRO_LIST, horizontal=True, key="radio_region", label_visibility="collapsed")

col_btn1, col_btn2 = st.columns([1, 8])
with col_btn1:
    search_clicked = st.button("🔍 검색", type="primary", use_container_width=True)
with col_btn2:
    if st.button("🔄 데이터 새로고침"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

st.markdown("</div>", unsafe_allow_html=True)

if search_clicked:
    st.session_state["search_done"]   = True
    st.session_state["search_region"] = st.session_state["radio_region"]

# ─────────────────────────────────────────────
# 페이지 로드 시 즉시 프리페치 (캐시에 올려둠)
# → 검색 버튼 클릭 시 캐시에서 즉시 반환
# ─────────────────────────────────────────────
with st.spinner("📡 데이터 준비 중…"):
    _raw = load_raw_data()
    if not _raw.empty:
        _processed = build_processed_df(_raw)

if not st.session_state["search_done"]:
    st.markdown("""
    <div style="text-align:center; padding: 5rem 0; color: #94a3b8;">
      <div style="font-size:4rem; margin-bottom:1rem;">🔍</div>
      <div style="font-size:1.3rem; font-weight:700; color:#334155; margin-bottom:.5rem;">지역을 선택하고 검색 버튼을 눌러주세요</div>
      <div style="font-size:.95rem; color:#64748b;">전국 또는 광역시도를 선택하면 계약 현황이 표시됩니다</div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# ─────────────────────────────────────────────
# 검색 결과 (캐시에서 즉시)
# ─────────────────────────────────────────────
region_to_show = st.session_state["search_region"]

raw_df = load_raw_data()
if raw_df.empty:
    st.stop()
processed_df = build_processed_df(raw_df)

display_df = processed_df.copy() if region_to_show == "전국" else processed_df[processed_df["광역단위"] == region_to_show].copy()

# ─────────────────────────────────────────────
# 통계 카드
# ─────────────────────────────────────────────
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
with c1:
    st.markdown(f'<div class="stat-card"><div class="stat-num">{total_count:,}</div><div class="stat-label">전체 계약 건수</div></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="stat-card"><div class="stat-num" style="color:#16a34a">{active_count:,}</div><div class="stat-label">진행중 계약</div></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="stat-card"><div class="stat-num" style="color:#ea580c">{expiring_soon:,}</div><div class="stat-label">3개월 내 만료 예정</div></div>', unsafe_allow_html=True)
with c4:
    st.markdown(f'<div class="stat-card"><div class="stat-num" style="color:#7c3aed">{amount_str}</div><div class="stat-label">총 계약금액</div></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# 세부 필터
# ─────────────────────────────────────────────
with st.expander("🎛️ 결과 내 세부 필터", expanded=False):
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        sel_agency  = st.multiselect("수요기관",  sorted(display_df["★가공_수요기관"].dropna().unique()), placeholder="전체")
    with fc2:
        sel_company = st.multiselect("업체명",    sorted(display_df["★가공_업체명"].dropna().unique()),   placeholder="전체")
    with fc3:
        sel_status  = st.multiselect("계약 상태", ["진행중", "3개월 내 만료", "만료됨"],                  placeholder="전체")
    kw = st.text_input("🔎 계약명 키워드 검색", placeholder="예: CCTV, 통합관제, 영상...")

filtered_df = display_df.copy()
if sel_agency:  filtered_df = filtered_df[filtered_df["★가공_수요기관"].isin(sel_agency)]
if sel_company: filtered_df = filtered_df[filtered_df["★가공_업체명"].isin(sel_company)]
if kw:          filtered_df = filtered_df[filtered_df["★가공_계약명"].str.contains(kw, case=False, na=False)]
if sel_status:
    conds = []
    if "진행중"        in sel_status: conds.append(~filtered_df["남은기간"].str.contains("만료|정보부족|오류", na=True))
    if "3개월 내 만료" in sel_status: conds.append(filtered_df["남은기간"].str.match(r"^[0-3]개월", na=False))
    if "만료됨"        in sel_status: conds.append(filtered_df["남은기간"].str.contains("만료", na=False))
    if conds:
        combined = conds[0]
        for c in conds[1:]: combined |= c
        filtered_df = filtered_df[combined]

# ─────────────────────────────────────────────
# 결과 헤더 + 다운로드
# ─────────────────────────────────────────────
COLS = ["★가공_수요기관", "★가공_계약명", "★가공_업체명", "★가공_계약금액",
        "계약일자", "착수일자", "★가공_계약만료일", "남은기간", "계약상세정보URL"]

st.divider()
rc, dc = st.columns([6, 2])
with rc:
    st.markdown(f'<div class="section-title">📊 {region_to_show} 계약 현황 — {len(filtered_df):,}건</div>', unsafe_allow_html=True)
with dc:
    exp_df = filtered_df[COLS].copy()
    exp_df.columns = [c.replace("★가공_", "").replace("계약상세정보URL", "URL") for c in exp_df.columns]
    st.download_button(
        "📥 CSV 다운로드",
        data=exp_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
        file_name=f"계약현황_{region_to_show}_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
        use_container_width=True,
    )

# 정렬
sort_map = {
    "수요기관": "★가공_수요기관",
    "계약금액 (높은순)": "★가공_계약금액",
    "계약만료일 (빠른순)": "★가공_계약만료일",
    "계약일자 (최신순)": "계약일자",
}
sort_choice = st.selectbox("정렬 기준", list(sort_map.keys()), label_visibility="collapsed")
filtered_df = filtered_df.sort_values(sort_map[sort_choice], ascending=sort_choice not in ["계약금액 (높은순)", "계약일자 (최신순)"])

final_out = filtered_df[COLS].copy()
final_out.columns = [c.replace("★가공_", "").replace("계약상세정보URL", "URL") for c in final_out.columns]


# ─────────────────────────────────────────────
# HTML 테이블 렌더링
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
    return (f'<span style="background:{bg};color:{color};padding:2px 8px;'
            f'border-radius:999px;font-size:.78rem;font-weight:600;white-space:nowrap;">{val}</span>')


def render_table(df: pd.DataFrame) -> str:
    COL_LABELS = {
        "수요기관": "수요기관", "계약명": "계약명", "업체명": "업체명",
        "계약금액": "계약금액(원)", "계약일자": "계약일자", "착수일자": "착수일자",
        "계약만료일": "계약만료일", "남은기간": "남은기간", "URL": "상세",
    }
    TH = ("background:#1e3a5f;color:#fff;padding:10px 12px;font-size:.8rem;font-weight:700;"
          "white-space:nowrap;border-bottom:2px solid #2563eb;text-align:left;")
    TD = "padding:9px 12px;font-size:.82rem;color:#1e293b;border-bottom:1px solid #e2e8f0;vertical-align:middle;"

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
                try:    fmt = f"{int(val):,}"
                except: fmt = str(val)
                cell = f'<td style="{TD}text-align:right;font-variant-numeric:tabular-nums;">{fmt}</td>'
            elif col in ("수요기관", "계약명", "업체명"):
                cell = f'<td style="{TD}max-width:220px;word-break:keep-all;">{str(val)}</td>'
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
            f'<table style="width:100%;border-collapse:collapse;min-width:900px;">'
            f'<thead><tr>{headers}</tr></thead>'
            f'<tbody>{"".join(rows)}</tbody>'
            f'</table></div>')


st.markdown(render_table(final_out), unsafe_allow_html=True)
