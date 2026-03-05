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
# 설정 (최상단에 1회만)
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="지자체 유지보수 계약 현황",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────
# 커스텀 CSS (세련된 다크 테마)
# ─────────────────────────────────────────────
st.markdown("""
<style>
  /* 전체 배경 */
  .stApp { background: #0f1117; color: #e2e8f0; }

  /* 헤더 */
  .hero {
    background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
    border: 1px solid #334155;
    border-radius: 16px;
    padding: 2.5rem 3rem;
    margin-bottom: 2rem;
    text-align: center;
  }
  .hero h1 { font-size: 2rem; font-weight: 700; color: #f1f5f9; margin: 0 0 .5rem; }
  .hero p  { color: #94a3b8; font-size: 1rem; margin: 0; }

  /* 검색 패널 */
  .search-panel {
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 12px;
    padding: 1.5rem 2rem;
    margin-bottom: 1.5rem;
  }

  /* 통계 카드 */
  .stat-card {
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 10px;
    padding: 1.2rem 1.5rem;
    text-align: center;
  }
  .stat-num  { font-size: 1.8rem; font-weight: 700; color: #38bdf8; }
  .stat-label{ font-size: .8rem; color: #94a3b8; margin-top: .2rem; }

  /* 필터 바 */
  .filter-bar {
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 10px;
    padding: 1rem 1.5rem;
    margin-bottom: 1rem;
  }

  /* 라디오/버튼 스타일 */
  div[data-testid="stRadio"] > label { color: #cbd5e1; font-weight: 600; }
  div[data-testid="stRadio"] div[role="radiogroup"] { gap: .4rem; }
  div[data-testid="stRadio"] label > div:first-child { background: #334155 !important; }
  div[data-testid="stRadio"] label[data-baseweb="radio"]:has(input:checked) > div:first-child {
    background: #0ea5e9 !important;
  }

  /* 검색 버튼 */
  div[data-testid="stButton"] > button[kind="primary"] {
    background: linear-gradient(135deg, #0ea5e9, #6366f1) !important;
    border: none !important;
    color: white !important;
    font-weight: 700 !important;
    border-radius: 8px !important;
    padding: .6rem 2.5rem !important;
    font-size: 1rem !important;
    transition: opacity .2s;
  }
  div[data-testid="stButton"] > button[kind="primary"]:hover { opacity: .85; }

  /* 다운로드 버튼 */
  div[data-testid="stDownloadButton"] > button {
    background: #1e293b !important;
    border: 1px solid #475569 !important;
    color: #e2e8f0 !important;
    border-radius: 8px !important;
  }

  /* 데이터프레임 */
  div[data-testid="stDataFrame"] { border-radius: 10px; overflow: hidden; }

  /* select / multiselect */
  div[data-testid="stMultiSelect"] label,
  div[data-testid="stSelectbox"] label { color: #94a3b8 !important; font-size: .85rem !important; }

  /* 구분선 */
  hr { border-color: #334155; }

  /* 배지 */
  .badge-active  { background:#065f46; color:#6ee7b7; padding:2px 8px; border-radius:99px; font-size:.75rem; font-weight:600; }
  .badge-expired { background:#7f1d1d; color:#fca5a5; padding:2px 8px; border-radius:99px; font-size:.75rem; font-weight:600; }
  .badge-warn    { background:#78350f; color:#fde68a; padding:2px 8px; border-radius:99px; font-size:.75rem; font-weight:600; }

  /* 섹션 제목 */
  .section-title { font-size:1.1rem; font-weight:700; color:#e2e8f0; margin-bottom:.8rem; }
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

DISTRICT_SET = set(FULL_DISTRICT_LIST)  # O(1) lookup

METRO_LIST = [
    "전국", "서울특별시", "부산광역시", "대구광역시", "인천광역시", "광주광역시",
    "대전광역시", "울산광역시", "세종특별자치시", "경기도", "강원특별자치도",
    "충청북도", "충청남도", "전북특별자치도", "전라남도", "경상북도", "경상남도", "제주특별자치도",
]

# ─────────────────────────────────────────────
# 데이터 로드 (캐싱 — 10분마다 갱신)
# ─────────────────────────────────────────────
@st.cache_data(ttl=600, show_spinner="📡 구글 시트에서 데이터를 불러오는 중…")
def load_raw_data() -> pd.DataFrame:
    auth_json = os.environ.get("GOOGLE_AUTH_JSON")
    if not auth_json:
        st.error("❌ 'GOOGLE_AUTH_JSON' 환경 변수가 설정되지 않았습니다.")
        return pd.DataFrame()
    try:
        creds_dict = json.loads(auth_json)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        ws = client.open("나라장터_용역계약내역").get_worksheet(0)
        records = ws.get_all_records()
        return pd.DataFrame(records)
    except Exception as e:
        st.error(f"❌ 시트 로드 오류: {e}")
        return pd.DataFrame()


# ─────────────────────────────────────────────
# 전처리 (캐싱)
# ─────────────────────────────────────────────
def parse_date(val):
    if not val:
        return None
    clean = re.sub(r"[^0-9]", "", str(val))
    if len(clean) >= 8:
        try:
            return datetime.strptime(clean[:8], "%Y%m%d")
        except Exception:
            return None
    return None


def calculate_logic(row):
    try:
        cntrct_date   = parse_date(row.get("계약일자"))
        start_date    = parse_date(row.get("착수일자"))
        period_raw    = str(row.get("계약기간", ""))
        total_finish  = parse_date(row.get("총완수일자"))

        final_expire = None
        this_m  = re.search(r"금차\s*[:\s]*(\d+)", period_raw)
        total_m = re.search(r"(총차|총용역|총)\s*[:\s]*(\d+)", period_raw)
        this_val  = int(this_m.group(1))  if this_m  else 0
        total_val = int(total_m.group(2)) if total_m else 0

        if this_val != total_val and total_finish:
            final_expire = total_finish

        if not final_expire and total_val > 0:
            base = start_date or cntrct_date
            if base:
                final_expire = base + relativedelta(days=total_val)

        if not final_expire:
            d = re.sub(r"[^0-9]", "", period_raw)
            if len(d) >= 8:
                final_expire = parse_date(d[:8])

        if not final_expire and total_finish:
            final_expire = total_finish

        if not final_expire:
            return "정보부족", "정보부족"

        today      = datetime.now()
        expire_str = final_expire.strftime("%Y-%m-%d")

        if final_expire < today:
            return expire_str, "만료됨"

        diff   = relativedelta(final_expire, today)
        months = diff.years * 12 + diff.months
        return expire_str, f"{months}개월 {diff.days}일"
    except Exception:
        return "계산불가", "오류"


def clean_contract_name(name: str) -> str:
    name = str(name).replace(" ", "")
    name = re.sub(r"\d+차분?", "", name)
    return re.sub(r"\d+", "", name)


@st.cache_data(ttl=600, show_spinner=False)
def build_processed_df(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()

    # 기관 필터
    def _ok(a):
        a = str(a).strip()
        return any(a.startswith(d) for d in FULL_DISTRICT_LIST)

    df = df[df["★가공_수요기관"].apply(_ok)]
    df = df[~df["★가공_수요기관"].str.contains("교육청", na=False)]

    # 계약명 필터
    df = df[df["★가공_계약명"].str.contains("유지", na=False)]
    df = df[df["★가공_계약명"].str.contains("통합관제|통합|CCTV", na=False)]
    df = df[~df["★가공_계약명"].str.contains("상수도|청사|악취|미세먼지", na=False)]

    # 날짜 계산
    results = df.apply(lambda r: pd.Series(calculate_logic(r)), axis=1)
    df[["★가공_계약만료일", "남은기간"]] = results

    df["temp_date"]          = pd.to_datetime(df["계약일자"], errors="coerce")
    df["contract_group_key"] = df["★가공_계약명"].apply(clean_contract_name)

    df = df.sort_values(
        ["★가공_수요기관", "contract_group_key", "★가공_업체명", "temp_date"],
        ascending=[True, True, True, False],
    )

    active_df  = df[df["남은기간"] != "만료됨"].drop_duplicates(
        ["★가공_수요기관", "contract_group_key", "★가공_업체명"], keep="first"
    )
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

    def _metro(a):
        a = str(a)
        for m in METRO_LIST[1:]:
            if a.startswith(m):
                return m
        return "기타"

    out["광역단위"] = out["★가공_수요기관"].apply(_metro)
    out["★가공_계약금액"] = (
        pd.to_numeric(out["★가공_계약금액"], errors="coerce").fillna(0).astype(int)
    )
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

selected_region = st.radio(
    label="",
    options=METRO_LIST,
    horizontal=True,
    key="region_radio",
    label_visibility="collapsed",
)

col_btn1, col_btn2 = st.columns([1, 8])
with col_btn1:
    search_clicked = st.button("🔍 검색", type="primary", use_container_width=True)
with col_btn2:
    if st.button("🔄 데이터 새로고침", use_container_width=False):
        st.cache_data.clear()
        st.rerun()

st.markdown("</div>", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# 검색 전 초기 화면
# ─────────────────────────────────────────────
if not search_clicked and "search_done" not in st.session_state:
    st.markdown("""
    <div style="text-align:center; padding: 5rem 0; color: #475569;">
      <div style="font-size:4rem; margin-bottom:1rem;">🔍</div>
      <div style="font-size:1.3rem; font-weight:600; color:#64748b; margin-bottom:.5rem;">지역을 선택하고 검색 버튼을 눌러주세요</div>
      <div style="font-size:.95rem; color:#475569;">전국 또는 광역시도를 선택하면 계약 현황이 표시됩니다</div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# ─────────────────────────────────────────────
# 검색 실행
# ─────────────────────────────────────────────
if search_clicked:
    st.session_state["search_done"] = True
    st.session_state["search_region"] = selected_region

region_to_show = st.session_state.get("search_region", selected_region)

# 데이터 로드 및 전처리
with st.spinner("⚙️ 데이터 처리 중…"):
    raw_df = load_raw_data()
    if raw_df.empty:
        st.stop()
    processed_df = build_processed_df(raw_df)

# 지역 필터
if region_to_show == "전국":
    display_df = processed_df.copy()
else:
    display_df = processed_df[processed_df["광역단위"] == region_to_show].copy()

# ─────────────────────────────────────────────
# 통계 카드
# ─────────────────────────────────────────────
total_count    = len(display_df)
active_count   = len(display_df[~display_df["남은기간"].str.contains("만료", na=False) & (display_df["남은기간"] != "정보부족") & (display_df["남은기간"] != "계산불가")])
expiring_soon  = len(display_df[display_df["남은기간"].str.match(r"^[0-3]개월", na=False)])
total_amount   = display_df["★가공_계약금액"].sum()

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f'<div class="stat-card"><div class="stat-num">{total_count:,}</div><div class="stat-label">전체 계약 건수</div></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="stat-card"><div class="stat-num" style="color:#4ade80">{active_count:,}</div><div class="stat-label">진행중 계약</div></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="stat-card"><div class="stat-num" style="color:#fb923c">{expiring_soon:,}</div><div class="stat-label">3개월 내 만료 예정</div></div>', unsafe_allow_html=True)
with c4:
    amount_str = f"{total_amount/100_000_000:.1f}억" if total_amount >= 100_000_000 else f"{total_amount:,}원"
    st.markdown(f'<div class="stat-card"><div class="stat-num" style="color:#c084fc">{amount_str}</div><div class="stat-label">총 계약금액</div></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# 결과 내 필터
# ─────────────────────────────────────────────
with st.expander("🎛️ 결과 내 세부 필터", expanded=False):
    fc1, fc2, fc3 = st.columns(3)

    with fc1:
        agency_opts = sorted(display_df["★가공_수요기관"].dropna().unique().tolist())
        sel_agency  = st.multiselect("수요기관", agency_opts, placeholder="전체")

    with fc2:
        company_opts = sorted(display_df["★가공_업체명"].dropna().unique().tolist())
        sel_company  = st.multiselect("업체명", company_opts, placeholder="전체")

    with fc3:
        status_opts = ["진행중", "3개월 내 만료", "만료됨"]
        sel_status  = st.multiselect("계약 상태", status_opts, placeholder="전체")

    kw = st.text_input("🔎 계약명 키워드 검색", placeholder="예: CCTV, 통합관제, 영상...")

# 필터 적용
filtered_df = display_df.copy()

if sel_agency:
    filtered_df = filtered_df[filtered_df["★가공_수요기관"].isin(sel_agency)]
if sel_company:
    filtered_df = filtered_df[filtered_df["★가공_업체명"].isin(sel_company)]
if kw:
    filtered_df = filtered_df[filtered_df["★가공_계약명"].str.contains(kw, case=False, na=False)]
if sel_status:
    conds = []
    if "진행중" in sel_status:
        conds.append(~filtered_df["남은기간"].str.contains("만료|정보부족|오류", na=True))
    if "3개월 내 만료" in sel_status:
        conds.append(filtered_df["남은기간"].str.match(r"^[0-3]개월", na=False))
    if "만료됨" in sel_status:
        conds.append(filtered_df["남은기간"].str.contains("만료", na=False))
    if conds:
        combined = conds[0]
        for c in conds[1:]:
            combined = combined | c
        filtered_df = filtered_df[combined]

# ─────────────────────────────────────────────
# 결과 출력
# ─────────────────────────────────────────────
st.divider()
result_col, dl_col = st.columns([6, 2])
with result_col:
    st.markdown(f'<div class="section-title">📊 {region_to_show} 계약 현황 — {len(filtered_df):,}건</div>', unsafe_allow_html=True)
with dl_col:
    cols_to_show = ["★가공_수요기관", "★가공_계약명", "★가공_업체명", "★가공_계약금액",
                    "계약일자", "착수일자", "★가공_계약만료일", "남은기간", "계약상세정보URL"]
    export_df = filtered_df[cols_to_show].copy()
    export_df.columns = [c.replace("★가공_", "").replace("계약상세정보URL", "URL") for c in export_df.columns]
    csv_bytes = export_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button(
        label="📥 CSV 다운로드",
        data=csv_bytes,
        file_name=f"계약현황_{region_to_show}_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
        use_container_width=True,
    )

# 정렬 옵션
sort_col_map = {
    "수요기관": "★가공_수요기관",
    "계약금액 (높은순)": "★가공_계약금액",
    "계약만료일 (빠른순)": "★가공_계약만료일",
    "계약일자 (최신순)": "계약일자",
}
sort_choice = st.selectbox("정렬 기준", list(sort_col_map.keys()), label_visibility="collapsed")
sort_key    = sort_col_map[sort_choice]
asc_flag    = sort_choice not in ["계약금액 (높은순)", "계약일자 (최신순)"]
filtered_df = filtered_df.sort_values(sort_key, ascending=asc_flag)

# 컬럼 정리
final_out = filtered_df[cols_to_show].copy()
final_out.columns = [c.replace("★가공_", "").replace("계약상세정보URL", "URL") for c in final_out.columns]

st.dataframe(
    final_out,
    column_config={
        "URL": st.column_config.LinkColumn("상세정보", display_text="🔗 보기"),
        "계약금액": st.column_config.NumberColumn("계약금액(원)", format="localized"),
        "남은기간": st.column_config.TextColumn("남은기간"),
    },
    use_container_width=True,
    hide_index=True,
    height=700,
)
