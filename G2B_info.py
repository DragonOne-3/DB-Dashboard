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
# 커스텀 CSS (클린 라이트 테마)
# ─────────────────────────────────────────────
st.markdown("""
<style>
  /* 전체 배경 */
  .stApp { background: #f4f6fb; }

  /* ── 히어로 헤더 ── */
  .hero {
    background: linear-gradient(135deg, #1e3a5f 0%, #2563eb 100%);
    border-radius: 16px;
    padding: 2.5rem 3rem;
    margin-bottom: 2rem;
    text-align: center;
    box-shadow: 0 4px 20px rgba(37,99,235,.25);
  }
  .hero h1 { font-size: 2rem; font-weight: 800; color: #fff; margin: 0 0 .5rem; letter-spacing: -.5px; }
  .hero p  { color: rgba(255,255,255,.75); font-size: 1rem; margin: 0; }

  /* ── 검색 패널 ── */
  .search-panel {
    background: #fff;
    border: 1px solid #e2e8f0;
    border-radius: 14px;
    padding: 1.5rem 2rem;
    margin-bottom: 1.5rem;
    box-shadow: 0 2px 8px rgba(0,0,0,.06);
  }

  /* ── 통계 카드 ── */
  .stat-card {
    background: #fff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 1.3rem 1.5rem;
    text-align: center;
    box-shadow: 0 2px 8px rgba(0,0,0,.05);
  }
  .stat-num   { font-size: 1.9rem; font-weight: 800; color: #2563eb; line-height: 1.1; }
  .stat-label { font-size: .78rem; color: #64748b; margin-top: .3rem; font-weight: 500; letter-spacing: .3px; }

  /* ── 섹션 제목 ── */
  .section-title {
    font-size: 1rem; font-weight: 700; color: #1e293b;
    margin-bottom: .8rem; letter-spacing: -.2px;
  }

  /* ── 검색 버튼 ── */
  div[data-testid="stButton"] > button[kind="primary"] {
    background: linear-gradient(135deg, #2563eb, #7c3aed) !important;
    border: none !important;
    color: #fff !important;
    font-weight: 700 !important;
    border-radius: 8px !important;
    padding: .55rem 2rem !important;
    font-size: .95rem !important;
    box-shadow: 0 3px 10px rgba(37,99,235,.35) !important;
    transition: opacity .18s, transform .18s;
  }
  div[data-testid="stButton"] > button[kind="primary"]:hover {
    opacity: .9; transform: translateY(-1px);
  }

  /* ── 다운로드 버튼 ── */
  div[data-testid="stDownloadButton"] > button {
    background: #fff !important;
    border: 1.5px solid #2563eb !important;
    color: #2563eb !important;
    font-weight: 600 !important;
    border-radius: 8px !important;
  }

  /* ── 데이터프레임 (내부 스크롤 제거) ── */
  div[data-testid="stDataFrame"] {
    border-radius: 12px;
    overflow: visible !important;
    box-shadow: 0 2px 12px rgba(0,0,0,.08);
  }
  /* iframe height 자동 확장 */
  div[data-testid="stDataFrame"] iframe {
    min-height: unset !important;
  }

  /* ── 구분선 ── */
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

DISTRICT_SET = set(FULL_DISTRICT_LIST)

METRO_LIST = [
    "전국", "서울특별시", "부산광역시", "대구광역시", "인천광역시", "광주광역시",
    "대전광역시", "울산광역시", "세종특별자치시", "경기도", "강원특별자치도",
    "충청북도", "충청남도", "전북특별자치도", "전라남도", "경상북도", "경상남도", "제주특별자치도",
]

# ─────────────────────────────────────────────
# session_state 초기화
# ─────────────────────────────────────────────
if "search_done" not in st.session_state:
    st.session_state["search_done"] = False
if "search_region" not in st.session_state:
    st.session_state["search_region"] = "전국"
# 라디오 선택값을 별도 key로 관리 (선택해도 검색 결과에 즉시 반영 안 됨)
if "radio_region" not in st.session_state:
    st.session_state["radio_region"] = "전국"


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

    def _ok(a):
        a = str(a).strip()
        return any(a.startswith(d) for d in FULL_DISTRICT_LIST)

    df = df[df["★가공_수요기관"].apply(_ok)]
    df = df[~df["★가공_수요기관"].str.contains("교육청", na=False)]

    df = df[df["★가공_계약명"].str.contains("유지", na=False)]
    df = df[df["★가공_계약명"].str.contains("통합관제|통합|CCTV", na=False)]
    df = df[~df["★가공_계약명"].str.contains("상수도|청사|악취|미세먼지", na=False)]

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

# ★ 핵심 수정 1: 라디오 선택값을 별도 session_state key로 저장
#   → 선택해도 search_region이 바뀌지 않으므로 결과 영역이 리렌더되지 않음
selected_region = st.radio(
    label="",
    options=METRO_LIST,
    horizontal=True,
    key="radio_region",          # session_state["radio_region"]에만 저장됨
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

# 검색 버튼 클릭 시에만 search_region을 업데이트
if search_clicked:
    st.session_state["search_done"]   = True
    st.session_state["search_region"] = st.session_state["radio_region"]

# ─────────────────────────────────────────────
# 검색 전 초기 화면
# ─────────────────────────────────────────────
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
# 검색 실행 — search_region 기준으로 표시
# ─────────────────────────────────────────────
region_to_show = st.session_state["search_region"]

with st.spinner("⚙️ 데이터 처리 중…"):
    raw_df = load_raw_data()
    if raw_df.empty:
        st.stop()
    processed_df = build_processed_df(raw_df)

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
    st.markdown(f'<div class="stat-card"><div class="stat-num" style="color:#16a34a">{active_count:,}</div><div class="stat-label">진행중 계약</div></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="stat-card"><div class="stat-num" style="color:#ea580c">{expiring_soon:,}</div><div class="stat-label">3개월 내 만료 예정</div></div>', unsafe_allow_html=True)
with c4:
    amount_str = f"{total_amount/100_000_000:.1f}억" if total_amount >= 100_000_000 else f"{total_amount:,}원"
    st.markdown(f'<div class="stat-card"><div class="stat-num" style="color:#7c3aed">{amount_str}</div><div class="stat-label">총 계약금액</div></div>', unsafe_allow_html=True)

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

final_out = filtered_df[cols_to_show].copy()
final_out.columns = [c.replace("★가공_", "").replace("계약상세정보URL", "URL") for c in final_out.columns]

# ★ 핵심 수정 2: st.dataframe 대신 HTML 테이블로 직접 렌더링
#   → Streamlit의 iframe 고정높이 제약을 완전히 우회, 페이지 스크롤만 사용


def _status_badge(val: str) -> str:
    val = str(val)
    if "만료됨" in val or "계약만료" in val:
        color, bg = "#b91c1c", "#fef2f2"
    elif re.match(r"^[0-3]개월", val):
        color, bg = "#c2410c", "#fff7ed"
    elif "정보부족" in val or "오류" in val or "계산불가" in val:
        color, bg = "#64748b", "#f1f5f9"
    else:
        color, bg = "#15803d", "#f0fdf4"
    return (
        f'<span style="background:{bg};color:{color};padding:2px 8px;'
        f'border-radius:999px;font-size:.78rem;font-weight:600;white-space:nowrap;">'
        f'{val}</span>'
    )


def render_html_table(df: pd.DataFrame) -> str:
    col_labels = {
        "수요기관": "수요기관",
        "계약명": "계약명",
        "업체명": "업체명",
        "계약금액": "계약금액(원)",
        "계약일자": "계약일자",
        "착수일자": "착수일자",
        "계약만료일": "계약만료일",
        "남은기간": "남은기간",
        "URL": "상세",
    }

    th_style = (
        "background:#1e3a5f;color:#fff;padding:10px 12px;"
        "font-size:.8rem;font-weight:700;white-space:nowrap;"
        "border-bottom:2px solid #2563eb;text-align:left;"
    )
    td_base = (
        "padding:9px 12px;font-size:.82rem;color:#1e293b;"
        "border-bottom:1px solid #e2e8f0;vertical-align:middle;"
    )

    headers = "".join(
        f'<th style="{th_style}">{col_labels.get(c, c)}</th>' for c in df.columns
    )

    rows_html = []
    for i, (_, row) in enumerate(df.iterrows()):
        bg = "#fff" if i % 2 == 0 else "#f8fafc"
        cells = []
        for col in df.columns:
            val = row[col]
            if col == "URL":
                cell = (
                    f'<td style="{td_base}text-align:center;">'
                    f'<a href="{val}" target="_blank" style="color:#2563eb;font-weight:600;'
                    f'text-decoration:none;">🔗 보기</a></td>'
                )
            elif col == "남은기간":
                cell = f'<td style="{td_base}">{_status_badge(str(val))}</td>'
            elif col == "계약금액":
                try:
                    formatted = f"{int(val):,}"
                except Exception:
                    formatted = str(val)
                cell = f'<td style="{td_base}text-align:right;font-variant-numeric:tabular-nums;">{formatted}</td>'
            elif col in ("수요기관", "계약명", "업체명"):
                cell = (
                    f'<td style="{td_base}max-width:220px;word-break:keep-all;">'
                    f'{str(val)}</td>'
                )
            else:
                cell = f'<td style="{td_base}white-space:nowrap;">{str(val)}</td>'
            cells.append(cell)

        rows_html.append(
            f'<tr style="background:{bg};" '
            f'onmouseover="this.style.background=\'#eff6ff\'" '
            f'onmouseout="this.style.background=\'{bg}\'">'
            + "".join(cells)
            + "</tr>"
        )

    table = f"""
    <div style="width:100%;overflow-x:auto;border-radius:12px;
                box-shadow:0 2px 12px rgba(0,0,0,.08);margin-top:.5rem;">
      <table style="width:100%;border-collapse:collapse;min-width:900px;">
        <thead><tr>{headers}</tr></thead>
        <tbody>{"".join(rows_html)}</tbody>
      </table>
    </div>
    """
    return table


st.markdown(render_html_table(final_out), unsafe_allow_html=True)
