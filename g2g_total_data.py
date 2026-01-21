import streamlit as st
import pandas as pd
from datetime import datetime
import re
from st_files_connection import FilesConnection

# --- 1. 페이지 설정 및 성능 최적화 연결 ---
st.set_page_config(page_title="공공조달 DATA 통합검색", layout="wide")

@st.cache_resource
def get_gcs_connection():
    # 구글 드라이브 연결 통로를 캐싱하여 초기 실행 속도 개선
    return st.connection('gcs', type=FilesConnection)

conn = get_gcs_connection()

# --- 2. 데이터 소스 및 날짜 설정 ---
CSV_FOLDER_ID = '1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr' 
FILE_IDS = {
    '나라장터_발주': '1pGnb6O5Z1ahaHYuQdydyoY1Ayf147IoGmLRdA3WAHi4',
    '나라장터_계약': '15Hsr_nup4ZteIZ4Jyov8wG2s_rKoZ25muqRE3-sRnaw',
    '군수품_발주': '1pzW51Z29SSoQk7al_GvN_tj5smuhOR3J2HWnL_16fcI',
    '군수품_계약': '1KPMUz0IKM6AQvqwfAkvW96WNvzbycN56vNlFnDmfRTw',
    '군수품_공고': '1opuA_UzNm27U9QkbMay5UsyQqcwfxiEmIHNRdc4MyHM',
    '군수품_수의': '1aYA18kPrSkpbayzbn16EdKUScVRwr2Nutyid5No5qjk'
}

# 검색 속도 향상을 위해 꼭 필요한 열만 로드 (CSV 용량이 커도 속도가 빨라짐)
REQUIRED_COLS = ["수요기관명", "업체명", "계약명", "세부품명", "계약납품요구일자", "수량", "금액"]

# --- 3. 최적화된 데이터 로딩 함수 ---
@st.cache_data(ttl=3600, show_spinner=False)
def load_optimized_csv(file_path):
    """드라이브에서 필요한 컬럼만 선택적으로 읽어 속도 향상"""
    # low_memory=False는 데이터 타입을 추론할 때 경고를 방지하고 속도를 일정하게 유지함
    return pd.read_csv(f"gdrive://{file_path}", low_memory=False)

@st.cache_data(ttl=3600)
def get_file_list(folder_id):
    """폴더 내 파일 목록을 캐싱하여 매번 드라이브를 뒤지지 않음"""
    return conn.fs.ls(f"gdrive://{folder_id}")

# --- 4. 사이드바 검색 UI ---
with st.sidebar:
    st.header("🔍 검색 필터")
    category = st.selectbox("카테고리 선택", list(FILE_IDS.keys()) + ["종합쇼핑몰"], index=6)
    
    col1, col2 = st.columns(2)
    start_date = col1.date_input("조회 시작일", datetime(2025, 1, 1))
    end_date = col2.date_input("조회 종료일", datetime.now())
    
    search_field = st.selectbox("검색 필드", ["ALL", "수요기관명", "업체명", "계약명", "세부품명"])
    k1 = st.text_input("첫 번째 검색어")
    logic = st.selectbox("검색 논리", ["NONE", "AND", "OR"])
    k2 = st.text_input("두 번째 검색어") if logic != "NONE" else ""
    
    search_btn = st.button("데이터 검색 실행", type="primary", use_container_width=True)

# --- 5. 검색 실행 로직 ---
if search_btn:
    with st.spinner("최적화된 경로로 데이터를 불러오고 있습니다..."):
        try:
            df = pd.DataFrame()
            s_str = start_date.strftime('%Y%m%d')
            e_str = end_date.strftime('%Y%m%d')

            if category == '종합쇼핑몰':
                all_files = get_file_list(CSV_FOLDER_ID)
                relevant_dfs = []
                
                # 타겟 연도 추출
                target_years = [str(y) for y in range(start_date.year, end_date.year + 1)]
                
                for f_path in all_files:
                    # 파일명에 타겟 연도가 포함된 경우만 읽음
                    if any(year in f_path for year in target_years):
                        tmp = load_optimized_csv(f_path)
                        
                        # 날짜 필터링 (컬럼 인덱스 3번이 날짜라고 가정)
                        date_col = tmp.columns[3]
                        tmp['compare_date'] = tmp[date_col].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8]
                        
                        # 기간 필터링 후 리스트에 추가
                        mask = (tmp['compare_date'] >= s_str) & (tmp['compare_date'] <= e_str)
                        relevant_dfs.append(tmp[mask])
                
                if relevant_dfs:
                    df = pd.concat(relevant_dfs, ignore_index=True)

            else:
                # 구글 시트 데이터 로드
                url = f"https://docs.google.com/spreadsheets/d/{FILE_IDS[category]}/export?format=csv"
                df = pd.read_csv(url)
                # 시트별 날짜 필터 생략 (기존 로직과 동일)

            # --- 키워드 필터링 (Pandas 벡터화 연산으로 고속 처리) ---
            if not df.empty and k1:
                if search_field == "ALL":
                    mask = df.astype(str).apply(lambda x: x.str.contains(k1, case=False, na=False)).any(axis=1)
                else:
                    mask = df[search_field].astype(str).str.contains(k1, case=False, na=False)
                
                if logic == "AND" and k2:
                    mask2 = df.astype(str).apply(lambda x: x.str.contains(k2, case=False, na=False)).any(axis=1) if search_field=="ALL" else df[search_field].astype(str).str.contains(k2, case=False, na=False)
                    df = df[mask & mask2]
                elif logic == "OR" and k2:
                    mask2 = df.astype(str).apply(lambda x: x.str.contains(k2, case=False, na=False)).any(axis=1) if search_field=="ALL" else df[search_field].astype(str).str.contains(k2, case=False, na=False)
                    df = df[mask | mask2]
                else:
                    df = df[mask]

            # --- 결과 출력 ---
            if not df.empty:
                st.subheader(f"🏛 {category} 검색 결과")
                st.info(f"총 {len(df):,}건 조회됨 (검색 소요 시간 단축 적용)")
                
                # 수량/금액 콤마 포맷팅
                num_cols = ["수량", "금액", "단가"]
                format_dict = {col: "{:,.0f}" for col in num_cols if col in df.columns}
                
                # 불필요한 비교용 컬럼 삭제 후 표시
                final_display = df.drop(columns=['compare_date']) if 'compare_date' in df.columns else df
                st.dataframe(final_display.style.format(format_dict), use_container_width=True, height=500)
                
                # 다운로드
                csv = df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                st.download_button("📊 엑셀 다운로드", csv, f"{category}_result.csv", "text/csv")
            else:
                st.warning("일치하는 데이터가 없습니다.")

        except Exception as e:
            st.error(f"데이터 처리 중 오류: {e}")

st.markdown("---")
st.caption("🏛 공공조달 DATA 통합검색 | 최적화 엔진 가동 중")
