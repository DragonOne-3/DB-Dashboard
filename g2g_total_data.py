import streamlit as st
import pandas as pd
from datetime import datetime
import re
from st_files_connection import FilesConnection

# --- 1. 페이지 및 연결 설정 ---
st.set_page_config(page_title="공공조달 DATA 통합검색", layout="wide")

@st.cache_resource
def get_gcs_connection():
    # 구글 드라이브 연결 통로 캐싱 (Running st.connection 시간 단축)
    return st.connection('gcs', type=FilesConnection)

conn = get_gcs_connection()

# --- 2. 데이터 소스 설정 ---
# 종합쇼핑몰용 폴더 ID
CSV_FOLDER_ID = '1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr' 

# 나머지 구글 시트 기반 데이터 ID
SHEET_FILE_IDS = {
    '나라장터_발주': '1pGnb6O5Z1ahaHYuQdydyoY1Ayf147IoGmLRdA3WAHi4',
    '나라장터_계약': '15Hsr_nup4ZteIZ4Jyov8wG2s_rKoZ25muqRE3-sRnaw',
    '군수품_발주': '1pzW51Z29SSoQk7al_GvN_tj5smuhOR3J2HWnL_16fcI',
    '군수품_계약': '1KPMUz0IKM6AQvqwfAkvW96WNvzbycN56vNlFnDmfRTw',
    '군수품_공고': '1opuA_UzNm27U9QkbMay5UsyQqcwfxiEmIHNRdc4MyHM',
    '군수품_수의': '1aYA18kPrSkpbayzbn16EdKUScVRwr2Nutyid5No5qjk'
}

# 날짜 컬럼 매핑 (카테고리별로 다름)
DATE_COL_MAP = {
    '군수품_발주': '발주예정월', '군수품_수의': '개찰일자', '군수품_계약': '계약일자',
    '군수품_공고': '공고일자', '나라장터_계약': '★가공_계약일', '종합쇼핑몰': '계약납품요구일자',
    '나라장터_발주': None # 발주계획은 날짜 필터 제외하거나 필요시 설정
}

# --- 3. 데이터 로딩 함수 (성능 최적화) ---
@st.cache_data(ttl=3600, show_spinner=False)
def load_csv_from_drive(file_path):
    """드라이브의 CSV 파일을 읽어옴"""
    return pd.read_csv(f"gdrive://{file_path}", low_memory=False)

@st.cache_data(ttl=3600, show_spinner=False)
def load_data_from_sheet(sheet_id):
    """구글 시트를 CSV 형태로 변환하여 읽어옴 (속도 빠름)"""
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
    return pd.read_csv(url, low_memory=False)

# --- 4. 사이드바 검색 UI ---
with st.sidebar:
    st.header("🔍 검색 필터")
    # 카테고리 목록 통합
    category = st.selectbox("카테고리 선택", list(SHEET_FILE_IDS.keys()) + ["종합쇼핑몰"], index=6)
    
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
    with st.spinner("데이터를 실시간으로 분석 중입니다..."):
        try:
            df = pd.DataFrame()
            s_str = start_date.strftime('%Y%m%d')
            e_str = end_date.strftime('%Y%m%d')

            # [A] 종합쇼핑몰 처리 (CSV 폴더 방식)
            if category == '종합쇼핑몰':
                all_files = conn.fs.ls(f"gdrive://{CSV_FOLDER_ID}")
                relevant_dfs = []
                target_years = [str(y) for y in range(start_date.year, end_date.year + 1)]
                
                for f_path in all_files:
                    if any(year in f_path for year in target_years):
                        tmp = load_csv_from_drive(f_path)
                        # 날짜 필터 (4번째 열 Index 3 기준)
                        date_col = tmp.columns[3]
                        tmp['compare_date'] = tmp[date_col].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8]
                        mask = (tmp['compare_date'] >= s_str) & (tmp['compare_date'] <= e_str)
                        relevant_dfs.append(tmp[mask])
                
                if relevant_dfs:
                    df = pd.concat(relevant_dfs, ignore_index=True)

            # [B] 기타 카테고리 처리 (구글 시트 방식)
            else:
                df = load_data_from_sheet(SHEET_FILE_IDS[category])
                date_col_name = DATE_COL_MAP.get(category)
                if date_col_name and date_col_name in df.columns:
                    df['compare_date'] = df[date_col_name].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8]
                    df = df[(df['compare_date'] >= s_str) & (df['compare_date'] <= e_str)]

            # --- 키워드 필터링 (통합 로직) ---
            if not df.empty and k1:
                if search_field == "ALL":
                    mask = df.astype(str).apply(lambda x: x.str.contains(k1, case=False, na=False)).any(axis=1)
                else:
                    mask = df[search_field].astype(str).str.contains(k1, case=False, na=False) if search_field in df.columns else [True]*len(df)
                
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
                st.success(f"조회 완료: {len(df):,}건")
                
                # 금액/수량 콤마 포맷팅
                num_cols = ["수량", "금액", "단가"]
                format_dict = {col: "{:,.0f}" for col in num_cols if col in df.columns}
                
                # 보조 컬럼 삭제 후 최신순 출력
                display_df = df.drop(columns=['compare_date']) if 'compare_date' in df.columns else df
                st.dataframe(display_df.style.format(format_dict), use_container_width=True, height=600)
                
                # 다운로드 버튼
                csv = df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                st.download_button("📊 엑셀(CSV) 다운로드", csv, f"{category}_검색결과.csv", "text/csv")
            else:
                st.warning("일치하는 데이터가 없습니다.")

        except Exception as e:
            st.error(f"실행 중 오류 발생: {e}")

st.markdown("---")
st.caption("🏛 공공조달 DATA 통합검색 시스템 | 종합쇼핑몰(대용량) + 나라장터/군수품(실시간 시트)")
