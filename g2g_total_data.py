import streamlit as st
import pandas as pd
from datetime import datetime
import re
from st_files_connection import FilesConnection

# --- 1. 페이지 및 연결 설정 ---
st.set_page_config(page_title="공공조달 DATA 통합검색", layout="wide")

# 구글 드라이브 연결 (Secrets에 설정된 인증 정보 사용)
conn = st.connection('gcs', type=FilesConnection)

# --- 2. 데이터 소스 설정 ---
CSV_FOLDER_ID = '1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr' # 종합쇼핑몰 CSV 폴더
FILE_IDS = {
    '나라장터_발주': '1pGnb6O5Z1ahaHYuQdydyoY1Ayf147IoGmLRdA3WAHi4',
    '나라장터_계약': '15Hsr_nup4ZteIZ4Jyov8wG2s_rKoZ25muqRE3-sRnaw',
    '군수품_발주': '1pzW51Z29SSoQk7al_GvN_tj5smuhOR3J2HWnL_16fcI',
    '군수품_계약': '1KPMUz0IKM6AQvqwfAkvW96WNvzbycN56vNlFnDmfRTw',
    '군수품_공고': '1opuA_UzNm27U9QkbMay5UsyQqcwfxiEmIHNRdc4MyHM',
    '군수품_수의': '1aYA18kPrSkpbayzbn16EdKUScVRwr2Nutyid5No5qjk'
}

# 각 카테고리별 날짜 컬럼명
DATE_COL_MAP = {
    '군수품_발주': '발주예정월', '군수품_수의': '개찰일자', '군수품_계약': '계약일자',
    '군수품_공고': '공고일자', '나라장터_계약': '★가공_계약일', '종합쇼핑몰': '계약납품요구일자'
}

# --- 3. 사이드바 UI (검색 필터) ---
st.sidebar.header("🔍 검색 필터")
category = st.sidebar.selectbox("카테고리 선택", list(FILE_IDS.keys()) + ["종합쇼핑몰"], index=6)

col1, col2 = st.sidebar.columns(2)
start_date = col1.date_input("시작일", datetime(2025, 1, 1))
end_date = col2.date_input("종료일", datetime.now())

search_field = st.sidebar.selectbox("검색 필드", ["ALL", "수요기관명", "업체명", "계약명", "세부품명"])
k1 = st.sidebar.text_input("첫 번째 검색어")
logic = st.sidebar.selectbox("검색 논리", ["NONE", "AND", "OR"])
k2 = st.sidebar.text_input("두 번째 검색어") if logic != "NONE" else ""

# --- 4. 데이터 로드 및 검색 실행 ---
if st.sidebar.button("데이터 검색 실행", type="primary"):
    with st.spinner("구글 드라이브에서 데이터를 분석 중입니다..."):
        try:
            df = pd.DataFrame()
            s_str = start_date.strftime('%Y%m%d')
            e_str = end_date.strftime('%Y%m%d')

            # [A] 종합쇼핑몰 - 구글 드라이브 폴더 내 CSV 검색
            if category == '종합쇼핑몰':
                files = conn.fs.ls(f"gdrive://{CSV_FOLDER_ID}")
                relevant_dfs = []
                for f_path in files:
                    # 파일명에 포함된 연도가 검색 범위 내인지 확인
                    year_match = re.search(r'202\d', f_path)
                    if year_match and (start_date.year <= int(year_match.group()) <= end_date.year):
                        # CSV 읽기 (Pandas는 40MB도 거뜬합니다)
                        tmp = pd.read_csv(f"gdrive://{f_path}")
                        # 날짜 컬럼(보통 4번째 열) 가공
                        tmp['compare_date'] = tmp.iloc[:, 3].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8]
                        relevant_dfs.append(tmp[(tmp['compare_date'] >= s_str) & (tmp['compare_date'] <= e_str)])
                if relevant_dfs:
                    df = pd.concat(relevant_dfs, ignore_index=True)
            
            # [B] 기타 카테고리 - 구글 시트 직접 연결
            else:
                url = f"https://docs.google.com/spreadsheets/d/{FILE_IDS[category]}/export?format=csv"
                df = pd.read_csv(url)
                date_col = DATE_COL_MAP.get(category)
                if date_col and date_col in df.columns:
                    df['compare_date'] = df[date_col].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8]
                    df = df[(df['compare_date'] >= s_str) & (df['compare_date'] <= e_str)]

            # --- 키워드 필터링 적용 ---
            if not df.empty and k1:
                if search_field == "ALL":
                    mask = df.astype(str).apply(lambda x: x.str.contains(k1, case=False)).any(axis=1)
                else:
                    mask = df[search_field].astype(str).str.contains(k1, case=False)
                
                if logic == "AND" and k2:
                    mask2 = df.astype(str).apply(lambda x: x.str.contains(k2, case=False)).any(axis=1) if search_field=="ALL" else df[search_field].astype(str).str.contains(k2, case=False)
                    df = df[mask & mask2]
                elif logic == "OR" and k2:
                    mask2 = df.astype(str).apply(lambda x: x.str.contains(k2, case=False)).any(axis=1) if search_field=="ALL" else df[search_field].astype(str).str.contains(k2, case=False)
                    df = df[mask | mask2]
                else:
                    df = df[mask]

            # --- 결과 화면 표시 ---
            if not df.empty:
                st.success(f"검색 결과: {len(df):,}건")
                # 숫자 포맷팅 (지수 표기 방지 및 콤마 추가)
                num_cols = ["수량", "금액", "단가"]
                format_dict = {col: "{:,.0f}" for col in num_cols if col in df.columns}
                st.dataframe(df.style.format(format_dict), use_container_width=True)
                
                # 다운로드 버튼
                csv_data = df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                st.download_button("📊 검색결과 엑셀 다운로드", csv_data, f"{category}_검색결과.csv", "text/csv")
            else:
                st.warning("조건에 일치하는 데이터가 없습니다.")

        except Exception as e:
            st.error(f"오류 발생: {e}")
