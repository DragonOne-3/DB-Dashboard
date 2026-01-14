import streamlit as st
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import os

# --- 1. 설정 및 API 정보 ---
# GitHub Secrets에 저장된 DATA_GO_KR_API_KEY를 사용합니다.
API_KEY = os.environ.get('DATA_GO_KR_API_KEY')
API_URL = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'

st.set_page_config(page_title="용역 유지보수 내역 조회", layout="wide")

def clean_name(raw_text, index):
    """[1^코드^명칭^...] 형태의 데이터에서 명칭만 추출"""
    if not raw_text or '^' not in raw_text:
        return raw_text
    parts = raw_text.replace('[', '').replace(']', '').split('^')
    return parts[index] if len(parts) > index else raw_text

def fetch_maintenance_data():
    """전년도 1월 1일부터 어제까지의 데이터 수집"""
    now = datetime.now()
    # 전년도 1월 1일 계산
    start_date = datetime(now.year - 1, 1, 1).strftime("%Y%m%d")
    # 어제 날짜 계산
    end_date = (now - timedelta(days=1)).strftime("%Y%m%d")
    
    keywords = ['통합관제센터', 'CCTV']
    all_rows = []

    # 화면에 로딩 상태 표시
    status_text = st.empty()
    status_text.info(f"데이터 조회 기간: {start_date} ~ {end_date}")

    for kw in keywords:
        params = {
            'serviceKey': API_KEY,
            'pageNo': '1',
            'numOfRows': '999',
            'inqryDiv': '1', # 계약체결일 기준
            'type': 'xml',
            'inqryBgnDate': start_date,
            'inqryEndDate': end_date,
            'cntrctNm': kw
        }
        
        try:
            res = requests.get(API_URL, params=params, timeout=30)
            if res.status_code == 200:
                root = ET.fromstring(res.content)
                items = root.findall('.//item')
                for item in items:
                    cntrct_nm = item.findtext('cntrctNm', '')
                    
                    # [필수 조건] '유지' 단어가 포함된 계약만 필터링
                    if '유지' not in cntrct_nm:
                        continue
                        
                    demand = clean_name(item.findtext('dminsttList', ''), 2)
                    corp = clean_name(item.findtext('corpList', ''), 3)
                    date = item.findtext('cntrctDate', '00000000')
                    amt = int(item.findtext('totCntrctAmt', '0'))
                    
                    all_rows.append({
                        '계약일자': date,
                        '수요기관명': demand,
                        '계약명': cntrct_nm,
                        '업체명': corp,
                        '계약금액': amt
                    })
        except Exception as e:
            st.error(f"API 호출 오류 ({kw}): {e}")

    status_text.empty()
    return pd.DataFrame(all_rows)

# --- 2. 웹 UI 구성 ---
st.title("🏛️ 나라장터 유지보수 계약 통합 조회")
st.markdown(f"**검색 조건:** 전년도 1월 1일 ~ 어제 / 키워드: `통합관제센터`, `CCTV` (제목 내 **'유지'** 포함 필수)")

if st.button("🚀 최신 데이터 불러오기"):
    with st.spinner("조달청 데이터를 분석 중입니다..."):
        df = fetch_maintenance_data()
        
        if not df.empty:
            # --- 3. 중복 제거 (수요기관명 기준 가장 최근 날짜만 남김) ---
            df = df.sort_values(by='계약일자', ascending=True)
            df = df.drop_duplicates(subset=['수요기관명'], keep='last')
            
            # 날짜 형식 변환 (20241025 -> 2024-10-25)
            df['계약일자'] = pd.to_datetime(df['계약일자'], format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d')
            
            # 화면 표시를 위해 최신순 재정렬
            df = df.sort_values(by='계약일자', ascending=False)

            # 요약 지표 표시
            m1, m2, m3 = st.columns(3)
            m1.metric("총 계약 기관", f"{len(df)}곳")
            m2.metric("총 계약 규모", f"{df['계약금액'].sum():,}원")
            m3.metric("가장 최근 계약", df['계약일자'].iloc[0] if not df.empty else "-")

            # 데이터 테이블
            st.dataframe(
                df.style.format({'계약금액': '{:,}원'}),
                use_container_width=True,
                height=500
            )
            
            # 다운로드 버튼
            csv = df.to_csv(index=False).encode('utf-8-sig')
            st.download_button("📥 결과 엑셀(CSV) 다운로드", data=csv, file_name=f"유지보수_계약_조회_{datetime.now().strftime('%Y%m%d')}.csv", mime="text/csv")
        else:
            st.warning("조건에 해당하는 유지보수 계약 내역이 없습니다.")
