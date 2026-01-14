import streamlit as st
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import os
import re

API_KEY = os.environ.get('DATA_GO_KR_API_KEY')
API_URL = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'

st.set_page_config(page_title="용역 유지보수 내역 조회", layout="wide")

def clean_name(raw_text, index):
    if not raw_text or '^' not in raw_text:
        return raw_text
    parts = raw_text.replace('[', '').replace(']', '').split('^')
    return parts[index] if len(parts) > index else raw_text

def fetch_maintenance_data():
    now = datetime.now()
    start_date = datetime(now.year - 1, 1, 1).strftime("%Y%m%d")
    end_date = (now - timedelta(days=1)).strftime("%Y%m%d")
    
    keywords = ['통합관제센터', 'CCTV']
    all_rows = []

    status_slot = st.empty()

    for kw in keywords:
        page_no = 1
        while True:
            status_slot.info(f"🔍 '{kw}' 데이터 수집 중... (페이지: {page_no})")
            params = {
                'serviceKey': API_KEY,
                'pageNo': str(page_no),
                'numOfRows': '999',
                'inqryDiv': '1',
                'type': 'xml',
                'inqryBgnDate': start_date,
                'inqryEndDate': end_date,
                'cntrctNm': kw
            }
            
            try:
                res = requests.get(API_URL, params=params, timeout=30)
                root = ET.fromstring(res.content)
                
                items = root.findall('.//item')
                if not items:
                    break
                
                for item in items:
                    cntrct_nm = item.findtext('cntrctNm', '')
                    
                    # '유지' 단어 포함 여부 확인
                    if '유지' in cntrct_nm.replace(" ", ""):
                        demand = clean_name(item.findtext('dminsttList', ''), 2)
                        corp = clean_name(item.findtext('corpList', ''), 3)
                        
                        cntrct_date_raw = item.findtext('cntrctDate') or item.findtext('cntrctCnclsDate') or ''
                        end_date_raw = item.findtext('ttalScmpltDate', '') # 총완수일자
                        amt = int(item.findtext('totCntrctAmt', '0'))
                        
                        # --- 계약만료일 계산 로직 ---
                        final_end_date = "-"
                        if end_date_raw and cntrct_date_raw:
                            if '일' in end_date_raw: # '365일' 형식
                                try:
                                    days = int(re.sub(r'[^0-9]', '', end_date_raw))
                                    start_dt = datetime.strptime(cntrct_date_raw, "%Y%m%d")
                                    final_end_date = (start_dt + timedelta(days=days)).strftime("%Y-%m-%d")
                                except: final_end_date = end_date_raw
                            else: # '20261231' 형식
                                try:
                                    final_end_date = datetime.strptime(end_date_raw, "%Y%m%d").strftime("%Y-%m-%d")
                                except: final_end_date = end_date_raw

                        all_rows.append({
                            '계약일자': cntrct_date_raw,
                            '수요기관명': demand,
                            '계약명': cntrct_nm,
                            '업체명': corp,
                            '계약금액': amt,
                            '계약만료일': final_end_date
                        })
                
                total_count_el = root.find('.//totalCount')
                if total_count_el is not None:
                    total_count = int(total_count_el.text)
                    if page_no * 999 >= total_count:
                        break
                else:
                    break
                page_no += 1
                
            except Exception as e:
                st.error(f"오류 발생: {e}")
                break

    status_slot.empty()
    return pd.DataFrame(all_rows)

st.title("🏛️ 나라장터 유지보수 계약 통합 조회")

if st.button("🚀 최신 데이터 불러오기"):
    with st.spinner("전년도부터 어제까지의 데이터를 분석 중입니다..."):
        df = fetch_maintenance_data()
    
    if not df.empty:
        # 중복 제거 (수요기관명 기준 가장 최근 계약일자 남김)
        df = df.sort_values(by='계약일자', ascending=True)
        df = df.drop_duplicates(subset=['수요기관명'], keep='last')
        
        # 날짜 보기 좋게 변경
        df['계약일자'] = pd.to_datetime(df['계약일자'], format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d')
        df = df.sort_values(by='계약일자', ascending=False)

        # 요약 표시
        m1, m2, m3 = st.columns(3)
        m1.metric("총 계약 기관", f"{len(df)}곳")
        m2.metric("총 계약 규모", f"{df['계약금액'].sum():,}원")
        m3.metric("조회 범위", "전년도 1월 ~ 어제")

        # 데이터 테이블 출력 (컬럼 순서 조정)
        st.dataframe(
            df[['계약일자', '수요기관명', '계약명', '업체명', '계약금액', '계약만료일']].style.format({'계약금액': '{:,}원'}),
            use_container_width=True,
            height=600
        )
        
        # CSV 다운로드
        csv = df.to_csv(index=False).encode('utf-8-sig')
        st.download_button("📥 결과 다운로드 (CSV)", data=csv, file_name=f"유지보수_현황_{datetime.now().strftime('%Y%m%d')}.csv", mime="text/csv")
    else:
        st.warning("⚠️ 조건에 맞는 데이터가 없습니다. API 키나 사이트상의 실제 등록 여부를 확인해 보세요.")
