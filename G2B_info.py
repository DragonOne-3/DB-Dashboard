import streamlit as st
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import os
import re
import time

API_KEY = os.environ.get('DATA_GO_KR_API_KEY')
API_URL = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'

st.set_page_config(page_title="용역 유지보수 내역 조회", layout="wide")

def clean_name(raw_text, index):
    if not raw_text or '^' not in raw_text:
        return raw_text
    parts = raw_text.replace('[', '').replace(']', '').split('^')
    return parts[index] if len(parts) > index else raw_text

def fetch_api_data(start_date, end_date, keyword):
    """특정 기간과 키워드에 대해 페이지네이션을 처리하며 데이터 수집"""
    rows = []
    page_no = 1
    while True:
        params = {
            'serviceKey': API_KEY,
            'pageNo': str(page_no),
            'numOfRows': '999',
            'inqryDiv': '1',
            'type': 'xml',
            'inqryBgnDate': start_date,
            'inqryEndDate': end_date,
            'cntrctNm': keyword
        }
        try:
            res = requests.get(API_URL, params=params, timeout=30)
            if not res.text.strip().startswith('<?xml'):
                break
                
            root = ET.fromstring(res.content)
            items = root.findall('.//item')
            if not items:
                break
            
            for item in items:
                cntrct_nm = item.findtext('cntrctNm', '')
                if '유지' in cntrct_nm.replace(" ", ""):
                    demand = clean_name(item.findtext('dminsttList', ''), 2)
                    corp = clean_name(item.findtext('corpList', ''), 3)
                    cntrct_date_raw = item.findtext('cntrctDate') or item.findtext('cntrctCnclsDate') or ''
                    end_date_raw = item.findtext('ttalScmpltDate', '')
                    amt = int(item.findtext('totCntrctAmt', '0'))
                    
                    # 계약만료일 계산
                    final_end_date = "-"
                    if end_date_raw and cntrct_date_raw:
                        if '일' in end_date_raw:
                            try:
                                days = int(re.sub(r'[^0-9]', '', end_date_raw))
                                start_dt = datetime.strptime(cntrct_date_raw, "%Y%m%d")
                                final_end_date = (start_dt + timedelta(days=days)).strftime("%Y-%m-%d")
                            except: final_end_date = end_date_raw
                        else:
                            try:
                                final_end_date = datetime.strptime(end_date_raw, "%Y%m%d").strftime("%Y-%m-%d")
                            except: final_end_date = end_date_raw

                    rows.append({
                        '계약일자': cntrct_date_raw,
                        '수요기관명': demand,
                        '계약명': cntrct_nm,
                        '업체명': corp,
                        '계약금액': amt,
                        '계약만료일': final_end_date
                    })
            
            total_count_el = root.find('.//totalCount')
            if total_count_el is not None:
                if page_no * 999 >= int(total_count_el.text):
                    break
            else:
                break
            page_no += 1
            time.sleep(0.2)
        except:
            break
    return rows

def main_fetch_logic():
    now = datetime.now()
    yesterday = (now - timedelta(days=1))
    
    # --- 기간 쪼개기 로직 ---
    # 1. 작년 1월 1일 ~ 작년 12월 31일 (1년)
    # 2. 올해 1월 1일 ~ 어제 (나머지)
    date_ranges = [
        (datetime(now.year - 1, 1, 1).strftime("%Y%m%d"), datetime(now.year - 1, 12, 31).strftime("%Y%m%d")),
        (datetime(now.year, 1, 1).strftime("%Y%m%d"), yesterday.strftime("%Y%m%d"))
    ]
    
    keywords = ['통합관제', 'CCTV']
    all_data = []
    
    status_slot = st.empty()
    
    for start, end in date_ranges:
        for kw in keywords:
            status_slot.info(f"🔍 기간 조회 중: {start} ~ {end} | 키워드: {kw}")
            data = fetch_api_data(start, end, kw)
            all_data.extend(data)
            
    status_slot.empty()
    return pd.DataFrame(all_data)

# --- UI 부분 ---
st.title("🏛️ 나라장터 유지보수 계약 통합 조회 (기간 분할 모드)")

if st.button("🚀 데이터 불러오기"):
    df = main_fetch_logic()
    
    if not df.empty:
        # 중복 제거 (기관별 최신 건)
        df = df.sort_values(by='계약일자', ascending=True)
        df = df.drop_duplicates(subset=['수요기관명'], keep='last')
        
        df['계약일자'] = pd.to_datetime(df['계약일자'], format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d')
        df = df.sort_values(by='계약일자', ascending=False)

        st.metric("총 계약 기관", f"{len(df)}곳")
        st.dataframe(df.style.format({'계약금액': '{:,}원'}), use_container_width=True, height=600)
    else:
        st.warning("데이터가 없습니다. 기간 설정을 확인하세요.")
