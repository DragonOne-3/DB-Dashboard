import streamlit as st
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import os
import re
import time

# --- 1. 설정 및 API 정보 ---
API_KEY = os.environ.get('DATA_GO_KR_API_KEY')
API_URL = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'

st.set_page_config(page_title="용역 유지보수 내역 조회", layout="wide")

def clean_name(raw_text, index):
    if not raw_text or '^' not in raw_text:
        return raw_text
    parts = raw_text.replace('[', '').replace(']', '').split('^')
    return parts[index] if len(parts) > index else raw_text

def fetch_api_data(start_date, end_date, keyword):
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
            
            # 1. HTTP 상태 코드 확인 (404, 500 등)
            if res.status_code != 200:
                st.error(f"❌ HTTP 오류 발생: 상태 코드 {res.status_code}")
                st.expander("상세 응답 내용 보기").code(res.text)
                break

            # 2. XML 형식 확인 (가장 흔한 오류: HTML 에러 페이지가 올 때)
            content = res.text.strip()
            if not content.startswith('<?xml') and not content.startswith('<response'):
                st.error(f"⚠️ API가 XML이 아닌 데이터를 반환했습니다. (키워드: {keyword})")
                st.expander("실제 서버 응답 메시지 확인").code(content)
                break
                
            root = ET.fromstring(res.content)
            
            # 3. 공공데이터포털 자체 에러 코드 확인 (인증키, 트래픽 초과 등)
            result_code_el = root.find('.//resultCode')
            if result_code_el is not None and result_code_el.text != '00':
                msg = root.find('.//resultMsg').text if root.find('.//resultMsg') is not None else "알 수 없는 에러"
                st.warning(f"🔔 API 서버 메시지: {msg} (코드: {result_code_el.text})")
                break

            items = root.findall('.//item')
            if not items:
                break
            
            for item in items:
                cntrct_nm = item.findtext('cntrctNm', '')
                if '유지' in cntrct_nm.replace(" ", ""):
                    demand = clean_name(item.findtext('dminsttList', ''), 2)
                    corp = clean_name(item.findtext('corpList', ''), 3)
                    c_date = item.findtext('cntrctDate') or item.findtext('cntrctCnclsDate') or ''
                    e_date = item.findtext('ttalScmpltDate', '')
                    amt = int(item.findtext('totCntrctAmt', '0'))
                    
                    # 만료일 계산
                    final_end_date = "-"
                    if e_date and c_date:
                        try:
                            if '일' in e_date:
                                days = int(re.sub(r'[^0-9]', '', e_date))
                                start_dt = datetime.strptime(c_date[:8], "%Y%m%d")
                                final_end_date = (start_dt + timedelta(days=days)).strftime("%Y-%m-%d")
                            else:
                                final_end_date = datetime.strptime(e_date[:8], "%Y%m%d").strftime("%Y-%m-%d")
                        except: final_end_date = e_date

                    rows.append({
                        '계약일자': c_date[:8],
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
            time.sleep(0.3) # API 제한 방지

        except ET.ParseError as e:
            st.error(f"❌ XML 해석 실패 (Parse Error): {e}")
            st.expander("해석에 실패한 원본 텍스트 확인").code(res.text)
            break
        except Exception as e:
            st.error(f"❌ 실행 중 오류 발생: {str(e)}")
            break
            
    return rows

def main_fetch_logic():
    now = datetime.now()
    yesterday = (now - timedelta(days=1))
    
    # 기간 분할 (1년 단위)
    date_ranges = [
        (datetime(now.year - 1, 1, 1).strftime("%Y%m%d"), datetime(now.year - 1, 12, 31).strftime("%Y%m%d")),
        (datetime(now.year, 1, 1).strftime("%Y%m%d"), yesterday.strftime("%Y%m%d"))
    ]
    
    keywords = ['통합관제', 'CCTV']
    all_data = []
    
    status_slot = st.empty()
    
    for start, end in date_ranges:
        for kw in keywords:
            status_slot.info(f"⏳ 데이터 수집 중: {start}~{end} | 키워드: {kw}")
            data = fetch_api_data(start, end, kw)
            all_data.extend(data)
            
    status_slot.empty()
    return pd.DataFrame(all_data)

# --- UI ---
st.title("🏛️ 나라장터 유지보수 계약 통합 조회")

if st.button("🚀 최신 데이터 불러오기"):
    # API 키 존재 여부 먼저 확인
    if not API_KEY:
        st.error("❌ API 키가 설정되지 않았습니다. Streamlit Secrets 설정을 확인하세요.")
    else:
        df = main_fetch_logic()
        
        if not df.empty:
            df = df.sort_values(by='계약일자', ascending=True)
            df = df.drop_duplicates(subset=['수요기관명'], keep='last')
            df['계약일자'] = pd.to_datetime(df['계약일자'], format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d')
            df = df.sort_values(by='계약일자', ascending=False)

            st.success(f"✅ 조회가 완료되었습니다. (총 {len(df)}건)")
            st.dataframe(df.style.format({'계약금액': '{:,}원'}), use_container_width=True, height=600)
        else:
            st.warning("⚠️ 검색 결과가 없습니다. 위에 표시된 오류 내역이 없다면 해당 조건의 데이터가 실제로 없는 것입니다.")
