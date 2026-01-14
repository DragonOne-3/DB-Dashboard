import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re

# --- 226개 광역+기초 통합 리스트 (중복 이름 구분용) ---
FULL_DISTRICT_LIST = [
    "서울특별시 종로구", "서울특별시 중구", "서울특별시 용산구", "서울특별시 성동구", "서울특별시 광진구", "서울특별시 동대문구", "서울특별시 중랑구", "서울특별시 성북구", "서울특별시 강북구", "서울특별시 도봉구", "서울특별시 노원구", "서울특별시 은평구", "서울특별시 서대문구", "서울특별시 마포구", "서울특별시 양천구", "서울특별시 강서구", "서울특별시 구로구", "서울특별시 금천구", "서울특별시 영등포구", "서울특별시 동작구", "서울특별시 관악구", "서울특별시 서초구", "서울특별시 강남구", "서울특별시 송파구", "서울특별시 강동구",
    "부산광역시 중구", "부산광역시 서구", "부산광역시 동구", "부산광역시 영도구", "부산광역시 부산진구", "부산광역시 동래구", "부산광역시 남구", "부산광역시 북구", "부산광역시 해운대구", "부산광역시 사하구", "부산광역시 금정구", "부산광역시 강서구", "부산광역시 연제구", "부산광역시 수영구", "부산광역시 사상구", "부산광역시 기장군",
    "대구광역시 중구", "대구광역시 동구", "대구광역시 서구", "대구광역시 남구", "대구광역시 북구", "대구광역시 수성구", "대구광역시 달서구", "대구광역시 달성군", "대구광역시 군위군",
    "인천광역시 중구", "인천광역시 동구", "인천광역시 미추홀구", "인천광역시 연수구", "인천광역시 남동구", "인천광역시 부평구", "인천광역시 계양구", "인천광역시 서구", "인천광역시 강화군", "인천광역시 옹진군",
    "광주광역시 동구", "광주광역시 서구", "광주광역시 남구", "광주광역시 북구", "광주광역시 광산구",
    "대전광역시 동구", "대전광역시 중구", "대전광역시 서구", "대전광역시 유성구", "대전광역시 대덕구",
    "울산광역시 중구", "울산광역시 남구", "울산광역시 동구", "울산광역시 북구", "울산광역시 울주군",
    "세종특별자치시",
    "경기도 수원시", "경기도 성남시", "경기도 의정부시", "경기도 안양시", "경기도 부천시", "경기도 광명시", "경기도 평택시", "경기도 동두천시", "경기도 안산시", "경기도 고양시", "경기도 과천시", "경기도 구리시", "경기도 남양주시", "경기도 오산시", "경기도 시흥시", "경기도 군포시", "경기도 의왕시", "경기도 하남시", "경기도 용인시", "경기도 파주시", "경기도 이천시", "경기도 안성시", "경기도 김포시", "경기도 화성시", "경기도 광주시", "경기도 양주시", "경기도 포천시", "경기도 여주시", "경기도 연천군", "경기도 가평군", "경기도 양평군",
    "강원특별자치도 춘천시", "강원특별자치도 원주시", "강원특별자치도 강릉시", "강원특별자치도 동해시", "강원특별자치도 태백시", "강원특별자치도 속초시", "강원특별자치도 삼척시", "강원특별자치도 홍천군", "강원특별자치도 횡성군", "강원특별자치도 영월군", "강원특별자치도 평창군", "강원특별자치도 정선군", "강원특별자치도 철원군", "강원특별자치도 화천군", "강원특별자치도 양구군", "강원특별자치도 인제군", "강원특별자치도 고성군", "강원특별자치도 양양군",
    "충청북도 청주시", "충청북도 충주시", "충청북도 제천시", "충청북도 보은군", "충청북도 옥천군", "충청북도 영동군", "충청북도 증평군", "충청북도 진천군", "충청북도 괴산군", "충청북도 음성군", "충청북도 단양군",
    "충청남도 천안시", "충청남도 공주시", "충청남도 보령시", "충청남도 아산시", "충청남도 서산시", "충청남도 논산시", "충청남도 계룡시", "충청남도 당진시", "충청남도 금산군", "충청남도 부여군", "충청남도 서천군", "충청남도 청양군", "충청남도 홍성군", "충청남도 예산군", "충청남도 태안군",
    "전북특별자치도 전주시", "전북특별자치도 군산시", "전북특별자치도 익산시", "전북특별자치도 정읍시", "전북특별자치도 남원시", "전북특별자치도 김제시", "전북특별자치도 완주군", "전북특별자치도 진안군", "전북특별자치도 무주군", "전북특별자치도 장수군", "전북특별자치도 임실군", "전북특별자치도 순창군", "전북특별자치도 고창군", "전북특별자치도 부안군",
    "전라남도 목포시", "전라남도 여수시", "전라남도 순천시", "전라남도 나주시", "전라남도 광양시", "전라남도 담양군", "전라남도 곡성군", "전라남도 구례군", "전라남도 고흥군", "전라남도 보성군", "전라남도 화순군", "전라남도 장흥군", "전라남도 강진군", "전라남도 해남군", "전라남도 영암군", "전라남도 무안군", "전라남도 함평군", "전라남도 영광군", "전라남도 장성군", "전라남도 완도군", "전라남도 진도군", "전라남도 신안군",
    "경상북도 포항시", "경상북도 경주시", "경상북도 김천시", "경상북도 안동시", "경상북도 구미시", "경상북도 영주시", "경상북도 상주시", "경상북도 문경시", "경상북도 경산시", "경상북도 의성군", "경상북도 청송군", "경상북도 영양군", "경상북도 영덕군", "경상북도 청도군", "경상북도 고령군", "경상북도 성주군", "경상북도 칠곡군", "경상북도 예천군", "경상북도 봉화군", "경상북도 울진군", "경상북도 울릉군",
    "경상남도 창원시", "경상남도 진주시", "경상남도 통영시", "경상남도 사천시", "경상남도 김해시", "경상남도 밀양시", "경상남도 거제시", "경상남도 양산시", "경상남도 의령군", "경상남도 함안군", "경상남도 창녕군", "경상남도 고성군", "경상남도 남해군", "경상남도 하동군", "경상남도 산청군", "경상남도 함양군", "경상남도 거창군", "경상남도 합천군",
    "제주특별자치도 제주시", "제주특별자치도 서귀포시"
]
def get_data_from_gsheet():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    
    if auth_json is None:
        st.error("❌ 'GOOGLE_AUTH_JSON' 환경 변수가 설정되지 않았습니다. 로컬 테스트 시에는 JSON 파일을 직접 참조하도록 코드를 수정하거나 환경 변수를 등록하세요.")
        return pd.DataFrame()

    try:
        creds_dict = json.loads(auth_json)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        sh = client.open("나라장터_용역계약내역")
        ws = sh.get_worksheet(0)
        return pd.DataFrame(ws.get_all_records())
    except Exception as e:
        st.error(f"❌ 시트 로드 중 오류: {e}")
        return pd.DataFrame()
def calculate_remain_period(row):
    """오늘 날짜 기준으로 계약 만료일까지 남은 기간 계산"""
    try:
        # 1. 만료일 가져오기 (시트의 '계약만료일' 컬럼 사용)
        expire_raw = str(row.get('계약만료일', ''))
        # 2. 날짜 형식 추출 (YYYY-MM-DD 또는 YYYYMMDD 등)
        expire_date_str = re.sub(r'[^0-9]', '', expire_raw)
        
        if len(expire_date_str) < 8:
            return "정보부족"
            
        expire_date = datetime.strptime(expire_date_str[:8], "%Y%m%d")
        today = datetime.now()
        
        if expire_date < today:
            return "만료됨"
            
        # 3. M개월 D일 계산
        diff = relativedelta(expire_date, today)
        months = diff.years * 12 + diff.months
        return f"{months}개월 {diff.days}일"
    except:
        return "계산불가"

def load_data():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    if not auth_json:
        st.error("Secrets에 GOOGLE_AUTH_JSON 설정이 필요합니다.")
        return pd.DataFrame()

    try:
        creds_dict = json.loads(auth_json)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        
        sh = client.open("나라장터_용역계약내역")
        ws = sh.get_worksheet(0)
        
        # 데이터 로드 (첫 줄을 제목으로 인식)
        data = ws.get_all_records()
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"데이터 로드 오류: {e}")
        return pd.DataFrame()

# --- 화면 구성 ---
st.set_page_config(layout="wide", page_title="지자체 계약정보")
st.title("🏛️ 전국 기초자치단체 용역 계약 현황")

df = load_data()

if not df.empty:
    try:
        # 1. 기초자치단체 필터링 (광역+기초 명칭 포함 여부)
        # 시트의 컬럼명이 '수요기관'이라고 가정합니다. (아까 바꾸신 이름 확인)
        target_col = '수요기관' if '수요기관' in df.columns else '★가공_수요기관'
        
        df = df[df[target_col].apply(lambda x: any(dist in str(x) for dist in FULL_DISTRICT_LIST))]

        # 2. 남은 기간 실시간 계산
        df['남은기간'] = df.apply(calculate_remain_period, axis=1)

        # 3. 동일 지자체 내 최근 계약일자 데이터만 남기기
        # 시트의 컬럼명이 '계약일자'라고 가정합니다.
        date_col = '계약일자' if '계약일자' in df.columns else '★가공_계약일'
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        df = df.sort_values(by=[target_col, date_col], ascending=[True, False])
        df = df.drop_duplicates(subset=[target_col], keep='first')

        # 4. 최종 표출 데이터 정리 (요청하신 순서)
        # 컬럼명이 시트와 정확히 일치해야 합니다.
        display_cols = [
            target_col, '계약명', '업체명', '계약금액', 
            date_col, '착수일자', '계약만료일', '남은기간', '상세URL'
        ]
        
        # 시트에 존재하지 않는 컬럼이 있을 경우를 대비한 안전 처리
        final_cols = [c for c in display_cols if c in df.columns or c == '남은기간']
        result_df = df[final_cols].copy()

        # 5. 테이블 출력 (각 항목 정렬 가능)
        st.dataframe(
            result_df,
            column_config={
                "상세URL": st.column_config.LinkColumn("계약상세정보URL"),
                "계약금액": st.column_config.NumberColumn(format="%d원"),
                date_col: st.column_config.DateColumn("계약일자")
            },
            use_container_width=True,
            hide_index=True
        )

    except Exception as e:
        st.error(f"데이터 처리 중 오류: {e}")
else:
    st.info("데이터를 불러오는 중이거나 시트가 비어있습니다.")
