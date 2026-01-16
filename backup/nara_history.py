import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re
import io

# --- 1. 226개 광역+기초 통합 리스트 ---
FULL_DISTRICT_LIST = [
    "서울특별시", "부산광역시", "대구광역시", "인천광역시", "광주광역시", "대전광역시", "울산광역시", "세종특별자치시",
    "경기도", "강원특별자치도", "충청북도", "충청남도", "전북특별자치도", "전라남도", "경상북도", "경상남도", "제주특별자치도",
    "인제군", "홍천군", "횡성군", "영월군", "평창군", "정선군", "철원군", "화천군", "양구군", "고성군", "양양군"
]

METRO_LIST = ["전국", "서울특별시", "부산광역시", "대구광역시", "인천광역시", "광주광역시", "대전광역시", "울산광역시", "세종특별자치시", "경기도", "강원특별자치도", "충청북도", "충청남도", "전북특별자치도", "전라남도", "경상북도", "경상남도", "제주특별자치도"]

def get_data_from_gsheet():
    auth_json = os.environ.get('GOOGLE_AUTH_JSON')
    if auth_json is None:
        st.error("❌ 'GOOGLE_AUTH_JSON' 환경 변수가 설정되지 않았습니다.")
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

def parse_date(date_val):
    if not date_val: return None
    clean_val = re.sub(r'[^0-9]', '', str(date_val))
    if len(clean_val) >= 8:
        try: return datetime.strptime(clean_val[:8], "%Y%m%d")
        except: return None
    return None

def calculate_logic(row):
    try:
        cntrct_date = parse_date(row.get('계약일자'))
        start_date = parse_date(row.get('착수일자'))
        period_raw = str(row.get('계약기간', ''))
        total_finish_date = parse_date(row.get('총완수일자'))
        
        final_expire_dt = None
        total_match = re.search(r'(총차|총용역|총)\s*[:\s]*(\d+)', period_raw)
        total_days = int(total_match.group(2)) if total_match else 0
        
        if total_days > 0:
            base_date = start_date if start_date else cntrct_date
            if base_date:
                final_expire_dt = base_date + relativedelta(days=total_days)
        if not final_expire_dt and total_finish_date:
            final_expire_dt = total_finish_date
        if not final_expire_dt:
            date_in_period = re.sub(r'[^0-9]', '', period_raw)
            if len(date_in_period) >= 8:
                final_expire_dt = parse_date(date_in_period[:8])

        if not final_expire_dt:
            return "정보부족", "정보부족"

        today = datetime.now()
        expire_str = final_expire_dt.strftime('%Y-%m-%d')
        if final_expire_dt < today:
            return expire_str, "만료됨"
        else:
            diff = relativedelta(final_expire_dt, today)
            months = diff.years * 12 + diff.months
            remain_str = f"{months}개월 {diff.days}일"
            return expire_str, remain_str
    except:
        return "계산불가", "오류"

st.set_page_config(layout="wide")
st.title("🏛️ 전국 지자체별 유지보수 계약 현황")

try:
    df = get_data_from_gsheet()
    if not df.empty:
        # 1. 필터링 로직 강화 (START WITH 대신 '포함' 여부로 확인)
        def filter_agency(agency_name):
            agency_name = str(agency_name).strip()
            # FULL_DISTRICT_LIST의 단어가 기관명에 들어있는지 확인
            return any(dist in agency_name for dist in FULL_DISTRICT_LIST)

        df = df[df['★가공_수요기관'].apply(filter_agency)]
        df = df[df['★가공_계약명'].str.contains("유지", na=False)]
        df = df[df['★가공_계약명'].str.contains("통합관제", na=False)]

        # 2. 계약 날짜 및 만료 계산
        df[['★가공_계약만료일', '남은기간']] = df.apply(lambda r: pd.Series(calculate_logic(r)), axis=1)
        df['temp_date'] = pd.to_datetime(df['계약일자'], errors='coerce')

        # 3. 중복 제거용 그룹키 생성
        def clean_contract_name(name):
            name = str(name).replace(" ", "")
            name = re.sub(r'\d+차분?', '', name)
            return re.sub(r'\d+', '', name)

        df['contract_group_key'] = df['★가공_계약명'].apply(clean_contract_name)

        # 4. 데이터 분리 및 중복 제거
        # 진행중인 데이터
        active_df = df[df['남은기간'] != "만료됨"].copy()
        active_df = active_df.sort_values('temp_date', ascending=False).drop_duplicates(['★가공_수요기관', 'contract_group_key', '★가공_업체명'])

        # 만료된 데이터 (2025년 이전 포함 전체)
        expired_df = df[df['남은기간'] == "만료됨"].copy()
        expired_df = expired_df.sort_values('temp_date', ascending=False)

        # 5. [인제군 보완 로직] 진행 중인 계약이 없는 모든 기관에 대해 보완
        # 현재 화면에 나올 기관 리스트
        all_target_agencies = df['★가공_수요기관'].unique()
        agencies_with_active = active_df['★가공_수요기관'].unique()
        
        # 유효 계약이 없는 기관들
        missing_agencies = [ag for ag in all_target_agencies if ag not in agencies_with_active]
        
        # 유효 계약이 없는 기관의 만료 데이터 중 가장 최신 것들만 추출
        fallback_df = expired_df[expired_df['★가공_수요기관'].isin(missing_agencies)].copy()
        fallback_df = fallback_df.drop_duplicates(['★가공_수요기관'], keep='first')
        
        def format_expired_label(date_str):
            try: return f"{date_str[:4]}년 계약만료"
            except: return "계약만료"
        
        fallback_df['남은기간'] = fallback_df['★가공_계약만료일'].apply(format_expired_label)

        # 6. 최종 데이터 병합
        final_processed_df = pd.concat([active_df, fallback_df], ignore_index=True)

        # 7. 광역단위 설정
        def get_metro_name(agency):
            agency_str = str(agency)
            for metro in METRO_LIST[1:]:
                if metro in agency_str: return metro
            return "기타"
        
        final_processed_df['광역단위'] = final_processed_df['★가공_수요기관'].apply(get_metro_name)
        final_processed_df['★가공_계약금액'] = pd.to_numeric(final_processed_df['★가공_계약금액'], errors='coerce').fillna(0).astype(int)

        # --- UI 출력 ---
        st.subheader("📍 지역별 필터 선택")
        selected_region = st.radio("광역시도를 선택하세요:", METRO_LIST, horizontal=True)
        display_df = final_processed_df.copy() if selected_region == "전국" else final_processed_df[final_processed_df['광역단위'] == selected_region].copy()

        st.divider()
        st.write(f"### 📊 {selected_region} 분석 현황 (총 {len(display_df)}건)")
        
        cols = ['★가공_수요기관', '★가공_계약명', '★가공_업체명', '★가공_계약금액', '계약일자', '착수일자', '★가공_계약만료일', '남은기간', '계약상세정보URL']
        final_out = display_df[cols].copy()
        final_out.columns = [c.replace('★가공_', '') for c in final_out.columns]
        final_out.columns = [c.replace('계약상세정보URL', 'URL') for c in final_out.columns]

        st.dataframe(
            final_out,
            column_config={
                "URL": st.column_config.LinkColumn("상세정보"),
                "계약금액": st.column_config.NumberColumn("계약금액(원)", format="localized"),
            },
            use_container_width=True, hide_index=True, height=800
        )

        csv = final_out.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
        st.download_button("📥 데이터 다운로드", csv, f"현황_{selected_region}.csv", "text/csv")

    else:
        st.warning("데이터가 없습니다.")
except Exception as e:
    st.error(f"오류 발생: {e}")
