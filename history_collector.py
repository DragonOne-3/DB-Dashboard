import os
import io
import json
import time
import datetime
import requests
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseUpload


# =================================================================================
# 1. 설정 정보 (main.py와 동일하게 맞춤)
# =================================================================================
MY_DIRECT_KEY = os.environ.get('DATA_GO_KR_API_KEY')
AUTH_JSON_STR = os.environ.get('GOOGLE_AUTH_JSON')

# main.py의 SHOPPING_FOLDER_ID와 반드시 동일해야 같은 폴더/같은 {year}.csv에 쌓입니다.
SHOPPING_FOLDER_ID = "1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr"

# 국문 헤더 (총 39개 필드) - main.py의 HEADER_KOR와 동일
HEADER_KOR = [
    '조달구분명', '계약구분명', '계약납품구분명', '계약납품요구일자', '계약납품요구번호', '변경차수', '최종변경차수여부',
    '수요기관명', '수요기관구분명', '수요기관지역명', '수요기관코드', '물품분류번호', '품명', '세부물품분류번호',
    '세부품명', '물품식별번호', '물품규격명', '단가', '수량', '단위', '금액', '업체명', '업체기업구분명', '계약명',
    '우수제품여부', '공사용자재직접구매대상여부', '다수공급자계약여부', '다수공급자계약2단계진행여부', '단가계약번호',
    '단가계약변경차수', '최초계약(납품요구)일자', '계약체결방법명', '증감수량', '증감금액', '납품장소명', '납품기한일자',
    '업체사업자등록번호', '인도조건명', '물품순번'
]

# 품목 리스트 (main.py의 keywords와 동일, 중복 제거해서 정렬)
keywords = sorted(list(set([
    "1종금속제가요전선관",     "3차원프린터",    "450/750V 유연성단심비닐절연전선",    "450/750V 일반용유연성단심비닐절연전선",    "450/750V유연성단심비닐절연전선",    "AV스위쳐",    "CD녹음및플레이어",    "DVD드라이브",   "IP전화기",    "LAP외피광케이블",    "LED가로등기구",    "LED경관조명기구",
    "LED다운라이트",    "LED램프",    "LED보안등기구",    "LED실내조명등",    "LED터널용등기구",    "LED투광등기구",    "PA용스피커",    "SSD저장장치",    "UTP케이블",    "가로등자동점멸기",    "가로등주부속자재",    "견인용갈고리",    "결선보드유닛",    "경관조명기구",    "경광등",    "계장제어장치",
    "고주파동축케이블",    "광분배함",    "광송수신기",    "광송수신모듈",    "광수신기",    "광점퍼코드",    "교육용소프트웨어",    "교통관제시스템",    "교통신호등",    "교통신호제어기",    "구내단자함",    "구내방송장치",    "그래픽용어댑터",   "그레이팅덮개",    "금속기둥",    "금속상자",    "기상전광판",
    "기억유닛",    "난연전력케이블",    "난연접지용비닐절연전선",    "냉각팬",    "네트워크스위치",    "네트워크시스템장비용랙",    "네트워크회의용소프트웨어",    "논슬립",   "누전차단기",    "다목적승용차",    "대기오염측정기",    "데스크톱컴퓨터",    "데이터베이스관리소프트웨어",
    "도난방지기",    "도로안전표지판지주",    "도로표지병",    "도서관리시스템",    "동작분석기",    "등기구보강대",    "디바이더",    "디스크어레이",    "디지털비디오레코더",    "라디오튜너",    "랙캐비닛용패널",    "랜접속카드",    "레이더",    "레이드저장장치",    "레이드컨트롤러",    "레이스웨이",
    "리모트앰프",    "릴레이유닛",    "마그네틱카드판독기",    "마을무선방송장치",   "마이크로폰",    "마이크스탠드",    "매트릭스로직유닛",    "멀티미디어학습장치",    "멀티스크린컴퓨터",    "멀티탭",    "메가폰",    "무선랜액세스포인트",   "무선마이크장치",    "무선인식리더기",
    "무선통신장치",    "무인교통감시장치",    "무정전전원장치",    "밀폐고정형납축전지",    "바닥형보행신호등",    "바코드시스템",    "방송수신기",    "방화벽장치",    "배선장치",    "버스및차량정보안내장치",    "베어본컴퓨터",    "벨",    "보건용마스크",    "보관용선반",    "보안소프트웨어",
    "보안용카메라",    "보행매트",    "보행신호음성안내보조장치",    "보행자안전차단기",    "보행자작동신호기",    "볼라드",    "분배기",    "분석및과학용소프트웨어",    "분전반",    "분전함",    "브래킷",    "비대면방역감지장비",    "비디오네트워킹장비",    "비디오믹서",    "비디오프로젝터",
    "비상경보기",    "비상유닛",    "산업관리소프트웨어",    "서지흡수기",    "세탁물건조기",    "소방용방화복세탁기",    "소프트웨어유지및지원서비스",    "소형기기용충전기",    "솔내시스템",    "송신기",    "수업자동녹화시스템",    "수위조절기",    "스위치박스",    "스위칭모드전원공급장치",    "스테이플",
    "스테인리스가로등주",    "스피커",    "스피커선택유닛",    "스피커스탠드",    "시스템관리소프트웨어",    "식별용태그",    "안내전광판",    "안내판",    "액정모니터",    "엔코더",    "열선감지기",    "영사대",    "영사용스크린",    "영상감시장치",    "영상분배기",    "영상정보디스플레이장치",    "영상회의시스템",
    "오디오모니터",    "오디오믹서",    "오디오앰프",    "온습도트랜스미터",    "우산빗물제거기",    "운영체제",    "원격단말장치(RTU)",    "원격자동검침시스템",    "유틸리티소프트웨어",    "융복합UTP케이블",    "융복합그래픽용어댑터",    "융복합네트워크스위치",    "융복합네트워크시스템장비용랙",    "융복합대기오염측정기",
    "융복합데스크톱컴퓨터",    "융복합무선데이터통신장비",    "융복합배선장치",    "융복합베어본컴퓨터",    "융복합서지흡수기",    "융복합안내전광판",    "융복합액정모니터",    "융복합영상감시장치",    "융복합카메라브래킷",    "융복합화염감지기",    "응용과학용소프트웨어",    "의료용살충제",    "이퀄라이저",
    "인증관리시스템",    "인터랙티브화이트보드",    "인터콤장비",    "인터폰",    "자기식테이프",    "자동변속기",    "자동승강조명장치",    "장치제어보드",    "적외선방사기",    "적외선카메라",    "적외선탐지기",    "전동기제어반",     "전력공급장치",    "전원공급장치",    "전자카드",    "절연전선및피복선",    
    "접지봉",    "접지판",    "정보통신공사",    "정보화교육서비스",    "제어케이블",    "조명용제어장치",    "조명타워",    "종합폴",    "주차경보등",    "주차관제주변기기",    "주차권판독기",    "주차안내판",    "주차요금계산기",    "주차주제어장치",
    "주파수분할다중화장치",    "지도소프트웨어",    "차량감지기",    "차량검지기",    "차량도장서비스",    "차량번호판독기",    "차량인식기",    "차량지지용아우트리거",    "차량차단기",    "철근콘크리트공사",    "철제가로등주",    "출입통제시스템",    "카드락",    "카드인쇄기",    "카메라받침대",
    "카메라브래킷",    "카메라컨트롤러",    "카메라하우징",    "카메라회전대",    "캠코더",    "컨버터",    "컴바이너",    "컴퓨터망전환장치",    "컴퓨터및주변기기설치",    "컴퓨터서버",    "컴퓨터정맥인식장치",    "컴퓨터지문인식장치",    "케이블타이",    "콘솔익스텐더",    "콘텐츠관리소프트웨어",    "콤바인",
    "탐조등",    "태양광가로등",    "태양광발전장치",    "태양전지조절기",    "테이프백업장치",    "텔레비전",    "텔레비전거치대",    "토공사",    "통신소프트웨어",    "통신용변조기",    "통신케이블어셈블리",    "통합배선반",     "특수목적컴퓨터",    "패키지소프트웨어개발및도입서비스",    "패키지용품",
    "폐쇄형배전반",    "포장공사",    "폴리에틸렌전선관",    "풀박스",    "플러그용잭",    "피뢰탄기반",    "하드디스크드라이브",    "호온스피커"  
])))

DEDUP_SUBSET = ["계약납품요구일자", "수요기관명", "품명", "금액"]


# =================================================================================
# 2. 유틸리티 함수
# =================================================================================
def get_drive_service_for_script():
    """main.py와 동일한 Drive API 인증 방식 (gspread 아님)"""
    info = json.loads(AUTH_JSON_STR)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=creds), creds


def get_year_ranges(start_date_str, end_date_str):
    """YYYYMMDD ~ YYYYMMDD 구간을 연도 경계로 쪼갠 (year, s_date, e_date) 리스트 생성.
    2026.csv 처럼 파일이 연도 단위이므로, 연도가 바뀌는 지점에서만 나눈다."""
    start = datetime.datetime.strptime(start_date_str, "%Y%m%d").date()
    end = datetime.datetime.strptime(end_date_str, "%Y%m%d").date()
    if start > end:
        raise ValueError(f"START_DATE({start_date_str})가 END_DATE({end_date_str})보다 뒤입니다.")

    ranges = []
    cur = start
    while cur <= end:
        year_end = datetime.date(cur.year, 12, 31)
        seg_end = min(year_end, end)
        ranges.append((cur.year, cur.strftime("%Y%m%d"), seg_end.strftime("%Y%m%d")))
        cur = datetime.date(cur.year + 1, 1, 1)
    return ranges


def fetch_all_pages_data(keyword, start_date, end_date, retries=3):
    """키워드별 전체 페이지 데이터 수집 (파라미터명 'type'으로 수정, 타임아웃 재시도 추가)"""
    all_data = []
    current_page = 1
    while True:
        url = "https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getSpcifyPrdlstPrcureInfoList"
        params = {
            'numOfRows': '999',
            'pageNo': str(current_page),
            'ServiceKey': MY_DIRECT_KEY,
            'type': 'xml',  # main.py와 통일 (기존 'Type_A'는 오타로 추정)
            'inqryDiv': '1',
            'inqryPrdctDiv': '2',
            'inqryBgnDate': start_date,
            'inqryEndDate': end_date,
            'dtilPrdctClsfcNoNm': keyword,
        }

        page_ok = False
        for attempt in range(retries):
            try:
                res = requests.get(url, params=params, timeout=30)
                if res.status_code == 200 and "<item>" in res.text:
                    root = ET.fromstring(res.content)
                    items = root.findall('.//item')
                    for item in items:
                        all_data.append([elem.text if elem.text else '' for elem in item])

                    total_count_elem = root.find('.//totalCount')
                    page_ok = True
                    if total_count_elem is None or not items:
                        return all_data
                    total_count = int(total_count_elem.text)
                    if len(all_data) >= total_count:
                        return all_data
                    break  # 다음 페이지로 진행
                else:
                    # 정상 응답이지만 item 없음 = 해당 페이지엔 데이터 없음 (정상 종료)
                    page_ok = True
                    return all_data
            except requests.exceptions.Timeout:
                wait = (attempt + 1) * 5
                print(f"      ⏳ [{keyword}] p{current_page} 타임아웃 ({attempt + 1}/{retries}), {wait}초 후 재시도...")
                time.sleep(wait)
            except Exception as e:
                print(f"      ❌ [{keyword}] p{current_page} 에러: {e}")
                break

        if not page_ok:
            print(f"      ⚠️ [{keyword}] p{current_page} 최종 실패, 해당 키워드 이후 페이지 스킵 (데이터 유실 가능)")
            break

        current_page += 1
        time.sleep(0.3)

    return all_data


def save_shopping_by_year(drive_service, creds, year, new_df):
    """main.py PART 1과 동일한 방식: SHOPPING_FOLDER_ID 안의 {year}.csv 하나에 병합 저장"""
    file_name = f"{year}.csv"
    query = f"name='{file_name}' and '{SHOPPING_FOLDER_ID}' in parents and trashed=false"
    res = drive_service.files().list(q=query, fields="files(id)").execute()
    items = res.get("files", [])
    file_id = items[0]["id"] if items else None

    if file_id:
        resp = requests.get(
            f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media",
            headers={"Authorization": f"Bearer {creds.token}"},
        )
        try:
            old_df = pd.read_csv(io.BytesIO(resp.content), encoding="utf-8-sig", low_memory=False)
            merged_df = pd.concat([old_df, new_df], ignore_index=True)
        except Exception as e:
            print(f"   ⚠️ 기존 {file_name} 읽기 오류, 신규 데이터만 사용: {e}")
            merged_df = new_df
    else:
        merged_df = new_df

    before = len(merged_df)
    merged_df = merged_df.drop_duplicates(subset=DEDUP_SUBSET, keep="last")
    removed = before - len(merged_df)

    csv_bytes = merged_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    media = MediaIoBaseUpload(io.BytesIO(csv_bytes), mimetype="text/csv")

    if file_id:
        drive_service.files().update(fileId=file_id, media_body=media).execute()
    else:
        drive_service.files().create(
            body={"name": file_name, "parents": [SHOPPING_FOLDER_ID]},
            media_body=media,
        ).execute()
        print(f"   🆕 {file_name} 신규 생성")

    print(f"✅ [{year}] {file_name} 저장 완료 (신규수집 {len(new_df):,}건, 중복제거 {removed:,}건, 최종 {len(merged_df):,}건)")


# =================================================================================
# 3. 메인 로직
# =================================================================================
def main():
    if not MY_DIRECT_KEY or not AUTH_JSON_STR:
        print("❌ DATA_GO_KR_API_KEY 또는 GOOGLE_AUTH_JSON이 설정되지 않았습니다.")
        return

    drive_service, drive_creds = get_drive_service_for_script()

    start_date_env = os.environ.get('START_DATE')
    end_date_env = os.environ.get('END_DATE')

    if start_date_env and end_date_env:
        year_ranges = get_year_ranges(start_date_env, end_date_env)
    else:
        # 하위호환: START_YEAR/END_YEAR로 연 단위 백필
        start_year = int(os.environ.get('START_YEAR', 2021))
        end_year = int(os.environ.get('END_YEAR', 2025))
        year_ranges = [
            (yr, f"{yr}0101", f"{yr}1231")
            for yr in range(start_year, end_year + 1)
        ]

    for year, s_date, e_date in year_ranges:
        print(f"📅 [{year}년 / {s_date}~{e_date}] 수집 시작...")

        final_data = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(fetch_all_pages_data, kw, s_date, e_date): kw
                for kw in keywords
            }
            for future in as_completed(futures):
                kw = futures[future]
                try:
                    data = future.result()
                    if data:
                        final_data.extend(data)
                        print(f"   ✅ {kw}: {len(data)}건 수집")
                except Exception as e:
                    print(f"   ❌ {kw}: 수집 실패 - {e}")

        if not final_data:
            print(f"   ⚠️ [{year}] 수집된 데이터 없음, 저장 스킵")
            continue

        new_df = pd.DataFrame(final_data, columns=HEADER_KOR)
        save_shopping_by_year(drive_service, drive_creds, year, new_df)


if __name__ == "__main__":
    main()
