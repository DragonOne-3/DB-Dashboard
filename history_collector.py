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
    '네트워크시스템장비용랙', '영상감시장치', 'PA용스피커', '안내판', '카메라브래킷', '액정모니터', '광송수신모듈',
    '전원공급장치', '광분배함', '컨버터', '컴퓨터서버', '하드디스크드라이브', '네트워크스위치', '광점퍼코드', '풀박스',
    '서지흡수기', '디지털비디오레코더', '스피커', '오디오앰프', '브래킷', 'UTP케이블', '정보통신공사',
    '영상정보디스플레이장치', '송신기', '난연전력케이블', '1종금속제가요전선관', '호온스피커', '누전차단기',
    '방송수신기', 'LAP외피광케이블', '폴리에틸렌전선관', '리모트앰프', '랙캐비닛용패널', '베어본컴퓨터', '분배기',
    '결선보드유닛', '벨', '난연접지용비닐절연전선', '경광등', '데스크톱컴퓨터', '특수목적컴퓨터', '철근콘크리트공사',
    '토공사', '안내전광판', '접지봉', '카메라회전대', '무선랜액세스포인트', '컴퓨터망전환장치', '포장공사',
    '고주파동축케이블', '카메라하우징', '인터폰', '스위칭모드전원공급장치', '금속상자', '열선감지기', '태양전지조절기',
    '밀폐고정형납축전지', 'IP전화기', '디스크어레이', '그래픽용어댑터', '인터콤장비', '기억유닛', '컴퓨터지문인식장치',
    '랜접속카드', '접지판', '제어케이블', '비디오네트워킹장비', '레이스웨이', '콘솔익스텐더', '전자카드',
    '비대면방역감지장비', '온습도트랜스미터', '도난방지기', '융복합영상감시장치', '멀티스크린컴퓨터',
    '컴퓨터정맥인식장치', '카메라컨트롤러', 'SSD저장장치', '원격단말장치(RTU)', '융복합네트워크스위치',
    '융복합액정모니터', '융복합데스크톱컴퓨터', '융복합그래픽용어댑터', '융복합베어본컴퓨터', '융복합서지흡수기',
    '배선장치', '융복합배선장치', '융복합카메라브래킷', '융복합네트워크시스템장비용랙', '융복합UTP케이블',
    '테이프백업장치', '자기식테이프', '레이드저장장치', '광송수신기', '450/750V 유연성단심비닐절연전선', '솔내시스템',
    '450/750V유연성단심비닐절연전선', '카메라받침대', '텔레비전거치대', '광수신기', '무선통신장치', '동작분석기',
    '전력공급장치', '450/750V 일반용유연성단심비닐절연전선', '분전함', '비디오믹서', '절연전선및피복선', '레이더',
    '적외선방사기', '보안용카메라', '통신소프트웨어', '분석및과학용소프트웨어', '소프트웨어유지및지원서비스',
    '교통관제시스템', '산업관리소프트웨어', '시스템관리소프트웨어', '적외선카메라', '주차경보등', '주차관제주변기기',
    '주차권판독기', '주차안내판', '주차요금계산기', '주차주제어장치', '차량감지기', '차량인식기', '차량차단기',
    '패키지소프트웨어개발및도입서비스', '무선인식리더기', '바코드시스템', '출입통제시스템', '카드인쇄기'
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
