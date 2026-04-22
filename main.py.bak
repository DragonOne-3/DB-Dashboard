import os, json, datetime, time, requests
import xml.etree.ElementTree as ET
import pandas as pd
import io
import threading
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseUpload
from concurrent.futures import ThreadPoolExecutor, as_completed

# =================================================================================
# 1. 설정 및 환경 변수
# =================================================================================
MY_DIRECT_KEY = os.environ.get('DATA_GO_KR_API_KEY')
AUTH_JSON_STR = os.environ.get('GOOGLE_AUTH_JSON')

# ★ 나라장터 공고 연도별 파일 저장 폴더 ID
NOTICE_FOLDER_ID = "1AsvVmayEmTtY92d1SfXxNi6bL0Zjw5mg"

# ★ 종합쇼핑몰 연도별 CSV 저장 폴더 ID
SHOPPING_FOLDER_ID = "1N2GjNTpOvtn-5Vbg5zf6Y8kf4xuq0qTr"

HEADER_KOR = [
    '조달구분명', '계약구분명', '계약납품구분명', '계약납품요구일자', '계약납품요구번호',
    '변경차수', '최종변경차수여부', '수요기관명', '수요기관구분명', '수요기관지역명',
    '수요기관코드', '물품분류번호', '품명', '세부물품분류번호', '세부품명',
    '물품식별번호', '물품규격명', '단가', '수량', '단위', '금액', '업체명',
    '업체기업구분명', '계약명', '우수제품여부', '공사용자재직접구매대상여부',
    '다수공급자계약여부', '다수공급자계약2단계진행여부', '단가계약번호', '단가계약변경차수',
    '최초계약(납품요구)일자', '계약체결방법명', '증감수량', '증감금액', '납품장소명',
    '납품기한일자', '업체사업자등록번호', '인도조건명', '물품순번'
]

CAT_KEYWORDS = {
    '영상감시장치': ['CCTV', '통합관제', '영상감시장치', '영상정보처리기기'],
    '국방': ['국방', '부대', '작전', '경계', '방위', '군사', '무인화', '사령부', '군대', '중요시설', '주둔지', '과학화', '육군', '해군', '공군', '해병'],
    '솔루션': ['데이터', '플랫폼', '솔루션', '주차', '출입', 'GIS'],
    '스마트도시': ['ITS', '스마트시티', '스마트도시']
}

keywords = sorted(list(set([
    '네트워크시스템장비용랙', '영상감시장치', 'PA용스피커', '안내판', '카메라브래킷', '액정모니터',
    '광송수신모듈', '전원공급장치', '광분배함', '컨버터', '컴퓨터서버', '하드디스크드라이브',
    '네트워크스위치', '광점퍼코드', '풀박스', '서지흡수기', '디지털비디오레코더',
    '스피커', '오디오앰프', '브래킷', 'UTP케이블', '정보통신공사', '영상정보디스플레이장치',
    '송신기', '난연전력케이블', '1종금속제가요전선관', '호온스피커', '누전차단기', '방송수신기',
    'LAP외피광케이블', '폴리에틸렌전선관', '리모트앰프', '랙캐비닛용패널', '베어본컴퓨터',
    '분배기', '결선보드유닛', '벨', '난연접지용비닐절연전선', '경광등', '데스크톱컴퓨터',
    '특수목적컴퓨터', '철근콘크리트공사', '토공사', '안내전광판', '접지봉', '카메라회전대',
    '무선랜액세스포인트', '컴퓨터망전환장치', '포장공사', '고주파동축케이블', '카메라하우징',
    '인터폰', '스위칭모드전원공급장치', '금속상자', '열선감지기', '태양전지조절기',
    '밀폐고정형납축전지', 'IP전화기', '디스크어레이', '그래픽용어댑터', '인터콤장비',
    '기억유닛', '컴퓨터지문인식장치', '랜접속카드', '접지판', '제어케이블', '비디오네트워킹장비',
    '레이스웨이', '콘솔익스텐더', '전자카드', '비대면방역감지장비', '온습도트랜스미터',
    '도난방지기', '융복합영상감시장치', '멀티스크린컴퓨터', '컴퓨터정맥인식장치',
    '카메라컨트롤러', 'SSD저장장치', '원격단말장치(RTU)', '융복합네트워크스위치',
    '융복합액정모니터', '융복합데스크톱컴퓨터', '융복합그래픽용어댑터', '융복합베어본컴퓨터',
    '융복합서지흡수기', '배선장치', '융복합배선장치', '융복합카메라브래킷',
    '융복합네트워크시스템장비용랙', '융복합UTP케이블', '테이프백업장치', '자기식테이프',
    '레이드저장장치', '광송수신기', '450/750V 유연성단심비닐절연전선', '솔내시스템',
    '450/750V유연성단심비닐절연전선', '카메라받침대', '텔레비전거치대', '광수신기',
    '무선통신장치', '동작분석기', '전력공급장치', '450/750V 일반용유연성단심비닐절연전선',
    '분전함', '비디오믹서', '절연전선및피복선', '레이더', '적외선방사기', '보안용카메라',
    '통신소프트웨어', '분석및과학용소프트웨어', '소프트웨어유지및지원서비스',
    '교통관제시스템', '산업관리소프트웨어', '시스템관리소프트웨어', '적외선카메라',
    '주차경보등', '주차관제주변기기', '주차권판독기', '주차안내판', '주차요금계산기',
    '주차주제어장치', '차량감지기', '차량인식기', '차량차단기',
    '패키지소프트웨어개발및도입서비스', '무선인식리더기', '바코드시스템', '출입통제시스템', '카드인쇄기'
])))

NOTICE_API_MAP = {
    '공사': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwkPPSSrch',
    '물품': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThngPPSSrch',
    '용역': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoServcPPSSrch'
}


# =================================================================================
# 2. 유틸리티 함수
# =================================================================================

def get_drive_service_for_script():
    info = json.loads(AUTH_JSON_STR)
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=['https://www.googleapis.com/auth/drive']
    )
    return build('drive', 'v3', credentials=creds), creds


def get_target_date():
    now = datetime.datetime.now(datetime.UTC) + datetime.timedelta(hours=9)
    return now - datetime.timedelta(days=1)


def classify_text(text):
    for cat, kws in CAT_KEYWORDS.items():
        if any(kw in str(text) for kw in kws):
            return cat
    return '기타'


def format_html_table(data_list, title):
    html = f"<div style='margin-top:25px;'><h4 style='color:#2c3e50; border-bottom:2px solid #34495e; padding-bottom:8px;'>{title}</h4>"
    if not data_list:
        html += "<p style='color:#888; padding:10px;'>- 해당 내역이 없습니다.</p></div>"
        return html

    html += "<table border='1' style='border-collapse:collapse; width:100%; font-size:13px; line-height:1.8;'>"
    html += "<tr style='background-color:#f8f9fa;'><th>수요기관</th><th>명칭(링크)</th><th>업체명</th><th>금액</th></tr>"

    for item in data_list:
        corp_name = item.get('corp', '-')
        bg = "background-color:#FFF9C4;" if "이노뎁" in corp_name else ""
        amt_val = item.get('amt', '0')
        amt_str = f"{int(amt_val):,}원" if str(amt_val).isdigit() else amt_val
        link_name = f"<a href='{item['url']}' target='_blank' style='color:#1a73e8; text-decoration:none;'>{item['nm']}</a>"
        html += f"<tr style='{bg}'><td style='padding:8px; text-align:center;'>{item['org']}</td>"
        html += f"<td style='padding:8px;'>{link_name}</td>"
        html += f"<td style='padding:8px; text-align:center;'>{corp_name}</td>"
        html += f"<td style='padding:8px; text-align:right;'>{amt_str}</td></tr>"
    html += "</table></div>"
    return html


def fetch_api_data_from_g2b(kw, d_str, retries=3):
    url = "https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getSpcifyPrdlstPrcureInfoList"
    params = {
        'numOfRows': '999', 'pageNo': '1', 'ServiceKey': MY_DIRECT_KEY,
        'type': 'xml', 'inqryDiv': '1', 'inqryPrdctDiv': '2',
        'inqryBgnDate': d_str, 'inqryEndDate': d_str, 'dtilPrdctClsfcNoNm': kw
    }
    for attempt in range(retries):
        try:
            res = requests.get(url, params=params, timeout=60)  # 15 → 60초
            if res.status_code == 200 and "<item>" in res.text:
                root = ET.fromstring(res.content)
                return [[elem.text if elem.text else '' for elem in item]
                        for item in root.findall('.//item')]
            return []
        except requests.exceptions.Timeout:
            wait = (attempt + 1) * 5  # 5초, 10초, 15초 간격으로 재시도
            print(f"[{kw}] 타임아웃 ({attempt+1}/{retries}), {wait}초 후 재시도...")
            time.sleep(wait)
        except Exception as e:
            print(f"[{kw}] 오류: {e}")
            return []
    print(f"[{kw}] 최종 실패")
    return []


def fetch_notice_data(category, url, d_str):
    params = {
        'serviceKey': MY_DIRECT_KEY, 'pageNo': '1', 'numOfRows': '999',
        'inqryDiv': '1', 'type': 'json',
        'inqryBgnDt': d_str + "0000", 'inqryEndDt': d_str + "2359"
    }
    try:
        res = requests.get(url, params=params, timeout=15)
        if res.status_code == 200:
            return pd.DataFrame(
                res.json().get('response', {}).get('body', {}).get('items', [])
            )
    except:
        pass
    return pd.DataFrame()


def fetch_single_contract(kw_s, d_str):
    api_url_servc = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'
    results = []
    p = {
        'serviceKey': MY_DIRECT_KEY, 'inqryDiv': '1', 'type': 'xml',
        'inqryBgnDate': d_str, 'inqryEndDate': d_str, 'cntrctNm': kw_s
    }
    try:
        r = requests.get(api_url_servc, params=p, timeout=20)
        if r.status_code == 200:
            root = ET.fromstring(r.content)
            for item in root.findall('.//item'):
                detail_url = item.findtext('cntrctDtlInfoUrl') or "https://www.g2b.go.kr"
                raw_demand = item.findtext('dminsttList', '-')
                clean_demand = raw_demand.replace('[', '').replace(']', '').split('^')[2] \
                    if '^' in raw_demand else raw_demand
                raw_corp = item.findtext('corpList', '-')
                clean_corp = raw_corp.replace('[', '').replace(']', '').split('^')[3] \
                    if '^' in raw_corp else raw_corp
                results.append({
                    'org': clean_demand,
                    'nm': item.findtext('cntrctNm', '-'),
                    'corp': clean_corp,
                    'amt': item.findtext('totCntrctAmt', '0'),
                    'url': detail_url
                })
    except Exception as e:
        print(f"계약 데이터 수집 오류 ({kw_s}): {e}")
    return results


def save_notice_by_year(drive_service, creds, cat_name, new_df, year):
    file_name = f"나라장터_공고_{cat_name}_{year}년.csv"
    res = drive_service.files().list(
        q=f"name='{file_name}' and '{NOTICE_FOLDER_ID}' in parents and trashed=false",
        fields='files(id)'
    ).execute()
    items = res.get('files', [])
    file_id = items[0]['id'] if items else None

    if file_id:
        resp = requests.get(
            f'https://www.googleapis.com/drive/v3/files/{file_id}?alt=media',
            headers={'Authorization': f'Bearer {creds.token}'}
        )
        try:
            old_df = pd.read_csv(io.BytesIO(resp.content), encoding='utf-8-sig', low_memory=False)
            new_df = pd.concat([old_df, new_df], ignore_index=True)
        except Exception as e:
            print(f"기존 파일 읽기 오류 ({file_name}): {e}")

    if 'bidNtceNo' in new_df.columns:
        new_df.drop_duplicates(subset=['bidNtceNo'], keep='last', inplace=True)

    csv_bytes = new_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
    media = MediaIoBaseUpload(io.BytesIO(csv_bytes), mimetype='text/csv')

    if file_id:
        drive_service.files().update(fileId=file_id, media_body=media).execute()
    else:
        drive_service.files().create(
            body={'name': file_name, 'parents': [NOTICE_FOLDER_ID]},
            media_body=media
        ).execute()
    print(f"✅ [{cat_name}] {file_name} 저장 완료 ({len(new_df):,}건)")


# =================================================================================
# 3. 메인 로직
# =================================================================================
def main():
    if not MY_DIRECT_KEY or not AUTH_JSON_STR:
        return

    target_dt    = get_target_date()
    d_str        = target_dt.strftime("%Y%m%d")
    year         = target_dt.year
    display_date = target_dt.strftime("%Y년 %m월 %d일")
    weekday_str  = ["월", "화", "수", "목", "금", "토", "일"][target_dt.weekday()]

    drive_service, drive_creds = get_drive_service_for_script()
    keywords_notice_all = [kw for sublist in CAT_KEYWORDS.values() for kw in sublist]

    # -------------------------------------------------------------------------
    # PART 1: 종합쇼핑몰 3자단가 수집
    # ★ 변경: 폴더 ID 지정하여 파일 검색
    # -------------------------------------------------------------------------
    final_data = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_api_data_from_g2b, kw, d_str): kw for kw in keywords}
        for future in as_completed(futures):
            data = future.result()
            if data:
                final_data.extend(data)

    school_stats, innodep_today_dict, innodep_total_amt = {}, {}, 0

    if final_data:
        new_df = pd.DataFrame(final_data, columns=HEADER_KOR)

        # ★ 폴더 ID 지정하여 검색
        query = f"name='{target_dt.year}.csv' and '{SHOPPING_FOLDER_ID}' in parents and trashed=false"
        res = drive_service.files().list(q=query, fields='files(id)').execute()
        items = res.get('files', [])
        f_id = items[0]['id'] if items else None

        if f_id:
            resp = requests.get(
                f'https://www.googleapis.com/drive/v3/files/{f_id}?alt=media',
                headers={'Authorization': f'Bearer {drive_creds.token}'}
            )
            old_df = pd.read_csv(io.BytesIO(resp.content), encoding='utf-8-sig', low_memory=False)
            df_to_upload = pd.concat([old_df, new_df], ignore_index=True).drop_duplicates(
                subset=['계약납품요구일자', '수요기관명', '품명', '금액'], keep='last'
            )
            media = MediaIoBaseUpload(
                io.BytesIO(df_to_upload.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')),
                mimetype='text/csv'
            )
            drive_service.files().update(fileId=f_id, media_body=media).execute()
        else:
            # 파일이 없으면 새로 생성
            media = MediaIoBaseUpload(
                io.BytesIO(new_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')),
                mimetype='text/csv'
            )
            drive_service.files().create(
                body={'name': f'{target_dt.year}.csv', 'parents': [SHOPPING_FOLDER_ID]},
                media_body=media
            ).execute()
            print(f"✅ 종합쇼핑몰 {target_dt.year}.csv 신규 생성 완료")

        for row in final_data:
            org = str(row[7])
            comp = str(row[21])
            amt_val = str(row[20])
            item_nm = str(row[14])
            cntrct = str(row[23])
            amt = int(amt_val.replace(',', '').split('.')[0])

            if '학교' in org and '지능형' in cntrct and 'CCTV' in cntrct:
                if org not in school_stats:
                    school_stats[org] = {'total_amt': 0, 'main_vendor': comp}
                school_stats[org]['total_amt'] += amt

            if '이노뎁' in comp:
                innodep_today_dict[org] = innodep_today_dict.get(org, 0) + amt
                innodep_total_amt += amt

    # -------------------------------------------------------------------------
    # PART 2: 나라장터 입찰 공고 수집 → 연도별 파일로 저장 (변경 없음)
    # -------------------------------------------------------------------------
    notice_mail_buckets = {cat: [] for cat in CAT_KEYWORDS}
    all_notice_count = 0

    for cat_api, api_url in NOTICE_API_MAP.items():
        n_df = fetch_notice_data(cat_api, api_url, d_str)
        if not n_df.empty:
            all_notice_count += len(n_df)
            save_notice_by_year(drive_service, drive_creds, cat_api, n_df, year)

            pattern = '|'.join(keywords_notice_all)
            filtered = n_df[n_df['bidNtceNm'].str.contains(pattern, na=False, case=False)]
            for _, row in filtered.iterrows():
                cat_found = classify_text(row['bidNtceNm'])
                if cat_found in notice_mail_buckets:
                    notice_mail_buckets[cat_found].append({
                        'org': row.get('dminsttNm', '-'),
                        'nm':  row.get('bidNtceNm', '-'),
                        'amt': row.get('presmptPrce', '별도공고'),
                        'url': row.get('bidNtceDtlUrl', '#')
                    })

    # -------------------------------------------------------------------------
    # PART 3: 나라장터 용역 계약 내역 수집 (변경 없음)
    # -------------------------------------------------------------------------
    contract_mail_buckets = {cat: [] for cat in CAT_KEYWORDS}
    collected_servc = []

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(fetch_single_contract, kw_s, d_str): kw_s
            for kw_s in keywords_notice_all
        }
        for future in as_completed(futures):
            collected_servc.extend(future.result())

    unique_servc_list = list({f"{d['org']}_{d['nm']}": d for d in collected_servc}.values())
    for s in unique_servc_list:
        cat_found = classify_text(s['nm'])
        if cat_found in contract_mail_buckets:
            contract_mail_buckets[cat_found].append(s)

    exclude_keywords = ['학교', '민방위', '교육청']

    def is_valid_org(org_name):
        for word in exclude_keywords:
            if word in org_name:
                return False
        return True

    notice_mail_buckets['국방']   = [i for i in notice_mail_buckets['국방']   if is_valid_org(i['org'])]
    contract_mail_buckets['국방'] = [i for i in contract_mail_buckets['국방'] if is_valid_org(i['org'])]

    # -------------------------------------------------------------------------
    # PART 4: 최종 HTML 리포트 조립 (변경 없음)
    # -------------------------------------------------------------------------
    report_html = f"""
    <div style="font-family:'Malgun Gothic'; line-height:2.0; border:1px solid #ddd; padding:20px; border-radius:10px;">
        <h1 style="color:#1a73e8; margin-top:0;">📋 조달청 데이터 자동 수집 리포트</h1>
        <b>🔹 수집날짜 :</b> {display_date}({weekday_str}요일)<br>
        <b>🔹 종합쇼핑몰 3자단가 데이터 :</b> {len(final_data):,}건<br>
        <b>🔹 나라장터 공고 데이터 :</b> {all_notice_count:,}건 (필터링 전 전체)<br>
        <b>🔹 나라장터 계약 데이터 :</b> {len(unique_servc_list):,}건<br>
        <b>🔹 상태 :</b> 성공
        <hr style="border:0.5px solid #eee; margin:20px 0;">

        <h1 style='color:#e67e22;'>🛒 종합쇼핑몰 3자단가 요약</h1>
        <b>★ 학교 지능형 CCTV 납품 현황</b>
        <div style='padding-left:10px; border-left:3px solid #e67e22;'>
    """

    if school_stats:
        for sch, info in school_stats.items():
            report_html += f"<p style='margin:5px 0;'>- {sch} / {info['total_amt']:,}원 / {info['main_vendor']}</p>"
        report_html += f"<b>👉 학교 내역 총 {len(school_stats)}건 / 총액 {sum(s['total_amt'] for s in school_stats.values()):,}원</b>"
    else:
        report_html += "<p> - 학교 내역 0건</p>"

    report_html += "</div><br><b>★ 이노뎁 나라장터 납품 실적</b><div style='padding-left:10px; border-left:3px solid #e67e22;'>"

    if innodep_today_dict:
        for org, amt in innodep_today_dict.items():
            report_html += f"<p style='margin:5px 0;'>- {org} / 총액 {amt:,}원</p>"
        report_html += f"<b>👉 이노뎁 납품내역 총 {len(innodep_today_dict)}건 / 총액 {innodep_total_amt:,}원</b>"
    else:
        report_html += "<p> - 이노뎁 납품내역 0건</p>"

    report_html += "</div>"

    report_html += "<h1 style='margin-top:35px; color:#d32f2f;'>📢 나라장터 입찰 공고 요약</h1>"
    for i, cat in enumerate(CAT_KEYWORDS.keys(), 1):
        report_html += format_html_table(notice_mail_buckets[cat], f"{i}) {cat} 요약")

    report_html += "<h1 style='margin-top:35px; color:#1a73e8;'>📝 나라장터 계약 내역 요약</h1>"
    for i, cat in enumerate(CAT_KEYWORDS.keys(), 1):
        report_html += format_html_table(contract_mail_buckets[cat], f"{i}) {cat} 요약")

    report_html += "</div>"

    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:
            f.write(f"collect_date={d_str}\nfull_report<<EOF\n{report_html}\nEOF\n")


if __name__ == "__main__":
    main()
