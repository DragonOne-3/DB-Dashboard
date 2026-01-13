import os, json, datetime, time, requests
import xml.etree.ElementTree as ET
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# 1. 설정 정보
MY_DIRECT_KEY = "8ccf45461f6834ad0643601f5884d4a79460e441026b3f5041b7da0c67183374"
AUTH_JSON_STR = os.environ.get('GOOGLE_AUTH_JSON')

# [요청사항] 국문 헤더 44개 (생략 없이 전체 적용)
HEADER_KOR = ['조달구분명',	'계약구분명',	'계약납품구분명',	'계약납품요구일자',	'계약납품요구번호',	'변경차수',	'최종변경차수여부',	'수요기관명',	'수요기관구분명',	'수요기관지역명',	'수요기관코드',	'물품분류번호',	'품명',	'세부물품분류번호',	'세부품명',	'물품식별번호',	'물품규격명',	'단가',	'수량',	'단위',	'금액',	'업체명',	'업체기업구분명',	'계약명',	'우수제품여부',	'공사용자재직접구매대상여부',	'다수공급자계약여부',	'다수공급자계약2단계진행여부',	'단가계약번호',	'단가계약변경차수',	'최초계약(납품요구)일자',	'계약체결방법명',	'증감수량',	'증감금액',	'납품장소명',	'납품기한일자',	'업체사업자등록번호',	'인도조건명',	'물품순번']

# 품목 리스트 (전체 리스트 사용)
keywords = [
    '네트워크시스템장비용랙','영상감시장치','PA용스피커','안내판','카메라브래킷','액정모니터','광송수신모듈','전원공급장치','광분배함','컨버터','컴퓨터서버','하드디스크드라이브','네트워크스위치','광점퍼코드','풀박스','서지흡수기','디지털비디오레코더',
    '스피커','오디오앰프','브래킷','UTP케이블','정보통신공사','영상정보디스플레이장치','송신기','난연전력케이블','1종금속제가요전선관','호온스피커','누전차단기','방송수신기','LAP외피광케이블','폴리에틸렌전선관','리모트앰프',
    '랙캐비닛용패널','베어본컴퓨터','분배기','결선보드유닛','벨','난연접지용비닐절연전선','경광등','데스크톱컴퓨터','특수목적컴퓨터','철근콘크리트공사','토공사','안내전광판','접지봉','카메라회전대','무선랜액세스포인트','컴퓨터망전환장치',
    '포장공사','고주파동축케이블','카메라하우징','인터폰','스위칭모드전원공급장치','금속상자','열선감지기','태양전지조절기','밀폐고정형납축전지','IP전화기','디스크어레이','그래픽용어댑터','인터콤장비','기억유닛','컴퓨터지문인식장치','랜접속카드',
    '접지판','제어케이블','비디오네트워킹장비','레이스웨이','콘솔익스텐더','전자카드','비대면방역감지장비','온습도트랜스미터','도난방지기','융복합영상감시장치','멀티스크린컴퓨터','컴퓨터정맥인식장치','카메라컨트롤러','SSD저장장치','원격단말장치(RTU)',
    '융복합네트워크스위치','융복합액정모니터','융복합데스크톱컴퓨터','융복합그래픽용어댑터','융복합베어본컴퓨터','융복합서지흡수기','배선장치','융복합배선장치','융복합카메라브래킷','융복합네트워크시스템장비용랙','융복합UTP케이블','테이프백업장치',
    '자기식테이프','레이드저장장치','광송수신기','450/750V 유연성단심비닐절연전선','솔내시스템','450/750V유연성단심비닐절연전선','카메라받침대','텔레비전거치대','광수신기','무선통신장치','동작분석기','전력공급장치','450/750V 일반용유연성단심비닐절연전선','분전함',
    '비디오믹서','절연전선및피복선','레이더','적외선방사기', '보안용카메라', '통신소프트웨어','분석및과학용소프트웨어'
]

def fetch_all_pages_data(keyword, start_date, end_date):
    """[핵심] 999건이 넘어도 페이지를 넘기며 끝까지 가져오는 함수"""
    all_data = []
    current_page = 1
    
    while True:
        url = "https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getSpcifyPrdlstPrcureInfoList"
        params = {
            'numOfRows': '999',
            'pageNo': str(current_page),
            'ServiceKey': MY_DIRECT_KEY,
            'Type_A': 'xml',
            'inqryDiv': '1',
            'inqryPrdctDiv': '2',
            'inqryBgnDate': start_date,
            'inqryEndDate': end_date,
            'dtilPrdctClsfcNoNm': keyword
        }
        
        try:
            res = requests.get(url, params=params, timeout=30)
            if res.status_code == 200 and "<item>" in res.text:
                root = ET.fromstring(res.content)
                items = root.findall('.//item')
                
                # 이번 페이지 데이터 저장
                for item in items:
                    all_data.append([elem.text if elem.text else '' for elem in item])
                
                # 전체 개수 파악
                total_count = int(root.find('.//totalCount').text)
                print(f"   -> [{keyword}] {current_page}페이지 수집 중... ({len(all_data)}/{total_count})")
                
                # 수집된 데이터가 전체 개수보다 크거나 같으면 종료
                if len(all_data) >= total_count or not items:
                    break
                    
                current_page += 1
                time.sleep(0.5) # 서버 보호
            else:
                print(f"   -> {keyword}: 응답 오류 또는 데이터 없음")
                break
        except Exception as e:
            print(f"   -> 에러 발생: {e}")
            break
            
    return all_data

def main():
    creds_dict = json.loads(AUTH_JSON_STR)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)

    # --- [수집 설정] ---
    # 6개월 단위로 수집 날짜를 바꿔가며 실행하세요
    year = "2025"
    quarter = "4분기"
    s_date = "20251001"
    e_date = "20251231"
    sheet_name = "2025_4분기"
    # ------------------

    # 파일 및 시트 로드 (수동 생성 권장)
    sh = client.open(f"조달청_납품내역_{year}_{quarter}")
    try:
        ws = sh.worksheet(sheet_name)
    except:
        ws = sh.add_worksheet(title=sheet_name, rows="10000", cols="44")
        ws.append_row(HEADER_KOR)

    for kw in keywords:
        print(f"🚀 {kw} 기간 수집 시작 ({s_date} ~ {e_date})")
        data = fetch_all_pages_data(kw, s_date, e_date)
        if data:
            ws.append_rows(data)
            print(f"✅ {kw} 총 {len(data)}건 저장 완료!")
        time.sleep(1)

if __name__ == "__main__":
    main()
