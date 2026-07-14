# 음식물처리기 조달 영업 대시보드 V4

## 교체 파일

저장소 루트:

- `procurement_common.py` — 새로 추가
- `munitions_plan.py`
- `munitions_contract.py`
- `munitions_notice.py`
- `dashboard_data.py`
- `requirements-dashboard.txt`

웹 화면:

- `docs/index.html`
- `docs/leads.html`

GitHub Actions:

- `.github/workflows/update-dashboard.yml`

## Google Sheets 구조

다음 스프레드시트는 각각 첫 번째 탭 하나만 사용합니다.

- 군수품조달_국내_발주계획
- 군수품조달_국내_계약정보
- 군수품조달_국내_입찰공고

기존 날짜별·월별 탭은 삭제하고 첫 번째 탭만 남겨도 됩니다. 첫 번째 탭의 내용까지 비우면 다음 실행에서 자동으로 최근 1년 전체 수집 모드가 실행됩니다.

## 첫 실행

GitHub → Actions → `Update Food Waste Dashboard` → Run workflow에서 `full_refresh`를 체크해 실행합니다.

첫 실행 동작:

1. 발주계획 최근 13개월 API 조회
2. 계약정보 최근 1년 조회
3. 입찰공고 최근 1년 조회
4. 식별번호 우선 중복 제거
5. 최근 365일 범위만 단일 탭에 재작성
6. 대시보드용 JSON 생성
7. 자동 커밋·푸시

## 매일 자동 실행

매일 06:20 KST에 실행됩니다.

- 발주계획: 현재 월과 직전 월 재조회
- 계약정보: 최근 7일 재조회
- 입찰공고: 최근 7일 재조회
- 새 API 응답이 같은 식별번호의 기존 데이터를 덮어씀
- 365일보다 오래된 데이터 자동 삭제

## 출력 파일

- `docs/data/summary.json`: 메인 대시보드 전용 경량 데이터
- `docs/data/leads.json`: 상세 리드 화면 데이터
- `docs/data.json`: 기존 링크 호환용 전체 데이터

메인 화면은 `summary.json`, 리드 화면은 `leads.json`만 읽으므로 초기 로딩량이 줄어듭니다.

## 중복 제거

각 수집기는 계약번호·공고번호·판단번호 등 원본 식별자를 우선 사용합니다. 식별자가 없는 경우 제목, 기관, 날짜, 금액을 정규화한 SHA-1 키를 사용합니다.

## 필수 GitHub Secrets

- `DATA_GO_KR_API_KEY`
- `GOOGLE_AUTH_JSON`

## 기존 Workflow

기존 음식물처리기 수집·대시보드 생성 Workflow의 `schedule`은 제거하거나 파일을 비활성화해야 합니다. 두 Workflow가 동시에 실행되면 불필요한 API 호출과 시트 충돌이 발생할 수 있습니다.

## 주의

첫 1년 전체 수집은 API 호출량과 Google Sheets 행 수에 따라 시간이 걸립니다. 병렬 수집은 기본 3개 작업으로 제한해 공공데이터 API에 과도한 요청이 가지 않도록 했습니다.
