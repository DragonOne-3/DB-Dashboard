# 음식물처리기 조달 영업 레이더 V2

## 교체할 파일

- `munitions_plan.py`
- `munitions_contract.py`
- `munitions_notice.py`
- `dashboard_data.py`
- `docs/index.html`
- `docs/leads.html`
- `.github/workflows/update-dashboard.yml`
- `requirements-dashboard.txt`

기존 레포의 다른 파일은 삭제하거나 덮어쓰지 않습니다.

## 자동 실행

워크플로는 매일 한국시간 06:20에 다음 순서로 실행됩니다.

1. 발주계획: 현재 월과 직전 월을 월별 탭에 덮어쓰기
2. 계약정보: 최근 7일을 날짜별 탭에 덮어쓰기
3. 입찰공고: 최근 7일을 날짜별 탭에 덮어쓰기
4. `dashboard_data.py`로 `docs/data.json` 생성
5. 변경된 `docs/data.json` 자동 커밋·푸시

같은 날 여러 번 실행해도 날짜별 탭을 덮어쓰기 때문에 중복 누적되지 않습니다. 며칠 실행이 실패해도 최근 7일을 다시 조회하여 자동 복구합니다.

## 적용 순서

1. 위 파일들을 같은 경로에 수기로 교체합니다.
2. 기존 음식물처리기 관련 자동 워크플로의 `schedule`을 제거하거나 비활성화합니다.
3. GitHub Secrets에 다음 값이 있는지 확인합니다.
   - `DATA_GO_KR_API_KEY`
   - `GOOGLE_AUTH_JSON`
4. 서비스 계정 이메일에 세 스프레드시트 편집 권한이 있는지 확인합니다.
5. Actions → `Update Food Waste Dashboard` → `Run workflow`로 수동 테스트합니다.
6. 성공 후 GitHub Pages 화면과 `docs/data.json` 갱신 시각을 확인합니다.

## 주의

계약정보와 입찰공고는 기존 첫 번째 탭에 누적 저장하던 방식에서 `YYYYMMDD` 날짜별 탭 방식으로 변경됩니다. `dashboard_data.py`는 모든 탭을 읽으므로 정상 동작합니다. 동일 스프레드시트를 읽는 다른 코드가 `get_worksheet(0)`만 사용한다면 별도 확인이 필요합니다.
