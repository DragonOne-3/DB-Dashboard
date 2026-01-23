name: G2B Multi-Year Collector

on:
  workflow_dispatch:
    inputs:
      start_date:
        description: '수집 시작일 (YYYYMMDD)'
        required: true
        default: '20240101'
      end_date:
        description: '수집 종료일 (YYYYMMDD)'
        required: true
        default: '20251231'

jobs:
  collect:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: pip install pandas requests google-api-python-client google-auth-httplib2 google-auth-oauthlib
      - name: Run Script
        env:
          DATA_GO_KR_API_KEY: ${{ secrets.DATA_GO_KR_API_KEY }}
          GOOGLE_AUTH_JSON: ${{ secrets.GOOGLE_AUTH_JSON }}
        run: python G2B_notice.py ${{ github.event.inputs.start_date }} ${{ github.event.inputs.end_date }}
