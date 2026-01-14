import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import time

# --- 설정 ---
# 키가 이미 인코딩되어 있다면 그대로 사용하기 위해 unquote 처리
API_KEY = requests.utils.unquote(os.environ.get('DATA_GO_KR_API_KEY'))
API_URL = 'http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'

def fetch_data_safe(kw, date_str):
    params = {
        'serviceKey': API_KEY,
        'pageNo': '1',
        'numOfRows': '999',
        'inqryDiv': '1',
        'type': 'xml',
        'inqryBgnDate': date_str,
        'inqryEndDate': date_str,
        'cntrctNm': kw
    }
    try:
        # verify=False를 추가하여 보안 인증 문제 우회 (필요시)
        res = requests.get(API_URL, params=params, timeout=30)
        
        # 만약 XML이 아닌 데이터가 오면 내용을 찍어봄
        if not res.text.strip().startswith('<?xml') and not res.text.strip().startswith('<response'):
            print(f"   ⚠️ 서버 응답 이상 (키워드: {kw}): {res.text[:100]}")
            return []
            
        root = ET.fromstring(res.content)
        items = root.findall('.//item')
        
        rows = []
        for item in items:
            raw = {child.tag: child.text for child in item}
            # ... (가공 로직 동일)
            rows.append(raw)
        return rows
    except Exception as e:
        print(f"   ❌ '{kw}' 통신 오류: {e}")
        return []

# main 로직은 동일하게 유지
