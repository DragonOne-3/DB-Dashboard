import os, json, datetime, time, requests
import xml.etree.ElementTree as ET
import pandas as pd
import io
import threading
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseUpload
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

# =================================================================================
# 1. 설정 및 환경 변수
# =================================================================================
MY_DIRECT_KEY = os.environ.get('DATA_GO_KR_API_KEY')
AUTH_JSON_STR = os.environ.get('GOOGLE_AUTH_JSON')

NOTICE_FOLDER_ID   = "1AsvVmayEmTtY92d1SfXxNi6bL0Zjw5mg"
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

# ★ 경쟁사 키워드 목록 (업체명 기준)
COMPETITOR_KEYWORDS = [
    '이노뎁', '한화비전', '아이디스', '다화테크놀로지', '하이크비전',
    '씨게이트', '씨앤비텍', 'SKT', 'KT', '유니뷰'
]

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


def format_amt(val):
    """금액 포맷팅 헬퍼"""
    try:
        v = int(str(val).replace(',', '').split('.')[0])
        if v >= 100_000_000:
            return f"{v / 100_000_000:.1f}억원"
        elif v >= 10_000:
            return f"{v / 10_000:.0f}만원"
        return f"{v:,}원"
    except:
        return str(val)


# =================================================================================
# 3. HTML 리포트 빌더 (전면 개편)
# =================================================================================

def build_html_report(
    display_date, weekday_str, d_str,
    final_data,
    school_stats, innodep_today_dict, innodep_total_amt,
    competitor_stats,       # ★ NEW: {업체명: 총금액}
    org_stats_top20,        # ★ NEW: [(수요기관, 총금액), ...]
    notice_mail_buckets,
    contract_mail_buckets,
    all_notice_count,
    unique_servc_list
):
    # ── 색상 팔레트 ──────────────────────────────────────────────────────────────
    COLORS = [
        '#4361ee', '#7209b7', '#f72585', '#4cc9f0', '#4ade80',
        '#fb923c', '#a78bfa', '#34d399', '#f59e0b', '#60a5fa'
    ]

    # ── Chart.js 데이터 준비 ─────────────────────────────────────────────────────
    # 경쟁사 차트
    comp_labels = json.dumps([k for k in competitor_stats], ensure_ascii=False)
    comp_values = json.dumps([v // 10000 for v in competitor_stats.values()])  # 만원 단위
    comp_colors = json.dumps(COLORS[:len(competitor_stats)])

    # 수요기관 상위 20개 차트
    org_labels = json.dumps([o[0] for o in org_stats_top20], ensure_ascii=False)
    org_values = json.dumps([o[1] // 10000 for o in org_stats_top20])  # 만원 단위

    # ── 이노뎁 카드 섹션 ─────────────────────────────────────────────────────────
    innodep_card_rows = ""
    for org, amt in sorted(innodep_today_dict.items(), key=lambda x: -x[1]):
        innodep_card_rows += f"""
        <tr>
          <td style="padding:10px 14px; border-bottom:1px solid #f0f0f0;">{org}</td>
          <td style="padding:10px 14px; border-bottom:1px solid #f0f0f0; text-align:right;
                     font-weight:600; color:#4361ee;">{format_amt(amt)}</td>
        </tr>"""
    if not innodep_card_rows:
        innodep_card_rows = '<tr><td colspan="2" style="padding:16px; color:#999; text-align:center;">납품 내역 없음</td></tr>'

    # ── 학교 CCTV 카드 섹션 ──────────────────────────────────────────────────────
    school_card_rows = ""
    for sch, info in sorted(school_stats.items(), key=lambda x: -x[1]['total_amt']):
        school_card_rows += f"""
        <tr>
          <td style="padding:10px 14px; border-bottom:1px solid #f0f0f0;">{sch}</td>
          <td style="padding:10px 14px; border-bottom:1px solid #f0f0f0; text-align:right;
                     font-weight:600; color:#7209b7;">{format_amt(info['total_amt'])}</td>
          <td style="padding:10px 14px; border-bottom:1px solid #f0f0f0; color:#555;">{info['main_vendor']}</td>
        </tr>"""
    if not school_card_rows:
        school_card_rows = '<tr><td colspan="3" style="padding:16px; color:#999; text-align:center;">해당 내역 없음</td></tr>'

    # ── 공고/계약 섹션 빌더 ───────────────────────────────────────────────────────
    def build_notice_section(buckets, section_title, icon, accent):
        html = f"""
        <div class="section-card">
          <div class="section-header" style="background:{accent};">
            <span>{icon} {section_title}</span>
            <span class="badge">{sum(len(v) for v in buckets.values())}건</span>
          </div>
          <div style="padding:0 20px 20px;">"""

        cat_icons = {'영상감시장치': '📷', '국방': '🛡️', '솔루션': '💡', '스마트도시': '🏙️'}

        for cat, items in buckets.items():
            cat_icon = cat_icons.get(cat, '📌')
            html += f"""
            <div class="cat-block">
              <div class="cat-title">{cat_icon} {cat}
                <span class="cat-count">{len(items)}건</span>
              </div>"""

            if not items:
                html += '<p class="empty-msg">해당 내역이 없습니다.</p>'
            else:
                html += """
              <table class="data-table">
                <colgroup>
                  <col style="width:22%"><col style="width:38%">
                  <col style="width:22%"><col style="width:18%">
                </colgroup>
                <thead>
                  <tr><th>수요기관</th><th>공고명</th><th>업체명</th><th>금액</th></tr>
                </thead><tbody>"""
                for item in items:
                    corp = item.get('corp', '-')
                    is_innodep = '이노뎁' in corp
                    row_style = 'background:#fff8e1;' if is_innodep else ''
                    badge = '<span class="innodep-badge">★이노뎁</span>' if is_innodep else ''
                    amt_val = item.get('amt', '0')
                    try:
                        amt_str = format_amt(int(str(amt_val).replace(',', '').split('.')[0]))
                    except:
                        amt_str = str(amt_val) if amt_val else '별도공고'

                    html += f"""
                  <tr style="{row_style}">
                    <td style="padding:9px 12px; white-space:nowrap;">{item.get('org','-')}</td>
                    <td style="padding:9px 12px;">
                      <a href="{item.get('url','#')}" target="_blank" class="table-link">
                        {item.get('nm','-')}
                      </a>
                    </td>
                    <td style="padding:9px 12px; text-align:center;">{corp}{badge}</td>
                    <td style="padding:9px 12px; text-align:right; font-weight:600;">{amt_str}</td>
                  </tr>"""
                html += "</tbody></table>"
            html += "</div>"
        html += "</div></div>"
        return html

    notice_section   = build_notice_section(notice_mail_buckets,   '나라장터 입찰 공고', '📢', '#d32f2f')
    contract_section = build_notice_section(contract_mail_buckets, '나라장터 계약 내역', '📝', '#1565c0')

    # ── 최종 HTML 조립 ────────────────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<style>
  /* ── 기본 리셋 ── */
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: 'Apple SD Gothic Neo', 'Malgun Gothic', -apple-system, sans-serif;
    background: #f4f6fb;
    color: #1e293b;
    line-height: 1.6;
  }}

  /* ── 전체 래퍼 ── */
  .wrapper {{
    max-width: 900px;
    margin: 0 auto;
    padding: 20px 16px 40px;
  }}

  /* ── 헤더 배너 ── */
  .report-header {{
    background: linear-gradient(135deg, #1e3a8a 0%, #4361ee 60%, #7209b7 100%);
    border-radius: 16px;
    padding: 30px 32px;
    color: #fff;
    margin-bottom: 24px;
    display: flex;
    flex-direction: column;
    gap: 6px;
  }}
  .report-header h1 {{
    font-size: 22px;
    font-weight: 700;
    letter-spacing: -0.3px;
  }}
  .report-header .sub {{
    font-size: 13px;
    opacity: 0.85;
  }}
  .kpi-row {{
    display: flex;
    gap: 12px;
    margin-top: 14px;
    flex-wrap: wrap;
  }}
  .kpi-chip {{
    background: rgba(255,255,255,0.18);
    border-radius: 20px;
    padding: 5px 14px;
    font-size: 12px;
    font-weight: 600;
    backdrop-filter: blur(4px);
  }}

  /* ── 섹션 카드 ── */
  .section-card {{
    background: #fff;
    border-radius: 14px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.07);
    margin-bottom: 24px;
    overflow: hidden;
  }}
  .section-header {{
    padding: 14px 20px;
    color: #fff;
    font-size: 15px;
    font-weight: 700;
    display: flex;
    justify-content: space-between;
    align-items: center;
    border-radius: 14px 14px 0 0;
  }}
  .badge {{
    background: rgba(255,255,255,0.25);
    border-radius: 10px;
    padding: 2px 10px;
    font-size: 12px;
    font-weight: 600;
  }}

  /* ── 2-컬럼 그리드 ── */
  .grid-2 {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 24px;
    margin-bottom: 24px;
  }}

  /* ── 내부 테이블 ── */
  .inner-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }}
  .inner-table th {{
    background: #f8fafc;
    padding: 10px 14px;
    font-size: 11px;
    font-weight: 700;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    border-bottom: 2px solid #e2e8f0;
    text-align: left;
  }}
  .inner-table tr:hover td {{ background: #f8fafc; }}

  /* ── 데이터 테이블 (공고/계약) ── */
  .data-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 12.5px;
    margin-top: 4px;
  }}
  .data-table th {{
    background: #f1f5f9;
    padding: 8px 12px;
    font-size: 11px;
    font-weight: 700;
    color: #475569;
    border-bottom: 2px solid #e2e8f0;
    text-align: left;
    position: sticky;
    top: 0;
  }}
  .data-table td {{
    border-bottom: 1px solid #f1f5f9;
    font-size: 12px;
    color: #334155;
    vertical-align: middle;
  }}
  .data-table tr:last-child td {{ border-bottom: none; }}
  .data-table tr:hover td {{ background: #f8fafc; }}

  .table-link {{
    color: #4361ee;
    text-decoration: none;
    font-weight: 500;
  }}
  .table-link:hover {{ text-decoration: underline; }}

  .innodep-badge {{
    display: inline-block;
    background: #fbbf24;
    color: #78350f;
    font-size: 10px;
    font-weight: 700;
    padding: 1px 6px;
    border-radius: 4px;
    margin-left: 4px;
    white-space: nowrap;
  }}

  /* ── 카테고리 블록 ── */
  .cat-block {{
    margin: 16px 0;
    border-radius: 10px;
    border: 1px solid #e8ecf3;
    overflow: hidden;
  }}
  .cat-title {{
    background: #f8fafc;
    padding: 10px 16px;
    font-size: 13px;
    font-weight: 700;
    color: #334155;
    border-bottom: 1px solid #e8ecf3;
    display: flex;
    align-items: center;
    gap: 6px;
  }}
  .cat-count {{
    margin-left: auto;
    background: #e0e7ff;
    color: #3730a3;
    font-size: 11px;
    font-weight: 700;
    padding: 2px 8px;
    border-radius: 8px;
  }}
  .empty-msg {{
    padding: 14px 16px;
    color: #94a3b8;
    font-size: 13px;
    text-align: center;
  }}

  /* ── 차트 컨테이너 ── */
  .chart-wrap {{
    padding: 20px;
    position: relative;
    height: 300px;
  }}
  .chart-wrap-bar {{
    padding: 20px;
    position: relative;
    height: 400px;
  }}

  /* ── 통계 카드 (이노뎁 KPI) ── */
  .stat-grid {{
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 0;
  }}
  .stat-box {{
    padding: 20px;
    text-align: center;
    border-right: 1px solid #f0f0f0;
  }}
  .stat-box:last-child {{ border-right: none; }}
  .stat-label {{
    font-size: 11px;
    color: #94a3b8;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 4px;
  }}
  .stat-value {{
    font-size: 22px;
    font-weight: 800;
    color: #1e293b;
  }}
  .stat-sub {{
    font-size: 11px;
    color: #64748b;
    margin-top: 2px;
  }}
</style>
</head>
<body>
<div class="wrapper">

  <!-- ── 헤더 ── -->
  <div class="report-header">
    <div class="sub">📋 조달청 데이터 자동 수집 리포트 · 자동생성</div>
    <h1>🗓️ {display_date} ({weekday_str}요일) 일일 리포트</h1>
    <div class="kpi-row">
      <span class="kpi-chip">🛒 3자단가 {len(final_data):,}건</span>
      <span class="kpi-chip">📢 나라장터 공고 {all_notice_count:,}건</span>
      <span class="kpi-chip">📝 계약 {len(unique_servc_list):,}건</span>
      <span class="kpi-chip" style="background:rgba(251,191,36,0.35);">✅ 수집 완료</span>
    </div>
  </div>

  <!-- ═══════════════════════════════════════════════════════════
       SECTION 1 : 이노뎁 납품 실적 요약
  ════════════════════════════════════════════════════════════════ -->
  <div class="section-card">
    <div class="section-header" style="background:linear-gradient(90deg,#4361ee,#7209b7);">
      <span>🏆 이노뎁 납품 실적 요약</span>
      <span class="badge">{len(innodep_today_dict)}개 기관</span>
    </div>

    <!-- KPI 3종 -->
    <div class="stat-grid" style="border-bottom:1px solid #f0f0f0;">
      <div class="stat-box">
        <div class="stat-label">오늘 총 납품액</div>
        <div class="stat-value" style="color:#4361ee;">{format_amt(innodep_total_amt)}</div>
        <div class="stat-sub">당일 집계 기준</div>
      </div>
      <div class="stat-box">
        <div class="stat-label">납품 기관 수</div>
        <div class="stat-value">{len(innodep_today_dict)}</div>
        <div class="stat-sub">개 수요기관</div>
      </div>
      <div class="stat-box">
        <div class="stat-label">학교 CCTV</div>
        <div class="stat-value" style="color:#7209b7;">{len(school_stats)}</div>
        <div class="stat-sub">건 납품</div>
      </div>
    </div>

    <!-- 납품 기관 상세 -->
    <div style="padding:0 20px 20px;">
      <div style="font-size:12px; font-weight:700; color:#64748b; padding:14px 0 8px; letter-spacing:0.5px;">
        📍 기관별 납품 내역
      </div>
      <table class="inner-table">
        <thead>
          <tr><th>수요기관명</th><th style="text-align:right;">납품금액</th></tr>
        </thead>
        <tbody>
          {innodep_card_rows}
        </tbody>
      </table>
    </div>

    <!-- 학교 CCTV 납품 -->
    <div style="padding:0 20px 20px;">
      <div style="font-size:12px; font-weight:700; color:#64748b; padding:0 0 8px; letter-spacing:0.5px;">
        🏫 학교 지능형 CCTV 납품 현황
      </div>
      <table class="inner-table">
        <thead>
          <tr><th>학교명</th><th style="text-align:right;">금액</th><th>납품 업체</th></tr>
        </thead>
        <tbody>
          {school_card_rows}
        </tbody>
      </table>
    </div>
  </div>

  <!-- ═══════════════════════════════════════════════════════════
       SECTION 2 : 경쟁사 현황 + 수요기관 TOP20 (2컬럼 그리드)
  ════════════════════════════════════════════════════════════════ -->
  <div class="grid-2">

    <!-- 경쟁사별 현황 도넛 차트 -->
    <div class="section-card" style="margin-bottom:0;">
      <div class="section-header" style="background:#0f172a;">
        <span>📊 경쟁사별 납품 현황</span>
        <span class="badge">금액 기준</span>
      </div>
      <div class="chart-wrap">
        <canvas id="compChart"></canvas>
      </div>
      <div style="padding:0 16px 16px;">
        <table class="inner-table">
          <thead>
            <tr><th>업체명</th><th style="text-align:right;">납품금액</th></tr>
          </thead>
          <tbody>
            {"".join(f'<tr><td style="padding:8px 14px; border-bottom:1px solid #f0f0f0;"><span style=\"display:inline-block;width:10px;height:10px;border-radius:50%;background:{COLORS[i % len(COLORS)]};margin-right:6px;\"></span>{k}</td><td style="padding:8px 14px; text-align:right; font-weight:600; border-bottom:1px solid #f0f0f0;">{format_amt(v)}</td></tr>' for i,(k,v) in enumerate(competitor_stats.items())) if competitor_stats else '<tr><td colspan="2" style="padding:14px; text-align:center; color:#999;">데이터 없음</td></tr>'}
          </tbody>
        </table>
      </div>
    </div>

    <!-- 수요기관 TOP 20 -->
    <div class="section-card" style="margin-bottom:0;">
      <div class="section-header" style="background:#0f172a;">
        <span>🏛️ 수요기관 TOP 20</span>
        <span class="badge">3자단가 기준</span>
      </div>
      <div style="padding:16px;">
        <table class="inner-table">
          <thead>
            <tr><th>#</th><th>수요기관</th><th style="text-align:right;">총 금액</th></tr>
          </thead>
          <tbody>
            {"".join(f'<tr><td style="padding:8px 14px; border-bottom:1px solid #f0f0f0; color:#94a3b8; font-size:11px; font-weight:700;">{i+1:02d}</td><td style="padding:8px 14px; border-bottom:1px solid #f0f0f0;">{org}</td><td style="padding:8px 14px; border-bottom:1px solid #f0f0f0; text-align:right; font-weight:700; color:#4361ee;">{format_amt(amt)}</td></tr>' for i,(org,amt) in enumerate(org_stats_top20)) if org_stats_top20 else '<tr><td colspan="3" style="padding:14px; text-align:center; color:#999;">데이터 없음</td></tr>'}
          </tbody>
        </table>
      </div>
    </div>

  </div><!-- /grid-2 -->

  <!-- 수요기관 TOP20 바 차트 (전체폭) -->
  <div class="section-card">
    <div class="section-header" style="background:#1e3a8a;">
      <span>📈 수요기관 TOP 20 납품 현황 (3자단가)</span>
      <span class="badge">단위: 만원</span>
    </div>
    <div class="chart-wrap-bar">
      <canvas id="orgChart"></canvas>
    </div>
  </div>

  <!-- ═══════════════════════════════════════════════════════════
       SECTION 3 : 나라장터 입찰 공고
  ════════════════════════════════════════════════════════════════ -->
  {notice_section}

  <!-- ═══════════════════════════════════════════════════════════
       SECTION 4 : 나라장터 계약 내역
  ════════════════════════════════════════════════════════════════ -->
  {contract_section}

  <!-- 푸터 -->
  <div style="text-align:center; font-size:11px; color:#94a3b8; padding-top:10px;">
    자동 생성 리포트 · {display_date} · 조달청 공공데이터 API 기반
  </div>
</div>

<!-- ── Chart.js CDN ── -->
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script>
(function() {{
  // 공통 폰트 설정
  Chart.defaults.font.family = "'Apple SD Gothic Neo','Malgun Gothic',sans-serif";
  Chart.defaults.font.size = 12;

  // ─── 경쟁사 도넛 차트 ────────────────────────────────────────────
  var compLabels = {comp_labels};
  var compValues = {comp_values};
  var compColors = {comp_colors};

  if (compValues.length > 0 && compValues.some(v => v > 0)) {{
    var compCtx = document.getElementById('compChart').getContext('2d');
    new Chart(compCtx, {{
      type: 'doughnut',
      data: {{
        labels: compLabels,
        datasets: [{{
          data: compValues,
          backgroundColor: compColors,
          borderWidth: 2,
          borderColor: '#ffffff',
          hoverBorderWidth: 0,
          hoverOffset: 8
        }}]
      }},
      options: {{
        responsive: true,
        maintainAspectRatio: false,
        cutout: '62%',
        plugins: {{
          legend: {{
            position: 'bottom',
            labels: {{
              padding: 12,
              boxWidth: 12,
              font: {{ size: 11 }},
              color: '#334155'
            }}
          }},
          tooltip: {{
            callbacks: {{
              label: function(ctx) {{
                var val = ctx.parsed;
                return ' ' + ctx.label + ': ' + val.toLocaleString() + '만원';
              }}
            }}
          }}
        }}
      }}
    }});
  }} else {{
    document.getElementById('compChart').parentElement.innerHTML =
      '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:#94a3b8;font-size:13px;">경쟁사 데이터 없음</div>';
  }}

  // ─── 수요기관 TOP20 수평 바 차트 ────────────────────────────────
  var orgLabels = {org_labels};
  var orgValues = {org_values};

  if (orgValues.length > 0) {{
    var orgCtx = document.getElementById('orgChart').getContext('2d');

    // 그라디언트
    var grad = orgCtx.createLinearGradient(0, 0, orgCtx.canvas.width, 0);
    grad.addColorStop(0, '#4361ee');
    grad.addColorStop(1, '#7209b7');

    new Chart(orgCtx, {{
      type: 'bar',
      data: {{
        labels: orgLabels,
        datasets: [{{
          label: '납품금액(만원)',
          data: orgValues,
          backgroundColor: grad,
          borderRadius: 6,
          borderSkipped: false,
          barThickness: 14
        }}]
      }},
      options: {{
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        plugins: {{
          legend: {{ display: false }},
          tooltip: {{
            callbacks: {{
              label: function(ctx) {{
                return ' ' + ctx.parsed.x.toLocaleString() + '만원';
              }}
            }}
          }}
        }},
        scales: {{
          x: {{
            grid: {{ color: '#f1f5f9' }},
            ticks: {{
              color: '#64748b',
              font: {{ size: 11 }},
              callback: function(v) {{
                if (v >= 10000) return (v/10000).toFixed(0) + '억';
                return v.toLocaleString();
              }}
            }}
          }},
          y: {{
            grid: {{ display: false }},
            ticks: {{
              color: '#334155',
              font: {{ size: 11 }},
              maxRotation: 0
            }}
          }}
        }}
      }}
    }});
  }} else {{
    document.getElementById('orgChart').parentElement.innerHTML =
      '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:#94a3b8;font-size:13px;">수요기관 데이터 없음</div>';
  }}
}})();
</script>
</body>
</html>"""

    return html


# =================================================================================
# 4. 기존 API 호출 함수 (원본 유지)
# =================================================================================

def fetch_api_data_from_g2b(kw, d_str, retries=3):
    url = "https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getSpcifyPrdlstPrcureInfoList"
    params = {
        'numOfRows': '999', 'pageNo': '1', 'ServiceKey': MY_DIRECT_KEY,
        'type': 'xml', 'inqryDiv': '1', 'inqryPrdctDiv': '2',
        'inqryBgnDate': d_str, 'inqryEndDate': d_str, 'dtilPrdctClsfcNoNm': kw
    }
    for attempt in range(retries):
        try:
            res = requests.get(url, params=params, timeout=60)
            if res.status_code == 200 and "<item>" in res.text:
                root = ET.fromstring(res.content)
                return [[elem.text if elem.text else '' for elem in item]
                        for item in root.findall('.//item')]
            return []
        except requests.exceptions.Timeout:
            wait = (attempt + 1) * 5
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
    # ★ http → https 수정
    api_url_servc = 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'
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
# 5. 메인 로직
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

    # ─────────────────────────────────────────────────────────────────────────────
    # PART 1: 종합쇼핑몰 3자단가 수집
    # ─────────────────────────────────────────────────────────────────────────────
    final_data = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_api_data_from_g2b, kw, d_str): kw for kw in keywords}
        for future in as_completed(futures):
            data = future.result()
            if data:
                final_data.extend(data)

    school_stats        = {}
    innodep_today_dict  = {}
    innodep_total_amt   = 0
    competitor_stats    = defaultdict(int)   # ★ 경쟁사별 총금액
    org_stats           = defaultdict(int)   # ★ 수요기관별 총금액

    if final_data:
        new_df = pd.DataFrame(final_data, columns=HEADER_KOR)

        # Drive CSV 저장 (기존 로직 유지)
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
            media = MediaIoBaseUpload(
                io.BytesIO(new_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')),
                mimetype='text/csv'
            )
            drive_service.files().create(
                body={'name': f'{target_dt.year}.csv', 'parents': [SHOPPING_FOLDER_ID]},
                media_body=media
            ).execute()
            print(f"✅ 종합쇼핑몰 {target_dt.year}.csv 신규 생성 완료")

        # ★ 집계 로직 (이노뎁 / 경쟁사 / 수요기관 / 학교)
        for row in final_data:
            org      = str(row[7])
            comp     = str(row[21])
            amt_val  = str(row[20])
            item_nm  = str(row[14])
            cntrct   = str(row[23])
            try:
                amt = int(amt_val.replace(',', '').split('.')[0])
            except:
                amt = 0

            # 수요기관별 집계 (전체)
            if org:
                org_stats[org] += amt

            # 경쟁사별 집계
            for comp_kw in COMPETITOR_KEYWORDS:
                if comp_kw in comp:
                    competitor_stats[comp_kw] += amt
                    break  # 한 업체에 한 키워드만 매칭

            # 학교 지능형 CCTV
            if '학교' in org and '지능형' in cntrct and 'CCTV' in cntrct:
                if org not in school_stats:
                    school_stats[org] = {'total_amt': 0, 'main_vendor': comp}
                school_stats[org]['total_amt'] += amt

            # 이노뎁
            if '이노뎁' in comp:
                innodep_today_dict[org] = innodep_today_dict.get(org, 0) + amt
                innodep_total_amt += amt

    # ★ 수요기관 TOP 20 정렬
    org_stats_top20 = sorted(org_stats.items(), key=lambda x: -x[1])[:20]

    # ★ 경쟁사 통계 정렬 (금액 내림차순, 0 제외)
    competitor_stats = dict(
        sorted(
            ((k, v) for k, v in competitor_stats.items() if v > 0),
            key=lambda x: -x[1]
        )
    )

    # ─────────────────────────────────────────────────────────────────────────────
    # PART 2: 나라장터 입찰 공고 수집
    # ─────────────────────────────────────────────────────────────────────────────
    notice_mail_buckets = {cat: [] for cat in CAT_KEYWORDS}
    all_notice_count    = 0

    for cat_api, api_url in NOTICE_API_MAP.items():
        n_df = fetch_notice_data(cat_api, api_url, d_str)
        if not n_df.empty:
            all_notice_count += len(n_df)
            save_notice_by_year(drive_service, drive_creds, cat_api, n_df, year)

            pattern  = '|'.join(keywords_notice_all)
            filtered = n_df[n_df['bidNtceNm'].str.contains(pattern, na=False, case=False)]
            for _, row in filtered.iterrows():
                cat_found = classify_text(row['bidNtceNm'])
                if cat_found in notice_mail_buckets:
                    notice_mail_buckets[cat_found].append({
                        'org': row.get('dminsttNm', '-'),
                        'nm':  row.get('bidNtceNm', '-'),
                        'corp': '-',
                        'amt': row.get('presmptPrce', '별도공고'),
                        'url': row.get('bidNtceDtlUrl', '#')
                    })

    # ─────────────────────────────────────────────────────────────────────────────
    # PART 3: 나라장터 용역 계약 수집
    # ─────────────────────────────────────────────────────────────────────────────
    contract_mail_buckets = {cat: [] for cat in CAT_KEYWORDS}
    collected_servc       = []

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
        return not any(word in org_name for word in exclude_keywords)

    notice_mail_buckets['국방']   = [i for i in notice_mail_buckets['국방']   if is_valid_org(i['org'])]
    contract_mail_buckets['국방'] = [i for i in contract_mail_buckets['국방'] if is_valid_org(i['org'])]

    # ─────────────────────────────────────────────────────────────────────────────
    # PART 4: HTML 리포트 조립 → GitHub Actions output
    # ─────────────────────────────────────────────────────────────────────────────
    report_html = build_html_report(
        display_date        = display_date,
        weekday_str         = weekday_str,
        d_str               = d_str,
        final_data          = final_data,
        school_stats        = school_stats,
        innodep_today_dict  = innodep_today_dict,
        innodep_total_amt   = innodep_total_amt,
        competitor_stats    = competitor_stats,
        org_stats_top20     = org_stats_top20,
        notice_mail_buckets = notice_mail_buckets,
        contract_mail_buckets = contract_mail_buckets,
        all_notice_count    = all_notice_count,
        unique_servc_list   = unique_servc_list,
    )

    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:
            f.write(f"collect_date={d_str}\nfull_report<<EOF\n{report_html}\nEOF\n")


if __name__ == "__main__":
    main()
