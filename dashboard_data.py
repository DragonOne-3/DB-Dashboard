import os, json, re
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo
import gspread
from oauth2client.service_account import ServiceAccountCredentials

GOOGLE_AUTH_JSON = os.environ['GOOGLE_AUTH_JSON']
KEYWORD = os.environ.get('DASHBOARD_KEYWORD', '음식물')
OUTPUT_DIR = os.environ.get('DASHBOARD_OUTPUT_DIR', 'docs/data')
LEGACY_OUTPUT_PATH = os.environ.get('DASHBOARD_OUTPUT_PATH', 'docs/data.json')
SEOUL = ZoneInfo('Asia/Seoul')

SOURCE_SPREADSHEETS = {
    '발주계획': '군수품조달_국내_발주계획',
    '계약정보': '군수품조달_국내_계약정보',
    '입찰공고': '군수품조달_국내_입찰공고',
}
EXCLUDE_TABS = {'백필_진행상황'}

TITLE_COLUMNS = ['대표품목명','공고명','입찰공고명','입찰명','사업명','품명','계약명','수요품명','구매품명','용역명','건명','bidNm','itemNm','cntrctNm','prcurePlanNm']
AGENCY_COLUMNS = ['발주기관','수요기관','수요부대','기관명','ornt','dminsttNm','orderInsttNm']
COMPANY_COLUMNS = ['업체명','계약업체명','계약상대자','계약상대자명','계약업체','낙찰업체명','낙찰자명','업체상호','상호','업체','cntrctEntrpsNm','cntrctCorpNm','cntrctCompanyNm','sucsfbidEntrpsNm','corpNm','companyNm']
STATUS_COLUMNS = ['진행상태','계약상태','공고구분','pblancSe','bidNtceSttusNm']
BID_RATE_COLUMNS = ['낙찰률','낙찰율','sucsfbidRate','bidRate']
DATE_COLUMNS = ['공고일자','계약일자','계약일','발주예정월','등록일자','작성일자','pblancDate','cntrctDate','orderPrearngeMt','rgstDt']
DEADLINE_COLUMNS = ['입찰서제출마감일시','입찰참가등록마감일시','개찰일시','biddocPresentnClosDt','bidPartcptRegistClosDt','opengDt']
IDENTIFIER_COLUMNS = ['G2B공고번호','공고번호','공고차수','계약번호','판단번호','사업번호','g2bPblancNo','pblancNo','pblancOdr','cntrctNo','dcsNo','bidNtceNo']
LOOKUP_COLUMNS = {
    '발주계획': ['판단번호','사업번호','dcsNo','prcurePlanNo'],
    '입찰공고': ['G2B공고번호','g2bPblancNo','공고번호','pblancNo','판단번호','dcsNo'],
    '계약정보': ['계약번호','cntrctNo','계약명','cntrctNm'],
}
AMOUNT_COLUMNS = {
    '발주계획':['예산금액','추정금액','사업금액','예정금액','budgetAmount','orderPlanAmount','asignBdgtAmt'],
    '입찰공고':['기초예비가격','기초예가','추정가격','배정예산','예정가격','예산금액','bsicExpt','bsisPrdprc','presmptPrce','asignBdgtAmt'],
    '계약정보':['계약금액','계약금액(원)','총계약금액','총계약금액(원)','계약총액','계약액','최종계약금액','낙찰금액','낙찰금액(원)','계약단가','예산금액','cntrctAmt','cntrctAmount','totCntrctAmt','totalCntrctAmt','sucsfbidAmt','sucsfbidAmount'],
}

RENTAL = ['임차','렌탈','리스','대여']
SERVICE = ['폐기물','위탁처리','수집운반','수거','운반','처리용역']
MAINT = ['정비','유지보수','수리','보수','위탁관리']
ACCESSORY = ['처리대','처리통','잔반통','쓰레기통','봉투','보관판넬','부수자재']
CORE = ['음식물처리기','음식물 처리기','음식물쓰레기처리기','음식물 쓰레기 처리기','감량기','감량기기','분쇄기','건조기','탈수기','자원화기기']
CANCELLED = {'취소공고','유찰','종료','계약종료'}

def first(row, cols):
    for c in cols:
        v = row.get(c)
        if v not in (None, ''):
            return str(v).strip()
    return None

def digits(v):
    return ''.join(ch for ch in str(v or '') if ch.isdigit())

def number(v):
    if v in (None, ''): return 0
    try: return int(float(str(v).replace(',','').strip()))
    except: return 0

def norm_title(t):
    """공백/대소문자 차이로 같은 사업이 다르게 인식되지 않도록 정규화."""
    return re.sub(r'\s+', '', str(t or '')).upper()

def title_of(row):
    v = first(row, TITLE_COLUMNS)
    if v: return v
    for v in row.values():
        if v and KEYWORD in str(v): return str(v).strip()
    return '(제목 없음)'

def amount_of(row, source):
    # 출처별 우선 필드를 먼저 사용합니다.
    for c in AMOUNT_COLUMNS[source]:
        if row.get(c) not in (None, ''):
            n = number(row[c])
            if n > 0:
                return n

    # 실제 스프레드시트의 컬럼명이 조금 달라도 계약금액을 찾도록 보완합니다.
    ranked = []
    for k, v in row.items():
        if v in (None, ''):
            continue
        key = str(k).replace(' ', '').lower()
        score = 0
        if source == '계약정보':
            if '계약금액' in key or 'cntrctamt' in key or 'contractamount' in key:
                score = 100
            elif '낙찰금액' in key or 'sucsfbidamt' in key:
                score = 90
            elif '계약총액' in key or '총계약' in key:
                score = 85
            elif '금액' in key and '단가' not in key:
                score = 50
        else:
            if any(x in key for x in ('금액','가격','예가','예산','amount','price','expt')):
                score = 50
        if score:
            n = number(v)
            if n > 0:
                ranked.append((score, n))
    return max(ranked, default=(0, 0))[1]

def date_key(row):
    for c in DATE_COLUMNS:
        d = digits(row.get(c))
        if len(d) >= 8: return d[:8]
        if len(d) >= 6: return d[:6]
    return ''

def deadline_key(row):
    vals = []
    for c in DEADLINE_COLUMNS:
        d = digits(row.get(c))
        if len(d) >= 8: vals.append(d[:12])
    return max(vals) if vals else ''

def category(title):
    compact = title.replace(' ','')
    if any(k.replace(' ','') in compact for k in CORE):
        return '장비임차' if any(k in title for k in RENTAL) else '장비구매'
    if any(k in title for k in ACCESSORY): return '부속·비품'
    if any(k in title for k in MAINT): return '유지보수'
    if any(k in title for k in SERVICE): return '폐기물처리용역'
    if any(k in title for k in RENTAL): return '기타임차'
    return '기타음식물'

def is_equipment(cat):
    return cat in {'장비구매','장비임차','부속·비품','유지보수'}

def status_of(row):
    return first(row, STATUS_COLUMNS) or '미상'

def is_open(source, row, status):
    if source == '계약정보' or status in CANCELLED: return False
    now = datetime.now(SEOUL).strftime('%Y%m%d%H%M')
    if source == '입찰공고':
        dl = deadline_key(row)
        return bool(dl and dl >= now[:len(dl)])
    dk = date_key(row)
    return bool(dk and dk[:6] >= now[:6])

def bid_rate(row):
    v = first(row, BID_RATE_COLUMNS)
    if not v: return None
    try: return float(re.sub(r'[^0-9.]','',v))
    except: return None

def lookup_keyword(row, source):
    # D2B 통합검색은 계약번호보다 계약명이 검색 성공률이 높습니다.
    if source == '계약정보':
        return title_of(row)
    return first(row, LOOKUP_COLUMNS[source]) or title_of(row)

def dedupe(row, source):
    ids = [str(row[c]).strip() for c in IDENTIFIER_COLUMNS if row.get(c) not in (None,'')]
    if ids: return source + '|' + '|'.join(ids)
    return source+'|'+title_of(row)+'|'+(first(row,AGENCY_COLUMNS) or '')+'|'+date_key(row)+'|'+str(amount_of(row,source))

def score_row(r):
    if not r['_open']: return 0
    score = 20
    if r['_amount'] >= 100_000_000: score += 30
    elif r['_amount'] >= 30_000_000: score += 20
    elif r['_amount'] > 0: score += 10
    if r['_category'] in {'장비구매','장비임차'}: score += 25
    elif r['_category'] in {'유지보수','부속·비품'}: score += 15
    if r['_source'] == '입찰공고': score += 15
    elif r['_source'] == '발주계획': score += 10
    return min(score, 100)

def consolidate(rows):
    """같은 사업(정규화한 사업명+발주기관)이 여러 탭/회차에 걸쳐 반복 등록된 경우
    가장 최신 1건만 남기고, 몇 번 반복 등록됐는지는 _repeat_count로 남깁니다."""
    groups = defaultdict(list)
    for r in rows:
        key = (norm_title(r['_title']), r['_agency'] or '')
        groups[key].append(r)
    out = []
    for grp in groups.values():
        grp.sort(key=lambda r: (r['_date_sort_key'] or '', r['_amount']), reverse=True)
        best = grp[0]
        best['_repeat_count'] = len(grp)
        out.append(best)
    return out

def scan(client, name, source):
    ss = client.open(name); rows=[]; seen=set(); headers=[]
    for ws in ss.worksheets():
        if ws.title in EXCLUDE_TABS: continue
        vals = ws.get_all_values()
        if len(vals) < 2: continue
        hdr = vals[0]
        for h in hdr:
            if h and h not in headers: headers.append(h)
        for vr in vals[1:]:
            if not any(vr) or not any(KEYWORD in str(v) for v in vr if v): continue
            row = dict(zip(hdr, vr)); key = dedupe(row, source)
            if key in seen: continue
            seen.add(key)
            t = title_of(row); cat = category(t); st = status_of(row)
            row.update({
                '_source': source, '_title': t, '_detail_category': cat, '_category': cat,
                '_amount': amount_of(row,source), '_date_sort_key': date_key(row), '_date': date_key(row),
                '_deadline': deadline_key(row), '_agency': first(row,AGENCY_COLUMNS),
                '_company': first(row,COMPANY_COLUMNS), '_status': st,
                '_is_open': is_open(source,row,st), '_open': is_open(source,row,st),
                '_bid_rate': bid_rate(row), '_tab': ws.title,
                '_lookup_keyword': lookup_keyword(row, source),
            })
            row['_score'] = score_row(row)
            row['_grade'] = 'A' if row['_score'] >= 70 else ('B' if row['_score'] >= 45 else ('C' if row['_score'] > 0 else '-'))
            rows.append(row)
    return headers, consolidate(rows)

def monthly(rows):
    d=defaultdict(lambda:{'count':0,'amount':0,'open_count':0,'open_amount':0})
    for r in rows:
        m=r['_date'][:6]
        if len(m)!=6: continue
        b=d[m]; b['count']+=1; b['amount']+=r['_amount']
        if r['_open']: b['open_count']+=1; b['open_amount']+=r['_amount']
    return [{'month':m,**d[m]} for m in sorted(d)[-12:]]

def agency_rank(rows):
    a=defaultdict(lambda:{'count':0,'amount':0,'open_count':0,'open_amount':0})
    for r in rows:
        if not r['_agency']: continue
        b=a[r['_agency']]; b['count']+=1; b['amount']+=r['_amount']
        if r['_open']: b['open_count']+=1; b['open_amount']+=r['_amount']
    out=[{'agency':k,**v} for k,v in a.items()]
    return sorted(out,key=lambda x:(x['open_amount'],x['amount'],x['count']),reverse=True)[:30]

def competitors(rows):
    a=defaultdict(lambda:{'count':0,'amount':0,'rates':[]})
    for r in rows:
        if not r['_company']: continue
        b=a[r['_company']]; b['count']+=1; b['amount']+=r['_amount']
        if r['_bid_rate'] is not None: b['rates'].append(r['_bid_rate'])
    out=[]
    for k,v in a.items():
        out.append({'company':k,'count':v['count'],'amount':v['amount'],'avg_rate':round(sum(v['rates'])/len(v['rates']),2) if v['rates'] else None})
    return sorted(out,key=lambda x:(x['amount'],x['count']),reverse=True)[:30]

def compact(r):
    return {
        'source':r['_source'],'title':r['_title'],'agency':r['_agency'],'company':r['_company'],
        'amount':r['_amount'],'status':r['_status'],'date':r['_date'],'deadline':r['_deadline'],
        'category':r['_category'],'score':r['_score'],'grade':r['_grade'],
        'lookup_keyword':r['_lookup_keyword'],'open':bool(r['_open']),'bid_rate':r['_bid_rate'],
        'amount_missing': bool(r['_source']=='계약정보' and not r['_amount']),
        'repeat_count': r.get('_repeat_count', 1),
    }

def main():
    scope=['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds=ServiceAccountCredentials.from_json_keyfile_dict(json.loads(GOOGLE_AUTH_JSON),scope)
    client=gspread.authorize(creds)
    all_rows=[]; sources={}; pipeline={}; month_data={}; agencies={}
    for source,name in SOURCE_SPREADSHEETS.items():
        print('스캔:',source)
        headers,rows=scan(client,name,source); all_rows += rows
        sources[source]={'columns':headers,'equipment':[r for r in rows if is_equipment(r['_category'])],'other':[r for r in rows if not is_equipment(r['_category'])]}
        pipeline[source]={'count':len(rows),'amount':sum(r['_amount'] for r in rows),'open_count':sum(r['_open'] for r in rows),'open_amount':sum(r['_amount'] for r in rows if r['_open'])}
        month_data[source]=monthly(rows); agencies[source]=agency_rank(rows)
    contracts=[r for r in all_rows if r['_source']=='계약정보']
    open_rows=sorted([r for r in all_rows if r['_open']],key=lambda r:(r['_score'],r['_amount'],r['_deadline']),reverse=True)
    comp=competitors(contracts)
    equip=[r for r in all_rows if is_equipment(r['_category'])]
    services=[r for r in all_rows if r['_category']=='폐기물처리용역']
    rates=[r['_bid_rate'] for r in contracts if r['_bid_rate'] is not None]
    now=datetime.now(SEOUL); cm=now.strftime('%Y%m')
    contract_records=sorted([compact(r) for r in contracts],key=lambda r:(r['date'],r['amount']),reverse=True)
    contract_agencies=defaultdict(lambda:{'count':0,'amount':0})
    for r in contracts:
        if r['_agency']:
            contract_agencies[r['_agency']]['count'] += 1
            contract_agencies[r['_agency']]['amount'] += r['_amount']
    contract_agency_summary=sorted([{'agency':k,**v} for k,v in contract_agencies.items()],key=lambda x:(x['amount'],x['count']),reverse=True)
    payload={
        'updated_at':now.strftime('%Y-%m-%d %H:%M:%S'),'keyword':KEYWORD,'pipeline':pipeline,
        'monthly':month_data,'agencies':agencies,'competitors':{'계약정보':comp},
        'priority_leads':[compact(r) for r in open_rows[:30]],
        'dashboard_records':[compact(r) for r in sorted(all_rows,key=lambda r:(r['_date'],r['_amount']),reverse=True)],
        'contract_records':contract_records,'contract_agencies':contract_agency_summary,
        'kpi':{
            'total_count':len(all_rows),'open_count':len(open_rows),'open_amount':sum(r['_amount'] for r in open_rows),
            'equipment_count':len(equip),'equipment_amount':sum(r['_amount'] for r in equip),
            'service_count':len(services),'service_amount':sum(r['_amount'] for r in services),
            'contract_amount':sum(r['_amount'] for r in contracts),'company_count':len(comp),
            'avg_bid_rate':round(sum(rates)/len(rates),2) if rates else None,
            'current_month_count':sum(1 for r in all_rows if r['_date'].startswith(cm)),
        }
    }
    os.makedirs(OUTPUT_DIR,exist_ok=True)
    with open(os.path.join(OUTPUT_DIR,'summary.json'),'w',encoding='utf-8') as f: json.dump(payload,f,ensure_ascii=False,separators=(',',':'))
    with open(os.path.join(OUTPUT_DIR,'leads.json'),'w',encoding='utf-8') as f: json.dump({'updated_at':payload['updated_at'],'keyword':KEYWORD,'sources':sources,'kpi':payload['kpi']},f,ensure_ascii=False,separators=(',',':'))
    full={**payload,'sources':sources}
    os.makedirs(os.path.dirname(LEGACY_OUTPUT_PATH) or '.',exist_ok=True)
    with open(LEGACY_OUTPUT_PATH,'w',encoding='utf-8') as f: json.dump(full,f,ensure_ascii=False,separators=(',',':'))
    print('완료:',len(all_rows),'건, 진행중',len(open_rows),'건, 계약기관',len(contract_agency_summary),'개')

if __name__=='__main__': main()
