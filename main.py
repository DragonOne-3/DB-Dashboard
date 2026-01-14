if final_data:
        ws.append_rows(final_data)
        
        # --- [중복 제거 로직 추가] ---
        # 기준: 기관명(7), 업체명(21), 금액(20), 세부품명(14)
        unique_final_data = {}
        for row in final_data:
            try:
                # 중복 판단을 위한 고유 키 생성
                key = (str(row[7]), str(row[21]), str(row[20]), str(row[14]))
                if key not in unique_final_data:
                    unique_final_data[key] = row
            except IndexError:
                continue
        
        # 중복이 제거된 데이터 리스트
        deduplicated_data = list(unique_final_data.values())

        school_stats = {} 
        innodep_today_dict = {}  # 이노뎁 중복 방지용 딕셔너리
        innodep_total_amt = 0

        # 중복 제거된 데이터를 바탕으로 분석 시작
        for row in deduplicated_data:
            try:
                org_name = str(row[7])      # 수요기관명
                item_name = str(row[14])    # 세부품명 (중복기준 포함)
                amt_val = str(row[20])      # 금액
                comp_name = str(row[21])    # 업체명
                contract_name = str(row[23])# 계약명
                
                amt_raw = amt_val.replace(',', '').split('.')[0]
                amt = int(amt_raw) if amt_raw else 0
            except (IndexError, ValueError): 
                continue

            # 1. 학교 & 지능형 CCTV 분석
            if '학교' in org_name and '지능형' in contract_name and 'CCTV' in contract_name:
                if org_name not in school_stats:
                    school_stats[org_name] = {'total_amt': 0, 'main_vendor': '', 'vendor_priority': 3}
                
                school_stats[org_name]['total_amt'] += amt
                
                priority = 3
                if '영상감시장치' in item_name: priority = 1
                elif '보안용카메라' in item_name: priority = 2
                
                if priority < school_stats[org_name]['vendor_priority']:
                    school_stats[org_name]['main_vendor'] = comp_name
                    school_stats[org_name]['vendor_priority'] = priority
                elif school_stats[org_name]['main_vendor'] == '':
                    school_stats[org_name]['main_vendor'] = comp_name

            # 2. 이노뎁 실적 추출 (중복 제거된 데이터 기준)
            if '이노뎁' in comp_name:
                # 동일 기관의 여러 건이 있을 수 있으므로 금액 합산 방식으로 처리
                if org_name in innodep_today_dict:
                    innodep_today_dict[org_name] += amt
                else:
                    innodep_today_dict[org_name] = amt
                innodep_total_amt += amt

        # --- 메일 본문 구성 ---
        summary_lines = []
        summary_lines.append("⭐ 오늘자 학교 지능형 CCTV 납품 현황:")
        if school_stats:
            for school, info in school_stats.items():
                summary_lines.append(f"- {school} [{info['main_vendor']}]: {info['total_amt']:,}원")
        else:
            summary_lines.append(" 0건")
        
        summary_lines.append(" ") 
        
        summary_lines.append("🏢 오늘자 이노뎁 실적:")
        if innodep_today_dict:
            for org, amt in innodep_today_dict.items():
                summary_lines.append(f"- {org}: {amt:,}원")
            summary_lines.append(f"** 총합계: {innodep_total_amt:,}원")
        else:
            summary_lines.append(" 0건")
