from common.utils.amount_utils import convert_amount_unit, convert_amount_org_holder, convert_amount_org_holder_1, \
    convert_amount_org_holder_2
from common.utils.cache_utils import get_cache_path, load_cache, save_cache
from common.http.http_utils import EASTMONEY_API_URL, fetch_eastmoney_api
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name


async def get_org_holder(stock_info: StockInfo, page_size=8, page_number=1):
    """获取机构持仓数据"""
    cache_path = get_cache_path("org_holder", stock_info.stock_code)
    
    # 检查缓存
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    params = {
        "reportName": "RPT_MAIN_ORGHOLD",
        "columns": "ALL",
        "quoteColumns": "",
        "filter": f"(SECURITY_CODE=\"{stock_info.stock_code}\")",
        "pageNumber": str(page_number),
        "pageSize": str(page_size),
        "sortTypes": "",
        "sortColumns": "",
        "source": "WEB",
        "client": "WEB"
    }
    
    data = await fetch_eastmoney_api(EASTMONEY_API_URL, params)
    if data.get("result") and data["result"].get("data"):
        result = data["result"]["data"]
        save_cache(cache_path, result)
        return result
    else:
        return []


async def get_shareholder_increase(stock_info: StockInfo, page_size=300, page_number=1):
    """获取股东增持数据"""
    cache_path = get_cache_path("shareholder_increase", stock_info.stock_code)
    
    # 检查缓存
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    params = {
        "sortColumns": "END_DATE,SECURITY_CODE,EITIME",
        "sortTypes": "-1,-1,-1",
        "pageSize": str(page_size),
        "pageNumber": str(page_number),
        "reportName": "RPT_SHARE_HOLDER_INCREASE",
        "quoteColumns": "f2~01~SECURITY_CODE~NEWEST_PRICE,f3~01~SECURITY_CODE~CHANGE_RATE_QUOTES",
        "quoteType": "0",
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "filter": f"(SECURITY_CODE=\"{stock_info.stock_code}\")"
    }
    data = await fetch_eastmoney_api(EASTMONEY_API_URL, params)
    if data.get("result") and data["result"].get("data"):
        result = data["result"]["data"]
        save_cache(cache_path, result)
        return result
    else:
        return None


async def get_holder_detail(stock_info: StockInfo, report_date=None, page_num=1, page_size=100, sh_type="", sh_code="", sort_field="HOLDER_CODE", sort_direc=1):
    """获取股票主力持仓明细"""
    from datetime import datetime
    if report_date is None:
        report_date = datetime.now().strftime("%Y-%m-%d")
    
    cache_path = get_cache_path("holder_detail", stock_info.stock_code)
    
    # 检查缓存
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    url = "https://data.eastmoney.com/dataapi/zlsj/detail"
    params = {
        "SHType": sh_type,
        "SHCode": sh_code,
        "SCode": stock_info.stock_code,
        "ReportDate": report_date,
        "sortField": sort_field,
        "sortDirec": sort_direc,
        "pageNum": page_num,
        "pageSize": page_size
    }
    data = await fetch_eastmoney_api(url, params)
    if data and data.get('data'):
        result = data['data']
        save_cache(cache_path, result)
        return result
    else:
        return []


async def get_shareholder_increase_markdown(stock_info: StockInfo, page_size=20):
    """获取股东增减持明细并转换为markdown"""
    items = await get_shareholder_increase(stock_info, page_size)
    if not items:
        return ""
    header = f"## <{stock_info.stock_name}（{stock_info.stock_code_normalize}）> - 股东增减持明细"
    markdown = f"{header}\n\n"
    markdown += "| 股东名称 | 增减 | 变动数量(万股) | 占总股本比例 | 占流通股比例 | 持股总数(万股) | 占总股本比例 | 持流通股数(万股) | 占流通股比例 | 变动开始日 | 变动截止日 | 公告日 |\n"
    markdown += "|---------|------|--------------|------------|------------|--------------|------------|----------------|------------|----------|----------|--------|\n"
    for item in items[:page_size]:
        holder_name = item.get('HOLDER_NAME', '--')
        direction = item.get('DIRECTION', '--')
        change_num = convert_amount_unit((item.get('CHANGE_NUM') or 0) * 10000)
        change_rate = f"{round(item.get('AFTER_CHANGE_RATE', 0), 2)}%" if item.get('AFTER_CHANGE_RATE') else '--'
        change_free_ratio = f"{round(item.get('CHANGE_FREE_RATIO', 0), 2)}%" if item.get('CHANGE_FREE_RATIO') else '--'
        after_holder_num = convert_amount_unit((item.get('AFTER_HOLDER_NUM') or 0) * 10000)
        hold_ratio = f"{round(item.get('HOLD_RATIO', 0), 2)}%" if item.get('HOLD_RATIO') else '--'
        free_shares = convert_amount_unit((item.get('FREE_SHARES') or 0) * 10000)
        free_shares_ratio = f"{round(item.get('FREE_SHARES_RATIO', 0), 2)}%" if item.get('FREE_SHARES_RATIO') else '--'
        start_date = item.get('START_DATE', '--')[:10] if item.get('START_DATE') else '--'
        end_date = item.get('END_DATE', '--')[:10] if item.get('END_DATE') else '--'
        notice_date = item.get('NOTICE_DATE', '--')[:10] if item.get('NOTICE_DATE') else '--'
        markdown += f"| {holder_name} | {direction} | {change_num} | {change_rate} | {change_free_ratio} | {after_holder_num} | {hold_ratio} | {free_shares} | {free_shares_ratio} | {start_date} | {end_date} | {notice_date} |"
    return markdown + "\n"


async def get_shareholder_increase_json(stock_info: StockInfo, page_size=20):
    """获取股东增减持明细并转换为JSON格式"""
    from datetime import datetime, timedelta

    items = await get_shareholder_increase(stock_info, page_size)
    if not items:
        return []
    
    # 计算一年前的日期
    one_year_ago = datetime.now() - timedelta(days=365)
    
    result = []
    for item in items[:page_size]:
        notice_date_str = item.get('NOTICE_DATE', '')
        if notice_date_str:
            notice_date = datetime.strptime(notice_date_str[:10], '%Y-%m-%d')
            # 过滤近一年的数据
            if notice_date < one_year_ago:
                continue
        
        result.append({
            "股东名称": item.get('HOLDER_NAME', '--'),
            "增减": item.get('DIRECTION', '--'),
            "变动数量(万股)": convert_amount_unit((item.get('CHANGE_NUM') or 0) * 10000),
            "占总股本比例": f"{round(item.get('AFTER_CHANGE_RATE', 0), 2)}%" if item.get('AFTER_CHANGE_RATE') else '--',
            "占流通股比例": f"{round(item.get('CHANGE_FREE_RATIO', 0), 2)}%" if item.get('CHANGE_FREE_RATIO') else '--',
            "持股总数(万股)": convert_amount_unit((item.get('AFTER_HOLDER_NUM') or 0) * 10000),
            "持股占总股本比例": f"{round(item.get('HOLD_RATIO', 0), 2)}%" if item.get('HOLD_RATIO') else '--',
            "持流通股数(万股)": convert_amount_unit((item.get('FREE_SHARES') or 0) * 10000),
            "持股占流通股比例": f"{round(item.get('FREE_SHARES_RATIO', 0), 2)}%" if item.get('FREE_SHARES_RATIO') else '--',
            "变动开始日": item.get('START_DATE', '--')[:10] if item.get('START_DATE') else '--',
            "变动截止日": item.get('END_DATE', '--')[:10] if item.get('END_DATE') else '--',
            "公告日": notice_date_str[:10] if notice_date_str else '--'
        })
    return result


async def get_holder_detail_markdown(stock_info: StockInfo, report_date=None, page_size=100):
    """获取股票主力持仓明细并转换为markdown"""
    from datetime import datetime
    if report_date is None:
        report_date = datetime.now().strftime("%Y-%m-%d")
    data = await get_holder_detail(stock_info, report_date, page_size=page_size)
    if not data:
        return ""
    markdown = f"## 主力持仓明细 (报告日期: {report_date})\n\n"
    markdown += "| 序号 | 机构名称 | 机构属性 | 持股总数(万股) | 持股市值(亿元) | 占总股本比例(%) | 占流通股本比例(%) |\n"
    markdown += "|------|---------|---------|--------------|--------------|----------------|-----------------|\n"
    for idx, item in enumerate(data, 1):
        holder_name = item.get('HOLDER_NAME', '--')
        org_type = item.get('ORG_TYPE', '--')
        total_shares = convert_amount_unit(item.get('TOTAL_SHARES', 0))
        market_cap = convert_amount_unit(item.get('HOLD_MARKET_CAP', 0))
        total_ratio = round(item.get('TOTAL_SHARES_RATIO', 0), 2)
        free_ratio = round(item.get('FREE_SHARES_RATIO', 0), 2)
        markdown += f"| {idx} | {holder_name} | {org_type} | {total_shares} | {market_cap} | {total_ratio} | {free_ratio} |\n"
    return markdown + "\n"


async def get_org_holder_markdown(stock_info: StockInfo, page_size=8):
    """获取机构持仓明细并转换为markdown"""
    holder_data = await get_org_holder(stock_info, page_size)
    if not holder_data:
        return ""
    from collections import defaultdict
    grouped_data = defaultdict(list)
    for item in holder_data:
        report_date = item.get('REPORT_DATE', '--')[:10] if item.get('REPORT_DATE') else '--'
        grouped_data[report_date].append(item)
    markdown = ""
    for report_date, items in grouped_data.items():
        header = f"## <{stock_info.stock_name}（{stock_info.stock_code_normalize}）> - {report_date} 机构持仓明细"
        markdown += f"""{header}

| 机构名称 | 持股家数(家) | 持股总数(万股) | 持股市值(亿元) |占总股本比例(%) | 占流通股比例(%) |
|---------|------------|--------------|--------------|--------------|---------------|
"""
        has_other = any(item.get('ORG_TYPE_NAME') == '其他' for item in items)
        
        for item in items:
            org_name = item.get('ORG_TYPE_NAME', '--')
            hold_num = item.get('HOULD_NUM')
            free_share = f"{convert_amount_org_holder(item.get('FREE_SHARES', 0))}" if item.get('FREE_SHARES') else '--'
            free_market_cap = f"{convert_amount_org_holder_1(item.get('FREE_MARKET_CAP', 0))}" if item.get('FREE_MARKET_CAP') else '--'
            free_total_ratio = f"{round(item.get('TOTALSHARES_RATIO', 0), 2)}%" if item.get('TOTALSHARES_RATIO') else '--'
            free_share_ratio = f"{round((item.get('FREESHARES_RATIO') or 0), 2)}%"
            markdown += f"| {org_name} | {hold_num} | {free_share} | {free_market_cap} | {free_total_ratio} | {free_share_ratio} |\n"
        
        if not has_other:
            markdown += "| 其他 | - | 0.00 | - | - | - |\n"
        
        markdown += "\n"
    return markdown


async def get_org_holder_json(stock_info: StockInfo, page_size=8, fields=None):
    """获取机构持仓明细并转换为JSON格式
    
    Args:
        stock_info: 股票信息
        page_size: 返回数据条数
        fields: 指定返回的字段列表（英文key），如 ['ORG_TYPE_NAME', 'HOULD_NUM', 'FREE_SHARES']，None表示返回所有字段
    """
    holder_data = await get_org_holder(stock_info, page_size)
    if not holder_data:
        return []
    from collections import defaultdict
    grouped_data = defaultdict(list)
    for item in holder_data:
        report_date = item.get('REPORT_DATE', '--')[:10] if item.get('REPORT_DATE') else '--'
        grouped_data[report_date].append(item)
    
    # 字段映射
    field_map = {
        'ORG_TYPE_NAME': '机构名称',
        'HOULD_NUM': '持股家数(家)',
        'FREE_SHARES': '持股总数(万股)',
        'FREE_MARKET_CAP': '持股市值(亿元)',
        'TOTALSHARES_RATIO': '占总股本比例(%)',
        'FREESHARES_RATIO': '占流通股比例(%)',
        'HOLDCHA_NUM': '持股变化(万股)'
    }
    
    result = []
    for report_date, items in grouped_data.items():
        report_items = []
        has_other = any(item.get('ORG_TYPE_NAME') == '其他' for item in items)
        
        for item in items:
            row = {}
            for en_key, cn_key in field_map.items():
                if fields and en_key not in fields:
                    continue
                
                if en_key == 'ORG_TYPE_NAME':
                    row[cn_key] = item.get(en_key, '--')
                elif en_key == 'HOULD_NUM':
                    row[cn_key] = item.get(en_key)
                elif en_key == 'FREE_SHARES':
                    row[cn_key] = convert_amount_org_holder_1(item.get(en_key, 0)) if item.get(en_key) else '--'
                elif en_key == 'FREE_MARKET_CAP':
                    row[cn_key] = convert_amount_org_holder_2(item.get(en_key, 0)) if item.get(en_key) else '--'
                elif en_key == 'TOTALSHARES_RATIO':
                    row[cn_key] = f"{round(item.get(en_key, 0), 2)}%" if item.get(en_key) else '--'
                elif en_key == 'FREESHARES_RATIO':
                    row[cn_key] = f"{round((item.get(en_key) or 0), 2)}%"
                elif en_key == 'HOLDCHA_NUM':
                    row[cn_key] = convert_amount_org_holder_1(item.get(en_key, 0))
            
            report_items.append(row)
        
        if not has_other and (not fields or 'ORG_TYPE_NAME' in fields):
            other_row = {}
            for en_key, cn_key in field_map.items():
                if fields and en_key not in fields:
                    continue
                if en_key == 'ORG_TYPE_NAME':
                    other_row[cn_key] = '其他'
                elif en_key == 'HOULD_NUM':
                    other_row[cn_key] = '-'
                elif en_key == 'FREE_SHARES':
                    other_row[cn_key] = '0.00'
                else:
                    other_row[cn_key] = '-'
            report_items.append(other_row)
        
        result.append({
            "报告日期": report_date,
            "机构持仓": report_items
        })
    return result


async def get_org_holder_by_type(stock_info: StockInfo, org_type: str, page_size=8, fields=None):
    """获取指定机构类型的持仓数据
    
    Args:
        stock_info: 股票信息
        org_type: 机构类型，如 '社保', '公募', '险资', 'QFII', '信托', '券商', '银行', '其他'
        page_size: 返回数据条数
        fields: 指定返回的字段列表（英文key），如 ['ORG_TYPE_NAME', 'HOULD_NUM', 'FREE_SHARES']，None表示返回所有字段
    """
    all_data = await get_org_holder_json(stock_info, page_size, fields)
    
    result = []
    for period in all_data:
        filtered_items = [item for item in period["机构持仓"] if item.get("机构名称") == org_type]
        if filtered_items:
            result.append({
                "报告日期": period["报告日期"],
                "机构持仓": filtered_items
            })
    
    return result


async def get_org_holder_count(stock_info: StockInfo, page_size=8):
    """获取每个季度持股机构的总数"""
    holder_data = await get_org_holder(stock_info, page_size)
    if not holder_data:
        return []
    from collections import defaultdict
    grouped_data = defaultdict(int)
    for item in holder_data:
        report_date = item.get('REPORT_DATE', '--')[:10] if item.get('REPORT_DATE') else '--'
        hold_num = item.get('HOULD_NUM', 0) or 0
        grouped_data[report_date] += hold_num
    
    return [{"报告日期": date, "机构总数": count} for date, count in grouped_data.items()]


async def get_holder_number(stock_info: StockInfo, page_size=12, page_number=1):
    """获取股东人数数据"""
    cache_path = get_cache_path("holder_number_" + str(page_number), stock_info.stock_code)
    
    # 检查缓存
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    params = {
        "reportName": "RPT_F10_EH_HOLDERNUM",
        "columns": "SECUCODE,SECURITY_CODE,END_DATE,HOLDER_TOTAL_NUM,TOTAL_NUM_RATIO,AVG_FREE_SHARES,AVG_FREESHARES_RATIO,HOLD_FOCUS,PRICE,AVG_HOLD_AMT,HOLD_RATIO_TOTAL,FREEHOLD_RATIO_TOTAL",
        "quoteColumns": "",
        "filter": f"(SECUCODE=\"{stock_info.stock_code_normalize}\")",
        "pageNumber": str(page_number),
        "pageSize": str(page_size),
        "sortTypes": "-1",
        "sortColumns": "END_DATE",
        "source": "HSF10",
        "client": "PC"
    }
    
    data = await fetch_eastmoney_api(EASTMONEY_API_URL, params)
    if data.get("result") and data["result"].get("data"):
        result = data["result"]["data"]
        save_cache(cache_path, result)
        return result
    else:
        return []


async def get_holder_number_json(stock_info: StockInfo, page_size=20, lang='en', fields=None):
    """
    获取股东人数数据并转换为JSON格式
    
    Args:
        stock_info: 股票信息
        page_size: 返回数据条数
        lang: 语言，'en'返回英文key，'cn'返回中文key
        fields: 指定返回的字段列表，可以使用英文或中文字段名，为None时返回所有字段
    """
    data = []
    page_num = (page_size + 7) // 8
    for i in range(1, page_num + 1):
        page_data = await get_holder_number(stock_info, 8, i)
        if page_data:
            data.extend(page_data)
    if not data:
        return []
    
    # 字段映射
    field_map_en = {
        'end_date': 'END_DATE',
        'holder_total_num': 'HOLDER_TOTAL_NUM',
        'total_num_ratio': 'TOTAL_NUM_RATIO',
        'avg_free_shares': 'AVG_FREE_SHARES',
        'avg_freeshares_ratio': 'AVG_FREESHARES_RATIO',
        'hold_focus': 'HOLD_FOCUS',
        'price': 'PRICE',
        'avg_hold_amt': 'AVG_HOLD_AMT'
    }
    
    field_map_cn = {
        '截止日期': 'END_DATE',
        '股东总数': 'HOLDER_TOTAL_NUM',
        '较上期变化': 'TOTAL_NUM_RATIO',
        '人均流通股(股)': 'AVG_FREE_SHARES',
        '较上期变化率': 'AVG_FREESHARES_RATIO',
        '筹码集中度': 'HOLD_FOCUS',
        '股价(元)': 'PRICE',
        '人均持股金额(元)': 'AVG_HOLD_AMT'
    }
    
    # 英文到中文的映射
    en_to_cn = {
        'end_date': '截止日期',
        'holder_total_num': '股东总数',
        'total_num_ratio': '较上期变化',
        'avg_free_shares': '人均流通股(股)',
        'avg_freeshares_ratio': '较上期变化率',
        'hold_focus': '筹码集中度',
        'price': '股价(元)',
        'avg_hold_amt': '人均持股金额(元)'
    }
    
    field_map = field_map_cn if lang == 'cn' else field_map_en
    
    # 处理fields，支持英文和中文字段名
    if fields:
        selected_fields = []
        for field in fields:
            if field in field_map:
                selected_fields.append(field)
            elif field in field_map_en:
                # 如果是英文字段且lang='cn'，转换为中文
                selected_fields.append(en_to_cn[field] if lang == 'cn' else field)
            elif field in field_map_cn:
                # 如果是中文字段且lang='en'，转换为英文
                cn_to_en = {v: k for k, v in en_to_cn.items()}
                selected_fields.append(cn_to_en[field] if lang == 'en' else field)
    else:
        selected_fields = list(field_map.keys())
    
    result = []
    for item in data:
        row = {}
        for field in selected_fields:
            if field not in field_map:
                continue
            
            raw_key = field_map[field]
            value = item.get(raw_key)
            
            if raw_key == 'END_DATE':
                row[field] = value[:10] if value else '--'
            elif raw_key == 'HOLD_FOCUS':
                if value:
                    try:
                        row[field] = f"{round(float(value), 2)}%"
                    except (ValueError, TypeError):
                        row[field] = str(value)
                else:
                    row[field] = '--'
            elif raw_key in ['TOTAL_NUM_RATIO', 'AVG_FREESHARES_RATIO']:
                row[field] = f"{round(float(value), 2)}%" if value else '--'
            elif raw_key in ['AVG_FREE_SHARES', 'PRICE', 'AVG_HOLD_AMT']:
                row[field] = f"{float(value):.2f}" if value else '--'
            else:
                row[field] = value if value else '--'
        
        result.append(row)
    return result[:page_size]


async def get_holder_number_json_cn(stock_info: StockInfo, page_size=20, fields=None):
    """获取股东人数数据并转换为JSON格式 - 中文key（兼容旧接口）"""
    return await get_holder_number_json(stock_info, page_size, lang='cn', fields=fields)


if __name__ == "__main__":
    import asyncio
    import json
    
    async def main():
        stock_name = "北方华创"
        stock_info: StockInfo = get_stock_info_by_name(stock_name)
        # 测试股东人数数据
        result = await get_org_holder_by_type(stock_info, '社保')
        print("股东人数数据 (JSON格式):")
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    asyncio.run(main())
