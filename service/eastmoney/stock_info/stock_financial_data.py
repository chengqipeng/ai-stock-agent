from common.utils.amount_utils import convert_amount_unit
from common.http.http_utils import EASTMONEY_API_URL, fetch_eastmoney_api


async def get_financial_data(stock_code="002371", page_size=5, page_number=1):
    """获取财务数据"""
    params = {
        "sortColumns": "REPORTDATE",
        "sortTypes": "-1",
        "pageSize": str(page_size),
        "pageNumber": str(page_number),
        "columns": "ALL",
        "filter": f"(SECURITY_CODE=\"{stock_code}\")",
        "reportName": "RPT_LICO_FN_CPD"
    }

    data = await fetch_eastmoney_api(EASTMONEY_API_URL, params)
    if data.get("result") and data["result"].get("data"):
        return data["result"]["data"]
    else:
        raise Exception(f"未获取到股票 {stock_code} 的财务数据")


async def get_financial_report(stock_code="002371", page_size=15, page_number=1):
    """业绩报表明细"""
    params = {
        "sortColumns": "REPORTDATE",
        "sortTypes": "-1",
        "pageSize": str(page_size),
        "pageNumber": str(page_number),
        "columns": "ALL",
        "filter": f"(SECURITY_CODE=\"{stock_code}\")",
        "reportName": "RPT_LICO_FN_CPD"
    }

    data = await fetch_eastmoney_api(EASTMONEY_API_URL, params)
    if data.get("result") and data["result"].get("data"):
        return data["result"]["data"]
    else:
        raise Exception(f"未获取到股票 {stock_code} 的财务报表数据")


async def get_financial_fast_report(stock_code="002371", page_size=15, page_number=1):
    """获取业绩预告数据"""
    params = {
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "pageSize": str(page_size),
        "pageNumber": str(page_number),
        "columns": "ALL",
        "filter": f"(SECURITY_CODE=\"{stock_code}\")",
        "reportName": "RPT_FCI_PERFORMANCEE"
    }

    data = await fetch_eastmoney_api(EASTMONEY_API_URL, params)
    if data.get("result") and data["result"].get("data"):
        return data["result"]["data"]
    else:
        raise Exception(f"未获取到股票 {stock_code} 的业绩预告数据")


async def get_performance_forecast(stock_code="002371", page_size=15, page_number=1):
    """获取业绩预告数据"""
    params = {
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "pageSize": str(page_size),
        "pageNumber": str(page_number),
        "columns": "ALL",
        "filter": f"(SECURITY_CODE=\"{stock_code}\")",
        "reportName": "RPT_PUBLIC_OP_NEWPREDICT"
    }

    data = await fetch_eastmoney_api(EASTMONEY_API_URL, params)
    if data.get("result") and data["result"].get("data"):
        return data["result"]["data"]
    else:
        raise Exception(f"未获取到股票 {stock_code} 的业绩预告数据")


async def get_financial_report_markdown(stock_code, page_size=5, stock_name=None):
    """获取业绩报表明细并转换为markdown"""
    report_data = await get_financial_report(stock_code, page_size)
    if not report_data:
        return ""
    header = f"## <{stock_code} {stock_name}> - 业绩报表明细" if stock_name else "## 业绩报表明细"
    markdown = f"""{header}
| 截止日期 | 每股收益(元) | 每股收益(扣除)(元) | 营业总收入（元） | 营业总收入同步增长（%） | 营业总收入季度环比增长（%） | 净利润（元） | 净利润同步增长（%） | 净利润环比增长（%） | 每股净资产(元) | 净资产收益率(%) | 每股经营现金流量(元) | 销售毛利率(%) | 利润分配 | 首次公告日期 |
|----------|-----------|------------------|---------------|----------------------|------------------------|------------|------------------|------------------|--------------|---------------|-------------------|--------------|--------|------------|
"""
    for item in report_data:
        report_date = item.get('REPORTDATE', '--')[:10] if item.get('REPORTDATE') else '--'
        basic_eps = round(item.get('BASIC_EPS', 0), 2) if item.get('BASIC_EPS') else '--'
        deduct_eps = round(item.get('DEDUCT_BASIC_EPS', 0), 2) if item.get('DEDUCT_BASIC_EPS') else '-'
        total_income = item.get('TOTAL_OPERATE_INCOME')
        income_str = convert_amount_unit(total_income) if total_income else '--'
        ystz = f"{round(item.get('YSTZ', 0), 2)}%" if item.get('YSTZ') else '--'
        yshz = f"{round(item.get('YSHZ', 0), 2)}%" if item.get('YSHZ') else '--'
        net_profit = item.get('PARENT_NETPROFIT')
        profit_str = convert_amount_unit(net_profit) if net_profit else '--'
        sjltz = f"{round(item.get('SJLTZ', 0), 2)}%" if item.get('SJLTZ') else '--'
        sjlhz = f"{round(item.get('SJLHZ', 0), 2)}%" if item.get('SJLHZ') else '--'
        bps = round(item.get('BPS', 0), 2) if item.get('BPS') else '--'
        roe = f"{round(item.get('WEIGHTAVG_ROE', 0), 2)}%" if item.get('WEIGHTAVG_ROE') else '--'
        mgjyxjje = round(item.get('MGJYXJJE', 0), 2) if item.get('MGJYXJJE') else '--'
        xsmll = f"{round(item.get('XSMLL', 0), 2)}%" if item.get('XSMLL') else '--'
        assigndscrpt = item.get('ASSIGNDSCRPT', '-') if item.get('ASSIGNDSCRPT') else '-'
        notice_date = item.get('NOTICE_DATE', '--')[:10] if item.get('NOTICE_DATE') else '--'
        markdown += f"| {report_date} | {basic_eps} | {deduct_eps} | {income_str} | {ystz} | {yshz} | {profit_str} | {sjltz} | {sjlhz} | {bps} | {roe} | {mgjyxjje} | {xsmll} | {assigndscrpt} | {notice_date} |\n"
    return markdown + "\n"


async def get_financial_fast_report_markdown(stock_code, page_size=15):
    """获取业绩快报明细并转换为markdown"""
    forecast_data = await get_financial_fast_report(stock_code, page_size)
    if not forecast_data:
        return ""
    markdown = """## 业绩快报明细

| 截止日期 | 每股收益(元) | 营业总收入 | | | | 净利润 | | | | 每股净资产(元) | 净资产收益率(%) | 公告日期 |
|----------|-------------|-----------|---------|---------|---------|--------|---------|---------|---------|---------------|----------------|----------|
| | | 营业收入(元) | 去年同期(元) | 同比增长(%) | 季度环比增长(%) | 净利润(元) | 去年同期(元) | 同比增长(%) | 季度环比增长(%) | | | |
"""
    for item in forecast_data:
        report_date = item.get('REPORT_DATE', '--')[:10] if item.get('REPORT_DATE') else '--'
        basic_eps = round(item.get('BASIC_EPS', 0), 2) if item.get('BASIC_EPS') else '--'
        total_income = item.get('TOTAL_OPERATE_INCOME')
        income_str = convert_amount_unit(total_income) if total_income else '--'
        total_income_sq = item.get('TOTAL_OPERATE_INCOME_SQ')
        income_sq_str = convert_amount_unit(total_income_sq) if total_income_sq else '--'
        ystz = f"{round(item.get('YSTZ', 0), 2)}%" if item.get('YSTZ') else '--'
        djdyshz = f"{round(item.get('DJDYSHZ', 0), 2)}%" if item.get('DJDYSHZ') else '--'
        net_profit = item.get('PARENT_NETPROFIT')
        profit_str = convert_amount_unit(net_profit) if net_profit else '--'
        net_profit_sq = item.get('PARENT_NETPROFIT_SQ')
        profit_sq_str = convert_amount_unit(net_profit_sq) if net_profit_sq else '--'
        jlrtbzcl = f"{round(item.get('JLRTBZCL', 0), 2)}%" if item.get('JLRTBZCL') else '--'
        djdjlhz = f"{round(item.get('DJDJLHZ', 0), 2)}%" if item.get('DJDJLHZ') else '--'
        bvps = round(item.get('PARENT_BVPS', 0), 2) if item.get('PARENT_BVPS') else '--'
        roe = f"{round(item.get('WEIGHTAVG_ROE', 0), 2)}%" if item.get('WEIGHTAVG_ROE') else '--'
        notice_date = item.get('NOTICE_DATE', '--')[:10] if item.get('NOTICE_DATE') else '--'
        markdown += f"| {report_date} | {basic_eps} | {income_str} | {income_sq_str} | {ystz} | {djdyshz} | {profit_str} | {profit_sq_str} | {jlrtbzcl} | {djdjlhz} | {bvps} | {roe} | {notice_date} |\n"
    return markdown


async def get_performance_forecast_markdown(stock_code, page_size=15, stock_name=None):
    """获取业绩预告明细并转换为markdown"""
    forecast_data = await get_performance_forecast(stock_code, page_size)
    if not forecast_data:
        return ""
    header = f"## <{stock_code} {stock_name}> - 业绩预告明细" if stock_name else "## 业绩预告明细"
    markdown = f"""{header}
| 截止日期 | 预测指标 | 业绩变动 | 预测数值(元) | 业绩变动同比 | 业绩变动环比 | 业绩变动原因 | 预告类型 | 上年同期值(元) | 公告日期 |
|----------|---------|---------|------------|------------|------------|------------|---------|--------------|----------|
"""
    for item in forecast_data:
        report_date = item.get('REPORT_DATE', '--')[:10] if item.get('REPORT_DATE') else '--'
        predict_finance = item.get('PREDICT_FINANCE', '--')
        predict_content = item.get('PREDICT_CONTENT', '--')
        amt_lower = item.get('PREDICT_AMT_LOWER')
        amt_upper = item.get('PREDICT_AMT_UPPER')
        if predict_finance == '每股收益':
            predict_value = f"{amt_lower}～{amt_upper}" if amt_lower and amt_upper else '--'
        else:
            predict_value = f"{convert_amount_unit(amt_lower)}～{convert_amount_unit(amt_upper)}" if amt_lower and amt_upper else '--'
        add_lower = item.get('ADD_AMP_LOWER')
        add_upper = item.get('ADD_AMP_UPPER')
        add_amp = f"{round(add_lower, 2)}%～{round(add_upper, 2)}%" if add_lower is not None and add_upper is not None else '-'
        ratio_lower = item.get('PREDICT_RATIO_LOWER')
        ratio_upper = item.get('PREDICT_RATIO_UPPER')
        predict_ratio = f"{round(ratio_lower, 2)}%～{round(ratio_upper, 2)}%" if ratio_lower is not None and ratio_upper is not None else '-'
        change_reason = item.get('CHANGE_REASON_EXPLAIN', '--')
        predict_type = item.get('PREDICT_TYPE', '--')
        preyear = item.get('PREYEAR_SAME_PERIOD')
        if predict_finance == '每股收益':
            preyear_str = str(preyear) if preyear else '--'
        else:
            preyear_str = convert_amount_unit(preyear) if preyear else '--'
        notice_date = item.get('NOTICE_DATE', '--')[:10] if item.get('NOTICE_DATE') else '--'
        markdown += f"| {report_date} | {predict_finance} | {predict_content} | {predict_value} | {add_amp} | {predict_ratio} | {change_reason} | {predict_type} | {preyear_str} | {notice_date} |\n"
    return markdown + "\n"
