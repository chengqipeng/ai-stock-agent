import aiohttp
import asyncio
from common.utils.amount_utils import convert_amount_unit

# 最近期数配置
MAX_RECENT_PERIODS = 12

# 金额类字段
AMOUNT_FIELDS = ['TOTALOPERATEREVE', 'PARENTNETPROFIT', 'KCFJCXSYJLR', 'MLR', 'SINGLE_QUARTER_REVENUE']

# 财务指标定义
FINANCIAL_INDICATORS = [
    ('基本每股收益(元)', 'EPSJB'),
    ('扣非每股收益(元)', 'EPSKCJB'),
    ('稀释每股收益(元)', 'EPSXS'),
    ('每股净资产(元)', 'BPS'),
    ('每股公积金(元)', 'MGZBGJ'),
    ('每股未分配利润(元)', 'MGWFPLR'),
    ('每股经营现金流(元)', 'MGJYXJJE'),
    ('营业总收入(元)', 'TOTALOPERATEREVE'),
    ('单季度营业收入(元)', 'SINGLE_QUARTER_REVENUE'),
    ('毛利润(元)', 'MLR'),
    ('归母净利润(元)', 'PARENTNETPROFIT'),
    ('扣非净利润(元)', 'KCFJCXSYJLR'),
    ('营业总收入同比增长(%)', 'TOTALOPERATEREVETZ'),
    ('归属净利润同比增长(%)', 'PARENTNETPROFITTZ'),
    ('扣非净利润同比增长(%)', 'KCFJCXSYJLRTZ'),
    ('营业总收入环比增长(%)', 'YYZSRGDHBZC'),
    ('归属净利润环比增长(%)', 'NETPROFITRPHBZC'),
    ('扣非净利润环比增长(%)', 'KFJLRGDHBZC'),
    ('净资产收益率(加权)(%)', 'ROEJQ'),
    ('净资产收益率(扣非/加权)(%)', 'ROEKCJQ'),
    ('总资产收益率(加权)(%)', 'ZZCJLL'),
    ('毛利率(%)', 'XSMLL'),
    ('净利率(%)', 'XSJLL'),
    ('预收账款/营业收入', 'YSZKYYSR'),
    ('销售净现金流/营业收入', 'XSJXLYYSR'),
    ('经营现金流/营业收入', 'JYXJLYYSR'),
    ('实际税率(%)', 'TAXRATE'),
    ('流动比率', 'LD'),
    ('速动比率', 'SD'),
    ('现金流量比率', 'XJLLB'),
    ('资产负债率(%)', 'ZCFZL'),
    ('权益系数', 'QYCS'),
    ('产权比率', 'CQBL'),
    ('总资产周转天数(天)', 'ZZCZZTS'),
    ('存货周转天数(天)', 'CHZZTS'),
    ('应收账款周转天数(天)', 'YSZKZZTS'),
    ('总资产周转率(次)', 'TOAZZL'),
    ('存货周转率(次)', 'CHZZL'),
    ('应收账款周转率(次)', 'YSZKZZL'),
]


async def get_financial_data_to_json(secucode="002371.SZ", indicator_keys=None):
    """将财务数据转换为JSON格式"""
    data_list = await get_main_financial_data(secucode)
    if not data_list:
        return []
    
    recent_data = data_list[:MAX_RECENT_PERIODS]
    _calculate_single_quarter_revenue(recent_data)
    indicators = FINANCIAL_INDICATORS if indicator_keys is None else [(n, k) for n, k in FINANCIAL_INDICATORS if k in indicator_keys]
    
    result = []
    for d in recent_data:
        period_data = {"报告期": d.get('REPORT_DATE_NAME', '')}
        for name, key in indicators:
            val = d.get(key)
            if val is None:
                period_data[name] = None
            elif isinstance(val, (int, float)):
                if key in AMOUNT_FIELDS:
                    period_data[name] = convert_amount_unit(val)
                else:
                    period_data[name] = round(val, 4)
            else:
                period_data[name] = str(val)
        result.append(period_data)
    
    return result


async def get_financial_data_to_markdown(secucode="002371.SZ", indicator_keys=None):
    """将财务数据转换为Markdown格式"""
    data_list = await get_main_financial_data(secucode)
    if not data_list:
        return "暂无财务数据"
    
    recent_data = data_list[:MAX_RECENT_PERIODS]
    _calculate_single_quarter_revenue(recent_data)
    
    md = "## 主要财务指标\n\n"
    md += "| 指标 | " + " | ".join([d.get('REPORT_DATE_NAME', '') for d in recent_data]) + " |\n"
    md += "|" + "---|" * (len(recent_data) + 1) + "\n"
    
    indicators = FINANCIAL_INDICATORS if indicator_keys is None else [(n, k) for n, k in FINANCIAL_INDICATORS if k in indicator_keys]
    
    for name, key in indicators:
        row = f"| {name} | "
        values = []
        for d in recent_data:
            val = d.get(key)
            if val is None:
                values.append("-")
            elif isinstance(val, (int, float)):
                if key in AMOUNT_FIELDS:
                    values.append(convert_amount_unit(val))
                else:
                    values.append(f"{val:.4f}")
            else:
                values.append(str(val))
        row += " | ".join(values) + " |\n"
        md += row
    
    return md


def _calculate_single_quarter_revenue(data_list):
    """计算单季度营业收入"""
    for i, d in enumerate(data_list):
        report_date = d.get('REPORT_DATE_NAME', '')
        total_revenue = d.get('TOTALOPERATEREVE')
        
        if total_revenue is None:
            d['SINGLE_QUARTER_REVENUE'] = None
            continue
        
        year = report_date[:4]
        
        if '年报' in report_date:
            prev_q3 = next((data_list[j].get('TOTALOPERATEREVE') for j in range(i+1, len(data_list)) 
                           if '三季报' in data_list[j].get('REPORT_DATE_NAME', '') and 
                           data_list[j].get('REPORT_DATE_NAME', '')[:4] == year), None)
            d['SINGLE_QUARTER_REVENUE'] = total_revenue - prev_q3 if prev_q3 is not None else None
        elif '三季报' in report_date:
            prev_q2 = next((data_list[j].get('TOTALOPERATEREVE') for j in range(i+1, len(data_list)) 
                           if '中报' in data_list[j].get('REPORT_DATE_NAME', '') and 
                           data_list[j].get('REPORT_DATE_NAME', '')[:4] == year), None)
            d['SINGLE_QUARTER_REVENUE'] = total_revenue - prev_q2 if prev_q2 is not None else None
        elif '中报' in report_date:
            prev_q1 = next((data_list[j].get('TOTALOPERATEREVE') for j in range(i+1, len(data_list)) 
                           if '一季报' in data_list[j].get('REPORT_DATE_NAME', '') and 
                           data_list[j].get('REPORT_DATE_NAME', '')[:4] == year), None)
            d['SINGLE_QUARTER_REVENUE'] = total_revenue - prev_q1 if prev_q1 is not None else None
        elif '一季报' in report_date:
            d['SINGLE_QUARTER_REVENUE'] = total_revenue
        else:
            d['SINGLE_QUARTER_REVENUE'] = None


async def get_main_financial_data(secucode="002371.SZ", page_size=200, page_number=1):
    """获取主要财务指标数据"""
    url = "https://datacenter.eastmoney.com/securities/api/data/get"
    
    params = {
        "type": "RPT_F10_FINANCE_MAINFINADATA",
        "sty": "APP_F10_MAINFINADATA",
        "quoteColumns": "",
        "filter": f'(SECUCODE="{secucode}")',
        "p": str(page_number),
        "ps": str(page_size),
        "sr": "-1",
        "st": "REPORT_DATE",
        "source": "HSF10",
        "client": "PC",
        "v": "029085162688901034"
    }
    
    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Origin": "https://emweb.securities.eastmoney.com",
        "Referer": "https://emweb.securities.eastmoney.com/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=headers) as response:
            data = await response.json(content_type=None)
            if data.get("result") and data["result"].get("data"):
                return data["result"]["data"]
            else:
                raise Exception(f"未获取到证券代码 {secucode} 的主要财务指标数据")


if __name__ == "__main__":
    async def main():
        markdown = await get_financial_data_to_markdown("002371.SZ")
        print(markdown)

        json = await get_financial_data_to_json("002371.SZ")
        print(json)
    
    asyncio.run(main())
