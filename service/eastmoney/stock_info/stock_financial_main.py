import aiohttp
import asyncio
from common.utils.amount_utils import convert_amount_unit
from common.utils.cache_utils import get_cache_path, load_cache, save_cache

# 最近期数配置
MAX_RECENT_PERIODS = 13

# 金额类字段
AMOUNT_FIELDS = ['TOTALOPERATEREVE', 'PARENTNETPROFIT', 'KCFJCXSYJLR', 'MLR', 'SINGLE_QUARTER_REVENUE', 'SINGLE_QUARTER_KCFJCXSYJLR', 'SINGLE_QUARTER_PARENTNETPROFIT']

# 财务指标定义
FINANCIAL_INDICATORS = [
    ('报告日期', 'REPORT_DATE'),
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
    ('单季归母净利润(元)', 'SINGLE_QUARTER_PARENTNETPROFIT'),
    ('扣非净利润(元)', 'KCFJCXSYJLR'),
    ('单季扣非净利润(元)', 'SINGLE_QUARTER_KCFJCXSYJLR'),
    ('营业总收入同比增长(%)', 'TOTALOPERATEREVETZ'),
    ('归属净利润同比增长(%)', 'PARENTNETPROFITTZ'),
    ('扣非净利润同比增长(%)', 'KCFJCXSYJLRTZ'),
    ('单季归母净利润同比增长(%)', 'SINGLE_QUARTER_PARENTNETPROFITTZ'),
    ('单季扣非净利润同比增长(%)', 'SINGLE_QUARTER_KCFJCXSYJLRTZ'),
    ('营业总收入环比增长(%)', 'YYZSRGDHBZC'),
    ('归属净利润环比增长(%)', 'NETPROFITRPHBZC'),
    ('扣非净利润环比增长(%)', 'KFJLRGDHBZC'),
    ('净资产收益率(加权)(%)', 'ROEJQ'),
    ('净资产收益率(扣非/加权)(%)', 'ROEKCJQ'),
    ('净资产收益率_1(扣非/加权)(%)', 'ROEKCJQ_1'),
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
    _calculate_single_quarter_parentnetprofit(recent_data)
    _calculate_single_quarter_kcfjcxsyjlr(recent_data)
    _calculate_single_quarter_yoy_growth(recent_data)
    _calculate_epskcjb(recent_data)
    _calculate_roe_kcjq(recent_data)
    indicators = FINANCIAL_INDICATORS if indicator_keys is None else [(n, k) for n, k in FINANCIAL_INDICATORS if k in indicator_keys]
    
    result = []
    for d in recent_data:
        period_data = {
            "报告期": d.get('REPORT_DATE_NAME', ''),
            "报告日期": d.get('REPORT_DATE', '')
        }
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
                val_str = str(val)
                period_data[name] = val_str[:10] if val_str else None
        result.append(period_data)
    
    return result


async def get_financial_data_to_markdown(secucode="002371.SZ", indicator_keys=None):
    """将财务数据转换为Markdown格式"""
    data_list = await get_main_financial_data(secucode)
    if not data_list:
        return "暂无财务数据"
    
    recent_data = data_list[:MAX_RECENT_PERIODS]
    _calculate_single_quarter_revenue(recent_data)
    _calculate_single_quarter_parentnetprofit(recent_data)
    _calculate_single_quarter_kcfjcxsyjlr(recent_data)
    _calculate_single_quarter_yoy_growth(recent_data)
    _calculate_epskcjb(recent_data)
    _calculate_roe_kcjq(recent_data)
    
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


def _calculate_epskcjb(data_list):
    """计算扣非每股收益"""
    for d in data_list:
        basic_eps = d.get('EPSJB')
        net_profit = d.get('PARENTNETPROFIT')
        deducted_profit = d.get('KCFJCXSYJLR')
        
        if basic_eps is not None and net_profit is not None and deducted_profit is not None and net_profit != 0:
            calculated_epskcjb = round(basic_eps * (deducted_profit / net_profit), 4)
            if d.get('EPSKCJB') is None:
                d['EPSKCJB'] = calculated_epskcjb


def _calculate_roe_kcjq(data_list):
    """计算净资产收益率(扣非/加权)，用ROEKCJQ_1覆盖ROEKCJQ"""
    for d in data_list:
        roe_weighted = d.get('ROEJQ')
        koufei = d.get('KCFJCXSYJLR')
        guimu = d.get('PARENTNETPROFIT')
        
        # 计算ROEKCJQ_1（新字段）
        if roe_weighted is not None and koufei is not None and guimu is not None and guimu != 0:
            d['ROEKCJQ_1'] = round(roe_weighted * (koufei / guimu), 4)
            d['ROEKCJQ'] = d['ROEKCJQ_1']  # 用计算值覆盖原值
        else:
            d['ROEKCJQ_1'] = None


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


def _calculate_single_quarter_parentnetprofit(data_list):
    """计算单季归母净利润"""
    for i, d in enumerate(data_list):
        report_date = d.get('REPORT_DATE_NAME', '')
        parentnetprofit = d.get('PARENTNETPROFIT')
        
        if parentnetprofit is None:
            d['SINGLE_QUARTER_PARENTNETPROFIT'] = None
            continue
        
        year = report_date[:4]
        
        if '年报' in report_date:
            prev_q3 = next((data_list[j].get('PARENTNETPROFIT') for j in range(i+1, len(data_list)) 
                           if '三季报' in data_list[j].get('REPORT_DATE_NAME', '') and 
                           data_list[j].get('REPORT_DATE_NAME', '')[:4] == year), None)
            d['SINGLE_QUARTER_PARENTNETPROFIT'] = parentnetprofit - prev_q3 if prev_q3 is not None else None
        elif '三季报' in report_date:
            prev_q2 = next((data_list[j].get('PARENTNETPROFIT') for j in range(i+1, len(data_list)) 
                           if '中报' in data_list[j].get('REPORT_DATE_NAME', '') and 
                           data_list[j].get('REPORT_DATE_NAME', '')[:4] == year), None)
            d['SINGLE_QUARTER_PARENTNETPROFIT'] = parentnetprofit - prev_q2 if prev_q2 is not None else None
        elif '中报' in report_date:
            prev_q1 = next((data_list[j].get('PARENTNETPROFIT') for j in range(i+1, len(data_list)) 
                           if '一季报' in data_list[j].get('REPORT_DATE_NAME', '') and 
                           data_list[j].get('REPORT_DATE_NAME', '')[:4] == year), None)
            d['SINGLE_QUARTER_PARENTNETPROFIT'] = parentnetprofit - prev_q1 if prev_q1 is not None else None
        elif '一季报' in report_date:
            d['SINGLE_QUARTER_PARENTNETPROFIT'] = parentnetprofit
        else:
            d['SINGLE_QUARTER_PARENTNETPROFIT'] = None


def _calculate_single_quarter_kcfjcxsyjlr(data_list):
    """计算单季扣非净利润"""
    for i, d in enumerate(data_list):
        report_date = d.get('REPORT_DATE_NAME', '')
        kcfjcxsyjlr = d.get('KCFJCXSYJLR')
        
        if kcfjcxsyjlr is None:
            d['SINGLE_QUARTER_KCFJCXSYJLR'] = None
            continue
        
        year = report_date[:4]
        
        if '年报' in report_date:
            prev_q3 = next((data_list[j].get('KCFJCXSYJLR') for j in range(i+1, len(data_list)) 
                           if '三季报' in data_list[j].get('REPORT_DATE_NAME', '') and 
                           data_list[j].get('REPORT_DATE_NAME', '')[:4] == year), None)
            d['SINGLE_QUARTER_KCFJCXSYJLR'] = kcfjcxsyjlr - prev_q3 if prev_q3 is not None else None
        elif '三季报' in report_date:
            prev_q2 = next((data_list[j].get('KCFJCXSYJLR') for j in range(i+1, len(data_list)) 
                           if '中报' in data_list[j].get('REPORT_DATE_NAME', '') and 
                           data_list[j].get('REPORT_DATE_NAME', '')[:4] == year), None)
            d['SINGLE_QUARTER_KCFJCXSYJLR'] = kcfjcxsyjlr - prev_q2 if prev_q2 is not None else None
        elif '中报' in report_date:
            prev_q1 = next((data_list[j].get('KCFJCXSYJLR') for j in range(i+1, len(data_list)) 
                           if '一季报' in data_list[j].get('REPORT_DATE_NAME', '') and 
                           data_list[j].get('REPORT_DATE_NAME', '')[:4] == year), None)
            d['SINGLE_QUARTER_KCFJCXSYJLR'] = kcfjcxsyjlr - prev_q1 if prev_q1 is not None else None
        elif '一季报' in report_date:
            d['SINGLE_QUARTER_KCFJCXSYJLR'] = kcfjcxsyjlr
        else:
            d['SINGLE_QUARTER_KCFJCXSYJLR'] = None


def _calculate_single_quarter_yoy_growth(data_list):
    """计算单季度同比增长率"""
    for i, d in enumerate(data_list):
        report_date = d.get('REPORT_DATE_NAME', '')
        sq_parentnetprofit = d.get('SINGLE_QUARTER_PARENTNETPROFIT')
        sq_kcfjcxsyjlr = d.get('SINGLE_QUARTER_KCFJCXSYJLR')
        
        year = report_date[:4]
        prev_year = str(int(year) - 1)
        
        # 查找去年同期数据
        for j in range(i+1, len(data_list)):
            prev_report = data_list[j].get('REPORT_DATE_NAME', '')
            if prev_report[:4] == prev_year and prev_report[4:] == report_date[4:]:
                # 计算单季归母净利润同比增长
                if sq_parentnetprofit is not None:
                    prev_sq_parentnetprofit = data_list[j].get('SINGLE_QUARTER_PARENTNETPROFIT')
                    if prev_sq_parentnetprofit is not None and prev_sq_parentnetprofit != 0:
                        d['SINGLE_QUARTER_PARENTNETPROFITTZ'] = round((sq_parentnetprofit - prev_sq_parentnetprofit) / abs(prev_sq_parentnetprofit) * 100, 4)
                    else:
                        d['SINGLE_QUARTER_PARENTNETPROFITTZ'] = None
                else:
                    d['SINGLE_QUARTER_PARENTNETPROFITTZ'] = None
                
                # 计算单季扣非净利润同比增长
                if sq_kcfjcxsyjlr is not None:
                    prev_sq_kcfjcxsyjlr = data_list[j].get('SINGLE_QUARTER_KCFJCXSYJLR')
                    if prev_sq_kcfjcxsyjlr is not None and prev_sq_kcfjcxsyjlr != 0:
                        d['SINGLE_QUARTER_KCFJCXSYJLRTZ'] = round((sq_kcfjcxsyjlr - prev_sq_kcfjcxsyjlr) / abs(prev_sq_kcfjcxsyjlr) * 100, 4)
                    else:
                        d['SINGLE_QUARTER_KCFJCXSYJLRTZ'] = None
                else:
                    d['SINGLE_QUARTER_KCFJCXSYJLRTZ'] = None
                break
        else:
            d['SINGLE_QUARTER_PARENTNETPROFITTZ'] = None
            d['SINGLE_QUARTER_KCFJCXSYJLRTZ'] = None


async def get_main_financial_data(secucode="002371.SZ", page_size=200, page_number=1):
    """获取主要财务指标数据"""
    stock_code = secucode.split('.')[0]
    cache_path = get_cache_path("financial_main", stock_code)
    
    # 检查缓存
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    # 获取数据
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
                result = data["result"]["data"]
                # 保存缓存
                save_cache(cache_path, result)
                return result
            else:
                raise Exception(f"未获取到证券代码 {secucode} 的主要财务指标数据")


if __name__ == "__main__":
    async def main():
        markdown = await get_financial_data_to_markdown("002371.SZ")
        print(markdown)

        json = await get_financial_data_to_json("002371.SZ")
        print(json)
    
    asyncio.run(main())
