from typing import Dict, Any

from common.prompt.can_slim.A_Earnings_Increases_prompt import A_EARNINGS_INCREASES_PROMPT_TEMPLATE
from common.constants.can_slim_final_outputs import A_FINAL_OUTPUT
from common.utils.stock_info_utils import StockInfo
from service.eastmoney.stock_info.stock_financial_main import get_financial_data_to_json
from service.can_slim.base_can_slim_service import BaseCanSlimService


def calculate_cagr(eps_compare_data):
    """通过eps_compare_data计算复合增速(CAGR)
    使用第1条数据的EPSJB除以3年前EPS得到的值减1
    返回: (cagr值, 描述信息)
    """
    if not eps_compare_data or len(eps_compare_data) < 4:
        return None, None
    
    latest_data = eps_compare_data[0]
    three_years_ago_data = eps_compare_data[12]
    
    latest_eps = latest_data.get('基本每股收益(元)')
    three_years_ago_eps = three_years_ago_data.get('基本每股收益(元)')
    
    if not latest_eps or not three_years_ago_eps or three_years_ago_eps <= 0:
        return None, None
    
    cagr_value = round(((latest_eps / three_years_ago_eps) ** (1/3) - 1) * 100, 4)
    
    latest_date = latest_data.get('报告日期', '')
    three_years_ago_date = three_years_ago_data.get('报告日期', '')
    description = f"CAGR为{three_years_ago_date}到{latest_date}的EPS数据，公式：((最新年度EPS/三年前年度EPS)^(1/3) - 1) * 100，计算值为{cagr_value:.4f}%"
    
    return cagr_value, description


def calculate_reality_check(raw_data):
    """计算现金流验证数据：每股经营现金流/每股收益
    每年只取年度（四季度）数据，如果没有四季度则取最近一季度的数据
    返回: 处理后的数据列表
    """
    if not raw_data:
        return []
    
    yearly_data = {}
    for item in raw_data:
        report_period = item.get('报告期', '')
        report_date = item.get('报告日期', '')
        mgjyxjje = item.get('每股经营现金流(元)')
        epsjb = item.get('基本每股收益(元)')
        
        if not report_period or not report_date:
            continue
        
        year = report_period[:4]
        
        # 判断是否为年报或四季度
        is_annual = '年报' in report_period or '12-31' in report_date
        
        if year not in yearly_data:
            yearly_data[year] = {'data': item, 'is_annual': is_annual}
        elif is_annual and not yearly_data[year]['is_annual']:
            # 如果找到年报，替换非年报数据
            yearly_data[year] = {'data': item, 'is_annual': is_annual}
    
    # 计算比率并构建结果
    result = []
    for year in sorted(yearly_data.keys(), reverse=True):
        item = yearly_data[year]['data']
        mgjyxjje = item.get('每股经营现金流(元)')
        epsjb = item.get('基本每股收益(元)')
        
        ratio = None
        if mgjyxjje is not None and epsjb is not None and epsjb != 0:
            ratio = round(mgjyxjje / epsjb, 4)
        
        result.append({
            '报告期': item.get('报告期', ''),
            '报告日期': item.get('报告日期', ''),
            '每股经营现金流(元)': mgjyxjje,
            '基本每股收益(元)': epsjb,
            '现金流/收益比': ratio
        })
    
    return result

class AEarningsIncreasesService(BaseCanSlimService):
    """A年度盈利增长分析服务"""
    
    async def collect_data(self) -> Dict[str, Any]:
        return {
            'eps_kc': await get_financial_data_to_json(self.stock_info, indicator_keys=['REPORT_DATE', 'EPSKCJB']),
            'roe': await get_financial_data_to_json(self.stock_info, indicator_keys=['REPORT_DATE', 'ROEKCJQ']),
            'eps_compare': await get_financial_data_to_json(self.stock_info, indicator_keys=['REPORT_DATE', 'EPSJB']),
            'cash_flow': await get_financial_data_to_json(self.stock_info, indicator_keys=['REPORT_DATE', 'MGJYXJJE']),
            'profit_growth': await get_financial_data_to_json(self.stock_info, indicator_keys=['REPORT_DATE', 'KCFJCXSYJLRTZ']),
            'raw_reality_check': await get_financial_data_to_json(self.stock_info, indicator_keys=['REPORT_DATE', 'MGJYXJJE', 'EPSJB'])
        }
    
    async def process_data(self) -> None:
        self.data_cache['reality_check'] = calculate_reality_check(self.data_cache['raw_reality_check'])
        self.data_cache['cagr_value'], self.data_cache['cagr_description'] = calculate_cagr(self.data_cache['eps_compare'])
        roe_data = self.data_cache.get('roe') or []
        latest_roe = next((r for r in roe_data if r.get('净资产收益率(扣非/加权)(%)') is not None), {})
        self.data_cache['latest_roe_year'] = (latest_roe.get('报告日期', '') or '')[:4]
        self.data_cache['latest_roe_value'] = latest_roe.get('净资产收益率(扣非/加权)(%)')
        reality_check = self.data_cache.get('reality_check') or []
        latest_cf = reality_check[0] if reality_check else {}
        self.data_cache['latest_cashflow_year'] = (latest_cf.get('报告日期', '') or '')[:4]
        self.data_cache['latest_cashflow_ratio'] = latest_cf.get('现金流/收益比')
    
    def get_prompt_template(self) -> str:
        return A_EARNINGS_INCREASES_PROMPT_TEMPLATE
    
    def get_prompt_params(self) -> Dict[str, Any]:
        return {
            'eps_kc_data_json': self.to_json(self.data_cache['eps_kc']),
            'roe_data_json': self.to_json(self.data_cache['roe']),
            'cash_flow_data_json': self.to_json(self.data_cache['cash_flow']),
            'profit_growth_data_json': self.to_json(self.data_cache['profit_growth']),
            'cagr_value': self.data_cache['cagr_value'],
            'cagr_description': self.data_cache['cagr_description'],
            'reality_check_data_json': self.to_json(self.data_cache['reality_check']),
            'latest_roe_year': self.data_cache['latest_roe_year'],
            'latest_roe_value': self.data_cache['latest_roe_value'],
            'latest_cashflow_year': self.data_cache['latest_cashflow_year'],
            'latest_cashflow_ratio': self.data_cache['latest_cashflow_ratio']
        }
    
    def get_final_output_instruction(self) -> str:
        return A_FINAL_OUTPUT


async def execute_A_Earnings_Increases(stock_info: StockInfo, deep_thinking: bool = False) -> str:
    """执行A年度盈利增长分析"""
    service = AEarningsIncreasesService(stock_info)
    return await service.execute(deep_thinking)
