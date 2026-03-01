import logging
from typing import Dict, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

from common.prompt.can_slim.C_Quarterly_Earnings_prompt import C_QUARTERLY_EARNINGS_PROMPT_TEMPLATE, C_FAST_REPORT_SECTION
from common.constants.can_slim_final_outputs import C_FINAL_OUTPUT
from common.utils.stock_info_utils import StockInfo
from service.eastmoney.forecast.stock_institution_forecast_list import get_institution_forecast_future_to_json, get_institution_forecast_historical_to_json
from service.eastmoney.forecast.stock_institution_forecast_summary import get_institution_forecast_summary_future_json, get_institution_forecast_summary_historical_json
from service.eastmoney.stock_info.stock_financial_main import get_financial_data_to_json
from service.eastmoney.stock_info.stock_financial_data import get_financial_fast_report_cn
from service.can_slim.base_can_slim_service import BaseCanSlimService

class CQuarterlyEarningsService(BaseCanSlimService):
    """C季度盈利分析服务"""
    
    async def collect_data(self) -> Dict[str, Any]:
        data = {
            'financial_revenue': await get_financial_data_to_json(self.stock_info, indicator_keys=['REPORT_DATE', 'TOTALOPERATEREVETZ', 'SINGLE_QUARTER_REVENUE', 'TOTALOPERATEREVE', 'SINGLE_QUARTER_REVENUETZ']),
            'financial_profit': await get_financial_data_to_json(self.stock_info, indicator_keys=['REPORT_DATE', 'SINGLE_QUARTER_PARENTNETPROFITTZ', 'SINGLE_QUARTER_KCFJCXSYJLRTZ']),
            'financial_eps': await get_financial_data_to_json(self.stock_info, indicator_keys=['REPORT_DATE', 'EPSJB']),
            'historical_forecast': await get_institution_forecast_historical_to_json(stock_info=self.stock_info),
            'future_forecast': await get_institution_forecast_future_to_json(stock_info=self.stock_info),
            'historical_forecast_summary': await get_institution_forecast_summary_historical_json(stock_info=self.stock_info),
            'future_forecast_summary': await get_institution_forecast_summary_future_json(stock_info=self.stock_info)
        }
        # 获取业绩快报，过滤最近半年数据，无数据则不加入
        data['fast_report'] = await self._fetch_recent_fast_report()
        return data

    async def _fetch_recent_fast_report(self):
        """获取业绩快报数据，仅保留公告日期在最近半个月内的记录，无数据返回None"""
        try:
            raw = await get_financial_fast_report_cn(self.stock_info)
            cutoff = datetime.now() - timedelta(days=15)
            recent = []
            for item in raw:
                notice_date_str = item.get('公告日期', '')
                if not notice_date_str:
                    continue
                try:
                    notice_date = datetime.strptime(notice_date_str[:10], '%Y-%m-%d')
                    if notice_date >= cutoff:
                        recent.append(item)
                except (ValueError, TypeError) as e:
                    logger.debug("业绩快报日期解析失败: notice_date_str=%s, %s", notice_date_str, e)
                    continue
            return recent if recent else None
        except Exception as e:
            logger.warning("_fetch_recent_fast_report 获取业绩快报失败 [%s]: %s", self.stock_info.stock_name, e)
            return None
    
    def get_prompt_template(self) -> str:
        return C_QUARTERLY_EARNINGS_PROMPT_TEMPLATE
    
    def get_prompt_params(self) -> Dict[str, Any]:
        # 有最近半年业绩快报数据时才加入该section
        fast_report = self.data_cache.get('fast_report')
        if fast_report:
            fast_report_section = C_FAST_REPORT_SECTION.format(
                fast_report_json=self.to_json(fast_report)
            )
        else:
            fast_report_section = ''

        return {
            'financial_revenue_json': self.to_json(self.data_cache['financial_revenue']),
            'financial_profit_json': self.to_json(self.data_cache['financial_profit']),
            'financial_eps_json': self.to_json(self.data_cache['financial_eps']),
            'historical_forecast_json': self.to_json(self.data_cache['historical_forecast']),
            'future_forecast_json': self.to_json(self.data_cache['future_forecast']),
            'historical_forecast_summary': self.data_cache['historical_forecast_summary'],
            'future_forecast_summary': self.data_cache['future_forecast_summary'],
            'fast_report_section': fast_report_section
        }
    
    def get_final_output_instruction(self) -> str:
        return C_FINAL_OUTPUT


async def execute_C_Quarterly_Earnings(stock_info: StockInfo, deep_thinking: bool = False) -> str:
    """执行C季度盈利分析"""
    service = CQuarterlyEarningsService(stock_info)
    return await service.execute(deep_thinking)
