import json
from datetime import datetime

from common.prompt.can_slim.S_Demand_prompt import S_DEMAND_PROMPT_TEMPLATE
from common.utils.stock_info_utils import StockInfo
from service.eastmoney.stock_info.stock_financial_main_with_total_share import get_equity_data_to_json
from service.eastmoney.stock_info.stock_history_flow import get_fund_flow_history_json, get_fund_flow_history_json_cn
from service.eastmoney.stock_info.stock_holder_data import get_org_holder_json
from service.eastmoney.stock_info.stock_lock_up_period import get_stock_lock_up_period_year_range
from service.eastmoney.stock_info.stock_realtime import get_stock_realtime_json
from service.eastmoney.stock_info.stock_repurchase import get_stock_repurchase_json
from service.eastmoney.stock_info.stock_top_ten_shareholders_circulation import get_top_ten_shareholders_circulation_by_dates
from service.eastmoney.technical.stock_day_range_kline import get_moving_averages_json
from service.llm.deepseek_client import DeepSeekClient

async def get_5_day_volume_ratio(stock_info: StockInfo):
    """计算5日量比"""
    moving_averages_result = await get_moving_averages_json(stock_info, ['close_5_sma'], 50)
    fund_flow_history_json = await get_fund_flow_history_json(stock_info, ['date', 'close_price', 'change_pct'])

    ma_dict = {item['date']: item['close_5_sma'] for item in moving_averages_result['data']}
    
    result = []
    for item in fund_flow_history_json['data'][:50]:
        date = item['date']
        close_price = item['close_price']
        change_pct = item['change_pct']
        
        if date in ma_dict and ma_dict[date]:
            close_5_sma = ma_dict[date]
            quantity_relative_ratio = close_price / close_5_sma
            result.append({
                'date': date,
                'close_price': close_price,
                'change_pct': change_pct,
                'quantity_relative_ratio': round(quantity_relative_ratio, 4)
            })
    
    return result

async def build_S_Demand_prompt(stock_info: StockInfo) -> str:
    """构建S供需分析提示词"""
    equity_data_with_total_shares_to_json = await get_equity_data_to_json(stock_info, ['END_DATE', 'TOTAL_SHARES'])
    equity_data_with_unlimited_shares_to_json = await get_equity_data_to_json(stock_info, ['END_DATE', 'UNLIMITED_SHARES'])
    top_ten_shareholders_circulation_by_dates = await get_top_ten_shareholders_circulation_by_dates(stock_info, page_size=3, limit=3)
    org_holder_json = await get_org_holder_json(stock_info)
    moving_averages_result = await get_moving_averages_json(stock_info, ['close_50_sma'], 50)
    stock_realtime_json = await get_stock_realtime_json(stock_info, ['stock_name', 'stock_code', 'volume'])
    fund_flow_history_json_cn = await get_fund_flow_history_json_cn(stock_info, ['date', 'change_hand', 'trading_volume', 'trading_amount'])
    stock_lock_up_period_year_range = await get_stock_lock_up_period_year_range(stock_info)
    stock_repurchase_json = await get_stock_repurchase_json(stock_info)
    five_day_volume_ratio = await get_5_day_volume_ratio(stock_info)
    
    return S_DEMAND_PROMPT_TEMPLATE.format(
        current_date=datetime.now().strftime('%Y-%m-%d'),
        stock_name=stock_info.stock_name,
        stock_code=stock_info.stock_code_normalize,
        total_shares_json=json.dumps(equity_data_with_total_shares_to_json, ensure_ascii=False, indent=2),
        unlimited_shares_json=json.dumps(equity_data_with_unlimited_shares_to_json, ensure_ascii=False, indent=2),
        top_ten_holders_json=json.dumps(top_ten_shareholders_circulation_by_dates[:10], ensure_ascii=False, indent=2),
        org_holder_json=json.dumps(org_holder_json[:2], ensure_ascii=False, indent=2),
        moving_averages_json=json.dumps(moving_averages_result['data'], ensure_ascii=False, indent=2),
        stock_realtime_json=json.dumps(stock_realtime_json, ensure_ascii=False, indent=2),
        five_day_volume_ratio_json=json.dumps(five_day_volume_ratio, ensure_ascii=False, indent=2),
        fund_flow_history_json_cn=json.dumps(fund_flow_history_json_cn, ensure_ascii=False, indent=2),
        stock_lock_up_period_json=json.dumps(stock_lock_up_period_year_range, ensure_ascii=False, indent=2),
        stock_repurchase_json=json.dumps(stock_repurchase_json, ensure_ascii=False, indent=2)
    )

async def execute_S_Demand(stock_info: StockInfo, deep_thinking: bool = False) -> str:
    """
    执行S供需分析
    
    Args:
        stock_info: 股票信息对象
        deep_thinking: 是否使用思考模式，默认False
    
    Returns:
        分析结果字符串
    """
    prompt = await build_S_Demand_prompt(stock_info)

    print(prompt)
    print("\n =============================== \n")
    
    model = "deepseek-reasoner" if deep_thinking else "deepseek-chat"
    client = DeepSeekClient()
    
    result = ""
    async for content in client.chat_stream(
        messages=[{"role": "user", "content": prompt}],
        model=model
    ):
        result += content

    return result