import json
from datetime import datetime

from common.prompt.can_slim.S_Demand_prompt import S_DEMAND_PROMPT_TEMPLATE
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.eastmoney.stock_info.stock_financial_main_with_total_share import get_equity_data_to_json
from service.eastmoney.stock_info.stock_history_flow import get_fund_flow_history_json, get_fund_flow_history_json_cn, \
    get_20day_volume_avg_cn, get_5day_volume_avg
from service.eastmoney.stock_info.stock_holder_data import get_org_holder_json
from service.eastmoney.stock_info.stock_lock_up_period import get_stock_lock_up_period_year_range
from service.eastmoney.stock_info.stock_realtime import get_stock_realtime_json
from service.eastmoney.stock_info.stock_repurchase import get_stock_repurchase_json
from service.eastmoney.stock_info.stock_top_ten_shareholders_circulation import get_top_ten_shareholders_circulation_by_dates
from service.llm.deepseek_client import DeepSeekClient

async def get_5_day_volume_ratio(stock_info: StockInfo):
    """计算5日量比"""
    day_5_volume_avg_result = await get_5day_volume_avg(stock_info, 200)
    fund_flow_history_json = await get_fund_flow_history_json(stock_info, ['date', 'close_price', 'trading_volume', 'change_pct'])

    ma_dict = {item['date']: item['volume_avg'] for item in day_5_volume_avg_result}
    
    result = []
    for item in fund_flow_history_json['data'][:50]:
        date = item['date']
        trading_volume = item['trading_volume']
        change_pct = item['change_pct']
        close_price = item['close_price']
        
        if date in ma_dict and ma_dict[date]:
            volume_20_avg = ma_dict[date]
            quantity_relative_ratio = trading_volume / volume_20_avg
            result.append({
                '日期': date,
                '交易量': trading_volume,
                '收盘价': close_price,
                '涨跌幅': change_pct,
                '量比（今日成交量/5日均量）': round(quantity_relative_ratio, 4)
            })
    
    return result

async def build_S_Demand_prompt(stock_info: StockInfo) -> str:
    """构建S供需分析提示词"""
    equity_data_with_total_shares_to_json = await get_equity_data_to_json(stock_info, ['END_DATE', 'TOTAL_SHARES'])
    equity_data_with_unlimited_shares_to_json = await get_equity_data_to_json(stock_info, ['END_DATE', 'UNLIMITED_SHARES'])
    top_ten_shareholders_circulation_by_dates = await get_top_ten_shareholders_circulation_by_dates(stock_info, page_size=3, limit=3)
    org_holder_json = await get_org_holder_json(stock_info)
    stock_realtime_json = await get_stock_realtime_json(stock_info, ['stock_name', 'stock_code', 'volume'])
    fund_flow_history_json_cn = await get_fund_flow_history_json_cn(stock_info, ['date', 'change_hand', 'trading_volume', 'trading_amount'])
    stock_lock_up_period_year_range = await get_stock_lock_up_period_year_range(stock_info)
    stock_repurchase_json = await get_stock_repurchase_json(stock_info)
    five_day_volume_ratio = await get_5_day_volume_ratio(stock_info)
    day_20_volume_avg_cn = await get_20day_volume_avg_cn(stock_info, 50)
    
    return S_DEMAND_PROMPT_TEMPLATE.format(
        current_date=datetime.now().strftime('%Y-%m-%d'),
        stock_name=stock_info.stock_name,
        stock_code=stock_info.stock_code_normalize,
        total_shares_json=json.dumps(equity_data_with_total_shares_to_json, ensure_ascii=False, indent=2),
        unlimited_shares_json=json.dumps(equity_data_with_unlimited_shares_to_json, ensure_ascii=False, indent=2),
        top_ten_holders_json=json.dumps(top_ten_shareholders_circulation_by_dates[:10], ensure_ascii=False, indent=2),
        org_holder_json=json.dumps(org_holder_json[:2], ensure_ascii=False, indent=2),
        day_20_volume_avg_cn=json.dumps(day_20_volume_avg_cn, ensure_ascii=False, indent=2),
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

if __name__ == "__main__":
    import asyncio
    
    async def main():
        stock_name = "北方华创"
        stock_info = get_stock_info_by_name(stock_name)
        result = await get_5_day_volume_ratio(stock_info)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    asyncio.run(main())