import json
from datetime import datetime

from common.prompt.can_slim.I_Sponsorship_prompt import I_SPONSORSHIP_PROMPT_TEMPLATE
from common.utils.stock_info_utils import StockInfo
from service.eastmoney.stock_info.stock_holder_data import get_org_holder_count, get_org_holder_by_type, get_org_holder_json
from service.eastmoney.stock_info.stock_new_major_shareholders_detector import get_detect_new_major_shareholders
from service.eastmoney.stock_info.stock_northbound_funds import get_northbound_funds_cn
from service.eastmoney.stock_info.stock_top_ten_shareholders_circulation import get_top_ten_shareholders_circulation_by_dates
from service.llm.deepseek_client import DeepSeekClient


async def build_I_Sponsorship_prompt(stock_info: StockInfo) -> str:
    """构建I机构认同度分析提示词"""
    org_holder_count = await get_org_holder_count(stock_info)
    org_holder_json = await get_org_holder_json(stock_info)
    top_ten_name_shareholders_circulation_by_dates = await get_top_ten_shareholders_circulation_by_dates(
        stock_info, page_size=3, limit=3, fields=['rank', 'holder_name', 'report_date']
    )
    top_ten_hold_change_shareholders_circulation_by_dates = await get_top_ten_shareholders_circulation_by_dates(
        stock_info, page_size=3, limit=3, fields=['rank', 'holder_name', 'hold_change', 'report_date']
    )
    northbound_funds = await get_northbound_funds_cn(
        stock_info, ['TRADE_DATE', 'ADD_MARKET_CAP', 'ADD_SHARES_AMP', 'ADD_SHARES_AMP']
    )
    org_holder_by_type_she_bao = await get_org_holder_by_type(stock_info, '社保')
    detect_new_major_shareholders = await get_detect_new_major_shareholders(stock_info)
    
    return I_SPONSORSHIP_PROMPT_TEMPLATE.format(
        current_date=datetime.now().strftime('%Y-%m-%d'),
        stock_name=stock_info.stock_name,
        stock_code=stock_info.stock_code_normalize,
        org_holder_count_json=json.dumps(org_holder_count, ensure_ascii=False, indent=2),
        org_holder_json=json.dumps(org_holder_json, ensure_ascii=False, indent=2),
        top_ten_name_shareholders_json=json.dumps(top_ten_name_shareholders_circulation_by_dates, ensure_ascii=False, indent=2),
        northbound_funds_json=json.dumps(northbound_funds, ensure_ascii=False, indent=2),
        org_holder_she_bao_json=json.dumps(org_holder_by_type_she_bao, ensure_ascii=False, indent=2),
        top_ten_hold_change_json=json.dumps(top_ten_hold_change_shareholders_circulation_by_dates, ensure_ascii=False, indent=2),
        detect_new_major_shareholders_json=json.dumps(detect_new_major_shareholders, ensure_ascii=False, indent=2)
    )


async def execute_I_Sponsorship(stock_info: StockInfo, deep_thinking: bool = False) -> str:
    """
    执行I机构认同度分析
    
    Args:
        stock_info: 股票信息对象
        deep_thinking: 是否使用思考模式，默认False
    
    Returns:
        分析结果字符串
    """
    prompt = await build_I_Sponsorship_prompt(stock_info)

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
