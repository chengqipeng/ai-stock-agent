from common.utils.amount_utils import normalize_stock_code
from .stock_detail import get_stock_base_info_markdown
from .stock_realtime import get_stock_realtime_markdown
from .fund_flow import (
    get_main_fund_flow_markdown,
    get_trade_distribution_markdown,
    get_fund_flow_history_markdown
)
from .industry_data import get_industry_market_data

async def get_similar_companies_data(stock_name, stock_code, similar_company_num=5):
    """获取相似公司的资金流向数据"""
    industry_data = await get_industry_market_data(stock_code, similar_company_num)
    similar_prompt = f"\n**以下是A股市场中和<{stock_code} {stock_name}>业务相关性最高的{similar_company_num}家上市公司的资金流向数据**\n"
    for company in industry_data:
        code = company.get('SECUCODE')
        name = company.get('CORRE_SECURITY_NAME')
        similar_secid = normalize_stock_code(f"{code}")
        try:
            fund_flow_md = await get_main_fund_flow_markdown(similar_secid)
            fund_flow_md = f"## <{code} {name}>：\n#" + fund_flow_md + "\n\n"
            fund_flow_md += f"## <{code} {name}>: \n#" + (await get_trade_distribution_markdown(similar_secid))
            similar_prompt += fund_flow_md + "\n\n"
        except Exception as e:
            print(f"  <{code} {name}> 主力当日资金流向: 获取失败 - {str(e)}\n")
        try:
            history_md = await get_fund_flow_history_markdown(similar_secid, 12)
            history_md = f"## <{code} {name}>：\n#" + history_md
            similar_prompt += history_md + "\n\n"
        except Exception as e:
            print(f"  <{code} {name}> 历史资金流向: 获取失败 - {str(e)}")
    return similar_prompt
