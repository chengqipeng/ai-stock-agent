from service.eastmoney.stock_info.stock_base_info import get_stock_base_info_markdown
from service.eastmoney.stock_info.stock_financial_data import get_financial_report_markdown
from service.eastmoney.stock_info.stock_fund_flow import get_main_fund_flow_markdown
from service.eastmoney.stock_info.stock_history_flow import get_fund_flow_history_markdown
from service.eastmoney.stock_info.stock_holder_data import get_org_holder_markdown, get_shareholder_increase_markdown
from service.eastmoney.stock_info.stock_realtime import get_stock_realtime_markdown
from service.eastmoney.technical.stock_day_range_kline import generate_can_slim_50_200_summary
from service.llm.deepseek_client import DeepSeekClient
from service.llm.gemini_client import GeminiClient


def _get_analysis_json_format():
    return "\n结果只能输出json格式数据：{{'stock_code': '<股票代码>', 'stock_name': '<股票名称>', 'score': '<评分，按0-100分，评分需严格按照can slim的7个维度进行评估>', 'reason': '<如果分析缺少数据需要明确缺少的数据是什么，不超过50字，没有则不用返回>'}}\n"


def _get_analysis_header(stock_code: str, stock_name: str, mode: str = "full") -> str:
    """生成分析提示头部"""
    if mode == "score":
        return (
            f"# 使用欧奈尔CAN SLIM规则分析一下<{stock_code} {stock_name}>，基于模型的杯柄形态、突破点、基于技术形态等7个维度的指标判断是否属于优质股票\n"
            "# 1.分析必须使用当日交易信息、历史资金流向、实时成交分布、股票基本信息、业绩报表、高管减持、机构持仓变化、10/50/200日均线等数据必须严格使用已提供的【东方财富数据集】\n"
            "# 2.分析中依赖的相对强度RS、机构赞助维度、形态突破维度、杯柄形态等指标需要从历史交易记录中进行提炼\n"
            "# 3.以下是【东方财富数据集】：\n"
        )

    if mode == "analyse":
        return (
            f"# 1. 使用欧奈尔CAN SLIM规则分析一下<{stock_code} {stock_name}>，是否符合买入条件：基于模型的最终判断，稳健买入价格区间：基于技术形态（如杯柄形态、突破点）等进行深度行业调研给出的建议\n"
             "# 2.分析涉及当日交易信息、历史资金流向、股票基本信息、业绩报表、高管减持、机构持仓变化、、10/50/200日均线等数据必须严格使用已提供的【东方财富数据集】\n"
             "# 3.呈现形式：请以表格或分级标题的形式输出。\n"
             "# 4.以下是【东方财富数据集】：\n"
        )
    
    return (
        f"# 使用欧奈尔CAN SLIM规则分析一下<{stock_code} {stock_name}>，是否符合买入条件：基于模型的最终判断，稳健买入价格区间：基于技术形态（如杯柄形态、突破点）给出的建议\n"
        "# 1.分析涉及当日交易信息、历史资金流向、股票基本信息、业绩报表、高管减持、机构持仓变化、10/50/200日均线等数据必须严格使用已提供的【东方财富数据集】\n"
        #"# 2.参考【东方财富数据集】中A股市场业务相关性最高的上市公司的主力和机构的实时和历史买入卖出交易数据\n"
        f"# 2.同时针对股票<{stock_code} {stock_name}>执行深度行业调研，要求如下：\n"
        "## 2.1.**行业动态**： 检索近 6 个月内该行业的核心技术变革、重大投融资事件及市场格局变化。\n"
        "## 2.2.**销售预测**： 搜集权威机构（如券商研报、咨询公司）对该企业或所属细分赛道的未来3-5年营收增速、出货量或市场份额的预测数据。\n"
        "## 2.3.**政策环境**： 重点调研欧美市场对同类产品的准入门槛、关税政策、环保指令或技术性贸易壁垒（如反倾销、出口管制）。\n"
        "## 2.4.**数据规范**： 所有的核心事实、数据点必须紧跟 [来源链接/机构名称]。\n"
        "# 3.呈现形式：请以表格或分级标题的形式输出。\n"
        "# 4.必须在明细结论中备注数据来源。\n"
        "# 5.以下是【东方财富数据集】：\n"
    )


async def _build_stock_markdown(secid: str, stock_name: str, history_page_size=120, include_ma: bool = False) -> str:
    """构建股票数据markdown"""
    stock_code = secid.split('.')[-1]
    parts = []
    
    parts.append(await get_stock_realtime_markdown(secid, stock_code, stock_name))
    parts.append(await get_main_fund_flow_markdown(secid, stock_code, stock_name))
    parts.append(await get_stock_base_info_markdown(secid, stock_code, stock_name))
    parts.append(await get_fund_flow_history_markdown(secid, stock_code, stock_name, history_page_size))
    parts.append(await get_financial_report_markdown(stock_code, stock_name=stock_name))
    #parts.append(await get_performance_forecast_markdown(stock_code, stock_name=stock_name))
    parts.append(await get_org_holder_markdown(stock_code, stock_name=stock_name))
    
    increase_md = await get_shareholder_increase_markdown(stock_code, stock_name=stock_name)
    if increase_md:
        parts.append(increase_md)
    
    if include_ma:
        parts.append(await generate_can_slim_50_200_summary(secid, stock_code, stock_name))
    
    return "\n\n".join(parts)


async def get_stock_markdown(secid="0.002371", stock_name=None,):
    """获取股票数据并返回格式化的markdown"""
    try:
        stock_code = secid.split('.')[-1]
        header = _get_analysis_header(stock_code, stock_name, mode="full")
        body = await _build_stock_markdown(secid, stock_name, include_ma=True)
        return header + body
    except Exception as e:
        return f"# 错误\n\n获取股票数据失败: {str(e)}"


async def get_stock_markdown_for_score(secid="0.002371", stock_name=None, history_page_size=30):
    """获取股票数据并返回格式化的markdown（评分模式）"""
    try:
        stock_code = secid.split('.')[-1]
        header = _get_analysis_header(stock_code, stock_name, mode="score")
        body = await _build_stock_markdown(secid, stock_name, history_page_size, include_ma=True)
        json_result = _get_analysis_json_format()
        return header + body + json_result
    except Exception as e:
        return f"# 错误\n\n获取股票数据失败: {str(e)}"

async def get_stock_markdown_for_llm_analyse(secid="0.002371", stock_name=None, history_page_size=90):
    """获取股票数据并返回格式化的markdown（LLM分析模式）"""
    try:
        stock_code = secid.split('.')[-1]
        header = _get_analysis_header(stock_code, stock_name, mode="analyse")
        body = await _build_stock_markdown(secid, stock_name, include_ma=False)
        return header + body
    except Exception as e:
        return f"# 错误\n\n获取股票数据失败: {str(e)}"


async def get_stock_markdown_with_llm_result(secid="0.002371", stock_name=None, history_page_size=90, llm_type="deepseek"):
    """获取股票数据并调用LLM返回分析结果"""
    try:
        prompt = await get_stock_markdown_for_llm_analyse(secid, stock_name, history_page_size)
        
        if llm_type == "gemini":
            client = GeminiClient()
            response = await client.chat(
                messages=[{"role": "user", "content": prompt}],
                model="gemini-3-pro-all",
                temperature=0.7
            )
        else:
            client = DeepSeekClient()
            response = await client.chat(
                messages=[{"role": "user", "content": prompt}],
                model="deepseek-chat",
                temperature=0.7
            )
        
        return response.get("choices", [{}])[0].get("message", {}).get("content", "")
    except Exception as e:
        raise Exception(f"基本面数据分析失败: {str(e)}")
