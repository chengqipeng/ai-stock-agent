from .stock_base_info import get_stock_base_info_markdown
from .stock_realtime import get_stock_realtime_markdown
from .stock_fund_history_flow import (
    get_main_fund_flow_markdown,
    get_fund_flow_history_markdown
)
from .stock_financial_data import (
    get_financial_report_markdown,
    get_performance_forecast_markdown
)
from .stock_holder_data import (
    get_shareholder_increase_markdown,
    get_org_holder_markdown
)


async def get_stock_markdown(secid="0.002371", stock_name=None, history_page_size=60):
    """获取股票数据并返回格式化的markdown"""
    try:
        stock_code = secid.split('.')[-1]
        markdown = (""
                    f"# 使用欧奈尔CAN SLIM规则分析一下<{stock_code} {stock_name}>，是否符合买入条件：基于模型的最终判断，稳健买入价格区间：基于技术形态（如杯柄形态、突破点）给出的建议\n"
                    "# 1.分析涉及当日交易信息、当日资金流向、实时成交分布、股票基本信息、业绩报表、业绩预告、高管减持、机构持仓变化等数据必须严格使用已提供的【东方财富数据集】\n"
                    "# 2.参考【东方财富数据集】中A股市场业务相关性最高的上市公司的主力和机构的实时和历史买入卖出交易数据\n"
                    f"# 3.同时针对股票<{stock_code} {stock_name}>执行深度行业调研，要求如下：\n"
                    "## 3.1.**行业动态**： 检索近 6 个月内该行业的核心技术变革、重大投融资事件及市场格局变化。\n"
                    "## 3.2.**销售预测**： 搜集权威机构（如券商研报、咨询公司）对该企业或所属细分赛道的未来3-5年营收增速、出货量或市场份额的预测数据。\n"
                    "## 3.3.**政策环境**： 重点调研欧美市场对同类产品的准入门槛、关税政策、环保指令或技术性贸易壁垒（如反倾销、出口管制）。\n"
                    "## 3.4.**数据规范**： 所有的核心事实、数据点必须紧跟 [来源链接/机构名称]。\n"
                    "# 4.呈现形式：请以表格或分级标题的形式输出。\n"
                    "# 5.必须在明细结论中备注数据来源。\n"
                    "# 6.以下是【东方财富数据集】：\n")
        
        markdown += f"## <{stock_code} {stock_name}> - 当日交易信息\n" + (await get_stock_realtime_markdown(secid)).replace("## 当日交易信息", "") + "\n\n"
        markdown += f"## <{stock_code} {stock_name}> - 当日资金流向\n" + (await get_main_fund_flow_markdown(secid)).replace("## 当日资金流向", "") + "\n\n"
        #markdown += f"## <{stock_code} {stock_name}> - 实时成交分布\n" + (await get_trade_distribution_markdown(secid)).replace("## 实时成交分布", "") + "\n\n"

        try:
            markdown += f"## <{stock_code} {stock_name}> - 股票基本信息\n" + (await get_stock_base_info_markdown(secid)).replace("## 股票基本信息", "") + "\n"
        except Exception as e:
            markdown += f"## <{stock_code} {stock_name}> - 股票基本信息错误\n\n获取失败: {str(e)}\n"

        try:
            markdown += f"## <{stock_code} {stock_name}> - 历史资金流向\n" + (await get_fund_flow_history_markdown(secid, history_page_size)).replace("## 历史资金流向", "") + "\n"
        except Exception as e:
            markdown += f"## <{stock_code} {stock_name}> - 历史资金流向错误\n\n获取失败: {str(e)}\n"

        try:
            markdown += f"## <{stock_code} {stock_name}> - 业绩报表明细\n" + (await get_financial_report_markdown(stock_code)).replace("## 业绩报表明细", "") + "\n"
        except Exception as e:
            markdown += f"## <{stock_code} {stock_name}> - 业绩报表明细错误\n\n获取失败: {str(e)}\n"

        try:
            markdown += f"## <{stock_code} {stock_name}> - 业绩预告明细\n" + (await get_performance_forecast_markdown(stock_code)).replace("## 业绩预告明细", "") + "\n"
        except Exception as e:
            markdown += f"## <{stock_code} {stock_name}> - 业绩预告明细错误\n\n获取失败: {str(e)}\n"

        try:
            org_md = await get_org_holder_markdown(stock_code)
            markdown += org_md.replace("##", f"## <{stock_code} {stock_name}> -") + "\n"
        except Exception as e:
            markdown += f"## <{stock_code} {stock_name}> - 机构持仓明细错误\n\n获取失败: {str(e)}\n\n"

        try:
            increase_markdown = await get_shareholder_increase_markdown(stock_code)
            if increase_markdown:
                markdown += increase_markdown.replace("##", f"## <{stock_code} {stock_name}> -")
        except Exception as e:
            markdown += f"## <{stock_code} {stock_name}> - 股东增减持明细\n: {str(e)}"

        return markdown
    except Exception as e:
        return f"# 错误\n\n获取股票数据失败: {str(e)}"


async def get_stock_markdown_for_score(secid="0.002371", stock_name=None, history_page_size=30):
    """获取股票数据并返回格式化的markdown"""
    try:
        stock_code = secid.split('.')[-1]
        markdown = (""
                    f"# 使用欧奈尔CAN SLIM规则分析一下<{stock_code} {stock_name}>，基于模型的最终判断、（如杯柄形态、突破点）判断是否属于优质股票，只能输出json数据，json格式：\n"
                    "{'stock_code': '<股票代码>', 'stock_name': '<股票名称>', 'score': '<到评分，按0-100分>', 'is_good': '0/1  0 差 1 优质 大于65分属于优质'}"
                    "# 1.分析涉及当日交易信息、当日资金流向、实时成交分布、股票基本信息、业绩报表、业绩预告、高管减持、机构持仓变化等数据必须严格使用已提供的【东方财富数据集】\n"
                    "# 2.以下是【东方财富数据集】：\n")

        markdown += f"## <{stock_code} {stock_name}> - 当日交易信息\n" + (
            await get_stock_realtime_markdown(secid)).replace("## 当日交易信息", "") + "\n\n"
        markdown += f"## <{stock_code} {stock_name}> - 当日资金流向\n" + (
            await get_main_fund_flow_markdown(secid)).replace("## 当日资金流向", "") + "\n\n"
        # markdown += f"## <{stock_code} {stock_name}> - 实时成交分布\n" + (
        #     await get_trade_distribution_markdown(secid)).replace("## 实时成交分布", "") + "\n\n"

        try:
            markdown += f"## <{stock_code} {stock_name}> - 股票基本信息\n" + (
                await get_stock_base_info_markdown(secid)).replace("## 股票基本信息", "") + "\n"
        except Exception as e:
            markdown += f"## <{stock_code} {stock_name}> - 股票基本信息错误\n\n获取失败: {str(e)}\n"

        try:
            markdown += f"## <{stock_code} {stock_name}> - 历史资金流向\n" + (
                await get_fund_flow_history_markdown(secid, history_page_size)).replace("## 历史资金流向", "") + "\n"
        except Exception as e:
            markdown += f"## <{stock_code} {stock_name}> - 历史资金流向错误\n\n获取失败: {str(e)}\n"

        try:
            markdown += f"## <{stock_code} {stock_name}> - 业绩报表明细\n" + (
                await get_financial_report_markdown(stock_code)).replace("## 业绩报表明细", "") + "\n"
        except Exception as e:
            markdown += f"## <{stock_code} {stock_name}> - 业绩报表明细错误\n\n获取失败: {str(e)}\n"

        try:
            markdown += f"## <{stock_code} {stock_name}> - 业绩预告明细\n" + (
                await get_performance_forecast_markdown(stock_code)).replace("## 业绩预告明细", "") + "\n"
        except Exception as e:
            markdown += f"## <{stock_code} {stock_name}> - 业绩预告明细错误\n\n获取失败: {str(e)}\n"

        try:
            org_md = await get_org_holder_markdown(stock_code)
            markdown += org_md.replace("##", f"## <{stock_code} {stock_name}> -") + "\n"
        except Exception as e:
            markdown += f"## <{stock_code} {stock_name}> - 机构持仓明细错误\n\n获取失败: {str(e)}\n\n"

        try:
            increase_markdown = await get_shareholder_increase_markdown(stock_code)
            if increase_markdown:
                markdown += increase_markdown.replace("##", f"## <{stock_code} {stock_name}> -")
        except Exception as e:
            markdown += f"## <{stock_code} {stock_name}> - 股东增减持明细\n: {str(e)}"

        return markdown
    except Exception as e:
        return f"# 错误\n\n获取股票数据失败: {str(e)}"
