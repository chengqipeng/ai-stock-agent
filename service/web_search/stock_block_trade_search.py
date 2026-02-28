"""大宗交易数据搜索模块。

通过百度搜索获取个股近期大宗交易信息，爬取网页内容后
通过大模型提取结构化的大宗交易记录。
"""

import asyncio
import json
import logging
import time
from datetime import datetime

from common.utils.llm_utils import parse_llm_json
from common.utils.stock_info_utils import StockInfo
from service.llm.deepseek_client import DeepSeekClient
from service.web_search.baidu_search import baidu_search
from service.web_search.stock_news_search import _clean_text, _dedup_by_title

logger = logging.getLogger(__name__)

# ── 缓存（当日有效） ──
_block_trade_cache: dict[str, dict] = {}
_CACHE_TTL = 4 * 60 * 60  # 4小时


def _build_extract_prompt(stock_info: StockInfo, search_items: list[dict]) -> str:
    """构建大模型提取大宗交易数据的提示词"""
    now_str = datetime.now().strftime('%Y-%m-%d')
    items_json = json.dumps(search_items, ensure_ascii=False)
    return (
        "# Role\n"
        "你是一位专业的证券数据分析师，擅长从非结构化文本中精确提取大宗交易数据。\n\n"
        f"当前日期：{now_str}\n"
        f"目标公司：{stock_info.stock_name}（{stock_info.stock_code_normalize}）\n\n"
        "# Task\n"
        "从以下搜索结果中提取与目标公司**直接相关**的大宗交易记录。\n\n"
        "# 提取规则\n"
        "1. 只提取明确标注为「大宗交易」的记录，不要混淆龙虎榜、融资融券等其他数据\n"
        "2. 每条记录尽量提取以下字段（缺失字段填null）：\n"
        "   - trade_date: 交易日期（YYYY-MM-DD格式）\n"
        "   - price: 成交价（元）\n"
        "   - volume: 成交量（万股）\n"
        "   - amount: 成交额（万元）\n"
        "   - premium_rate: 溢价率（%，正数为溢价，负数为折价）\n"
        "   - buyer: 买方营业部\n"
        "   - seller: 卖方营业部\n"
        "   - close_price: 当日收盘价（元）\n"
        "3. 只保留近30天内的记录\n"
        "4. 如果搜索结果中没有任何大宗交易数据，返回空数组\n\n"
        "# 搜索结果\n"
        f"{items_json}\n\n"
        "# Output\n"
        "只返回JSON数组，每个元素为一条大宗交易记录。禁止输出任何解释。\n"
        '示例：[{"trade_date":"2026-02-20","price":68.5,"volume":50,"amount":3425,'
        '"premium_rate":-2.15,"buyer":"中信证券北京总部","seller":"机构专用","close_price":69.8}]\n'
        "无数据时返回：[]\n"
    )


async def search_block_trade(stock_info: StockInfo, days: int = 30) -> list[dict]:
    """搜索并提取个股近期大宗交易数据。

    流程：缓存检查 -> 百度搜索 -> 文本清洗 -> 去重 -> 大模型提取结构化数据
    """
    cache_key = f"block_trade_{stock_info.stock_code_normalize}_{days}"
    now = time.time()

    cached = _block_trade_cache.get(cache_key)
    if cached and (now - cached['ts']) < _CACHE_TTL:
        logger.info(f"命中大宗交易缓存 [{stock_info.stock_name}]")
        return cached['data']

    query = f"{stock_info.stock_name} 大宗交易"
    try:
        results = await baidu_search(query=query, days=days, top_k=10)
        if not results:
            _block_trade_cache[cache_key] = {'data': [], 'ts': now}
            return []

        # 清洗 + 构建搜索条目
        search_items = []
        for i, item in enumerate(results, 1):
            title = _clean_text(item.get('title') or '')
            content = _clean_text(item.get('content') or '')
            if not title:
                continue
            search_items.append({
                'id': i,
                'title': title,
                'date': item.get('date', ''),
                'content': content[:800],
            })

        if not search_items:
            _block_trade_cache[cache_key] = {'data': [], 'ts': now}
            return []

        search_items = _dedup_by_title(search_items)

        # 大模型提取结构化数据
        client = DeepSeekClient()
        prompt = _build_extract_prompt(stock_info, search_items)
        response = await client.chat(
            messages=[{"role": "user", "content": prompt}],
            model="deepseek-chat",
            temperature=0.1,
        )
        resp_content = response['choices'][0]['message']['content']
        records = parse_llm_json(resp_content)

        if not isinstance(records, list):
            records = []

        _block_trade_cache[cache_key] = {'data': records, 'ts': now}
        return records

    except Exception as e:
        logger.warning(f"搜索大宗交易失败 [{stock_info.stock_name}]: {e}")
        if cached:
            return cached['data']
        return []


def _assess_block_trade_next_day_impact(records: list[dict], next_trading_day: str) -> str:
    """根据大宗交易日期判断对下一个交易日的影响程度。

    大宗交易发生在A股收盘后（15:00-15:30），属于盘后交易。
    - 交易日期 == 上一个交易日 → 最新盘后大宗交易，对下一个交易日有直接影响
    - 交易日期在近3个交易日内 → 近期大宗交易，对下一个交易日有间接影响
    - 交易日期更早 → 历史大宗交易，对下一个交易日影响较弱
    """
    if not records or not next_trading_day:
        return '无法判断'

    from datetime import datetime, timedelta
    import chinese_calendar

    try:
        next_td = datetime.strptime(next_trading_day, '%Y-%m-%d').date()
        # 找到上一个交易日（即next_trading_day的前一个交易日）
        prev_td = next_td - timedelta(days=1)
        while prev_td.weekday() >= 5 or chinese_calendar.is_holiday(prev_td):
            prev_td -= timedelta(days=1)
        prev_td_str = prev_td.strftime('%Y-%m-%d')

        latest_trade_date = records[0].get('trade_date', '')
        if not latest_trade_date:
            return '交易日期缺失，无法判断'

        if latest_trade_date == prev_td_str:
            return f"最新大宗交易发生在{latest_trade_date}（上一个交易日盘后15:00-15:30），对{next_trading_day}开盘有直接影响"
        elif latest_trade_date >= (prev_td - timedelta(days=5)).strftime('%Y-%m-%d'):
            return f"最近大宗交易发生在{latest_trade_date}，距下一个交易日{next_trading_day}较近，仍有间接影响"
        else:
            return f"最近大宗交易发生在{latest_trade_date}，距下一个交易日{next_trading_day}较远，影响已减弱"
    except (ValueError, ImportError):
        return '无法判断'


def compute_block_trade_summary(records: list[dict], next_trading_day: str = '') -> dict:
    """预计算大宗交易摘要，供提示词直接引用。

    Args:
        records: 大宗交易记录列表
        next_trading_day: 下一个交易日日期（YYYY-MM-DD），用于判断对次日的影响
    """
    if not records:
        return {'状态': '近期无大宗交易记录', '交易笔数': 0}

    total_count = len(records)
    total_amount = 0
    premium_list = []
    discount_count = 0
    premium_count = 0

    for r in records:
        amt = r.get('amount')
        if amt and isinstance(amt, (int, float)):
            total_amount += amt
        pr = r.get('premium_rate')
        if pr is not None and isinstance(pr, (int, float)):
            premium_list.append(pr)
            if pr < 0:
                discount_count += 1
            elif pr > 0:
                premium_count += 1

    avg_premium = round(sum(premium_list) / len(premium_list), 2) if premium_list else None

    # 判断整体特征
    if discount_count > premium_count:
        trade_character = f"以折价成交为主（折价{discount_count}笔/溢价{premium_count}笔），平均溢价率{avg_premium}%"
    elif premium_count > discount_count:
        trade_character = f"以溢价成交为主（溢价{premium_count}笔/折价{discount_count}笔），平均溢价率{avg_premium}%"
    else:
        trade_character = f"折溢价均衡（折价{discount_count}笔/溢价{premium_count}笔），平均溢价率{avg_premium}%"

    # 检查是否有机构席位
    has_org_buyer = any('机构' in (r.get('buyer') or '') for r in records)
    has_org_seller = any('机构' in (r.get('seller') or '') for r in records)
    org_note = ''
    if has_org_seller and not has_org_buyer:
        org_note = '机构席位出现在卖方，可能存在机构减持'
    elif has_org_buyer and not has_org_seller:
        org_note = '机构席位出现在买方，可能存在机构建仓'
    elif has_org_buyer and has_org_seller:
        org_note = '机构席位买卖双方均有出现'

    # 对下一个交易日的影响判断
    next_day_impact = _assess_block_trade_next_day_impact(records, next_trading_day)

    return {
        '交易笔数': total_count,
        '累计成交额（万元）': round(total_amount, 2),
        '交易特征': trade_character,
        '机构席位情况': org_note if org_note else '未发现机构专用席位',
        '最近交易日期': records[0].get('trade_date', '--'),
        '对下一个交易日影响': next_day_impact,
        '交易明细': records[:5],  # 最多展示5条
    }


if __name__ == "__main__":
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name('生益科技')
        print(f"=== {stock_info.stock_name}（{stock_info.stock_code_normalize}）大宗交易数据 ===\n")

        records = await search_block_trade(stock_info, days=30)
        if records:
            print(f"找到 {len(records)} 条大宗交易记录：")
            print(json.dumps(records, ensure_ascii=False, indent=2))
            print("\n--- 预计算摘要 ---")
            summary = compute_block_trade_summary(records)
            print(json.dumps(summary, ensure_ascii=False, indent=2))
        else:
            print("近期无大宗交易记录")

    asyncio.run(main())
