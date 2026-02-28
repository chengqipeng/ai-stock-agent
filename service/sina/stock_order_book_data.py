"""五档盘口数据模块。

通过新浪财经实时行情接口获取股票五档买卖挂单数据。
接口：hq.sinajs.cn/list=sh600183 或 sz000001
返回字段中 index 10-29 为五档买卖盘口数据。
"""

import aiohttp
import logging
import re
import time

from common.utils.stock_info_utils import StockInfo

logger = logging.getLogger(__name__)

# ── 缓存（30秒TTL，盘口数据实时性要求高） ──
_order_book_cache: dict[str, dict] = {}
_CACHE_TTL = 30  # 30秒

_SINA_HQ_URL = "https://hq.sinajs.cn"


def _build_sina_symbol(stock_info: StockInfo) -> str:
    """将 StockInfo 转换为新浪行情接口的股票代码格式。
    
    新浪格式：sh600183（上海）、sz000001（深圳）
    """
    code, market = stock_info.stock_code_normalize.split('.')
    prefix = 'sh' if market == 'SH' else 'sz'
    return f"{prefix}{code}"


async def get_order_book(stock_info: StockInfo) -> dict:
    """获取股票五档盘口数据。
    
    Returns:
        dict: 包含五档买卖盘口数据，格式如：
        {
            "股票名称": "生益科技",
            "当前价": 18.50,
            "买一": {"价格": 18.49, "数量（手）": 469},
            ...
            "卖五": {"价格": 18.55, "数量（手）": 312},
        }
    """
    cache_key = stock_info.stock_code_normalize
    now = time.time()

    cached = _order_book_cache.get(cache_key)
    if cached and (now - cached['ts']) < _CACHE_TTL:
        logger.info(f"命中盘口缓存 [{stock_info.stock_name}]")
        return cached['data']

    symbol = _build_sina_symbol(stock_info)
    url = f"{_SINA_HQ_URL}/list={symbol}"

    headers = {
        "Referer": "https://finance.sina.com.cn/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                text = await response.text(encoding='gbk')

        # 解析返回数据：var hq_str_sh600183="name,open,prev_close,cur,..."
        match = re.search(r'"(.+)"', text)
        if not match:
            raise ValueError(f"新浪行情接口返回数据异常: {text[:200]}")

        fields = match.group(1).split(',')
        if len(fields) < 32:
            raise ValueError(f"字段数不足，期望>=32，实际{len(fields)}")

        result = _parse_order_book(fields, stock_info.stock_name)
        _order_book_cache[cache_key] = {'data': result, 'ts': now}
        return result

    except Exception as e:
        logger.warning(f"获取盘口数据失败 [{stock_info.stock_name}]: {e}")
        if cached:
            return cached['data']
        return {}


def _parse_order_book(fields: list[str], stock_name: str) -> dict:
    """解析新浪行情接口返回的字段列表，提取五档盘口数据。
    
    新浪字段索引（0-based）：
    0: 股票名称, 1: 今开, 2: 昨收, 3: 当前价, 4: 最高, 5: 最低
    6: 竞买价(买一价), 7: 竞卖价(卖一价)
    8: 成交量(股), 9: 成交额(元)
    10-11: 买一(量,价), 12-13: 买二, 14-15: 买三, 16-17: 买四, 18-19: 买五
    20-21: 卖一(量,价), 22-23: 卖二, 24-25: 卖三, 26-27: 卖四, 28-29: 卖五
    30: 日期, 31: 时间
    """
    def _safe_float(val):
        try:
            return float(val)
        except (ValueError, TypeError) as e:
            logger.debug("_safe_float 转换失败: val=%s, %s", val, e)
            return 0.0

    def _safe_int(val):
        """成交量单位为股，转换为手（÷100）"""
        try:
            return int(float(val)) // 100
        except (ValueError, TypeError) as e:
            logger.debug("_safe_int 转换失败: val=%s, %s", val, e)
            return 0

    cur_price = _safe_float(fields[3])
    prev_close = _safe_float(fields[2])

    result = {
        '股票名称': stock_name,
        '当前价': cur_price,
        '今开': _safe_float(fields[1]),
        '昨收': prev_close,
        '最高': _safe_float(fields[4]),
        '最低': _safe_float(fields[5]),
        '成交量（手）': _safe_int(fields[8]),
        '成交额（元）': _safe_float(fields[9]),
    }

    # 五档买盘：index 10-19，每2个一组（量, 价）
    buy_labels = ['买一', '买二', '买三', '买四', '买五']
    for i, label in enumerate(buy_labels):
        vol_idx = 10 + i * 2
        price_idx = 11 + i * 2
        result[label] = {
            '价格': _safe_float(fields[price_idx]),
            '数量（手）': _safe_int(fields[vol_idx]),
        }

    # 五档卖盘：index 20-29，每2个一组（量, 价）
    sell_labels = ['卖一', '卖二', '卖三', '卖四', '卖五']
    for i, label in enumerate(sell_labels):
        vol_idx = 20 + i * 2
        price_idx = 21 + i * 2
        result[label] = {
            '价格': _safe_float(fields[price_idx]),
            '数量（手）': _safe_int(fields[vol_idx]),
        }

    return result


def compute_order_book_summary(order_book: dict) -> dict:
    """预计算五档盘口摘要，供提示词直接引用。
    
    分析维度：
    1. 买卖力量对比（总挂单量比较）
    2. 大单压力/支撑检测
    3. 买卖价差（spread）
    4. 挂单集中度
    """
    if not order_book:
        return {'状态': '未获取到盘口数据'}

    buy_labels = ['买一', '买二', '买三', '买四', '买五']
    sell_labels = ['卖一', '卖二', '卖三', '卖四', '卖五']

    buy_volumes = []
    sell_volumes = []
    buy_prices = []
    sell_prices = []

    for label in buy_labels:
        item = order_book.get(label, {})
        buy_volumes.append(item.get('数量（手）', 0))
        buy_prices.append(item.get('价格', 0))

    for label in sell_labels:
        item = order_book.get(label, {})
        sell_volumes.append(item.get('数量（手）', 0))
        sell_prices.append(item.get('价格', 0))

    total_buy = sum(buy_volumes)
    total_sell = sum(sell_volumes)
    cur_price = order_book.get('当前价', 0)
    prev_close = order_book.get('昨收', 0)

    # 买卖力量比
    if total_sell > 0:
        buy_sell_ratio = round(total_buy / total_sell, 2)
    else:
        buy_sell_ratio = float('inf') if total_buy > 0 else 1.0

    # 力量判断
    if buy_sell_ratio > 1.5:
        power_desc = f"买盘明显强于卖盘（买卖比{buy_sell_ratio}:1），下方支撑较强"
    elif buy_sell_ratio > 1.1:
        power_desc = f"买盘略强于卖盘（买卖比{buy_sell_ratio}:1），多方小幅占优"
    elif buy_sell_ratio > 0.9:
        power_desc = f"买卖力量基本均衡（买卖比{buy_sell_ratio}:1）"
    elif buy_sell_ratio > 0.67:
        power_desc = f"卖盘略强于买盘（买卖比{buy_sell_ratio}:1），空方小幅占优"
    else:
        power_desc = f"卖盘明显强于买盘（买卖比{buy_sell_ratio}:1），上方压力较大"

    # 买卖价差（spread）
    buy1_price = buy_prices[0] if buy_prices else 0
    sell1_price = sell_prices[0] if sell_prices else 0
    spread = round(sell1_price - buy1_price, 3) if sell1_price and buy1_price else 0
    spread_pct = round(spread / buy1_price * 100, 3) if buy1_price else 0

    # 大单检测（单笔挂单 > 总量的30%视为大单）
    big_order_threshold = max(total_buy, total_sell) * 0.3 if max(total_buy, total_sell) > 0 else 100
    big_buy_orders = [(buy_labels[i], buy_volumes[i], buy_prices[i])
                      for i in range(5) if buy_volumes[i] > big_order_threshold]
    big_sell_orders = [(sell_labels[i], sell_volumes[i], sell_prices[i])
                       for i in range(5) if sell_volumes[i] > big_order_threshold]

    big_order_desc = []
    for label, vol, price in big_buy_orders:
        big_order_desc.append(f"{label}({price}元)有{vol}手大单托底")
    for label, vol, price in big_sell_orders:
        big_order_desc.append(f"{label}({price}元)有{vol}手大单压盘")

    return {
        '当前价': cur_price,
        '昨收': prev_close,
        '五档买盘总量（手）': total_buy,
        '五档卖盘总量（手）': total_sell,
        '买卖力量比': buy_sell_ratio,
        '力量判断': power_desc,
        '买一卖一价差': f"{spread}元（{spread_pct}%）",
        '大单情况': big_order_desc if big_order_desc else '五档内无明显大单',
        '五档明细': {
            '买盘': {buy_labels[i]: f"{buy_prices[i]}元/{buy_volumes[i]}手" for i in range(5)},
            '卖盘': {sell_labels[i]: f"{sell_prices[i]}元/{sell_volumes[i]}手" for i in range(5)},
        },
    }


if __name__ == "__main__":
    import asyncio
    import json
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name('生益科技')
        print(f"=== {stock_info.stock_name}（{stock_info.stock_code_normalize}）五档盘口 ===\n")

        order_book = await get_order_book(stock_info)
        if order_book:
            print("原始盘口数据：")
            print(json.dumps(order_book, ensure_ascii=False, indent=2))
            print("\n--- 预计算摘要 ---")
            summary = compute_order_book_summary(order_book)
            print(json.dumps(summary, ensure_ascii=False, indent=2))
        else:
            print("未获取到盘口数据")

    asyncio.run(main())
