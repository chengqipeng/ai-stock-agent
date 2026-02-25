import asyncio
import pandas as pd
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline_by_db_cache
from service.eastmoney.technical.stock_day_boll import calculate_bollinger_bands
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name


def _build_dataframe(klines: list) -> pd.DataFrame:
    rows = []
    for kline in klines:
        fields = kline.split(',')
        rows.append({
            'date':   fields[0],
            'open':   float(fields[1]),
            'close':  float(fields[2]),
            'high':   float(fields[3]),
            'low':    float(fields[4]),
            'volume': float(fields[5]),
        })
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    return df.set_index('date')


def _detect_wyckoff_accumulation(df: pd.DataFrame, lookback_top=250, lookback_bot=60, window=11, cur=None) -> tuple[bool, dict]:
    """
    检测【底量远超顶量】威科夫吸筹信号。
    条件 A：空间跌幅达标（底部均价 < 顶部均价 × 70%）
    条件 B：极值量对比（底部最大量 > 顶部最大量 × 1.3 倍）
    条件 C：右侧企稳（收盘价 >= MA20 且 > 底区最低价）
    条件 D：量价协同（底量当天收大阳线或长下影线，非放量大阴线）
    条件 E：缩量回踩测试（底量后出现缩量且不破底的回踩，威科夫 Test）
    条件 F：时间维度（底量到当前横盘 >= 20 天，筹码换手充分）
    """
    if len(df) < lookback_top:
        return False, {'error': '数据长度不足'}

    df = df.copy()
    df['ma20'] = df['close'].rolling(window=20).mean()

    if cur is None:
        cur = len(df) - 1
    top_start = max(0, cur - lookback_top + 1)
    bot_start = max(0, cur - lookback_bot + 1)

    # Bug1修复：先确定底区，顶区只在底区之前查找，确保时序正确（顶必须早于底）
    bot_idx = bot_start + df['low'].iloc[bot_start:cur + 1].values.argmin()
    top_idx = top_start + df['high'].iloc[top_start:bot_idx].values.argmax()

    top_zone = df.iloc[max(0, top_idx - window): min(top_idx + window + 1, bot_idx)]
    # Bug2修复：bot_zone右边界不超过cur，避免引入未来数据
    bot_zone = df.iloc[max(0, bot_idx - window): min(bot_idx + window + 1, cur + 1)]

    avg_price_top = top_zone['close'].mean()
    avg_price_bot = bot_zone['close'].mean()
    max_vol_top   = top_zone['volume'].max()
    max_vol_bot   = bot_zone['volume'].max()

    current_close = df['close'].iloc[cur]
    current_ma20  = df['ma20'].iloc[cur]

    cond_a = avg_price_bot < avg_price_top * 0.70
    cond_b = max_vol_bot > max_vol_top * 1.30
    # Bug3修复：确保底量已发生在过去（bot_idx < cur），当前价格才有企稳意义
    bot_low = bot_zone['low'].min()
    # 条件C严格按算法：收盘价 >= MA20 且 > 底区最低价
    cond_c = (bot_idx < cur) and (current_close >= current_ma20) and (current_close > bot_low)

    # 条件 D：量价协同——底量当天 K 线形态
    bot_vol_day_idx = bot_zone['volume'].values.argmax()
    bot_vol_day = bot_zone.iloc[bot_vol_day_idx]
    body = bot_vol_day['close'] - bot_vol_day['open']
    # 下影线 = min(open, close) - low，与阴阳无关
    lower_shadow = min(bot_vol_day['open'], bot_vol_day['close']) - bot_vol_day['low']
    is_big_yang = body / bot_vol_day['open'] > 0.02          # 大阳线：涨幅 > 2%
    is_long_lower = lower_shadow > abs(body) * 1.5 if abs(body) > 0 else False  # 长下影线
    cond_d = is_big_yang or is_long_lower

    # 条件 E：缩量回踩测试（底量 zone 结束后到 cur 之间，出现成交量 < 底量最大量 50% 且低点不破底区最低价）
    bot_zone_end = bot_idx + window + 1  # 不 clamp，after_bot 切片自然截断到 cur
    cond_e = False
    test_low = None
    if bot_zone_end < cur:
        after_bot = df.iloc[bot_zone_end: cur]  # 不含当天，回踩测试只看历史过程
        shrink_mask = after_bot['volume'] < max_vol_bot * 0.50
        no_break_mask = after_bot['low'] > bot_low
        test_days = (shrink_mask & no_break_mask).sum()
        cond_e = test_days >= 3
        test_low = round(after_bot['low'].min(), 2) if len(after_bot) else None

    # 条件 F：时间维度——底量到当前天数 >= 20
    days_since_bot = cur - bot_idx
    cond_f = days_since_bot >= 20

    details = {
        'top_zone_date':  df.index[top_idx],
        'bot_zone_date':  df.index[bot_idx],
        'avg_price_top':  round(avg_price_top, 2),
        'avg_price_bot':  round(avg_price_bot, 2),
        'max_vol_top':    round(max_vol_top / 10000, 2),
        'max_vol_bot':    round(max_vol_bot / 10000, 2),
        'vol_ratio':      round(max_vol_bot / max_vol_top, 2) if max_vol_top else None,
        'price_drop_pct': round((1 - avg_price_bot / avg_price_top) * 100, 2),
        'cond_a_passed':  cond_a,
        'cond_b_passed':  cond_b,
        'cond_c_passed':  cond_c,
        'cond_d_passed':  cond_d,
        'cond_e_passed':  cond_e,
        'cond_f_passed':  cond_f,
        'bot_vol_day_yang': is_big_yang,
        'bot_vol_day_lower_shadow': is_long_lower,
        'test_low':       test_low,
        'days_since_bot': days_since_bot,
    }
    return cond_a and cond_b and cond_c and cond_d and cond_e and cond_f, details


def _log_result(stock_name: str, raw_df: pd.DataFrame, matches: list, lookback_top: int, lookback_bot: int, window: int) -> None:
    print("\n========== 底量远超顶量信号日志 ==========")
    print(f"""【策略逻辑说明】
股票：{stock_name}
策略：识别「底量远超顶量预示主力长期建仓」威科夫吸筹信号，需同时满足以下3个条件：
  条件A（空间跌幅）：底部均价 < 顶部均价 × 70%（跌幅超30%）
  条件B（量能对比）：底部最大量 > 顶部最大量 × 1.3 倍
  条件C（右侧企稳）：当前收盘价 >= MA20，且 > 底区最低价
  条件D（量价协同）：底量当天收大阳线（涨幅>2%）或长下影线（下影>实体1.5倍）
  条件E（缩量回踩）：底量后出现 ≥3 天缩量（<底量50%）且不破底的测试
  条件F（时间维度）：底量到当前 ≥20 天，筹码换手充分
顶部回溯：{lookback_top} 天，底部回溯：{lookback_bot} 天，区间延伸：前后各 {window} 天
共找到匹配信号：{len(matches)} 个""")
    print("==========================================\n")


def _build_result(df: pd.DataFrame, signal: bool, details: dict) -> dict:
    cur_date = df.index[details.get('cur', len(df) - 1)].strftime('%Y-%m-%d') if 'cur' in details else df.index[-1].strftime('%Y-%m-%d')
    return {
        '交易日': cur_date,
        f'底量远超顶量（{cur_date}）': bool(signal),
        '顶部区间日期': details.get('top_zone_date', '').strftime('%Y-%m-%d') if not isinstance(details.get('top_zone_date'), str) else details.get('top_zone_date'),
        '底部区间日期': details.get('bot_zone_date', '').strftime('%Y-%m-%d') if not isinstance(details.get('bot_zone_date'), str) else details.get('bot_zone_date'),
        '顶部均价': details.get('avg_price_top'),
        '底部均价': details.get('avg_price_bot'),
        '顶部最大量（万）': details.get('max_vol_top'),
        '底部最大量（万）': details.get('max_vol_bot'),
        '底顶量比': details.get('vol_ratio'),
        '价格跌幅(%)': details.get('price_drop_pct'),
        '条件A_跌幅达标': bool(details.get('cond_a_passed')),
        '条件B_量能对比': bool(details.get('cond_b_passed')),
        '条件C_右侧企稳': bool(details.get('cond_c_passed')),
        '条件D_量价协同': bool(details.get('cond_d_passed')),
        '条件E_缩量回踩': bool(details.get('cond_e_passed')),
        '条件F_时间维度': bool(details.get('cond_f_passed')),
        '底量日阳线': bool(details.get('bot_vol_day_yang')),
        '底量日长下影': bool(details.get('bot_vol_day_lower_shadow')),
        '回踩最低价': details.get('test_low'),
        '底量至今天数': details.get('days_since_bot'),
    }


async def get_bottom_far_top_volume_indicates_cn(stock_info: StockInfo, limit=500, lookback_top=250, lookback_bot=60, window=11, top_n=10) -> list[dict]:
    """获取底量远超顶量信号，返回最近 top_n 个匹配的中文 key JSON 列表"""
    klines = await get_stock_day_range_kline_by_db_cache(stock_info, limit=limit)
    raw_df = _build_dataframe(klines)
    df = raw_df.copy()

    boll_records = await calculate_bollinger_bands(stock_info)
    boll_mb = pd.Series(
        {pd.Timestamp(r['date']): r['boll'] for r in boll_records},
        name='boll_mb',
    )
    df['boll_mb'] = boll_mb.reindex(df.index)

    matches = []
    total = len(df)
    for cur in range(total - 1, lookback_top - 2, -1):
        signal, details = _detect_wyckoff_accumulation(df, lookback_top, lookback_bot, window, cur=cur)
        if signal:
            details['cur'] = cur
            matches.append(_build_result(df, signal, details))
            if len(matches) >= top_n:
                break

    _log_result(stock_info.stock_name, raw_df, matches, lookback_top, lookback_bot, window)
    return matches


async def scan_stocks_bottom_far_top_volume(limit_stocks=50) -> list[dict]:
    """扫描 STOCKS 前 limit_stocks 只股票，返回满足底量远超顶量信号的结果列表"""
    from common.constants.stocks_data import STOCKS
    from common.utils.stock_info_utils import get_stock_info_by_name

    results = []
    for stock in STOCKS[:limit_stocks]:
        stock_info = get_stock_info_by_name(stock['name'])
        if stock_info is None:
            continue
        matches = await get_bottom_far_top_volume_indicates_cn(stock_info)
        if matches:
            results.extend(matches)
    return results if results else [{'message': '未找到满足条件的股票'}]


if __name__ == '__main__':
    async def main():
        import json
        stock_info: StockInfo = get_stock_info_by_name('中国卫通')
        result = await get_bottom_far_top_volume_indicates_cn(stock_info)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(main())
