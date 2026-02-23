import asyncio
import pandas as pd
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline
from service.eastmoney.technical.stock_day_boll import calculate_bollinger_bands
from common.utils.stock_info_utils import StockInfo


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


def _detect_wyckoff_accumulation(df: pd.DataFrame, lookback_top=250, lookback_bot=60, window=5) -> tuple[bool, dict]:
    """
    检测【底量远超顶量】威科夫吸筹信号。
    条件 A：空间跌幅达标（底部均价 < 顶部均价 × 70%）
    条件 B：极值量对比（底部最大量 > 顶部最大量 × 1.3 倍）
    条件 C：右侧企稳（收盘价 >= MA20 且 > 底区最低价）
    """
    if len(df) < lookback_top:
        return False, {'error': '数据长度不足'}

    df = df.copy()
    df['ma20'] = df['close'].rolling(window=20).mean()

    cur = len(df) - 1
    top_start = max(0, cur - lookback_top + 1)
    bot_start = max(0, cur - lookback_bot + 1)

    top_idx = top_start + df['high'].iloc[top_start:cur + 1].values.argmax()
    bot_idx = bot_start + df['low'].iloc[bot_start:cur + 1].values.argmin()

    top_zone = df.iloc[max(0, top_idx - window): top_idx + window + 1]
    bot_zone = df.iloc[max(0, bot_idx - window): bot_idx + window + 1]

    avg_price_top = top_zone['close'].mean()
    avg_price_bot = bot_zone['close'].mean()
    max_vol_top   = top_zone['volume'].max()
    max_vol_bot   = bot_zone['volume'].max()

    current_close = df['close'].iloc[cur]
    current_ma20  = df['ma20'].iloc[cur]

    cond_a = avg_price_bot < avg_price_top * 0.70
    cond_b = max_vol_bot > max_vol_top * 1.30
    cond_c = (current_close >= current_ma20) and (current_close > bot_zone['low'].min())

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
    }
    return cond_a and cond_b and cond_c, details


async def get_bottom_far_top_volume_indicates_cn(stock_info: StockInfo, limit=500, lookback_top=250, lookback_bot=60, window=5) -> dict:
    """获取底量远超顶量信号，返回中文 key 的 JSON 结构"""
    klines = await get_stock_day_range_kline(stock_info, limit=limit)
    df = _build_dataframe(klines)

    boll_records = await calculate_bollinger_bands(stock_info)
    boll_mb = pd.Series(
        {pd.Timestamp(r['date']): r['boll'] for r in boll_records},
        name='boll_mb',
    )
    df['boll_mb'] = boll_mb.reindex(df.index)

    signal, details = _detect_wyckoff_accumulation(df, lookback_top, lookback_bot, window)

    latest_date = df.index[-1].strftime('%Y-%m-%d')

    print("\n========== 底量远超顶量信号日志 ==========")
    print(f"""【策略逻辑说明】
股票：{stock_info.stock_name}
策略：识别「底量远超顶量预示主力长期建仓」威科夫吸筹信号，需同时满足以下3个条件：
  条件A（空间跌幅）：底部均价 < 顶部均价 × 70%（跌幅超30%）
  条件B（量能对比）：底部最大量 > 顶部最大量 × 1.3 倍
  条件C（右侧企稳）：当前收盘价 >= MA20，且 > 底区最低价
顶部回溯：{lookback_top} 天，底部回溯：{lookback_bot} 天，区间延伸：前后各 {window} 天""")
    print(f"信号结果：{'✅ 触发' if signal else '❌ 未触发'}")
    print(f"明细：{details}")
    print("==========================================\n")

    return {
        '最新交易日': latest_date,
        f'底量远超顶量（{latest_date}）': bool(signal),
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
    }


if __name__ == '__main__':
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info: StockInfo = get_stock_info_by_name('北方华创')
        import json
        result = await get_bottom_far_top_volume_indicates_cn(stock_info)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(main())
