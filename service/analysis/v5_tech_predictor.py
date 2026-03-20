"""
V5技术形态预测器 — 基于蜻蜓布林+OBV上升+收阳 (回测75.8%胜率)
================================================================
从 five_strategy_deep_v5.py 回测验证的高胜率策略中提取，
用于生产环境的未来5日方向预测。

三大核心策略:
  1. 蜻蜓布林+OBV+收阳  (75.8%胜率, 91信号)
  2. 空中加油+缩量        (65.7%胜率, 70信号)
  3. 蜻蜓布林(单独)       (58.2%胜率, 高信号量)

输出: v5_pred_direction, v5_confidence, v5_strategy, v5_reason
"""
import logging
from datetime import datetime, timedelta

from dao.stock_kline_dao import get_kline_data

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
#  技术指标计算 (复用 shipanxian_enhanced_backtest 的算法)
# ═══════════════════════════════════════════════════════════

def _calc_ma(values: list[float], period: int) -> list[float]:
    ma = [0.0] * len(values)
    for i in range(period - 1, len(values)):
        ma[i] = sum(values[i - period + 1:i + 1]) / period
    return ma


def _calc_ema(values: list[float], period: int) -> list[float]:
    ema = [0.0] * len(values)
    if not values:
        return ema
    k = 2.0 / (period + 1)
    ema[0] = values[0]
    for i in range(1, len(values)):
        ema[i] = values[i] * k + ema[i - 1] * (1 - k)
    return ema


def _calc_rsi(closes: list[float], period: int = 14) -> list[float]:
    n = len(closes)
    rsi = [50.0] * n
    if n < 2:
        return rsi
    gains = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        diff = closes[i] - closes[i - 1]
        if diff > 0:
            gains[i] = diff
        else:
            losses[i] = -diff
    avg_gain = sum(gains[1:period + 1]) / period if n > period else 0
    avg_loss = sum(losses[1:period + 1]) / period if n > period else 0
    if avg_loss > 0:
        rsi[period] = 100 - 100 / (1 + avg_gain / avg_loss)
    elif avg_gain > 0:
        rsi[period] = 100
    for i in range(period + 1, n):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss > 0:
            rsi[i] = 100 - 100 / (1 + avg_gain / avg_loss)
        elif avg_gain > 0:
            rsi[i] = 100
        else:
            rsi[i] = 50
    return rsi


def _calc_kdj(highs, lows, closes, n=9):
    length = len(closes)
    kv = [50.0] * length
    dv = [50.0] * length
    jv = [50.0] * length
    for i in range(n - 1, length):
        hn = max(highs[i - n + 1:i + 1])
        ln = min(lows[i - n + 1:i + 1])
        rsv = (closes[i] - ln) / (hn - ln) * 100 if hn != ln else 50
        kv[i] = kv[i - 1] * 2 / 3 + rsv / 3
        dv[i] = dv[i - 1] * 2 / 3 + kv[i] / 3
        jv[i] = 3 * kv[i] - 2 * dv[i]
    return kv, dv, jv


# ═══════════════════════════════════════════════════════════
#  指标预计算
# ═══════════════════════════════════════════════════════════

def _precompute(klines):
    """预计算全部技术指标，需要至少260根K线。"""
    n = len(klines)
    if n < 260:
        return None

    c = [float(k.get('close_price', 0) or 0) for k in klines]
    o = [float(k.get('open_price', 0) or 0) for k in klines]
    h = [float(k.get('high_price', 0) or 0) for k in klines]
    l = [float(k.get('low_price', 0) or 0) for k in klines]
    v = [float(k.get('trading_volume', 0) or 0) for k in klines]
    ch = [float(k.get('change_hand', 0) or 0) for k in klines]

    ma5 = _calc_ma(c, 5)
    ma10 = _calc_ma(c, 10)
    ma20 = _calc_ma(c, 20)
    ma60 = _calc_ma(c, 60)
    vm5 = _calc_ma(v, 5)

    ema12 = _calc_ema(c, 12)
    ema26 = _calc_ema(c, 26)
    dif = [ema12[i] - ema26[i] for i in range(n)]
    dea = _calc_ema(dif, 9)
    macd_bar = [2 * (dif[i] - dea[i]) for i in range(n)]

    rsi14 = _calc_rsi(c, 14)

    # OBV
    obv = [0.0] * n
    for i in range(1, n):
        if c[i] > c[i - 1]:
            obv[i] = obv[i - 1] + v[i]
        elif c[i] < c[i - 1]:
            obv[i] = obv[i - 1] - v[i]
        else:
            obv[i] = obv[i - 1]

    # 布林带(20,2)
    boll_up = [0.0] * n
    boll_dn = [0.0] * n
    for i in range(19, n):
        w = c[i - 19:i + 1]
        avg = sum(w) / 20
        std = (sum((x - avg) ** 2 for x in w) / 20) ** 0.5
        boll_up[i] = avg + 2 * std
        boll_dn[i] = avg - 2 * std

    return {
        'n': n, 'c': c, 'o': o, 'h': h, 'l': l, 'v': v, 'ch': ch,
        'ma5': ma5, 'ma10': ma10, 'ma20': ma20, 'ma60': ma60,
        'vm5': vm5,
        'dif': dif, 'dea': dea, 'macd_bar': macd_bar,
        'rsi14': rsi14,
        'obv': obv, 'boll_up': boll_up, 'boll_dn': boll_dn,
    }


# ═══════════════════════════════════════════════════════════
#  策略1: 蜻蜓布林 (基础形态)
# ═══════════════════════════════════════════════════════════

def _check_qingting_boll(ind, i):
    """蜻蜓布林: 蜻蜓点水回踩均线 + 布林收窄 + 布林下轨"""
    if i < 60:
        return False
    cv, ov, hv, lv = ind['c'][i], ind['o'][i], ind['h'][i], ind['l'][i]
    v = ind['v']
    ma20v, ma60v = ind['ma20'][i], ind['ma60'][i]
    if ma20v <= 0 and ma60v <= 0:
        return False
    # 回踩MA20或MA60(偏离≤5%且收盘站回)
    hui_20 = ma20v > 0 and abs(lv - ma20v) / ma20v <= 0.05 and cv > ma20v * 0.99
    hui_60 = ma60v > 0 and abs(lv - ma60v) / ma60v <= 0.05 and cv > ma60v * 0.99
    if not (hui_20 or hui_60):
        return False
    # 下影线占比>20%
    amp = hv - lv
    if amp <= 0:
        return False
    if (min(ov, cv) - lv) / amp <= 0.20:
        return False
    # 非大阴线(涨跌>-2%)
    if (cv - ov) / max(ov, 0.01) <= -0.02:
        return False
    # 量能不萎缩(≥前日80%)
    if i < 1 or v[i - 1] <= 0 or v[i] < v[i - 1] * 0.8:
        return False
    # 布林带收窄(<10%)
    mid = ind['ma20'][i]
    if mid <= 0:
        return False
    width = (ind['boll_up'][i] - ind['boll_dn'][i]) / mid
    if width >= 0.10:
        return False
    # 价格在布林下半区
    dn = ind['boll_dn'][i]
    if dn <= 0 or cv > (mid + dn) / 2:
        return False
    return True


# ═══════════════════════════════════════════════════════════
#  策略2: 空中加油 (MACD收敛后再发散)
# ═══════════════════════════════════════════════════════════

def _check_s6_strict(ind, i):
    """空中加油: DIF-DEA收敛后DIF回升+红柱放大"""
    if i < 4:
        return False
    dif, dea, bar = ind['dif'], ind['dea'], ind['macd_bar']
    # 前一日DIF-DEA收敛(差值≤0.02)
    if dif[i - 1] - dea[i - 1] > 0.02:
        return False
    # 金叉状态(DIF>DEA)
    if dif[i - 1] <= dea[i - 1]:
        return False
    # DEA>0
    if dea[i - 1] <= 0:
        return False
    # DEA上升
    if dea[i - 1] <= dea[i - 2]:
        return False
    # DIF连续下降2天
    for d in range(1, 3):
        if i - d - 1 < 0 or dif[i - d] >= dif[i - d - 1]:
            return False
    # 红柱连续缩短2天
    for d in range(1, 3):
        if i - d - 1 < 0 or bar[i - d] >= bar[i - d - 1]:
            return False
    # 当日DIF回升+红柱放大
    if dif[i] < dif[i - 1]:
        return False
    if bar[i] <= bar[i - 1]:
        return False
    return True


# ═══════════════════════════════════════════════════════════
#  过滤器
# ═══════════════════════════════════════════════════════════

def _filter_obv_up(ind, i):
    """OBV上升: 当日OBV > 5日前OBV"""
    return i >= 5 and ind['obv'][i] > ind['obv'][i - 5]


def _filter_yang(ind, i):
    """收阳: 收盘价 > 开盘价"""
    return ind['c'][i] > ind['o'][i]


def _filter_suoliang(ind, i):
    """缩量: 当日成交量 < 5日均量×0.8"""
    return ind['vm5'][i] > 0 and ind['v'][i] < ind['vm5'][i] * 0.8


# ═══════════════════════════════════════════════════════════
#  组合策略检测 — 对最新一根K线进行信号判定
# ═══════════════════════════════════════════════════════════

def _detect_signals(ind):
    """在最后一根K线上检测所有策略信号。

    Returns:
        list[dict]: 命中的策略列表，按优先级排序
        每个dict包含: strategy, confidence, reason
    """
    i = ind['n'] - 1  # 最后一根K线
    hits = []

    qingting = _check_qingting_boll(ind, i)
    obv_up = _filter_obv_up(ind, i)
    yang = _filter_yang(ind, i)
    s6 = _check_s6_strict(ind, i)
    suoliang = _filter_suoliang(ind, i)

    # 策略1: 蜻蜓布林+OBV上升+收阳 (75.8%胜率, 最高优先级)
    if qingting and obv_up and yang:
        parts = ['蜻蜓布林回踩支撑']
        parts.append('OBV5日上升(资金流入)')
        parts.append('收阳确认多头')
        hits.append({
            'strategy': 'v5_qt_obv_yang',
            'confidence': 'high',
            'reason': '; '.join(parts),
            'win_rate': 75.8,
            'priority': 1,
        })

    # 策略2: 空中加油+缩量 (65.7%胜率)
    if s6 and suoliang:
        hits.append({
            'strategy': 'v5_s6_suoliang',
            'confidence': 'medium',
            'reason': 'MACD空中加油(DIF回升红柱放大); 缩量洗盘确认',
            'win_rate': 65.7,
            'priority': 2,
        })

    # 策略3: 蜻蜓布林(单独) (58.2%胜率, 信号量大)
    if qingting and not (obv_up and yang):
        hits.append({
            'strategy': 'v5_qt_boll',
            'confidence': 'low',
            'reason': '蜻蜓布林回踩支撑(无OBV/收阳确认)',
            'win_rate': 58.2,
            'priority': 3,
        })

    return hits


# ═══════════════════════════════════════════════════════════
#  主入口: 对单只股票进行V5技术形态预测
# ═══════════════════════════════════════════════════════════

def _predict_from_klines(stock_code: str, klines: list[dict]) -> dict | None:
    """从已加载的K线数据计算V5技术形态信号（内部函数，不查DB）。"""
    try:
        if not klines or len(klines) < 260:
            return None

        ind = _precompute(klines)
        if ind is None:
            return None

        hits = _detect_signals(ind)
        if not hits:
            return None

        best = min(hits, key=lambda x: x['priority'])
        signal_date = str(klines[-1].get('date', ''))

        i = ind['n'] - 1
        close = ind['c'][i]
        ma20 = ind['ma20'][i]
        boll_width = 0
        if ma20 > 0:
            boll_width = round((ind['boll_up'][i] - ind['boll_dn'][i]) / ma20 * 100, 1)

        reason_parts = [best['reason']]
        reason_parts.append(f'布林宽{boll_width}%')
        if close > 0 and ma20 > 0:
            pos = round((close - ma20) / ma20 * 100, 1)
            reason_parts.append(f'偏离MA20 {pos:+.1f}%')

        all_strategies = [h['strategy'] for h in hits]

        return {
            'v5_pred_direction': 'UP',
            'v5_confidence': best['confidence'],
            'v5_strategy': best['strategy'],
            'v5_reason': '; '.join(reason_parts)[:200],
            'v5_win_rate': best['win_rate'],
            'v5_signal_date': signal_date,
            'v5_signal_count': len(hits),
            'v5_all_strategies': ','.join(all_strategies)[:100],
        }
    except Exception as e:
        logger.warning("V5技术预测失败 [%s]: %s", stock_code, e)
        return None


def predict_v5_tech(stock_code: str, latest_date: str) -> dict | None:
    """对单只股票执行V5技术形态5日预测。

    需要从数据库加载足够的K线数据(≥260根)来计算技术指标。

    Args:
        stock_code: 股票代码(如 002230.SZ)
        latest_date: 最新交易日(YYYY-MM-DD)

    Returns:
        dict with v5_* fields, or None if data insufficient / no signal
    """
    try:
        klines = get_kline_data(stock_code, end_date=latest_date, limit=500)
        return _predict_from_klines(stock_code, klines)
    except Exception as e:
        logger.warning("V5技术预测失败 [%s]: %s", stock_code, e)
        return None


def _batch_load_klines(stock_codes: list[str], latest_date: str) -> dict:
    """批量加载K线数据，一次查询获取所有股票最近500根K线。

    Returns:
        dict: {stock_code: [kline_dict, ...]}  按日期升序
    """
    from datetime import datetime, timedelta
    from collections import defaultdict
    from dao import get_connection

    # 500个交易日 ≈ 2年，用730天日历日覆盖
    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    lookback_start = (dt_latest - timedelta(days=730)).strftime('%Y-%m-%d')

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    stock_klines = defaultdict(list)

    try:
        batch_size = 200
        for i in range(0, len(stock_codes), batch_size):
            batch = stock_codes[i:i + batch_size]
            ph = ','.join(['%s'] * len(batch))
            cur.execute(
                f"SELECT stock_code, `date`, open_price, close_price, high_price, "
                f"low_price, trading_volume, change_hand "
                f"FROM stock_kline WHERE stock_code IN ({ph}) "
                f"AND `date` >= %s AND `date` <= %s "
                f"ORDER BY stock_code, `date`",
                batch + [lookback_start, latest_date])
            for row in cur.fetchall():
                stock_klines[row['stock_code']].append(row)
    finally:
        cur.close()
        conn.close()

    logger.info("V5批量加载K线: %d只股票有数据", len(stock_klines))
    return dict(stock_klines)


def batch_predict_v5_tech(stock_codes: list[str], latest_date: str,
                          progress_callback=None) -> dict:
    """批量执行V5技术形态预测（批量加载K线，避免逐只查DB）。

    Args:
        stock_codes: 股票代码列表
        latest_date: 最新交易日
        progress_callback: 进度回调 (total, done)

    Returns:
        dict: {stock_code: v5_result_dict}
    """
    # 1. 批量加载所有股票K线
    all_klines = _batch_load_klines(stock_codes, latest_date)

    # 2. 逐只计算信号（纯CPU计算，无DB查询）
    results = {}
    total = len(stock_codes)

    for idx, code in enumerate(stock_codes):
        klines = all_klines.get(code)
        if klines:
            result = _predict_from_klines(code, klines)
            if result:
                results[code] = result

        if progress_callback and (idx % 500 == 0 or idx == total - 1):
            progress_callback(total, idx + 1)

    logger.info("V5技术预测完成: %d/%d 只有信号", len(results), total)
    return results
