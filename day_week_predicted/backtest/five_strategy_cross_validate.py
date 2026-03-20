#!/usr/bin/env python3
"""
5大选股策略 周级别交叉验证回测
==============================
将5张图片中的通达信选股公式转化为Python规则，
在历史K线上回测验证其对"未来一周涨跌"的预测能力。

策略列表：
  S1. 暴力洗盘 — 强势股大跌洗盘后反弹
  S2. 堆量挖坑 — 量能金叉+回踩均线后启动
  S3. 蜻蜓点水 — 均线精准回踩长下影线
  S4. 出水芙蓉 — 一阳穿三线突破
  S5. 店大欺客 — 龙头首阴低吸

回测方式：
  - 信号日次日开盘买入，持有5个交易日（约一周）后收盘卖出
  - 滚动时间窗口交叉验证（按季度分fold）
  - 单策略 + 多策略投票组合 + 与试盘线策略交叉对比

用法：
    python -m day_week_predicted.backtest.five_strategy_cross_validate
"""
import sys
import math
import logging
import json
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, '.')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

from dao.stock_kline_dao import get_kline_data, get_all_stock_codes
from day_week_predicted.backtest.shipanxian_enhanced_backtest import (
    calc_ma, calc_ema, calc_macd, calc_rsi, calc_kdj, calc_stats,
)


# ═══════════════════════════════════════════════════════════════════
#  通用指标预计算
# ═══════════════════════════════════════════════════════════════════

def precompute_indicators(klines: list[dict]) -> dict | None:
    """预计算所有策略需要的技术指标，返回指标字典。"""
    n = len(klines)
    if n < 260:  # 需要MA250
        return None

    closes = [float(k['close_price']) for k in klines]
    opens = [float(k['open_price']) for k in klines]
    highs = [float(k['high_price']) for k in klines]
    lows = [float(k['low_price']) for k in klines]
    vols = [float(k['trading_volume']) for k in klines]
    change_hands = [float(k.get('change_hand', 0) or 0) for k in klines]
    names = [str(k.get('stock_name', '')) for k in klines]

    # 均线
    ma5 = calc_ma(closes, 5)
    ma10 = calc_ma(closes, 10)
    ma20 = calc_ma(closes, 20)
    ma60 = calc_ma(closes, 60)
    ma250 = calc_ma(closes, 250)

    # EMA
    ema20 = calc_ema(closes, 20)
    ema40 = calc_ema(closes, 40)
    ema60 = calc_ema(closes, 60)

    # 量均线
    vol_ma5 = calc_ma(vols, 5)
    vol_ma10 = calc_ma(vols, 10)
    vol_ma20 = calc_ma(vols, 20)

    # MACD
    dif, dea, macd_bar = calc_macd(closes)

    # RSI(6)
    rsi6 = calc_rsi(closes, 6)

    # KDJ
    k_vals, d_vals, j_vals = calc_kdj(highs, lows, closes)

    return {
        'n': n, 'closes': closes, 'opens': opens, 'highs': highs,
        'lows': lows, 'vols': vols, 'change_hands': change_hands,
        'ma5': ma5, 'ma10': ma10, 'ma20': ma20, 'ma60': ma60, 'ma250': ma250,
        'ema20': ema20, 'ema40': ema40, 'ema60': ema60,
        'vol_ma5': vol_ma5, 'vol_ma10': vol_ma10, 'vol_ma20': vol_ma20,
        'dif': dif, 'dea': dea, 'macd_bar': macd_bar,
        'rsi6': rsi6, 'k': k_vals, 'd': d_vals, 'j': j_vals,
    }


def _safe_div(a, b, default=0.0):
    return a / b if b and b != 0 else default


def _is_st_or_special(stock_code: str) -> bool:
    """根据代码判断是否为ST/科创/创业/北交所。"""
    return stock_code.startswith('688') or stock_code.startswith('30') or stock_code.startswith('8')


def _is_688(stock_code: str) -> bool:
    return stock_code.startswith('688')


def _is_30(stock_code: str) -> bool:
    return stock_code.startswith('30')


# ═══════════════════════════════════════════════════════════════════
#  策略 S1: 暴力洗盘
# ═══════════════════════════════════════════════════════════════════
# 核心：多头排列中大跌>4.5%，近9日有涨停，获利盘65%-90%，成本比率>1.15
# 注：WINNER/COST 无法精确计算（需筹码分布），用近似替代

def _approx_winner(closes: list[float], i: int, period: int = 60) -> float:
    """近似获利盘比例：当前价格在近N日收盘价中的百分位。"""
    if i < period:
        return 0.5
    window = closes[i - period:i + 1]
    below = sum(1 for p in window if p <= closes[i])
    return below / len(window)


def _approx_cost_ratio(closes: list[float], i: int, period: int = 50) -> float:
    """近似成本比率：CLOSE / 近N日中位数价格。"""
    if i < period:
        return 1.0
    window = sorted(closes[i - period:i + 1])
    median_cost = window[len(window) // 2]
    return _safe_div(closes[i], median_cost, 1.0)


def detect_s1_baoli_xipan(ind: dict, i: int, stock_code: str) -> bool:
    """暴力洗盘信号检测。"""
    if i < 60:
        return False
    c, o = ind['closes'], ind['opens']
    # 跌幅条件: 当日跌幅 > 4.5%
    if i < 1 or c[i - 1] <= 0:
        return False
    drop_pct = (c[i] - c[i - 1]) / c[i - 1] * 100
    if drop_pct >= -4.5:
        return False

    # 涨停存在: 近9日内有涨停(涨幅>=9.9%)
    has_limit_up = False
    for j in range(max(0, i - 8), i + 1):
        if j >= 1 and c[j - 1] > 0:
            chg = (c[j] - c[j - 1]) / c[j - 1]
            if chg >= 0.099:
                has_limit_up = True
                break
    if not has_limit_up:
        return False

    # 多头排列: MA5 > MA10 > MA20 > MA60
    if not (ind['ma5'][i] > ind['ma10'][i] > ind['ma20'][i] > ind['ma60'][i] > 0):
        return False

    # 获利盘条件 (近似)
    winner = _approx_winner(c, i)
    if not (0.65 <= winner <= 0.90):
        return False

    # 成本比率 (近似)
    cost_ratio = _approx_cost_ratio(c, i)
    if cost_ratio <= 1.15:
        return False

    return True


# ═══════════════════════════════════════════════════════════════════
#  策略 S2: 堆量挖坑
# ═══════════════════════════════════════════════════════════════════
# 核心：排除ST/688，量能金叉+放量，价格回踩10日线后收复，60日线向上

def detect_s2_duiliang_wakeng(ind: dict, i: int, stock_code: str) -> bool:
    """堆量挖坑信号检测。"""
    if i < 60:
        return False
    # 排除688
    if _is_688(stock_code):
        return False
    # VOL > 1 (非停牌)
    if ind['vols'][i] <= 1:
        return False

    c, l = ind['closes'], ind['lows']
    v = ind['vols']

    # 量能黄金交叉: VOL_MA5 上穿 VOL_MA10
    vm5, vm10 = ind['vol_ma5'], ind['vol_ma10']
    if i < 1 or vm5[i] <= 0 or vm10[i] <= 0:
        return False
    cross = vm5[i] > vm10[i] and vm5[i - 1] <= vm10[i - 1]
    if not cross:
        return False

    # 今日放量: V > VOL_MA5 * 1.2
    if v[i] <= vm5[i] * 1.2:
        return False

    # 挖坑深度: C / REF(C, 5) < 1.00
    if i < 5 or c[i - 5] <= 0:
        return False
    if c[i] / c[i - 5] >= 1.00:
        return False

    # 曾触及均线: L <= MA10 * 1.02
    ma10_val = ind['ma10'][i]
    if ma10_val <= 0:
        return False
    if l[i] > ma10_val * 1.02:
        return False

    # 收盘收复: C >= MA10
    if c[i] < ma10_val:
        return False

    # 趋势向上: C > MA60 AND MA60 > REF(MA60, 1)
    ma60_val = ind['ma60'][i]
    if ma60_val <= 0 or i < 1:
        return False
    if c[i] <= ma60_val:
        return False
    if ind['ma60'][i] <= ind['ma60'][i - 1]:
        return False

    return True


# ═══════════════════════════════════════════════════════════════════
#  策略 S3: 蜻蜓点水
# ═══════════════════════════════════════════════════════════════════
# 核心：回踩20/60日线(2%内)，长下影阳线，无上影，量温和放大，4日最低

def detect_s3_qingting_dianshui(ind: dict, i: int, stock_code: str) -> bool:
    """蜻蜓点水信号检测。"""
    if i < 60:
        return False

    c, o, h, l = ind['closes'][i], ind['opens'][i], ind['highs'][i], ind['lows'][i]
    v = ind['vols']
    ma20_val = ind['ma20'][i]
    ma60_val = ind['ma60'][i]

    if ma20_val <= 0 or ma60_val <= 0:
        return False

    # 回踩20日线 OR 回踩60日线 (幅度2%内，收盘在线上)
    hui_20 = abs(l - ma20_val) / ma20_val <= 0.02 and c > ma20_val
    hui_60 = abs(l - ma60_val) / ma60_val <= 0.02 and c > ma60_val
    if not (hui_20 or hui_60):
        return False

    # 下影线比例 > 0.3
    amplitude = h - l
    if amplitude <= 0:
        return False
    lower_shadow = min(o, c) - l
    if lower_shadow / amplitude <= 0.3:
        return False

    # 收阳线 (允许微阴)
    if (c - o) / o <= -0.01:
        return False

    # 成交量温和放大: V > REF(V,1)*1.1 AND V < REF(V,1)*4
    if i < 1 or v[i - 1] <= 0:
        return False
    if not (v[i] > v[i - 1] * 1.1 and v[i] < v[i - 1] * 4):
        return False

    # 上影线判断: (HIGH - MAX(C,O)) / MAX(C,O) <= 0.01
    upper_shadow = h - max(c, o)
    if max(c, o) <= 0:
        return False
    if upper_shadow / max(c, o) > 0.01:
        return False

    # 最低价上涨: L > REF(L,1) * 1.01 (放宽: 原1.03太严)
    if i < 1:
        return False
    if l <= ind['lows'][i - 1] * 1.01:
        return False

    # 四日最低价: L <= LLV(L, 4) * 1.005 (放宽: 允许微小误差)
    if i < 3:
        return False
    llv4 = min(ind['lows'][j] for j in range(i - 3, i + 1))
    if l > llv4 * 1.005:
        return False

    return True


# ═══════════════════════════════════════════════════════════════════
#  策略 S4: 出水芙蓉
# ═══════════════════════════════════════════════════════════════════
# 核心：开盘在EMA60下，收盘突破EMA20/40/60，均线粘合，放量，MACD金叉

def detect_s4_chushui_furong(ind: dict, i: int, stock_code: str) -> bool:
    """出水芙蓉信号检测。"""
    if i < 250:
        return False

    c = ind['closes'][i]
    o = ind['opens'][i]
    v = ind['vols'][i]

    ema20 = ind['ema20'][i]
    ema40 = ind['ema40'][i]
    ema60 = ind['ema60'][i]
    ma250 = ind['ma250'][i]
    vm10 = ind['vol_ma10'][i]

    if ema60 <= 0 or ema40 <= 0 or ema20 <= 0 or ma250 <= 0 or vm10 <= 0:
        return False

    # A1: 开盘在EMA60下
    if o >= ema60:
        return False

    # A2: 收盘突破EMA20/40/60
    max_ema = max(ema20, ema40, ema60)
    if c <= max_ema:
        return False

    # A3: 放量 VOL/VMA10 > 1.2
    if v / vm10 <= 1.2:
        return False

    # A4: 涨幅 > 4.9%
    if i < 1 or ind['closes'][i - 1] <= 0:
        return False
    if c / ind['closes'][i - 1] <= 1.049:
        return False

    # A5: 均线粘合 MAX(EMA20,40,60)/MIN(EMA20,40,60) < 1.1
    min_ema = min(ema20, ema40, ema60)
    if min_ema <= 0 or max_ema / min_ema >= 1.1:
        return False

    # A6: 前期振幅 < 25%
    if i < 21:
        return False
    hhv20 = max(ind['highs'][j] for j in range(i - 20, i))
    llv20 = min(ind['lows'][j] for j in range(i - 20, i))
    if llv20 <= 0 or hhv20 / llv20 >= 1.25:
        return False

    # A8: 距年线不远 CLOSE/MA250 < 1.15
    if c / ma250 >= 1.15:
        return False

    # A10: MACD金叉 DIF > DEA
    if ind['dif'][i] <= ind['dea'][i]:
        return False

    # A11: RSI6 < 80
    if ind['rsi6'][i] >= 80:
        return False

    return True


# ═══════════════════════════════════════════════════════════════════
#  策略 S5: 店大欺客战法
# ═══════════════════════════════════════════════════════════════════
# 核心：排除ST/688/30，5日内>=2次涨停，连阳后首阴，缩量，趋势支撑

def detect_s5_dianda_qike(ind: dict, i: int, stock_code: str) -> bool:
    """店大欺客战法信号检测。"""
    if i < 20:
        return False

    c, o = ind['closes'], ind['opens']
    v = ind['vols']

    # 基础条件：排除ST/688/30
    if _is_688(stock_code) or _is_30(stock_code):
        return False
    if v[i] <= 1:
        return False

    # 涨停判断: C/REF(C,1) >= 1.0987
    limit_up_count = 0
    for j in range(max(1, i - 4), i + 1):
        if c[j - 1] > 0 and c[j] / c[j - 1] >= 1.0987:
            limit_up_count += 1

    # 龙头基因: 5日内至少2次涨停
    if limit_up_count < 2:
        return False

    # 当日收阴
    if c[i] >= o[i]:
        return False

    # N日内阳线天数 >= 3 (首阴条件)
    yang_count = 0
    for j in range(max(0, i - 4), i + 1):
        if c[j] >= o[j]:
            yang_count += 1
    if yang_count < 3:
        return False

    # 量能健康: V / MA(V,20) < 2.5
    vm20 = ind['vol_ma20'][i]
    if vm20 <= 0:
        return False
    if v[i] / vm20 >= 2.5:
        return False

    # 趋势支撑: C > MA5 AND C > MA10
    if c[i] <= ind['ma5'][i] or c[i] <= ind['ma10'][i]:
        return False
    if ind['ma5'][i] <= 0 or ind['ma10'][i] <= 0:
        return False

    return True


# ═══════════════════════════════════════════════════════════════════
#  试盘线策略 (用于交叉对比)
# ═══════════════════════════════════════════════════════════════════

def detect_shipanxian_from_ind(ind: dict, i: int, klines: list[dict]) -> bool:
    """试盘线信号检测（复用已有逻辑，但基于预计算指标）。"""
    if i < 50:
        return False
    c, o, h, l = ind['closes'][i], ind['opens'][i], ind['highs'][i], ind['lows'][i]
    v = ind['vols'][i]
    prev_c = ind['closes'][i - 1]
    prev_v = ind['vols'][i - 1]

    if c <= 0 or prev_c <= 0 or prev_v <= 0:
        return False

    # 上影线 > 2.5%
    if (h - max(o, c)) / c <= 0.025:
        return False
    # 最高涨幅 > 7%
    if (h - prev_c) / prev_c <= 0.07:
        return False
    # 收盘涨幅 > 3%
    if (c - prev_c) / prev_c <= 0.03:
        return False
    # 股价低位 CLOSE/LLV(LOW,50) < 1.4
    llv50 = min(ind['lows'][j] for j in range(i - 49, i + 1))
    if llv50 <= 0 or c / llv50 >= 1.4:
        return False
    # 收盘5日新高
    if c < max(ind['closes'][j] for j in range(i - 4, i + 1)):
        return False
    # 量倍增
    if v / prev_v <= 2:
        return False
    # 量5日新高
    if v < max(ind['vols'][j] for j in range(i - 4, i + 1)):
        return False
    # 低价低位
    llv20 = min(ind['lows'][j] for j in range(i - 19, i + 1))
    if llv20 <= 0 or l / llv20 >= 1.2:
        return False
    # 低价高位
    hhv20 = max(ind['highs'][j] for j in range(i - 19, i + 1))
    if hhv20 <= 0 or l / hhv20 <= 0.9:
        return False

    return True


# ═══════════════════════════════════════════════════════════════════
#  策略注册表
# ═══════════════════════════════════════════════════════════════════

STRATEGIES = {
    'S1_暴力洗盘': detect_s1_baoli_xipan,
    'S2_堆量挖坑': detect_s2_duiliang_wakeng,
    'S3_蜻蜓点水': detect_s3_qingting_dianshui,
    'S4_出水芙蓉': detect_s4_chushui_furong,
    'S5_店大欺客': detect_s5_dianda_qike,
}

HOLD_DAYS = [1, 3, 5, 7, 10]


# ═══════════════════════════════════════════════════════════════════
#  单股回测核心
# ═══════════════════════════════════════════════════════════════════

def backtest_stock_five(stock_code: str) -> list[dict]:
    """对单只股票运行全部5个策略+试盘线，收集信号。"""
    klines = get_kline_data(stock_code, limit=500)
    if len(klines) < 260:
        return []

    ind = precompute_indicators(klines)
    if ind is None:
        return []

    signals = []
    n = ind['n']

    for i in range(250, n):
        # 检测每个策略
        triggered = {}
        for sname, sfunc in STRATEGIES.items():
            try:
                triggered[sname] = sfunc(ind, i, stock_code)
            except Exception:
                triggered[sname] = False

        # 试盘线
        try:
            triggered['S0_试盘线'] = detect_shipanxian_from_ind(ind, i, klines)
        except Exception:
            triggered['S0_试盘线'] = False

        # 如果没有任何策略触发，跳过
        if not any(triggered.values()):
            continue

        # 次日开盘买入
        if i + 1 >= n:
            continue
        buy_price = float(klines[i + 1]['open_price'])
        if buy_price <= 0:
            continue

        sig = {
            'stock_code': stock_code,
            'signal_date': str(klines[i]['date']),
            'signal_close': ind['closes'][i],
            'buy_price': buy_price,
        }

        # 各持有期收益
        for hd in HOLD_DAYS:
            sell_idx = i + 1 + hd
            if sell_idx < n:
                sell_price = float(klines[sell_idx]['close_price'])
                sig[f'return_{hd}d'] = round((sell_price - buy_price) / buy_price * 100, 2)

        # 记录各策略触发状态
        for sname, hit in triggered.items():
            sig[f'hit_{sname}'] = hit

        # 投票数（5策略中触发几个）
        vote_count = sum(1 for sn in STRATEGIES if triggered.get(sn, False))
        sig['vote_count'] = vote_count

        signals.append(sig)

    return signals


# ═══════════════════════════════════════════════════════════════════
#  交叉验证 (按季度分fold)
# ═══════════════════════════════════════════════════════════════════

def time_series_cv(all_signals: list[dict], strategy_name: str, hd: int = 5) -> dict:
    """按季度做时间序列交叉验证，返回各fold和汇总统计。"""
    key = f'return_{hd}d'
    hit_key = f'hit_{strategy_name}'

    filtered = [s for s in all_signals if s.get(hit_key, False) and key in s]
    if not filtered:
        return {'folds': [], 'overall': {'count': 0}}

    # 按季度分组
    quarters = defaultdict(list)
    for s in filtered:
        d = s['signal_date']
        y, m = d[:4], int(d[5:7])
        q = f"{y}Q{(m - 1) // 3 + 1}"
        quarters[q].append(s[key])

    sorted_qs = sorted(quarters.keys())
    folds = []
    for q in sorted_qs:
        rets = quarters[q]
        wins = sum(1 for r in rets if r > 0)
        total = len(rets)
        folds.append({
            'quarter': q,
            'count': total,
            'win_rate': round(wins / total * 100, 1) if total > 0 else 0,
            'avg_return': round(sum(rets) / total, 2) if total > 0 else 0,
        })

    # 汇总
    all_rets = [s[key] for s in filtered]
    total = len(all_rets)
    wins = sum(1 for r in all_rets if r > 0)
    avg = sum(all_rets) / total
    sorted_r = sorted(all_rets)

    return {
        'folds': folds,
        'overall': {
            'count': total,
            'win_rate': round(wins / total * 100, 1),
            'avg_return': round(avg, 2),
            'median_return': round(sorted_r[total // 2], 2),
            'max_return': round(max(all_rets), 2),
            'min_return': round(min(all_rets), 2),
        }
    }


# ═══════════════════════════════════════════════════════════════════
#  主回测入口
# ═══════════════════════════════════════════════════════════════════

def run_five_strategy_backtest(sample_limit: int = 300):
    """主入口：运行5策略交叉验证回测。"""
    t_start = datetime.now()
    logger.info("=" * 80)
    logger.info("  5大选股策略 周级别交叉验证回测")
    logger.info("  策略: 暴力洗盘 | 堆量挖坑 | 蜻蜓点水 | 出水芙蓉 | 店大欺客")
    logger.info("  对照: 试盘线")
    logger.info("  持有天数: %s | 样本上限: %d", HOLD_DAYS, sample_limit)
    logger.info("=" * 80)

    all_codes = sorted(get_all_stock_codes())
    if sample_limit > 0:
        all_codes = all_codes[:sample_limit]
    logger.info("回测股票数: %d", len(all_codes))

    all_signals = []
    for idx, code in enumerate(all_codes):
        if (idx + 1) % 50 == 0:
            logger.info("  进度: %d/%d (信号累计: %d)", idx + 1, len(all_codes), len(all_signals))
        sigs = backtest_stock_five(code)
        all_signals.extend(sigs)

    logger.info("  总信号数: %d", len(all_signals))
    if not all_signals:
        logger.info("  无信号，结束。")
        return

    # ─── 1. 各策略独立表现 ─────────────────────────────────
    all_strategy_names = list(STRATEGIES.keys()) + ['S0_试盘线']

    logger.info("")
    logger.info("=" * 80)
    logger.info("  [1] 各策略独立表现 (5日=约一周)")
    logger.info("=" * 80)
    logger.info("  %-14s %6s  %7s  %7s  %7s  %7s  %7s",
                "策略", "信号数", "1d胜率", "3d胜率", "5d胜率", "7d胜率", "10d胜率")
    logger.info("  " + "-" * 75)

    strategy_stats = {}
    for sname in all_strategy_names:
        hit_key = f'hit_{sname}'
        filtered = [s for s in all_signals if s.get(hit_key, False)]
        stats_by_hd = {}
        for hd in HOLD_DAYS:
            stats_by_hd[hd] = calc_stats(filtered, hd)
        strategy_stats[sname] = stats_by_hd

        s5 = stats_by_hd[5]
        if s5['count'] > 0:
            wr_strs = []
            for hd in HOLD_DAYS:
                st = stats_by_hd[hd]
                wr_strs.append(f"{st['win_rate']:6.1f}%" if st['count'] > 0 else "     -")
            logger.info("  %-14s %6d  %s", sname, s5['count'], "  ".join(wr_strs))
        else:
            logger.info("  %-14s %6d  (无信号)", sname, 0)

    # ─── 2. 各策略5日收益详情 ──────────────────────────────
    logger.info("")
    logger.info("=" * 80)
    logger.info("  [2] 各策略5日(一周)收益详情")
    logger.info("=" * 80)
    logger.info("  %-14s %6s  %7s  %7s  %7s  %8s  %8s",
                "策略", "信号", "胜率", "均收益", "中位数", "最大", "最小")
    logger.info("  " + "-" * 75)

    for sname in all_strategy_names:
        s5 = strategy_stats[sname][5]
        if s5['count'] > 0:
            logger.info("  %-14s %6d  %6.1f%%  %6.2f%%  %6.2f%%  %7.2f%%  %7.2f%%",
                         sname, s5['count'], s5['win_rate'], s5['avg_return'],
                         s5['median_return'], s5['max_return'], s5['min_return'])

    # ─── 3. 时间序列交叉验证 ──────────────────────────────
    logger.info("")
    logger.info("=" * 80)
    logger.info("  [3] 时间序列交叉验证 (按季度fold, 5日持有)")
    logger.info("=" * 80)

    for sname in all_strategy_names:
        cv = time_series_cv(all_signals, sname, hd=5)
        if cv['overall']['count'] == 0:
            continue
        logger.info("")
        logger.info("  ── %s ──", sname)
        logger.info("    %-8s %5s  %7s  %7s", "季度", "信号", "胜率", "均收益")
        for f in cv['folds']:
            logger.info("    %-8s %5d  %6.1f%%  %6.2f%%",
                         f['quarter'], f['count'], f['win_rate'], f['avg_return'])
        o = cv['overall']
        logger.info("    %-8s %5d  %6.1f%%  %6.2f%%  (中位%.2f%% 最大%.2f%% 最小%.2f%%)",
                     "汇总", o['count'], o['win_rate'], o['avg_return'],
                     o['median_return'], o['max_return'], o['min_return'])

    # ─── 4. 多策略投票组合 ─────────────────────────────────
    logger.info("")
    logger.info("=" * 80)
    logger.info("  [4] 多策略投票组合 (5策略中N个同时触发)")
    logger.info("=" * 80)
    logger.info("  %-20s %6s  %7s  %7s  %7s",
                "组合", "信号", "5d胜率", "5d均收", "5d中位")
    logger.info("  " + "-" * 60)

    for min_votes in [1, 2, 3]:
        filtered = [s for s in all_signals if s.get('vote_count', 0) >= min_votes]
        s5 = calc_stats(filtered, 5)
        if s5['count'] > 0:
            logger.info("  >=%-2d策略触发       %6d  %6.1f%%  %6.2f%%  %6.2f%%",
                         min_votes, s5['count'], s5['win_rate'],
                         s5['avg_return'], s5['median_return'])

    # ─── 5. 策略两两交叉组合 ──────────────────────────────
    logger.info("")
    logger.info("=" * 80)
    logger.info("  [5] 策略两两交叉组合 (AND逻辑)")
    logger.info("=" * 80)
    logger.info("  %-28s %5s  %7s  %7s",
                "组合", "信号", "5d胜率", "5d均收")
    logger.info("  " + "-" * 55)

    snames = list(STRATEGIES.keys())
    for a in range(len(snames)):
        for b in range(a + 1, len(snames)):
            sa, sb = snames[a], snames[b]
            filtered = [s for s in all_signals
                        if s.get(f'hit_{sa}', False) and s.get(f'hit_{sb}', False)]
            s5 = calc_stats(filtered, 5)
            if s5['count'] >= 3:
                logger.info("  %-28s %5d  %6.1f%%  %6.2f%%",
                             f"{sa} + {sb}", s5['count'], s5['win_rate'], s5['avg_return'])

    # ─── 6. 与试盘线交叉验证 ──────────────────────────────
    logger.info("")
    logger.info("=" * 80)
    logger.info("  [6] 5策略 × 试盘线 交叉增强")
    logger.info("=" * 80)
    logger.info("  %-28s %5s  %7s  %7s",
                "组合", "信号", "5d胜率", "5d均收")
    logger.info("  " + "-" * 55)

    for sname in STRATEGIES:
        filtered = [s for s in all_signals
                    if s.get(f'hit_{sname}', False) and s.get('hit_S0_试盘线', False)]
        s5 = calc_stats(filtered, 5)
        if s5['count'] >= 1:
            logger.info("  %-28s %5d  %6.1f%%  %6.2f%%",
                         f"{sname} + 试盘线", s5['count'], s5['win_rate'], s5['avg_return'])

    # ─── 7. 月度稳定性分析 ────────────────────────────────
    logger.info("")
    logger.info("=" * 80)
    logger.info("  [7] 各策略月度胜率稳定性 (5日持有)")
    logger.info("=" * 80)

    for sname in all_strategy_names:
        hit_key = f'hit_{sname}'
        filtered = [s for s in all_signals if s.get(hit_key, False) and 'return_5d' in s]
        if len(filtered) < 5:
            continue

        monthly = defaultdict(list)
        for s in filtered:
            month = s['signal_date'][:7]
            monthly[month].append(s['return_5d'])

        months_positive = sum(1 for rets in monthly.values()
                              if sum(1 for r in rets if r > 0) / len(rets) > 0.5)
        total_months = len(monthly)
        stability = months_positive / total_months * 100 if total_months > 0 else 0

        logger.info("  %s: %d个月中%d个月胜率>50%% (稳定性%.1f%%)",
                     sname, total_months, months_positive, stability)

    # ─── 8. 结论 ──────────────────────────────────────────
    logger.info("")
    logger.info("=" * 80)
    logger.info("  [8] 综合结论")
    logger.info("=" * 80)

    # 找最佳单策略
    best_single = None
    best_wr = 0
    for sname in all_strategy_names:
        s5 = strategy_stats[sname][5]
        if s5['count'] >= 10 and s5['win_rate'] > best_wr:
            best_wr = s5['win_rate']
            best_single = sname

    if best_single:
        s5 = strategy_stats[best_single][5]
        logger.info("  最佳单策略: %s (胜率%.1f%%, 均收益%.2f%%, %d信号)",
                     best_single, s5['win_rate'], s5['avg_return'], s5['count'])

    # 找最佳投票组合
    for min_v in [3, 2]:
        filtered = [s for s in all_signals if s.get('vote_count', 0) >= min_v]
        s5 = calc_stats(filtered, 5)
        if s5['count'] >= 5:
            logger.info("  最佳投票组合: >=%d策略触发 (胜率%.1f%%, 均收益%.2f%%, %d信号)",
                         min_v, s5['win_rate'], s5['avg_return'], s5['count'])
            break

    elapsed = (datetime.now() - t_start).total_seconds()
    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 80)

    # 保存结果
    result_summary = {
        'run_time': str(datetime.now()),
        'total_signals': len(all_signals),
        'stock_count': len(all_codes),
        'strategies': {},
    }
    for sname in all_strategy_names:
        s5 = strategy_stats[sname][5]
        result_summary['strategies'][sname] = s5

    try:
        with open('data_results/five_strategy_cv_result.json', 'w', encoding='utf-8') as f:
            json.dump(result_summary, f, ensure_ascii=False, indent=2)
        logger.info("  结果已保存到 data_results/five_strategy_cv_result.json")
    except Exception as e:
        logger.warning("  保存结果失败: %s", e)

    return all_signals


if __name__ == '__main__':
    run_five_strategy_backtest(sample_limit=300)
