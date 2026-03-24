#!/usr/bin/env python3
"""
量价关系理论 V2 — 优化增强 + 防过拟合验证
==========================================
在V1的17个场景基础上，引入辅助过滤条件提高准确率，
同时用严格的样本外测试和滚动验证防止过拟合。

优化方向（基于V1回测发现的问题）：
  1. 均线趋势过滤：位置判断从"静态区间"升级为"动态趋势"
  2. 量能连续性：不只看5日均量，还看量能是否持续放大/缩小
  3. 大盘环境过滤：牛市/熊市/震荡市下分别验证
  4. 换手率辅助：区分真实换手和对倒
  5. 波动率归一化：高波动股和低波动股分开统计
  6. 修正预期方向：基于V1数据反转不合理的预期

防过拟合措施：
  - 前半段(探索期) vs 后半段(验证期) 样本外测试
  - 按月滚动一致性检验
  - 最小样本量要求(每月≥20)
  - 不优化阈值，只用学术文献中的标准值

用法：
    source .venv/bin/activate
    python -m tools.backtest_volume_price_v2
"""
import json
import logging
import sys
import time
import math
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dao import get_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent.parent / "data_results"


def _f(v):
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def load_stock_codes(limit=2000):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT stock_code FROM stock_kline "
        "WHERE stock_code NOT LIKE '4%%' AND stock_code NOT LIKE '8%%' "
        "AND stock_code NOT LIKE '9%%' "
        "ORDER BY stock_code LIMIT %s", (limit,))
    codes = [r['stock_code'] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return codes


def load_kline_data(stock_codes, start_date, end_date):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = defaultdict(list)
    bs = 300
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, close_price, open_price, high_price, "
            f"low_price, trading_volume, change_percent, change_hand "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [start_date, end_date])
        for row in cur.fetchall():
            result[row['stock_code']].append({
                'date': str(row['date']),
                'close': _f(row['close_price']),
                'open': _f(row['open_price']),
                'high': _f(row['high_price']),
                'low': _f(row['low_price']),
                'volume': _f(row['trading_volume']),
                'pct': _f(row['change_percent']),
                'turnover': _f(row.get('change_hand')),
            })
    cur.close()
    conn.close()
    return dict(result)


def load_market_index(start_date, end_date):
    """加载上证指数作为大盘基准"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute(
        "SELECT `date`, close_price, change_percent FROM stock_kline "
        "WHERE stock_code = '000001' AND `date` >= %s AND `date` <= %s "
        "ORDER BY `date`", (start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{'date': str(r['date']), 'close': _f(r['close_price']),
             'pct': _f(r['change_percent'])} for r in rows]


# ═══════════════════════════════════════════════════════════════
# V2 增强特征计算
# ═══════════════════════════════════════════════════════════════

def compute_features(klines):
    """
    计算完整的量价特征集，返回 dict 或 None。
    所有阈值使用学术标准值，不做优化。
    """
    n = len(klines)
    if n < 60:
        return None

    close = [k['close'] for k in klines]
    volume = [k['volume'] for k in klines]
    pct = [k['pct'] for k in klines]
    turnover = [k['turnover'] for k in klines]
    high = [k['high'] for k in klines]
    low = [k['low'] for k in klines]
    opn = [k['open'] for k in klines]

    if close[-1] <= 0 or volume[-1] <= 0:
        return None

    # ── 1. 位置判断（增强版：结合均线趋势）──
    # 60日区间位置
    h60 = max(h for h in high[-60:] if h > 0) if any(h > 0 for h in high[-60:]) else 0
    l60 = min(l for l in low[-60:] if l > 0) if any(l > 0 for l in low[-60:]) else 0
    if h60 <= l60 or h60 == 0:
        return None
    pos_pct = (close[-1] - l60) / (h60 - l60)

    # 均线趋势：MA5 vs MA20 vs MA60
    ma5 = sum(close[-5:]) / 5
    ma20 = sum(close[-20:]) / 20
    ma60 = sum(close[-60:]) / 60

    # 均线多头排列：MA5 > MA20 > MA60
    ma_bullish = ma5 > ma20 > ma60
    # 均线空头排列：MA5 < MA20 < MA60
    ma_bearish = ma5 < ma20 < ma60
    # MA20斜率（20日前vs现在）
    if n >= 40:
        ma20_prev = sum(close[-40:-20]) / 20
        ma20_slope = (ma20 / ma20_prev - 1) * 100 if ma20_prev > 0 else 0
    else:
        ma20_slope = 0

    # 综合位置判断
    if pos_pct <= 0.33:
        position = 'low'
    elif pos_pct >= 0.67:
        position = 'high'
    else:
        position = 'mid'

    # ── 2. 成交量分析（增强版：连续性 + 换手率）──
    vol_20 = sum(volume[-20:]) / 20
    vol_5 = sum(volume[-5:]) / 5
    if vol_20 <= 0:
        return None
    vol_ratio = vol_5 / vol_20

    # 量能连续性：5日内每日量是否持续放大
    vol_expanding = all(volume[-i] >= volume[-i-1] * 0.9 for i in range(1, 4)
                        if volume[-i-1] > 0)
    vol_contracting = all(volume[-i] <= volume[-i-1] * 1.1 for i in range(1, 4)
                          if volume[-i-1] > 0)

    # 换手率分析
    turn_5 = sum(turnover[-5:]) / 5 if all(t >= 0 for t in turnover[-5:]) else 0
    turn_20 = sum(turnover[-20:]) / 20 if all(t >= 0 for t in turnover[-20:]) else 0
    turn_ratio = turn_5 / turn_20 if turn_20 > 0 else 1

    # 量能状态
    if vol_ratio >= 1.5:
        vol_state = 'high_vol'
    elif vol_ratio <= 0.7:
        vol_state = 'low_vol'
    else:
        vol_state = 'normal_vol'

    # ── 3. 价格走势（增强版：考虑K线形态）──
    base_close = close[-6] if close[-6] > 0 else close[-5]
    if base_close <= 0:
        return None
    ret_5d = (close[-1] / base_close - 1) * 100

    if ret_5d > 3:
        price_action = 'rising'
    elif ret_5d < -3:
        price_action = 'falling'
    else:
        price_action = 'flat'

    # ── 4. 量价效率 ──
    price_change_abs = abs(ret_5d)
    efficiency = price_change_abs / vol_ratio if vol_ratio > 0.01 else 0

    # ── 5. 波动率（用于归一化）──
    if n >= 20:
        rets_20 = [(close[i] / close[i-1] - 1) * 100 for i in range(n-20, n)
                    if close[i-1] > 0]
        if rets_20:
            mean_r = sum(rets_20) / len(rets_20)
            volatility = (sum((r - mean_r)**2 for r in rets_20) / len(rets_20)) ** 0.5
        else:
            volatility = 0
    else:
        volatility = 0

    # 波动率分类
    if volatility > 3:
        vol_class = 'high_volatility'
    elif volatility < 1.5:
        vol_class = 'low_volatility'
    else:
        vol_class = 'normal_volatility'

    # ── 6. K线形态辅助 ──
    # 近5日长下影线数量
    long_lower_shadows = 0
    long_upper_shadows = 0
    for i in range(-5, 0):
        body = abs(close[i] - opn[i])
        lower = min(close[i], opn[i]) - low[i] if low[i] > 0 else 0
        upper = high[i] - max(close[i], opn[i]) if high[i] > 0 else 0
        if body > 0:
            if lower > body * 1.5:
                long_lower_shadows += 1
            if upper > body * 1.5:
                long_upper_shadows += 1

    # ── 7. 前期出货痕迹 ──
    has_distribution = False
    if n >= 25:
        prior = klines[-25:-5]
        if len(prior) >= 10:
            prior_vol_avg = sum(k['volume'] for k in prior if k['volume'] > 0) / max(len(prior), 1)
            stagnation = sum(1 for k in prior[-10:]
                            if k['volume'] > prior_vol_avg * 1.5 and abs(k['pct']) < 1)
            reversals = sum(1 for k in prior[-10:]
                           if k['high'] > 0 and
                           (k['high'] - max(k['close'], k['open'])) > abs(k['close'] - k['open']) * 1.5)
            has_distribution = stagnation >= 2 or reversals >= 2

    # ── 8. (已移除) ──

    # ── 9. 连续涨跌天数 ──
    consec_up = 0
    consec_down = 0
    for i in range(n - 1, max(n - 10, 0), -1):
        if pct[i] > 0:
            if consec_down == 0:
                consec_up += 1
            else:
                break
        elif pct[i] < 0:
            if consec_up == 0:
                consec_down += 1
            else:
                break
        else:
            break

    return {
        'position': position,
        'pos_pct': round(pos_pct, 3),
        'ma_bullish': ma_bullish,
        'ma_bearish': ma_bearish,
        'ma20_slope': round(ma20_slope, 2),
        'vol_state': vol_state,
        'vol_ratio': round(vol_ratio, 3),
        'vol_expanding': vol_expanding,
        'vol_contracting': vol_contracting,
        'turn_5': round(turn_5, 2),
        'turn_ratio': round(turn_ratio, 2),
        'price_action': price_action,
        'ret_5d': round(ret_5d, 2),
        'efficiency': round(efficiency, 2),
        'volatility': round(volatility, 2),
        'vol_class': vol_class,
        'long_lower_shadows': long_lower_shadows,
        'long_upper_shadows': long_upper_shadows,
        'has_distribution': has_distribution,
        'consec_up': consec_up,
        'consec_down': consec_down,
    }


# ═══════════════════════════════════════════════════════════════
# V2 场景分类 + 增强过滤条件
# ═══════════════════════════════════════════════════════════════

def classify_v2(feat):
    """
    V2场景分类：在原始场景基础上叠加增强条件。
    返回 (base_scenario, enhanced_scenario, expected_direction)
    
    增强逻辑基于V1回测发现：
    - 低位放量上涨(A1)失败率高 → 需要均线确认
    - 高位放量上涨(A2a)失败率高 → 需要反转预期
    - 缩量下跌(B3)反向 → 修正为看涨
    - 低位放量下跌(B1b)反向 → 修正为看涨
    """
    pos = feat['position']
    vol = feat['vol_state']
    pa = feat['price_action']
    
    if vol == 'normal_vol':
        return None, None, None  # 正常量不分类

    base = None
    enhanced = None
    expected = None

    # ── 上涨阶段 ──
    if pa == 'rising':
        if pos == 'low' and vol == 'high_vol':
            base = 'A1_低位放量上涨'
            # V1发现：低位放量上涨准确率仅44%
            # 增强条件：需要MA20向上 + 量能持续放大 → 真启动
            if feat['ma20_slope'] > 0 and feat['vol_expanding']:
                enhanced = 'A1+_低位放量上涨_均线确认'
                expected = 'UP'
            elif feat['ma_bearish']:
                # 均线空头中的放量反弹 → 大概率是反弹非反转
                enhanced = 'A1-_低位放量上涨_均线空头'
                expected = 'DOWN'
            else:
                enhanced = 'A1_低位放量上涨_基础'
                expected = 'UP'

        elif pos == 'high' and vol == 'high_vol':
            base = 'A2_高位放量上涨'
            # V1发现：高位放量上涨后大概率回调(41.7%)
            # 修正：高位放量上涨 → 预期回调（均值回归）
            if feat['efficiency'] > 3:
                # 高效上涨但在高位 → 短期可能继续但风险大
                if feat['consec_up'] >= 3:
                    enhanced = 'A2a-_高位放量连涨_过热'
                    expected = 'DOWN'  # 连涨过热，回调概率大
                else:
                    enhanced = 'A2a_高位放量高效上涨'
                    expected = 'DOWN'  # V1数据修正：高位放量→回调
            else:
                enhanced = 'A2b_高位放量低效上涨'
                expected = 'DOWN'

        elif pos == 'low' and vol == 'low_vol':
            base = 'A3_低位缩量上涨'
            if feat['ma20_slope'] < -3:
                # 长期下跌趋势中的缩量反弹
                enhanced = 'A3b_低位缩量上涨_趋势下行'
                expected = 'DOWN'
            else:
                enhanced = 'A3_低位缩量上涨_基础'
                expected = 'NEUTRAL'

        elif pos == 'high' and vol == 'low_vol':
            base = 'A4_高位缩量上涨'
            if feat['ret_5d'] > 5:
                # 高位缩量大涨：V1验证52.8%，增强条件
                if feat['turn_5'] < 3:
                    enhanced = 'A4a+_高位缩量大涨_低换手'
                    expected = 'UP'  # 筹码锁定好
                else:
                    enhanced = 'A4a_高位缩量大涨'
                    expected = 'UP'
            else:
                if feat['turn_5'] < 3:
                    enhanced = 'A4b+_高位缩量小涨_低换手'
                    expected = 'NEUTRAL'
                else:
                    enhanced = 'A4b_高位缩量小涨'
                    expected = 'NEUTRAL'

    # ── 下跌阶段 ──
    elif pa == 'falling':
        if pos == 'low' and vol == 'high_vol':
            base = 'B1_低位放量下跌'
            # V1发现：低位放量下跌后反而容易反弹
            if feat['long_lower_shadows'] >= 2:
                enhanced = 'B1a_低位放量下跌_暴力换手'
                expected = 'UP'
            else:
                # V1修正：低位放量下跌(B1b)预期DOWN仅44.7%
                # 实际上低位放量下跌 = 恐慌释放 → 反弹概率更大
                if feat['vol_ratio'] > 2.0 and feat['consec_down'] >= 3:
                    enhanced = 'B1+_低位放量恐慌释放'
                    expected = 'UP'  # 恐慌释放后反弹
                else:
                    enhanced = 'B1b_低位放量下跌'
                    expected = 'NEUTRAL'

        elif pos == 'high' and vol == 'high_vol':
            base = 'B2_高位放量下跌'
            # V1验证54.4%，增强条件
            if feat['has_distribution']:
                enhanced = 'B2+_高位放量下跌_有出货痕迹'
                expected = 'DOWN'
            elif feat['ma_bearish']:
                enhanced = 'B2+_高位放量下跌_均线空头'
                expected = 'DOWN'
            else:
                enhanced = 'B2_高位放量下跌_基础'
                expected = 'DOWN'

        elif pos == 'low' and vol == 'low_vol':
            base = 'B3_缩量下跌'
            # V1发现：缩量下跌预期DOWN仅39.7%，实际均收益+1.08%
            # 修正：低位缩量下跌 = 卖压衰竭 → 看涨
            if feat['vol_contracting'] and feat['consec_down'] >= 3:
                enhanced = 'B3+_低位缩量连跌_卖压衰竭'
                expected = 'UP'  # 连续缩量下跌 = 卖压耗尽
            elif feat['ma20_slope'] < -5:
                enhanced = 'B3-_低位缩量下跌_趋势恶化'
                expected = 'DOWN'
            else:
                enhanced = 'B3_低位缩量下跌_基础'
                expected = 'UP'  # 修正预期

        elif pos == 'high' and vol == 'low_vol':
            base = 'B4_高位缩量下跌'
            if feat['has_distribution']:
                enhanced = 'B4a_高位缩量下跌_出货后'
                expected = 'DOWN'
            else:
                if feat['ma_bullish']:
                    enhanced = 'B4b+_高位缩量下跌_均线多头'
                    expected = 'UP'  # 均线多头中的缩量回调 = 假摔
                else:
                    enhanced = 'B4b_高位缩量下跌_假摔'
                    expected = 'NEUTRAL'

    # ── 横盘阶段 ──
    elif pa == 'flat':
        if pos == 'low' and vol == 'high_vol':
            base = 'C1_低位放量横盘'
            # V1验证52.7%，增强条件
            if feat['vol_expanding'] and feat['ma20_slope'] > -1:
                enhanced = 'C1+_低位放量横盘_持续堆量'
                expected = 'UP'
            else:
                enhanced = 'C1_低位放量横盘_基础'
                expected = 'NEUTRAL'

        elif pos == 'high' and vol == 'high_vol':
            base = 'C2_高位放量横盘'
            if feat['long_upper_shadows'] >= 2:
                enhanced = 'C2-_高位放量横盘_上影线多'
                expected = 'DOWN'  # 上方抛压重
            elif feat['turn_ratio'] > 1.5:
                enhanced = 'C2-_高位放量横盘_换手激增'
                expected = 'DOWN'  # 换手激增 = 分歧大
            else:
                enhanced = 'C2_高位放量横盘_基础'
                expected = 'NEUTRAL'

        elif pos == 'low' and vol == 'low_vol':
            base = 'C3_低位缩量横盘'
            enhanced = 'C3_低位缩量横盘'
            expected = 'FLAT'  # V1已验证65.2%

        elif pos == 'high' and vol == 'low_vol':
            base = 'C4_高位缩量横盘'
            # V1验证54.5%，增强条件
            if feat['ma_bullish'] and feat['ma20_slope'] > 0:
                enhanced = 'C4+_高位缩量横盘_均线多头'
                expected = 'UP'
            else:
                enhanced = 'C4_高位缩量横盘_基础'
                expected = 'NEUTRAL'

    return base, enhanced, expected


# ═══════════════════════════════════════════════════════════════
# 未来收益计算
# ═══════════════════════════════════════════════════════════════

def compute_future(klines, idx):
    base = klines[idx]['close']
    if base <= 0:
        return None
    r = {}
    for h in (5, 10, 20):
        if idx + h < len(klines) and klines[idx + h]['close'] > 0:
            r[f'ret_{h}d'] = round((klines[idx + h]['close'] / base - 1) * 100, 2)
    if idx + 10 < len(klines):
        sl = klines[idx + 1: idx + 11]
        vh = [k['high'] for k in sl if k.get('high', 0) > 0]
        vl = [k['low'] for k in sl if k.get('low', 0) > 0]
        r['max_up_10d'] = round((max(vh) / base - 1) * 100, 2) if vh else 0
        r['max_dn_10d'] = round((min(vl) / base - 1) * 100, 2) if vl else 0
    return r if r else None


# ═══════════════════════════════════════════════════════════════
# 统计工具
# ═══════════════════════════════════════════════════════════════

def calc_stats(records, expected_dir):
    """计算一组记录的统计指标"""
    if not records:
        return None
    rets_5 = [r['future']['ret_5d'] for r in records if 'ret_5d' in r['future']]
    rets_10 = [r['future']['ret_10d'] for r in records if 'ret_10d' in r['future']]
    n = len(rets_5)
    if n == 0:
        return None

    avg_5 = sum(rets_5) / n
    avg_10 = sum(rets_10) / len(rets_10) if rets_10 else 0
    median_5 = sorted(rets_5)[n // 2]

    if expected_dir == 'UP':
        correct = sum(1 for r in rets_5 if r > 0)
    elif expected_dir == 'DOWN':
        correct = sum(1 for r in rets_5 if r < 0)
    elif expected_dir == 'FLAT':
        correct = sum(1 for r in rets_5 if abs(r) < 3)
    else:  # NEUTRAL
        correct = sum(1 for r in rets_5 if r > 0)  # 默认看涨方向

    acc = correct / n

    # 盈亏比
    if expected_dir == 'UP' or expected_dir == 'NEUTRAL':
        wins = [r for r in rets_5 if r > 0]
        losses = [-r for r in rets_5 if r < 0]
    elif expected_dir == 'DOWN':
        wins = [-r for r in rets_5 if r < 0]
        losses = [r for r in rets_5 if r > 0]
    else:
        wins = losses = []

    avg_win = sum(wins) / len(wins) if wins else 0
    avg_loss = sum(losses) / len(losses) if losses else 0.001
    plr = round(avg_win / avg_loss, 2) if avg_loss > 0 else 99

    # 期望收益 = 胜率 × 平均盈利 - 败率 × 平均亏损
    expected_pnl = acc * avg_win - (1 - acc) * avg_loss

    return {
        'n': n,
        'acc_5d': round(acc, 4),
        'avg_ret_5d': round(avg_5, 2),
        'avg_ret_10d': round(avg_10, 2),
        'median_5d': round(median_5, 2),
        'plr': plr,
        'expected_pnl': round(expected_pnl, 2),
    }


# ═══════════════════════════════════════════════════════════════
# 主回测 + 防过拟合验证
# ═══════════════════════════════════════════════════════════════

def run_backtest_v2():
    t0 = time.time()
    print("=" * 75)
    print("量价关系理论 V2 — 优化增强 + 防过拟合验证")
    print("=" * 75)

    # ── 加载数据 ──
    logger.info("[1/5] 加载数据...")
    stock_codes = load_stock_codes(2000)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=400)).strftime('%Y-%m-%d')
    kline_data = load_kline_data(stock_codes, start_date, end_date)
    market = load_market_index(start_date, end_date)
    logger.info("  %d只股票, 大盘%d日", len(kline_data), len(market))

    # 大盘环境分类（按月）
    mkt_monthly = defaultdict(list)
    for m in market:
        month = m['date'][:7]
        mkt_monthly[month].append(m['pct'])
    mkt_regime = {}
    for month, pcts in mkt_monthly.items():
        total = sum(pcts)
        if total > 3:
            mkt_regime[month] = 'bull'
        elif total < -3:
            mkt_regime[month] = 'bear'
        else:
            mkt_regime[month] = 'sideways'

    # ── 扫描所有场景 ──
    logger.info("[2/5] 扫描量价场景...")
    all_records = []
    total_scanned = 0

    for code, klines in kline_data.items():
        if len(klines) < 80:
            continue
        for i in range(60, len(klines) - 10):
            total_scanned += 1
            hist = klines[:i + 1]
            feat = compute_features(hist)
            if feat is None:
                continue
            base, enhanced, expected = classify_v2(feat)
            if enhanced is None:
                continue
            future = compute_future(klines, i)
            if future is None:
                continue
            date_str = klines[i]['date']
            month = date_str[:7]
            regime = mkt_regime.get(month, 'unknown')

            all_records.append({
                'code': code,
                'date': date_str,
                'month': month,
                'regime': regime,
                'base': base,
                'enhanced': enhanced,
                'expected': expected,
                'feat': feat,
                'future': future,
            })

    logger.info("  扫描%d日, 分类%d条 (%.1f%%)",
                total_scanned, len(all_records),
                len(all_records) / total_scanned * 100 if total_scanned else 0)

    # ── 样本外测试：前半段探索 vs 后半段验证 ──
    logger.info("[3/5] 样本外测试...")
    all_months = sorted(set(r['month'] for r in all_records))
    mid = len(all_months) // 2
    explore_months = set(all_months[:mid])
    validate_months = set(all_months[mid:])

    explore_records = [r for r in all_records if r['month'] in explore_months]
    validate_records = [r for r in all_records if r['month'] in validate_months]

    logger.info("  探索期: %s ~ %s (%d条)", all_months[0], all_months[mid-1], len(explore_records))
    logger.info("  验证期: %s ~ %s (%d条)", all_months[mid], all_months[-1], len(validate_records))

    # ── 全量统计 ──
    logger.info("[4/5] 统计各场景...")
    scenarios = sorted(set(r['enhanced'] for r in all_records))
    expected_map = {}
    for r in all_records:
        expected_map[r['enhanced']] = r['expected']

    report = {
        'meta': {
            'total_scanned': total_scanned,
            'total_classified': len(all_records),
            'n_stocks': len(kline_data),
            'date_range': f'{start_date} ~ {end_date}',
            'explore_period': f'{all_months[0]} ~ {all_months[mid-1]}',
            'validate_period': f'{all_months[mid]} ~ {all_months[-1]}',
        },
        'scenarios': {},
    }

    for sc in scenarios:
        exp = expected_map.get(sc, 'UP')
        sc_all = [r for r in all_records if r['enhanced'] == sc]
        sc_explore = [r for r in explore_records if r['enhanced'] == sc]
        sc_validate = [r for r in validate_records if r['enhanced'] == sc]

        stats_all = calc_stats(sc_all, exp)
        stats_explore = calc_stats(sc_explore, exp)
        stats_validate = calc_stats(sc_validate, exp)

        # 按大盘环境分
        by_regime = {}
        for regime in ['bull', 'bear', 'sideways']:
            sc_regime = [r for r in sc_all if r['regime'] == regime]
            by_regime[regime] = calc_stats(sc_regime, exp)

        # 按波动率分
        by_vol_class = {}
        for vc in ['low_volatility', 'normal_volatility', 'high_volatility']:
            sc_vc = [r for r in sc_all if r['feat']['vol_class'] == vc]
            by_vol_class[vc] = calc_stats(sc_vc, exp)

        # 月度一致性
        monthly_accs = []
        for month in all_months:
            sc_month = [r for r in sc_all if r['month'] == month]
            ms = calc_stats(sc_month, exp)
            if ms and ms['n'] >= 20:
                monthly_accs.append(ms['acc_5d'])

        monthly_consistency = None
        if len(monthly_accs) >= 3:
            m_mean = sum(monthly_accs) / len(monthly_accs)
            m_std = (sum((a - m_mean)**2 for a in monthly_accs) / len(monthly_accs)) ** 0.5
            m_win = sum(1 for a in monthly_accs if a > 0.5) / len(monthly_accs)
            monthly_consistency = {
                'n_months': len(monthly_accs),
                'mean_acc': round(m_mean, 4),
                'std': round(m_std, 4),
                'monthly_win_rate': round(m_win, 4),
                'min_acc': round(min(monthly_accs), 4),
                'max_acc': round(max(monthly_accs), 4),
            }

        # 过拟合检测
        overfit_signal = False
        if stats_explore and stats_validate:
            gap = stats_explore['acc_5d'] - stats_validate['acc_5d']
            overfit_signal = gap > 0.05

        report['scenarios'][sc] = {
            'expected': exp,
            'all': stats_all,
            'explore': stats_explore,
            'validate': stats_validate,
            'overfit_gap': round(stats_explore['acc_5d'] - stats_validate['acc_5d'], 4)
                if stats_explore and stats_validate else None,
            'overfit_signal': overfit_signal,
            'by_regime': by_regime,
            'by_volatility': by_vol_class,
            'monthly_consistency': monthly_consistency,
        }

    # ── V1 vs V2 对比 ──
    logger.info("[5/5] V1 vs V2 对比...")
    # 按base场景聚合V2增强版的表现
    base_comparison = defaultdict(lambda: {'v2_records': [], 'v2_enhanced': {}})
    for r in all_records:
        if r['base']:
            base_comparison[r['base']]['v2_records'].append(r)
    for base_name, data in base_comparison.items():
        # V2整体（该base下所有增强场景合并）
        recs = data['v2_records']
        # 按增强场景分
        by_enhanced = defaultdict(list)
        for r in recs:
            by_enhanced[r['enhanced']].append(r)
        for enh, enh_recs in by_enhanced.items():
            exp = expected_map.get(enh, 'UP')
            data['v2_enhanced'][enh] = calc_stats(enh_recs, exp)

    report['base_comparison'] = {
        k: {
            'n_total': len(v['v2_records']),
            'enhanced_breakdown': v['v2_enhanced'],
        }
        for k, v in base_comparison.items()
    }

    # 保存
    output_path = OUTPUT_DIR / "volume_price_v2_backtest.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    # ═══════════════════════════════════════════════════════════
    # 打印结果
    # ═══════════════════════════════════════════════════════════
    print(f"\n数据: {len(kline_data)}只股票, {start_date}~{end_date}")
    print(f"扫描: {total_scanned}日, 分类: {len(all_records)}条")
    print(f"探索期: {all_months[0]}~{all_months[mid-1]} ({len(explore_records)}条)")
    print(f"验证期: {all_months[mid]}~{all_months[-1]} ({len(validate_records)}条)")

    print(f"\n{'─' * 120}")
    print(f"  {'场景':<35s} {'预期':>4s} {'样本':>6s} "
          f"{'全量准确':>8s} {'探索期':>7s} {'验证期':>7s} {'过拟合':>6s} "
          f"{'均收益5d':>8s} {'盈亏比':>6s} {'月胜率':>6s} {'月std':>6s} {'判定':>4s}")
    print(f"  {'─' * 115}")

    # 按准确率排序
    sorted_scenarios = sorted(
        report['scenarios'].items(),
        key=lambda x: x[1]['all']['acc_5d'] if x[1]['all'] else 0,
        reverse=True
    )

    for sc, data in sorted_scenarios:
        s = data['all']
        if not s or s['n'] < 30:
            continue

        exp = data['expected']
        explore_acc = data['explore']['acc_5d'] if data['explore'] else 0
        validate_acc = data['validate']['acc_5d'] if data['validate'] else 0
        gap = data.get('overfit_gap', 0) or 0
        overfit = '⚠️' if data.get('overfit_signal') else '  '

        mc = data.get('monthly_consistency', {})
        m_win = mc.get('monthly_win_rate', 0) if mc else 0
        m_std = mc.get('std', 0) if mc else 0

        # 综合判定
        # 通过条件：验证期准确率>52% + 月胜率>50% + 无过拟合
        if exp == 'FLAT':
            passed = validate_acc > 0.55 and not data.get('overfit_signal')
        else:
            passed = (validate_acc > 0.52 and m_win >= 0.5
                      and not data.get('overfit_signal'))
        verdict = '✅' if passed else ('⚠️' if validate_acc > 0.50 else '❌')

        print(f"  {sc:<35s} {exp:>4s} {s['n']:>6d} "
              f"{s['acc_5d']:>8.1%} {explore_acc:>7.1%} {validate_acc:>7.1%} {overfit:>6s} "
              f"{s['avg_ret_5d']:>+8.2f}% {s['plr']:>6.1f} {m_win:>6.1%} {m_std:>6.1%} {verdict:>4s}")

    # ── 大盘环境分析 ──
    print(f"\n{'─' * 120}")
    print("📊 大盘环境影响（仅显示验证期通过的场景）")
    print(f"{'─' * 120}")

    for sc, data in sorted_scenarios:
        s = data['all']
        if not s or s['n'] < 30:
            continue
        validate_acc = data['validate']['acc_5d'] if data['validate'] else 0
        if validate_acc <= 0.50:
            continue

        regimes = data.get('by_regime', {})
        parts = []
        for regime in ['bull', 'bear', 'sideways']:
            rs = regimes.get(regime)
            if rs and rs['n'] >= 10:
                parts.append(f"{regime}={rs['acc_5d']:.1%}({rs['n']})")
        if parts:
            print(f"  {sc:<35s}: {', '.join(parts)}")

    # ── V1 vs V2 对比 ──
    print(f"\n{'─' * 120}")
    print("🔄 V1 vs V2 对比（按原始场景聚合）")
    print(f"{'─' * 120}")

    # V1 结果
    v1_path = OUTPUT_DIR / "volume_price_theory_backtest.json"
    v1_data = {}
    if v1_path.exists():
        with open(v1_path) as f:
            v1_raw = json.load(f)
            v1_data = v1_raw.get('scenario_analysis', {})

    for base_name in sorted(base_comparison.keys()):
        bc = report['base_comparison'].get(base_name, {})
        v1 = v1_data.get(base_name, {})
        v1_acc = v1.get('accuracy_5d', 0)
        v1_n = v1.get('n', 0)

        print(f"\n  {base_name} (V1: {v1_acc:.1%}, n={v1_n})")
        enhanced = bc.get('enhanced_breakdown', {})
        for enh_name, enh_stats in sorted(enhanced.items(),
                                           key=lambda x: x[1]['acc_5d'] if x[1] else 0,
                                           reverse=True):
            if enh_stats and enh_stats['n'] >= 10:
                delta = enh_stats['acc_5d'] - v1_acc if v1_acc > 0 else 0
                marker = '↑' if delta > 0.03 else ('↓' if delta < -0.03 else '→')
                print(f"    {marker} {enh_name:<40s}: {enh_stats['acc_5d']:.1%} "
                      f"({enh_stats['n']}条) Δ={delta:+.1%}")

    # ── 最终推荐 ──
    print(f"\n{'═' * 120}")
    print("📋 最终推荐：验证期通过 + 月度稳定 + 无过拟合的场景")
    print(f"{'═' * 120}")

    recommended = []
    for sc, data in sorted_scenarios:
        s = data['all']
        if not s or s['n'] < 50:
            continue
        validate_acc = data['validate']['acc_5d'] if data['validate'] else 0
        mc = data.get('monthly_consistency', {})
        m_win = mc.get('monthly_win_rate', 0) if mc else 0
        exp = data['expected']

        if exp == 'FLAT':
            ok = validate_acc > 0.55 and not data.get('overfit_signal')
        else:
            ok = (validate_acc > 0.52 and m_win >= 0.5
                  and not data.get('overfit_signal'))
        if ok:
            recommended.append((sc, data))

    if recommended:
        for sc, data in recommended:
            s = data['all']
            mc = data.get('monthly_consistency', {})
            validate_acc = data['validate']['acc_5d'] if data['validate'] else 0
            print(f"\n  ✅ {sc}")
            print(f"     预期: {data['expected']} | 全量: {s['acc_5d']:.1%} | "
                  f"验证期: {validate_acc:.1%} | 月胜率: {mc.get('monthly_win_rate', 0):.0%} | "
                  f"均收益5d: {s['avg_ret_5d']:+.2f}% | 盈亏比: {s['plr']:.1f} | "
                  f"样本: {s['n']}")
    else:
        print("\n  无场景通过全部验证条件")

    print(f"\n⏱️  耗时: {time.time() - t0:.1f}秒")
    print(f"📁 报告: {output_path}")
    print("=" * 75)

    return report


if __name__ == '__main__':
    run_backtest_v2()
