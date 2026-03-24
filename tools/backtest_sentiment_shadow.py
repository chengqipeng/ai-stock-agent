#!/usr/bin/env python3
"""
情绪面因子回测 v2：影线 + 量价 + 价格位置（逆向修正版）
========================================================
基于 v1 回测发现：A股情绪面信号是逆向指标。
  - 传统"看跌"信号（恐慌情绪）→ 实际对应正收益（反转买入机会）
  - 传统"看涨"信号（乐观情绪）→ 实际对应负收益（追高陷阱）

v2 修正：
  1. 根据 v1 实测数据修正各场景的预期方向
  2. 评分逻辑反转：恐慌情绪 → 看涨，乐观情绪 → 看跌
  3. 增加"逆向情绪强度"作为连续因子

理论基础：
  - CGW (1993): 放量下跌=非信息性交易→反转；缩量下跌=信息性→持续
  - 行为金融: A股散户占比高，情绪过度反应后均值回归
  - 影线 = 盘中多空博弈结果，量 = 博弈参与度，价格位置 = 心理锚定

用法：
    source .venv/bin/activate
    python -m tools.backtest_sentiment_shadow
"""
import json
import logging
import sys
import time
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


def _to_float(v):
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def load_stock_codes(limit=3000):
    """加载股票代码，排除ST、北交所等"""
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
    """批量加载K线数据"""
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
                'close': _to_float(row['close_price']),
                'open': _to_float(row['open_price']),
                'high': _to_float(row['high_price']),
                'low': _to_float(row['low_price']),
                'volume': _to_float(row['trading_volume']),
                'change_percent': _to_float(row['change_percent']),
                'turnover': _to_float(row.get('change_hand')),
            })
    cur.close()
    conn.close()
    return dict(result)


# ═══════════════════════════════════════════════════════════════
# 影线 + 量价 + 价格位置 → 情绪场景分类
# ═══════════════════════════════════════════════════════════════

def compute_shadow_features(klines, lookback=5):
    """
    计算近N日的影线特征（取均值降噪）。
    返回 (avg_upper_shadow_ratio, avg_lower_shadow_ratio, max_upper, max_lower)
    """
    uppers, lowers = [], []
    for k in klines[-lookback:]:
        hl = k['high'] - k['low']
        if hl <= 0:
            continue
        upper = (k['high'] - max(k['close'], k['open'])) / hl
        lower = (min(k['close'], k['open']) - k['low']) / hl
        uppers.append(upper)
        lowers.append(lower)
    if not uppers:
        return None, None, None, None
    return (
        sum(uppers) / len(uppers),
        sum(lowers) / len(lowers),
        max(uppers),
        max(lowers),
    )


def classify_sentiment_scenario(klines):
    """
    将当前K线状态分类为情绪面场景。

    维度：
      1. 价格位置：低位(price_pos < 0.33) / 高位(price_pos > 0.67)
      2. 影线类型：长上影(avg > 0.4) / 长下影(avg > 0.4) / 无明显影线
      3. 成交量：放量(vol_ratio > 1.3) / 缩量(vol_ratio < 0.7)

    返回 (scenario_name, details_dict) 或 (None, None)
    """
    if len(klines) < 60:
        return None, None

    # ── 价格位置 ──
    close_60 = [k['close'] for k in klines[-60:] if k['close'] > 0]
    if not close_60:
        return None, None
    h60 = max(close_60)
    l60 = min(close_60)
    if h60 <= l60:
        return None, None
    current = klines[-1]['close']
    if current <= 0:
        return None, None
    price_pos = (current - l60) / (h60 - l60)

    if price_pos <= 0.33:
        pos_label = 'low'
    elif price_pos >= 0.67:
        pos_label = 'high'
    else:
        return None, None  # 中间位置不参与，减少噪声

    # ── 影线特征（近5日均值）──
    shadow = compute_shadow_features(klines, lookback=5)
    if shadow[0] is None:
        return None, None
    avg_upper, avg_lower, max_upper, max_lower = shadow

    # 判断影线类型：阈值0.4表示影线占K线实体的40%以上
    has_long_upper = avg_upper > 0.35
    has_long_lower = avg_lower > 0.35

    if has_long_upper and has_long_lower:
        shadow_label = 'both'  # 十字星型，上下影线都长
    elif has_long_upper:
        shadow_label = 'upper'
    elif has_long_lower:
        shadow_label = 'lower'
    else:
        return None, None  # 无明显影线，不参与

    # ── 成交量 ──
    vol_20 = [k['volume'] for k in klines[-20:] if k['volume'] > 0]
    vol_5 = [k['volume'] for k in klines[-5:] if k['volume'] > 0]
    if not vol_20 or not vol_5:
        return None, None
    avg_vol_20 = sum(vol_20) / len(vol_20)
    avg_vol_5 = sum(vol_5) / len(vol_5)
    if avg_vol_20 <= 0:
        return None, None
    vol_ratio = avg_vol_5 / avg_vol_20

    if vol_ratio >= 1.3:
        vol_label = 'high_vol'
    elif vol_ratio <= 0.7:
        vol_label = 'low_vol'
    else:
        vol_label = 'normal_vol'

    # ── 近5日涨跌幅 ──
    if len(klines) >= 6 and klines[-6]['close'] > 0:
        ret_5d = (klines[-1]['close'] / klines[-6]['close'] - 1) * 100
    else:
        ret_5d = sum(k['change_percent'] for k in klines[-5:])

    details = {
        'price_pos': round(price_pos, 3),
        'pos_label': pos_label,
        'avg_upper_shadow': round(avg_upper, 3),
        'avg_lower_shadow': round(avg_lower, 3),
        'shadow_label': shadow_label,
        'vol_ratio': round(vol_ratio, 2),
        'vol_label': vol_label,
        'ret_5d': round(ret_5d, 2),
    }

    # ── 场景分类 ──
    # 低位场景
    if pos_label == 'low':
        if shadow_label == 'lower' and vol_label == 'high_vol':
            return 'S1_低位长下影放量', details  # 恐慌抛售被承接
        elif shadow_label == 'lower' and vol_label == 'low_vol':
            return 'S2_低位长下影缩量', details  # 试探性下探
        elif shadow_label == 'upper' and vol_label == 'high_vol':
            return 'S3_低位长上影放量', details  # 反弹受阻但有资金
        elif shadow_label == 'upper' and vol_label == 'low_vol':
            return 'S4_低位长上影缩量', details  # 反弹乏力
        elif shadow_label == 'both' and vol_label == 'high_vol':
            return 'S5_低位十字星放量', details  # 多空激烈博弈
        elif shadow_label == 'both' and vol_label == 'low_vol':
            return 'S6_低位十字星缩量', details  # 犹豫不决
        elif shadow_label == 'lower' and vol_label == 'normal_vol':
            return 'S7_低位长下影正常量', details
        elif shadow_label == 'upper' and vol_label == 'normal_vol':
            return 'S8_低位长上影正常量', details

    # 高位场景
    elif pos_label == 'high':
        if shadow_label == 'upper' and vol_label == 'high_vol':
            return 'S9_高位长上影放量', details   # 冲高回落出货
        elif shadow_label == 'upper' and vol_label == 'low_vol':
            return 'S10_高位长上影缩量', details  # 上攻无力
        elif shadow_label == 'lower' and vol_label == 'high_vol':
            return 'S11_高位长下影放量', details  # 多空分歧大
        elif shadow_label == 'lower' and vol_label == 'low_vol':
            return 'S12_高位长下影缩量', details  # 盘中波动
        elif shadow_label == 'both' and vol_label == 'high_vol':
            return 'S13_高位十字星放量', details  # 变盘信号
        elif shadow_label == 'both' and vol_label == 'low_vol':
            return 'S14_高位十字星缩量', details  # 高位犹豫
        elif shadow_label == 'upper' and vol_label == 'normal_vol':
            return 'S15_高位长上影正常量', details
        elif shadow_label == 'lower' and vol_label == 'normal_vol':
            return 'S16_高位长下影正常量', details

    return None, None


# ═══════════════════════════════════════════════════════════════
# 理论预期
# ═══════════════════════════════════════════════════════════════

SENTIMENT_EXPECTATIONS = {
    # ── 低位场景（v1实测修正）──
    # v1发现：低位长下影放量并非看涨（48.5%），低位长上影缩量反而看涨（62.4%上涨）
    'S1_低位长下影放量':   {'expected': 'DOWN', 'desc': '低位恐慌性抛售，短期惯性下跌'},
    'S2_低位长下影缩量':   {'expected': 'FLAT', 'desc': '试探性下探，方向不明（v1验证70.5%）'},
    'S3_低位长上影放量':   {'expected': 'DOWN', 'desc': '反弹受阻+放量=上方套牢盘抛压'},
    'S4_低位长上影缩量':   {'expected': 'UP',   'desc': '缩量试探上攻，抛压已轻（v1逆向验证）'},
    'S5_低位十字星放量':   {'expected': 'UP',   'desc': '多空激烈博弈，变盘在即'},
    'S6_低位十字星缩量':   {'expected': 'FLAT', 'desc': '犹豫不决，方向不明'},
    'S7_低位长下影正常量': {'expected': 'UP',   'desc': '下方有支撑（v1: 53%，弱信号）'},
    'S8_低位长上影正常量': {'expected': 'UP',   'desc': '低位试探上攻，正常量=非过热（v1: 66.8%涨幅<3%）'},

    # ── 高位场景（v1实测修正）──
    # v1发现：高位长上影放量仅53.5%下跌，高位长上影缩量反而60%上涨
    # 最强信号：高位长下影放量 63.9%下跌，均收益-2.14%
    'S9_高位长上影放量':   {'expected': 'DOWN', 'desc': '冲高回落+放量（v1: 53.5%，弱看跌）'},
    'S10_高位长上影缩量':  {'expected': 'UP',   'desc': '缩量上影=试探性冲高，非出货（v1逆向验证）'},
    'S11_高位长下影放量':  {'expected': 'DOWN', 'desc': '高位放量长下影=恐慌性抛售开始（v1最强信号63.9%）'},
    'S12_高位长下影缩量':  {'expected': 'FLAT', 'desc': '盘中波动，信号弱（v1验证62.1%）'},
    'S13_高位十字星放量':  {'expected': 'DOWN', 'desc': '变盘信号，高位十字星偏空'},
    'S14_高位十字星缩量':  {'expected': 'DOWN', 'desc': '高位犹豫，动能衰减'},
    'S15_高位长上影正常量': {'expected': 'UP',   'desc': '正常量上影=健康回调非出货（v1逆向验证）'},
    'S16_高位长下影正常量': {'expected': 'FLAT', 'desc': '盘中波动，方向不明（v1验证65.9%）'},
}


def compute_future_returns(klines, idx, horizons=(5, 10)):
    """计算未来N日的收益率"""
    base = klines[idx]['close']
    if base <= 0:
        return {}
    rets = {}
    for h in horizons:
        if idx + h < len(klines):
            future = klines[idx + h]['close']
            if future > 0:
                rets[f'ret_{h}d'] = round((future / base - 1) * 100, 2)
    if idx + 10 < len(klines):
        future_slice = klines[idx + 1: idx + 11]
        highs = [k['high'] for k in future_slice if k['high'] > 0]
        lows = [k['low'] for k in future_slice if k['low'] > 0]
        if highs:
            rets['max_gain_10d'] = round((max(highs) / base - 1) * 100, 2)
        if lows:
            rets['max_loss_10d'] = round((min(lows) / base - 1) * 100, 2)
    return rets


# ═══════════════════════════════════════════════════════════════
# 综合情绪评分回测（连续评分 vs 离散场景）
# ═══════════════════════════════════════════════════════════════

def compute_sentiment_score(klines):
    """
    计算综合情绪评分（-1 到 +1），v2逆向修正版。

    核心逻辑（基于v1回测验证）：
      - A股情绪面是逆向指标：恐慌=买入机会，乐观=追高陷阱
      - 高位长下影放量是最强看跌信号（v1: 63.9%准确率）
      - 低位长上影缩量反而看涨（v1: 62.4%上涨）
      - 高位长上影缩量反而看涨（v1: 60%上涨）

    正分 = 看涨，负分 = 看跌
    """
    if len(klines) < 60:
        return None

    close = [k['close'] for k in klines]
    volume = [k['volume'] for k in klines]

    # 价格位置
    close_60 = [c for c in close[-60:] if c > 0]
    if not close_60:
        return None
    h60, l60 = max(close_60), min(close_60)
    if h60 <= l60:
        return None
    price_pos = (close[-1] - l60) / (h60 - l60)

    # 影线
    shadow = compute_shadow_features(klines, lookback=5)
    if shadow[0] is None:
        return None
    avg_upper, avg_lower, _, _ = shadow

    # 量比
    vol_20 = [v for v in volume[-20:] if v > 0]
    vol_5 = [v for v in volume[-5:] if v > 0]
    if not vol_20 or not vol_5:
        return None
    avg_v20 = sum(vol_20) / len(vol_20)
    if avg_v20 <= 0:
        return None
    vol_ratio = (sum(vol_5) / len(vol_5)) / avg_v20

    # 近5日收益
    if len(close) >= 6 and close[-6] > 0:
        ret_5d = (close[-1] / close[-6] - 1) * 100
    else:
        return None

    score = 0.0

    # ── 高位信号（v1验证最强的区域）──
    if price_pos > 0.67:
        # 最强看跌：高位长下影放量（v1: 63.9%下跌，均收益-2.14%）
        if avg_lower > 0.35 and vol_ratio > 1.2:
            score -= 0.4
        # 逆向看涨：高位长上影缩量（v1: 60%上涨）
        if avg_upper > 0.35 and vol_ratio < 0.8:
            score += 0.25
        # 高位长上影正常量（v1: 55%上涨，弱看涨）
        if avg_upper > 0.35 and 0.8 <= vol_ratio <= 1.3:
            score += 0.1

    # ── 低位信号 ──
    elif price_pos < 0.33:
        # 逆向看涨：低位长上影缩量（v1: 62.4%上涨，均收益+1.12%）
        if avg_upper > 0.35 and vol_ratio < 0.8:
            score += 0.3
        # 低位长下影正常量（v1: 53%上涨，弱看涨）
        if avg_lower > 0.35 and 0.8 <= vol_ratio <= 1.3:
            score += 0.1
        # 低位长下影放量（v1: 51.5%下跌，弱看跌）
        if avg_lower > 0.35 and vol_ratio > 1.2:
            score -= 0.15

    # ── 量价配合（逆向逻辑）──
    # 放量下跌 → 非信息性交易 → 反转看涨（CGW理论）
    if ret_5d < -2 and vol_ratio > 1.3:
        score += 0.2
    # 缩量上涨 → 非信息性 → 反转看跌
    elif ret_5d > 2 and vol_ratio < 0.7:
        score -= 0.15
    # 放量上涨 → 信息性 → 持续看涨（但高位要打折）
    elif ret_5d > 2 and vol_ratio > 1.3:
        if price_pos < 0.67:
            score += 0.15
        else:
            score -= 0.1  # 高位放量追涨是陷阱

    return max(-1.0, min(1.0, score))


# ═══════════════════════════════════════════════════════════════
# 主回测逻辑
# ═══════════════════════════════════════════════════════════════

def run_backtest():
    t0 = time.time()
    print("=" * 70)
    print("情绪面因子回测 v2：逆向修正版（影线 + 量价 + 价格位置）")
    print("=" * 70)

    # 加载数据
    logger.info("[1/4] 加载数据...")
    stock_codes = load_stock_codes(200)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=400)).strftime('%Y-%m-%d')
    kline_data = load_kline_data(stock_codes, start_date, end_date)
    logger.info("  加载完成: %d只股票", len(kline_data))

    # ── Part 1: 离散场景回测 ──
    logger.info("[2/4] 扫描情绪场景...")
    scenario_results = defaultdict(list)
    total_scanned = 0
    total_classified = 0

    for code, klines in kline_data.items():
        if len(klines) < 80:
            continue
        for i in range(60, len(klines) - 10):
            total_scanned += 1
            hist = klines[:i + 1]
            scenario, details = classify_sentiment_scenario(hist)
            if scenario is None:
                continue
            total_classified += 1
            future = compute_future_returns(klines, i)
            if not future:
                continue
            scenario_results[scenario].append({
                'code': code,
                'date': klines[i]['date'],
                'details': details,
                'future': future,
            })

    logger.info("  扫描完成: %d个交易日, %d个被分类 (%.1f%%)",
                total_scanned, total_classified,
                total_classified / total_scanned * 100 if total_scanned else 0)

    # ── Part 2: 连续评分回测 ──
    logger.info("[3/4] 连续情绪评分回测...")
    score_buckets = defaultdict(list)  # 按评分区间分桶

    sample_count = 0
    for code, klines in kline_data.items():
        if len(klines) < 80:
            continue
        for i in range(60, len(klines) - 10):
            hist = klines[:i + 1]
            score = compute_sentiment_score(hist)
            if score is None:
                continue
            future = compute_future_returns(klines, i)
            if 'ret_5d' not in future:
                continue
            # 分桶：[-1,-0.3), [-0.3,-0.1), [-0.1,0.1), [0.1,0.3), [0.3,1]
            if score <= -0.3:
                bucket = 'strong_bearish'
            elif score <= -0.1:
                bucket = 'mild_bearish'
            elif score < 0.1:
                bucket = 'neutral'
            elif score < 0.3:
                bucket = 'mild_bullish'
            else:
                bucket = 'strong_bullish'
            score_buckets[bucket].append(future['ret_5d'])
            sample_count += 1

    logger.info("  评分回测完成: %d个样本", sample_count)

    # ── Part 3: 统计分析 ──
    logger.info("[4/4] 统计分析...")

    # 场景分析
    scenario_report = {}
    for scenario in sorted(SENTIMENT_EXPECTATIONS.keys()):
        records = scenario_results.get(scenario, [])
        if not records:
            scenario_report[scenario] = {'n': 0, 'note': '无样本'}
            continue

        expected = SENTIMENT_EXPECTATIONS[scenario]['expected']
        desc = SENTIMENT_EXPECTATIONS[scenario]['desc']

        rets_5d = [r['future']['ret_5d'] for r in records if 'ret_5d' in r['future']]
        rets_10d = [r['future']['ret_10d'] for r in records if 'ret_10d' in r['future']]

        n = len(rets_5d)
        if n == 0:
            scenario_report[scenario] = {'n': 0, 'note': '无有效样本'}
            continue

        avg_5d = sum(rets_5d) / n
        avg_10d = sum(rets_10d) / len(rets_10d) if rets_10d else 0
        median_5d = sorted(rets_5d)[n // 2]

        # 方向准确率
        if expected == 'UP':
            correct_5d = sum(1 for r in rets_5d if r > 0)
            correct_10d = sum(1 for r in rets_10d if r > 0)
        elif expected == 'DOWN':
            correct_5d = sum(1 for r in rets_5d if r < 0)
            correct_10d = sum(1 for r in rets_10d if r < 0)
        else:
            correct_5d = sum(1 for r in rets_5d if abs(r) < 3)
            correct_10d = sum(1 for r in rets_10d if abs(r) < 5)

        acc_5d = correct_5d / n
        acc_10d = correct_10d / len(rets_10d) if rets_10d else 0

        # 盈亏比
        if expected == 'UP':
            wins = [r for r in rets_5d if r > 0]
            losses = [-r for r in rets_5d if r < 0]
        elif expected == 'DOWN':
            wins = [-r for r in rets_5d if r < 0]
            losses = [r for r in rets_5d if r > 0]
        else:
            wins = losses = []

        avg_win = sum(wins) / len(wins) if wins else 0
        avg_loss = sum(losses) / len(losses) if losses else 0
        plr = avg_win / avg_loss if avg_loss > 0 else float('inf')

        scenario_report[scenario] = {
            'n': n,
            'expected': expected,
            'desc': desc,
            'accuracy_5d': round(acc_5d, 4),
            'accuracy_10d': round(acc_10d, 4),
            'avg_return_5d': round(avg_5d, 2),
            'avg_return_10d': round(avg_10d, 2),
            'median_return_5d': round(median_5d, 2),
            'profit_loss_ratio': round(plr, 2) if plr != float('inf') else 'inf',
        }

    # 评分分桶分析
    score_report = {}
    for bucket in ['strong_bearish', 'mild_bearish', 'neutral', 'mild_bullish', 'strong_bullish']:
        rets = score_buckets.get(bucket, [])
        if not rets:
            score_report[bucket] = {'n': 0}
            continue
        n = len(rets)
        avg = sum(rets) / n
        up_pct = sum(1 for r in rets if r > 0) / n
        score_report[bucket] = {
            'n': n,
            'avg_return_5d': round(avg, 3),
            'up_ratio': round(up_pct, 4),
            'down_ratio': round(1 - up_pct, 4),
            'median': round(sorted(rets)[n // 2], 3),
        }

    # 保存报告
    full_report = {
        'meta': {
            'total_scanned': total_scanned,
            'total_classified': total_classified,
            'n_stocks': len(kline_data),
            'date_range': f'{start_date} ~ {end_date}',
            'run_time_sec': round(time.time() - t0, 1),
        },
        'scenario_analysis': scenario_report,
        'score_bucket_analysis': score_report,
    }

    output_path = OUTPUT_DIR / "sentiment_shadow_backtest_v2.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(full_report, f, ensure_ascii=False, indent=2, default=str)

    # ═══════════════════════════════════════════════════════════
    # 打印结果
    # ═══════════════════════════════════════════════════════════
    print(f"\n数据范围: {start_date} ~ {end_date}, {len(kline_data)}只股票")
    print(f"扫描: {total_scanned}个交易日, 分类: {total_classified}个 ({total_classified / total_scanned * 100:.1f}%)")

    # 场景分析
    for cat_name, prefix in [("📉 低位场景", "S1,S2,S3,S4,S5,S6,S7,S8"),
                              ("📈 高位场景", "S9,S10,S11,S12,S13,S14,S15,S16")]:
        prefixes = prefix.split(',')
        print(f"\n{'─' * 80}")
        print(f"{cat_name}")
        print(f"{'─' * 80}")
        print(f"  {'场景':<24s} {'样本':>6s} {'预期':>4s} {'5日准确率':>8s} {'10日准确率':>9s} "
              f"{'均收益5d':>8s} {'盈亏比':>6s} {'判定':>4s}")
        print(f"  {'─' * 75}")

        for s in sorted(scenario_report.keys()):
            if not any(s.startswith(p) for p in prefixes):
                continue
            r = scenario_report[s]
            if r.get('n', 0) == 0:
                print(f"  {s:<24s} {'无样本':>6s}")
                continue

            acc5 = r['accuracy_5d']
            expected = r['expected']
            if expected == 'FLAT':
                verdict = '✅' if acc5 > 0.4 else '❌'
            else:
                verdict = '✅' if acc5 > 0.55 else ('⚠️' if acc5 > 0.50 else '❌')

            plr = r['profit_loss_ratio']
            plr_str = f"{plr:.1f}" if isinstance(plr, (int, float)) else plr

            print(f"  {s:<24s} {r['n']:>6d} {expected:>4s} "
                  f"{acc5:>8.1%} {r['accuracy_10d']:>9.1%} "
                  f"{r['avg_return_5d']:>+8.2f}% {plr_str:>6s} {verdict:>4s}")

    # 评分分桶
    print(f"\n{'─' * 80}")
    print("📊 综合情绪评分分桶分析")
    print(f"{'─' * 80}")
    print(f"  {'评分区间':<18s} {'样本':>8s} {'均收益5d':>10s} {'上涨比例':>8s} {'中位数':>8s}")
    print(f"  {'─' * 55}")
    for bucket in ['strong_bearish', 'mild_bearish', 'neutral', 'mild_bullish', 'strong_bullish']:
        r = score_report.get(bucket, {})
        n = r.get('n', 0)
        if n == 0:
            print(f"  {bucket:<18s} {'无样本':>8s}")
            continue
        print(f"  {bucket:<18s} {n:>8d} {r['avg_return_5d']:>+10.3f}% "
              f"{r['up_ratio']:>8.1%} {r['median']:>+8.3f}%")

    # 总结
    print(f"\n{'═' * 80}")
    print("📋 情绪面因子验证总结")
    print(f"{'═' * 80}")

    verified, partially, failed = [], [], []
    for s, r in scenario_report.items():
        if r.get('n', 0) < 50:
            continue
        acc5 = r.get('accuracy_5d', 0)
        expected = r.get('expected', '')
        if expected == 'FLAT':
            (verified if acc5 > 0.4 else failed).append(s)
        else:
            if acc5 > 0.55:
                verified.append(s)
            elif acc5 > 0.50:
                partially.append(s)
            else:
                failed.append(s)

    print(f"\n  ✅ 验证通过 (>55%): {len(verified)}个")
    for s in verified:
        r = scenario_report[s]
        print(f"     {s}: {r['accuracy_5d']:.1%} ({r['n']}样本) — {r.get('desc', '')}")

    print(f"\n  ⚠️  部分成立 (50-55%): {len(partially)}个")
    for s in partially:
        r = scenario_report[s]
        print(f"     {s}: {r['accuracy_5d']:.1%} ({r['n']}样本) — {r.get('desc', '')}")

    print(f"\n  ❌ 未验证 (<50%): {len(failed)}个")
    for s in failed:
        r = scenario_report[s]
        print(f"     {s}: {r['accuracy_5d']:.1%} ({r['n']}样本) — {r.get('desc', '')}")

    # 单调性检验
    print(f"\n{'─' * 80}")
    print("🔬 评分单调性检验（评分越高，未来收益是否越高？）")
    print(f"{'─' * 80}")
    bucket_order = ['strong_bearish', 'mild_bearish', 'neutral', 'mild_bullish', 'strong_bullish']
    avgs = [score_report.get(b, {}).get('avg_return_5d', None) for b in bucket_order]
    valid_avgs = [a for a in avgs if a is not None]
    if len(valid_avgs) >= 3:
        monotonic = all(valid_avgs[i] <= valid_avgs[i + 1] for i in range(len(valid_avgs) - 1))
        spread = valid_avgs[-1] - valid_avgs[0] if valid_avgs else 0
        print(f"  从最看跌到最看涨的均收益: {' → '.join(f'{a:+.3f}%' if a is not None else 'N/A' for a in avgs)}")
        print(f"  单调递增: {'✅ 是' if monotonic else '❌ 否'}")
        print(f"  多空收益差: {spread:+.3f}%")
        if spread > 0.5:
            print(f"  → 情绪评分有区分度，可作为因子使用")
        elif spread > 0:
            print(f"  → 情绪评分有一定区分度，但信号较弱")
        else:
            print(f"  → 情绪评分无区分度，需要调整")

    print(f"\n⏱️  总耗时: {time.time() - t0:.1f}秒")
    print(f"📁 完整报告: {output_path}")
    print("=" * 70)

    return full_report


if __name__ == '__main__':
    run_backtest()
