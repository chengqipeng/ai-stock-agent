#!/usr/bin/env python3
"""
情绪因子准确率评估
==================
将挖掘出的Top情绪因子组合成综合评分，评估方向预测准确率。

评估维度：
  1. 单因子准确率：每个因子独立预测涨跌的准确率
  2. 组合因子准确率：多因子加权组合后的准确率
  3. 高置信度筛选：只在信号强时出手的准确率
  4. 不同持有期：5日/10日准确率对比

Top因子（来自因子挖掘回测）：
  close_position_5d  IC=-0.083  尾盘位置（最强）
  down_vol_ratio     IC=+0.064  下跌日成交量占比
  big_move_ratio     IC=-0.070  大阳大阴频率
  price_position     IC=-0.067  价格位置
  turnover_spike     IC=-0.048  换手率异常
  small_net_pct_5d   IC=-0.034  散户净流入（逆向）
  upper_shadow_5d    IC=+0.026  上影线（逆向）
  skewness_20d       IC=-0.035  偏度（彩票效应）

用法：
    source .venv/bin/activate
    python -m tools.backtest_sentiment_accuracy
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


def load_stock_codes(limit=200):
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
            f"low_price, trading_volume, change_percent, change_hand, amplitude "
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
                'amplitude': _to_float(row.get('amplitude')),
            })
    cur.close()
    conn.close()
    return dict(result)


def load_fund_flow_data(stock_codes, start_date):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = defaultdict(list)
    bs = 300
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, big_net_pct, small_net_pct "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s ORDER BY `date`",
            batch + [start_date])
        for row in cur.fetchall():
            result[row['stock_code']].append({
                'date': str(row['date']),
                'big_net_pct': _to_float(row.get('big_net_pct')),
                'small_net_pct': _to_float(row.get('small_net_pct')),
            })
    cur.close()
    conn.close()
    return dict(result)


# ═══════════════════════════════════════════════════════════════
# 单因子计算
# ═══════════════════════════════════════════════════════════════

def compute_single_factors(klines, fund_flows_by_date=None):
    """计算所有单因子，返回 dict[name -> value] 或 None"""
    if len(klines) < 60:
        return None
    close = [k['close'] for k in klines]
    open_ = [k['open'] for k in klines]
    high = [k['high'] for k in klines]
    low = [k['low'] for k in klines]
    volume = [k['volume'] for k in klines]
    pct = [k['change_percent'] for k in klines]
    turnover = [k['turnover'] for k in klines]

    if close[-1] <= 0 or volume[-1] <= 0:
        return None

    factors = {}

    # F1: close_position_5d（尾盘位置，IC=-0.083）
    cps = []
    for k in klines[-5:]:
        hl = k['high'] - k['low']
        if hl > 0:
            cps.append((k['close'] - k['low']) / hl)
    if cps:
        factors['close_position_5d'] = sum(cps) / len(cps)

    # F2: down_vol_ratio（下跌日成交量占比，IC=+0.064）
    down_vol = sum(volume[len(volume) - 5 + i] for i in range(5)
                   if pct[len(pct) - 5 + i] < 0)
    total_vol = sum(volume[-5:])
    if total_vol > 0:
        factors['down_vol_ratio'] = down_vol / total_vol

    # F3: big_move_ratio（大阳大阴频率，IC=-0.070）
    big_up = sum(1 for p in pct[-10:] if p > 3)
    big_down = sum(1 for p in pct[-10:] if p < -3)
    factors['big_move_ratio'] = (big_up - big_down) / 10

    # F4: price_position（价格位置，IC=-0.067）
    close_60 = [c for c in close[-60:] if c > 0]
    if close_60:
        h60, l60 = max(close_60), min(close_60)
        if h60 > l60:
            factors['price_position'] = (close[-1] - l60) / (h60 - l60)

    # F5: turnover_spike（换手率异常，IC=-0.048）
    turn_20 = [t for t in turnover[-20:] if t > 0]
    if turn_20 and turnover[-1] > 0:
        avg_t20 = sum(turn_20) / len(turn_20)
        if avg_t20 > 0:
            factors['turnover_spike'] = turnover[-1] / avg_t20

    # F6: upper_shadow_5d（上影线逆向，IC=+0.026）
    uppers = []
    for k in klines[-5:]:
        hl = k['high'] - k['low']
        if hl > 0:
            uppers.append((k['high'] - max(k['close'], k['open'])) / hl)
    if uppers:
        factors['upper_shadow_5d'] = sum(uppers) / len(uppers)

    # F7: skewness_20d（偏度，IC=-0.035）
    if len(pct) >= 20:
        recent = pct[-20:]
        m = sum(recent) / 20
        s = (sum((p - m) ** 2 for p in recent) / 19) ** 0.5
        if s > 0:
            factors['skewness_20d'] = sum((p - m) ** 3 for p in recent) / (20 * s ** 3)

    # F8: lower_shadow_5d（下影线，IC=-0.022）
    lowers = []
    for k in klines[-5:]:
        hl = k['high'] - k['low']
        if hl > 0:
            lowers.append((min(k['close'], k['open']) - k['low']) / hl)
    if lowers:
        factors['lower_shadow_5d'] = sum(lowers) / len(lowers)

    # F9: amplitude_5d（振幅，IC=-0.059）
    amp = [k.get('amplitude', 0) or 0 for k in klines[-5:]]
    amp = [a for a in amp if a > 0]
    if amp:
        factors['amplitude_5d'] = sum(amp) / len(amp)

    # F10: turnover_ratio（换手率比，IC=-0.035）
    turn_5 = [t for t in turnover[-5:] if t > 0]
    if turn_20 and turn_5:
        avg_t5 = sum(turn_5) / len(turn_5)
        avg_t20 = sum(turn_20) / len(turn_20)
        if avg_t20 > 0:
            factors['turnover_ratio'] = avg_t5 / avg_t20

    # F11: small_net_pct_5d（散户净流入，IC=-0.034）
    if fund_flows_by_date:
        small_nets = []
        for k in klines[-5:]:
            ff = fund_flows_by_date.get(k['date'])
            if ff:
                small_nets.append(ff['small_net_pct'])
        if small_nets:
            factors['small_net_pct_5d'] = sum(small_nets) / len(small_nets)

    return factors


# ═══════════════════════════════════════════════════════════════
# 组合评分：将多因子合成一个方向信号
# ═══════════════════════════════════════════════════════════════

# 因子方向和权重（基于IC绝对值）
# IC < 0 的因子：值越大 → 未来收益越低 → 看跌信号
# IC > 0 的因子：值越大 → 未来收益越高 → 看涨信号
FACTOR_CONFIG = {
    # (IC方向, 权重)  IC方向: -1表示因子值越大越看跌, +1表示越大越看涨
    'close_position_5d': (-1, 0.25),   # 最强因子，高权重
    'down_vol_ratio':    (+1, 0.15),   # 第二强
    'big_move_ratio':    (-1, 0.12),
    'price_position':    (-1, 0.12),
    'turnover_spike':    (-1, 0.08),
    'upper_shadow_5d':   (+1, 0.06),
    'skewness_20d':      (-1, 0.06),
    'amplitude_5d':      (-1, 0.06),
    'turnover_ratio':    (-1, 0.05),
    'small_net_pct_5d':  (-1, 0.05),
}


def compute_composite_score(factors, historical_stats):
    """
    计算组合情绪评分。

    步骤：
      1. 每个因子值做z-score标准化（基于历史均值/标准差）
      2. 乘以IC方向（使所有因子方向一致：正=看涨）
      3. 加权求和

    返回 float（正=看涨，负=看跌）
    """
    score = 0.0
    total_weight = 0.0

    for fname, (direction, weight) in FACTOR_CONFIG.items():
        if fname not in factors:
            continue
        val = factors[fname]
        stats = historical_stats.get(fname)
        if not stats or stats['std'] == 0:
            continue

        # z-score标准化
        z = (val - stats['mean']) / stats['std']
        # 方向调整：使正值=看涨
        z_directed = z * direction
        score += z_directed * weight
        total_weight += weight

    if total_weight == 0:
        return None
    return score / total_weight


def compute_future_returns(klines, idx, horizons=(5, 10)):
    base = klines[idx]['close']
    if base <= 0:
        return {}
    rets = {}
    for h in horizons:
        if idx + h < len(klines):
            future = klines[idx + h]['close']
            if future > 0:
                rets[f'ret_{h}d'] = round((future / base - 1) * 100, 4)
    return rets


# ═══════════════════════════════════════════════════════════════
# 准确率统计工具
# ═══════════════════════════════════════════════════════════════

def calc_accuracy_stats(predictions):
    """
    统计准确率。
    predictions: list of (predicted_direction, actual_return)
      predicted_direction: 'UP' or 'DOWN'
      actual_return: float (%)
    """
    if not predictions:
        return None
    n = len(predictions)
    correct = sum(1 for d, r in predictions
                  if (d == 'UP' and r > 0) or (d == 'DOWN' and r < 0))
    accuracy = correct / n

    # 盈亏比（假设按预测方向操作）
    wins, losses = [], []
    for d, r in predictions:
        pnl = r if d == 'UP' else -r
        if pnl > 0:
            wins.append(pnl)
        elif pnl < 0:
            losses.append(abs(pnl))

    avg_win = sum(wins) / len(wins) if wins else 0
    avg_loss = sum(losses) / len(losses) if losses else 0
    plr = avg_win / avg_loss if avg_loss > 0 else float('inf')

    # 期望收益
    avg_pnl = sum(r if d == 'UP' else -r for d, r in predictions) / n

    return {
        'n': n,
        'accuracy': round(accuracy, 4),
        'avg_pnl': round(avg_pnl, 4),
        'avg_win': round(avg_win, 4),
        'avg_loss': round(avg_loss, 4),
        'profit_loss_ratio': round(plr, 2) if plr != float('inf') else 'inf',
        'win_count': len(wins),
        'loss_count': len(losses),
    }


# ═══════════════════════════════════════════════════════════════
# 主回测
# ═══════════════════════════════════════════════════════════════

def run_backtest():
    t0 = time.time()
    print("=" * 80)
    print("情绪因子准确率评估")
    print("=" * 80)

    logger.info("[1/5] 加载数据...")
    stock_codes = load_stock_codes(200)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=400)).strftime('%Y-%m-%d')
    kline_data = load_kline_data(stock_codes, start_date, end_date)
    fund_flow_data = load_fund_flow_data(stock_codes, start_date)
    logger.info("  K线: %d只, 资金流向: %d只", len(kline_data), len(fund_flow_data))

    # ── Pass 1: 收集因子历史分布（用于z-score标准化）──
    logger.info("[2/5] 收集因子分布...")
    factor_values = defaultdict(list)

    for code, klines in kline_data.items():
        if len(klines) < 80:
            continue
        ff_by_date = {f['date']: f for f in fund_flow_data.get(code, [])}
        # 只用前半段数据计算分布（避免前视偏差）
        half = len(klines) // 2
        for i in range(60, half):
            hist = klines[:i + 1]
            factors = compute_single_factors(hist, ff_by_date)
            if factors is None:
                continue
            for fname, fval in factors.items():
                if fval is not None:
                    factor_values[fname].append(fval)

    historical_stats = {}
    for fname, vals in factor_values.items():
        if len(vals) < 100:
            continue
        m = sum(vals) / len(vals)
        s = (sum((v - m) ** 2 for v in vals) / (len(vals) - 1)) ** 0.5
        historical_stats[fname] = {'mean': m, 'std': s, 'n': len(vals)}

    logger.info("  %d个因子有足够历史数据", len(historical_stats))

    # ── Pass 2: 后半段数据做预测回测 ──
    logger.info("[3/5] 回测预测准确率...")

    # 单因子预测结果
    single_factor_preds = defaultdict(lambda: {'5d': [], '10d': []})
    # 组合评分预测结果
    composite_preds = {'5d': [], '10d': []}
    # 按置信度分层
    composite_by_confidence = {
        'all': {'5d': [], '10d': []},
        'high': {'5d': [], '10d': []},     # |score| > 0.5
        'medium': {'5d': [], '10d': []},   # 0.3 < |score| <= 0.5
        'low': {'5d': [], '10d': []},      # |score| <= 0.3
    }

    total_scanned = 0
    total_predicted = 0

    for code, klines in kline_data.items():
        if len(klines) < 80:
            continue
        ff_by_date = {f['date']: f for f in fund_flow_data.get(code, [])}
        half = len(klines) // 2

        for i in range(max(60, half), len(klines) - 10):
            total_scanned += 1
            hist = klines[:i + 1]
            factors = compute_single_factors(hist, ff_by_date)
            if factors is None:
                continue

            future = compute_future_returns(klines, i)
            if 'ret_5d' not in future:
                continue

            total_predicted += 1
            ret_5d = future['ret_5d']
            ret_10d = future.get('ret_10d')

            # ── 单因子准确率 ──
            for fname, (direction, _) in FACTOR_CONFIG.items():
                if fname not in factors:
                    continue
                val = factors[fname]
                stats = historical_stats.get(fname)
                if not stats or stats['std'] == 0:
                    continue
                z = (val - stats['mean']) / stats['std'] * direction
                pred = 'UP' if z > 0 else 'DOWN'
                single_factor_preds[fname]['5d'].append((pred, ret_5d))
                if ret_10d is not None:
                    single_factor_preds[fname]['10d'].append((pred, ret_10d))

            # ── 组合评分 ──
            score = compute_composite_score(factors, historical_stats)
            if score is None:
                continue

            pred = 'UP' if score > 0 else 'DOWN'
            composite_preds['5d'].append((pred, ret_5d))
            if ret_10d is not None:
                composite_preds['10d'].append((pred, ret_10d))

            # 按置信度分层
            abs_score = abs(score)
            composite_by_confidence['all']['5d'].append((pred, ret_5d))
            if ret_10d is not None:
                composite_by_confidence['all']['10d'].append((pred, ret_10d))

            if abs_score > 0.5:
                conf = 'high'
            elif abs_score > 0.3:
                conf = 'medium'
            else:
                conf = 'low'
            composite_by_confidence[conf]['5d'].append((pred, ret_5d))
            if ret_10d is not None:
                composite_by_confidence[conf]['10d'].append((pred, ret_10d))

    logger.info("  扫描: %d, 预测: %d", total_scanned, total_predicted)

    # ── 统计分析 ──
    logger.info("[4/5] 统计分析...")

    # 单因子准确率
    single_report = {}
    for fname in FACTOR_CONFIG:
        preds_5d = single_factor_preds[fname]['5d']
        preds_10d = single_factor_preds[fname]['10d']
        if not preds_5d:
            continue
        single_report[fname] = {
            '5d': calc_accuracy_stats(preds_5d),
            '10d': calc_accuracy_stats(preds_10d),
        }

    # 组合准确率
    composite_report = {
        '5d': calc_accuracy_stats(composite_preds['5d']),
        '10d': calc_accuracy_stats(composite_preds['10d']),
    }

    # 按置信度分层
    confidence_report = {}
    for conf in ['all', 'high', 'medium', 'low']:
        confidence_report[conf] = {
            '5d': calc_accuracy_stats(composite_by_confidence[conf]['5d']),
            '10d': calc_accuracy_stats(composite_by_confidence[conf]['10d']),
        }

    # ── 按预测方向分别统计 ──
    up_preds_5d = [(d, r) for d, r in composite_preds['5d'] if d == 'UP']
    down_preds_5d = [(d, r) for d, r in composite_preds['5d'] if d == 'DOWN']
    direction_report = {
        'UP_5d': calc_accuracy_stats(up_preds_5d),
        'DOWN_5d': calc_accuracy_stats(down_preds_5d),
    }

    # 保存
    logger.info("[5/5] 保存报告...")
    full_report = {
        'meta': {
            'total_scanned': total_scanned,
            'total_predicted': total_predicted,
            'n_stocks': len(kline_data),
            'date_range': f'{start_date} ~ {end_date}',
            'backtest_period': '后半段（前半段用于标准化）',
            'run_time_sec': round(time.time() - t0, 1),
        },
        'single_factor_accuracy': single_report,
        'composite_accuracy': composite_report,
        'confidence_accuracy': confidence_report,
        'direction_accuracy': direction_report,
    }

    output_path = OUTPUT_DIR / "sentiment_accuracy_backtest.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(full_report, f, ensure_ascii=False, indent=2, default=str)

    # ═══════════════════════════════════════════════════════════
    # 打印结果
    # ═══════════════════════════════════════════════════════════
    print(f"\n数据: {len(kline_data)}只股票, {start_date} ~ {end_date}")
    print(f"回测样本: {total_predicted}个（后半段数据，前半段用于标准化）")

    # 单因子准确率
    print(f"\n{'═' * 80}")
    print("📊 单因子方向预测准确率")
    print(f"{'═' * 80}")
    print(f"  {'因子名':<25s} {'样本':>7s} {'5日准确率':>8s} {'5日期望':>8s} "
          f"{'10日准确率':>9s} {'盈亏比':>6s}")
    print(f"  {'─' * 70}")

    # 按5日准确率排序
    sorted_single = sorted(single_report.items(),
                           key=lambda x: x[1]['5d']['accuracy'] if x[1]['5d'] else 0,
                           reverse=True)
    for fname, r in sorted_single:
        s5 = r['5d']
        s10 = r['10d']
        if not s5:
            continue
        plr = s5['profit_loss_ratio']
        plr_str = f"{plr:.2f}" if isinstance(plr, (int, float)) else plr
        acc10 = f"{s10['accuracy']:.1%}" if s10 else 'N/A'
        print(f"  {fname:<25s} {s5['n']:>7d} {s5['accuracy']:>8.1%} "
              f"{s5['avg_pnl']:>+8.3f}% {acc10:>9s} {plr_str:>6s}")

    # 组合准确率
    print(f"\n{'═' * 80}")
    print("📊 组合情绪评分准确率")
    print(f"{'═' * 80}")
    for period in ['5d', '10d']:
        s = composite_report[period]
        if not s:
            continue
        plr = s['profit_loss_ratio']
        plr_str = f"{plr:.2f}" if isinstance(plr, (int, float)) else plr
        print(f"  {period}: 准确率={s['accuracy']:.1%}, 期望收益={s['avg_pnl']:+.3f}%, "
              f"盈亏比={plr_str}, 样本={s['n']}")

    # 按置信度分层
    print(f"\n{'═' * 80}")
    print("📊 按置信度分层准确率（|score|越大信号越强）")
    print(f"{'═' * 80}")
    print(f"  {'置信度':<10s} {'样本':>7s} {'5日准确率':>8s} {'5日期望':>8s} "
          f"{'10日准确率':>9s} {'盈亏比':>6s}")
    print(f"  {'─' * 55}")
    for conf in ['high', 'medium', 'low', 'all']:
        s5 = confidence_report[conf]['5d']
        s10 = confidence_report[conf]['10d']
        if not s5:
            continue
        plr = s5['profit_loss_ratio']
        plr_str = f"{plr:.2f}" if isinstance(plr, (int, float)) else plr
        acc10 = f"{s10['accuracy']:.1%}" if s10 else 'N/A'
        label = {'high': '高(>0.5)', 'medium': '中(0.3~0.5)',
                 'low': '低(<0.3)', 'all': '全部'}[conf]
        print(f"  {label:<10s} {s5['n']:>7d} {s5['accuracy']:>8.1%} "
              f"{s5['avg_pnl']:>+8.3f}% {acc10:>9s} {plr_str:>6s}")

    # 按方向分别统计
    print(f"\n{'═' * 80}")
    print("📊 按预测方向分别统计（5日）")
    print(f"{'═' * 80}")
    for label, key in [('预测涨(UP)', 'UP_5d'), ('预测跌(DOWN)', 'DOWN_5d')]:
        s = direction_report[key]
        if not s:
            continue
        plr = s['profit_loss_ratio']
        plr_str = f"{plr:.2f}" if isinstance(plr, (int, float)) else plr
        print(f"  {label}: 准确率={s['accuracy']:.1%}, 期望={s['avg_pnl']:+.3f}%, "
              f"盈亏比={plr_str}, 样本={s['n']}")

    # 总结
    print(f"\n{'═' * 80}")
    print("📋 总结")
    print(f"{'═' * 80}")
    s5_all = composite_report['5d']
    s5_high = confidence_report['high']['5d']
    if s5_all:
        print(f"\n  组合评分整体5日准确率: {s5_all['accuracy']:.1%} ({s5_all['n']}样本)")
    if s5_high and s5_high['n'] > 50:
        print(f"  高置信度5日准确率:     {s5_high['accuracy']:.1%} ({s5_high['n']}样本)")
        improvement = s5_high['accuracy'] - s5_all['accuracy'] if s5_all else 0
        if improvement > 0:
            print(f"  → 高置信度比整体提升 {improvement:.1%}")

    # 最佳单因子
    if sorted_single:
        best_name, best_r = sorted_single[0]
        print(f"\n  最佳单因子: {best_name}")
        print(f"    5日准确率: {best_r['5d']['accuracy']:.1%}, "
              f"期望收益: {best_r['5d']['avg_pnl']:+.3f}%")

    print(f"\n⏱️  总耗时: {time.time() - t0:.1f}秒")
    print(f"📁 报告: {output_path}")
    print("=" * 80)

    return full_report


if __name__ == '__main__':
    run_backtest()
