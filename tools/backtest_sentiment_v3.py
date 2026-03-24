#!/usr/bin/env python3
"""
情绪因子准确率提升 v3 — 目标 >65%
==================================
策略：
  1. 增加更多有理论基础的情绪因子（不拟合噪声）
  2. 严格时间序列分割：训练期→验证期→测试期（3段）
  3. 多因子一致性过滤：N个因子中至少K个同方向才出信号
  4. 高置信度阈值：只在信号极强时出手
  5. 加入市场环境过滤：大盘趋势作为过滤条件

新增因子（有学术/实证基础）：
  - 成交额集中度（Amihud变体）
  - 涨停/跌停接近度（A股制度因子）
  - 量能衰竭度（连续缩量天数）
  - 日内波动vs日间波动比（噪声交易者指标）
  - 资金流向动量（大单净流入趋势）
  - 价格加速度（二阶动量）
  - 均线偏离度（乖离率BIAS）
  - 成交量分布偏度（量的不对称性）

用法：
    source .venv/bin/activate
    python -m tools.backtest_sentiment_v3
"""
import json
import logging
import math
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


def _f(v):
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def load_stock_codes(limit=500):
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
            f"low_price, trading_volume, trading_amount, change_percent, "
            f"change_hand, amplitude "
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
                'amount': _f(row.get('trading_amount')),
                'pct': _f(row['change_percent']),
                'turnover': _f(row.get('change_hand')),
                'amplitude': _f(row.get('amplitude')),
            })
    cur.close()
    conn.close()
    return dict(result)


def load_fund_flow(stock_codes, start_date):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = defaultdict(list)
    bs = 300
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, big_net, big_net_pct, "
            f"small_net, small_net_pct, net_flow "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s ORDER BY `date`",
            batch + [start_date])
        for row in cur.fetchall():
            result[row['stock_code']].append({
                'date': str(row['date']),
                'big_net': _f(row.get('big_net')),
                'big_net_pct': _f(row.get('big_net_pct')),
                'small_net': _f(row.get('small_net')),
                'small_net_pct': _f(row.get('small_net_pct')),
                'net_flow': _f(row.get('net_flow')),
            })
    cur.close()
    conn.close()
    return dict(result)


# ═══════════════════════════════════════════════════════════════
# 全量情绪因子计算（v2验证有效 + 新增因子）
# ═══════════════════════════════════════════════════════════════

def compute_factors(klines, ff_by_date=None):
    """计算全量情绪因子。需要至少60根K线。"""
    n = len(klines)
    if n < 60:
        return None

    close = [k['close'] for k in klines]
    open_ = [k['open'] for k in klines]
    high = [k['high'] for k in klines]
    low = [k['low'] for k in klines]
    vol = [k['volume'] for k in klines]
    pct = [k['pct'] for k in klines]
    turn = [k['turnover'] for k in klines]
    amp = [k.get('amplitude', 0) or 0 for k in klines]

    if close[-1] <= 0 or vol[-1] <= 0:
        return None

    f = {}

    # ════════════════════════════════════════════
    # A. v2验证有效的因子（保留）
    # ════════════════════════════════════════════

    # A1: close_position_5d（尾盘位置，IC=-0.083）
    cps = []
    for k in klines[-5:]:
        hl = k['high'] - k['low']
        if hl > 0:
            cps.append((k['close'] - k['low']) / hl)
    if cps:
        f['close_pos'] = sum(cps) / len(cps)

    # A2: down_vol_ratio（下跌日成交量占比，IC=+0.064）
    dv = sum(vol[n - 5 + i] for i in range(5) if pct[n - 5 + i] < 0)
    tv = sum(vol[-5:])
    if tv > 0:
        f['down_vol_r'] = dv / tv

    # A3: big_move_ratio（大阳大阴频率，IC=-0.070）
    bu = sum(1 for p in pct[-10:] if p > 3)
    bd = sum(1 for p in pct[-10:] if p < -3)
    f['big_move'] = (bu - bd) / 10

    # A4: price_position（价格位置，IC=-0.067）
    c60 = [c for c in close[-60:] if c > 0]
    if c60:
        h60, l60 = max(c60), min(c60)
        if h60 > l60:
            f['price_pos'] = (close[-1] - l60) / (h60 - l60)

    # A5: turnover_spike（换手率异常，IC=-0.048）
    t20 = [t for t in turn[-20:] if t > 0]
    if t20 and turn[-1] > 0:
        at20 = sum(t20) / len(t20)
        if at20 > 0:
            f['turn_spike'] = turn[-1] / at20

    # A6: skewness_20d（偏度，IC=-0.035）
    if len(pct) >= 20:
        rp = pct[-20:]
        m = sum(rp) / 20
        s = (sum((p - m) ** 2 for p in rp) / 19) ** 0.5
        if s > 0:
            f['skew_20'] = sum((p - m) ** 3 for p in rp) / (20 * s ** 3)

    # A7: upper_shadow_5d（上影线逆向，IC=+0.026）
    us = []
    for k in klines[-5:]:
        hl = k['high'] - k['low']
        if hl > 0:
            us.append((k['high'] - max(k['close'], k['open'])) / hl)
    if us:
        f['upper_shd'] = sum(us) / len(us)

    # A8: lower_shadow_5d（下影线，IC=-0.022）
    ls = []
    for k in klines[-5:]:
        hl = k['high'] - k['low']
        if hl > 0:
            ls.append((min(k['close'], k['open']) - k['low']) / hl)
    if ls:
        f['lower_shd'] = sum(ls) / len(ls)

    # ════════════════════════════════════════════
    # B. 新增因子（有理论基础）
    # ════════════════════════════════════════════

    # B1: 涨停/跌停接近度（A股制度因子）
    # 接近涨停=极度乐观，接近跌停=极度恐慌
    if close[-2] > 0:
        limit_up = close[-2] * 1.1
        limit_down = close[-2] * 0.9
        f['limit_proximity'] = (close[-1] - limit_down) / (limit_up - limit_down)

    # B2: 量能衰竭度（连续缩量天数）
    shrink_days = 0
    for i in range(n - 1, max(n - 11, 0), -1):
        if i > 0 and vol[i] < vol[i - 1]:
            shrink_days += 1
        else:
            break
    f['vol_exhaust'] = shrink_days

    # B3: 日内波动vs日间波动比（噪声交易者指标，French & Roll 1986）
    # 高比值=日内噪声交易多=散户活跃
    intraday_vols = []
    overnight_vols = []
    for i in range(-10, 0):
        idx = n + i
        if idx > 0 and close[idx - 1] > 0 and high[idx] > 0 and low[idx] > 0:
            intraday = (high[idx] - low[idx]) / close[idx - 1] * 100
            overnight = abs(open_[idx] - close[idx - 1]) / close[idx - 1] * 100
            intraday_vols.append(intraday)
            overnight_vols.append(overnight)
    if intraday_vols and overnight_vols:
        avg_intra = sum(intraday_vols) / len(intraday_vols)
        avg_over = sum(overnight_vols) / len(overnight_vols)
        if avg_over > 0.01:
            f['noise_ratio'] = avg_intra / avg_over

    # B4: 价格加速度（二阶动量）
    # 一阶动量在加速还是减速
    if n >= 15 and close[-6] > 0 and close[-11] > 0:
        mom_recent = (close[-1] / close[-6] - 1) * 100
        mom_prev = (close[-6] / close[-11] - 1) * 100
        f['price_accel'] = mom_recent - mom_prev

    # B5: 均线偏离度（乖离率BIAS，经典技术指标）
    if n >= 20:
        ma20 = sum(close[-20:]) / 20
        if ma20 > 0:
            f['bias_20'] = (close[-1] / ma20 - 1) * 100

    # B6: 成交量分布偏度（量的不对称性）
    # 上涨日的量 vs 下跌日的量的不对称性
    up_vols = [vol[n - 10 + i] for i in range(10) if pct[n - 10 + i] > 0 and vol[n - 10 + i] > 0]
    dn_vols = [vol[n - 10 + i] for i in range(10) if pct[n - 10 + i] < 0 and vol[n - 10 + i] > 0]
    if up_vols and dn_vols:
        f['vol_asymmetry'] = (sum(up_vols) / len(up_vols)) / (sum(dn_vols) / len(dn_vols))

    # B7: 真实波幅比（ATR/价格，归一化波动率）
    trs = []
    for i in range(-14, 0):
        idx = n + i
        if idx > 0:
            tr = max(high[idx] - low[idx],
                     abs(high[idx] - close[idx - 1]),
                     abs(low[idx] - close[idx - 1]))
            trs.append(tr)
    if trs and close[-1] > 0:
        f['atr_pct'] = (sum(trs) / len(trs)) / close[-1] * 100

    # B8: 量价效率（单位成交量推动的价格变动）
    v20 = sum(vol[-20:]) / 20 if sum(vol[-20:]) > 0 else 0
    v5 = sum(vol[-5:]) / 5 if sum(vol[-5:]) > 0 else 0
    if v20 > 0 and close[-6] > 0:
        price_chg = abs(close[-1] / close[-6] - 1) * 100
        vol_r = v5 / v20
        if vol_r > 0.01:
            f['vp_efficiency'] = price_chg / vol_r

    # B9: 连涨/连跌天数
    cd = 0
    for p in reversed(pct):
        if p < 0:
            cd += 1
        else:
            break
    cu = 0
    for p in reversed(pct):
        if p > 0:
            cu += 1
        else:
            break
    f['consec_net'] = cu - cd

    # B10: 振幅趋势（近5日vs前5日振幅变化）
    a5 = [a for a in amp[-5:] if a > 0]
    a10 = [a for a in amp[-10:-5] if a > 0]
    if a5 and a10:
        f['amp_trend'] = (sum(a5) / len(a5)) / (sum(a10) / len(a10))

    # B11: 量比趋势（量比的变化方向）
    if n >= 15 and v20 > 0:
        v5_prev = sum(vol[-10:-5]) / 5 if sum(vol[-10:-5]) > 0 else 0
        if v5_prev > 0:
            f['vol_trend'] = (v5 / v20) - (v5_prev / v20)

    # B12: 收盘价相对开盘价（日内方向一致性）
    co_ratios = []
    for k in klines[-5:]:
        if k['open'] > 0:
            co_ratios.append((k['close'] - k['open']) / k['open'] * 100)
    if co_ratios:
        f['close_open_5d'] = sum(co_ratios) / len(co_ratios)

    # ════════════════════════════════════════════
    # C. 资金流向因子
    # ════════════════════════════════════════════
    if ff_by_date:
        ff_recent = []
        for k in klines[-5:]:
            ff = ff_by_date.get(k['date'])
            if ff:
                ff_recent.append(ff)

        if len(ff_recent) >= 3:
            # C1: 散户净流入（逆向指标，IC=-0.034）
            f['small_net'] = sum(x['small_net_pct'] for x in ff_recent) / len(ff_recent)

            # C2: 大单净流入
            f['big_net'] = sum(x['big_net_pct'] for x in ff_recent) / len(ff_recent)

            # C3: 大单-小单分歧
            f['big_small_div'] = f['big_net'] - f['small_net']

            # C4: 资金流向动量（近2日vs前3日）
            if len(ff_recent) >= 5:
                recent_2 = sum(x['net_flow'] for x in ff_recent[-2:]) / 2
                prev_3 = sum(x['net_flow'] for x in ff_recent[:3]) / 3
                if abs(prev_3) > 0.01:
                    f['flow_momentum'] = recent_2 - prev_3

    return f


# ═══════════════════════════════════════════════════════════════
# 评分与预测引擎
# ═══════════════════════════════════════════════════════════════

def build_factor_stats(all_factor_records):
    """从训练数据构建因子统计（均值、标准差、IC方向）"""
    stats = {}
    for fname in all_factor_records:
        vals = [r['val'] for r in all_factor_records[fname]]
        rets = [r['ret'] for r in all_factor_records[fname]]
        n = len(vals)
        if n < 200:
            continue
        m = sum(vals) / n
        s = (sum((v - m) ** 2 for v in vals) / (n - 1)) ** 0.5
        if s == 0:
            continue

        # 计算IC（Spearman秩相关）
        f_sorted = sorted(range(n), key=lambda i: vals[i])
        r_sorted = sorted(range(n), key=lambda i: rets[i])
        f_ranks = [0] * n
        r_ranks = [0] * n
        for rank, idx in enumerate(f_sorted):
            f_ranks[idx] = rank
        for rank, idx in enumerate(r_sorted):
            r_ranks[idx] = rank
        mf = sum(f_ranks) / n
        mr = sum(r_ranks) / n
        cov = sum((f_ranks[i] - mf) * (r_ranks[i] - mr) for i in range(n))
        sf = (sum((f_ranks[i] - mf) ** 2 for i in range(n))) ** 0.5
        sr = (sum((r_ranks[i] - mr) ** 2 for i in range(n))) ** 0.5
        ic = cov / (sf * sr) if sf > 0 and sr > 0 else 0

        stats[fname] = {
            'mean': m, 'std': s, 'ic': ic, 'n': n,
            'direction': -1 if ic < 0 else 1,  # IC方向
            'abs_ic': abs(ic),
        }
    return stats


def predict_direction(factors, factor_stats, min_agree=5, top_k=12):
    """
    多因子投票预测方向。

    策略：
      1. 选取|IC|最大的top_k个因子
      2. 每个因子做z-score，乘以IC方向
      3. 统计看涨/看跌的因子数量
      4. 至少min_agree个因子同方向才出信号
      5. 返回 (direction, confidence_score, n_agree) 或 (None, 0, 0)

    这种投票机制比加权求和更抗过拟合，因为不依赖精确权重。
    """
    # 选top_k因子
    ranked = sorted(factor_stats.items(), key=lambda x: x[1]['abs_ic'], reverse=True)
    top_factors = [(fname, fs) for fname, fs in ranked[:top_k] if fname in factors]

    if len(top_factors) < min_agree:
        return None, 0, 0

    votes_up = 0
    votes_down = 0
    weighted_score = 0.0
    total_weight = 0.0

    for fname, fs in top_factors:
        val = factors[fname]
        z = (val - fs['mean']) / fs['std']
        directed_z = z * fs['direction']  # 正=看涨

        if directed_z > 0.2:   # 弱阈值，避免噪声
            votes_up += 1
        elif directed_z < -0.2:
            votes_down += 1

        weighted_score += directed_z * fs['abs_ic']
        total_weight += fs['abs_ic']

    n_total = votes_up + votes_down
    if n_total == 0:
        return None, 0, 0

    if votes_up >= min_agree and votes_up > votes_down:
        confidence = weighted_score / total_weight if total_weight > 0 else 0
        return 'UP', confidence, votes_up
    elif votes_down >= min_agree and votes_down > votes_up:
        confidence = weighted_score / total_weight if total_weight > 0 else 0
        return 'DOWN', confidence, votes_down
    else:
        return None, 0, 0


def compute_future(klines, idx):
    base = klines[idx]['close']
    if base <= 0:
        return {}
    r = {}
    for h in (5, 10):
        if idx + h < len(klines) and klines[idx + h]['close'] > 0:
            r[f'ret_{h}d'] = round((klines[idx + h]['close'] / base - 1) * 100, 4)
    return r


# ═══════════════════════════════════════════════════════════════
# 准确率统计
# ═══════════════════════════════════════════════════════════════

def calc_stats(preds):
    """preds: list of (direction, actual_return)"""
    if not preds:
        return None
    n = len(preds)
    correct = sum(1 for d, r in preds if (d == 'UP' and r > 0) or (d == 'DOWN' and r < 0))
    wins, losses = [], []
    for d, r in preds:
        pnl = r if d == 'UP' else -r
        (wins if pnl > 0 else losses).append(abs(pnl))
    aw = sum(wins) / len(wins) if wins else 0
    al = sum(losses) / len(losses) if losses else 0
    avg_pnl = sum(r if d == 'UP' else -r for d, r in preds) / n
    return {
        'n': n,
        'accuracy': round(correct / n, 4),
        'avg_pnl': round(avg_pnl, 4),
        'avg_win': round(aw, 4),
        'avg_loss': round(al, 4),
        'plr': round(aw / al, 2) if al > 0 else 'inf',
    }


# ═══════════════════════════════════════════════════════════════
# 主回测
# ═══════════════════════════════════════════════════════════════

def run_backtest():
    t0 = time.time()
    print("=" * 80)
    print("情绪因子准确率提升 v3 — 目标 >65%")
    print("=" * 80)

    logger.info("[1/6] 加载数据...")
    stock_codes = load_stock_codes(300)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=500)).strftime('%Y-%m-%d')
    kline_data = load_kline_data(stock_codes, start_date, end_date)
    ff_data = load_fund_flow(stock_codes, start_date)
    logger.info("  K线: %d只, 资金流向: %d只", len(kline_data), len(ff_data))

    # ── 时间序列3段分割 ──
    # 训练期(40%) → 验证期(30%) → 测试期(30%)
    # 训练期：学习因子统计分布和IC
    # 验证期：调参（min_agree, top_k, 置信度阈值）
    # 测试期：最终评估（不能再调参）

    logger.info("[2/6] 训练期 — 收集因子分布和IC...")
    train_records = defaultdict(list)  # fname -> [{val, ret}]
    train_count = 0

    for code, klines in kline_data.items():
        if len(klines) < 80:
            continue
        ff_by_date = {f['date']: f for f in ff_data.get(code, [])}
        # 训练期：前40%
        train_end = int(len(klines) * 0.4)

        for i in range(60, min(train_end, len(klines) - 10)):
            hist = klines[:i + 1]
            factors = compute_factors(hist, ff_by_date)
            if not factors:
                continue
            future = compute_future(klines, i)
            if 'ret_5d' not in future:
                continue
            train_count += 1
            for fname, fval in factors.items():
                if fval is not None and not isinstance(fval, str):
                    train_records[fname].append({'val': fval, 'ret': future['ret_5d']})

    factor_stats = build_factor_stats(train_records)
    logger.info("  训练样本: %d, 有效因子: %d", train_count, len(factor_stats))

    # 打印因子IC
    ranked_factors = sorted(factor_stats.items(), key=lambda x: x[1]['abs_ic'], reverse=True)
    print(f"\n训练期因子IC排名（{train_count}样本）:")
    for fname, fs in ranked_factors[:15]:
        print(f"  {fname:<18s} IC={fs['ic']:+.4f}  |IC|={fs['abs_ic']:.4f}  n={fs['n']}")

    # ── 验证期：寻找最优参数 ──
    logger.info("[3/6] 验证期 — 参数优化...")
    best_config = None
    best_acc = 0

    # 参数搜索空间（小范围，避免过拟合）
    param_grid = [
        # (min_agree, top_k, conf_threshold)
        (4, 10, 0.3),
        (5, 10, 0.3),
        (5, 12, 0.3),
        (6, 12, 0.3),
        (5, 10, 0.5),
        (5, 12, 0.5),
        (6, 12, 0.5),
        (6, 14, 0.5),
        (7, 14, 0.5),
        (5, 10, 0.7),
        (6, 12, 0.7),
        (7, 14, 0.7),
        (7, 12, 0.5),
        (8, 14, 0.5),
        (6, 10, 0.5),
    ]

    val_results = []
    for min_agree, top_k, conf_thresh in param_grid:
        preds = []
        for code, klines in kline_data.items():
            if len(klines) < 80:
                continue
            ff_by_date = {f['date']: f for f in ff_data.get(code, [])}
            train_end = int(len(klines) * 0.4)
            val_end = int(len(klines) * 0.7)

            for i in range(max(60, train_end), min(val_end, len(klines) - 10)):
                hist = klines[:i + 1]
                factors = compute_factors(hist, ff_by_date)
                if not factors:
                    continue
                direction, conf, n_agree = predict_direction(
                    factors, factor_stats, min_agree=min_agree, top_k=top_k)
                if direction is None or abs(conf) < conf_thresh:
                    continue
                future = compute_future(klines, i)
                if 'ret_5d' not in future:
                    continue
                preds.append((direction, future['ret_5d']))

        stats = calc_stats(preds)
        if stats and stats['n'] >= 100:
            val_results.append({
                'params': (min_agree, top_k, conf_thresh),
                'stats': stats,
            })
            if stats['accuracy'] > best_acc:
                best_acc = stats['accuracy']
                best_config = (min_agree, top_k, conf_thresh)

    # 打印验证期结果
    val_results.sort(key=lambda x: x['stats']['accuracy'], reverse=True)
    print(f"\n验证期参数搜索结果（Top 10）:")
    print(f"  {'min_agree':>9s} {'top_k':>5s} {'conf':>5s} {'样本':>6s} {'准确率':>6s} {'期望':>8s} {'盈亏比':>6s}")
    print(f"  {'─' * 50}")
    for vr in val_results[:10]:
        p = vr['params']
        s = vr['stats']
        plr = f"{s['plr']:.2f}" if isinstance(s['plr'], (int, float)) else s['plr']
        print(f"  {p[0]:>9d} {p[1]:>5d} {p[2]:>5.1f} {s['n']:>6d} {s['accuracy']:>6.1%} "
              f"{s['avg_pnl']:>+8.3f}% {plr:>6s}")

    if not best_config:
        print("\n❌ 验证期无有效配置")
        return

    logger.info("  最优参数: min_agree=%d, top_k=%d, conf=%.1f, 验证准确率=%.1f%%",
                *best_config, best_acc * 100)

    # ── 测试期：最终评估 ──
    logger.info("[4/6] 测试期 — 最终评估（不再调参）...")
    min_agree, top_k, conf_thresh = best_config

    test_preds_5d = []
    test_preds_10d = []
    test_by_direction = {'UP': [], 'DOWN': []}
    test_by_confidence = {'very_high': [], 'high': [], 'medium': []}
    test_monthly = defaultdict(list)  # 按月统计

    for code, klines in kline_data.items():
        if len(klines) < 80:
            continue
        ff_by_date = {f['date']: f for f in ff_data.get(code, [])}
        val_end = int(len(klines) * 0.7)

        for i in range(max(60, val_end), len(klines) - 10):
            hist = klines[:i + 1]
            factors = compute_factors(hist, ff_by_date)
            if not factors:
                continue
            direction, conf, n_agree = predict_direction(
                factors, factor_stats, min_agree=min_agree, top_k=top_k)
            if direction is None or abs(conf) < conf_thresh:
                continue
            future = compute_future(klines, i)
            if 'ret_5d' not in future:
                continue

            test_preds_5d.append((direction, future['ret_5d']))
            test_by_direction[direction].append((direction, future['ret_5d']))

            # 月份
            month = klines[i]['date'][:7]
            test_monthly[month].append((direction, future['ret_5d']))

            if 'ret_10d' in future:
                test_preds_10d.append((direction, future['ret_10d']))

            # 置信度分层
            ac = abs(conf)
            if ac > 0.8:
                test_by_confidence['very_high'].append((direction, future['ret_5d']))
            elif ac > 0.5:
                test_by_confidence['high'].append((direction, future['ret_5d']))
            else:
                test_by_confidence['medium'].append((direction, future['ret_5d']))

    logger.info("  测试样本: %d", len(test_preds_5d))

    # ── 统计 ──
    logger.info("[5/6] 统计分析...")
    test_5d = calc_stats(test_preds_5d)
    test_10d = calc_stats(test_preds_10d)

    # ── 打印 ──
    logger.info("[6/6] 输出报告...")

    print(f"\n{'═' * 80}")
    print(f"📊 测试期结果（样本外，不参与任何调参）")
    print(f"{'═' * 80}")
    print(f"  最优参数: min_agree={min_agree}, top_k={top_k}, conf_thresh={conf_thresh}")

    if test_5d:
        plr = f"{test_5d['plr']:.2f}" if isinstance(test_5d['plr'], (int, float)) else test_5d['plr']
        print(f"\n  5日方向准确率:  {test_5d['accuracy']:.1%}  ({test_5d['n']}样本)")
        print(f"  5日期望收益:    {test_5d['avg_pnl']:+.3f}%")
        print(f"  5日盈亏比:      {plr}")
    if test_10d:
        print(f"  10日方向准确率: {test_10d['accuracy']:.1%}  ({test_10d['n']}样本)")
        print(f"  10日期望收益:   {test_10d['avg_pnl']:+.3f}%")

    # 按方向
    print(f"\n  按预测方向:")
    for d in ['UP', 'DOWN']:
        s = calc_stats(test_by_direction[d])
        if s:
            plr = f"{s['plr']:.2f}" if isinstance(s['plr'], (int, float)) else s['plr']
            print(f"    {d:>4s}: 准确率={s['accuracy']:.1%}, 期望={s['avg_pnl']:+.3f}%, "
                  f"盈亏比={plr}, n={s['n']}")

    # 按置信度
    print(f"\n  按置信度分层:")
    for conf_label in ['very_high', 'high', 'medium']:
        s = calc_stats(test_by_confidence[conf_label])
        if s and s['n'] >= 10:
            plr = f"{s['plr']:.2f}" if isinstance(s['plr'], (int, float)) else s['plr']
            label = {'very_high': '极高(>0.8)', 'high': '高(0.5~0.8)', 'medium': '中等'}[conf_label]
            print(f"    {label:<14s}: 准确率={s['accuracy']:.1%}, 期望={s['avg_pnl']:+.3f}%, "
                  f"盈亏比={plr}, n={s['n']}")

    # 按月稳定性
    print(f"\n  按月稳定性:")
    monthly_accs = []
    for month in sorted(test_monthly.keys()):
        s = calc_stats(test_monthly[month])
        if s and s['n'] >= 10:
            monthly_accs.append(s['accuracy'])
            print(f"    {month}: 准确率={s['accuracy']:.1%}, n={s['n']}, 期望={s['avg_pnl']:+.3f}%")
    if monthly_accs:
        avg_monthly = sum(monthly_accs) / len(monthly_accs)
        std_monthly = (sum((a - avg_monthly) ** 2 for a in monthly_accs) / max(len(monthly_accs) - 1, 1)) ** 0.5
        print(f"    ── 月均准确率: {avg_monthly:.1%} ± {std_monthly:.1%}")
        if std_monthly < 0.05:
            print(f"    ── 稳定性: ✅ 良好（标准差<5%）")
        elif std_monthly < 0.10:
            print(f"    ── 稳定性: ⚠️ 一般（标准差5~10%）")
        else:
            print(f"    ── 稳定性: ❌ 不稳定（标准差>10%）")

    # 总结
    print(f"\n{'═' * 80}")
    print("📋 总结")
    print(f"{'═' * 80}")
    if test_5d:
        acc = test_5d['accuracy']
        if acc >= 0.65:
            print(f"\n  ✅ 5日准确率 {acc:.1%} ≥ 65%，达标")
        elif acc >= 0.60:
            print(f"\n  ⚠️ 5日准确率 {acc:.1%}，接近目标但未达65%")
        else:
            print(f"\n  ❌ 5日准确率 {acc:.1%}，未达65%目标")

        # 检查过拟合
        val_best = val_results[0]['stats'] if val_results else None
        if val_best:
            val_acc = val_best['accuracy']
            overfit = val_acc - acc
            print(f"  验证期准确率: {val_acc:.1%}, 测试期: {acc:.1%}, 差距: {overfit:+.1%}")
            if overfit < 0.03:
                print(f"  → 过拟合检查: ✅ 差距<3%，无明显过拟合")
            elif overfit < 0.05:
                print(f"  → 过拟合检查: ⚠️ 差距3~5%，轻微过拟合")
            else:
                print(f"  → 过拟合检查: ❌ 差距>5%，存在过拟合")

    # 保存
    report = {
        'meta': {
            'n_stocks': len(kline_data),
            'date_range': f'{start_date} ~ {end_date}',
            'split': '40% train / 30% val / 30% test',
            'best_params': {'min_agree': min_agree, 'top_k': top_k, 'conf_thresh': conf_thresh},
            'run_time': round(time.time() - t0, 1),
        },
        'factor_ic': {fname: {'ic': fs['ic'], 'abs_ic': fs['abs_ic']}
                      for fname, fs in ranked_factors[:15]},
        'validation_results': [{'params': vr['params'], **vr['stats']}
                               for vr in val_results[:10]],
        'test_5d': test_5d,
        'test_10d': test_10d,
        'test_by_direction': {d: calc_stats(p) for d, p in test_by_direction.items()},
        'test_by_confidence': {c: calc_stats(p) for c, p in test_by_confidence.items()},
        'test_monthly': {m: calc_stats(p) for m, p in sorted(test_monthly.items())},
    }
    output_path = OUTPUT_DIR / "sentiment_v3_backtest.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n⏱️  总耗时: {time.time() - t0:.1f}秒")
    print(f"📁 报告: {output_path}")
    print("=" * 80)
    return report


if __name__ == '__main__':
    run_backtest()
