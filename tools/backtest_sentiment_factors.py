#!/usr/bin/env python3
"""
情绪面因子全面挖掘回测
======================
在影线+量价+价格位置（v2）基础上，挖掘更多情绪因子维度：

  1. 资金流向情绪：大单vs小单博弈、主力净流入趋势、资金分歧度
  2. 振幅情绪：高振幅=多空分歧大、低振幅=方向一致
  3. 连续涨跌情绪：过度反应→均值回归（行为金融）
  4. 换手率异常：散户行为因子（A股特色）
  5. 缺口情绪：跳空方向+回补概率
  6. 尾盘行为：尾盘拉升/砸盘（A股T+1制度下信息含量高）
  7. 量价背离强度

方法：
  - 每个因子独立计算，按分位数分5档
  - 统计每档的未来5日/10日收益，检验单调性
  - 最终输出每个因子的IC值和分档收益

用法：
    source .venv/bin/activate
    python -m tools.backtest_sentiment_factors
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
            f"low_price, trading_volume, trading_amount, change_percent, "
            f"change_hand, amplitude "
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
                'amount': _to_float(row.get('trading_amount')),
                'change_percent': _to_float(row['change_percent']),
                'turnover': _to_float(row.get('change_hand')),
                'amplitude': _to_float(row.get('amplitude')),
            })
    cur.close()
    conn.close()
    return dict(result)


def load_fund_flow_data(stock_codes, start_date):
    """加载资金流向数据"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = defaultdict(list)
    bs = 300
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, big_net, big_net_pct, "
            f"mid_net, mid_net_pct, small_net, small_net_pct, "
            f"net_flow, main_net_5day "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s ORDER BY `date`",
            batch + [start_date])
        for row in cur.fetchall():
            result[row['stock_code']].append({
                'date': str(row['date']),
                'big_net': _to_float(row.get('big_net')),
                'big_net_pct': _to_float(row.get('big_net_pct')),
                'mid_net': _to_float(row.get('mid_net')),
                'mid_net_pct': _to_float(row.get('mid_net_pct')),
                'small_net': _to_float(row.get('small_net')),
                'small_net_pct': _to_float(row.get('small_net_pct')),
                'net_flow': _to_float(row.get('net_flow')),
                'main_net_5day': _to_float(row.get('main_net_5day')),
            })
    cur.close()
    conn.close()
    return dict(result)


# ═══════════════════════════════════════════════════════════════
# 情绪因子计算（纯K线数据，不依赖外部数据源）
# ═══════════════════════════════════════════════════════════════

def compute_all_sentiment_factors(klines, fund_flows=None):
    """
    计算所有情绪因子。返回 dict[factor_name -> float] 或 None。
    需要至少60根K线。
    """
    if len(klines) < 60:
        return None

    close = [k['close'] for k in klines]
    open_ = [k['open'] for k in klines]
    high = [k['high'] for k in klines]
    low = [k['low'] for k in klines]
    volume = [k['volume'] for k in klines]
    pct = [k['change_percent'] for k in klines]
    turnover = [k['turnover'] for k in klines]
    amplitude = [k.get('amplitude', 0) or 0 for k in klines]

    if close[-1] <= 0 or volume[-1] <= 0:
        return None

    factors = {}

    # ── 因子1: 振幅情绪（高振幅=多空分歧大）──
    # 近5日平均振幅 vs 20日平均振幅
    amp_5 = [a for a in amplitude[-5:] if a > 0]
    amp_20 = [a for a in amplitude[-20:] if a > 0]
    if amp_5 and amp_20:
        avg_amp_5 = sum(amp_5) / len(amp_5)
        avg_amp_20 = sum(amp_20) / len(amp_20)
        # 振幅比：>1 表示近期分歧加大
        factors['amplitude_ratio'] = avg_amp_5 / avg_amp_20 if avg_amp_20 > 0 else 1.0
        factors['amplitude_5d'] = avg_amp_5

    # ── 因子2: 连续涨跌情绪（过度反应因子）──
    # 连跌天数（行为金融：过度反应后均值回归）
    consec_down = 0
    for p in reversed(pct):
        if p < 0:
            consec_down += 1
        else:
            break
    consec_up = 0
    for p in reversed(pct):
        if p > 0:
            consec_up += 1
        else:
            break
    factors['consec_down'] = consec_down
    factors['consec_up'] = consec_up
    # 净连续方向：正=连涨，负=连跌
    factors['consec_net'] = consec_up - consec_down

    # ── 因子3: 换手率异常（散户行为因子）──
    # A股特色：异常高换手后反转概率高
    turn_20 = [t for t in turnover[-20:] if t > 0]
    turn_5 = [t for t in turnover[-5:] if t > 0]
    if turn_20 and turn_5:
        avg_turn_20 = sum(turn_20) / len(turn_20)
        avg_turn_5 = sum(turn_5) / len(turn_5)
        if avg_turn_20 > 0:
            factors['turnover_ratio'] = avg_turn_5 / avg_turn_20
            # 最后一天的换手率异常度
            if turnover[-1] > 0:
                factors['turnover_spike'] = turnover[-1] / avg_turn_20

    # ── 因子4: 缺口情绪（跳空方向）──
    # 近5日跳空缺口统计
    gap_ups, gap_downs = 0, 0
    total_gap = 0.0
    for i in range(-5, 0):
        idx = len(close) + i
        if idx > 0 and close[idx - 1] > 0:
            gap = (open_[idx] - close[idx - 1]) / close[idx - 1] * 100
            total_gap += gap
            if gap > 0.5:
                gap_ups += 1
            elif gap < -0.5:
                gap_downs += 1
    factors['gap_net_5d'] = total_gap  # 净跳空幅度
    factors['gap_up_count'] = gap_ups
    factors['gap_down_count'] = gap_downs

    # ── 因子5: 尾盘行为（收盘价在日内的位置）──
    # close_position: 1=收在最高价，0=收在最低价
    # A股T+1制度下，尾盘信息含量高
    close_positions = []
    for i in range(-5, 0):
        idx = len(close) + i
        hl = high[idx] - low[idx]
        if hl > 0:
            close_positions.append((close[idx] - low[idx]) / hl)
    if close_positions:
        factors['close_position_5d'] = sum(close_positions) / len(close_positions)
        # 尾盘趋势：近2日 vs 前3日
        if len(close_positions) >= 5:
            recent_2 = sum(close_positions[-2:]) / 2
            prev_3 = sum(close_positions[:3]) / 3
            factors['close_position_trend'] = recent_2 - prev_3

    # ── 因子6: 量价背离强度 ──
    factors['vol_price_diverge'] = 0

    # ── 因子7: 影线逆向情绪（v2验证有效）──
    uppers, lowers = [], []
    for k in klines[-5:]:
        hl = k['high'] - k['low']
        if hl > 0:
            uppers.append((k['high'] - max(k['close'], k['open'])) / hl)
            lowers.append((min(k['close'], k['open']) - k['low']) / hl)
    if uppers:
        factors['upper_shadow_5d'] = sum(uppers) / len(uppers)
        factors['lower_shadow_5d'] = sum(lowers) / len(lowers)

    # ── 因子8: 价格位置（情绪锚定）──
    close_60 = [c for c in close[-60:] if c > 0]
    if close_60:
        h60, l60 = max(close_60), min(close_60)
        if h60 > l60:
            factors['price_position'] = (close[-1] - l60) / (h60 - l60)

    # ── 因子9: 下跌日成交量占比（卖压指标）──
    down_vol = sum(volume[len(volume) - 5 + i] for i in range(5)
                   if pct[len(pct) - 5 + i] < 0)
    total_vol = sum(volume[-5:])
    factors['down_vol_ratio'] = down_vol / total_vol if total_vol > 0 else 0.5

    # ── 因子10: 波动率变化（恐慌/平静转换）──
    if len(pct) >= 20:
        vol_recent = pct[-5:]
        vol_prev = pct[-20:-5]
        m_r = sum(vol_recent) / 5
        m_p = sum(vol_prev) / 15
        std_r = (sum((p - m_r) ** 2 for p in vol_recent) / 4) ** 0.5 if len(vol_recent) > 1 else 0
        std_p = (sum((p - m_p) ** 2 for p in vol_prev) / 14) ** 0.5 if len(vol_prev) > 1 else 0
        factors['volatility_ratio'] = std_r / std_p if std_p > 0 else 1.0

    # ── 因子11: 涨跌幅偏度（彩票效应）──
    if len(pct) >= 20:
        recent = pct[-20:]
        m = sum(recent) / 20
        s = (sum((p - m) ** 2 for p in recent) / 19) ** 0.5
        if s > 0:
            factors['skewness_20d'] = sum((p - m) ** 3 for p in recent) / (20 * s ** 3)

    # ── 因子12: 大阳/大阴线频率 ──
    big_up = sum(1 for p in pct[-10:] if p > 3)
    big_down = sum(1 for p in pct[-10:] if p < -3)
    factors['big_move_ratio'] = (big_up - big_down) / 10

    # ── 资金流向因子（如果有数据）──
    if fund_flows and len(fund_flows) >= 5:
        # 因子13: 大单净流入占比（主力行为）
        big_nets = [f['big_net_pct'] for f in fund_flows[-5:]]
        factors['big_net_pct_5d'] = sum(big_nets) / len(big_nets)

        # 因子14: 小单净流入占比（散户行为，逆向指标）
        small_nets = [f['small_net_pct'] for f in fund_flows[-5:]]
        factors['small_net_pct_5d'] = sum(small_nets) / len(small_nets)

        # 因子15: 大单vs小单分歧度
        factors['big_small_diverge'] = factors['big_net_pct_5d'] - factors['small_net_pct_5d']

        # 因子16: 主力5日净额趋势
        if len(fund_flows) >= 2:
            factors['main_flow_trend'] = fund_flows[-1]['main_net_5day'] - fund_flows[-2].get('main_net_5day', 0)

    return factors


# ═══════════════════════════════════════════════════════════════
# 回测引擎
# ═══════════════════════════════════════════════════════════════

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


def compute_ic(factor_vals, return_vals):
    """计算因子IC（Spearman秩相关系数）"""
    n = len(factor_vals)
    if n < 30:
        return None
    # 秩排名
    f_ranked = sorted(range(n), key=lambda i: factor_vals[i])
    r_ranked = sorted(range(n), key=lambda i: return_vals[i])
    f_ranks = [0] * n
    r_ranks = [0] * n
    for rank, idx in enumerate(f_ranked):
        f_ranks[idx] = rank
    for rank, idx in enumerate(r_ranked):
        r_ranks[idx] = rank
    # Spearman
    mean_f = sum(f_ranks) / n
    mean_r = sum(r_ranks) / n
    cov = sum((f_ranks[i] - mean_f) * (r_ranks[i] - mean_r) for i in range(n))
    std_f = (sum((f_ranks[i] - mean_f) ** 2 for i in range(n))) ** 0.5
    std_r = (sum((r_ranks[i] - mean_r) ** 2 for i in range(n))) ** 0.5
    if std_f == 0 or std_r == 0:
        return 0
    return cov / (std_f * std_r)


def quintile_analysis(factor_vals, return_vals):
    """分5档分析因子收益"""
    n = len(factor_vals)
    if n < 50:
        return None
    # 按因子值排序
    paired = sorted(zip(factor_vals, return_vals), key=lambda x: x[0])
    q_size = n // 5
    quintiles = {}
    for q in range(5):
        start = q * q_size
        end = start + q_size if q < 4 else n
        q_rets = [p[1] for p in paired[start:end]]
        q_factors = [p[0] for p in paired[start:end]]
        quintiles[f'Q{q + 1}'] = {
            'n': len(q_rets),
            'avg_return': round(sum(q_rets) / len(q_rets), 4) if q_rets else 0,
            'up_ratio': round(sum(1 for r in q_rets if r > 0) / len(q_rets), 4) if q_rets else 0,
            'factor_range': f'{q_factors[0]:.3f} ~ {q_factors[-1]:.3f}' if q_factors else '',
        }
    # 单调性：Q1到Q5的均收益是否单调
    avgs = [quintiles[f'Q{q + 1}']['avg_return'] for q in range(5)]
    monotonic_up = all(avgs[i] <= avgs[i + 1] for i in range(4))
    monotonic_down = all(avgs[i] >= avgs[i + 1] for i in range(4))
    spread = avgs[4] - avgs[0]
    return {
        'quintiles': quintiles,
        'monotonic': 'up' if monotonic_up else ('down' if monotonic_down else 'none'),
        'spread_Q5_Q1': round(spread, 4),
    }


def run_backtest():
    t0 = time.time()
    print("=" * 70)
    print("情绪面因子全面挖掘回测")
    print("=" * 70)

    # 加载数据
    logger.info("[1/3] 加载数据...")
    stock_codes = load_stock_codes(200)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=400)).strftime('%Y-%m-%d')
    kline_data = load_kline_data(stock_codes, start_date, end_date)
    logger.info("  K线加载完成: %d只股票", len(kline_data))

    # 加载资金流向
    fund_flow_data = load_fund_flow_data(stock_codes, start_date)
    logger.info("  资金流向加载完成: %d只股票有数据", len(fund_flow_data))

    # 构建资金流向索引（按stock_code+date快速查找）
    ff_index = {}
    for code, flows in fund_flow_data.items():
        for f in flows:
            ff_index[(code, f['date'])] = f

    # 扫描所有交易日，计算因子和未来收益
    logger.info("[2/3] 计算因子...")
    # factor_name -> [(factor_value, future_return_5d)]
    factor_data = defaultdict(lambda: {'vals': [], 'rets': []})
    total_scanned = 0
    total_computed = 0

    for code, klines in kline_data.items():
        if len(klines) < 80:
            continue

        # 构建该股票的资金流向时间序列
        ff_list = fund_flow_data.get(code, [])
        ff_by_date = {f['date']: f for f in ff_list}

        for i in range(60, len(klines) - 10):
            total_scanned += 1
            hist = klines[:i + 1]
            current_date = klines[i]['date']

            # 获取对应的资金流向数据（近5日）
            ff_recent = []
            for j in range(max(0, i - 4), i + 1):
                d = klines[j]['date']
                if d in ff_by_date:
                    ff_recent.append(ff_by_date[d])

            factors = compute_all_sentiment_factors(hist, ff_recent if ff_recent else None)
            if factors is None:
                continue

            future = compute_future_returns(klines, i)
            if 'ret_5d' not in future:
                continue

            total_computed += 1
            ret_5d = future['ret_5d']

            for fname, fval in factors.items():
                if fval is not None and not isinstance(fval, str):
                    factor_data[fname]['vals'].append(fval)
                    factor_data[fname]['rets'].append(ret_5d)

    logger.info("  扫描完成: %d个交易日, %d个有效计算", total_scanned, total_computed)

    # 分析每个因子
    logger.info("[3/3] 因子分析...")
    factor_report = {}

    for fname in sorted(factor_data.keys()):
        vals = factor_data[fname]['vals']
        rets = factor_data[fname]['rets']
        n = len(vals)
        if n < 100:
            factor_report[fname] = {'n': n, 'note': '样本不足'}
            continue

        ic = compute_ic(vals, rets)
        qa = quintile_analysis(vals, rets)

        factor_report[fname] = {
            'n': n,
            'ic': round(ic, 4) if ic is not None else None,
            'abs_ic': round(abs(ic), 4) if ic is not None else None,
            'quintile': qa,
        }

    # 保存报告
    full_report = {
        'meta': {
            'total_scanned': total_scanned,
            'total_computed': total_computed,
            'n_stocks': len(kline_data),
            'n_stocks_with_fund_flow': len(fund_flow_data),
            'date_range': f'{start_date} ~ {end_date}',
            'run_time_sec': round(time.time() - t0, 1),
        },
        'factor_analysis': factor_report,
    }

    output_path = OUTPUT_DIR / "sentiment_factors_backtest.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(full_report, f, ensure_ascii=False, indent=2, default=str)

    # ═══════════════════════════════════════════════════════════
    # 打印结果
    # ═══════════════════════════════════════════════════════════
    print(f"\n数据范围: {start_date} ~ {end_date}")
    print(f"股票: {len(kline_data)}只 (其中{len(fund_flow_data)}只有资金流向)")
    print(f"扫描: {total_scanned}个交易日, 有效: {total_computed}个")

    # 按|IC|排序
    ranked = sorted(
        [(fname, r) for fname, r in factor_report.items() if r.get('ic') is not None],
        key=lambda x: abs(x[1]['ic']),
        reverse=True,
    )

    print(f"\n{'═' * 90}")
    print("📊 因子IC排名（按|IC|降序，IC>0.03有参考价值）")
    print(f"{'═' * 90}")
    print(f"  {'排名':>4s} {'因子名':<25s} {'样本':>7s} {'IC':>8s} {'|IC|':>6s} "
          f"{'单调性':>6s} {'Q5-Q1':>8s} {'Q1均收益':>9s} {'Q5均收益':>9s} {'判定':>4s}")
    print(f"  {'─' * 85}")

    for rank, (fname, r) in enumerate(ranked, 1):
        ic = r['ic']
        qa = r.get('quintile')
        mono = qa['monotonic'] if qa else 'N/A'
        spread = qa['spread_Q5_Q1'] if qa else 0
        q1_ret = qa['quintiles']['Q1']['avg_return'] if qa else 0
        q5_ret = qa['quintiles']['Q5']['avg_return'] if qa else 0

        # 判定：|IC| > 0.03 且有单调性
        abs_ic = abs(ic)
        if abs_ic > 0.05 and mono != 'none':
            verdict = '🔥'
        elif abs_ic > 0.03 and mono != 'none':
            verdict = '✅'
        elif abs_ic > 0.03:
            verdict = '⚠️'
        elif abs_ic > 0.02:
            verdict = '🔍'
        else:
            verdict = '❌'

        mono_str = '↑' if mono == 'up' else ('↓' if mono == 'down' else '~')

        print(f"  {rank:>4d} {fname:<25s} {r['n']:>7d} {ic:>+8.4f} {abs_ic:>6.4f} "
              f"{mono_str:>6s} {spread:>+8.4f} {q1_ret:>+9.4f}% {q5_ret:>+9.4f}% {verdict:>4s}")

    # 详细打印Top因子的分档收益
    print(f"\n{'═' * 90}")
    print("🔬 Top因子分档详情（|IC| > 0.02的因子）")
    print(f"{'═' * 90}")

    for fname, r in ranked:
        if abs(r['ic']) < 0.02:
            continue
        qa = r.get('quintile')
        if not qa:
            continue
        print(f"\n  📌 {fname} (IC={r['ic']:+.4f}, n={r['n']})")
        print(f"     {'档位':<6s} {'样本':>6s} {'均收益':>10s} {'上涨比例':>8s} {'因子范围':<25s}")
        for q in ['Q1', 'Q2', 'Q3', 'Q4', 'Q5']:
            qd = qa['quintiles'][q]
            print(f"     {q:<6s} {qd['n']:>6d} {qd['avg_return']:>+10.4f}% "
                  f"{qd['up_ratio']:>8.1%} {qd['factor_range']:<25s}")
        print(f"     单调性: {qa['monotonic']}, Q5-Q1 spread: {qa['spread_Q5_Q1']:+.4f}%")

    # 总结
    print(f"\n{'═' * 90}")
    print("📋 总结")
    print(f"{'═' * 90}")
    strong = [f for f, r in ranked if abs(r['ic']) > 0.05 and r.get('quintile', {}).get('monotonic', 'none') != 'none']
    good = [f for f, r in ranked if 0.03 < abs(r['ic']) <= 0.05 and r.get('quintile', {}).get('monotonic', 'none') != 'none']
    weak = [f for f, r in ranked if 0.02 < abs(r['ic']) <= 0.03]

    print(f"\n  🔥 强因子 (|IC|>0.05 + 单调): {len(strong)}个")
    for f in strong:
        r = factor_report[f]
        print(f"     {f}: IC={r['ic']:+.4f}")
    print(f"\n  ✅ 有效因子 (|IC|>0.03 + 单调): {len(good)}个")
    for f in good:
        r = factor_report[f]
        print(f"     {f}: IC={r['ic']:+.4f}")
    print(f"\n  🔍 待观察 (|IC| 0.02~0.03): {len(weak)}个")
    for f in weak:
        r = factor_report[f]
        print(f"     {f}: IC={r['ic']:+.4f}")

    print(f"\n⏱️  总耗时: {time.time() - t0:.1f}秒")
    print(f"📁 完整报告: {output_path}")
    print("=" * 70)

    return full_report


if __name__ == '__main__':
    run_backtest()
