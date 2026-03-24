#!/usr/bin/env python3
"""
量价关系理论 V3 — 多条件叠加 + 全量数据 + 严格防过拟合
======================================================
V2发现：单一量价场景准确率天花板约60%，难以突破65%。
V3策略：放弃"场景分类"思路，改用"条件叠加评分"：
  - 每个独立条件贡献±1分
  - 只有多个条件同时满足（高分）时才出信号
  - 高分信号 = 多个独立因子共振 → 准确率更高

条件来源（全部基于V1/V2验证过的有效因子，不新增阈值）：
  1. 均值回归力：高位放量上涨后回调概率大（V2验证59%）
  2. 卖压衰竭力：低位缩量下跌后反弹概率大（V2验证59%）
  3. 连涨过热：连涨≥3日后回调概率大（V2验证59%）
  4. 波动率放大：高波动+放量 → 方向信号更强
  5. 均线趋势：均线空头/多头排列确认方向
  6. 量价效率：放量但价格不动 = 方向即将改变

防过拟合：
  - 3折时间交叉验证（每折独立验证）
  - 月度滚动一致性
  - 不优化任何阈值（全部用V1/V2已验证的标准值）
  - 全量数据（所有可用股票）

用法：
    source .venv/bin/activate
    python -m tools.backtest_volume_price_v3
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)
OUTPUT_DIR = Path(__file__).parent.parent / "data_results"


def _f(v):
    try:
        return float(v) if v is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


def load_all_stock_codes():
    """加载全部股票代码（排除北交所、ST等）"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT stock_code FROM stock_kline "
        "WHERE stock_code NOT LIKE '4%%' AND stock_code NOT LIKE '8%%' "
        "AND stock_code NOT LIKE '9%%' ORDER BY stock_code")
    codes = [r['stock_code'] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return codes


def load_kline_batch(stock_codes, start_date, end_date):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = defaultdict(list)
    bs = 500
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
                'd': str(row['date']),
                'c': _f(row['close_price']),
                'o': _f(row['open_price']),
                'h': _f(row['high_price']),
                'l': _f(row['low_price']),
                'v': _f(row['trading_volume']),
                'p': _f(row['change_percent']),
                't': _f(row.get('change_hand')),
            })
    cur.close()
    conn.close()
    return dict(result)


# ═══════════════════════════════════════════════════════════════
# 核心：多条件评分系统
# ═══════════════════════════════════════════════════════════════

def score_bearish(klines):
    """
    看跌评分：每个独立条件+1分，返回(总分, 触发条件列表)。
    所有条件均来自V1/V2已验证的有效因子。
    """
    n = len(klines)
    if n < 60:
        return 0, []

    c = [k['c'] for k in klines]
    v = [k['v'] for k in klines]
    p = [k['p'] for k in klines]
    h = [k['h'] for k in klines]
    l = [k['l'] for k in klines]
    o = [k['o'] for k in klines]

    if c[-1] <= 0 or v[-1] <= 0:
        return 0, []

    score = 0
    reasons = []

    # ── 条件1: 高位（60日区间上1/3）──
    h60 = max(x for x in h[-60:] if x > 0) if any(x > 0 for x in h[-60:]) else 0
    l60 = min(x for x in l[-60:] if x > 0) if any(x > 0 for x in l[-60:]) else 0
    if h60 > l60 > 0:
        pos = (c[-1] - l60) / (h60 - l60)
    else:
        return 0, []

    if pos >= 0.67:
        score += 1
        reasons.append('高位')

    # ── 条件2: 近5日放量（量比>1.5）──
    vol_20 = sum(v[-20:]) / 20
    vol_5 = sum(v[-5:]) / 5
    if vol_20 <= 0:
        return 0, []
    vol_ratio = vol_5 / vol_20

    if vol_ratio >= 1.5:
        score += 1
        reasons.append(f'放量{vol_ratio:.1f}x')

    # ── 条件3: 近5日上涨（涨幅>3%）──
    base = c[-6] if n >= 6 and c[-6] > 0 else c[-5]
    if base <= 0:
        return 0, []
    ret_5d = (c[-1] / base - 1) * 100

    if ret_5d > 3:
        score += 1
        reasons.append(f'近5日涨{ret_5d:.1f}%')

    # ── 条件4: 连涨≥3日 ──
    consec_up = 0
    for i in range(n - 1, max(n - 10, 0), -1):
        if p[i] > 0:
            consec_up += 1
        else:
            break
    if consec_up >= 3:
        score += 1
        reasons.append(f'连涨{consec_up}日')

    # ── 条件5: 高波动率（20日波动率>3%）──
    rets_20 = [(c[i] / c[i-1] - 1) * 100 for i in range(n-20, n) if c[i-1] > 0]
    if rets_20:
        mean_r = sum(rets_20) / len(rets_20)
        volatility = (sum((r - mean_r)**2 for r in rets_20) / len(rets_20)) ** 0.5
    else:
        volatility = 0
    if volatility > 3:
        score += 1
        reasons.append(f'高波动{volatility:.1f}%')

    # ── 条件6: 量价效率低（放量但涨幅小 = 量价损耗）──
    if vol_ratio > 1.2 and 0 < abs(ret_5d) < 2:
        score += 1
        reasons.append('量价损耗')

    # ── 条件7: 上影线多（近5日≥2根长上影线）──
    upper_shadows = 0
    for i in range(-5, 0):
        body = abs(c[i] - o[i])
        upper = h[i] - max(c[i], o[i]) if h[i] > 0 else 0
        if body > 0 and upper > body * 1.5:
            upper_shadows += 1
    if upper_shadows >= 2:
        score += 1
        reasons.append(f'上影线{upper_shadows}根')

    return score, reasons


def score_bullish(klines):
    """
    看涨评分：每个独立条件+1分。
    """
    n = len(klines)
    if n < 60:
        return 0, []

    c = [k['c'] for k in klines]
    v = [k['v'] for k in klines]
    p = [k['p'] for k in klines]
    h = [k['h'] for k in klines]
    l = [k['l'] for k in klines]
    o = [k['o'] for k in klines]

    if c[-1] <= 0 or v[-1] <= 0:
        return 0, []

    score = 0
    reasons = []

    # ── 条件1: 低位（60日区间下1/3）──
    h60 = max(x for x in h[-60:] if x > 0) if any(x > 0 for x in h[-60:]) else 0
    l60 = min(x for x in l[-60:] if x > 0) if any(x > 0 for x in l[-60:]) else 0
    if h60 > l60 > 0:
        pos = (c[-1] - l60) / (h60 - l60)
    else:
        return 0, []

    if pos <= 0.33:
        score += 1
        reasons.append('低位')

    # ── 条件2: 近5日缩量（量比<0.7）──
    vol_20 = sum(v[-20:]) / 20
    vol_5 = sum(v[-5:]) / 5
    if vol_20 <= 0:
        return 0, []
    vol_ratio = vol_5 / vol_20

    if vol_ratio <= 0.7:
        score += 1
        reasons.append(f'缩量{vol_ratio:.1f}x')

    # ── 条件3: 近5日下跌（跌幅>3%）──
    base = c[-6] if n >= 6 and c[-6] > 0 else c[-5]
    if base <= 0:
        return 0, []
    ret_5d = (c[-1] / base - 1) * 100

    if ret_5d < -3:
        score += 1
        reasons.append(f'近5日跌{ret_5d:.1f}%')

    # ── 条件4: 连跌≥3日 ──
    consec_down = 0
    for i in range(n - 1, max(n - 10, 0), -1):
        if p[i] < 0:
            consec_down += 1
        else:
            break
    if consec_down >= 3:
        score += 1
        reasons.append(f'连跌{consec_down}日')

    # ── 条件5: 量能持续萎缩 ──
    vol_contracting = all(v[-i] <= v[-i-1] * 1.1 for i in range(1, 4) if v[-i-1] > 0)
    if vol_contracting:
        score += 1
        reasons.append('量能持续萎缩')

    # ── 条件6: 下影线多（近5日≥2根长下影线 = 有承接）──
    lower_shadows = 0
    for i in range(-5, 0):
        body = abs(c[i] - o[i])
        lower = min(c[i], o[i]) - l[i] if l[i] > 0 else 0
        if body > 0 and lower > body * 1.5:
            lower_shadows += 1
    if lower_shadows >= 2:
        score += 1
        reasons.append(f'下影线{lower_shadows}根')

    # ── 条件7: MA20斜率转平或向上（卖压减弱）──
    ma20 = sum(c[-20:]) / 20
    if n >= 40:
        ma20_prev = sum(c[-40:-20]) / 20
        ma20_slope = (ma20 / ma20_prev - 1) * 100 if ma20_prev > 0 else 0
    else:
        ma20_slope = 0
    if -2 < ma20_slope < 2:
        score += 1
        reasons.append('MA20走平')

    return score, reasons


# ═══════════════════════════════════════════════════════════════
# 未来收益 + 统计
# ═══════════════════════════════════════════════════════════════

def future_ret(klines, idx, horizon=5):
    base = klines[idx]['c']
    if base <= 0 or idx + horizon >= len(klines):
        return None
    fut = klines[idx + horizon]['c']
    return round((fut / base - 1) * 100, 2) if fut > 0 else None


def stats(rets, direction='UP'):
    """计算准确率和收益统计"""
    if not rets:
        return None
    n = len(rets)
    if direction == 'UP':
        correct = sum(1 for r in rets if r > 0)
    else:
        correct = sum(1 for r in rets if r < 0)
    acc = correct / n
    avg = sum(rets) / n
    med = sorted(rets)[n // 2]

    if direction == 'UP':
        wins = [r for r in rets if r > 0]
        losses = [-r for r in rets if r < 0]
    else:
        wins = [-r for r in rets if r < 0]
        losses = [r for r in rets if r > 0]
    aw = sum(wins) / len(wins) if wins else 0
    al = sum(losses) / len(losses) if losses else 0.001
    plr = round(aw / al, 2) if al > 0 else 99
    ev = round(acc * aw - (1 - acc) * al, 2)

    return {'n': n, 'acc': round(acc, 4), 'avg': round(avg, 2),
            'med': round(med, 2), 'plr': plr, 'ev': ev}


# ═══════════════════════════════════════════════════════════════
# 主回测
# ═══════════════════════════════════════════════════════════════

def run_v3():
    t0 = time.time()
    print("=" * 80)
    print("量价关系 V3 — 多条件叠加评分 + 全量数据 + 3折交叉验证")
    print("=" * 80)

    # ── 加载全量数据 ──
    logger.info("[1/4] 加载全量数据...")
    all_codes = load_all_stock_codes()
    logger.info("  全部股票: %d只", len(all_codes))

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=450)).strftime('%Y-%m-%d')
    kline_data = load_kline_batch(all_codes, start_date, end_date)
    logger.info("  加载完成: %d只有数据", len(kline_data))

    # ── 扫描评分 ──
    logger.info("[2/4] 扫描评分...")
    bear_records = defaultdict(list)  # score -> [(date, ret_5d, ret_10d, reasons)]
    bull_records = defaultdict(list)
    total = 0

    for code, klines in kline_data.items():
        if len(klines) < 80:
            continue
        for i in range(60, len(klines) - 10):
            total += 1
            hist = klines[:i + 1]

            # 看跌评分
            bs, br = score_bearish(hist)
            if bs >= 2:
                r5 = future_ret(klines, i, 5)
                r10 = future_ret(klines, i, 10)
                if r5 is not None:
                    bear_records[bs].append({
                        'date': klines[i]['d'],
                        'month': klines[i]['d'][:7],
                        'code': code,
                        'r5': r5,
                        'r10': r10,
                        'reasons': br,
                    })

            # 看涨评分
            us, ur = score_bullish(hist)
            if us >= 2:
                r5 = future_ret(klines, i, 5)
                r10 = future_ret(klines, i, 10)
                if r5 is not None:
                    bull_records[us].append({
                        'date': klines[i]['d'],
                        'month': klines[i]['d'][:7],
                        'code': code,
                        'r5': r5,
                        'r10': r10,
                        'reasons': ur,
                    })

    logger.info("  扫描%d日完成", total)

    # ── 3折时间交叉验证 ──
    logger.info("[3/4] 3折时间交叉验证...")
    all_months = sorted(set(
        r['month'] for recs in bear_records.values() for r in recs
    ) | set(
        r['month'] for recs in bull_records.values() for r in recs
    ))
    n_months = len(all_months)
    fold_size = n_months // 3

    folds = [
        set(all_months[:fold_size]),
        set(all_months[fold_size:2*fold_size]),
        set(all_months[2*fold_size:]),
    ]
    fold_names = [
        f"{all_months[0]}~{all_months[fold_size-1]}",
        f"{all_months[fold_size]}~{all_months[2*fold_size-1]}",
        f"{all_months[2*fold_size]}~{all_months[-1]}",
    ]

    report = {'meta': {
        'total_scanned': total,
        'n_stocks': len(kline_data),
        'date_range': f'{start_date} ~ {end_date}',
        'n_months': n_months,
        'folds': fold_names,
    }}

    # ── 看跌信号分析 ──
    print(f"\n数据: {len(kline_data)}只股票, {start_date}~{end_date}, 扫描{total}日")
    print(f"3折: {' | '.join(fold_names)}")

    print(f"\n{'═' * 80}")
    print("📉 看跌信号（高位放量上涨后回调）")
    print(f"{'═' * 80}")
    print(f"  {'评分':>4s} {'样本':>7s} {'全量准确':>8s} {'均收益5d':>9s} "
          f"{'折1':>7s} {'折2':>7s} {'折3':>7s} {'最低折':>7s} {'月胜率':>6s} {'月std':>6s} {'判定':>4s}")
    print(f"  {'─' * 78}")

    bear_report = {}
    for score in sorted(bear_records.keys(), reverse=True):
        recs = bear_records[score]
        rets_5 = [r['r5'] for r in recs]
        s_all = stats(rets_5, 'DOWN')
        if not s_all or s_all['n'] < 30:
            continue

        # 3折验证
        fold_accs = []
        for fi, fold_months in enumerate(folds):
            fold_rets = [r['r5'] for r in recs if r['month'] in fold_months]
            fs = stats(fold_rets, 'DOWN')
            fold_accs.append(fs['acc'] if fs and fs['n'] >= 10 else None)

        valid_folds = [a for a in fold_accs if a is not None]
        min_fold = min(valid_folds) if valid_folds else 0

        # 月度一致性
        monthly = defaultdict(list)
        for r in recs:
            monthly[r['month']].append(r['r5'])
        m_accs = []
        for month, mrs in monthly.items():
            ms = stats(mrs, 'DOWN')
            if ms and ms['n'] >= 10:
                m_accs.append(ms['acc'])
        m_win = sum(1 for a in m_accs if a > 0.5) / len(m_accs) if m_accs else 0
        m_std = (sum((a - sum(m_accs)/len(m_accs))**2 for a in m_accs) / len(m_accs))**0.5 if len(m_accs) >= 2 else 0

        # 判定：全量>65% + 最低折>55% + 月胜率>60%
        passed = (s_all['acc'] >= 0.65 and min_fold >= 0.55 and m_win >= 0.6)
        good = (s_all['acc'] >= 0.60 and min_fold >= 0.52)
        verdict = '✅' if passed else ('⚠️' if good else '❌')

        fa_strs = [f"{a:.1%}" if a is not None else "  N/A" for a in fold_accs]

        print(f"  {score:>4d} {s_all['n']:>7d} {s_all['acc']:>8.1%} {s_all['avg']:>+9.2f}% "
              f"{fa_strs[0]:>7s} {fa_strs[1]:>7s} {fa_strs[2]:>7s} {min_fold:>7.1%} "
              f"{m_win:>6.0%} {m_std:>6.1%} {verdict:>4s}")

        bear_report[f'bear_score_{score}'] = {
            'n': s_all['n'], 'acc': s_all['acc'], 'avg_ret': s_all['avg'],
            'plr': s_all['plr'], 'ev': s_all['ev'],
            'fold_accs': fold_accs, 'min_fold': min_fold,
            'monthly_win_rate': round(m_win, 3), 'monthly_std': round(m_std, 4),
            'verdict': verdict,
        }

    # ── 看涨信号分析 ──
    print(f"\n{'═' * 80}")
    print("📈 看涨信号（低位缩量下跌后反弹）")
    print(f"{'═' * 80}")
    print(f"  {'评分':>4s} {'样本':>7s} {'全量准确':>8s} {'均收益5d':>9s} "
          f"{'折1':>7s} {'折2':>7s} {'折3':>7s} {'最低折':>7s} {'月胜率':>6s} {'月std':>6s} {'判定':>4s}")
    print(f"  {'─' * 78}")

    bull_report = {}
    for score in sorted(bull_records.keys(), reverse=True):
        recs = bull_records[score]
        rets_5 = [r['r5'] for r in recs]
        s_all = stats(rets_5, 'UP')
        if not s_all or s_all['n'] < 30:
            continue

        fold_accs = []
        for fi, fold_months in enumerate(folds):
            fold_rets = [r['r5'] for r in recs if r['month'] in fold_months]
            fs = stats(fold_rets, 'UP')
            fold_accs.append(fs['acc'] if fs and fs['n'] >= 10 else None)

        valid_folds = [a for a in fold_accs if a is not None]
        min_fold = min(valid_folds) if valid_folds else 0

        monthly = defaultdict(list)
        for r in recs:
            monthly[r['month']].append(r['r5'])
        m_accs = []
        for month, mrs in monthly.items():
            ms = stats(mrs, 'UP')
            if ms and ms['n'] >= 10:
                m_accs.append(ms['acc'])
        m_win = sum(1 for a in m_accs if a > 0.5) / len(m_accs) if m_accs else 0
        m_std = (sum((a - sum(m_accs)/len(m_accs))**2 for a in m_accs) / len(m_accs))**0.5 if len(m_accs) >= 2 else 0

        passed = (s_all['acc'] >= 0.65 and min_fold >= 0.55 and m_win >= 0.6)
        good = (s_all['acc'] >= 0.60 and min_fold >= 0.52)
        verdict = '✅' if passed else ('⚠️' if good else '❌')

        fa_strs = [f"{a:.1%}" if a is not None else "  N/A" for a in fold_accs]

        print(f"  {score:>4d} {s_all['n']:>7d} {s_all['acc']:>8.1%} {s_all['avg']:>+9.2f}% "
              f"{fa_strs[0]:>7s} {fa_strs[1]:>7s} {fa_strs[2]:>7s} {min_fold:>7.1%} "
              f"{m_win:>6.0%} {m_std:>6.1%} {verdict:>4s}")

        bull_report[f'bull_score_{score}'] = {
            'n': s_all['n'], 'acc': s_all['acc'], 'avg_ret': s_all['avg'],
            'plr': s_all['plr'], 'ev': s_all['ev'],
            'fold_accs': fold_accs, 'min_fold': min_fold,
            'monthly_win_rate': round(m_win, 3), 'monthly_std': round(m_std, 4),
            'verdict': verdict,
        }

    report['bear_signals'] = bear_report
    report['bull_signals'] = bull_report

    # ── 条件贡献度分析 ──
    logger.info("[4/4] 条件贡献度分析...")
    print(f"\n{'═' * 80}")
    print("🔍 看跌条件贡献度（各条件独立准确率）")
    print(f"{'═' * 80}")

    # 分析每个条件在高分信号中的出现频率和贡献
    bear_high = []
    for sc in sorted(bear_records.keys(), reverse=True):
        if sc >= 4:
            bear_high.extend(bear_records[sc])

    if bear_high:
        reason_stats = defaultdict(lambda: {'total': 0, 'correct': 0})
        for r in bear_high:
            is_correct = r['r5'] < 0
            for reason in r['reasons']:
                key = reason.split('(')[0].split('.')[0]  # 去掉数值部分
                # 提取条件名
                for tag in ['高位', '放量', '近5日涨', '连涨', '高波动', '量价损耗', '上影线']:
                    if tag in reason:
                        reason_stats[tag]['total'] += 1
                        if is_correct:
                            reason_stats[tag]['correct'] += 1
                        break

        for tag, st in sorted(reason_stats.items(), key=lambda x: x[1]['correct']/max(x[1]['total'],1), reverse=True):
            if st['total'] >= 10:
                acc = st['correct'] / st['total']
                print(f"  {tag:<12s}: {acc:.1%} ({st['total']}次)")

    print(f"\n{'═' * 80}")
    print("🔍 看涨条件贡献度")
    print(f"{'═' * 80}")

    bull_high = []
    for sc in sorted(bull_records.keys(), reverse=True):
        if sc >= 4:
            bull_high.extend(bull_records[sc])

    if bull_high:
        reason_stats = defaultdict(lambda: {'total': 0, 'correct': 0})
        for r in bull_high:
            is_correct = r['r5'] > 0
            for reason in r['reasons']:
                for tag in ['低位', '缩量', '近5日跌', '连跌', '量能持续萎缩', '下影线', 'MA20走平']:
                    if tag in reason:
                        reason_stats[tag]['total'] += 1
                        if is_correct:
                            reason_stats[tag]['correct'] += 1
                        break

        for tag, st in sorted(reason_stats.items(), key=lambda x: x[1]['correct']/max(x[1]['total'],1), reverse=True):
            if st['total'] >= 10:
                acc = st['correct'] / st['total']
                print(f"  {tag:<16s}: {acc:.1%} ({st['total']}次)")

    # ── 最终推荐 ──
    print(f"\n{'═' * 80}")
    print("📋 最终推荐（全量≥65% + 最低折≥55% + 月胜率≥60%）")
    print(f"{'═' * 80}")

    any_passed = False
    for label, rpt in [('看跌', bear_report), ('看涨', bull_report)]:
        for name, data in sorted(rpt.items(), key=lambda x: x[1]['acc'], reverse=True):
            if data['verdict'] == '✅':
                any_passed = True
                print(f"\n  ✅ {label} {name}")
                print(f"     准确率: {data['acc']:.1%} | 样本: {data['n']} | "
                      f"均收益: {data['avg_ret']:+.2f}% | 盈亏比: {data['plr']:.1f} | "
                      f"期望: {data['ev']:+.2f}%")
                print(f"     3折: {data['fold_accs']} | 最低折: {data['min_fold']:.1%}")
                print(f"     月胜率: {data['monthly_win_rate']:.0%} | 月std: {data['monthly_std']:.1%}")

    if not any_passed:
        print("\n  无信号通过全部严格条件，显示最接近的：")
        all_items = [(f'看跌 {k}', v) for k, v in bear_report.items()]
        all_items += [(f'看涨 {k}', v) for k, v in bull_report.items()]
        all_items.sort(key=lambda x: x[1]['acc'], reverse=True)
        for name, data in all_items[:5]:
            print(f"  ⚠️ {name}: {data['acc']:.1%} (n={data['n']}, "
                  f"最低折={data['min_fold']:.1%}, 月胜率={data['monthly_win_rate']:.0%})")

    # 保存
    output_path = OUTPUT_DIR / "volume_price_v3_backtest.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n⏱️  耗时: {time.time() - t0:.1f}秒")
    print(f"📁 报告: {output_path}")
    print("=" * 80)
    return report


if __name__ == '__main__':
    run_v3()
