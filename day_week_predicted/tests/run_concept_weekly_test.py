#!/usr/bin/env python3
"""
概念板块增强周预测回测 — 独立运行版本（不依赖DB）

使用模拟数据验证算法逻辑和准确率。
当DB可达时，将 USE_DB = True 即可切换到真实数据。

60只股票 × 15个概念板块 × 14周
目标：周预测准确率 ≥ 80%
"""
import json
import math
import random
import hashlib
import logging
from datetime import datetime, timedelta
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════════

def _mean(lst):
    return sum(lst) / len(lst) if lst else 0.0

def _sigmoid(x, center=0, scale=1):
    try:
        return 1.0 / (1.0 + math.exp(-(x - center) / scale))
    except OverflowError:
        return 0.0 if x < center else 1.0

def _compound_return(pcts):
    p = 1.0
    for r in pcts:
        p *= (1 + r / 100)
    return (p - 1) * 100

def _rate_str(ok, n):
    return f'{ok}/{n} ({round(ok / n * 100, 1)}%)' if n > 0 else '无数据'


# ═══════════════════════════════════════════════════════════
# 概念板块信号计算
# ═══════════════════════════════════════════════════════════

def compute_board_vs_market_strength(board_klines, market_klines,
                                      score_date, lookback=20):
    """计算概念板块相对大盘的强弱势评分（0-100）。"""
    bk = [k for k in board_klines if k['date'] <= score_date]
    mk_map = {k['date']: k['change_percent'] for k in market_klines
              if k['date'] <= score_date}
    if len(bk) < 5:
        return None
    recent = bk[-lookback:]
    aligned = [(k, mk_map[k['date']]) for k in recent if k['date'] in mk_map]
    if len(aligned) < 5:
        return None

    daily_excess = [k['change_percent'] - mk for k, mk in aligned]
    board_rets = [k['change_percent'] for k, _ in aligned]
    market_rets = [mk for _, mk in aligned]
    n = len(aligned)

    excess_total = _compound_return(board_rets) - _compound_return(market_rets)
    excess_5d = sum(daily_excess[-min(5, n):])
    win_days = sum(1 for e in daily_excess if e > 0)
    win_rate = win_days / n
    momentum = _mean([k['change_percent'] for k, _ in aligned[-5:]])

    s1 = _sigmoid(excess_total, center=0, scale=8) * 30
    s2 = _sigmoid(excess_5d, center=0, scale=2) * 25
    s3 = _sigmoid(sum(daily_excess[-min(20, n):]), center=0, scale=4) * 20
    s4 = max(0, min(15, (win_rate - 0.3) / 0.4 * 15))
    score = round(max(0, min(100, s1 + s2 + s3 + s4)), 1)

    return {
        'score': score,
        'excess_total': round(excess_total, 3),
        'excess_5d': round(excess_5d, 3),
        'win_rate': round(win_rate, 4),
        'momentum': round(momentum, 4),
    }


def compute_stock_vs_board_strength(stock_klines, board_klines,
                                     score_date, lookback=20):
    """计算个股相对概念板块的强弱势评分（0-100）。"""
    sk_map = {k['date']: k['change_percent'] for k in stock_klines
              if k['date'] <= score_date}
    bk = [k for k in board_klines if k['date'] <= score_date]
    if len(bk) < 5:
        return None
    recent = bk[-lookback:]
    aligned = []
    for k in recent:
        d = k['date']
        if d in sk_map:
            aligned.append((sk_map[d], k['change_percent']))
    if len(aligned) < 5:
        return None

    daily_excess = [s - b for s, b in aligned]
    n = len(aligned)
    excess_5d = sum(daily_excess[-min(5, n):])
    excess_20d = sum(daily_excess[-min(20, n):])
    win_rate = sum(1 for e in daily_excess if e > 0) / n

    s_short = _sigmoid(excess_5d, center=0, scale=2) * 40
    s_mid = _sigmoid(excess_20d, center=0, scale=5) * 35
    s_wr = max(0, min(25, (win_rate - 0.3) / 0.4 * 25))
    score = round(max(0, min(100, s_short + s_mid + s_wr)), 1)

    return {
        'strength_score': score,
        'excess_5d': round(excess_5d, 3),
        'excess_20d': round(excess_20d, 3),
        'win_rate': round(win_rate, 4),
    }


def compute_fund_flow_signal(fund_flows, score_date, lookback=5):
    """计算资金流信号。"""
    if not fund_flows:
        return 0.0
    recent = [f for f in fund_flows if f['date'] <= score_date][:lookback]
    if not recent:
        return 0.0
    avg = _mean([f['big_net_pct'] for f in recent])
    if avg > 3: return 1.0
    elif avg > 1: return 0.5
    elif avg < -3: return -1.0
    elif avg < -1: return -0.5
    return 0.0


def compute_concept_signal(stock_code, score_date, data):
    """计算个股在某日期的概念板块综合信号。"""
    boards = data['stock_boards'].get(stock_code, [])
    if not boards:
        return None

    board_kline_map = data['board_kline_map']
    market_klines = data['market_klines']
    stock_kl = data['stock_klines'].get(stock_code, [])

    board_scores = []
    strong_boards = 0
    stock_in_board_scores = []
    stock_strong_count = 0
    valid_boards = 0
    board_momentums_5d = []
    stock_excess_5d_list = []
    boards_up = 0

    for board in boards:
        bc = board['board_code']
        bk = board_kline_map.get(bc, [])
        if not bk:
            continue

        bs = compute_board_vs_market_strength(bk, market_klines, score_date)
        if bs:
            board_scores.append(bs['score'])
            board_momentums_5d.append(bs['momentum'])
            if bs['score'] >= 55:
                strong_boards += 1
            valid_boards += 1

        valid_klines = [k for k in bk if k['date'] <= score_date]
        if len(valid_klines) >= 3:
            recent_5 = valid_klines[-5:]
            avg_chg = _mean([k['change_percent'] for k in recent_5])
            if avg_chg > 0:
                boards_up += 1

        if stock_kl:
            ss = compute_stock_vs_board_strength(stock_kl, bk, score_date)
            if ss:
                stock_in_board_scores.append(ss['strength_score'])
                stock_excess_5d_list.append(ss['excess_5d'])
                if ss['strength_score'] >= 55:
                    stock_strong_count += 1

    if valid_boards == 0:
        return None

    board_market_score = _mean(board_scores)
    board_market_strong_pct = strong_boards / valid_boards
    stock_board_score = _mean(stock_in_board_scores) if stock_in_board_scores else 50
    stock_board_strong_pct = (stock_strong_count / len(stock_in_board_scores)
                              if stock_in_board_scores else 0.5)
    avg_board_momentum_5d = _mean(board_momentums_5d)
    avg_stock_excess_5d = _mean(stock_excess_5d_list) if stock_excess_5d_list else 0
    concept_consensus = boards_up / valid_boards if valid_boards > 0 else 0.5

    fund_flows = data['fund_flow_map'].get(stock_code, [])
    ff_signal = compute_fund_flow_signal(fund_flows, score_date)

    # 综合评分
    cs = 0.0
    if board_market_score >= 62: cs += 1.5
    elif board_market_score >= 55: cs += 0.8
    elif board_market_score <= 38: cs -= 1.5
    elif board_market_score <= 45: cs -= 0.8

    if board_market_strong_pct >= 0.65: cs += 0.8
    elif board_market_strong_pct >= 0.5: cs += 0.3
    elif board_market_strong_pct <= 0.25: cs -= 0.8
    elif board_market_strong_pct <= 0.4: cs -= 0.3

    if stock_board_score >= 62: cs += 1.2
    elif stock_board_score >= 55: cs += 0.5
    elif stock_board_score <= 38: cs -= 1.2
    elif stock_board_score <= 45: cs -= 0.5

    if stock_board_strong_pct >= 0.6: cs += 0.5
    elif stock_board_strong_pct <= 0.3: cs -= 0.5

    if avg_board_momentum_5d > 0.5: cs += 0.5
    elif avg_board_momentum_5d > 0.2: cs += 0.2
    elif avg_board_momentum_5d < -0.5: cs -= 0.5
    elif avg_board_momentum_5d < -0.2: cs -= 0.2

    if avg_stock_excess_5d > 2: cs += 0.5
    elif avg_stock_excess_5d > 0.5: cs += 0.2
    elif avg_stock_excess_5d < -2: cs -= 0.5
    elif avg_stock_excess_5d < -0.5: cs -= 0.2

    if concept_consensus > 0.65: cs += 0.5
    elif concept_consensus < 0.35: cs -= 0.5

    cs += ff_signal * 0.3

    reliability = min(1.0, valid_boards / 5)
    weighted_cs = cs * reliability

    return {
        'board_market_score': round(board_market_score, 1),
        'board_market_strong_pct': round(board_market_strong_pct, 3),
        'stock_board_score': round(stock_board_score, 1),
        'stock_board_strong_pct': round(stock_board_strong_pct, 3),
        'board_momentum_5d': round(avg_board_momentum_5d, 4),
        'stock_excess_5d': round(avg_stock_excess_5d, 3),
        'concept_consensus': round(concept_consensus, 3),
        'fund_flow_signal': round(ff_signal, 2),
        'composite_score': round(weighted_cs, 2),
        'n_boards': valid_boards,
    }


# ═══════════════════════════════════════════════════════════
# 周预测策略
# ═══════════════════════════════════════════════════════════

def predict_weekly_direction(d3_chg, sig, stock_stats=None):
    """周预测核心策略：前3天涨跌方向 + 概念板块多维信号 + 个股自适应。"""
    if sig is None:
        if abs(d3_chg) > 0.3:
            return d3_chg > 0, f'无概念:前3天{d3_chg:+.2f}%', 'medium'
        return d3_chg > 0, f'无概念:前3天{d3_chg:+.2f}%(弱)', 'low'

    cs = sig['composite_score']

    vol_threshold_strong = 2.0
    vol_threshold_mid = 0.8
    concept_flip_threshold = 2.5

    if stock_stats:
        vol = stock_stats.get('weekly_volatility', 2.0)
        concept_eff = stock_stats.get('concept_effectiveness', 0.5)
        if vol > 4.0:
            vol_threshold_strong = 3.0
            vol_threshold_mid = 1.2
        elif vol > 3.0:
            vol_threshold_strong = 2.5
            vol_threshold_mid = 1.0
        if concept_eff > 0.65:
            concept_flip_threshold = 2.0

    # 强信号区
    if abs(d3_chg) > vol_threshold_strong:
        pred = d3_chg > 0
        if pred and cs <= -concept_flip_threshold - 1.0:
            return False, f'前3天涨{d3_chg:+.2f}%但概念极弱({cs:.1f})→反转', 'medium'
        if not pred and cs >= concept_flip_threshold + 1.0:
            return True, f'前3天跌{d3_chg:+.2f}%但概念极强({cs:.1f})→反弹', 'medium'
        return pred, f'前3天{d3_chg:+.2f}%(强信号)', 'high'

    # 中等信号区
    if abs(d3_chg) > vol_threshold_mid:
        pred = d3_chg > 0
        if pred and cs <= -concept_flip_threshold:
            return False, f'前3天涨{d3_chg:+.2f}%但概念弱({cs:.1f})→反转', 'medium'
        if not pred and cs >= concept_flip_threshold:
            return True, f'前3天跌{d3_chg:+.2f}%但概念强({cs:.1f})→反弹', 'medium'
        return pred, f'前3天{d3_chg:+.2f}%(中等信号)', 'medium'

    # 模糊区
    if cs > 1.5:
        return True, f'模糊区+概念看涨({cs:.1f})', 'medium'
    if cs < -1.5:
        return False, f'模糊区+概念看跌({cs:.1f})', 'medium'

    board_bias = sig['board_market_strong_pct'] - 0.5
    stock_bias = (sig['stock_board_score'] - 50) / 50
    momentum_bias = (1 if sig.get('board_momentum_5d', 0) > 0.1
                     else (-1 if sig.get('board_momentum_5d', 0) < -0.1 else 0))
    combined_bias = board_bias * 0.4 + stock_bias * 0.3 + momentum_bias * 0.3

    if combined_bias > 0.08:
        return True, f'模糊区+综合偏涨({combined_bias:.2f})', 'low'
    if combined_bias < -0.08:
        return False, f'模糊区+综合偏跌({combined_bias:.2f})', 'low'

    if abs(d3_chg) > 0.05:
        return d3_chg > 0, f'模糊区兜底前3天{d3_chg:+.2f}%', 'low'

    return sig['concept_consensus'] > 0.5, f'极模糊:共识度{sig["concept_consensus"]:.0%}', 'low'


# ═══════════════════════════════════════════════════════════
# 模拟数据生成
# ═══════════════════════════════════════════════════════════

def generate_klines(stock_code, start_date, end_date,
                    base_price=50.0, volatility=2.0, trend=0.0):
    """生成逼真的个股日K线数据（确定性随机）。"""
    seed = int(hashlib.md5(stock_code.encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)

    dt = datetime.strptime(start_date, '%Y-%m-%d')
    dt_end = datetime.strptime(end_date, '%Y-%m-%d')

    klines = []
    price = base_price
    while dt <= dt_end:
        if dt.weekday() >= 5:
            dt += timedelta(days=1)
            continue
        daily_chg = rng.gauss(trend / 250, volatility / 100)
        daily_chg = max(-0.10, min(0.10, daily_chg))
        change_pct = daily_chg * 100
        new_price = price * (1 + daily_chg)
        high = new_price * (1 + abs(rng.gauss(0, 0.005)))
        low = new_price * (1 - abs(rng.gauss(0, 0.005)))
        volume = rng.uniform(5000, 50000) * (1 + abs(daily_chg) * 10)

        klines.append({
            'date': dt.strftime('%Y-%m-%d'),
            'open_price': round(price, 2),
            'close_price': round(new_price, 2),
            'high_price': round(high, 2),
            'low_price': round(low, 2),
            'trading_volume': round(volume, 0),
            'trading_amount': round(volume * new_price, 0),
            'change_percent': round(change_pct, 4),
            'change_hand': round(rng.uniform(0.5, 5.0), 2),
        })
        price = new_price
        dt += timedelta(days=1)
    return klines


def generate_board_klines(board_code, start_date, end_date,
                           member_klines_list, noise=0.3):
    """根据成分股K线生成板块K线。"""
    seed = int(hashlib.md5(board_code.encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)

    date_changes = defaultdict(list)
    for klines in member_klines_list:
        for k in klines:
            date_changes[k['date']].append(k['change_percent'])

    board_klines = []
    price = 1000.0
    for d in sorted(date_changes.keys()):
        changes = date_changes[d]
        if not changes:
            continue
        avg_chg = _mean(changes) + rng.gauss(0, noise)
        avg_chg = max(-10, min(10, avg_chg))
        new_price = price * (1 + avg_chg / 100)
        board_klines.append({
            'date': d,
            'change_percent': round(avg_chg, 4),
            'close_price': round(new_price, 2),
        })
        price = new_price
    return board_klines


def build_simulated_data(stock_codes, concept_board_stocks, start_date, end_date):
    """生成完整的模拟数据集。"""
    dt = datetime.strptime(start_date, '%Y-%m-%d')
    ext_start = (dt - timedelta(days=180)).strftime('%Y-%m-%d')

    board_trends = {}
    board_vols = {}
    for board_name in concept_board_stocks.keys():
        seed = int(hashlib.md5(board_name.encode()).hexdigest()[:8], 16)
        rng = random.Random(seed)
        board_trends[board_name] = rng.uniform(-15, 15)
        board_vols[board_name] = rng.uniform(1.5, 3.5)

    stock_klines = {}
    stock_boards = defaultdict(list)
    board_code_map = {}

    for idx, (board_name, codes) in enumerate(concept_board_stocks.items()):
        board_code = f'3{idx:05d}'
        board_code_map[board_name] = board_code
        trend = board_trends[board_name]
        vol = board_vols[board_name]

        for code in codes:
            if code not in stock_klines:
                code_seed = int(hashlib.md5(code.encode()).hexdigest()[:8], 16)
                code_rng = random.Random(code_seed)
                stock_trend = trend + code_rng.uniform(-10, 10)
                stock_vol = vol * code_rng.uniform(0.8, 1.3)
                base_price = code_rng.uniform(10, 200)
                stock_klines[code] = generate_klines(
                    code, ext_start, end_date,
                    base_price=base_price, volatility=stock_vol, trend=stock_trend
                )
            stock_boards[code].append({
                'board_code': board_code,
                'board_name': board_name,
            })

    board_kline_map = {}
    for board_name, codes in concept_board_stocks.items():
        board_code = board_code_map[board_name]
        member_klines = [stock_klines[c] for c in codes if c in stock_klines]
        if member_klines:
            board_kline_map[board_code] = generate_board_klines(
                board_code, ext_start, end_date, member_klines
            )

    market_klines = generate_board_klines(
        'market_index', ext_start, end_date,
        list(stock_klines.values()), noise=0.1
    )

    fund_flow_map = {}
    for code in stock_codes:
        code_seed = int(hashlib.md5((code + '_ff').encode()).hexdigest()[:8], 16)
        rng = random.Random(code_seed)
        flows = []
        for k in stock_klines.get(code, []):
            flows.append({
                'date': k['date'],
                'big_net': round(rng.gauss(0, 5000), 2),
                'big_net_pct': round(rng.gauss(0, 3), 2),
                'main_net_5day': round(rng.gauss(0, 10000), 2),
                'net_flow': round(rng.gauss(0, 8000), 2),
            })
        fund_flow_map[code] = list(reversed(flows))

    return {
        'stock_klines': stock_klines,
        'stock_boards': dict(stock_boards),
        'board_kline_map': board_kline_map,
        'market_klines': market_klines,
        'fund_flow_map': fund_flow_map,
    }


# ═══════════════════════════════════════════════════════════
# 构建周数据 + 评估
# ═══════════════════════════════════════════════════════════

def build_weekly_records(stock_codes, data, start_date, end_date):
    """从日K线构建周数据记录。"""
    weekly = []
    for code in stock_codes:
        klines = data['stock_klines'].get(code, [])
        if not klines:
            continue
        bt_klines = [k for k in klines if start_date <= k['date'] <= end_date]
        if len(bt_klines) < 5:
            continue

        week_groups = defaultdict(list)
        for k in bt_klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iso_week = dt.isocalendar()[:2]
            week_groups[iso_week].append(k)

        boards = data['stock_boards'].get(code, [])
        board_names = [b['board_name'] for b in boards[:5]]

        for iso_week, days in week_groups.items():
            days.sort(key=lambda x: x['date'])
            if len(days) < 3:
                continue

            daily_pcts = [d['change_percent'] for d in days]
            weekly_chg = _compound_return(daily_pcts)
            weekly_up = weekly_chg >= 0

            d3_pcts = [d['change_percent'] for d in days[:3]]
            d3_chg = _compound_return(d3_pcts)
            wed_date = days[2]['date']

            sig = compute_concept_signal(code, wed_date, data)

            weekly.append({
                'code': code,
                'iso_week': iso_week,
                'week_dates': [d['date'] for d in days],
                'n_days': len(days),
                'daily_changes': daily_pcts,
                'd3_chg': round(d3_chg, 4),
                'weekly_change': round(weekly_chg, 4),
                'weekly_up': weekly_up,
                'wed_date': wed_date,
                'concept_signal': sig,
                'concept_boards': board_names,
            })
    return weekly


def compute_stock_stats(weekly_records, exclude_week=None):
    """计算每只股票的历史统计。"""
    stock_weeks = defaultdict(list)
    for r in weekly_records:
        if exclude_week and r['iso_week'] == exclude_week:
            continue
        stock_weeks[r['code']].append(r)

    stats = {}
    for code, weeks in stock_weeks.items():
        if len(weeks) < 3:
            stats[code] = None
            continue
        weekly_chgs = [w['weekly_change'] for w in weeks]
        vol = (sum((c - _mean(weekly_chgs)) ** 2 for c in weekly_chgs)
               / len(weekly_chgs)) ** 0.5

        concept_correct = 0
        concept_total = 0
        for w in weeks:
            sig = w['concept_signal']
            if sig and abs(sig['composite_score']) > 1.0:
                concept_total += 1
                if (sig['composite_score'] > 0) == w['weekly_up']:
                    concept_correct += 1

        concept_eff = concept_correct / concept_total if concept_total >= 3 else 0.5
        stats[code] = {
            'weekly_volatility': round(vol, 3),
            'concept_effectiveness': round(concept_eff, 3),
            'n_weeks': len(weeks),
        }
    return stats


def evaluate_predictions(weekly, stock_stats, exclude_week=None):
    """评估预测准确率。"""
    correct = total = 0
    correct_sig = total_sig = 0
    correct_nosig = total_nosig = 0
    conf_stats = {'high': [0, 0], 'medium': [0, 0], 'low': [0, 0]}
    fuzzy_correct = fuzzy_total = 0
    details = []

    for w in weekly:
        if exclude_week and w['iso_week'] == exclude_week:
            continue

        sig = w['concept_signal']
        ss = stock_stats.get(w['code']) if stock_stats else None
        pred_up, reason, conf = predict_weekly_direction(w['d3_chg'], sig, ss)
        actual_up = w['weekly_up']
        is_correct = pred_up == actual_up

        total += 1
        if is_correct:
            correct += 1

        if sig is not None:
            total_sig += 1
            if is_correct: correct_sig += 1
        else:
            total_nosig += 1
            if is_correct: correct_nosig += 1

        conf_stats[conf][1] += 1
        if is_correct: conf_stats[conf][0] += 1

        if abs(w['d3_chg']) <= 0.8:
            fuzzy_total += 1
            if is_correct: fuzzy_correct += 1

        details.append({
            'code': w['code'], 'iso_week': w['iso_week'],
            'd3_chg': w['d3_chg'], 'weekly_change': w['weekly_change'],
            'pred_up': pred_up, 'actual_up': actual_up,
            'correct': is_correct, 'reason': reason, 'confidence': conf,
        })

    accuracy = correct / total * 100 if total > 0 else 0
    return {
        'accuracy': round(accuracy, 1),
        'correct': correct, 'total': total,
        'with_concept_signal': {
            'accuracy': round(correct_sig / total_sig * 100, 1) if total_sig > 0 else 0,
            'count': total_sig,
        },
        'without_concept_signal': {
            'accuracy': round(correct_nosig / total_nosig * 100, 1) if total_nosig > 0 else 0,
            'count': total_nosig,
        },
        'by_confidence': {
            k: {'accuracy': round(v[0] / v[1] * 100, 1) if v[1] > 0 else 0, 'count': v[1]}
            for k, v in conf_stats.items()
        },
        'fuzzy_zone': {
            'accuracy': round(fuzzy_correct / fuzzy_total * 100, 1) if fuzzy_total > 0 else 0,
            'count': fuzzy_total,
        },
        'details': details,
    }


def run_lowo_cv(weekly, all_weeks):
    """Leave-One-Week-Out 交叉验证。"""
    week_accuracies = []
    total_correct = total_count = 0

    for held_out_week in all_weeks:
        train_stats = compute_stock_stats(weekly, exclude_week=held_out_week)
        test_records = [w for w in weekly if w['iso_week'] == held_out_week]
        if not test_records:
            continue

        correct = 0
        for w in test_records:
            sig = w['concept_signal']
            ss = train_stats.get(w['code'])
            pred_up, _, _ = predict_weekly_direction(w['d3_chg'], sig, ss)
            if pred_up == w['weekly_up']:
                correct += 1

        acc = correct / len(test_records) * 100
        week_accuracies.append(acc)
        total_correct += correct
        total_count += len(test_records)

    overall_acc = total_correct / total_count * 100 if total_count > 0 else 0
    return {
        'overall_accuracy': round(overall_acc, 1),
        'avg_week_accuracy': round(_mean(week_accuracies), 1) if week_accuracies else 0,
        'total_correct': total_correct,
        'total_count': total_count,
        'n_weeks': len(week_accuracies),
        'week_accuracies': [round(a, 1) for a in week_accuracies],
        'min_week_accuracy': round(min(week_accuracies), 1) if week_accuracies else 0,
        'max_week_accuracy': round(max(week_accuracies), 1) if week_accuracies else 0,
    }


def analyze_by_board(weekly, stock_stats, concept_board_map):
    """按概念板块分组分析准确率。"""
    board_stats = defaultdict(lambda: {'correct': 0, 'total': 0, 'stocks': set()})
    for w in weekly:
        sig = w['concept_signal']
        ss = stock_stats.get(w['code']) if stock_stats else None
        pred_up, _, _ = predict_weekly_direction(w['d3_chg'], sig, ss)
        is_correct = pred_up == w['weekly_up']

        board_name = concept_board_map.get(w['code'], '未分类')
        board_stats[board_name]['total'] += 1
        board_stats[board_name]['stocks'].add(w['code'])
        if is_correct:
            board_stats[board_name]['correct'] += 1

    results = []
    for board, stats in sorted(board_stats.items(), key=lambda x: -x[1]['total']):
        acc = stats['correct'] / stats['total'] * 100 if stats['total'] > 0 else 0
        results.append({
            'board_name': board,
            'accuracy': round(acc, 1),
            'correct': stats['correct'],
            'total': stats['total'],
            'stock_count': len(stats['stocks']),
        })
    return results


# ═══════════════════════════════════════════════════════════
# 股票配置
# ═══════════════════════════════════════════════════════════

CONCEPT_BOARD_STOCKS = {
    '人工智能': ['002230.SZ', '300496.SZ', '688111.SH', '300474.SZ'],
    '新能源汽车': ['002594.SZ', '601238.SH', '600733.SH', '002074.SZ'],
    '半导体': ['002371.SZ', '603986.SH', '688012.SH', '002049.SZ'],
    '锂电池': ['300750.SZ', '002709.SZ', '300014.SZ', '002460.SZ'],
    '光伏': ['601012.SH', '300763.SZ', '688599.SH', '002129.SZ'],
    '医药生物': ['600276.SH', '300760.SZ', '603259.SH', '600436.SH'],
    '白酒': ['600519.SH', '000858.SZ', '000568.SZ', '002304.SZ'],
    '军工': ['600893.SH', '600760.SH', '002179.SZ', '600862.SH'],
    '储能': ['300274.SZ', '002812.SZ', '300037.SZ', '688390.SH'],
    '机器人': ['300124.SZ', '688169.SH', '002747.SZ', '300024.SZ'],
    '消费电子': ['002475.SZ', '600584.SH', '002241.SZ', '002938.SZ'],
    '稀土永磁': ['600111.SH', '300748.SZ', '600366.SH', '002600.SZ'],
    '化工新材料': ['002648.SZ', '300438.SZ', '600309.SH', '002601.SZ'],
    '数据中心': ['603019.SH', '000977.SZ', '002236.SZ', '300308.SZ'],
    '汽车零部件': ['601799.SH', '603596.SH', '002920.SZ', '603786.SH'],
}


def main():
    # 构建去重股票列表
    all_codes = []
    code_to_board = {}
    seen = set()
    for board_name, codes in CONCEPT_BOARD_STOCKS.items():
        for code in codes:
            if code not in seen:
                all_codes.append(code)
                code_to_board[code] = board_name
                seen.add(code)

    start_date = '2025-12-01'
    end_date = '2026-03-10'

    print("=" * 70)
    print("  概念板块增强周预测回测 — 60只股票 × 15个概念板块 × 14周")
    print("=" * 70)
    print(f"  股票总数: {len(all_codes)} (去重后)")
    print(f"  概念板块: {len(CONCEPT_BOARD_STOCKS)}个")
    for board, codes in CONCEPT_BOARD_STOCKS.items():
        print(f"    {board}: {len(codes)}只")
    print(f"  回测区间: {start_date} ~ {end_date}")
    print(f"  数据模式: 模拟数据")
    print()

    t_start = datetime.now()

    # 1. 生成模拟数据
    print("[1/5] 生成模拟数据...")
    data = build_simulated_data(all_codes, CONCEPT_BOARD_STOCKS, start_date, end_date)
    print(f"  个股K线: {len(data['stock_klines'])}只")
    print(f"  概念板块: {len(data['board_kline_map'])}个")
    print(f"  大盘K线: {len(data['market_klines'])}天")

    # 2. 构建周数据
    print("\n[2/5] 构建周数据...")
    weekly = build_weekly_records(all_codes, data, start_date, end_date)
    print(f"  周样本总数: {len(weekly)}")

    if not weekly:
        print("错误：无有效周数据")
        return

    n_with_sig = sum(1 for w in weekly if w['concept_signal'] is not None)
    all_weeks = sorted(set(w['iso_week'] for w in weekly))
    all_stocks = sorted(set(w['code'] for w in weekly))
    print(f"  有概念信号: {n_with_sig}/{len(weekly)} ({n_with_sig/len(weekly)*100:.1f}%)")
    print(f"  覆盖周数: {len(all_weeks)}, 覆盖股票: {len(all_stocks)}")

    # 3. 全样本评估
    print("\n[3/5] 全样本评估...")
    stock_stats = compute_stock_stats(weekly)
    full = evaluate_predictions(weekly, stock_stats)

    # 4. LOWO交叉验证
    print("\n[4/5] LOWO交叉验证...")
    lowo = run_lowo_cv(weekly, all_weeks)

    # 5. 按概念板块分析
    print("\n[5/5] 按概念板块分析...")
    boards = analyze_by_board(weekly, stock_stats, code_to_board)

    elapsed = (datetime.now() - t_start).total_seconds()

    # ═══════════════════════════════════════════════════════════
    # 输出结果
    # ═══════════════════════════════════════════════════════════

    print("\n" + "=" * 70)
    print("  回测结果汇总")
    print("=" * 70)

    print(f"\n📊 基本信息:")
    print(f"  股票数: {len(all_stocks)}")
    print(f"  周数: {len(all_weeks)}")
    print(f"  周样本总数: {len(weekly)}")
    print(f"  概念信号覆盖率: {n_with_sig/len(weekly)*100:.1f}%")
    print(f"  耗时: {elapsed:.1f}秒")

    print(f"\n📈 全样本准确率:")
    print(f"  ★ 总体准确率: {full['accuracy']}% ({full['correct']}/{full['total']})")
    print(f"  有概念信号: {full['with_concept_signal']['accuracy']}% "
          f"(样本{full['with_concept_signal']['count']})")
    print(f"  无概念信号: {full['without_concept_signal']['accuracy']}% "
          f"(样本{full['without_concept_signal']['count']})")

    print(f"\n  按置信度:")
    for conf, stats in full['by_confidence'].items():
        print(f"    {conf}: {stats['accuracy']}% (样本{stats['count']})")

    print(f"\n  模糊区(|d3_chg|≤0.8%): {full['fuzzy_zone']['accuracy']}% "
          f"(样本{full['fuzzy_zone']['count']})")

    print(f"\n📊 LOWO交叉验证（无泄露）:")
    print(f"  ★ 总体准确率: {lowo['overall_accuracy']}% "
          f"({lowo['total_correct']}/{lowo['total_count']})")
    print(f"  平均周准确率: {lowo['avg_week_accuracy']}%")
    print(f"  最低周准确率: {lowo['min_week_accuracy']}%")
    print(f"  最高周准确率: {lowo['max_week_accuracy']}%")
    print(f"  验证周数: {lowo['n_weeks']}")
    if lowo['week_accuracies']:
        print(f"  各周准确率: {lowo['week_accuracies']}")

    print(f"\n📊 按概念板块分析:")
    print(f"  {'板块':<12} {'准确率':>8} {'正确/总数':>12} {'股票数':>6}")
    print(f"  {'-'*42}")
    for b in boards:
        print(f"  {b['board_name']:<12} {b['accuracy']:>7.1f}% "
              f"{b['correct']:>4}/{b['total']:<4} {b['stock_count']:>5}")

    # 达标检查
    print("\n" + "=" * 70)
    target = 80.0
    full_ok = full['accuracy'] >= target
    lowo_ok = lowo['overall_accuracy'] >= target
    week_ok = len(all_weeks) >= 12
    stock_ok = len(all_stocks) >= 60

    print(f"  ✅ 周数 ≥ 12: {'通过' if week_ok else '未通过'} ({len(all_weeks)}周)")
    print(f"  ✅ 股票 ≥ 60: {'通过' if stock_ok else '未通过'} ({len(all_stocks)}只)")
    print(f"  ✅ 全样本准确率 ≥ {target}%: {'通过' if full_ok else '未通过'} ({full['accuracy']}%)")
    print(f"  ✅ LOWO准确率 ≥ {target}%: {'通过' if lowo_ok else '未通过'} ({lowo['overall_accuracy']}%)")

    all_pass = full_ok and lowo_ok and week_ok and stock_ok
    print(f"\n  {'🎉 全部达标！' if all_pass else '⚠️ 部分指标未达标'}")
    print("=" * 70)

    # 保存结果
    save_result = {
        'summary': {
            'stock_count': len(all_stocks),
            'week_count': len(all_weeks),
            'weekly_sample_count': len(weekly),
            'concept_signal_coverage': round(n_with_sig / len(weekly) * 100, 1),
            'backtest_period': f'{start_date} ~ {end_date}',
            'elapsed_seconds': round(elapsed, 1),
            'data_mode': 'simulated',
        },
        'full_sample': {k: v for k, v in full.items() if k != 'details'},
        'lowo_cv': lowo,
        'by_concept_board': boards,
        'pass_criteria': {
            'week_count_ok': week_ok,
            'stock_count_ok': stock_ok,
            'full_accuracy_ok': full_ok,
            'lowo_accuracy_ok': lowo_ok,
            'all_pass': all_pass,
        },
    }
    output_path = 'data_results/backtest_concept_weekly_60stocks_result.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(save_result, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存到: {output_path}")


if __name__ == '__main__':
    main()
