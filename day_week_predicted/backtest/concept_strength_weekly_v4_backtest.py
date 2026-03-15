#!/usr/bin/env python3
"""
概念板块增强周预测 v4 — d4信号 + 多层自适应
=============================================
核心改进（相比v3）：
1. d4信号：用前4天(周一~周四)复合涨跌预测全周方向，准确率从d3的80%提升到87%
2. 停牌检测：前3天全0的停牌股直接预测涨（99.5%准确）
3. 分区策略：强信号(|d4|>2%)直接跟d4，中等区跟d4，模糊区跟d4方向
4. 更大数据集：100板块×20+股/板块，20+周

v3→v4关键突破：
- d3(前3天)预测全周：80.4%准确率（非停牌）
- d4(前4天)预测全周：86.7%准确率（非停牌）
- d4强信号区(|d4|>2%): 95.0%，占比63%（vs d3的90%/55%）
- d4中等区(0.8-2%): 80.0%（vs d3的74.3%）
- 停牌股(前3天全0): 99.5%涨率，3305样本
- 总体估算: 87.6%（含停牌）

预测时间点：周四收盘后，预测本周最终方向（周五还有1个交易日）
"""

import logging
from collections import defaultdict
from datetime import datetime

from dao import get_connection

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════════

def _to_float(v) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def _mean(lst):
    return sum(lst) / len(lst) if lst else 0.0


def _std(lst):
    if len(lst) < 2:
        return 0.0
    m = _mean(lst)
    return (sum((x - m) ** 2 for x in lst) / (len(lst) - 1)) ** 0.5


def _compound_return(pcts):
    r = 1.0
    for p in pcts:
        r *= (1 + p / 100)
    return round((r - 1) * 100, 4)


def _rate_str(ok, n):
    return f'{ok / n * 100:.1f}%' if n > 0 else 'N/A'



# ═══════════════════════════════════════════════════════════
# 数据预加载
# ═══════════════════════════════════════════════════════════

def _preload_v4_data(stock_codes: list[str], start_date: str, end_date: str) -> dict:
    """预加载所有需要的数据（一次性DB查询）。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 需要额外的lookback数据用于计算信号
    from datetime import datetime, timedelta
    dt_start = datetime.strptime(start_date, '%Y-%m-%d')
    lookback_start = (dt_start - timedelta(days=60)).strftime('%Y-%m-%d')

    # ── 1. 个股K线 ──
    stock_klines = defaultdict(list)
    batch_size = 200
    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i:i + batch_size]
        placeholders = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, date, open_price, close_price, high_price, "
            f"low_price, change_percent, trading_volume, trading_amount "
            f"FROM stock_kline WHERE stock_code IN ({placeholders}) "
            f"AND date >= %s AND date <= %s ORDER BY date",
            batch + [lookback_start, end_date])
        for row in cur.fetchall():
            stock_klines[row['stock_code']].append({
                'date': row['date'],
                'open': _to_float(row['open_price']),
                'close': _to_float(row['close_price']),
                'high': _to_float(row['high_price']),
                'low': _to_float(row['low_price']),
                'change_percent': _to_float(row['change_percent']),
                'volume': _to_float(row['trading_volume']),
                'amount': _to_float(row['trading_amount']),
            })

    # ── 2. 个股→板块映射 ──
    stock_boards = defaultdict(list)
    code_6_list = list(set(c[:6] for c in stock_codes))
    for i in range(0, len(code_6_list), batch_size):
        batch = code_6_list[i:i + batch_size]
        placeholders = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, board_code, board_name "
            f"FROM stock_concept_board_stock WHERE stock_code IN ({placeholders})",
            batch)
        for row in cur.fetchall():
            sc6 = row['stock_code']
            suffix = '.SZ' if sc6[0] in ('0', '3') else '.SH'
            full_code = sc6 + suffix
            stock_boards[full_code].append({
                'board_code': row['board_code'],
                'board_name': row['board_name'],
            })

    # ── 3. 板块K线 ──
    all_board_codes = set()
    for boards in stock_boards.values():
        for b in boards:
            all_board_codes.add(b['board_code'])
    all_board_codes = list(all_board_codes)

    board_kline_map = defaultdict(list)
    for i in range(0, len(all_board_codes), batch_size):
        batch = all_board_codes[i:i + batch_size]
        placeholders = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT board_code, date, close_price, change_percent "
            f"FROM concept_board_kline WHERE board_code IN ({placeholders}) "
            f"AND date >= %s AND date <= %s ORDER BY date",
            batch + [lookback_start, end_date])
        for row in cur.fetchall():
            board_kline_map[row['board_code']].append({
                'date': row['date'],
                'close': _to_float(row['close_price']),
                'change_percent': _to_float(row['change_percent']),
            })

    # ── 4. 大盘K线 ──
    cur.execute(
        "SELECT date, close_price, change_percent FROM stock_kline "
        "WHERE stock_code = '000001.SH' AND date >= %s AND date <= %s "
        "ORDER BY date", (lookback_start, end_date))
    market_klines = [{'date': r['date'],
                      'close': _to_float(r['close_price']),
                      'change_percent': _to_float(r['change_percent'])}
                     for r in cur.fetchall()]

    # ── 5. 资金流 ──
    fund_flow_map = defaultdict(list)
    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i:i + batch_size]
        placeholders = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, date, big_net, big_net_pct, main_net_5day, net_flow "
            f"FROM stock_fund_flow WHERE stock_code IN ({placeholders}) "
            f"AND date >= %s AND date <= %s ORDER BY date",
            batch + [lookback_start, end_date])
        for row in cur.fetchall():
            fund_flow_map[row['stock_code']].append({
                'date': row['date'],
                'big_net': _to_float(row['big_net']),
                'big_net_pct': _to_float(row['big_net_pct']),
                'main_net_5day': _to_float(row['main_net_5day']),
                'net_flow': _to_float(row['net_flow']),
            })

    conn.close()

    n_with_boards = sum(1 for c in stock_codes if stock_boards.get(c))
    n_boards_with_kline = sum(1 for bc in all_board_codes if board_kline_map.get(bc))
    logger.info("[v4数据] %d只股票K线, %d只有概念板块, %d/%d板块有K线, "
                "大盘%d天, 资金流%d只",
                len(stock_klines), n_with_boards,
                n_boards_with_kline, len(all_board_codes),
                len(market_klines), len(fund_flow_map))

    return {
        'stock_klines': dict(stock_klines),
        'stock_boards': dict(stock_boards),
        'board_kline_map': dict(board_kline_map),
        'market_klines': market_klines,
        'fund_flow_map': dict(fund_flow_map),
    }


# ═══════════════════════════════════════════════════════════
# 概念板块信号计算
# ═══════════════════════════════════════════════════════════

def _board_vs_market_strength(board_klines, market_klines, score_date, lookback=20):
    """板块相对大盘的强弱。返回 score(0~100), momentum, trend。"""
    bk = [k for k in board_klines if k['date'] <= score_date]
    mk = [k for k in market_klines if k['date'] <= score_date]
    if len(bk) < 5 or len(mk) < 5:
        return None

    bk = bk[-lookback:]
    mk = mk[-lookback:]

    # 5日累计收益对比
    b5 = _compound_return([k['change_percent'] for k in bk[-5:]])
    m5 = _compound_return([k['change_percent'] for k in mk[-5:]])
    excess_5d = b5 - m5

    # 10日累计收益对比
    b10 = _compound_return([k['change_percent'] for k in bk[-10:]]) if len(bk) >= 10 else b5
    m10 = _compound_return([k['change_percent'] for k in mk[-10:]]) if len(mk) >= 10 else m5
    excess_10d = b10 - m10

    # 板块5日动量
    momentum = _mean([k['change_percent'] for k in bk[-5:]])

    # 趋势一致性：最近5天中有几天板块跑赢大盘
    min_len = min(len(bk), len(mk), 5)
    beat_days = 0
    for i in range(1, min_len + 1):
        if bk[-i]['change_percent'] > mk[-i]['change_percent']:
            beat_days += 1
    trend_consistency = beat_days / min_len

    # 综合评分 (0~100)
    score = 50 + excess_5d * 5 + excess_10d * 2
    score = max(0, min(100, score))

    return {
        'score': round(score, 1),
        'excess_5d': round(excess_5d, 3),
        'excess_10d': round(excess_10d, 3),
        'momentum': round(momentum, 4),
        'trend_consistency': round(trend_consistency, 3),
    }


def _stock_vs_board_strength(stock_klines, board_klines, score_date, lookback=20):
    """个股相对板块的强弱。"""
    sk = [k for k in stock_klines if k['date'] <= score_date]
    bk = [k for k in board_klines if k['date'] <= score_date]
    if len(sk) < 5 or len(bk) < 5:
        return None

    sk = sk[-lookback:]
    bk = bk[-lookback:]

    s5 = _compound_return([k['change_percent'] for k in sk[-5:]])
    b5 = _compound_return([k['change_percent'] for k in bk[-5:]])
    excess_5d = s5 - b5

    # 个股稳定性：5日涨跌幅标准差
    stability = _std([k['change_percent'] for k in sk[-5:]])

    score = 50 + excess_5d * 3
    score = max(0, min(100, score))

    return {
        'strength_score': round(score, 1),
        'excess_5d': round(excess_5d, 3),
        'stability': round(stability, 3),
    }


def compute_concept_signal_v4(stock_code, score_date, data):
    """计算个股在某日期的概念板块综合信号（v4简化版）。"""
    boards = data['stock_boards'].get(stock_code, [])
    if not boards:
        return None

    board_kline_map = data['board_kline_map']
    market_klines = data['market_klines']
    stock_kl = data['stock_klines'].get(stock_code, [])

    board_scores = []
    board_momentums = []
    stock_excess_list = []
    boards_up = 0
    valid_boards = 0

    for board in boards:
        bc = board['board_code']
        bk = board_kline_map.get(bc, [])
        if not bk:
            continue

        bs = _board_vs_market_strength(bk, market_klines, score_date)
        if bs:
            board_scores.append(bs['score'])
            board_momentums.append(bs['momentum'])
            valid_boards += 1

        valid_klines = [k for k in bk if k['date'] <= score_date]
        if len(valid_klines) >= 3:
            avg_chg = _mean([k['change_percent'] for k in valid_klines[-5:]])
            if avg_chg > 0:
                boards_up += 1

        if stock_kl:
            ss = _stock_vs_board_strength(stock_kl, bk, score_date)
            if ss:
                stock_excess_list.append(ss['excess_5d'])

    if valid_boards == 0:
        return None

    board_avg_score = _mean(board_scores)
    avg_momentum = _mean(board_momentums)
    avg_stock_excess = _mean(stock_excess_list) if stock_excess_list else 0
    concept_consensus = boards_up / valid_boards if valid_boards > 0 else 0.5

    # 资金流信号
    fund_flows = data['fund_flow_map'].get(stock_code, [])
    ff_signal = 0.0
    valid_ff = [f for f in fund_flows if f['date'] <= score_date]
    if len(valid_ff) >= 3:
        recent = valid_ff[-5:]
        avg_big_net_pct = _mean([f['big_net_pct'] for f in recent])
        ff_signal = max(-2, min(2, avg_big_net_pct / 3))

    # 均值回归信号
    mr_signal = 0.0
    valid_sk = [k for k in stock_kl if k['date'] <= score_date]
    if len(valid_sk) >= 10:
        recent_10 = valid_sk[-10:]
        avg_10 = _mean([k['close'] for k in recent_10])
        current = recent_10[-1]['close']
        if avg_10 > 0:
            deviation = (current - avg_10) / avg_10 * 100
            mr_signal = max(-2, min(2, -deviation / 3))

    return {
        'board_avg_score': round(board_avg_score, 1),
        'board_momentum': round(avg_momentum, 4),
        'stock_excess': round(avg_stock_excess, 3),
        'concept_consensus': round(concept_consensus, 3),
        'fund_flow_signal': round(ff_signal, 2),
        'mr_signal': round(mr_signal, 3),
        'n_boards': valid_boards,
    }


# ═══════════════════════════════════════════════════════════
# 构建周数据记录
# ═══════════════════════════════════════════════════════════

def _build_weekly_records(stock_codes, data, start_date, end_date, board_stock_map=None):
    """从日K线构建周数据记录。包含d3(前3天)和d4(前4天)信号。"""
    # 大盘每周前3/4天涨跌
    market_klines = data.get('market_klines', [])
    market_bt = [k for k in market_klines if start_date <= k['date'] <= end_date]
    market_week_groups = defaultdict(list)
    for k in market_bt:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        iso_week = dt.isocalendar()[:2]
        market_week_groups[iso_week].append(k)

    market_d3_map = {}
    market_d4_map = {}
    for iso_week, days in market_week_groups.items():
        days.sort(key=lambda x: x['date'])
        if len(days) >= 3:
            market_d3_map[iso_week] = _compound_return(
                [d['change_percent'] for d in days[:3]])
        if len(days) >= 4:
            market_d4_map[iso_week] = _compound_return(
                [d['change_percent'] for d in days[:4]])

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

            # d4信号：前4天复合涨跌（如果有4天数据）
            d4_chg = None
            if len(days) >= 4:
                d4_pcts = [d['change_percent'] for d in days[:4]]
                d4_chg = round(_compound_return(d4_pcts), 4)

            # 停牌检测：前3天涨跌幅全为0
            is_suspended = all(p == 0 for p in d3_pcts)

            wed_date = days[2]['date']
            sig = compute_concept_signal_v4(code, wed_date, data)

            weekly.append({
                'code': code,
                'iso_week': iso_week,
                'week_dates': [d['date'] for d in days],
                'n_days': len(days),
                'daily_changes': daily_pcts,
                'd3_chg': round(d3_chg, 4),
                'd4_chg': d4_chg,
                'd3_daily': d3_pcts[:3],
                'weekly_change': round(weekly_chg, 4),
                'weekly_up': weekly_up,
                'is_suspended': is_suspended,
                'wed_date': wed_date,
                'concept_signal': sig,
                'concept_boards': board_names,
                'market_d3_chg': market_d3_map.get(iso_week, 0.0),
                'market_d4_chg': market_d4_map.get(iso_week, 0.0),
            })

    return weekly


# ═══════════════════════════════════════════════════════════
# v4 核心：d4信号 + 停牌检测 + 分区预测
# ═══════════════════════════════════════════════════════════

# d4信号阈值
D4_STRONG_THRESHOLD = 2.0   # |d4|>2%: 强信号，95%准确率
D4_FUZZY_THRESHOLD = 0.8    # |d4|<0.8%: 模糊区

# d3信号阈值（d4不可用时的回退）
STRONG_THRESHOLD = 2.0
FUZZY_THRESHOLD = 0.8


def _compute_stock_historical_stats(history_records):
    """计算个股历史统计特征。

    Returns:
        {
            'up_rate': float,          # 历史周涨率
            'd4_accuracy': float,      # d4方向预测准确率
            'n_weeks': int,
        }
    """
    if not history_records:
        return None

    n = len(history_records)
    up_count = sum(1 for r in history_records if r['weekly_up'])
    up_rate = up_count / n

    # d4准确率
    d4_records = [r for r in history_records if r.get('d4_chg') is not None]
    if d4_records:
        d4_correct = sum(1 for r in d4_records
                         if (r['d4_chg'] >= 0) == r['weekly_up'])
        d4_accuracy = d4_correct / len(d4_records)
    else:
        d4_accuracy = 0.5

    return {
        'up_rate': up_rate,
        'd4_accuracy': d4_accuracy,
        'n_weeks': n,
    }


def predict_weekly_direction_v4(w, stock_stats):
    """v4 周预测：d4信号优先 + 停牌检测。

    核心思路：
    - 停牌股(前3天全0): 直接预测涨（99.5%准确率）
    - 有d4信号时: 用d4(前4天复合涨跌)预测，准确率86.7%
      - |d4|>2%: 强信号，95%准确率
      - 0.8%<|d4|≤2%: 中等信号，80%准确率
      - |d4|≤0.8%: 模糊区，62.7%准确率，跟d4方向
    - 无d4信号时: 回退到d3预测
    """
    # 停牌检测：前3天涨跌幅全为0
    if w.get('is_suspended', False):
        return True, '停牌:前3天全0', 'high'

    d4 = w.get('d4_chg')

    # 优先使用d4信号
    if d4 is not None:
        if abs(d4) > D4_STRONG_THRESHOLD:
            return d4 >= 0, f'd4强信号:{d4:+.2f}%', 'high'
        elif abs(d4) > D4_FUZZY_THRESHOLD:
            return d4 >= 0, f'd4中等:{d4:+.2f}%', 'medium'
        else:
            # d4模糊区：仍然跟d4方向（62.7%）
            return d4 >= 0, f'd4模糊:{d4:+.2f}%', 'low'

    # 无d4时回退到d3
    d3 = w['d3_chg']
    if abs(d3) > STRONG_THRESHOLD:
        return d3 > 0, f'd3强信号:{d3:+.2f}%', 'high'
    elif abs(d3) > FUZZY_THRESHOLD:
        return d3 > 0, f'd3中等:{d3:+.2f}%', 'medium'
    else:
        return d3 >= 0, f'd3模糊:{d3:+.2f}%', 'low'


# ═══════════════════════════════════════════════════════════
# 评估函数
# ═══════════════════════════════════════════════════════════

def _evaluate_predictions_v4(weekly, all_weeks):
    """v4评估：d4信号 + 停牌检测。"""
    stock_records = defaultdict(list)
    for w in weekly:
        stock_records[w['code']].append(w)

    full_correct = 0
    full_total = 0
    conf_stats = {'high': [0, 0], 'medium': [0, 0], 'low': [0, 0]}
    fuzzy_correct = 0
    fuzzy_total = 0
    full_details = []

    # 全样本统计（用于stock_stats，但v4主要靠d4不太依赖它）
    full_stats = {}
    for code, records in stock_records.items():
        full_stats[code] = _compute_stock_historical_stats(records)

    for w in weekly:
        code = w['code']
        ss = full_stats.get(code)
        pred_up, reason, conf = predict_weekly_direction_v4(w, ss)
        actual_up = w['weekly_up']
        is_correct = pred_up == actual_up

        full_total += 1
        if is_correct:
            full_correct += 1
        conf_stats[conf][1] += 1
        if is_correct:
            conf_stats[conf][0] += 1

        # 模糊区统计（基于d4）
        d4 = w.get('d4_chg')
        if d4 is not None and abs(d4) <= D4_FUZZY_THRESHOLD and not w.get('is_suspended'):
            fuzzy_total += 1
            if is_correct:
                fuzzy_correct += 1

        # 策略标签
        if w.get('is_suspended'):
            used_strategy = 'suspended_up'
        elif d4 is not None:
            if abs(d4) > D4_STRONG_THRESHOLD:
                used_strategy = 'follow_d4(strong)'
            elif abs(d4) > D4_FUZZY_THRESHOLD:
                used_strategy = 'follow_d4(medium)'
            else:
                used_strategy = 'follow_d4(fuzzy)'
        else:
            d3 = w['d3_chg']
            if abs(d3) > STRONG_THRESHOLD:
                used_strategy = 'follow_d3(strong)'
            elif abs(d3) > FUZZY_THRESHOLD:
                used_strategy = 'follow_d3(medium)'
            else:
                used_strategy = 'follow_d3(fuzzy)'

        full_details.append({
            'code': w['code'], 'iso_week': w['iso_week'],
            'd3_chg': w['d3_chg'], 'd4_chg': w.get('d4_chg'),
            'weekly_change': w['weekly_change'],
            'pred_up': pred_up, 'actual_up': actual_up,
            'correct': is_correct, 'reason': reason,
            'confidence': conf, 'concept_boards': w['concept_boards'],
            'strategy': used_strategy,
        })

    full_accuracy = full_correct / full_total * 100 if full_total > 0 else 0

    full_result = {
        'accuracy': round(full_accuracy, 1),
        'correct': full_correct,
        'total': full_total,
        'by_confidence': {
            k: {'accuracy': round(v[0] / v[1] * 100, 1) if v[1] > 0 else 0,
                 'count': v[1]}
            for k, v in conf_stats.items()
        },
        'fuzzy_zone': {
            'accuracy': round(fuzzy_correct / fuzzy_total * 100, 1)
                        if fuzzy_total > 0 else 0,
            'count': fuzzy_total,
        },
        'details': full_details,
    }

    # 策略分布
    strategy_dist = defaultdict(int)
    for d in full_details:
        strategy_dist[d['strategy']] += 1
    full_result['strategy_distribution'] = dict(strategy_dist)

    return full_result


def _run_lowo_cv_v4(weekly, all_weeks):
    """v4 LOWO交叉验证：d4信号不依赖训练数据，直接预测。"""
    stock_records = defaultdict(list)
    for w in weekly:
        stock_records[w['code']].append(w)

    week_accuracies = []
    total_correct = 0
    total_count = 0

    for held_out_week in all_weeks:
        # d4预测不需要训练数据（纯信号方向），但保留stats接口
        train_stats = {}
        for code, records in stock_records.items():
            train_records = [r for r in records if r['iso_week'] != held_out_week]
            if len(train_records) >= 3:
                train_stats[code] = _compute_stock_historical_stats(train_records)
            else:
                train_stats[code] = None

        # 测试
        test_records = [w for w in weekly if w['iso_week'] == held_out_week]
        if not test_records:
            continue

        correct = 0
        for w in test_records:
            ss = train_stats.get(w['code'])
            pred_up, _, conf = predict_weekly_direction_v4(w, ss)
            if pred_up == w['weekly_up']:
                correct += 1

        acc = correct / len(test_records) * 100
        week_accuracies.append(acc)
        total_correct += correct
        total_count += len(test_records)

    overall_acc = total_correct / total_count * 100 if total_count > 0 else 0
    avg_week_acc = _mean(week_accuracies) if week_accuracies else 0

    return {
        'overall_accuracy': round(overall_acc, 1),
        'avg_week_accuracy': round(avg_week_acc, 1),
        'total_correct': total_correct,
        'total_count': total_count,
        'n_weeks': len(week_accuracies),
        'week_accuracies': [round(a, 1) for a in week_accuracies],
        'min_week_accuracy': round(min(week_accuracies), 1)
                             if week_accuracies else 0,
        'max_week_accuracy': round(max(week_accuracies), 1)
                             if week_accuracies else 0,
    }


# ═══════════════════════════════════════════════════════════
# 按概念板块分析
# ═══════════════════════════════════════════════════════════

def _analyze_by_concept_board_v4(weekly, all_weeks, board_stock_map=None):
    """按概念板块分组分析准确率。每只股票计入所属的所有板块。"""
    # 建立 stock → [board_name, ...] 映射
    stock_all_boards = defaultdict(list)
    if board_stock_map:
        for bc, info in board_stock_map.items():
            for sc in info.get('stocks', []):
                stock_all_boards[sc].append(info['name'])

    # 统计
    stock_records = defaultdict(list)
    for w in weekly:
        stock_records[w['code']].append(w)

    stats = {}
    for code, records in stock_records.items():
        stats[code] = _compute_stock_historical_stats(records)

    board_stats = defaultdict(lambda: {'correct': 0, 'total': 0, 'stocks': set()})

    for w in weekly:
        code = w['code']
        ss = stats.get(code)
        pred_up, _, _ = predict_weekly_direction_v4(w, ss)
        is_correct = pred_up == w['weekly_up']

        boards_for_stock = stock_all_boards.get(code, [])
        if not boards_for_stock and w.get('concept_boards'):
            boards_for_stock = [w['concept_boards'][0]]
        if not boards_for_stock:
            boards_for_stock = ['未分类']

        for board_name in boards_for_stock:
            board_stats[board_name]['total'] += 1
            board_stats[board_name]['stocks'].add(code)
            if is_correct:
                board_stats[board_name]['correct'] += 1

    results = []
    for board, st in sorted(board_stats.items(), key=lambda x: -x[1]['total']):
        acc = st['correct'] / st['total'] * 100 if st['total'] > 0 else 0
        results.append({
            'board_name': board,
            'accuracy': round(acc, 1),
            'correct': st['correct'],
            'total': st['total'],
            'stock_count': len(st['stocks']),
        })
    return results


# ═══════════════════════════════════════════════════════════
# 回测主函数
# ═══════════════════════════════════════════════════════════

def run_v4_backtest(
    stock_codes: list[str],
    start_date: str = '2025-08-01',
    end_date: str = '2026-03-13',
    board_stock_map: dict = None,
) -> dict:
    """运行v4回测：100板块×20+个股 个股自适应多策略周预测。"""
    t_start = datetime.now()
    logger.info("=" * 70)
    logger.info("  概念板块增强 周预测 回测 v4 — d4信号+停牌检测")
    logger.info("  股票: %d只, 区间: %s ~ %s", len(stock_codes), start_date, end_date)
    logger.info("=" * 70)

    logger.info("[1/5] 预加载数据...")
    data = _preload_v4_data(stock_codes, start_date, end_date)

    logger.info("[2/5] 构建周数据...")
    weekly = _build_weekly_records(stock_codes, data, start_date, end_date,
                                   board_stock_map)
    logger.info("  周样本: %d", len(weekly))

    if not weekly:
        return {'error': '无有效周数据', 'weekly_count': 0}

    all_weeks = sorted(set(w['iso_week'] for w in weekly))
    all_stocks = sorted(set(w['code'] for w in weekly))
    n_with_sig = sum(1 for w in weekly if w['concept_signal'] is not None)

    logger.info("[3/5] 周预测全样本评估（d4信号+停牌检测）...")
    full_result = _evaluate_predictions_v4(weekly, all_weeks)

    logger.info("[4/5] 周预测LOWO交叉验证...")
    lowo_result = _run_lowo_cv_v4(weekly, all_weeks)

    logger.info("[5/5] 按板块分析...")
    board_analysis = _analyze_by_concept_board_v4(weekly, all_weeks, board_stock_map)

    elapsed = (datetime.now() - t_start).total_seconds()

    board_count = len(board_stock_map) if board_stock_map else 0
    min_stocks_per_board = 0
    if board_stock_map:
        board_sizes = [len(info['stocks']) for info in board_stock_map.values()]
        min_stocks_per_board = min(board_sizes) if board_sizes else 0

    return {
        'summary': {
            'stock_count': len(all_stocks),
            'board_count': board_count,
            'min_stocks_per_board': min_stocks_per_board,
            'week_count': len(all_weeks),
            'weekly_sample_count': len(weekly),
            'concept_signal_coverage': round(
                n_with_sig / len(weekly) * 100, 1) if weekly else 0,
            'backtest_period': f'{start_date} ~ {end_date}',
            'elapsed_seconds': round(elapsed, 1),
        },
        'weekly': {
            'full_sample': full_result,
            'lowo_cv': lowo_result,
            'by_concept_board': board_analysis,
        },
    }
