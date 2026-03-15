"""
概念板块增强周预测回测引擎 v3
=================================
- 60个概念板块，每板块 ≥ 20只个股
- 15+周回测数据
- 个股自适应预测算法
- 目标准确率 ≥ 85%

核心策略：
1. 前3天涨跌方向作为主信号
2. 概念板块整体强弱 + 个股在板块中的相对强弱 修正预测
3. 个股历史统计自适应调整阈值和权重
4. 均值回归 + 资金流 + 日内模式 辅助信号
"""

import logging
from collections import defaultdict
from datetime import datetime, timedelta

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
    return f'{ok}/{n}={ok / n * 100:.1f}%' if n > 0 else '0/0'


# ═══════════════════════════════════════════════════════════
# 数据加载（批量从DB加载，支持1200+只股票）
# ═══════════════════════════════════════════════════════════

def _preload_v3_data(stock_codes: list[str], start_date: str, end_date: str) -> dict:
    """批量从DB预加载所有数据。针对大规模股票池优化。"""
    from dao import get_connection

    codes_6 = []
    full_map = {}
    for c in stock_codes:
        c6 = c.split('.')[0] if '.' in c else c
        codes_6.append(c6)
        full_map[c6] = c

    dt = datetime.strptime(start_date, '%Y-%m-%d')
    ext_start = (dt - timedelta(days=180)).strftime('%Y-%m-%d')

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        # 1. 个股K线（分批加载避免SQL过长）
        stock_klines = {}
        batch_size = 200
        all_query_codes = list(set(codes_6 + stock_codes))
        for i in range(0, len(all_query_codes), batch_size):
            batch = all_query_codes[i:i + batch_size]
            ph = ','.join(['%s'] * len(batch))
            cur.execute(
                f"SELECT stock_code, `date`, open_price, close_price, high_price, "
                f"low_price, trading_volume, trading_amount, change_percent, "
                f"change_hand, amplitude "
                f"FROM stock_kline "
                f"WHERE stock_code IN ({ph}) AND `date` >= %s AND `date` <= %s "
                f"ORDER BY stock_code, `date` ASC",
                (*batch, ext_start, end_date),
            )
            for r in cur.fetchall():
                code = r['stock_code']
                full = full_map.get(code, code)
                if full not in stock_klines:
                    stock_klines[full] = []
                stock_klines[full].append({
                    'date': r['date'],
                    'open_price': _to_float(r['open_price']),
                    'close_price': _to_float(r['close_price']),
                    'high_price': _to_float(r['high_price']),
                    'low_price': _to_float(r['low_price']),
                    'trading_volume': _to_float(r['trading_volume']),
                    'trading_amount': _to_float(r['trading_amount']),
                    'change_percent': _to_float(r['change_percent']),
                    'change_hand': _to_float(r['change_hand']),
                })

        # 2. 个股-概念板块映射（分批）
        stock_boards = defaultdict(list)
        all_board_codes = set()
        for i in range(0, len(codes_6), batch_size):
            batch = codes_6[i:i + batch_size]
            ph = ','.join(['%s'] * len(batch))
            cur.execute(
                f"SELECT stock_code, board_code, board_name "
                f"FROM stock_concept_board_stock "
                f"WHERE stock_code IN ({ph}) ORDER BY stock_code, board_code",
                tuple(batch),
            )
            for r in cur.fetchall():
                full = full_map.get(r['stock_code'], r['stock_code'])
                stock_boards[full].append({
                    'board_code': r['board_code'],
                    'board_name': r['board_name'],
                })
                all_board_codes.add(r['board_code'])

        # 3. 概念板块K线（分批）
        board_kline_map = defaultdict(list)
        bc_list = list(all_board_codes)
        for i in range(0, len(bc_list), batch_size):
            batch = bc_list[i:i + batch_size]
            ph = ','.join(['%s'] * len(batch))
            cur.execute(
                f"SELECT board_code, `date`, change_percent, close_price "
                f"FROM concept_board_kline "
                f"WHERE board_code IN ({ph}) AND `date` >= %s AND `date` <= %s "
                f"ORDER BY board_code, `date` ASC",
                (*batch, ext_start, end_date),
            )
            for r in cur.fetchall():
                board_kline_map[r['board_code']].append({
                    'date': r['date'],
                    'change_percent': _to_float(r['change_percent']),
                    'close_price': _to_float(r['close_price']),
                })

        # 4. 大盘K线
        cur.execute(
            "SELECT `date`, change_percent, close_price FROM stock_kline "
            "WHERE stock_code = '000001.SH' AND `date` >= %s AND `date` <= %s "
            "ORDER BY `date` ASC",
            (ext_start, end_date),
        )
        market_klines = [
            {'date': r['date'], 'change_percent': _to_float(r['change_percent']),
             'close_price': _to_float(r['close_price'])}
            for r in cur.fetchall()
        ]

        # 5. 资金流数据（分批）
        fund_flow_map = {}
        for i in range(0, len(all_query_codes), batch_size):
            batch = all_query_codes[i:i + batch_size]
            ph = ','.join(['%s'] * len(batch))
            cur.execute(
                f"SELECT stock_code, `date`, big_net, big_net_pct, "
                f"main_net_5day, net_flow "
                f"FROM stock_fund_flow "
                f"WHERE stock_code IN ({ph}) AND `date` >= %s AND `date` <= %s "
                f"ORDER BY stock_code, `date` DESC",
                (*batch, ext_start, end_date),
            )
            for r in cur.fetchall():
                code = r['stock_code']
                full = full_map.get(code, code)
                if full not in fund_flow_map:
                    fund_flow_map[full] = []
                fund_flow_map[full].append({
                    'date': r['date'],
                    'big_net': _to_float(r['big_net']),
                    'big_net_pct': _to_float(r['big_net_pct']),
                    'main_net_5day': _to_float(r['main_net_5day']),
                    'net_flow': _to_float(r['net_flow']),
                })

    finally:
        cur.close()
        conn.close()

    n_with_boards = sum(1 for c in stock_codes if c in stock_boards)
    n_with_kline = sum(1 for bc in all_board_codes if bc in board_kline_map)
    logger.info("[v3数据] %d只股票K线, %d只有概念板块, %d/%d板块有K线, "
                "大盘%d天, 资金流%d只",
                len(stock_klines), n_with_boards, n_with_kline,
                len(all_board_codes), len(market_klines), len(fund_flow_map))

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

def _compute_board_vs_market(board_klines, market_klines, score_date, lookback=20):
    """板块 vs 大盘强弱。"""
    bk = [k for k in board_klines if k['date'] <= score_date]
    mk = [k for k in market_klines if k['date'] <= score_date]
    if len(bk) < 5 or len(mk) < 5:
        return None

    bk = bk[-lookback:]
    mk_dates = {k['date']: k for k in mk}

    strong_days = 0
    total_days = 0
    board_5d = []
    board_10d = []

    for b in bk:
        m = mk_dates.get(b['date'])
        if m:
            total_days += 1
            if b['change_percent'] > m['change_percent']:
                strong_days += 1
            board_5d.append(b['change_percent'])
            board_10d.append(b['change_percent'])

    if total_days < 3:
        return None

    board_5d = board_5d[-5:]
    board_10d = board_10d[-10:]

    score = strong_days / total_days * 100
    momentum = _mean(board_5d) if board_5d else 0
    trend_10d = _mean(board_10d) if board_10d else 0

    # 趋势一致性：近5日方向一致的比例
    if len(board_5d) >= 3:
        pos = sum(1 for x in board_5d if x > 0)
        neg = sum(1 for x in board_5d if x < 0)
        consistency = max(pos, neg) / len(board_5d)
    else:
        consistency = 0.5

    return {
        'score': round(score, 1),
        'momentum': round(momentum, 4),
        'trend_10d': round(trend_10d, 4),
        'trend_consistency': round(consistency, 3),
    }


def _compute_stock_vs_board(stock_klines, board_klines, score_date, lookback=20):
    """个股 vs 板块强弱。"""
    sk = [k for k in stock_klines if k['date'] <= score_date]
    bk = [k for k in board_klines if k['date'] <= score_date]
    if len(sk) < 5 or len(bk) < 5:
        return None

    sk = sk[-lookback:]
    bk_dates = {k['date']: k for k in bk}

    strong_days = 0
    total_days = 0
    excess_5d = []

    for s in sk:
        b = bk_dates.get(s['date'])
        if b:
            total_days += 1
            if s['change_percent'] > b['change_percent']:
                strong_days += 1
            excess_5d.append(s['change_percent'] - b['change_percent'])

    if total_days < 3:
        return None

    excess_5d = excess_5d[-5:]
    score = strong_days / total_days * 100

    # 稳定性：超额收益的波动率
    stability = 1.0 / (1.0 + _std(excess_5d)) if len(excess_5d) >= 3 else 0.5

    return {
        'strength_score': round(score, 1),
        'excess_5d': round(_mean(excess_5d), 3),
        'stability': round(stability, 3),
    }


def _compute_fund_flow_signal(fund_flows, score_date, lookback=5):
    """资金流信号。"""
    if not fund_flows:
        return 0.0
    recent = [f for f in fund_flows if f['date'] <= score_date]
    recent.sort(key=lambda x: x['date'], reverse=True)
    recent = recent[:lookback]
    if not recent:
        return 0.0

    big_nets = [f['big_net_pct'] for f in recent if f['big_net_pct'] != 0]
    if not big_nets:
        return 0.0

    avg = _mean(big_nets)
    if avg > 3:
        return 1.0
    elif avg > 1:
        return 0.5
    elif avg < -3:
        return -1.0
    elif avg < -1:
        return -0.5
    return 0.0


def _compute_mean_reversion(stock_klines, score_date, lookback=10):
    """均值回归信号。"""
    kl = [k for k in stock_klines if k['date'] <= score_date]
    if len(kl) < lookback:
        return 0.0

    recent = kl[-lookback:]
    prices = [k['close_price'] for k in recent if k['close_price'] > 0]
    if len(prices) < 5:
        return 0.0

    ma = _mean(prices)
    current = prices[-1]
    if ma == 0:
        return 0.0

    deviation = (current - ma) / ma * 100

    if deviation > 8:
        return -1.0
    elif deviation > 4:
        return -0.5
    elif deviation < -8:
        return 1.0
    elif deviation < -4:
        return 0.5
    return -deviation / 10


def compute_concept_signal_v3(stock_code, score_date, data):
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
    board_momentums = []
    stock_excess_list = []
    boards_up = 0
    trend_consistencies = []
    stock_stabilities = []
    board_trend_10d = []

    for board in boards:
        bc = board['board_code']
        bk = board_kline_map.get(bc, [])
        if not bk:
            continue

        bs = _compute_board_vs_market(bk, market_klines, score_date)
        if bs:
            board_scores.append(bs['score'])
            board_momentums.append(bs['momentum'])
            trend_consistencies.append(bs['trend_consistency'])
            board_trend_10d.append(bs['trend_10d'])
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
            ss = _compute_stock_vs_board(stock_kl, bk, score_date)
            if ss:
                stock_in_board_scores.append(ss['strength_score'])
                stock_excess_list.append(ss['excess_5d'])
                stock_stabilities.append(ss['stability'])
                if ss['strength_score'] >= 55:
                    stock_strong_count += 1

    if valid_boards == 0:
        return None

    board_market_score = _mean(board_scores)
    board_market_strong_pct = strong_boards / valid_boards
    stock_board_score = _mean(stock_in_board_scores) if stock_in_board_scores else 50
    stock_board_strong_pct = (stock_strong_count / len(stock_in_board_scores)
                              if stock_in_board_scores else 0.5)
    avg_momentum = _mean(board_momentums)
    avg_stock_excess = _mean(stock_excess_list) if stock_excess_list else 0
    concept_consensus = boards_up / valid_boards if valid_boards > 0 else 0.5
    avg_trend_consistency = _mean(trend_consistencies) if trend_consistencies else 0.5
    avg_stock_stability = _mean(stock_stabilities) if stock_stabilities else 0.5
    avg_trend_10d = _mean(board_trend_10d) if board_trend_10d else 0

    fund_flows = data['fund_flow_map'].get(stock_code, [])
    ff_signal = _compute_fund_flow_signal(fund_flows, score_date)
    mr_signal = _compute_mean_reversion(stock_kl, score_date)

    # 综合评分
    cs = 0.0
    if board_market_score >= 62:
        cs += 1.5
    elif board_market_score >= 55:
        cs += 0.8
    elif board_market_score <= 38:
        cs -= 1.5
    elif board_market_score <= 45:
        cs -= 0.8

    if board_market_strong_pct >= 0.65:
        cs += 0.8
    elif board_market_strong_pct >= 0.5:
        cs += 0.3
    elif board_market_strong_pct <= 0.25:
        cs -= 0.8
    elif board_market_strong_pct <= 0.4:
        cs -= 0.3

    if stock_board_score >= 62:
        cs += 1.2
    elif stock_board_score >= 55:
        cs += 0.5
    elif stock_board_score <= 38:
        cs -= 1.2
    elif stock_board_score <= 45:
        cs -= 0.5

    if stock_board_strong_pct >= 0.6:
        cs += 0.5
    elif stock_board_strong_pct <= 0.3:
        cs -= 0.5

    if avg_momentum > 0.5:
        cs += 0.5
    elif avg_momentum > 0.2:
        cs += 0.2
    elif avg_momentum < -0.5:
        cs -= 0.5
    elif avg_momentum < -0.2:
        cs -= 0.2

    if avg_stock_excess > 2:
        cs += 0.5
    elif avg_stock_excess > 0.5:
        cs += 0.2
    elif avg_stock_excess < -2:
        cs -= 0.5
    elif avg_stock_excess < -0.5:
        cs -= 0.2

    if concept_consensus > 0.65:
        cs += 0.5
    elif concept_consensus < 0.35:
        cs -= 0.5

    cs += ff_signal * 0.3
    cs += mr_signal * 0.5

    reliability = min(1.0, valid_boards / 5) * (0.7 + 0.3 * avg_trend_consistency)
    weighted_cs = cs * reliability

    return {
        'board_market_score': round(board_market_score, 1),
        'board_market_strong_pct': round(board_market_strong_pct, 3),
        'stock_board_score': round(stock_board_score, 1),
        'stock_board_strong_pct': round(stock_board_strong_pct, 3),
        'board_momentum_5d': round(avg_momentum, 4),
        'board_trend_10d': round(avg_trend_10d, 4),
        'stock_excess_5d': round(avg_stock_excess, 3),
        'concept_consensus': round(concept_consensus, 3),
        'fund_flow_signal': round(ff_signal, 2),
        'mr_signal': round(mr_signal, 3),
        'trend_consistency': round(avg_trend_consistency, 3),
        'stock_stability': round(avg_stock_stability, 3),
        'composite_score': round(weighted_cs, 2),
        'n_boards': valid_boards,
    }


# ═══════════════════════════════════════════════════════════
# 周预测核心策略 v3（个股自适应）
# ═══════════════════════════════════════════════════════════

def predict_weekly_direction_v3(d3_chg, sig, stock_stats=None, daily_changes=None,
                                market_d3_chg=0.0):
    """v3 周预测：前3天方向 + 概念板块信号 + 个股自适应。

    v3.13 最终优化策略：
    经过12轮迭代优化和深度信号分析（46600样本），确认最优策略：

    信号区间划分（个股自适应波动率阈值）：
    - 强信号区(|d3|>vol_strong): 跟随d3方向 → 92.7% (high)
    - 中等信号区(vol_mid<|d3|<vol_strong): 跟随d3方向 → 83.0% (medium)
      概念板块强一致时提升为high
    - 模糊区(|d3|<vol_mid): 跟随d3方向 → 67.0% (low)

    深度分析结论（基于网格搜索、特征相关性、oracle分析）：
    - d3_chg是最强预测信号(corr=+0.527)，其他信号无法提供额外增益
    - 概念信号在模糊区为反向指标(corr=-0.125)，不应使用
    - mr_signal虽有-0.436相关性，但与d3高度共线，无独立贡献
    - 理论准确率上限: 82.2%（d3分段oracle）
    - 全样本85%数学上不可达（需fuzzy区>100%准确率）
    """
    if sig is None:
        return d3_chg >= 0, f'无概念:前3天{d3_chg:+.2f}%', 'medium'

    cs = sig['composite_score']
    board_momentum = sig.get('board_momentum_5d', 0)

    # ── 个股自适应阈值 ──
    vol_threshold_strong = 2.0
    vol_threshold_mid = 0.8

    if stock_stats:
        vol = stock_stats.get('weekly_volatility', 2.0)
        if vol > 5.0:
            vol_threshold_strong = 3.5
            vol_threshold_mid = 1.5
        elif vol > 4.0:
            vol_threshold_strong = 3.0
            vol_threshold_mid = 1.2
        elif vol > 3.0:
            vol_threshold_strong = 2.5
            vol_threshold_mid = 1.0

    # ── 强信号区 ──
    if abs(d3_chg) > vol_threshold_strong:
        return d3_chg > 0, f'前3天{d3_chg:+.2f}%(强信号)', 'high'

    # ── 中等信号区 ──
    if abs(d3_chg) > vol_threshold_mid:
        pred = d3_chg > 0
        concept_strong_agree = ((pred and cs > 1.5 and board_momentum > 0.1) or
                                (not pred and cs < -1.5 and board_momentum < -0.1))
        if concept_strong_agree:
            return pred, f'前3天{d3_chg:+.2f}%(中等+强概念)', 'high'
        return pred, f'前3天{d3_chg:+.2f}%(中等信号)', 'medium'

    # ── 模糊区（|d3_chg| ≤ vol_threshold_mid）──
    # 跟随d3方向（d3>=0预测涨，d3<0预测跌）
    # 这是模糊区的最优策略(67.0%)，任何信号组合均无法超越
    return d3_chg >= 0, f'模糊区:前3天{d3_chg:+.2f}%', 'low'


# ═══════════════════════════════════════════════════════════
# 构建周数据记录 v3
# ═══════════════════════════════════════════════════════════

def _build_weekly_records_v3(stock_codes, data, start_date, end_date,
                              board_stock_map=None):
    """从日K线构建周数据记录（v3：支持板块-个股映射）。

    Args:
        board_stock_map: {board_code: [stock_code, ...]} 板块→个股映射
    """
    # 预计算大盘每周前3天涨跌
    market_klines = data.get('market_klines', [])
    market_bt = [k for k in market_klines if start_date <= k['date'] <= end_date]
    market_week_groups = defaultdict(list)
    for k in market_bt:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        iso_week = dt.isocalendar()[:2]
        market_week_groups[iso_week].append(k)

    market_d3_map = {}  # iso_week → market d3 compound change
    for iso_week, days in market_week_groups.items():
        days.sort(key=lambda x: x['date'])
        if len(days) >= 3:
            d3_pcts = [d['change_percent'] for d in days[:3]]
            market_d3_map[iso_week] = _compound_return(d3_pcts)

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
            sig = compute_concept_signal_v3(code, wed_date, data)

            weekly.append({
                'code': code,
                'iso_week': iso_week,
                'week_dates': [d['date'] for d in days],
                'n_days': len(days),
                'daily_changes': daily_pcts,
                'd3_chg': round(d3_chg, 4),
                'd3_daily': d3_pcts[:3],
                'weekly_change': round(weekly_chg, 4),
                'weekly_up': weekly_up,
                'wed_date': wed_date,
                'concept_signal': sig,
                'concept_boards': board_names,
                'market_d3_chg': market_d3_map.get(iso_week, 0.0),
            })

    return weekly


# ═══════════════════════════════════════════════════════════
# 个股自适应统计 v3
# ═══════════════════════════════════════════════════════════

def _compute_stock_stats_v3(weekly_records, exclude_week=None):
    """计算每只股票的周预测历史统计（v3增强版）。

    新增字段：
    - d3_direction_accuracy: 前3天方向预测全周方向的历史准确率
    """
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
        vol = _std(weekly_chgs)

        # 前3天方向预测全周方向的准确率
        d3_correct = 0
        d3_total = 0
        for w in weeks:
            if abs(w['d3_chg']) > 0.1:
                d3_total += 1
                if (w['d3_chg'] > 0) == w['weekly_up']:
                    d3_correct += 1
        d3_acc = d3_correct / d3_total if d3_total >= 3 else 0.5

        # 概念信号有效率
        concept_correct = 0
        concept_total = 0
        for w in weeks:
            sig = w['concept_signal']
            if sig and abs(sig['composite_score']) > 1.0:
                concept_total += 1
                if (sig['composite_score'] > 0) == w['weekly_up']:
                    concept_correct += 1
        concept_eff = (concept_correct / concept_total
                       if concept_total >= 3 else 0.5)

        # 均值回归有效率
        mr_correct = 0
        mr_total = 0
        for w in weeks:
            sig = w['concept_signal']
            if sig and abs(sig.get('mr_signal', 0)) > 0.3:
                mr_total += 1
                if (sig['mr_signal'] > 0) == w['weekly_up']:
                    mr_correct += 1
        mr_eff = mr_correct / mr_total if mr_total >= 3 else 0.5

        stats[code] = {
            'weekly_volatility': round(vol, 3),
            'd3_direction_accuracy': round(d3_acc, 3),
            'concept_effectiveness': round(concept_eff, 3),
            'mr_effectiveness': round(mr_eff, 3),
            'n_weeks': len(weeks),
        }

    return stats


# ═══════════════════════════════════════════════════════════
# 周预测评估 v3
# ═══════════════════════════════════════════════════════════

def _evaluate_predictions_v3(weekly, stock_stats, exclude_week=None):
    """评估周预测准确率（v3）。"""
    correct = 0
    total = 0
    conf_stats = {'high': [0, 0], 'medium': [0, 0], 'low': [0, 0]}
    fuzzy_correct = 0
    fuzzy_total = 0
    details = []

    for w in weekly:
        if exclude_week and w['iso_week'] == exclude_week:
            continue

        sig = w['concept_signal']
        ss = stock_stats.get(w['code']) if stock_stats else None
        pred_up, reason, conf = predict_weekly_direction_v3(
            w['d3_chg'], sig, ss, w.get('d3_daily'),
            w.get('market_d3_chg', 0.0))
        actual_up = w['weekly_up']
        is_correct = pred_up == actual_up

        total += 1
        if is_correct:
            correct += 1
        conf_stats[conf][1] += 1
        if is_correct:
            conf_stats[conf][0] += 1
        if abs(w['d3_chg']) <= 0.8:
            fuzzy_total += 1
            if is_correct:
                fuzzy_correct += 1

        details.append({
            'code': w['code'], 'iso_week': w['iso_week'],
            'd3_chg': w['d3_chg'], 'weekly_change': w['weekly_change'],
            'pred_up': pred_up, 'actual_up': actual_up,
            'correct': is_correct, 'reason': reason,
            'confidence': conf, 'concept_boards': w['concept_boards'],
        })

    accuracy = correct / total * 100 if total > 0 else 0
    return {
        'accuracy': round(accuracy, 1), 'correct': correct, 'total': total,
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
        'details': details,
    }


# ═══════════════════════════════════════════════════════════
# LOWO 交叉验证 v3
# ═══════════════════════════════════════════════════════════

def _run_lowo_cv_v3(weekly, all_weeks):
    """周预测 Leave-One-Week-Out 交叉验证（v3）。"""
    week_accuracies = []
    total_correct = 0
    total_count = 0

    for held_out_week in all_weeks:
        train_stats = _compute_stock_stats_v3(weekly, exclude_week=held_out_week)
        test_records = [w for w in weekly if w['iso_week'] == held_out_week]
        if not test_records:
            continue

        correct = 0
        for w in test_records:
            sig = w['concept_signal']
            ss = train_stats.get(w['code'])
            pred_up, _, _ = predict_weekly_direction_v3(
                w['d3_chg'], sig, ss, w.get('d3_daily'),
                w.get('market_d3_chg', 0.0))
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
# 按概念板块分析 v3
# ═══════════════════════════════════════════════════════════

def _analyze_by_concept_board_v3(records, stock_stats, board_stock_map=None):
    """按概念板块分组分析准确率（v3）。

    每只股票的预测结果会计入它所属的所有板块。

    Args:
        board_stock_map: {board_code: {'name': str, 'stocks': [code, ...]}}
    """
    # 建立 stock → [board_name, ...] 映射（一只股票可属于多个板块）
    stock_all_boards = defaultdict(list)
    if board_stock_map:
        for bc, info in board_stock_map.items():
            for sc in info.get('stocks', []):
                stock_all_boards[sc].append(info['name'])

    board_stats = defaultdict(lambda: {
        'correct': 0, 'total': 0, 'stocks': set()})

    for r in records:
        sig = r['concept_signal']
        ss = stock_stats.get(r['code']) if stock_stats else None
        pred_up, _, _ = predict_weekly_direction_v3(
            r['d3_chg'], sig, ss, r.get('d3_daily'),
            r.get('market_d3_chg', 0.0))
        actual_up = r['weekly_up']
        is_correct = pred_up == actual_up

        # 将结果计入该股票所属的所有板块
        boards_for_stock = stock_all_boards.get(r['code'], [])
        if not boards_for_stock and r.get('concept_boards'):
            boards_for_stock = [r['concept_boards'][0]]
        if not boards_for_stock:
            boards_for_stock = ['未分类']

        for board_name in boards_for_stock:
            board_stats[board_name]['total'] += 1
            board_stats[board_name]['stocks'].add(r['code'])
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
# 回测主函数 v3
# ═══════════════════════════════════════════════════════════

def run_v3_backtest(
    stock_codes: list[str],
    start_date: str = '2025-11-01',
    end_date: str = '2026-03-13',
    board_stock_map: dict = None,
) -> dict:
    """运行v3回测：60板块×20+个股 周预测（DB模式）。

    Args:
        stock_codes: 所有参与回测的股票代码列表（带后缀）
        start_date: 回测起始日期
        end_date: 回测结束日期
        board_stock_map: {board_code: {'name': str, 'stocks': [code, ...]}}
    """
    t_start = datetime.now()
    logger.info("=" * 70)
    logger.info("  概念板块增强 周预测 回测 v3")
    logger.info("  股票: %d只, 区间: %s ~ %s", len(stock_codes), start_date, end_date)
    logger.info("=" * 70)

    logger.info("[1/5] 预加载数据...")
    data = _preload_v3_data(stock_codes, start_date, end_date)

    logger.info("[2/5] 构建周数据...")
    weekly = _build_weekly_records_v3(stock_codes, data, start_date, end_date,
                                      board_stock_map)
    logger.info("  周样本: %d", len(weekly))

    if not weekly:
        return {'error': '无有效周数据', 'weekly_count': 0}

    all_weeks = sorted(set(w['iso_week'] for w in weekly))
    all_stocks = sorted(set(w['code'] for w in weekly))
    n_with_sig = sum(1 for w in weekly if w['concept_signal'] is not None)

    logger.info("[3/5] 周预测全样本评估...")
    stock_stats = _compute_stock_stats_v3(weekly)
    full_result = _evaluate_predictions_v3(weekly, stock_stats)

    logger.info("[4/5] 周预测LOWO交叉验证...")
    lowo_result = _run_lowo_cv_v3(weekly, all_weeks)

    logger.info("[5/5] 按板块分析...")
    board_analysis = _analyze_by_concept_board_v3(
        weekly, stock_stats, board_stock_map)

    elapsed = (datetime.now() - t_start).total_seconds()

    # 统计板块覆盖
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
