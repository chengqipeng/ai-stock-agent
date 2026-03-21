#!/usr/bin/env python3
"""
杯柄形态多因子交叉分析
========================
深度分析杯柄形态与哪些因子组合能进一步提高预测准确率。

分析维度：
1. 杯柄内部属性（U形/V形、有柄/无柄、突破/未突破、量缩/量增）
2. CAN SLIM 各维度分数交叉
3. 资金流向因子
4. 板块动量因子
5. 龙虎榜因子
6. 技术指标因子（均线、波动率、换手率）

输出：每个因子组合的胜率、平均收益、样本数，找出最优组合。
"""
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, '.')

from dao import get_connection
from service.can_slim_algo.can_slim_scorer import (
    score_stock, detect_cup_with_handle, _sf, _compound_return, _mean, _std,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# 数据加载（复用 backtest 的逻辑，增加龙虎榜和板块强弱数据）
# ═══════════════════════════════════════════════════════════

_INDEX_MAPPING = {
    "300": "399001.SZ", "301": "399001.SZ",
    "000": "399001.SZ", "001": "399001.SZ", "002": "399001.SZ", "003": "399001.SZ",
    "600": "000001.SH", "601": "000001.SH", "603": "000001.SH", "605": "000001.SH",
    "688": "000001.SH", "689": "000001.SH",
}


def _get_stock_index(stock_code: str) -> str:
    prefix3 = stock_code[:3]
    return _INDEX_MAPPING.get(prefix3, "399001.SZ" if stock_code.endswith(".SZ") else "000001.SH")


def _select_diversified_stocks(n: int = 200) -> list[str]:
    """从不同概念板块中均匀选取股票"""
    import random
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute("SELECT DISTINCT stock_code FROM stock_kline")
        valid_codes = {r['stock_code'] for r in cur.fetchall()
                       if not r['stock_code'].endswith('.BJ')}
        cur.execute(
            "SELECT board_code, stock_code FROM stock_concept_board_stock ORDER BY board_code")
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    board_stocks = defaultdict(list)
    for r in rows:
        raw = r['stock_code']
        if '.' not in raw:
            if raw.startswith(('0', '3')):
                full = f"{raw}.SZ"
            elif raw.startswith('6'):
                full = f"{raw}.SH"
            else:
                continue
        else:
            full = raw
        if full in valid_codes:
            board_stocks[r['board_code']].append(full)

    random.seed(42)
    for bc in board_stocks:
        random.shuffle(board_stocks[bc])

    sorted_boards = sorted(board_stocks.keys(), key=lambda bc: len(board_stocks[bc]), reverse=True)
    selected, selected_set = [], set()
    board_idx = {bc: 0 for bc in sorted_boards}

    for _ in range(50):
        if len(selected) >= n:
            break
        added = 0
        for bc in sorted_boards:
            if len(selected) >= n:
                break
            idx = board_idx[bc]
            while idx < len(board_stocks[bc]):
                code = board_stocks[bc][idx]
                idx += 1
                if code not in selected_set:
                    selected.append(code)
                    selected_set.add(code)
                    added += 1
                    break
            board_idx[bc] = idx
        if added == 0:
            break

    if len(selected) < n:
        remaining = [c for c in sorted(valid_codes) if c not in selected_set]
        random.shuffle(remaining)
        selected.extend(remaining[:n - len(selected)])
    return selected


def _load_extended_data(stock_codes: list[str], start_date: str, end_date: str) -> dict:
    """加载扩展数据集，包含龙虎榜、板块强弱、概念板块K线等。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    bs = 200

    # 1. 个股K线
    logger.info("[1/7] 加载个股K线...")
    stock_klines = defaultdict(list)
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, open_price, close_price, high_price, "
            f"low_price, trading_volume, trading_amount, change_percent, change_hand "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [start_date, end_date])
        for r in cur.fetchall():
            d = r['date'] if isinstance(r['date'], str) else str(r['date'])
            stock_klines[r['stock_code']].append({
                'date': d,
                'close_price': _sf(r['close_price']),
                'open_price': _sf(r['open_price']),
                'high_price': _sf(r['high_price']),
                'low_price': _sf(r['low_price']),
                'trading_volume': _sf(r['trading_volume']),
                'trading_amount': _sf(r['trading_amount']),
                'change_percent': _sf(r['change_percent']),
                'change_hand': _sf(r.get('change_hand', 0)),
            })

    # 2. 指数K线
    logger.info("[2/7] 加载指数K线...")
    idx_codes = list(set(_get_stock_index(c) for c in stock_codes))
    for idx in ('000001.SH', '399001.SZ'):
        if idx not in idx_codes:
            idx_codes.append(idx)
    ph = ','.join(['%s'] * len(idx_codes))
    cur.execute(
        f"SELECT stock_code, `date`, close_price, change_percent, trading_volume "
        f"FROM stock_kline WHERE stock_code IN ({ph}) "
        f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
        idx_codes + [start_date, end_date])
    market_klines = defaultdict(list)
    for r in cur.fetchall():
        d = r['date'] if isinstance(r['date'], str) else str(r['date'])
        market_klines[r['stock_code']].append({
            'date': d, 'close_price': _sf(r['close_price']),
            'change_percent': _sf(r['change_percent']),
            'trading_volume': _sf(r['trading_volume']),
        })

    # 3. 财报数据
    logger.info("[3/7] 加载财报数据...")
    finance_data = defaultdict(list)
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, report_date, data_json "
            f"FROM stock_finance WHERE stock_code IN ({ph}) ORDER BY report_date DESC", batch)
        for r in cur.fetchall():
            try:
                data = json.loads(r['data_json']) if isinstance(r['data_json'], str) else r['data_json']
                if isinstance(data, dict):
                    data['报告日期'] = r['report_date']
                    finance_data[r['stock_code']].append(data)
            except (json.JSONDecodeError, TypeError):
                pass

    # 4. 资金流向
    logger.info("[4/7] 加载资金流向...")
    fund_flow = defaultdict(list)
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, big_net, big_net_pct, main_net_5day, "
            f"net_flow, mid_net, small_net "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date` DESC",
            batch + [start_date, end_date])
        for r in cur.fetchall():
            d = r['date'] if isinstance(r['date'], str) else str(r['date'])
            fund_flow[r['stock_code']].append({
                'date': d, 'big_net': _sf(r['big_net']),
                'big_net_pct': _sf(r['big_net_pct']),
                'main_net_5day': _sf(r['main_net_5day']),
                'net_flow': _sf(r['net_flow']),
                'mid_net': _sf(r.get('mid_net', 0)),
                'small_net': _sf(r.get('small_net', 0)),
            })

    # 5. 龙虎榜
    logger.info("[5/7] 加载龙虎榜...")
    dragon_tiger = defaultdict(list)
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, trade_date, buy_amount, sell_amount "
            f"FROM stock_dragon_tiger WHERE stock_code IN ({ph}) "
            f"AND trade_date >= %s AND trade_date <= %s",
            batch + [start_date, end_date])
        for r in cur.fetchall():
            dragon_tiger[r['stock_code']].append({
                'date': r['trade_date'],
                'buy_amount': r.get('buy_amount', '0'),
                'sell_amount': r.get('sell_amount', '0'),
            })

    # 6. 板块强弱
    logger.info("[6/7] 加载板块强弱...")
    concept_strength = defaultdict(list)
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, board_name, strength_score, strength_level, "
            f"excess_5d, excess_20d, rank_in_board, board_total_stocks "
            f"FROM stock_concept_strength WHERE stock_code IN ({ph})", batch)
        for r in cur.fetchall():
            concept_strength[r['stock_code']].append({
                'board_name': r['board_name'],
                'strength_score': _sf(r['strength_score']),
                'strength_level': r['strength_level'],
                'excess_5d': _sf(r.get('excess_5d', 0)),
                'excess_20d': _sf(r.get('excess_20d', 0)),
                'rank_in_board': r.get('rank_in_board', 0),
                'board_total_stocks': r.get('board_total_stocks', 0),
            })

    # 7. 概念板块成分
    logger.info("[7/7] 加载概念板块成分...")
    stock_boards = defaultdict(list)
    codes_6 = [c.split('.')[0] for c in stock_codes]
    for i in range(0, len(codes_6), bs):
        batch = codes_6[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, board_name FROM stock_concept_board_stock "
            f"WHERE stock_code IN ({ph})", batch)
        for r in cur.fetchall():
            raw = r['stock_code']
            if raw.startswith(('0', '3')):
                full = f"{raw}.SZ"
            elif raw.startswith('6'):
                full = f"{raw}.SH"
            else:
                continue
            stock_boards[full].append(r['board_name'])

    conn.close()
    logger.info("  扩展数据加载完成")

    return {
        'stock_klines': stock_klines, 'market_klines': market_klines,
        'finance_data': finance_data, 'fund_flow': fund_flow,
        'dragon_tiger': dragon_tiger, 'concept_strength': concept_strength,
        'stock_boards': stock_boards,
    }


# ═══════════════════════════════════════════════════════════
# 技术指标计算
# ═══════════════════════════════════════════════════════════

def _calc_technical_factors(klines: list[dict], cutoff: str) -> dict:
    """从K线中提取技术因子（截至 cutoff 日期）。"""
    kl = [k for k in klines if k['date'] <= cutoff]
    if len(kl) < 60:
        return {}

    closes = [k['close_price'] for k in kl if k['close_price'] > 0]
    volumes = [k['trading_volume'] for k in kl if k['trading_volume'] > 0]
    hands = [k.get('change_hand', 0) for k in kl]
    pcts = [k['change_percent'] for k in kl]

    if len(closes) < 60:
        return {}

    latest = closes[-1]
    ma5 = _mean(closes[-5:])
    ma10 = _mean(closes[-10:])
    ma20 = _mean(closes[-20:])
    ma60 = _mean(closes[-60:])
    ma120 = _mean(closes[-120:]) if len(closes) >= 120 else _mean(closes)

    # 均线多头排列程度
    ma_bull_count = sum([
        1 if latest > ma5 else 0,
        1 if ma5 > ma10 else 0,
        1 if ma10 > ma20 else 0,
        1 if ma20 > ma60 else 0,
        1 if ma60 > ma120 else 0,
    ])

    # 波动率（20日）
    vol_20 = _std(pcts[-20:]) if len(pcts) >= 20 else 0

    # 换手率
    avg_hand_5 = _mean(hands[-5:]) if hands else 0
    avg_hand_20 = _mean(hands[-20:]) if hands else 0

    # 量比（5日均量 / 20日均量）
    vol_5 = _mean(volumes[-5:]) if len(volumes) >= 5 else 0
    vol_20_avg = _mean(volumes[-20:]) if len(volumes) >= 20 else 1
    vol_ratio = vol_5 / vol_20_avg if vol_20_avg > 0 else 1

    # 近期涨幅
    ret_5 = _compound_return(pcts[-5:]) if len(pcts) >= 5 else 0
    ret_10 = _compound_return(pcts[-10:]) if len(pcts) >= 10 else 0
    ret_20 = _compound_return(pcts[-20:]) if len(pcts) >= 20 else 0
    ret_60 = _compound_return(pcts[-60:]) if len(pcts) >= 60 else 0

    # 距离52周高点
    high_250 = max(closes[-250:]) if len(closes) >= 250 else max(closes)
    pct_from_high = (latest - high_250) / high_250 * 100 if high_250 > 0 else -100

    # RSI(14)
    gains, losses = [], []
    for i in range(-14, 0):
        if abs(i) < len(pcts):
            p = pcts[i]
            gains.append(max(0, p))
            losses.append(max(0, -p))
    avg_gain = _mean(gains) if gains else 0
    avg_loss = _mean(losses) if losses else 1
    rsi_14 = 100 - 100 / (1 + avg_gain / avg_loss) if avg_loss > 0 else 50

    return {
        'ma_bull_count': ma_bull_count,
        'vol_20': round(vol_20, 2),
        'avg_hand_5': round(avg_hand_5, 2),
        'avg_hand_20': round(avg_hand_20, 2),
        'vol_ratio': round(vol_ratio, 2),
        'ret_5': round(ret_5, 2),
        'ret_10': round(ret_10, 2),
        'ret_20': round(ret_20, 2),
        'ret_60': round(ret_60, 2),
        'pct_from_high': round(pct_from_high, 1),
        'rsi_14': round(rsi_14, 1),
    }


def _calc_fund_flow_factors(fund_flow: list[dict], cutoff: str) -> dict:
    """从资金流数据中提取因子。"""
    ff = [f for f in fund_flow if f['date'] <= cutoff]
    if len(ff) < 5:
        return {}

    recent_5 = ff[:5]
    recent_10 = ff[:10]
    recent_20 = ff[:20]

    big_net_5 = _mean([f['big_net'] for f in recent_5])
    big_net_10 = _mean([f['big_net'] for f in recent_10])
    big_net_20 = _mean([f['big_net'] for f in recent_20]) if len(recent_20) >= 10 else big_net_10

    # 主力资金趋势：近5日 vs 前5日
    if len(ff) >= 10:
        prev_5 = ff[5:10]
        big_net_prev = _mean([f['big_net'] for f in prev_5])
        fund_trend = big_net_5 - big_net_prev
    else:
        fund_trend = 0

    # 大单净占比
    big_pct_5 = _mean([f['big_net_pct'] for f in recent_5])

    # 5日主力净额
    main_5d = _mean([f['main_net_5day'] for f in recent_5])

    # 净流入天数占比
    inflow_days = sum(1 for f in recent_10 if f['net_flow'] > 0)
    inflow_ratio = inflow_days / len(recent_10) if recent_10 else 0

    return {
        'big_net_5': round(big_net_5, 1),
        'big_net_10': round(big_net_10, 1),
        'big_net_20': round(big_net_20, 1),
        'fund_trend': round(fund_trend, 1),
        'big_pct_5': round(big_pct_5, 2),
        'main_5d': round(main_5d, 1),
        'inflow_ratio': round(inflow_ratio, 2),
    }


def _calc_board_factors(concept_strength: list[dict], stock_boards: list[str]) -> dict:
    """板块因子。"""
    if not concept_strength:
        return {'avg_strength': 0, 'max_strength': 0, 'strong_board_count': 0, 'board_count': len(stock_boards)}

    scores = [s['strength_score'] for s in concept_strength]
    strong_count = sum(1 for s in concept_strength if s['strength_level'] == '强势')
    avg_excess_5d = _mean([s['excess_5d'] for s in concept_strength])
    avg_excess_20d = _mean([s['excess_20d'] for s in concept_strength])

    # 板块内排名百分位（越小越好）
    rank_pcts = []
    for s in concept_strength:
        if s['board_total_stocks'] > 0:
            rank_pcts.append(s['rank_in_board'] / s['board_total_stocks'])
    avg_rank_pct = _mean(rank_pcts) if rank_pcts else 0.5

    return {
        'avg_strength': round(_mean(scores), 1),
        'max_strength': round(max(scores), 1) if scores else 0,
        'strong_board_count': strong_count,
        'board_count': len(stock_boards),
        'avg_excess_5d': round(avg_excess_5d, 2),
        'avg_excess_20d': round(avg_excess_20d, 2),
        'avg_rank_pct': round(avg_rank_pct, 3),
    }


def _has_dragon_tiger(dragon_tiger: list[dict], cutoff: str, lookback_days: int = 30) -> dict:
    """检查近期是否有龙虎榜记录。"""
    cutoff_dt = datetime.strptime(cutoff[:10], '%Y-%m-%d')
    start_dt = cutoff_dt - timedelta(days=lookback_days)
    start_str = start_dt.strftime('%Y-%m-%d')

    recent = [d for d in dragon_tiger if start_str <= d['date'] <= cutoff]
    return {
        'has_dragon_tiger': len(recent) > 0,
        'dragon_tiger_count': len(recent),
    }


# ═══════════════════════════════════════════════════════════
# 核心分析逻辑
# ═══════════════════════════════════════════════════════════

def _next_month(year: int, month: int):
    if month == 12:
        return year + 1, 1
    return year, month + 1


def _get_month_return(klines: list[dict], year: int, month: int):
    month_kl = [k for k in klines if k['date'].startswith(f"{year}-{month:02d}")]
    if not month_kl:
        return None
    pcts = [_sf(k.get('change_percent', 0)) for k in month_kl]
    return _compound_return(pcts)


def run_factor_analysis(n_stocks: int = 200, n_months: int = 12) -> dict:
    """
    执行杯柄形态多因子交叉分析。

    对每笔交易记录完整的因子快照，然后按不同因子切片统计胜率和收益。
    """
    t_start = time.time()
    logger.info("=" * 70)
    logger.info("  杯柄形态多因子交叉分析")
    logger.info("  股票池: %d只, 回测: %d个月", n_stocks, n_months)
    logger.info("=" * 70)

    # 选股
    stock_codes = _select_diversified_stocks(n_stocks)

    # 获取最新交易日
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("SELECT MAX(`date`) as max_date FROM stock_kline WHERE stock_code IN ('000001.SH','399001.SZ')")
    latest_date = cur.fetchone()['max_date']
    cur.close()
    conn.close()

    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(n_months + 3) * 31 + 400)
    start_date = dt_start.strftime('%Y-%m-%d')

    # 加载扩展数据
    data = _load_extended_data(stock_codes, start_date, latest_date)

    # 确定回测月份
    bt_months = []
    y, m = dt_end.year, dt_end.month
    for _ in range(n_months + 2):
        bt_months.append((y, m))
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    bt_months.reverse()
    score_months = bt_months[:n_months]

    # ── 逐月收集交易记录 + 因子快照 ──
    all_trades = []

    for idx, (sy, sm) in enumerate(score_months):
        month_label = f"{sy}-{sm:02d}"
        # 计算月末日期（避免无效日期如 02-31）
        import calendar
        last_day = calendar.monthrange(sy, sm)[1]
        cutoff = f"{sy}-{sm:02d}-{last_day:02d}"
        logger.info("  [%d/%d] %s ...", idx + 1, len(score_months), month_label)

        # 持有月收益
        hy, hm = _next_month(sy, sm)

        for code in stock_codes:
            klines = data['stock_klines'].get(code, [])
            kl_cut = [k for k in klines if k['date'] <= cutoff]
            if len(kl_cut) < 60:
                continue

            idx_code = _get_stock_index(code)
            mkt_kl = data['market_klines'].get(idx_code, [])
            mkt_cut = [k for k in mkt_kl if k['date'] <= cutoff]

            fin = data['finance_data'].get(code, [])
            fin_cut = [f for f in fin if f.get('报告日期', '9999') <= cutoff]

            ff = data['fund_flow'].get(code, [])
            ff_cut = [f for f in ff if f.get('date', '') <= cutoff]

            try:
                result = score_stock(code, kl_cut, mkt_cut, fin_cut, ff_cut)
            except Exception:
                continue

            if result['composite'] < 60:
                continue

            # 计算持有收益
            ret = _get_month_return(klines, hy, hm)
            if ret is None:
                continue

            # 收集因子快照
            cup_handle = result.get('cup_handle')
            tech = _calc_technical_factors(klines, cutoff)
            ff_factors = _calc_fund_flow_factors(ff, cutoff)
            board_factors = _calc_board_factors(
                data['concept_strength'].get(code, []),
                data['stock_boards'].get(code, []))
            dt_factors = _has_dragon_tiger(data['dragon_tiger'].get(code, []), cutoff)

            trade = {
                'stock_code': code,
                'month': month_label,
                'composite': result['composite'],
                'dim_scores': result['dim_scores'],
                'return': round(ret, 2),
                'win': 1 if ret > 0 else 0,
                # 杯柄属性
                'has_cup_handle': bool(cup_handle),
                'ch_score': cup_handle.get('pattern_score', 0) if cup_handle else 0,
                'ch_breakout': cup_handle.get('breakout', False) if cup_handle else False,
                'ch_volume_confirm': cup_handle.get('volume_confirm', False) if cup_handle else False,
                'ch_cup_depth': cup_handle.get('cup_depth', 0) if cup_handle else 0,
                # 技术因子
                **{f'tech_{k}': v for k, v in tech.items()},
                # 资金流因子
                **{f'ff_{k}': v for k, v in ff_factors.items()},
                # 板块因子
                **{f'board_{k}': v for k, v in board_factors.items()},
                # 龙虎榜
                **dt_factors,
            }
            all_trades.append(trade)

    logger.info("  共收集 %d 笔交易记录", len(all_trades))

    # ═══════════════════════════════════════════════════════════
    # 多维度切片分析
    # ═══════════════════════════════════════════════════════════

    def _slice_stats(trades: list[dict], label: str = '') -> dict:
        if not trades:
            return {'label': label, 'n': 0, 'win_rate': 0, 'avg_return': 0, 'median_return': 0}
        wins = sum(t['win'] for t in trades)
        rets = [t['return'] for t in trades]
        return {
            'label': label,
            'n': len(trades),
            'win_rate': round(wins / len(trades) * 100, 1),
            'avg_return': round(_mean(rets), 2),
            'median_return': round(sorted(rets)[len(rets) // 2], 2),
            'std_return': round(_std(rets), 2) if len(rets) > 1 else 0,
        }

    analysis = {}

    # ── 1. 基线对比：杯柄 vs 非杯柄 ──
    ch_trades = [t for t in all_trades if t['has_cup_handle']]
    no_ch_trades = [t for t in all_trades if not t['has_cup_handle']]
    analysis['1_baseline'] = {
        'cup_handle': _slice_stats(ch_trades, '有杯柄'),
        'no_cup_handle': _slice_stats(no_ch_trades, '无杯柄'),
        'all': _slice_stats(all_trades, '全部'),
    }

    # ── 2. 杯柄内部属性切片 ──
    ch_breakout = [t for t in ch_trades if t['ch_breakout']]
    ch_no_breakout = [t for t in ch_trades if not t['ch_breakout']]
    ch_vol_confirm = [t for t in ch_trades if t['ch_volume_confirm']]
    ch_high_score = [t for t in ch_trades if t['ch_score'] >= 70]
    ch_mid_score = [t for t in ch_trades if 50 <= t['ch_score'] < 70]
    analysis['2_cup_handle_attributes'] = {
        'breakout': _slice_stats(ch_breakout, '杯柄+已突破'),
        'no_breakout': _slice_stats(ch_no_breakout, '杯柄+未突破'),
        'volume_confirm': _slice_stats(ch_vol_confirm, '杯柄+放量确认'),
        'high_score_70+': _slice_stats(ch_high_score, '杯柄分>=70'),
        'mid_score_50_70': _slice_stats(ch_mid_score, '杯柄分50-70'),
    }

    # ── 3. 杯柄 × CAN SLIM 各维度交叉 ──
    dim_cross = {}
    for dim in ['C', 'A', 'N', 'S', 'L', 'I', 'M']:
        high = [t for t in ch_trades if t['dim_scores'].get(dim, 0) >= 60]
        low = [t for t in ch_trades if t['dim_scores'].get(dim, 0) < 60]
        no_ch_high = [t for t in no_ch_trades if t['dim_scores'].get(dim, 0) >= 60]
        dim_cross[dim] = {
            f'杯柄+{dim}>=60': _slice_stats(high, f'杯柄+{dim}>=60'),
            f'杯柄+{dim}<60': _slice_stats(low, f'杯柄+{dim}<60'),
            f'无杯柄+{dim}>=60': _slice_stats(no_ch_high, f'无杯柄+{dim}>=60'),
        }
    analysis['3_dim_cross'] = dim_cross

    # ── 4. 杯柄 × 技术因子交叉 ──
    tech_cross = {}

    # 均线多头
    for threshold in [3, 4, 5]:
        key = f'ma_bull>={threshold}'
        trades_slice = [t for t in ch_trades if t.get('tech_ma_bull_count', 0) >= threshold]
        no_ch_slice = [t for t in no_ch_trades if t.get('tech_ma_bull_count', 0) >= threshold]
        tech_cross[key] = {
            'cup_handle': _slice_stats(trades_slice, f'杯柄+均线多头>={threshold}'),
            'no_cup_handle': _slice_stats(no_ch_slice, f'无杯柄+均线多头>={threshold}'),
        }

    # RSI 区间
    for lo, hi, label in [(30, 50, 'RSI低位'), (50, 65, 'RSI中位'), (65, 80, 'RSI高位')]:
        trades_slice = [t for t in ch_trades if lo <= t.get('tech_rsi_14', 50) < hi]
        no_ch_slice = [t for t in no_ch_trades if lo <= t.get('tech_rsi_14', 50) < hi]
        tech_cross[label] = {
            'cup_handle': _slice_stats(trades_slice, f'杯柄+{label}'),
            'no_cup_handle': _slice_stats(no_ch_slice, f'无杯柄+{label}'),
        }

    # 波动率
    for threshold, label in [(2.0, '低波动<2'), (3.5, '中波动<3.5')]:
        trades_slice = [t for t in ch_trades if t.get('tech_vol_20', 99) < threshold]
        tech_cross[label] = {
            'cup_handle': _slice_stats(trades_slice, f'杯柄+{label}'),
        }

    # 量比
    for lo, hi, label in [(0.5, 1.0, '量比缩量'), (1.0, 1.5, '量比温和'), (1.5, 5.0, '量比放量')]:
        trades_slice = [t for t in ch_trades if lo <= t.get('tech_vol_ratio', 1) < hi]
        tech_cross[label] = {
            'cup_handle': _slice_stats(trades_slice, f'杯柄+{label}'),
        }

    # 换手率
    for lo, hi, label in [(0, 3, '低换手<3%'), (3, 8, '中换手3-8%'), (8, 100, '高换手>8%')]:
        trades_slice = [t for t in ch_trades if lo <= t.get('tech_avg_hand_5', 0) < hi]
        tech_cross[label] = {
            'cup_handle': _slice_stats(trades_slice, f'杯柄+{label}'),
        }

    analysis['4_tech_cross'] = tech_cross

    # ── 5. 杯柄 × 资金流因子交叉 ──
    fund_cross = {}

    # 主力资金方向
    fund_cross['主力净流入'] = {
        'cup_handle': _slice_stats([t for t in ch_trades if t.get('ff_big_net_5', 0) > 0], '杯柄+主力净流入'),
        'no_cup_handle': _slice_stats([t for t in no_ch_trades if t.get('ff_big_net_5', 0) > 0], '无杯柄+主力净流入'),
    }
    fund_cross['主力净流出'] = {
        'cup_handle': _slice_stats([t for t in ch_trades if t.get('ff_big_net_5', 0) <= 0], '杯柄+主力净流出'),
    }

    # 资金趋势改善
    fund_cross['资金趋势改善'] = {
        'cup_handle': _slice_stats([t for t in ch_trades if t.get('ff_fund_trend', 0) > 100], '杯柄+资金趋势改善'),
    }

    # 净流入天数占比
    fund_cross['净流入天数>60%'] = {
        'cup_handle': _slice_stats([t for t in ch_trades if t.get('ff_inflow_ratio', 0) > 0.6], '杯柄+净流入>60%'),
        'no_cup_handle': _slice_stats([t for t in no_ch_trades if t.get('ff_inflow_ratio', 0) > 0.6], '无杯柄+净流入>60%'),
    }

    analysis['5_fund_flow_cross'] = fund_cross

    # ── 6. 杯柄 × 板块因子交叉 ──
    board_cross = {}

    board_cross['强势板块多'] = {
        'cup_handle': _slice_stats([t for t in ch_trades if t.get('board_strong_board_count', 0) >= 3], '杯柄+强势板块>=3'),
        'no_cup_handle': _slice_stats([t for t in no_ch_trades if t.get('board_strong_board_count', 0) >= 3], '无杯柄+强势板块>=3'),
    }
    board_cross['板块强度高'] = {
        'cup_handle': _slice_stats([t for t in ch_trades if t.get('board_avg_strength', 0) >= 60], '杯柄+板块强度>=60'),
    }
    board_cross['板块排名靠前'] = {
        'cup_handle': _slice_stats([t for t in ch_trades if 0 < t.get('board_avg_rank_pct', 1) <= 0.3], '杯柄+板块排名前30%'),
    }

    analysis['6_board_cross'] = board_cross

    # ── 7. 杯柄 × 龙虎榜交叉 ──
    dt_cross = {}
    dt_cross['有龙虎榜'] = {
        'cup_handle': _slice_stats([t for t in ch_trades if t.get('has_dragon_tiger')], '杯柄+有龙虎榜'),
        'no_cup_handle': _slice_stats([t for t in no_ch_trades if t.get('has_dragon_tiger')], '无杯柄+有龙虎榜'),
    }
    dt_cross['无龙虎榜'] = {
        'cup_handle': _slice_stats([t for t in ch_trades if not t.get('has_dragon_tiger')], '杯柄+无龙虎榜'),
    }
    analysis['7_dragon_tiger_cross'] = dt_cross

    # ── 8. 多因子组合（找最优组合） ──
    combos = []

    # 定义因子条件
    factor_conditions = {
        '杯柄': lambda t: t['has_cup_handle'],
        '突破': lambda t: t['ch_breakout'],
        '放量确认': lambda t: t['ch_volume_confirm'],
        'C>=60': lambda t: t['dim_scores'].get('C', 0) >= 60,
        'A>=50': lambda t: t['dim_scores'].get('A', 0) >= 50,
        'S>=60': lambda t: t['dim_scores'].get('S', 0) >= 60,
        'L>=60': lambda t: t['dim_scores'].get('L', 0) >= 60,
        'I>=50': lambda t: t['dim_scores'].get('I', 0) >= 50,
        'M>=50': lambda t: t['dim_scores'].get('M', 0) >= 50,
        '均线多头>=4': lambda t: t.get('tech_ma_bull_count', 0) >= 4,
        '均线多头=5': lambda t: t.get('tech_ma_bull_count', 0) == 5,
        'RSI<65': lambda t: t.get('tech_rsi_14', 50) < 65,
        '主力净流入': lambda t: t.get('ff_big_net_5', 0) > 0,
        '资金趋势改善': lambda t: t.get('ff_fund_trend', 0) > 100,
        '低波动': lambda t: t.get('tech_vol_20', 99) < 3.0,
        '量比温和': lambda t: 0.8 <= t.get('tech_vol_ratio', 1) <= 1.8,
        '中低换手': lambda t: t.get('tech_avg_hand_5', 0) < 8,
        '强势板块>=2': lambda t: t.get('board_strong_board_count', 0) >= 2,
        '近20日涨>0': lambda t: t.get('tech_ret_20', 0) > 0,
        '综合分>=70': lambda t: t['composite'] >= 70,
    }

    # 测试所有 2-4 因子组合（杯柄 + 1~3 个其他因子）
    other_factors = [k for k in factor_conditions if k != '杯柄']

    from itertools import combinations
    for combo_size in range(1, 4):
        for combo in combinations(other_factors, combo_size):
            conditions = ['杯柄'] + list(combo)
            filtered = all_trades
            for cond_name in conditions:
                filtered = [t for t in filtered if factor_conditions[cond_name](t)]
            if len(filtered) >= 8:  # 至少 8 笔交易才有统计意义
                stats = _slice_stats(filtered, ' + '.join(conditions))
                combos.append(stats)

    # 按胜率排序
    combos.sort(key=lambda x: (x['win_rate'], x['avg_return']), reverse=True)
    analysis['8_best_combos_by_winrate'] = combos[:30]

    # 按平均收益排序
    combos_by_return = sorted(combos, key=lambda x: (x['avg_return'], x['win_rate']), reverse=True)
    analysis['9_best_combos_by_return'] = combos_by_return[:30]

    # 按夏普比排序（收益/风险）
    for c in combos:
        c['sharpe'] = round(c['avg_return'] / c['std_return'], 2) if c['std_return'] > 0 else 0
    combos_by_sharpe = sorted(combos, key=lambda x: x['sharpe'], reverse=True)
    analysis['10_best_combos_by_sharpe'] = combos_by_sharpe[:30]

    elapsed = time.time() - t_start
    logger.info("  分析完成, 耗时 %.1fs", elapsed)

    return {
        'total_trades': len(all_trades),
        'cup_handle_trades': len(ch_trades),
        'no_cup_handle_trades': len(no_ch_trades),
        'combo_count': len(combos),
        'analysis': analysis,
        'elapsed_seconds': round(elapsed, 1),
    }


# ═══════════════════════════════════════════════════════════
# CLI 入口
# ═══════════════════════════════════════════════════════════

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    )

    import argparse
    parser = argparse.ArgumentParser(description='杯柄形态多因子交叉分析')
    parser.add_argument('--stocks', type=int, default=200, help='股票数量')
    parser.add_argument('--months', type=int, default=12, help='回测月数')
    parser.add_argument('--output', type=str, default='data_results/cup_handle_factor_analysis.json')
    args = parser.parse_args()

    result = run_factor_analysis(n_stocks=args.stocks, n_months=args.months)

    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    logger.info("结果已保存到: %s", args.output)

    # 打印关键发现
    a = result['analysis']
    print("\n" + "=" * 70)
    print("  杯柄形态多因子交叉分析 — 关键发现")
    print("=" * 70)

    print(f"\n总交易: {result['total_trades']}笔, "
          f"杯柄: {result['cup_handle_trades']}笔, "
          f"非杯柄: {result['no_cup_handle_trades']}笔")

    base = a['1_baseline']
    print(f"\n【基线对比】")
    print(f"  全部:   胜率={base['all']['win_rate']}%, 均收益={base['all']['avg_return']}%")
    print(f"  有杯柄: 胜率={base['cup_handle']['win_rate']}%, 均收益={base['cup_handle']['avg_return']}%")
    print(f"  无杯柄: 胜率={base['no_cup_handle']['win_rate']}%, 均收益={base['no_cup_handle']['avg_return']}%")

    print(f"\n【TOP 15 最优因子组合（按胜率）】")
    for i, c in enumerate(a['8_best_combos_by_winrate'][:15]):
        print(f"  {i+1:2d}. {c['label']:<50s} 胜率={c['win_rate']:5.1f}% 均收益={c['avg_return']:6.2f}% n={c['n']}")

    print(f"\n【TOP 15 最优因子组合（按收益）】")
    for i, c in enumerate(a['9_best_combos_by_return'][:15]):
        print(f"  {i+1:2d}. {c['label']:<50s} 胜率={c['win_rate']:5.1f}% 均收益={c['avg_return']:6.2f}% n={c['n']}")

    print(f"\n【TOP 15 最优因子组合（按夏普比）】")
    for i, c in enumerate(a['10_best_combos_by_sharpe'][:15]):
        print(f"  {i+1:2d}. {c['label']:<50s} 夏普={c['sharpe']:5.2f} 胜率={c['win_rate']:5.1f}% 均收益={c['avg_return']:6.2f}% n={c['n']}")
