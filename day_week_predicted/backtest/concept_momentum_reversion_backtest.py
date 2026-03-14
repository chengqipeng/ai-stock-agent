#!/usr/bin/env python3
"""
概念板块动量-回归双模型日预测回测引擎 v3

核心策略变化（完全不同于v1/v2的多因子堆叠）：
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. 双模型自适应切换：
   - 动量模型：趋势延续（近期涨→继续涨，近期跌→继续跌）
   - 回归模型：均值回归（涨多了→跌，跌多了→涨）
   - 根据个股近期"动量胜率"自动选择哪个模型

2. 概念板块信号仅做"确认/否决"：
   - 板块强势 + 个股强势 → 确认看涨
   - 板块弱势 + 个股弱势 → 确认看跌
   - 板块与个股矛盾 → 不修正，保持原判断

3. 极简因子（只保留最有效的5个）：
   - 短期动量/回归（核心）
   - RSI超买超卖
   - 连续涨跌天数
   - 成交量异动
   - 概念板块确认

4. 个股自适应：每只股票独立跟踪动量vs回归的历史胜率
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

数据源：全部从数据库获取
目标：日预测准确率（宽松）≥ 65%
"""

import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal

from dao import get_connection

logger = logging.getLogger(__name__)


def _to_float(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    return float(v)


def _mean(lst):
    return sum(lst) / len(lst) if lst else 0.0


def _std(lst):
    if len(lst) < 2:
        return 0.0
    m = _mean(lst)
    return math.sqrt(sum((x - m) ** 2 for x in lst) / len(lst))


def _rate_str(ok, n):
    return f'{ok}/{n} ({round(ok / n * 100, 1)}%)' if n > 0 else '无数据'


# ═══════════════════════════════════════════════════════════
# 数据预加载
# ═══════════════════════════════════════════════════════════

def _preload_all_data(stock_codes: list[str], start_date: str, end_date: str) -> dict:
    """一次性从DB预加载所有需要的数据。"""
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
        # 1. 个股K线
        stock_klines = {}
        if codes_6:
            all_q = list(set(codes_6 + stock_codes))
            ph = ','.join(['%s'] * len(all_q))
            cur.execute(
                f"SELECT stock_code, `date`, open_price, close_price, high_price, "
                f"low_price, trading_volume, change_percent "
                f"FROM stock_kline "
                f"WHERE stock_code IN ({ph}) AND `date` >= %s AND `date` <= %s "
                f"ORDER BY stock_code, `date` ASC",
                (*all_q, ext_start, end_date),
            )
            for r in cur.fetchall():
                full = full_map.get(r['stock_code'], r['stock_code'])
                if full not in stock_klines:
                    stock_klines[full] = []
                stock_klines[full].append({
                    'date': r['date'],
                    'open': _to_float(r['open_price']),
                    'close': _to_float(r['close_price']),
                    'high': _to_float(r['high_price']),
                    'low': _to_float(r['low_price']),
                    'vol': _to_float(r['trading_volume']),
                    'chg': _to_float(r['change_percent']),
                })

        # 2. 个股-概念板块映射
        stock_boards = defaultdict(list)
        all_board_codes = set()
        if codes_6:
            ph = ','.join(['%s'] * len(codes_6))
            cur.execute(
                f"SELECT stock_code, board_code, board_name "
                f"FROM stock_concept_board_stock "
                f"WHERE stock_code IN ({ph})",
                tuple(codes_6),
            )
            for r in cur.fetchall():
                full = full_map.get(r['stock_code'], r['stock_code'])
                stock_boards[full].append({
                    'board_code': r['board_code'],
                    'board_name': r['board_name'],
                })
                all_board_codes.add(r['board_code'])

        # 3. 概念板块K线
        board_kline_map = defaultdict(list)
        if all_board_codes:
            bc_list = list(all_board_codes)
            ph2 = ','.join(['%s'] * len(bc_list))
            cur.execute(
                f"SELECT board_code, `date`, change_percent, close_price "
                f"FROM concept_board_kline "
                f"WHERE board_code IN ({ph2}) AND `date` >= %s AND `date` <= %s "
                f"ORDER BY board_code, `date` ASC",
                (*bc_list, ext_start, end_date),
            )
            for r in cur.fetchall():
                board_kline_map[r['board_code']].append({
                    'date': r['date'],
                    'chg': _to_float(r['change_percent']),
                    'close': _to_float(r['close_price']),
                })

        # 4. 大盘K线
        cur.execute(
            "SELECT `date`, change_percent, close_price FROM stock_kline "
            "WHERE stock_code = '000001.SH' AND `date` >= %s AND `date` <= %s "
            "ORDER BY `date` ASC",
            (ext_start, end_date),
        )
        market_klines = [{'date': r['date'], 'chg': _to_float(r['change_percent']),
                          'close': _to_float(r['close_price'])} for r in cur.fetchall()]

        # 5. 个股板块内强弱评分
        strength_map = defaultdict(dict)
        if codes_6:
            ph4 = ','.join(['%s'] * len(codes_6))
            cur.execute(
                f"SELECT stock_code, board_code, strength_score, strength_level "
                f"FROM stock_concept_strength WHERE stock_code IN ({ph4})",
                tuple(codes_6),
            )
            for r in cur.fetchall():
                full = full_map.get(r['stock_code'], r['stock_code'])
                strength_map[full][r['board_code']] = {
                    'score': _to_float(r['strength_score']),
                    'level': r['strength_level'],
                }

    finally:
        cur.close()
        conn.close()

    logger.info("[预加载] %d只K线, %d只有板块, %d板块有K线, 大盘%d天, 强弱%d只",
                len(stock_klines), sum(1 for c in stock_codes if c in stock_boards),
                sum(1 for bc in all_board_codes if bc in board_kline_map),
                len(market_klines), sum(1 for c in stock_codes if c in strength_map))

    return {
        'stock_klines': dict(stock_klines),
        'stock_boards': dict(stock_boards),
        'board_kline_map': dict(board_kline_map),
        'market_klines': market_klines,
        'strength_map': dict(strength_map),
    }


# ═══════════════════════════════════════════════════════════
# 概念板块确认信号（极简版）
# ═══════════════════════════════════════════════════════════

def _concept_confirm(stock_code: str, score_date: str,
                     stock_klines: list[dict],
                     boards: list[dict],
                     board_kline_map: dict,
                     market_klines: list[dict],
                     strength_data: dict) -> dict:
    """概念板块确认信号。

    不产生独立方向信号，只返回：
    - board_bullish: 板块整体看涨确认度 (0~1)
    - stock_strong: 个股在板块中的强势度 (0~1)
    - confirm_up: 是否确认看涨
    - confirm_down: 是否确认看跌
    - neutral: 板块信号中性（不修正）
    """
    if not boards:
        return {'neutral': True, 'confirm_up': False, 'confirm_down': False,
                'board_bullish': 0.5, 'stock_strong': 0.5, 'n_boards': 0}

    mk_map = {k['date']: k['chg'] for k in market_klines if k['date'] <= score_date}

    board_up_count = 0
    board_total = 0
    board_excess_list = []
    stock_strength_scores = []

    for b in boards:
        bc = b['board_code']
        bk = board_kline_map.get(bc, [])
        bk_recent = [k for k in bk if k['date'] <= score_date]
        if len(bk_recent) < 5:
            continue

        board_total += 1

        # 板块近5日涨跌
        last5 = bk_recent[-5:]
        board_5d_ret = sum(k['chg'] for k in last5)
        market_5d_ret = sum(mk_map.get(k['date'], 0) for k in last5)
        excess = board_5d_ret - market_5d_ret
        board_excess_list.append(excess)

        if board_5d_ret > 0:
            board_up_count += 1

        # 个股在板块中的强弱
        sd = strength_data.get(bc)
        if sd:
            stock_strength_scores.append(sd['score'] / 100.0)

    if board_total == 0:
        return {'neutral': True, 'confirm_up': False, 'confirm_down': False,
                'board_bullish': 0.5, 'stock_strong': 0.5, 'n_boards': 0}

    board_bullish = board_up_count / board_total
    avg_excess = _mean(board_excess_list) if board_excess_list else 0.0
    stock_strong = _mean(stock_strength_scores) if stock_strength_scores else 0.5

    # 确认逻辑：只在信号足够强时才确认
    confirm_up = (board_bullish >= 0.65 and avg_excess > 0.5) or \
                 (board_bullish >= 0.55 and stock_strong > 0.65 and avg_excess > 0)
    confirm_down = (board_bullish <= 0.35 and avg_excess < -0.5) or \
                   (board_bullish <= 0.45 and stock_strong < 0.35 and avg_excess < 0)
    neutral = not confirm_up and not confirm_down

    return {
        'board_bullish': round(board_bullish, 3),
        'stock_strong': round(stock_strong, 3),
        'avg_excess': round(avg_excess, 3),
        'confirm_up': confirm_up,
        'confirm_down': confirm_down,
        'neutral': neutral,
        'n_boards': board_total,
    }


# ═══════════════════════════════════════════════════════════
# 个股自适应模型选择器
# ═══════════════════════════════════════════════════════════

class ModeSelector:
    """跟踪个股的动量vs回归模型历史胜率，自动选择。"""

    def __init__(self, window=25):
        self.window = window
        self.momentum_results = []  # True/False
        self.reversion_results = []

    def record(self, momentum_correct: bool, reversion_correct: bool):
        self.momentum_results.append(momentum_correct)
        self.reversion_results.append(reversion_correct)
        if len(self.momentum_results) > self.window:
            self.momentum_results = self.momentum_results[-self.window:]
            self.reversion_results = self.reversion_results[-self.window:]

    @property
    def momentum_rate(self):
        if len(self.momentum_results) < 5:
            return 0.5
        return sum(self.momentum_results) / len(self.momentum_results)

    @property
    def reversion_rate(self):
        if len(self.reversion_results) < 5:
            return 0.5
        return sum(self.reversion_results) / len(self.reversion_results)

    @property
    def best_mode(self):
        """返回当前最优模式和混合权重。"""
        mr = self.momentum_rate
        rr = self.reversion_rate
        # 如果两者差距不大，混合使用
        if abs(mr - rr) < 0.08:
            return 'blend', 0.5, 0.5
        elif mr > rr:
            w = min(0.8, 0.5 + (mr - rr) * 2)
            return 'momentum', w, 1 - w
        else:
            w = min(0.8, 0.5 + (rr - mr) * 2)
            return 'reversion', 1 - w, w


# ═══════════════════════════════════════════════════════════
# 核心预测算法
# ═══════════════════════════════════════════════════════════

def _predict_one_day(klines: list[dict], idx: int,
                     market_klines: list[dict],
                     concept_confirm: dict,
                     mode_selector: ModeSelector,
                     score_date: str) -> dict:
    """对单只股票单日做预测。

    返回预测方向、动量模型方向、回归模型方向。
    """
    if idx < 20:
        return None

    c = klines[idx]['close']
    c1 = klines[idx - 1]['close']
    if c <= 0 or c1 <= 0:
        return None

    chg_today = (c - c1) / c1 * 100

    # ── 近期收益率序列 ──
    returns = []
    for j in range(min(20, idx)):
        cj = klines[idx - j]['close']
        cj1 = klines[idx - j - 1]['close']
        if cj1 > 0:
            returns.append((cj - cj1) / cj1 * 100)

    if len(returns) < 10:
        return None

    vol = max(0.5, _std(returns))
    z = chg_today / vol

    # ── 动量模型信号 ──
    # 近3日累计收益方向 → 延续
    ret_3d = sum(returns[:3])
    ret_5d = sum(returns[:5])
    ret_10d = sum(returns[:10])

    # 动量强度：短期趋势方向
    momentum_score = 0.0
    if ret_3d > 0.5:
        momentum_score += 1.5
    elif ret_3d > 0:
        momentum_score += 0.5
    elif ret_3d < -0.5:
        momentum_score -= 1.5
    elif ret_3d < 0:
        momentum_score -= 0.5

    if ret_5d > 1.0:
        momentum_score += 1.0
    elif ret_5d < -1.0:
        momentum_score -= 1.0

    # 趋势一致性加成
    up_days_5 = sum(1 for r in returns[:5] if r > 0.1)
    down_days_5 = sum(1 for r in returns[:5] if r < -0.1)
    if up_days_5 >= 4:
        momentum_score += 1.0
    elif down_days_5 >= 4:
        momentum_score -= 1.0

    momentum_dir = '上涨' if momentum_score > 0 else '下跌'

    # ── 回归模型信号 ──
    reversion_score = 0.0

    # z-score回归
    if z > 2.0:
        reversion_score -= 3.0
    elif z > 1.3:
        reversion_score -= 1.5
    elif z > 0.8:
        reversion_score -= 0.5
    elif z < -2.0:
        reversion_score += 3.0
    elif z < -1.3:
        reversion_score += 1.5
    elif z < -0.8:
        reversion_score += 0.5

    # 2日累计z-score
    if idx >= 2:
        c2 = klines[idx - 2]['close']
        if c2 > 0:
            chg_2d = (c - c2) / c2 * 100
            z2 = chg_2d / (vol * 1.41)
            if z2 > 1.5:
                reversion_score -= 1.5
            elif z2 < -1.5:
                reversion_score += 1.5

    # 5日累计z-score
    if idx >= 5:
        c5 = klines[idx - 5]['close']
        if c5 > 0:
            chg_5d = (c - c5) / c5 * 100
            z5 = chg_5d / (vol * 2.24)
            if z5 > 1.2:
                reversion_score -= 1.0
            elif z5 < -1.2:
                reversion_score += 1.0

    # RSI超买超卖
    gains = [max(r, 0) for r in returns[:14]]
    losses = [max(-r, 0) for r in returns[:14]]
    avg_gain = _mean(gains)
    avg_loss = max(_mean(losses), 0.001)
    rsi = 100 - (100 / (1 + avg_gain / avg_loss))
    if rsi > 75:
        reversion_score -= 1.5
    elif rsi > 65:
        reversion_score -= 0.5
    elif rsi < 25:
        reversion_score += 1.5
    elif rsi < 35:
        reversion_score += 0.5

    # 连续涨跌
    streak_up = streak_down = 0
    for j in range(min(10, idx)):
        ij = idx - j
        if ij <= 0:
            break
        if klines[ij]['close'] > klines[ij - 1]['close']:
            if streak_down > 0:
                break
            streak_up += 1
        elif klines[ij]['close'] < klines[ij - 1]['close']:
            if streak_up > 0:
                break
            streak_down += 1
        else:
            break

    if streak_up >= 4:
        reversion_score -= 2.0
    elif streak_up >= 3:
        reversion_score -= 1.0
    elif streak_down >= 4:
        reversion_score += 2.0
    elif streak_down >= 3:
        reversion_score += 1.0

    reversion_dir = '上涨' if reversion_score > 0 else '下跌'

    # ── 成交量异动 ──
    vol_today = klines[idx].get('vol', 0) or 0
    vols_5 = [klines[idx - j].get('vol', 0) or 0 for j in range(min(5, idx + 1))]
    avg_vol = _mean(vols_5) if vols_5 else 1
    vol_ratio = vol_today / avg_vol if avg_vol > 0 else 1.0

    # 放量上涨→动量延续信号增强；缩量上涨→回归信号增强
    vol_momentum_adj = 0.0
    vol_reversion_adj = 0.0
    if chg_today > 0.5 and vol_ratio > 1.5:
        vol_momentum_adj = 0.5  # 放量上涨，动量增强
    elif chg_today > 0.5 and vol_ratio < 0.7:
        vol_reversion_adj = 0.5  # 缩量上涨，回归增强
    elif chg_today < -0.5 and vol_ratio > 1.5:
        vol_momentum_adj = 0.5  # 放量下跌，动量增强
    elif chg_today < -0.5 and vol_ratio < 0.7:
        vol_reversion_adj = 0.5  # 缩量下跌，回归增强

    # ── 大盘环境 ──
    market_adj = 0.0
    mk_filtered = [k for k in market_klines if k['date'] <= score_date]
    if len(mk_filtered) >= 5:
        mkt_5d = sum(k['chg'] for k in mk_filtered[-5:])
        mkt_today = mk_filtered[-1]['chg']
        # 大盘大跌后反弹概率高
        if mkt_today < -1.5:
            market_adj = 0.5  # 偏向回归（反弹）
        elif mkt_today > 1.5:
            market_adj = -0.3  # 偏向回归（回调）
        # 大盘连续走势
        if mkt_5d > 3:
            market_adj -= 0.3  # 大盘涨多了
        elif mkt_5d < -3:
            market_adj += 0.3  # 大盘跌多了

    # ── 模型选择与融合 ──
    mode, mom_w, rev_w = mode_selector.best_mode

    # 应用成交量调整
    adj_momentum = momentum_score + vol_momentum_adj
    adj_reversion = reversion_score + vol_reversion_adj + market_adj

    # 融合信号
    combined = adj_momentum * mom_w + adj_reversion * rev_w

    # ── 概念板块确认/否决 ──
    concept_adj = 0.0
    if not concept_confirm.get('neutral', True):
        if concept_confirm['confirm_up']:
            if combined > 0:
                concept_adj = 0.8  # 确认看涨
            elif combined < -0.5:
                concept_adj = 0.3  # 轻微修正
        elif concept_confirm['confirm_down']:
            if combined < 0:
                concept_adj = -0.8  # 确认看跌
            elif combined > 0.5:
                concept_adj = -0.3  # 轻微修正

    final_signal = combined + concept_adj

    # ── 方向决策 ──
    # 动态阈值：波动大的股票需要更强信号
    threshold = max(0.1, min(0.4, vol / 5.0))

    if final_signal > threshold:
        direction = '上涨'
    elif final_signal < -threshold:
        direction = '下跌'
    else:
        # 模糊区：优先回归模型（统计上更稳定）
        if abs(reversion_score) > 1.0:
            direction = reversion_dir
        elif abs(momentum_score) > 1.0:
            direction = momentum_dir
        else:
            # 兜底：看概念板块
            if concept_confirm.get('confirm_up'):
                direction = '上涨'
            elif concept_confirm.get('confirm_down'):
                direction = '下跌'
            else:
                direction = '上涨'  # A股微涨偏向

    # ── 极端修正 ──
    # 连续4天以上涨跌，强制回归
    if streak_up >= 4 and direction == '上涨' and abs(final_signal) < 2.0:
        direction = '下跌'
    elif streak_down >= 4 and direction == '下跌' and abs(final_signal) < 2.0:
        direction = '上涨'

    # 日内大幅波动后回归
    if z > 2.5 and direction == '上涨':
        direction = '下跌'
    elif z < -2.5 and direction == '下跌':
        direction = '上涨'

    confidence = 'high' if abs(final_signal) > 2.0 else ('medium' if abs(final_signal) > 0.8 else 'low')

    return {
        'direction': direction,
        'final_signal': round(final_signal, 3),
        'momentum_score': round(adj_momentum, 3),
        'reversion_score': round(adj_reversion, 3),
        'momentum_dir': momentum_dir,
        'reversion_dir': reversion_dir,
        'mode': mode,
        'mom_w': round(mom_w, 2),
        'rev_w': round(rev_w, 2),
        'concept_adj': round(concept_adj, 3),
        'z': round(z, 2),
        'rsi': round(rsi, 1),
        'vol_ratio': round(vol_ratio, 2),
        'confidence': confidence,
        'streak_up': streak_up,
        'streak_down': streak_down,
    }


# ═══════════════════════════════════════════════════════════
# 主回测函数
# ═══════════════════════════════════════════════════════════

def run_momentum_reversion_backtest(
    stock_codes: list[str],
    start_date: str = '2025-12-10',
    end_date: str = '2026-03-10',
    min_kline_days: int = 80,
    preloaded_data: dict = None,
) -> dict:
    """概念板块动量-回归双模型日预测回测。"""
    t_start = datetime.now()
    logger.info("开始动量-回归双模型回测: %d只, %s ~ %s", len(stock_codes), start_date, end_date)

    if preloaded_data:
        data = preloaded_data
    else:
        data = _preload_all_data(stock_codes, start_date, end_date)

    stock_klines = data['stock_klines']
    stock_boards = data['stock_boards']
    board_kline_map = data['board_kline_map']
    market_klines = data['market_klines']
    strength_map = data['strength_map']

    all_results = []
    stock_summaries = []
    board_stats = defaultdict(lambda: {'ok': 0, 'n': 0, 'loose_ok': 0, 'stocks': set()})
    skipped = 0

    for code in stock_codes:
        klines = stock_klines.get(code, [])
        klines = [k for k in klines if (k.get('vol') or 0) > 0]
        if len(klines) < min_kline_days:
            skipped += 1
            continue

        start_idx = None
        for i, k in enumerate(klines):
            if k['date'] >= start_date:
                start_idx = i
                break
        if start_idx is None or start_idx < 40:
            skipped += 1
            continue

        boards = stock_boards.get(code, [])
        str_data = strength_map.get(code, {})
        selector = ModeSelector(window=25)

        day_results = []
        for i in range(start_idx, len(klines) - 1):
            sd = klines[i]['date']
            if sd > end_date:
                break

            # 概念板块确认
            cc = _concept_confirm(
                code, sd, klines[:i + 1], boards,
                board_kline_map, market_klines, str_data,
            )

            # 预测
            pred = _predict_one_day(klines, i, market_klines, cc, selector, sd)
            if pred is None:
                continue

            # T+1实际
            base = klines[i]['close']
            nxt = klines[i + 1]
            if base <= 0:
                continue

            actual_chg = round((nxt['close'] - base) / base * 100, 2)
            actual_up = actual_chg >= 0

            pred_up = pred['direction'] == '上涨'
            loose_ok = (pred_up and actual_up) or (not pred_up and not actual_up)

            if actual_chg > 0.3:
                actual_dir = '上涨'
            elif actual_chg < -0.3:
                actual_dir = '下跌'
            else:
                actual_dir = '横盘'
            strict_ok = pred['direction'] == actual_dir

            # 更新模型选择器
            mom_correct = (pred['momentum_dir'] == '上涨' and actual_up) or \
                          (pred['momentum_dir'] == '下跌' and not actual_up)
            rev_correct = (pred['reversion_dir'] == '上涨' and actual_up) or \
                          (pred['reversion_dir'] == '下跌' and not actual_up)
            selector.record(mom_correct, rev_correct)

            day_results.append({
                'code': code, 'date': sd, 'next_date': nxt['date'],
                'pred_dir': pred['direction'], 'actual_chg': actual_chg,
                'actual_dir': actual_dir, 'loose_ok': loose_ok, 'strict_ok': strict_ok,
                'pred': pred, 'concept': cc,
                'boards': [b['board_name'] for b in boards[:3]],
            })

            for b in boards[:5]:
                bn = b['board_name']
                board_stats[bn]['n'] += 1
                if loose_ok:
                    board_stats[bn]['loose_ok'] += 1
                if strict_ok:
                    board_stats[bn]['ok'] += 1
                board_stats[bn]['stocks'].add(code)

        all_results.extend(day_results)

        if day_results:
            nd = len(day_results)
            lok = sum(1 for r in day_results if r['loose_ok'])
            sok = sum(1 for r in day_results if r['strict_ok'])
            stock_summaries.append({
                '股票代码': code,
                '概念板块': ', '.join([b['board_name'] for b in boards[:3]]),
                '回测天数': nd,
                '准确率(宽松)': _rate_str(lok, nd),
                '准确率(严格)': _rate_str(sok, nd),
                '动量胜率': f'{round(selector.momentum_rate * 100, 1)}%',
                '回归胜率': f'{round(selector.reversion_rate * 100, 1)}%',
                '当前模式': selector.best_mode[0],
            })
            logger.info("%s [%s] %d天 宽松%.1f%% 模式:%s 动量%.1f%% 回归%.1f%%",
                        code, ', '.join([b['board_name'] for b in boards[:2]]),
                        nd, lok / nd * 100, selector.best_mode[0],
                        selector.momentum_rate * 100, selector.reversion_rate * 100)

    elapsed = (datetime.now() - t_start).total_seconds()

    if not all_results:
        return {'状态': '无有效数据', '耗时(秒)': round(elapsed, 1), '跳过股票数': skipped}

    return _build_summary(all_results, stock_summaries, board_stats,
                          stock_codes, elapsed, start_date, end_date, skipped)


def _build_summary(all_results, stock_summaries, board_stats,
                   stock_codes, elapsed, start_date, end_date, skipped) -> dict:
    total = len(all_results)
    total_loose = sum(1 for r in all_results if r['loose_ok'])
    total_strict = sum(1 for r in all_results if r['strict_ok'])

    # 按预测方向
    dir_stats = defaultdict(lambda: {'n': 0, 'loose': 0, 'strict': 0})
    for r in all_results:
        d = r['pred_dir']
        dir_stats[d]['n'] += 1
        if r['loose_ok']:
            dir_stats[d]['loose'] += 1
        if r['strict_ok']:
            dir_stats[d]['strict'] += 1

    dir_summary = {}
    for d in ['上涨', '下跌']:
        s = dir_stats.get(d, {'n': 0, 'loose': 0, 'strict': 0})
        dir_summary[d] = {'样本数': s['n'], '宽松': _rate_str(s['loose'], s['n']),
                          '严格': _rate_str(s['strict'], s['n'])}

    # 按置信度
    conf_stats = defaultdict(lambda: {'n': 0, 'loose': 0})
    for r in all_results:
        c = r['pred'].get('confidence', 'low')
        conf_stats[c]['n'] += 1
        if r['loose_ok']:
            conf_stats[c]['loose'] += 1
    conf_summary = {c: {'样本数': s['n'], '宽松': _rate_str(s['loose'], s['n'])}
                    for c, s in conf_stats.items()}

    # 按模式
    mode_stats = defaultdict(lambda: {'n': 0, 'loose': 0})
    for r in all_results:
        m = r['pred'].get('mode', 'blend')
        mode_stats[m]['n'] += 1
        if r['loose_ok']:
            mode_stats[m]['loose'] += 1
    mode_summary = {m: {'样本数': s['n'], '宽松': _rate_str(s['loose'], s['n'])}
                    for m, s in mode_stats.items()}

    # 概念确认效果
    with_confirm = [r for r in all_results if not r['concept'].get('neutral', True)]
    without_confirm = [r for r in all_results if r['concept'].get('neutral', True)]
    concept_effect = {
        '有概念确认': {'样本数': len(with_confirm),
                       '宽松': _rate_str(sum(1 for r in with_confirm if r['loose_ok']), len(with_confirm))},
        '无概念确认': {'样本数': len(without_confirm),
                       '宽松': _rate_str(sum(1 for r in without_confirm if r['loose_ok']), len(without_confirm))},
    }

    # 板块统计Top20
    sorted_boards = sorted(board_stats.items(), key=lambda x: x[1]['n'], reverse=True)[:20]
    board_summary = {}
    for bn, s in sorted_boards:
        board_summary[bn] = {'股票数': len(s['stocks']), '样本数': s['n'],
                             '宽松': _rate_str(s['loose_ok'], s['n']),
                             '严格': _rate_str(s['ok'], s['n'])}

    # 排序
    stock_summaries.sort(
        key=lambda x: float(x['准确率(宽松)'].split('(')[1].replace('%)', '')),
        reverse=True)

    above_65 = sum(1 for s in stock_summaries
                   if float(s['准确率(宽松)'].split('(')[1].replace('%)', '')) >= 65)
    above_60 = sum(1 for s in stock_summaries
                   if float(s['准确率(宽松)'].split('(')[1].replace('%)', '')) >= 60)

    return {
        '回测类型': '概念板块动量-回归双模型日预测 v3',
        '回测时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        '耗时(秒)': round(elapsed, 1),
        '回测区间': f'{start_date} ~ {end_date}',
        '评判模式': '宽松（预测上涨→实际≥0%正确，预测下跌→实际≤0%正确）',
        '股票数': len(stock_codes),
        '有效股票数': len(stock_codes) - skipped,
        '跳过股票数': skipped,
        '总样本数': total,
        '总体准确率(宽松)': _rate_str(total_loose, total),
        '总体准确率(严格)': _rate_str(total_strict, total),
        '达标统计': {'≥65%': above_65, '≥60%': above_60, '总有效': len(stock_summaries)},
        '按预测方向': dir_summary,
        '按置信度': conf_summary,
        '按模式': mode_summary,
        '概念确认效果': concept_effect,
        '按概念板块(Top20)': board_summary,
        '各股票汇总': stock_summaries,
    }
