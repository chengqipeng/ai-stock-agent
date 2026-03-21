#!/usr/bin/env python3
"""
CAN SLIM 量化回测引擎
=====================
基于 can_slim_scorer 的纯规则评分，对历史数据进行滚动回测。

回测逻辑：
1. 每月末对所有股票进行 CAN SLIM 综合评分
2. 选取综合分 >= 阈值的股票作为"买入"信号
3. 持有 N 个月后卖出，计算收益
4. 统计胜率、平均收益、最大回撤等指标

用法：
    python -m service.can_slim_algo.can_slim_backtest
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
    score_stock, compute_canslim_composite, _sf, _compound_return, _mean, _std,
    score_C, score_A, score_N, score_S, score_L, score_I, score_M,
)

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
# 回测参数
# ═══════════════════════════════════════════════════════════

BUY_THRESHOLD = 60          # 综合分 >= 此值视为买入信号
HOLD_MONTHS = 1             # 持有月数
LOOKBACK_DAYS = 400         # K线回溯天数（含计算指标所需历史）
BACKTEST_MONTHS = 12        # 回测月数
TOP_N = 50                  # 每月最多选股数量


# ═══════════════════════════════════════════════════════════
# 数据加载
# ═══════════════════════════════════════════════════════════

_INDEX_MAPPING = {
    "300": "399001.SZ", "301": "399001.SZ",
    "000": "399001.SZ", "001": "399001.SZ", "002": "399001.SZ", "003": "399001.SZ",
    "600": "000001.SH", "601": "000001.SH", "603": "000001.SH", "605": "000001.SH",
    "688": "000001.SH", "689": "000001.SH",
}


def _get_stock_index(stock_code: str) -> str:
    prefix3 = stock_code[:3]
    if prefix3 in _INDEX_MAPPING:
        return _INDEX_MAPPING[prefix3]
    if stock_code.endswith(".SZ"):
        return "399001.SZ"
    return "000001.SH"


def _get_all_stock_codes() -> list[str]:
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute("SELECT DISTINCT stock_code FROM stock_kline")
        codes = [r['stock_code'] for r in cur.fetchall()
                 if not r['stock_code'].endswith('.BJ')]
        return sorted(codes)
    finally:
        cur.close()
        conn.close()


def _select_diversified_stocks(n: int = 200) -> list[str]:
    """
    从不同概念板块中均匀选取 n 只股票，确保板块多样性。

    策略：
    1. 查询所有概念板块及其成分股
    2. 按板块轮询，每个板块选 1 只，直到凑满 n 只
    3. 同一只股票不重复选取
    4. 只选有 K 线数据的股票（排除北交所）
    """
    import random
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        # 获取所有有 K 线数据的股票代码（排除北交所）
        cur.execute("SELECT DISTINCT stock_code FROM stock_kline")
        valid_codes = {r['stock_code'] for r in cur.fetchall()
                       if not r['stock_code'].endswith('.BJ')}

        # 获取所有板块及其成分股（6位代码 → 需要转换为带后缀格式）
        cur.execute(
            "SELECT board_code, board_name, stock_code, stock_name "
            "FROM stock_concept_board_stock ORDER BY board_code")
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    if not rows:
        logger.warning("概念板块成分股表为空，回退到随机选取")
        codes = sorted(valid_codes)
        random.seed(42)
        random.shuffle(codes)
        return codes[:n]

    # 按板块分组，将 6 位代码转换为带后缀格式
    board_stocks = defaultdict(list)  # board_code -> [full_code, ...]
    for r in rows:
        raw = r['stock_code']
        # 转换为带后缀格式
        if '.' not in raw:
            if raw.startswith(('0', '3')):
                full = f"{raw}.SZ"
            elif raw.startswith('6'):
                full = f"{raw}.SH"
            else:
                continue  # 跳过北交所等
        else:
            full = raw
        if full in valid_codes:
            board_stocks[r['board_code']].append(full)

    # 每个板块内部随机打乱
    random.seed(42)
    for bc in board_stocks:
        random.shuffle(board_stocks[bc])

    # 按板块成分股数量降序排列（大板块优先，保证覆盖面）
    sorted_boards = sorted(board_stocks.keys(),
                           key=lambda bc: len(board_stocks[bc]), reverse=True)

    selected = []
    selected_set = set()
    board_idx = {bc: 0 for bc in sorted_boards}  # 每个板块的当前指针
    board_selected_count = defaultdict(int)  # 每个板块已选数量

    # 轮询选股
    rounds = 0
    max_rounds = 50  # 防止死循环
    while len(selected) < n and rounds < max_rounds:
        added_this_round = 0
        for bc in sorted_boards:
            if len(selected) >= n:
                break
            stocks = board_stocks[bc]
            idx = board_idx[bc]
            # 从该板块中找下一只未选过的股票
            while idx < len(stocks):
                code = stocks[idx]
                idx += 1
                if code not in selected_set:
                    selected.append(code)
                    selected_set.add(code)
                    board_selected_count[bc] += 1
                    added_this_round += 1
                    break
            board_idx[bc] = idx
        rounds += 1
        if added_this_round == 0:
            break  # 所有板块都已耗尽

    # 如果板块选股不足 n 只，从剩余有效股票中补充
    if len(selected) < n:
        remaining = [c for c in sorted(valid_codes) if c not in selected_set]
        random.shuffle(remaining)
        selected.extend(remaining[:n - len(selected)])

    logger.info("  多样化选股完成: %d只, 覆盖%d个板块, 轮询%d轮",
                len(selected), len([bc for bc in board_selected_count if board_selected_count[bc] > 0]),
                rounds)
    return selected


def _get_latest_trade_date() -> str:
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT MAX(`date`) as max_date FROM stock_kline "
            "WHERE stock_code IN ('000001.SH', '399001.SZ')")
        row = cur.fetchone()
        return row['max_date'] if row else None
    finally:
        cur.close()
        conn.close()


def _load_all_data(stock_codes: list[str], start_date: str, end_date: str):
    """批量加载所有需要的数据。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    bs = 200

    # 1. 个股K线
    logger.info("[1/4] 加载个股K线...")
    stock_klines = defaultdict(list)
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, open_price, close_price, high_price, "
            f"low_price, trading_volume, trading_amount, change_percent "
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
                'change_percent': _sf(r['change_percent']),
            })

    # 2. 指数K线
    logger.info("[2/4] 加载指数K线...")
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
            'date': d,
            'close_price': _sf(r['close_price']),
            'change_percent': _sf(r['change_percent']),
            'trading_volume': _sf(r['trading_volume']),
        })

    # 3. 财报数据
    logger.info("[3/4] 加载财报数据...")
    finance_data = defaultdict(list)
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, report_date, data_json "
            f"FROM stock_finance WHERE stock_code IN ({ph}) "
            f"ORDER BY report_date DESC",
            batch)
        for r in cur.fetchall():
            try:
                data = json.loads(r['data_json']) if isinstance(r['data_json'], str) else r['data_json']
                if isinstance(data, dict):
                    data['报告日期'] = r['report_date']
                    finance_data[r['stock_code']].append(data)
            except (json.JSONDecodeError, TypeError):
                pass

    # 4. 资金流向
    logger.info("[4/4] 加载资金流向...")
    fund_flow = defaultdict(list)
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, big_net, big_net_pct, main_net_5day, net_flow "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date` DESC",
            batch + [start_date, end_date])
        for r in cur.fetchall():
            d = r['date'] if isinstance(r['date'], str) else str(r['date'])
            fund_flow[r['stock_code']].append({
                'date': d,
                'big_net': _sf(r['big_net']),
                'big_net_pct': _sf(r['big_net_pct']),
                'main_net_5day': _sf(r['main_net_5day']),
                'net_flow': _sf(r['net_flow']),
            })

    conn.close()
    logger.info("  数据加载完成: K线=%d只, 指数=%d, 财报=%d只, 资金流=%d只",
                len(stock_klines), len(market_klines), len(finance_data), len(fund_flow))

    return {
        'stock_klines': stock_klines,
        'market_klines': market_klines,
        'finance_data': finance_data,
        'fund_flow': fund_flow,
    }


# ═══════════════════════════════════════════════════════════
# 按月分组工具
# ═══════════════════════════════════════════════════════════

def _group_klines_by_month(klines: list[dict]) -> dict:
    """将 K 线按 (year, month) 分组，返回 {(y,m): [klines]}"""
    groups = defaultdict(list)
    for k in klines:
        d = k['date']
        try:
            dt = datetime.strptime(d[:10], '%Y-%m-%d')
            groups[(dt.year, dt.month)].append(k)
        except ValueError:
            pass
    return groups


def _get_month_end_klines(klines: list[dict], year: int, month: int) -> list[dict]:
    """获取截至某月末的所有 K 线数据"""
    cutoff = f"{year}-{month:02d}-31"
    return [k for k in klines if k['date'] <= cutoff]


def _get_month_return(klines: list[dict], year: int, month: int):
    """计算某月的涨跌幅"""
    month_kl = [k for k in klines
                if k['date'].startswith(f"{year}-{month:02d}")]
    if not month_kl:
        return None
    pcts = [_sf(k.get('change_percent', 0)) for k in month_kl]
    return _compound_return(pcts)


def _next_month(year: int, month: int):
    if month == 12:
        return year + 1, 1
    return year, month + 1


# ═══════════════════════════════════════════════════════════
# 回测主逻辑
# ═══════════════════════════════════════════════════════════

def run_backtest(n_months: int = BACKTEST_MONTHS,
                 buy_threshold: float = BUY_THRESHOLD,
                 hold_months: int = HOLD_MONTHS,
                 top_n: int = TOP_N,
                 stock_codes: list[str] = None,
                 progress_callback=None) -> dict:
    """
    执行 CAN SLIM 回测。

    Args:
        stock_codes: 指定回测的股票列表，为 None 时使用全部股票。

    流程：
    1. 加载全量数据
    2. 对每个回测月份，截取该月末之前的数据进行评分
    3. 选取 top_n 只高分股票作为买入组合
    4. 计算持有 hold_months 后的收益
    5. 汇总统计

    Returns:
        回测结果字典，包含胜率、收益率、每月明细等。
    """
    t_start = time.time()
    logger.info("=" * 70)
    logger.info("  CAN SLIM 量化回测启动")
    logger.info("  参数: 回测%d月, 买入阈值=%d, 持有%d月, TOP%d",
                n_months, buy_threshold, hold_months, top_n)
    logger.info("=" * 70)

    # 获取基础信息
    all_codes = stock_codes if stock_codes else _get_all_stock_codes()
    latest_date = _get_latest_trade_date()
    if not all_codes or not latest_date:
        logger.error("无法获取股票数据或最新交易日")
        return {'error': '数据不可用'}

    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(n_months + 3) * 31 + LOOKBACK_DAYS)
    start_date = dt_start.strftime('%Y-%m-%d')

    logger.info("  股票数: %d, 最新交易日: %s, 数据起始: %s",
                len(all_codes), latest_date, start_date)

    # 加载数据
    data = _load_all_data(all_codes, start_date, latest_date)

    # 确定回测月份序列
    bt_months = []
    y, m = dt_end.year, dt_end.month
    for _ in range(n_months + hold_months + 1):
        bt_months.append((y, m))
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    bt_months.reverse()

    # 评分月份 = 前 n_months 个月（需要后续月份来计算收益）
    score_months = bt_months[:n_months]

    logger.info("  回测月份: %s ~ %s (%d个月)",
                f"{score_months[0][0]}-{score_months[0][1]:02d}",
                f"{score_months[-1][0]}-{score_months[-1][1]:02d}",
                len(score_months))

    # ── 逐月回测 ──
    monthly_results = []
    total_trades = 0
    total_wins = 0
    all_returns = []

    for idx, (sy, sm) in enumerate(score_months):
        month_label = f"{sy}-{sm:02d}"
        logger.info("  [%d/%d] 评分月: %s ...", idx + 1, len(score_months), month_label)

        # 对每只股票评分
        stock_scores = []
        for code in all_codes:
            klines = data['stock_klines'].get(code, [])
            # 截取到该月末的数据
            cutoff = f"{sy}-{sm:02d}-31"
            kl_cut = [k for k in klines if k['date'] <= cutoff]
            if len(kl_cut) < 60:
                continue

            idx_code = _get_stock_index(code)
            mkt_kl = data['market_klines'].get(idx_code, [])
            mkt_cut = [k for k in mkt_kl if k['date'] <= cutoff]

            fin = data['finance_data'].get(code, [])
            # 只用该月末之前的财报
            fin_cut = [f for f in fin if f.get('报告日期', '9999') <= cutoff]

            ff = data['fund_flow'].get(code, [])
            ff_cut = [f for f in ff if f.get('date', '') <= cutoff]

            try:
                result = score_stock(code, kl_cut, mkt_cut, fin_cut, ff_cut)
                entry = {
                    'stock_code': code,
                    'composite': result['composite'],
                    'grade': result['grade'],
                    'dim_scores': result['dim_scores'],
                }
                if result.get('cup_handle'):
                    entry['cup_handle'] = result['cup_handle']
                stock_scores.append(entry)
            except Exception as e:
                logger.debug("评分失败 %s: %s", code, e)
                continue

        # 筛选买入信号
        qualified = [s for s in stock_scores if s['composite'] >= buy_threshold]
        qualified.sort(key=lambda x: x['composite'], reverse=True)
        selected = qualified[:top_n]

        if not selected:
            logger.info("    %s: 无符合条件的股票", month_label)
            monthly_results.append({
                'month': month_label,
                'selected_count': 0,
                'avg_return': None,
                'win_rate': None,
            })
            continue

        # 计算持有收益
        hy, hm = sy, sm
        for _ in range(hold_months):
            hy, hm = _next_month(hy, hm)

        month_returns = []
        month_details = []
        cup_handle_trades = 0
        cup_handle_wins = 0
        for s in selected:
            code = s['stock_code']
            klines = data['stock_klines'].get(code, [])
            ret = _get_month_return(klines, hy, hm)
            if ret is not None:
                month_returns.append(ret)
                all_returns.append(ret)
                total_trades += 1
                has_ch = bool(s.get('cup_handle'))
                if ret > 0:
                    total_wins += 1
                if has_ch:
                    cup_handle_trades += 1
                    if ret > 0:
                        cup_handle_wins += 1
                detail_entry = {
                    'stock_code': code,
                    'score': s['composite'],
                    'return': round(ret, 2),
                }
                if has_ch:
                    detail_entry['cup_handle'] = s['cup_handle'].get('detail', '')
                    detail_entry['pivot_price'] = s['cup_handle'].get('pivot_price')
                    detail_entry['breakout'] = s['cup_handle'].get('breakout', False)
                month_details.append(detail_entry)

        avg_ret = _mean(month_returns) if month_returns else None
        win_rate = sum(1 for r in month_returns if r > 0) / len(month_returns) * 100 if month_returns else None

        # 杯柄形态统计
        ch_info = ''
        if cup_handle_trades > 0:
            ch_wr = round(cup_handle_wins / cup_handle_trades * 100, 1)
            ch_info = f', 杯柄={cup_handle_trades}只(胜率{ch_wr}%)'

        logger.info("    %s: 选股%d只, 有效%d只, 平均收益=%.2f%%, 胜率=%.1f%%%s",
                    month_label, len(selected), len(month_returns),
                    avg_ret or 0, win_rate or 0, ch_info)

        monthly_results.append({
            'month': month_label,
            'selected_count': len(selected),
            'valid_count': len(month_returns),
            'avg_return': round(avg_ret, 2) if avg_ret is not None else None,
            'win_rate': round(win_rate, 1) if win_rate is not None else None,
            'cup_handle_trades': cup_handle_trades,
            'cup_handle_wins': cup_handle_wins,
            'top5': month_details[:5],
        })

        if progress_callback:
            progress_callback(len(score_months), idx + 1)

    # ── 汇总统计 ──
    elapsed = time.time() - t_start
    overall_win_rate = round(total_wins / total_trades * 100, 1) if total_trades > 0 else 0
    overall_avg_return = round(_mean(all_returns), 2) if all_returns else 0
    overall_median = round(sorted(all_returns)[len(all_returns) // 2], 2) if all_returns else 0
    overall_std = round(_std(all_returns), 2) if all_returns else 0
    max_return = round(max(all_returns), 2) if all_returns else 0
    min_return = round(min(all_returns), 2) if all_returns else 0

    # 月度胜率
    monthly_win_rates = [m['win_rate'] for m in monthly_results if m['win_rate'] is not None]
    avg_monthly_win_rate = round(_mean(monthly_win_rates), 1) if monthly_win_rates else 0

    # 杯柄 vs 非杯柄对比统计
    total_ch_trades = sum(m.get('cup_handle_trades', 0) for m in monthly_results)
    total_ch_wins = sum(m.get('cup_handle_wins', 0) for m in monthly_results)
    total_non_ch_trades = total_trades - total_ch_trades
    total_non_ch_wins = total_wins - total_ch_wins
    ch_returns = []
    non_ch_returns = []
    for m in monthly_results:
        for d in m.get('top5', []):
            ret = d.get('return')
            if ret is not None:
                if d.get('cup_handle'):
                    ch_returns.append(ret)
                else:
                    non_ch_returns.append(ret)

    cup_handle_comparison = {
        'cup_handle_trades': total_ch_trades,
        'cup_handle_wins': total_ch_wins,
        'cup_handle_win_rate': round(total_ch_wins / total_ch_trades * 100, 1) if total_ch_trades > 0 else 0,
        'non_cup_handle_trades': total_non_ch_trades,
        'non_cup_handle_wins': total_non_ch_wins,
        'non_cup_handle_win_rate': round(total_non_ch_wins / total_non_ch_trades * 100, 1) if total_non_ch_trades > 0 else 0,
        'detection_rate': round(total_ch_trades / total_trades * 100, 1) if total_trades > 0 else 0,
    }

    result = {
        'params': {
            'buy_threshold': buy_threshold,
            'hold_months': hold_months,
            'top_n': top_n,
            'backtest_months': n_months,
            'stock_pool_size': len(all_codes),
            'stock_pool_mode': 'custom' if stock_codes else 'all',
        },
        'summary': {
            'total_trades': total_trades,
            'total_wins': total_wins,
            'overall_win_rate': overall_win_rate,
            'avg_return': overall_avg_return,
            'median_return': overall_median,
            'std_return': overall_std,
            'max_return': max_return,
            'min_return': min_return,
            'avg_monthly_win_rate': avg_monthly_win_rate,
            'sharpe_approx': round(overall_avg_return / overall_std, 2) if overall_std > 0 else 0,
        },
        'cup_handle_comparison': cup_handle_comparison,
        'monthly_results': monthly_results,
        'elapsed_seconds': round(elapsed, 1),
    }

    logger.info("=" * 70)
    logger.info("  CAN SLIM 回测完成")
    logger.info("  总交易: %d, 胜率: %.1f%%, 平均收益: %.2f%%",
                total_trades, overall_win_rate, overall_avg_return)
    logger.info("  中位收益: %.2f%%, 标准差: %.2f%%", overall_median, overall_std)
    logger.info("  最大收益: %.2f%%, 最大亏损: %.2f%%", max_return, min_return)
    logger.info("  月均胜率: %.1f%%", avg_monthly_win_rate)
    logger.info("  杯柄形态: %d/%d笔(检出率%.1f%%), 杯柄胜率=%.1f%%, 非杯柄胜率=%.1f%%",
                total_ch_trades, total_trades,
                cup_handle_comparison['detection_rate'],
                cup_handle_comparison['cup_handle_win_rate'],
                cup_handle_comparison['non_cup_handle_win_rate'])
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 70)

    return result


# ═══════════════════════════════════════════════════════════
# 当前评分（实盘用）
# ═══════════════════════════════════════════════════════════

def run_current_scoring(top_n: int = 100, buy_threshold: float = BUY_THRESHOLD,
                        progress_callback=None) -> dict:
    """
    对当前最新数据进行 CAN SLIM 评分，返回 top_n 只高分股票。
    可用于实盘选股。
    """
    t_start = time.time()
    logger.info("=" * 70)
    logger.info("  CAN SLIM 当前评分启动 (阈值=%d, TOP%d)", buy_threshold, top_n)
    logger.info("=" * 70)

    all_codes = _get_all_stock_codes()
    latest_date = _get_latest_trade_date()
    if not all_codes or not latest_date:
        return {'error': '数据不可用'}

    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=LOOKBACK_DAYS)
    start_date = dt_start.strftime('%Y-%m-%d')

    data = _load_all_data(all_codes, start_date, latest_date)

    stock_scores = []
    for i, code in enumerate(all_codes):
        klines = data['stock_klines'].get(code, [])
        if len(klines) < 60:
            continue

        idx_code = _get_stock_index(code)
        mkt_kl = data['market_klines'].get(idx_code, [])
        fin = data['finance_data'].get(code, [])
        ff = data['fund_flow'].get(code, [])

        try:
            result = score_stock(code, klines, mkt_kl, fin, ff)
            stock_scores.append({
                'stock_code': code,
                'composite': result['composite'],
                'grade': result['grade'],
                'dim_scores': result['dim_scores'],
                'dim_details': result.get('dim_details', {}),
            })
        except Exception as e:
            logger.debug("评分失败 %s: %s", code, e)

        if progress_callback and (i + 1) % 500 == 0:
            progress_callback(len(all_codes), i + 1)

    stock_scores.sort(key=lambda x: x['composite'], reverse=True)
    qualified = [s for s in stock_scores if s['composite'] >= buy_threshold]

    elapsed = time.time() - t_start
    logger.info("  评分完成: %d只股票, %d只达标, 耗时%.1fs",
                len(stock_scores), len(qualified), elapsed)

    return {
        'date': latest_date,
        'total_scored': len(stock_scores),
        'qualified_count': len(qualified),
        'top_stocks': qualified[:top_n],
        'score_distribution': {
            '>=75': sum(1 for s in stock_scores if s['composite'] >= 75),
            '60-75': sum(1 for s in stock_scores if 60 <= s['composite'] < 75),
            '45-60': sum(1 for s in stock_scores if 45 <= s['composite'] < 60),
            '<45': sum(1 for s in stock_scores if s['composite'] < 45),
        },
        'elapsed_seconds': round(elapsed, 1),
    }


def run_diversified_backtest(n_stocks: int = 200,
                             n_months: int = BACKTEST_MONTHS,
                             buy_threshold: float = BUY_THRESHOLD,
                             hold_months: int = HOLD_MONTHS,
                             top_n: int = TOP_N,
                             progress_callback=None) -> dict:
    """
    使用不同概念板块的股票进行 CAN SLIM 回测。

    从概念板块中均匀选取 n_stocks 只股票，确保板块多样性，
    然后执行标准回测流程。
    """
    logger.info("=" * 70)
    logger.info("  多样化板块回测: 从不同概念板块选取 %d 只股票", n_stocks)
    logger.info("=" * 70)

    diversified_codes = _select_diversified_stocks(n_stocks)
    if not diversified_codes:
        return {'error': '无法选取多样化股票'}

    result = run_backtest(
        n_months=n_months,
        buy_threshold=buy_threshold,
        hold_months=hold_months,
        top_n=top_n,
        stock_codes=diversified_codes,
        progress_callback=progress_callback,
    )

    # 附加板块多样性信息
    result['params']['stock_pool_mode'] = 'diversified_boards'
    result['params']['requested_stocks'] = n_stocks
    result['params']['actual_stocks'] = len(diversified_codes)

    # 查询选中股票的板块分布
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        codes_6 = [c.split('.')[0] for c in diversified_codes]
        ph = ','.join(['%s'] * len(codes_6))
        cur.execute(
            f"SELECT board_name, COUNT(DISTINCT stock_code) as cnt "
            f"FROM stock_concept_board_stock WHERE stock_code IN ({ph}) "
            f"GROUP BY board_name ORDER BY cnt DESC LIMIT 30",
            codes_6)
        board_dist = {r['board_name']: r['cnt'] for r in cur.fetchall()}
        result['board_distribution'] = board_dist
        result['params']['boards_covered'] = len(board_dist)
    except Exception as e:
        logger.warning("查询板块分布失败: %s", e)
    finally:
        cur.close()
        conn.close()

    return result


# ═══════════════════════════════════════════════════════════
# CLI 入口
# ═══════════════════════════════════════════════════════════

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    )

    import argparse
    parser = argparse.ArgumentParser(description='CAN SLIM 量化回测')
    parser.add_argument('--mode', choices=['backtest', 'score', 'diversified'], default='diversified',
                        help='运行模式: backtest=全量回测, score=当前评分, diversified=多样化板块回测')
    parser.add_argument('--months', type=int, default=BACKTEST_MONTHS,
                        help=f'回测月数 (默认{BACKTEST_MONTHS})')
    parser.add_argument('--threshold', type=float, default=BUY_THRESHOLD,
                        help=f'买入阈值 (默认{BUY_THRESHOLD})')
    parser.add_argument('--hold', type=int, default=HOLD_MONTHS,
                        help=f'持有月数 (默认{HOLD_MONTHS})')
    parser.add_argument('--top', type=int, default=TOP_N,
                        help=f'每月最多选股数 (默认{TOP_N})')
    parser.add_argument('--stocks', type=int, default=200,
                        help='多样化回测的股票数量 (默认200)')
    parser.add_argument('--output', type=str, default=None,
                        help='结果输出文件路径 (JSON)')
    args = parser.parse_args()

    if args.mode == 'diversified':
        result = run_diversified_backtest(
            n_stocks=args.stocks,
            n_months=args.months,
            buy_threshold=args.threshold,
            hold_months=args.hold,
            top_n=args.top,
        )
    elif args.mode == 'backtest':
        result = run_backtest(
            n_months=args.months,
            buy_threshold=args.threshold,
            hold_months=args.hold,
            top_n=args.top,
        )
    else:
        result = run_current_scoring(
            top_n=args.top,
            buy_threshold=args.threshold,
        )

    # 输出结果
    output_path = args.output or f'data_results/canslim_{args.mode}_result.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    logger.info("结果已保存到: %s", output_path)
