#!/usr/bin/env python3
"""
全量股票验证 — CAN SLIM + 杯柄形态 + 最优因子组合
===================================================
用全部 5000+ 只 A 股进行回测验证，对比：
  A. 纯 CAN SLIM（综合分 >= 阈值）
  B. CAN SLIM + 杯柄形态
  C. CAN SLIM + 杯柄 + 低波动
  D. CAN SLIM + 杯柄 + 低波动 + RSI<65
  E. CAN SLIM + 杯柄 + 低波动 + RSI<65 + 中低换手（最优组合）
  F. CAN SLIM + 杯柄 + 突破 + 低波动 + 中低换手
  G. 无杯柄对照组

采用分批加载策略：每批 BATCH_SIZE 只股票独立加载→评分→释放，
指数K线全局加载一次复用。
"""
import calendar
import gc
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, '.')

from dao import get_connection
from service.can_slim_algo.can_slim_scorer import (
    score_stock, _sf, _compound_return, _mean, _std,
)

logger = logging.getLogger(__name__)

LOOKBACK_DAYS = 400
BATCH_SIZE = 300  # 每批处理股票数

_INDEX_MAPPING = {
    "300": "399001.SZ", "301": "399001.SZ",
    "000": "399001.SZ", "001": "399001.SZ", "002": "399001.SZ", "003": "399001.SZ",
    "600": "000001.SH", "601": "000001.SH", "603": "000001.SH", "605": "000001.SH",
    "688": "000001.SH", "689": "000001.SH",
}


def _get_stock_index(code: str) -> str:
    return _INDEX_MAPPING.get(code[:3], "399001.SZ" if code.endswith(".SZ") else "000001.SH")


def _next_month(y, m):
    return (y + 1, 1) if m == 12 else (y, m + 1)


def _get_month_return(klines, year, month):
    prefix = f"{year}-{month:02d}"
    pcts = [_sf(k.get('change_percent', 0)) for k in klines if k['date'].startswith(prefix)]
    return _compound_return(pcts) if pcts else None


def _calc_quick_factors(klines: list[dict], cutoff: str) -> dict:
    """快速计算技术因子。"""
    kl = [k for k in klines if k['date'] <= cutoff]
    if len(kl) < 60:
        return {}
    closes = [k['close_price'] for k in kl if k['close_price'] > 0]
    pcts = [k['change_percent'] for k in kl]
    hands = [_sf(k.get('change_hand', 0)) for k in kl]
    volumes = [k['trading_volume'] for k in kl if k['trading_volume'] > 0]
    if len(closes) < 60:
        return {}

    latest = closes[-1]
    ma5, ma10 = _mean(closes[-5:]), _mean(closes[-10:])
    ma20, ma60 = _mean(closes[-20:]), _mean(closes[-60:])
    ma120 = _mean(closes[-120:]) if len(closes) >= 120 else _mean(closes)
    ma_bull = sum([latest > ma5, ma5 > ma10, ma10 > ma20, ma20 > ma60, ma60 > ma120])
    vol_20 = _std(pcts[-20:]) if len(pcts) >= 20 else 99
    avg_hand_5 = _mean(hands[-5:]) if hands else 0

    gains = [max(0, p) for p in pcts[-14:]]
    losses = [max(0, -p) for p in pcts[-14:]]
    avg_gain = _mean(gains) if gains else 0
    avg_loss = _mean(losses) if losses else 1
    rsi = 100 - 100 / (1 + avg_gain / avg_loss) if avg_loss > 0 else 50

    return {'ma_bull': ma_bull, 'vol_20': round(vol_20, 2),
            'avg_hand_5': round(avg_hand_5, 2), 'rsi': round(rsi, 1)}


def _load_market_klines(start_date: str, end_date: str) -> dict:
    """一次性加载指数K线（数据量小）。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    idx_codes = ['000001.SH', '399001.SZ']
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
    cur.close()
    conn.close()
    return market_klines


def _load_batch(codes: list[str], start_date: str, end_date: str) -> dict:
    """加载一批股票的K线+财报+资金流。分子批查询避免单次过大。"""
    stock_klines = defaultdict(list)
    finance_data = defaultdict(list)
    fund_flow = defaultdict(list)

    sub_bs = 200  # 子批大小
    for si in range(0, len(codes), sub_bs):
        sub_codes = codes[si:si + sub_bs]
        conn = get_connection(use_dict_cursor=True)
        cur = conn.cursor()
        ph = ','.join(['%s'] * len(sub_codes))

        cur.execute(
            f"SELECT stock_code, `date`, open_price, close_price, high_price, "
            f"low_price, trading_volume, change_percent, change_hand "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            sub_codes + [start_date, end_date])
        for r in cur.fetchall():
            d = r['date'] if isinstance(r['date'], str) else str(r['date'])
            stock_klines[r['stock_code']].append({
                'date': d, 'close_price': _sf(r['close_price']),
                'open_price': _sf(r['open_price']),
                'high_price': _sf(r['high_price']),
                'low_price': _sf(r['low_price']),
                'trading_volume': _sf(r['trading_volume']),
                'change_percent': _sf(r['change_percent']),
                'change_hand': _sf(r.get('change_hand', 0)),
            })

        cur.execute(
            f"SELECT stock_code, report_date, data_json "
            f"FROM stock_finance WHERE stock_code IN ({ph}) ORDER BY report_date DESC", sub_codes)
        for r in cur.fetchall():
            try:
                data = json.loads(r['data_json']) if isinstance(r['data_json'], str) else r['data_json']
                if isinstance(data, dict):
                    data['报告日期'] = r['report_date']
                    finance_data[r['stock_code']].append(data)
            except (json.JSONDecodeError, TypeError):
                pass

        cur.execute(
            f"SELECT stock_code, `date`, big_net, big_net_pct, main_net_5day, net_flow "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date` DESC",
            sub_codes + [start_date, end_date])
        for r in cur.fetchall():
            d = r['date'] if isinstance(r['date'], str) else str(r['date'])
            fund_flow[r['stock_code']].append({
                'date': d, 'big_net': _sf(r['big_net']),
                'big_net_pct': _sf(r['big_net_pct']),
                'main_net_5day': _sf(r['main_net_5day']),
                'net_flow': _sf(r['net_flow']),
            })

        cur.close()
        conn.close()

    return {'stock_klines': stock_klines, 'finance_data': finance_data, 'fund_flow': fund_flow}


def run_full_validation(n_months: int = 12, buy_threshold: float = 60,
                        top_n: int = 50) -> dict:
    """
    全量股票分批验证。

    核心优化：每批 BATCH_SIZE 只股票独立加载→逐月评分→释放内存。
    指数K线全局加载一次复用。
    """
    t_start = time.time()
    logger.info("=" * 70)
    logger.info("  全量股票验证 — CAN SLIM + 杯柄 + 多因子组合")
    logger.info("=" * 70)

    # 获取全部股票代码
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT stock_code FROM stock_kline WHERE stock_code NOT LIKE '%%.BJ'")
    all_codes = sorted([r['stock_code'] for r in cur.fetchall()])
    cur.execute("SELECT MAX(`date`) as d FROM stock_kline WHERE stock_code IN ('000001.SH','399001.SZ')")
    latest_date = cur.fetchone()['d']
    cur.close()
    conn.close()

    # 排除指数本身
    all_codes = [c for c in all_codes if c not in ('000001.SH', '399001.SZ', '399006.SZ')]

    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(n_months + 3) * 31 + LOOKBACK_DAYS)
    start_date = dt_start.strftime('%Y-%m-%d')

    logger.info("  全量股票: %d只, 最新交易日: %s", len(all_codes), latest_date)

    # 加载指数K线（全局复用）
    market_klines = _load_market_klines(start_date, latest_date)
    logger.info("  指数K线加载完成: %d条", sum(len(v) for v in market_klines.values()))

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

    logger.info("  回测: %s ~ %s (%d个月), 阈值=%d",
                f"{score_months[0][0]}-{score_months[0][1]:02d}",
                f"{score_months[-1][0]}-{score_months[-1][1]:02d}",
                n_months, buy_threshold)

    # 预计算月份 cutoff 和持有月
    month_info = []
    for sy, sm in score_months:
        last_day = calendar.monthrange(sy, sm)[1]
        cutoff = f"{sy}-{sm:02d}-{last_day:02d}"
        hy, hm = _next_month(sy, sm)
        month_info.append((sy, sm, cutoff, hy, hm))

    # ── 策略累积器（只存统计量，不存全部交易明细以节省内存） ──
    strat_keys = ['A_canslim_only', 'B_canslim_cup_handle', 'C_ch_low_vol',
                  'D_ch_low_vol_rsi', 'E_optimal', 'F_ch_breakout_low_vol', 'G_no_cup_handle']
    strat_descs = {
        'A_canslim_only': '纯CAN SLIM',
        'B_canslim_cup_handle': 'CAN SLIM+杯柄',
        'C_ch_low_vol': '杯柄+低波动',
        'D_ch_low_vol_rsi': '杯柄+低波动+RSI<65',
        'E_optimal': '杯柄+低波动+RSI<65+中低换手(最优)',
        'F_ch_breakout_low_vol': '杯柄+突破+低波动+中低换手',
        'G_no_cup_handle': '无杯柄(对照组)',
    }
    # 全局累积
    global_stats = {k: {'n': 0, 'wins': 0, 'returns': []} for k in strat_keys}
    # 月度累积
    monthly_stats = {i: {k: {'n': 0, 'wins': 0, 'returns': []} for k in strat_keys}
                     for i in range(len(score_months))}

    # ── 分批处理 ──
    n_batches = (len(all_codes) + BATCH_SIZE - 1) // BATCH_SIZE
    logger.info("  分 %d 批处理（每批 %d 只）", n_batches, BATCH_SIZE)

    for bi in range(n_batches):
        batch_codes = all_codes[bi * BATCH_SIZE: (bi + 1) * BATCH_SIZE]
        t_batch = time.time()

        # 加载本批数据
        batch_data = _load_batch(batch_codes, start_date, latest_date)

        # 逐月评分
        for mi, (sy, sm, cutoff, hy, hm) in enumerate(month_info):
            for code in batch_codes:
                klines = batch_data['stock_klines'].get(code, [])
                kl_cut = [k for k in klines if k['date'] <= cutoff]
                if len(kl_cut) < 60:
                    continue

                idx_code = _get_stock_index(code)
                mkt_cut = [k for k in market_klines.get(idx_code, []) if k['date'] <= cutoff]
                fin = batch_data['finance_data'].get(code, [])
                fin_cut = [f for f in fin if f.get('报告日期', '9999') <= cutoff]
                ff = batch_data['fund_flow'].get(code, [])
                ff_cut = [f for f in ff if f.get('date', '') <= cutoff]

                try:
                    result = score_stock(code, kl_cut, mkt_cut, fin_cut, ff_cut)
                except Exception:
                    continue

                if result['composite'] < buy_threshold:
                    continue

                ret = _get_month_return(klines, hy, hm)
                if ret is None:
                    continue

                win = 1 if ret > 0 else 0
                has_ch = bool(result.get('cup_handle'))
                ch = result.get('cup_handle', {})
                tech = _calc_quick_factors(klines, cutoff)
                low_vol = tech.get('vol_20', 99) < 3.0
                rsi_ok = tech.get('rsi', 50) < 65
                low_hand = tech.get('avg_hand_5', 0) < 8
                breakout = ch.get('breakout', False) if has_ch else False

                # A: 全部达标
                for target in [global_stats, monthly_stats[mi]]:
                    target['A_canslim_only']['n'] += 1
                    target['A_canslim_only']['wins'] += win
                    target['A_canslim_only']['returns'].append(ret)

                if has_ch:
                    for target in [global_stats, monthly_stats[mi]]:
                        target['B_canslim_cup_handle']['n'] += 1
                        target['B_canslim_cup_handle']['wins'] += win
                        target['B_canslim_cup_handle']['returns'].append(ret)
                    if low_vol:
                        for target in [global_stats, monthly_stats[mi]]:
                            target['C_ch_low_vol']['n'] += 1
                            target['C_ch_low_vol']['wins'] += win
                            target['C_ch_low_vol']['returns'].append(ret)
                        if rsi_ok:
                            for target in [global_stats, monthly_stats[mi]]:
                                target['D_ch_low_vol_rsi']['n'] += 1
                                target['D_ch_low_vol_rsi']['wins'] += win
                                target['D_ch_low_vol_rsi']['returns'].append(ret)
                            if low_hand:
                                for target in [global_stats, monthly_stats[mi]]:
                                    target['E_optimal']['n'] += 1
                                    target['E_optimal']['wins'] += win
                                    target['E_optimal']['returns'].append(ret)
                        if breakout and low_hand:
                            for target in [global_stats, monthly_stats[mi]]:
                                target['F_ch_breakout_low_vol']['n'] += 1
                                target['F_ch_breakout_low_vol']['wins'] += win
                                target['F_ch_breakout_low_vol']['returns'].append(ret)
                else:
                    for target in [global_stats, monthly_stats[mi]]:
                        target['G_no_cup_handle']['n'] += 1
                        target['G_no_cup_handle']['wins'] += win
                        target['G_no_cup_handle']['returns'].append(ret)

        # 释放本批数据
        del batch_data
        gc.collect()

        elapsed_b = time.time() - t_batch
        a_total = global_stats['A_canslim_only']['n']
        logger.info("  批次 %d/%d (%d只): 累计达标=%d, 耗时=%.1fs",
                    bi + 1, n_batches, len(batch_codes), a_total, elapsed_b)

    # ── 汇总结果 ──
    elapsed = time.time() - t_start

    def _summarize(stats):
        n = stats['n']
        if n == 0:
            return {'n': 0}
        rets = stats['returns']
        return {
            'n': n, 'wins': stats['wins'],
            'win_rate': round(stats['wins'] / n * 100, 1),
            'avg_return': round(_mean(rets), 2),
            'median_return': round(sorted(rets)[n // 2], 2),
            'std_return': round(_std(rets), 2) if n > 1 else 0,
            'max_return': round(max(rets), 2),
            'min_return': round(min(rets), 2),
            'sharpe': round(_mean(rets) / _std(rets), 2) if n > 1 and _std(rets) > 0 else 0,
        }

    summary = {}
    for k in strat_keys:
        s = _summarize(global_stats[k])
        s['desc'] = strat_descs[k]
        summary[k] = s

    # 月度明细
    monthly_results = []
    for mi, (sy, sm, *_) in enumerate(month_info):
        month_label = f"{sy}-{sm:02d}"
        mr = {'month': month_label}
        for k in strat_keys:
            ms = monthly_stats[mi][k]
            if ms['n'] > 0:
                mr[k] = {
                    'n': ms['n'],
                    'win_rate': round(ms['wins'] / ms['n'] * 100, 1),
                    'avg_return': round(_mean(ms['returns']), 2),
                }
        monthly_results.append(mr)

    result = {
        'params': {
            'stock_pool': len(all_codes),
            'backtest_months': n_months,
            'buy_threshold': buy_threshold,
            'latest_date': latest_date,
            'batch_size': BATCH_SIZE,
        },
        'strategy_comparison': summary,
        'monthly_results': monthly_results,
        'elapsed_seconds': round(elapsed, 1),
    }

    # 打印对比表
    logger.info("=" * 70)
    logger.info("  全量验证完成 — 策略对比（%d只股票, %d个月）", len(all_codes), n_months)
    logger.info("=" * 70)
    logger.info("  %-45s %6s %7s %8s %8s %6s", '策略', '交易数', '胜率', '均收益', '中位收益', '夏普')
    logger.info("  " + "-" * 82)
    for k in ['A_canslim_only', 'G_no_cup_handle', 'B_canslim_cup_handle',
              'C_ch_low_vol', 'D_ch_low_vol_rsi', 'E_optimal', 'F_ch_breakout_low_vol']:
        s = summary.get(k, {})
        if s.get('n', 0) > 0:
            logger.info("  %-45s %6d %6.1f%% %7.2f%% %7.2f%% %6.2f",
                        s['desc'], s['n'], s['win_rate'], s['avg_return'],
                        s['median_return'], s['sharpe'])
    logger.info("  耗时: %.1fs (%.1f分钟)", elapsed, elapsed / 60)
    logger.info("=" * 70)

    return result


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    )

    import argparse
    parser = argparse.ArgumentParser(description='全量股票验证')
    parser.add_argument('--months', type=int, default=12, help='回测月数')
    parser.add_argument('--threshold', type=float, default=60, help='买入阈值')
    parser.add_argument('--output', type=str, default='data_results/full_stock_validation_result.json')
    args = parser.parse_args()

    result = run_full_validation(n_months=args.months, buy_threshold=args.threshold)

    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    logger.info("结果已保存到: %s", args.output)
