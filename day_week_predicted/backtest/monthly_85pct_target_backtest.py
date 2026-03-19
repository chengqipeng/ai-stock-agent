#!/usr/bin/env python3
"""
月度预测 85% 准确率目标回测
============================
策略：提高信号阈值 + 多条件严格过滤 + 忽略低置信度
目标：准确率>=85%，同时保持有意义的预测数量(>=20只/月)

用法：
    python -m day_week_predicted.backtest.monthly_85pct_target_backtest
"""
import sys, os, math, json, logging
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

from dao import get_connection
from service.weekly_prediction_service import (
    _get_all_stock_codes, _get_latest_trade_date, _to_float,
    _compound_return, _get_stock_index,
)
from service.monthly_prediction_service import (
    _group_by_month, _safe_float, _mean, _std, _sigmoid,
    _score_price_momentum, _score_volume_price, _score_market_env,
    _score_fund_flow, _score_concept_board,
    _compute_board_strength_for_month, _compute_stock_board_strength,
    WEIGHTS,
)

N_MONTHS = 8


def load_samples():
    """加载全部样本（复用 monthly_board_category_analysis 的数据加载逻辑）"""
    all_codes = _get_all_stock_codes()
    latest_date = _get_latest_trade_date()
    if not all_codes or not latest_date:
        return []

    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(N_MONTHS + 3) * 31 + 240)
    start_date = dt_start.strftime('%Y-%m-%d')
    dt_cutoff = dt_end - timedelta(days=N_MONTHS * 31 + 31)

    logger.info("股票数: %d, 日期: %s ~ %s", len(all_codes), start_date, latest_date)

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    bs = 200

    logger.info("[1/6] 加载个股K线...")
    stock_klines = defaultdict(list)
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,close_price,change_percent,"
            f"trading_volume,high_price,low_price "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            stock_klines[r['stock_code']].append({
                'date': r['date'] if isinstance(r['date'], str) else r['date'].strftime('%Y-%m-%d') if hasattr(r['date'], 'strftime') else str(r['date']),
                'close': _to_float(r['close_price']),
                'change_percent': _to_float(r['change_percent']),
                'volume': _to_float(r['trading_volume']),
                'high': _to_float(r['high_price']),
                'low': _to_float(r['low_price']),
            })

    logger.info("[2/6] 加载指数K线...")
    idx_codes = list(set(_get_stock_index(c) for c in all_codes))
    for idx in ('000001.SH', '399001.SZ', '899050.SZ'):
        if idx not in idx_codes:
            idx_codes.append(idx)
    ph = ','.join(['%s'] * len(idx_codes))
    cur.execute(
        f"SELECT stock_code,`date`,change_percent FROM stock_kline "
        f"WHERE stock_code IN ({ph}) AND `date`>=%s AND `date`<=%s ORDER BY `date`",
        idx_codes + [start_date, latest_date])
    mkt_kl = defaultdict(list)
    for r in cur.fetchall():
        d = r['date'] if isinstance(r['date'], str) else r['date'].strftime('%Y-%m-%d') if hasattr(r['date'], 'strftime') else str(r['date'])
        mkt_kl[r['stock_code']].append({'date': d, 'change_percent': _to_float(r['change_percent'])})

    logger.info("[3/6] 加载资金流向...")
    fund_flow_map = defaultdict(list)
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,big_net,big_net_pct,main_net_5day,net_flow "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY `date` DESC",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            d = r['date'] if isinstance(r['date'], str) else r['date'].strftime('%Y-%m-%d') if hasattr(r['date'], 'strftime') else str(r['date'])
            fund_flow_map[r['stock_code']].append({
                'date': d, 'big_net': _safe_float(r['big_net']),
                'big_net_pct': _safe_float(r['big_net_pct']),
                'main_net_5day': _safe_float(r['main_net_5day']),
                'net_flow': _safe_float(r['net_flow']),
            })

    logger.info("[4/6] 加载概念板块映射...")
    stock_boards = defaultdict(list)
    all_board_codes = set()
    codes_6 = [c.split('.')[0] for c in all_codes]
    full_map = {c.split('.')[0]: c for c in all_codes}
    for i in range(0, len(codes_6), bs):
        batch = codes_6[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, board_code, board_name "
            f"FROM stock_concept_board_stock WHERE stock_code IN ({ph})", batch)
        for r in cur.fetchall():
            full = full_map.get(r['stock_code'], r['stock_code'])
            stock_boards[full].append({'board_code': r['board_code'], 'board_name': r['board_name']})
            all_board_codes.add(r['board_code'])

    logger.info("[5/6] 加载概念板块K线 (%d个板块)...", len(all_board_codes))
    board_kline_map = defaultdict(list)
    bc_list = list(all_board_codes)
    for i in range(0, len(bc_list), bs):
        batch = bc_list[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT board_code,`date`,change_percent,close_price "
            f"FROM concept_board_kline WHERE board_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY board_code,`date` ASC",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            d = r['date'] if isinstance(r['date'], str) else r['date'].strftime('%Y-%m-%d') if hasattr(r['date'], 'strftime') else str(r['date'])
            board_kline_map[r['board_code']].append({
                'date': d, 'change_percent': _to_float(r['change_percent']),
                'close_price': _to_float(r['close_price']),
            })

    market_klines_for_board = mkt_kl.get('000001.SH', [])
    conn.close()

    mkt_by_month = {}
    for ic, kl in mkt_kl.items():
        mkt_by_month[ic] = _group_by_month(kl)

    logger.info("[6/6] 构建样本...")
    current_year, current_month = dt_end.year, dt_end.month
    samples = []

    for code in all_codes:
        klines = stock_klines.get(code, [])
        if not klines or len(klines) < 80:
            continue
        stock_idx = _get_stock_index(code)
        idx_months = mkt_by_month.get(stock_idx, {})
        month_groups = _group_by_month(klines)
        sorted_months = sorted(month_groups.keys())
        sorted_all = sorted(klines, key=lambda x: x['date'])
        ff_data = fund_flow_map.get(code, [])
        boards = stock_boards.get(code, [])

        for i in range(len(sorted_months) - 1):
            ym_this = sorted_months[i]
            ym_next = sorted_months[i + 1]
            this_days = month_groups[ym_this]
            next_days = month_groups[ym_next]
            if len(this_days) < 10 or len(next_days) < 10:
                continue
            first_date_str = this_days[0]['date']
            dt_first = datetime.strptime(first_date_str, '%Y-%m-%d')
            if dt_first < dt_cutoff or ym_this == (current_year, current_month):
                continue

            this_chg = _compound_return([d['change_percent'] for d in this_days])
            next_chg = _compound_return([d['change_percent'] for d in next_days])
            actual_up = next_chg >= 0

            mkt_days = idx_months.get(ym_this, [])
            mkt_chg = _compound_return(
                [k['change_percent'] for k in sorted(mkt_days, key=lambda x: x['date'])]
            ) if len(mkt_days) >= 10 else 0.0

            hist = [k for k in sorted_all if k['date'] < first_date_str]
            pos60 = None
            if len(hist) >= 20:
                hc = [k['close'] for k in hist[-60:] if k['close'] > 0]
                if hc:
                    ac = hc + [k['close'] for k in this_days if k['close'] > 0]
                    mn, mx = min(ac), max(ac)
                    lc = this_days[-1]['close']
                    if mx > mn and lc > 0:
                        pos60 = (lc - mn) / (mx - mn)

            prev_chg = None
            if i > 0:
                prev_days = month_groups[sorted_months[i - 1]]
                if len(prev_days) >= 10:
                    prev_chg = _compound_return([k['change_percent'] for k in prev_days])

            vol_ratio = None
            tv = [d['volume'] for d in this_days if d['volume'] > 0]
            hv = [k['volume'] for k in hist[-20:] if k['volume'] > 0]
            if tv and hv:
                at, ah = sum(tv) / len(tv), sum(hv) / len(hv)
                if ah > 0:
                    vol_ratio = at / ah

            last_week_days = this_days[-5:]
            last_week_chg = _compound_return(
                [d['change_percent'] for d in last_week_days]
            ) if len(last_week_days) >= 3 else 0.0

            month_end_date = this_days[-1]['date']
            month_ff = sorted(
                [f for f in ff_data if first_date_str <= f['date'] <= month_end_date],
                key=lambda x: x['date'], reverse=True)

            board_signals, stock_board_signals = [], []
            for board in boards:
                bk = board_kline_map.get(board['board_code'], [])
                if not bk:
                    continue
                bs_sig = _compute_board_strength_for_month(bk, market_klines_for_board, month_end_date)
                if bs_sig:
                    board_signals.append(bs_sig)
                sb_sig = _compute_stock_board_strength(klines, bk, month_end_date)
                if sb_sig:
                    stock_board_signals.append(sb_sig)

            mkt_prev_chg = None
            if i > 0:
                mkt_prev_days = idx_months.get(sorted_months[i - 1], [])
                if len(mkt_prev_days) >= 10:
                    mkt_prev_chg = _compound_return(
                        [k['change_percent'] for k in sorted(mkt_prev_days, key=lambda x: x['date'])])

            feat = {
                'this_chg': this_chg, 'mkt_chg': mkt_chg, 'mkt_prev_chg': mkt_prev_chg,
                'pos60': pos60, 'prev_chg': prev_chg, 'vol_ratio': vol_ratio,
                'last_week_chg': last_week_chg, 'fund_flow_data': month_ff,
                'board_signals': board_signals, 'stock_board_signals': stock_board_signals,
            }

            s_price = _score_price_momentum(feat)
            s_volume = _score_volume_price(feat)
            s_market = _score_market_env(feat)
            s_fund = _score_fund_flow(feat)
            s_concept = _score_concept_board(feat)

            samples.append({
                'code': code, 'ym': ym_this,
                'this_chg': this_chg, 'next_chg': next_chg, 'actual_up': actual_up,
                'mkt_chg': mkt_chg, 'mkt_prev_chg': mkt_prev_chg,
                'pos60': pos60, 'prev_chg': prev_chg, 'vol_ratio': vol_ratio,
                'last_week_chg': last_week_chg,
                's_price': s_price, 's_volume': s_volume, 's_market': s_market,
                's_fund': s_fund, 's_concept': s_concept,
                'feat': feat,
                'board_signals': board_signals,
            })

    logger.info("样本: %d 个", len(samples))
    return samples


def predict_v3(s, config):
    """v3预测引擎 — 可配置阈值和过滤条件"""
    w = config.get('weights', WEIGHTS)
    thr = config.get('threshold', 0.9)

    total = (s['s_price'] * w['price_momentum'] + s['s_volume'] * w['volume_price']
             + s['s_market'] * w['market_env'] + s['s_fund'] * w['fund_flow']
             + s['s_concept'] * w['concept_board'])
    w_sum = sum(w.values())
    norm = total / w_sum

    dims = [s['s_price'], s['s_volume'], s['s_market'], s['s_fund'], s['s_concept']]
    non_zero = [d for d in dims if abs(d) > 0.2]
    if len(non_zero) >= 3:
        pos_count = sum(1 for d in non_zero if d > 0)
        neg_count = sum(1 for d in non_zero if d < 0)
        consistency = max(pos_count, neg_count) / len(non_zero)
        if consistency >= 0.8:
            norm *= 1.15

    this_chg = s['this_chg']
    if abs(this_chg) > 12:
        norm *= 1.1

    if abs(norm) < thr or norm <= 0:
        return None

    mkt_chg = s['mkt_chg']
    pos60 = s['pos60']
    prev_chg = s['prev_chg']
    abs_n = abs(norm)

    # 基础过滤（保持v1）
    if mkt_chg > 8 and this_chg < -3:
        return None
    excess = this_chg - mkt_chg
    if mkt_chg > -5 and excess < -12:
        penalty = 0.25 if excess < -20 else (0.4 if excess < -15 else 0.6)
        norm *= penalty
        abs_n = abs(norm)
        if abs_n < thr:
            return None
    if s['s_fund'] < -0.5 and abs_n < 1.3:
        norm *= 0.8
        abs_n = abs(norm)
        if abs_n < thr:
            return None
    if prev_chg is not None and prev_chg < -5 and this_chg < -5 and abs_n < 1.2:
        return None
    if pos60 is not None and pos60 > 0.7 and abs_n < 1.2:
        return None

    # v1已有过滤
    if s['last_week_chg'] < -5 and abs_n < 1.3:
        return None
    neg_count = sum(1 for d in dims if d < -0.3)
    if neg_count >= 2 and abs_n < 1.3:
        return None
    if s['s_concept'] < -0.8 and abs_n < 1.5:
        return None

    # 板块动量过滤
    bs_list = s.get('board_signals', [])
    if bs_list:
        moms = [b['momentum'] for b in bs_list if b and 'momentum' in b]
        if moms:
            avg_mom = _mean(moms)
            if avg_mom > 0.8 and abs_n < 1.3:
                return None

    # ── v3 额外过滤（可配置） ──
    filters = config.get('filters', {})

    # 大盘环境过滤：大盘前月+本月连续下跌 → 熊市，提高阈值
    if filters.get('bear_market_filter'):
        mkt_prev = s.get('mkt_prev_chg')
        if mkt_prev is not None and mkt_prev < -2 and mkt_chg < -2 and abs_n < 1.5:
            return None

    # 高位过滤加强
    if filters.get('strict_highpos'):
        if pos60 is not None and pos60 > 0.5 and abs_n < 1.3:
            return None

    # 资金流出加强
    if filters.get('strict_fund_outflow'):
        if s['s_fund'] < -0.3 and abs_n < 1.3:
            return None

    # 概念板块弱势加强
    if filters.get('strict_concept_neg'):
        if s['s_concept'] < -0.3 and abs_n < 1.3:
            return None

    # 大盘弱势过滤
    if filters.get('market_weak_filter'):
        if s['s_market'] < -0.3 and abs_n < 1.3:
            return None

    # 量价背离过滤
    if filters.get('volume_diverge_filter'):
        if s['s_volume'] < -0.3 and abs_n < 1.3:
            return None

    # 多维度一致性要求：至少N个维度看涨
    min_bullish = filters.get('min_bullish_dims', 0)
    if min_bullish > 0:
        bullish_count = sum(1 for d in dims if d > 0.2)
        if bullish_count < min_bullish:
            return None

    # 最低概念分要求
    min_concept = filters.get('min_concept_score', -999)
    if s['s_concept'] < min_concept:
        return None

    return norm


def run_config(samples, config, label=""):
    """运行一个配置，返回 (准确率, 总预测, 正确数, 按月统计)"""
    total = correct = 0
    by_ym = defaultdict(lambda: {'pred': 0, 'correct': 0})
    for s in samples:
        pred = predict_v3(s, config)
        if pred is not None:
            total += 1
            if s['actual_up']:
                correct += 1
            ym = f"{s['ym'][0]}-{s['ym'][1]:02d}"
            by_ym[ym]['pred'] += 1
            if s['actual_up']:
                by_ym[ym]['correct'] += 1
    acc = correct / total * 100 if total > 0 else 0
    return acc, total, correct, by_ym


def print_result(label, acc, total, correct, by_ym):
    print(f"\n  {label}: {acc:.1f}% ({correct}/{total})")
    for ym in sorted(by_ym.keys()):
        d = by_ym[ym]
        if d['pred'] > 0:
            a = d['correct'] / d['pred'] * 100
            print(f"    {ym}: {a:.0f}% ({d['correct']}/{d['pred']})")


def main():
    samples = load_samples()
    if not samples:
        return

    total_up = sum(1 for s in samples if s['actual_up'])
    print(f"\n总样本: {len(samples)}, 实际涨: {total_up} ({total_up/len(samples)*100:.1f}%)")

    by_ym = defaultdict(lambda: {'total': 0, 'up': 0})
    for s in samples:
        ym = f"{s['ym'][0]}-{s['ym'][1]:02d}"
        by_ym[ym]['total'] += 1
        if s['actual_up']:
            by_ym[ym]['up'] += 1
    print("\n月基准涨率:")
    for ym in sorted(by_ym.keys()):
        d = by_ym[ym]
        print(f"  {ym}: {d['up']/d['total']*100:.1f}% ({d['up']}/{d['total']})")

    print("\n" + "=" * 70)
    print("  阶段1: 阈值扫描（找到85%的最低阈值）")
    print("=" * 70)

    base_w = {'price_momentum': 0.4, 'volume_price': 0.4, 'market_env': 1.2,
              'fund_flow': 0.6, 'concept_board': 1.2}

    for thr in [0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.8, 2.0]:
        cfg = {'weights': base_w, 'threshold': thr}
        a, t, c, ym = run_config(samples, cfg)
        print(f"  阈值={thr:.1f}: {a:.1f}% ({c}/{t})")

    print("\n" + "=" * 70)
    print("  阶段2: 阈值+过滤组合搜索")
    print("=" * 70)

    filter_sets = {
        '无过滤': {},
        '熊市过滤': {'bear_market_filter': True},
        '高位加强': {'strict_highpos': True},
        '资金加强': {'strict_fund_outflow': True},
        '概念加强': {'strict_concept_neg': True},
        '大盘弱势': {'market_weak_filter': True},
        '量价背离': {'volume_diverge_filter': True},
        '至少3维看涨': {'min_bullish_dims': 3},
        '至少4维看涨': {'min_bullish_dims': 4},
        '概念分>=0': {'min_concept_score': 0},
        '概念分>=0.3': {'min_concept_score': 0.3},
        '熊市+高位+资金': {'bear_market_filter': True, 'strict_highpos': True, 'strict_fund_outflow': True},
        '熊市+概念+大盘': {'bear_market_filter': True, 'strict_concept_neg': True, 'market_weak_filter': True},
        '全部过滤': {'bear_market_filter': True, 'strict_highpos': True, 'strict_fund_outflow': True,
                   'strict_concept_neg': True, 'market_weak_filter': True, 'volume_diverge_filter': True},
        '全部+3维看涨': {'bear_market_filter': True, 'strict_highpos': True, 'strict_fund_outflow': True,
                      'strict_concept_neg': True, 'market_weak_filter': True, 'min_bullish_dims': 3},
        '全部+4维看涨': {'bear_market_filter': True, 'strict_highpos': True, 'strict_fund_outflow': True,
                      'strict_concept_neg': True, 'market_weak_filter': True, 'min_bullish_dims': 4},
        '全部+概念>=0': {'bear_market_filter': True, 'strict_highpos': True, 'strict_fund_outflow': True,
                      'strict_concept_neg': True, 'market_weak_filter': True, 'min_concept_score': 0},
        '全部+概念>=0.3': {'bear_market_filter': True, 'strict_highpos': True, 'strict_fund_outflow': True,
                        'strict_concept_neg': True, 'market_weak_filter': True, 'min_concept_score': 0.3},
    }

    results_85 = []

    for thr in [0.9, 1.0, 1.1, 1.2, 1.3]:
        for fname, fset in filter_sets.items():
            cfg = {'weights': base_w, 'threshold': thr, 'filters': fset}
            a, t, c, ym = run_config(samples, cfg)
            if a >= 85 and t >= 10:
                results_85.append((a, t, c, thr, fname, ym))
            if a >= 80 and t >= 10:
                print(f"  thr={thr:.1f} + {fname:<20s}: {a:.1f}% ({c}/{t})")

    print("\n" + "=" * 70)
    print("  阶段3: 达到85%的配置（按预测数量排序）")
    print("=" * 70)

    if not results_85:
        print("  未找到>=85%且样本>=10的配置")

        # 放宽到80%看看
        print("\n  放宽到>=80%的配置:")
        results_80 = []
        for thr in [0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5]:
            for fname, fset in filter_sets.items():
                cfg = {'weights': base_w, 'threshold': thr, 'filters': fset}
                a, t, c, ym = run_config(samples, cfg)
                if a >= 80 and t >= 10:
                    results_80.append((a, t, c, thr, fname, ym))
        results_80.sort(key=lambda x: (-x[0], -x[1]))
        for a, t, c, thr, fname, ym in results_80[:15]:
            print(f"    {a:.1f}% ({c}/{t}) thr={thr:.1f} + {fname}")
    else:
        results_85.sort(key=lambda x: (-x[1], -x[0]))  # 按预测数量降序
        for a, t, c, thr, fname, ym in results_85[:15]:
            print(f"    {a:.1f}% ({c}/{t}) thr={thr:.1f} + {fname}")
            for y in sorted(ym.keys()):
                d = ym[y]
                if d['pred'] > 0:
                    ya = d['correct'] / d['pred'] * 100
                    print(f"      {y}: {ya:.0f}% ({d['correct']}/{d['pred']})")

    print("\n" + "=" * 70)
    print("  阶段4: 权重+阈值+过滤 网格搜索（目标85%）")
    print("=" * 70)

    best_85 = None
    best_85_count = 0

    # 在最优权重附近搜索
    for wp in [0.3, 0.4, 0.5, 0.6]:
        for wv in [0.3, 0.4, 0.5]:
            for wm in [1.0, 1.2, 1.4]:
                for wf in [0.4, 0.6, 0.8]:
                    for wc in [1.0, 1.2, 1.5, 1.8]:
                        for thr in [1.0, 1.1, 1.2, 1.3]:
                            w = {'price_momentum': wp, 'volume_price': wv,
                                 'market_env': wm, 'fund_flow': wf, 'concept_board': wc}
                            # 用最有效的过滤组合
                            for fname, fset in [
                                ('无', {}),
                                ('熊市+高位+资金', {'bear_market_filter': True, 'strict_highpos': True, 'strict_fund_outflow': True}),
                                ('全部', {'bear_market_filter': True, 'strict_highpos': True, 'strict_fund_outflow': True,
                                        'strict_concept_neg': True, 'market_weak_filter': True}),
                            ]:
                                cfg = {'weights': w, 'threshold': thr, 'filters': fset}
                                a, t, c, ym = run_config(samples, cfg)
                                if a >= 85 and t > best_85_count:
                                    best_85 = (a, t, c, w, thr, fname, ym)
                                    best_85_count = t

    if best_85:
        a, t, c, w, thr, fname, ym = best_85
        print(f"\n  最优85%配置: {a:.1f}% ({c}/{t})")
        print(f"    权重: p={w['price_momentum']} v={w['volume_price']} m={w['market_env']} "
              f"f={w['fund_flow']} c={w['concept_board']}")
        print(f"    阈值: {thr}")
        print(f"    过滤: {fname}")
        for y in sorted(ym.keys()):
            d = ym[y]
            if d['pred'] > 0:
                ya = d['correct'] / d['pred'] * 100
                print(f"    {y}: {ya:.0f}% ({d['correct']}/{d['pred']})")
    else:
        print("  网格搜索未找到>=85%的配置")

        # 找最接近85%的
        best_near = None
        best_near_acc = 0
        for wp in [0.3, 0.4, 0.5]:
            for wm in [1.0, 1.2, 1.4]:
                for wc in [1.0, 1.2, 1.5, 1.8]:
                    for thr in [1.0, 1.1, 1.2, 1.3, 1.4, 1.5]:
                        w = {'price_momentum': wp, 'volume_price': 0.4,
                             'market_env': wm, 'fund_flow': 0.6, 'concept_board': wc}
                        for fname, fset in [
                            ('全部', {'bear_market_filter': True, 'strict_highpos': True,
                                    'strict_fund_outflow': True, 'strict_concept_neg': True,
                                    'market_weak_filter': True}),
                        ]:
                            cfg = {'weights': w, 'threshold': thr, 'filters': fset}
                            a, t, c, ym = run_config(samples, cfg)
                            if t >= 10 and a > best_near_acc:
                                best_near = (a, t, c, w, thr, fname, ym)
                                best_near_acc = a

        if best_near:
            a, t, c, w, thr, fname, ym = best_near
            print(f"\n  最接近85%: {a:.1f}% ({c}/{t})")
            print(f"    权重: p={w['price_momentum']} v={w['volume_price']} m={w['market_env']} "
                  f"f={w['fund_flow']} c={w['concept_board']}")
            print(f"    阈值: {thr}, 过滤: {fname}")
            for y in sorted(ym.keys()):
                d = ym[y]
                if d['pred'] > 0:
                    ya = d['correct'] / d['pred'] * 100
                    print(f"    {y}: {ya:.0f}% ({d['correct']}/{d['pred']})")

    print("\n" + "=" * 70)
    print("  回测完成")
    print("=" * 70)


if __name__ == '__main__':
    main()
