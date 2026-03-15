#!/usr/bin/env python3
"""分析v4数据中各种信号的预测能力，寻找突破82%天花板的方法。"""
import sys, logging
from collections import defaultdict
from datetime import datetime
sys.path.insert(0, '.')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

from day_week_predicted.backtest.concept_strength_weekly_v4_backtest import (
    _preload_v4_data, _build_weekly_records, _compound_return, _mean, _std,
    STRONG_THRESHOLD, FUZZY_THRESHOLD,
)

def main():
    from day_week_predicted.tests.test_concept_strength_weekly_v4_100boards import (
        fetch_boards_from_db, _check_db_available
    )
    if not _check_db_available():
        print("DB不可达"); return

    board_stock_map, all_codes = fetch_boards_from_db(min_stocks=20, target_boards=100)
    print(f"板块: {len(board_stock_map)}, 股票: {len(all_codes)}")

    data = _preload_v4_data(all_codes, '2025-08-01', '2026-03-13')
    weekly = _build_weekly_records(all_codes, data, '2025-08-01', '2026-03-13', board_stock_map)
    print(f"周样本: {len(weekly)}")

    # ── 分析1: 各种信号在模糊区的预测能力 ──
    print("\n" + "="*80)
    print("  信号分析：寻找突破82%天花板的方法")
    print("="*80)

    # 按d3区间分组
    zones = {'strong': [], 'medium': [], 'fuzzy': []}
    for w in weekly:
        d3 = abs(w['d3_chg'])
        if d3 > STRONG_THRESHOLD:
            zones['strong'].append(w)
        elif d3 > FUZZY_THRESHOLD:
            zones['medium'].append(w)
        else:
            zones['fuzzy'].append(w)

    for zname, records in zones.items():
        up = sum(1 for r in records if r['weekly_up'])
        d3_correct = sum(1 for r in records if (r['d3_chg'] >= 0) == r['weekly_up'])
        print(f"\n  {zname}: {len(records)}条, 涨率={up/len(records)*100:.1f}%, "
              f"d3准确率={d3_correct/len(records)*100:.1f}%")

    # ── 分析2: 日内模式分析（模糊区） ──
    print("\n" + "-"*60)
    print("  模糊区日内模式分析")
    print("-"*60)

    fuzzy = zones['fuzzy']
    # 3天涨跌模式
    pattern_stats = defaultdict(lambda: [0, 0])  # [up_count, total]
    for w in fuzzy:
        d3_daily = w.get('d3_daily', [])
        if len(d3_daily) >= 3:
            pattern = ''.join(['U' if d >= 0 else 'D' for d in d3_daily[:3]])
            pattern_stats[pattern][1] += 1
            if w['weekly_up']:
                pattern_stats[pattern][0] += 1

    print(f"  {'模式':<8s} {'涨率':>8s} {'样本数':>8s} {'预测方向':>8s} {'准确率':>8s}")
    for pat in sorted(pattern_stats.keys()):
        up, total = pattern_stats[pat]
        up_rate = up / total * 100
        # 预测：涨率>50%预测涨，否则预测跌
        pred_up = up_rate >= 50
        correct = up if pred_up else (total - up)
        acc = correct / total * 100
        print(f"  {pat:<8s} {up_rate:>6.1f}%  {total:>6d}    {'涨' if pred_up else '跌':>4s}  {acc:>6.1f}%")

    # ── 分析3: 板块d3方向作为信号 ──
    print("\n" + "-"*60)
    print("  板块d3方向 + 大盘d3方向 作为辅助信号")
    print("-"*60)

    # 计算每个板块每周的d3
    board_kline_map = data['board_kline_map']
    board_week_d3 = {}  # (board_code, iso_week) -> d3_chg
    for bc, klines in board_kline_map.items():
        bt_klines = [k for k in klines if '2025-08-01' <= k['date'] <= '2026-03-13']
        wg = defaultdict(list)
        for k in bt_klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            wg[iw].append(k)
        for iw, days in wg.items():
            days.sort(key=lambda x: x['date'])
            if len(days) >= 3:
                d3 = _compound_return([d['change_percent'] for d in days[:3]])
                board_week_d3[(bc, iw)] = d3

    # 对模糊区样本，看板块d3方向是否有预测力
    stock_boards = data['stock_boards']
    board_agree_stats = {'agree_up': [0,0], 'agree_down': [0,0],
                         'disagree': [0,0], 'no_board': [0,0]}
    market_agree_stats = {'agree': [0,0], 'disagree': [0,0]}

    for w in fuzzy:
        code = w['code']
        iw = w['iso_week']
        boards = stock_boards.get(code, [])

        # 板块d3方向投票
        board_ups = 0
        board_total = 0
        for b in boards:
            bd3 = board_week_d3.get((b['board_code'], iw))
            if bd3 is not None:
                board_total += 1
                if bd3 >= 0:
                    board_ups += 1

        if board_total > 0:
            board_vote_up = board_ups / board_total > 0.5
            stock_d3_up = w['d3_chg'] >= 0
            if board_vote_up == stock_d3_up:
                key = 'agree_up' if stock_d3_up else 'agree_down'
            else:
                key = 'disagree'
        else:
            key = 'no_board'
            board_vote_up = None

        board_agree_stats[key][1] += 1
        if w['weekly_up']:
            board_agree_stats[key][0] += 1

        # 大盘d3方向
        mkt_d3_up = w.get('market_d3_chg', 0) >= 0
        stock_d3_up = w['d3_chg'] >= 0
        mkey = 'agree' if mkt_d3_up == stock_d3_up else 'disagree'
        market_agree_stats[mkey][1] += 1
        if w['weekly_up']:
            market_agree_stats[mkey][0] += 1

    print("\n  板块d3与个股d3一致性（模糊区）:")
    for k, (up, total) in board_agree_stats.items():
        if total > 0:
            print(f"    {k:<14s}: 涨率={up/total*100:.1f}%, n={total}")

    print("\n  大盘d3与个股d3一致性（模糊区）:")
    for k, (up, total) in market_agree_stats.items():
        if total > 0:
            print(f"    {k:<14s}: 涨率={up/total*100:.1f}%, n={total}")

    # ── 分析4: 成交量信号 ──
    print("\n" + "-"*60)
    print("  成交量信号分析（模糊区）")
    print("-"*60)

    vol_signal_stats = {'vol_up': [0,0], 'vol_down': [0,0], 'vol_flat': [0,0]}
    for w in fuzzy:
        code = w['code']
        klines = data['stock_klines'].get(code, [])
        wed_date = w['wed_date']
        valid_k = [k for k in klines if k['date'] <= wed_date]
        if len(valid_k) < 10:
            continue
        # 最近3天平均成交量 vs 前10天平均
        recent_3 = valid_k[-3:]
        prev_10 = valid_k[-13:-3] if len(valid_k) >= 13 else valid_k[:-3]
        if not prev_10:
            continue
        avg_vol_3 = _mean([k['volume'] for k in recent_3])
        avg_vol_10 = _mean([k['volume'] for k in prev_10])
        if avg_vol_10 <= 0:
            continue
        vol_ratio = avg_vol_3 / avg_vol_10
        if vol_ratio > 1.3:
            key = 'vol_up'
        elif vol_ratio < 0.7:
            key = 'vol_down'
        else:
            key = 'vol_flat'
        vol_signal_stats[key][1] += 1
        if w['weekly_up']:
            vol_signal_stats[key][0] += 1

    for k, (up, total) in vol_signal_stats.items():
        if total > 0:
            print(f"    {k:<12s}: 涨率={up/total*100:.1f}%, n={total}")

    # ── 分析5: 概念信号分析 ──
    print("\n" + "-"*60)
    print("  概念信号分析（模糊区）")
    print("-"*60)

    sig_stats = {'strong_board': [0,0], 'weak_board': [0,0],
                 'neutral_board': [0,0], 'no_sig': [0,0]}
    for w in fuzzy:
        sig = w.get('concept_signal')
        if sig is None:
            sig_stats['no_sig'][1] += 1
            if w['weekly_up']:
                sig_stats['no_sig'][0] += 1
            continue
        bm = sig.get('board_momentum', 0)
        if bm > 0.3:
            key = 'strong_board'
        elif bm < -0.3:
            key = 'weak_board'
        else:
            key = 'neutral_board'
        sig_stats[key][1] += 1
        if w['weekly_up']:
            sig_stats[key][0] += 1

    for k, (up, total) in sig_stats.items():
        if total > 0:
            print(f"    {k:<16s}: 涨率={up/total*100:.1f}%, n={total}")

    # ── 分析6: 中等区信号分析 ──
    print("\n" + "-"*60)
    print("  中等区(0.8-2%)辅助信号分析")
    print("-"*60)

    medium = zones['medium']
    # 板块d3一致性
    med_board = {'agree': [0,0], 'disagree': [0,0], 'no_board': [0,0]}
    for w in medium:
        code = w['code']
        iw = w['iso_week']
        boards = stock_boards.get(code, [])
        board_ups = 0
        board_total = 0
        for b in boards:
            bd3 = board_week_d3.get((b['board_code'], iw))
            if bd3 is not None:
                board_total += 1
                if bd3 >= 0:
                    board_ups += 1
        if board_total > 0:
            board_vote_up = board_ups / board_total > 0.5
            stock_d3_up = w['d3_chg'] > 0
            key = 'agree' if board_vote_up == stock_d3_up else 'disagree'
        else:
            key = 'no_board'
        med_board[key][1] += 1
        if (w['d3_chg'] > 0) == w['weekly_up']:
            med_board[key][0] += 1

    print("\n  板块d3与个股d3一致时，d3预测准确率（中等区）:")
    for k, (ok, total) in med_board.items():
        if total > 0:
            print(f"    {k:<14s}: d3准确率={ok/total*100:.1f}%, n={total}")

    # ── 分析7: 综合最优策略估算 ──
    print("\n" + "="*80)
    print("  综合最优策略估算")
    print("="*80)

    # 强区: follow d3
    strong = zones['strong']
    s_ok = sum(1 for w in strong if (w['d3_chg'] > 0) == w['weekly_up'])
    print(f"\n  强区({len(strong)}): d3准确率={s_ok/len(strong)*100:.1f}%")

    # 中等区: follow d3
    m_ok = sum(1 for w in medium if (w['d3_chg'] > 0) == w['weekly_up'])
    print(f"  中等区({len(medium)}): d3准确率={m_ok/len(medium)*100:.1f}%")

    # 模糊区: 各种策略
    f_d3_ok = sum(1 for w in fuzzy if (w['d3_chg'] >= 0) == w['weekly_up'])
    f_up_ok = sum(1 for w in fuzzy if w['weekly_up'])
    print(f"  模糊区({len(fuzzy)}): d3准确率={f_d3_ok/len(fuzzy)*100:.1f}%, "
          f"always_up={f_up_ok/len(fuzzy)*100:.1f}%")

    # 估算: 如果模糊区用always_up
    total = len(weekly)
    est_correct = s_ok + m_ok + f_up_ok
    print(f"\n  估算(强d3+中d3+模糊always_up): {est_correct}/{total} = "
          f"{est_correct/total*100:.1f}%")

    # 估算: 如果模糊区用d3
    est2 = s_ok + m_ok + f_d3_ok
    print(f"  估算(全部follow_d3): {est2}/{total} = {est2/total*100:.1f}%")

    # 估算: 模糊区个股自适应(up_rate>=65%用always_up)
    stock_up_rates = defaultdict(lambda: [0, 0])
    for w in weekly:
        stock_up_rates[w['code']][1] += 1
        if w['weekly_up']:
            stock_up_rates[w['code']][0] += 1

    f_adaptive_ok = 0
    f_adaptive_detail = {'high_up': [0,0], 'low_up': [0,0], 'mid': [0,0]}
    for w in fuzzy:
        code = w['code']
        up, n = stock_up_rates[code]
        ur = up / n if n > 0 else 0.5
        if ur >= 0.65:
            pred_up = True
            cat = 'high_up'
        elif ur < 0.35:
            pred_up = False
            cat = 'low_up'
        else:
            pred_up = w['d3_chg'] >= 0
            cat = 'mid'
        correct = pred_up == w['weekly_up']
        f_adaptive_detail[cat][1] += 1
        if correct:
            f_adaptive_ok += 1
            f_adaptive_detail[cat][0] += 1

    est3 = s_ok + m_ok + f_adaptive_ok
    print(f"  估算(强d3+中d3+模糊自适应): {est3}/{total} = {est3/total*100:.1f}%")
    for cat, (ok, n) in f_adaptive_detail.items():
        if n > 0:
            print(f"    {cat}: {ok/n*100:.1f}% ({ok}/{n})")

    print("\n  完成分析。")

if __name__ == '__main__':
    main()
