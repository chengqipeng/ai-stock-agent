#!/usr/bin/env python3
"""深入分析v4信号组合，找到最优策略。"""
import sys, logging
from collections import defaultdict
from datetime import datetime
sys.path.insert(0, '.')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

from day_week_predicted.backtest.concept_strength_weekly_v4_backtest import (
    _preload_v4_data, _build_weekly_records, _compound_return, _mean,
    STRONG_THRESHOLD, FUZZY_THRESHOLD,
)

def main():
    from day_week_predicted.tests.test_concept_strength_weekly_v4_100boards import (
        fetch_boards_from_db, _check_db_available
    )
    if not _check_db_available():
        print("DB不可达"); return

    board_stock_map, all_codes = fetch_boards_from_db(min_stocks=20, target_boards=100)
    data = _preload_v4_data(all_codes, '2025-08-01', '2026-03-13')
    weekly = _build_weekly_records(all_codes, data, '2025-08-01', '2026-03-13', board_stock_map)
    print(f"周样本: {len(weekly)}")

    # 计算板块周d3
    board_kline_map = data['board_kline_map']
    stock_boards = data['stock_boards']
    board_week_d3 = {}
    for bc, klines in board_kline_map.items():
        bt = [k for k in klines if '2025-08-01' <= k['date'] <= '2026-03-13']
        wg = defaultdict(list)
        for k in bt:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            wg[iw].append(k)
        for iw, days in wg.items():
            days.sort(key=lambda x: x['date'])
            if len(days) >= 3:
                board_week_d3[(bc, iw)] = _compound_return([d['change_percent'] for d in days[:3]])

    # 个股历史涨率
    stock_up_rates = defaultdict(lambda: [0, 0])
    for w in weekly:
        stock_up_rates[w['code']][1] += 1
        if w['weekly_up']:
            stock_up_rates[w['code']][0] += 1

    # ── 分析UUU模式的本质 ──
    print("\n" + "="*80)
    print("  UUU模式深入分析")
    print("="*80)
    fuzzy = [w for w in weekly if abs(w['d3_chg']) <= FUZZY_THRESHOLD]
    uuu = [w for w in fuzzy if len(w.get('d3_daily',[])) >= 3
           and all(d >= 0 for d in w['d3_daily'][:3])]
    print(f"  UUU样本: {len(uuu)}")
    # 这些是不是停牌股？
    suspended = [w for w in uuu if all(d == 0 for d in w['d3_daily'][:3])]
    print(f"  其中全0(停牌): {len(suspended)}")
    non_susp_uuu = [w for w in uuu if not all(d == 0 for d in w['d3_daily'][:3])]
    if non_susp_uuu:
        up = sum(1 for w in non_susp_uuu if w['weekly_up'])
        print(f"  非停牌UUU: {len(non_susp_uuu)}, 涨率={up/len(non_susp_uuu)*100:.1f}%")
    if suspended:
        up = sum(1 for w in suspended if w['weekly_up'])
        print(f"  停牌UUU: {len(suspended)}, 涨率={up/len(suspended)*100:.1f}%")

    # ── 分析: 停牌股(d3=0)的影响 ──
    print("\n" + "="*80)
    print("  停牌股(d3=0)分析")
    print("="*80)
    all_suspended = [w for w in weekly if w['d3_chg'] == 0
                     and len(w.get('d3_daily',[])) >= 3
                     and all(d == 0 for d in w['d3_daily'][:3])]
    print(f"  停牌样本(前3天全0): {len(all_suspended)}")
    if all_suspended:
        up = sum(1 for w in all_suspended if w['weekly_up'])
        print(f"  涨率: {up/len(all_suspended)*100:.1f}%")
        # 这些全部预测涨，准确率就是涨率
        print(f"  如果全部预测涨: {up/len(all_suspended)*100:.1f}%")

    # 去掉停牌后的真实准确率
    non_susp = [w for w in weekly if not (w['d3_chg'] == 0
                and len(w.get('d3_daily',[])) >= 3
                and all(d == 0 for d in w['d3_daily'][:3]))]
    print(f"\n  非停牌样本: {len(non_susp)}")
    d3_ok = sum(1 for w in non_susp if (w['d3_chg'] >= 0) == w['weekly_up'])
    print(f"  非停牌d3准确率: {d3_ok/len(non_susp)*100:.1f}%")

    # ── 分析: 多信号组合在模糊区的效果 ──
    print("\n" + "="*80)
    print("  模糊区多信号组合分析")
    print("="*80)

    # 去掉停牌的模糊区
    fuzzy_real = [w for w in fuzzy if not (w['d3_chg'] == 0
                  and len(w.get('d3_daily',[])) >= 3
                  and all(d == 0 for d in w['d3_daily'][:3]))]
    print(f"  模糊区(去停牌): {len(fuzzy_real)}")

    # 信号1: 日内模式(连续方向)
    # 信号2: 板块d3方向投票
    # 信号3: 大盘d3方向
    # 信号4: 个股历史涨率
    # 组合投票

    combo_stats = defaultdict(lambda: [0, 0])  # votes -> [correct, total]
    for w in fuzzy_real:
        code = w['code']
        iw = w['iso_week']
        d3_daily = w.get('d3_daily', [0,0,0])[:3]

        # 信号1: d3方向
        vote_d3 = 1 if w['d3_chg'] >= 0 else -1

        # 信号2: 日内模式 - 最后一天方向
        vote_day3 = 1 if d3_daily[2] >= 0 else -1

        # 信号3: 板块d3投票
        boards = stock_boards.get(code, [])
        b_ups = sum(1 for b in boards if board_week_d3.get((b['board_code'], iw), 0) >= 0)
        b_total = sum(1 for b in boards if (b['board_code'], iw) in board_week_d3)
        vote_board = 1 if (b_total > 0 and b_ups / b_total > 0.5) else -1

        # 信号4: 大盘d3
        vote_market = 1 if w.get('market_d3_chg', 0) >= 0 else -1

        # 信号5: 个股历史涨率
        up_n, total_n = stock_up_rates[code]
        ur = up_n / total_n if total_n > 0 else 0.5
        vote_prior = 1 if ur >= 0.5 else -1

        # 总投票
        total_votes = vote_d3 + vote_board + vote_market + vote_prior
        pred_up = total_votes >= 0
        correct = pred_up == w['weekly_up']

        combo_stats[total_votes][1] += 1
        if correct:
            combo_stats[total_votes][0] += 1

    print(f"\n  投票组合(d3+板块+大盘+先验):")
    print(f"  {'投票':>6s} {'准确率':>8s} {'样本数':>8s}")
    total_combo_ok = 0
    total_combo_n = 0
    for votes in sorted(combo_stats.keys()):
        ok, n = combo_stats[votes]
        total_combo_ok += ok
        total_combo_n += n
        print(f"  {votes:>+4d}   {ok/n*100:.1f}%   {n:>6d}")
    print(f"  总计: {total_combo_ok/total_combo_n*100:.1f}% ({total_combo_ok}/{total_combo_n})")

    # ── 分析: 中等区板块一致性增强 ──
    print("\n" + "="*80)
    print("  中等区多信号组合")
    print("="*80)
    medium = [w for w in weekly if FUZZY_THRESHOLD < abs(w['d3_chg']) <= STRONG_THRESHOLD]
    med_combo = defaultdict(lambda: [0, 0])
    for w in medium:
        code = w['code']
        iw = w['iso_week']
        d3_up = w['d3_chg'] > 0

        boards = stock_boards.get(code, [])
        b_ups = sum(1 for b in boards if board_week_d3.get((b['board_code'], iw), 0) >= 0)
        b_total = sum(1 for b in boards if (b['board_code'], iw) in board_week_d3)
        board_agree = b_total > 0 and (b_ups / b_total > 0.5) == d3_up

        mkt_agree = (w.get('market_d3_chg', 0) >= 0) == d3_up

        key = f"board={'Y' if board_agree else 'N'}_mkt={'Y' if mkt_agree else 'N'}"
        med_combo[key][1] += 1
        if d3_up == w['weekly_up']:
            med_combo[key][0] += 1

    print(f"  {'组合':<20s} {'d3准确率':>10s} {'样本数':>8s}")
    for k in sorted(med_combo.keys()):
        ok, n = med_combo[k]
        print(f"  {k:<20s} {ok/n*100:.1f}%    {n:>6d}")

    # ── 最终估算 ──
    print("\n" + "="*80)
    print("  最终最优策略估算")
    print("="*80)

    # 策略: 
    # 强区: follow d3
    # 中等区: follow d3 (板块+大盘都同意时更高)
    # 模糊区-停牌: always up
    # 模糊区-高涨率(>=65%): always up
    # 模糊区-低涨率(<35%): always down
    # 模糊区-其他: 多信号投票(d3+板块+大盘+先验)

    total_ok = 0
    total_n = 0
    zone_stats = defaultdict(lambda: [0, 0])

    for w in weekly:
        d3 = w['d3_chg']
        code = w['code']
        iw = w['iso_week']
        d3_daily = w.get('d3_daily', [0,0,0])[:3]
        is_suspended = (d3 == 0 and all(d == 0 for d in d3_daily))

        if abs(d3) > STRONG_THRESHOLD:
            pred_up = d3 > 0
            zone = 'strong'
        elif abs(d3) > FUZZY_THRESHOLD:
            pred_up = d3 > 0
            zone = 'medium'
        elif is_suspended:
            pred_up = True
            zone = 'suspended'
        else:
            up_n, total_n_s = stock_up_rates[code]
            ur = up_n / total_n_s if total_n_s > 0 else 0.5
            if ur >= 0.65:
                pred_up = True
                zone = 'fuzzy_high_ur'
            elif ur < 0.35:
                pred_up = False
                zone = 'fuzzy_low_ur'
            else:
                # 多信号投票
                vote = 0
                vote += 1 if d3 >= 0 else -1
                boards = stock_boards.get(code, [])
                b_ups = sum(1 for b in boards if board_week_d3.get((b['board_code'], iw), 0) >= 0)
                b_t = sum(1 for b in boards if (b['board_code'], iw) in board_week_d3)
                vote += 1 if (b_t > 0 and b_ups / b_t > 0.5) else -1
                vote += 1 if w.get('market_d3_chg', 0) >= 0 else -1
                vote += 1 if ur >= 0.5 else -1
                pred_up = vote >= 0
                zone = 'fuzzy_vote'

        correct = pred_up == w['weekly_up']
        zone_stats[zone][1] += 1
        if correct:
            zone_stats[zone][0] += 1
            total_ok += 1
        total_n += 1

    print(f"\n  {'区域':<16s} {'准确率':>8s} {'样本数':>8s}")
    for z in ['strong','medium','suspended','fuzzy_high_ur','fuzzy_low_ur','fuzzy_vote']:
        ok, n = zone_stats[z]
        if n > 0:
            print(f"  {z:<16s} {ok/n*100:.1f}%   {n:>6d}")
    print(f"\n  总计: {total_ok}/{total_n} = {total_ok/total_n*100:.1f}%")

    print("\n  完成。")

if __name__ == '__main__':
    main()
