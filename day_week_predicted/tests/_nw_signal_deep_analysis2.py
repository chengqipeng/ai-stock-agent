#!/usr/bin/env python3
"""
下周预测信号深度分析 v2
======================
基于v1的发现，进一步挖掘高准确率组合条件。

核心发现：
- 本周跌>3% + 大盘跌>1% → 反转: 74.3% ★★★
- 本周跌>3% + 连跌>=4天 → 反转: ~63%
- 本周跌>3% + 尾日跌>1% → 反转: ~63%
- 跌的反转信号远强于涨的反转信号（不对称性）

策略：只在高置信条件下预测，其余标记"不确定"
"""
import sys
import logging
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, '.')

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

from dao import get_connection
from service.weekly_prediction_service import (
    _to_float, _compound_return, _mean, _std,
    _get_all_stock_codes, _get_latest_trade_date,
)


def load_records(n_weeks=29, sample_limit=2000):
    latest_date = _get_latest_trade_date()
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(n_weeks + 2) * 7)
    start_date = dt_start.strftime('%Y-%m-%d')

    all_codes = _get_all_stock_codes()
    if sample_limit > 0:
        import random
        random.seed(42)
        all_codes = random.sample(all_codes, min(sample_limit, len(all_codes)))

    logger.info("分析 %d 只股票, 区间 %s ~ %s", len(all_codes), start_date, latest_date)

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    stock_klines = defaultdict(list)
    batch_size = 200
    for i in range(0, len(all_codes), batch_size):
        batch = all_codes[i:i + batch_size]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, change_percent "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [start_date, latest_date])
        for row in cur.fetchall():
            stock_klines[row['stock_code']].append({
                'date': row['date'],
                'change_percent': _to_float(row['change_percent']),
            })

    cur.execute(
        "SELECT `date`, change_percent FROM stock_kline "
        "WHERE stock_code = '000001.SH' AND `date` >= %s AND `date` <= %s ORDER BY `date`",
        (start_date, latest_date))
    market_klines = [{'date': r['date'], 'change_percent': _to_float(r['change_percent'])} for r in cur.fetchall()]

    # 板块
    stock_boards = defaultdict(list)
    code_6_list = list(set(c[:6] for c in all_codes))
    for i in range(0, len(code_6_list), batch_size):
        batch = code_6_list[i:i + batch_size]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(f"SELECT stock_code, board_code FROM stock_concept_board_stock WHERE stock_code IN ({ph})", batch)
        for row in cur.fetchall():
            sc6 = row['stock_code']
            full = f'{sc6}.SH' if sc6.startswith('6') else f'{sc6}.SZ'
            stock_boards[full].append(row['board_code'])

    all_board_codes = set()
    for boards in stock_boards.values():
        all_board_codes.update(boards)

    board_klines = defaultdict(list)
    for i in range(0, len(list(all_board_codes)), batch_size):
        batch = list(all_board_codes)[i:i + batch_size]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT board_code, `date`, change_percent FROM concept_board_kline "
            f"WHERE board_code IN ({ph}) AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [start_date, latest_date])
        for row in cur.fetchall():
            board_klines[row['board_code']].append({
                'date': row['date'],
                'change_percent': _to_float(row['change_percent']),
            })
    conn.close()

    market_by_week = defaultdict(list)
    for k in market_klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        iw = dt.isocalendar()[:2]
        market_by_week[iw].append(k)

    board_by_week = defaultdict(lambda: defaultdict(list))
    for bc, kls in board_klines.items():
        for k in kls:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            board_by_week[bc][iw].append(k)

    records = []
    for code in all_codes:
        klines = stock_klines.get(code, [])
        if len(klines) < 20:
            continue

        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            wg[iw].append(k)

        sorted_weeks = sorted(wg.keys())
        all_abs = [abs(k['change_percent']) for k in klines]
        avg_vol = _mean(all_abs)

        for idx in range(len(sorted_weeks) - 1):
            iw_this = sorted_weeks[idx]
            iw_next = sorted_weeks[idx + 1]

            this_days = sorted(wg[iw_this], key=lambda x: x['date'])
            next_days = sorted(wg[iw_next], key=lambda x: x['date'])

            if len(this_days) < 3 or len(next_days) < 3:
                continue

            this_pcts = [d['change_percent'] for d in this_days]
            this_chg = _compound_return(this_pcts)
            next_pcts = [d['change_percent'] for d in next_days]
            next_chg = _compound_return(next_pcts)
            next_up = next_chg >= 0
            n_days = len(this_days)
            last_day_chg = this_pcts[-1]

            consec_up = 0
            consec_down = 0
            for p in reversed(this_pcts):
                if p > 0:
                    consec_up += 1
                    if consec_down > 0: break
                elif p < 0:
                    consec_down += 1
                    if consec_up > 0: break
                else:
                    break

            # 本周前半段和后半段
            first_half = _compound_return(this_pcts[:len(this_pcts)//2]) if len(this_pcts) >= 4 else this_pcts[0]
            second_half = _compound_return(this_pcts[len(this_pcts)//2:]) if len(this_pcts) >= 4 else this_pcts[-1]

            mw = market_by_week.get(iw_this, [])
            market_chg = _compound_return([k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]) if len(mw) >= 3 else 0

            boards = stock_boards.get(code, [])
            board_avg_chg = 0
            board_agree = 0
            if boards:
                b_chgs = []
                b_agree_count = 0
                for bc in boards:
                    bw = board_by_week.get(bc, {}).get(iw_this, [])
                    if len(bw) >= 3:
                        bc_chg = _compound_return([k['change_percent'] for k in sorted(bw, key=lambda x: x['date'])])
                        b_chgs.append(bc_chg)
                        if (bc_chg >= 0) == (this_chg >= 0):
                            b_agree_count += 1
                if b_chgs:
                    board_avg_chg = _mean(b_chgs)
                    board_agree = b_agree_count / len(b_chgs)

            records.append({
                'code': code,
                'this_chg': this_chg,
                'next_up': next_up,
                'next_chg': next_chg,
                'abs_this_chg': abs(this_chg),
                'last_day_chg': last_day_chg,
                'abs_last_day': abs(last_day_chg),
                'n_days': n_days,
                'consec_up': consec_up,
                'consec_down': consec_down,
                'market_chg': market_chg,
                'board_avg_chg': board_avg_chg,
                'board_agree': board_agree,
                'avg_vol': avg_vol,
                'this_up': this_chg >= 0,
                'first_half': first_half,
                'second_half': second_half,
            })

    logger.info("总记录数: %d", len(records))
    return records


def analyze_tiered_strategy(records):
    """分层策略：按优先级匹配条件，只在高置信时给预测"""

    print(f"\n{'='*70}")
    print(f"  分层策略分析（按优先级匹配，互斥）")
    print(f"{'='*70}")

    # 定义分层规则：(名称, 条件函数, 预测函数)
    # 预测函数: r -> True(涨)/False(跌)
    tiers = [
        # Tier 1: 极高置信（目标>=75%）
        ('T1: 本周跌>3% + 大盘跌>1%',
         lambda r: r['this_chg'] < -3 and r['market_chg'] < -1,
         lambda r: True),  # 反转→涨

        ('T1: 本周跌>5% + 连跌>=3天',
         lambda r: r['this_chg'] < -5 and r['consec_down'] >= 3,
         lambda r: True),  # 反转→涨

        ('T1: 本周跌>3% + 尾日跌>2%',
         lambda r: r['this_chg'] < -3 and r['last_day_chg'] < -2,
         lambda r: True),  # 反转→涨

        ('T1: 本周跌>3% + 板块一致跌>=80%',
         lambda r: r['this_chg'] < -3 and r['board_agree'] >= 0.8 and r['board_avg_chg'] < 0,
         lambda r: True),  # 反转→涨

        # Tier 2: 高置信（目标>=65%）
        ('T2: 本周跌>3% + 连跌>=3天',
         lambda r: r['this_chg'] < -3 and r['consec_down'] >= 3,
         lambda r: True),

        ('T2: 本周跌>3% + 尾日跌>1%',
         lambda r: r['this_chg'] < -3 and r['last_day_chg'] < -1,
         lambda r: True),

        ('T2: 本周跌>5%',
         lambda r: r['this_chg'] < -5,
         lambda r: True),

        ('T2: 本周涨>8% + 连涨>=3天',
         lambda r: r['this_chg'] > 8 and r['consec_up'] >= 3,
         lambda r: False),  # 反转→跌

        ('T2: 本周涨>5% + 大盘涨>1%',
         lambda r: r['this_chg'] > 5 and r['market_chg'] > 1,
         lambda r: False),

        # Tier 3: 中等置信（目标>=60%）
        ('T3: 本周跌>2% + 大盘跌',
         lambda r: r['this_chg'] < -2 and r['market_chg'] < 0,
         lambda r: True),

        ('T3: 本周跌>3%',
         lambda r: r['this_chg'] < -3,
         lambda r: True),

        ('T3: 本周涨>8%',
         lambda r: r['this_chg'] > 8,
         lambda r: False),

        ('T3: 连跌>=4天',
         lambda r: r['consec_down'] >= 4,
         lambda r: True),
    ]

    covered = set()
    tier_results = []
    total_correct = 0
    total_count = 0

    for name, cond_fn, pred_fn in tiers:
        subset = [(i, r) for i, r in enumerate(records) if cond_fn(r) and i not in covered]
        if not subset:
            continue

        ok = sum(1 for i, r in subset if pred_fn(r) == r['next_up'])
        acc = ok / len(subset) * 100
        for i, r in subset:
            covered.add(i)
        total_correct += ok
        total_count += len(subset)
        cum_acc = total_correct / total_count * 100
        marker = ' ★★★' if acc >= 70 else (' ★★' if acc >= 65 else (' ★' if acc >= 60 else ''))
        print(f"  {name:45s}  +{len(subset):5d}  准确率={acc:5.1f}%{marker}  累计={cum_acc:.1f}%")
        tier_results.append((name, len(subset), acc))

    remaining = len(records) - len(covered)
    print(f"\n  已覆盖: {len(covered)}/{len(records)} ({len(covered)/len(records)*100:.1f}%)")
    print(f"  未覆盖: {remaining} ({remaining/len(records)*100:.1f}%)")
    print(f"  已覆盖准确率: {total_correct}/{total_count} = {total_correct/total_count*100:.1f}%")

    # 如果只输出已覆盖的预测（未覆盖标记为"不确定"），准确率就是上面的值
    print(f"\n  → 如果只在已覆盖条件下给出预测，准确率 = {total_correct/total_count*100:.1f}%")
    print(f"  → 覆盖率 = {len(covered)/len(records)*100:.1f}%")

    return covered, total_correct, total_count


def analyze_refined_conditions(records):
    """更精细的条件搜索"""

    print(f"\n{'='*70}")
    print(f"  精细条件搜索（寻找>=70%准确率的条件）")
    print(f"{'='*70}")

    # 系统性搜索：本周跌幅 × 其他条件
    for chg_th in [-2, -3, -4, -5]:
        for mkt_th in [None, -0.5, -1, -1.5]:
            for consec_th in [None, 2, 3, 4]:
                for last_day_th in [None, -1, -2]:
                    cond_parts = [f'跌>{abs(chg_th)}%']
                    def make_cond(ct=chg_th, mt=mkt_th, cst=consec_th, ldt=last_day_th):
                        def cond(r):
                            if r['this_chg'] >= ct: return False
                            if mt is not None and r['market_chg'] >= mt: return False
                            if cst is not None and r['consec_down'] < cst: return False
                            if ldt is not None and r['last_day_chg'] >= ldt: return False
                            return True
                        return cond

                    cond_fn = make_cond()
                    subset = [r for r in records if cond_fn(r)]
                    if len(subset) < 50:
                        continue

                    ok = sum(1 for r in subset if r['next_up'])  # 反转→涨
                    acc = ok / len(subset) * 100
                    if acc >= 65:
                        name = f'跌>{abs(chg_th)}%'
                        if mkt_th is not None: name += f' + 大盘跌>{abs(mkt_th)}%'
                        if consec_th is not None: name += f' + 连跌>={consec_th}天'
                        if last_day_th is not None: name += f' + 尾日跌>{abs(last_day_th)}%'
                        marker = ' ★★★' if acc >= 70 else ' ★★'
                        print(f"  {name:55s}  N={len(subset):5d}  准确率={acc:5.1f}%{marker}")

    # 系统性搜索：本周涨幅 × 其他条件（反转→跌）
    print()
    for chg_th in [2, 3, 4, 5, 8]:
        for mkt_th in [None, 0.5, 1, 1.5]:
            for consec_th in [None, 2, 3, 4]:
                for last_day_th in [None, 1, 2]:
                    def make_cond(ct=chg_th, mt=mkt_th, cst=consec_th, ldt=last_day_th):
                        def cond(r):
                            if r['this_chg'] <= ct: return False
                            if mt is not None and r['market_chg'] <= mt: return False
                            if cst is not None and r['consec_up'] < cst: return False
                            if ldt is not None and r['last_day_chg'] <= ldt: return False
                            return True
                        return cond

                    cond_fn = make_cond()
                    subset = [r for r in records if cond_fn(r)]
                    if len(subset) < 50:
                        continue

                    ok = sum(1 for r in subset if not r['next_up'])  # 反转→跌
                    acc = ok / len(subset) * 100
                    if acc >= 60:
                        name = f'涨>{chg_th}%'
                        if mkt_th is not None: name += f' + 大盘涨>{mkt_th}%'
                        if consec_th is not None: name += f' + 连涨>={consec_th}天'
                        if last_day_th is not None: name += f' + 尾日涨>{last_day_th}%'
                        marker = ' ★★★' if acc >= 70 else (' ★★' if acc >= 65 else ' ★')
                        print(f"  {name:55s}  N={len(subset):5d}  准确率={acc:5.1f}%{marker}")


def simulate_final_strategy(records):
    """模拟最终策略：只在高置信条件下预测，计算整体准确率"""

    print(f"\n{'='*70}")
    print(f"  最终策略模拟")
    print(f"{'='*70}")

    # 最终规则集（按优先级，互斥匹配）
    rules = [
        # 跌反转（高置信）
        ('跌>3%+大盘跌>1%→涨', lambda r: r['this_chg'] < -3 and r['market_chg'] < -1, True),
        ('跌>5%+连跌>=3天→涨', lambda r: r['this_chg'] < -5 and r['consec_down'] >= 3, True),
        ('跌>4%+尾日跌>2%→涨', lambda r: r['this_chg'] < -4 and r['last_day_chg'] < -2, True),
        ('跌>3%+连跌>=4天→涨', lambda r: r['this_chg'] < -3 and r['consec_down'] >= 4, True),
        ('跌>2%+大盘跌>1%→涨', lambda r: r['this_chg'] < -2 and r['market_chg'] < -1, True),
        ('跌>3%+尾日跌>1%→涨', lambda r: r['this_chg'] < -3 and r['last_day_chg'] < -1, True),
        ('跌>5%→涨', lambda r: r['this_chg'] < -5, True),
        ('跌>3%+连跌>=3天→涨', lambda r: r['this_chg'] < -3 and r['consec_down'] >= 3, True),
        # 涨反转（较高置信）
        ('涨>8%+连涨>=3天→跌', lambda r: r['this_chg'] > 8 and r['consec_up'] >= 3, False),
        ('涨>8%→跌', lambda r: r['this_chg'] > 8, False),
    ]

    covered = set()
    total_correct = 0
    total_count = 0

    for name, cond_fn, pred_up in rules:
        subset = [(i, r) for i, r in enumerate(records) if cond_fn(r) and i not in covered]
        if not subset:
            continue
        ok = sum(1 for i, r in subset if pred_up == r['next_up'])
        acc = ok / len(subset) * 100
        for i, _ in subset:
            covered.add(i)
        total_correct += ok
        total_count += len(subset)
        cum_acc = total_correct / total_count * 100
        marker = ' ★★★' if acc >= 70 else (' ★★' if acc >= 65 else (' ★' if acc >= 60 else ''))
        print(f"  {name:35s}  +{len(subset):5d}  准确率={acc:5.1f}%{marker}  累计={cum_acc:.1f}%")

    print(f"\n  最终结果:")
    print(f"  覆盖: {len(covered)}/{len(records)} ({len(covered)/len(records)*100:.1f}%)")
    print(f"  准确率: {total_correct}/{total_count} = {total_correct/total_count*100:.1f}%")
    print(f"  未覆盖: {len(records)-len(covered)} 只标记为'不确定'")


if __name__ == '__main__':
    records = load_records(n_weeks=29, sample_limit=2000)
    analyze_tiered_strategy(records)
    analyze_refined_conditions(records)
    simulate_final_strategy(records)
