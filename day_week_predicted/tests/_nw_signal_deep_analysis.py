#!/usr/bin/env python3
"""
下周预测信号深度分析
===================
目标：找出哪些条件组合下，下周方向预测准确率能达到70%+

分析维度：
1. 本周涨跌幅幅度分桶（反转信号强度）
2. 本周连续涨/跌天数
3. 板块动量方向与个股方向一致性
4. 大盘本周方向
5. 周五（尾日）涨跌幅
6. 个股波动率分桶
7. 多信号组合
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


def run_analysis(n_weeks=29, sample_limit=1000):
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

    # 加载数据
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

    # 大盘
    cur.execute(
        "SELECT `date`, change_percent FROM stock_kline "
        "WHERE stock_code = '000001.SH' AND `date` >= %s AND `date` <= %s ORDER BY `date`",
        (start_date, latest_date))
    market_klines = [{'date': r['date'], 'change_percent': _to_float(r['change_percent'])} for r in cur.fetchall()]

    # 板块映射
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
    all_board_codes = list(all_board_codes)

    board_klines = defaultdict(list)
    for i in range(0, len(all_board_codes), batch_size):
        batch = all_board_codes[i:i + batch_size]
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

    # 大盘按周分组
    market_by_week = defaultdict(list)
    for k in market_klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        iw = dt.isocalendar()[:2]
        market_by_week[iw].append(k)

    # 板块按周分组
    board_by_week = defaultdict(lambda: defaultdict(list))
    for bc, kls in board_klines.items():
        for k in kls:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            board_by_week[bc][iw].append(k)

    logger.info("数据加载完成: %d只股票, %d板块", len(stock_klines), len(board_klines))

    # ── 逐股票逐周分析 ──
    records = []  # 每条记录: {this_week_chg, next_week_up, ...features}

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

        # 计算个股波动率（全区间）
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

            # 连续涨/跌天数
            consec_up = 0
            consec_down = 0
            for p in reversed(this_pcts):
                if p > 0:
                    consec_up += 1
                    if consec_down > 0:
                        break
                elif p < 0:
                    consec_down += 1
                    if consec_up > 0:
                        break
                else:
                    break

            # 大盘本周
            mw = market_by_week.get(iw_this, [])
            market_chg = _compound_return([k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]) if len(mw) >= 3 else 0

            # 板块动量
            boards = stock_boards.get(code, [])
            board_avg_chg = 0
            board_agree = 0  # 板块方向与个股方向一致的比例
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

            # 个股与大盘方向一致
            stock_market_agree = (this_chg >= 0) == (market_chg >= 0)

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
                'stock_market_agree': stock_market_agree,
                'avg_vol': avg_vol,
                'this_up': this_chg >= 0,
            })

    logger.info("总记录数: %d", len(records))

    # ═══════════════════════════════════════════════════════════
    # 分析各维度
    # ═══════════════════════════════════════════════════════════

    def analyze_bucket(name, key_fn):
        """按 key_fn 分桶，统计每桶的下周反转准确率"""
        buckets = defaultdict(lambda: [0, 0])  # [correct, total]
        for r in records:
            bucket = key_fn(r)
            if bucket is None:
                continue
            # 反转预测：本周涨→下周跌，本周跌→下周涨
            pred_next_up = not r['this_up']
            correct = pred_next_up == r['next_up']
            buckets[bucket][1] += 1
            if correct:
                buckets[bucket][0] += 1

        print(f"\n{'='*60}")
        print(f"  {name} (反转策略)")
        print(f"{'='*60}")
        for bucket in sorted(buckets.keys()):
            ok, total = buckets[bucket]
            acc = ok / total * 100 if total > 0 else 0
            marker = ' ★★★' if acc >= 70 else (' ★★' if acc >= 60 else (' ★' if acc >= 55 else ''))
            print(f"  {str(bucket):30s}  {ok:5d}/{total:5d}  ({acc:5.1f}%){marker}")

    def analyze_bucket_follow(name, key_fn):
        """按 key_fn 分桶，统计每桶的下周跟随准确率"""
        buckets = defaultdict(lambda: [0, 0])
        for r in records:
            bucket = key_fn(r)
            if bucket is None:
                continue
            # 跟随预测：本周涨→下周涨，本周跌→下周跌
            pred_next_up = r['this_up']
            correct = pred_next_up == r['next_up']
            buckets[bucket][1] += 1
            if correct:
                buckets[bucket][0] += 1

        print(f"\n{'='*60}")
        print(f"  {name} (跟随策略)")
        print(f"{'='*60}")
        for bucket in sorted(buckets.keys()):
            ok, total = buckets[bucket]
            acc = ok / total * 100 if total > 0 else 0
            marker = ' ★★★' if acc >= 70 else (' ★★' if acc >= 60 else (' ★' if acc >= 55 else ''))
            print(f"  {str(bucket):30s}  {ok:5d}/{total:5d}  ({acc:5.1f}%){marker}")

    # 1. 本周涨跌幅幅度
    def chg_bucket(r):
        a = r['abs_this_chg']
        if a < 1: return '|chg|<1%'
        if a < 2: return '|chg|1-2%'
        if a < 3: return '|chg|2-3%'
        if a < 5: return '|chg|3-5%'
        if a < 8: return '|chg|5-8%'
        return '|chg|>=8%'
    analyze_bucket("1. 本周涨跌幅幅度", chg_bucket)

    # 2. 尾日涨跌幅
    def last_day_bucket(r):
        a = r['abs_last_day']
        if a < 0.5: return '|尾日|<0.5%'
        if a < 1: return '|尾日|0.5-1%'
        if a < 2: return '|尾日|1-2%'
        if a < 3: return '|尾日|2-3%'
        return '|尾日|>=3%'
    analyze_bucket("2. 尾日涨跌幅", last_day_bucket)

    # 3. 连续涨/跌天数
    def consec_bucket(r):
        if r['consec_up'] >= 4: return '连涨>=4天'
        if r['consec_up'] == 3: return '连涨3天'
        if r['consec_down'] >= 4: return '连跌>=4天'
        if r['consec_down'] == 3: return '连跌3天'
        return '其他'
    analyze_bucket("3. 连续涨/跌天数", consec_bucket)

    # 4. 大盘方向
    def market_bucket(r):
        if r['market_chg'] > 1: return '大盘涨>1%'
        if r['market_chg'] > 0: return '大盘微涨0-1%'
        if r['market_chg'] > -1: return '大盘微跌0-1%'
        return '大盘跌>1%'
    analyze_bucket("4. 大盘方向", market_bucket)

    # 5. 板块一致性
    def board_agree_bucket(r):
        if r['board_agree'] == 0: return '无板块数据'
        if r['board_agree'] >= 0.8: return '板块高度一致>=80%'
        if r['board_agree'] >= 0.6: return '板块较一致60-80%'
        return '板块分歧<60%'
    analyze_bucket("5. 板块一致性", board_agree_bucket)

    # 6. 个股波动率
    def vol_bucket(r):
        v = r['avg_vol']
        if v < 1: return '低波动<1%'
        if v < 2: return '中波动1-2%'
        if v < 3: return '中高波动2-3%'
        return '高波动>=3%'
    analyze_bucket("6. 个股波动率", vol_bucket)

    # 7. 跟随策略 - 板块动量
    def board_momentum_bucket(r):
        bm = r['board_avg_chg']
        if bm > 2: return '板块强涨>2%'
        if bm > 0.5: return '板块涨0.5-2%'
        if bm > -0.5: return '板块平-0.5~0.5%'
        if bm > -2: return '板块跌-2~-0.5%'
        return '板块强跌<-2%'
    analyze_bucket_follow("7. 板块动量(跟随)", board_momentum_bucket)

    # 8. 组合信号：本周大涨(>3%) + 尾日涨
    print(f"\n{'='*60}")
    print(f"  8. 组合信号分析")
    print(f"{'='*60}")

    combos = {
        '本周涨>3% + 尾日涨>1%': lambda r: r['this_chg'] > 3 and r['last_day_chg'] > 1,
        '本周涨>3% + 尾日跌': lambda r: r['this_chg'] > 3 and r['last_day_chg'] < 0,
        '本周跌>3% + 尾日跌>1%': lambda r: r['this_chg'] < -3 and r['last_day_chg'] < -1,
        '本周跌>3% + 尾日涨': lambda r: r['this_chg'] < -3 and r['last_day_chg'] > 0,
        '本周涨>5%': lambda r: r['this_chg'] > 5,
        '本周跌>5%': lambda r: r['this_chg'] < -5,
        '本周涨>3% + 连涨>=3天': lambda r: r['this_chg'] > 3 and r['consec_up'] >= 3,
        '本周跌>3% + 连跌>=3天': lambda r: r['this_chg'] < -3 and r['consec_down'] >= 3,
        '本周涨>2% + 板块一致>=80%': lambda r: r['this_chg'] > 2 and r['board_agree'] >= 0.8,
        '本周跌>2% + 板块一致>=80%': lambda r: r['this_chg'] < -2 and r['board_agree'] >= 0.8,
        '本周涨>3% + 大盘涨>1%': lambda r: r['this_chg'] > 3 and r['market_chg'] > 1,
        '本周跌>3% + 大盘跌>1%': lambda r: r['this_chg'] < -3 and r['market_chg'] < -1,
        '低波动 + 本周|chg|>2%': lambda r: r['avg_vol'] < 1.5 and r['abs_this_chg'] > 2,
        '高波动 + 本周|chg|>5%': lambda r: r['avg_vol'] > 2.5 and r['abs_this_chg'] > 5,
        '本周涨>2% + 尾日涨>1% + 连涨>=3': lambda r: r['this_chg'] > 2 and r['last_day_chg'] > 1 and r['consec_up'] >= 3,
        '本周跌>2% + 尾日跌>1% + 连跌>=3': lambda r: r['this_chg'] < -2 and r['last_day_chg'] < -1 and r['consec_down'] >= 3,
        '停牌(本周chg=0)': lambda r: r['abs_this_chg'] < 0.01,
    }

    for name, cond_fn in combos.items():
        subset = [r for r in records if cond_fn(r)]
        if not subset:
            print(f"  {name:45s}  无样本")
            continue
        # 反转预测
        rev_ok = sum(1 for r in subset if (not r['this_up']) == r['next_up'])
        rev_acc = rev_ok / len(subset) * 100
        # 跟随预测
        fol_ok = sum(1 for r in subset if r['this_up'] == r['next_up'])
        fol_acc = fol_ok / len(subset) * 100
        best = max(rev_acc, fol_acc)
        strategy = '反转' if rev_acc >= fol_acc else '跟随'
        marker = ' ★★★' if best >= 70 else (' ★★' if best >= 60 else (' ★' if best >= 55 else ''))
        print(f"  {name:45s}  N={len(subset):5d}  反转={rev_acc:5.1f}%  跟随={fol_acc:5.1f}%  最优={strategy}({best:.1f}%){marker}")

    # 9. 停牌股分析
    print(f"\n{'='*60}")
    print(f"  9. 停牌/微幅股分析")
    print(f"{'='*60}")
    for threshold in [0.01, 0.1, 0.3, 0.5]:
        subset = [r for r in records if r['abs_this_chg'] < threshold]
        if not subset:
            continue
        up_ok = sum(1 for r in subset if r['next_up'])
        up_acc = up_ok / len(subset) * 100
        print(f"  |本周chg|<{threshold}%: N={len(subset):5d}  下周涨={up_acc:.1f}%  下周跌={100-up_acc:.1f}%")

    # 10. 综合：如果只预测高置信场景，整体准确率能到多少？
    print(f"\n{'='*60}")
    print(f"  10. 高置信场景筛选（目标>=70%）")
    print(f"{'='*60}")

    # 定义高置信条件
    high_conf_conditions = [
        ('停牌(chg<0.01%)', lambda r: r['abs_this_chg'] < 0.01, 'up'),  # 停牌→涨
        ('本周涨>5% → 反转', lambda r: r['this_chg'] > 5, 'reverse'),
        ('本周跌>5% → 反转', lambda r: r['this_chg'] < -5, 'reverse'),
        ('本周涨>3% + 连涨>=3天 → 反转', lambda r: r['this_chg'] > 3 and r['consec_up'] >= 3, 'reverse'),
        ('本周跌>3% + 连跌>=3天 → 反转', lambda r: r['this_chg'] < -3 and r['consec_down'] >= 3, 'reverse'),
        ('本周涨>3% + 尾日涨>1% → 反转', lambda r: r['this_chg'] > 3 and r['last_day_chg'] > 1, 'reverse'),
        ('本周跌>3% + 尾日跌>1% → 反转', lambda r: r['this_chg'] < -3 and r['last_day_chg'] < -1, 'reverse'),
    ]

    total_correct = 0
    total_count = 0
    covered = set()

    for name, cond_fn, strategy in high_conf_conditions:
        subset = [(i, r) for i, r in enumerate(records) if cond_fn(r) and i not in covered]
        if not subset:
            print(f"  {name:45s}  无新增样本")
            continue

        ok = 0
        for i, r in subset:
            if strategy == 'up':
                pred_up = True
            elif strategy == 'reverse':
                pred_up = not r['this_up']
            else:
                pred_up = r['this_up']
            if pred_up == r['next_up']:
                ok += 1
            covered.add(i)

        acc = ok / len(subset) * 100
        total_correct += ok
        total_count += len(subset)
        cum_acc = total_correct / total_count * 100 if total_count > 0 else 0
        print(f"  {name:45s}  +{len(subset):5d}  准确率={acc:5.1f}%  累计={total_correct}/{total_count}({cum_acc:.1f}%)")

    # 剩余的用基线策略
    remaining = [(i, r) for i, r in enumerate(records) if i not in covered]
    if remaining:
        # 基线：反转
        rem_ok = sum(1 for i, r in remaining if (not r['this_up']) == r['next_up'])
        rem_acc = rem_ok / len(remaining) * 100
        print(f"  {'剩余(基线反转)':45s}  {len(remaining):5d}  准确率={rem_acc:5.1f}%")
        total_correct += rem_ok
        total_count += len(remaining)
        final_acc = total_correct / total_count * 100
        print(f"\n  最终整体准确率: {total_correct}/{total_count} = {final_acc:.1f}%")
        print(f"  高置信覆盖率: {len(covered)}/{len(records)} = {len(covered)/len(records)*100:.1f}%")


if __name__ == '__main__':
    run_analysis(n_weeks=29, sample_limit=1000)
