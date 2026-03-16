"""验证规则引擎下周预测的回测准确率。

要求：从至少200个不同概念板块中各选1只个股，确保样本多样性。
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import logging
import random
from collections import defaultdict
from datetime import datetime, timedelta
from dao import get_connection

logger = logging.getLogger(__name__)

def _to_float(v) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0

def _compound_return(pcts):
    r = 1.0
    for p in pcts:
        r *= (1 + p / 100)
    return (r - 1) * 100


def select_stocks_from_boards(min_boards=200):
    """从至少 min_boards 个不同概念板块中各选1只个股。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 获取所有概念板块及其成分股数量（至少5只成分股的板块）
    cur.execute(
        "SELECT board_code, board_name, COUNT(*) as cnt "
        "FROM stock_concept_board_stock "
        "GROUP BY board_code, board_name "
        "HAVING cnt >= 5 "
        "ORDER BY cnt DESC"
    )
    boards = cur.fetchall()
    logger.info("共 %d 个概念板块（成分股>=5）", len(boards))

    if len(boards) < min_boards:
        logger.warning("板块数量 %d < %d，使用全部板块", len(boards), min_boards)

    # 获取最新交易日，用于筛选有K线数据的股票
    cur.execute("SELECT MAX(`date`) as d FROM stock_kline WHERE stock_code = '000001.SH'")
    latest_date = cur.fetchone()['d']

    # 获取有K线数据的股票集合
    cur.execute(
        "SELECT DISTINCT stock_code FROM stock_kline WHERE `date` = %s "
        "AND (stock_code LIKE '6%%.SH' OR stock_code LIKE '0%%.SZ' OR stock_code LIKE '3%%.SZ') "
        "AND stock_code NOT LIKE '399%%' AND stock_code != '000001.SH'",
        (latest_date,))
    valid_codes = set(r['stock_code'] for r in cur.fetchall())

    # 从每个板块中选1只股票（随机选，避免偏差）
    random.seed(42)
    selected = set()
    board_count = 0
    board_stock_map = {}  # board_code -> selected stock

    for board in boards:
        bc = board['board_code']
        cur.execute(
            "SELECT stock_code FROM stock_concept_board_stock WHERE board_code = %s",
            (bc,))
        members = [r['stock_code'] for r in cur.fetchall()]

        # 转换为完整代码并筛选有效的
        full_codes = []
        for m in members:
            if m.startswith('6'):
                fc = f'{m}.SH'
            elif m.startswith('0') or m.startswith('3'):
                fc = f'{m}.SZ'
            else:
                continue
            if fc in valid_codes and fc not in selected:
                full_codes.append(fc)

        if not full_codes:
            continue

        pick = random.choice(full_codes)
        selected.add(pick)
        board_stock_map[bc] = (board['board_name'], pick)
        board_count += 1

    conn.close()

    logger.info("从 %d 个板块中选出 %d 只不同个股", board_count, len(selected))
    return list(selected), board_count, latest_date


def main():
    from service.weekly_prediction_service import (
        _nw_extract_features, _nw_match_rule, _NW_RULES
    )

    # 1. 从至少200个不同概念板块中选股
    sample_codes, board_count, latest_date = select_stocks_from_boards(min_boards=200)

    if board_count < 200:
        print(f"[警告] 只有 {board_count} 个板块，不足200个")
        return

    # 2. 加载K线数据
    n_weeks = 29
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(n_weeks + 2) * 7)
    start_date = dt_start.strftime('%Y-%m-%d')

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    stock_klines = defaultdict(list)
    batch_size = 200
    for i in range(0, len(sample_codes), batch_size):
        batch = sample_codes[i:i + batch_size]
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
    market_klines = [{'date': r['date'], 'change_percent': _to_float(r['change_percent'])}
                     for r in cur.fetchall()]
    conn.close()

    market_by_week = defaultdict(list)
    for k in market_klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        iw = dt.isocalendar()[:2]
        market_by_week[iw].append(k)

    # 3. 回测
    total_correct = 0
    total_predicted = 0
    total_all = 0
    stocks_with_data = 0
    rule_stats = defaultdict(lambda: {'correct': 0, 'total': 0})

    for code in sample_codes:
        klines = stock_klines.get(code, [])
        if len(klines) < 20:
            continue
        stocks_with_data += 1

        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            wg[iw].append(k)

        sorted_weeks = sorted(wg.keys())

        for idx in range(len(sorted_weeks) - 1):
            iw_this = sorted_weeks[idx]
            iw_next = sorted_weeks[idx + 1]

            this_days = sorted(wg[iw_this], key=lambda x: x['date'])
            next_days = sorted(wg[iw_next], key=lambda x: x['date'])

            if len(this_days) < 3 or len(next_days) < 3:
                continue

            this_pcts = [d['change_percent'] for d in this_days]
            next_pcts = [d['change_percent'] for d in next_days]
            next_chg = _compound_return(next_pcts)
            actual_next_up = next_chg >= 0

            mw = market_by_week.get(iw_this, [])
            market_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            total_all += 1

            feat = _nw_extract_features(this_pcts, market_chg)
            rule = _nw_match_rule(feat)

            if rule is None:
                continue

            pred_up = rule['pred_up']
            correct = pred_up == actual_next_up
            total_predicted += 1
            if correct:
                total_correct += 1

            rule_stats[rule['name']]['total'] += 1
            if correct:
                rule_stats[rule['name']]['correct'] += 1

    coverage = total_predicted / total_all * 100 if total_all > 0 else 0
    accuracy = total_correct / total_predicted * 100 if total_predicted > 0 else 0

    print(f"\n{'='*70}")
    print(f"  规则引擎下周预测回测验证（多板块多样性）")
    print(f"{'='*70}")
    print(f"  概念板块数: {board_count}")
    print(f"  选股数: {len(sample_codes)} (有K线数据: {stocks_with_data})")
    print(f"  回测周数: {n_weeks}")
    print(f"  总周-股样本: {total_all}")
    print(f"  命中规则样本: {total_predicted} (覆盖率: {coverage:.1f}%)")
    print(f"  正确: {total_correct}")
    print(f"  准确率: {accuracy:.1f}%")
    print(f"\n  各规则统计:")
    for name, stats in sorted(rule_stats.items(), key=lambda x: -x[1]['total']):
        acc = stats['correct'] / stats['total'] * 100 if stats['total'] > 0 else 0
        marker = ' ★★★' if acc >= 70 else (' ★★' if acc >= 65 else (' ★' if acc >= 60 else ''))
        print(f"    {name:35s}  N={stats['total']:5d}  准确率={acc:5.1f}%{marker}")
    print(f"{'='*70}")


if __name__ == '__main__':
    main()
