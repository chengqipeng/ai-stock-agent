#!/usr/bin/env python3
"""
概念板块增强周预测 v4 — 100板块×20+个股 回测测试
=================================================
- 从数据库动态获取100个概念板块，每板块≥20只个股
- 20+周回测数据
- d4信号(前4天复合涨跌)预测 + 停牌检测
- 目标准确率 ≥ 85%
"""

import json
import logging
import socket
import sys
from collections import defaultdict

sys.path.insert(0, '.')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

START_DATE = '2025-08-01'
END_DATE = '2026-03-13'
TARGET_ACCURACY = 85.0
MIN_BOARDS = 100
MIN_STOCKS_PER_BOARD = 20
MIN_WEEKS = 20


# ═══════════════════════════════════════════════════════════
# 数据库查询：动态获取板块和个股
# ═══════════════════════════════════════════════════════════

def _check_db_available():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex(('106.14.194.144', 3306))
        sock.close()
        return result == 0
    except Exception:
        return False


def _add_suffix(code_6: str) -> str:
    if code_6.startswith(('0', '3')):
        return f'{code_6}.SZ'
    elif code_6.startswith('6'):
        return f'{code_6}.SH'
    return code_6


def fetch_boards_from_db(min_stocks=20, target_boards=100):
    """从数据库获取满足条件的概念板块及其个股。

    策略：从不同规模范围选取板块以保证多样性。
    """
    from dao import get_connection

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT board_code, board_name, COUNT(*) as cnt "
            "FROM stock_concept_board_stock "
            "GROUP BY board_code, board_name "
            "HAVING cnt >= %s "
            "ORDER BY cnt ASC",
            (min_stocks,),
        )
        boards = cur.fetchall()
        logger.info("满足≥%d只股票的板块: %d个", min_stocks, len(boards))

        # 按规模分层选取
        small = [b for b in boards if b['cnt'] <= 40]
        medium = [b for b in boards if 41 <= b['cnt'] <= 80]
        large = [b for b in boards if b['cnt'] > 80]

        logger.info("  20-40只: %d, 41-80只: %d, 81+只: %d",
                     len(small), len(medium), len(large))

        selected = []
        per_tier = target_boards // 3
        remainder = target_boards - per_tier * 3

        for tier, n_pick in [(small, per_tier),
                              (medium, per_tier),
                              (large, per_tier + remainder)]:
            if len(tier) <= n_pick:
                selected.extend(tier)
            else:
                step = len(tier) / n_pick
                for i in range(n_pick):
                    idx = int(i * step)
                    selected.append(tier[idx])

        selected_codes = {b['board_code'] for b in selected}
        if len(selected) < target_boards:
            for b in boards:
                if b['board_code'] not in selected_codes:
                    selected.append(b)
                    selected_codes.add(b['board_code'])
                    if len(selected) >= target_boards:
                        break

        logger.info("选取板块: %d个", len(selected))

        # 获取每个板块的个股
        board_stock_map = {}
        all_codes = set()
        board_codes = [b['board_code'] for b in selected]

        batch_size = 50
        for i in range(0, len(board_codes), batch_size):
            batch = board_codes[i:i + batch_size]
            ph = ','.join(['%s'] * len(batch))
            cur.execute(
                f"SELECT board_code, board_name, stock_code "
                f"FROM stock_concept_board_stock "
                f"WHERE board_code IN ({ph})",
                tuple(batch),
            )
            for r in cur.fetchall():
                bc = r['board_code']
                if bc not in board_stock_map:
                    board_stock_map[bc] = {'name': r['board_name'], 'stocks': []}
                full_code = _add_suffix(r['stock_code'])
                board_stock_map[bc]['stocks'].append(full_code)
                all_codes.add(full_code)

        valid_boards = {}
        for bc, info in board_stock_map.items():
            if len(info['stocks']) >= min_stocks:
                valid_boards[bc] = info

        all_stock_codes = sorted(all_codes)
        logger.info("有效板块: %d, 总股票: %d (去重)",
                     len(valid_boards), len(all_stock_codes))

        return valid_boards, all_stock_codes

    finally:
        cur.close()
        conn.close()


# ═══════════════════════════════════════════════════════════
# 输出函数
# ═══════════════════════════════════════════════════════════

def print_section(title, char='═', width=90):
    print(f"\n{char * width}")
    print(f"  {title}")
    print(f"{char * width}")


def print_subsection(title, char='─', width=90):
    print(f"\n  {char * 60}")
    print(f"  {title}")
    print(f"  {char * 60}")


def print_basic_info(summary):
    print_section("一、基本信息")
    print(f"  数据模式:           数据库（DB实盘数据）")
    print(f"  股票数:             {summary['stock_count']} 只")
    print(f"  概念板块数:         {summary['board_count']} 个")
    print(f"  每板块最少股票:     {summary['min_stocks_per_board']} 只")
    print(f"  回测周数:           {summary['week_count']} 周")
    print(f"  周预测样本数:       {summary['weekly_sample_count']} 条")
    print(f"  概念信号覆盖:       {summary['concept_signal_coverage']}%")
    print(f"  回测区间:           {summary['backtest_period']}")
    print(f"  耗时:               {summary['elapsed_seconds']}s")


def print_weekly_accuracy(weekly_result):
    print_section("二、周预测准确率")

    full = weekly_result.get('full_sample', {})
    lowo = weekly_result.get('lowo_cv', {})

    print_subsection("全样本评估")
    print(f"  准确率:  {full.get('accuracy', 0)}%  "
          f"({full.get('correct', 0)}/{full.get('total', 0)})")

    by_conf = full.get('by_confidence', {})
    for level in ['high', 'medium', 'low']:
        c = by_conf.get(level, {})
        print(f"  {level:8s}: {c.get('accuracy', 0):5.1f}%  "
              f"(n={c.get('count', 0)})")

    fz = full.get('fuzzy_zone', {})
    print(f"  模糊区:   {fz.get('accuracy', 0)}%  (n={fz.get('count', 0)})")

    # 策略分布
    sd = full.get('strategy_distribution', {})
    if sd:
        print(f"\n  策略分布:")
        for sname, cnt in sorted(sd.items(), key=lambda x: -x[1]):
            print(f"    {sname:<22s}: {cnt:>6d} 条样本")

    print_subsection("LOWO 交叉验证")
    print(f"  总体准确率:  {lowo.get('overall_accuracy', 0)}%  "
          f"({lowo.get('total_correct', 0)}/{lowo.get('total_count', 0)})")
    print(f"  平均周准确率: {lowo.get('avg_week_accuracy', 0)}%")
    print(f"  周数:         {lowo.get('n_weeks', 0)}")
    print(f"  最低周:       {lowo.get('min_week_accuracy', 0)}%")
    print(f"  最高周:       {lowo.get('max_week_accuracy', 0)}%")

    accs = lowo.get('week_accuracies', [])
    if accs:
        print(f"\n  各周准确率:")
        for i, a in enumerate(accs):
            bar = '█' * int(a / 2)
            print(f"    W{i+1:02d}: {a:5.1f}% {bar}")

    su = lowo.get('strategy_usage', {})
    if su:
        print(f"\n  LOWO策略使用统计:")
        for sname, cnt in sorted(su.items(), key=lambda x: -x[1]):
            print(f"    {sname:<22s}: {cnt:>6d} 次")


def print_board_analysis(board_results):
    print_section("三、按概念板块分析（全部板块准确率）")
    if not board_results:
        print("  无板块分析数据")
        return

    sorted_boards = sorted(board_results, key=lambda x: -x['accuracy'])

    print(f"  {'板块名称':<20s} {'准确率':>8s} {'正确/总数':>12s} {'股票数':>6s}")
    print(f"  {'─' * 56}")

    above_target = 0
    for b in sorted_boards:
        mark = '✓' if b['accuracy'] >= TARGET_ACCURACY else ' '
        print(f"  {mark} {b['board_name']:<18s} {b['accuracy']:>6.1f}%  "
              f"{b['correct']:>4d}/{b['total']:<4d}    {b['stock_count']:>4d}")
        if b['accuracy'] >= TARGET_ACCURACY:
            above_target += 1

    print(f"\n  达标板块(≥{TARGET_ACCURACY}%): {above_target}/{len(sorted_boards)} "
          f"({above_target/len(sorted_boards)*100:.0f}%)")

    accs = [b['accuracy'] for b in sorted_boards if b['total'] >= 5]
    if accs:
        from day_week_predicted.backtest.concept_strength_weekly_v4_backtest import _mean, _std
        print(f"  平均准确率: {_mean(accs):.1f}%  标准差: {_std(accs):.1f}%")


def print_per_stock_analysis(details):
    print_section("四、个股准确率分布")
    if not details:
        print("  无数据")
        return

    stock_stats = defaultdict(lambda: {'correct': 0, 'total': 0})
    for d in details:
        stock_stats[d['code']]['total'] += 1
        if d['correct']:
            stock_stats[d['code']]['correct'] += 1

    accs = []
    for code, st in stock_stats.items():
        if st['total'] >= 3:
            accs.append(st['correct'] / st['total'] * 100)

    if not accs:
        print("  样本不足")
        return

    bins = [(100, 100), (90, 99.9), (85, 89.9), (80, 84.9),
            (70, 79.9), (60, 69.9), (0, 59.9)]
    print(f"  {'准确率区间':<16s} {'股票数':>8s} {'占比':>8s}")
    print(f"  {'─' * 40}")
    for lo, hi in bins:
        cnt = sum(1 for a in accs if lo <= a <= hi)
        pct = cnt / len(accs) * 100 if accs else 0
        bar = '█' * int(pct / 2)
        print(f"  {lo:>3.0f}%-{hi:>5.1f}%  {cnt:>6d}  {pct:>5.1f}%  {bar}")

    from day_week_predicted.backtest.concept_strength_weekly_v4_backtest import _mean, _std
    print(f"\n  个股数: {len(accs)}, 平均: {_mean(accs):.1f}%, "
          f"中位数: {sorted(accs)[len(accs)//2]:.1f}%, "
          f"标准差: {_std(accs):.1f}%")


def print_error_analysis(details):
    print_section("五、错误分析")
    if not details:
        return

    bins = [(-999, -3), (-3, -1), (-1, -0.3), (-0.3, 0.3),
            (0.3, 1), (1, 3), (3, 999)]
    labels = ['<-3%', '-3~-1%', '-1~-0.3%', '±0.3%',
              '0.3~1%', '1~3%', '>3%']

    all_by_bin = defaultdict(int)
    err_by_bin = defaultdict(int)
    for d in details:
        for i, (lo, hi) in enumerate(bins):
            if lo <= d['d3_chg'] < hi:
                all_by_bin[labels[i]] += 1
                if not d['correct']:
                    err_by_bin[labels[i]] += 1
                break

    print(f"  {'前3天涨跌区间':<14s} {'错误数':>6s} {'总数':>6s} {'错误率':>8s}")
    print(f"  {'─' * 42}")
    for label in labels:
        total = all_by_bin[label]
        errs = err_by_bin[label]
        rate = errs / total * 100 if total > 0 else 0
        print(f"  {label:<14s} {errs:>6d} {total:>6d} {rate:>6.1f}%")

    # 按策略分析错误
    print()
    strat_err = defaultdict(int)
    strat_total = defaultdict(int)
    for d in details:
        s = d.get('strategy', 'unknown')
        strat_total[s] += 1
        if not d['correct']:
            strat_err[s] += 1

    print(f"  {'策略':<22s} {'错误数':>6s} {'总数':>8s} {'错误率':>8s}")
    print(f"  {'─' * 50}")
    for s in sorted(strat_total.keys()):
        t = strat_total[s]
        e = strat_err[s]
        rate = e / t * 100 if t > 0 else 0
        print(f"  {s:<22s} {e:>6d} {t:>8d} {rate:>6.1f}%")


def print_strategy_analysis(details):
    print_section("六、策略有效性分析")
    if not details:
        return

    # 按策略+置信度分析
    strat_conf = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    for d in details:
        s = d.get('strategy', 'unknown')
        c = d.get('confidence', 'low')
        strat_conf[s][c][1] += 1
        if d['correct']:
            strat_conf[s][c][0] += 1

    for s in sorted(strat_conf.keys()):
        total_ok = sum(v[0] for v in strat_conf[s].values())
        total_n = sum(v[1] for v in strat_conf[s].values())
        acc = total_ok / total_n * 100 if total_n > 0 else 0
        print(f"  {s}: 总准确率 {acc:.1f}% ({total_ok}/{total_n})")
        for c in ['high', 'medium', 'low']:
            ok, n = strat_conf[s][c]
            if n > 0:
                print(f"    {c:8s}: {ok/n*100:.1f}% ({ok}/{n})")


def print_sample_details(details, max_rows=60):
    print_section("七、预测样本明细（部分）")
    if not details:
        return

    errors = [d for d in details if not d['correct']]
    correct = [d for d in details if d['correct']]
    show = errors[:max_rows // 2] + correct[:max_rows // 2]
    show.sort(key=lambda x: (x['code'], str(x['iso_week'])))

    print(f"  {'代码':<12s} {'周':>8s} {'前3天':>7s} {'全周':>7s} "
          f"{'预测':>4s} {'实际':>4s} {'结果':>4s} {'策略':<16s} {'原因':<24s}")
    print(f"  {'─' * 95}")

    for d in show[:max_rows]:
        iw = d['iso_week']
        wk = f"{iw[0]}-W{iw[1]:02d}" if isinstance(iw, tuple) else str(iw)
        pred = '↑' if d['pred_up'] else '↓'
        actual = '↑' if d['actual_up'] else '↓'
        mark = '✓' if d['correct'] else '✗'
        strat = d.get('strategy', '')[:16]
        reason = d.get('reason', '')[:24]
        print(f"  {d['code']:<12s} {wk:>8s} {d['d3_chg']:>+6.2f}% "
              f"{d['weekly_change']:>+6.2f}% {pred:>4s} {actual:>4s} "
              f"{mark:>4s} {strat:<16s} {reason}")


def print_pass_criteria(summary, weekly_result):
    print_section("八、达标检查", char='═')

    w_full = weekly_result.get('full_sample', {})
    w_lowo = weekly_result.get('lowo_cv', {})

    # 高置信度准确率
    high_med_correct = 0
    high_med_total = 0
    for d in w_full.get('details', []):
        if d['confidence'] in ('high', 'medium'):
            high_med_total += 1
            if d['correct']:
                high_med_correct += 1
    high_med_acc = high_med_correct / high_med_total * 100 if high_med_total > 0 else 0

    checks = [
        (f'概念板块 ≥ {MIN_BOARDS}',
         summary['board_count'] >= MIN_BOARDS,
         f"{summary['board_count']} 个"),
        (f'每板块股票 ≥ {MIN_STOCKS_PER_BOARD}',
         summary['min_stocks_per_board'] >= MIN_STOCKS_PER_BOARD,
         f"最少 {summary['min_stocks_per_board']} 只"),
        (f'回测周数 ≥ {MIN_WEEKS}',
         summary['week_count'] >= MIN_WEEKS,
         f"{summary['week_count']} 周"),
        (f'周预测全样本准确率 ≥ {TARGET_ACCURACY}%',
         w_full.get('accuracy', 0) >= TARGET_ACCURACY,
         f"{w_full.get('accuracy', 0)}%"),
        (f'周预测LOWO准确率 ≥ {TARGET_ACCURACY}%',
         w_lowo.get('overall_accuracy', 0) >= TARGET_ACCURACY,
         f"{w_lowo.get('overall_accuracy', 0)}%"),
        (f'高置信度(high+medium)准确率 ≥ 85%',
         high_med_acc >= 85.0,
         f"{high_med_acc:.1f}% ({high_med_total}条)"),
    ]

    all_pass = True
    for name, passed, value in checks:
        mark = '✓ 通过' if passed else '✗ 未通过'
        print(f"  {mark}  {name}  →  {value}")
        if not passed:
            all_pass = False

    print()
    if all_pass:
        print("  ══════════════════════════════════════")
        print("  ★★★  全部达标  ★★★")
        print("  ══════════════════════════════════════")
    else:
        print("  ⚠️  部分指标未达标，需要优化")

    return all_pass


# ═══════════════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════════════

def main():
    from day_week_predicted.backtest.concept_strength_weekly_v4_backtest import (
        run_v4_backtest,
    )

    print("=" * 90)
    print("  概念板块增强 周预测 回测 v4 — 详细测试报告")
    print(f"  {MIN_BOARDS}个概念板块 × {MIN_STOCKS_PER_BOARD}+个股/板块 × "
          f"{MIN_WEEKS}+周 — d4信号+停牌检测")
    print("=" * 90)

    if not _check_db_available():
        print("\n  ✗ 数据库不可达，v4必须使用DB实盘数据")
        return

    print(f"\n  [1] 从数据库获取板块和个股...")
    board_stock_map, all_stock_codes = fetch_boards_from_db(
        min_stocks=MIN_STOCKS_PER_BOARD, target_boards=MIN_BOARDS)

    if len(board_stock_map) < MIN_BOARDS:
        print(f"\n  ✗ 有效板块不足: {len(board_stock_map)} < {MIN_BOARDS}")
        return

    board_sizes = [len(info['stocks']) for info in board_stock_map.values()]
    print(f"  板块数: {len(board_stock_map)}")
    print(f"  总股票: {len(all_stock_codes)} (去重)")
    print(f"  每板块股票: {min(board_sizes)}~{max(board_sizes)}, "
          f"平均{sum(board_sizes)/len(board_sizes):.0f}")
    print(f"  回测区间: {START_DATE} ~ {END_DATE}")
    print()

    print(f"  [2] 运行v4回测...")
    result = run_v4_backtest(
        stock_codes=all_stock_codes,
        start_date=START_DATE,
        end_date=END_DATE,
        board_stock_map=board_stock_map,
    )

    if 'error' in result:
        print(f"\n  回测失败: {result['error']}")
        return

    summary = result['summary']
    weekly_result = result['weekly']
    w_details = weekly_result['full_sample'].get('details', [])

    # 输出报告
    print_basic_info(summary)
    print_weekly_accuracy(weekly_result)
    print_board_analysis(weekly_result.get('by_concept_board', []))
    print_per_stock_analysis(w_details)
    print_error_analysis(w_details)
    print_strategy_analysis(w_details)
    print_sample_details(w_details, max_rows=60)
    all_pass = print_pass_criteria(summary, weekly_result)

    # 保存结果
    output_path = 'data_results/backtest_concept_strength_v4_result.json'
    ser_details = []
    for d in w_details[:500]:
        sd = dict(d)
        sd['iso_week'] = (f"{d['iso_week'][0]}-W{d['iso_week'][1]:02d}"
                          if isinstance(d['iso_week'], tuple)
                          else str(d['iso_week']))
        ser_details.append(sd)

    save_result = {
        'summary': summary,
        'weekly': {
            'full_sample': {k: v for k, v in weekly_result['full_sample'].items()
                           if k != 'details'},
            'lowo_cv': weekly_result.get('lowo_cv', {}),
            'by_concept_board': weekly_result.get('by_concept_board', []),
        },
        'sample_details': ser_details,
        'pass_criteria': {
            'board_count_ok': summary['board_count'] >= MIN_BOARDS,
            'min_stocks_ok': summary['min_stocks_per_board'] >= MIN_STOCKS_PER_BOARD,
            'week_count_ok': summary['week_count'] >= MIN_WEEKS,
            'weekly_full_ok': weekly_result['full_sample'].get('accuracy', 0) >= TARGET_ACCURACY,
            'weekly_lowo_ok': weekly_result.get('lowo_cv', {}).get(
                'overall_accuracy', 0) >= TARGET_ACCURACY,
            'all_pass': all_pass,
        },
    }
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(save_result, f, ensure_ascii=False, indent=2)
    print(f"\n  详细结果已保存到: {output_path}")
    print()


if __name__ == '__main__':
    main()
