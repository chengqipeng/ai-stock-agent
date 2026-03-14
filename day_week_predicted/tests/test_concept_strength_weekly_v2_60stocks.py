#!/usr/bin/env python3
"""
概念板块增强 周预测+5日预测 回测测试 v2 — 60只股票，15个概念板块

在v1基础上新增5日滚动预测评估：
- 周预测：策略C（前3天方向）+ 概念板块信号
- 5日预测：给定任意交易日，预测未来5个交易日累计涨跌方向

目标：周预测准确率 ≥ 80%，5日预测准确率 ≥ 80%
"""
import json
import sys
import os
import logging
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from day_week_predicted.backtest.concept_strength_weekly_v2_backtest import (
    run_v2_backtest,
    run_v2_backtest_simulated,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

STOCK_NAMES = {
    '002230.SZ': '科大讯飞', '300496.SZ': '中科创达', '688111.SH': '金山办公', '300474.SZ': '景嘉微',
    '002594.SZ': '比亚迪',   '601238.SH': '广汽集团', '600733.SH': '北汽蓝谷', '002074.SZ': '国轩高科',
    '002371.SZ': '北方华创', '603986.SH': '兆易创新', '688012.SH': '中微公司', '002049.SZ': '紫光国微',
    '300750.SZ': '宁德时代', '002709.SZ': '天赐材料', '300014.SZ': '亿纬锂能', '002460.SZ': '赣锋锂业',
    '601012.SH': '隆基绿能', '300763.SZ': '锦浪科技', '688599.SH': '天合光能', '002129.SZ': 'TCL中环',
    '600276.SH': '恒瑞医药', '300760.SZ': '迈瑞医疗', '603259.SH': '药明康德', '600436.SH': '片仔癀',
    '600519.SH': '贵州茅台', '000858.SZ': '五粮液',   '000568.SZ': '泸州老窖', '002304.SZ': '洋河股份',
    '600893.SH': '航发动力', '600760.SH': '中航沈飞', '002179.SZ': '中航光电', '600862.SH': '中航高科',
    '300274.SZ': '阳光电源', '002812.SZ': '恩捷股份', '300037.SZ': '新宙邦',   '688390.SH': '固德威',
    '300124.SZ': '汇川技术', '688169.SH': '石头科技', '002747.SZ': '埃斯顿',   '300024.SZ': '机器人',
    '002475.SZ': '立讯精密', '600584.SH': '长电科技', '002241.SZ': '歌尔股份', '002938.SZ': '鹏鼎控股',
    '600111.SH': '北方稀土', '300748.SZ': '金力永磁', '600366.SH': '宁波韵升', '002600.SZ': '领益智造',
    '002648.SZ': '卫星化学', '300438.SZ': '鹏辉能源', '600309.SH': '万华化学', '002601.SZ': '龙蟒佰利',
    '603019.SH': '中科曙光', '000977.SZ': '浪潮信息', '002236.SZ': '大华股份', '300308.SZ': '中际旭创',
    '601799.SH': '星宇股份', '603596.SH': '伯特利',   '002920.SZ': '德赛西威', '603786.SH': '科博达',
}

CONCEPT_BOARD_STOCKS = {
    '人工智能':   ['002230.SZ', '300496.SZ', '688111.SH', '300474.SZ'],
    '新能源汽车': ['002594.SZ', '601238.SH', '600733.SH', '002074.SZ'],
    '半导体':     ['002371.SZ', '603986.SH', '688012.SH', '002049.SZ'],
    '锂电池':     ['300750.SZ', '002709.SZ', '300014.SZ', '002460.SZ'],
    '光伏':       ['601012.SH', '300763.SZ', '688599.SH', '002129.SZ'],
    '医药生物':   ['600276.SH', '300760.SZ', '603259.SH', '600436.SH'],
    '白酒':       ['600519.SH', '000858.SZ', '000568.SZ', '002304.SZ'],
    '军工':       ['600893.SH', '600760.SH', '002179.SZ', '600862.SH'],
    '储能':       ['300274.SZ', '002812.SZ', '300037.SZ', '688390.SH'],
    '机器人':     ['300124.SZ', '688169.SH', '002747.SZ', '300024.SZ'],
    '消费电子':   ['002475.SZ', '600584.SH', '002241.SZ', '002938.SZ'],
    '稀土永磁':   ['600111.SH', '300748.SZ', '600366.SH', '002600.SZ'],
    '化工新材料': ['002648.SZ', '300438.SZ', '600309.SH', '002601.SZ'],
    '数据中心':   ['603019.SH', '000977.SZ', '002236.SZ', '300308.SZ'],
    '汽车零部件': ['601799.SH', '603596.SH', '002920.SZ', '603786.SH'],
}


def _check_db_available():
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex(('106.14.194.144', 3306))
        sock.close()
        return result == 0
    except Exception:
        return False


def build_stock_list():
    all_codes = []
    code_to_board = {}
    seen = set()
    for board_name, codes in CONCEPT_BOARD_STOCKS.items():
        for code in codes:
            if code not in seen:
                all_codes.append(code)
                code_to_board[code] = board_name
                seen.add(code)
    return all_codes, code_to_board


def _sn(code):
    return STOCK_NAMES.get(code, code)


def print_section(title, char='═', width=90):
    print(f"\n{char * width}")
    print(f"  {title}")
    print(f"{char * width}")


def print_subsection(title, char='─', width=90):
    print(f"\n  {char * 60}")
    print(f"  {title}")
    print(f"  {char * 60}")


# ═══════════════════════════════════════════════════════════
# 输出函数
# ═══════════════════════════════════════════════════════════

def print_basic_info(summary, data_mode):
    print_section("一、基本信息")
    print(f"  数据模式:           {data_mode}")
    print(f"  股票数:             {summary['stock_count']} 只")
    print(f"  概念板块数:         15 个")
    print(f"  回测周数:           {summary['week_count']} 周")
    print(f"  周预测样本数:       {summary['weekly_sample_count']} 条")
    print(f"  5日预测样本数:      {summary['fiveday_sample_count']} 条")
    print(f"  5日评估日期数:      {summary['fiveday_eval_dates']} 个")
    print(f"  概念信号覆盖(周):   {summary['concept_signal_coverage_weekly']}%")
    print(f"  概念信号覆盖(5日):  {summary['concept_signal_coverage_5day']}%")
    print(f"  回测区间:           {summary['backtest_period']}")
    print(f"  耗时:               {summary['elapsed_seconds']} 秒")


def print_weekly_accuracy(weekly_result):
    """输出周预测准确率。"""
    print_section("二、周预测准确率")
    full = weekly_result['full_sample']
    lowo = weekly_result['lowo_cv']

    print_subsection("2.1 全样本准确率")
    print(f"  ★ 总体准确率:   {full['accuracy']}%  ({full['correct']}/{full['total']})")

    print_subsection("2.2 按置信度分层")
    print(f"  {'置信度':<10} {'准确率':>8} {'样本数':>8}")
    print(f"  {'-'*30}")
    for conf in ['high', 'medium', 'low']:
        stats = full['by_confidence'][conf]
        print(f"  {conf:<10} {stats['accuracy']:>7.1f}% {stats['count']:>7}")

    print_subsection("2.3 模糊区分析 (|前3天涨跌| ≤ 0.8%)")
    fz = full['fuzzy_zone']
    print(f"  模糊区准确率:   {fz['accuracy']}%  (样本 {fz['count']})")

    print_subsection("2.4 LOWO 交叉验证")
    if lowo:
        print(f"  ★ 总体准确率:   {lowo['overall_accuracy']}%  "
              f"({lowo['total_correct']}/{lowo['total_count']})")
        print(f"  平均周准确率:   {lowo['avg_week_accuracy']}%")
        print(f"  最低周准确率:   {lowo['min_week_accuracy']}%")
        print(f"  最高周准确率:   {lowo['max_week_accuracy']}%")
        print(f"  验证周数:       {lowo['n_weeks']}")
        if lowo.get('week_accuracies'):
            print(f"  各周准确率:     {lowo['week_accuracies']}")
            above_80 = sum(1 for a in lowo['week_accuracies'] if a >= 80)
            print(f"  ≥80% 的周数:    {above_80}/{lowo['n_weeks']}")


def print_5day_accuracy(fiveday_result):
    """输出5日预测准确率。"""
    print_section("三、5日滚动预测准确率")
    full = fiveday_result['full_sample']
    cv = fiveday_result['cv']

    print_subsection("3.1 全样本准确率")
    print(f"  ★ 总体准确率:   {full['accuracy']}%  ({full['correct']}/{full['total']})")

    print_subsection("3.2 按置信度分层")
    print(f"  {'置信度':<10} {'准确率':>8} {'样本数':>8}")
    print(f"  {'-'*30}")
    for conf in ['high', 'medium', 'low']:
        stats = full['by_confidence'][conf]
        print(f"  {conf:<10} {stats['accuracy']:>7.1f}% {stats['count']:>7}")

    print_subsection("3.3 模糊区分析 (|近3日涨跌| ≤ 1.0%)")
    fz = full['fuzzy_zone']
    print(f"  模糊区准确率:   {fz['accuracy']}%  (样本 {fz['count']})")

    print_subsection("3.4 时间折叠交叉验证")
    if cv:
        print(f"  ★ 总体准确率:   {cv['overall_accuracy']}%  "
              f"({cv['total_correct']}/{cv['total_count']})")
        print(f"  平均折准确率:   {cv['avg_fold_accuracy']}%")
        print(f"  最低折准确率:   {cv['min_fold_accuracy']}%")
        print(f"  最高折准确率:   {cv['max_fold_accuracy']}%")
        print(f"  折数:           {cv['n_folds']}")
        if cv.get('fold_accuracies'):
            print(f"  各折准确率:     {cv['fold_accuracies']}")
            above_80 = sum(1 for a in cv['fold_accuracies'] if a >= 80)
            print(f"  ≥80% 的折数:    {above_80}/{cv['n_folds']}")


def print_board_analysis(weekly_boards, fiveday_boards):
    """输出按概念板块分析。"""
    print_section("四、按概念板块分析")

    print_subsection("4.1 周预测 — 按板块")
    print(f"  {'板块':<12} {'准确率':>8} {'正确':>6} {'总数':>6} {'评级':>6}")
    print(f"  {'-'*42}")
    for b in sorted(weekly_boards, key=lambda x: -x['accuracy']):
        grade = '优秀' if b['accuracy'] >= 85 else ('良好' if b['accuracy'] >= 80 else (
            '合格' if b['accuracy'] >= 75 else '待优化'))
        print(f"  {b['board_name']:<12} {b['accuracy']:>7.1f}% {b['correct']:>5} "
              f"{b['total']:>5} {grade:>6}")
    if weekly_boards:
        avg = sum(b['accuracy'] for b in weekly_boards) / len(weekly_boards)
        above_80 = sum(1 for b in weekly_boards if b['accuracy'] >= 80)
        print(f"\n  板块平均准确率: {avg:.1f}%  ≥80%板块: {above_80}/{len(weekly_boards)}")

    print_subsection("4.2 5日预测 — 按板块")
    print(f"  {'板块':<12} {'准确率':>8} {'正确':>6} {'总数':>6} {'评级':>6}")
    print(f"  {'-'*42}")
    for b in sorted(fiveday_boards, key=lambda x: -x['accuracy']):
        grade = '优秀' if b['accuracy'] >= 85 else ('良好' if b['accuracy'] >= 80 else (
            '合格' if b['accuracy'] >= 75 else '待优化'))
        print(f"  {b['board_name']:<12} {b['accuracy']:>7.1f}% {b['correct']:>5} "
              f"{b['total']:>5} {grade:>6}")
    if fiveday_boards:
        avg = sum(b['accuracy'] for b in fiveday_boards) / len(fiveday_boards)
        above_80 = sum(1 for b in fiveday_boards if b['accuracy'] >= 80)
        print(f"\n  板块平均准确率: {avg:.1f}%  ≥80%板块: {above_80}/{len(fiveday_boards)}")


def print_per_stock_analysis(weekly_details, fiveday_details, concept_board_map):
    """输出个股维度分析。"""
    print_section("五、个股维度分析")

    # 周预测按股票统计
    print_subsection("5.1 周预测 — 个股准确率")
    w_stats = defaultdict(lambda: {'correct': 0, 'total': 0})
    for d in weekly_details:
        w_stats[d['code']]['total'] += 1
        if d['correct']:
            w_stats[d['code']]['correct'] += 1

    print(f"  {'代码':<12} {'名称':<8} {'板块':<10} {'准确率':>7} {'正确/总':>8}")
    print(f"  {'-'*50}")
    for code in sorted(w_stats.keys(), key=lambda c: -w_stats[c]['correct'] / max(w_stats[c]['total'], 1)):
        s = w_stats[code]
        acc = s['correct'] / s['total'] * 100 if s['total'] > 0 else 0
        board = concept_board_map.get(code, '?')
        print(f"  {code:<12} {_sn(code):<8} {board:<10} {acc:>6.1f}% {s['correct']:>3}/{s['total']:<3}")

    # 5日预测按股票统计
    print_subsection("5.2 5日预测 — 个股准确率")
    f_stats = defaultdict(lambda: {'correct': 0, 'total': 0})
    for d in fiveday_details:
        f_stats[d['code']]['total'] += 1
        if d['correct']:
            f_stats[d['code']]['correct'] += 1

    print(f"  {'代码':<12} {'名称':<8} {'板块':<10} {'准确率':>7} {'正确/总':>8}")
    print(f"  {'-'*50}")
    for code in sorted(f_stats.keys(), key=lambda c: -f_stats[c]['correct'] / max(f_stats[c]['total'], 1)):
        s = f_stats[code]
        acc = s['correct'] / s['total'] * 100 if s['total'] > 0 else 0
        board = concept_board_map.get(code, '?')
        print(f"  {code:<12} {_sn(code):<8} {board:<10} {acc:>6.1f}% {s['correct']:>3}/{s['total']:<3}")


def print_weekly_detail_table(weekly_details, concept_board_map):
    """输出逐周预测明细表。"""
    print_section("六、逐周预测明细")

    week_groups = defaultdict(list)
    for d in weekly_details:
        week_groups[d['iso_week']].append(d)

    for iso_week in sorted(week_groups.keys()):
        records = week_groups[iso_week]
        correct = sum(1 for r in records if r['correct'])
        total = len(records)
        acc = correct / total * 100 if total > 0 else 0

        print_subsection(f"第 {iso_week[0]}-W{iso_week[1]:02d} 周  "
                         f"准确率: {acc:.1f}% ({correct}/{total})")
        print(f"  {'代码':<12} {'名称':<8} {'板块':<10} {'前3天':>7} {'全周':>7} "
              f"{'预测':>4} {'实际':>4} {'结果':>4} {'置信':>6}")
        print(f"  {'-'*80}")

        for r in sorted(records, key=lambda x: (not x['correct'], x['code'])):
            board = concept_board_map.get(r['code'], '?')
            pred_dir = '涨' if r['pred_up'] else '跌'
            actual_dir = '涨' if r['actual_up'] else '跌'
            mark = '✓' if r['correct'] else '✗'
            print(f"  {r['code']:<12} {_sn(r['code']):<8} {board:<10} "
                  f"{r['d3_chg']:>+6.2f}% {r['weekly_change']:>+6.2f}% "
                  f"{pred_dir:>4} {actual_dir:>4} {mark:>4} {r['confidence']:>6}")


def print_5day_sample_table(fiveday_details, concept_board_map, max_rows=100):
    """输出5日预测样本明细（截取前max_rows条）。"""
    print_section("七、5日预测样本明细（部分）")

    shown = fiveday_details[:max_rows]
    print(f"  显示前 {len(shown)}/{len(fiveday_details)} 条")
    print(f"  {'代码':<12} {'名称':<8} {'板块':<10} {'评估日':>12} {'近3日':>7} "
          f"{'未来5日':>8} {'预测':>4} {'实际':>4} {'结果':>4} {'置信':>6}")
    print(f"  {'-'*95}")

    for r in shown:
        board = concept_board_map.get(r['code'], '?')
        pred_dir = '涨' if r['pred_up'] else '跌'
        actual_dir = '涨' if r['actual_up'] else '跌'
        mark = '✓' if r['correct'] else '✗'
        print(f"  {r['code']:<12} {_sn(r['code']):<8} {board:<10} "
              f"{r['eval_date']:>12} {r['recent_3d_chg']:>+6.2f}% "
              f"{r['future_5d_chg']:>+7.2f}% "
              f"{pred_dir:>4} {actual_dir:>4} {mark:>4} {r['confidence']:>6}")


def print_error_analysis(weekly_details, fiveday_details):
    """输出错误分析。"""
    print_section("八、预测错误分析")

    # 周预测错误
    w_errors = [d for d in weekly_details if not d['correct']]
    print_subsection("8.1 周预测错误分布")
    print(f"  错误总数: {len(w_errors)}/{len(weekly_details)} "
          f"({len(w_errors) / len(weekly_details) * 100:.1f}%)")
    for conf in ['high', 'medium', 'low']:
        ce = [e for e in w_errors if e['confidence'] == conf]
        ct = sum(1 for d in weekly_details if d['confidence'] == conf)
        rate = len(ce) / ct * 100 if ct > 0 else 0
        print(f"  {conf:<8} 错误: {len(ce):>3}/{ct:<3} (错误率 {rate:.1f}%)")

    # 5日预测错误
    f_errors = [d for d in fiveday_details if not d['correct']]
    print_subsection("8.2 5日预测错误分布")
    print(f"  错误总数: {len(f_errors)}/{len(fiveday_details)} "
          f"({len(f_errors) / len(fiveday_details) * 100:.1f}%)")
    for conf in ['high', 'medium', 'low']:
        ce = [e for e in f_errors if e['confidence'] == conf]
        ct = sum(1 for d in fiveday_details if d['confidence'] == conf)
        rate = len(ce) / ct * 100 if ct > 0 else 0
        print(f"  {conf:<8} 错误: {len(ce):>3}/{ct:<3} (错误率 {rate:.1f}%)")


def print_signal_effectiveness(weekly_details, fiveday_details):
    """输出信号有效性分析。"""
    print_section("九、信号有效性分析")

    for label, details in [('周预测', weekly_details), ('5日预测', fiveday_details)]:
        print_subsection(f"9.x {label} — 按理由分类")
        reason_stats = defaultdict(lambda: {'correct': 0, 'total': 0})
        for d in details:
            reason = d['reason']
            if '强信号' in reason:
                key = '强信号区'
            elif '中等信号' in reason:
                key = '中等信号区'
            elif '反转' in reason or '反弹' in reason:
                key = '反转/反弹修正'
            elif '模糊区综合' in reason:
                key = '模糊区综合'
            elif '极模糊' in reason:
                key = '极端模糊'
            elif '无概念' in reason:
                key = '无概念信号'
            elif '延续' in reason:
                key = '趋势延续'
            else:
                key = '其他'
            reason_stats[key]['total'] += 1
            if d['correct']:
                reason_stats[key]['correct'] += 1

        print(f"  {'理由分类':<20} {'准确率':>8} {'正确':>6} {'总数':>6}")
        print(f"  {'-'*44}")
        for key in sorted(reason_stats.keys(), key=lambda k: -reason_stats[k]['total']):
            rs = reason_stats[key]
            acc = rs['correct'] / rs['total'] * 100 if rs['total'] > 0 else 0
            print(f"  {key:<20} {acc:>7.1f}% {rs['correct']:>5} {rs['total']:>5}")


def print_weekly_trend_chart(lowo):
    """输出各周准确率趋势图。"""
    print_section("十、周预测准确率趋势")
    if not lowo or not lowo.get('week_accuracies'):
        print("  无数据")
        return
    accs = lowo['week_accuracies']
    print(f"  {'周序号':>6} {'准确率':>8} {'柱状图'}")
    print(f"  {'-'*60}")
    for i, acc in enumerate(accs, 1):
        bar = '█' * int(acc / 2)
        mark = '✓' if acc >= 80 else '✗'
        print(f"  W{i:>4} {acc:>7.1f}% {mark} |{bar}")
    ref_bar = '·' * 40
    print(f"  {'80%线':>6} {'80.0%':>8}   |{ref_bar}")


def print_5day_cv_chart(cv):
    """输出5日预测交叉验证趋势图。"""
    print_section("十一、5日预测交叉验证趋势")
    if not cv or not cv.get('fold_accuracies'):
        print("  无数据")
        return
    accs = cv['fold_accuracies']
    print(f"  {'折序号':>6} {'准确率':>8} {'柱状图'}")
    print(f"  {'-'*60}")
    for i, acc in enumerate(accs, 1):
        bar = '█' * int(acc / 2)
        mark = '✓' if acc >= 80 else '✗'
        print(f"  F{i:>4} {acc:>7.1f}% {mark} |{bar}")
    ref_bar = '·' * 40
    print(f"  {'80%线':>6} {'80.0%':>8}   |{ref_bar}")


def print_pass_criteria(summary, weekly_result, fiveday_result):
    """输出达标检查。"""
    print_section("十二、达标检查", char='═')

    w_full = weekly_result.get('full_sample', {})
    w_lowo = weekly_result.get('lowo_cv', {})
    f_full = fiveday_result.get('full_sample', {})
    f_cv = fiveday_result.get('cv', {})

    target = 80.0
    checks = [
        ('周数 ≥ 12', summary['week_count'] >= 12,
         f"{summary['week_count']} 周"),
        ('股票 ≥ 60', summary['stock_count'] >= 60,
         f"{summary['stock_count']} 只"),
        ('每板块 ≥ 4只', True, '15 × 4 = 60'),
        ('周预测全样本准确率 ≥ 80%',
         w_full.get('accuracy', 0) >= target,
         f"{w_full.get('accuracy', 0)}%"),
        ('周预测LOWO准确率 ≥ 80%',
         w_lowo.get('overall_accuracy', 0) >= target,
         f"{w_lowo.get('overall_accuracy', 0)}%"),
        ('5日预测全样本准确率 ≥ 80%',
         f_full.get('accuracy', 0) >= target,
         f"{f_full.get('accuracy', 0)}%"),
        ('5日预测CV准确率 ≥ 80%',
         f_cv.get('overall_accuracy', 0) >= target,
         f"{f_cv.get('overall_accuracy', 0)}%"),
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
    stock_codes, concept_board_map = build_stock_list()

    print("=" * 90)
    print("  概念板块增强 周预测+5日预测 回测 v2 — 详细测试报告")
    print("  60只股票 × 15个概念板块 × 12+周")
    print("=" * 90)
    print(f"  股票总数: {len(stock_codes)} (去重后)")
    print(f"  概念板块: {len(CONCEPT_BOARD_STOCKS)}个, 每板块 4 只")
    print(f"  回测区间: 2025-12-01 ~ 2026-03-10 (约14周)")
    print()

    db_available = _check_db_available()
    data_mode = '数据库（DB实盘数据）' if db_available else '模拟数据（DB不可达）'

    if db_available:
        print(f"  数据模式: {data_mode}")
        result = run_v2_backtest(
            stock_codes=stock_codes,
            start_date='2025-12-01',
            end_date='2026-03-10',
            concept_board_map=concept_board_map,
        )
    else:
        print(f"  数据模式: {data_mode}")
        result = run_v2_backtest_simulated(
            stock_codes=stock_codes,
            concept_board_stocks=CONCEPT_BOARD_STOCKS,
            start_date='2025-12-01',
            end_date='2026-03-10',
            concept_board_map=concept_board_map,
        )

    if 'error' in result:
        print(f"\n回测失败: {result['error']}")
        return

    summary = result['summary']
    weekly_result = result['weekly']
    fiveday_result = result['fiveday']

    w_details = weekly_result['full_sample'].get('details', [])
    f_details = fiveday_result['full_sample'].get('details', [])

    # 输出报告
    print_basic_info(summary, data_mode)
    print_weekly_accuracy(weekly_result)
    print_5day_accuracy(fiveday_result)
    print_board_analysis(
        weekly_result.get('by_concept_board', []),
        fiveday_result.get('by_concept_board', []))
    print_per_stock_analysis(w_details, f_details, concept_board_map)
    print_weekly_detail_table(w_details, concept_board_map)
    print_5day_sample_table(f_details, concept_board_map, max_rows=80)
    print_error_analysis(w_details, f_details)
    print_signal_effectiveness(w_details, f_details)
    print_weekly_trend_chart(weekly_result.get('lowo_cv'))
    print_5day_cv_chart(fiveday_result.get('cv'))
    all_pass = print_pass_criteria(summary, weekly_result, fiveday_result)

    # 保存结果
    output_path = 'data_results/backtest_concept_strength_v2_result.json'
    ser_w_details = []
    for d in w_details:
        sd = dict(d)
        sd['iso_week'] = f"{d['iso_week'][0]}-W{d['iso_week'][1]:02d}"
        ser_w_details.append(sd)

    save_result = {
        'summary': summary,
        'weekly': {
            'full_sample': {k: v for k, v in weekly_result['full_sample'].items()
                           if k != 'details'},
            'lowo_cv': weekly_result.get('lowo_cv', {}),
            'by_concept_board': weekly_result.get('by_concept_board', []),
        },
        'fiveday': {
            'full_sample': {k: v for k, v in fiveday_result['full_sample'].items()
                           if k != 'details'},
            'cv': fiveday_result.get('cv', {}),
            'by_concept_board': fiveday_result.get('by_concept_board', []),
        },
        'weekly_details': ser_w_details,
        'fiveday_details_sample': f_details[:100],
        'pass_criteria': {
            'week_count_ok': summary['week_count'] >= 12,
            'stock_count_ok': summary['stock_count'] >= 60,
            'weekly_full_ok': weekly_result['full_sample'].get('accuracy', 0) >= 80,
            'weekly_lowo_ok': weekly_result.get('lowo_cv', {}).get(
                'overall_accuracy', 0) >= 80,
            'fiveday_full_ok': fiveday_result['full_sample'].get('accuracy', 0) >= 80,
            'fiveday_cv_ok': fiveday_result.get('cv', {}).get(
                'overall_accuracy', 0) >= 80,
            'all_pass': all_pass,
        },
    }
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(save_result, f, ensure_ascii=False, indent=2)
    print(f"\n  详细结果已保存到: {output_path}")
    print()


if __name__ == '__main__':
    main()
