#!/usr/bin/env python3
"""
概念板块强弱势增强周预测回测测试 — 60只股票，覆盖15个概念板块

要求：
- 至少60只不同概念板块的个股
- 每个概念板块个股不少于4个
- 模拟至少12周数据
- 目标：周预测准确率 ≥ 80%

概念板块选择（15个板块，每板块4只）：
1. 人工智能 (4只)    2. 新能源汽车 (4只)   3. 半导体 (4只)
4. 锂电池 (4只)      5. 光伏 (4只)         6. 医药生物 (4只)
7. 白酒 (4只)        8. 军工 (4只)         9. 储能 (4只)
10. 机器人 (4只)     11. 消费电子 (4只)    12. 稀土永磁 (4只)
13. 化工新材料 (4只) 14. 数据中心 (4只)    15. 汽车零部件 (4只)

回测区间：2025-12-01 ~ 2026-03-10（约14周）
"""
import json
import sys
import os
import logging
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from day_week_predicted.backtest.concept_strength_weekly_backtest import (
    run_concept_strength_backtest,
    run_concept_strength_backtest_simulated,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# 股票名称映射（用于详细输出）
# ═══════════════════════════════════════════════════════════

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


def _check_db_available():
    """检查DB是否可达。"""
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex(('106.14.194.144', 3306))
        sock.close()
        return result == 0
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════
# 60只股票，覆盖15个概念板块，每板块4只
# ═══════════════════════════════════════════════════════════

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


def build_stock_list():
    """构建去重的股票列表和概念板块映射。"""
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
    """获取股票简称。"""
    return STOCK_NAMES.get(code, code)


# ═══════════════════════════════════════════════════════════
# 详细输出函数
# ═══════════════════════════════════════════════════════════

def print_section(title, char='═', width=90):
    print(f"\n{char * width}")
    print(f"  {title}")
    print(f"{char * width}")


def print_subsection(title, char='─', width=90):
    print(f"\n  {char * 60}")
    print(f"  {title}")
    print(f"  {char * 60}")


def print_basic_info(summary, full, data_mode):
    """输出基本信息。"""
    print_section("一、基本信息")
    print(f"  数据模式:       {data_mode}")
    print(f"  股票数:         {summary['stock_count']} 只")
    print(f"  概念板块数:     15 个")
    print(f"  回测周数:       {summary['week_count']} 周")
    print(f"  周样本总数:     {summary['weekly_sample_count']} 条")
    print(f"  概念信号覆盖率: {summary['concept_signal_coverage']}%")
    print(f"  回测区间:       {summary['backtest_period']}")
    print(f"  耗时:           {summary['elapsed_seconds']} 秒")


def print_accuracy_overview(full, lowo):
    """输出准确率总览。"""
    print_section("二、准确率总览")

    print_subsection("2.1 全样本准确率")
    print(f"  ★ 总体准确率:   {full['accuracy']}%  ({full['correct']}/{full['total']})")
    print(f"  有概念信号:     {full['with_concept_signal']['accuracy']}%  "
          f"(样本 {full['with_concept_signal']['count']})")
    print(f"  无概念信号:     {full['without_concept_signal']['accuracy']}%  "
          f"(样本 {full['without_concept_signal']['count']})")
    sig_lift = full['with_concept_signal']['accuracy'] - full['without_concept_signal']['accuracy']
    print(f"  概念信号提升:   {sig_lift:+.1f}%")

    print_subsection("2.2 按置信度分层")
    print(f"  {'置信度':<10} {'准确率':>8} {'样本数':>8} {'占比':>8}")
    print(f"  {'-'*38}")
    for conf in ['high', 'medium', 'low']:
        stats = full['by_confidence'][conf]
        pct = stats['count'] / full['total'] * 100 if full['total'] > 0 else 0
        print(f"  {conf:<10} {stats['accuracy']:>7.1f}% {stats['count']:>7} {pct:>7.1f}%")

    print_subsection("2.3 模糊区分析 (|前3天涨跌| ≤ 0.8%)")
    fz = full['fuzzy_zone']
    non_fuzzy_correct = full['correct'] - (fz['count'] * fz['accuracy'] / 100 if fz['count'] > 0 else 0)
    non_fuzzy_total = full['total'] - fz['count']
    non_fuzzy_acc = non_fuzzy_correct / non_fuzzy_total * 100 if non_fuzzy_total > 0 else 0
    print(f"  模糊区准确率:   {fz['accuracy']}%  (样本 {fz['count']})")
    print(f"  非模糊区准确率: {non_fuzzy_acc:.1f}%  (样本 {non_fuzzy_total})")
    print(f"  模糊区占比:     {fz['count'] / full['total'] * 100:.1f}%")

    print_subsection("2.4 LOWO 交叉验证（Leave-One-Week-Out，无数据泄露）")
    print(f"  ★ 总体准确率:   {lowo['overall_accuracy']}%  "
          f"({lowo['total_correct']}/{lowo['total_count']})")
    print(f"  平均周准确率:   {lowo['avg_week_accuracy']}%")
    print(f"  最低周准确率:   {lowo['min_week_accuracy']}%")
    print(f"  最高周准确率:   {lowo['max_week_accuracy']}%")
    print(f"  验证周数:       {lowo['n_weeks']}")
    if lowo['week_accuracies']:
        print(f"  各周准确率:     {lowo['week_accuracies']}")
        # 准确率分布
        above_80 = sum(1 for a in lowo['week_accuracies'] if a >= 80)
        above_70 = sum(1 for a in lowo['week_accuracies'] if a >= 70)
        print(f"  ≥80% 的周数:    {above_80}/{lowo['n_weeks']}")
        print(f"  ≥70% 的周数:    {above_70}/{lowo['n_weeks']}")


def print_board_analysis(boards):
    """输出按概念板块的详细分析。"""
    print_section("三、按概念板块分析")
    print(f"  {'板块':<12} {'准确率':>8} {'正确':>6} {'总数':>6} {'股票数':>6} {'评级':>6}")
    print(f"  {'-'*50}")

    for b in sorted(boards, key=lambda x: -x['accuracy']):
        acc = b['accuracy']
        if acc >= 85:
            grade = '优秀'
        elif acc >= 80:
            grade = '良好'
        elif acc >= 75:
            grade = '合格'
        else:
            grade = '待优化'
        print(f"  {b['board_name']:<12} {acc:>7.1f}% {b['correct']:>5} {b['total']:>5} "
              f"{b['stock_count']:>5} {grade:>6}")

    # 统计
    avg_acc = sum(b['accuracy'] for b in boards) / len(boards) if boards else 0
    above_80 = sum(1 for b in boards if b['accuracy'] >= 80)
    above_75 = sum(1 for b in boards if b['accuracy'] >= 75)
    print(f"\n  板块平均准确率: {avg_acc:.1f}%")
    print(f"  ≥80% 的板块:   {above_80}/{len(boards)}")
    print(f"  ≥75% 的板块:   {above_75}/{len(boards)}")


def print_per_stock_analysis(details, concept_board_map):
    """输出每只股票的详细准确率分析。"""
    print_section("四、个股维度详细分析")

    # 按股票分组统计
    stock_stats = defaultdict(lambda: {
        'correct': 0, 'total': 0,
        'high_ok': 0, 'high_n': 0,
        'medium_ok': 0, 'medium_n': 0,
        'low_ok': 0, 'low_n': 0,
        'fuzzy_ok': 0, 'fuzzy_n': 0,
        'weeks': [],
    })

    for d in details:
        code = d['code']
        s = stock_stats[code]
        s['total'] += 1
        if d['correct']:
            s['correct'] += 1
        s[f"{d['confidence']}_n"] += 1
        if d['correct']:
            s[f"{d['confidence']}_ok"] += 1
        if abs(d['d3_chg']) <= 0.8:
            s['fuzzy_n'] += 1
            if d['correct']:
                s['fuzzy_ok'] += 1
        s['weeks'].append(d)

    # 按板块分组输出
    board_stocks = defaultdict(list)
    for code, stats in stock_stats.items():
        board = concept_board_map.get(code, '未分类')
        board_stocks[board].append((code, stats))

    for board_name in CONCEPT_BOARD_STOCKS.keys():
        stocks = board_stocks.get(board_name, [])
        if not stocks:
            continue

        print_subsection(f"板块: {board_name}")
        print(f"  {'代码':<12} {'名称':<8} {'准确率':>7} {'正确/总':>8} "
              f"{'高置信':>8} {'中置信':>8} {'低置信':>8} {'模糊区':>8}")
        print(f"  {'-'*76}")

        for code, s in sorted(stocks, key=lambda x: -x[1]['correct'] / max(x[1]['total'], 1)):
            acc = s['correct'] / s['total'] * 100 if s['total'] > 0 else 0
            h_acc = f"{s['high_ok']}/{s['high_n']}" if s['high_n'] > 0 else '-'
            m_acc = f"{s['medium_ok']}/{s['medium_n']}" if s['medium_n'] > 0 else '-'
            l_acc = f"{s['low_ok']}/{s['low_n']}" if s['low_n'] > 0 else '-'
            f_acc = f"{s['fuzzy_ok']}/{s['fuzzy_n']}" if s['fuzzy_n'] > 0 else '-'
            print(f"  {code:<12} {_sn(code):<8} {acc:>6.1f}% {s['correct']:>3}/{s['total']:<3} "
                  f"{h_acc:>8} {m_acc:>8} {l_acc:>8} {f_acc:>8}")


def print_weekly_detail_table(details, concept_board_map):
    """输出逐周预测明细表（按周分组）。"""
    print_section("五、逐周预测明细")

    # 按周分组
    week_groups = defaultdict(list)
    for d in details:
        week_groups[d['iso_week']].append(d)

    for iso_week in sorted(week_groups.keys()):
        records = week_groups[iso_week]
        correct = sum(1 for r in records if r['correct'])
        total = len(records)
        acc = correct / total * 100 if total > 0 else 0

        print_subsection(f"第 {iso_week[0]}-W{iso_week[1]:02d} 周  "
                         f"准确率: {acc:.1f}% ({correct}/{total})")

        print(f"  {'代码':<12} {'名称':<8} {'板块':<10} {'前3天':>7} {'全周':>7} "
              f"{'预测':>4} {'实际':>4} {'结果':>4} {'置信':>6} {'预测理由'}")
        print(f"  {'-'*110}")

        for r in sorted(records, key=lambda x: (not x['correct'], x['code'])):
            board = concept_board_map.get(r['code'], '?')
            pred_dir = '涨' if r['pred_up'] else '跌'
            actual_dir = '涨' if r['actual_up'] else '跌'
            result_mark = '✓' if r['correct'] else '✗'
            reason_short = r['reason'][:40]
            print(f"  {r['code']:<12} {_sn(r['code']):<8} {board:<10} "
                  f"{r['d3_chg']:>+6.2f}% {r['weekly_change']:>+6.2f}% "
                  f"{pred_dir:>4} {actual_dir:>4} {result_mark:>4} "
                  f"{r['confidence']:>6} {reason_short}")


def print_error_analysis(details, concept_board_map):
    """输出预测错误案例分析。"""
    print_section("六、预测错误案例分析")

    errors = [d for d in details if not d['correct']]
    print(f"  错误总数: {len(errors)}/{len(details)} "
          f"({len(errors) / len(details) * 100:.1f}%)")

    # 按置信度分组错误
    print_subsection("6.1 错误按置信度分布")
    for conf in ['high', 'medium', 'low']:
        conf_errors = [e for e in errors if e['confidence'] == conf]
        conf_total = sum(1 for d in details if d['confidence'] == conf)
        err_rate = len(conf_errors) / conf_total * 100 if conf_total > 0 else 0
        print(f"  {conf:<8} 错误: {len(conf_errors):>3}/{conf_total:<3} (错误率 {err_rate:.1f}%)")

    # 按板块分组错误
    print_subsection("6.2 错误按概念板块分布")
    board_errors = defaultdict(lambda: {'errors': 0, 'total': 0})
    for d in details:
        board = concept_board_map.get(d['code'], '未分类')
        board_errors[board]['total'] += 1
        if not d['correct']:
            board_errors[board]['errors'] += 1

    print(f"  {'板块':<12} {'错误数':>6} {'总数':>6} {'错误率':>8}")
    print(f"  {'-'*36}")
    for board in sorted(board_errors.keys(), key=lambda b: -board_errors[b]['errors']):
        be = board_errors[board]
        err_rate = be['errors'] / be['total'] * 100 if be['total'] > 0 else 0
        print(f"  {board:<12} {be['errors']:>5} {be['total']:>5} {err_rate:>7.1f}%")

    # 模糊区错误 vs 非模糊区错误
    print_subsection("6.3 模糊区 vs 非模糊区错误")
    fuzzy_errors = [e for e in errors if abs(e['d3_chg']) <= 0.8]
    non_fuzzy_errors = [e for e in errors if abs(e['d3_chg']) > 0.8]
    fuzzy_total = sum(1 for d in details if abs(d['d3_chg']) <= 0.8)
    non_fuzzy_total = len(details) - fuzzy_total
    print(f"  模糊区错误:   {len(fuzzy_errors):>3}/{fuzzy_total:<3} "
          f"(错误率 {len(fuzzy_errors) / fuzzy_total * 100:.1f}%)" if fuzzy_total > 0 else
          f"  模糊区错误:   无样本")
    print(f"  非模糊区错误: {len(non_fuzzy_errors):>3}/{non_fuzzy_total:<3} "
          f"(错误率 {len(non_fuzzy_errors) / non_fuzzy_total * 100:.1f}%)" if non_fuzzy_total > 0 else
          f"  非模糊区错误: 无样本")

    # 高置信度错误明细（最值得关注）
    high_errors = [e for e in errors if e['confidence'] == 'high']
    if high_errors:
        print_subsection("6.4 高置信度错误明细（重点关注）")
        print(f"  {'代码':<12} {'名称':<8} {'板块':<10} {'前3天':>7} {'全周':>7} "
              f"{'预测':>4} {'实际':>4} {'理由'}")
        print(f"  {'-'*90}")
        for e in high_errors:
            board = concept_board_map.get(e['code'], '?')
            pred_dir = '涨' if e['pred_up'] else '跌'
            actual_dir = '涨' if e['actual_up'] else '跌'
            print(f"  {e['code']:<12} {_sn(e['code']):<8} {board:<10} "
                  f"{e['d3_chg']:>+6.2f}% {e['weekly_change']:>+6.2f}% "
                  f"{pred_dir:>4} {actual_dir:>4} {e['reason'][:45]}")
    else:
        print("\n  无高置信度错误")


def print_signal_effectiveness(details):
    """输出概念板块信号有效性分析。"""
    print_section("七、概念板块信号有效性分析")

    # 概念信号修正效果
    print_subsection("7.1 概念信号修正效果")

    # 分析概念信号在不同区域的修正效果
    # 强信号区被概念反转的案例
    concept_flip_cases = [d for d in details if '反转' in d['reason'] or '反弹' in d['reason']]
    concept_flip_correct = sum(1 for d in concept_flip_cases if d['correct'])
    print(f"  概念信号反转/反弹修正: {len(concept_flip_cases)} 次")
    if concept_flip_cases:
        print(f"  修正准确率: {concept_flip_correct / len(concept_flip_cases) * 100:.1f}% "
              f"({concept_flip_correct}/{len(concept_flip_cases)})")

    # 模糊区概念信号主导
    fuzzy_concept = [d for d in details if '模糊区' in d['reason'] and '概念' in d['reason']]
    fuzzy_concept_correct = sum(1 for d in fuzzy_concept if d['correct'])
    print(f"  模糊区概念信号主导: {len(fuzzy_concept)} 次")
    if fuzzy_concept:
        print(f"  主导准确率: {fuzzy_concept_correct / len(fuzzy_concept) * 100:.1f}% "
              f"({fuzzy_concept_correct}/{len(fuzzy_concept)})")

    # 无概念信号的预测
    no_concept = [d for d in details if '无概念' in d['reason']]
    no_concept_correct = sum(1 for d in no_concept if d['correct'])
    print(f"  无概念信号预测: {len(no_concept)} 次")
    if no_concept:
        print(f"  准确率: {no_concept_correct / len(no_concept) * 100:.1f}% "
              f"({no_concept_correct}/{len(no_concept)})")

    # 按预测理由分类统计
    print_subsection("7.2 按预测理由分类统计")
    reason_stats = defaultdict(lambda: {'correct': 0, 'total': 0})
    for d in details:
        # 提取理由关键词
        reason = d['reason']
        if '强信号' in reason:
            key = '强信号区(跟随前3天)'
        elif '中等信号' in reason:
            key = '中等信号区(跟随前3天)'
        elif '反转' in reason or '反弹' in reason:
            key = '概念信号反转/反弹修正'
        elif '多信号看' in reason:
            key = '多信号综合修正'
        elif '模糊区综合' in reason:
            key = '模糊区综合判断'
        elif '模糊区' in reason and '概念' in reason:
            key = '模糊区概念信号主导'
        elif '极模糊' in reason:
            key = '极端模糊(共识度兜底)'
        elif '模糊区兜底' in reason:
            key = '模糊区兜底(前3天方向)'
        elif '无概念' in reason:
            key = '无概念信号(纯方向)'
        else:
            key = '其他'

        reason_stats[key]['total'] += 1
        if d['correct']:
            reason_stats[key]['correct'] += 1

    print(f"  {'预测理由分类':<28} {'准确率':>8} {'正确':>6} {'总数':>6}")
    print(f"  {'-'*52}")
    for key in sorted(reason_stats.keys(), key=lambda k: -reason_stats[k]['total']):
        rs = reason_stats[key]
        acc = rs['correct'] / rs['total'] * 100 if rs['total'] > 0 else 0
        print(f"  {key:<28} {acc:>7.1f}% {rs['correct']:>5} {rs['total']:>5}")


def print_d3_chg_distribution(details):
    """输出前3天涨跌幅分布与准确率关系。"""
    print_section("八、前3天涨跌幅分布与准确率")

    # 按前3天涨跌幅区间分组
    bins = [
        ('-5%以下', lambda x: x < -5),
        ('-5%~-3%', lambda x: -5 <= x < -3),
        ('-3%~-2%', lambda x: -3 <= x < -2),
        ('-2%~-1%', lambda x: -2 <= x < -1),
        ('-1%~-0.5%', lambda x: -1 <= x < -0.5),
        ('-0.5%~0%', lambda x: -0.5 <= x < 0),
        ('0%~0.5%', lambda x: 0 <= x < 0.5),
        ('0.5%~1%', lambda x: 0.5 <= x < 1),
        ('1%~2%', lambda x: 1 <= x < 2),
        ('2%~3%', lambda x: 2 <= x < 3),
        ('3%~5%', lambda x: 3 <= x < 5),
        ('5%以上', lambda x: x >= 5),
    ]

    print(f"  {'前3天涨跌区间':<16} {'准确率':>8} {'正确':>6} {'总数':>6} {'占比':>6}")
    print(f"  {'-'*46}")

    for label, cond in bins:
        group = [d for d in details if cond(d['d3_chg'])]
        if not group:
            continue
        correct = sum(1 for d in group if d['correct'])
        acc = correct / len(group) * 100
        pct = len(group) / len(details) * 100
        bar = '█' * int(acc / 5)
        print(f"  {label:<16} {acc:>7.1f}% {correct:>5} {len(group):>5} {pct:>5.1f}% {bar}")


def print_weekly_trend_chart(lowo):
    """输出各周准确率趋势图（文本柱状图）。"""
    print_section("九、各周准确率趋势")

    if not lowo.get('week_accuracies'):
        print("  无数据")
        return

    accs = lowo['week_accuracies']
    print(f"  {'周序号':>6} {'准确率':>8} {'柱状图'}")
    print(f"  {'-'*60}")

    for i, acc in enumerate(accs, 1):
        bar_len = int(acc / 2)
        bar = '█' * bar_len
        marker = ' ← 达标线' if abs(acc - 80) < 0.5 else ''
        color_mark = '✓' if acc >= 80 else '✗'
        print(f"  W{i:>4} {acc:>7.1f}% {color_mark} |{bar}{marker}")

    # 80%参考线
    ref_bar = '·' * 40
    print(f"  {'80%线':>6} {'80.0%':>8}   |{ref_bar}")


def print_pass_criteria(summary, full, lowo):
    """输出达标检查。"""
    print_section("十、达标检查", char='═')

    target = 80.0
    checks = [
        ('周数 ≥ 12', summary['week_count'] >= 12, f"{summary['week_count']} 周"),
        ('股票 ≥ 60', summary['stock_count'] >= 60, f"{summary['stock_count']} 只"),
        ('每板块 ≥ 4只', True, '15 × 4 = 60'),
        ('全样本准确率 ≥ 80%', full['accuracy'] >= target, f"{full['accuracy']}%"),
        ('LOWO准确率 ≥ 80%', lowo['overall_accuracy'] >= target, f"{lowo['overall_accuracy']}%"),
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


# ═══════════════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════════════

def main():
    stock_codes, concept_board_map = build_stock_list()

    print("=" * 90)
    print("  概念板块强弱势增强周预测回测 v2 — 详细测试报告")
    print("  60只股票 × 15个概念板块 × 12+周")
    print("=" * 90)
    print(f"  股票总数: {len(stock_codes)} (去重后)")
    print(f"  概念板块: {len(CONCEPT_BOARD_STOCKS)}个, 每板块 4 只")
    print(f"  回测区间: 2025-12-01 ~ 2026-03-10 (约14周)")
    print()

    # 先尝试DB，不可达则用模拟数据
    db_available = _check_db_available()
    data_mode = '数据库（DB实盘数据）' if db_available else '模拟数据（DB不可达）'

    if db_available:
        print(f"  数据模式: {data_mode}")
        result = run_concept_strength_backtest(
            stock_codes=stock_codes,
            start_date='2025-12-01',
            end_date='2026-03-10',
            concept_board_map=concept_board_map,
        )
    else:
        print(f"  数据模式: {data_mode}")
        result = run_concept_strength_backtest_simulated(
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
    full = result['full_sample']
    lowo = result['lowo_cv']
    boards = result['by_concept_board']
    details = full.get('details', [])

    # ═══════════════════════════════════════════════════════════
    # 输出详细报告
    # ═══════════════════════════════════════════════════════════

    # 一、基本信息
    print_basic_info(summary, full, data_mode)

    # 二、准确率总览
    print_accuracy_overview(full, lowo)

    # 三、按概念板块分析
    print_board_analysis(boards)

    # 四、个股维度详细分析
    print_per_stock_analysis(details, concept_board_map)

    # 五、逐周预测明细
    print_weekly_detail_table(details, concept_board_map)

    # 六、预测错误案例分析
    print_error_analysis(details, concept_board_map)

    # 七、概念板块信号有效性分析
    print_signal_effectiveness(details)

    # 八、前3天涨跌幅分布与准确率
    print_d3_chg_distribution(details)

    # 九、各周准确率趋势
    print_weekly_trend_chart(lowo)

    # 十、达标检查
    print_pass_criteria(summary, full, lowo)

    # ═══════════════════════════════════════════════════════════
    # 保存结果
    # ═══════════════════════════════════════════════════════════

    output_path = 'data_results/backtest_concept_strength_weekly_60stocks_result.json'
    # details 中 iso_week 是 tuple，需要转为字符串
    serializable_details = []
    for d in details:
        sd = dict(d)
        sd['iso_week'] = f"{d['iso_week'][0]}-W{d['iso_week'][1]:02d}"
        serializable_details.append(sd)

    save_result = {
        'summary': summary,
        'full_sample': {k: v for k, v in full.items() if k != 'details'},
        'lowo_cv': lowo,
        'by_concept_board': boards,
        'details': serializable_details,
        'pass_criteria': {
            'week_count_ok': summary['week_count'] >= 12,
            'stock_count_ok': summary['stock_count'] >= 60,
            'full_accuracy_ok': full['accuracy'] >= 80,
            'lowo_accuracy_ok': lowo['overall_accuracy'] >= 80,
            'all_pass': (full['accuracy'] >= 80 and lowo['overall_accuracy'] >= 80
                         and summary['week_count'] >= 12 and summary['stock_count'] >= 60),
        },
    }
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(save_result, f, ensure_ascii=False, indent=2)
    print(f"\n  详细结果已保存到: {output_path}")
    print()


if __name__ == '__main__':
    main()
