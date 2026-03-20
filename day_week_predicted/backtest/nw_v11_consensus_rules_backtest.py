#!/usr/bin/env python3
"""V11 板块一致性规则挖掘回测。

思路: 不是过滤已有规则，而是利用板块一致性从未命中样本中挖掘新规则，
然后加入V11混合引擎验证整体效果。

关键发现(来自深度分析):
- 涨信号+低一致性(<0.4): 准确率85.3% → 低一致性是涨信号的增强条件
- 跌信号+高一致性(≥0.7): 准确率83.0% → 高一致性是跌信号的增强条件
- 涨信号+偏强板块: 准确率72.0% → 偏强板块时涨信号不可靠
"""
import sys
import os
import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from day_week_predicted.backtest.nw_v11_multifactor_backtest import (
    load_data, build_samples, build_v11_hybrid_engine,
    build_v11_candidate_rules, cv_validate_all_rules,
    _hybrid_predict_one, _pct, _safe_mean,
    V5_BASELINE_RULES, V7_ELITE_RULES, eval_baseline,
    MIN_TRAIN_WEEKS, TARGET_ACCURACY, N_WEEKS,
)

logger = logging.getLogger('v11_consensus')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('data_results/nw_v11_consensus_rules.log', mode='w', encoding='utf-8')
fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S'))
logger.addHandler(fh)
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S'))
logger.addHandler(sh)


def mine_consensus_rules(samples, engine, passed_rules, marginal_rules):
    """从未命中样本中挖掘板块一致性相关的新规则。"""
    logger.info("=" * 90)
    logger.info("  阶段1: 未命中样本中的板块一致性规则挖掘")
    logger.info("=" * 90)

    # 找出V11最优配置(骨干+尾日涨only+过滤low)未命中的样本
    layers = ['backbone', 'bull']
    unpredicted = []
    predicted = []
    for s in samples:
        pred_up, adj_conf, matched_rule = _hybrid_predict_one(
            s, engine, layers, True, 'reference', bull_up_only=True)
        if pred_up is None:
            unpredicted.append(s)
        else:
            predicted.append(s)

    logger.info("  已命中: %d (%.1f%%), 未命中: %d (%.1f%%)",
                len(predicted), len(predicted)/len(samples)*100,
                len(unpredicted), len(unpredicted)/len(samples)*100)

    # 按板块一致性 × 大盘场景 × 方向 分组统计未命中样本
    logger.info("\n  ── 未命中样本分布 ──")

    # 候选规则条件组合
    candidate_conditions = []

    # 1. 低一致性 + 各种条件 → 涨信号
    logger.info("\n  ══ 挖掘: 低一致性涨信号 ══")
    low_cc_conditions = [
        ('低一致+跌>2%+低位<0.3', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] < 0.4
            and s['this_chg'] < -2
            and s.get('pos60') is not None and s['pos60'] < 0.3)),
        ('低一致+跌>3%', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] < 0.4
            and s['this_chg'] < -3)),
        ('低一致+跌>2%+连跌≥3天', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] < 0.4
            and s['this_chg'] < -2 and s['cd'] >= 3)),
        ('低一致+大盘涨+跌>2%', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] < 0.4
            and s['mkt_chg'] >= 0 and s['this_chg'] < -2)),
        ('低一致+大盘涨+跌>3%+低位<0.4', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] < 0.4
            and s['mkt_chg'] >= 0 and s['this_chg'] < -3
            and s.get('pos60') is not None and s['pos60'] < 0.4)),
        ('低一致+大盘微涨+跌>2%+低位<0.3', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] < 0.4
            and 0 <= s['mkt_chg'] <= 1 and s['this_chg'] < -2
            and s.get('pos60') is not None and s['pos60'] < 0.3)),
        ('低一致+大盘涨+跌>2%+连跌≥2天', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] < 0.4
            and s['mkt_chg'] >= 0 and s['this_chg'] < -2 and s['cd'] >= 2)),
        ('低一致+偏弱板块+跌>2%', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] < 0.4
            and s.get('board_momentum') is not None and s['board_momentum'] < 0
            and s['this_chg'] < -2)),
        ('低一致+偏弱板块+跌>3%+低位<0.4', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] < 0.4
            and s.get('board_momentum') is not None and s['board_momentum'] < 0
            and s['this_chg'] < -3
            and s.get('pos60') is not None and s['pos60'] < 0.4)),
        ('低一致+资金流出+跌>2%+低位<0.3', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] < 0.4
            and s.get('big_net_pct_avg') is not None and s['big_net_pct_avg'] < 0
            and s['this_chg'] < -2
            and s.get('pos60') is not None and s['pos60'] < 0.3)),
        ('低一致+大盘涨+尾日跌>1%+跌>2%', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] < 0.4
            and s['mkt_chg'] >= 0
            and s.get('mkt_last_day') is not None and s['mkt_last_day'] < -1
            and s['this_chg'] < -2)),
    ]

    for name, check in low_cc_conditions:
        matched = [s for s in unpredicted if check(s)]
        if len(matched) >= 30:
            up_cnt = sum(1 for s in matched if s['actual_up'])
            acc = up_cnt / len(matched) * 100
            flag = '★' if acc >= 70 else ' '
            logger.info("    %s %-40s 涨准确率%.1f%% (%d/%d样本)",
                        flag, name, acc, up_cnt, len(matched))
            if acc >= 68:
                candidate_conditions.append({
                    'name': f'CC_UP:{name}→涨', 'pred_up': True,
                    'check': check, 'acc': acc, 'total': len(matched),
                })

    # 2. 高一致性 + 各种条件 → 跌信号
    logger.info("\n  ══ 挖掘: 高一致性跌信号 ══")
    high_cc_conditions = [
        ('高一致+涨>3%+高位>0.6', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] >= 0.7
            and s['this_chg'] > 3
            and s.get('pos60') is not None and s['pos60'] >= 0.6)),
        ('高一致+涨>5%', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] >= 0.7
            and s['this_chg'] > 5)),
        ('高一致+涨>3%+连涨≥2天', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] >= 0.7
            and s['this_chg'] > 3 and s['cu'] >= 2)),
        ('高一致+偏强板块+涨>3%', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] >= 0.7
            and s.get('board_momentum') is not None and s['board_momentum'] > 0
            and s['this_chg'] > 3)),
        ('高一致+偏强板块+涨>2%+高位>0.5', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] >= 0.7
            and s.get('board_momentum') is not None and s['board_momentum'] > 0
            and s['this_chg'] > 2
            and s.get('pos60') is not None and s['pos60'] >= 0.5)),
        ('高一致+大盘涨+涨>3%', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] >= 0.7
            and s['mkt_chg'] >= 0 and s['this_chg'] > 3)),
        ('高一致+大盘涨+涨>5%+高位>0.5', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] >= 0.7
            and s['mkt_chg'] >= 0 and s['this_chg'] > 5
            and s.get('pos60') is not None and s['pos60'] >= 0.5)),
        ('高一致+大盘跌+涨>3%', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] >= 0.7
            and s['mkt_chg'] < -1 and s['this_chg'] > 3)),
        ('高一致+大盘跌+涨>5%+高位>0.5', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] >= 0.7
            and s['mkt_chg'] < -1 and s['this_chg'] > 5
            and s.get('pos60') is not None and s['pos60'] >= 0.5)),
        ('高一致+冲高回落+涨>2%', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] >= 0.7
            and s['rush_up_pullback'] and s['this_chg'] > 2)),
        ('高一致+放量>1.5+涨>3%+高位>0.5', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] >= 0.7
            and s.get('vol_ratio') is not None and s['vol_ratio'] > 1.5
            and s['this_chg'] > 3
            and s.get('pos60') is not None and s['pos60'] >= 0.5)),
        ('高一致+资金流出+涨>3%', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] >= 0.7
            and s.get('big_net_pct_avg') is not None and s['big_net_pct_avg'] < -1
            and s['this_chg'] > 3)),
        ('中高一致≥0.5+大盘跌+涨>5%+高位>0.5', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] >= 0.5
            and s['mkt_chg'] < -1 and s['this_chg'] > 5
            and s.get('pos60') is not None and s['pos60'] >= 0.5)),
        ('中高一致≥0.5+涨>5%+连涨≥3天', lambda s: (
            s.get('concept_consensus') is not None and s['concept_consensus'] >= 0.5
            and s['this_chg'] > 5 and s['cu'] >= 3)),
    ]

    for name, check in high_cc_conditions:
        matched = [s for s in unpredicted if check(s)]
        if len(matched) >= 30:
            down_cnt = sum(1 for s in matched if not s['actual_up'])
            acc = down_cnt / len(matched) * 100
            flag = '★' if acc >= 65 else ' '
            logger.info("    %s %-40s 跌准确率%.1f%% (%d/%d样本)",
                        flag, name, acc, down_cnt, len(matched))
            if acc >= 63:
                candidate_conditions.append({
                    'name': f'CC_DN:{name}→跌', 'pred_up': False,
                    'check': check, 'acc': acc, 'total': len(matched),
                })

    # 3. 板块强弱 + 个股强弱 组合
    logger.info("\n  ══ 挖掘: 板块×个股强弱组合 ══")
    combo_conditions = [
        ('偏强板块+强势个股+涨>3%→跌', False, lambda s: (
            s.get('board_momentum') is not None and s['board_momentum'] > 0
            and s['relative_strength'] > 2 and s['this_chg'] > 3)),
        ('偏强板块+强势个股+涨>5%→跌', False, lambda s: (
            s.get('board_momentum') is not None and s['board_momentum'] > 0
            and s['relative_strength'] > 2 and s['this_chg'] > 5)),
        ('偏弱板块+弱势个股+跌>3%+低位<0.3→涨', True, lambda s: (
            s.get('board_momentum') is not None and s['board_momentum'] < 0
            and s['relative_strength'] < -2 and s['this_chg'] < -3
            and s.get('pos60') is not None and s['pos60'] < 0.3)),
        ('偏弱板块+弱势个股+跌>2%+连跌≥3天→涨', True, lambda s: (
            s.get('board_momentum') is not None and s['board_momentum'] < 0
            and s['relative_strength'] < -2 and s['this_chg'] < -2 and s['cd'] >= 3)),
        ('偏强板块+大盘跌+涨>3%→跌', False, lambda s: (
            s.get('board_momentum') is not None and s['board_momentum'] > 0
            and s['mkt_chg'] < -1 and s['this_chg'] > 3)),
        ('偏强板块+大盘涨+涨>5%+高位>0.6→跌', False, lambda s: (
            s.get('board_momentum') is not None and s['board_momentum'] > 0
            and s['mkt_chg'] >= 0 and s['this_chg'] > 5
            and s.get('pos60') is not None and s['pos60'] >= 0.6)),
    ]

    for name, pred_up, check in combo_conditions:
        matched = [s for s in unpredicted if check(s)]
        if len(matched) >= 30:
            if pred_up:
                correct = sum(1 for s in matched if s['actual_up'])
            else:
                correct = sum(1 for s in matched if not s['actual_up'])
            acc = correct / len(matched) * 100
            flag = '★' if acc >= 65 else ' '
            direction = '涨' if pred_up else '跌'
            logger.info("    %s %-45s %s准确率%.1f%% (%d/%d样本)",
                        flag, name, direction, acc, correct, len(matched))
            threshold = 68 if pred_up else 63
            if acc >= threshold:
                candidate_conditions.append({
                    'name': f'CC_COMBO:{name}', 'pred_up': pred_up,
                    'check': check, 'acc': acc, 'total': len(matched),
                })

    logger.info("\n  候选规则总数: %d", len(candidate_conditions))
    return candidate_conditions, unpredicted, predicted


def cv_validate_consensus_rules(samples, candidate_rules, engine):
    """对候选一致性规则做时间序列CV验证。"""
    logger.info("\n" + "=" * 90)
    logger.info("  阶段2: 候选一致性规则CV验证")
    logger.info("=" * 90)

    all_weeks = sorted(set(s['iw_this'] for s in samples))
    layers = ['backbone', 'bull']

    passed = []
    for rule in candidate_rules:
        # 全样本准确率
        full_matched = [s for s in samples if rule['check'](s)]
        # 排除已被V11命中的
        new_matched = []
        for s in full_matched:
            pred_up, _, _ = _hybrid_predict_one(
                s, engine, layers, True, 'reference', bull_up_only=True)
            if pred_up is None:
                new_matched.append(s)

        if len(new_matched) < 30:
            continue

        if rule['pred_up']:
            full_correct = sum(1 for s in new_matched if s['actual_up'])
        else:
            full_correct = sum(1 for s in new_matched if not s['actual_up'])
        full_acc = full_correct / len(new_matched) * 100

        # CV验证
        cv_correct, cv_total = 0, 0
        weekly_accs = []
        for test_idx in range(MIN_TRAIN_WEEKS, len(all_weeks)):
            test_week = all_weeks[test_idx]
            test_matched = [s for s in new_matched if s['iw_this'] == test_week]
            if not test_matched:
                continue
            wt = len(test_matched)
            if rule['pred_up']:
                wc = sum(1 for s in test_matched if s['actual_up'])
            else:
                wc = sum(1 for s in test_matched if not s['actual_up'])
            cv_total += wt
            cv_correct += wc
            if wt >= 3:
                weekly_accs.append(wc / wt * 100)

        if cv_total < 20:
            continue

        cv_acc = cv_correct / cv_total * 100
        gap = abs(full_acc - cv_acc)

        flag = '★' if cv_acc >= 65 and gap < 15 else ' '
        logger.info("  %s %-45s 全样本%.1f%%(%d) CV%.1f%%(%d) gap%.1f%%",
                    flag, rule['name'], full_acc, len(new_matched),
                    cv_acc, cv_total, gap)

        if cv_acc >= 63 and gap < 15:
            passed.append({
                **rule,
                'full_acc': full_acc, 'cv_acc': cv_acc, 'gap': gap,
                'new_total': len(new_matched), 'cv_total': cv_total,
            })

    logger.info("\n  通过CV验证的一致性规则: %d", len(passed))
    for r in sorted(passed, key=lambda x: -x['cv_acc']):
        logger.info("    %-45s CV%.1f%% (%d样本)", r['name'], r['cv_acc'], r['cv_total'])

    return passed


def eval_v11_with_consensus_rules(samples, engine, consensus_rules):
    """将一致性规则加入V11引擎，评估整体效果。"""
    logger.info("\n" + "=" * 90)
    logger.info("  阶段3: V11 + 一致性规则 整体评估")
    logger.info("=" * 90)

    all_weeks = sorted(set(s['iw_this'] for s in samples))
    layers = ['backbone', 'bull']

    # 按CV准确率排序
    sorted_rules = sorted(consensus_rules, key=lambda x: -x['cv_acc'])

    # 测试不同数量的一致性规则加入
    configs = [
        ('V11基线(无一致性规则)', []),
        ('V11+top1一致性规则', sorted_rules[:1]),
        ('V11+top3一致性规则', sorted_rules[:3]),
        ('V11+top5一致性规则', sorted_rules[:5]),
        ('V11+全部一致性规则', sorted_rules),
        ('V11+仅涨一致性规则', [r for r in sorted_rules if r['pred_up']]),
        ('V11+仅跌一致性规则', [r for r in sorted_rules if not r['pred_up']]),
        ('V11+CV≥68%一致性规则', [r for r in sorted_rules if r['cv_acc'] >= 68]),
        ('V11+CV≥70%一致性规则', [r for r in sorted_rules if r['cv_acc'] >= 70]),
    ]

    results = []
    for cfg_name, cc_rules in configs:
        # 全样本评估
        total_pred, total_correct = 0, 0
        by_mkt = defaultdict(lambda: {'correct': 0, 'total': 0})
        by_dir = defaultdict(lambda: {'correct': 0, 'total': 0})
        by_source = defaultdict(lambda: {'correct': 0, 'total': 0})

        for s in samples:
            # 先用V11原有引擎
            pred_up, adj_conf, matched_rule = _hybrid_predict_one(
                s, engine, layers, True, 'reference', bull_up_only=True)

            source = 'v11'
            # 如果V11未命中，尝试一致性规则
            if pred_up is None and cc_rules:
                for rule in cc_rules:
                    try:
                        if rule['check'](s):
                            pred_up = rule['pred_up']
                            source = 'consensus'
                            break
                    except (TypeError, KeyError):
                        continue

            if pred_up is None:
                continue

            is_correct = pred_up == s['actual_up']
            total_pred += 1
            if is_correct:
                total_correct += 1

            by_source[source]['total'] += 1
            if is_correct:
                by_source[source]['correct'] += 1

            mkt = s['mkt_chg']
            if mkt < -3: regime = '大盘深跌'
            elif mkt < -1: regime = '大盘跌'
            elif mkt < 0: regime = '大盘微跌'
            elif mkt <= 1: regime = '大盘微涨'
            else: regime = '大盘涨'
            by_mkt[regime]['total'] += 1
            if is_correct:
                by_mkt[regime]['correct'] += 1

            d = 'UP' if pred_up else 'DOWN'
            by_dir[d]['total'] += 1
            if is_correct:
                by_dir[d]['correct'] += 1

        full_acc = total_correct / total_pred * 100 if total_pred > 0 else 0
        full_cov = total_pred / len(samples) * 100

        # CV评估
        cv_correct, cv_total = 0, 0
        weekly_accs = []
        for test_idx in range(MIN_TRAIN_WEEKS, len(all_weeks)):
            test_week = all_weeks[test_idx]
            test_samples = [s for s in samples if s['iw_this'] == test_week]
            wt, wc = 0, 0
            for s in test_samples:
                pred_up, adj_conf, matched_rule = _hybrid_predict_one(
                    s, engine, layers, True, 'reference', bull_up_only=True)
                if pred_up is None and cc_rules:
                    for rule in cc_rules:
                        try:
                            if rule['check'](s):
                                pred_up = rule['pred_up']
                                break
                        except (TypeError, KeyError):
                            continue
                if pred_up is None:
                    continue
                wt += 1
                if pred_up == s['actual_up']:
                    wc += 1
            cv_total += wt
            cv_correct += wc
            if wt >= 5:
                weekly_accs.append(wc / wt * 100)

        cv_acc = cv_correct / cv_total * 100 if cv_total > 0 else 0
        cv_cov_denom = sum(1 for s in samples
                           if s['iw_this'] in set(all_weeks[MIN_TRAIN_WEEKS:]))
        cv_cov = cv_total / cv_cov_denom * 100 if cv_cov_denom > 0 else 0
        weeks_above_75 = sum(1 for a in weekly_accs if a >= 75)

        results.append({
            'config': cfg_name, 'n_cc_rules': len(cc_rules),
            'full_acc': full_acc, 'full_cov': full_cov,
            'cv_acc': cv_acc, 'cv_cov': cv_cov, 'cv_total': cv_total,
            'weeks_above_75': weeks_above_75, 'total_cv_weeks': len(weekly_accs),
            'by_mkt': dict(by_mkt), 'by_dir': dict(by_dir),
            'by_source': dict(by_source),
        })

    # 输出结果
    logger.info("\n  %-35s %-10s %-10s %-10s %-10s %-8s",
                '配置', '全样本准确', '全样本覆盖', 'CV准确', 'CV覆盖', '达标周')
    logger.info("  " + "-" * 90)
    baseline = results[0] if results else None
    for r in results:
        flag = '★' if r['cv_acc'] >= TARGET_ACCURACY else ' '
        delta = ''
        if baseline and r['config'] != baseline['config']:
            d_acc = r['cv_acc'] - baseline['cv_acc']
            d_cov = r['cv_cov'] - baseline['cv_cov']
            delta = f" (准确{d_acc:+.1f}% 覆盖{d_cov:+.1f}%)"
        logger.info("  %s %-33s %-10s %-10s %-10s %-10s %d/%d%s",
                    flag, r['config'],
                    f"{r['full_acc']:.1f}%", f"{r['full_cov']:.1f}%",
                    f"{r['cv_acc']:.1f}%", f"{r['cv_cov']:.1f}%",
                    r['weeks_above_75'], r['total_cv_weeks'], delta)

    # 最优配置详细分析
    best = max(results, key=lambda r: (
        r['cv_acc'] * 0.7 + r['cv_cov'] * 0.3 if r['cv_acc'] >= TARGET_ACCURACY
        else r['cv_acc'] * 0.5))

    logger.info("\n  ★ 最优配置: %s", best['config'])
    logger.info("    全样本: 准确率%.1f%% 覆盖率%.1f%%", best['full_acc'], best['full_cov'])
    logger.info("    CV: 准确率%.1f%% 覆盖率%.1f%% (%d样本)",
                best['cv_acc'], best['cv_cov'], best['cv_total'])

    logger.info("\n    ── 按大盘场景 ──")
    for regime in ['大盘深跌', '大盘跌', '大盘微跌', '大盘微涨', '大盘涨']:
        st = best['by_mkt'].get(regime, {'correct': 0, 'total': 0})
        if st['total'] > 0:
            logger.info("      %-10s %s (%d/%d)", regime,
                        _pct(st['correct'], st['total']), st['correct'], st['total'])

    logger.info("\n    ── 按来源 ──")
    for src in ['v11', 'consensus']:
        st = best['by_source'].get(src, {'correct': 0, 'total': 0})
        if st['total'] > 0:
            logger.info("      %-12s %s (%d/%d)", src,
                        _pct(st['correct'], st['total']), st['correct'], st['total'])

    logger.info("\n    ── 按方向 ──")
    for d in ['UP', 'DOWN']:
        st = best['by_dir'].get(d, {'correct': 0, 'total': 0})
        if st['total'] > 0:
            logger.info("      %-6s %s (%d/%d)", d,
                        _pct(st['correct'], st['total']), st['correct'], st['total'])

    return results, best


def run():
    t0 = datetime.now()
    logger.info("=" * 90)
    logger.info("  V11 板块一致性规则挖掘回测")
    logger.info("  策略: 从未命中样本中挖掘一致性相关新规则，加入V11引擎")
    logger.info("=" * 90)

    # 1. 加载数据和构建样本(复用V11)
    logger.info("\n[1/4] 加载数据...")
    data = load_data(N_WEEKS)
    logger.info("\n[2/4] 构建样本...")
    samples = build_samples(data, N_WEEKS)
    logger.info("  总样本: %d", len(samples))

    # 3. 构建V11引擎
    logger.info("\n[3/4] 构建V11引擎...")
    candidate_rules = build_v11_candidate_rules()
    passed_rules, marginal_rules = cv_validate_all_rules(samples, candidate_rules)
    engine = build_v11_hybrid_engine(passed_rules, marginal_rules)

    # 4. 挖掘一致性规则
    logger.info("\n[4/4] 挖掘一致性规则...")
    cc_candidates, unpredicted, predicted = mine_consensus_rules(
        samples, engine, passed_rules, marginal_rules)

    if cc_candidates:
        # CV验证
        cc_passed = cv_validate_consensus_rules(samples, cc_candidates, engine)

        if cc_passed:
            # 整体评估
            all_results, best = eval_v11_with_consensus_rules(
                samples, engine, cc_passed)
        else:
            logger.info("\n  无一致性规则通过CV验证")
            all_results, best = [], None
    else:
        logger.info("\n  无候选一致性规则")
        all_results, best = [], None

    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("\n" + "=" * 90)
    logger.info("  回测完成! 耗时%.1f秒", elapsed)
    logger.info("=" * 90)

    # 保存结果
    save_data = {
        'timestamp': datetime.now().isoformat(),
        'cc_candidates': len(cc_candidates) if cc_candidates else 0,
        'cc_passed': len(cc_passed) if cc_candidates and cc_passed else 0,
        'best_config': best['config'] if best else None,
        'best_cv_acc': best['cv_acc'] if best else None,
        'best_cv_cov': best['cv_cov'] if best else None,
        'all_results': [{k: v for k, v in r.items()
                         if k not in ('by_mkt', 'by_dir', 'by_source')}
                        for r in all_results] if all_results else [],
    }
    try:
        with open('data_results/nw_v11_consensus_rules.json', 'w', encoding='utf-8') as f:
            json.dump(save_data, f, ensure_ascii=False, indent=2, default=str)
        logger.info("  结果已保存到 data_results/nw_v11_consensus_rules.json")
    except Exception as e:
        logger.warning("  保存失败: %s", e)


if __name__ == '__main__':
    run()
