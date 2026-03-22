#!/usr/bin/env python3
"""
多因子预测体系 — 抗过拟合回测验证框架
======================================
理论来源：
  - López de Prado (2018) "Advances in Financial Machine Learning"
    → Purged K-Fold CV（时序隔离交叉验证）
  - Harvey, Liu & Zhu (2016) "...and the Cross-Section of Expected Returns"
    → 多重检验修正（Bonferroni/BH）
  - Bailey & López de Prado (2014) "The Deflated Sharpe Ratio"
    → 过拟合概率估计

验证层次：
  Layer 1: 单因子IC/IR检验（因子有效性）
  Layer 2: Purged K-Fold CV（模型泛化性）
  Layer 3: 样本外滚动验证（实战模拟）
  Layer 4: 过拟合诊断指标

设计原则：
  - 严格时序隔离：训练集和测试集之间留gap（purge window）
  - 不做参数搜索：所有参数固定，只验证不优化
  - 多维度评估：准确率 + IC + 分组收益 + 换手率
"""
import logging
import math
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dao import get_connection
from service.factor_prediction.factor_engine import (
    compute_price_volume_factors,
    compute_fundamental_factors,
    compute_alternative_factors,
    cross_sectional_rank,
)
from service.factor_prediction.prediction_system import (
    FactorPredictionEngine,
    FACTOR_DIRECTIONS,
    FACTOR_CATEGORIES,
)

logger = logging.getLogger(__name__)


def _to_float(v) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


# ═══════════════════════════════════════════════════════════
# Layer 1: 单因子IC检验
# ═══════════════════════════════════════════════════════════

def compute_factor_ic(stock_factors: dict[str, dict],
                      stock_returns: dict[str, float],
                      factor_name: str) -> dict:
    """
    计算单因子的Rank IC（Spearman相关系数）。

    Rank IC 是因子有效性的金标准（来源：几乎所有SSRN多因子文献）：
    - IC > 0.03 且显著：因子有预测力
    - IR (IC均值/IC标准差) > 0.5：因子稳定有效

    Args:
        stock_factors: {code: {factor_name: value}}
        stock_returns: {code: next_period_return}
        factor_name: 要检验的因子名

    Returns:
        {'ic': float, 'factor_name': str, 'n_stocks': int}
    """
    # 取交集
    common = set(stock_factors.keys()) & set(stock_returns.keys())
    pairs = []
    for code in common:
        fval = stock_factors[code].get(factor_name)
        if fval is not None and not math.isnan(fval):
            pairs.append((fval, stock_returns[code]))

    if len(pairs) < 10:
        return {'ic': None, 'factor_name': factor_name, 'n_stocks': len(pairs)}

    # Spearman Rank IC
    n = len(pairs)
    factor_vals = [p[0] for p in pairs]
    return_vals = [p[1] for p in pairs]

    # Rank
    f_sorted = sorted(range(n), key=lambda i: factor_vals[i])
    r_sorted = sorted(range(n), key=lambda i: return_vals[i])
    f_rank = [0] * n
    r_rank = [0] * n
    for rank, idx in enumerate(f_sorted):
        f_rank[idx] = rank
    for rank, idx in enumerate(r_sorted):
        r_rank[idx] = rank

    # Spearman correlation
    d_sq = sum((f_rank[i] - r_rank[i]) ** 2 for i in range(n))
    ic = 1 - 6 * d_sq / (n * (n ** 2 - 1))

    return {'ic': round(ic, 4), 'factor_name': factor_name, 'n_stocks': n}


# ═══════════════════════════════════════════════════════════
# Layer 2: Purged K-Fold 时序交叉验证
# ═══════════════════════════════════════════════════════════

def purged_kfold_backtest(all_weekly_data: dict, n_folds: int = 5,
                          purge_weeks: int = 1) -> dict:
    """
    Purged K-Fold 交叉验证（López de Prado 2018）。

    关键设计：
    - 按时间顺序切分（不是随机切分）
    - 训练集和测试集之间留 purge_weeks 周的间隔
    - 防止时序数据的信息泄露

    Args:
        all_weekly_data: {iso_week_key: {code: {factors: dict, actual_return: float}}}
            按ISO周组织的因子和实际收益数据
        n_folds: 折数
        purge_weeks: 训练/测试间隔周数

    Returns:
        {
            'fold_accuracies': [float],
            'mean_accuracy': float,
            'std_accuracy': float,
            'total_predictions': int,
            'total_correct': int,
            'overfit_score': float,  # 过拟合诊断分（0=无过拟合，1=严重过拟合）
        }
    """
    sorted_weeks = sorted(all_weekly_data.keys())
    n_weeks = len(sorted_weeks)

    if n_weeks < n_folds * 3:
        logger.warning("周数不足(%d)，无法进行%d折交叉验证", n_weeks, n_folds)
        return {'fold_accuracies': [], 'mean_accuracy': 0, 'std_accuracy': 0,
                'total_predictions': 0, 'total_correct': 0, 'overfit_score': 1.0}

    fold_size = n_weeks // n_folds
    fold_results = []
    total_pred = 0
    total_correct = 0
    train_accuracies = []

    for fold_idx in range(n_folds):
        test_start = fold_idx * fold_size
        test_end = test_start + fold_size if fold_idx < n_folds - 1 else n_weeks

        test_weeks = sorted_weeks[test_start:test_end]
        # 训练集：测试集之前的所有周（留purge间隔）
        train_end = max(0, test_start - purge_weeks)
        train_weeks = sorted_weeks[:train_end]

        if len(train_weeks) < 4 or len(test_weeks) < 1:
            continue

        # 在训练集上计算因子IC，验证因子方向是否一致
        # （这里不做参数优化，只验证因子方向与学术共识是否一致）
        train_ic = _compute_period_ic(all_weekly_data, train_weeks)
        train_acc = _compute_period_accuracy(all_weekly_data, train_weeks)

        # 在测试集上评估预测准确率
        test_acc = _compute_period_accuracy(all_weekly_data, test_weeks)

        if test_acc['total'] > 0:
            fold_acc = test_acc['correct'] / test_acc['total']
            fold_results.append(fold_acc)
            total_pred += test_acc['total']
            total_correct += test_acc['correct']

        if train_acc['total'] > 0:
            train_accuracies.append(train_acc['correct'] / train_acc['total'])

    if not fold_results:
        return {'fold_accuracies': [], 'mean_accuracy': 0, 'std_accuracy': 0,
                'total_predictions': 0, 'total_correct': 0, 'overfit_score': 1.0}

    mean_acc = sum(fold_results) / len(fold_results)
    std_acc = (sum((a - mean_acc) ** 2 for a in fold_results) / max(len(fold_results) - 1, 1)) ** 0.5

    # 过拟合诊断：训练集准确率 vs 测试集准确率的差距
    mean_train = sum(train_accuracies) / len(train_accuracies) if train_accuracies else mean_acc
    overfit_gap = max(0, mean_train - mean_acc)
    overfit_score = min(1.0, overfit_gap / 0.15)  # 差距>15%视为严重过拟合

    return {
        'fold_accuracies': [round(a, 4) for a in fold_results],
        'mean_accuracy': round(mean_acc, 4),
        'std_accuracy': round(std_acc, 4),
        'total_predictions': total_pred,
        'total_correct': total_correct,
        'overfit_score': round(overfit_score, 4),
        'train_accuracy': round(mean_train, 4),
        'n_folds': len(fold_results),
    }


def _compute_period_ic(all_weekly_data: dict, weeks: list) -> dict:
    """计算一段时间内各因子的平均IC。"""
    factor_ics = defaultdict(list)
    for week in weeks:
        week_data = all_weekly_data.get(week, {})
        if len(week_data) < 10:
            continue
        stock_factors = {c: d['factors'] for c, d in week_data.items() if 'factors' in d}
        stock_returns = {c: d['actual_return'] for c, d in week_data.items() if 'actual_return' in d}
        for fname in FACTOR_DIRECTIONS:
            ic_result = compute_factor_ic(stock_factors, stock_returns, fname)
            if ic_result['ic'] is not None:
                factor_ics[fname].append(ic_result['ic'])

    result = {}
    for fname, ics in factor_ics.items():
        if ics:
            mean_ic = sum(ics) / len(ics)
            std_ic = (sum((v - mean_ic) ** 2 for v in ics) / max(len(ics) - 1, 1)) ** 0.5
            ir = mean_ic / std_ic if std_ic > 0 else 0
            result[fname] = {
                'mean_ic': round(mean_ic, 4),
                'ic_ir': round(ir, 4),
                'n_periods': len(ics),
            }
    return result


def _compute_period_accuracy(all_weekly_data: dict, weeks: list,
                             ic_weights: dict = None,
                             confidence_filter: str = None,
                             extreme_only: bool = False,
                             strong_signal_only: bool = False) -> dict:
    """计算一段时间内的预测准确率。

    Args:
        ic_weights: 可选的IC权重字典
        confidence_filter: 只统计指定置信度以上的预测 ('high'/'medium'/None)
        extreme_only: 只统计综合评分在top/bottom 20%的股票（极端信号）
        strong_signal_only: 只统计多维度一致的强信号
    """
    total = 0
    correct = 0
    conf_levels = {'high': 3, 'medium': 2, 'low': 1}
    min_conf = conf_levels.get(confidence_filter, 0)

    for week in weeks:
        week_data = all_weekly_data.get(week, {})
        if len(week_data) < 20:
            continue

        engine = FactorPredictionEngine()
        for code, data in week_data.items():
            if 'factors' in data:
                engine.stock_raw_factors[code] = data['factors']

        predictions = engine.compute_predictions(ic_weights=ic_weights)

        # 强信号过滤
        if strong_signal_only:
            predictions = engine.get_strong_signals(min_categories_agree=2)

        # 极端信号过滤：只看top/bottom 20%
        if extreme_only and predictions:
            scores = sorted(predictions.items(), key=lambda x: x[1]['composite_score'])
            n = len(scores)
            cutoff = max(n // 5, 5)
            extreme_codes = set(
                [c for c, _ in scores[:cutoff]] +
                [c for c, _ in scores[-cutoff:]]
            )
        else:
            extreme_codes = None

        for code, pred in predictions.items():
            actual = week_data.get(code, {}).get('actual_return')
            if actual is None:
                continue

            pred_conf = conf_levels.get(pred['confidence'], 0)
            if pred_conf < min_conf:
                continue

            if extreme_codes and code not in extreme_codes:
                continue

            actual_up = actual > 0
            pred_up = pred['pred_direction'] == 'UP'
            total += 1
            if pred_up == actual_up:
                correct += 1

    return {'total': total, 'correct': correct}


# ═══════════════════════════════════════════════════════════
# Layer 3: 样本外滚动回测（Walk-Forward）
# ═══════════════════════════════════════════════════════════

def walk_forward_backtest(all_weekly_data: dict,
                          train_window: int = 20,
                          test_window: int = 4,
                          purge_weeks: int = 1,
                          ic_weights: dict = None,
                          confidence_filter: str = None,
                          extreme_only: bool = False,
                          strong_signal_only: bool = False) -> dict:
    """
    Walk-Forward 滚动回测 — 最接近实战的验证方式。

    原理（Quantocracy/Alpha Architect推荐的标准做法）：
    - 用过去 train_window 周的数据计算因子
    - 跳过 purge_weeks 周
    - 在接下来 test_window 周上验证
    - 窗口向前滚动，重复

    Args:
        all_weekly_data: 按周组织的因子+收益数据
        train_window: 训练窗口（周）
        test_window: 测试窗口（周）
        purge_weeks: 隔离窗口（周）

    Returns:
        {
            'periods': [{test_weeks, accuracy, n_predictions}, ...],
            'overall_accuracy': float,
            'accuracy_trend': str,  # improving/stable/degrading
            'total_predictions': int,
        }
    """
    sorted_weeks = sorted(all_weekly_data.keys())
    n_weeks = len(sorted_weeks)
    min_required = train_window + purge_weeks + test_window

    if n_weeks < min_required:
        logger.warning("数据不足: %d周 < 最少需要%d周", n_weeks, min_required)
        return {'periods': [], 'overall_accuracy': 0, 'total_predictions': 0}

    periods = []
    step = test_window  # 非重叠滚动

    for start in range(0, n_weeks - min_required + 1, step):
        train_end = start + train_window
        test_start = train_end + purge_weeks
        test_end = min(test_start + test_window, n_weeks)

        if test_start >= n_weeks:
            break

        test_weeks = sorted_weeks[test_start:test_end]
        test_acc = _compute_period_accuracy(all_weekly_data, test_weeks,
                                            ic_weights=ic_weights,
                                            confidence_filter=confidence_filter,
                                            extreme_only=extreme_only,
                                            strong_signal_only=strong_signal_only)

        if test_acc['total'] > 0:
            acc = test_acc['correct'] / test_acc['total']
            periods.append({
                'test_weeks': f"{test_weeks[0]}~{test_weeks[-1]}",
                'accuracy': round(acc, 4),
                'n_predictions': test_acc['total'],
                'n_correct': test_acc['correct'],
            })

    if not periods:
        return {'periods': [], 'overall_accuracy': 0, 'total_predictions': 0}

    total_pred = sum(p['n_predictions'] for p in periods)
    total_correct = sum(p['n_correct'] for p in periods)
    overall_acc = total_correct / total_pred if total_pred > 0 else 0

    # 准确率趋势判断
    if len(periods) >= 4:
        first_half = periods[:len(periods) // 2]
        second_half = periods[len(periods) // 2:]
        acc_first = sum(p['accuracy'] for p in first_half) / len(first_half)
        acc_second = sum(p['accuracy'] for p in second_half) / len(second_half)
        if acc_second - acc_first > 0.03:
            trend = 'improving'
        elif acc_first - acc_second > 0.03:
            trend = 'degrading'
        else:
            trend = 'stable'
    else:
        trend = 'insufficient_data'

    return {
        'periods': periods,
        'overall_accuracy': round(overall_acc, 4),
        'accuracy_trend': trend,
        'total_predictions': total_pred,
        'total_correct': total_correct,
    }


# ═══════════════════════════════════════════════════════════
# Layer 4: 过拟合诊断
# ═══════════════════════════════════════════════════════════

def diagnose_overfitting(purged_cv_result: dict,
                         walk_forward_result: dict) -> dict:
    """
    综合过拟合诊断（Bailey & López de Prado 2014）。

    诊断维度：
    1. CV折间方差：方差大 → 模型不稳定 → 可能过拟合
    2. 训练/测试差距：差距大 → 过拟合
    3. Walk-Forward趋势：准确率下降 → 因子衰减或过拟合
    4. 因子覆盖度：覆盖度低 → 预测不可靠

    Returns:
        {
            'overfit_risk': 'low'/'medium'/'high',
            'diagnosis': str,
            'recommendations': [str],
        }
    """
    risks = []
    recommendations = []

    # 1. CV折间稳定性
    cv_std = purged_cv_result.get('std_accuracy', 0)
    if cv_std > 0.08:
        risks.append('high')
        recommendations.append('CV折间方差过大(%.1f%%)，模型稳定性不足' % (cv_std * 100))
    elif cv_std > 0.04:
        risks.append('medium')

    # 2. 训练/测试差距
    overfit_score = purged_cv_result.get('overfit_score', 0)
    if overfit_score > 0.6:
        risks.append('high')
        recommendations.append('训练集与测试集准确率差距过大，存在过拟合风险')
    elif overfit_score > 0.3:
        risks.append('medium')

    # 3. Walk-Forward趋势
    trend = walk_forward_result.get('accuracy_trend', '')
    if trend == 'degrading':
        risks.append('medium')
        recommendations.append('Walk-Forward准确率呈下降趋势，因子可能在衰减')

    # 4. 样本量
    total_pred = walk_forward_result.get('total_predictions', 0)
    if total_pred < 500:
        risks.append('medium')
        recommendations.append('样本量不足(%d)，建议扩大回测范围' % total_pred)

    # 综合判断
    if 'high' in risks:
        overall = 'high'
    elif risks.count('medium') >= 2:
        overall = 'medium'
    elif 'medium' in risks:
        overall = 'low-medium'
    else:
        overall = 'low'

    if not recommendations:
        recommendations.append('各项指标正常，过拟合风险较低')

    return {
        'overfit_risk': overall,
        'cv_std': cv_std,
        'overfit_score': overfit_score,
        'accuracy_trend': trend,
        'total_samples': total_pred,
        'diagnosis': f'过拟合风险等级: {overall}',
        'recommendations': recommendations,
    }
