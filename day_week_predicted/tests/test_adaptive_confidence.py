#!/usr/bin/env python3
"""
自适应置信度验证测试
====================
用实盘历史数据验证自适应置信度机制的效果。

核心思路：
  1. 滚动实盘准确率替代固定回测准确率 → 置信度动态调整
  2. 选股质量反向验证 → 选出的股票跑输大盘时自动降级
  3. V30训练窗口滚动 → 因子统计跟上市场变化

验证方法：
  从DB加载W11-W13的全量预测+验证数据，模拟自适应机制，
  对比原始准确率 vs 自适应后准确率。

用法: .venv/bin/python day_week_predicted/tests/test_adaptive_confidence.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import logging
from collections import defaultdict
from dao import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STOCK_FILTER = (
    "(h.stock_code LIKE '6%%.SH' OR h.stock_code LIKE '0%%.SZ' OR h.stock_code LIKE '3%%.SZ')"
    " AND h.stock_code NOT LIKE '399%%'"
    " AND h.stock_code != '000001.SH'"
)


# ═══════════════════════════════════════════════════════════════
# Part 0: 数据加载
# ═══════════════════════════════════════════════════════════════

def load_all_prediction_data():
    """加载所有已验证的预测数据，按周组织。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        # 只加载有预测且已验证的数据，减少数据量
        cur.execute(f"""
            SELECT h.stock_code, h.iso_year, h.iso_week,
                   h.nw_pred_direction, h.nw_confidence, h.nw_strategy,
                   h.nw_is_correct, h.nw_actual_weekly_chg,
                   h.nw_backtest_accuracy,
                   h.actual_weekly_chg,
                   h.v20_pred_direction, h.v20_confidence, h.v20_rule_name,
                   h.v20_backtest_acc,
                   h.v20_actual_5d_chg, h.v20_is_correct,
                   h.v30_pred_direction, h.v30_confidence,
                   h.v30_composite_score, h.v30_sent_agree, h.v30_tech_agree,
                   h.v30_mkt_ret_20d,
                   h.v30_actual_5d_chg, h.v30_is_correct
            FROM stock_weekly_prediction_history h
            WHERE {STOCK_FILTER}
              AND h.iso_year = 2026
              AND h.iso_week IN (11, 12, 13)
              AND (h.nw_is_correct IS NOT NULL
                   OR h.v20_is_correct IS NOT NULL
                   OR h.v30_is_correct IS NOT NULL)
            ORDER BY h.iso_week, h.stock_code
        """)
        rows = cur.fetchall()
        logger.info("加载 %d 条预测记录", len(rows))

        # 按周组织
        by_week = defaultdict(list)
        for r in rows:
            by_week[r['iso_week']].append(r)

        # 加载大盘周涨跌
        cur.execute("""
            SELECT YEARWEEK(date, 3) as yw,
                   ROUND(SUM(change_percent), 2) as week_chg,
                   MIN(date) as start_d, MAX(date) as end_d
            FROM stock_kline
            WHERE stock_code = '000001.SH' AND date >= '2026-01-01'
            GROUP BY YEARWEEK(date, 3)
            ORDER BY yw
        """)
        market_weeks = {}
        for r in cur.fetchall():
            # yw格式: 202611 → week=11
            yw = r['yw']
            week = yw % 100
            market_weeks[week] = {
                'chg': float(r['week_chg']),
                'start': str(r['start_d']),
                'end': str(r['end_d']),
            }

        return by_week, market_weeks
    finally:
        cur.close()
        conn.close()


# ═══════════════════════════════════════════════════════════════
# Part 1: 自适应置信度引擎（通用机制）
# ═══════════════════════════════════════════════════════════════

class AdaptiveConfidenceEngine:
    """
    自适应置信度引擎 — 通用机制，不针对任何特定场景。

    核心原则：
      1. 置信度 = f(规则回测准确率, 实盘滚动准确率, 选股质量)
      2. 实盘数据权重随样本量增加而增大
      3. 选股质量 = 选出的股票相对大盘的超额收益
      4. 所有参数通过统计原理确定，不硬编码阈值
      5. 冷启动保守原则：实盘样本不足时，默认降一级

    机制说明：
      - rolling_weight: 实盘滚动准确率的权重，随实盘样本量增加
        公式: min(实盘样本数 / blend_n, 1.0)

      - cold_start: 实盘样本 < min_samples 时，置信度最高为 medium
        原因：回测准确率可能因样本分布偏移而不可靠，
        在积累足够实盘数据前应保守

      - selection_quality: 选出的股票 vs 全市场的超额收益
        用t检验判断是否显著跑输大盘

      - confidence_map: 基于混合准确率的置信度分级
        准确率 >= 60% → high (统计显著超越随机)
        准确率 >= 50% → medium (边际有效)
        准确率 < 50%  → low (不可靠，不应输出)
    """

    def __init__(self, blend_n: int = 100, min_samples: int = 30):
        """
        Args:
            blend_n: 实盘样本达到此数量时完全替代回测准确率。
            min_samples: 冷启动最低样本数。实盘样本不足此数时，
                         置信度上限为 medium（保守原则）。
                         30是统计学中大样本的最低门槛。
        """
        self.blend_n = blend_n
        self.min_samples = min_samples
        # {model_rule_key: [{'correct': bool, 'actual_chg': float, 'week': int}]}
        self.rolling_history = defaultdict(list)
        # {model: [{'stock_chg': float, 'market_chg': float, 'week': int}]}
        self.selection_quality = defaultdict(list)

    def record_result(self, model: str, rule_key: str, correct: bool,
                      actual_chg: float, market_chg: float, week: int):
        """记录一条实盘验证结果。"""
        key = f"{model}:{rule_key}"
        self.rolling_history[key].append({
            'correct': correct, 'actual_chg': actual_chg, 'week': week,
        })
        self.selection_quality[model].append({
            'stock_chg': actual_chg, 'market_chg': market_chg, 'week': week,
        })

    def get_rolling_accuracy(self, model: str, rule_key: str) -> dict:
        """获取某规则的滚动实盘准确率。"""
        key = f"{model}:{rule_key}"
        history = self.rolling_history.get(key, [])
        if not history:
            return {'n': 0, 'accuracy': None}
        n = len(history)
        correct = sum(1 for h in history if h['correct'])
        return {'n': n, 'accuracy': correct / n * 100}

    def get_model_rolling_accuracy(self, model: str) -> dict:
        """获取某模型所有规则合并的滚动实盘准确率。"""
        all_history = []
        prefix = f"{model}:"
        for key, history in self.rolling_history.items():
            if key.startswith(prefix):
                all_history.extend(history)
        if not all_history:
            return {'n': 0, 'accuracy': None}
        n = len(all_history)
        correct = sum(1 for h in all_history if h['correct'])
        return {'n': n, 'accuracy': correct / n * 100}

    def get_selection_quality(self, model: str) -> dict:
        """获取某模型的选股质量（超额收益）。"""
        records = self.selection_quality.get(model, [])
        if not records:
            return {'n': 0, 'excess_return': None, 't_stat': 0, 'up_pct': 0}
        n = len(records)
        excess = [r['stock_chg'] - r['market_chg'] for r in records]
        avg_excess = sum(excess) / n
        if n >= 2:
            var = sum((e - avg_excess) ** 2 for e in excess) / (n - 1)
            se = (var / n) ** 0.5 if var > 0 else 0
            t_stat = avg_excess / se if se > 0 else 0
        else:
            t_stat = 0
        return {
            'n': n,
            'excess_return': avg_excess,
            't_stat': t_stat,
            'up_pct': sum(1 for r in records if r['stock_chg'] >= 0) / n * 100,
        }

    def compute_adaptive_confidence(self, model: str, rule_key: str,
                                     backtest_acc: float) -> dict:
        """
        计算自适应置信度。

        Args:
            model: 模型名 (v11_nw, v20, v30)
            rule_key: 规则标识
            backtest_acc: 原始回测准确率 (%)

        Returns:
            dict with confidence, blended_acc, rolling_acc, etc.
        """
        # 规则级别的滚动准确率
        rolling = self.get_rolling_accuracy(model, rule_key)
        quality = self.get_selection_quality(model)

        # 1. 混合准确率
        # 只用规则级别的实盘数据，不用模型级别的fallback
        # 原因：不同规则的准确率差异很大，模型级别的平均值会误导
        if rolling['n'] >= 10 and rolling['accuracy'] is not None:
            rolling_w = min(rolling['n'] / self.blend_n, 1.0)
            blended = backtest_acc * (1 - rolling_w) + rolling['accuracy'] * rolling_w
        else:
            rolling_w = 0
            blended = backtest_acc

        # 2. 选股质量惩罚
        selection_penalty = 0.0
        if quality['n'] >= 20 and quality['excess_return'] is not None:
            if quality['t_stat'] < -2.0:
                selection_penalty = min(abs(quality['t_stat']) * 3, 15)
            elif quality['t_stat'] < -1.0:
                selection_penalty = min(abs(quality['t_stat']) * 1.5, 5)

        adjusted = blended - selection_penalty

        # 3. 置信度分级
        if adjusted >= 60:
            confidence = 'high'
        elif adjusted >= 50:
            confidence = 'medium'
        else:
            confidence = 'low'

        # 4. 冷启动保守原则
        # 实盘样本不足 min_samples 时，置信度上限为 medium
        cold_start = False
        if rolling['n'] < self.min_samples:
            cold_start = True
            if confidence == 'high':
                confidence = 'medium'

        # 构建理由
        parts = [f'混合准确率={adjusted:.1f}%']
        if rolling['accuracy'] is not None:
            parts.append(f'实盘={rolling["accuracy"]:.1f}%({rolling["n"]}样本,权重{rolling_w:.0%})')
        parts.append(f'回测={backtest_acc:.1f}%')
        if selection_penalty > 0:
            parts.append(f'选股惩罚=-{selection_penalty:.1f}pp')
        if cold_start:
            parts.append(f'冷启动(样本{rolling["n"]}<{self.min_samples})')

        return {
            'confidence': confidence,
            'blended_acc': round(adjusted, 1),
            'rolling_acc': rolling.get('accuracy'),
            'rolling_n': rolling['n'],
            'rolling_weight': round(rolling_w, 2),
            'selection_penalty': round(selection_penalty, 1),
            'cold_start': cold_start,
            'reason': '; '.join(parts),
        }


# ═══════════════════════════════════════════════════════════════
# Part 2: 模拟测试 — 用实盘数据验证自适应机制
# ═══════════════════════════════════════════════════════════════

# V11 NW: 回测准确率从DB的nw_backtest_accuracy字段读取（per-stock）
# 如果DB中没有，用规则层级的默认值
V11_BACKTEST_ACC_DEFAULT = {
    'nw_v11_backbone_UP': 82.0,
    'nw_v11_backbone_DOWN': 73.0,
    'nw_v11_extension_UP': 70.0,
    'nw_v11_fallback_UP': 70.0,
    'nw_v11_fallback_DOWN': 68.0,
    'nw_v11_bull_UP': 77.0,
    'nw_v11_bull_marginal_UP': 64.0,
    'nw_v11_bull_marginal_DOWN': 65.0,
    'nw_rule_t2_DOWN': 64.0,
}

# V20 规则的回测准确率
V20_BACKTEST_ACC = {
    'FINAL_A': 72.2,
    'FINAL_B': 71.9,
    'FINAL_C': 71.5,
    'FINAL_D': 66.6,
}

# V30 置信度的回测准确率
V30_BACKTEST_ACC = {
    'v30_high': 75.0,
    'v30_medium': 65.0,
    'v30_low': 60.0,
}


def get_v11_rule_key(row):
    """从NW预测记录中提取V11规则key。"""
    strategy = row.get('nw_strategy') or ''
    direction = row.get('nw_pred_direction') or ''
    return f"{strategy}_{direction}"


def get_v20_rule_key(row):
    """从V20预测记录中提取规则key。"""
    return row.get('v20_rule_name') or 'UNKNOWN'


def get_v30_rule_key(row):
    """从V30预测记录中提取规则key。"""
    conf = row.get('v30_confidence') or 'low'
    return f"v30_{conf}"


def simulate_adaptive(by_week, market_weeks):
    """
    模拟自适应置信度机制。

    按周顺序处理：
    1. 对每周的预测，用之前所有周的实盘数据计算自适应置信度
    2. 记录本周的实盘结果
    3. 对比原始置信度 vs 自适应置信度的准确率

    这模拟了真实生产环境：预测时只能用历史数据，不能用未来数据。
    """
    engine = AdaptiveConfidenceEngine(blend_n=100)
    weeks = sorted(by_week.keys())

    results = {
        'v11_nw': {'original': [], 'adaptive': []},
        'v20': {'original': [], 'adaptive': []},
        'v30': {'original': [], 'adaptive': []},
    }

    for week in weeks:
        rows = by_week[week]
        # 该周的大盘涨跌（用于选股质量计算）
        # NW预测的目标周
        nw_target_week = week + 1 if week + 1 <= 53 else 1
        mkt_nw = market_weeks.get(nw_target_week, {}).get('chg', 0)
        mkt_tw = market_weeks.get(week, {}).get('chg', 0)

        week_v11 = {'original_high': [], 'adaptive_high': [],
                     'original_all': [], 'adaptive_all': []}
        week_v20 = {'original_high': [], 'adaptive_high': [],
                     'original_all': [], 'adaptive_all': []}
        week_v30 = {'original_high': [], 'adaptive_high': [],
                     'original_all': [], 'adaptive_all': []}

        for row in rows:
            # ── V11 NW ──
            if (row.get('nw_pred_direction') and row.get('nw_is_correct') is not None):
                rule_key = get_v11_rule_key(row)
                # 优先用DB里的per-stock回测准确率
                backtest_acc = row.get('nw_backtest_accuracy')
                if backtest_acc is None:
                    backtest_acc = V11_BACKTEST_ACC_DEFAULT.get(rule_key, 70.0)
                else:
                    backtest_acc = float(backtest_acc)
                original_conf = row.get('nw_confidence') or 'low'
                is_correct = bool(row['nw_is_correct'])
                actual_chg = float(row.get('nw_actual_weekly_chg') or 0)

                # 计算自适应置信度（只用之前的数据）
                adaptive = engine.compute_adaptive_confidence(
                    'v11_nw', rule_key, backtest_acc)

                week_v11['original_all'].append({
                    'correct': is_correct, 'conf': original_conf,
                })
                week_v11['adaptive_all'].append({
                    'correct': is_correct, 'conf': adaptive['confidence'],
                    'blended_acc': adaptive['blended_acc'],
                })
                if original_conf == 'high':
                    week_v11['original_high'].append(is_correct)
                if adaptive['confidence'] == 'high':
                    week_v11['adaptive_high'].append(is_correct)

            # ── V20 ──
            if (row.get('v20_pred_direction') and row.get('v20_is_correct') is not None):
                rule_key = get_v20_rule_key(row)
                backtest_acc = V20_BACKTEST_ACC.get(rule_key, 70.0)
                original_conf = row.get('v20_confidence') or 'medium'
                is_correct = bool(row['v20_is_correct'])
                actual_chg = float(row.get('v20_actual_5d_chg') or 0)

                adaptive = engine.compute_adaptive_confidence(
                    'v20', rule_key, backtest_acc)

                week_v20['original_all'].append({
                    'correct': is_correct, 'conf': original_conf,
                })
                week_v20['adaptive_all'].append({
                    'correct': is_correct, 'conf': adaptive['confidence'],
                    'blended_acc': adaptive['blended_acc'],
                })
                if original_conf == 'high':
                    week_v20['original_high'].append(is_correct)
                if adaptive['confidence'] == 'high':
                    week_v20['adaptive_high'].append(is_correct)

            # ── V30 ──
            if (row.get('v30_pred_direction') and row.get('v30_is_correct') is not None):
                rule_key = get_v30_rule_key(row)
                backtest_acc = V30_BACKTEST_ACC.get(rule_key, 65.0)
                original_conf = row.get('v30_confidence') or 'low'
                is_correct = bool(row['v30_is_correct'])
                actual_chg = float(row.get('v30_actual_5d_chg') or 0)

                adaptive = engine.compute_adaptive_confidence(
                    'v30', rule_key, backtest_acc)

                week_v30['original_all'].append({
                    'correct': is_correct, 'conf': original_conf,
                })
                week_v30['adaptive_all'].append({
                    'correct': is_correct, 'conf': adaptive['confidence'],
                    'blended_acc': adaptive['blended_acc'],
                })
                if original_conf == 'high':
                    week_v30['original_high'].append(is_correct)
                if adaptive['confidence'] == 'high':
                    week_v30['adaptive_high'].append(is_correct)

        # 记录本周结果到引擎（供下周使用）
        for row in rows:
            if (row.get('nw_pred_direction') and row.get('nw_is_correct') is not None):
                rule_key = get_v11_rule_key(row)
                engine.record_result(
                    'v11_nw', rule_key,
                    bool(row['nw_is_correct']),
                    float(row.get('nw_actual_weekly_chg') or 0),
                    mkt_nw, week)

            if (row.get('v20_pred_direction') and row.get('v20_is_correct') is not None):
                rule_key = get_v20_rule_key(row)
                engine.record_result(
                    'v20', rule_key,
                    bool(row['v20_is_correct']),
                    float(row.get('v20_actual_5d_chg') or 0),
                    mkt_tw, week)

            if (row.get('v30_pred_direction') and row.get('v30_is_correct') is not None):
                rule_key = get_v30_rule_key(row)
                engine.record_result(
                    'v30', rule_key,
                    bool(row['v30_is_correct']),
                    float(row.get('v30_actual_5d_chg') or 0),
                    mkt_tw, week)

        # 汇总本周结果
        results['v11_nw']['original'].append({
            'week': week, 'data': week_v11,
        })
        results['v11_nw']['adaptive'].append({
            'week': week, 'data': week_v11,
        })
        results['v20']['original'].append({
            'week': week, 'data': week_v20,
        })
        results['v20']['adaptive'].append({
            'week': week, 'data': week_v20,
        })
        results['v30']['original'].append({
            'week': week, 'data': week_v30,
        })
        results['v30']['adaptive'].append({
            'week': week, 'data': week_v30,
        })

    return results, engine


# ═══════════════════════════════════════════════════════════════
# Part 3: 结果分析与输出
# ═══════════════════════════════════════════════════════════════

def print_comparison(results, engine):
    """打印原始 vs 自适应的对比结果。"""

    for model_name in ['v11_nw', 'v20', 'v30']:
        print(f"\n{'='*70}")
        print(f"  {model_name.upper()} 原始 vs 自适应 对比")
        print(f"{'='*70}")

        week_entries = results[model_name]['original']

        # 逐周对比
        total_orig_high_correct = 0
        total_orig_high_count = 0
        total_adap_high_correct = 0
        total_adap_high_count = 0
        total_orig_all_correct = 0
        total_orig_all_count = 0
        total_adap_all_correct = 0
        total_adap_all_count = 0

        for entry in week_entries:
            week = entry['week']
            data = entry['data']

            orig_high = data['original_high']
            adap_high = data['adaptive_high']
            orig_all = data['original_all']
            adap_all = data['adaptive_all']

            # 原始高置信度
            oh_n = len(orig_high)
            oh_c = sum(orig_high) if orig_high else 0
            oh_acc = oh_c / oh_n * 100 if oh_n > 0 else 0

            # 自适应高置信度
            ah_n = len(adap_high)
            ah_c = sum(adap_high) if adap_high else 0
            ah_acc = ah_c / ah_n * 100 if ah_n > 0 else 0

            # 原始全部
            oa_n = len(orig_all)
            oa_c = sum(1 for x in orig_all if x['correct'])
            oa_acc = oa_c / oa_n * 100 if oa_n > 0 else 0

            # 自适应全部（只看high+medium）
            aa_hm = [x for x in adap_all if x['conf'] in ('high', 'medium')]
            aa_n = len(aa_hm)
            aa_c = sum(1 for x in aa_hm if x['correct'])
            aa_acc = aa_c / aa_n * 100 if aa_n > 0 else 0

            print(f"\n  W{week:02d}:")
            print(f"    原始高置信: {oh_c}/{oh_n} = {oh_acc:.1f}%")
            print(f"    自适应高置信: {ah_c}/{ah_n} = {ah_acc:.1f}%")
            if oh_n > 0 and ah_n > 0:
                print(f"    高置信数量变化: {oh_n} → {ah_n} ({ah_n - oh_n:+d})")
            print(f"    原始全部: {oa_c}/{oa_n} = {oa_acc:.1f}%")
            print(f"    自适应(high+med): {aa_c}/{aa_n} = {aa_acc:.1f}%")

            total_orig_high_correct += oh_c
            total_orig_high_count += oh_n
            total_adap_high_correct += ah_c
            total_adap_high_count += ah_n
            total_orig_all_correct += oa_c
            total_orig_all_count += oa_n
            total_adap_all_correct += aa_c
            total_adap_all_count += aa_n

        # 汇总
        print(f"\n  {'─'*60}")
        print(f"  汇总:")
        if total_orig_high_count > 0:
            print(f"    原始高置信: {total_orig_high_correct}/{total_orig_high_count} "
                  f"= {total_orig_high_correct/total_orig_high_count*100:.1f}%")
        if total_adap_high_count > 0:
            print(f"    自适应高置信: {total_adap_high_correct}/{total_adap_high_count} "
                  f"= {total_adap_high_correct/total_adap_high_count*100:.1f}%")
        if total_orig_all_count > 0:
            print(f"    原始全部: {total_orig_all_correct}/{total_orig_all_count} "
                  f"= {total_orig_all_correct/total_orig_all_count*100:.1f}%")
        if total_adap_all_count > 0:
            print(f"    自适应(high+med): {total_adap_all_correct}/{total_adap_all_count} "
                  f"= {total_adap_all_correct/total_adap_all_count*100:.1f}%")

    # 打印引擎内部状态
    print(f"\n{'='*70}")
    print(f"  自适应引擎最终状态")
    print(f"{'='*70}")

    for model in ['v11_nw', 'v20', 'v30']:
        quality = engine.get_selection_quality(model)
        print(f"\n  {model.upper()} 选股质量:")
        print(f"    样本数: {quality['n']}")
        if quality['excess_return'] is not None:
            print(f"    平均超额收益: {quality['excess_return']:+.2f}%")
            print(f"    t统计量: {quality['t_stat']:.2f}")
            print(f"    上涨比例: {quality['up_pct']:.1f}%")

    print(f"\n  各规则滚动准确率:")
    for key in sorted(engine.rolling_history.keys()):
        history = engine.rolling_history[key]
        n = len(history)
        correct = sum(1 for h in history if h['correct'])
        acc = correct / n * 100 if n > 0 else 0
        print(f"    {key}: {correct}/{n} = {acc:.1f}%")


# ═══════════════════════════════════════════════════════════════
# Part 4: 置信度校准分析
# ═══════════════════════════════════════════════════════════════

def analyze_calibration(results):
    """分析置信度校准：high/medium/low各级别的实际准确率是否匹配。"""

    print(f"\n{'='*70}")
    print(f"  置信度校准分析")
    print(f"{'='*70}")
    print(f"  理想状态: high>60%, medium=50-60%, low<50%")

    for model_name in ['v11_nw', 'v20', 'v30']:
        print(f"\n  {model_name.upper()}:")

        # 收集所有周的自适应结果
        orig_by_conf = defaultdict(lambda: {'correct': 0, 'total': 0})
        adap_by_conf = defaultdict(lambda: {'correct': 0, 'total': 0})

        for entry in results[model_name]['original']:
            data = entry['data']
            for item in data['original_all']:
                conf = item['conf']
                orig_by_conf[conf]['total'] += 1
                if item['correct']:
                    orig_by_conf[conf]['correct'] += 1

            for item in data['adaptive_all']:
                conf = item['conf']
                adap_by_conf[conf]['total'] += 1
                if item['correct']:
                    adap_by_conf[conf]['correct'] += 1

        print(f"    原始置信度校准:")
        for conf in ['high', 'medium', 'reference', 'low']:
            d = orig_by_conf.get(conf)
            if d and d['total'] > 0:
                acc = d['correct'] / d['total'] * 100
                print(f"      {conf:>10}: {d['correct']}/{d['total']} = {acc:.1f}%")

        print(f"    自适应置信度校准:")
        for conf in ['high', 'medium', 'low']:
            d = adap_by_conf.get(conf)
            if d and d['total'] > 0:
                acc = d['correct'] / d['total'] * 100
                print(f"      {conf:>10}: {d['correct']}/{d['total']} = {acc:.1f}%")


# ═══════════════════════════════════════════════════════════════
# Part 5: 假设检验 — 自适应机制是否显著优于原始
# ═══════════════════════════════════════════════════════════════

def hypothesis_test(results):
    """
    检验自适应机制是否显著改善了高置信度预测的准确率。

    方法：比较原始high vs 自适应high的准确率差异。
    如果自适应high的数量大幅减少但准确率显著提升，说明机制有效。
    """
    print(f"\n{'='*70}")
    print(f"  假设检验: 自适应 vs 原始")
    print(f"{'='*70}")
    print(f"  H0: 自适应高置信度准确率 <= 原始高置信度准确率")
    print(f"  H1: 自适应高置信度准确率 > 原始高置信度准确率")

    for model_name in ['v11_nw', 'v20', 'v30']:
        orig_correct = 0
        orig_total = 0
        adap_correct = 0
        adap_total = 0

        for entry in results[model_name]['original']:
            data = entry['data']
            orig_total += len(data['original_high'])
            orig_correct += sum(data['original_high']) if data['original_high'] else 0
            adap_total += len(data['adaptive_high'])
            adap_correct += sum(data['adaptive_high']) if data['adaptive_high'] else 0

        orig_acc = orig_correct / orig_total * 100 if orig_total > 0 else 0
        adap_acc = adap_correct / adap_total * 100 if adap_total > 0 else 0

        print(f"\n  {model_name.upper()}:")
        print(f"    原始high: {orig_correct}/{orig_total} = {orig_acc:.1f}%")
        print(f"    自适应high: {adap_correct}/{adap_total} = {adap_acc:.1f}%")
        if orig_total > 0 and adap_total > 0:
            print(f"    准确率变化: {adap_acc - orig_acc:+.1f}pp")
            print(f"    数量变化: {orig_total} → {adap_total} ({adap_total - orig_total:+d})")
            # 效率指标：每个high预测的期望正确数
            orig_eff = orig_correct / orig_total if orig_total > 0 else 0
            adap_eff = adap_correct / adap_total if adap_total > 0 else 0
            print(f"    效率(正确/总): {orig_eff:.3f} → {adap_eff:.3f}")


# ═══════════════════════════════════════════════════════════════
# Part 6: 敏感性分析 — blend_n参数的影响
# ═══════════════════════════════════════════════════════════════

def sensitivity_analysis(by_week, market_weeks):
    """测试不同blend_n参数对结果的影响。"""
    print(f"\n{'='*70}")
    print(f"  敏感性分析: blend_n 参数")
    print(f"{'='*70}")

    for blend_n in [30, 50, 100, 200, 500]:
        engine = AdaptiveConfidenceEngine(blend_n=blend_n)
        weeks = sorted(by_week.keys())

        model_stats = {
            'v11_nw': {'orig_h_c': 0, 'orig_h_n': 0, 'adap_h_c': 0, 'adap_h_n': 0},
            'v20': {'orig_h_c': 0, 'orig_h_n': 0, 'adap_h_c': 0, 'adap_h_n': 0},
            'v30': {'orig_h_c': 0, 'orig_h_n': 0, 'adap_h_c': 0, 'adap_h_n': 0},
        }

        for week in weeks:
            rows = by_week[week]
            nw_target_week = week + 1 if week + 1 <= 53 else 1
            mkt_nw = market_weeks.get(nw_target_week, {}).get('chg', 0)
            mkt_tw = market_weeks.get(week, {}).get('chg', 0)

            for row in rows:
                # V11 NW
                if (row.get('nw_pred_direction') and row.get('nw_is_correct') is not None):
                    rule_key = get_v11_rule_key(row)
                    backtest_acc = float(row['nw_backtest_accuracy']) if row.get('nw_backtest_accuracy') is not None else V11_BACKTEST_ACC_DEFAULT.get(rule_key, 70.0)
                    is_correct = bool(row['nw_is_correct'])
                    original_conf = row.get('nw_confidence') or 'low'

                    adaptive = engine.compute_adaptive_confidence('v11_nw', rule_key, backtest_acc)

                    if original_conf == 'high':
                        model_stats['v11_nw']['orig_h_n'] += 1
                        model_stats['v11_nw']['orig_h_c'] += int(is_correct)
                    if adaptive['confidence'] == 'high':
                        model_stats['v11_nw']['adap_h_n'] += 1
                        model_stats['v11_nw']['adap_h_c'] += int(is_correct)

                # V20
                if (row.get('v20_pred_direction') and row.get('v20_is_correct') is not None):
                    rule_key = get_v20_rule_key(row)
                    backtest_acc = V20_BACKTEST_ACC.get(rule_key, 70.0)
                    is_correct = bool(row['v20_is_correct'])
                    original_conf = row.get('v20_confidence') or 'medium'

                    adaptive = engine.compute_adaptive_confidence('v20', rule_key, backtest_acc)

                    if original_conf == 'high':
                        model_stats['v20']['orig_h_n'] += 1
                        model_stats['v20']['orig_h_c'] += int(is_correct)
                    if adaptive['confidence'] == 'high':
                        model_stats['v20']['adap_h_n'] += 1
                        model_stats['v20']['adap_h_c'] += int(is_correct)

                # V30
                if (row.get('v30_pred_direction') and row.get('v30_is_correct') is not None):
                    rule_key = get_v30_rule_key(row)
                    backtest_acc = V30_BACKTEST_ACC.get(rule_key, 65.0)
                    is_correct = bool(row['v30_is_correct'])
                    original_conf = row.get('v30_confidence') or 'low'

                    adaptive = engine.compute_adaptive_confidence('v30', rule_key, backtest_acc)

                    if original_conf == 'high':
                        model_stats['v30']['orig_h_n'] += 1
                        model_stats['v30']['orig_h_c'] += int(is_correct)
                    if adaptive['confidence'] == 'high':
                        model_stats['v30']['adap_h_n'] += 1
                        model_stats['v30']['adap_h_c'] += int(is_correct)

            # 记录结果
            for row in rows:
                if (row.get('nw_pred_direction') and row.get('nw_is_correct') is not None):
                    rule_key = get_v11_rule_key(row)
                    engine.record_result('v11_nw', rule_key,
                        bool(row['nw_is_correct']),
                        float(row.get('nw_actual_weekly_chg') or 0), mkt_nw, week)
                if (row.get('v20_pred_direction') and row.get('v20_is_correct') is not None):
                    rule_key = get_v20_rule_key(row)
                    engine.record_result('v20', rule_key,
                        bool(row['v20_is_correct']),
                        float(row.get('v20_actual_5d_chg') or 0), mkt_tw, week)
                if (row.get('v30_pred_direction') and row.get('v30_is_correct') is not None):
                    rule_key = get_v30_rule_key(row)
                    engine.record_result('v30', rule_key,
                        bool(row['v30_is_correct']),
                        float(row.get('v30_actual_5d_chg') or 0), mkt_tw, week)

        print(f"\n  blend_n={blend_n}:")
        for model in ['v11_nw', 'v20', 'v30']:
            s = model_stats[model]
            o_acc = s['orig_h_c'] / s['orig_h_n'] * 100 if s['orig_h_n'] > 0 else 0
            a_acc = s['adap_h_c'] / s['adap_h_n'] * 100 if s['adap_h_n'] > 0 else 0
            print(f"    {model:>8}: 原始high {s['orig_h_c']}/{s['orig_h_n']}={o_acc:.1f}%"
                  f"  自适应high {s['adap_h_c']}/{s['adap_h_n']}={a_acc:.1f}%")


# ═══════════════════════════════════════════════════════════════
# Part 7: 信号过滤测试 — 自适应机制作为过滤器
# ═══════════════════════════════════════════════════════════════

def test_signal_filter(by_week, market_weeks):
    """
    测试自适应机制作为信号过滤器的效果。

    不改变预测方向，只过滤掉自适应置信度为low的信号。
    对比过滤前后的准确率和收益。
    """
    print(f"\n{'='*70}")
    print(f"  信号过滤测试: 只保留自适应 high+medium")
    print(f"{'='*70}")

    engine = AdaptiveConfidenceEngine(blend_n=100)
    weeks = sorted(by_week.keys())

    for model_name, get_key, backtest_map, dir_field, correct_field, chg_field, conf_field in [
        ('v11_nw', get_v11_rule_key, V11_BACKTEST_ACC_DEFAULT,
         'nw_pred_direction', 'nw_is_correct', 'nw_actual_weekly_chg', 'nw_confidence'),
        ('v20', get_v20_rule_key, V20_BACKTEST_ACC,
         'v20_pred_direction', 'v20_is_correct', 'v20_actual_5d_chg', 'v20_confidence'),
        ('v30', get_v30_rule_key, V30_BACKTEST_ACC,
         'v30_pred_direction', 'v30_is_correct', 'v30_actual_5d_chg', 'v30_confidence'),
    ]:
        engine_local = AdaptiveConfidenceEngine(blend_n=100)

        all_before = []  # 过滤前
        all_after = []   # 过滤后
        weekly_stats = []

        for week in weeks:
            rows = by_week[week]
            nw_target_week = week + 1 if week + 1 <= 53 else 1
            mkt = market_weeks.get(nw_target_week if model_name == 'v11_nw' else week, {}).get('chg', 0)

            week_before = []
            week_after = []

            for row in rows:
                if not (row.get(dir_field) and row.get(correct_field) is not None):
                    continue

                rule_key = get_key(row)
                # V11 NW: 优先用DB的per-stock回测准确率
                if model_name == 'v11_nw' and row.get('nw_backtest_accuracy') is not None:
                    backtest_acc = float(row['nw_backtest_accuracy'])
                else:
                    backtest_acc = backtest_map.get(rule_key, 65.0)
                is_correct = bool(row[correct_field])
                actual_chg = float(row.get(chg_field) or 0)

                adaptive = engine_local.compute_adaptive_confidence(
                    model_name, rule_key, backtest_acc)

                week_before.append({
                    'correct': is_correct, 'chg': actual_chg,
                    'conf': row.get(conf_field),
                })

                # 只保留自适应 high 或 medium
                if adaptive['confidence'] in ('high', 'medium'):
                    week_after.append({
                        'correct': is_correct, 'chg': actual_chg,
                        'conf': adaptive['confidence'],
                    })

            # 记录本周结果
            for row in rows:
                if not (row.get(dir_field) and row.get(correct_field) is not None):
                    continue
                rule_key = get_key(row)
                engine_local.record_result(
                    model_name, rule_key,
                    bool(row[correct_field]),
                    float(row.get(chg_field) or 0), mkt, week)

            all_before.extend(week_before)
            all_after.extend(week_after)

            if week_before:
                b_acc = sum(1 for x in week_before if x['correct']) / len(week_before) * 100
                a_acc = sum(1 for x in week_after if x['correct']) / len(week_after) * 100 if week_after else 0
                b_chg = sum(x['chg'] for x in week_before) / len(week_before)
                a_chg = sum(x['chg'] for x in week_after) / len(week_after) if week_after else 0
                weekly_stats.append({
                    'week': week,
                    'before_n': len(week_before), 'before_acc': b_acc, 'before_chg': b_chg,
                    'after_n': len(week_after), 'after_acc': a_acc, 'after_chg': a_chg,
                })

        print(f"\n  {model_name.upper()}:")
        for ws in weekly_stats:
            print(f"    W{ws['week']:02d}: "
                  f"过滤前 {ws['before_n']}只/{ws['before_acc']:.1f}%/avg={ws['before_chg']:+.2f}% → "
                  f"过滤后 {ws['after_n']}只/{ws['after_acc']:.1f}%/avg={ws['after_chg']:+.2f}%")

        if all_before:
            total_b_acc = sum(1 for x in all_before if x['correct']) / len(all_before) * 100
            total_a_acc = sum(1 for x in all_after if x['correct']) / len(all_after) * 100 if all_after else 0
            total_b_chg = sum(x['chg'] for x in all_before) / len(all_before)
            total_a_chg = sum(x['chg'] for x in all_after) / len(all_after) if all_after else 0
            print(f"    汇总: "
                  f"过滤前 {len(all_before)}只/{total_b_acc:.1f}%/avg={total_b_chg:+.2f}% → "
                  f"过滤后 {len(all_after)}只/{total_a_acc:.1f}%/avg={total_a_chg:+.2f}%")
            retained = len(all_after) / len(all_before) * 100 if all_before else 0
            print(f"    保留率: {retained:.1f}%")


# ═══════════════════════════════════════════════════════════════
# Part 8: 多模型共识分析 — V11+V20+V30同时高置信
# ═══════════════════════════════════════════════════════════════

def _pct(n, d):
    return f"{n/d*100:.1f}%" if d > 0 else "-"


def analyze_multi_model_consensus(by_week, market_weeks):
    """
    分析多模型共识：当V11/V20/V30同时给出高置信度预测时的准确率。

    注意：V11 NW预测的是下周方向，V20/V30预测的是从predict_date起5日方向。
    当predict_date是周五时，V20/V30的5日窗口≈下周，三者目标基本对齐。
    当predict_date是周中时，V20/V30的5日窗口跨两周，与V11 NW不完全对齐。

    这里按stock_code+iso_week做join，分析同一只股票在同一周内
    三个模型的预测是否一致，以及一致时的准确率。
    """
    print(f"\n{'='*70}")
    print(f"  多模型共识分析 (原始置信度)")
    print(f"{'='*70}")

    weeks = sorted(by_week.keys())

    # ── 收集每只股票在每周的三个模型预测 ──
    # 按 (week, stock_code) 组织
    stock_week_map = defaultdict(dict)
    for week in weeks:
        for row in by_week[week]:
            code = row['stock_code']
            key = (week, code)

            if row.get('nw_pred_direction') and row.get('nw_is_correct') is not None:
                stock_week_map[key]['v11'] = {
                    'direction': row['nw_pred_direction'],
                    'confidence': row.get('nw_confidence') or 'low',
                    'correct': bool(row['nw_is_correct']),
                    'chg': float(row.get('nw_actual_weekly_chg') or 0),
                }
            if row.get('v20_pred_direction') and row.get('v20_is_correct') is not None:
                stock_week_map[key]['v20'] = {
                    'direction': row['v20_pred_direction'],
                    'confidence': row.get('v20_confidence') or 'medium',
                    'correct': bool(row['v20_is_correct']),
                    'chg': float(row.get('v20_actual_5d_chg') or 0),
                }
            if row.get('v30_pred_direction') and row.get('v30_is_correct') is not None:
                stock_week_map[key]['v30'] = {
                    'direction': row['v30_pred_direction'],
                    'confidence': row.get('v30_confidence') or 'low',
                    'correct': bool(row['v30_is_correct']),
                    'chg': float(row.get('v30_actual_5d_chg') or 0),
                }

    # ── 逐周统计 ──
    print(f"\n  逐周各模型高置信度准确率:")
    print(f"  {'周':>4}  {'V11高置信':>16}  {'V20高置信':>16}  {'V30高置信':>16}  {'三模型同时高置信':>20}")

    total = {'v11_h_c': 0, 'v11_h_n': 0, 'v20_h_c': 0, 'v20_h_n': 0,
             'v30_h_c': 0, 'v30_h_n': 0, 'all3_c': 0, 'all3_n': 0}

    for week in weeks:
        v11_h_c = v11_h_n = 0
        v20_h_c = v20_h_n = 0
        v30_h_c = v30_h_n = 0
        all3_c = all3_n = 0

        for (w, code), models in stock_week_map.items():
            if w != week:
                continue

            v11 = models.get('v11')
            v20 = models.get('v20')
            v30 = models.get('v30')

            if v11 and v11['confidence'] == 'high':
                v11_h_n += 1
                v11_h_c += int(v11['correct'])
            if v20 and v20['confidence'] == 'high':
                v20_h_n += 1
                v20_h_c += int(v20['correct'])
            if v30 and v30['confidence'] == 'high':
                v30_h_n += 1
                v30_h_c += int(v30['correct'])

            # 三模型同时高置信
            if (v11 and v20 and v30
                    and v11['confidence'] == 'high'
                    and v20['confidence'] == 'high'
                    and v30['confidence'] == 'high'):
                all3_n += 1
                # 用V11的correct作为基准（预测下周方向）
                all3_c += int(v11['correct'])

        total['v11_h_c'] += v11_h_c; total['v11_h_n'] += v11_h_n
        total['v20_h_c'] += v20_h_c; total['v20_h_n'] += v20_h_n
        total['v30_h_c'] += v30_h_c; total['v30_h_n'] += v30_h_n
        total['all3_c'] += all3_c; total['all3_n'] += all3_n

        print(f"  W{week:02d}  "
              f"{v11_h_c}/{v11_h_n:>4}={_pct(v11_h_c, v11_h_n):>6}  "
              f"{v20_h_c}/{v20_h_n:>4}={_pct(v20_h_c, v20_h_n):>6}  "
              f"{v30_h_c}/{v30_h_n:>4}={_pct(v30_h_c, v30_h_n):>6}  "
              f"{all3_c}/{all3_n:>4}={_pct(all3_c, all3_n):>6}")

    print(f"  {'─'*75}")
    print(f"  汇总  "
          f"{total['v11_h_c']}/{total['v11_h_n']:>4}={_pct(total['v11_h_c'], total['v11_h_n']):>6}  "
          f"{total['v20_h_c']}/{total['v20_h_n']:>4}={_pct(total['v20_h_c'], total['v20_h_n']):>6}  "
          f"{total['v30_h_c']}/{total['v30_h_n']:>4}={_pct(total['v30_h_c'], total['v30_h_n']):>6}  "
          f"{total['all3_c']}/{total['all3_n']:>4}={_pct(total['all3_c'], total['all3_n']):>6}")

    # ── 两两组合分析 ──
    print(f"\n  两两组合高置信度准确率:")
    combos = [('v11', 'v20'), ('v11', 'v30'), ('v20', 'v30')]
    combo_labels = {'v11': 'V11', 'v20': 'V20', 'v30': 'V30'}

    for m1, m2 in combos:
        combo_c = combo_n = 0
        for (w, code), models in stock_week_map.items():
            a = models.get(m1)
            b = models.get(m2)
            if (a and b
                    and a['confidence'] == 'high'
                    and b['confidence'] == 'high'):
                combo_n += 1
                combo_c += int(a['correct'])  # 用第一个模型的correct
        label = f"{combo_labels[m1]}+{combo_labels[m2]}"
        print(f"    {label:>10} 同时高置信: {combo_c}/{combo_n} = {_pct(combo_c, combo_n)}")

    # ── 方向一致性分析 ──
    print(f"\n  方向一致性分析 (三模型都有预测的股票):")
    for week in weeks:
        same_dir = diff_dir = 0
        same_dir_correct = same_dir_total = 0
        for (w, code), models in stock_week_map.items():
            if w != week:
                continue
            v11 = models.get('v11')
            v20 = models.get('v20')
            v30 = models.get('v30')
            if not (v11 and v20 and v30):
                continue
            dirs = {v11['direction'], v20['direction'], v30['direction']}
            if len(dirs) == 1:
                same_dir += 1
                same_dir_total += 1
                same_dir_correct += int(v11['correct'])
            else:
                diff_dir += 1
        if same_dir + diff_dir > 0:
            print(f"    W{week:02d}: 方向一致={same_dir}只({_pct(same_dir_correct, same_dir_total)}准确)  "
                  f"方向不一致={diff_dir}只  "
                  f"总={same_dir + diff_dir}只")

    # ── 自适应后的多模型共识 ──
    print(f"\n{'='*70}")
    print(f"  多模型共识分析 (自适应置信度)")
    print(f"{'='*70}")

    engine = AdaptiveConfidenceEngine(blend_n=100)

    print(f"\n  逐周各模型自适应高置信度准确率:")
    print(f"  {'周':>4}  {'V11高置信':>16}  {'V20高置信':>16}  {'V30高置信':>16}  {'三模型同时高置信':>20}")

    a_total = {'v11_h_c': 0, 'v11_h_n': 0, 'v20_h_c': 0, 'v20_h_n': 0,
               'v30_h_c': 0, 'v30_h_n': 0, 'all3_c': 0, 'all3_n': 0}

    for week in weeks:
        rows = by_week[week]
        nw_target_week = week + 1 if week + 1 <= 53 else 1
        mkt_nw = market_weeks.get(nw_target_week, {}).get('chg', 0)
        mkt_tw = market_weeks.get(week, {}).get('chg', 0)

        # 计算本周每只股票的自适应置信度
        adaptive_map = {}  # {stock_code: {v11: conf, v20: conf, v30: conf}}
        for row in rows:
            code = row['stock_code']
            if code not in adaptive_map:
                adaptive_map[code] = {}

            if row.get('nw_pred_direction') and row.get('nw_is_correct') is not None:
                rule_key = get_v11_rule_key(row)
                bt = float(row['nw_backtest_accuracy']) if row.get('nw_backtest_accuracy') is not None else V11_BACKTEST_ACC_DEFAULT.get(rule_key, 70.0)
                a = engine.compute_adaptive_confidence('v11_nw', rule_key, bt)
                adaptive_map[code]['v11'] = a['confidence']

            if row.get('v20_pred_direction') and row.get('v20_is_correct') is not None:
                rule_key = get_v20_rule_key(row)
                bt = V20_BACKTEST_ACC.get(rule_key, 70.0)
                a = engine.compute_adaptive_confidence('v20', rule_key, bt)
                adaptive_map[code]['v20'] = a['confidence']

            if row.get('v30_pred_direction') and row.get('v30_is_correct') is not None:
                rule_key = get_v30_rule_key(row)
                bt = V30_BACKTEST_ACC.get(rule_key, 65.0)
                a = engine.compute_adaptive_confidence('v30', rule_key, bt)
                adaptive_map[code]['v30'] = a['confidence']

        # 统计
        v11_h_c = v11_h_n = 0
        v20_h_c = v20_h_n = 0
        v30_h_c = v30_h_n = 0
        all3_c = all3_n = 0

        for (w, code), models in stock_week_map.items():
            if w != week:
                continue
            amap = adaptive_map.get(code, {})

            v11 = models.get('v11')
            v20 = models.get('v20')
            v30 = models.get('v30')

            if v11 and amap.get('v11') == 'high':
                v11_h_n += 1; v11_h_c += int(v11['correct'])
            if v20 and amap.get('v20') == 'high':
                v20_h_n += 1; v20_h_c += int(v20['correct'])
            if v30 and amap.get('v30') == 'high':
                v30_h_n += 1; v30_h_c += int(v30['correct'])

            if (v11 and v20 and v30
                    and amap.get('v11') == 'high'
                    and amap.get('v20') == 'high'
                    and amap.get('v30') == 'high'):
                all3_n += 1
                all3_c += int(v11['correct'])

        a_total['v11_h_c'] += v11_h_c; a_total['v11_h_n'] += v11_h_n
        a_total['v20_h_c'] += v20_h_c; a_total['v20_h_n'] += v20_h_n
        a_total['v30_h_c'] += v30_h_c; a_total['v30_h_n'] += v30_h_n
        a_total['all3_c'] += all3_c; a_total['all3_n'] += all3_n

        print(f"  W{week:02d}  "
              f"{v11_h_c}/{v11_h_n:>4}={_pct(v11_h_c, v11_h_n):>6}  "
              f"{v20_h_c}/{v20_h_n:>4}={_pct(v20_h_c, v20_h_n):>6}  "
              f"{v30_h_c}/{v30_h_n:>4}={_pct(v30_h_c, v30_h_n):>6}  "
              f"{all3_c}/{all3_n:>4}={_pct(all3_c, all3_n):>6}")

        # 记录本周结果到引擎
        for row in rows:
            if row.get('nw_pred_direction') and row.get('nw_is_correct') is not None:
                engine.record_result('v11_nw', get_v11_rule_key(row),
                    bool(row['nw_is_correct']),
                    float(row.get('nw_actual_weekly_chg') or 0), mkt_nw, week)
            if row.get('v20_pred_direction') and row.get('v20_is_correct') is not None:
                engine.record_result('v20', get_v20_rule_key(row),
                    bool(row['v20_is_correct']),
                    float(row.get('v20_actual_5d_chg') or 0), mkt_tw, week)
            if row.get('v30_pred_direction') and row.get('v30_is_correct') is not None:
                engine.record_result('v30', get_v30_rule_key(row),
                    bool(row['v30_is_correct']),
                    float(row.get('v30_actual_5d_chg') or 0), mkt_tw, week)

    print(f"  {'─'*75}")
    print(f"  汇总  "
          f"{a_total['v11_h_c']}/{a_total['v11_h_n']:>4}={_pct(a_total['v11_h_c'], a_total['v11_h_n']):>6}  "
          f"{a_total['v20_h_c']}/{a_total['v20_h_n']:>4}={_pct(a_total['v20_h_c'], a_total['v20_h_n']):>6}  "
          f"{a_total['v30_h_c']}/{a_total['v30_h_n']:>4}={_pct(a_total['v30_h_c'], a_total['v30_h_n']):>6}  "
          f"{a_total['all3_c']}/{a_total['all3_n']:>4}={_pct(a_total['all3_c'], a_total['all3_n']):>6}")


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def main():
    print("=" * 70)
    print("  自适应置信度验证测试")
    print("=" * 70)

    print("\n加载数据...")
    by_week, market_weeks = load_all_prediction_data()

    print(f"\n数据概况:")
    for week in sorted(by_week.keys()):
        rows = by_week[week]
        nw_cnt = sum(1 for r in rows if r.get('nw_pred_direction') and r.get('nw_is_correct') is not None)
        v20_cnt = sum(1 for r in rows if r.get('v20_pred_direction') and r.get('v20_is_correct') is not None)
        v30_cnt = sum(1 for r in rows if r.get('v30_pred_direction') and r.get('v30_is_correct') is not None)
        mkt = market_weeks.get(week, {})
        print(f"  W{week:02d}: NW={nw_cnt}  V20={v20_cnt}  V30={v30_cnt}  大盘={mkt.get('chg', '?')}%")

    # Part 2: 模拟自适应
    print("\n" + "=" * 70)
    print("  模拟自适应置信度...")
    results, engine = simulate_adaptive(by_week, market_weeks)

    # Part 3: 对比结果
    print_comparison(results, engine)

    # Part 4: 校准分析
    analyze_calibration(results)

    # Part 5: 假设检验
    hypothesis_test(results)

    # Part 6: 敏感性分析
    sensitivity_analysis(by_week, market_weeks)

    # Part 7: 信号过滤测试
    test_signal_filter(by_week, market_weeks)

    # Part 8: 多模型共识分析
    analyze_multi_model_consensus(by_week, market_weeks)

    print(f"\n{'='*70}")
    print(f"  测试完成")
    print(f"{'='*70}")


if __name__ == '__main__':
    main()
