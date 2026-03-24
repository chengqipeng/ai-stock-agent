#!/usr/bin/env python3
"""
V12 过拟合深度诊断
==================
系统性检测V12预测引擎中的过拟合风险。

诊断维度：
  1. 阈值审计：逐一审查所有硬编码阈值的来源（学术 vs 数据拟合）
  2. 循环推理检测：哪些阈值是用同一份数据设定又验证的？
  3. 时间序列交叉验证：滚动窗口OOS测试
  4. 参数敏感性分析：阈值扰动±20%后准确率变化
  5. 时间稳定性：5折时间交叉验证
  6. 样本量充分性：每周预测数的统计显著性
  7. 选择偏差：200股 vs 5000股性能差距
  8. 市场环境依赖：按大盘涨跌分组的准确率

用法：
    source .venv/bin/activate
    python -m tools.analyze_v12_overfitting
"""
import json
import math
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dao import get_connection
from service.v12_prediction.v12_engine import V12PredictionEngine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent.parent / "data_results"


def _to_float(v):
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


# ═══════════════════════════════════════════════════════════
# 数据加载
# ═══════════════════════════════════════════════════════════

def load_stock_codes(limit=5000):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT stock_code, COUNT(*) AS cnt
        FROM stock_kline
        WHERE stock_code NOT LIKE '%%.BJ'
        GROUP BY stock_code
        HAVING cnt >= 120
        ORDER BY cnt DESC
        LIMIT %s
    """, (limit,))
    codes = [r['stock_code'] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return codes


def load_kline_data(stock_codes, start_date, end_date):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = defaultdict(list)
    bs = 300
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, close_price, open_price, high_price, "
            f"low_price, trading_volume, change_percent, change_hand "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [start_date, end_date])
        for row in cur.fetchall():
            result[row['stock_code']].append({
                'date': str(row['date']),
                'close': _to_float(row['close_price']),
                'open': _to_float(row['open_price']),
                'high': _to_float(row['high_price']),
                'low': _to_float(row['low_price']),
                'volume': _to_float(row['trading_volume']),
                'change_percent': _to_float(row['change_percent']),
                'turnover': _to_float(row.get('change_hand')),
            })
    cur.close()
    conn.close()
    return dict(result)


def load_fund_flow_data(stock_codes, start_date):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = defaultdict(list)
    bs = 300
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, big_net_pct, net_flow, main_net_5day "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s ORDER BY `date` DESC",
            batch + [start_date])
        for row in cur.fetchall():
            result[row['stock_code']].append({
                'date': str(row['date']),
                'big_net_pct': _to_float(row.get('big_net_pct')),
                'net_flow': _to_float(row.get('net_flow')),
                'main_net_5day': _to_float(row.get('main_net_5day')),
            })
    cur.close()
    conn.close()
    return dict(result)


def load_market_klines(start_date, end_date):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute(
        "SELECT `date`, close_price, change_percent "
        "FROM stock_kline WHERE stock_code = '000001.SH' "
        "AND `date` >= %s AND `date` <= %s ORDER BY `date`",
        (start_date, end_date))
    result = []
    for row in cur.fetchall():
        result.append({
            'date': str(row['date']),
            'close': _to_float(row.get('close_price')),
            'change_percent': _to_float(row['change_percent']),
        })
    cur.close()
    conn.close()
    return result


def group_by_week(klines):
    weeks = defaultdict(list)
    for k in klines:
        d = datetime.strptime(k['date'][:10], '%Y-%m-%d')
        iso = d.isocalendar()
        key = f"{iso[0]}-W{iso[1]:02d}"
        weeks[key].append(k)
    return dict(weeks)


# ═══════════════════════════════════════════════════════════
# 核心：准备回测数据（复用逻辑）
# ═══════════════════════════════════════════════════════════

def prepare_backtest_data(stock_codes, n_weeks=100):
    """准备回测数据，返回 stock_weekly, all_week_keys, market_klines"""
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=n_weeks * 7 + 120)).strftime('%Y-%m-%d')

    logger.info("加载数据: %d只股票, %s ~ %s", len(stock_codes), start_date, end_date)
    kline_data = load_kline_data(stock_codes, start_date, end_date)
    fund_flow_data = load_fund_flow_data(stock_codes, start_date)
    market_klines = load_market_klines(start_date, end_date)
    logger.info("  K线: %d只, 资金流: %d只, 大盘: %d条",
                len(kline_data), len(fund_flow_data), len(market_klines))

    stock_weekly = {}
    all_week_keys = set()

    for code in stock_codes:
        klines = kline_data.get(code, [])
        if len(klines) < 80:
            continue
        weekly_groups = group_by_week(klines)
        sorted_weeks = sorted(weekly_groups.keys())
        if len(sorted_weeks) < 4:
            continue

        fund_flow = fund_flow_data.get(code, [])
        week_info = {}

        for wi in range(len(sorted_weeks) - 1):
            week_key = sorted_weeks[wi]
            next_week_key = sorted_weeks[wi + 1]
            week_klines = weekly_groups[week_key]
            next_klines = weekly_groups[next_week_key]
            last_date = week_klines[-1]['date']

            idx = None
            for j, k in enumerate(klines):
                if k['date'] == last_date:
                    idx = j
                    break
            if idx is None or idx < 60:
                continue

            base_close = week_klines[-1].get('close', 0)
            end_close = next_klines[-1].get('close', 0)
            if base_close <= 0 or end_close <= 0:
                continue

            actual_return = (end_close / base_close - 1) * 100
            hist_ff = [f for f in fund_flow if f['date'] <= last_date][:20]

            week_info[week_key] = {
                'hist_klines': klines[:idx + 1],
                'hist_ff': hist_ff,
                'actual_return': actual_return,
                'last_date': last_date,
            }
            all_week_keys.add(week_key)

        if week_info:
            stock_weekly[code] = week_info

    return stock_weekly, sorted(all_week_keys), market_klines


def run_predictions_for_weeks(stock_weekly, week_keys, market_klines, engine_factory=None):
    """
    对指定周运行预测，返回 [(week, code, pred, actual_return), ...]
    engine_factory: 可选，返回自定义引擎实例的工厂函数
    """
    results = []
    for week_key in week_keys:
        engine = engine_factory() if engine_factory else V12PredictionEngine()

        # 计算截面中位数
        week_vols = []
        week_turns = []
        for code, week_info in stock_weekly.items():
            if week_key not in week_info:
                continue
            kls = week_info[week_key]['hist_klines']
            if len(kls) >= 20:
                pcts = [k.get('change_percent', 0) or 0 for k in kls]
                recent = pcts[-20:]
                m = sum(recent) / 20
                vol = (sum((p - m) ** 2 for p in recent) / 19) ** 0.5
                week_vols.append(vol)
                turnover_vals = [k.get('turnover', 0) or 0 for k in kls]
                avg_turn = sum(turnover_vals[-20:]) / 20
                week_turns.append(avg_turn)
        vol_median = sorted(week_vols)[len(week_vols) // 2] if week_vols else None
        turn_median = sorted(week_turns)[len(week_turns) // 2] if week_turns else None

        for code, week_info in stock_weekly.items():
            if week_key not in week_info:
                continue
            data = week_info[week_key]
            last_date = data.get('last_date', '')
            mkt_hist = [m for m in market_klines if m['date'] <= last_date] if market_klines else None

            pred = engine.predict_single(code, data['hist_klines'], data['hist_ff'],
                                         mkt_hist, vol_median, turn_median)
            if pred is not None:
                results.append((week_key, code, pred, data['actual_return']))

    return results


def calc_accuracy(results):
    """计算准确率"""
    if not results:
        return 0.0, 0
    correct = sum(1 for _, _, pred, actual in results
                  if (pred['pred_direction'] == 'UP') == (actual > 0))
    return correct / len(results), len(results)


# ═══════════════════════════════════════════════════════════
# 诊断1: 阈值审计 — 逐一审查所有硬编码阈值
# ═══════════════════════════════════════════════════════════

def audit_thresholds():
    """
    审计V12引擎中所有硬编码阈值，分类为：
    - academic: 来自学术文献的标准值
    - statistical: 来自统计学标准（如中位数、2σ）
    - data_derived: 从回测数据中拟合得到（过拟合风险）
    - hybrid: 学术理论+数据验证微调
    """
    thresholds = [
        # Layer 1: 极端条件分类器
        {
            'name': '看涨条件A: 周跌>3% + 尾日跌>2% + 连跌≥3天',
            'values': {'week_chg': -3, 'last_pct': -2, 'consec_down': 3},
            'source': 'academic',
            'reference': 'Chen et al. (2024) + De Bondt & Thaler (1985)',
            'risk': 'LOW',
            'note': '3%/2%/3天是反转文献中常用的极端条件定义，非数据拟合',
        },
        {
            'name': '看涨条件B: 周跌>5% + 尾日跌>2%',
            'values': {'week_chg': -5, 'last_pct': -2},
            'source': 'academic',
            'reference': 'Chen et al. (2023) "Maxing Out Reversals"',
            'risk': 'LOW',
            'note': '5%周跌幅是Chen论文中定义的"极端反转"阈值',
        },
        {
            'name': '看涨条件C: 周跌>3% + RSI<35 + 60日低位<30%',
            'values': {'week_chg': -3, 'rsi': 35, 'price_pos': 0.3},
            'source': 'academic',
            'reference': 'Wilder RSI + George & Hwang (2004)',
            'risk': 'LOW',
            'note': 'RSI 30/70是Wilder原文阈值，35是常见放宽版本',
        },
        {
            'name': '看涨条件D: 周跌>7%',
            'values': {'week_chg': -7},
            'source': 'academic',
            'reference': 'Chen et al. (2023): 极端MAX反转',
            'risk': 'LOW',
            'note': '7%是A股涨跌停附近的极端值',
        },
        {
            'name': '看跌条件E: 周涨>5% + 换手>6% + 高位>75%',
            'values': {'week_chg': 5, 'avg_turn': 6, 'price_pos': 0.75},
            'source': 'hybrid',
            'reference': 'Chen, Wang, Yu (2024) Salience Theory',
            'risk': 'MEDIUM',
            'note': '换手率6%阈值可能受A股特定市场结构影响，非纯学术',
        },
        {
            'name': '看跌条件F: 周涨>5% + 上影线>30% + 换手>5%',
            'values': {'week_chg': 5, 'upper_shadow': 0.3, 'avg_turn': 5},
            'source': 'hybrid',
            'reference': 'S&C K线形态 + 国君191',
            'risk': 'MEDIUM',
            'note': '上影线30%是K线形态分析的经验值，有一定主观性',
        },

        # Layer 2: 过滤和置信度
        {
            'name': 'extreme_score < 5 过滤',
            'values': {'threshold': 5},
            'source': 'data_derived',
            'reference': '代码注释: "50周回测 score_5=66.1%, score_6+=78.0%"',
            'risk': 'HIGH',
            'note': '⚠️ 典型循环推理：阈值5是从回测数据中选出的最优分界点，'
                    '然后在同一数据上验证。这是最严重的过拟合风险。',
        },
        {
            'name': 'DOWN方向 extreme_score < 7 过滤',
            'values': {'threshold': 7},
            'source': 'data_derived',
            'reference': '代码注释: "全量回测验证（5000股×100周）"',
            'risk': 'HIGH',
            'note': '⚠️ 循环推理：阈值7是从全量回测中观察到DOWN方向表现差后设定的，'
                    '然后在同一数据上验证效果。',
        },
        {
            'name': 'n_supporting >= 4 升级置信度',
            'values': {'threshold': 4},
            'source': 'hybrid',
            'reference': 'Microalphas "Combining Weak Predictors" + 多数投票原则',
            'risk': 'MEDIUM',
            'note': '4/5多数投票有理论基础，但升级规则是在200股数据上设计，'
                    '然后在同一数据上验证（76.2% vs 44.0%）。'
                    '5000股验证差距缩小到66.7% vs 55.0%，说明200股结果有过拟合。',
        },
        {
            'name': 'n_supporting == 3 降级high→medium',
            'values': {'threshold': 3},
            'source': 'data_derived',
            'reference': '200股验证: ns=3+协同 53.7%',
            'risk': 'MEDIUM',
            'note': '降级规则基于200股观察结果设计，有一定数据窥探风险',
        },
        {
            'name': 'decline_decel权重从1.5降到0.5',
            'values': {'weight': 0.5},
            'source': 'data_derived',
            'reference': '200股回测: 有dd 53.2% vs 无dd 57.3%',
            'risk': 'MEDIUM',
            'note': '权重调整基于回测观察，但方向合理（降低无效信号权重）',
        },
        {
            'name': '缩量确认: max_vol_ratio < 1.2',
            'values': {'threshold': 1.2},
            'source': 'academic',
            'reference': 'Wyckoff Volume Spread Analysis',
            'risk': 'LOW',
            'note': '1.2倍均量是Wyckoff理论中"无放量"的标准定义',
        },
        {
            'name': 'IVOL惩罚: 2倍中位数',
            'values': {'multiplier': 2},
            'source': 'statistical',
            'reference': '统计学异常值标准 + Ang et al. (2006)',
            'risk': 'LOW',
            'note': '2倍中位数是统计学标准，且当前代码中IVOL惩罚已不影响置信度',
        },
        {
            'name': '偏离显著性: DS > 1.0',
            'values': {'threshold': 1.0},
            'source': 'statistical',
            'reference': '1个标准差 = 统计学标准',
            'risk': 'LOW',
            'note': '1σ是统计学标准，且当前代码中低显著性惩罚已不影响置信度',
        },
        {
            'name': '信号权重: reversal=3.0, rsi=1.5, mf=1.0, ps=1.0, dd=0.5',
            'values': {'reversal': 3.0, 'rsi_reversion': 1.5, 'money_flow': 1.0,
                       'price_structure': 1.0, 'decline_decel': 0.5},
            'source': 'hybrid',
            'reference': '学术文献相对重要性 + 回测微调dd权重',
            'risk': 'MEDIUM',
            'note': 'reversal权重3.0来自SSRN反转文献的核心地位，'
                    'dd从1.5降到0.5是数据驱动的调整',
        },
    ]

    # 统计
    risk_counts = defaultdict(int)
    source_counts = defaultdict(int)
    for t in thresholds:
        risk_counts[t['risk']] += 1
        source_counts[t['source']] += 1

    return {
        'thresholds': thresholds,
        'summary': {
            'total_thresholds': len(thresholds),
            'by_risk': dict(risk_counts),
            'by_source': dict(source_counts),
            'high_risk_items': [t['name'] for t in thresholds if t['risk'] == 'HIGH'],
            'data_derived_items': [t['name'] for t in thresholds if t['source'] == 'data_derived'],
        }
    }


# ═══════════════════════════════════════════════════════════
# 诊断2: 时间序列交叉验证（5折时间CV）
# ═══════════════════════════════════════════════════════════

def temporal_cross_validation(stock_weekly, all_weeks, market_klines, n_folds=5):
    """
    5折时间交叉验证：将100周分成5个20周的时间段，
    分别计算每段的准确率，检查时间稳定性。
    
    过拟合信号：如果不同时间段准确率差异>15pp，说明模型对特定时期过拟合。
    """
    n = len(all_weeks)
    fold_size = n // n_folds
    
    fold_results = []
    for fold_idx in range(n_folds):
        start_idx = fold_idx * fold_size
        end_idx = start_idx + fold_size if fold_idx < n_folds - 1 else n
        fold_weeks = all_weeks[start_idx:end_idx]
        
        results = run_predictions_for_weeks(stock_weekly, fold_weeks, market_klines)
        acc, total = calc_accuracy(results)
        
        # 按置信度分
        high_results = [(w, c, p, a) for w, c, p, a in results if p['confidence'] == 'high']
        high_acc, high_total = calc_accuracy(high_results)
        
        fold_results.append({
            'fold': fold_idx + 1,
            'weeks': f"{fold_weeks[0]} ~ {fold_weeks[-1]}",
            'n_weeks': len(fold_weeks),
            'overall_accuracy': round(acc, 4),
            'total_predictions': total,
            'high_accuracy': round(high_acc, 4),
            'high_total': high_total,
        })
        logger.info("  Fold %d (%s~%s): overall=%.1f%% (%d), high=%.1f%% (%d)",
                     fold_idx + 1, fold_weeks[0], fold_weeks[-1],
                     acc * 100, total, high_acc * 100, high_total)
    
    # 计算稳定性指标
    accs = [f['overall_accuracy'] for f in fold_results]
    high_accs = [f['high_accuracy'] for f in fold_results if f['high_total'] > 0]
    
    acc_range = max(accs) - min(accs) if accs else 0
    acc_std = (sum((a - sum(accs)/len(accs))**2 for a in accs) / len(accs))**0.5 if accs else 0
    high_range = max(high_accs) - min(high_accs) if high_accs else 0
    high_std = (sum((a - sum(high_accs)/len(high_accs))**2 for a in high_accs) / len(high_accs))**0.5 if high_accs else 0
    
    return {
        'folds': fold_results,
        'stability': {
            'overall_range': round(acc_range, 4),
            'overall_std': round(acc_std, 4),
            'overall_mean': round(sum(accs)/len(accs), 4) if accs else 0,
            'high_range': round(high_range, 4),
            'high_std': round(high_std, 4),
            'high_mean': round(sum(high_accs)/len(high_accs), 4) if high_accs else 0,
            'overfitting_signal': acc_range > 0.15,
            'high_overfitting_signal': high_range > 0.15,
        }
    }


# ═══════════════════════════════════════════════════════════
# 诊断3: 滚动窗口OOS测试（Train/Test Split）
# ═══════════════════════════════════════════════════════════

def rolling_window_oos(stock_weekly, all_weeks, market_klines):
    """
    滚动窗口样本外测试：
    - 用前50周作为"训练期"（模型设计参考期）
    - 后50周作为"测试期"（纯样本外）
    - 比较两期的准确率差距
    
    过拟合信号：如果训练期准确率显著高于测试期（>5pp），说明过拟合。
    """
    n = len(all_weeks)
    mid = n // 2
    
    train_weeks = all_weeks[:mid]
    test_weeks = all_weeks[mid:]
    
    train_results = run_predictions_for_weeks(stock_weekly, train_weeks, market_klines)
    test_results = run_predictions_for_weeks(stock_weekly, test_weeks, market_klines)
    
    train_acc, train_n = calc_accuracy(train_results)
    test_acc, test_n = calc_accuracy(test_results)
    
    # 按置信度分
    train_high = [(w, c, p, a) for w, c, p, a in train_results if p['confidence'] == 'high']
    test_high = [(w, c, p, a) for w, c, p, a in test_results if p['confidence'] == 'high']
    train_high_acc, train_high_n = calc_accuracy(train_high)
    test_high_acc, test_high_n = calc_accuracy(test_high)
    
    gap = train_acc - test_acc
    high_gap = train_high_acc - test_high_acc
    
    return {
        'train_period': f"{train_weeks[0]} ~ {train_weeks[-1]}",
        'test_period': f"{test_weeks[0]} ~ {test_weeks[-1]}",
        'train': {
            'overall_accuracy': round(train_acc, 4),
            'total': train_n,
            'high_accuracy': round(train_high_acc, 4),
            'high_total': train_high_n,
        },
        'test': {
            'overall_accuracy': round(test_acc, 4),
            'total': test_n,
            'high_accuracy': round(test_high_acc, 4),
            'high_total': test_high_n,
        },
        'gap': {
            'overall_gap': round(gap, 4),
            'high_gap': round(high_gap, 4),
            'overfitting_signal': gap > 0.05,
            'high_overfitting_signal': high_gap > 0.05,
            'interpretation': (
                '训练期>测试期 → 过拟合风险' if gap > 0.05
                else '测试期>训练期 → 无过拟合（可能欠拟合）' if gap < -0.05
                else '差距<5pp → 过拟合风险低'
            ),
        }
    }


# ═══════════════════════════════════════════════════════════
# 诊断4: 参数敏感性分析
# ═══════════════════════════════════════════════════════════

def parameter_sensitivity(stock_weekly, all_weeks, market_klines):
    """
    参数敏感性分析：对关键阈值做±20%扰动，观察准确率变化。
    
    过拟合信号：如果微小扰动导致准确率大幅变化（>5pp），说明模型对该参数过拟合。
    
    注意：由于V12引擎的阈值是硬编码的，我们通过修改引擎代码的方式无法实现。
    替代方案：直接在预测结果上做后验分析，模拟阈值变化的效果。
    """
    # 基准结果
    base_results = run_predictions_for_weeks(stock_weekly, all_weeks, market_klines)
    base_acc, base_n = calc_accuracy(base_results)
    base_high = [(w, c, p, a) for w, c, p, a in base_results if p['confidence'] == 'high']
    base_high_acc, base_high_n = calc_accuracy(base_high)
    
    sensitivity_tests = []
    
    # 测试1: extreme_score阈值敏感性
    # 当前: score < 5 被过滤。测试: score < 4 和 score < 6
    for threshold_name, filter_fn, desc in [
        ('extreme_score≥4 (放宽)', lambda p: p['extreme_score'] >= 4, '放宽极端分数门槛到4'),
        ('extreme_score≥6 (收紧)', lambda p: p['extreme_score'] >= 6, '收紧极端分数门槛到6'),
        ('extreme_score≥5 (当前)', lambda p: p['extreme_score'] >= 5, '当前设置（基准）'),
    ]:
        filtered = [(w, c, p, a) for w, c, p, a in base_results if filter_fn(p)]
        acc, n = calc_accuracy(filtered)
        sensitivity_tests.append({
            'parameter': threshold_name,
            'description': desc,
            'accuracy': round(acc, 4),
            'total': n,
            'delta_vs_base': round(acc - base_acc, 4),
        })
    
    # 测试2: n_supporting阈值敏感性
    # 当前: n_supporting >= 3。测试不同阈值的效果
    for ns_threshold in [3, 4, 5]:
        filtered = [(w, c, p, a) for w, c, p, a in base_results
                     if p.get('n_supporting', 0) >= ns_threshold]
        acc, n = calc_accuracy(filtered)
        sensitivity_tests.append({
            'parameter': f'n_supporting≥{ns_threshold}',
            'description': f'要求至少{ns_threshold}个信号支持',
            'accuracy': round(acc, 4),
            'total': n,
            'delta_vs_base': round(acc - base_acc, 4),
        })
    
    # 测试3: 大盘协同性的影响
    aligned_only = [(w, c, p, a) for w, c, p, a in base_results if p.get('market_aligned')]
    indep_only = [(w, c, p, a) for w, c, p, a in base_results if not p.get('market_aligned')]
    aligned_acc, aligned_n = calc_accuracy(aligned_only)
    indep_acc, indep_n = calc_accuracy(indep_only)
    sensitivity_tests.append({
        'parameter': 'market_aligned=True only',
        'description': '只保留大盘协同的预测',
        'accuracy': round(aligned_acc, 4),
        'total': aligned_n,
        'delta_vs_base': round(aligned_acc - base_acc, 4),
    })
    sensitivity_tests.append({
        'parameter': 'market_aligned=False only',
        'description': '只保留个股独立的预测',
        'accuracy': round(indep_acc, 4),
        'total': indep_n,
        'delta_vs_base': round(indep_acc - base_acc, 4),
    })
    
    # 测试4: UP vs DOWN方向
    up_only = [(w, c, p, a) for w, c, p, a in base_results if p['pred_direction'] == 'UP']
    down_only = [(w, c, p, a) for w, c, p, a in base_results if p['pred_direction'] == 'DOWN']
    up_acc, up_n = calc_accuracy(up_only)
    down_acc, down_n = calc_accuracy(down_only)
    sensitivity_tests.append({
        'parameter': 'UP方向 only',
        'description': '只看UP方向预测',
        'accuracy': round(up_acc, 4),
        'total': up_n,
        'delta_vs_base': round(up_acc - base_acc, 4),
    })
    sensitivity_tests.append({
        'parameter': 'DOWN方向 only',
        'description': '只看DOWN方向预测',
        'accuracy': round(down_acc, 4),
        'total': down_n,
        'delta_vs_base': round(down_acc - base_acc, 4),
    })
    
    # 测试5: 置信度升级/降级规则的影响
    # 模拟：如果没有n_supporting升级规则，high confidence会怎样？
    # 原始high = extreme_score>=6 + market_aligned
    # 升级后的high还包括 ns>=4 + market_aligned 升级上来的
    strict_high = [(w, c, p, a) for w, c, p, a in base_results
                   if p['extreme_score'] >= 6 and p.get('market_aligned')]
    strict_high_acc, strict_high_n = calc_accuracy(strict_high)
    sensitivity_tests.append({
        'parameter': '原始high（无升级规则）',
        'description': 'extreme_score>=6 + market_aligned，不含ns>=4升级',
        'accuracy': round(strict_high_acc, 4),
        'total': strict_high_n,
        'delta_vs_base': round(strict_high_acc - base_high_acc, 4),
    })
    
    return {
        'base': {
            'overall_accuracy': round(base_acc, 4),
            'total': base_n,
            'high_accuracy': round(base_high_acc, 4),
            'high_total': base_high_n,
        },
        'tests': sensitivity_tests,
    }


# ═══════════════════════════════════════════════════════════
# 诊断5: 统计显著性检验
# ═══════════════════════════════════════════════════════════

def statistical_significance(stock_weekly, all_weeks, market_klines):
    """
    统计显著性分析：
    1. 每周预测数是否足够做统计推断？
    2. 整体准确率是否显著优于随机（50%）？
    3. 高置信度准确率的置信区间
    
    使用二项检验（Binomial Test）的正态近似。
    """
    results = run_predictions_for_weeks(stock_weekly, all_weeks, market_klines)
    
    # 按周统计
    weekly_stats = defaultdict(lambda: {'total': 0, 'correct': 0})
    for week, code, pred, actual in results:
        weekly_stats[week]['total'] += 1
        if (pred['pred_direction'] == 'UP') == (actual > 0):
            weekly_stats[week]['correct'] += 1
    
    # 每周样本量分析
    weekly_n = [v['total'] for v in weekly_stats.values()]
    weeks_below_30 = sum(1 for n in weekly_n if n < 30)
    weeks_below_10 = sum(1 for n in weekly_n if n < 10)
    
    # 整体二项检验
    total_n = len(results)
    total_correct = sum(1 for _, _, p, a in results if (p['pred_direction'] == 'UP') == (a > 0))
    p_hat = total_correct / total_n if total_n > 0 else 0
    # Z-test vs 50%
    se = (0.5 * 0.5 / total_n) ** 0.5 if total_n > 0 else 1
    z_score = (p_hat - 0.5) / se if se > 0 else 0
    # 95% CI
    se_hat = (p_hat * (1 - p_hat) / total_n) ** 0.5 if total_n > 0 else 0
    ci_lower = p_hat - 1.96 * se_hat
    ci_upper = p_hat + 1.96 * se_hat
    
    # 高置信度二项检验
    high_results = [(w, c, p, a) for w, c, p, a in results if p['confidence'] == 'high']
    high_n = len(high_results)
    high_correct = sum(1 for _, _, p, a in high_results if (p['pred_direction'] == 'UP') == (a > 0))
    high_p = high_correct / high_n if high_n > 0 else 0
    high_se = (0.5 * 0.5 / high_n) ** 0.5 if high_n > 0 else 1
    high_z = (high_p - 0.5) / high_se if high_se > 0 else 0
    high_se_hat = (high_p * (1 - high_p) / high_n) ** 0.5 if high_n > 0 else 0
    high_ci_lower = high_p - 1.96 * high_se_hat
    high_ci_upper = high_p + 1.96 * high_se_hat
    
    # 每周准确率的分布
    weekly_accs = [v['correct'] / v['total'] for v in weekly_stats.values() if v['total'] >= 10]
    weekly_mean = sum(weekly_accs) / len(weekly_accs) if weekly_accs else 0
    weekly_std = (sum((a - weekly_mean)**2 for a in weekly_accs) / len(weekly_accs))**0.5 if weekly_accs else 0
    weeks_above_50 = sum(1 for a in weekly_accs if a > 0.5)
    win_rate = weeks_above_50 / len(weekly_accs) if weekly_accs else 0
    
    return {
        'sample_size': {
            'total_predictions': total_n,
            'total_weeks': len(weekly_stats),
            'weeks_below_30_predictions': weeks_below_30,
            'weeks_below_10_predictions': weeks_below_10,
            'median_weekly_n': sorted(weekly_n)[len(weekly_n)//2] if weekly_n else 0,
            'min_weekly_n': min(weekly_n) if weekly_n else 0,
            'max_weekly_n': max(weekly_n) if weekly_n else 0,
        },
        'overall_test': {
            'accuracy': round(p_hat, 4),
            'z_score_vs_50pct': round(z_score, 2),
            'p_value_approx': '< 0.001' if abs(z_score) > 3.29 else '< 0.01' if abs(z_score) > 2.58 else '< 0.05' if abs(z_score) > 1.96 else '> 0.05',
            'ci_95': [round(ci_lower, 4), round(ci_upper, 4)],
            'significant': abs(z_score) > 1.96,
        },
        'high_confidence_test': {
            'accuracy': round(high_p, 4),
            'n': high_n,
            'z_score_vs_50pct': round(high_z, 2),
            'p_value_approx': '< 0.001' if abs(high_z) > 3.29 else '< 0.01' if abs(high_z) > 2.58 else '< 0.05' if abs(high_z) > 1.96 else '> 0.05',
            'ci_95': [round(high_ci_lower, 4), round(high_ci_upper, 4)],
            'significant': abs(high_z) > 1.96,
        },
        'weekly_distribution': {
            'n_valid_weeks': len(weekly_accs),
            'mean_accuracy': round(weekly_mean, 4),
            'std_accuracy': round(weekly_std, 4),
            'weeks_above_50pct': weeks_above_50,
            'weekly_win_rate': round(win_rate, 4),
            'cv': round(weekly_std / weekly_mean, 4) if weekly_mean > 0 else 0,
        }
    }


# ═══════════════════════════════════════════════════════════
# 诊断6: 市场环境依赖分析
# ═══════════════════════════════════════════════════════════

def market_regime_analysis(stock_weekly, all_weeks, market_klines):
    """
    按大盘周涨跌幅分组，分析V12在不同市场环境下的表现。
    
    过拟合信号：如果准确率严重依赖特定市场环境（如只在暴跌后有效），
    说明模型捕捉的是市场环境而非个股信号。
    """
    results = run_predictions_for_weeks(stock_weekly, all_weeks, market_klines)
    
    # 计算每周大盘涨跌
    mkt_weekly = group_by_week(market_klines)
    mkt_week_chg = {}
    sorted_mkt_weeks = sorted(mkt_weekly.keys())
    for i, wk in enumerate(sorted_mkt_weeks):
        kls = mkt_weekly[wk]
        if len(kls) >= 2:
            first_close = kls[0].get('close', 0)
            last_close = kls[-1].get('close', 0)
            if first_close > 0:
                mkt_week_chg[wk] = (last_close / first_close - 1) * 100
    
    # 按大盘环境分组
    regimes = {
        '暴跌(<-3%)': lambda chg: chg < -3,
        '下跌(-3%~-1%)': lambda chg: -3 <= chg < -1,
        '震荡(-1%~+1%)': lambda chg: -1 <= chg <= 1,
        '上涨(+1%~+3%)': lambda chg: 1 < chg <= 3,
        '暴涨(>+3%)': lambda chg: chg > 3,
    }
    
    regime_results = {}
    for regime_name, regime_fn in regimes.items():
        regime_weeks = [wk for wk, chg in mkt_week_chg.items() if regime_fn(chg)]
        regime_preds = [(w, c, p, a) for w, c, p, a in results if w in regime_weeks]
        acc, n = calc_accuracy(regime_preds)
        
        # 高置信度
        high_preds = [(w, c, p, a) for w, c, p, a in regime_preds if p['confidence'] == 'high']
        high_acc, high_n = calc_accuracy(high_preds)
        
        regime_results[regime_name] = {
            'n_weeks': len(regime_weeks),
            'overall_accuracy': round(acc, 4),
            'total_predictions': n,
            'high_accuracy': round(high_acc, 4),
            'high_predictions': high_n,
        }
    
    # 计算环境依赖度：不同环境准确率的标准差
    accs = [v['overall_accuracy'] for v in regime_results.values() if v['total_predictions'] > 0]
    regime_std = (sum((a - sum(accs)/len(accs))**2 for a in accs) / len(accs))**0.5 if accs else 0
    
    return {
        'regimes': regime_results,
        'market_week_changes': {wk: round(chg, 2) for wk, chg in sorted(mkt_week_chg.items())},
        'regime_dependency': {
            'accuracy_std_across_regimes': round(regime_std, 4),
            'high_dependency': regime_std > 0.10,
            'interpretation': (
                '准确率严重依赖市场环境（std>10pp）→ 模型捕捉的是β而非α' if regime_std > 0.10
                else '准确率对市场环境有一定依赖（5-10pp）→ 部分β暴露' if regime_std > 0.05
                else '准确率对市场环境依赖较低（<5pp）→ 主要是α信号'
            ),
        }
    }


# ═══════════════════════════════════════════════════════════
# 诊断7: 数据窥探偏差量化
# ═══════════════════════════════════════════════════════════

def data_snooping_assessment():
    """
    量化数据窥探偏差（Data Snooping Bias）。
    
    基于开发历史，统计"看数据→调参→再验证"的迭代次数。
    每次迭代都增加了过拟合风险。
    
    Harvey, Liu & Zhu (2016 RFS): "...and the Cross-Section of Expected Returns"
    建议对多重检验做Bonferroni校正。
    """
    iterations = [
        {
            'iteration': 1,
            'description': '初始V12设计：极端条件+信号投票',
            'data_used': '200股×50周',
            'changes': '设定extreme_score≥5阈值',
            'risk': 'HIGH — 阈值5是从数据中选出的最优分界点',
        },
        {
            'iteration': 2,
            'description': '市场过滤器改为软降级',
            'data_used': '200股×50周',
            'changes': '硬过滤→置信度降级',
            'risk': 'LOW — 方向性改变，不涉及阈值优化',
        },
        {
            'iteration': 3,
            'description': '添加decline_deceleration信号',
            'data_used': '200股×50周',
            'changes': '新增信号，权重1.5',
            'risk': 'MEDIUM — 信号设计基于理论，但权重可能受数据影响',
        },
        {
            'iteration': 4,
            'description': 'DOWN方向不对称阈值',
            'data_used': '200股×50周 → 5000股×100周',
            'changes': 'DOWN要求score≥7',
            'risk': 'HIGH — 阈值7是从回测数据中观察DOWN表现差后设定',
        },
        {
            'iteration': 5,
            'description': 'IVOL过滤器',
            'data_used': '200股×50周',
            'changes': '2倍中位数惩罚',
            'risk': 'LOW — 2倍中位数是统计学标准',
        },
        {
            'iteration': 6,
            'description': '全量数据验证',
            'data_used': '5000股×100周',
            'changes': '发现200股→5000股性能下降',
            'risk': 'N/A — 纯验证，未改参数',
        },
        {
            'iteration': 7,
            'description': '三个学术质量因子',
            'data_used': '200股 → 5000股',
            'changes': '缩量确认、低换手、偏离显著性',
            'risk': 'LOW — 因子来自学术文献，阈值来自统计标准',
        },
        {
            'iteration': 8,
            'description': 'money_flow数据偏差发现',
            'data_used': '5000股×100周',
            'changes': '发现MF信号集中在后20周',
            'risk': 'N/A — 纯分析，未改参数',
        },
        {
            'iteration': 9,
            'description': 'n_supporting升级/降级规则',
            'data_used': '200股 → 5000股验证',
            'changes': 'ns≥4+aligned升级, ns=3降级',
            'risk': 'MEDIUM — 理论基础(多数投票)，但规则在200股上设计',
        },
        {
            'iteration': 10,
            'description': 'decline_decel降权',
            'data_used': '200股回测',
            'changes': '权重1.5→0.5',
            'risk': 'MEDIUM — 基于200股观察dd无效后调整',
        },
        {
            'iteration': 11,
            'description': '移除无效因子（低显著性、IVOL惩罚）',
            'data_used': '5000股×100周',
            'changes': '移除两个无效的置信度调整',
            'risk': 'MEDIUM — 基于全量数据观察无效后移除',
        },
        {
            'iteration': 12,
            'description': 'DOWN方向置信度降级',
            'data_used': '5000股×100周',
            'changes': 'DOWN: high→medium, medium→low',
            'risk': 'MEDIUM — 基于全量数据DOWN盈亏比倒挂的观察',
        },
    ]
    
    high_risk_count = sum(1 for it in iterations if it['risk'].startswith('HIGH'))
    medium_risk_count = sum(1 for it in iterations if it['risk'].startswith('MEDIUM'))
    total_iterations = len(iterations)
    
    # Bonferroni校正：如果做了N次独立检验，显著性水平应除以N
    # 实际上这些检验不完全独立，但给出一个保守估计
    effective_tests = high_risk_count + medium_risk_count * 0.5
    bonferroni_alpha = 0.05 / max(1, effective_tests)
    
    return {
        'iterations': iterations,
        'summary': {
            'total_iterations': total_iterations,
            'high_risk_iterations': high_risk_count,
            'medium_risk_iterations': medium_risk_count,
            'effective_independent_tests': round(effective_tests, 1),
            'bonferroni_corrected_alpha': round(bonferroni_alpha, 4),
            'required_z_score': round(2.576 if bonferroni_alpha < 0.01 else 1.96, 3),
        }
    }


# ═══════════════════════════════════════════════════════════
# 诊断8: 极端周贡献分析
# ═══════════════════════════════════════════════════════════

def extreme_week_contribution(stock_weekly, all_weeks, market_klines):
    """
    分析极端周（大盘暴跌后反弹）对整体准确率的贡献。
    
    如果去掉最好的N周后准确率大幅下降，说明模型的"准确率"
    主要来自少数极端事件，而非持续稳定的预测能力。
    """
    results = run_predictions_for_weeks(stock_weekly, all_weeks, market_klines)
    
    # 按周统计
    weekly_stats = defaultdict(lambda: {'total': 0, 'correct': 0, 'results': []})
    for week, code, pred, actual in results:
        is_correct = (pred['pred_direction'] == 'UP') == (actual > 0)
        weekly_stats[week]['total'] += 1
        if is_correct:
            weekly_stats[week]['correct'] += 1
        weekly_stats[week]['results'].append((week, code, pred, actual))
    
    # 按准确率排序
    week_accs = []
    for wk, stats in weekly_stats.items():
        if stats['total'] > 0:
            week_accs.append({
                'week': wk,
                'accuracy': stats['correct'] / stats['total'],
                'n': stats['total'],
                'correct': stats['correct'],
            })
    week_accs.sort(key=lambda x: x['accuracy'], reverse=True)
    
    # 去掉最好的3/5/10周后的准确率
    strip_results = {}
    for strip_n in [3, 5, 10]:
        top_weeks = set(w['week'] for w in week_accs[:strip_n])
        remaining = [(w, c, p, a) for w, c, p, a in results if w not in top_weeks]
        acc, n = calc_accuracy(remaining)
        
        # 被去掉的周的统计
        stripped = [(w, c, p, a) for w, c, p, a in results if w in top_weeks]
        stripped_acc, stripped_n = calc_accuracy(stripped)
        
        strip_results[f'去掉最好{strip_n}周'] = {
            'remaining_accuracy': round(acc, 4),
            'remaining_n': n,
            'stripped_accuracy': round(stripped_acc, 4),
            'stripped_n': stripped_n,
            'stripped_weeks': [w['week'] for w in week_accs[:strip_n]],
            'accuracy_drop': round(acc - (sum(1 for _, _, p, a in results if (p['pred_direction'] == 'UP') == (a > 0)) / len(results)), 4) if results else 0,
        }
    
    # 去掉最差的3/5/10周后的准确率
    week_accs_asc = sorted(week_accs, key=lambda x: x['accuracy'])
    for strip_n in [3, 5, 10]:
        bottom_weeks = set(w['week'] for w in week_accs_asc[:strip_n])
        remaining = [(w, c, p, a) for w, c, p, a in results if w not in bottom_weeks]
        acc, n = calc_accuracy(remaining)
        
        stripped = [(w, c, p, a) for w, c, p, a in results if w in bottom_weeks]
        stripped_acc, stripped_n = calc_accuracy(stripped)
        
        strip_results[f'去掉最差{strip_n}周'] = {
            'remaining_accuracy': round(acc, 4),
            'remaining_n': n,
            'stripped_accuracy': round(stripped_acc, 4),
            'stripped_n': stripped_n,
            'stripped_weeks': [w['week'] for w in week_accs_asc[:strip_n]],
        }
    
    # 大量预测周分析（n>200的周）
    big_weeks = [w for w in week_accs if w['n'] > 200]
    big_week_names = set(w['week'] for w in big_weeks)
    big_results = [(w, c, p, a) for w, c, p, a in results if w in big_week_names]
    small_results = [(w, c, p, a) for w, c, p, a in results if w not in big_week_names]
    big_acc, big_n = calc_accuracy(big_results)
    small_acc, small_n = calc_accuracy(small_results)
    
    return {
        'top_10_weeks': week_accs[:10],
        'bottom_10_weeks': week_accs_asc[:10],
        'strip_analysis': strip_results,
        'big_week_analysis': {
            'big_weeks_n200plus': {
                'accuracy': round(big_acc, 4),
                'total': big_n,
                'n_weeks': len(big_weeks),
            },
            'normal_weeks': {
                'accuracy': round(small_acc, 4),
                'total': small_n,
            },
            'interpretation': (
                '大量预测周准确率显著不同 → 模型对极端市场事件敏感'
                if abs(big_acc - small_acc) > 0.05 else
                '大量预测周与正常周准确率接近 → 模型稳定性较好'
            ),
        }
    }


# ═══════════════════════════════════════════════════════════
# 诊断9: money_flow数据可用性偏差
# ═══════════════════════════════════════════════════════════

def money_flow_bias_analysis(stock_weekly, all_weeks, market_klines):
    """
    分析money_flow信号的数据可用性偏差。
    
    已知问题：money_flow数据只在后~20周可用，
    这意味着前80周和后20周的信号组成完全不同。
    """
    results = run_predictions_for_weeks(stock_weekly, all_weeks, market_klines)
    
    # 按是否有money_flow信号分组
    has_mf = []
    no_mf = []
    for w, c, p, a in results:
        sig_names = [s['signal'] for s in p.get('signals', [])]
        if 'money_flow' in sig_names:
            has_mf.append((w, c, p, a))
        else:
            no_mf.append((w, c, p, a))
    
    mf_acc, mf_n = calc_accuracy(has_mf)
    no_mf_acc, no_mf_n = calc_accuracy(no_mf)
    
    # MF信号的周分布
    mf_weeks = defaultdict(int)
    for w, c, p, a in has_mf:
        mf_weeks[w] += 1
    
    # 前50周 vs 后50周的MF信号数
    mid = len(all_weeks) // 2
    first_half_weeks = set(all_weeks[:mid])
    second_half_weeks = set(all_weeks[mid:])
    
    mf_first_half = sum(v for k, v in mf_weeks.items() if k in first_half_weeks)
    mf_second_half = sum(v for k, v in mf_weeks.items() if k in second_half_weeks)
    
    return {
        'with_money_flow': {
            'accuracy': round(mf_acc, 4),
            'total': mf_n,
        },
        'without_money_flow': {
            'accuracy': round(no_mf_acc, 4),
            'total': no_mf_n,
        },
        'distribution': {
            'first_half_mf_signals': mf_first_half,
            'second_half_mf_signals': mf_second_half,
            'concentration_ratio': round(mf_second_half / max(1, mf_first_half + mf_second_half), 4),
        },
        'bias_assessment': {
            'has_bias': mf_second_half > mf_first_half * 3,
            'interpretation': (
                'money_flow数据严重偏向后半段 → 后半段准确率可能被MF信号人为提升'
                if mf_second_half > mf_first_half * 3
                else 'money_flow数据分布相对均匀'
            ),
        }
    }


# ═══════════════════════════════════════════════════════════
# 主函数：运行所有诊断
# ═══════════════════════════════════════════════════════════

def run_overfitting_analysis():
    t0 = time.time()
    logger.info("=" * 70)
    logger.info("V12 过拟合深度诊断")
    logger.info("=" * 70)
    
    # 加载数据（5000股×100周）
    logger.info("[0/9] 加载数据...")
    stock_codes = load_stock_codes(5000)
    stock_weekly, all_weeks, market_klines = prepare_backtest_data(stock_codes, n_weeks=100)
    # 只取最后100周
    if len(all_weeks) > 100:
        all_weeks = all_weeks[-100:]
    logger.info("  数据准备完成: %d只股票, %d周", len(stock_weekly), len(all_weeks))
    
    report = {}
    
    # 诊断1: 阈值审计
    logger.info("[1/9] 阈值审计...")
    report['threshold_audit'] = audit_thresholds()
    logger.info("  完成: %d个阈值, HIGH风险%d个",
                report['threshold_audit']['summary']['total_thresholds'],
                report['threshold_audit']['summary']['by_risk'].get('HIGH', 0))
    
    # 诊断2: 5折时间交叉验证
    logger.info("[2/9] 5折时间交叉验证...")
    report['temporal_cv'] = temporal_cross_validation(stock_weekly, all_weeks, market_klines)
    logger.info("  完成: overall range=%.1f%%, high range=%.1f%%",
                report['temporal_cv']['stability']['overall_range'] * 100,
                report['temporal_cv']['stability']['high_range'] * 100)
    
    # 诊断3: 滚动窗口OOS
    logger.info("[3/9] 滚动窗口OOS测试...")
    report['rolling_oos'] = rolling_window_oos(stock_weekly, all_weeks, market_klines)
    logger.info("  完成: train=%.1f%%, test=%.1f%%, gap=%.1f%%",
                report['rolling_oos']['train']['overall_accuracy'] * 100,
                report['rolling_oos']['test']['overall_accuracy'] * 100,
                report['rolling_oos']['gap']['overall_gap'] * 100)
    
    # 诊断4: 参数敏感性
    logger.info("[4/9] 参数敏感性分析...")
    report['parameter_sensitivity'] = parameter_sensitivity(stock_weekly, all_weeks, market_klines)
    logger.info("  完成: %d项测试", len(report['parameter_sensitivity']['tests']))
    
    # 诊断5: 统计显著性
    logger.info("[5/9] 统计显著性检验...")
    report['statistical_significance'] = statistical_significance(stock_weekly, all_weeks, market_klines)
    logger.info("  完成: z=%.2f, significant=%s",
                report['statistical_significance']['overall_test']['z_score_vs_50pct'],
                report['statistical_significance']['overall_test']['significant'])
    
    # 诊断6: 市场环境依赖
    logger.info("[6/9] 市场环境依赖分析...")
    report['market_regime'] = market_regime_analysis(stock_weekly, all_weeks, market_klines)
    logger.info("  完成: regime std=%.1f%%",
                report['market_regime']['regime_dependency']['accuracy_std_across_regimes'] * 100)
    
    # 诊断7: 数据窥探偏差
    logger.info("[7/9] 数据窥探偏差评估...")
    report['data_snooping'] = data_snooping_assessment()
    logger.info("  完成: %d次迭代, %d次高风险",
                report['data_snooping']['summary']['total_iterations'],
                report['data_snooping']['summary']['high_risk_iterations'])
    
    # 诊断8: 极端周贡献
    logger.info("[8/9] 极端周贡献分析...")
    report['extreme_week'] = extreme_week_contribution(stock_weekly, all_weeks, market_klines)
    logger.info("  完成")
    
    # 诊断9: money_flow偏差
    logger.info("[9/9] money_flow数据偏差分析...")
    report['money_flow_bias'] = money_flow_bias_analysis(stock_weekly, all_weeks, market_klines)
    logger.info("  完成: MF信号集中度=%.1f%%",
                report['money_flow_bias']['distribution']['concentration_ratio'] * 100)
    
    # ═══════════════════════════════════════════════════════════
    # 综合评估
    # ═══════════════════════════════════════════════════════════
    
    overfitting_signals = []
    
    # 检查各诊断的过拟合信号
    if report['temporal_cv']['stability']['overfitting_signal']:
        overfitting_signals.append('时间交叉验证: overall准确率跨折差异>15pp')
    if report['temporal_cv']['stability']['high_overfitting_signal']:
        overfitting_signals.append('时间交叉验证: high准确率跨折差异>15pp')
    if report['rolling_oos']['gap']['overfitting_signal']:
        overfitting_signals.append('OOS测试: 训练期准确率>测试期>5pp')
    if report['rolling_oos']['gap']['high_overfitting_signal']:
        overfitting_signals.append('OOS测试: high训练期>测试期>5pp')
    if report['market_regime']['regime_dependency']['high_dependency']:
        overfitting_signals.append('市场环境: 准确率严重依赖市场环境(std>10pp)')
    if report['money_flow_bias']['bias_assessment']['has_bias']:
        overfitting_signals.append('money_flow: 数据严重偏向后半段')
    if report['threshold_audit']['summary']['by_risk'].get('HIGH', 0) > 0:
        overfitting_signals.append(f"阈值审计: {report['threshold_audit']['summary']['by_risk']['HIGH']}个HIGH风险阈值")
    
    # 综合评分
    n_signals = len(overfitting_signals)
    if n_signals >= 5:
        overall_risk = 'HIGH'
    elif n_signals >= 3:
        overall_risk = 'MEDIUM-HIGH'
    elif n_signals >= 1:
        overall_risk = 'MEDIUM'
    else:
        overall_risk = 'LOW'
    
    report['overall_assessment'] = {
        'overfitting_risk': overall_risk,
        'n_overfitting_signals': n_signals,
        'signals': overfitting_signals,
        'run_time_sec': round(time.time() - t0, 1),
    }
    
    # 保存
    output_path = OUTPUT_DIR / "v12_overfitting_analysis.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    logger.info("\n结果已保存: %s", output_path)
    
    # 打印摘要
    print("\n" + "=" * 70)
    print("V12 过拟合深度诊断 — 综合报告")
    print("=" * 70)
    
    print(f"\n🔴 综合过拟合风险: {overall_risk}")
    print(f"   检测到 {n_signals} 个过拟合信号:")
    for sig in overfitting_signals:
        print(f"   ⚠️  {sig}")
    
    print(f"\n📋 阈值审计:")
    audit = report['threshold_audit']['summary']
    print(f"   总阈值: {audit['total_thresholds']}")
    print(f"   HIGH风险: {audit['by_risk'].get('HIGH', 0)} | MEDIUM: {audit['by_risk'].get('MEDIUM', 0)} | LOW: {audit['by_risk'].get('LOW', 0)}")
    print(f"   数据拟合: {audit['by_source'].get('data_derived', 0)} | 混合: {audit['by_source'].get('hybrid', 0)} | 学术: {audit['by_source'].get('academic', 0)}")
    
    print(f"\n📊 5折时间CV:")
    for fold in report['temporal_cv']['folds']:
        print(f"   Fold{fold['fold']}: {fold['overall_accuracy']:.1%} ({fold['total_predictions']}条) | high: {fold['high_accuracy']:.1%} ({fold['high_total']}条)")
    stab = report['temporal_cv']['stability']
    print(f"   → overall range={stab['overall_range']:.1%}, std={stab['overall_std']:.1%}")
    print(f"   → high range={stab['high_range']:.1%}, std={stab['high_std']:.1%}")
    
    print(f"\n🔄 OOS测试 (前50周 vs 后50周):")
    oos = report['rolling_oos']
    print(f"   训练期: {oos['train']['overall_accuracy']:.1%} ({oos['train']['total']}条) | high: {oos['train']['high_accuracy']:.1%} ({oos['train']['high_total']}条)")
    print(f"   测试期: {oos['test']['overall_accuracy']:.1%} ({oos['test']['total']}条) | high: {oos['test']['high_accuracy']:.1%} ({oos['test']['high_total']}条)")
    print(f"   → gap: {oos['gap']['overall_gap']:.1%} | high gap: {oos['gap']['high_gap']:.1%}")
    print(f"   → {oos['gap']['interpretation']}")
    
    print(f"\n🎛️ 参数敏感性:")
    for test in report['parameter_sensitivity']['tests']:
        delta = test['delta_vs_base']
        marker = '⚠️' if abs(delta) > 0.05 else '  '
        print(f"   {marker} {test['parameter']:30s}: {test['accuracy']:.1%} ({test['total']}条) Δ={delta:+.1%}")
    
    print(f"\n📈 统计显著性:")
    sig = report['statistical_significance']
    print(f"   整体: z={sig['overall_test']['z_score_vs_50pct']:.2f}, p{sig['overall_test']['p_value_approx']}, CI={sig['overall_test']['ci_95']}")
    print(f"   高置信: z={sig['high_confidence_test']['z_score_vs_50pct']:.2f}, p{sig['high_confidence_test']['p_value_approx']}, CI={sig['high_confidence_test']['ci_95']}")
    print(f"   周胜率: {sig['weekly_distribution']['weeks_above_50pct']}/{sig['weekly_distribution']['n_valid_weeks']} ({sig['weekly_distribution']['weekly_win_rate']:.1%})")
    print(f"   周准确率CV: {sig['weekly_distribution']['cv']:.2f}")
    
    print(f"\n🌍 市场环境依赖:")
    for regime, data in report['market_regime']['regimes'].items():
        if data['total_predictions'] > 0:
            print(f"   {regime:20s}: {data['overall_accuracy']:.1%} ({data['total_predictions']}条, {data['n_weeks']}周)")
    print(f"   → {report['market_regime']['regime_dependency']['interpretation']}")
    
    print(f"\n🔬 数据窥探:")
    ds = report['data_snooping']['summary']
    print(f"   迭代次数: {ds['total_iterations']}, HIGH风险: {ds['high_risk_iterations']}, MEDIUM: {ds['medium_risk_iterations']}")
    print(f"   有效独立检验: {ds['effective_independent_tests']}")
    print(f"   Bonferroni校正α: {ds['bonferroni_corrected_alpha']:.4f}")
    
    print(f"\n📅 极端周贡献:")
    for key, val in report['extreme_week']['strip_analysis'].items():
        if '最好' in key:
            print(f"   {key}: 剩余准确率={val['remaining_accuracy']:.1%} ({val['remaining_n']}条)")
    bw = report['extreme_week']['big_week_analysis']
    print(f"   大量预测周(n>200): {bw['big_weeks_n200plus']['accuracy']:.1%} ({bw['big_weeks_n200plus']['total']}条)")
    print(f"   正常周: {bw['normal_weeks']['accuracy']:.1%} ({bw['normal_weeks']['total']}条)")
    
    print(f"\n💰 money_flow偏差:")
    mf = report['money_flow_bias']
    print(f"   有MF信号: {mf['with_money_flow']['accuracy']:.1%} ({mf['with_money_flow']['total']}条)")
    print(f"   无MF信号: {mf['without_money_flow']['accuracy']:.1%} ({mf['without_money_flow']['total']}条)")
    print(f"   MF集中度: 前半段{mf['distribution']['first_half_mf_signals']}条 vs 后半段{mf['distribution']['second_half_mf_signals']}条")
    
    print(f"\n⏱️  总耗时: {time.time() - t0:.1f}秒")
    print("=" * 70)
    
    return report


if __name__ == '__main__':
    run_overfitting_analysis()
