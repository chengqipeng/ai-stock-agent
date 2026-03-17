#!/usr/bin/env python3
"""
时间序列交叉验证回测 — 验证V4规则引擎的样本外表现
==================================================
核心目的：
  V4规则引擎在29周全样本回测中达到80.9%准确率，但存在数据窥探风险。
  本回测使用滚动窗口(expanding window)交叉验证，确保每个预测都是
  基于"过去数据训练、未来数据测试"的真正样本外结果。

方法：
  - 将29周数据分为多个fold
  - 每个fold: 用前N周数据统计规则准确率，在第N+1周测试
  - 最终报告所有测试周的汇总准确率（真正的样本外表现）
  - 对比全样本回测 vs 交叉验证的差距 = 过拟合程度

额外分析：
  1. 阈值敏感性分析：对关键阈值做±20%扰动，观察准确率变化
  2. 上证/深证分市场验证
  3. 成交量信号的样本外验证

用法：
    python -m day_week_predicted.backtest.nw_timeseries_cv_backtest
"""
import sys
import logging
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, '.')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s',
                    datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

from dao import get_connection
from service.weekly_prediction_service import (
    _get_all_stock_codes, _get_latest_trade_date, _to_float,
    _compound_return, _mean, _get_stock_index,
    _detect_volume_patterns, _adjust_nw_confidence_by_volume,
)

N_WEEKS = 29
MIN_TRAIN_WEEKS = 12  # 最少训练周数


# ═══════════════════════════════════════════════════════════
# V4规则集（与生产代码一致，但参数化以支持敏感性分析）
# ═══════════════════════════════════════════════════════════

def build_rules(params=None):
    """构建规则集，支持参数化阈值用于敏感性分析。"""
    p = {
        'r1_mkt_th': -3.0,    # R1: 大盘深跌阈值
        'r1_stk_th': -2.0,    # R1: 个股跌幅阈值
        'r2_stk_th': -5.0,    # R2: 个股跌幅阈值
        'r2_pos_th': 0.7,     # R2: 高位阈值
        'r3_stk_th': -3.0,    # R3: 个股跌幅阈值
        'r3_prev_th': -2.0,   # R3: 前周跌幅阈值
        'r4_stk_th': -3.0,    # R4: 个股跌幅阈值
        'r4_pos_th': 0.2,     # R4: 低位阈值
        'r5_stk_th': -2.0,    # R5: 深证个股跌幅阈值
        'r5a_cd_th': 3,       # R5a: 连跌天数阈值
        'r5b_pos_th': 0.2,    # R5b: 低位阈值
        'r6a_stk_th': 5.0,    # R6a: 深证涨幅阈值
        'r6b_cu_th': 4,       # R6b: 连涨天数阈值
        'r6c_cu_th': 3,       # R6c: 连涨天数阈值
        'r7_stk_th': -3.0,    # R7: 跌幅阈值
        'r7_cu_th': 3,        # R7: 连涨天数阈值
        'r7_pos_th': 0.6,     # R7: 位置阈值
    }
    if params:
        p.update(params)

    return [
        {'name': 'R1:大盘深跌+个股跌→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f, _p=p: f['this_chg'] < _p['r1_stk_th'] and f['mkt_chg'] < _p['r1_mkt_th']},
        {'name': 'R2:上证+大盘跌+跌>5%+非高位→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f, _p=p: (f['this_chg'] < _p['r2_stk_th'] and f['suffix'] == 'SH'
                                   and -3 <= f['mkt_chg'] < -1
                                   and not (f['pos60'] is not None and f['pos60'] >= _p['r2_pos_th']))},
        {'name': 'R3:上证+大盘跌+跌>3%+前周跌→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f, _p=p: (f['this_chg'] < _p['r3_stk_th'] and f['suffix'] == 'SH'
                                   and -3 <= f['mkt_chg'] < -1
                                   and f['prev_chg'] is not None and f['prev_chg'] < _p['r3_prev_th']
                                   and not (f['pos60'] is not None and f['pos60'] >= 0.8))},
        {'name': 'R4:上证+大盘跌+跌>3%+低位→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f, _p=p: (f['this_chg'] < _p['r4_stk_th'] and f['suffix'] == 'SH'
                                   and -3 <= f['mkt_chg'] < -1
                                   and f['pos60'] is not None and f['pos60'] < _p['r4_pos_th'])},
        {'name': 'R5a:深证+大盘微跌+跌+连跌3天→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f, _p=p: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                                   and f['this_chg'] < _p['r5_stk_th'] and f['cd'] >= _p['r5a_cd_th'])},
        {'name': 'R5b:深证+大盘微跌+跌+低位→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f, _p=p: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                                   and f['this_chg'] < _p['r5_stk_th']
                                   and f['pos60'] is not None and f['pos60'] < _p['r5b_pos_th'])},
        {'name': 'R5c:深证+大盘微跌+跌>2%→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f, _p=p: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                                   and f['this_chg'] < _p['r5_stk_th'])},
        {'name': 'R6a:深证+大盘跌+涨>5%→跌', 'pred_up': False, 'tier': 1,
         'check': lambda f, _p=p: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                                   and f['this_chg'] > _p['r6a_stk_th'])},
        {'name': 'R6b:深证+大盘跌+涨+连涨4天→跌', 'pred_up': False, 'tier': 1,
         'check': lambda f, _p=p: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                                   and f['this_chg'] > 2 and f['cu'] >= _p['r6b_cu_th'])},
        {'name': 'R6c:深证+大盘跌+涨+连涨3天→跌', 'pred_up': False, 'tier': 1,
         'check': lambda f, _p=p: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                                   and f['this_chg'] > 2 and f['cu'] >= _p['r6c_cu_th'])},
        {'name': 'R7:跌+前期连涨+非高位→跌', 'pred_up': False, 'tier': 2,
         'check': lambda f, _p=p: (f['this_chg'] < _p['r7_stk_th'] and f['cu'] >= _p['r7_cu_th']
                                   and f['pos60'] is not None and f['pos60'] < _p['r7_pos_th'])},
        {'name': 'R8:上证+大盘微跌+涨+前周跌→跌', 'pred_up': False, 'tier': 2,
         'check': lambda f, _p=p: (f['suffix'] == 'SH' and -1 <= f['mkt_chg'] < 0
                                   and f['this_chg'] > 2
                                   and f['prev_chg'] is not None and f['prev_chg'] < -3)},
    ]


def match_rule(feat, rules):
    """匹配第一条命中的规则。"""
    for rule in rules:
        try:
            if rule['check'](feat):
                return rule
        except (TypeError, KeyError):
            continue
    return None


# ═══════════════════════════════════════════════════════════
# 数据加载
# ═══════════════════════════════════════════════════════════

def load_all_data(n_weeks, sample_limit=0):
    """加载全部数据，返回按股票和周分组的结构。

    Args:
        sample_limit: 限制股票数量（0=全部），用于快速验证
    """
    latest_date = _get_latest_trade_date()
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(n_weeks + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')

    all_codes = _get_all_stock_codes()
    if sample_limit > 0:
        all_codes = all_codes[:sample_limit]
    logger.info("股票数: %d", len(all_codes))

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 加载个股K线
    stock_klines = defaultdict(list)
    bs = 200
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,open_price,close_price,high_price,"
            f"low_price,change_percent,trading_volume "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            stock_klines[r['stock_code']].append({
                'date': r['date'],
                'open': _to_float(r['open_price']),
                'close': _to_float(r['close_price']),
                'high': _to_float(r['high_price']),
                'low': _to_float(r['low_price']),
                'change_percent': _to_float(r['change_percent']),
                'volume': _to_float(r['trading_volume']),
            })
        if (i + bs) % 1000 == 0:
            logger.info("  加载K线进度: %d/%d", min(i + bs, len(all_codes)), len(all_codes))

    # 加载指数K线
    idx_codes = list(set(_get_stock_index(c) for c in all_codes))
    for idx in ('000001.SH', '399001.SZ', '899050.SZ'):
        if idx not in idx_codes:
            idx_codes.append(idx)
    ph = ','.join(['%s'] * len(idx_codes))
    cur.execute(
        f"SELECT stock_code,`date`,change_percent FROM stock_kline "
        f"WHERE stock_code IN ({ph}) AND `date`>=%s AND `date`<=%s ORDER BY `date`",
        idx_codes + [start_date, latest_date])
    market_klines = defaultdict(list)
    for r in cur.fetchall():
        market_klines[r['stock_code']].append({
            'date': r['date'],
            'change_percent': _to_float(r['change_percent']),
        })
    conn.close()

    return {
        'all_codes': all_codes,
        'stock_klines': dict(stock_klines),
        'market_klines': dict(market_klines),
        'latest_date': latest_date,
        'dt_end': dt_end,
    }


def build_weekly_samples(data, n_weeks):
    """将数据组织为按(stock, week)索引的样本列表。"""
    dt_end = data['dt_end']
    dt_cutoff = dt_end - timedelta(days=n_weeks * 7 + 14)
    samples = []

    for code in data['all_codes']:
        klines = data['stock_klines'].get(code, [])
        if not klines or len(klines) < 60:
            continue

        stock_idx = _get_stock_index(code)
        suffix = stock_idx.split('.')[-1] if '.' in stock_idx else ''

        # 按周分组
        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            wg[iw].append(k)

        # 指数按周分组
        mkt_klines = data['market_klines'].get(stock_idx, [])
        mkt_wg = defaultdict(list)
        for k in mkt_klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            mkt_wg[iw].append(k)

        sorted_weeks = sorted(wg.keys())

        for i in range(len(sorted_weeks) - 1):
            iw_this = sorted_weeks[i]
            iw_next = sorted_weeks[i + 1]

            this_days = sorted(wg[iw_this], key=lambda x: x['date'])
            next_days = sorted(wg[iw_next], key=lambda x: x['date'])

            if len(this_days) < 3 or len(next_days) < 3:
                continue

            dt_this = datetime.strptime(this_days[0]['date'], '%Y-%m-%d')
            if dt_this < dt_cutoff:
                continue

            this_pcts = [d['change_percent'] for d in this_days]
            next_pcts = [d['change_percent'] for d in next_days]
            this_chg = _compound_return(this_pcts)
            next_chg = _compound_return(next_pcts)

            mw = mkt_wg.get(iw_this, [])
            mkt_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            # 连涨/连跌
            cd, cu = 0, 0
            for p in reversed(this_pcts):
                if p < 0:
                    cd += 1
                    if cu > 0: break
                elif p > 0:
                    cu += 1
                    if cd > 0: break
                else:
                    break

            # 价格位置
            sorted_all = sorted(klines, key=lambda x: x['date'])
            first_date = this_days[0]['date']
            hist = [k for k in sorted_all if k['date'] < first_date]
            pos60 = None
            if len(hist) >= 20:
                hc = [k.get('close', 0) for k in hist[-60:] if k.get('close', 0) > 0]
                if hc:
                    all_c = hc + [k.get('close', 0) for k in this_days if k.get('close', 0) > 0]
                    mn, mx = min(all_c), max(all_c)
                    lc = this_days[-1].get('close', 0)
                    if mx > mn and lc > 0:
                        pos60 = (lc - mn) / (mx - mn)

            # 前周涨跌幅
            prev_chg = None
            if i > 0:
                prev_iw = sorted_weeks[i - 1]
                prev_days = sorted(wg[prev_iw], key=lambda x: x['date'])
                if len(prev_days) >= 3:
                    prev_chg = _compound_return([d['change_percent'] for d in prev_days])

            samples.append({
                'code': code,
                'iw_this': iw_this,
                'iw_next': iw_next,
                'this_chg': this_chg,
                'mkt_chg': mkt_chg,
                'cd': cd,
                'cu': cu,
                'pos60': pos60,
                'prev_chg': prev_chg,
                'suffix': suffix,
                'last_day': this_pcts[-1] if this_pcts else 0,
                'next_chg': next_chg,
                'actual_up': next_chg >= 0,
                'this_days': this_days,
                'all_klines': klines,
            })

    return samples


# ═══════════════════════════════════════════════════════════
# 核心验证：时间序列交叉验证
# ═══════════════════════════════════════════════════════════

def run_timeseries_cv(samples, n_weeks):
    """滚动窗口交叉验证。

    将29周分为:
      - Fold 1: 训练周1-12, 测试周13
      - Fold 2: 训练周1-13, 测试周14
      - ...
      - Fold 17: 训练周1-28, 测试周29

    每个fold中，用训练集统计各规则的准确率，
    然后在测试集上评估。如果某规则在训练集中准确率<55%，
    则在测试集中不使用该规则（动态规则筛选）。
    """
    # 获取所有唯一的周
    all_weeks_set = sorted(set(s['iw_this'] for s in samples))
    logger.info("总周数: %d, 最小训练周数: %d", len(all_weeks_set), MIN_TRAIN_WEEKS)

    if len(all_weeks_set) < MIN_TRAIN_WEEKS + 1:
        logger.error("数据不足以进行交叉验证")
        return

    rules = build_rules()

    # ── 方案A: 全规则集（不做动态筛选）──
    cv_a_total = 0
    cv_a_correct = 0
    cv_a_by_week = {}
    cv_a_by_rule = defaultdict(lambda: {'correct': 0, 'total': 0})

    # ── 方案B: 动态规则筛选（训练集准确率<55%的规则不用）──
    cv_b_total = 0
    cv_b_correct = 0
    cv_b_by_week = {}

    # ── 方案C: 全样本回测（对照组）──
    full_total = 0
    full_correct = 0
    full_by_rule = defaultdict(lambda: {'correct': 0, 'total': 0})

    # 全样本回测
    for s in samples:
        feat = {k: s[k] for k in ('this_chg', 'mkt_chg', 'cd', 'cu', 'pos60',
                                   'prev_chg', 'suffix', 'last_day')}
        rule = match_rule(feat, rules)
        if rule:
            full_total += 1
            full_by_rule[rule['name']]['total'] += 1
            if rule['pred_up'] == s['actual_up']:
                full_correct += 1
                full_by_rule[rule['name']]['correct'] += 1

    # 滚动窗口交叉验证
    for test_idx in range(MIN_TRAIN_WEEKS, len(all_weeks_set)):
        test_week = all_weeks_set[test_idx]
        train_weeks = set(all_weeks_set[:test_idx])

        train_samples = [s for s in samples if s['iw_this'] in train_weeks]
        test_samples = [s for s in samples if s['iw_this'] == test_week]

        if not test_samples:
            continue

        # 训练集：统计各规则准确率
        train_rule_stats = defaultdict(lambda: {'correct': 0, 'total': 0})
        for s in train_samples:
            feat = {k: s[k] for k in ('this_chg', 'mkt_chg', 'cd', 'cu', 'pos60',
                                       'prev_chg', 'suffix', 'last_day')}
            rule = match_rule(feat, rules)
            if rule:
                train_rule_stats[rule['name']]['total'] += 1
                if rule['pred_up'] == s['actual_up']:
                    train_rule_stats[rule['name']]['correct'] += 1

        # 动态筛选：训练集准确率>=55%且样本>=30的规则
        valid_rules = set()
        for rn, st in train_rule_stats.items():
            if st['total'] >= 30:
                acc = st['correct'] / st['total']
                if acc >= 0.55:
                    valid_rules.add(rn)

        # 测试集评估
        week_a_total, week_a_correct = 0, 0
        week_b_total, week_b_correct = 0, 0

        for s in test_samples:
            feat = {k: s[k] for k in ('this_chg', 'mkt_chg', 'cd', 'cu', 'pos60',
                                       'prev_chg', 'suffix', 'last_day')}
            rule = match_rule(feat, rules)
            if rule:
                # 方案A: 全规则
                week_a_total += 1
                cv_a_by_rule[rule['name']]['total'] += 1
                if rule['pred_up'] == s['actual_up']:
                    week_a_correct += 1
                    cv_a_by_rule[rule['name']]['correct'] += 1

                # 方案B: 动态筛选
                if rule['name'] in valid_rules:
                    week_b_total += 1
                    if rule['pred_up'] == s['actual_up']:
                        week_b_correct += 1

        cv_a_total += week_a_total
        cv_a_correct += week_a_correct
        cv_a_by_week[test_week] = (week_a_correct, week_a_total)

        cv_b_total += week_b_total
        cv_b_correct += week_b_correct
        cv_b_by_week[test_week] = (week_b_correct, week_b_total)

    return {
        'full': {'correct': full_correct, 'total': full_total, 'by_rule': dict(full_by_rule)},
        'cv_a': {'correct': cv_a_correct, 'total': cv_a_total,
                 'by_week': cv_a_by_week, 'by_rule': dict(cv_a_by_rule)},
        'cv_b': {'correct': cv_b_correct, 'total': cv_b_total, 'by_week': cv_b_by_week},
    }


# ═══════════════════════════════════════════════════════════
# 阈值敏感性分析
# ═══════════════════════════════════════════════════════════

def run_sensitivity_analysis(samples):
    """对关键阈值做±20%扰动，观察准确率变化。

    如果准确率对阈值变化非常敏感（>5%波动），说明规则可能过拟合。
    稳健的规则应该在阈值扰动下保持相对稳定的准确率。
    """
    base_rules = build_rules()

    # 基线准确率
    base_total, base_correct = 0, 0
    for s in samples:
        feat = {k: s[k] for k in ('this_chg', 'mkt_chg', 'cd', 'cu', 'pos60',
                                   'prev_chg', 'suffix', 'last_day')}
        rule = match_rule(feat, base_rules)
        if rule:
            base_total += 1
            if rule['pred_up'] == s['actual_up']:
                base_correct += 1
    base_acc = base_correct / base_total * 100 if base_total > 0 else 0

    # 对每个关键阈值做扰动
    perturbations = {
        'r1_mkt_th': [-3.6, -3.3, -3.0, -2.7, -2.4],   # ±20%
        'r1_stk_th': [-2.4, -2.2, -2.0, -1.8, -1.6],
        'r5_stk_th': [-2.4, -2.2, -2.0, -1.8, -1.6],
        'r6a_stk_th': [4.0, 4.5, 5.0, 5.5, 6.0],
        'r2_pos_th': [0.56, 0.63, 0.7, 0.77, 0.84],
        'r4_pos_th': [0.16, 0.18, 0.2, 0.22, 0.24],
    }

    results = {}
    for param_name, values in perturbations.items():
        param_results = []
        for val in values:
            rules = build_rules({param_name: val})
            total, correct = 0, 0
            for s in samples:
                feat = {k: s[k] for k in ('this_chg', 'mkt_chg', 'cd', 'cu', 'pos60',
                                           'prev_chg', 'suffix', 'last_day')}
                rule = match_rule(feat, rules)
                if rule:
                    total += 1
                    if rule['pred_up'] == s['actual_up']:
                        correct += 1
            acc = correct / total * 100 if total > 0 else 0
            param_results.append({'value': val, 'accuracy': acc, 'total': total})
        results[param_name] = param_results

    return {'base_accuracy': base_acc, 'base_total': base_total, 'perturbations': results}


# ═══════════════════════════════════════════════════════════
# 成交量信号样本外验证
# ═══════════════════════════════════════════════════════════

def run_volume_cv(samples, n_weeks):
    """成交量信号的时间序列交叉验证。"""
    all_weeks_set = sorted(set(s['iw_this'] for s in samples))
    rules = build_rules()

    # 统计容器
    cv_confirm = {'correct': 0, 'total': 0}
    cv_contradict = {'correct': 0, 'total': 0}
    cv_no_signal = {'correct': 0, 'total': 0}
    cv_by_conf = defaultdict(lambda: {'correct': 0, 'total': 0})

    for test_idx in range(MIN_TRAIN_WEEKS, len(all_weeks_set)):
        test_week = all_weeks_set[test_idx]
        test_samples = [s for s in samples if s['iw_this'] == test_week]

        for s in test_samples:
            feat = {k: s[k] for k in ('this_chg', 'mkt_chg', 'cd', 'cu', 'pos60',
                                       'prev_chg', 'suffix', 'last_day')}
            rule = match_rule(feat, rules)
            if not rule:
                continue

            pred_up = rule['pred_up']
            is_correct = pred_up == s['actual_up']

            # 成交量信号
            vol_patterns = _detect_volume_patterns(s['this_days'], s['all_klines'])
            vol_dir = vol_patterns.get('vol_direction')

            if vol_dir is None:
                cv_no_signal['total'] += 1
                if is_correct:
                    cv_no_signal['correct'] += 1
                conf = 'high' if rule['tier'] == 1 else 'reference'
            else:
                vol_agrees = (vol_dir == 'up') == pred_up
                if vol_agrees:
                    cv_confirm['total'] += 1
                    if is_correct:
                        cv_confirm['correct'] += 1
                else:
                    cv_contradict['total'] += 1
                    if is_correct:
                        cv_contradict['correct'] += 1

                # 置信度修正
                conf = 'high' if rule['tier'] == 1 else 'reference'
                conf, _ = _adjust_nw_confidence_by_volume(pred_up, conf, vol_patterns)

            cv_by_conf[conf]['total'] += 1
            if is_correct:
                cv_by_conf[conf]['correct'] += 1

    return {
        'confirm': dict(cv_confirm),
        'contradict': dict(cv_contradict),
        'no_signal': dict(cv_no_signal),
        'by_confidence': {k: dict(v) for k, v in cv_by_conf.items()},
    }


# ═══════════════════════════════════════════════════════════
# 分市场验证
# ═══════════════════════════════════════════════════════════

def run_market_split_analysis(samples):
    """分上证/深证/北证验证，检查规则在不同市场的稳定性。"""
    rules = build_rules()
    by_market = defaultdict(lambda: defaultdict(lambda: {'correct': 0, 'total': 0}))
    by_market_overall = defaultdict(lambda: {'correct': 0, 'total': 0})

    for s in samples:
        feat = {k: s[k] for k in ('this_chg', 'mkt_chg', 'cd', 'cu', 'pos60',
                                   'prev_chg', 'suffix', 'last_day')}
        rule = match_rule(feat, rules)
        if rule:
            mkt = s['suffix']
            by_market[mkt][rule['name']]['total'] += 1
            by_market_overall[mkt]['total'] += 1
            if rule['pred_up'] == s['actual_up']:
                by_market[mkt][rule['name']]['correct'] += 1
                by_market_overall[mkt]['correct'] += 1

    return {
        'by_market_overall': {k: dict(v) for k, v in by_market_overall.items()},
        'by_market_rule': {k: {rn: dict(rv) for rn, rv in v.items()}
                          for k, v in by_market.items()},
    }


# ═══════════════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════════════

def run_backtest(n_weeks=N_WEEKS, sample_limit=0):
    t0 = datetime.now()
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"

    logger.info("=" * 80)
    logger.info("  时间序列交叉验证回测 (n_weeks=%d, sample_limit=%d)", n_weeks, sample_limit)
    logger.info("=" * 80)

    # 1. 加载数据
    logger.info("\n[1/5] 加载数据...")
    data = load_all_data(n_weeks, sample_limit=sample_limit)
    logger.info("  数据加载完成")

    # 2. 构建样本
    logger.info("\n[2/5] 构建样本...")
    samples = build_weekly_samples(data, n_weeks)
    logger.info("  总样本数: %d", len(samples))

    if not samples:
        logger.error("  无有效样本，退出")
        return

    # 3. 时间序列交叉验证
    logger.info("\n[3/5] 时间序列交叉验证...")
    cv_results = run_timeseries_cv(samples, n_weeks)

    full = cv_results['full']
    cv_a = cv_results['cv_a']
    cv_b = cv_results['cv_b']

    logger.info("")
    logger.info("=" * 80)
    logger.info("  ══ 核心结果：全样本 vs 交叉验证 ══")
    logger.info("=" * 80)
    logger.info("")
    logger.info("  全样本回测:       %s (%d/%d)",
                _p(full['correct'], full['total']), full['correct'], full['total'])
    logger.info("  交叉验证(全规则):  %s (%d/%d)",
                _p(cv_a['correct'], cv_a['total']), cv_a['correct'], cv_a['total'])
    logger.info("  交叉验证(动态筛选): %s (%d/%d)",
                _p(cv_b['correct'], cv_b['total']), cv_b['correct'], cv_b['total'])

    if full['total'] > 0 and cv_a['total'] > 0:
        full_acc = full['correct'] / full['total'] * 100
        cv_a_acc = cv_a['correct'] / cv_a['total'] * 100
        gap = full_acc - cv_a_acc
        logger.info("")
        logger.info("  ⚠️  过拟合差距: %.1f%% (全样本%.1f%% - 交叉验证%.1f%%)",
                    gap, full_acc, cv_a_acc)
        if gap > 5:
            logger.info("  ❌ 过拟合风险高: 差距>5%%，规则可能对历史数据过度拟合")
        elif gap > 2:
            logger.info("  ⚠️  过拟合风险中: 差距2-5%%，需关注")
        else:
            logger.info("  ✅ 过拟合风险低: 差距<2%%，规则较稳健")

    # 按规则分层（交叉验证）
    logger.info("")
    logger.info("  ── 交叉验证按规则分层 ──")
    for rn in sorted(cv_a['by_rule'].keys()):
        st = cv_a['by_rule'][rn]
        full_st = full['by_rule'].get(rn, {'correct': 0, 'total': 0})
        cv_acc = st['correct'] / st['total'] * 100 if st['total'] > 0 else 0
        full_acc_r = full_st['correct'] / full_st['total'] * 100 if full_st['total'] > 0 else 0
        gap = full_acc_r - cv_acc
        flag = '⚠️' if gap > 5 else '✅'
        logger.info("    %s %-45s 全样本%s(%d) → CV%s(%d) 差距%+.1f%%",
                    flag, rn,
                    _p(full_st['correct'], full_st['total']), full_st['total'],
                    _p(st['correct'], st['total']), st['total'],
                    gap)

    # 按测试周的准确率波动
    logger.info("")
    logger.info("  ── 按测试周准确率 ──")
    week_accs = []
    for iw in sorted(cv_a['by_week'].keys()):
        c, t = cv_a['by_week'][iw]
        if t > 0:
            acc = c / t * 100
            week_accs.append(acc)
            logger.info("    周%s: %s (%d/%d)", iw, _p(c, t), c, t)
    if week_accs:
        mean_acc = _mean(week_accs)
        std_acc = (sum((a - mean_acc)**2 for a in week_accs) / len(week_accs))**0.5
        logger.info("    准确率范围: %.1f%% ~ %.1f%%, 均值: %.1f%%, 标准差: %.1f%%",
                    min(week_accs), max(week_accs), mean_acc, std_acc)

    # 4. 阈值敏感性分析
    logger.info("")
    logger.info("=" * 80)
    logger.info("  ══ 阈值敏感性分析 ══")
    logger.info("=" * 80)
    sens = run_sensitivity_analysis(samples)
    logger.info("  基线准确率: %.1f%% (%d样本)", sens['base_accuracy'], sens['base_total'])
    logger.info("")
    for param, results in sens['perturbations'].items():
        accs = [r['accuracy'] for r in results]
        acc_range = max(accs) - min(accs)
        flag = '⚠️' if acc_range > 3 else '✅'
        logger.info("  %s %s: 准确率范围 %.1f%%~%.1f%% (波动%.1f%%)",
                    flag, param, min(accs), max(accs), acc_range)
        for r in results:
            logger.info("      值=%.2f → %.1f%% (%d样本)",
                        r['value'], r['accuracy'], r['total'])

    # 5. 成交量信号样本外验证
    logger.info("")
    logger.info("=" * 80)
    logger.info("  ══ 成交量信号交叉验证 ══")
    logger.info("=" * 80)
    vol_cv = run_volume_cv(samples, n_weeks)
    logger.info("")
    for label, key in [('确认(方向一致)', 'confirm'),
                       ('矛盾(方向相反)', 'contradict'),
                       ('无信号', 'no_signal')]:
        st = vol_cv[key]
        logger.info("  %-20s %s (%d/%d)", label,
                    _p(st['correct'], st['total']), st['correct'], st['total'])
    if vol_cv['confirm']['total'] > 0 and vol_cv['contradict']['total'] > 0:
        conf_acc = vol_cv['confirm']['correct'] / vol_cv['confirm']['total'] * 100
        cont_acc = vol_cv['contradict']['correct'] / vol_cv['contradict']['total'] * 100
        logger.info("  确认 vs 矛盾差距: %.1f个百分点", conf_acc - cont_acc)

    logger.info("")
    logger.info("  ── 置信度分层（交叉验证）──")
    for conf in sorted(vol_cv['by_confidence'].keys()):
        st = vol_cv['by_confidence'][conf]
        logger.info("    %-12s %s (%d/%d)", conf,
                    _p(st['correct'], st['total']), st['correct'], st['total'])

    # 6. 分市场验证
    logger.info("")
    logger.info("=" * 80)
    logger.info("  ══ 分市场验证 ══")
    logger.info("=" * 80)
    mkt_results = run_market_split_analysis(samples)
    for mkt in sorted(mkt_results['by_market_overall'].keys()):
        st = mkt_results['by_market_overall'][mkt]
        logger.info("  %s: %s (%d/%d)", mkt,
                    _p(st['correct'], st['total']), st['correct'], st['total'])
        for rn in sorted(mkt_results['by_market_rule'].get(mkt, {}).keys()):
            rst = mkt_results['by_market_rule'][mkt][rn]
            if rst['total'] >= 10:
                logger.info("    %-45s %s (%d)",
                            rn, _p(rst['correct'], rst['total']), rst['total'])

    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("")
    logger.info("  总耗时: %.1fs", elapsed)
    logger.info("=" * 80)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--sample', type=int, default=0, help='限制股票数量(0=全部)')
    parser.add_argument('--weeks', type=int, default=N_WEEKS, help='回测周数')
    args = parser.parse_args()
    run_backtest(n_weeks=args.weeks, sample_limit=args.sample)
