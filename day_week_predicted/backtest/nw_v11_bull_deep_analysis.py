#!/usr/bin/env python3
"""V11 大盘涨场景 — 全维度深度因子挖掘。

比第一版分析大幅扩展:
  1. 行业/概念板块维度: 板块动量、板块一致性、逆板块
  2. 资金流向维度: 大单净流入/流出、主力5日净额、资金流向反转
  3. 量价关系维度: 量比、换手率比、量价背离、缩量/放量
  4. 技术形态维度: 冲高回落、探底回升、上影线、振幅
  5. 多因子组合: 2~4因子交叉组合穷举
  6. 按大盘涨幅细分: 微涨(0~1%)、涨(1~2%)、大涨(>2%)分别试算
  7. 按交易所分: 上证/深证分别试算
  8. 时间序列CV: 滚动窗口验证，防止过拟合

目标: 找到大盘涨时CV>=65%的规则，或至少找到CV>=60%的可用边际规则。
"""
import sys, os, json
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from datetime import datetime, timedelta
from collections import defaultdict
import logging

from dao.stock_kline_dao import get_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s',
                    datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

N_WEEKS = 29
MIN_TRAIN_WEEKS = 21  # CV从第22周开始

# ── 复用V11的数据加载和样本构建 ──
from day_week_predicted.backtest.nw_v11_multifactor_backtest import (
    load_data, build_samples,
)


def _pct(n, t):
    return f"{n/t*100:.1f}%" if t > 0 else "N/A"


def _safe_mean(lst):
    return sum(lst) / len(lst) if lst else 0


def deep_bull_analysis(samples):
    """全维度大盘涨场景因子挖掘。"""
    all_weeks = sorted(set(s['iw_this'] for s in samples))
    cv_start = MIN_TRAIN_WEEKS
    cv_weeks = all_weeks[cv_start:]

    # 大盘涨样本(mkt>=0%)
    bull = [s for s in samples if s['mkt_chg'] >= 0]
    # 细分
    slight_up = [s for s in samples if 0 <= s['mkt_chg'] <= 1]
    mid_up = [s for s in samples if 1 < s['mkt_chg'] <= 2]
    strong_up = [s for s in samples if s['mkt_chg'] > 2]

    logger.info("=" * 100)
    logger.info("  V11 大盘涨场景 — 全维度深度因子挖掘 V2")
    logger.info("=" * 100)

    # 基础统计
    for name, sub in [('大盘涨(>=0%)', bull), ('微涨(0~1%)', slight_up),
                       ('涨(1~2%)', mid_up), ('大涨(>2%)', strong_up)]:
        up = sum(1 for s in sub if s['actual_up'])
        logger.info("  %-18s 样本:%d 涨:%d(%.1f%%) 跌:%d(%.1f%%)",
                    name, len(sub), up, up/len(sub)*100 if sub else 0,
                    len(sub)-up, (len(sub)-up)/len(sub)*100 if sub else 0)

    # ── 评估函数 ──
    candidates = []

    def _eval(name, pred_up, check_fn, sub_samples=None, min_cv=30):
        """评估单条规则，返回(cv_acc, cv_total)或None。"""
        pool = sub_samples if sub_samples is not None else bull
        total, correct = 0, 0
        for s in pool:
            try:
                if check_fn(s):
                    total += 1
                    if pred_up == s['actual_up']:
                        correct += 1
            except (TypeError, KeyError, ZeroDivisionError):
                continue
        if total < 30:
            return None
        full_acc = correct / total * 100

        # 时间序列CV
        cv_total, cv_correct = 0, 0
        for tw in cv_weeks:
            for s in pool:
                if s['iw_this'] != tw:
                    continue
                try:
                    if check_fn(s):
                        cv_total += 1
                        if pred_up == s['actual_up']:
                            cv_correct += 1
                except (TypeError, KeyError, ZeroDivisionError):
                    continue
        if cv_total < min_cv:
            return None
        cv_acc = cv_correct / cv_total * 100
        gap = full_acc - cv_acc

        # 按周统计准确率
        week_accs = []
        for tw in cv_weeks:
            wt, wc = 0, 0
            for s in pool:
                if s['iw_this'] != tw:
                    continue
                try:
                    if check_fn(s):
                        wt += 1
                        if pred_up == s['actual_up']:
                            wc += 1
                except (TypeError, KeyError, ZeroDivisionError):
                    continue
            if wt >= 5:
                week_accs.append(wc / wt * 100)

        weeks_above_65 = sum(1 for a in week_accs if a >= 65)
        weeks_above_60 = sum(1 for a in week_accs if a >= 60)

        flag = '★' if cv_acc >= 65 and abs(gap) < 15 else ('⚠' if cv_acc >= 60 else ' ')
        d = '涨' if pred_up else '跌'
        logger.info("  %s [%s] %-62s 全%s(%d) CV%s(%d) gap%+.1f%% w65:%d/%d w60:%d/%d",
                    flag, d, name,
                    _pct(correct, total), total,
                    _pct(cv_correct, cv_total), cv_total, gap,
                    weeks_above_65, len(week_accs),
                    weeks_above_60, len(week_accs))

        candidates.append({
            'name': name, 'pred_up': pred_up,
            'full_acc': full_acc, 'cv_acc': cv_acc, 'gap': gap,
            'total': total, 'cv_total': cv_total,
            'weeks_above_65': weeks_above_65, 'weeks_above_60': weeks_above_60,
            'total_weeks': len(week_accs),
        })
        return cv_acc, cv_total

    # ================================================================
    # 第1部分: 资金流向维度(之前分析不够深入)
    # ================================================================
    logger.info("\n" + "=" * 100)
    logger.info("  [1] 资金流向维度")
    logger.info("=" * 100)

    logger.info("\n  ── 1.1 大单净流入 × 涨跌 ──")
    # 大单净流入+个股跌 → 主力抄底 → 涨
    for ff_th in [0.5, 1, 2, 3, 5]:
        for chg_th in [-1, -2, -3, -5]:
            _eval(f'大单净流入>{ff_th}%+跌>{abs(chg_th)}%', True,
                  lambda s, f=ff_th, c=chg_th: (s['big_net_pct_avg'] is not None
                      and s['big_net_pct_avg'] > f and s['this_chg'] < c))

    logger.info("\n  ── 1.2 大单净流入+位置 ──")
    for ff_th in [1, 2, 3]:
        _eval(f'大单净流入>{ff_th}%+低位<0.3', True,
              lambda s, f=ff_th: (s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] > f
                  and s['pos60'] is not None and s['pos60'] < 0.3))
        _eval(f'大单净流入>{ff_th}%+低位<0.4', True,
              lambda s, f=ff_th: (s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] > f
                  and s['pos60'] is not None and s['pos60'] < 0.4))
        _eval(f'大单净流入>{ff_th}%+高位>0.7', False,
              lambda s, f=ff_th: (s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] > f
                  and s['pos60'] is not None and s['pos60'] >= 0.7))

    logger.info("\n  ── 1.3 大单净流出+涨幅 → 跌 ──")
    for ff_th in [-1, -2, -3, -5]:
        for chg_th in [2, 3, 5, 8]:
            _eval(f'大单净流出<{ff_th}%+涨>{chg_th}%', False,
                  lambda s, f=ff_th, c=chg_th: (s['big_net_pct_avg'] is not None
                      and s['big_net_pct_avg'] < f and s['this_chg'] > c))

    logger.info("\n  ── 1.4 大单净流出+高位 → 跌 ──")
    for ff_th in [-1, -2, -3]:
        for pos_th in [0.6, 0.7, 0.8]:
            _eval(f'大单净流出<{ff_th}%+高位>{pos_th}', False,
                  lambda s, f=ff_th, p=pos_th: (s['big_net_pct_avg'] is not None
                      and s['big_net_pct_avg'] < f
                      and s['pos60'] is not None and s['pos60'] >= p))

    logger.info("\n  ── 1.5 主力5日净额 ──")
    for mn_th in [0, 500, 1000, 2000]:
        _eval(f'主力5日净额>{mn_th}万+跌>2%', True,
              lambda s, m=mn_th: (s['main_net_5day_latest'] is not None
                  and s['main_net_5day_latest'] > m and s['this_chg'] < -2))
    for mn_th in [0, -500, -1000, -2000]:
        _eval(f'主力5日净额<{mn_th}万+涨>3%', False,
              lambda s, m=mn_th: (s['main_net_5day_latest'] is not None
                  and s['main_net_5day_latest'] < m and s['this_chg'] > 3))

    logger.info("\n  ── 1.6 资金流向反转(大单流入但个股跌 / 大单流出但个股涨) ──")
    _eval('资金流入>1%但跌>2%+低位<0.4', True,
          lambda s: (s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] > 1
              and s['this_chg'] < -2
              and s['pos60'] is not None and s['pos60'] < 0.4))
    _eval('资金流入>2%但跌>3%+低位<0.4', True,
          lambda s: (s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] > 2
              and s['this_chg'] < -3
              and s['pos60'] is not None and s['pos60'] < 0.4))
    _eval('资金流出<-1%但涨>3%+高位>0.6', False,
          lambda s: (s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < -1
              and s['this_chg'] > 3
              and s['pos60'] is not None and s['pos60'] >= 0.6))
    _eval('资金流出<-2%但涨>5%+高位>0.6', False,
          lambda s: (s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < -2
              and s['this_chg'] > 5
              and s['pos60'] is not None and s['pos60'] >= 0.6))

    # ================================================================
    # 第2部分: 概念板块维度
    # ================================================================
    logger.info("\n" + "=" * 100)
    logger.info("  [2] 概念板块维度")
    logger.info("=" * 100)

    logger.info("\n  ── 2.1 板块动量 × 个股涨跌 ──")
    # 板块强+个股弱 → 补涨
    for bm_th in [1, 2, 3]:
        for chg_th in [-2, -3, -5]:
            _eval(f'板块涨>{bm_th}%+个股跌>{abs(chg_th)}%', True,
                  lambda s, b=bm_th, c=chg_th: (s['board_momentum'] is not None
                      and s['board_momentum'] > b and s['this_chg'] < c))
    # 板块弱+个股强 → 回调
    for bm_th in [-1, -2, -3]:
        for chg_th in [3, 5, 8]:
            _eval(f'板块跌<{bm_th}%+个股涨>{chg_th}%', False,
                  lambda s, b=bm_th, c=chg_th: (s['board_momentum'] is not None
                      and s['board_momentum'] < b and s['this_chg'] > c))

    logger.info("\n  ── 2.2 板块一致性 ──")
    # 概念板块一致看涨+个股跌 → 补涨
    for cc_th in [0.6, 0.7, 0.8]:
        _eval(f'板块一致性>{cc_th}+个股跌>2%', True,
              lambda s, c=cc_th: (s['concept_consensus'] is not None
                  and s['concept_consensus'] > c and s['this_chg'] < -2))
        _eval(f'板块一致性>{cc_th}+个股跌>3%', True,
              lambda s, c=cc_th: (s['concept_consensus'] is not None
                  and s['concept_consensus'] > c and s['this_chg'] < -3))
    # 概念板块一致看跌+个股涨 → 回调
    for cc_th in [0.3, 0.2, 0.1]:
        _eval(f'板块一致性<{cc_th}+个股涨>3%', False,
              lambda s, c=cc_th: (s['concept_consensus'] is not None
                  and s['concept_consensus'] < c and s['this_chg'] > 3))
        _eval(f'板块一致性<{cc_th}+个股涨>5%', False,
              lambda s, c=cc_th: (s['concept_consensus'] is not None
                  and s['concept_consensus'] < c and s['this_chg'] > 5))

    logger.info("\n  ── 2.3 板块动量+位置+资金 组合 ──")
    _eval('板块涨>2%+个股跌>3%+低位<0.3', True,
          lambda s: (s['board_momentum'] is not None and s['board_momentum'] > 2
              and s['this_chg'] < -3
              and s['pos60'] is not None and s['pos60'] < 0.3))
    _eval('板块涨>1%+个股跌>2%+资金流入>1%', True,
          lambda s: (s['board_momentum'] is not None and s['board_momentum'] > 1
              and s['this_chg'] < -2
              and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] > 1))
    _eval('板块涨>2%+个股跌>2%+连跌≥2天', True,
          lambda s: (s['board_momentum'] is not None and s['board_momentum'] > 2
              and s['this_chg'] < -2 and s['cd'] >= 2))
    _eval('板块跌<-1%+个股涨>5%+高位>0.7', False,
          lambda s: (s['board_momentum'] is not None and s['board_momentum'] < -1
              and s['this_chg'] > 5
              and s['pos60'] is not None and s['pos60'] >= 0.7))
    _eval('板块跌<-1%+个股涨>3%+资金流出<-1%', False,
          lambda s: (s['board_momentum'] is not None and s['board_momentum'] < -1
              and s['this_chg'] > 3
              and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < -1))

    # ================================================================
    # 第3部分: 量价关系维度(深度)
    # ================================================================
    logger.info("\n" + "=" * 100)
    logger.info("  [3] 量价关系维度(深度)")
    logger.info("=" * 100)

    logger.info("\n  ── 3.1 量比 × 涨跌 × 位置 ──")
    # 缩量下跌+低位 → 恐慌出尽 → 涨
    for vr_th in [0.5, 0.6, 0.7, 0.8]:
        for chg_th in [-2, -3, -5]:
            _eval(f'缩量<{vr_th}+跌>{abs(chg_th)}%+低位<0.3', True,
                  lambda s, v=vr_th, c=chg_th: (s['vol_ratio'] is not None and s['vol_ratio'] < v
                      and s['this_chg'] < c
                      and s['pos60'] is not None and s['pos60'] < 0.3))
    # 放量上涨+高位 → 出货 → 跌
    for vr_th in [1.3, 1.5, 2.0]:
        for chg_th in [3, 5, 8]:
            _eval(f'放量>{vr_th}+涨>{chg_th}%+高位>0.7', False,
                  lambda s, v=vr_th, c=chg_th: (s['vol_ratio'] is not None and s['vol_ratio'] > v
                      and s['this_chg'] > c
                      and s['pos60'] is not None and s['pos60'] >= 0.7))

    logger.info("\n  ── 3.2 换手率比 ──")
    for tr_th in [0.5, 0.6, 0.7]:
        _eval(f'换手率比<{tr_th}+跌>2%+低位<0.3', True,
              lambda s, t=tr_th: (s['turnover_ratio'] is not None and s['turnover_ratio'] < t
                  and s['this_chg'] < -2
                  and s['pos60'] is not None and s['pos60'] < 0.3))
    for tr_th in [1.5, 2.0, 2.5]:
        _eval(f'换手率比>{tr_th}+涨>3%+高位>0.7', False,
              lambda s, t=tr_th: (s['turnover_ratio'] is not None and s['turnover_ratio'] > t
                  and s['this_chg'] > 3
                  and s['pos60'] is not None and s['pos60'] >= 0.7))

    logger.info("\n  ── 3.3 量价背离 ──")
    # 涨+缩量(量价背离) → 跌
    _eval('量价背离(涨+缩量)+高位>0.6', False,
          lambda s: s['vol_price_diverge'] == -1
              and s['pos60'] is not None and s['pos60'] >= 0.6)
    _eval('量价背离(涨+缩量)+高位>0.7', False,
          lambda s: s['vol_price_diverge'] == -1
              and s['pos60'] is not None and s['pos60'] >= 0.7)
    _eval('量价背离(涨+缩量)+涨>3%', False,
          lambda s: s['vol_price_diverge'] == -1 and s['this_chg'] > 3)
    _eval('量价背离(涨+缩量)+涨>5%', False,
          lambda s: s['vol_price_diverge'] == -1 and s['this_chg'] > 5)
    # 跌+放量(量价背离) → 涨
    _eval('量价背离(跌+放量)+低位<0.3', True,
          lambda s: s['vol_price_diverge'] == 1
              and s['pos60'] is not None and s['pos60'] < 0.3)
    _eval('量价背离(跌+放量)+低位<0.4', True,
          lambda s: s['vol_price_diverge'] == 1
              and s['pos60'] is not None and s['pos60'] < 0.4)

    # ================================================================
    # 第4部分: 技术形态维度(深度)
    # ================================================================
    logger.info("\n" + "=" * 100)
    logger.info("  [4] 技术形态维度(深度)")
    logger.info("=" * 100)

    logger.info("\n  ── 4.1 冲高回落 × 多因子 ──")
    _eval('冲高回落+涨>3%', False, lambda s: s['rush_up_pullback'] and s['this_chg'] > 3)
    _eval('冲高回落+涨>5%', False, lambda s: s['rush_up_pullback'] and s['this_chg'] > 5)
    _eval('冲高回落+涨>3%+高位>0.6', False,
          lambda s: s['rush_up_pullback'] and s['this_chg'] > 3
              and s['pos60'] is not None and s['pos60'] >= 0.6)
    _eval('冲高回落+涨>3%+资金流出<-1%', False,
          lambda s: s['rush_up_pullback'] and s['this_chg'] > 3
              and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < -1)
    _eval('冲高回落+涨>3%+放量>1.3', False,
          lambda s: s['rush_up_pullback'] and s['this_chg'] > 3
              and s['vol_ratio'] is not None and s['vol_ratio'] > 1.3)
    _eval('冲高回落+涨>5%+连涨≥2天', False,
          lambda s: s['rush_up_pullback'] and s['this_chg'] > 5 and s['cu'] >= 2)
    _eval('冲高回落+涨>3%+上影线>0.5', False,
          lambda s: s['rush_up_pullback'] and s['this_chg'] > 3
              and s['upper_shadow_ratio'] is not None and s['upper_shadow_ratio'] > 0.5)

    logger.info("\n  ── 4.2 探底回升 × 多因子 ──")
    _eval('探底回升+跌>2%', True, lambda s: s['dip_recovery'] and s['this_chg'] < -2)
    _eval('探底回升+跌>2%+低位<0.3', True,
          lambda s: s['dip_recovery'] and s['this_chg'] < -2
              and s['pos60'] is not None and s['pos60'] < 0.3)
    _eval('探底回升+跌>2%+资金流入>1%', True,
          lambda s: s['dip_recovery'] and s['this_chg'] < -2
              and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] > 1)
    _eval('探底回升+跌>3%+连跌≥2天', True,
          lambda s: s['dip_recovery'] and s['this_chg'] < -3 and s['cd'] >= 2)
    _eval('探底回升+跌>2%+缩量<0.8', True,
          lambda s: s['dip_recovery'] and s['this_chg'] < -2
              and s['vol_ratio'] is not None and s['vol_ratio'] < 0.8)

    logger.info("\n  ── 4.3 上影线 × 多因子 ──")
    for us_th in [0.4, 0.5, 0.6, 0.7]:
        _eval(f'上影线>{us_th}+涨>3%', False,
              lambda s, u=us_th: s['upper_shadow_ratio'] is not None and s['upper_shadow_ratio'] > u
                  and s['this_chg'] > 3)
        _eval(f'上影线>{us_th}+涨>5%+高位>0.6', False,
              lambda s, u=us_th: (s['upper_shadow_ratio'] is not None and s['upper_shadow_ratio'] > u
                  and s['this_chg'] > 5
                  and s['pos60'] is not None and s['pos60'] >= 0.6))

    logger.info("\n  ── 4.4 振幅 × 涨跌 ──")
    for amp_th in [3, 5, 7, 10]:
        _eval(f'周振幅>{amp_th}%+跌>2%+低位<0.3', True,
              lambda s, a=amp_th: (s['week_amp'] is not None and s['week_amp'] > a
                  and s['this_chg'] < -2
                  and s['pos60'] is not None and s['pos60'] < 0.3))
        _eval(f'周振幅>{amp_th}%+涨>3%+高位>0.7', False,
              lambda s, a=amp_th: (s['week_amp'] is not None and s['week_amp'] > a
                  and s['this_chg'] > 3
                  and s['pos60'] is not None and s['pos60'] >= 0.7))

    logger.info("\n  ── 4.5 日内极值 ──")
    for md_th in [-3, -5, -7]:
        _eval(f'周内最大日跌>{abs(md_th)}%+低位<0.3', True,
              lambda s, m=md_th: s['max_day_down'] < m
                  and s['pos60'] is not None and s['pos60'] < 0.3)
        _eval(f'周内最大日跌>{abs(md_th)}%+连跌≥2天', True,
              lambda s, m=md_th: s['max_day_down'] < m and s['cd'] >= 2)
    for mu_th in [5, 7, 10]:
        _eval(f'周内最大日涨>{mu_th}%+高位>0.7', False,
              lambda s, m=mu_th: s['max_day_up'] > m
                  and s['pos60'] is not None and s['pos60'] >= 0.7)
        _eval(f'周内最大日涨>{mu_th}%+资金流出<-1%', False,
              lambda s, m=mu_th: s['max_day_up'] > m
                  and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < -1)

    logger.info("\n  ── 4.6 波动率 ──")
    for std_th in [1, 2, 3]:
        _eval(f'个股波动率>{std_th}+跌>2%+低位<0.3', True,
              lambda s, st=std_th: (s['stock_vol_std'] is not None and s['stock_vol_std'] > st
                  and s['this_chg'] < -2
                  and s['pos60'] is not None and s['pos60'] < 0.3))
        _eval(f'个股波动率>{std_th}+涨>3%+高位>0.7', False,
              lambda s, st=std_th: (s['stock_vol_std'] is not None and s['stock_vol_std'] > st
                  and s['this_chg'] > 3
                  and s['pos60'] is not None and s['pos60'] >= 0.7))

    # ================================================================
    # 第5部分: 动量/趋势维度
    # ================================================================
    logger.info("\n" + "=" * 100)
    logger.info("  [5] 动量/趋势维度")
    logger.info("=" * 100)

    logger.info("\n  ── 5.1 3周动量 × 位置 × 涨跌 ──")
    for mom_th in [-5, -8, -10, -15, -20]:
        _eval(f'3周动量<{mom_th}%+低位<0.3', True,
              lambda s, m=mom_th: s['momentum_3w'] is not None and s['momentum_3w'] < m
                  and s['pos60'] is not None and s['pos60'] < 0.3)
        _eval(f'3周动量<{mom_th}%+低位<0.2', True,
              lambda s, m=mom_th: s['momentum_3w'] is not None and s['momentum_3w'] < m
                  and s['pos60'] is not None and s['pos60'] < 0.2)
    for mom_th in [5, 8, 10, 15, 20]:
        _eval(f'3周动量>{mom_th}%+高位>0.7', False,
              lambda s, m=mom_th: s['momentum_3w'] is not None and s['momentum_3w'] > m
                  and s['pos60'] is not None and s['pos60'] >= 0.7)
        _eval(f'3周动量>{mom_th}%+高位>0.8', False,
              lambda s, m=mom_th: s['momentum_3w'] is not None and s['momentum_3w'] > m
                  and s['pos60'] is not None and s['pos60'] >= 0.8)

    logger.info("\n  ── 5.2 相对强弱 × 位置 ──")
    for rs_th in [-3, -5, -8, -10]:
        _eval(f'相对强弱<{rs_th}%+低位<0.3', True,
              lambda s, r=rs_th: s['relative_strength'] < r
                  and s['pos60'] is not None and s['pos60'] < 0.3)
        _eval(f'相对强弱<{rs_th}%+连跌≥3天', True,
              lambda s, r=rs_th: s['relative_strength'] < r and s['cd'] >= 3)
    for rs_th in [3, 5, 8, 10]:
        _eval(f'相对强弱>{rs_th}%+高位>0.7', False,
              lambda s, r=rs_th: s['relative_strength'] > r
                  and s['pos60'] is not None and s['pos60'] >= 0.7)
        _eval(f'相对强弱>{rs_th}%+连涨≥3天', False,
              lambda s, r=rs_th: s['relative_strength'] > r and s['cu'] >= 3)

    logger.info("\n  ── 5.3 前周涨跌 × 本周涨跌(连续性) ──")
    # 连续两周跌+低位 → 涨
    for p_th in [-2, -3, -5]:
        for t_th in [-2, -3, -5]:
            _eval(f'前周跌>{abs(p_th)}%+本周跌>{abs(t_th)}%+低位<0.3', True,
                  lambda s, p=p_th, t=t_th: (s['prev_chg'] is not None and s['prev_chg'] < p
                      and s['this_chg'] < t
                      and s['pos60'] is not None and s['pos60'] < 0.3))
    # 连续两周涨+高位 → 跌
    for p_th in [2, 3, 5]:
        for t_th in [3, 5, 8]:
            _eval(f'前周涨>{p_th}%+本周涨>{t_th}%+高位>0.7', False,
                  lambda s, p=p_th, t=t_th: (s['prev_chg'] is not None and s['prev_chg'] > p
                      and s['this_chg'] > t
                      and s['pos60'] is not None and s['pos60'] >= 0.7))

    logger.info("\n  ── 5.4 前两周动量 ──")
    _eval('前两周均跌>2%+本周跌>2%+低位<0.3', True,
          lambda s: (s['prev_chg'] is not None and s['prev_chg'] < -2
              and s['prev2_chg'] is not None and s['prev2_chg'] < -2
              and s['this_chg'] < -2
              and s['pos60'] is not None and s['pos60'] < 0.3))
    _eval('前两周均涨>2%+本周涨>3%+高位>0.7', False,
          lambda s: (s['prev_chg'] is not None and s['prev_chg'] > 2
              and s['prev2_chg'] is not None and s['prev2_chg'] > 2
              and s['this_chg'] > 3
              and s['pos60'] is not None and s['pos60'] >= 0.7))

    # ================================================================
    # 第6部分: 大盘环境因子
    # ================================================================
    logger.info("\n" + "=" * 100)
    logger.info("  [6] 大盘环境因子")
    logger.info("=" * 100)

    logger.info("\n  ── 6.1 大盘尾日 × 个股 ──")
    # 大盘尾日跌+个股跌 → 恐慌出尽
    for mld_th in [-1, -2]:
        for chg_th in [-2, -3, -5]:
            _eval(f'大盘尾日跌>{abs(mld_th)}%+个股跌>{abs(chg_th)}%', True,
                  lambda s, m=mld_th, c=chg_th: (s['mkt_last_day'] is not None
                      and s['mkt_last_day'] < m and s['this_chg'] < c))
            _eval(f'大盘尾日跌>{abs(mld_th)}%+个股跌>{abs(chg_th)}%+低位<0.3', True,
                  lambda s, m=mld_th, c=chg_th: (s['mkt_last_day'] is not None
                      and s['mkt_last_day'] < m and s['this_chg'] < c
                      and s['pos60'] is not None and s['pos60'] < 0.3))
    # 大盘尾日涨+个股涨 → 追高
    for mld_th in [1, 2]:
        for chg_th in [3, 5]:
            _eval(f'大盘尾日涨>{mld_th}%+个股涨>{chg_th}%+高位>0.7', False,
                  lambda s, m=mld_th, c=chg_th: (s['mkt_last_day'] is not None
                      and s['mkt_last_day'] > m and s['this_chg'] > c
                      and s['pos60'] is not None and s['pos60'] >= 0.7))

    logger.info("\n  ── 6.2 大盘波动率 ──")
    for mv_th in [1, 1.5, 2]:
        _eval(f'大盘波动率>{mv_th}+个股跌>2%+低位<0.3', True,
              lambda s, m=mv_th: (s['mkt_vol_std'] is not None and s['mkt_vol_std'] > m
                  and s['this_chg'] < -2
                  and s['pos60'] is not None and s['pos60'] < 0.3))
        _eval(f'大盘波动率>{mv_th}+个股涨>3%+高位>0.7', False,
              lambda s, m=mv_th: (s['mkt_vol_std'] is not None and s['mkt_vol_std'] > m
                  and s['this_chg'] > 3
                  and s['pos60'] is not None and s['pos60'] >= 0.7))

    # ================================================================
    # 第7部分: 交易所分组 × 多因子
    # ================================================================
    logger.info("\n" + "=" * 100)
    logger.info("  [7] 交易所分组 × 多因子")
    logger.info("=" * 100)

    for sx_name, sx_code in [('上证', 'SH'), ('深证', 'SZ')]:
        logger.info(f"\n  ── 7.{1 if sx_code=='SH' else 2} {sx_name} ──")
        # 涨信号
        for chg_th in [-2, -3, -5]:
            _eval(f'{sx_name}+跌>{abs(chg_th)}%+连跌≥3天+低位<0.4', True,
                  lambda s, sx=sx_code, c=chg_th: (s['suffix'] == sx and s['this_chg'] < c
                      and s['cd'] >= 3
                      and s['pos60'] is not None and s['pos60'] < 0.4))
            _eval(f'{sx_name}+跌>{abs(chg_th)}%+连跌≥4天', True,
                  lambda s, sx=sx_code, c=chg_th: (s['suffix'] == sx and s['this_chg'] < c
                      and s['cd'] >= 4))
            _eval(f'{sx_name}+跌>{abs(chg_th)}%+资金流入>1%', True,
                  lambda s, sx=sx_code, c=chg_th: (s['suffix'] == sx and s['this_chg'] < c
                      and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] > 1))
            _eval(f'{sx_name}+跌>{abs(chg_th)}%+前周跌>2%+低位<0.3', True,
                  lambda s, sx=sx_code, c=chg_th: (s['suffix'] == sx and s['this_chg'] < c
                      and s['prev_chg'] is not None and s['prev_chg'] < -2
                      and s['pos60'] is not None and s['pos60'] < 0.3))
        # 跌信号
        for chg_th in [3, 5, 8]:
            _eval(f'{sx_name}+涨>{chg_th}%+连涨≥3天+高位>0.7', False,
                  lambda s, sx=sx_code, c=chg_th: (s['suffix'] == sx and s['this_chg'] > c
                      and s['cu'] >= 3
                      and s['pos60'] is not None and s['pos60'] >= 0.7))
            _eval(f'{sx_name}+涨>{chg_th}%+冲高回落', False,
                  lambda s, sx=sx_code, c=chg_th: (s['suffix'] == sx and s['this_chg'] > c
                      and s['rush_up_pullback']))
            _eval(f'{sx_name}+涨>{chg_th}%+资金流出<-1%+高位>0.6', False,
                  lambda s, sx=sx_code, c=chg_th: (s['suffix'] == sx and s['this_chg'] > c
                      and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < -1
                      and s['pos60'] is not None and s['pos60'] >= 0.6))

    # ================================================================
    # 第8部分: 高阶多因子组合(3~4因子)
    # ================================================================
    logger.info("\n" + "=" * 100)
    logger.info("  [8] 高阶多因子组合(3~4因子)")
    logger.info("=" * 100)

    logger.info("\n  ── 8.1 涨信号: 超跌+资金+板块+位置 ──")
    _eval('跌>3%+连跌≥3天+资金流入>1%+低位<0.4', True,
          lambda s: (s['this_chg'] < -3 and s['cd'] >= 3
              and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] > 1
              and s['pos60'] is not None and s['pos60'] < 0.4))
    _eval('跌>2%+连跌≥4天+资金流入>0.5%+低位<0.4', True,
          lambda s: (s['this_chg'] < -2 and s['cd'] >= 4
              and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] > 0.5
              and s['pos60'] is not None and s['pos60'] < 0.4))
    _eval('跌>3%+连跌≥3天+板块涨>1%+低位<0.4', True,
          lambda s: (s['this_chg'] < -3 and s['cd'] >= 3
              and s['board_momentum'] is not None and s['board_momentum'] > 1
              and s['pos60'] is not None and s['pos60'] < 0.4))
    _eval('跌>2%+连跌≥3天+缩量<0.7+低位<0.3', True,
          lambda s: (s['this_chg'] < -2 and s['cd'] >= 3
              and s['vol_ratio'] is not None and s['vol_ratio'] < 0.7
              and s['pos60'] is not None and s['pos60'] < 0.3))
    _eval('跌>3%+前周跌>2%+资金流入>1%+低位<0.3', True,
          lambda s: (s['this_chg'] < -3
              and s['prev_chg'] is not None and s['prev_chg'] < -2
              and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] > 1
              and s['pos60'] is not None and s['pos60'] < 0.3))
    _eval('跌>5%+低位<0.2+缩量<0.7', True,
          lambda s: (s['this_chg'] < -5
              and s['pos60'] is not None and s['pos60'] < 0.2
              and s['vol_ratio'] is not None and s['vol_ratio'] < 0.7))
    _eval('跌>3%+低位<0.2+连跌≥2天+前周跌>2%', True,
          lambda s: (s['this_chg'] < -3
              and s['pos60'] is not None and s['pos60'] < 0.2
              and s['cd'] >= 2
              and s['prev_chg'] is not None and s['prev_chg'] < -2))
    _eval('跌>2%+连跌≥4天+低位<0.3+缩量<0.8', True,
          lambda s: (s['this_chg'] < -2 and s['cd'] >= 4
              and s['pos60'] is not None and s['pos60'] < 0.3
              and s['vol_ratio'] is not None and s['vol_ratio'] < 0.8))
    _eval('跌>3%+连跌≥4天+低位<0.3+前周跌>2%', True,
          lambda s: (s['this_chg'] < -3 and s['cd'] >= 4
              and s['pos60'] is not None and s['pos60'] < 0.3
              and s['prev_chg'] is not None and s['prev_chg'] < -2))
    _eval('SH+跌>2%+连跌≥3天+低位<0.3', True,
          lambda s: (s['suffix'] == 'SH' and s['this_chg'] < -2 and s['cd'] >= 3
              and s['pos60'] is not None and s['pos60'] < 0.3))
    _eval('SH+跌>3%+低位<0.2+前周跌>2%', True,
          lambda s: (s['suffix'] == 'SH' and s['this_chg'] < -3
              and s['pos60'] is not None and s['pos60'] < 0.2
              and s['prev_chg'] is not None and s['prev_chg'] < -2))

    logger.info("\n  ── 8.2 跌信号: 过热+资金+板块+位置 ──")
    _eval('涨>5%+连涨≥3天+资金流出<-1%+高位>0.7', False,
          lambda s: (s['this_chg'] > 5 and s['cu'] >= 3
              and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < -1
              and s['pos60'] is not None and s['pos60'] >= 0.7))
    _eval('涨>3%+连涨≥4天+高位>0.7+放量>1.3', False,
          lambda s: (s['this_chg'] > 3 and s['cu'] >= 4
              and s['pos60'] is not None and s['pos60'] >= 0.7
              and s['vol_ratio'] is not None and s['vol_ratio'] > 1.3))
    _eval('涨>5%+冲高回落+资金流出<-1%', False,
          lambda s: (s['this_chg'] > 5 and s['rush_up_pullback']
              and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < -1))
    _eval('涨>5%+冲高回落+高位>0.7', False,
          lambda s: (s['this_chg'] > 5 and s['rush_up_pullback']
              and s['pos60'] is not None and s['pos60'] >= 0.7))
    _eval('涨>8%+高位>0.8+放量>1.5', False,
          lambda s: (s['this_chg'] > 8
              and s['pos60'] is not None and s['pos60'] >= 0.8
              and s['vol_ratio'] is not None and s['vol_ratio'] > 1.5))
    _eval('涨>5%+高位>0.8+资金流出<-2%', False,
          lambda s: (s['this_chg'] > 5
              and s['pos60'] is not None and s['pos60'] >= 0.8
              and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < -2))
    _eval('涨>3%+连涨≥3天+板块跌<-1%+高位>0.6', False,
          lambda s: (s['this_chg'] > 3 and s['cu'] >= 3
              and s['board_momentum'] is not None and s['board_momentum'] < -1
              and s['pos60'] is not None and s['pos60'] >= 0.6))
    _eval('涨>5%+上影线>0.5+高位>0.7+放量>1.3', False,
          lambda s: (s['this_chg'] > 5
              and s['upper_shadow_ratio'] is not None and s['upper_shadow_ratio'] > 0.5
              and s['pos60'] is not None and s['pos60'] >= 0.7
              and s['vol_ratio'] is not None and s['vol_ratio'] > 1.3))
    _eval('涨>8%+连涨≥2天+资金流出<-1%+高位>0.6', False,
          lambda s: (s['this_chg'] > 8 and s['cu'] >= 2
              and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < -1
              and s['pos60'] is not None and s['pos60'] >= 0.6))
    _eval('前周涨>3%+本周涨>5%+高位>0.8', False,
          lambda s: (s['prev_chg'] is not None and s['prev_chg'] > 3
              and s['this_chg'] > 5
              and s['pos60'] is not None and s['pos60'] >= 0.8))
    _eval('前周涨>5%+本周涨>5%+高位>0.7', False,
          lambda s: (s['prev_chg'] is not None and s['prev_chg'] > 5
              and s['this_chg'] > 5
              and s['pos60'] is not None and s['pos60'] >= 0.7))
    _eval('3周动量>15%+高位>0.8+涨>3%', False,
          lambda s: (s['momentum_3w'] is not None and s['momentum_3w'] > 15
              and s['pos60'] is not None and s['pos60'] >= 0.8
              and s['this_chg'] > 3))
    _eval('3周动量>20%+高位>0.7', False,
          lambda s: (s['momentum_3w'] is not None and s['momentum_3w'] > 20
              and s['pos60'] is not None and s['pos60'] >= 0.7))

    # ================================================================
    # 第9部分: 按大盘涨幅细分试算(微涨/涨/大涨分别)
    # ================================================================
    logger.info("\n" + "=" * 100)
    logger.info("  [9] 按大盘涨幅细分试算")
    logger.info("=" * 100)

    for regime_name, regime_samples in [('微涨(0~1%)', slight_up),
                                         ('涨(1~2%)', mid_up),
                                         ('大涨(>2%)', strong_up)]:
        logger.info(f"\n  ── 9.x {regime_name} ──")
        if len(regime_samples) < 100:
            logger.info("    样本不足，跳过")
            continue

        # 涨信号
        for chg_th in [-2, -3, -5]:
            _eval(f'{regime_name}+跌>{abs(chg_th)}%+低位<0.3', True,
                  lambda s, c=chg_th: (s['this_chg'] < c
                      and s['pos60'] is not None and s['pos60'] < 0.3),
                  sub_samples=regime_samples)
            _eval(f'{regime_name}+跌>{abs(chg_th)}%+连跌≥3天', True,
                  lambda s, c=chg_th: s['this_chg'] < c and s['cd'] >= 3,
                  sub_samples=regime_samples)
            _eval(f'{regime_name}+跌>{abs(chg_th)}%+连跌≥4天+低位<0.4', True,
                  lambda s, c=chg_th: (s['this_chg'] < c and s['cd'] >= 4
                      and s['pos60'] is not None and s['pos60'] < 0.4),
                  sub_samples=regime_samples)
            _eval(f'{regime_name}+跌>{abs(chg_th)}%+资金流入>1%', True,
                  lambda s, c=chg_th: (s['this_chg'] < c
                      and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] > 1),
                  sub_samples=regime_samples)
            _eval(f'{regime_name}+跌>{abs(chg_th)}%+缩量<0.7+低位<0.3', True,
                  lambda s, c=chg_th: (s['this_chg'] < c
                      and s['vol_ratio'] is not None and s['vol_ratio'] < 0.7
                      and s['pos60'] is not None and s['pos60'] < 0.3),
                  sub_samples=regime_samples)
        # 跌信号
        for chg_th in [3, 5, 8]:
            _eval(f'{regime_name}+涨>{chg_th}%+高位>0.7', False,
                  lambda s, c=chg_th: (s['this_chg'] > c
                      and s['pos60'] is not None and s['pos60'] >= 0.7),
                  sub_samples=regime_samples)
            _eval(f'{regime_name}+涨>{chg_th}%+冲高回落', False,
                  lambda s, c=chg_th: s['this_chg'] > c and s['rush_up_pullback'],
                  sub_samples=regime_samples)
            _eval(f'{regime_name}+涨>{chg_th}%+连涨≥3天+高位>0.6', False,
                  lambda s, c=chg_th: (s['this_chg'] > c and s['cu'] >= 3
                      and s['pos60'] is not None and s['pos60'] >= 0.6),
                  sub_samples=regime_samples)
            _eval(f'{regime_name}+涨>{chg_th}%+资金流出<-1%', False,
                  lambda s, c=chg_th: (s['this_chg'] > c
                      and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < -1),
                  sub_samples=regime_samples)

    # ================================================================
    # 第10部分: 尾日因子(周五效应)
    # ================================================================
    logger.info("\n" + "=" * 100)
    logger.info("  [10] 尾日因子(周五效应)")
    logger.info("=" * 100)

    logger.info("\n  ── 10.1 尾日涨跌 × 周涨跌 ──")
    # 尾日大跌+周跌 → 恐慌出尽 → 涨
    for ld_th in [-2, -3, -5]:
        for chg_th in [-2, -3, -5]:
            _eval(f'尾日跌>{abs(ld_th)}%+周跌>{abs(chg_th)}%+低位<0.3', True,
                  lambda s, l=ld_th, c=chg_th: (s['last_day'] < l and s['this_chg'] < c
                      and s['pos60'] is not None and s['pos60'] < 0.3))
    # 尾日大涨+周涨 → 追高 → 跌
    for ld_th in [2, 3, 5]:
        for chg_th in [3, 5]:
            _eval(f'尾日涨>{ld_th}%+周涨>{chg_th}%+高位>0.7', False,
                  lambda s, l=ld_th, c=chg_th: (s['last_day'] > l and s['this_chg'] > c
                      and s['pos60'] is not None and s['pos60'] >= 0.7))

    logger.info("\n  ── 10.2 尾日 × 资金 ──")
    _eval('尾日跌>3%+资金流入>1%+低位<0.4', True,
          lambda s: (s['last_day'] < -3
              and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] > 1
              and s['pos60'] is not None and s['pos60'] < 0.4))
    _eval('尾日涨>3%+资金流出<-1%+高位>0.6', False,
          lambda s: (s['last_day'] > 3
              and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < -1
              and s['pos60'] is not None and s['pos60'] >= 0.6))

    # ================================================================
    # 总结
    # ================================================================
    logger.info("\n" + "=" * 100)
    logger.info("  总结")
    logger.info("=" * 100)

    # CV>=65%的规则
    good = [c for c in candidates if c['cv_acc'] >= 65 and abs(c['gap']) < 15]
    good.sort(key=lambda x: -x['cv_acc'])
    logger.info("\n  ★ CV>=65%%的规则(%d条):", len(good))
    if not good:
        logger.info("    (无)")
    for c in good:
        d = '涨' if c['pred_up'] else '跌'
        logger.info("    [%s] %-60s CV%.1f%%(%d) 全%.1f%%(%d) gap%+.1f%% w65:%d/%d",
                    d, c['name'], c['cv_acc'], c['cv_total'],
                    c['full_acc'], c['total'], c['gap'],
                    c['weeks_above_65'], c['total_weeks'])

    # CV>=60%的规则
    marginal = [c for c in candidates if 60 <= c['cv_acc'] < 65 and abs(c['gap']) < 15]
    marginal.sort(key=lambda x: -x['cv_acc'])
    logger.info("\n  ⚠ CV>=60%%的边际规则(前30条, 共%d条):", len(marginal))
    for c in marginal[:30]:
        d = '涨' if c['pred_up'] else '跌'
        logger.info("    [%s] %-60s CV%.1f%%(%d) 全%.1f%%(%d) gap%+.1f%% w65:%d/%d w60:%d/%d",
                    d, c['name'], c['cv_acc'], c['cv_total'],
                    c['full_acc'], c['total'], c['gap'],
                    c['weeks_above_65'], c['total_weeks'],
                    c['weeks_above_60'], c['total_weeks'])

    # CV>=58%的规则
    low_marginal = [c for c in candidates if 58 <= c['cv_acc'] < 60 and abs(c['gap']) < 15]
    low_marginal.sort(key=lambda x: -x['cv_acc'])
    logger.info("\n  CV>=58%%的低边际规则(前20条, 共%d条):", len(low_marginal))
    for c in low_marginal[:20]:
        d = '涨' if c['pred_up'] else '跌'
        logger.info("    [%s] %-60s CV%.1f%%(%d) 全%.1f%%(%d) gap%+.1f%%",
                    d, c['name'], c['cv_acc'], c['cv_total'],
                    c['full_acc'], c['total'], c['gap'])

    # 保存结果
    result = {
        'timestamp': datetime.now().isoformat(),
        'total_bull_samples': len(bull),
        'total_candidates': len(candidates),
        'good_rules_65': [c for c in good],
        'marginal_rules_60': [c for c in marginal],
        'low_marginal_58': [c for c in low_marginal[:20]],
    }
    try:
        with open('data_results/nw_v11_bull_deep_v2.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
        logger.info("\n  结果已保存到 data_results/nw_v11_bull_deep_v2.json")
    except Exception as e:
        logger.warning("  保存失败: %s", e)

    return candidates


def run():
    logger.info("加载数据...")
    data = load_data(N_WEEKS)
    logger.info("构建样本...")
    samples = build_samples(data, N_WEEKS)
    if not samples:
        logger.error("无有效样本!")
        return
    logger.info("总样本: %d", len(samples))
    deep_bull_analysis(samples)


if __name__ == '__main__':
    run()
