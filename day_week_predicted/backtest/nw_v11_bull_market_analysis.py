#!/usr/bin/env python3
"""
V11大盘涨场景深度分析
=====================
专门分析大盘涨(mkt>=0%)时各种因子组合的准确率，
寻找可用的预测规则来填补V11的覆盖空白。

用法:
    python -m day_week_predicted.backtest.nw_v11_bull_market_analysis
"""
import sys, logging, math
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, '.')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s',
                    datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

from day_week_predicted.backtest.nw_v11_multifactor_backtest import (
    load_data, build_samples, _safe_mean, _safe_std, _pct,
    N_WEEKS, MIN_TRAIN_WEEKS,
)


def analyze_bull_market(samples):
    """深度分析大盘涨时的因子分布和准确率。"""
    all_weeks = sorted(set(s['iw_this'] for s in samples))

    # 按大盘场景分组
    regimes = {
        '大盘微涨(0~1%)': lambda s: 0 <= s['mkt_chg'] <= 1,
        '大盘涨(1~2%)': lambda s: 1 < s['mkt_chg'] <= 2,
        '大盘大涨(>2%)': lambda s: s['mkt_chg'] > 2,
        '大盘涨(>0%)': lambda s: s['mkt_chg'] > 0,
        '大盘涨(>=0%)': lambda s: s['mkt_chg'] >= 0,
    }

    logger.info("=" * 100)
    logger.info("  V11大盘涨场景深度分析")
    logger.info("=" * 100)

    # 1. 基础统计
    logger.info("\n[1] 各大盘场景基础统计:")
    for name, cond in regimes.items():
        sub = [s for s in samples if cond(s)]
        if not sub:
            continue
        up = sum(1 for s in sub if s['actual_up'])
        logger.info("  %-20s 样本:%d 涨:%d(%.1f%%) 跌:%d(%.1f%%)",
                    name, len(sub), up, up/len(sub)*100,
                    len(sub)-up, (len(sub)-up)/len(sub)*100)

    # 2. 大盘涨时(mkt>=0%)的因子分布
    bull_samples = [s for s in samples if s['mkt_chg'] >= 0]
    logger.info("\n  大盘涨(mkt>=0%%)总样本: %d", len(bull_samples))

    # 3. 大范围试算: 大盘涨时各种条件组合的准确率
    logger.info("\n[2] 大盘涨时(mkt>=0%%)候选规则试算:")
    logger.info("  %-65s %-12s %-12s %-8s", '规则', '全样本', 'CV', 'gap')
    logger.info("  " + "-" * 100)

    candidates = []

    def _eval_rule(name, pred_up, check_fn):
        total, correct = 0, 0
        for s in bull_samples:
            try:
                if check_fn(s):
                    total += 1
                    if pred_up == s['actual_up']:
                        correct += 1
            except (TypeError, KeyError):
                continue
        if total < 20:
            return
        full_acc = correct / total * 100

        # CV
        cv_total, cv_correct = 0, 0
        for ti in range(MIN_TRAIN_WEEKS, len(all_weeks)):
            tw = all_weeks[ti]
            for s in bull_samples:
                if s['iw_this'] != tw:
                    continue
                try:
                    if check_fn(s):
                        cv_total += 1
                        if pred_up == s['actual_up']:
                            cv_correct += 1
                except (TypeError, KeyError):
                    continue
        cv_acc = cv_correct / cv_total * 100 if cv_total > 0 else 0
        gap = full_acc - cv_acc

        flag = '★' if cv_acc >= 65 and abs(gap) < 12 else ('⚠' if cv_acc >= 58 else ' ')
        d = '涨' if pred_up else '跌'
        logger.info("  %s [%s] %-60s %s(%d) %s(%d) %+.1f%%",
                    flag, d, name,
                    _pct(correct, total), total,
                    _pct(cv_correct, cv_total), cv_total, gap)

        candidates.append({
            'name': name, 'pred_up': pred_up,
            'full_acc': full_acc, 'cv_acc': cv_acc, 'gap': gap,
            'total': total, 'cv_total': cv_total,
        })

    # ── 涨信号: 大盘涨+个股逆势跌 → 超跌反弹 ──
    logger.info("\n  ── 涨信号: 超跌反弹 ──")

    # 个股跌幅 × 位置
    for chg_th in [-2, -3, -5, -7]:
        _eval_rule(f'跌>{abs(chg_th)}%', True,
                   lambda s, c=chg_th: s['this_chg'] < c)
        _eval_rule(f'跌>{abs(chg_th)}%+低位<0.3', True,
                   lambda s, c=chg_th: s['this_chg'] < c and s['pos60'] is not None and s['pos60'] < 0.3)
        _eval_rule(f'跌>{abs(chg_th)}%+低位<0.2', True,
                   lambda s, c=chg_th: s['this_chg'] < c and s['pos60'] is not None and s['pos60'] < 0.2)
        _eval_rule(f'跌>{abs(chg_th)}%+非高位', True,
                   lambda s, c=chg_th: s['this_chg'] < c and not (s['pos60'] is not None and s['pos60'] >= 0.7))

    # 个股跌 × 连跌
    for chg_th in [-2, -3]:
        for cd_th in [2, 3, 4]:
            _eval_rule(f'跌>{abs(chg_th)}%+连跌≥{cd_th}天', True,
                       lambda s, c=chg_th, d=cd_th: s['this_chg'] < c and s['cd'] >= d)
            _eval_rule(f'跌>{abs(chg_th)}%+连跌≥{cd_th}天+低位<0.4', True,
                       lambda s, c=chg_th, d=cd_th: s['this_chg'] < c and s['cd'] >= d
                       and s['pos60'] is not None and s['pos60'] < 0.4)

    # 个股跌 × 前周跌
    for chg_th in [-2, -3]:
        _eval_rule(f'跌>{abs(chg_th)}%+前周跌>2%', True,
                   lambda s, c=chg_th: s['this_chg'] < c and s['prev_chg'] is not None and s['prev_chg'] < -2)
        _eval_rule(f'跌>{abs(chg_th)}%+前周跌>2%+非高位', True,
                   lambda s, c=chg_th: (s['this_chg'] < c and s['prev_chg'] is not None and s['prev_chg'] < -2
                                        and not (s['pos60'] is not None and s['pos60'] >= 0.7)))

    # 个股跌 × 成交量
    for chg_th in [-2, -3]:
        _eval_rule(f'跌>{abs(chg_th)}%+缩量<0.8', True,
                   lambda s, c=chg_th: s['this_chg'] < c and s['vol_ratio'] is not None and s['vol_ratio'] < 0.8)
        _eval_rule(f'跌>{abs(chg_th)}%+缩量<0.7+非高位', True,
                   lambda s, c=chg_th: (s['this_chg'] < c and s['vol_ratio'] is not None and s['vol_ratio'] < 0.7
                                        and not (s['pos60'] is not None and s['pos60'] >= 0.7)))

    # 个股跌 × 资金流向
    for chg_th in [-2, -3]:
        _eval_rule(f'跌>{abs(chg_th)}%+大单净流入>1%', True,
                   lambda s, c=chg_th: s['this_chg'] < c and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] > 1)
        _eval_rule(f'跌>{abs(chg_th)}%+大单净流入>2%', True,
                   lambda s, c=chg_th: s['this_chg'] < c and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] > 2)

    # 个股跌 × 板块
    for chg_th in [-2, -3]:
        _eval_rule(f'跌>{abs(chg_th)}%+板块跌<-1%', True,
                   lambda s, c=chg_th: s['this_chg'] < c and s['board_momentum'] is not None and s['board_momentum'] < -1)
        _eval_rule(f'跌>{abs(chg_th)}%+板块涨>1%', True,
                   lambda s, c=chg_th: s['this_chg'] < c and s['board_momentum'] is not None and s['board_momentum'] > 1)

    # 个股跌 × 尾日
    for chg_th in [-2, -3]:
        _eval_rule(f'跌>{abs(chg_th)}%+尾日跌>2%', True,
                   lambda s, c=chg_th: s['this_chg'] < c and s['last_day'] < -2)
        _eval_rule(f'跌>{abs(chg_th)}%+尾日跌>3%', True,
                   lambda s, c=chg_th: s['this_chg'] < c and s['last_day'] < -3)

    # 个股跌 × 探底回升/动量
    _eval_rule('跌>2%+探底回升', True,
               lambda s: s['this_chg'] < -2 and s['dip_recovery'])
    _eval_rule('跌>2%+探底回升+非高位', True,
               lambda s: s['this_chg'] < -2 and s['dip_recovery']
               and not (s['pos60'] is not None and s['pos60'] >= 0.7))
    _eval_rule('3周动量<-8%+非高位', True,
               lambda s: s['momentum_3w'] is not None and s['momentum_3w'] < -8
               and not (s['pos60'] is not None and s['pos60'] >= 0.7))
    _eval_rule('3周动量<-10%+低位<0.3', True,
               lambda s: s['momentum_3w'] is not None and s['momentum_3w'] < -10
               and s['pos60'] is not None and s['pos60'] < 0.3)
    _eval_rule('相对强弱<-5%+非高位', True,
               lambda s: s['relative_strength'] < -5
               and not (s['pos60'] is not None and s['pos60'] >= 0.7))
    _eval_rule('相对强弱<-3%+低位<0.3', True,
               lambda s: s['relative_strength'] < -3
               and s['pos60'] is not None and s['pos60'] < 0.3)

    # 个股跌 × 交易所
    for suffix in ['SZ', 'SH']:
        for chg_th in [-2, -3]:
            _eval_rule(f'{suffix}+跌>{abs(chg_th)}%+非高位', True,
                       lambda s, sx=suffix, c=chg_th: s['suffix'] == sx and s['this_chg'] < c
                       and not (s['pos60'] is not None and s['pos60'] >= 0.7))
            _eval_rule(f'{suffix}+跌>{abs(chg_th)}%+低位<0.3', True,
                       lambda s, sx=suffix, c=chg_th: s['suffix'] == sx and s['this_chg'] < c
                       and s['pos60'] is not None and s['pos60'] < 0.3)

    # ── 跌信号: 大盘涨+个股过热 → 回调 ──
    logger.info("\n  ── 跌信号: 过热回调 ──")

    for chg_th in [3, 5, 8, 10]:
        _eval_rule(f'涨>{chg_th}%', False,
                   lambda s, c=chg_th: s['this_chg'] > c)
        _eval_rule(f'涨>{chg_th}%+高位≥0.6', False,
                   lambda s, c=chg_th: s['this_chg'] > c and s['pos60'] is not None and s['pos60'] >= 0.6)
        _eval_rule(f'涨>{chg_th}%+高位≥0.7', False,
                   lambda s, c=chg_th: s['this_chg'] > c and s['pos60'] is not None and s['pos60'] >= 0.7)
        _eval_rule(f'涨>{chg_th}%+高位≥0.8', False,
                   lambda s, c=chg_th: s['this_chg'] > c and s['pos60'] is not None and s['pos60'] >= 0.8)

    # 涨 × 连涨
    for chg_th in [3, 5]:
        for cu_th in [2, 3, 4]:
            _eval_rule(f'涨>{chg_th}%+连涨≥{cu_th}天', False,
                       lambda s, c=chg_th, u=cu_th: s['this_chg'] > c and s['cu'] >= u)
            _eval_rule(f'涨>{chg_th}%+连涨≥{cu_th}天+高位≥0.6', False,
                       lambda s, c=chg_th, u=cu_th: s['this_chg'] > c and s['cu'] >= u
                       and s['pos60'] is not None and s['pos60'] >= 0.6)

    # 涨 × 放量
    for chg_th in [3, 5]:
        _eval_rule(f'涨>{chg_th}%+放量>1.3', False,
                   lambda s, c=chg_th: s['this_chg'] > c and s['vol_ratio'] is not None and s['vol_ratio'] > 1.3)
        _eval_rule(f'涨>{chg_th}%+放量>1.5+高位≥0.6', False,
                   lambda s, c=chg_th: (s['this_chg'] > c and s['vol_ratio'] is not None and s['vol_ratio'] > 1.5
                                        and s['pos60'] is not None and s['pos60'] >= 0.6))

    # 涨 × 量价背离
    for chg_th in [3, 5]:
        _eval_rule(f'涨>{chg_th}%+量价背离', False,
                   lambda s, c=chg_th: s['this_chg'] > c and s['vol_price_diverge'] == -1)
        _eval_rule(f'涨>{chg_th}%+量价背离+高位≥0.6', False,
                   lambda s, c=chg_th: s['this_chg'] > c and s['vol_price_diverge'] == -1
                   and s['pos60'] is not None and s['pos60'] >= 0.6)

    # 涨 × 冲高回落
    for chg_th in [3, 5]:
        _eval_rule(f'涨>{chg_th}%+冲高回落', False,
                   lambda s, c=chg_th: s['this_chg'] > c and s['rush_up_pullback'])
        _eval_rule(f'涨>{chg_th}%+冲高回落+高位≥0.6', False,
                   lambda s, c=chg_th: s['this_chg'] > c and s['rush_up_pullback']
                   and s['pos60'] is not None and s['pos60'] >= 0.6)

    # 涨 × 上影线
    for chg_th in [3, 5]:
        _eval_rule(f'涨>{chg_th}%+上影线>0.5', False,
                   lambda s, c=chg_th: s['this_chg'] > c
                   and s['upper_shadow_ratio'] is not None and s['upper_shadow_ratio'] > 0.5)
        _eval_rule(f'涨>{chg_th}%+上影线>0.6+高位≥0.6', False,
                   lambda s, c=chg_th: (s['this_chg'] > c
                   and s['upper_shadow_ratio'] is not None and s['upper_shadow_ratio'] > 0.6
                   and s['pos60'] is not None and s['pos60'] >= 0.6))

    # 涨 × 资金流出
    for chg_th in [3, 5]:
        _eval_rule(f'涨>{chg_th}%+大单净流出<-1%', False,
                   lambda s, c=chg_th: s['this_chg'] > c and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < -1)
        _eval_rule(f'涨>{chg_th}%+大单净流出<-2%', False,
                   lambda s, c=chg_th: s['this_chg'] > c and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < -2)

    # 涨 × 3周动量
    _eval_rule('3周动量>10%+高位≥0.7', False,
               lambda s: s['momentum_3w'] is not None and s['momentum_3w'] > 10
               and s['pos60'] is not None and s['pos60'] >= 0.7)
    _eval_rule('3周动量>8%+高位≥0.6', False,
               lambda s: s['momentum_3w'] is not None and s['momentum_3w'] > 8
               and s['pos60'] is not None and s['pos60'] >= 0.6)
    _eval_rule('相对强弱>5%+高位≥0.7', False,
               lambda s: s['relative_strength'] > 5
               and s['pos60'] is not None and s['pos60'] >= 0.7)

    # 涨 × 多因子组合
    for chg_th in [5, 8]:
        _eval_rule(f'涨>{chg_th}%+高位≥0.7+放量>1.3', False,
                   lambda s, c=chg_th: (s['this_chg'] > c
                   and s['pos60'] is not None and s['pos60'] >= 0.7
                   and s['vol_ratio'] is not None and s['vol_ratio'] > 1.3))
        _eval_rule(f'涨>{chg_th}%+高位≥0.7+大单流出<-1%', False,
                   lambda s, c=chg_th: (s['this_chg'] > c
                   and s['pos60'] is not None and s['pos60'] >= 0.7
                   and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < -1))
        _eval_rule(f'涨>{chg_th}%+连涨≥3天+高位≥0.7', False,
                   lambda s, c=chg_th: (s['this_chg'] > c and s['cu'] >= 3
                   and s['pos60'] is not None and s['pos60'] >= 0.7))

    # ── 按大盘微涨/涨分别试算 ──
    logger.info("\n  ── 仅大盘微涨(0~1%%)时 ──")
    slight_up = [s for s in samples if 0 <= s['mkt_chg'] <= 1]
    logger.info("  微涨样本: %d", len(slight_up))

    def _eval_rule_sub(name, pred_up, check_fn, sub_samples, prefix=''):
        total, correct = 0, 0
        for s in sub_samples:
            try:
                if check_fn(s):
                    total += 1
                    if pred_up == s['actual_up']:
                        correct += 1
            except (TypeError, KeyError):
                continue
        if total < 15:
            return
        full_acc = correct / total * 100
        cv_total, cv_correct = 0, 0
        for ti in range(MIN_TRAIN_WEEKS, len(all_weeks)):
            tw = all_weeks[ti]
            for s in sub_samples:
                if s['iw_this'] != tw:
                    continue
                try:
                    if check_fn(s):
                        cv_total += 1
                        if pred_up == s['actual_up']:
                            cv_correct += 1
                except (TypeError, KeyError):
                    continue
        cv_acc = cv_correct / cv_total * 100 if cv_total > 0 else 0
        gap = full_acc - cv_acc
        flag = '★' if cv_acc >= 65 and abs(gap) < 12 else ('⚠' if cv_acc >= 58 else ' ')
        d = '涨' if pred_up else '跌'
        logger.info("  %s [%s] %s%-55s %s(%d) %s(%d) %+.1f%%",
                    flag, d, prefix, name,
                    _pct(correct, total), total,
                    _pct(cv_correct, cv_total), cv_total, gap)

    for chg_th in [-2, -3, -5]:
        _eval_rule_sub(f'跌>{abs(chg_th)}%+非高位', True,
                       lambda s, c=chg_th: s['this_chg'] < c and not (s['pos60'] is not None and s['pos60'] >= 0.7),
                       slight_up, '微涨+')
        _eval_rule_sub(f'跌>{abs(chg_th)}%+低位<0.3', True,
                       lambda s, c=chg_th: s['this_chg'] < c and s['pos60'] is not None and s['pos60'] < 0.3,
                       slight_up, '微涨+')
        _eval_rule_sub(f'跌>{abs(chg_th)}%+连跌≥2天', True,
                       lambda s, c=chg_th: s['this_chg'] < c and s['cd'] >= 2,
                       slight_up, '微涨+')

    logger.info("\n  ── 仅大盘涨(>1%%)时 ──")
    strong_up = [s for s in samples if s['mkt_chg'] > 1]
    logger.info("  大盘涨样本: %d", len(strong_up))

    for chg_th in [-2, -3, -5]:
        _eval_rule_sub(f'跌>{abs(chg_th)}%+非高位', True,
                       lambda s, c=chg_th: s['this_chg'] < c and not (s['pos60'] is not None and s['pos60'] >= 0.7),
                       strong_up, '大盘涨+')
        _eval_rule_sub(f'跌>{abs(chg_th)}%+低位<0.3', True,
                       lambda s, c=chg_th: s['this_chg'] < c and s['pos60'] is not None and s['pos60'] < 0.3,
                       strong_up, '大盘涨+')

    # 3. 总结
    logger.info("\n" + "=" * 100)
    logger.info("  总结: CV>=65%%的规则")
    logger.info("=" * 100)
    good = [c for c in candidates if c['cv_acc'] >= 65 and abs(c['gap']) < 12]
    good.sort(key=lambda x: -x['cv_acc'])
    for c in good:
        d = '涨' if c['pred_up'] else '跌'
        logger.info("  [%s] %-60s 全样本%s(%d) CV%s(%d)",
                    d, c['name'],
                    _pct(int(c['full_acc']*c['total']/100), c['total']), c['total'],
                    _pct(int(c['cv_acc']*c['cv_total']/100), c['cv_total']), c['cv_total'])

    logger.info("\n  CV>=58%%的规则(边际):")
    marginal = [c for c in candidates if 58 <= c['cv_acc'] < 65 and abs(c['gap']) < 12]
    marginal.sort(key=lambda x: -x['cv_acc'])
    for c in marginal[:20]:
        d = '涨' if c['pred_up'] else '跌'
        logger.info("  [%s] %-60s 全样本%s(%d) CV%s(%d)",
                    d, c['name'],
                    _pct(int(c['full_acc']*c['total']/100), c['total']), c['total'],
                    _pct(int(c['cv_acc']*c['cv_total']/100), c['cv_total']), c['cv_total'])


def run():
    logger.info("加载数据...")
    data = load_data(N_WEEKS)
    logger.info("构建样本...")
    samples = build_samples(data, N_WEEKS)
    if not samples:
        logger.error("无样本!")
        return
    analyze_bull_market(samples)


if __name__ == '__main__':
    run()
