#!/usr/bin/env python3
"""
V4规则引擎 月度涨跌预测回测
============================
基于V4全场景规则引擎，将周级别预测扩展到月级别。

核心思路：
  - 用本月的特征（涨跌幅、大盘环境、位置、连涨连跌、量比等）预测下月涨跌
  - 规则从V4v4周规则适配而来，阈值根据月度波动特性调整
  - 多维度验证：全样本、滚动窗口、按市场、按规则、按月份

月度规则设计（基于V4v4周规则扩展）：
  涨信号:
    MR1: 大盘月跌>5% + 个股月跌>3% → 涨 (类比R1大盘深跌)
    MR2: 上证+大盘月跌2-5%+个股月跌>8%+非高位 → 涨 (类比R2)
    MR3: 上证+大盘月跌2-5%+个股月跌>5%+前月跌 → 涨 (类比R3)
    MR4: 上证+大盘月跌2-5%+个股月跌>5%+低位 → 涨 (类比R4)
    MR5a: 深证+大盘月微跌(0~2%)+个股月跌>3%+连跌≥15天 → 涨 (类比R5a)
    MR5b: 深证+大盘月微跌+个股月跌>3%+低位<0.2 → 涨 (类比R5b)
    MR5c: 深证+大盘月微跌+个股月跌>3% → 涨 (类比R5c)
  跌信号:
    MR6: 深证+大盘月跌2~5%+个股月涨>8% → 跌 (类比R6)
    MR6b: 深证+大盘月跌2~5%+个股月涨>3%+连涨≥15天 → 跌 (类比R6b)
    MR7: 个股月跌>5%+前期连涨≥10天+非高位 → 跌 (类比R7)
    MR8: 个股月涨>15%+尾周跌>5%+前月涨>5% → 跌 (类比R8)

用法：
    python -m day_week_predicted.backtest.monthly_v4_rules_backtest
"""
import sys
import logging
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, '.')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

from dao import get_connection
from service.weekly_prediction_service import (
    _get_all_stock_codes, _get_latest_trade_date, _to_float,
    _compound_return, _get_stock_index,
)

N_MONTHS = 7  # 回测月数


# ══════════════════════════════════════════════════
# 月度V4规则集 V2 — 基于第一轮回测数据反馈优化
# ══════════════════════════════════════════════════
# 第一轮发现：
#   - MR5b/MR5c 涨信号准确率27-31%，严重反向 → 删除或反转
#   - MR10(月涨>12%+高位→跌) 51.7%接近随机 → 提高阈值
#   - MR8(月大涨+尾周回落) 49.1%无效 → 删除
#   - MR12(缩量月跌+低位→涨) 69.6%有效 → 保留提升优先级
#   - MR11(连续大涨后回调) 60.9% → 保留
#   - MR9(月跌>10%+低位→涨) 58.7% → 收紧条件
#   - Tier 1 仅40.1% → 周规则直接搬月度不适用，需重新设计
# ══════════════════════════════════════════════════
MONTHLY_V4_RULES = [
    # ══════════════════════════════════════════════════
    # Tier 1: 高置信涨信号
    # ══════════════════════════════════════════════════

    # MR1: 缩量月跌+低位 → 涨 (第一轮69.6%，最佳规则)
    {'name': 'MR1:缩量月跌+低位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -5
                         and f['vol_ratio'] is not None and f['vol_ratio'] < 0.6
                         and f['pos60'] is not None and f['pos60'] < 0.3)},

    # MR2: 月跌>15%+极低位<0.15 → 涨 (深跌极端反弹，收紧MR9)
    {'name': 'MR2:月跌>15%+极低位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -15
                         and f['pos60'] is not None and f['pos60'] < 0.15)},

    # MR3: 大盘月深跌>5%+个股月跌>5%+低位<0.3 → 涨 (加低位约束)
    {'name': 'MR3:大盘深跌+个股跌+低位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -5 and f['mkt_chg'] < -5
                         and f['pos60'] is not None and f['pos60'] < 0.3)},

    # MR4: 月跌>10%+缩量<0.7 → 涨 (放宽缩量条件)
    {'name': 'MR4:月跌>10%+缩量→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -10
                         and f['vol_ratio'] is not None and f['vol_ratio'] < 0.7)},

    # ══════════════════════════════════════════════════
    # Tier 1: 高置信跌信号
    # ══════════════════════════════════════════════════

    # MR5: 月涨>8%+前月大涨>8% → 跌 (第一轮60.9%，连续大涨回调)
    {'name': 'MR5:月涨>8%+前月大涨→跌', 'pred_up': False, 'tier': 1,
     'check': lambda f: (f['this_chg'] > 8
                         and f['prev_chg'] is not None and f['prev_chg'] > 8)},

    # MR6: 深证+大盘月跌2~5%+个股月涨>8% → 跌 (第一轮60.7%)
    {'name': 'MR6:深证+大盘跌+月涨>8%→跌', 'pred_up': False, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ'
                         and -5 <= f['mkt_chg'] < -2
                         and f['this_chg'] > 8)},

    # MR7: 深证+大盘微跌+个股月跌>3% → 跌 (第一轮反向27-31%，反转为跌信号)
    {'name': 'MR7:深证+大盘微跌+月跌>3%→跌', 'pred_up': False, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ'
                         and -2 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -3)},

    # ══════════════════════════════════════════════════
    # Tier 2: 中等置信
    # ══════════════════════════════════════════════════

    # MR8: 月涨>20%+高位>0.85 → 跌 (收紧MR10阈值)
    {'name': 'MR8:月涨>20%+极高位→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['this_chg'] > 20
                         and f['pos60'] is not None and f['pos60'] > 0.85)},

    # MR9: 月跌>10%+低位<0.3 → 涨 (保留但降级)
    {'name': 'MR9:月跌>10%+低位→涨', 'pred_up': True, 'tier': 2,
     'check': lambda f: (f['this_chg'] < -10
                         and f['pos60'] is not None and f['pos60'] < 0.3)},

    # MR10: 月涨>15%+前月涨>5%+高位>0.7 → 跌 (多条件组合)
    {'name': 'MR10:月大涨+前月涨+高位→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['this_chg'] > 15
                         and f['prev_chg'] is not None and f['prev_chg'] > 5
                         and f['pos60'] is not None and f['pos60'] > 0.7)},

    # MR11: 大盘月跌>3%+个股月涨>5%+高位>0.7 → 跌 (逆势高位)
    {'name': 'MR11:大盘跌+个股涨+高位→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['mkt_chg'] < -3 and f['this_chg'] > 5
                         and f['pos60'] is not None and f['pos60'] > 0.7)},

    # MR12: 月跌>5%+前月跌>5%+低位<0.25 → 涨 (连续下跌+低位反弹)
    {'name': 'MR12:连续月跌+低位→涨', 'pred_up': True, 'tier': 2,
     'check': lambda f: (f['this_chg'] < -5
                         and f['prev_chg'] is not None and f['prev_chg'] < -5
                         and f['pos60'] is not None and f['pos60'] < 0.25)},

    # MR13: 放量月涨>10%+高位>0.75 → 跌 (放量冲高回落)
    {'name': 'MR13:放量月涨+高位→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['this_chg'] > 10
                         and f['vol_ratio'] is not None and f['vol_ratio'] > 1.5
                         and f['pos60'] is not None and f['pos60'] > 0.75)},
]


def _group_by_month(klines):
    """将日K线按自然月分组。返回 {(year, month): [kline_dicts]}"""
    groups = defaultdict(list)
    for k in klines:
        d = k['date']
        if isinstance(d, str):
            dt = datetime.strptime(d, '%Y-%m-%d')
        else:
            dt = d
        groups[(dt.year, dt.month)].append(k)
    for key in groups:
        groups[key].sort(key=lambda x: x['date'])
    return groups


def run_backtest(n_months=N_MONTHS, sample_limit=0):
    t0 = datetime.now()
    logger.info("=" * 80)
    logger.info("  V4规则引擎 月度涨跌预测回测")
    logger.info("  回测月数: %d", n_months)
    logger.info("=" * 80)

    latest_date = _get_latest_trade_date()
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    # 需要额外的历史数据用于计算pos60等
    dt_start = dt_end - timedelta(days=(n_months + 3) * 31 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')

    # 回测起始月份（只评估最近n_months个月）
    dt_cutoff = dt_end - timedelta(days=n_months * 31 + 31)

    all_codes = _get_all_stock_codes()
    if sample_limit > 0:
        all_codes = all_codes[:sample_limit]
    logger.info("股票数: %d", len(all_codes))

    # ── 加载数据 ──
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    logger.info("加载个股K线...")
    stock_klines = defaultdict(list)
    bs = 200
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,close_price,change_percent,trading_volume "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            stock_klines[r['stock_code']].append({
                'date': r['date'], 'close': _to_float(r['close_price']),
                'change_percent': _to_float(r['change_percent']),
                'volume': _to_float(r['trading_volume']),
            })

    logger.info("加载指数K线...")
    idx_codes = list(set(_get_stock_index(c) for c in all_codes))
    for idx in ('000001.SH', '399001.SZ', '899050.SZ'):
        if idx not in idx_codes:
            idx_codes.append(idx)
    ph = ','.join(['%s'] * len(idx_codes))
    cur.execute(
        f"SELECT stock_code,`date`,change_percent FROM stock_kline "
        f"WHERE stock_code IN ({ph}) AND `date`>=%s AND `date`<=%s ORDER BY `date`",
        idx_codes + [start_date, latest_date])
    mkt_kl = defaultdict(list)
    for r in cur.fetchall():
        mkt_kl[r['stock_code']].append({
            'date': r['date'],
            'change_percent': _to_float(r['change_percent']),
        })
    conn.close()

    # 指数按月分组
    mkt_by_month = {}
    for ic, kl in mkt_kl.items():
        mkt_by_month[ic] = _group_by_month(kl)

    logger.info("数据加载完成, 开始月度回测...")

    # ── 统计变量 ──
    all_month_samples = 0
    by_rule = defaultdict(lambda: {'correct': 0, 'total': 0})
    by_tier = defaultdict(lambda: {'correct': 0, 'total': 0})
    total_pred = 0
    total_correct = 0
    by_suffix = defaultdict(lambda: {'pred': 0, 'correct': 0, 'total': 0})
    by_ym = defaultdict(lambda: {'pred': 0, 'correct': 0, 'total': 0})
    # 滚动窗口验证
    rolling_results = []
    # 详细记录（用于后续分析）
    all_details = []

    processed = 0
    for code in all_codes:
        klines = stock_klines.get(code, [])
        if not klines or len(klines) < 60:
            continue

        stock_idx = _get_stock_index(code)
        suffix = stock_idx.split('.')[-1] if '.' in stock_idx else ''
        idx_months = mkt_by_month.get(stock_idx, {})

        month_groups = _group_by_month(klines)
        sorted_months = sorted(month_groups.keys())
        sorted_all = sorted(klines, key=lambda x: x['date'])

        for i in range(len(sorted_months) - 1):
            ym_this = sorted_months[i]
            ym_next = sorted_months[i + 1]
            this_days = month_groups[ym_this]
            next_days = month_groups[ym_next]

            if len(this_days) < 10 or len(next_days) < 10:
                continue

            # 检查是否在回测窗口内
            dt_first = datetime.strptime(this_days[0]['date'], '%Y-%m-%d')
            if dt_first < dt_cutoff:
                continue

            # 本月涨跌
            this_pcts = [d['change_percent'] for d in this_days]
            this_chg = _compound_return(this_pcts)

            # 下月涨跌（实际值）
            next_chg = _compound_return(
                [d['change_percent'] for d in next_days])
            actual_up = next_chg >= 0

            # 大盘本月涨跌
            mkt_days = idx_months.get(ym_this, [])
            mkt_chg = _compound_return(
                [k['change_percent'] for k in sorted(
                    mkt_days, key=lambda x: x['date'])]
            ) if len(mkt_days) >= 10 else 0.0

            # 60日位置
            first_date = this_days[0]['date']
            hist = [k for k in sorted_all if k['date'] < first_date]
            pos60 = None
            if len(hist) >= 20:
                hc = [k['close'] for k in hist[-60:] if k['close'] > 0]
                if hc:
                    ac = hc + [k['close'] for k in this_days
                               if k['close'] > 0]
                    mn, mx = min(ac), max(ac)
                    lc = this_days[-1]['close']
                    if mx > mn and lc > 0:
                        pos60 = (lc - mn) / (mx - mn)

            # 前月涨跌
            prev_chg = None
            if i > 0:
                prev_ym = sorted_months[i - 1]
                prev_days = month_groups[prev_ym]
                if len(prev_days) >= 10:
                    prev_chg = _compound_return(
                        [k['change_percent'] for k in prev_days])

            # 连跌/连涨天数（本月末尾）
            cd = 0
            cu = 0
            for p in reversed(this_pcts):
                if p < 0:
                    cd += 1
                    if cu > 0:
                        break
                elif p > 0:
                    cu += 1
                    if cd > 0:
                        break
                else:
                    break

            # 量比（本月 vs 前20日均量）
            vol_ratio = None
            tv = [d['volume'] for d in this_days if d['volume'] > 0]
            hv = [k['volume'] for k in hist[-20:] if k['volume'] > 0]
            if tv and hv:
                at = sum(tv) / len(tv)
                ah = sum(hv) / len(hv)
                if ah > 0:
                    vol_ratio = at / ah

            # 尾周涨跌（本月最后5个交易日）
            last_week_days = this_days[-5:]
            last_week_chg = _compound_return(
                [d['change_percent'] for d in last_week_days]
            ) if len(last_week_days) >= 3 else 0.0

            # 最后一天涨跌
            last_day = this_pcts[-1] if this_pcts else 0

            all_month_samples += 1
            by_suffix[suffix]['total'] += 1
            ym_str = f"{ym_this[0]}-{ym_this[1]:02d}"
            by_ym[ym_str]['total'] += 1

            feat = {
                'this_chg': this_chg,
                'mkt_chg': mkt_chg,
                'pos60': pos60,
                'prev_chg': prev_chg,
                'cd': cd,
                'cu': cu,
                'vol_ratio': vol_ratio,
                'suffix': suffix,
                'last_day': last_day,
                'last_week_chg': last_week_chg,
            }

            # 规则匹配
            matched = None
            for rule in MONTHLY_V4_RULES:
                if rule['check'](feat):
                    matched = rule
                    break

            if matched:
                is_correct = matched['pred_up'] == actual_up
                total_pred += 1
                if is_correct:
                    total_correct += 1
                by_rule[matched['name']]['total'] += 1
                if is_correct:
                    by_rule[matched['name']]['correct'] += 1
                by_tier[matched['tier']]['total'] += 1
                if is_correct:
                    by_tier[matched['tier']]['correct'] += 1
                by_suffix[suffix]['pred'] += 1
                if is_correct:
                    by_suffix[suffix]['correct'] += 1
                by_ym[ym_str]['pred'] += 1
                if is_correct:
                    by_ym[ym_str]['correct'] += 1

                all_details.append({
                    'code': code, 'ym': ym_str,
                    'this_chg': round(this_chg, 2),
                    'next_chg': round(next_chg, 2),
                    'mkt_chg': round(mkt_chg, 2),
                    'rule': matched['name'],
                    'pred_up': matched['pred_up'],
                    'actual_up': actual_up,
                    'correct': is_correct,
                    'tier': matched['tier'],
                })

        processed += 1
        if processed % 1000 == 0:
            logger.info("  已处理 %d/%d ...", processed, len(all_codes))

    # ── 滚动窗口验证 ──
    logger.info("执行滚动窗口验证...")
    all_yms = sorted(by_ym.keys())
    for i in range(len(all_yms)):
        ym = all_yms[i]
        ym_details = [d for d in all_details if d['ym'] == ym]
        if not ym_details:
            continue
        ym_correct = sum(1 for d in ym_details if d['correct'])
        ym_total = len(ym_details)
        rolling_results.append({
            'month': ym,
            'accuracy': round(ym_correct / ym_total * 100, 1)
                        if ym_total > 0 else 0,
            'correct': ym_correct,
            'total': ym_total,
            'all_samples': by_ym[ym]['total'],
            'coverage': round(ym_total / by_ym[ym]['total'] * 100, 1)
                        if by_ym[ym]['total'] > 0 else 0,
        })

    # ── 输出结果 ──
    elapsed = (datetime.now() - t0).total_seconds()
    _p = lambda c, t: f"{c / t * 100:.1f}%" if t > 0 else "N/A"

    logger.info("")
    logger.info("=" * 80)
    logger.info("  V4规则引擎 月度涨跌预测回测结果")
    logger.info("=" * 80)

    logger.info("  总可评估月样本: %d", all_month_samples)
    logger.info("  预测命中: %s (%d/%d) 覆盖率%s",
                _p(total_correct, total_pred), total_correct, total_pred,
                _p(total_pred, all_month_samples))

    logger.info("")
    logger.info("  ── 按Tier ──")
    for t in sorted(by_tier.keys()):
        s = by_tier[t]
        logger.info("    Tier %d: %s (%d/%d)", t,
                     _p(s['correct'], s['total']), s['correct'], s['total'])

    logger.info("")
    logger.info("  ── 按规则 ──")
    for rn in sorted(by_rule.keys()):
        s = by_rule[rn]
        logger.info("    %-50s %s (%d/%d)", rn,
                     _p(s['correct'], s['total']), s['correct'], s['total'])

    logger.info("")
    logger.info("  ── 按市场 ──")
    for sfx in sorted(by_suffix.keys()):
        s = by_suffix[sfx]
        logger.info("    %s: 预测%s (%d/%d) 覆盖%s",
                     sfx, _p(s['correct'], s['pred']),
                     s['correct'], s['pred'],
                     _p(s['pred'], s['total']))

    logger.info("")
    logger.info("  ── 按月份(滚动窗口) ──")
    for r in rolling_results:
        logger.info("    %s: 准确率%s (%d/%d) 覆盖%s (总样本%d)",
                     r['month'], f"{r['accuracy']:.1f}%",
                     r['correct'], r['total'],
                     f"{r['coverage']:.1f}%", r['all_samples'])

    # 稳定性检验：各月准确率标准差
    if rolling_results:
        accs = [r['accuracy'] for r in rolling_results if r['total'] >= 10]
        if len(accs) >= 2:
            avg_acc = sum(accs) / len(accs)
            std_acc = (sum((a - avg_acc) ** 2 for a in accs)
                       / len(accs)) ** 0.5
            logger.info("")
            logger.info("  ── 稳定性检验 ──")
            logger.info("    月均准确率: %.1f%%", avg_acc)
            logger.info("    准确率标准差: %.1f%%", std_acc)
            logger.info("    最高月: %.1f%%  最低月: %.1f%%",
                         max(accs), min(accs))
            logger.info("    稳定性评级: %s",
                         "优" if std_acc < 5 else
                         "良" if std_acc < 10 else
                         "中" if std_acc < 15 else "差")

    # 涨跌信号分别统计
    up_rules = [d for d in all_details if d['pred_up']]
    dn_rules = [d for d in all_details if not d['pred_up']]
    if up_rules:
        up_correct = sum(1 for d in up_rules if d['correct'])
        logger.info("")
        logger.info("  ── 涨/跌信号分析 ──")
        logger.info("    涨信号: %s (%d/%d)",
                     _p(up_correct, len(up_rules)),
                     up_correct, len(up_rules))
    if dn_rules:
        dn_correct = sum(1 for d in dn_rules if d['correct'])
        logger.info("    跌信号: %s (%d/%d)",
                     _p(dn_correct, len(dn_rules)),
                     dn_correct, len(dn_rules))

    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 80)

    return {
        'total_samples': all_month_samples,
        'total_pred': total_pred,
        'total_correct': total_correct,
        'accuracy': round(total_correct / total_pred * 100, 1)
                    if total_pred > 0 else 0,
        'coverage': round(total_pred / all_month_samples * 100, 1)
                    if all_month_samples > 0 else 0,
        'by_rule': dict(by_rule),
        'by_tier': dict(by_tier),
        'by_month': rolling_results,
        'details': all_details,
    }


if __name__ == '__main__':
    run_backtest(n_months=N_MONTHS, sample_limit=0)
