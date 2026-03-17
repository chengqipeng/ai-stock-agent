#!/usr/bin/env python3
"""
成交量深度调优 V2 — 精确策略构建与验证
================================================================
基于V1分析发现的关键因子，构建多层精确策略并验证。

核心发现:
  1. 大盘跌幅是最强因子: 跌3-5%→86.3%, 跌>5%→91.0%
  2. 深证大盘跌1-2%只有50.2%, 跌2-3%只有40.2% — 主要拖累源
  3. 个股跌幅: 跌>5%→68.2%, 跌>8%→77.6%
  4. 价格位置: 低位<0.2→77.4%, 高位>0.8→47.4%
  5. 连跌天数: ≥2天→78.7%, ≥4天→84.9%
  6. 量比: 缩量<0.6→73.8%, 巨量>3.0→57.5%
  7. 前周动量: 前周跌→77.1%, 前周大涨→63.5%
  8. 尾日放量1.2-2.0→76.3%, 量能递减→61.7%

策略思路:
  - Tier1a: 大盘深跌(>3%) + 个股跌 → 高置信涨 (目标85%+)
  - Tier1b: 大盘跌(1-3%) + 多因子过滤 → 中高置信涨 (目标70%+)
  - Tier1c: 连跌+缩量+低位 → 补充覆盖 (目标65%+)
  - 整体目标: ≥70%准确率, 覆盖率≥10%

用法:
    python -m day_week_predicted.backtest.volume_deep_optimize_v2
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
    _get_all_stock_codes,
    _get_latest_trade_date,
    _to_float,
    _compound_return,
    _mean,
    _std,
    _get_stock_index,
)


def _compute_sample_features(this_days, all_klines, this_pcts, market_chg, stock_idx):
    """计算单个样本的全部特征。"""
    feat = {}
    feat['this_chg'] = _compound_return(this_pcts)
    feat['market_chg'] = market_chg
    feat['stock_idx'] = stock_idx

    # 连跌/连涨天数
    consec_down = 0
    consec_up = 0
    for p in reversed(this_pcts):
        if p < 0:
            consec_down += 1
            if consec_up > 0:
                break
        elif p > 0:
            consec_up += 1
            if consec_down > 0:
                break
        else:
            break
    feat['consec_down'] = consec_down
    feat['consec_up'] = consec_up
    feat['last_day_chg'] = this_pcts[-1] if this_pcts else 0

    sorted_all = sorted(all_klines, key=lambda x: x['date'])
    first_date = this_days[0]['date']
    hist = [k for k in sorted_all if k['date'] < first_date]

    paired = [(k['change_percent'], k.get('volume', 0)) for k in this_days if k.get('volume', 0) > 0]
    if not paired or len(hist) < 20:
        return feat

    week_vols = [p[1] for p in paired]
    week_avg_vol = _mean(week_vols)

    h20_vols = [k.get('volume', 0) for k in hist[-20:] if k.get('volume', 0) > 0]
    h60_vols = [k.get('volume', 0) for k in hist[-60:] if k.get('volume', 0) > 0]
    avg20 = _mean(h20_vols) if h20_vols else 0
    avg60 = _mean(h60_vols) if h60_vols else 0

    feat['vol_ratio_20'] = week_avg_vol / avg20 if avg20 > 0 else None

    # 价格位置
    hist_closes = [k.get('close', 0) for k in hist[-60:] if k.get('close', 0) > 0]
    if hist_closes:
        all_c = hist_closes + [k.get('close', 0) for k in this_days if k.get('close', 0) > 0]
        min_c, max_c = min(all_c), max(all_c)
        latest_c = this_days[-1].get('close', 0)
        feat['price_pos_60'] = (latest_c - min_c) / (max_c - min_c) if max_c > min_c and latest_c > 0 else None
    else:
        feat['price_pos_60'] = None

    # 尾日量比
    last_vol = this_days[-1].get('volume', 0)
    feat['last_day_vol_ratio'] = last_vol / avg20 if avg20 > 0 and last_vol > 0 else None

    # 周内量能趋势
    if len(week_vols) >= 4:
        mid = len(week_vols) // 2
        feat['vol_intra_ratio'] = _mean(week_vols[mid:]) / _mean(week_vols[:mid]) if _mean(week_vols[:mid]) > 0 else None
    else:
        feat['vol_intra_ratio'] = None

    # 前一周涨跌幅
    prev_klines = hist[-5:] if len(hist) >= 5 else hist
    feat['prev_week_chg'] = _compound_return([k['change_percent'] for k in prev_klines]) if prev_klines else None

    # 振幅
    week_highs = [k.get('high', 0) for k in this_days if k.get('high', 0) > 0]
    week_lows = [k.get('low', 0) for k in this_days if k.get('low', 0) > 0]
    if week_highs and week_lows:
        wk_high, wk_low = max(week_highs), min(week_lows)
        feat['week_amplitude'] = (wk_high - wk_low) / wk_low * 100 if wk_low > 0 else None
    else:
        feat['week_amplitude'] = None

    # 下影线比例（最后一天）
    last_k = this_days[-1]
    o, c, h, l = last_k.get('open', 0), last_k.get('close', 0), last_k.get('high', 0), last_k.get('low', 0)
    total = h - l
    if total > 0:
        feat['lower_shadow_ratio'] = (min(o, c) - l) / total
    else:
        feat['lower_shadow_ratio'] = None

    return feat


# ══════════════════════════════════════════════════════════
# 新规则定义 — 基于V1分析结果
# ══════════════════════════════════════════════════════════

_INDEX_MKT_THRESHOLD = {
    '000001.SH': 1.0,
    '399001.SZ': 1.5,
    '899050.SZ': 2.0,
}

def _new_rules_match(feat):
    """新版多层规则引擎 V2.5 — 最终优化版。

    策略: 只保留单独≥65%准确率的规则，去掉Tier2。
    T1a(89.6%) + T1b-SH(73.4%) + 上证其他过滤规则
    深证在大盘跌1-3%区间全部放弃（准确率<50%）
    """
    chg = feat.get('this_chg', 0)
    mkt = feat.get('market_chg', 0)
    cd = feat.get('consec_down', 0)
    pp = feat.get('price_pos_60')
    vr = feat.get('vol_ratio_20')
    prev_chg = feat.get('prev_week_chg')
    last_vol_r = feat.get('last_day_vol_ratio')
    vol_intra = feat.get('vol_intra_ratio')
    idx = feat.get('stock_idx', '000001.SH')
    mkt_t = _INDEX_MKT_THRESHOLD.get(idx, 1.0)

    # ── Tier 1a: 大盘深跌(>3%) + 个股跌>2% → 涨 ──
    # 回测: 89.6%, 6297样本 (上证+深证都很高)
    if chg < -2 and mkt < -3:
        return ('T1a:大盘深跌>3%+个股跌→涨', True, 1)

    # ── 以下仅上证 (深证在大盘跌1-3%区间准确率<50%, 全部放弃) ──
    if idx == '000001.SH' and mkt < -1.0 and mkt >= -3:

        # T1b-SH: 个股跌>5% + 非高位 → 涨 (73.4%)
        if chg < -5:
            is_high_pos = pp is not None and pp >= 0.7
            if not is_high_pos:
                return ('T1b-SH:上证+大盘跌+跌>5%+非高位→涨', True, 1)

        # T1c-SH: 个股跌>3% + 前周跌<-2% + 非高位 → 涨
        if chg < -3 and prev_chg is not None and prev_chg < -2:
            is_high_pos = pp is not None and pp >= 0.8
            if not is_high_pos:
                return ('T1d-SH:上证+大盘跌+跌>3%+前周跌→涨', True, 1)

        # T1e-SH: 个股跌>3% + 低位<0.2 → 涨
        if chg < -3 and pp is not None and pp < 0.2:
            return ('T1e-SH:上证+大盘跌+跌>3%+低位→涨', True, 1)

    return None


def run_v2_backtest(n_weeks=29, sample_limit=0):
    """V2策略回测。"""
    t_start = datetime.now()
    logger.info("=" * 80)
    logger.info("  V2策略回测 — 目标: 70%%准确率")
    logger.info("=" * 80)

    latest_date = _get_latest_trade_date()
    if not latest_date:
        logger.error("无法获取最新交易日")
        return
    logger.info("最新交易日: %s", latest_date)

    all_codes = _get_all_stock_codes()
    if sample_limit > 0:
        all_codes = all_codes[:sample_limit]
    logger.info("股票数: %d", len(all_codes))

    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(n_weeks + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')

    # ── 加载数据 ──
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    logger.info("加载个股K线...")
    stock_klines = defaultdict(list)
    batch_size = 200
    for i in range(0, len(all_codes), batch_size):
        batch = all_codes[i:i + batch_size]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, open_price, close_price, high_price, "
            f"low_price, change_percent, trading_volume "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [start_date, latest_date])
        for row in cur.fetchall():
            stock_klines[row['stock_code']].append({
                'date': row['date'],
                'open': _to_float(row['open_price']),
                'close': _to_float(row['close_price']),
                'high': _to_float(row['high_price']),
                'low': _to_float(row['low_price']),
                'change_percent': _to_float(row['change_percent']),
                'volume': _to_float(row['trading_volume']),
            })

    logger.info("加载指数K线...")
    all_index_codes = list(set(_get_stock_index(c) for c in all_codes))
    for idx in ('000001.SH', '399001.SZ', '899050.SZ'):
        if idx not in all_index_codes:
            all_index_codes.append(idx)
    ph_idx = ','.join(['%s'] * len(all_index_codes))
    cur.execute(
        f"SELECT stock_code, `date`, change_percent FROM stock_kline "
        f"WHERE stock_code IN ({ph_idx}) AND `date` >= %s AND `date` <= %s "
        f"ORDER BY `date`", all_index_codes + [start_date, latest_date])
    market_klines_by_index = defaultdict(list)
    for r in cur.fetchall():
        market_klines_by_index[r['stock_code']].append({
            'date': r['date'],
            'change_percent': _to_float(r['change_percent']),
        })
    conn.close()

    market_by_week_by_index = {}
    for idx_code, klines_list in market_klines_by_index.items():
        by_week = defaultdict(list)
        for k in klines_list:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            by_week[iw].append(k)
        market_by_week_by_index[idx_code] = by_week

    logger.info("数据加载完成: %d只股票", len(stock_klines))

    # ── 回测 ──
    all_weeks = 0
    # 旧规则
    old_total = 0
    old_correct = 0
    old_by_rule = defaultdict(lambda: {'correct': 0, 'total': 0})
    # 新规则
    new_total = 0
    new_correct = 0
    new_by_rule = defaultdict(lambda: {'correct': 0, 'total': 0})
    new_by_tier = defaultdict(lambda: {'correct': 0, 'total': 0})

    processed = 0
    for code in all_codes:
        klines = stock_klines.get(code, [])
        if not klines or len(klines) < 60:
            continue

        stock_idx = _get_stock_index(code)
        idx_by_week = market_by_week_by_index.get(stock_idx, {})

        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            wg[iw].append(k)

        sorted_weeks = sorted(wg.keys())

        for i in range(len(sorted_weeks) - 1):
            iw_this = sorted_weeks[i]
            iw_next = sorted_weeks[i + 1]

            this_days = sorted(wg[iw_this], key=lambda x: x['date'])
            next_days = sorted(wg[iw_next], key=lambda x: x['date'])

            if len(this_days) < 3 or len(next_days) < 3:
                continue

            dt_this = datetime.strptime(this_days[0]['date'], '%Y-%m-%d')
            if dt_this < dt_end - timedelta(days=n_weeks * 7 + 14):
                continue

            this_pcts = [d['change_percent'] for d in this_days]
            next_pcts = [d['change_percent'] for d in next_days]
            next_week_chg = _compound_return(next_pcts)
            actual_up = next_week_chg >= 0

            mw = idx_by_week.get(iw_this, [])
            market_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            all_weeks += 1

            # 计算特征
            feat = _compute_sample_features(this_days, klines, this_pcts, market_chg, stock_idx)

            # 旧规则 (简化版)
            this_chg = feat['this_chg']
            mkt_t = _INDEX_MKT_THRESHOLD.get(stock_idx, 1.0)
            old_rule = None
            if this_chg < -2 and market_chg < -mkt_t:
                old_rule = '跌>2%+大盘深跌→涨'
                old_pred_up = True
            elif this_chg < -5 and stock_idx == '000001.SH':
                old_rule = '周跌>5%→继续跌'
                old_pred_up = False

            if old_rule:
                old_total += 1
                old_by_rule[old_rule]['total'] += 1
                is_correct = old_pred_up == actual_up
                if is_correct:
                    old_correct += 1
                    old_by_rule[old_rule]['correct'] += 1

            # 新规则
            result = _new_rules_match(feat)
            if result:
                rn, pred_up, tier = result
                new_total += 1
                new_by_rule[rn]['total'] += 1
                new_by_tier[tier]['total'] += 1
                is_correct = pred_up == actual_up
                if is_correct:
                    new_correct += 1
                    new_by_rule[rn]['correct'] += 1
                    new_by_tier[tier]['correct'] += 1

        processed += 1
        if processed % 500 == 0:
            logger.info("  已处理 %d/%d ...", processed, len(all_codes))

    # ── 输出结果 ──
    elapsed = (datetime.now() - t_start).total_seconds()
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"

    logger.info("")
    logger.info("=" * 80)
    logger.info("  V2策略回测结果")
    logger.info("=" * 80)

    logger.info("")
    logger.info("  ── 1. 整体对比 ──")
    logger.info("    总可评估周数: %d", all_weeks)
    logger.info("    旧规则: 准确率 %s (%d/%d) 覆盖%s",
                _p(old_correct, old_total), old_correct, old_total, _p(old_total, all_weeks))
    logger.info("    新规则: 准确率 %s (%d/%d) 覆盖%s",
                _p(new_correct, new_total), new_correct, new_total, _p(new_total, all_weeks))

    logger.info("")
    logger.info("  ── 2. 旧规则按规则分层 ──")
    for rn in sorted(old_by_rule.keys()):
        s = old_by_rule[rn]
        logger.info("    %-30s %s (%d/%d)", rn, _p(s['correct'], s['total']), s['correct'], s['total'])

    logger.info("")
    logger.info("  ── 3. 新规则按规则分层 ──")
    for rn in sorted(new_by_rule.keys()):
        s = new_by_rule[rn]
        logger.info("    %-40s %s (%d/%d)", rn, _p(s['correct'], s['total']), s['correct'], s['total'])

    logger.info("")
    logger.info("  ── 4. 新规则按Tier分层 ──")
    for tier in sorted(new_by_tier.keys()):
        s = new_by_tier[tier]
        logger.info("    Tier %d: %s (%d/%d)", tier, _p(s['correct'], s['total']), s['correct'], s['total'])

    logger.info("")
    logger.info("  ── 5. 改进幅度 ──")
    if old_total > 0 and new_total > 0:
        old_acc = old_correct / old_total * 100
        new_acc = new_correct / new_total * 100
        logger.info("    准确率: %.1f%% → %.1f%% (%+.1f%%)", old_acc, new_acc, new_acc - old_acc)
        logger.info("    覆盖率: %.1f%% → %.1f%%",
                    old_total / all_weeks * 100, new_total / all_weeks * 100)
        logger.info("    有效预测数: %d → %d (%+d)", old_total, new_total, new_total - old_total)

    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 80)


if __name__ == '__main__':
    run_v2_backtest(n_weeks=29, sample_limit=0)
