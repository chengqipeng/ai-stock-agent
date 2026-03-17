#!/usr/bin/env python3
"""
成交量深度调优分析 — 目标: 下周预测准确率 → 70%
================================================================
当前基线: 65.5% (Tier1: 68.7%, Tier2: 54.8%)
目标: 整体 ≥ 70%

分析维度:
  1. 成交量特征全面扫描 — 发现新的高准确率条件组合
  2. 阈值网格搜索 — 优化现有规则的参数
  3. 多因子组合 — 量价+涨跌幅+大盘+连涨连跌交叉分析
  4. 条件过滤 — 找到能剔除低准确率样本的过滤条件
  5. 新规则发现 — 从数据中挖掘新的高准确率规则

用法:
    python -m day_week_predicted.backtest.volume_deep_optimize
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
    _nw_extract_features,
    _nw_match_rule,
)

# ── 指数阈值 ──
_INDEX_MKT_THRESHOLD = {
    '000001.SH': 1.0,
    '399001.SZ': 1.5,
    '899050.SZ': 2.0,
}


def _compute_vol_features(this_days, all_klines):
    """计算丰富的成交量特征集。"""
    feat = {}
    if not this_days or not all_klines:
        return feat

    sorted_all = sorted(all_klines, key=lambda x: x['date'])
    first_date = this_days[0]['date']
    hist = [k for k in sorted_all if k['date'] < first_date]

    # 保持 vols 和 chgs 对齐（只取有量的日子）
    paired = [(k['change_percent'], k.get('volume', 0)) for k in this_days if k.get('volume', 0) > 0]
    if not paired:
        return feat
    week_chgs_paired = [p[0] for p in paired]
    week_vols = [p[1] for p in paired]
    week_chgs = [k['change_percent'] for k in this_days]
    week_chg = _compound_return(week_chgs)

    if not week_vols or len(hist) < 20:
        return feat

    week_avg_vol = _mean(week_vols)

    # 历史均量
    h20_vols = [k.get('volume', 0) for k in hist[-20:] if k.get('volume', 0) > 0]
    h60_vols = [k.get('volume', 0) for k in hist[-60:] if k.get('volume', 0) > 0]
    avg20 = _mean(h20_vols) if h20_vols else 0
    avg60 = _mean(h60_vols) if h60_vols else 0

    feat['vol_ratio_20'] = week_avg_vol / avg20 if avg20 > 0 else None
    feat['vol_ratio_60'] = week_avg_vol / avg60 if avg60 > 0 else None

    # 量价相关性（使用对齐的数据）
    if len(week_chgs_paired) >= 3 and _std(week_vols) > 0 and _std(week_chgs_paired) > 0:
        n = len(week_chgs_paired)
        mc = _mean(week_chgs_paired)
        mv = _mean(week_vols)
        cov = sum((week_chgs_paired[i] - mc) * (week_vols[i] - mv) for i in range(n)) / n
        feat['vol_price_corr'] = cov / (_std(week_chgs_paired) * _std(week_vols))
    else:
        feat['vol_price_corr'] = None

    # 周内量能趋势（前半 vs 后半）
    if len(week_vols) >= 4:
        mid = len(week_vols) // 2
        first_half_vol = _mean(week_vols[:mid])
        second_half_vol = _mean(week_vols[mid:])
        feat['vol_intra_ratio'] = second_half_vol / first_half_vol if first_half_vol > 0 else None
    else:
        feat['vol_intra_ratio'] = None

    # 最大单日量 / 均量
    max_vol = max(week_vols)
    feat['max_vol_ratio'] = max_vol / avg20 if avg20 > 0 else None

    # 最后一天量 vs 均量
    last_vol = this_days[-1].get('volume', 0)
    feat['last_day_vol_ratio'] = last_vol / avg20 if avg20 > 0 and last_vol > 0 else None

    # 价格位置（60日）
    hist_closes = [k.get('close', 0) for k in hist[-60:] if k.get('close', 0) > 0]
    if hist_closes:
        all_c = hist_closes + [k.get('close', 0) for k in this_days if k.get('close', 0) > 0]
        min_c, max_c = min(all_c), max(all_c)
        latest_c = this_days[-1].get('close', 0)
        if max_c > min_c and latest_c > 0:
            feat['price_pos_60'] = (latest_c - min_c) / (max_c - min_c)
        else:
            feat['price_pos_60'] = None
    else:
        feat['price_pos_60'] = None

    # 振幅（本周）
    week_highs = [k.get('high', 0) for k in this_days if k.get('high', 0) > 0]
    week_lows = [k.get('low', 0) for k in this_days if k.get('low', 0) > 0]
    if week_highs and week_lows:
        wk_high = max(week_highs)
        wk_low = min(week_lows)
        feat['week_amplitude'] = (wk_high - wk_low) / wk_low * 100 if wk_low > 0 else None
    else:
        feat['week_amplitude'] = None

    # 上影线/下影线比例（最后一天）
    last_k = this_days[-1]
    o, c, h, l = last_k.get('open', 0), last_k.get('close', 0), last_k.get('high', 0), last_k.get('low', 0)
    body = abs(c - o)
    total = h - l
    if total > 0 and body > 0:
        feat['upper_shadow_ratio'] = (h - max(o, c)) / total
        feat['lower_shadow_ratio'] = (min(o, c) - l) / total
    else:
        feat['upper_shadow_ratio'] = None
        feat['lower_shadow_ratio'] = None

    # 连续缩量/放量天数
    consec_expand = 0
    consec_shrink = 0
    for k in reversed(this_days):
        v = k.get('volume', 0)
        if v > avg20 * 1.2:
            consec_expand += 1
        elif v < avg20 * 0.8:
            consec_shrink += 1
        else:
            break
    feat['consec_expand_days'] = consec_expand
    feat['consec_shrink_days'] = consec_shrink

    # 天量信号（单日>3倍60日均量）
    feat['has_sky_vol'] = any(k.get('volume', 0) > avg60 * 3.0 for k in this_days) if avg60 > 0 else False

    # 地量信号（单日<0.3倍20日均量）
    feat['has_ground_vol'] = any(0 < k.get('volume', 0) < avg20 * 0.3 for k in this_days) if avg20 > 0 else False

    # 量能标准差（波动性）
    feat['vol_std_ratio'] = _std(week_vols) / avg20 if avg20 > 0 and len(week_vols) >= 2 else None

    # 前一周涨跌幅
    prev_week_klines = hist[-5:] if len(hist) >= 5 else hist
    if prev_week_klines:
        feat['prev_week_chg'] = _compound_return([k['change_percent'] for k in prev_week_klines])
    else:
        feat['prev_week_chg'] = None

    # 前一周量比
    prev_vols = [k.get('volume', 0) for k in prev_week_klines if k.get('volume', 0) > 0]
    if prev_vols and avg20 > 0:
        feat['prev_vol_ratio'] = _mean(prev_vols) / avg20
    else:
        feat['prev_vol_ratio'] = None

    feat['week_chg'] = week_chg

    return feat



def run_deep_analysis(n_weeks=29, sample_limit=0):
    """深度分析：扫描所有特征组合，找到提升准确率的路径。"""
    t_start = datetime.now()
    logger.info("=" * 80)
    logger.info("  成交量深度调优分析 — 目标: 70%%准确率")
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

    # ══════════════════════════════════════════════════════════
    # 收集所有样本的特征 + 实际结果
    # ══════════════════════════════════════════════════════════
    samples = []  # list of dict: {feat, vol_feat, rule, actual_up, next_chg, ...}

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
            this_week_chg = _compound_return(this_pcts)

            mw = idx_by_week.get(iw_this, [])
            market_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            # 基线规则匹配
            feat = _nw_extract_features(this_pcts, market_chg, market_index=stock_idx)
            rule = _nw_match_rule(feat)

            # 成交量特征
            vol_feat = _compute_vol_features(this_days, klines)

            samples.append({
                'code': code,
                'stock_idx': stock_idx,
                'feat': feat,
                'vol_feat': vol_feat,
                'rule': rule,
                'actual_up': actual_up,
                'next_chg': next_week_chg,
                'this_chg': this_week_chg,
                'market_chg': market_chg,
            })

        processed += 1
        if processed % 500 == 0:
            logger.info("  已处理 %d/%d ...", processed, len(all_codes))

    logger.info("总样本数: %d", len(samples))

    # ══════════════════════════════════════════════════════════
    # 分析1: Tier1规则的成交量条件细分
    # ══════════════════════════════════════════════════════════
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"

    logger.info("")
    logger.info("=" * 80)
    logger.info("  分析1: Tier1规则(跌>2%%+大盘深跌→涨)的成交量条件细分")
    logger.info("=" * 80)

    tier1_samples = [s for s in samples if s['rule'] and s['rule']['name'] == '跌>2%+大盘深跌→涨']
    logger.info("  Tier1样本数: %d", len(tier1_samples))

    # 按vol_ratio_20分桶
    vol_ratio_buckets = {
        '缩量(<0.6)': lambda vf: vf.get('vol_ratio_20') is not None and vf['vol_ratio_20'] < 0.6,
        '微缩(0.6-0.8)': lambda vf: vf.get('vol_ratio_20') is not None and 0.6 <= vf['vol_ratio_20'] < 0.8,
        '正常(0.8-1.2)': lambda vf: vf.get('vol_ratio_20') is not None and 0.8 <= vf['vol_ratio_20'] < 1.2,
        '放量(1.2-1.8)': lambda vf: vf.get('vol_ratio_20') is not None and 1.2 <= vf['vol_ratio_20'] < 1.8,
        '大放量(1.8-3.0)': lambda vf: vf.get('vol_ratio_20') is not None and 1.8 <= vf['vol_ratio_20'] < 3.0,
        '巨量(>3.0)': lambda vf: vf.get('vol_ratio_20') is not None and vf['vol_ratio_20'] >= 3.0,
    }
    logger.info("  按量比(vol_ratio_20)分桶:")
    for label, fn in vol_ratio_buckets.items():
        matched = [s for s in tier1_samples if fn(s['vol_feat'])]
        correct = sum(1 for s in matched if s['actual_up'])
        logger.info("    %-20s %s (%d/%d)", label, _p(correct, len(matched)), correct, len(matched))

    # 按price_pos_60分桶
    pp_buckets = {
        '低位(<0.2)': lambda vf: vf.get('price_pos_60') is not None and vf['price_pos_60'] < 0.2,
        '偏低(0.2-0.4)': lambda vf: vf.get('price_pos_60') is not None and 0.2 <= vf['price_pos_60'] < 0.4,
        '中位(0.4-0.6)': lambda vf: vf.get('price_pos_60') is not None and 0.4 <= vf['price_pos_60'] < 0.6,
        '偏高(0.6-0.8)': lambda vf: vf.get('price_pos_60') is not None and 0.6 <= vf['price_pos_60'] < 0.8,
        '高位(>0.8)': lambda vf: vf.get('price_pos_60') is not None and vf['price_pos_60'] >= 0.8,
    }
    logger.info("  按价格位置(price_pos_60)分桶:")
    for label, fn in pp_buckets.items():
        matched = [s for s in tier1_samples if fn(s['vol_feat'])]
        correct = sum(1 for s in matched if s['actual_up'])
        logger.info("    %-20s %s (%d/%d)", label, _p(correct, len(matched)), correct, len(matched))

    # 按vol_price_corr分桶
    vpc_buckets = {
        '强负相关(<-0.3)': lambda vf: vf.get('vol_price_corr') is not None and vf['vol_price_corr'] < -0.3,
        '弱负相关(-0.3~0)': lambda vf: vf.get('vol_price_corr') is not None and -0.3 <= vf['vol_price_corr'] < 0,
        '弱正相关(0~0.3)': lambda vf: vf.get('vol_price_corr') is not None and 0 <= vf['vol_price_corr'] < 0.3,
        '强正相关(>0.3)': lambda vf: vf.get('vol_price_corr') is not None and vf['vol_price_corr'] >= 0.3,
    }
    logger.info("  按量价相关性(vol_price_corr)分桶:")
    for label, fn in vpc_buckets.items():
        matched = [s for s in tier1_samples if fn(s['vol_feat'])]
        correct = sum(1 for s in matched if s['actual_up'])
        logger.info("    %-20s %s (%d/%d)", label, _p(correct, len(matched)), correct, len(matched))

    # 按周振幅分桶
    amp_buckets = {
        '低振幅(<5%)': lambda vf: vf.get('week_amplitude') is not None and vf['week_amplitude'] < 5,
        '中振幅(5-10%)': lambda vf: vf.get('week_amplitude') is not None and 5 <= vf['week_amplitude'] < 10,
        '高振幅(10-15%)': lambda vf: vf.get('week_amplitude') is not None and 10 <= vf['week_amplitude'] < 15,
        '极高振幅(>15%)': lambda vf: vf.get('week_amplitude') is not None and vf['week_amplitude'] >= 15,
    }
    logger.info("  按周振幅分桶:")
    for label, fn in amp_buckets.items():
        matched = [s for s in tier1_samples if fn(s['vol_feat'])]
        correct = sum(1 for s in matched if s['actual_up'])
        logger.info("    %-20s %s (%d/%d)", label, _p(correct, len(matched)), correct, len(matched))

    # 按个股跌幅深度分桶
    chg_buckets = {
        '跌2-3%': lambda s: -3 <= s['this_chg'] < -2,
        '跌3-5%': lambda s: -5 <= s['this_chg'] < -3,
        '跌5-8%': lambda s: -8 <= s['this_chg'] < -5,
        '跌8-12%': lambda s: -12 <= s['this_chg'] < -8,
        '跌>12%': lambda s: s['this_chg'] < -12,
    }
    logger.info("  按个股跌幅深度分桶:")
    for label, fn in chg_buckets.items():
        matched = [s for s in tier1_samples if fn(s)]
        correct = sum(1 for s in matched if s['actual_up'])
        logger.info("    %-20s %s (%d/%d)", label, _p(correct, len(matched)), correct, len(matched))

    # 按大盘跌幅深度分桶
    mkt_buckets = {
        '大盘跌1-2%': lambda s: -2 <= s['market_chg'] < -1,
        '大盘跌2-3%': lambda s: -3 <= s['market_chg'] < -2,
        '大盘跌3-5%': lambda s: -5 <= s['market_chg'] < -3,
        '大盘跌>5%': lambda s: s['market_chg'] < -5,
    }
    logger.info("  按大盘跌幅深度分桶:")
    for label, fn in mkt_buckets.items():
        matched = [s for s in tier1_samples if fn(s)]
        correct = sum(1 for s in matched if s['actual_up'])
        logger.info("    %-20s %s (%d/%d)", label, _p(correct, len(matched)), correct, len(matched))

    # 按连跌天数分桶
    cd_buckets = {
        '连跌0天': lambda s: s['feat']['consec_down'] == 0,
        '连跌1天': lambda s: s['feat']['consec_down'] == 1,
        '连跌2天': lambda s: s['feat']['consec_down'] == 2,
        '连跌3天': lambda s: s['feat']['consec_down'] == 3,
        '连跌4天': lambda s: s['feat']['consec_down'] == 4,
        '连跌≥5天': lambda s: s['feat']['consec_down'] >= 5,
    }
    logger.info("  按连跌天数分桶:")
    for label, fn in cd_buckets.items():
        matched = [s for s in tier1_samples if fn(s)]
        correct = sum(1 for s in matched if s['actual_up'])
        logger.info("    %-20s %s (%d/%d)", label, _p(correct, len(matched)), correct, len(matched))

    # ══════════════════════════════════════════════════════════
    # 分析2: Tier1多因子交叉 — 找高准确率组合
    # ══════════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  分析2: Tier1多因子交叉 — 找≥70%%准确率的组合")
    logger.info("=" * 80)

    # 网格搜索: 跌幅阈值 × 大盘阈值 × 量比条件 × 价格位置
    combos_70 = []  # (label, accuracy, correct, total)

    chg_thresholds = [-2, -3, -4, -5, -6, -8]
    mkt_thresholds = [-1.0, -1.5, -2.0, -2.5, -3.0]
    vol_conditions = [
        ('任意量比', lambda vf: True),
        ('缩量<0.8', lambda vf: vf.get('vol_ratio_20') is not None and vf['vol_ratio_20'] < 0.8),
        ('正常0.8-1.5', lambda vf: vf.get('vol_ratio_20') is not None and 0.8 <= vf['vol_ratio_20'] < 1.5),
        ('放量>1.2', lambda vf: vf.get('vol_ratio_20') is not None and vf['vol_ratio_20'] > 1.2),
        ('放量>1.5', lambda vf: vf.get('vol_ratio_20') is not None and vf['vol_ratio_20'] > 1.5),
        ('非巨量<3.0', lambda vf: vf.get('vol_ratio_20') is None or vf['vol_ratio_20'] < 3.0),
    ]
    pp_conditions = [
        ('任意位置', lambda vf: True),
        ('低位<0.3', lambda vf: vf.get('price_pos_60') is not None and vf['price_pos_60'] < 0.3),
        ('非高位<0.7', lambda vf: vf.get('price_pos_60') is None or vf['price_pos_60'] < 0.7),
        ('非高位<0.8', lambda vf: vf.get('price_pos_60') is None or vf['price_pos_60'] < 0.8),
    ]

    # 所有可能命中Tier1的样本（不限于当前规则）
    all_down_samples = [s for s in samples if s['this_chg'] < -2 and s['market_chg'] < -1]

    for chg_t in chg_thresholds:
        for mkt_t in mkt_thresholds:
            for vol_label, vol_fn in vol_conditions:
                for pp_label, pp_fn in pp_conditions:
                    matched = [
                        s for s in all_down_samples
                        if s['this_chg'] < chg_t
                        and s['market_chg'] < mkt_t
                        and vol_fn(s['vol_feat'])
                        and pp_fn(s['vol_feat'])
                    ]
                    if len(matched) < 100:  # 样本太少不可靠
                        continue
                    correct = sum(1 for s in matched if s['actual_up'])
                    acc = correct / len(matched) * 100
                    if acc >= 70.0:
                        label = f"跌>{abs(chg_t)}%+大盘跌>{abs(mkt_t)}%+{vol_label}+{pp_label}"
                        combos_70.append((label, acc, correct, len(matched)))

    # 去重并排序
    combos_70.sort(key=lambda x: (-x[1], -x[3]))
    # 只保留前30个
    logger.info("  找到 %d 个≥70%%准确率的组合 (样本≥100)", len(combos_70))
    for label, acc, correct, total in combos_70[:30]:
        logger.info("    %.1f%% (%d/%d) %s", acc, correct, total, label)

    # ══════════════════════════════════════════════════════════
    # 分析3: Tier2规则优化 — 找到提升路径
    # ══════════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  分析3: Tier2规则(周跌>5%%→继续跌)优化")
    logger.info("=" * 80)

    tier2_samples = [s for s in samples if s['rule'] and s['rule']['name'] == '周跌>5%→继续跌']
    logger.info("  Tier2样本数: %d", len(tier2_samples))

    # 按量比分桶
    logger.info("  按量比分桶:")
    for label, fn in vol_ratio_buckets.items():
        matched = [s for s in tier2_samples if fn(s['vol_feat'])]
        correct = sum(1 for s in matched if not s['actual_up'])  # Tier2预测跌
        logger.info("    %-20s %s (%d/%d)", label, _p(correct, len(matched)), correct, len(matched))

    # 按价格位置分桶
    logger.info("  按价格位置分桶:")
    for label, fn in pp_buckets.items():
        matched = [s for s in tier2_samples if fn(s['vol_feat'])]
        correct = sum(1 for s in matched if not s['actual_up'])
        logger.info("    %-20s %s (%d/%d)", label, _p(correct, len(matched)), correct, len(matched))

    # 按跌幅深度分桶
    t2_chg_buckets = {
        '跌5-7%': lambda s: -7 <= s['this_chg'] < -5,
        '跌7-10%': lambda s: -10 <= s['this_chg'] < -7,
        '跌10-15%': lambda s: -15 <= s['this_chg'] < -10,
        '跌>15%': lambda s: s['this_chg'] < -15,
    }
    logger.info("  按跌幅深度分桶:")
    for label, fn in t2_chg_buckets.items():
        matched = [s for s in tier2_samples if fn(s)]
        correct = sum(1 for s in matched if not s['actual_up'])
        logger.info("    %-20s %s (%d/%d)", label, _p(correct, len(matched)), correct, len(matched))

    # Tier2也做网格搜索
    logger.info("  Tier2网格搜索(预测跌):")
    t2_combos = []
    for chg_t in [-5, -6, -7, -8, -10]:
        for vol_label, vol_fn in vol_conditions:
            for pp_label, pp_fn in pp_conditions:
                matched = [
                    s for s in samples
                    if s['this_chg'] < chg_t
                    and s['stock_idx'] == '000001.SH'
                    and vol_fn(s['vol_feat'])
                    and pp_fn(s['vol_feat'])
                ]
                if len(matched) < 50:
                    continue
                correct = sum(1 for s in matched if not s['actual_up'])
                acc = correct / len(matched) * 100
                if acc >= 58:
                    label = f"跌>{abs(chg_t)}%+SH+{vol_label}+{pp_label}"
                    t2_combos.append((label, acc, correct, len(matched)))

    t2_combos.sort(key=lambda x: (-x[1], -x[3]))
    for label, acc, correct, total in t2_combos[:15]:
        logger.info("    %.1f%% (%d/%d) %s", acc, correct, total, label)

    # ══════════════════════════════════════════════════════════
    # 分析4: 新规则发现 — 从未命中样本中挖掘
    # ══════════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  分析4: 新规则发现 — 从全量样本中挖掘高准确率模式")
    logger.info("=" * 80)

    # 4a: 大盘涨+个股跌 → ?
    logger.info("  4a: 大盘涨+个股跌 (逆势下跌):")
    for mkt_t in [0.5, 1.0, 1.5, 2.0]:
        for chg_t in [-2, -3, -5]:
            matched = [s for s in samples if s['market_chg'] > mkt_t and s['this_chg'] < chg_t]
            if len(matched) < 50:
                continue
            up_correct = sum(1 for s in matched if s['actual_up'])
            down_correct = len(matched) - up_correct
            up_acc = up_correct / len(matched) * 100
            down_acc = down_correct / len(matched) * 100
            best = '涨' if up_acc > down_acc else '跌'
            best_acc = max(up_acc, down_acc)
            logger.info("    大盘涨>%.1f%%+个股跌>%d%%: 预测%s %.1f%% (%d/%d)",
                        mkt_t, abs(chg_t), best, best_acc,
                        max(up_correct, down_correct), len(matched))

    # 4b: 连跌+放量 → ?
    logger.info("  4b: 连跌天数+量比组合:")
    for cd in [2, 3, 4, 5]:
        for vr_label, vr_lo, vr_hi in [('缩量', 0, 0.8), ('正常', 0.8, 1.5), ('放量', 1.5, 99)]:
            matched = [
                s for s in samples
                if s['feat']['consec_down'] >= cd
                and s['vol_feat'].get('vol_ratio_20') is not None
                and vr_lo <= s['vol_feat']['vol_ratio_20'] < vr_hi
            ]
            if len(matched) < 50:
                continue
            up_correct = sum(1 for s in matched if s['actual_up'])
            up_acc = up_correct / len(matched) * 100
            logger.info("    连跌≥%d天+%s: 预测涨 %.1f%% (%d/%d)",
                        cd, vr_label, up_acc, up_correct, len(matched))

    # 4c: 连涨+缩量 → ?
    logger.info("  4c: 连涨天数+量比组合:")
    for cu in [2, 3, 4, 5]:
        for vr_label, vr_lo, vr_hi in [('缩量', 0, 0.8), ('正常', 0.8, 1.5), ('放量', 1.5, 99)]:
            matched = [
                s for s in samples
                if s['feat']['consec_up'] >= cu
                and s['vol_feat'].get('vol_ratio_20') is not None
                and vr_lo <= s['vol_feat']['vol_ratio_20'] < vr_hi
            ]
            if len(matched) < 50:
                continue
            down_correct = sum(1 for s in matched if not s['actual_up'])
            down_acc = down_correct / len(matched) * 100
            logger.info("    连涨≥%d天+%s: 预测跌 %.1f%% (%d/%d)",
                        cu, vr_label, down_acc, down_correct, len(matched))

    # 4d: 天量阴线 → ?
    logger.info("  4d: 天量信号组合:")
    sky_samples = [s for s in samples if s['vol_feat'].get('has_sky_vol')]
    logger.info("    天量样本数: %d", len(sky_samples))
    for pp_label, pp_fn in pp_buckets.items():
        matched = [s for s in sky_samples if pp_fn(s['vol_feat'])]
        if len(matched) < 30:
            continue
        down_correct = sum(1 for s in matched if not s['actual_up'])
        down_acc = down_correct / len(matched) * 100
        logger.info("    天量+%s: 预测跌 %.1f%% (%d/%d)",
                    pp_label, down_acc, down_correct, len(matched))

    # 4e: 地量信号 → ?
    logger.info("  4e: 地量信号组合:")
    ground_samples = [s for s in samples if s['vol_feat'].get('has_ground_vol')]
    logger.info("    地量样本数: %d", len(ground_samples))
    for pp_label, pp_fn in pp_buckets.items():
        matched = [s for s in ground_samples if pp_fn(s['vol_feat'])]
        if len(matched) < 30:
            continue
        up_correct = sum(1 for s in matched if s['actual_up'])
        up_acc = up_correct / len(matched) * 100
        logger.info("    地量+%s: 预测涨 %.1f%% (%d/%d)",
                    pp_label, up_acc, up_correct, len(matched))

    # ══════════════════════════════════════════════════════════
    # 分析5: 周内量能形态 — 前半vs后半
    # ══════════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  分析5: 周内量能形态分析")
    logger.info("=" * 80)

    # 5a: 周内量能递增/递减对Tier1的影响
    logger.info("  5a: Tier1 + 周内量能趋势:")
    intra_buckets = {
        '量能递减(<0.7)': lambda vf: vf.get('vol_intra_ratio') is not None and vf['vol_intra_ratio'] < 0.7,
        '量能平稳(0.7-1.3)': lambda vf: vf.get('vol_intra_ratio') is not None and 0.7 <= vf['vol_intra_ratio'] < 1.3,
        '量能递增(>1.3)': lambda vf: vf.get('vol_intra_ratio') is not None and vf['vol_intra_ratio'] >= 1.3,
    }
    for label, fn in intra_buckets.items():
        matched = [s for s in tier1_samples if fn(s['vol_feat'])]
        correct = sum(1 for s in matched if s['actual_up'])
        logger.info("    %-25s %s (%d/%d)", label, _p(correct, len(matched)), correct, len(matched))

    # 5b: 最后一天量比
    logger.info("  5b: Tier1 + 最后一天量比:")
    last_vol_buckets = {
        '尾日缩量(<0.6)': lambda vf: vf.get('last_day_vol_ratio') is not None and vf['last_day_vol_ratio'] < 0.6,
        '尾日正常(0.6-1.2)': lambda vf: vf.get('last_day_vol_ratio') is not None and 0.6 <= vf['last_day_vol_ratio'] < 1.2,
        '尾日放量(1.2-2.0)': lambda vf: vf.get('last_day_vol_ratio') is not None and 1.2 <= vf['last_day_vol_ratio'] < 2.0,
        '尾日大放量(>2.0)': lambda vf: vf.get('last_day_vol_ratio') is not None and vf['last_day_vol_ratio'] >= 2.0,
    }
    for label, fn in last_vol_buckets.items():
        matched = [s for s in tier1_samples if fn(s['vol_feat'])]
        correct = sum(1 for s in matched if s['actual_up'])
        logger.info("    %-25s %s (%d/%d)", label, _p(correct, len(matched)), correct, len(matched))

    # 5c: 上影线/下影线
    logger.info("  5c: Tier1 + K线形态(最后一天):")
    shadow_buckets = {
        '长上影(>0.3)': lambda vf: vf.get('upper_shadow_ratio') is not None and vf['upper_shadow_ratio'] > 0.3,
        '长下影(>0.3)': lambda vf: vf.get('lower_shadow_ratio') is not None and vf['lower_shadow_ratio'] > 0.3,
        '十字星(上下影均>0.25)': lambda vf: (
            vf.get('upper_shadow_ratio') is not None and vf.get('lower_shadow_ratio') is not None
            and vf['upper_shadow_ratio'] > 0.25 and vf['lower_shadow_ratio'] > 0.25
        ),
    }
    for label, fn in shadow_buckets.items():
        matched = [s for s in tier1_samples if fn(s['vol_feat'])]
        correct = sum(1 for s in matched if s['actual_up'])
        logger.info("    %-30s %s (%d/%d)", label, _p(correct, len(matched)), correct, len(matched))

    # ══════════════════════════════════════════════════════════
    # 分析6: 前一周动量 + 本周信号 交叉
    # ══════════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  分析6: 前一周动量 + 本周信号 交叉")
    logger.info("=" * 80)

    logger.info("  6a: Tier1 + 前一周涨跌:")
    prev_buckets = {
        '前周大跌(<-5%)': lambda vf: vf.get('prev_week_chg') is not None and vf['prev_week_chg'] < -5,
        '前周跌(-5~-2%)': lambda vf: vf.get('prev_week_chg') is not None and -5 <= vf['prev_week_chg'] < -2,
        '前周微跌(-2~0%)': lambda vf: vf.get('prev_week_chg') is not None and -2 <= vf['prev_week_chg'] < 0,
        '前周涨(0~3%)': lambda vf: vf.get('prev_week_chg') is not None and 0 <= vf['prev_week_chg'] < 3,
        '前周大涨(>3%)': lambda vf: vf.get('prev_week_chg') is not None and vf['prev_week_chg'] >= 3,
    }
    for label, fn in prev_buckets.items():
        matched = [s for s in tier1_samples if fn(s['vol_feat'])]
        correct = sum(1 for s in matched if s['actual_up'])
        logger.info("    %-25s %s (%d/%d)", label, _p(correct, len(matched)), correct, len(matched))

    # 6b: 两周连跌深度
    logger.info("  6b: 两周连跌深度(本周跌+前周跌):")
    two_week_buckets = {
        '两周累跌<-4%': lambda s, vf: (
            vf.get('prev_week_chg') is not None and vf['prev_week_chg'] < 0
            and s['this_chg'] + vf['prev_week_chg'] < -4
        ),
        '两周累跌<-7%': lambda s, vf: (
            vf.get('prev_week_chg') is not None and vf['prev_week_chg'] < 0
            and s['this_chg'] + vf['prev_week_chg'] < -7
        ),
        '两周累跌<-10%': lambda s, vf: (
            vf.get('prev_week_chg') is not None and vf['prev_week_chg'] < 0
            and s['this_chg'] + vf['prev_week_chg'] < -10
        ),
    }
    for label, fn in two_week_buckets.items():
        matched = [s for s in tier1_samples if fn(s, s['vol_feat'])]
        correct = sum(1 for s in matched if s['actual_up'])
        logger.info("    %-25s %s (%d/%d)", label, _p(correct, len(matched)), correct, len(matched))

    # 6c: 前周涨+本周跌(获利回吐后反弹)
    logger.info("  6c: 前周涨+本周跌(获利回吐):")
    for prev_t in [1, 2, 3, 5]:
        for this_t in [-2, -3, -5]:
            matched = [
                s for s in samples
                if s['vol_feat'].get('prev_week_chg') is not None
                and s['vol_feat']['prev_week_chg'] > prev_t
                and s['this_chg'] < this_t
            ]
            if len(matched) < 50:
                continue
            up_correct = sum(1 for s in matched if s['actual_up'])
            up_acc = up_correct / len(matched) * 100
            logger.info("    前周涨>%d%%+本周跌>%d%%: 预测涨 %.1f%% (%d/%d)",
                        prev_t, abs(this_t), up_acc, up_correct, len(matched))

    # ══════════════════════════════════════════════════════════
    # 分析7: 最优过滤条件 — 从Tier1中剔除低准确率子集
    # ══════════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  分析7: Tier1最优过滤条件 — 剔除低准确率子集")
    logger.info("=" * 80)

    # 找出Tier1中准确率<60%的子集特征
    t1_wrong = [s for s in tier1_samples if not s['actual_up']]
    t1_right = [s for s in tier1_samples if s['actual_up']]
    logger.info("  Tier1正确: %d, 错误: %d", len(t1_right), len(t1_wrong))

    # 对比正确/错误样本的特征分布
    def _avg_feat(samples_list, key):
        vals = [s['vol_feat'].get(key) for s in samples_list if s['vol_feat'].get(key) is not None]
        return _mean(vals) if vals else None

    feat_keys = ['vol_ratio_20', 'vol_ratio_60', 'vol_price_corr', 'price_pos_60',
                 'week_amplitude', 'vol_intra_ratio', 'last_day_vol_ratio',
                 'upper_shadow_ratio', 'lower_shadow_ratio', 'prev_week_chg']
    logger.info("  正确vs错误样本特征均值对比:")
    for key in feat_keys:
        right_avg = _avg_feat(t1_right, key)
        wrong_avg = _avg_feat(t1_wrong, key)
        if right_avg is not None and wrong_avg is not None:
            logger.info("    %-25s 正确=%.3f  错误=%.3f  差=%.3f",
                        key, right_avg, wrong_avg, right_avg - wrong_avg)

    # 尝试各种过滤条件
    logger.info("")
    logger.info("  过滤条件效果(从Tier1中剔除):")
    filters = [
        ('剔除巨量>3.0', lambda s: not (s['vol_feat'].get('vol_ratio_20') is not None and s['vol_feat']['vol_ratio_20'] >= 3.0)),
        ('剔除巨量>2.5', lambda s: not (s['vol_feat'].get('vol_ratio_20') is not None and s['vol_feat']['vol_ratio_20'] >= 2.5)),
        ('剔除高位>0.8', lambda s: not (s['vol_feat'].get('price_pos_60') is not None and s['vol_feat']['price_pos_60'] >= 0.8)),
        ('剔除高位>0.7', lambda s: not (s['vol_feat'].get('price_pos_60') is not None and s['vol_feat']['price_pos_60'] >= 0.7)),
        ('剔除高振幅>15%', lambda s: not (s['vol_feat'].get('week_amplitude') is not None and s['vol_feat']['week_amplitude'] >= 15)),
        ('剔除长上影>0.4', lambda s: not (s['vol_feat'].get('upper_shadow_ratio') is not None and s['vol_feat']['upper_shadow_ratio'] > 0.4)),
        ('剔除前周大涨>5%', lambda s: not (s['vol_feat'].get('prev_week_chg') is not None and s['vol_feat']['prev_week_chg'] > 5)),
        ('剔除量价强正相关>0.5', lambda s: not (s['vol_feat'].get('vol_price_corr') is not None and s['vol_feat']['vol_price_corr'] > 0.5)),
        ('组合:剔除巨量+高位', lambda s: not (
            (s['vol_feat'].get('vol_ratio_20') is not None and s['vol_feat']['vol_ratio_20'] >= 3.0)
            or (s['vol_feat'].get('price_pos_60') is not None and s['vol_feat']['price_pos_60'] >= 0.8)
        )),
        ('组合:剔除巨量+高位+高振幅', lambda s: not (
            (s['vol_feat'].get('vol_ratio_20') is not None and s['vol_feat']['vol_ratio_20'] >= 3.0)
            or (s['vol_feat'].get('price_pos_60') is not None and s['vol_feat']['price_pos_60'] >= 0.8)
            or (s['vol_feat'].get('week_amplitude') is not None and s['vol_feat']['week_amplitude'] >= 15)
        )),
    ]

    base_correct = sum(1 for s in tier1_samples if s['actual_up'])
    base_total = len(tier1_samples)
    base_acc = base_correct / base_total * 100 if base_total > 0 else 0
    logger.info("    基线: %.1f%% (%d/%d)", base_acc, base_correct, base_total)

    for label, fn in filters:
        filtered = [s for s in tier1_samples if fn(s)]
        correct = sum(1 for s in filtered if s['actual_up'])
        total = len(filtered)
        acc = correct / total * 100 if total > 0 else 0
        removed = base_total - total
        logger.info("    %-35s %.1f%% (%d/%d) 剔除%d个 %+.1f%%",
                    label, acc, correct, total, removed, acc - base_acc)

    # ══════════════════════════════════════════════════════════
    # 分析8: 按指数分别分析 — 找各指数最优参数
    # ══════════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  分析8: 按指数分别分析Tier1")
    logger.info("=" * 80)

    for idx_code, idx_name in [('000001.SH', '上证'), ('399001.SZ', '深证'), ('899050.SZ', '北证50')]:
        idx_t1 = [s for s in tier1_samples if s['stock_idx'] == idx_code]
        if not idx_t1:
            continue
        correct = sum(1 for s in idx_t1 if s['actual_up'])
        logger.info("  %s Tier1: %s (%d/%d)", idx_name, _p(correct, len(idx_t1)), correct, len(idx_t1))

        # 按跌幅细分
        for chg_label, chg_fn in chg_buckets.items():
            matched = [s for s in idx_t1 if chg_fn(s)]
            c = sum(1 for s in matched if s['actual_up'])
            if matched:
                logger.info("    %-20s %s (%d/%d)", chg_label, _p(c, len(matched)), c, len(matched))

        # 按大盘跌幅细分
        for mkt_label, mkt_fn in mkt_buckets.items():
            matched = [s for s in idx_t1 if mkt_fn(s)]
            c = sum(1 for s in matched if s['actual_up'])
            if matched:
                logger.info("    %-20s %s (%d/%d)", mkt_label, _p(c, len(matched)), c, len(matched))

    # ══════════════════════════════════════════════════════════
    # 分析9: 综合最优策略模拟
    # ══════════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  分析9: 综合最优策略模拟")
    logger.info("=" * 80)

    # 策略A: 当前基线
    rule_matched = [s for s in samples if s['rule'] is not None]
    a_correct = sum(1 for s in rule_matched if (s['rule']['pred_up'] == s['actual_up']))
    logger.info("  策略A(当前基线): %.1f%% (%d/%d) 覆盖%.1f%%",
                a_correct / len(rule_matched) * 100 if rule_matched else 0,
                a_correct, len(rule_matched),
                len(rule_matched) / len(samples) * 100)

    # 策略B: Tier1收紧(跌>3%+大盘跌>1.5%) + 剔除高位
    b_matched = [
        s for s in samples
        if (s['this_chg'] < -3 and s['market_chg'] < -1.5
            and not (s['vol_feat'].get('price_pos_60') is not None and s['vol_feat']['price_pos_60'] >= 0.8))
    ]
    b_correct = sum(1 for s in b_matched if s['actual_up'])
    logger.info("  策略B(收紧Tier1): %.1f%% (%d/%d) 覆盖%.1f%%",
                b_correct / len(b_matched) * 100 if b_matched else 0,
                b_correct, len(b_matched),
                len(b_matched) / len(samples) * 100)

    # 策略C: 更激进收紧
    c_matched = [
        s for s in samples
        if (s['this_chg'] < -3 and s['market_chg'] < -2.0
            and not (s['vol_feat'].get('price_pos_60') is not None and s['vol_feat']['price_pos_60'] >= 0.7)
            and not (s['vol_feat'].get('vol_ratio_20') is not None and s['vol_feat']['vol_ratio_20'] >= 3.0))
    ]
    c_correct = sum(1 for s in c_matched if s['actual_up'])
    logger.info("  策略C(激进收紧): %.1f%% (%d/%d) 覆盖%.1f%%",
                c_correct / len(c_matched) * 100 if c_matched else 0,
                c_correct, len(c_matched),
                len(c_matched) / len(samples) * 100)

    # 策略D: 最激进 — 只保留最高准确率条件
    d_matched = [
        s for s in samples
        if (s['this_chg'] < -5 and s['market_chg'] < -2.0
            and not (s['vol_feat'].get('price_pos_60') is not None and s['vol_feat']['price_pos_60'] >= 0.7))
    ]
    d_correct = sum(1 for s in d_matched if s['actual_up'])
    logger.info("  策略D(最激进): %.1f%% (%d/%d) 覆盖%.1f%%",
                d_correct / len(d_matched) * 100 if d_matched else 0,
                d_correct, len(d_matched),
                len(d_matched) / len(samples) * 100)

    # 策略E: 多层组合 — Tier1收紧 + 新规则补充覆盖率
    # Tier1收紧部分
    e_tier1 = [
        s for s in samples
        if (s['this_chg'] < -3 and s['market_chg'] < -1.5
            and not (s['vol_feat'].get('price_pos_60') is not None and s['vol_feat']['price_pos_60'] >= 0.8))
    ]
    e_tier1_ids = set(id(s) for s in e_tier1)
    # 新规则: 连跌≥3天+缩量+低位
    e_new1 = [
        s for s in samples
        if id(s) not in e_tier1_ids
        and s['feat']['consec_down'] >= 3
        and s['vol_feat'].get('vol_ratio_20') is not None and s['vol_feat']['vol_ratio_20'] < 0.8
        and s['vol_feat'].get('price_pos_60') is not None and s['vol_feat']['price_pos_60'] < 0.3
    ]
    e_all = e_tier1 + e_new1
    e_correct = sum(1 for s in e_tier1 if s['actual_up']) + sum(1 for s in e_new1 if s['actual_up'])
    logger.info("  策略E(多层组合): %.1f%% (%d/%d) 覆盖%.1f%%",
                e_correct / len(e_all) * 100 if e_all else 0,
                e_correct, len(e_all),
                len(e_all) / len(samples) * 100)
    logger.info("    其中Tier1收紧: %d个, 新规则补充: %d个", len(e_tier1), len(e_new1))

    elapsed = (datetime.now() - t_start).total_seconds()
    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 80)


if __name__ == '__main__':
    run_deep_analysis(n_weeks=29, sample_limit=0)
