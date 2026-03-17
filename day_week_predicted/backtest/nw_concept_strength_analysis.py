#!/usr/bin/env python3
"""
概念板块强弱势 × 下周预测 深度因子分析
======================================
探索概念板块强弱势信号能否提升下周预测的准确率和覆盖率。

分析维度：
1. 板块动量(近5日均涨跌) vs 下周涨跌方向
2. 板块共识度(看涨板块占比) vs 下周涨跌方向
3. 个股 vs 板块超额收益 vs 下周涨跌方向
4. 与现有V4规则的交叉分析

用法：
    python -m day_week_predicted.backtest.nw_concept_strength_analysis
"""
import sys, logging
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
    _compound_return, _get_stock_index, _mean,
    _nw_extract_features, _nw_match_rule,
)

N_WEEKS = 29
SAMPLE_STOCKS = 800  # 采样股票数，加速分析


def run_analysis():
    t0 = datetime.now()
    logger.info("=" * 80)
    logger.info("  概念板块强弱势 × 下周预测 深度因子分析")
    logger.info("=" * 80)

    latest_date = _get_latest_trade_date()
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(N_WEEKS + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')
    dt_cutoff = dt_end - timedelta(days=N_WEEKS * 7 + 14)

    all_codes = _get_all_stock_codes()
    # 采样加速
    import random
    random.seed(42)
    if len(all_codes) > SAMPLE_STOCKS:
        all_codes = sorted(random.sample(all_codes, SAMPLE_STOCKS))
    logger.info("股票数: %d (采样)", len(all_codes))

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 1. 加载个股K线
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
    logger.info("个股K线加载完成: %d只", len(stock_klines))

    # 2. 加载大盘K线
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
            'date': r['date'], 'change_percent': _to_float(r['change_percent']),
        })

    mkt_by_week = {}
    for ic, kl in mkt_kl.items():
        bw = defaultdict(list)
        for k in kl:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            bw[dt.isocalendar()[:2]].append(k)
        mkt_by_week[ic] = bw

    # 3. 加载个股→板块映射
    code_6_list = list(set(c[:6] for c in all_codes))
    stock_boards = defaultdict(list)
    for i in range(0, len(code_6_list), bs):
        batch = code_6_list[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, board_code, board_name "
            f"FROM stock_concept_board_stock WHERE stock_code IN ({ph})", batch)
        for r in cur.fetchall():
            sc6 = r['stock_code']
            suffix = '.SZ' if sc6[0] in ('0', '3') else ('.SH' if sc6[0] == '6' else '.BJ')
            full_code = sc6 + suffix
            stock_boards[full_code].append({
                'board_code': r['board_code'], 'board_name': r['board_name'],
            })
    logger.info("板块映射加载完成: %d只有板块", len(stock_boards))

    # 4. 加载板块K线
    all_board_codes = list(set(b['board_code'] for bl in stock_boards.values() for b in bl))
    board_klines = defaultdict(list)
    for i in range(0, len(all_board_codes), bs):
        batch = all_board_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT board_code,`date`,change_percent "
            f"FROM concept_board_kline WHERE board_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            board_klines[r['board_code']].append({
                'date': r['date'], 'change_percent': _to_float(r['change_percent']),
            })
    conn.close()
    logger.info("板块K线加载完成: %d个板块", len(board_klines))

    # 按周分组板块K线
    board_by_week = {}
    for bc, kl in board_klines.items():
        bw = defaultdict(list)
        for k in kl:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            bw[dt.isocalendar()[:2]].append(k)
        board_by_week[bc] = bw

    logger.info("数据加载完成, 开始分析...")

    # ═══════════════════════════════════════════════════════════
    # 因子分析
    # ═══════════════════════════════════════════════════════════

    # 收集所有样本的因子值
    samples = []
    processed = 0

    for code in all_codes:
        klines = stock_klines.get(code, [])
        if not klines or len(klines) < 60:
            continue

        stock_idx = _get_stock_index(code)
        suffix = stock_idx.split('.')[-1] if '.' in stock_idx else ''
        idx_bw = mkt_by_week.get(stock_idx, {})
        boards = stock_boards.get(code, [])

        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            wg[dt.isocalendar()[:2]].append(k)

        sorted_weeks = sorted(wg.keys())
        sorted_all = sorted(klines, key=lambda x: x['date'])

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

            this_chg = _compound_return([d['change_percent'] for d in this_days])
            next_chg = _compound_return([d['change_percent'] for d in next_days])
            actual_up = next_chg >= 0

            mw = idx_bw.get(iw_this, [])
            mkt_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            # 计算板块因子
            board_momentum = None
            concept_consensus = None
            board_excess = None  # 个股 vs 板块超额收益

            if boards:
                momentums = []
                boards_up = 0
                valid_boards = 0
                board_chgs = []
                for b in boards:
                    bw = board_by_week.get(b['board_code'], {})
                    bk_this = bw.get(iw_this, [])
                    if len(bk_this) >= 3:
                        bc = _compound_return(
                            [k['change_percent'] for k in sorted(bk_this, key=lambda x: x['date'])])
                        board_chgs.append(bc)
                        valid_boards += 1
                        if bc > 0:
                            boards_up += 1
                        momentums.append(bc)
                if momentums:
                    board_momentum = round(_mean(momentums), 4)
                if valid_boards > 0:
                    concept_consensus = round(boards_up / valid_boards, 3)
                if board_chgs:
                    avg_board = _mean(board_chgs)
                    board_excess = round(this_chg - avg_board, 4)

            # 计算价格位置和前周涨跌
            first_date = this_days[0]['date']
            hist = [k for k in sorted_all if k['date'] < first_date]
            pos60 = None
            if len(hist) >= 20:
                hc = [k['close'] for k in hist[-60:] if k['close'] > 0]
                if hc:
                    ac = hc + [k['close'] for k in this_days if k['close'] > 0]
                    mn, mx = min(ac), max(ac)
                    lc = this_days[-1]['close']
                    if mx > mn and lc > 0:
                        pos60 = (lc - mn) / (mx - mn)

            prev_chg = None
            pk = hist[-5:] if len(hist) >= 5 else hist
            if pk:
                prev_chg = _compound_return([k['change_percent'] for k in pk])

            # V4规则匹配
            this_pcts = [d['change_percent'] for d in this_days]
            feat = _nw_extract_features(this_pcts, mkt_chg,
                                        market_index=stock_idx,
                                        price_pos_60=pos60,
                                        prev_week_chg=prev_chg)
            rule = _nw_match_rule(feat)
            v4_matched = rule is not None

            samples.append({
                'code': code, 'suffix': suffix,
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'next_chg': next_chg, 'actual_up': actual_up,
                'board_momentum': board_momentum,
                'concept_consensus': concept_consensus,
                'board_excess': board_excess,
                'pos60': pos60, 'prev_chg': prev_chg,
                'v4_matched': v4_matched,
                'v4_rule': rule['name'] if rule else None,
                'v4_pred_up': rule['pred_up'] if rule else None,
            })

        processed += 1
        if processed % 1000 == 0:
            logger.info("  已处理 %d/%d ...", processed, len(all_codes))

    logger.info("样本收集完成: %d个", len(samples))

    # ═══════════════════════════════════════════════════════════
    # 分析1: 板块动量 vs 下周涨跌
    # ═══════════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  分析1: 板块动量(本周板块均涨跌) vs 下周涨跌")
    logger.info("=" * 80)

    momentum_bins = [
        ('板块大跌<-3%', lambda s: s['board_momentum'] is not None and s['board_momentum'] < -3),
        ('板块跌-3~-1%', lambda s: s['board_momentum'] is not None and -3 <= s['board_momentum'] < -1),
        ('板块微跌-1~0%', lambda s: s['board_momentum'] is not None and -1 <= s['board_momentum'] < 0),
        ('板块微涨0~1%', lambda s: s['board_momentum'] is not None and 0 <= s['board_momentum'] < 1),
        ('板块涨1~3%', lambda s: s['board_momentum'] is not None and 1 <= s['board_momentum'] < 3),
        ('板块大涨>3%', lambda s: s['board_momentum'] is not None and s['board_momentum'] >= 3),
    ]

    for label, filt in momentum_bins:
        subset = [s for s in samples if filt(s)]
        if not subset:
            continue
        up_count = sum(1 for s in subset if s['actual_up'])
        up_rate = up_count / len(subset) * 100
        logger.info("  %-20s  样本=%5d  下周涨率=%.1f%%  (涨%d/跌%d)",
                     label, len(subset), up_rate, up_count, len(subset) - up_count)

    # ═══════════════════════════════════════════════════════════
    # 分析2: 板块共识度 vs 下周涨跌
    # ═══════════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  分析2: 板块共识度(看涨板块占比) vs 下周涨跌")
    logger.info("=" * 80)

    consensus_bins = [
        ('全部看跌(0%)', lambda s: s['concept_consensus'] is not None and s['concept_consensus'] == 0),
        ('多数看跌(0~30%)', lambda s: s['concept_consensus'] is not None and 0 < s['concept_consensus'] <= 0.3),
        ('偏看跌(30~50%)', lambda s: s['concept_consensus'] is not None and 0.3 < s['concept_consensus'] <= 0.5),
        ('偏看涨(50~70%)', lambda s: s['concept_consensus'] is not None and 0.5 < s['concept_consensus'] <= 0.7),
        ('多数看涨(70~100%)', lambda s: s['concept_consensus'] is not None and 0.7 < s['concept_consensus'] < 1.0),
        ('全部看涨(100%)', lambda s: s['concept_consensus'] is not None and s['concept_consensus'] == 1.0),
    ]

    for label, filt in consensus_bins:
        subset = [s for s in samples if filt(s)]
        if not subset:
            continue
        up_count = sum(1 for s in subset if s['actual_up'])
        up_rate = up_count / len(subset) * 100
        logger.info("  %-25s  样本=%5d  下周涨率=%.1f%%", label, len(subset), up_rate)

    # ═══════════════════════════════════════════════════════════
    # 分析3: 个股vs板块超额收益 vs 下周涨跌
    # ═══════════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  分析3: 个股vs板块超额收益 vs 下周涨跌")
    logger.info("=" * 80)

    excess_bins = [
        ('大幅跑输<-5%', lambda s: s['board_excess'] is not None and s['board_excess'] < -5),
        ('跑输-5~-2%', lambda s: s['board_excess'] is not None and -5 <= s['board_excess'] < -2),
        ('微跑输-2~0%', lambda s: s['board_excess'] is not None and -2 <= s['board_excess'] < 0),
        ('微跑赢0~2%', lambda s: s['board_excess'] is not None and 0 <= s['board_excess'] < 2),
        ('跑赢2~5%', lambda s: s['board_excess'] is not None and 2 <= s['board_excess'] < 5),
        ('大幅跑赢>5%', lambda s: s['board_excess'] is not None and s['board_excess'] >= 5),
    ]

    for label, filt in excess_bins:
        subset = [s for s in samples if filt(s)]
        if not subset:
            continue
        up_count = sum(1 for s in subset if s['actual_up'])
        up_rate = up_count / len(subset) * 100
        logger.info("  %-20s  样本=%5d  下周涨率=%.1f%%", label, len(subset), up_rate)

    # ═══════════════════════════════════════════════════════════
    # 分析4: V4未命中样本中，板块因子的预测能力
    # ═══════════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  分析4: V4未命中样本 × 板块因子交叉分析")
    logger.info("=" * 80)

    unmatched = [s for s in samples if not s['v4_matched']]
    logger.info("  V4未命中样本: %d (占%.1f%%)", len(unmatched),
                len(unmatched) / len(samples) * 100 if samples else 0)

    # 在未命中样本中，按板块动量+个股涨跌交叉分析
    cross_bins = [
        # 涨信号候选
        ('板块跌+个股跌>2%', lambda s: (s['board_momentum'] is not None and s['board_momentum'] < -1
                                        and s['this_chg'] < -2)),
        ('板块跌+个股跌>3%', lambda s: (s['board_momentum'] is not None and s['board_momentum'] < -1
                                        and s['this_chg'] < -3)),
        ('板块大跌+个股跌', lambda s: (s['board_momentum'] is not None and s['board_momentum'] < -3
                                       and s['this_chg'] < 0)),
        ('全部看跌+个股跌>2%', lambda s: (s['concept_consensus'] is not None and s['concept_consensus'] == 0
                                          and s['this_chg'] < -2)),
        ('全部看跌+个股跌>3%', lambda s: (s['concept_consensus'] is not None and s['concept_consensus'] == 0
                                          and s['this_chg'] < -3)),
        ('大幅跑输板块+跌>2%', lambda s: (s['board_excess'] is not None and s['board_excess'] < -3
                                          and s['this_chg'] < -2)),
        # 跌信号候选
        ('板块涨+个股涨>3%', lambda s: (s['board_momentum'] is not None and s['board_momentum'] > 1
                                        and s['this_chg'] > 3)),
        ('板块大涨+个股涨>5%', lambda s: (s['board_momentum'] is not None and s['board_momentum'] > 3
                                          and s['this_chg'] > 5)),
        ('全部看涨+个股涨>3%', lambda s: (s['concept_consensus'] is not None and s['concept_consensus'] == 1.0
                                          and s['this_chg'] > 3)),
        ('大幅跑赢板块+涨>3%', lambda s: (s['board_excess'] is not None and s['board_excess'] > 3
                                          and s['this_chg'] > 3)),
    ]

    for label, filt in cross_bins:
        subset = [s for s in unmatched if filt(s)]
        if not subset:
            logger.info("  %-30s  样本=0", label)
            continue
        up_count = sum(1 for s in subset if s['actual_up'])
        up_rate = up_count / len(subset) * 100
        # 判断是涨信号还是跌信号
        is_up_signal = '跌' in label.split('+')[-1] if '+' in label else '跌' in label
        if is_up_signal:
            acc = up_rate  # 涨信号：涨率就是准确率
        else:
            acc = 100 - up_rate  # 跌信号：跌率就是准确率
        logger.info("  %-30s  样本=%5d  下周涨率=%.1f%%  预测准确率=%.1f%%",
                     label, len(subset), up_rate, acc)

    # ═══════════════════════════════════════════════════════════
    # 分析5: 板块因子 × 大盘 × 市场 多维交叉
    # ═══════════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  分析5: V4未命中 × 板块因子 × 大盘 × 市场 多维交叉")
    logger.info("=" * 80)

    multi_cross = [
        # 涨信号 - 板块弱+个股弱 → 均值回归
        ('SH+大盘微跌+板块跌+个股跌>2%',
         lambda s: s['suffix'] == 'SH' and -1 <= s['mkt_chg'] < 0
                   and s['board_momentum'] is not None and s['board_momentum'] < -1
                   and s['this_chg'] < -2),
        ('SZ+大盘微跌+板块跌+个股跌>2%',
         lambda s: s['suffix'] == 'SZ' and -1 <= s['mkt_chg'] < 0
                   and s['board_momentum'] is not None and s['board_momentum'] < -1
                   and s['this_chg'] < -2),
        ('SH+大盘涨+板块跌+个股跌>3%',
         lambda s: s['suffix'] == 'SH' and s['mkt_chg'] > 0
                   and s['board_momentum'] is not None and s['board_momentum'] < -1
                   and s['this_chg'] < -3),
        ('SZ+大盘涨+板块跌+个股跌>3%',
         lambda s: s['suffix'] == 'SZ' and s['mkt_chg'] > 0
                   and s['board_momentum'] is not None and s['board_momentum'] < -1
                   and s['this_chg'] < -3),
        ('大盘涨+全部看跌+个股跌>2%',
         lambda s: s['mkt_chg'] > 0
                   and s['concept_consensus'] is not None and s['concept_consensus'] == 0
                   and s['this_chg'] < -2),
        ('大盘涨+板块大跌+个股跌>2%',
         lambda s: s['mkt_chg'] > 0
                   and s['board_momentum'] is not None and s['board_momentum'] < -3
                   and s['this_chg'] < -2),
        # 涨信号 - 板块强+个股弱 → 补涨
        ('板块涨>1%+个股跌>3%+跑输>3%',
         lambda s: s['board_momentum'] is not None and s['board_momentum'] > 1
                   and s['this_chg'] < -3
                   and s['board_excess'] is not None and s['board_excess'] < -3),
        ('板块涨>2%+个股跌>2%+低位',
         lambda s: s['board_momentum'] is not None and s['board_momentum'] > 2
                   and s['this_chg'] < -2
                   and s['pos60'] is not None and s['pos60'] < 0.3),
        # 跌信号 - 板块弱+个股强 → 回调
        ('板块跌+个股涨>3%+跑赢>3%',
         lambda s: s['board_momentum'] is not None and s['board_momentum'] < -1
                   and s['this_chg'] > 3
                   and s['board_excess'] is not None and s['board_excess'] > 3),
        ('板块跌+个股涨>5%+高位',
         lambda s: s['board_momentum'] is not None and s['board_momentum'] < -1
                   and s['this_chg'] > 5
                   and s['pos60'] is not None and s['pos60'] > 0.7),
        # 跌信号 - 板块强+个股强 → 过热回调
        ('板块大涨>3%+个股涨>5%+高位',
         lambda s: s['board_momentum'] is not None and s['board_momentum'] > 3
                   and s['this_chg'] > 5
                   and s['pos60'] is not None and s['pos60'] > 0.7),
        ('全部看涨+个股涨>5%+高位',
         lambda s: s['concept_consensus'] is not None and s['concept_consensus'] == 1.0
                   and s['this_chg'] > 5
                   and s['pos60'] is not None and s['pos60'] > 0.7),
    ]

    for label, filt in multi_cross:
        subset = [s for s in unmatched if filt(s)]
        if len(subset) < 20:
            logger.info("  %-45s  样本=%d (不足)", label, len(subset))
            continue
        up_count = sum(1 for s in subset if s['actual_up'])
        up_rate = up_count / len(subset) * 100
        # 判断信号方向
        is_up_signal = '个股跌' in label
        if is_up_signal:
            acc = up_rate
            direction = '涨'
        else:
            acc = 100 - up_rate
            direction = '跌'
        marker = ' ★' if acc >= 68 else (' ☆' if acc >= 65 else '')
        logger.info("  %-45s  样本=%5d  预测%s准确率=%.1f%%%s",
                     label, len(subset), direction, acc, marker)

    # ═══════════════════════════════════════════════════════════
    # 分析6: V4命中样本中，板块因子的增强/削弱效果
    # ═══════════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  分析6: V4命中样本 × 板块因子 增强/削弱效果")
    logger.info("=" * 80)

    matched = [s for s in samples if s['v4_matched']]
    logger.info("  V4命中样本: %d", len(matched))

    # 按V4预测方向分组
    for pred_label, pred_up in [('预测涨', True), ('预测跌', False)]:
        pred_samples = [s for s in matched if s['v4_pred_up'] == pred_up]
        if not pred_samples:
            continue

        base_correct = sum(1 for s in pred_samples if s['actual_up'] == pred_up)
        base_acc = base_correct / len(pred_samples) * 100
        logger.info("  %s 基线: %.1f%% (%d/%d)", pred_label, base_acc,
                     base_correct, len(pred_samples))

        # 板块确认 vs 板块矛盾
        if pred_up:
            confirm = [s for s in pred_samples if s['board_momentum'] is not None and s['board_momentum'] < -1]
            conflict = [s for s in pred_samples if s['board_momentum'] is not None and s['board_momentum'] > 1]
        else:
            confirm = [s for s in pred_samples if s['board_momentum'] is not None and s['board_momentum'] > 1]
            conflict = [s for s in pred_samples if s['board_momentum'] is not None and s['board_momentum'] < -1]

        if confirm:
            cc = sum(1 for s in confirm if s['actual_up'] == pred_up)
            logger.info("    板块确认: %.1f%% (%d/%d)", cc / len(confirm) * 100, cc, len(confirm))
        if conflict:
            cc = sum(1 for s in conflict if s['actual_up'] == pred_up)
            logger.info("    板块矛盾: %.1f%% (%d/%d)", cc / len(conflict) * 100, cc, len(conflict))

    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 80)


if __name__ == '__main__':
    run_analysis()
