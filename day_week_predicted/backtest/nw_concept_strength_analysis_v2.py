#!/usr/bin/env python3
"""
概念板块强弱势 × 下周预测 深度因子分析 V2
==========================================
基于V1分析结果，深入挖掘：
1. 板块动量作为V4规则的置信度修正因子
2. 板块大跌/全部看跌 → 涨信号的独立规则可行性
3. 板块大涨 → 跌信号的独立规则可行性
4. 与大盘、个股涨跌、价格位置的多维交叉

V1关键发现：
- 板块大跌<-3%: 下周涨率66.5% (2436样本)
- 全部看跌(0%): 下周涨率62.0% (3712样本)
- 板块大涨>3%: 下周涨率43.5% (4627样本) → 跌率56.5%
- V4预测涨+板块确认: 85.6% vs 基线82.5%
- V4预测跌+板块确认: 83.6% vs 基线71.3%

用法：
    python -m day_week_predicted.backtest.nw_concept_strength_analysis_v2
"""
import sys, logging, random
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
SAMPLE_STOCKS = 1200  # 增大采样以提高统计可靠性


def run_analysis():
    t0 = datetime.now()
    logger.info("=" * 80)
    logger.info("  概念板块 × 下周预测 深度分析 V2")
    logger.info("=" * 80)

    latest_date = _get_latest_trade_date()
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(N_WEEKS + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')
    dt_cutoff = dt_end - timedelta(days=N_WEEKS * 7 + 14)

    all_codes = _get_all_stock_codes()
    random.seed(42)
    if len(all_codes) > SAMPLE_STOCKS:
        all_codes = sorted(random.sample(all_codes, SAMPLE_STOCKS))
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

    # 加载大盘K线
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

    # 加载板块映射
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

    # 加载板块K线
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
    logger.info("数据加载完成")

    board_by_week = {}
    for bc, kl in board_klines.items():
        bw = defaultdict(list)
        for k in kl:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            bw[dt.isocalendar()[:2]].append(k)
        board_by_week[bc] = bw

    # 收集样本
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

            this_pcts = [d['change_percent'] for d in this_days]
            this_chg = _compound_return(this_pcts)
            next_chg = _compound_return([d['change_percent'] for d in next_days])
            actual_up = next_chg >= 0

            mw = idx_bw.get(iw_this, [])
            mkt_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            # 板块因子
            board_momentum = None
            concept_consensus = None
            board_excess = None
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
                    board_excess = round(this_chg - _mean(board_chgs), 4)

            # 价格位置和前周
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

            # 连涨连跌
            cd = 0
            cu = 0
            for p in reversed(this_pcts):
                if p < 0:
                    cd += 1
                    if cu > 0: break
                elif p > 0:
                    cu += 1
                    if cd > 0: break
                else: break

            # V4规则
            feat = _nw_extract_features(this_pcts, mkt_chg,
                                        market_index=stock_idx,
                                        price_pos_60=pos60,
                                        prev_week_chg=prev_chg)
            rule = _nw_match_rule(feat)

            samples.append({
                'code': code, 'suffix': suffix,
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'next_chg': next_chg, 'actual_up': actual_up,
                'board_momentum': board_momentum,
                'concept_consensus': concept_consensus,
                'board_excess': board_excess,
                'pos60': pos60, 'prev_chg': prev_chg,
                'cd': cd, 'cu': cu,
                'v4_matched': rule is not None,
                'v4_rule': rule['name'] if rule else None,
                'v4_pred_up': rule['pred_up'] if rule else None,
                'v4_tier': rule['tier'] if rule else None,
            })
        processed += 1
        if processed % 200 == 0:
            logger.info("  已处理 %d/%d ...", processed, len(all_codes))

    logger.info("样本: %d个", len(samples))
    unmatched = [s for s in samples if not s['v4_matched']]
    logger.info("V4未命中: %d (%.1f%%)", len(unmatched), len(unmatched)/len(samples)*100)

    # ═══════════════════════════════════════════════════════════
    # 深度分析: V4未命中样本中的高准确率组合
    # ═══════════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  深度分析: V4未命中样本中的高准确率组合")
    logger.info("=" * 80)

    # 系统性搜索: 板块动量 × 个股涨跌 × 大盘 × 价格位置 × 连涨连跌
    combos = []

    # 涨信号候选
    for bm_label, bm_filt in [
        ('板块大跌<-3%', lambda s: s['board_momentum'] is not None and s['board_momentum'] < -3),
        ('板块跌<-1%', lambda s: s['board_momentum'] is not None and s['board_momentum'] < -1),
        ('全部看跌', lambda s: s['concept_consensus'] is not None and s['concept_consensus'] == 0),
        ('多数看跌<30%', lambda s: s['concept_consensus'] is not None and s['concept_consensus'] <= 0.3),
    ]:
        for chg_label, chg_filt in [
            ('个股跌>5%', lambda s: s['this_chg'] < -5),
            ('个股跌>3%', lambda s: s['this_chg'] < -3),
            ('个股跌>2%', lambda s: s['this_chg'] < -2),
        ]:
            for extra_label, extra_filt in [
                ('', lambda s: True),
                ('+低位<0.3', lambda s: s['pos60'] is not None and s['pos60'] < 0.3),
                ('+连跌≥3天', lambda s: s['cd'] >= 3),
                ('+前周跌', lambda s: s['prev_chg'] is not None and s['prev_chg'] < -2),
                ('+大盘涨', lambda s: s['mkt_chg'] > 0),
                ('+大盘微跌', lambda s: -1 <= s['mkt_chg'] < 0),
                ('+跑输板块>3%', lambda s: s['board_excess'] is not None and s['board_excess'] < -3),
            ]:
                label = f"{bm_label}+{chg_label}{extra_label}"
                subset = [s for s in unmatched if bm_filt(s) and chg_filt(s) and extra_filt(s)]
                if len(subset) < 50:
                    continue
                up_count = sum(1 for s in subset if s['actual_up'])
                up_rate = up_count / len(subset) * 100
                if up_rate >= 65:
                    combos.append(('涨', label, len(subset), up_rate))

    # 跌信号候选
    for bm_label, bm_filt in [
        ('板块大涨>3%', lambda s: s['board_momentum'] is not None and s['board_momentum'] > 3),
        ('板块涨>1%', lambda s: s['board_momentum'] is not None and s['board_momentum'] > 1),
        ('全部看涨', lambda s: s['concept_consensus'] is not None and s['concept_consensus'] == 1.0),
        ('多数看涨>70%', lambda s: s['concept_consensus'] is not None and s['concept_consensus'] >= 0.7),
    ]:
        for chg_label, chg_filt in [
            ('个股涨>5%', lambda s: s['this_chg'] > 5),
            ('个股涨>3%', lambda s: s['this_chg'] > 3),
            ('个股涨>2%', lambda s: s['this_chg'] > 2),
        ]:
            for extra_label, extra_filt in [
                ('', lambda s: True),
                ('+高位>0.7', lambda s: s['pos60'] is not None and s['pos60'] > 0.7),
                ('+连涨≥3天', lambda s: s['cu'] >= 3),
                ('+前周涨', lambda s: s['prev_chg'] is not None and s['prev_chg'] > 2),
                ('+大盘跌', lambda s: s['mkt_chg'] < -1),
                ('+跑赢板块>3%', lambda s: s['board_excess'] is not None and s['board_excess'] > 3),
            ]:
                label = f"{bm_label}+{chg_label}{extra_label}"
                subset = [s for s in unmatched if bm_filt(s) and chg_filt(s) and extra_filt(s)]
                if len(subset) < 50:
                    continue
                up_count = sum(1 for s in subset if s['actual_up'])
                down_rate = (1 - up_count / len(subset)) * 100
                if down_rate >= 60:
                    combos.append(('跌', label, len(subset), down_rate))

    # 按准确率排序输出
    combos.sort(key=lambda x: -x[3])
    logger.info("  找到 %d 个候选组合 (涨≥65%%, 跌≥60%%)", len(combos))
    logger.info("")
    for direction, label, n, acc in combos[:40]:
        marker = ' ★★' if acc >= 70 else (' ★' if acc >= 65 else '')
        logger.info("  [%s] %-55s  样本=%5d  准确率=%.1f%%%s",
                     direction, label, n, acc, marker)

    # ═══════════════════════════════════════════════════════════
    # V4命中样本: 板块因子作为置信度修正
    # ═══════════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  V4命中样本: 板块因子置信度修正效果")
    logger.info("=" * 80)

    matched = [s for s in samples if s['v4_matched']]

    for pred_label, pred_up in [('预测涨', True), ('预测跌', False)]:
        pred_samples = [s for s in matched if s['v4_pred_up'] == pred_up]
        if not pred_samples:
            continue
        base_c = sum(1 for s in pred_samples if s['actual_up'] == pred_up)
        base_acc = base_c / len(pred_samples) * 100
        logger.info("")
        logger.info("  %s 基线: %.1f%% (%d/%d)", pred_label, base_acc, base_c, len(pred_samples))

        # 板块动量确认/矛盾
        for bm_label, bm_filt in [
            ('板块大跌<-3%', lambda s: s['board_momentum'] is not None and s['board_momentum'] < -3),
            ('板块跌<-1%', lambda s: s['board_momentum'] is not None and s['board_momentum'] < -1),
            ('板块微跌-1~0%', lambda s: s['board_momentum'] is not None and -1 <= s['board_momentum'] < 0),
            ('板块微涨0~1%', lambda s: s['board_momentum'] is not None and 0 <= s['board_momentum'] < 1),
            ('板块涨>1%', lambda s: s['board_momentum'] is not None and s['board_momentum'] > 1),
            ('板块大涨>3%', lambda s: s['board_momentum'] is not None and s['board_momentum'] > 3),
        ]:
            subset = [s for s in pred_samples if bm_filt(s)]
            if len(subset) < 10:
                continue
            cc = sum(1 for s in subset if s['actual_up'] == pred_up)
            acc = cc / len(subset) * 100
            delta = acc - base_acc
            logger.info("    %-20s  %5.1f%% (%d/%d)  %+.1f%%",
                         bm_label, acc, cc, len(subset), delta)

        # 共识度确认/矛盾
        logger.info("    ── 共识度 ──")
        for cc_label, cc_filt in [
            ('全部看跌(0%)', lambda s: s['concept_consensus'] is not None and s['concept_consensus'] == 0),
            ('多数看跌(<30%)', lambda s: s['concept_consensus'] is not None and 0 < s['concept_consensus'] <= 0.3),
            ('偏看跌(30~50%)', lambda s: s['concept_consensus'] is not None and 0.3 < s['concept_consensus'] <= 0.5),
            ('偏看涨(50~70%)', lambda s: s['concept_consensus'] is not None and 0.5 < s['concept_consensus'] <= 0.7),
            ('多数看涨(>70%)', lambda s: s['concept_consensus'] is not None and 0.7 < s['concept_consensus'] < 1.0),
            ('全部看涨(100%)', lambda s: s['concept_consensus'] is not None and s['concept_consensus'] == 1.0),
        ]:
            subset = [s for s in pred_samples if cc_filt(s)]
            if len(subset) < 10:
                continue
            cc = sum(1 for s in subset if s['actual_up'] == pred_up)
            acc = cc / len(subset) * 100
            delta = acc - base_acc
            logger.info("    %-20s  %5.1f%% (%d/%d)  %+.1f%%",
                         cc_label, acc, cc, len(subset), delta)

    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 80)


if __name__ == '__main__':
    run_analysis()
