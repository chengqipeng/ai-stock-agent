#!/usr/bin/env python3
"""验证英维克和中际旭创的下周预测 + 回测准确率"""
import sys
sys.path.insert(0, '.')

import logging
logging.basicConfig(level=logging.WARNING)

from collections import defaultdict
from datetime import datetime, timedelta
from dao import get_connection
from service.weekly_prediction_service import (
    _to_float, _compound_return, _get_stock_index,
    _nw_extract_features, _nw_match_rule,
    _detect_volume_patterns, _adjust_nw_confidence_by_volume,
    _adjust_nw_confidence_by_board,
    _get_latest_trade_date,
)

STOCKS = {'002837.SZ': '英维克', '300308.SZ': '中际旭创'}
N_WEEKS = 29


def load_data():
    latest_date = _get_latest_trade_date()
    print(f"最新交易日: {latest_date}")
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(N_WEEKS + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    stock_klines = {}
    for code in STOCKS:
        cur.execute(
            "SELECT stock_code, `date`, open_price, close_price, high_price, "
            "low_price, change_percent, trading_volume "
            "FROM stock_kline WHERE stock_code = %s "
            "AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            [code, start_date, latest_date])
        stock_klines[code] = [{
            'date': r['date'], 'open': _to_float(r['open_price']),
            'close': _to_float(r['close_price']), 'high': _to_float(r['high_price']),
            'low': _to_float(r['low_price']),
            'change_percent': _to_float(r['change_percent']),
            'volume': _to_float(r['trading_volume']),
        } for r in cur.fetchall()]

    idx_codes = list(set(_get_stock_index(c) for c in STOCKS))
    for idx in ('000001.SH', '399001.SZ', '899050.SZ'):
        if idx not in idx_codes:
            idx_codes.append(idx)
    ph = ','.join(['%s'] * len(idx_codes))
    cur.execute(
        f"SELECT stock_code, `date`, change_percent FROM stock_kline "
        f"WHERE stock_code IN ({ph}) AND `date` >= %s AND `date` <= %s ORDER BY `date`",
        idx_codes + [start_date, latest_date])
    mkt_klines = defaultdict(list)
    for r in cur.fetchall():
        mkt_klines[r['stock_code']].append({
            'date': r['date'], 'change_percent': _to_float(r['change_percent'])
        })
    conn.close()

    mkt_by_week = {}
    for idx_code, kl in mkt_klines.items():
        bw = defaultdict(list)
        for k in kl:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            bw[dt.isocalendar()[:2]].append(k)
        mkt_by_week[idx_code] = bw

    return latest_date, dt_end, stock_klines, mkt_klines, mkt_by_week


def compute_features(this_days, klines, stock_idx, idx_by_week, iw_this):
    """计算V3特征，与生产代码 _predict_next_week 一致"""
    daily_pcts = [d['change_percent'] for d in this_days]

    mw = idx_by_week.get(iw_this, [])
    market_chg = _compound_return(
        [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
    ) if len(mw) >= 3 else 0.0

    sorted_all = sorted(klines, key=lambda x: x['date'])
    first_date = this_days[0]['date']
    hist = [k for k in sorted_all if k['date'] < first_date]

    price_pos_60 = None
    if len(hist) >= 20:
        hc = [k.get('close', 0) for k in hist[-60:] if k.get('close', 0) > 0]
        if hc:
            all_c = hc + [k.get('close', 0) for k in this_days if k.get('close', 0) > 0]
            mn, mx = min(all_c), max(all_c)
            lc = this_days[-1].get('close', 0)
            if mx > mn and lc > 0:
                price_pos_60 = round((lc - mn) / (mx - mn), 4)

    prev_week_chg = None
    pk = hist[-5:] if len(hist) >= 5 else hist
    if pk:
        prev_week_chg = _compound_return([k['change_percent'] for k in pk])

    feat = _nw_extract_features(
        daily_pcts, market_chg, market_index=stock_idx,
        price_pos_60=price_pos_60, prev_week_chg=prev_week_chg)

    return feat, market_chg, price_pos_60, prev_week_chg


def validate_stock(code, name, klines, mkt_by_week, dt_end):
    print(f"\n{'='*70}")
    print(f"  {name} ({code})")
    print(f"{'='*70}")

    # 数据检查
    if not klines:
        print(f"  ⚠️ 数据问题: 无K线数据")
        return
    print(f"  K线: {len(klines)}条, {klines[0]['date']} ~ {klines[-1]['date']}")

    stock_idx = _get_stock_index(code)
    print(f"  对应指数: {stock_idx}")

    if len(klines) < 60:
        print(f"  ⚠️ 数据问题: 仅{len(klines)}条K线, 需要至少60条")
        return

    idx_by_week = mkt_by_week.get(stock_idx, {})
    if not idx_by_week:
        print(f"  ⚠️ 数据问题: 指数{stock_idx}无K线数据")
        return

    # 按周分组
    wg = defaultdict(list)
    for k in klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        wg[dt.isocalendar()[:2]].append(k)

    sorted_weeks = sorted(wg.keys())
    dt_cutoff = dt_end - timedelta(days=N_WEEKS * 7 + 14)

    total = 0
    correct = 0
    details = []

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

        feat, market_chg, pos60, prev_chg = compute_features(
            this_days, klines, stock_idx, idx_by_week, iw_this)
        rule = _nw_match_rule(feat)

        wk = f"{iw_this[0]}-W{iw_this[1]:02d}"
        pos_s = f"{pos60:.2f}" if pos60 is not None else "-"
        prev_s = f"{prev_chg:+.1f}%" if prev_chg is not None else "-"

        if rule is None:
            details.append(
                f"  {wk}  本周{this_chg:+.1f}%  大盘{market_chg:+.1f}%  "
                f"pos={pos_s}  前周={prev_s}  → 未触发  (下周{next_chg:+.1f}%)")
            continue

        pred_up = rule['pred_up']
        tier = rule['tier']
        conf = 'high' if tier == 1 else 'reference'

        vol_patterns = _detect_volume_patterns(this_days, klines)
        vn = ''
        if vol_patterns.get('vol_direction'):
            conf, vn = _adjust_nw_confidence_by_volume(pred_up, conf, vol_patterns)

        is_correct = pred_up == actual_up
        total += 1
        if is_correct:
            correct += 1

        mark = '✓' if is_correct else '✗'
        pred_s = '涨' if pred_up else '跌'
        act_s = '涨' if actual_up else '跌'
        details.append(
            f"  {wk}  本周{this_chg:+.1f}%  大盘{market_chg:+.1f}%  "
            f"pos={pos_s}  前周={prev_s}  → [{rule['name']}]  "
            f"预测{pred_s} 实际{act_s} {mark}  置信:{conf}  下周{next_chg:+.1f}%"
            f"{'  '+vn if vn else ''}")

    # 输出回测结果
    print(f"\n  回测({N_WEEKS}周):")
    if total > 0:
        print(f"  准确率: {correct/total*100:.1f}% ({correct}/{total})")
    else:
        print(f"  无规则触发")

    print(f"\n  逐周详情:")
    for d in details:
        print(d)

    # 当前周下周预测（数据不足时回退到上一周）
    if sorted_weeks:
        last_iw = sorted_weeks[-1]
        last_days = sorted(wg[last_iw], key=lambda x: x['date'])

        if len(last_days) >= 3:
            # 当前周数据充足，直接预测
            use_iw = last_iw
            use_days = last_days
            label = "当前周"
        elif len(sorted_weeks) >= 2:
            # 当前周数据不足，回退到上一完整周
            prev_iw = sorted_weeks[-2]
            prev_days = sorted(wg[prev_iw], key=lambda x: x['date'])
            print(f"\n  当前周 {last_iw[0]}-W{last_iw[1]:02d} 仅{len(last_days)}天数据"
                  f"({', '.join(d['date'] for d in last_days)})，回退到上一周分析")
            use_iw = prev_iw
            use_days = prev_days
            label = "上一周"
        else:
            print(f"\n  当前周仅{len(last_days)}天数据, 且无上一周可回退")
            use_iw = None
            use_days = None
            label = None

        if use_iw and use_days and len(use_days) >= 3:
            this_chg = _compound_return([d['change_percent'] for d in use_days])
            feat, mkt_chg, pos60, prev_chg = compute_features(
                use_days, klines, stock_idx, idx_by_week, use_iw)
            rule = _nw_match_rule(feat)

            print(f"\n  ── {label} {use_iw[0]}-W{use_iw[1]:02d} "
                  f"({use_days[0]['date']}~{use_days[-1]['date']}) ──")
            pos_s = f"{pos60:.2f}" if pos60 is not None else "-"
            prev_s = f"{prev_chg:+.1f}%" if prev_chg is not None else "-"
            print(f"  周涨跌: {this_chg:+.1f}%  大盘: {mkt_chg:+.1f}%  "
                  f"价格位置: {pos_s}  前周: {prev_s}")

            if rule:
                pred = '涨' if rule['pred_up'] else '跌'
                conf = 'high' if rule['tier'] == 1 else 'reference'
                vol_patterns = _detect_volume_patterns(use_days, klines)
                vn = ''
                if vol_patterns.get('vol_direction'):
                    conf, vn = _adjust_nw_confidence_by_volume(rule['pred_up'], conf, vol_patterns)
                target = "本周" if label == "上一周" else "下周"
                print(f"  ★ {target}预测: {pred}  规则: {rule['name']}  "
                      f"置信: {conf}{'  '+vn if vn else ''}")
            else:
                target = "本周" if label == "上一周" else "下周"
                print(f"  ★ {target}预测: 未触发任何规则（不确定）")


def main():
    latest_date, dt_end, stock_klines, mkt_klines, mkt_by_week = load_data()

    for code, name in STOCKS.items():
        klines = stock_klines.get(code, [])
        validate_stock(code, name, klines, mkt_by_week, dt_end)


if __name__ == '__main__':
    main()
