#!/usr/bin/env python3
"""调试沪电股份(002463.SZ)下周预测为何是"不确定" """
import sys
sys.path.insert(0, '.')

import logging
logging.basicConfig(level=logging.WARNING)

from collections import defaultdict
from datetime import datetime, timedelta
from dao import get_connection
from service.weekly_prediction_service import (
    _to_float, _compound_return, _get_stock_index,
    _nw_extract_features, _nw_match_rule, _NW_RULES,
    _get_latest_trade_date,
)

CODE = '002463.SZ'
NAME = '沪电股份'


def main():
    latest_date = _get_latest_trade_date()
    print(f"最新交易日: {latest_date}")

    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=250)
    start_date = dt_start.strftime('%Y-%m-%d')

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 个股K线
    cur.execute(
        "SELECT stock_code, `date`, open_price, close_price, high_price, "
        "low_price, change_percent, trading_volume "
        "FROM stock_kline WHERE stock_code = %s "
        "AND `date` >= %s AND `date` <= %s ORDER BY `date`",
        [CODE, start_date, latest_date])
    klines = [{
        'date': r['date'], 'open': _to_float(r['open_price']),
        'close': _to_float(r['close_price']), 'high': _to_float(r['high_price']),
        'low': _to_float(r['low_price']),
        'change_percent': _to_float(r['change_percent']),
        'volume': _to_float(r['trading_volume']),
    } for r in cur.fetchall()]

    # 大盘K线
    stock_idx = _get_stock_index(CODE)
    print(f"对应指数: {stock_idx}")

    cur.execute(
        "SELECT stock_code, `date`, change_percent FROM stock_kline "
        "WHERE stock_code = %s AND `date` >= %s AND `date` <= %s ORDER BY `date`",
        [stock_idx, start_date, latest_date])
    mkt_klines = [{
        'date': r['date'], 'change_percent': _to_float(r['change_percent'])
    } for r in cur.fetchall()]
    conn.close()

    print(f"个股K线: {len(klines)}条, {klines[0]['date']} ~ {klines[-1]['date']}")
    print(f"指数K线: {len(mkt_klines)}条")

    # 按周分组
    wg = defaultdict(list)
    for k in klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        wg[dt.isocalendar()[:2]].append(k)

    mkt_wg = defaultdict(list)
    for k in mkt_klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        mkt_wg[dt.isocalendar()[:2]].append(k)

    sorted_weeks = sorted(wg.keys())

    # 找到最新的完整周（>=3天）
    last_iw = None
    for iw in reversed(sorted_weeks):
        if len(wg[iw]) >= 3:
            last_iw = iw
            break

    if not last_iw:
        print("无完整周数据")
        return

    this_days = sorted(wg[last_iw], key=lambda x: x['date'])
    print(f"\n{'='*60}")
    print(f"  分析周: {last_iw[0]}-W{last_iw[1]:02d}")
    print(f"  日期: {this_days[0]['date']} ~ {this_days[-1]['date']} ({len(this_days)}天)")
    print(f"{'='*60}")

    # 本周日K线详情
    print(f"\n本周日K线:")
    for d in this_days:
        print(f"  {d['date']}  收盘:{d['close']:.2f}  涨跌:{d['change_percent']:+.2f}%  "
              f"成交量:{d['volume']:.0f}")

    daily_pcts = [d['change_percent'] for d in this_days]
    this_chg = _compound_return(daily_pcts)
    print(f"\n本周复合涨跌幅: {this_chg:+.2f}%")

    # 大盘本周
    mw = sorted(mkt_wg.get(last_iw, []), key=lambda x: x['date'])
    market_chg = _compound_return([k['change_percent'] for k in mw]) if len(mw) >= 3 else 0.0
    print(f"大盘本周涨跌幅: {market_chg:+.2f}%")
    print(f"大盘日K线:")
    for d in mw:
        print(f"  {d['date']}  涨跌:{d['change_percent']:+.2f}%")

    # 价格位置
    sorted_all = sorted(klines, key=lambda x: x['date'])
    first_date = this_days[0]['date']
    hist = [k for k in sorted_all if k['date'] < first_date]

    price_pos_60 = None
    if len(hist) >= 20:
        hc = [k['close'] for k in hist[-60:] if k.get('close', 0) > 0]
        if hc:
            all_c = hc + [k['close'] for k in this_days if k.get('close', 0) > 0]
            mn, mx = min(all_c), max(all_c)
            lc = this_days[-1]['close']
            if mx > mn and lc > 0:
                price_pos_60 = round((lc - mn) / (mx - mn), 4)
    print(f"\n价格位置(60日): {price_pos_60}")

    # 前一周涨跌
    prev_week_chg = None
    pk = hist[-5:] if len(hist) >= 5 else hist
    if pk:
        prev_week_chg = _compound_return([k['change_percent'] for k in pk])
    print(f"前一周涨跌幅: {prev_week_chg:+.2f}%" if prev_week_chg else "前一周涨跌幅: None")

    # 连涨连跌
    consec_down = 0
    consec_up = 0
    for p in reversed(daily_pcts):
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
    print(f"连跌天数: {consec_down}, 连涨天数: {consec_up}")

    # 市场后缀
    market_suffix = stock_idx.split('.')[-1] if '.' in stock_idx else ''
    print(f"市场后缀: {market_suffix}")

    # 提取特征
    feat = _nw_extract_features(
        daily_pcts, market_chg, market_index=stock_idx,
        price_pos_60=price_pos_60, prev_week_chg=prev_week_chg)

    print(f"\n{'='*60}")
    print(f"  特征值:")
    print(f"{'='*60}")
    for k, v in feat.items():
        print(f"  {k}: {v}")

    # 逐条规则检查
    print(f"\n{'='*60}")
    print(f"  逐条规则匹配检查:")
    print(f"{'='*60}")

    chg = feat['this_week_chg']
    mkt = feat['market_chg']
    cd = feat['consec_down']
    cu = feat['consec_up']
    ld = feat['last_day_chg']
    extra = {
        'ff_signal': feat.get('ff_signal'),
        'vol_ratio': feat.get('vol_ratio'),
        'vol_price_corr': feat.get('vol_price_corr'),
        'finance_score': feat.get('finance_score'),
        '_mkt_threshold': feat.get('_mkt_threshold', 1.0),
        '_market_suffix': feat.get('_market_suffix', ''),
        '_price_pos_60': feat.get('_price_pos_60'),
        '_prev_week_chg': feat.get('_prev_week_chg'),
    }

    for i, rule in enumerate(_NW_RULES):
        matched = rule['check'](chg, mkt, cd, cu, ld, **extra)
        pred = '涨' if rule['pred_up'] else '跌'
        tier = rule['tier']
        status = '✓ 命中' if matched else '✗ 未命中'
        print(f"\n  [{i+1}] {rule['name']} (T{tier}, 预测{pred})")
        print(f"      {status}")

        # 详细说明为什么未命中
        if not matched:
            name = rule['name']
            reasons = []
            if 'R1' in name or '大盘深跌' in name:
                if chg >= -2: reasons.append(f"个股跌幅{chg:+.1f}%，需<-2%")
                if mkt >= -3: reasons.append(f"大盘跌幅{mkt:+.1f}%，需<-3%")
            elif '上证' in name and '跌>5%' in name:
                if market_suffix != 'SH': reasons.append(f"市场={market_suffix}，需SH")
                if chg >= -5: reasons.append(f"个股跌幅{chg:+.1f}%，需<-5%")
                if not (-3 <= mkt < -1): reasons.append(f"大盘{mkt:+.1f}%，需-3~-1%")
            elif '上证' in name and '前周跌→涨' in name:
                if market_suffix != 'SH': reasons.append(f"市场={market_suffix}，需SH")
                if chg >= -3: reasons.append(f"个股跌幅{chg:+.1f}%，需<-3%")
                if not (-3 <= mkt < -1): reasons.append(f"大盘{mkt:+.1f}%，需-3~-1%")
                if prev_week_chg is not None and prev_week_chg >= -2:
                    reasons.append(f"前周{prev_week_chg:+.1f}%，需<-2%")
            elif '深证+大盘微跌+跌+连跌' in name:
                if market_suffix != 'SZ': reasons.append(f"市场={market_suffix}，需SZ")
                if not (-1 <= mkt < 0): reasons.append(f"大盘{mkt:+.1f}%，需-1~0%")
                if chg >= -2: reasons.append(f"个股跌幅{chg:+.1f}%，需<-2%")
                if cd < 3: reasons.append(f"连跌{cd}天，需≥3天")
            elif '深证+大盘微跌+跌+低位' in name:
                if market_suffix != 'SZ': reasons.append(f"市场={market_suffix}，需SZ")
                if not (-1 <= mkt < 0): reasons.append(f"大盘{mkt:+.1f}%，需-1~0%")
                if chg >= -2: reasons.append(f"个股跌幅{chg:+.1f}%，需<-2%")
                if price_pos_60 is not None and price_pos_60 >= 0.2:
                    reasons.append(f"价格位置{price_pos_60:.2f}，需<0.2")
            elif '深证+大盘微跌+跌>2%' in name:
                if market_suffix != 'SZ': reasons.append(f"市场={market_suffix}，需SZ")
                if not (-1 <= mkt < 0): reasons.append(f"大盘{mkt:+.1f}%，需-1~0%")
                if chg >= -2: reasons.append(f"个股跌幅{chg:+.1f}%，需<-2%")
            elif '深证+大盘跌+涨>5%' in name:
                if market_suffix != 'SZ': reasons.append(f"市场={market_suffix}，需SZ")
                if not (-3 <= mkt < -1): reasons.append(f"大盘{mkt:+.1f}%，需-3~-1%")
                if chg <= 5: reasons.append(f"个股涨幅{chg:+.1f}%，需>5%")
            elif '深证+大盘跌+涨+连涨' in name:
                if market_suffix != 'SZ': reasons.append(f"市场={market_suffix}，需SZ")
                if not (-3 <= mkt < -1): reasons.append(f"大盘{mkt:+.1f}%，需-3~-1%")
                if chg <= 2: reasons.append(f"个股涨幅{chg:+.1f}%，需>2%")
                if cu < 3: reasons.append(f"连涨{cu}天，需≥3天")
            elif '跌+前期连涨+非高位' in name:
                if chg >= -3: reasons.append(f"个股跌幅{chg:+.1f}%，需<-3%")
                if cu < 3: reasons.append(f"连涨{cu}天，需≥3天")
                if price_pos_60 is not None and price_pos_60 >= 0.6:
                    reasons.append(f"价格位置{price_pos_60:.2f}，需<0.6")
            elif '上证+大盘微跌+涨+前周跌' in name:
                if market_suffix != 'SH': reasons.append(f"市场={market_suffix}，需SH")
                if not (-1 <= mkt < 0): reasons.append(f"大盘{mkt:+.1f}%，需-1~0%")
                if chg <= 2: reasons.append(f"个股涨幅{chg:+.1f}%，需>2%")
                if prev_week_chg is not None and prev_week_chg >= -3:
                    reasons.append(f"前周{prev_week_chg:+.1f}%，需<-3%")
            elif '资金' in name or '财报' in name:
                reasons.append("需要实盘资金流向/财报数据")

            if reasons:
                for r in reasons:
                    print(f"      原因: {r}")

    # 总结
    rule = _nw_match_rule(feat)
    print(f"\n{'='*60}")
    if rule:
        print(f"  最终匹配: {rule['name']} → {'涨' if rule['pred_up'] else '跌'}")
    else:
        print(f"  最终结果: 未命中任何规则 → 不确定")
        print(f"\n  关键原因分析:")
        print(f"  - 个股本周涨跌: {chg:+.2f}%")
        print(f"  - 大盘本周涨跌: {mkt:+.2f}%")
        print(f"  - 市场: {market_suffix} (对应指数: {stock_idx})")

        # 分析最接近触发的规则
        print(f"\n  最接近触发的规则:")
        if market_suffix == 'SZ':
            if -1 <= mkt < 0 and chg < -2:
                print(f"  → R5c几乎命中，但大盘{mkt:+.2f}%不在-1~0%范围")
            elif mkt < -1:
                if chg < -2:
                    print(f"  → 大盘跌{mkt:+.2f}%在-3~-1%范围，但个股也跌，无对应涨规则")
                elif chg > 2:
                    if chg > 5:
                        print(f"  → R6a接近: 深证+大盘跌+涨>5%")
                    elif cu >= 3:
                        print(f"  → R6c接近: 深证+大盘跌+涨+连涨3天")
            if mkt >= 0:
                print(f"  → 大盘涨{mkt:+.2f}%，当前无大盘涨时的规则")
        elif market_suffix == 'SH':
            if mkt >= 0:
                print(f"  → 大盘涨{mkt:+.2f}%，当前无大盘涨时的规则")
            elif -1 <= mkt < 0:
                if chg < 0:
                    print(f"  → 上证微跌环境，个股跌{chg:+.2f}%，无对应涨规则(需跌>5%或前周跌)")

    # 回测最近几周
    print(f"\n{'='*60}")
    print(f"  最近8周预测回顾:")
    print(f"{'='*60}")
    recent_weeks = sorted_weeks[-9:]  # 取最近9周（8对）
    for i in range(len(recent_weeks) - 1):
        iw_this = recent_weeks[i]
        iw_next = recent_weeks[i + 1]
        td = sorted(wg[iw_this], key=lambda x: x['date'])
        nd = sorted(wg[iw_next], key=lambda x: x['date'])
        if len(td) < 3:
            continue
        tc = _compound_return([d['change_percent'] for d in td])
        nc = _compound_return([d['change_percent'] for d in nd]) if len(nd) >= 1 else None

        mw2 = sorted(mkt_wg.get(iw_this, []), key=lambda x: x['date'])
        mc = _compound_return([k['change_percent'] for k in mw2]) if len(mw2) >= 3 else 0.0

        # 前周
        idx_prev = sorted_weeks.index(iw_this)
        pw_chg = None
        if idx_prev > 0:
            prev_iw = sorted_weeks[idx_prev - 1]
            prev_d = sorted(wg[prev_iw], key=lambda x: x['date'])
            if prev_d:
                pw_chg = _compound_return([d['change_percent'] for d in prev_d])

        # 价格位置
        h2 = [k for k in sorted_all if k['date'] < td[0]['date']]
        pp2 = None
        if len(h2) >= 20:
            hc2 = [k['close'] for k in h2[-60:] if k.get('close', 0) > 0]
            if hc2:
                ac2 = hc2 + [k['close'] for k in td if k.get('close', 0) > 0]
                mn2, mx2 = min(ac2), max(ac2)
                lc2 = td[-1]['close']
                if mx2 > mn2 and lc2 > 0:
                    pp2 = round((lc2 - mn2) / (mx2 - mn2), 4)

        f2 = _nw_extract_features(
            [d['change_percent'] for d in td], mc,
            market_index=stock_idx, price_pos_60=pp2, prev_week_chg=pw_chg)
        r2 = _nw_match_rule(f2)

        wk = f"{iw_this[0]}-W{iw_this[1]:02d}"
        pred_s = f"[{r2['name']}]→{'涨' if r2['pred_up'] else '跌'}" if r2 else "不确定"
        act_s = f"下周{nc:+.1f}%" if nc is not None else "?"
        pp_s = f"{pp2:.2f}" if pp2 is not None else "-"
        pw_s = f"{pw_chg:+.1f}%" if pw_chg is not None else "-"
        print(f"  {wk}  本周{tc:+.1f}%  大盘{mc:+.1f}%  pos={pp_s}  前周={pw_s}  "
              f"→ {pred_s}  {act_s}")


if __name__ == '__main__':
    main()
