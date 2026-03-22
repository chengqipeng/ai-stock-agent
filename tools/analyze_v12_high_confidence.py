#!/usr/bin/env python3
"""
V12 High Confidence 深度分析
============================
专门拆解 high 置信度预测的细节：
- 周度分布和准确率
- UP vs DOWN 方向分布
- 极端分数分布
- 各信号独立贡献
- 收益分布（正确/错误预测的实际涨跌幅）
- 按大盘环境分析
- 按周跌幅区间分析
"""
import argparse
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dao import get_connection
from service.v12_prediction.v12_engine import V12PredictionEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s',
                    datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data_results"


def _to_float(v) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def load_stock_codes(limit: int = 200) -> list[str]:
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT stock_code, COUNT(*) AS cnt
        FROM stock_kline
        WHERE stock_code NOT LIKE '%%.BJ'
        GROUP BY stock_code
        HAVING cnt >= 120
        ORDER BY cnt DESC
        LIMIT %s
    """, (limit,))
    codes = [r['stock_code'] for r in cur.fetchall()]
    cur.close()
    conn.close()
    logger.info("加载 %d 只股票", len(codes))
    return codes


def load_kline_data(stock_codes, start_date, end_date):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = defaultdict(list)
    bs = 300
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, close_price, open_price, high_price, "
            f"low_price, trading_volume, change_percent, change_hand "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [start_date, end_date])
        for row in cur.fetchall():
            result[row['stock_code']].append({
                'date': str(row['date']),
                'close': _to_float(row['close_price']),
                'open': _to_float(row['open_price']),
                'high': _to_float(row['high_price']),
                'low': _to_float(row['low_price']),
                'volume': _to_float(row['trading_volume']),
                'change_percent': _to_float(row['change_percent']),
                'turnover': _to_float(row.get('change_hand')),
            })
    cur.close()
    conn.close()
    return dict(result)


def load_fund_flow_data(stock_codes, start_date):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = defaultdict(list)
    bs = 300
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, big_net_pct, net_flow, main_net_5day "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s ORDER BY `date` DESC",
            batch + [start_date])
        for row in cur.fetchall():
            result[row['stock_code']].append({
                'date': str(row['date']),
                'big_net_pct': _to_float(row.get('big_net_pct')),
                'net_flow': _to_float(row.get('net_flow')),
                'main_net_5day': _to_float(row.get('main_net_5day')),
            })
    cur.close()
    conn.close()
    return dict(result)


def load_market_klines(start_date, end_date):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute(
        "SELECT `date`, close_price, change_percent "
        "FROM stock_kline WHERE stock_code = '000001.SH' "
        "AND `date` >= %s AND `date` <= %s ORDER BY `date`",
        (start_date, end_date))
    result = []
    for row in cur.fetchall():
        result.append({
            'date': str(row['date']),
            'close': _to_float(row.get('close_price')),
            'change_percent': _to_float(row['change_percent']),
        })
    cur.close()
    conn.close()
    return result


def group_by_week(klines):
    weeks = defaultdict(list)
    for k in klines:
        d = datetime.strptime(k['date'][:10], '%Y-%m-%d')
        iso = d.isocalendar()
        key = f"{iso[0]}-W{iso[1]:02d}"
        weeks[key].append(k)
    return dict(weeks)


def run_analysis(n_weeks: int = 50, n_stocks: int = 200):
    t0 = time.time()
    logger.info("=" * 60)
    logger.info("V12 High Confidence 深度分析")
    logger.info("=" * 60)

    stock_codes = load_stock_codes(n_stocks)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=n_weeks * 7 + 120)).strftime('%Y-%m-%d')

    kline_data = load_kline_data(stock_codes, start_date, end_date)
    fund_flow_data = load_fund_flow_data(stock_codes, start_date)
    market_klines = load_market_klines(start_date, end_date)

    # 按周组织数据
    stock_weekly = {}
    all_week_keys = set()

    for code in stock_codes:
        klines = kline_data.get(code, [])
        if len(klines) < 80:
            continue
        weekly_groups = group_by_week(klines)
        sorted_weeks = sorted(weekly_groups.keys())
        if len(sorted_weeks) < 4:
            continue

        fund_flow = fund_flow_data.get(code, [])
        week_info = {}

        for wi in range(len(sorted_weeks) - 1):
            week_key = sorted_weeks[wi]
            next_week_key = sorted_weeks[wi + 1]
            week_klines = weekly_groups[week_key]
            next_klines = weekly_groups[next_week_key]
            last_date = week_klines[-1]['date']

            idx = None
            for j, k in enumerate(klines):
                if k['date'] == last_date:
                    idx = j
                    break
            if idx is None or idx < 60:
                continue

            base_close = week_klines[-1].get('close', 0)
            end_close = next_klines[-1].get('close', 0)
            if base_close <= 0 or end_close <= 0:
                continue

            actual_return = (end_close / base_close - 1) * 100
            hist_ff = [f for f in fund_flow if f['date'] <= last_date][:20]

            week_info[week_key] = {
                'hist_klines': klines[:idx + 1],
                'hist_ff': hist_ff,
                'actual_return': actual_return,
                'last_date': last_date,
            }
            all_week_keys.add(week_key)

        if week_info:
            stock_weekly[code] = week_info

    sorted_all_weeks = sorted(all_week_keys)
    if len(sorted_all_weeks) > n_weeks:
        sorted_all_weeks = sorted_all_weeks[-n_weeks:]

    # 收集所有 high confidence 预测明细
    high_records = []

    for week_key in sorted_all_weeks:
        engine = V12PredictionEngine()

        # 计算本周截面波动率中位数
        week_vols = []
        week_turns = []
        for code, week_info in stock_weekly.items():
            if week_key not in week_info:
                continue
            kls = week_info[week_key]['hist_klines']
            if len(kls) >= 20:
                pcts = [k.get('change_percent', 0) or 0 for k in kls]
                recent = pcts[-20:]
                m = sum(recent) / 20
                vol = (sum((p - m) ** 2 for p in recent) / 19) ** 0.5
                week_vols.append(vol)
                turnover_vals = [k.get('turnover', 0) or 0 for k in kls]
                avg_turn = sum(turnover_vals[-20:]) / 20
                week_turns.append(avg_turn)
        vol_median = sorted(week_vols)[len(week_vols) // 2] if week_vols else None
        turn_median = sorted(week_turns)[len(week_turns) // 2] if week_turns else None

        for code, week_info in stock_weekly.items():
            if week_key not in week_info:
                continue
            data = week_info[week_key]
            last_date = data.get('last_date', '')
            mkt_hist = [m for m in market_klines if m['date'] <= last_date] if market_klines else None

            pred = engine.predict_single(code, data['hist_klines'], data['hist_ff'], mkt_hist, vol_median, turn_median)
            if pred is not None and pred['confidence'] == 'high':
                actual = data['actual_return']
                actual_up = actual > 0
                pred_up = pred['pred_direction'] == 'UP'
                is_correct = pred_up == actual_up

                high_records.append({
                    'week': week_key,
                    'stock_code': code,
                    'pred_direction': pred['pred_direction'],
                    'extreme_score': pred['extreme_score'],
                    'composite_score': pred['composite_score'],
                    'n_signals': pred['n_signals'],
                    'avg_strength': pred['avg_strength'],
                    'market_aligned': pred.get('market_aligned', True),
                    'actual_return': round(actual, 2),
                    'is_correct': is_correct,
                    'reason': pred['reason'],
                    'signal_scores': {s['signal']: s['score'] for s in pred.get('signals', [])},
                    'week_chg': round(pred.get('conditions', {}).get('week_chg', 0), 2),
                    'rsi': round(pred.get('conditions', {}).get('rsi', 50), 1),
                    'price_pos': round(pred.get('conditions', {}).get('price_pos', 0.5), 3),
                })

    logger.info("收集到 %d 条 high confidence 预测", len(high_records))

    # ── 分析 ──
    total = len(high_records)
    correct = sum(1 for r in high_records if r['is_correct'])
    accuracy = correct / total if total > 0 else 0

    # 1. 按周分布
    by_week = defaultdict(lambda: {'total': 0, 'correct': 0, 'returns': []})
    for r in high_records:
        by_week[r['week']]['total'] += 1
        by_week[r['week']]['returns'].append(r['actual_return'])
        if r['is_correct']:
            by_week[r['week']]['correct'] += 1

    # 2. 按方向
    by_dir = defaultdict(lambda: {'total': 0, 'correct': 0, 'returns': []})
    for r in high_records:
        by_dir[r['pred_direction']]['total'] += 1
        by_dir[r['pred_direction']]['returns'].append(r['actual_return'])
        if r['is_correct']:
            by_dir[r['pred_direction']]['correct'] += 1

    # 3. 按极端分数
    by_es = defaultdict(lambda: {'total': 0, 'correct': 0})
    for r in high_records:
        es = r['extreme_score']
        key = f"score_{es}" if es <= 8 else "score_9+"
        by_es[key]['total'] += 1
        if r['is_correct']:
            by_es[key]['correct'] += 1

    # 4. 各信号在 high 中的独立准确率
    by_signal = defaultdict(lambda: {'total': 0, 'correct': 0})
    for r in high_records:
        actual_up = r['actual_return'] > 0
        for sig_name, sig_score in r['signal_scores'].items():
            sig_up = sig_score > 0
            by_signal[sig_name]['total'] += 1
            if sig_up == actual_up:
                by_signal[sig_name]['correct'] += 1

    # 5. 收益分布
    correct_returns = sorted([r['actual_return'] for r in high_records if r['is_correct']])
    wrong_returns = sorted([r['actual_return'] for r in high_records if not r['is_correct']])
    all_returns = sorted([r['actual_return'] for r in high_records])

    # 6. 按 week_chg 区间
    by_weekchg = defaultdict(lambda: {'total': 0, 'correct': 0, 'returns': []})
    for r in high_records:
        wc = r['week_chg']
        if wc < -10:
            bucket = '<-10%'
        elif wc < -7:
            bucket = '-10~-7%'
        elif wc < -5:
            bucket = '-7~-5%'
        elif wc < -3:
            bucket = '-5~-3%'
        elif wc > 10:
            bucket = '>10%'
        elif wc > 5:
            bucket = '5~10%'
        else:
            bucket = '-3~5%'
        by_weekchg[bucket]['total'] += 1
        by_weekchg[bucket]['returns'].append(r['actual_return'])
        if r['is_correct']:
            by_weekchg[bucket]['correct'] += 1

    # 7. 大盘当周涨跌 vs high准确率
    mkt_week_pct = {}
    if market_klines:
        mkt_by_week = group_by_week(market_klines)
        for wk, kls in mkt_by_week.items():
            mkt_week_pct[wk] = sum(k.get('change_percent', 0) or 0 for k in kls)

    by_mkt_regime = defaultdict(lambda: {'total': 0, 'correct': 0})
    for r in high_records:
        mkt_ret = mkt_week_pct.get(r['week'], 0)
        if mkt_ret < -3:
            regime = '大盘暴跌(<-3%)'
        elif mkt_ret < -1:
            regime = '大盘小跌(-3~-1%)'
        elif mkt_ret < 1:
            regime = '大盘震荡(-1~1%)'
        elif mkt_ret < 3:
            regime = '大盘小涨(1~3%)'
        else:
            regime = '大盘大涨(>3%)'
        by_mkt_regime[regime]['total'] += 1
        if r['is_correct']:
            by_mkt_regime[regime]['correct'] += 1

    # 8. 按 composite_score 强度分段
    by_score_strength = defaultdict(lambda: {'total': 0, 'correct': 0})
    for r in high_records:
        cs = abs(r['composite_score'])
        if cs > 0.4:
            bucket = '强信号(>0.4)'
        elif cs > 0.25:
            bucket = '中信号(0.25~0.4)'
        else:
            bucket = '弱信号(<0.25)'
        by_score_strength[bucket]['total'] += 1
        if r['is_correct']:
            by_score_strength[bucket]['correct'] += 1

    # 9. 按 RSI 区间
    by_rsi = defaultdict(lambda: {'total': 0, 'correct': 0})
    for r in high_records:
        rsi = r['rsi']
        if rsi < 25:
            bucket = 'RSI<25'
        elif rsi < 35:
            bucket = 'RSI 25-35'
        elif rsi < 50:
            bucket = 'RSI 35-50'
        elif rsi > 70:
            bucket = 'RSI>70'
        else:
            bucket = 'RSI 50-70'
        by_rsi[bucket]['total'] += 1
        if r['is_correct']:
            by_rsi[bucket]['correct'] += 1

    # 10. 错误预测集中度
    wrong_records = [r for r in high_records if not r['is_correct']]
    wrong_by_week = defaultdict(int)
    for r in wrong_records:
        wrong_by_week[r['week']] += 1

    # ── 输出报告 ──
    print("\n" + "=" * 70)
    print("V12 High Confidence 深度分析报告")
    print("=" * 70)

    print(f"\n📊 总览: {total}条 high 预测, 准确率 {accuracy:.1%}")
    print(f"   正确: {correct}, 错误: {total - correct}")

    print(f"\n📈 按预测方向:")
    for d in ('UP', 'DOWN'):
        dd = by_dir.get(d, {'total': 0, 'correct': 0, 'returns': []})
        if dd['total'] > 0:
            acc = dd['correct'] / dd['total']
            avg_ret = sum(dd['returns']) / len(dd['returns'])
            print(f"   {d:5s}: {acc:.1%} ({dd['total']}条), 平均实际收益: {avg_ret:+.2f}%")

    print(f"\n🔥 按极端分数:")
    for key in sorted(by_es.keys()):
        d = by_es[key]
        if d['total'] > 0:
            acc = d['correct'] / d['total']
            print(f"   {key:12s}: {acc:.1%} ({d['total']}条)")

    print(f"\n📡 各信号在 high 中的独立准确率:")
    for sig in sorted(by_signal.keys()):
        d = by_signal[sig]
        if d['total'] > 0:
            acc = d['correct'] / d['total']
            print(f"   {sig:25s}: {acc:.1%} ({d['total']}条)")

    print(f"\n💪 按综合得分强度:")
    for bucket in ['强信号(>0.4)', '中信号(0.25~0.4)', '弱信号(<0.25)']:
        d = by_score_strength.get(bucket, {'total': 0, 'correct': 0})
        if d['total'] > 0:
            acc = d['correct'] / d['total']
            print(f"   {bucket:20s}: {acc:.1%} ({d['total']}条)")

    print(f"\n📉 按RSI区间:")
    for bucket in ['RSI<25', 'RSI 25-35', 'RSI 35-50', 'RSI 50-70', 'RSI>70']:
        d = by_rsi.get(bucket, {'total': 0, 'correct': 0})
        if d['total'] > 0:
            acc = d['correct'] / d['total']
            print(f"   {bucket:15s}: {acc:.1%} ({d['total']}条)")

    print(f"\n💰 收益分布:")
    if correct_returns:
        mid = len(correct_returns) // 2
        print(f"   正确预测: 平均{sum(correct_returns)/len(correct_returns):+.2f}%, "
              f"中位数{correct_returns[mid]:+.2f}%, "
              f"范围[{correct_returns[0]:+.1f}%, {correct_returns[-1]:+.1f}%]")
    if wrong_returns:
        mid = len(wrong_returns) // 2
        print(f"   错误预测: 平均{sum(wrong_returns)/len(wrong_returns):+.2f}%, "
              f"中位数{wrong_returns[mid]:+.2f}%, "
              f"范围[{wrong_returns[0]:+.1f}%, {wrong_returns[-1]:+.1f}%]")
    if all_returns:
        print(f"   全部预测: 平均{sum(all_returns)/len(all_returns):+.2f}%")
        # 盈亏比
        gains = [r for r in all_returns if r > 0]
        losses = [r for r in all_returns if r < 0]
        if gains and losses:
            avg_gain = sum(gains) / len(gains)
            avg_loss = abs(sum(losses) / len(losses))
            print(f"   盈亏比: {avg_gain:.2f}% / {avg_loss:.2f}% = {avg_gain/avg_loss:.2f}")

    print(f"\n📉 按周跌幅区间:")
    for bucket in ['<-10%', '-10~-7%', '-7~-5%', '-5~-3%', '-3~5%', '5~10%', '>10%']:
        d = by_weekchg.get(bucket, {'total': 0, 'correct': 0, 'returns': []})
        if d['total'] > 0:
            acc = d['correct'] / d['total']
            avg_ret = sum(d['returns']) / len(d['returns'])
            print(f"   {bucket:12s}: {acc:.1%} ({d['total']}条), 下周平均: {avg_ret:+.2f}%")

    print(f"\n🌍 按大盘当周涨跌:")
    for regime in ['大盘暴跌(<-3%)', '大盘小跌(-3~-1%)', '大盘震荡(-1~1%)',
                   '大盘小涨(1~3%)', '大盘大涨(>3%)']:
        d = by_mkt_regime.get(regime, {'total': 0, 'correct': 0})
        if d['total'] > 0:
            acc = d['correct'] / d['total']
            print(f"   {regime:20s}: {acc:.1%} ({d['total']}条)")

    print(f"\n📅 周度明细 (high only):")
    for wk in sorted(by_week.keys()):
        d = by_week[wk]
        if d['total'] > 0:
            acc = d['correct'] / d['total']
            avg_ret = sum(d['returns']) / len(d['returns'])
            mkt = mkt_week_pct.get(wk, 0)
            marker = '✅' if acc >= 0.7 else ('⚠️' if acc >= 0.5 else '❌')
            print(f"   {marker} {wk}: {acc:.0%} ({d['correct']}/{d['total']}), "
                  f"平均收益{avg_ret:+.1f}%, 大盘{mkt:+.1f}%")

    print(f"\n❌ 错误预测集中度 (top 5 周):")
    sorted_wrong = sorted(wrong_by_week.items(), key=lambda x: x[1], reverse=True)[:5]
    for wk, cnt in sorted_wrong:
        total_wk = by_week[wk]['total']
        mkt = mkt_week_pct.get(wk, 0)
        print(f"   {wk}: {cnt}条错误 / {total_wk}条总预测, 大盘当周{mkt:+.1f}%")

    # 保存
    report = {
        'meta': {'total_high': total, 'accuracy': round(accuracy, 4),
                 'run_time_sec': round(time.time() - t0, 1)},
        'by_direction': {d: {'accuracy': round(v['correct']/v['total'], 4) if v['total'] else 0,
                             'total': v['total'],
                             'avg_return': round(sum(v['returns'])/len(v['returns']), 2) if v['returns'] else 0}
                         for d, v in by_dir.items()},
        'by_extreme_score': {k: {'accuracy': round(v['correct']/v['total'], 4) if v['total'] else 0,
                                  'total': v['total']}
                              for k, v in sorted(by_es.items())},
        'by_signal': {k: {'accuracy': round(v['correct']/v['total'], 4) if v['total'] else 0,
                           'total': v['total']}
                      for k, v in sorted(by_signal.items())},
        'by_week_chg': {k: {'accuracy': round(v['correct']/v['total'], 4) if v['total'] else 0,
                             'total': v['total'],
                             'avg_next_return': round(sum(v['returns'])/len(v['returns']), 2) if v['returns'] else 0}
                        for k, v in by_weekchg.items()},
        'by_market_regime': {k: {'accuracy': round(v['correct']/v['total'], 4) if v['total'] else 0,
                                  'total': v['total']}
                              for k, v in by_mkt_regime.items()},
        'by_composite_strength': {k: {'accuracy': round(v['correct']/v['total'], 4) if v['total'] else 0,
                                       'total': v['total']}
                                   for k, v in by_score_strength.items()},
        'by_rsi': {k: {'accuracy': round(v['correct']/v['total'], 4) if v['total'] else 0,
                        'total': v['total']}
                   for k, v in by_rsi.items()},
        'weekly_detail': [{'week': wk,
                           'accuracy': round(v['correct']/v['total'], 4) if v['total'] else 0,
                           'total': v['total'], 'correct': v['correct'],
                           'avg_return': round(sum(v['returns'])/len(v['returns']), 2) if v['returns'] else 0,
                           'mkt_pct': round(mkt_week_pct.get(wk, 0), 2)}
                          for wk, v in sorted(by_week.items())],
        'records': high_records,
    }

    output_path = OUTPUT_DIR / "v12_high_confidence_analysis.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    logger.info("详细结果已保存: %s", output_path)

    print(f"\n⏱️  耗时: {time.time() - t0:.1f}秒")
    print("=" * 70)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='V12 High Confidence 深度分析')
    parser.add_argument('--weeks', type=int, default=50, help='回测周数')
    parser.add_argument('--stocks', type=int, default=200, help='股票数量')
    args = parser.parse_args()
    run_analysis(n_weeks=args.weeks, n_stocks=args.stocks)
