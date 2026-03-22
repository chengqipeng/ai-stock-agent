#!/usr/bin/env python3
"""
V12 置信度校准诊断
==================
分析当前置信度分级的有效性，找出真正有区分度的维度。
目标：识别哪些因素真正影响预测准确率，哪些是噪声。

关键问题：
1. 当前high/medium/low的分级是否合理？
2. 哪些维度组合真正有区分度（>5pp差异）？
3. DOWN方向的降级是否过度？
4. 质量因子升降级是否有效？
"""
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


def _to_float(v):
    if v is None: return 0.0
    try: return float(v)
    except: return 0.0


def load_stock_codes(limit=200):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT stock_code, COUNT(*) AS cnt FROM stock_kline
        WHERE stock_code NOT LIKE '%%.BJ'
        GROUP BY stock_code HAVING cnt >= 120
        ORDER BY cnt DESC LIMIT %s
    """, (limit,))
    codes = [r['stock_code'] for r in cur.fetchall()]
    cur.close(); conn.close()
    return codes


def load_kline_data(stock_codes, start_date, end_date):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = defaultdict(list)
    bs = 300
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i+bs]
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
    cur.close(); conn.close()
    return dict(result)


def load_fund_flow_data(stock_codes, start_date):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = defaultdict(list)
    bs = 300
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i+bs]
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
    cur.close(); conn.close()
    return dict(result)


def load_market_klines(start_date, end_date):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute(
        "SELECT `date`, close_price, change_percent "
        "FROM stock_kline WHERE stock_code = '000001.SH' "
        "AND `date` >= %s AND `date` <= %s ORDER BY `date`",
        (start_date, end_date))
    result = [{'date': str(r['date']), 'close': _to_float(r.get('close_price')),
               'change_percent': _to_float(r['change_percent'])} for r in cur.fetchall()]
    cur.close(); conn.close()
    return result


def group_by_week(klines):
    weeks = defaultdict(list)
    for k in klines:
        d = datetime.strptime(k['date'][:10], '%Y-%m-%d')
        iso = d.isocalendar()
        weeks[f"{iso[0]}-W{iso[1]:02d}"].append(k)
    return dict(weeks)


def run_analysis(n_weeks=100, n_stocks=5000):
    t0 = time.time()
    logger.info("V12 置信度校准诊断 (stocks=%d, weeks=%d)", n_stocks, n_weeks)

    stock_codes = load_stock_codes(n_stocks)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=n_weeks * 7 + 120)).strftime('%Y-%m-%d')

    kline_data = load_kline_data(stock_codes, start_date, end_date)
    fund_flow_data = load_fund_flow_data(stock_codes, start_date)
    market_klines = load_market_klines(start_date, end_date)
    logger.info("数据加载完成: K线%d只, 资金流%d只", len(kline_data), len(fund_flow_data))

    mkt_by_week = group_by_week(market_klines)
    mkt_week_pct = {}
    for wk, kls in mkt_by_week.items():
        mkt_week_pct[wk] = sum(k.get('change_percent', 0) or 0 for k in kls)

    stock_weekly = {}
    all_week_keys = set()
    for code in stock_codes:
        klines = kline_data.get(code, [])
        if len(klines) < 80: continue
        weekly_groups = group_by_week(klines)
        sorted_weeks = sorted(weekly_groups.keys())
        if len(sorted_weeks) < 4: continue
        fund_flow = fund_flow_data.get(code, [])
        week_info = {}
        for wi in range(len(sorted_weeks) - 1):
            wk = sorted_weeks[wi]
            nwk = sorted_weeks[wi + 1]
            wkls = weekly_groups[wk]
            nkls = weekly_groups[nwk]
            last_date = wkls[-1]['date']
            idx = None
            for j, k in enumerate(klines):
                if k['date'] == last_date:
                    idx = j; break
            if idx is None or idx < 60: continue
            bc = wkls[-1].get('close', 0)
            ec = nkls[-1].get('close', 0)
            if bc <= 0 or ec <= 0: continue
            ar = (ec / bc - 1) * 100
            hff = [f for f in fund_flow if f['date'] <= last_date][:20]
            week_info[wk] = {'hist_klines': klines[:idx+1], 'hist_ff': hff,
                             'actual_return': ar, 'last_date': last_date}
            all_week_keys.add(wk)
        if week_info:
            stock_weekly[code] = week_info

    sorted_weeks = sorted(all_week_keys)
    if len(sorted_weeks) > n_weeks:
        sorted_weeks = sorted_weeks[-n_weeks:]
    logger.info("准备完成: %d只股票, %d周", len(stock_weekly), len(sorted_weeks))

    # 收集预测记录（包含所有原始特征，不受置信度分级影响）
    records = []
    for week_key in sorted_weeks:
        engine = V12PredictionEngine()
        week_vols, week_turns = [], []
        for code, wi in stock_weekly.items():
            if week_key not in wi: continue
            kls = wi[week_key]['hist_klines']
            if len(kls) >= 20:
                pcts = [k.get('change_percent', 0) or 0 for k in kls]
                r20 = pcts[-20:]
                m = sum(r20) / 20
                week_vols.append((sum((p-m)**2 for p in r20) / 19) ** 0.5)
                tvs = [k.get('turnover', 0) or 0 for k in kls]
                week_turns.append(sum(tvs[-20:]) / 20)
        vol_med = sorted(week_vols)[len(week_vols)//2] if week_vols else None
        turn_med = sorted(week_turns)[len(week_turns)//2] if week_turns else None
        mkt_pct = mkt_week_pct.get(week_key, 0)

        for code, wi in stock_weekly.items():
            if week_key not in wi: continue
            data = wi[week_key]
            ld = data['last_date']
            mh = [m for m in market_klines if m['date'] <= ld] if market_klines else None
            pred = engine.predict_single(code, data['hist_klines'], data['hist_ff'], mh, vol_med, turn_med)
            if pred is None: continue
            actual = data['actual_return']
            pred_up = pred['pred_direction'] == 'UP'
            is_correct = pred_up == (actual > 0)

            # 收集所有原始特征
            records.append({
                'week': week_key, 'code': code,
                'direction': pred['pred_direction'],
                'confidence': pred['confidence'],
                'extreme_score': pred['extreme_score'],
                'composite_score': pred.get('composite_score', 0),
                'n_signals': pred.get('n_signals', 0),
                'n_agree': pred.get('n_agree', 0),
                'avg_strength': pred.get('avg_strength', 0),
                'market_aligned': pred.get('market_aligned', False),
                'volume_confirmed': pred.get('volume_confirmed', False),
                'low_turnover_boost': pred.get('low_turnover_boost', False),
                'low_salience': pred.get('low_salience', False),
                'ivol_penalty': pred.get('ivol_penalty', False),
                'actual_return': actual,
                'is_correct': is_correct,
                'mkt_pct': mkt_pct,
                # 收益分析
                'dir_return': actual if pred['pred_direction'] == 'UP' else -actual,
            })

    logger.info("收集到 %d 条预测记录", len(records))

    def acc(recs):
        if not recs: return 0, 0
        c = sum(1 for r in recs if r['is_correct'])
        return round(c / len(recs), 4), len(recs)

    def avg_ret(recs):
        if not recs: return 0
        return round(sum(r['dir_return'] for r in recs) / len(recs), 4)

    print("\n" + "=" * 70)
    print("V12 置信度校准诊断")
    print("=" * 70)
    print(f"总计: {len(records)} 条预测")

    # ═══ 1. 当前置信度分级的有效性 ═══
    print(f"\n{'='*50}")
    print("1. 当前置信度分级有效性")
    print(f"{'='*50}")
    for conf in ['high', 'medium', 'low']:
        recs = [r for r in records if r['confidence'] == conf]
        a, n = acc(recs)
        ar = avg_ret(recs)
        print(f"   {conf:8s}: 准确率{a:.1%} ({n}条), 方向收益{ar:+.2f}%")

    # 分方向
    print(f"\n   按方向拆分:")
    for conf in ['high', 'medium', 'low']:
        for d in ['UP', 'DOWN']:
            recs = [r for r in records if r['confidence'] == conf and r['direction'] == d]
            a, n = acc(recs)
            ar = avg_ret(recs)
            if n > 0:
                print(f"   {conf:8s} {d:5s}: 准确率{a:.1%} ({n}条), 方向收益{ar:+.2f}%")

    # ═══ 2. 原始特征的区分度分析 ═══
    print(f"\n{'='*50}")
    print("2. 原始特征区分度（不受置信度分级影响）")
    print(f"{'='*50}")

    # 2a. extreme_score
    print(f"\n   极端分数:")
    for es in sorted(set(r['extreme_score'] for r in records)):
        recs = [r for r in records if r['extreme_score'] == es]
        a, n = acc(recs)
        ar = avg_ret(recs)
        print(f"   score={es}: 准确率{a:.1%} ({n}条), 方向收益{ar:+.2f}%")

    # 2b. market_aligned
    print(f"\n   大盘协同:")
    for ma in [True, False]:
        recs = [r for r in records if r['market_aligned'] == ma]
        a, n = acc(recs)
        ar = avg_ret(recs)
        label = '协同' if ma else '独立'
        print(f"   {label}: 准确率{a:.1%} ({n}条), 方向收益{ar:+.2f}%")

    # 2c. composite_score 分桶
    print(f"\n   综合得分分桶:")
    for lo, hi, label in [(0.3, 999, '强信号(>0.3)'), (0.2, 0.3, '中信号(0.2-0.3)'),
                           (0.1, 0.2, '弱信号(0.1-0.2)'), (-999, 0.1, '极弱(<0.1)')]:
        recs = [r for r in records if lo <= abs(r['composite_score']) < hi]
        a, n = acc(recs)
        ar = avg_ret(recs)
        if n > 0:
            print(f"   {label:20s}: 准确率{a:.1%} ({n}条), 方向收益{ar:+.2f}%")

    # 2d. n_agree (信号一致数)
    print(f"\n   信号一致数:")
    for na in sorted(set(r['n_agree'] for r in records)):
        recs = [r for r in records if r['n_agree'] == na]
        a, n = acc(recs)
        ar = avg_ret(recs)
        if n > 10:
            print(f"   n_agree={na}: 准确率{a:.1%} ({n}条), 方向收益{ar:+.2f}%")

    # 2e. avg_strength
    print(f"\n   平均信号强度分桶:")
    for lo, hi, label in [(0.3, 999, '强(>0.3)'), (0.2, 0.3, '中(0.2-0.3)'),
                           (0.1, 0.2, '弱(0.1-0.2)'), (0, 0.1, '极弱(<0.1)')]:
        recs = [r for r in records if lo <= r['avg_strength'] < hi]
        a, n = acc(recs)
        ar = avg_ret(recs)
        if n > 10:
            print(f"   {label:20s}: 准确率{a:.1%} ({n}条), 方向收益{ar:+.2f}%")

    # ═══ 3. 质量因子的真实区分度 ═══
    print(f"\n{'='*50}")
    print("3. 质量因子真实区分度")
    print(f"{'='*50}")

    for factor, label in [('volume_confirmed', '缩量确认'),
                           ('low_turnover_boost', '低换手'),
                           ('low_salience', '低显著性'),
                           ('ivol_penalty', 'IVOL惩罚')]:
        yes = [r for r in records if r[factor]]
        no = [r for r in records if not r[factor]]
        a_yes, n_yes = acc(yes)
        a_no, n_no = acc(no)
        ar_yes = avg_ret(yes)
        ar_no = avg_ret(no)
        diff = a_yes - a_no
        effective = "✓有效" if abs(diff) > 0.03 else "✗无效"
        print(f"\n   {label}:")
        print(f"     是: 准确率{a_yes:.1%} ({n_yes}条), 方向收益{ar_yes:+.2f}%")
        print(f"     否: 准确率{a_no:.1%} ({n_no}条), 方向收益{ar_no:+.2f}%")
        print(f"     差异: {diff:+.1%} → {effective}")

    # ═══ 4. 寻找真正有区分度的组合 ═══
    print(f"\n{'='*50}")
    print("4. 寻找真正有区分度的组合（>5pp差异）")
    print(f"{'='*50}")

    # 4a. extreme_score × market_aligned × direction
    print(f"\n   极端分数 × 大盘协同 × 方向:")
    for es_min in [5, 6, 7]:
        for ma in [True, False]:
            for d in ['UP', 'DOWN']:
                recs = [r for r in records if r['extreme_score'] >= es_min
                        and r['market_aligned'] == ma and r['direction'] == d]
                a, n = acc(recs)
                ar = avg_ret(recs)
                if n >= 50:
                    ma_label = '协同' if ma else '独立'
                    print(f"   score>={es_min}+{ma_label}+{d}: 准确率{a:.1%} ({n}条), 收益{ar:+.2f}%")

    # 4b. composite_score × market_aligned
    print(f"\n   综合得分 × 大盘协同:")
    for cs_min in [0.15, 0.20, 0.25, 0.30]:
        for ma in [True, False]:
            recs = [r for r in records if abs(r['composite_score']) >= cs_min
                    and r['market_aligned'] == ma]
            a, n = acc(recs)
            ar = avg_ret(recs)
            if n >= 50:
                ma_label = '协同' if ma else '独立'
                print(f"   |score|>={cs_min}+{ma_label}: 准确率{a:.1%} ({n}条), 收益{ar:+.2f}%")

    # 4c. 大盘环境 × 方向（最重要的交互）
    print(f"\n   大盘环境 × 方向:")
    for lo, hi, label in [(-999, -3, '暴跌'), (-3, -1, '小跌'), (-1, 1, '震荡'),
                           (1, 3, '小涨'), (3, 999, '大涨')]:
        for d in ['UP', 'DOWN']:
            recs = [r for r in records if lo <= r['mkt_pct'] < hi and r['direction'] == d]
            a, n = acc(recs)
            ar = avg_ret(recs)
            if n >= 20:
                print(f"   {label}+{d}: 准确率{a:.1%} ({n}条), 收益{ar:+.2f}%")

    # ═══ 5. 理想置信度分级（基于数据发现） ═══
    print(f"\n{'='*50}")
    print("5. 理想置信度分级探索")
    print(f"{'='*50}")

    # 基于上面的分析，尝试不同的分级标准
    # 方案A: 仅用extreme_score + market_aligned
    print(f"\n   方案A: extreme_score + market_aligned")
    tier_a_high = [r for r in records if r['extreme_score'] >= 6 and r['market_aligned']]
    tier_a_med = [r for r in records if (r['extreme_score'] >= 6 and not r['market_aligned'])
                  or (r['extreme_score'] == 5 and r['market_aligned'])]
    tier_a_low = [r for r in records if r['extreme_score'] == 5 and not r['market_aligned']]
    for label, recs in [('high', tier_a_high), ('medium', tier_a_med), ('low', tier_a_low)]:
        a, n = acc(recs)
        ar = avg_ret(recs)
        if n > 0:
            print(f"   {label:8s}: 准确率{a:.1%} ({n}条), 收益{ar:+.2f}%")

    # 方案B: composite_score强度 + market_aligned
    print(f"\n   方案B: |composite_score| + market_aligned")
    tier_b_high = [r for r in records if abs(r['composite_score']) >= 0.25 and r['market_aligned']]
    tier_b_med = [r for r in records if (abs(r['composite_score']) >= 0.25 and not r['market_aligned'])
                  or (abs(r['composite_score']) >= 0.15 and r['market_aligned'])]
    tier_b_low = [r for r in records if abs(r['composite_score']) < 0.15
                  or (abs(r['composite_score']) < 0.25 and not r['market_aligned'])]
    for label, recs in [('high', tier_b_high), ('medium', tier_b_med), ('low', tier_b_low)]:
        a, n = acc(recs)
        ar = avg_ret(recs)
        if n > 0:
            print(f"   {label:8s}: 准确率{a:.1%} ({n}条), 收益{ar:+.2f}%")

    # 方案C: UP方向 + extreme_score>=6 + market_aligned (最严格)
    print(f"\n   方案C: 方向 + extreme_score + market_aligned")
    tier_c_high = [r for r in records if r['direction'] == 'UP'
                   and r['extreme_score'] >= 6 and r['market_aligned']]
    tier_c_med = [r for r in records if r['direction'] == 'UP'
                  and (r['extreme_score'] >= 5) and not (r['extreme_score'] >= 6 and r['market_aligned'])]
    tier_c_low = [r for r in records if r['direction'] == 'DOWN']
    for label, recs in [('high(UP+6+协同)', tier_c_high), ('medium(UP其他)', tier_c_med),
                         ('low(DOWN)', tier_c_low)]:
        a, n = acc(recs)
        ar = avg_ret(recs)
        if n > 0:
            print(f"   {label:20s}: 准确率{a:.1%} ({n}条), 收益{ar:+.2f}%")

    # ═══ 6. 时间稳定性检验（关键） ═══
    print(f"\n{'='*50}")
    print("6. 时间稳定性检验（5段滚动）")
    print(f"{'='*50}")

    n_segments = 5
    seg_size = len(sorted_weeks) // n_segments
    for i in range(n_segments):
        seg_weeks = set(sorted_weeks[i*seg_size:(i+1)*seg_size])
        seg_recs = [r for r in records if r['week'] in seg_weeks]
        a, n = acc(seg_recs)
        # high confidence
        seg_high = [r for r in seg_recs if r['confidence'] == 'high']
        ah, nh = acc(seg_high)
        # UP + market_aligned
        seg_up_ma = [r for r in seg_recs if r['direction'] == 'UP' and r['market_aligned']]
        aum, num = acc(seg_up_ma)
        print(f"   段{i+1} ({sorted_weeks[i*seg_size]}~{sorted_weeks[min((i+1)*seg_size-1, len(sorted_weeks)-1)]}):")
        print(f"     全部: {a:.1%} ({n}条) | high: {ah:.1%} ({nh}条) | UP+协同: {aum:.1%} ({num}条)")

    # ═══ 7. 周度准确率分布 ═══
    print(f"\n{'='*50}")
    print("7. 周度准确率分布")
    print(f"{'='*50}")

    week_accs = []
    for wk in sorted_weeks:
        wk_recs = [r for r in records if r['week'] == wk]
        if len(wk_recs) >= 5:
            a, n = acc(wk_recs)
            week_accs.append(a)

    if week_accs:
        avg_wa = sum(week_accs) / len(week_accs)
        std_wa = (sum((a - avg_wa)**2 for a in week_accs) / len(week_accs)) ** 0.5
        above_60 = sum(1 for a in week_accs if a >= 0.6)
        above_50 = sum(1 for a in week_accs if a >= 0.5)
        below_30 = sum(1 for a in week_accs if a < 0.3)
        print(f"   周数: {len(week_accs)}")
        print(f"   平均: {avg_wa:.1%}, 标准差: {std_wa:.1%}")
        print(f"   >60%的周: {above_60}/{len(week_accs)} ({above_60/len(week_accs):.0%})")
        print(f"   >50%的周: {above_50}/{len(week_accs)} ({above_50/len(week_accs):.0%})")
        print(f"   <30%的周: {below_30}/{len(week_accs)} ({below_30/len(week_accs):.0%})")

        # 按大盘环境分类的周度准确率
        print(f"\n   按大盘环境的周度准确率分布:")
        for lo, hi, label in [(-999, -3, '暴跌'), (-3, -1, '小跌'), (-1, 1, '震荡'),
                               (1, 3, '小涨'), (3, 999, '大涨')]:
            env_weeks = []
            for wk in sorted_weeks:
                mp = mkt_week_pct.get(wk, 0)
                if lo <= mp < hi:
                    wk_recs = [r for r in records if r['week'] == wk]
                    if len(wk_recs) >= 5:
                        a, n = acc(wk_recs)
                        env_weeks.append(a)
            if env_weeks:
                avg_e = sum(env_weeks) / len(env_weeks)
                std_e = (sum((a - avg_e)**2 for a in env_weeks) / len(env_weeks)) ** 0.5
                print(f"   {label:6s}: 平均{avg_e:.1%}, 标准差{std_e:.1%}, {len(env_weeks)}周")

    print(f"\n⏱️  耗时: {time.time() - t0:.1f}秒")
    print("=" * 70)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--weeks', type=int, default=100)
    parser.add_argument('--stocks', type=int, default=200)
    args = parser.parse_args()
    run_analysis(n_weeks=args.weeks, n_stocks=args.stocks)
