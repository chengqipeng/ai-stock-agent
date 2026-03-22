#!/usr/bin/env python3
"""
V12 学术质量因子交叉分析
========================
深入分析三个学术质量因子的交叉效果，找到真正有效的组合。
重点关注：
1. 质量因子组合 vs 单因子效果
2. 大盘环境 × 质量因子的交互
3. UP vs DOWN方向 × 质量因子
4. 非暴跌周中质量因子的效果（关键：这才是真正需要改进的地方）
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
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


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
    logger.info("V12 学术质量因子交叉分析 (stocks=%d, weeks=%d)", n_stocks, n_weeks)

    stock_codes = load_stock_codes(n_stocks)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=n_weeks * 7 + 120)).strftime('%Y-%m-%d')

    kline_data = load_kline_data(stock_codes, start_date, end_date)
    fund_flow_data = load_fund_flow_data(stock_codes, start_date)
    market_klines = load_market_klines(start_date, end_date)
    logger.info("数据加载完成: K线%d只, 资金流%d只", len(kline_data), len(fund_flow_data))

    # 大盘周度涨跌
    mkt_by_week = group_by_week(market_klines)
    mkt_week_pct = {}
    for wk, kls in mkt_by_week.items():
        mkt_week_pct[wk] = sum(k.get('change_percent', 0) or 0 for k in kls)

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
            wk = sorted_weeks[wi]
            nwk = sorted_weeks[wi + 1]
            wkls = weekly_groups[wk]
            nkls = weekly_groups[nwk]
            last_date = wkls[-1]['date']
            idx = None
            for j, k in enumerate(klines):
                if k['date'] == last_date:
                    idx = j; break
            if idx is None or idx < 60:
                continue
            bc = wkls[-1].get('close', 0)
            ec = nkls[-1].get('close', 0)
            if bc <= 0 or ec <= 0:
                continue
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

    # 收集所有预测记录
    records = []
    for week_key in sorted_weeks:
        engine = V12PredictionEngine()
        # 截面中位数
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
            records.append({
                'week': week_key, 'code': code,
                'direction': pred['pred_direction'],
                'confidence': pred['confidence'],
                'extreme_score': pred['extreme_score'],
                'market_aligned': pred.get('market_aligned', False),
                'volume_confirmed': pred.get('volume_confirmed', False),
                'low_turnover_boost': pred.get('low_turnover_boost', False),
                'low_salience': pred.get('low_salience', False),
                'ivol_penalty': pred.get('ivol_penalty', False),
                'actual_return': actual,
                'is_correct': is_correct,
                'mkt_pct': mkt_pct,
                'composite_score': pred.get('composite_score', 0),
            })

    logger.info("收集到 %d 条预测记录", len(records))

    # ── 分析 ──
    def acc(recs):
        if not recs: return 0, 0
        c = sum(1 for r in recs if r['is_correct'])
        return round(c / len(recs), 4), len(recs)

    # 1. 大盘环境分类
    print("\n" + "=" * 70)
    print("V12 学术质量因子交叉分析")
    print("=" * 70)
    print(f"\n总计: {len(records)} 条预测")

    # 大盘环境分桶
    crash_weeks = [r for r in records if r['mkt_pct'] < -3]
    down_weeks = [r for r in records if -3 <= r['mkt_pct'] < -1]
    flat_weeks = [r for r in records if -1 <= r['mkt_pct'] < 1]
    up_weeks = [r for r in records if 1 <= r['mkt_pct'] < 3]
    rally_weeks = [r for r in records if r['mkt_pct'] >= 3]

    print(f"\n📊 按大盘环境:")
    for name, recs in [('暴跌(<-3%)', crash_weeks), ('小跌(-3~-1%)', down_weeks),
                        ('震荡(-1~1%)', flat_weeks), ('小涨(1~3%)', up_weeks),
                        ('大涨(>3%)', rally_weeks)]:
        a, n = acc(recs)
        print(f"   {name:15s}: {a:.1%} ({n}条)")

    # 2. 非暴跌周中的质量因子效果（关键分析）
    normal_weeks = [r for r in records if -3 <= r['mkt_pct'] <= 3]
    print(f"\n📈 非暴跌周 ({len(normal_weeks)}条) 中的质量因子效果:")

    # 质量因子组合
    combos = {
        'all_good (缩量+低换手+高显著)': [r for r in normal_weeks
            if r['volume_confirmed'] and r['low_turnover_boost'] and not r['low_salience']],
        '缩量+低换手': [r for r in normal_weeks
            if r['volume_confirmed'] and r['low_turnover_boost']],
        '缩量+高显著': [r for r in normal_weeks
            if r['volume_confirmed'] and not r['low_salience']],
        '低换手+高显著': [r for r in normal_weeks
            if r['low_turnover_boost'] and not r['low_salience']],
        '仅缩量': [r for r in normal_weeks if r['volume_confirmed']],
        '仅低换手': [r for r in normal_weeks if r['low_turnover_boost']],
        '仅高显著': [r for r in normal_weeks if not r['low_salience']],
        '无任何质量因子': [r for r in normal_weeks
            if not r['volume_confirmed'] and not r['low_turnover_boost'] and r['low_salience']],
        '全部非暴跌周': normal_weeks,
    }
    for name, recs in combos.items():
        a, n = acc(recs)
        if n > 0:
            print(f"   {name:35s}: {a:.1%} ({n}条)")

    # 3. UP方向 × 质量因子（最重要的组合）
    up_normal = [r for r in normal_weeks if r['direction'] == 'UP']
    print(f"\n🔼 UP方向 + 非暴跌周 ({len(up_normal)}条):")
    up_combos = {
        'UP+缩量+低换手+高显著': [r for r in up_normal
            if r['volume_confirmed'] and r['low_turnover_boost'] and not r['low_salience']],
        'UP+缩量+低换手': [r for r in up_normal
            if r['volume_confirmed'] and r['low_turnover_boost']],
        'UP+低换手+高显著': [r for r in up_normal
            if r['low_turnover_boost'] and not r['low_salience']],
        'UP+大盘协同+高显著': [r for r in up_normal
            if r['market_aligned'] and not r['low_salience']],
        'UP+大盘协同+低换手': [r for r in up_normal
            if r['market_aligned'] and r['low_turnover_boost']],
        'UP+大盘协同': [r for r in up_normal if r['market_aligned']],
        'UP+非IVOL惩罚': [r for r in up_normal if not r['ivol_penalty']],
        'UP全部': up_normal,
    }
    for name, recs in up_combos.items():
        a, n = acc(recs)
        if n > 0:
            print(f"   {name:35s}: {a:.1%} ({n}条)")

    # 4. 按extreme_score × 质量因子
    print(f"\n🔥 极端分数 × 质量因子:")
    for es_min in [5, 6, 7, 8]:
        es_recs = [r for r in records if r['extreme_score'] >= es_min]
        es_good = [r for r in es_recs if not r['low_salience'] and r['market_aligned']]
        a1, n1 = acc(es_recs)
        a2, n2 = acc(es_good)
        print(f"   score>={es_min}: 全部{a1:.1%}({n1}条) | +高显著+大盘协同{a2:.1%}({n2}条)")

    # 5. 按置信度 × 方向
    print(f"\n📊 置信度 × 方向:")
    for conf in ['high', 'medium', 'low']:
        for d in ['UP', 'DOWN']:
            recs = [r for r in records if r['confidence'] == conf and r['direction'] == d]
            a, n = acc(recs)
            if n > 0:
                print(f"   {conf:8s} {d:5s}: {a:.1%} ({n}条)")

    # 6. 收益分析
    print(f"\n💰 收益分析:")
    for conf in ['high', 'medium', 'low']:
        recs = [r for r in records if r['confidence'] == conf]
        if recs:
            rets = [r['actual_return'] for r in recs]
            # 方向调整收益（做对方向的收益）
            dir_rets = [r['actual_return'] if r['direction'] == 'UP' else -r['actual_return'] for r in recs]
            avg_dir = sum(dir_rets) / len(dir_rets)
            gains = [r for r in dir_rets if r > 0]
            losses = [r for r in dir_rets if r < 0]
            if gains and losses:
                pl_ratio = (sum(gains)/len(gains)) / (abs(sum(losses)/len(losses)))
            else:
                pl_ratio = 0
            print(f"   {conf:8s}: 方向调整平均收益{avg_dir:+.2f}%, 盈亏比{pl_ratio:.2f}")

    # 7. 时间稳定性：前半段 vs 后半段
    mid = len(sorted_weeks) // 2
    first_half_weeks = set(sorted_weeks[:mid])
    second_half_weeks = set(sorted_weeks[mid:])
    first_recs = [r for r in records if r['week'] in first_half_weeks]
    second_recs = [r for r in records if r['week'] in second_half_weeks]
    a1, n1 = acc(first_recs)
    a2, n2 = acc(second_recs)
    print(f"\n⏰ 时间稳定性:")
    print(f"   前半段: {a1:.1%} ({n1}条)")
    print(f"   后半段: {a2:.1%} ({n2}条)")

    # high confidence
    h1 = [r for r in first_recs if r['confidence'] == 'high']
    h2 = [r for r in second_recs if r['confidence'] == 'high']
    a1h, n1h = acc(h1)
    a2h, n2h = acc(h2)
    print(f"   前半段 high: {a1h:.1%} ({n1h}条)")
    print(f"   后半段 high: {a2h:.1%} ({n2h}条)")

    # 保存
    report = {
        'total_records': len(records),
        'run_time_sec': round(time.time() - t0, 1),
    }
    output_path = OUTPUT_DIR / "v12_quality_factor_analysis.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"\n⏱️  耗时: {time.time() - t0:.1f}秒")
    print("=" * 70)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--weeks', type=int, default=100)
    parser.add_argument('--stocks', type=int, default=5000)
    args = parser.parse_args()
    run_analysis(n_weeks=args.weeks, n_stocks=args.stocks)
