#!/usr/bin/env python3
"""
V30 情绪因子回测工具
====================
用法：
    source .venv/bin/activate
    python -m tools.run_v30_backtest
"""
import json, logging, sys, time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from service.v30_prediction.v30_engine import (
    V30Engine, compute_sentiment_factors, compute_technical_factors,
    compute_meta, vote_factors, build_factor_stats, _f,
    SENT_MIN, TECH_MIN, MKT_LOWER, MKT_UPPER, PRICE_MAX, TURN_MAX, SKEW_LO, SKEW_HI,
)
from dao import get_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)
OUTPUT_DIR = Path(__file__).parent.parent / "data_results"


def load_codes(limit=300):
    conn = get_connection(use_dict_cursor=True); cur = conn.cursor()
    cur.execute("SELECT DISTINCT stock_code FROM stock_kline "
                "WHERE stock_code NOT LIKE '4%%' AND stock_code NOT LIKE '8%%' "
                "AND stock_code NOT LIKE '9%%' ORDER BY stock_code LIMIT %s", (limit,))
    codes = [r['stock_code'] for r in cur.fetchall()]
    cur.close(); conn.close(); return codes

def load_klines(codes, start, end):
    conn = get_connection(use_dict_cursor=True); cur = conn.cursor()
    res = defaultdict(list)
    for i in range(0, len(codes), 300):
        batch = codes[i:i+300]; ph = ','.join(['%s']*len(batch))
        cur.execute(f"SELECT stock_code,`date`,close_price,open_price,high_price,"
                    f"low_price,trading_volume,change_percent,change_hand "
                    f"FROM stock_kline WHERE stock_code IN ({ph}) "
                    f"AND `date`>=%s AND `date`<=%s ORDER BY `date`", batch+[start,end])
        for r in cur.fetchall():
            res[r['stock_code']].append({
                'd': str(r['date']), 'c': _f(r['close_price']), 'o': _f(r['open_price']),
                'h': _f(r['high_price']), 'l': _f(r['low_price']), 'v': _f(r['trading_volume']),
                'p': _f(r['change_percent']), 't': _f(r.get('change_hand')),
            })
    cur.close(); conn.close(); return dict(res)

def load_ff(codes, start):
    conn = get_connection(use_dict_cursor=True); cur = conn.cursor()
    res = defaultdict(list)
    for i in range(0, len(codes), 300):
        batch = codes[i:i+300]; ph = ','.join(['%s']*len(batch))
        cur.execute(f"SELECT stock_code,`date`,big_net_pct,small_net_pct,net_flow "
                    f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
                    f"AND `date`>=%s ORDER BY `date`", batch+[start])
        for r in cur.fetchall():
            res[r['stock_code']].append({
                'd': str(r['date']), 'bn': _f(r.get('big_net_pct')),
                'sn': _f(r.get('small_net_pct')), 'nf': _f(r.get('net_flow')),
            })
    cur.close(); conn.close(); return dict(res)

def load_market(start, end):
    conn = get_connection(use_dict_cursor=True); cur = conn.cursor()
    cur.execute("SELECT `date`,close_price,change_percent FROM stock_kline "
                "WHERE stock_code='000001.SH' AND `date`>=%s AND `date`<=%s ORDER BY `date`", (start,end))
    rows = cur.fetchall(); cur.close(); conn.close()
    return {str(r['date']): {'c': _f(r['close_price']), 'p': _f(r['change_percent'])} for r in rows}

def calc_stats(preds):
    if not preds: return None
    n = len(preds)
    correct = sum(1 for _, r in preds if r > 0)
    wins = [r for _, r in preds if r > 0]
    losses = [abs(r) for _, r in preds if r <= 0]
    aw = sum(wins)/len(wins) if wins else 0
    al = sum(losses)/len(losses) if losses else 0
    return {'n': n, 'acc': round(correct/n, 4),
            'pnl': round(sum(r for _, r in preds)/n, 4),
            'aw': round(aw, 4), 'al': round(al, 4),
            'plr': round(aw/al, 2) if al > 0 else 999}


def run_backtest():
    t0 = time.time()
    print("=" * 70)
    print("V30 情绪因子回测")
    print("=" * 70)

    logger.info("[1/4] 加载数据...")
    codes = load_codes(300)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=500)).strftime('%Y-%m-%d')
    kdata = load_klines(codes, start_date, end_date)
    ffdata = load_ff(codes, start_date)
    mkt_by_date = load_market(start_date, end_date)
    logger.info("  K线: %d只, 资金流: %d只, 大盘: %d天", len(kdata), len(ffdata), len(mkt_by_date))

    # ── 训练V30引擎 ──
    logger.info("[2/4] 训练V30引擎...")
    engine = V30Engine()
    engine.train(kdata, ffdata, mkt_by_date)

    # ── Walk-Forward回测 ──
    logger.info("[3/4] Walk-Forward回测...")
    wf_preds = []
    wf_monthly = defaultdict(list)
    wf_by_conf = defaultdict(list)
    wf_retrain = 0

    for code, klines in kdata.items():
        if len(klines) < 130: continue
        ff_bd = {f['d']: f for f in ffdata.get(code, [])}
        local_ss, local_ts = None, None
        last_train = -999

        for i in range(120, len(klines) - 10):
            # 每60天重训练
            if i - last_train >= 60 or local_ss is None:
                ts_idx = max(0, i - 120)
                ls = defaultdict(list); lt = defaultdict(list)
                for ti in range(max(60, ts_idx), i):
                    sf = compute_sentiment_factors(klines[:ti+1], ff_bd)
                    tf = compute_technical_factors(klines[:ti+1], mkt_by_date)
                    if not sf or not tf: continue
                    base = klines[ti]['c']
                    if base <= 0 or ti+5 >= len(klines) or klines[ti+5]['c'] <= 0: continue
                    ret = (klines[ti+5]['c']/base-1)*100
                    for fn, fv in sf.items():
                        if fv is not None: ls[fn].append((fv, ret))
                    for fn, fv in tf.items():
                        if fv is not None: lt[fn].append((fv, ret))
                local_ss = build_factor_stats(ls, min_n=30)
                local_ts = build_factor_stats(lt, min_n=30)
                last_train = i; wf_retrain += 1

            if not local_ss or not local_ts: continue

            sf = compute_sentiment_factors(klines[:i+1], ff_bd)
            tf = compute_technical_factors(klines[:i+1], mkt_by_date)
            if not sf or not tf: continue

            meta = compute_meta(klines[:i+1], sf)
            mkt_c = [mkt_by_date.get(klines[j]['d'], {}).get('c', 0) for j in range(max(0,i-20), i+1)]
            mkt_c = [c for c in mkt_c if c > 0]
            mkt_ret = (mkt_c[-1]/mkt_c[0]-1)*100 if len(mkt_c) >= 2 else 0

            # 规则过滤
            if mkt_ret < MKT_LOWER or mkt_ret > MKT_UPPER: continue
            if meta['price'] > PRICE_MAX: continue
            if meta['avg_turn'] > TURN_MAX: continue
            skew = meta.get('skew_20')
            if skew is not None and (skew < SKEW_LO or skew > SKEW_HI): continue

            s_up, s_dn, s_ws, s_wt = vote_factors(sf, local_ss, 13)
            t_up, t_dn, t_ws, t_wt = vote_factors(tf, local_ts, 13)
            if s_up < SENT_MIN or s_up <= s_dn: continue
            if t_up < TECH_MIN or t_up <= t_dn: continue

            base = klines[i]['c']
            if base <= 0 or i+5 >= len(klines) or klines[i+5]['c'] <= 0: continue
            ret_5d = (klines[i+5]['c']/base-1)*100

            s_conf = s_ws/s_wt if s_wt > 0 else 0
            t_conf = t_ws/t_wt if t_wt > 0 else 0
            combined = s_conf * 0.6 + t_conf * 0.4
            conf = 'high' if combined >= 0.75 and s_up >= 12 else 'medium' if combined >= 0.59 else 'low'

            wf_preds.append(('UP', ret_5d))
            wf_monthly[klines[i]['d'][:7]].append(('UP', ret_5d))
            wf_by_conf[conf].append(('UP', ret_5d))

    # ── 输出结果 ──
    logger.info("[4/4] 输出结果...")
    wf_s = calc_stats(wf_preds)

    print(f"\n{'═' * 70}")
    print(f"📊 V30 Walk-Forward回测结果")
    print(f"{'═' * 70}")
    if wf_s:
        print(f"  5日准确率: {wf_s['acc']:.1%} ({wf_s['n']}样本)")
        print(f"  期望收益:  {wf_s['pnl']:+.3f}%/5天")
        print(f"  盈亏比:    {wf_s['plr']:.2f}")
        print(f"  重训练:    {wf_retrain}次")

    print(f"\n  按置信度:")
    for conf in ['high', 'medium', 'low']:
        cs = calc_stats(wf_by_conf.get(conf, []))
        if cs and cs['n'] >= 3:
            print(f"    {conf}: 准确率={cs['acc']:.1%}, 期望={cs['pnl']:+.3f}%, n={cs['n']}")

    print(f"\n  月度稳定性:")
    maccs = []
    for month in sorted(wf_monthly.keys()):
        ms = calc_stats(wf_monthly[month])
        if ms and ms['n'] >= 3:
            maccs.append(ms['acc'])
            print(f"    {month}: 准确率={ms['acc']:.1%}, n={ms['n']}, 期望={ms['pnl']:+.3f}%")
    if maccs:
        am = sum(maccs)/len(maccs)
        astd = (sum((a-am)**2 for a in maccs)/max(len(maccs)-1,1))**0.5
        print(f"    月均: {am:.1%} ± {astd:.1%}")

    # 保存
    report = {
        'meta': {'n_stocks': len(kdata), 'date_range': f'{start_date}~{end_date}',
                 'run_time': round(time.time()-t0, 1)},
        'walkforward': wf_s,
        'by_confidence': {c: calc_stats(p) for c, p in wf_by_conf.items()},
        'monthly': {m: calc_stats(p) for m, p in sorted(wf_monthly.items())},
    }
    out = OUTPUT_DIR / "v30_backtest.json"
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n⏱️  耗时: {time.time()-t0:.1f}秒")
    print(f"📁 报告: {out}")
    print("=" * 70)


if __name__ == '__main__':
    run_backtest()
