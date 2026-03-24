#!/usr/bin/env python3
"""
情绪因子 v4c — 高一致性子集精细分析
====================================
v4b发现：
  - sent_agree=11: 65.3% (75样本)
  - sent_agree=10: 63.8% (378样本)
  - sent_agree≥9:  ~62%+ (1168样本)

v4c策略：
  - 提高情绪因子一致性要求（sent_agree≥9或≥10）
  - 同时要求技术面确认
  - 更细粒度的参数搜索
  - 交叉验证：sent_agree × conf × mkt_filter

用法：
    source .venv/bin/activate
    python -m tools.backtest_sentiment_v4c
"""
import json, logging, sys, time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dao import get_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)
OUTPUT_DIR = Path(__file__).parent.parent / "data_results"

def _f(v):
    try: return float(v) if v is not None else 0.0
    except: return 0.0

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
                    f"low_price,trading_volume,change_percent,change_hand,amplitude "
                    f"FROM stock_kline WHERE stock_code IN ({ph}) "
                    f"AND `date`>=%s AND `date`<=%s ORDER BY `date`", batch+[start,end])
        for r in cur.fetchall():
            res[r['stock_code']].append({
                'd': str(r['date']), 'c': _f(r['close_price']), 'o': _f(r['open_price']),
                'h': _f(r['high_price']), 'l': _f(r['low_price']), 'v': _f(r['trading_volume']),
                'p': _f(r['change_percent']), 't': _f(r.get('change_hand')), 'a': _f(r.get('amplitude')),
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
    return [{'d': str(r['date']), 'c': _f(r['close_price']), 'p': _f(r['change_percent'])} for r in rows]


def compute_all_factors(klines, ff_by_date=None, mkt_by_date=None):
    """计算全部因子，返回 (sentiment_factors, technical_factors)"""
    n = len(klines)
    if n < 60: return None, None
    close = [k['c'] for k in klines]
    open_ = [k['o'] for k in klines]
    high = [k['h'] for k in klines]
    low = [k['l'] for k in klines]
    vol = [k['v'] for k in klines]
    pct = [k['p'] for k in klines]
    turn = [k['t'] for k in klines]
    amp = [k.get('a', 0) or 0 for k in klines]
    if close[-1] <= 0 or vol[-1] <= 0: return None, None

    sf, tf = {}, {}

    # ════ 情绪因子 ════
    rp = pct[-20:]; m = sum(rp)/20; s = (sum((p-m)**2 for p in rp)/19)**0.5
    if s > 0: sf['skew_20'] = sum((p-m)**3 for p in rp)/(20*s**3)

    c60 = [c for c in close[-60:] if c > 0]
    if c60:
        h60, l60 = max(c60), min(c60)
        if h60 > l60: sf['price_pos'] = (close[-1]-l60)/(h60-l60)

    ma20 = sum(close[-20:])/20
    if ma20 > 0: sf['bias_20'] = (close[-1]/ma20-1)*100

    iv, ov = [], []
    for i in range(-10, 0):
        idx = n+i
        if idx > 0 and close[idx-1] > 0:
            iv.append((high[idx]-low[idx])/close[idx-1]*100)
            ov.append(abs(open_[idx]-close[idx-1])/close[idx-1]*100)
    if iv and ov:
        ai, ao = sum(iv)/len(iv), sum(ov)/len(ov)
        if ao > 0.01: sf['noise_ratio'] = ai/ao

    bu = sum(1 for p in pct[-10:] if p > 3)
    bd = sum(1 for p in pct[-10:] if p < -3)
    sf['big_move'] = (bu-bd)/10

    uv = [vol[n-10+i] for i in range(10) if pct[n-10+i] > 0 and vol[n-10+i] > 0]
    dv = [vol[n-10+i] for i in range(10) if pct[n-10+i] < 0 and vol[n-10+i] > 0]
    if uv and dv: sf['vol_asym'] = (sum(uv)/len(uv))/(sum(dv)/len(dv))

    co = [(k['c']-k['o'])/k['o']*100 for k in klines[-5:] if k['o'] > 0]
    if co: sf['co_5d'] = sum(co)/len(co)

    if close[-2] > 0: sf['limit_prox'] = (close[-1]-close[-2]*0.9)/(close[-2]*0.2)

    if n >= 15 and close[-6] > 0 and close[-11] > 0:
        sf['price_accel'] = (close[-1]/close[-6]-1)*100-(close[-6]/close[-11]-1)*100

    us = []
    for k in klines[-5:]:
        hl = k['h']-k['l']
        if hl > 0: us.append((k['h']-max(k['c'],k['o']))/hl)
    if us: sf['upper_shd'] = sum(us)/len(us)

    t20 = [t for t in turn[-20:] if t > 0]
    if t20 and turn[-1] > 0:
        at20 = sum(t20)/len(t20)
        if at20 > 0: sf['turn_spike'] = turn[-1]/at20

    dvol = sum(vol[n-5+i] for i in range(5) if pct[n-5+i] < 0)
    tvol = sum(vol[-5:])
    if tvol > 0: sf['down_vol_r'] = dvol/tvol

    cps = []
    for k in klines[-5:]:
        hl = k['h']-k['l']
        if hl > 0: cps.append((k['c']-k['l'])/hl)
    if cps: sf['close_pos'] = sum(cps)/len(cps)

    if ff_by_date:
        ff_r = [ff_by_date[k['d']] for k in klines[-5:] if k['d'] in ff_by_date]
        if len(ff_r) >= 3:
            sf['small_net'] = sum(x['sn'] for x in ff_r)/len(ff_r)
            sf['big_net'] = sum(x['bn'] for x in ff_r)/len(ff_r)

    # ════ 技术因子 ════
    c20, v20 = close[-20:], vol[-20:]
    mc, mv = sum(c20)/20, sum(v20)/20
    cov = sum((c20[i]-mc)*(v20[i]-mv) for i in range(20))
    sc = sum((c-mc)**2 for c in c20)**0.5
    sv = sum((v-mv)**2 for v in v20)**0.5
    if sc > 0 and sv > 0: tf['vp_corr'] = cov/(sc*sv)

    if close[-21] > 0: tf['mom_20'] = (close[-1]/close[-21]-1)*100

    if mkt_by_date:
        stock_r = sum(pct[-20:])
        mkt_r = sum(mkt_by_date.get(klines[i]['d'], {}).get('p', 0) for i in range(max(0,n-20), n))
        tf['rel_str'] = stock_r - mkt_r

    ma5 = sum(close[-5:])/5
    if ma20 > 0: tf['ma_cross'] = (ma5/ma20-1)*100

    if mkt_by_date:
        mkt_c = [mkt_by_date.get(klines[i]['d'], {}).get('c', 0) for i in range(max(0,n-20), n)]
        mkt_c = [c for c in mkt_c if c > 0]
        if len(mkt_c) >= 2: tf['mkt_trend'] = (mkt_c[-1]/mkt_c[0]-1)*100

    if n >= 15:
        gains = [max(0, close[i]-close[i-1]) for i in range(n-14, n) if close[i-1] > 0]
        losses = [max(0, close[i-1]-close[i]) for i in range(n-14, n) if close[i-1] > 0]
        if gains and losses:
            ag, al = sum(gains)/14, sum(losses)/14
            tf['rsi_14'] = 100-100/(1+ag/al) if al > 0 else 100

    if n >= 61 and close[-61] > 0: tf['mom_60'] = (close[-1]/close[-61]-1)*100
    if close[-6] > 0: tf['rev_5'] = (close[-1]/close[-6]-1)*100

    t20v = [t for t in turn[-20:] if t > 0]
    if t20v: tf['turn_20'] = sum(t20v)/len(t20v)

    tf['vol_20'] = s

    v5 = sum(vol[-5:])/5; v20a = sum(vol[-20:])/20
    if v20a > 0: tf['vol_ratio'] = v5/v20a

    trs = []
    for i in range(-14, 0):
        idx = n+i
        if idx > 0: trs.append(max(high[idx]-low[idx], abs(high[idx]-close[idx-1]), abs(low[idx]-close[idx-1])))
    if trs and close[-1] > 0: tf['atr_pct'] = (sum(trs)/len(trs))/close[-1]*100

    cd = cu = 0
    for p in reversed(pct):
        if p < 0: cd += 1
        else: break
    for p in reversed(pct):
        if p > 0: cu += 1
        else: break
    tf['consec'] = cu - cd

    return sf, tf


def build_stats(records):
    stats = {}
    for fn, data in records.items():
        vals = [r[0] for r in data]; rets = [r[1] for r in data]
        nn = len(vals)
        if nn < 200: continue
        m = sum(vals)/nn; s = (sum((v-m)**2 for v in vals)/(nn-1))**0.5
        if s < 1e-10: continue
        fi = sorted(range(nn), key=lambda i: vals[i])
        ri = sorted(range(nn), key=lambda i: rets[i])
        fr, rr = [0]*nn, [0]*nn
        for rank, idx in enumerate(fi): fr[idx] = rank
        for rank, idx in enumerate(ri): rr[idx] = rank
        mf, mr = sum(fr)/nn, sum(rr)/nn
        cov = sum((fr[i]-mf)*(rr[i]-mr) for i in range(nn))
        sf = sum((fr[i]-mf)**2 for i in range(nn))**0.5
        sr = sum((rr[i]-mr)**2 for i in range(nn))**0.5
        ic = cov/(sf*sr) if sf > 0 and sr > 0 else 0
        stats[fn] = {'m': m, 's': s, 'ic': ic, 'aic': abs(ic), 'dir': -1 if ic < 0 else 1, 'n': nn}
    return stats


def vote_detail(factors, fstats, top_k=12):
    """投票预测，返回详细信息: (n_up, n_down, weighted_score, total_weight)"""
    ranked = sorted(fstats.items(), key=lambda x: x[1]['aic'], reverse=True)
    top = [(fn, fs) for fn, fs in ranked[:top_k] if fn in factors]
    up = dn = 0
    ws = wt = 0.0
    for fn, fs in top:
        z = (factors[fn]-fs['m'])/fs['s'] * fs['dir']
        if z > 0.2: up += 1
        elif z < -0.2: dn += 1
        ws += z * fs['aic']; wt += fs['aic']
    return up, dn, ws, wt


def compute_future(klines, idx):
    base = klines[idx]['c']
    if base <= 0: return {}
    r = {}
    for h in (5, 10):
        if idx+h < len(klines) and klines[idx+h]['c'] > 0:
            r[f'ret_{h}d'] = round((klines[idx+h]['c']/base-1)*100, 4)
    return r


def calc_stats(preds):
    if not preds: return None
    n = len(preds)
    correct = sum(1 for d, r in preds if (d == 'UP' and r > 0) or (d == 'DOWN' and r < 0))
    wins = [abs(r if d == 'UP' else -r) for d, r in preds if (r if d == 'UP' else -r) > 0]
    losses = [abs(r if d == 'UP' else -r) for d, r in preds if (r if d == 'UP' else -r) <= 0]
    aw = sum(wins)/len(wins) if wins else 0
    al = sum(losses)/len(losses) if losses else 0
    return {'n': n, 'acc': round(correct/n, 4),
            'pnl': round(sum(r if d == 'UP' else -r for d, r in preds)/n, 4),
            'aw': round(aw, 4), 'al': round(al, 4),
            'plr': round(aw/al, 2) if al > 0 else 'inf'}


def run_backtest():
    t0 = time.time()
    print("=" * 80)
    print("情绪因子 v4c — 高一致性子集精细分析")
    print("=" * 80)

    logger.info("[1/5] 加载数据...")
    codes = load_codes(300)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=500)).strftime('%Y-%m-%d')
    kdata = load_klines(codes, start_date, end_date)
    ffdata = load_ff(codes, start_date)
    mkt = load_market(start_date, end_date)
    mkt_by_date = {m['d']: m for m in mkt}
    logger.info("  K线: %d只, 资金流: %d只, 大盘: %d天", len(kdata), len(ffdata), len(mkt))

    # ── 训练期(40%) ──
    logger.info("[2/5] 训练期...")
    s_rec = defaultdict(list); t_rec = defaultdict(list); train_n = 0
    for code, klines in kdata.items():
        if len(klines) < 80: continue
        ff_bd = {f['d']: f for f in ffdata.get(code, [])}
        te = int(len(klines)*0.4)
        for i in range(60, min(te, len(klines)-10)):
            sf, tf = compute_all_factors(klines[:i+1], ff_bd, mkt_by_date)
            if not sf or not tf: continue
            fut = compute_future(klines, i)
            if 'ret_5d' not in fut: continue
            train_n += 1; ret = fut['ret_5d']
            for fn, fv in sf.items():
                if fv is not None: s_rec[fn].append((fv, ret))
            for fn, fv in tf.items():
                if fv is not None: t_rec[fn].append((fv, ret))

    ss = build_stats(s_rec); ts = build_stats(t_rec)
    logger.info("  训练: %d样本, 情绪: %d, 技术: %d", train_n, len(ss), len(ts))

    # ── 收集全部预测（验证+测试），用不同过滤条件分析 ──
    logger.info("[3/5] 收集验证期+测试期全部预测...")

    # 收集所有可能的预测点（不做任何过滤）
    all_predictions = []  # [{phase, code, date, s_up, s_dn, s_conf, t_up, t_dn, t_conf, mkt_ret, ret_5d, ret_10d}]

    for code, klines in kdata.items():
        if len(klines) < 80: continue
        ff_bd = {f['d']: f for f in ffdata.get(code, [])}
        te = int(len(klines)*0.4)
        ve = int(len(klines)*0.7)

        for i in range(max(60, te), len(klines)-10):
            sf, tf = compute_all_factors(klines[:i+1], ff_bd, mkt_by_date)
            if not sf or not tf: continue
            fut = compute_future(klines, i)
            if 'ret_5d' not in fut: continue

            # 情绪投票（用top_k=13，即全部情绪因子）
            s_up, s_dn, s_ws, s_wt = vote_detail(sf, ss, top_k=13)
            # 技术投票（用top_k=13，即全部技术因子）
            t_up, t_dn, t_ws, t_wt = vote_detail(tf, ts, top_k=13)

            s_conf = s_ws/s_wt if s_wt > 0 else 0
            t_conf = t_ws/t_wt if t_wt > 0 else 0

            # 大盘趋势
            mkt_c = [mkt_by_date.get(klines[j]['d'], {}).get('c', 0) for j in range(max(0,i-20), i+1)]
            mkt_c = [c for c in mkt_c if c > 0]
            mkt_ret = (mkt_c[-1]/mkt_c[0]-1)*100 if len(mkt_c) >= 2 else 0

            phase = 'val' if i < ve else 'test'
            all_predictions.append({
                'phase': phase, 'code': code, 'date': klines[i]['d'],
                's_up': s_up, 's_dn': s_dn, 's_conf': s_conf,
                't_up': t_up, 't_dn': t_dn, 't_conf': t_conf,
                'mkt_ret': mkt_ret,
                'ret_5d': fut['ret_5d'],
                'ret_10d': fut.get('ret_10d'),
            })

    val_preds = [p for p in all_predictions if p['phase'] == 'val']
    test_preds = [p for p in all_predictions if p['phase'] == 'test']
    logger.info("  验证期: %d, 测试期: %d", len(val_preds), len(test_preds))

    # ── 验证期：搜索最优过滤条件 ──
    logger.info("[4/5] 验证期 — 精细参数搜索...")

    # 过滤条件: (s_min_up, t_min_up, min_combined_conf, mkt_filter)
    # 只做UP预测: s_up >= s_min_up AND s_up > s_dn AND t_up >= t_min_up AND t_up > t_dn
    filter_params = []
    for s_min in range(5, 12):
        for t_min in range(2, 7):
            for cc in [0.0, 0.2, 0.3, 0.5]:
                for mf in [-3, -5, -99]:
                    filter_params.append((s_min, t_min, cc, mf))

    val_results = []
    for s_min, t_min, cc, mf in filter_params:
        preds = []
        for p in val_preds:
            if p['mkt_ret'] < mf: continue
            if p['s_up'] < s_min or p['s_up'] <= p['s_dn']: continue
            if p['t_up'] < t_min or p['t_up'] <= p['t_dn']: continue
            combined = p['s_conf']*0.6 + p['t_conf']*0.4
            if combined < cc: continue
            preds.append(('UP', p['ret_5d']))

        s = calc_stats(preds)
        if s and s['n'] >= 80:
            val_results.append({'p': (s_min, t_min, cc, mf), 's': s})

    val_results.sort(key=lambda x: x['s']['acc'], reverse=True)

    print(f"\n验证期Top 20（UP-only + 双确认）:")
    print(f"  {'s_min':>5s} {'t_min':>5s} {'conf':>5s} {'mkt':>4s} {'样本':>6s} {'准确率':>6s} {'期望':>8s} {'盈亏比':>6s}")
    for vr in val_results[:20]:
        p, s = vr['p'], vr['s']
        plr = f"{s['plr']:.2f}" if isinstance(s['plr'], (int, float)) else s['plr']
        print(f"  {p[0]:>5d} {p[1]:>5d} {p[2]:>5.1f} {p[3]:>4d} "
              f"{s['n']:>6d} {s['acc']:>6.1%} {s['pnl']:>+8.3f}% {plr:>6s}")

    if not val_results:
        print("❌ 无有效参数"); return

    # 选择最优参数（准确率最高且样本≥100）
    best = val_results[0]['p']
    # 也选一个样本更多的次优参数
    best_balanced = None
    for vr in val_results:
        if vr['s']['n'] >= 300 and vr['s']['acc'] >= 0.60:
            best_balanced = vr['p']
            break

    print(f"\n  最优参数: s_min={best[0]}, t_min={best[1]}, conf={best[2]}, mkt={best[3]}")
    if best_balanced:
        print(f"  平衡参数: s_min={best_balanced[0]}, t_min={best_balanced[1]}, "
              f"conf={best_balanced[2]}, mkt={best_balanced[3]}")


    # ── 测试期 ──
    logger.info("[5/5] 测试期评估...")

    def evaluate_on_test(params, label):
        s_min, t_min, cc, mf = params
        preds = []
        monthly = defaultdict(list)
        by_sn = defaultdict(list)
        details = []

        for p in test_preds:
            if p['mkt_ret'] < mf: continue
            if p['s_up'] < s_min or p['s_up'] <= p['s_dn']: continue
            if p['t_up'] < t_min or p['t_up'] <= p['t_dn']: continue
            combined = p['s_conf']*0.6 + p['t_conf']*0.4
            if combined < cc: continue

            preds.append(('UP', p['ret_5d']))
            monthly[p['date'][:7]].append(('UP', p['ret_5d']))
            by_sn[p['s_up']].append(('UP', p['ret_5d']))
            details.append(p)

        s = calc_stats(preds)
        print(f"\n{'═' * 80}")
        print(f"📊 {label}")
        print(f"{'═' * 80}")
        print(f"  参数: s_min={s_min}, t_min={t_min}, conf≥{cc}, mkt≥{mf}%")

        if not s:
            print("  ❌ 无预测样本"); return None, None

        plr = f"{s['plr']:.2f}" if isinstance(s['plr'], (int, float)) else s['plr']
        print(f"\n  5日准确率:  {s['acc']:.1%}  ({s['n']}样本)")
        print(f"  5日期望:    {s['pnl']:+.3f}%")
        print(f"  5日盈亏比:  {plr}")

        # 10日
        preds_10d = [('UP', p['ret_10d']) for p in details if p['ret_10d'] is not None]
        s10 = calc_stats(preds_10d)
        if s10:
            print(f"  10日准确率: {s10['acc']:.1%}  ({s10['n']}样本)")

        # 按情绪一致数
        print(f"\n  按情绪一致数:")
        for sn in sorted(by_sn.keys(), reverse=True):
            ss2 = calc_stats(by_sn[sn])
            if ss2 and ss2['n'] >= 10:
                plr2 = f"{ss2['plr']:.2f}" if isinstance(ss2['plr'], (int, float)) else ss2['plr']
                print(f"    sent={sn}: 准确率={ss2['acc']:.1%}, 期望={ss2['pnl']:+.3f}%, "
                      f"盈亏比={plr2}, n={ss2['n']}")

        # 累积式：从高一致性到低
        print(f"\n  累积式（sent_agree≥N）:")
        for min_sn in range(11, s_min-1, -1):
            subset = [('UP', p['ret_5d']) for p in details if p['s_up'] >= min_sn]
            ss2 = calc_stats(subset)
            if ss2 and ss2['n'] >= 20:
                plr2 = f"{ss2['plr']:.2f}" if isinstance(ss2['plr'], (int, float)) else ss2['plr']
                print(f"    sent≥{min_sn}: 准确率={ss2['acc']:.1%}, 期望={ss2['pnl']:+.3f}%, "
                      f"盈亏比={plr2}, n={ss2['n']}")

        # 月度
        print(f"\n  月度稳定性:")
        maccs = []
        for month in sorted(monthly.keys()):
            ms = calc_stats(monthly[month])
            if ms and ms['n'] >= 10:
                maccs.append(ms['acc'])
                print(f"    {month}: 准确率={ms['acc']:.1%}, n={ms['n']}, 期望={ms['pnl']:+.3f}%")
        if maccs:
            am = sum(maccs)/len(maccs)
            astd = (sum((a-am)**2 for a in maccs)/max(len(maccs)-1,1))**0.5
            print(f"    ── 月均: {am:.1%} ± {astd:.1%}")

        return s, details

    # 评估最优参数
    s_best, d_best = evaluate_on_test(best, f"最优参数 (验证期准确率最高)")

    # 评估平衡参数
    s_bal, d_bal = None, None
    if best_balanced and best_balanced != best:
        s_bal, d_bal = evaluate_on_test(best_balanced, f"平衡参数 (样本≥300)")

    # 总结
    print(f"\n{'═' * 80}")
    print("📋 总结")
    print(f"{'═' * 80}")
    if s_best:
        acc = s_best['acc']
        if acc >= 0.65:
            print(f"  ✅ 最优参数: {acc:.1%} ≥ 65%，达标！({s_best['n']}样本)")
        elif acc >= 0.60:
            print(f"  ⚠️ 最优参数: {acc:.1%}，接近目标 ({s_best['n']}样本)")
        else:
            print(f"  ❌ 最优参数: {acc:.1%} ({s_best['n']}样本)")

        # 过拟合
        va = val_results[0]['s']['acc']
        diff = va - acc
        print(f"  过拟合: 验证{va:.1%} vs 测试{acc:.1%}, 差距{diff:+.1%} "
              f"{'✅' if diff < 0.03 else '⚠️' if diff < 0.05 else '❌'}")

    if s_bal:
        print(f"  平衡参数: {s_bal['acc']:.1%} ({s_bal['n']}样本)")

    # 保存
    report = {
        'meta': {'n_stocks': len(kdata), 'date_range': f'{start_date}~{end_date}',
                 'split': '40/30/30', 'run_time': round(time.time()-t0, 1)},
        'sent_ic': {fn: fs['ic'] for fn, fs in sorted(ss.items(), key=lambda x: x[1]['aic'], reverse=True)[:12]},
        'tech_ic': {fn: fs['ic'] for fn, fs in sorted(ts.items(), key=lambda x: x[1]['aic'], reverse=True)[:12]},
        'best_params': best,
        'balanced_params': best_balanced,
        'validation_top10': [{'p': vr['p'], **vr['s']} for vr in val_results[:10]],
        'test_best': s_best,
        'test_balanced': s_bal,
    }
    out = OUTPUT_DIR / "sentiment_v4c_backtest.json"
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n⏱️  总耗时: {time.time()-t0:.1f}秒")
    print(f"📁 报告: {out}")
    print("=" * 80)


if __name__ == '__main__':
    run_backtest()
