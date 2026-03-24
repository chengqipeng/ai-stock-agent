#!/usr/bin/env python3
"""
情绪因子深度诊断 — 寻找提升空间
================================
诊断维度：
  1. 错误分析：预测错误的样本有什么共同特征？
  2. 因子交互：两两因子组合是否比单因子更强？
  3. 非线性效应：因子与收益的关系是否非线性？
  4. 市场状态分层：不同市场环境下准确率差异
  5. 个股特征分层：不同类型股票的准确率差异
  6. 时间衰减：因子IC是否随时间衰减？
  7. 未利用数据源：龙虎榜、盘口等数据的增量价值

目标：找到不依赖参数调优的结构性提升方向

用法：
    source .venv/bin/activate
    python -m tools.backtest_sentiment_v5_analysis
"""
import json, logging, math, sys, time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dao import get_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)
OUTPUT_DIR = Path(__file__).parent.parent / "data_results"

S_MIN = 11; T_MIN = 6; CONF_MIN = 0.2; MKT_FILTER = -3

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

def load_dragon_tiger(codes, start):
    """加载龙虎榜数据"""
    conn = get_connection(use_dict_cursor=True); cur = conn.cursor()
    res = defaultdict(list)
    try:
        for i in range(0, len(codes), 300):
            batch = codes[i:i+300]; ph = ','.join(['%s']*len(batch))
            cur.execute(f"SELECT stock_code,trade_date,buy_amount,sell_amount,"
                        f"reason FROM stock_dragon_tiger WHERE stock_code IN ({ph}) "
                        f"AND trade_date>=%s ORDER BY trade_date", batch+[start])
            for r in cur.fetchall():
                buy = _f(r.get('buy_amount'))
                sell = _f(r.get('sell_amount'))
                res[r['stock_code']].append({
                    'd': str(r['trade_date']),
                    'buy': buy, 'sell': sell,
                    'net': buy - sell,
                    'reason': r.get('reason', ''),
                })
    except Exception as e:
        logger.warning("龙虎榜加载失败: %s", e)
    cur.close(); conn.close()
    return dict(res)


# ═══════════════════════════════════════════════════════════════
# 因子计算（与v4c一致）
# ═══════════════════════════════════════════════════════════════

def compute_all_factors(klines, ff_by_date=None, mkt_by_date=None):
    n = len(klines)
    if n < 60: return None, None
    close = [k['c'] for k in klines]; open_ = [k['o'] for k in klines]
    high = [k['h'] for k in klines]; low = [k['l'] for k in klines]
    vol = [k['v'] for k in klines]; pct = [k['p'] for k in klines]
    turn = [k['t'] for k in klines]
    if close[-1] <= 0 or vol[-1] <= 0: return None, None
    sf, tf = {}, {}

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
    bu = sum(1 for p in pct[-10:] if p > 3); bd = sum(1 for p in pct[-10:] if p < -3)
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
    dvol = sum(vol[n-5+i] for i in range(5) if pct[n-5+i] < 0); tvol = sum(vol[-5:])
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

    c20, v20 = close[-20:], vol[-20:]
    mc, mv = sum(c20)/20, sum(v20)/20
    cov = sum((c20[i]-mc)*(v20[i]-mv) for i in range(20))
    sc = sum((c-mc)**2 for c in c20)**0.5; sv = sum((v-mv)**2 for v in v20)**0.5
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


def vote_detail(factors, fstats, top_k=13):
    ranked = sorted(fstats.items(), key=lambda x: x[1]['aic'], reverse=True)
    top = [(fn, fs) for fn, fs in ranked[:top_k] if fn in factors]
    up = dn = 0; ws = wt = 0.0
    zscores = {}
    for fn, fs in top:
        z = (factors[fn]-fs['m'])/fs['s'] * fs['dir']
        zscores[fn] = z
        if z > 0.2: up += 1
        elif z < -0.2: dn += 1
        ws += z * fs['aic']; wt += fs['aic']
    return up, dn, ws, wt, zscores


def compute_future(klines, idx):
    base = klines[idx]['c']
    if base <= 0: return {}
    r = {}
    for h in (3, 5, 10):
        if idx+h < len(klines) and klines[idx+h]['c'] > 0:
            r[f'ret_{h}d'] = round((klines[idx+h]['c']/base-1)*100, 4)
    return r


# ═══════════════════════════════════════════════════════════════
# 主分析
# ═══════════════════════════════════════════════════════════════

def run_analysis():
    t0 = time.time()
    print("=" * 80)
    print("情绪因子深度诊断 — 寻找提升空间")
    print("=" * 80)

    logger.info("[1/8] 加载数据...")
    codes = load_codes(300)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=500)).strftime('%Y-%m-%d')
    kdata = load_klines(codes, start_date, end_date)
    ffdata = load_ff(codes, start_date)
    mkt = load_market(start_date, end_date)
    mkt_by_date = {m['d']: m for m in mkt}
    dtdata = load_dragon_tiger(codes, start_date)
    logger.info("  K线: %d只, 资金流: %d只, 大盘: %d天, 龙虎榜: %d只",
                len(kdata), len(ffdata), len(mkt), len(dtdata))

    # 训练期
    logger.info("[2/8] 训练因子统计...")
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

    # 收集测试期全部预测（含正确和错误）
    logger.info("[3/8] 收集测试期预测...")
    all_preds = []  # 详细预测记录

    for code, klines in kdata.items():
        if len(klines) < 80: continue
        ff_bd = {f['d']: f for f in ffdata.get(code, [])}
        dt_bd = {d['d']: d for d in dtdata.get(code, [])}
        te = int(len(klines)*0.4)

        for i in range(max(60, te), len(klines)-10):
            sf, tf = compute_all_factors(klines[:i+1], ff_bd, mkt_by_date)
            if not sf or not tf: continue

            mkt_c = [mkt_by_date.get(klines[j]['d'], {}).get('c', 0) for j in range(max(0,i-20), i+1)]
            mkt_c = [c for c in mkt_c if c > 0]
            mkt_ret = (mkt_c[-1]/mkt_c[0]-1)*100 if len(mkt_c) >= 2 else 0
            if mkt_ret < MKT_FILTER: continue

            s_up, s_dn, s_ws, s_wt, s_zs = vote_detail(sf, ss, top_k=13)
            t_up, t_dn, t_ws, t_wt, t_zs = vote_detail(tf, ts, top_k=13)

            if s_up < S_MIN or s_up <= s_dn: continue
            if t_up < T_MIN or t_up <= t_dn: continue
            combined = (s_ws/s_wt*0.6 + t_ws/t_wt*0.4) if s_wt > 0 and t_wt > 0 else 0
            if combined < CONF_MIN: continue

            fut = compute_future(klines, i)
            if 'ret_5d' not in fut: continue

            correct = fut['ret_5d'] > 0
            # 额外特征
            price = klines[i]['c']
            avg_turn = sum(k['t'] for k in klines[max(0,i-20):i+1] if k['t'] > 0) / max(1, sum(1 for k in klines[max(0,i-20):i+1] if k['t'] > 0))
            avg_vol = sum(k['v'] for k in klines[max(0,i-20):i+1]) / 20
            # 龙虎榜：最近10天是否有龙虎榜
            has_dt = any(klines[j]['d'] in dt_bd for j in range(max(0,i-10), i+1))
            dt_net = 0
            for j in range(max(0,i-10), i+1):
                if klines[j]['d'] in dt_bd:
                    dt_net += dt_bd[klines[j]['d']]['net']

            # 大盘波动率
            mkt_pcts = [mkt_by_date.get(klines[j]['d'], {}).get('p', 0) for j in range(max(0,i-20), i+1)]
            mkt_vol = (sum(p**2 for p in mkt_pcts)/max(len(mkt_pcts),1))**0.5

            all_preds.append({
                'code': code, 'date': klines[i]['d'], 'correct': correct,
                'ret_5d': fut['ret_5d'], 'ret_3d': fut.get('ret_3d'),
                'ret_10d': fut.get('ret_10d'),
                's_up': s_up, 't_up': t_up, 'combined': combined,
                's_zscores': s_zs, 't_zscores': t_zs,
                'price': price, 'avg_turn': avg_turn, 'avg_vol': avg_vol,
                'mkt_ret': mkt_ret, 'mkt_vol': mkt_vol,
                'has_dt': has_dt, 'dt_net': dt_net,
                'sf': sf, 'tf': tf,
            })

    n_total = len(all_preds)
    n_correct = sum(1 for p in all_preds if p['correct'])
    n_wrong = n_total - n_correct
    logger.info("  总预测: %d, 正确: %d(%.1f%%), 错误: %d",
                n_total, n_correct, n_correct/n_total*100 if n_total else 0, n_wrong)


    # ══════════════════════════════════════════════════════════
    # 诊断1: 错误样本特征分析
    # ══════════════════════════════════════════════════════════
    logger.info("[4/8] 诊断1: 错误样本特征...")
    print(f"\n{'═' * 80}")
    print("🔍 诊断1: 正确 vs 错误样本的特征差异")
    print(f"{'═' * 80}")

    correct_preds = [p for p in all_preds if p['correct']]
    wrong_preds = [p for p in all_preds if not p['correct']]

    # 比较各维度
    dims = [
        ('price', '股价', lambda p: p['price']),
        ('avg_turn', '换手率', lambda p: p['avg_turn']),
        ('mkt_ret', '大盘20d涨幅', lambda p: p['mkt_ret']),
        ('mkt_vol', '大盘波动率', lambda p: p['mkt_vol']),
        ('combined', '综合置信度', lambda p: p['combined']),
        ('s_up', '情绪一致数', lambda p: p['s_up']),
        ('t_up', '技术一致数', lambda p: p['t_up']),
        ('ret_5d', '5日收益', lambda p: p['ret_5d']),
    ]
    print(f"  {'维度':<16s} {'正确均值':>10s} {'错误均值':>10s} {'差异':>10s} {'方向':>6s}")
    print(f"  {'─' * 56}")
    for key, label, fn in dims:
        cv = [fn(p) for p in correct_preds if fn(p) is not None]
        wv = [fn(p) for p in wrong_preds if fn(p) is not None]
        if cv and wv:
            cm, wm = sum(cv)/len(cv), sum(wv)/len(wv)
            diff = cm - wm
            direction = '✅高好' if diff > 0 else '⚠️低好' if diff < 0 else '—'
            print(f"  {label:<16s} {cm:>10.3f} {wm:>10.3f} {diff:>+10.3f} {direction:>6s}")

    # ══════════════════════════════════════════════════════════
    # 诊断2: 市场状态分层
    # ══════════════════════════════════════════════════════════
    logger.info("[5/8] 诊断2: 市场状态分层...")
    print(f"\n{'═' * 80}")
    print("🔍 诊断2: 不同市场状态下的准确率")
    print(f"{'═' * 80}")

    # 按大盘趋势分层
    mkt_bins = [(-99, -3, '大盘下跌(<-3%)'), (-3, 0, '大盘震荡(-3~0%)'),
                (0, 3, '大盘温和上涨(0~3%)'), (3, 99, '大盘强势(>3%)')]
    print(f"\n  按大盘20日趋势:")
    for lo, hi, label in mkt_bins:
        subset = [p for p in all_preds if lo <= p['mkt_ret'] < hi]
        if len(subset) >= 20:
            acc = sum(1 for p in subset if p['correct'])/len(subset)
            avg_pnl = sum(p['ret_5d'] for p in subset)/len(subset)
            print(f"    {label:<24s}: 准确率={acc:.1%}, 期望={avg_pnl:+.3f}%, n={len(subset)}")

    # 按大盘波动率分层
    mkt_vol_bins = [(0, 0.5, '低波动(<0.5%)'), (0.5, 1.0, '中波动(0.5~1%)'),
                    (1.0, 1.5, '高波动(1~1.5%)'), (1.5, 99, '极高波动(>1.5%)')]
    print(f"\n  按大盘波动率:")
    for lo, hi, label in mkt_vol_bins:
        subset = [p for p in all_preds if lo <= p['mkt_vol'] < hi]
        if len(subset) >= 20:
            acc = sum(1 for p in subset if p['correct'])/len(subset)
            avg_pnl = sum(p['ret_5d'] for p in subset)/len(subset)
            print(f"    {label:<24s}: 准确率={acc:.1%}, 期望={avg_pnl:+.3f}%, n={len(subset)}")

    # ══════════════════════════════════════════════════════════
    # 诊断3: 个股特征分层
    # ══════════════════════════════════════════════════════════
    logger.info("[6/8] 诊断3: 个股特征分层...")
    print(f"\n{'═' * 80}")
    print("🔍 诊断3: 不同个股特征下的准确率")
    print(f"{'═' * 80}")

    # 按股价分层
    price_bins = [(0, 10, '低价股(<10)'), (10, 30, '中价股(10~30)'),
                  (30, 60, '中高价(30~60)'), (60, 9999, '高价股(>60)')]
    print(f"\n  按股价:")
    for lo, hi, label in price_bins:
        subset = [p for p in all_preds if lo <= p['price'] < hi]
        if len(subset) >= 20:
            acc = sum(1 for p in subset if p['correct'])/len(subset)
            avg_pnl = sum(p['ret_5d'] for p in subset)/len(subset)
            print(f"    {label:<24s}: 准确率={acc:.1%}, 期望={avg_pnl:+.3f}%, n={len(subset)}")

    # 按换手率分层
    turn_bins = [(0, 1, '低换手(<1%)'), (1, 3, '中换手(1~3%)'),
                 (3, 8, '高换手(3~8%)'), (8, 999, '极高换手(>8%)')]
    print(f"\n  按换手率:")
    for lo, hi, label in turn_bins:
        subset = [p for p in all_preds if lo <= p['avg_turn'] < hi]
        if len(subset) >= 20:
            acc = sum(1 for p in subset if p['correct'])/len(subset)
            avg_pnl = sum(p['ret_5d'] for p in subset)/len(subset)
            print(f"    {label:<24s}: 准确率={acc:.1%}, 期望={avg_pnl:+.3f}%, n={len(subset)}")

    # 按龙虎榜
    print(f"\n  龙虎榜效应:")
    dt_yes = [p for p in all_preds if p['has_dt']]
    dt_no = [p for p in all_preds if not p['has_dt']]
    if dt_yes:
        acc_y = sum(1 for p in dt_yes if p['correct'])/len(dt_yes)
        pnl_y = sum(p['ret_5d'] for p in dt_yes)/len(dt_yes)
        print(f"    有龙虎榜(近10天): 准确率={acc_y:.1%}, 期望={pnl_y:+.3f}%, n={len(dt_yes)}")
    if dt_no:
        acc_n = sum(1 for p in dt_no if p['correct'])/len(dt_no)
        pnl_n = sum(p['ret_5d'] for p in dt_no)/len(dt_no)
        print(f"    无龙虎榜:         准确率={acc_n:.1%}, 期望={pnl_n:+.3f}%, n={len(dt_no)}")
    # 龙虎榜净买入方向
    dt_buy = [p for p in dt_yes if p['dt_net'] > 0]
    dt_sell = [p for p in dt_yes if p['dt_net'] < 0]
    if dt_buy and len(dt_buy) >= 5:
        acc_b = sum(1 for p in dt_buy if p['correct'])/len(dt_buy)
        print(f"    龙虎榜净买入:     准确率={acc_b:.1%}, n={len(dt_buy)}")
    if dt_sell and len(dt_sell) >= 5:
        acc_s = sum(1 for p in dt_sell if p['correct'])/len(dt_sell)
        print(f"    龙虎榜净卖出:     准确率={acc_s:.1%}, n={len(dt_sell)}")


    # ══════════════════════════════════════════════════════════
    # 诊断4: 因子非线性效应（分位数分析）
    # ══════════════════════════════════════════════════════════
    logger.info("[7/8] 诊断4: 因子非线性 + 交互 + 时间衰减...")
    print(f"\n{'═' * 80}")
    print("🔍 诊断4: 因子非线性效应（在已筛选的UP信号中）")
    print(f"{'═' * 80}")

    # 对每个关键因子，看其在已筛选信号中的分位数与准确率的关系
    key_factors = ['skew_20', 'price_pos', 'bias_20', 'noise_ratio', 'vp_corr', 'mom_20', 'rel_str']
    for fn in key_factors:
        vals = [(p['sf'].get(fn) or p['tf'].get(fn), p['correct'], p['ret_5d'])
                for p in all_preds if (p['sf'].get(fn) or p['tf'].get(fn)) is not None]
        if len(vals) < 100: continue
        vals.sort(key=lambda x: x[0])
        n = len(vals)
        q_size = n // 5
        print(f"\n  {fn} 五分位分析（在UP信号中）:")
        for q in range(5):
            start = q * q_size
            end = (q+1) * q_size if q < 4 else n
            subset = vals[start:end]
            acc = sum(1 for v, c, r in subset if c) / len(subset)
            avg_ret = sum(r for v, c, r in subset) / len(subset)
            avg_val = sum(v for v, c, r in subset) / len(subset)
            print(f"    Q{q+1}(avg={avg_val:+.3f}): 准确率={acc:.1%}, 期望={avg_ret:+.3f}%, n={len(subset)}")

    # ══════════════════════════════════════════════════════════
    # 诊断5: 因子交互效应（两两组合）
    # ══════════════════════════════════════════════════════════
    print(f"\n{'═' * 80}")
    print("🔍 诊断5: 因子交互效应（哪些因子组合能提升准确率）")
    print(f"{'═' * 80}")

    # 对每对因子，看同时处于极端值时的准确率
    interact_factors = ['skew_20', 'price_pos', 'bias_20', 'vp_corr', 'mom_20', 'rel_str', 'noise_ratio']
    print(f"\n  两因子同时极端看涨（z>0.5）时的准确率:")
    print(f"  {'因子A':<14s} {'因子B':<14s} {'准确率':>6s} {'期望':>8s} {'样本':>6s}")
    interactions = []
    for i, fa in enumerate(interact_factors):
        for fb in interact_factors[i+1:]:
            subset = []
            for p in all_preds:
                za = p['s_zscores'].get(fa) or p['t_zscores'].get(fa)
                zb = p['s_zscores'].get(fb) or p['t_zscores'].get(fb)
                if za is not None and zb is not None and za > 0.5 and zb > 0.5:
                    subset.append(p)
            if len(subset) >= 30:
                acc = sum(1 for p in subset if p['correct'])/len(subset)
                avg_pnl = sum(p['ret_5d'] for p in subset)/len(subset)
                interactions.append((fa, fb, acc, avg_pnl, len(subset)))

    interactions.sort(key=lambda x: x[2], reverse=True)
    for fa, fb, acc, pnl, n in interactions[:15]:
        marker = '🔥' if acc >= 0.70 else '✅' if acc >= 0.65 else ''
        print(f"  {fa:<14s} {fb:<14s} {acc:>6.1%} {pnl:>+8.3f}% {n:>6d} {marker}")

    # ══════════════════════════════════════════════════════════
    # 诊断6: 时间衰减分析
    # ══════════════════════════════════════════════════════════
    print(f"\n{'═' * 80}")
    print("🔍 诊断6: 因子IC时间衰减（按季度）")
    print(f"{'═' * 80}")

    # 按季度计算IC
    quarterly_ic = defaultdict(lambda: defaultdict(list))
    for code, klines in kdata.items():
        if len(klines) < 80: continue
        ff_bd = {f['d']: f for f in ffdata.get(code, [])}
        te = int(len(klines)*0.4)
        for i in range(max(60, te), len(klines)-10):
            sf, tf = compute_all_factors(klines[:i+1], ff_bd, mkt_by_date)
            if not sf or not tf: continue
            fut = compute_future(klines, i)
            if 'ret_5d' not in fut: continue
            quarter = klines[i]['d'][:7]  # 用月份近似
            ret = fut['ret_5d']
            for fn, fv in {**sf, **tf}.items():
                if fv is not None:
                    quarterly_ic[quarter][fn].append((fv, ret))

    # 计算每月top因子的IC
    top_fns = ['skew_20', 'price_pos', 'bias_20', 'vp_corr', 'mom_20']
    print(f"\n  {'月份':<10s}", end='')
    for fn in top_fns:
        print(f" {fn:>12s}", end='')
    print()
    for month in sorted(quarterly_ic.keys()):
        print(f"  {month:<10s}", end='')
        for fn in top_fns:
            data = quarterly_ic[month].get(fn, [])
            if len(data) < 50:
                print(f" {'N/A':>12s}", end='')
                continue
            vals = [d[0] for d in data]; rets = [d[1] for d in data]
            nn = len(vals)
            m = sum(vals)/nn; mr = sum(rets)/nn
            sv = (sum((v-m)**2 for v in vals))**0.5
            sr = (sum((r-mr)**2 for r in rets))**0.5
            if sv > 0 and sr > 0:
                ic = sum((vals[j]-m)*(rets[j]-mr) for j in range(nn))/(sv*sr)
                print(f" {ic:>+12.4f}", end='')
            else:
                print(f" {'0':>12s}", end='')
        print()


    # ══════════════════════════════════════════════════════════
    # 诊断7: 预测时间窗口优化
    # ══════════════════════════════════════════════════════════
    print(f"\n{'═' * 80}")
    print("🔍 诊断7: 不同预测窗口的准确率")
    print(f"{'═' * 80}")

    for horizon, key in [(3, 'ret_3d'), (5, 'ret_5d'), (10, 'ret_10d')]:
        subset = [(p['correct'] if key == 'ret_5d' else (p[key] > 0 if p[key] is not None else None), p[key])
                  for p in all_preds if p.get(key) is not None]
        subset = [(c, r) for c, r in subset if c is not None]
        if subset:
            acc = sum(1 for c, r in subset if c)/len(subset)
            avg_pnl = sum(r for c, r in subset)/len(subset)
            print(f"  {horizon}日: 准确率={acc:.1%}, 期望={avg_pnl:+.3f}%, n={len(subset)}")

    # ══════════════════════════════════════════════════════════
    # 诊断8: 综合提升建议
    # ══════════════════════════════════════════════════════════
    logger.info("[8/8] 综合分析...")
    print(f"\n{'═' * 80}")
    print("📋 综合提升建议")
    print(f"{'═' * 80}")

    # 1. 找到准确率最高的子集
    print(f"\n  1. 高准确率子集搜索:")
    # 按多维度组合搜索
    best_subsets = []

    # 大盘温和上涨 + 高一致性
    for mkt_lo in [0, 1, 2]:
        for s_min in [11, 12]:
            subset = [p for p in all_preds if p['mkt_ret'] >= mkt_lo and p['s_up'] >= s_min]
            if len(subset) >= 30:
                acc = sum(1 for p in subset if p['correct'])/len(subset)
                pnl = sum(p['ret_5d'] for p in subset)/len(subset)
                best_subsets.append((f"mkt≥{mkt_lo}%+sent≥{s_min}", acc, pnl, len(subset)))

    # 低波动 + 高一致性
    for vol_hi in [0.8, 1.0, 1.2]:
        subset = [p for p in all_preds if p['mkt_vol'] < vol_hi]
        if len(subset) >= 30:
            acc = sum(1 for p in subset if p['correct'])/len(subset)
            pnl = sum(p['ret_5d'] for p in subset)/len(subset)
            best_subsets.append((f"mkt_vol<{vol_hi}", acc, pnl, len(subset)))

    # 中价股 + 中换手
    for p_lo, p_hi in [(10, 60), (10, 30), (15, 50)]:
        for t_lo, t_hi in [(1, 8), (1, 5), (2, 6)]:
            subset = [p for p in all_preds if p_lo <= p['price'] < p_hi and t_lo <= p['avg_turn'] < t_hi]
            if len(subset) >= 30:
                acc = sum(1 for p in subset if p['correct'])/len(subset)
                pnl = sum(p['ret_5d'] for p in subset)/len(subset)
                best_subsets.append((f"价{p_lo}-{p_hi}+换手{t_lo}-{t_hi}", acc, pnl, len(subset)))

    best_subsets.sort(key=lambda x: x[1], reverse=True)
    print(f"  {'条件':<30s} {'准确率':>6s} {'期望':>8s} {'样本':>6s}")
    for label, acc, pnl, n in best_subsets[:15]:
        marker = '🔥' if acc >= 0.70 else '✅' if acc >= 0.65 else ''
        print(f"  {label:<30s} {acc:>6.1%} {pnl:>+8.3f}% {n:>6d} {marker}")

    # 2. 错误模式分析
    print(f"\n  2. 错误模式分析:")
    # 大幅亏损的样本特征
    big_loss = [p for p in all_preds if p['ret_5d'] < -5]
    if big_loss:
        avg_mkt = sum(p['mkt_ret'] for p in big_loss)/len(big_loss)
        avg_vol = sum(p['mkt_vol'] for p in big_loss)/len(big_loss)
        avg_turn = sum(p['avg_turn'] for p in big_loss)/len(big_loss)
        print(f"    大幅亏损(ret<-5%): {len(big_loss)}样本")
        print(f"      大盘均值: {avg_mkt:+.2f}%, 大盘波动: {avg_vol:.2f}%, 换手率: {avg_turn:.2f}%")
        # 这些样本的因子特征
        for fn in ['skew_20', 'price_pos', 'bias_20']:
            vals = [p['sf'].get(fn) for p in big_loss if p['sf'].get(fn) is not None]
            if vals:
                print(f"      {fn}: {sum(vals)/len(vals):+.3f}")

    # 3. 理论上限估计
    print(f"\n  3. 理论上限估计:")
    # 如果我们能完美过滤掉大幅亏损的样本
    filtered = [p for p in all_preds if p['ret_5d'] > -5]
    if filtered:
        acc_f = sum(1 for p in filtered if p['correct'])/len(filtered)
        print(f"    去除ret<-5%后: 准确率={acc_f:.1%} ({len(filtered)}样本)")
    filtered2 = [p for p in all_preds if p['ret_5d'] > -3]
    if filtered2:
        acc_f2 = sum(1 for p in filtered2 if p['correct'])/len(filtered2)
        print(f"    去除ret<-3%后: 准确率={acc_f2:.1%} ({len(filtered2)}样本)")

    # 4. 未利用信息
    print(f"\n  4. 未利用信息源评估:")
    print(f"    龙虎榜: {len(dtdata)}只股票有数据, {sum(1 for p in all_preds if p['has_dt'])}个信号有龙虎榜")
    print(f"    建议: ", end='')
    if dt_yes and len(dt_yes) >= 10:
        acc_dt = sum(1 for p in dt_yes if p['correct'])/len(dt_yes)
        acc_no = sum(1 for p in dt_no if p['correct'])/len(dt_no)
        if acc_dt > acc_no + 0.05:
            print(f"龙虎榜有增量价值(+{(acc_dt-acc_no)*100:.1f}%)")
        elif acc_dt < acc_no - 0.05:
            print(f"龙虎榜为负面信号({(acc_dt-acc_no)*100:+.1f}%)，可作为过滤条件")
        else:
            print(f"龙虎榜无显著增量")
    else:
        print("龙虎榜样本不足")

    # 保存
    report = {
        'meta': {'n_total': n_total, 'n_correct': n_correct, 'acc': round(n_correct/n_total, 4) if n_total else 0,
                 'run_time': round(time.time()-t0, 1)},
        'best_subsets': [{'label': l, 'acc': a, 'pnl': p, 'n': n} for l, a, p, n in best_subsets[:15]],
        'interactions': [{'fa': fa, 'fb': fb, 'acc': a, 'pnl': p, 'n': n} for fa, fb, a, p, n in interactions[:15]],
    }
    out = OUTPUT_DIR / "sentiment_v5_analysis.json"
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n⏱️  总耗时: {time.time()-t0:.1f}秒")
    print(f"📁 报告: {out}")
    print("=" * 80)


if __name__ == '__main__':
    run_analysis()
