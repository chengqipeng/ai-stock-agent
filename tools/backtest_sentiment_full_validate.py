#!/usr/bin/env python3
"""
情绪因子全量回测 — 详细结果 + 严格防过拟合
==========================================
验证方法（4种，互相印证）：
  M1: 固定训练(前40%) + 后60%评估
  M2: Walk-Forward（每60天重训练，最严格）
  M3: 5折时序交叉验证（不打乱时间顺序）
  M4: 扩展窗口（Expanding Window）

详细输出：
  - 每种方法的3d/5d/10d准确率、期望收益、盈亏比
  - 月度稳定性（每月准确率+样本数）
  - 按情绪一致数分层
  - 按股价/换手率/大盘状态分层
  - 规则过滤统计
  - 过拟合诊断（方法间差异、月度方差）
  - 每只股票的信号明细（可选）

用法：
    source .venv/bin/activate
    python -m tools.backtest_sentiment_full_validate
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

# ═══════════════════════════════════════════════════════════════
# 固定规则（来自v5诊断，不可调）
# ═══════════════════════════════════════════════════════════════
SENT_MIN = 11
TECH_MIN = 6
MKT_UPPER = 0
MKT_LOWER = -3
PRICE_MAX = 60
TURN_MAX = 8
SKEW_LO = -1.5
SKEW_HI = 0.3


def _f(v):
    try: return float(v) if v is not None else 0.0
    except: return 0.0


# ═══════════════════════════════════════════════════════════════
# 数据加载
# ═══════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════
# 因子计算（与final版一致）
# ═══════════════════════════════════════════════════════════════

def compute_all_factors(klines, ff_by_date=None, mkt_by_date=None):
    n = len(klines)
    if n < 60: return None, None, None
    close = [k['c'] for k in klines]; open_ = [k['o'] for k in klines]
    high = [k['h'] for k in klines]; low = [k['l'] for k in klines]
    vol = [k['v'] for k in klines]; pct = [k['p'] for k in klines]
    turn = [k['t'] for k in klines]
    if close[-1] <= 0 or vol[-1] <= 0: return None, None, None
    sf, tf = {}, {}

    # 情绪因子(15个)
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

    # 技术因子(13个)
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

    avg_turn = sum(t20)/len(t20) if t20 else 0
    meta = {'price': close[-1], 'avg_turn': avg_turn, 'skew_20': sf.get('skew_20')}
    return sf, tf, meta


# ═══════════════════════════════════════════════════════════════
# 统计 & 投票
# ═══════════════════════════════════════════════════════════════

def build_stats(records, min_n=200):
    stats = {}
    for fn, data in records.items():
        vals = [r[0] for r in data]; rets = [r[1] for r in data]
        nn = len(vals)
        if nn < min_n: continue
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


def vote_count(factors, fstats, top_k=13):
    ranked = sorted(fstats.items(), key=lambda x: x[1]['aic'], reverse=True)
    top = [(fn, fs) for fn, fs in ranked[:top_k] if fn in factors]
    up = dn = 0; ws = wt = 0.0
    for fn, fs in top:
        z = (factors[fn]-fs['m'])/fs['s'] * fs['dir']
        if z > 0.2: up += 1
        elif z < -0.2: dn += 1
        ws += z * fs['aic']; wt += fs['aic']
    return up, dn, ws, wt


def apply_rules(sf, tf, meta, ss, ts, mkt_ret):
    if mkt_ret < MKT_LOWER or mkt_ret > MKT_UPPER:
        return None, 0, 'R1:大盘不在震荡区间'
    if meta['price'] > PRICE_MAX:
        return None, 0, 'R2:股价过高'
    if meta['avg_turn'] > TURN_MAX:
        return None, 0, 'R2:换手率过高'
    skew = meta.get('skew_20')
    if skew is not None and (skew < SKEW_LO or skew > SKEW_HI):
        return None, 0, 'R3:skew极端值'
    s_up, s_dn, s_ws, s_wt = vote_count(sf, ss, top_k=13)
    t_up, t_dn, t_ws, t_wt = vote_count(tf, ts, top_k=13)
    if s_up < SENT_MIN or s_up <= s_dn:
        return None, 0, '情绪一致性不足'
    if t_up < TECH_MIN or t_up <= t_dn:
        return None, 0, '技术一致性不足'
    s_conf = s_ws/s_wt if s_wt > 0 else 0
    t_conf = t_ws/t_wt if t_wt > 0 else 0
    combined = s_conf * 0.6 + t_conf * 0.4
    return 'UP', combined, f's={s_up},t={t_up},c={combined:.2f}'


def get_mkt_ret(klines, i, mkt_by_date):
    mkt_c = [mkt_by_date.get(klines[j]['d'], {}).get('c', 0) for j in range(max(0,i-20), i+1)]
    mkt_c = [c for c in mkt_c if c > 0]
    return (mkt_c[-1]/mkt_c[0]-1)*100 if len(mkt_c) >= 2 else 0


def compute_future(klines, idx):
    base = klines[idx]['c']
    if base <= 0: return {}
    r = {}
    for h in (3, 5, 10):
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
            'plr': round(aw/al, 2) if al > 0 else 999,
            'win_n': len(wins), 'loss_n': len(losses)}

def fmt_plr(s):
    if s is None: return '—'
    return f"{s['plr']:.2f}" if isinstance(s['plr'], (int, float)) and s['plr'] < 900 else 'inf'


# ═══════════════════════════════════════════════════════════════
# 主回测
# ═══════════════════════════════════════════════════════════════

def run_backtest():
    t0 = time.time()
    print("=" * 80)
    print("情绪因子全量回测 — 详细结果 + 严格防过拟合")
    print("=" * 80)
    print(f"规则: sent≥{SENT_MIN}, tech≥{TECH_MIN}, 大盘{MKT_LOWER}%~{MKT_UPPER}%, "
          f"价<{PRICE_MAX}, 换手<{TURN_MAX}%, skew∈[{SKEW_LO},{SKEW_HI}]")

    # ── 加载数据 ──
    logger.info("[1/7] 加载数据...")
    codes = load_codes(300)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=500)).strftime('%Y-%m-%d')
    kdata = load_klines(codes, start_date, end_date)
    ffdata = load_ff(codes, start_date)
    mkt = load_market(start_date, end_date)
    mkt_by_date = {m['d']: m for m in mkt}
    logger.info("  K线: %d只, 资金流: %d只, 大盘: %d天", len(kdata), len(ffdata), len(mkt))

    # ══════════════════════════════════════════════════════════
    # M1: 固定训练(前40%) + 后60%评估
    # ══════════════════════════════════════════════════════════
    logger.info("[2/7] M1: 固定训练 + 后60%评估...")
    s_rec = defaultdict(list); t_rec = defaultdict(list); train_n = 0
    for code, klines in kdata.items():
        if len(klines) < 80: continue
        ff_bd = {f['d']: f for f in ffdata.get(code, [])}
        te = int(len(klines)*0.4)
        for i in range(60, min(te, len(klines)-10)):
            sf, tf, meta = compute_all_factors(klines[:i+1], ff_bd, mkt_by_date)
            if not sf or not tf: continue
            fut = compute_future(klines, i)
            if 'ret_5d' not in fut: continue
            train_n += 1; ret = fut['ret_5d']
            for fn, fv in sf.items():
                if fv is not None: s_rec[fn].append((fv, ret))
            for fn, fv in tf.items():
                if fv is not None: t_rec[fn].append((fv, ret))
    ss_fixed = build_stats(s_rec); ts_fixed = build_stats(t_rec)
    logger.info("  训练: %d样本, 情绪因子: %d, 技术因子: %d", train_n, len(ss_fixed), len(ts_fixed))

    # 收集M1信号（含详细信息）
    m1_signals = []
    m1_filtered = defaultdict(int)
    for code, klines in kdata.items():
        if len(klines) < 80: continue
        ff_bd = {f['d']: f for f in ffdata.get(code, [])}
        te = int(len(klines)*0.4)
        for i in range(max(60, te), len(klines)-10):
            sf, tf, meta = compute_all_factors(klines[:i+1], ff_bd, mkt_by_date)
            if not sf or not tf: continue
            mkt_ret = get_mkt_ret(klines, i, mkt_by_date)
            sig, conf, reason = apply_rules(sf, tf, meta, ss_fixed, ts_fixed, mkt_ret)
            if sig is None:
                m1_filtered[reason.split(':')[0]] += 1
                continue
            fut = compute_future(klines, i)
            if 'ret_5d' not in fut: continue
            s_up, _, _, _ = vote_count(sf, ss_fixed, 13)
            m1_signals.append({
                'code': code, 'date': klines[i]['d'], 'conf': conf,
                'ret_3d': fut.get('ret_3d'), 'ret_5d': fut['ret_5d'], 'ret_10d': fut.get('ret_10d'),
                's_up': s_up, 'price': meta['price'], 'avg_turn': meta['avg_turn'],
                'mkt_ret': mkt_ret,
            })

    # ══════════════════════════════════════════════════════════
    # M2: Walk-Forward（每60天重训练）
    # ══════════════════════════════════════════════════════════
    logger.info("[3/7] M2: Walk-Forward...")
    m2_signals = []
    wf_retrain = 0
    for code, klines in kdata.items():
        if len(klines) < 130: continue
        ff_bd = {f['d']: f for f in ffdata.get(code, [])}
        local_ss, local_ts = None, None
        last_train = -999
        for i in range(120, len(klines)-10):
            if i - last_train >= 60 or local_ss is None:
                ts_idx = max(0, i-120)
                ls = defaultdict(list); lt = defaultdict(list)
                for ti in range(max(60, ts_idx), i):
                    sf, tf, meta = compute_all_factors(klines[:ti+1], ff_bd, mkt_by_date)
                    if not sf or not tf: continue
                    fut = compute_future(klines, ti)
                    if 'ret_5d' not in fut: continue
                    ret = fut['ret_5d']
                    for fn, fv in sf.items():
                        if fv is not None: ls[fn].append((fv, ret))
                    for fn, fv in tf.items():
                        if fv is not None: lt[fn].append((fv, ret))
                local_ss = build_stats(ls, min_n=30)
                local_ts = build_stats(lt, min_n=30)
                last_train = i; wf_retrain += 1
            if not local_ss or not local_ts: continue
            sf, tf, meta = compute_all_factors(klines[:i+1], ff_bd, mkt_by_date)
            if not sf or not tf: continue
            mkt_ret = get_mkt_ret(klines, i, mkt_by_date)
            sig, conf, reason = apply_rules(sf, tf, meta, local_ss, local_ts, mkt_ret)
            if sig is None: continue
            fut = compute_future(klines, i)
            if 'ret_5d' not in fut: continue
            s_up, _, _, _ = vote_count(sf, local_ss, 13)
            m2_signals.append({
                'code': code, 'date': klines[i]['d'], 'conf': conf,
                'ret_3d': fut.get('ret_3d'), 'ret_5d': fut['ret_5d'], 'ret_10d': fut.get('ret_10d'),
                's_up': s_up, 'price': meta['price'], 'avg_turn': meta['avg_turn'],
                'mkt_ret': mkt_ret,
            })
    logger.info("  WF重训练: %d次, 信号: %d个", wf_retrain, len(m2_signals))


    # ══════════════════════════════════════════════════════════
    # M3: 5折时序交叉验证
    # ══════════════════════════════════════════════════════════
    logger.info("[4/7] M3: 5折时序交叉验证...")
    # 按时间排序所有日期，分5段
    all_dates = sorted(set(k['d'] for klines in kdata.values() for k in klines))
    n_dates = len(all_dates)
    fold_size = n_dates // 5
    m3_fold_results = []

    for fold in range(5):
        # 测试集：第fold段
        test_start = all_dates[fold * fold_size]
        test_end = all_dates[min((fold+1) * fold_size - 1, n_dates-1)]
        # 训练集：测试集之前的所有数据（严格时序）
        train_end = test_start

        # 训练
        fs_rec = defaultdict(list); ft_rec = defaultdict(list)
        for code, klines in kdata.items():
            if len(klines) < 80: continue
            ff_bd = {f['d']: f for f in ffdata.get(code, [])}
            for i in range(60, len(klines)-10):
                if klines[i]['d'] >= train_end: break
                sf, tf, meta = compute_all_factors(klines[:i+1], ff_bd, mkt_by_date)
                if not sf or not tf: continue
                fut = compute_future(klines, i)
                if 'ret_5d' not in fut: continue
                ret = fut['ret_5d']
                for fn, fv in sf.items():
                    if fv is not None: fs_rec[fn].append((fv, ret))
                for fn, fv in tf.items():
                    if fv is not None: ft_rec[fn].append((fv, ret))

        fold_ss = build_stats(fs_rec, min_n=50)
        fold_ts = build_stats(ft_rec, min_n=50)
        if not fold_ss or not fold_ts:
            m3_fold_results.append({'fold': fold+1, 'n': 0, 'acc': 0, 'pnl': 0, 'note': '训练不足'})
            continue

        # 测试
        fold_preds = []
        for code, klines in kdata.items():
            if len(klines) < 80: continue
            ff_bd = {f['d']: f for f in ffdata.get(code, [])}
            for i in range(60, len(klines)-10):
                if klines[i]['d'] < test_start or klines[i]['d'] > test_end: continue
                sf, tf, meta = compute_all_factors(klines[:i+1], ff_bd, mkt_by_date)
                if not sf or not tf: continue
                mkt_ret = get_mkt_ret(klines, i, mkt_by_date)
                sig, conf, reason = apply_rules(sf, tf, meta, fold_ss, fold_ts, mkt_ret)
                if sig is None: continue
                fut = compute_future(klines, i)
                if 'ret_5d' not in fut: continue
                fold_preds.append(('UP', fut['ret_5d']))

        fs = calc_stats(fold_preds)
        m3_fold_results.append({
            'fold': fold+1, 'period': f'{test_start}~{test_end}',
            'n': fs['n'] if fs else 0,
            'acc': fs['acc'] if fs else 0,
            'pnl': fs['pnl'] if fs else 0,
            'plr': fs['plr'] if fs else 0,
        })

    # ══════════════════════════════════════════════════════════
    # M4: 扩展窗口（Expanding Window）
    # ══════════════════════════════════════════════════════════
    logger.info("[5/7] M4: 扩展窗口...")
    # 每2个月为一个测试窗口，训练用该窗口之前的所有数据
    m4_signals = []
    window_months = 2
    # 按月分组
    month_set = sorted(set(d[:7] for d in all_dates))
    for wi in range(2, len(month_set), window_months):
        test_months = month_set[wi:wi+window_months]
        if not test_months: continue
        train_cutoff = test_months[0] + '-01'

        # 训练
        es_rec = defaultdict(list); et_rec = defaultdict(list)
        for code, klines in kdata.items():
            if len(klines) < 80: continue
            ff_bd = {f['d']: f for f in ffdata.get(code, [])}
            for i in range(60, len(klines)-10):
                if klines[i]['d'] >= train_cutoff: break
                sf, tf, meta = compute_all_factors(klines[:i+1], ff_bd, mkt_by_date)
                if not sf or not tf: continue
                fut = compute_future(klines, i)
                if 'ret_5d' not in fut: continue
                ret = fut['ret_5d']
                for fn, fv in sf.items():
                    if fv is not None: es_rec[fn].append((fv, ret))
                for fn, fv in tf.items():
                    if fv is not None: et_rec[fn].append((fv, ret))

        exp_ss = build_stats(es_rec, min_n=50)
        exp_ts = build_stats(et_rec, min_n=50)
        if not exp_ss or not exp_ts: continue

        # 测试
        for code, klines in kdata.items():
            if len(klines) < 80: continue
            ff_bd = {f['d']: f for f in ffdata.get(code, [])}
            for i in range(60, len(klines)-10):
                dm = klines[i]['d'][:7]
                if dm not in test_months: continue
                sf, tf, meta = compute_all_factors(klines[:i+1], ff_bd, mkt_by_date)
                if not sf or not tf: continue
                mkt_ret = get_mkt_ret(klines, i, mkt_by_date)
                sig, conf, reason = apply_rules(sf, tf, meta, exp_ss, exp_ts, mkt_ret)
                if sig is None: continue
                fut = compute_future(klines, i)
                if 'ret_5d' not in fut: continue
                s_up, _, _, _ = vote_count(sf, exp_ss, 13)
                m4_signals.append({
                    'code': code, 'date': klines[i]['d'], 'conf': conf,
                    'ret_3d': fut.get('ret_3d'), 'ret_5d': fut['ret_5d'], 'ret_10d': fut.get('ret_10d'),
                    's_up': s_up, 'price': meta['price'], 'avg_turn': meta['avg_turn'],
                    'mkt_ret': mkt_ret,
                })
    logger.info("  扩展窗口信号: %d个", len(m4_signals))


    # ══════════════════════════════════════════════════════════
    # M0: 无规则基线
    # ══════════════════════════════════════════════════════════
    logger.info("[6/7] M0: 无规则基线...")
    m0_preds = []
    for code, klines in kdata.items():
        if len(klines) < 80: continue
        ff_bd = {f['d']: f for f in ffdata.get(code, [])}
        te = int(len(klines)*0.4)
        for i in range(max(60, te), len(klines)-10):
            sf, tf, meta = compute_all_factors(klines[:i+1], ff_bd, mkt_by_date)
            if not sf or not tf: continue
            mkt_ret = get_mkt_ret(klines, i, mkt_by_date)
            if mkt_ret < -3: continue
            s_up, s_dn, _, _ = vote_count(sf, ss_fixed, 13)
            t_up, t_dn, _, _ = vote_count(tf, ts_fixed, 13)
            if s_up < SENT_MIN or s_up <= s_dn: continue
            if t_up < TECH_MIN or t_up <= t_dn: continue
            fut = compute_future(klines, i)
            if 'ret_5d' not in fut: continue
            m0_preds.append(('UP', fut['ret_5d']))

    # ══════════════════════════════════════════════════════════
    # 输出详细报告
    # ══════════════════════════════════════════════════════════
    logger.info("[7/7] 输出报告...")

    def analyze_signals(signals, label):
        """对一组信号做全维度分析"""
        if not signals:
            print(f"\n  {label}: 无信号")
            return {}

        # 基础统计
        p3 = [('UP', s['ret_3d']) for s in signals if s.get('ret_3d') is not None]
        p5 = [('UP', s['ret_5d']) for s in signals]
        p10 = [('UP', s['ret_10d']) for s in signals if s.get('ret_10d') is not None]
        s3, s5, s10 = calc_stats(p3), calc_stats(p5), calc_stats(p10)

        print(f"\n{'═' * 80}")
        print(f"📊 {label}")
        print(f"{'═' * 80}")
        print(f"  {'窗口':<6s} {'准确率':>8s} {'样本':>6s} {'期望收益':>10s} {'盈亏比':>8s} {'胜/负':>10s}")
        print(f"  {'─' * 52}")
        for lbl, st in [('3日', s3), ('5日', s5), ('10日', s10)]:
            if st:
                print(f"  {lbl:<6s} {st['acc']:>7.1%} {st['n']:>6d} {st['pnl']:>+9.3f}% "
                      f"{fmt_plr(st):>8s} {st['win_n']:>4d}/{st['loss_n']:<4d}")

        # 月度稳定性
        monthly = defaultdict(list)
        for s in signals:
            monthly[s['date'][:7]].append(('UP', s['ret_5d']))
        print(f"\n  月度稳定性(5日):")
        print(f"  {'月份':<10s} {'准确率':>8s} {'样本':>6s} {'期望':>10s} {'盈亏比':>8s}")
        print(f"  {'─' * 46}")
        maccs = []
        for month in sorted(monthly.keys()):
            ms = calc_stats(monthly[month])
            if ms and ms['n'] >= 3:
                maccs.append(ms['acc'])
                print(f"  {month:<10s} {ms['acc']:>7.1%} {ms['n']:>6d} {ms['pnl']:>+9.3f}% {fmt_plr(ms):>8s}")
        if maccs:
            am = sum(maccs)/len(maccs)
            astd = (sum((a-am)**2 for a in maccs)/max(len(maccs)-1,1))**0.5
            print(f"  {'月均':<10s} {am:>7.1%} {'±':>6s} {astd:>9.1%}")

        # 按情绪一致数分层
        by_sn = defaultdict(list)
        for s in signals:
            by_sn[s['s_up']].append(('UP', s['ret_5d']))
        print(f"\n  按情绪一致数(5日):")
        for sn in sorted(by_sn.keys(), reverse=True):
            ss = calc_stats(by_sn[sn])
            if ss and ss['n'] >= 3:
                print(f"    sent={sn}: 准确率={ss['acc']:.1%}, 期望={ss['pnl']:+.3f}%, n={ss['n']}")

        # 按股价分层
        price_bins = [(0, 10, '<10'), (10, 30, '10~30'), (30, 60, '30~60'), (60, 9999, '>60')]
        print(f"\n  按股价(5日):")
        for lo, hi, lbl in price_bins:
            sub = [('UP', s['ret_5d']) for s in signals if lo <= s['price'] < hi]
            ss = calc_stats(sub)
            if ss and ss['n'] >= 5:
                print(f"    价格{lbl}: 准确率={ss['acc']:.1%}, 期望={ss['pnl']:+.3f}%, n={ss['n']}")

        # 按换手率分层
        turn_bins = [(0, 1, '<1%'), (1, 3, '1~3%'), (3, 8, '3~8%'), (8, 999, '>8%')]
        print(f"\n  按换手率(5日):")
        for lo, hi, lbl in turn_bins:
            sub = [('UP', s['ret_5d']) for s in signals if lo <= s['avg_turn'] < hi]
            ss = calc_stats(sub)
            if ss and ss['n'] >= 5:
                print(f"    换手{lbl}: 准确率={ss['acc']:.1%}, 期望={ss['pnl']:+.3f}%, n={ss['n']}")

        # 按大盘状态分层
        mkt_bins = [(-3, -1, '-3~-1%'), (-1, 0, '-1~0%'), (0, 3, '0~3%')]
        print(f"\n  按大盘20d涨幅(5日):")
        for lo, hi, lbl in mkt_bins:
            sub = [('UP', s['ret_5d']) for s in signals if lo <= s['mkt_ret'] < hi]
            ss = calc_stats(sub)
            if ss and ss['n'] >= 5:
                print(f"    大盘{lbl}: 准确率={ss['acc']:.1%}, 期望={ss['pnl']:+.3f}%, n={ss['n']}")

        # 按置信度分层
        confs = sorted(set(round(s['conf'], 1) for s in signals))
        if len(confs) >= 3:
            print(f"\n  按置信度(5日):")
            # 分3档
            sorted_conf = sorted(s['conf'] for s in signals)
            c33 = sorted_conf[len(sorted_conf)//3]
            c66 = sorted_conf[2*len(sorted_conf)//3]
            for lo, hi, lbl in [(0, c33, '低'), (c33, c66, '中'), (c66, 999, '高')]:
                sub = [('UP', s['ret_5d']) for s in signals if lo <= s['conf'] < hi]
                ss = calc_stats(sub)
                if ss and ss['n'] >= 5:
                    print(f"    置信度{lbl}({lo:.2f}~{hi:.2f}): 准确率={ss['acc']:.1%}, 期望={ss['pnl']:+.3f}%, n={ss['n']}")

        # 信号明细（前20个）
        print(f"\n  信号明细(前20个):")
        print(f"  {'代码':<12s} {'日期':<12s} {'sent':>4s} {'conf':>6s} {'3d':>8s} {'5d':>8s} {'10d':>8s} {'结果':>4s}")
        print(f"  {'─' * 60}")
        for s in signals[:20]:
            r3 = f"{s['ret_3d']:+.2f}%" if s.get('ret_3d') is not None else '—'
            r5 = f"{s['ret_5d']:+.2f}%"
            r10 = f"{s['ret_10d']:+.2f}%" if s.get('ret_10d') is not None else '—'
            ok = '✅' if s['ret_5d'] > 0 else '❌'
            print(f"  {s['code']:<12s} {s['date']:<12s} {s['s_up']:>4d} {s['conf']:>6.2f} {r3:>8s} {r5:>8s} {r10:>8s} {ok:>4s}")

        return {'3d': s3, '5d': s5, '10d': s10, 'monthly_accs': maccs}

    # ── 输出各方法结果 ──
    r_m0 = calc_stats(m0_preds)
    print(f"\n{'═' * 80}")
    print(f"📊 M0: 无规则基线（仅投票过滤+大盘>-3%）")
    print(f"{'═' * 80}")
    if r_m0:
        print(f"  5日准确率: {r_m0['acc']:.1%} ({r_m0['n']}样本), 期望: {r_m0['pnl']:+.3f}%, 盈亏比: {fmt_plr(r_m0)}")

    r_m1 = analyze_signals(m1_signals, "M1: 固定训练(前40%) + 后60%评估 + 规则过滤")
    print(f"\n  规则过滤统计:")
    for reason, cnt in sorted(m1_filtered.items(), key=lambda x: -x[1]):
        print(f"    {reason}: 过滤{cnt}次")

    r_m2 = analyze_signals(m2_signals, "M2: Walk-Forward（每60天重训练，最严格）")
    print(f"  重训练次数: {wf_retrain}")

    r_m4 = analyze_signals(m4_signals, "M4: 扩展窗口（每2月一窗口）")

    # M3 交叉验证结果
    print(f"\n{'═' * 80}")
    print(f"📊 M3: 5折时序交叉验证")
    print(f"{'═' * 80}")
    print(f"  {'折':>4s} {'时段':<24s} {'准确率':>8s} {'样本':>6s} {'期望':>10s}")
    print(f"  {'─' * 56}")
    cv_accs = []
    for fr in m3_fold_results:
        if fr['n'] > 0:
            cv_accs.append(fr['acc'])
            print(f"  {fr['fold']:>4d} {fr.get('period','—'):<24s} {fr['acc']:>7.1%} {fr['n']:>6d} {fr['pnl']:>+9.3f}%")
        else:
            print(f"  {fr['fold']:>4d} {fr.get('period','—'):<24s} {'—':>8s} {0:>6d} {fr.get('note','')}")
    if cv_accs:
        cv_mean = sum(cv_accs)/len(cv_accs)
        cv_std = (sum((a-cv_mean)**2 for a in cv_accs)/max(len(cv_accs)-1,1))**0.5
        print(f"  {'均值':<28s} {cv_mean:>7.1%} {'±':>6s} {cv_std:>9.1%}")


    # ══════════════════════════════════════════════════════════
    # 过拟合诊断
    # ══════════════════════════════════════════════════════════
    print(f"\n{'═' * 80}")
    print("🔍 过拟合诊断")
    print(f"{'═' * 80}")

    accs = {}
    if r_m0: accs['M0_基线'] = r_m0['acc']
    if r_m1 and r_m1.get('5d'): accs['M1_固定训练'] = r_m1['5d']['acc']
    if r_m2 and r_m2.get('5d'): accs['M2_WalkForward'] = r_m2['5d']['acc']
    if cv_accs: accs['M3_交叉验证'] = cv_mean
    if r_m4 and r_m4.get('5d'): accs['M4_扩展窗口'] = r_m4['5d']['acc']

    print(f"\n  1. 各方法5日准确率对比:")
    print(f"  {'方法':<20s} {'准确率':>8s}")
    print(f"  {'─' * 30}")
    for name, acc in accs.items():
        print(f"  {name:<20s} {acc:>7.1%}")

    # 过拟合指标
    vals = list(accs.values())
    if len(vals) >= 2:
        best = max(vals); worst = min(vals)
        spread = best - worst
        mean_acc = sum(vals)/len(vals)
        std_acc = (sum((v-mean_acc)**2 for v in vals)/max(len(vals)-1,1))**0.5
        print(f"\n  2. 过拟合指标:")
        print(f"    方法间最大差距: {spread:.1%}")
        print(f"    方法间标准差:   {std_acc:.1%}")
        print(f"    方法间均值:     {mean_acc:.1%}")

        if spread < 0.03:
            print(f"    判定: ✅ 无过拟合（差距<3%）")
        elif spread < 0.06:
            print(f"    判定: ⚠️ 轻微过拟合（差距3~6%）")
        else:
            print(f"    判定: ❌ 存在过拟合（差距>6%）")

    # 月度稳定性对比
    print(f"\n  3. 月度稳定性:")
    for name, r in [('M1', r_m1), ('M2', r_m2), ('M4', r_m4)]:
        if r and r.get('monthly_accs'):
            ma = r['monthly_accs']
            am = sum(ma)/len(ma)
            astd = (sum((a-am)**2 for a in ma)/max(len(ma)-1,1))**0.5
            min_m = min(ma); max_m = max(ma)
            print(f"    {name}: 月均{am:.1%} ± {astd:.1%}, 范围[{min_m:.1%}, {max_m:.1%}], {len(ma)}个月")
    if cv_accs:
        print(f"    M3: 折均{cv_mean:.1%} ± {cv_std:.1%}, 范围[{min(cv_accs):.1%}, {max(cv_accs):.1%}], {len(cv_accs)}折")

    # ══════════════════════════════════════════════════════════
    # 最终结论
    # ══════════════════════════════════════════════════════════
    print(f"\n{'═' * 80}")
    print("📋 最终结论")
    print(f"{'═' * 80}")

    # 取最保守的准确率作为可信值
    conservative_accs = []
    if r_m2 and r_m2.get('5d'): conservative_accs.append(('Walk-Forward', r_m2['5d']['acc'], r_m2['5d']['n']))
    if cv_accs: conservative_accs.append(('5折CV', cv_mean, sum(fr['n'] for fr in m3_fold_results)))
    if r_m4 and r_m4.get('5d'): conservative_accs.append(('扩展窗口', r_m4['5d']['acc'], r_m4['5d']['n']))

    print(f"\n  {'方法':<16s} {'5日准确率':>10s} {'样本':>8s} {'可信度':>8s}")
    print(f"  {'─' * 46}")
    if r_m1 and r_m1.get('5d'):
        print(f"  {'M1固定训练':<16s} {r_m1['5d']['acc']:>9.1%} {r_m1['5d']['n']:>8d} {'参考':<8s}")
    for name, acc, n in conservative_accs:
        print(f"  {name:<16s} {acc:>9.1%} {n:>8d} {'⭐严格':<8s}")

    if conservative_accs:
        final_acc = min(a for _, a, _ in conservative_accs)
        final_name = [n for n, a, _ in conservative_accs if a == final_acc][0]
        print(f"\n  🎯 最终可信准确率（取最保守）: {final_acc:.1%} ({final_name})")
        if final_acc >= 0.65:
            print(f"     ✅ 达到65%目标，具有实际交易价值")
        elif final_acc >= 0.60:
            print(f"     ⚠️ 60~65%区间，有一定价值但需谨慎")
        else:
            print(f"     ❌ 未达60%，不建议实盘使用")

    # ══════════════════════════════════════════════════════════
    # 保存JSON
    # ══════════════════════════════════════════════════════════
    report = {
        'meta': {
            'n_stocks': len(kdata), 'date_range': f'{start_date}~{end_date}',
            'rules': {'sent_min': SENT_MIN, 'tech_min': TECH_MIN,
                      'mkt_range': [MKT_LOWER, MKT_UPPER],
                      'price_max': PRICE_MAX, 'turn_max': TURN_MAX,
                      'skew_range': [SKEW_LO, SKEW_HI]},
            'run_time': round(time.time()-t0, 1),
        },
        'M0_baseline': r_m0,
        'M1_fixed': {
            '3d': r_m1.get('3d') if r_m1 else None,
            '5d': r_m1.get('5d') if r_m1 else None,
            '10d': r_m1.get('10d') if r_m1 else None,
            'filtered': dict(m1_filtered),
            'n_signals': len(m1_signals),
        },
        'M2_walkforward': {
            '3d': r_m2.get('3d') if r_m2 else None,
            '5d': r_m2.get('5d') if r_m2 else None,
            '10d': r_m2.get('10d') if r_m2 else None,
            'retrain_count': wf_retrain,
            'n_signals': len(m2_signals),
        },
        'M3_cv': {
            'folds': m3_fold_results,
            'mean_acc': round(cv_mean, 4) if cv_accs else None,
            'std_acc': round(cv_std, 4) if cv_accs else None,
        },
        'M4_expanding': {
            '3d': r_m4.get('3d') if r_m4 else None,
            '5d': r_m4.get('5d') if r_m4 else None,
            '10d': r_m4.get('10d') if r_m4 else None,
            'n_signals': len(m4_signals),
        },
        'overfit_diagnosis': {
            'method_accs': accs,
            'spread': round(spread, 4) if len(vals) >= 2 else None,
            'std': round(std_acc, 4) if len(vals) >= 2 else None,
        },
        'signal_details': {
            'M1_first50': [{'code': s['code'], 'date': s['date'], 's_up': s['s_up'],
                            'conf': round(s['conf'], 3), 'ret_5d': s['ret_5d'],
                            'ret_3d': s.get('ret_3d'), 'ret_10d': s.get('ret_10d')}
                           for s in m1_signals[:50]],
            'M2_first50': [{'code': s['code'], 'date': s['date'], 's_up': s['s_up'],
                            'conf': round(s['conf'], 3), 'ret_5d': s['ret_5d'],
                            'ret_3d': s.get('ret_3d'), 'ret_10d': s.get('ret_10d')}
                           for s in m2_signals[:50]],
        },
    }
    out = OUTPUT_DIR / "sentiment_full_validate.json"
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n⏱️  总耗时: {time.time()-t0:.1f}秒")
    print(f"📁 报告: {out}")
    print("=" * 80)
    return report


if __name__ == '__main__':
    run_backtest()
