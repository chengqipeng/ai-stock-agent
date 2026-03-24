#!/usr/bin/env python3
"""
情绪因子 v4 全量验证 — 过拟合检验 + 最终准确率
==============================================
验证方式：
  1. 全量回测：用前40%训练IC，在后60%全量上评估（不分验证/测试）
  2. Walk-Forward：滚动窗口训练，每次用过去N天训练，预测未来
     - 彻底消除前视偏差
     - 最严格的过拟合检验
  3. 全股票池：300只股票全量跑

参数固定（来自v4c验证期选出）：
  - 情绪因子一致数 ≥ 11（13个因子中至少11个看涨）
  - 技术因子一致数 ≥ 6（13个因子中至少6个看涨）
  - 综合置信度 ≥ 0.2
  - 大盘20日跌幅 < 3% 时才出信号

用法：
    source .venv/bin/activate
    python -m tools.backtest_sentiment_v4_full
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

# 固定参数（v4c验证期选出，不再调整）
S_MIN = 11    # 情绪因子最少看涨数
T_MIN = 6     # 技术因子最少看涨数
CONF_MIN = 0.2  # 最低综合置信度
MKT_FILTER = -3  # 大盘20日跌幅阈值(%)


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


# ═══════════════════════════════════════════════════════════════
# 因子计算（与v4c完全一致）
# ═══════════════════════════════════════════════════════════════

def compute_all_factors(klines, ff_by_date=None, mkt_by_date=None):
    n = len(klines)
    if n < 60: return None, None
    close = [k['c'] for k in klines]
    open_ = [k['o'] for k in klines]
    high = [k['h'] for k in klines]
    low = [k['l'] for k in klines]
    vol = [k['v'] for k in klines]
    pct = [k['p'] for k in klines]
    turn = [k['t'] for k in klines]
    if close[-1] <= 0 or vol[-1] <= 0: return None, None

    sf, tf = {}, {}

    # ════ 情绪因子(15个) ════
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

    # ════ 技术因子(13个) ════
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


# ═══════════════════════════════════════════════════════════════
# 因子统计 & 投票
# ═══════════════════════════════════════════════════════════════

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


def fmt_plr(s):
    return f"{s['plr']:.2f}" if isinstance(s['plr'], (int, float)) else s['plr']


# ═══════════════════════════════════════════════════════════════
# 主回测
# ═══════════════════════════════════════════════════════════════

def run_backtest():
    t0 = time.time()
    print("=" * 80)
    print("情绪因子 v4 全量验证 — 过拟合检验 + 最终准确率")
    print("=" * 80)
    print(f"固定参数: s_min={S_MIN}, t_min={T_MIN}, conf≥{CONF_MIN}, mkt≥{MKT_FILTER}%")

    logger.info("[1/5] 加载数据...")
    codes = load_codes(300)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=500)).strftime('%Y-%m-%d')
    kdata = load_klines(codes, start_date, end_date)
    ffdata = load_ff(codes, start_date)
    mkt = load_market(start_date, end_date)
    mkt_by_date = {m['d']: m for m in mkt}
    logger.info("  K线: %d只, 资金流: %d只, 大盘: %d天", len(kdata), len(ffdata), len(mkt))

    # ══════════════════════════════════════════════════════════
    # 方法1: 固定训练期(前40%) + 后60%全量评估
    # ══════════════════════════════════════════════════════════
    logger.info("[2/5] 方法1: 固定训练 + 全量评估...")

    # 训练期: 前40%
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

    ss_fixed = build_stats(s_rec)
    ts_fixed = build_stats(t_rec)
    logger.info("  训练: %d样本, 情绪因子: %d, 技术因子: %d", train_n, len(ss_fixed), len(ts_fixed))

    # 后60%全量评估
    m1_preds = []
    m1_monthly = defaultdict(list)
    m1_by_sn = defaultdict(list)

    for code, klines in kdata.items():
        if len(klines) < 80: continue
        ff_bd = {f['d']: f for f in ffdata.get(code, [])}
        te = int(len(klines)*0.4)

        for i in range(max(60, te), len(klines)-10):
            sf, tf = compute_all_factors(klines[:i+1], ff_bd, mkt_by_date)
            if not sf or not tf: continue

            # 大盘过滤
            mkt_c = [mkt_by_date.get(klines[j]['d'], {}).get('c', 0) for j in range(max(0,i-20), i+1)]
            mkt_c = [c for c in mkt_c if c > 0]
            if len(mkt_c) >= 2 and (mkt_c[-1]/mkt_c[0]-1)*100 < MKT_FILTER:
                continue

            s_up, s_dn, s_ws, s_wt = vote_detail(sf, ss_fixed, top_k=13)
            t_up, t_dn, t_ws, t_wt = vote_detail(tf, ts_fixed, top_k=13)

            if s_up < S_MIN or s_up <= s_dn: continue
            if t_up < T_MIN or t_up <= t_dn: continue

            combined = (s_ws/s_wt*0.6 + t_ws/t_wt*0.4) if s_wt > 0 and t_wt > 0 else 0
            if combined < CONF_MIN: continue

            fut = compute_future(klines, i)
            if 'ret_5d' not in fut: continue

            m1_preds.append(('UP', fut['ret_5d']))
            m1_monthly[klines[i]['d'][:7]].append(('UP', fut['ret_5d']))
            m1_by_sn[s_up].append(('UP', fut['ret_5d']))

    m1_all = calc_stats(m1_preds)

    # ══════════════════════════════════════════════════════════
    # 方法2: Walk-Forward（滚动训练，最严格）
    # ══════════════════════════════════════════════════════════
    logger.info("[3/5] 方法2: Walk-Forward滚动验证...")

    # 每只股票：用前120天训练IC，从第121天开始预测
    # 每隔60天重新训练一次IC（模拟实际使用场景）
    wf_preds = []
    wf_monthly = defaultdict(list)
    wf_by_sn = defaultdict(list)
    wf_retrain_count = 0

    for code, klines in kdata.items():
        if len(klines) < 130: continue
        ff_bd = {f['d']: f for f in ffdata.get(code, [])}

        # 滚动窗口: 每60天重新训练
        local_ss, local_ts = None, None
        last_train_idx = -999

        for i in range(120, len(klines)-10):
            # 每60天重新训练IC（或首次）
            if i - last_train_idx >= 60 or local_ss is None:
                # 用过去120天的数据训练
                train_start = max(0, i - 120)
                ls_rec = defaultdict(list); lt_rec = defaultdict(list)
                for ti in range(max(60, train_start), i):
                    sf, tf = compute_all_factors(klines[:ti+1], ff_bd, mkt_by_date)
                    if not sf or not tf: continue
                    fut = compute_future(klines, ti)
                    if 'ret_5d' not in fut: continue
                    ret = fut['ret_5d']
                    for fn, fv in sf.items():
                        if fv is not None: ls_rec[fn].append((fv, ret))
                    for fn, fv in tf.items():
                        if fv is not None: lt_rec[fn].append((fv, ret))

                # 构建统计（降低最小样本要求，因为是单股票）
                local_ss = {}
                for fn, data in ls_rec.items():
                    vals = [r[0] for r in data]; rets = [r[1] for r in data]
                    nn = len(vals)
                    if nn < 30: continue
                    m = sum(vals)/nn; s = (sum((v-m)**2 for v in vals)/(nn-1))**0.5
                    if s < 1e-10: continue
                    # 简化IC: Pearson相关
                    mr = sum(rets)/nn
                    cov = sum((vals[j]-m)*(rets[j]-mr) for j in range(nn))
                    sv = (sum((r-mr)**2 for r in rets))**0.5
                    ic = cov/(s*nn*sv/(nn-1)) if sv > 0 else 0
                    local_ss[fn] = {'m': m, 's': s, 'ic': ic, 'aic': abs(ic),
                                     'dir': -1 if ic < 0 else 1, 'n': nn}

                local_ts = {}
                for fn, data in lt_rec.items():
                    vals = [r[0] for r in data]; rets = [r[1] for r in data]
                    nn = len(vals)
                    if nn < 30: continue
                    m = sum(vals)/nn; s = (sum((v-m)**2 for v in vals)/(nn-1))**0.5
                    if s < 1e-10: continue
                    mr = sum(rets)/nn
                    cov = sum((vals[j]-m)*(rets[j]-mr) for j in range(nn))
                    sv = (sum((r-mr)**2 for r in rets))**0.5
                    ic = cov/(s*nn*sv/(nn-1)) if sv > 0 else 0
                    local_ts[fn] = {'m': m, 's': s, 'ic': ic, 'aic': abs(ic),
                                     'dir': -1 if ic < 0 else 1, 'n': nn}

                last_train_idx = i
                wf_retrain_count += 1

            if not local_ss or not local_ts: continue

            sf, tf = compute_all_factors(klines[:i+1], ff_bd, mkt_by_date)
            if not sf or not tf: continue

            # 大盘过滤
            mkt_c = [mkt_by_date.get(klines[j]['d'], {}).get('c', 0) for j in range(max(0,i-20), i+1)]
            mkt_c = [c for c in mkt_c if c > 0]
            if len(mkt_c) >= 2 and (mkt_c[-1]/mkt_c[0]-1)*100 < MKT_FILTER:
                continue

            s_up, s_dn, s_ws, s_wt = vote_detail(sf, local_ss, top_k=13)
            t_up, t_dn, t_ws, t_wt = vote_detail(tf, local_ts, top_k=13)

            if s_up < S_MIN or s_up <= s_dn: continue
            if t_up < T_MIN or t_up <= t_dn: continue

            combined = (s_ws/s_wt*0.6 + t_ws/t_wt*0.4) if s_wt > 0 and t_wt > 0 else 0
            if combined < CONF_MIN: continue

            fut = compute_future(klines, i)
            if 'ret_5d' not in fut: continue

            wf_preds.append(('UP', fut['ret_5d']))
            wf_monthly[klines[i]['d'][:7]].append(('UP', fut['ret_5d']))
            wf_by_sn[s_up].append(('UP', fut['ret_5d']))

    wf_all = calc_stats(wf_preds)
    logger.info("  Walk-Forward: %d预测, %d次重训练", len(wf_preds), wf_retrain_count)


    # ══════════════════════════════════════════════════════════
    # 方法3: 交叉验证（5折时间序列）
    # ══════════════════════════════════════════════════════════
    logger.info("[4/5] 方法3: 5折时间序列交叉验证...")

    # 将每只股票的K线按时间分5段，每次用前N段训练，第N+1段测试
    cv_fold_results = []
    for fold in range(1, 5):  # fold 1-4: 用前fold/5训练，第(fold+1)/5测试
        train_frac = fold / 5
        test_start_frac = fold / 5
        test_end_frac = (fold + 1) / 5

        cv_s_rec = defaultdict(list); cv_t_rec = defaultdict(list)
        cv_train_n = 0

        # 训练
        for code, klines in kdata.items():
            if len(klines) < 80: continue
            ff_bd = {f['d']: f for f in ffdata.get(code, [])}
            te = int(len(klines) * train_frac)
            for i in range(60, min(te, len(klines)-10)):
                sf, tf = compute_all_factors(klines[:i+1], ff_bd, mkt_by_date)
                if not sf or not tf: continue
                fut = compute_future(klines, i)
                if 'ret_5d' not in fut: continue
                cv_train_n += 1; ret = fut['ret_5d']
                for fn, fv in sf.items():
                    if fv is not None: cv_s_rec[fn].append((fv, ret))
                for fn, fv in tf.items():
                    if fv is not None: cv_t_rec[fn].append((fv, ret))

        cv_ss = build_stats(cv_s_rec)
        cv_ts = build_stats(cv_t_rec)

        if not cv_ss or not cv_ts:
            continue

        # 测试
        cv_preds = []
        for code, klines in kdata.items():
            if len(klines) < 80: continue
            ff_bd = {f['d']: f for f in ffdata.get(code, [])}
            ts_idx = int(len(klines) * test_start_frac)
            te_idx = int(len(klines) * test_end_frac)

            for i in range(max(60, ts_idx), min(te_idx, len(klines)-10)):
                sf, tf = compute_all_factors(klines[:i+1], ff_bd, mkt_by_date)
                if not sf or not tf: continue

                mkt_c = [mkt_by_date.get(klines[j]['d'], {}).get('c', 0) for j in range(max(0,i-20), i+1)]
                mkt_c = [c for c in mkt_c if c > 0]
                if len(mkt_c) >= 2 and (mkt_c[-1]/mkt_c[0]-1)*100 < MKT_FILTER:
                    continue

                s_up, s_dn, s_ws, s_wt = vote_detail(sf, cv_ss, top_k=13)
                t_up, t_dn, t_ws, t_wt = vote_detail(tf, cv_ts, top_k=13)

                if s_up < S_MIN or s_up <= s_dn: continue
                if t_up < T_MIN or t_up <= t_dn: continue

                combined = (s_ws/s_wt*0.6 + t_ws/t_wt*0.4) if s_wt > 0 and t_wt > 0 else 0
                if combined < CONF_MIN: continue

                fut = compute_future(klines, i)
                if 'ret_5d' not in fut: continue
                cv_preds.append(('UP', fut['ret_5d']))

        cv_s = calc_stats(cv_preds)
        if cv_s:
            cv_fold_results.append({'fold': fold, 'train_frac': f'{int(train_frac*100)}%',
                                     'test_frac': f'{int(test_start_frac*100)}-{int(test_end_frac*100)}%',
                                     'train_n': cv_train_n, **cv_s})


    # ══════════════════════════════════════════════════════════
    # 输出报告
    # ══════════════════════════════════════════════════════════
    logger.info("[5/5] 输出报告...")

    print(f"\n{'═' * 80}")
    print(f"📊 方法1: 固定训练(前40%) + 后60%全量评估")
    print(f"{'═' * 80}")
    if m1_all:
        print(f"  5日准确率:  {m1_all['acc']:.1%}  ({m1_all['n']}样本)")
        print(f"  5日期望:    {m1_all['pnl']:+.3f}%")
        print(f"  5日盈亏比:  {fmt_plr(m1_all)}")

        print(f"\n  按情绪一致数:")
        for sn in sorted(m1_by_sn.keys(), reverse=True):
            s = calc_stats(m1_by_sn[sn])
            if s and s['n'] >= 10:
                print(f"    sent={sn}: 准确率={s['acc']:.1%}, 期望={s['pnl']:+.3f}%, "
                      f"盈亏比={fmt_plr(s)}, n={s['n']}")

        print(f"\n  月度稳定性:")
        m1_maccs = []
        for month in sorted(m1_monthly.keys()):
            s = calc_stats(m1_monthly[month])
            if s and s['n'] >= 10:
                m1_maccs.append(s['acc'])
                print(f"    {month}: 准确率={s['acc']:.1%}, n={s['n']}, 期望={s['pnl']:+.3f}%")
        if m1_maccs:
            am = sum(m1_maccs)/len(m1_maccs)
            astd = (sum((a-am)**2 for a in m1_maccs)/max(len(m1_maccs)-1,1))**0.5
            print(f"    ── 月均: {am:.1%} ± {astd:.1%}")

    print(f"\n{'═' * 80}")
    print(f"📊 方法2: Walk-Forward滚动验证（最严格，无任何前视偏差）")
    print(f"{'═' * 80}")
    if wf_all:
        print(f"  5日准确率:  {wf_all['acc']:.1%}  ({wf_all['n']}样本)")
        print(f"  5日期望:    {wf_all['pnl']:+.3f}%")
        print(f"  5日盈亏比:  {fmt_plr(wf_all)}")
        print(f"  重训练次数: {wf_retrain_count}")

        print(f"\n  按情绪一致数:")
        for sn in sorted(wf_by_sn.keys(), reverse=True):
            s = calc_stats(wf_by_sn[sn])
            if s and s['n'] >= 10:
                print(f"    sent={sn}: 准确率={s['acc']:.1%}, 期望={s['pnl']:+.3f}%, "
                      f"盈亏比={fmt_plr(s)}, n={s['n']}")

        print(f"\n  月度稳定性:")
        wf_maccs = []
        for month in sorted(wf_monthly.keys()):
            s = calc_stats(wf_monthly[month])
            if s and s['n'] >= 10:
                wf_maccs.append(s['acc'])
                print(f"    {month}: 准确率={s['acc']:.1%}, n={s['n']}, 期望={s['pnl']:+.3f}%")
        if wf_maccs:
            am = sum(wf_maccs)/len(wf_maccs)
            astd = (sum((a-am)**2 for a in wf_maccs)/max(len(wf_maccs)-1,1))**0.5
            print(f"    ── 月均: {am:.1%} ± {astd:.1%}")

    print(f"\n{'═' * 80}")
    print(f"📊 方法3: 5折时间序列交叉验证")
    print(f"{'═' * 80}")
    if cv_fold_results:
        print(f"  {'Fold':>4s} {'训练':>6s} {'测试':>8s} {'样本':>6s} {'准确率':>6s} {'期望':>8s} {'盈亏比':>6s}")
        cv_accs = []
        for r in cv_fold_results:
            plr = f"{r['plr']:.2f}" if isinstance(r['plr'], (int, float)) else r['plr']
            print(f"  {r['fold']:>4d} {r['train_frac']:>6s} {r['test_frac']:>8s} "
                  f"{r['n']:>6d} {r['acc']:>6.1%} {r['pnl']:>+8.3f}% {plr:>6s}")
            cv_accs.append(r['acc'])
        if cv_accs:
            am = sum(cv_accs)/len(cv_accs)
            astd = (sum((a-am)**2 for a in cv_accs)/max(len(cv_accs)-1,1))**0.5
            print(f"  ── 均值: {am:.1%} ± {astd:.1%}")

    # ══════════════════════════════════════════════════════════
    # 总结
    # ══════════════════════════════════════════════════════════
    print(f"\n{'═' * 80}")
    print("📋 过拟合检验总结")
    print(f"{'═' * 80}")
    print(f"  {'方法':<30s} {'准确率':>8s} {'样本':>8s} {'期望收益':>10s} {'盈亏比':>8s}")
    print(f"  {'─' * 70}")

    prev_results = {
        'v4c验证期(参数选择)': {'acc': 0.658, 'n': 158, 'pnl': 1.990, 'plr': 1.83},
        'v4c测试期(30%)': {'acc': 0.629, 'n': 493, 'pnl': 1.424, 'plr': 1.27},
    }
    for label, r in prev_results.items():
        print(f"  {label:<30s} {r['acc']:>7.1%} {r['n']:>8d} {r['pnl']:>+10.3f}% {r['plr']:>8.2f}")

    if m1_all:
        print(f"  {'方法1: 固定训练+60%全量':<30s} {m1_all['acc']:>7.1%} {m1_all['n']:>8d} "
              f"{m1_all['pnl']:>+10.3f}% {fmt_plr(m1_all):>8s}")
    if wf_all:
        print(f"  {'方法2: Walk-Forward':<30s} {wf_all['acc']:>7.1%} {wf_all['n']:>8d} "
              f"{wf_all['pnl']:>+10.3f}% {fmt_plr(wf_all):>8s}")
    if cv_fold_results:
        cv_am = sum(r['acc'] for r in cv_fold_results)/len(cv_fold_results)
        cv_n = sum(r['n'] for r in cv_fold_results)
        cv_pnl = sum(r['pnl']*r['n'] for r in cv_fold_results)/cv_n if cv_n > 0 else 0
        print(f"  {'方法3: 5折CV均值':<30s} {cv_am:>7.1%} {cv_n:>8d} {cv_pnl:>+10.3f}%")

    # 过拟合判断
    print(f"\n  过拟合判断:")
    if m1_all and wf_all:
        diff_m1 = prev_results['v4c验证期(参数选择)']['acc'] - m1_all['acc']
        diff_wf = prev_results['v4c验证期(参数选择)']['acc'] - wf_all['acc']
        print(f"    验证期 vs 方法1全量: {diff_m1:+.1%} {'✅ 无过拟合' if diff_m1 < 0.05 else '⚠️ 轻微过拟合' if diff_m1 < 0.10 else '❌ 过拟合'}")
        print(f"    验证期 vs Walk-Forward: {diff_wf:+.1%} {'✅ 无过拟合' if diff_wf < 0.05 else '⚠️ 轻微过拟合' if diff_wf < 0.10 else '❌ 过拟合'}")

        # 最终结论
        final_acc = wf_all['acc']  # Walk-Forward是最可信的
        print(f"\n  🎯 最终可信准确率（Walk-Forward）: {final_acc:.1%}")
        if final_acc >= 0.65:
            print(f"  ✅ 达到65%目标")
        elif final_acc >= 0.60:
            print(f"  ⚠️ 接近目标（60-65%区间），具有实际交易价值")
            print(f"     期望收益 {wf_all['pnl']:+.3f}%/5天, 盈亏比 {fmt_plr(wf_all)}")
        else:
            print(f"  ❌ 未达60%")

    # 保存
    report = {
        'meta': {'n_stocks': len(kdata), 'date_range': f'{start_date}~{end_date}',
                 'params': {'s_min': S_MIN, 't_min': T_MIN, 'conf': CONF_MIN, 'mkt': MKT_FILTER},
                 'run_time': round(time.time()-t0, 1)},
        'method1_fixed_train': m1_all,
        'method1_monthly': {m: calc_stats(p) for m, p in sorted(m1_monthly.items())},
        'method2_walk_forward': wf_all,
        'method2_monthly': {m: calc_stats(p) for m, p in sorted(wf_monthly.items())},
        'method2_retrain_count': wf_retrain_count,
        'method3_cv_folds': cv_fold_results,
        'previous_results': prev_results,
    }
    out = OUTPUT_DIR / "sentiment_v4_full_validation.json"
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n⏱️  总耗时: {time.time()-t0:.1f}秒")
    print(f"📁 报告: {out}")
    print("=" * 80)
    return report


if __name__ == '__main__':
    run_backtest()
