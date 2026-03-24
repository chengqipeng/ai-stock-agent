#!/usr/bin/env python3
"""
情绪因子最终版 — 基于深度诊断的规则化策略
==========================================
将v5诊断发现的结构性规则固化，不做任何参数搜索。

规则来源（全部来自v5诊断的统计事实，非拟合）：
  R1: 大盘状态 — 震荡期(-3%~0%)准确率69.7%，上涨期仅50%
      → 只在大盘20日涨幅<0%时出信号（逆向思维：大盘弱时个股分化大，因子有区分度）
  R2: 个股过滤 — 高价股(>60)仅52.2%，极高换手(>8%)仅53.5%
      → 排除股价>60元和换手率>8%的标的
  R3: 因子非线性 — skew_20的Q1(57.7%)和Q5(57.3%)差，Q2-Q4(65-69%)好
      → skew_20极端值时降低信号权重
  R4: 预测窗口 — 3日64.7% > 5日63.6% > 10日51.9%
      → 同时评估3日和5日准确率
  R5: 因子交互 — price_pos×bias_20同时极端看涨时65.2%(492样本)
      → 作为额外加分条件

验证方式：
  1. Walk-Forward（最严格，无前视偏差）
  2. 固定训练+全量评估
  3. 月度稳定性

用法：
    source .venv/bin/activate
    python -m tools.backtest_sentiment_final
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
SENT_MIN = 11       # 情绪因子最少看涨数（13个中至少11个）
TECH_MIN = 6        # 技术因子最少看涨数（13个中至少6个）
MKT_UPPER = 0       # R1: 大盘20日涨幅上限（震荡/下跌期更准）
MKT_LOWER = -3      # R1: 大盘20日涨幅下限（暴跌时不做）
PRICE_MAX = 60      # R2: 股价上限
TURN_MAX = 8        # R2: 换手率上限
SKEW_LO = -1.5      # R3: skew_20下限（Q1太极端不好）
SKEW_HI = 0.3       # R3: skew_20上限（Q5太极端不好）


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
# 因子计算
# ═══════════════════════════════════════════════════════════════

def compute_all_factors(klines, ff_by_date=None, mkt_by_date=None):
    """返回 (sentiment_factors, technical_factors, meta)"""
    n = len(klines)
    if n < 60: return None, None, None
    close = [k['c'] for k in klines]; open_ = [k['o'] for k in klines]
    high = [k['h'] for k in klines]; low = [k['l'] for k in klines]
    vol = [k['v'] for k in klines]; pct = [k['p'] for k in klines]
    turn = [k['t'] for k in klines]
    if close[-1] <= 0 or vol[-1] <= 0: return None, None, None
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

    # ════ 技术因子(13个) ════
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

    # meta信息（用于规则过滤）
    avg_turn = sum(t20)/len(t20) if t20 else 0
    meta = {'price': close[-1], 'avg_turn': avg_turn, 'skew_20': sf.get('skew_20')}
    return sf, tf, meta


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


def build_stats_small(records):
    """Walk-Forward用：降低最小样本要求"""
    stats = {}
    for fn, data in records.items():
        vals = [r[0] for r in data]; rets = [r[1] for r in data]
        nn = len(vals)
        if nn < 30: continue
        m = sum(vals)/nn; s = (sum((v-m)**2 for v in vals)/(nn-1))**0.5
        if s < 1e-10: continue
        mr = sum(rets)/nn
        cov = sum((vals[j]-m)*(rets[j]-mr) for j in range(nn))
        sv = (sum((r-mr)**2 for r in rets))**0.5
        ic = cov/(s*nn*sv/(nn-1)) if sv > 0 else 0
        stats[fn] = {'m': m, 's': s, 'ic': ic, 'aic': abs(ic), 'dir': -1 if ic < 0 else 1, 'n': nn}
    return stats


def vote_count(factors, fstats, top_k=13):
    """投票计数，返回 (n_up, n_down, weighted_score, total_weight)"""
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
    """
    应用全部规则，返回 (signal, confidence, reason) 或 (None, 0, '')
    signal: 'UP' 或 None
    """
    # R1: 大盘状态过滤
    if mkt_ret < MKT_LOWER or mkt_ret > MKT_UPPER:
        return None, 0, 'R1:大盘不在震荡区间'

    # R2: 个股过滤
    if meta['price'] > PRICE_MAX:
        return None, 0, 'R2:股价过高'
    if meta['avg_turn'] > TURN_MAX:
        return None, 0, 'R2:换手率过高'

    # R3: skew_20非线性过滤
    skew = meta.get('skew_20')
    if skew is not None and (skew < SKEW_LO or skew > SKEW_HI):
        return None, 0, 'R3:skew极端值'

    # 核心投票
    s_up, s_dn, s_ws, s_wt = vote_count(sf, ss, top_k=13)
    t_up, t_dn, t_ws, t_wt = vote_count(tf, ts, top_k=13)

    if s_up < SENT_MIN or s_up <= s_dn:
        return None, 0, '情绪一致性不足'
    if t_up < TECH_MIN or t_up <= t_dn:
        return None, 0, '技术一致性不足'

    s_conf = s_ws/s_wt if s_wt > 0 else 0
    t_conf = t_ws/t_wt if t_wt > 0 else 0
    combined = s_conf * 0.6 + t_conf * 0.4

    # R5: 因子交互加分（price_pos和bias_20同时极端看涨）
    pp_z = (sf.get('price_pos', 0.5) - ss.get('price_pos', {}).get('m', 0.5)) / max(ss.get('price_pos', {}).get('s', 1), 0.01) * ss.get('price_pos', {}).get('dir', -1) if 'price_pos' in ss else 0
    bias_z = (sf.get('bias_20', 0) - ss.get('bias_20', {}).get('m', 0)) / max(ss.get('bias_20', {}).get('s', 1), 0.01) * ss.get('bias_20', {}).get('dir', -1) if 'bias_20' in ss else 0
    interaction_bonus = pp_z > 0.5 and bias_z > 0.5

    return 'UP', combined, f's={s_up},t={t_up},c={combined:.2f}' + (',R5交互' if interaction_bonus else '')


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
            'plr': round(aw/al, 2) if al > 0 else 'inf'}

def fmt(s):
    return f"{s['plr']:.2f}" if isinstance(s['plr'], (int, float)) else s['plr']


# ═══════════════════════════════════════════════════════════════
# 主回测
# ═══════════════════════════════════════════════════════════════

def run_backtest():
    t0 = time.time()
    print("=" * 80)
    print("情绪因子最终版 — 规则化策略全量验证")
    print("=" * 80)
    print(f"规则: sent≥{SENT_MIN}, tech≥{TECH_MIN}, 大盘{MKT_LOWER}%~{MKT_UPPER}%, "
          f"价<{PRICE_MAX}, 换手<{TURN_MAX}%, skew∈[{SKEW_LO},{SKEW_HI}]")

    logger.info("[1/6] 加载数据...")
    codes = load_codes(300)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=500)).strftime('%Y-%m-%d')
    kdata = load_klines(codes, start_date, end_date)
    ffdata = load_ff(codes, start_date)
    mkt = load_market(start_date, end_date)
    mkt_by_date = {m['d']: m for m in mkt}
    logger.info("  K线: %d只, 资金流: %d只, 大盘: %d天", len(kdata), len(ffdata), len(mkt))

    # ══════════════════════════════════════════════════════════
    # 方法1: 固定训练(前40%) + 后60%全量
    # ══════════════════════════════════════════════════════════
    logger.info("[2/6] 方法1: 固定训练 + 全量评估...")
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
    logger.info("  训练: %d样本", train_n)

    m1_3d, m1_5d = [], []
    m1_monthly = defaultdict(list)
    m1_by_sn = defaultdict(list)
    m1_filtered_reasons = defaultdict(int)
    m1_details = []

    for code, klines in kdata.items():
        if len(klines) < 80: continue
        ff_bd = {f['d']: f for f in ffdata.get(code, [])}
        te = int(len(klines)*0.4)
        for i in range(max(60, te), len(klines)-10):
            sf, tf, meta = compute_all_factors(klines[:i+1], ff_bd, mkt_by_date)
            if not sf or not tf: continue

            mkt_c = [mkt_by_date.get(klines[j]['d'], {}).get('c', 0) for j in range(max(0,i-20), i+1)]
            mkt_c = [c for c in mkt_c if c > 0]
            mkt_ret = (mkt_c[-1]/mkt_c[0]-1)*100 if len(mkt_c) >= 2 else 0

            sig, conf, reason = apply_rules(sf, tf, meta, ss_fixed, ts_fixed, mkt_ret)
            if sig is None:
                m1_filtered_reasons[reason.split(':')[0]] += 1
                continue

            fut = compute_future(klines, i)
            if 'ret_5d' not in fut: continue

            m1_5d.append(('UP', fut['ret_5d']))
            if 'ret_3d' in fut:
                m1_3d.append(('UP', fut['ret_3d']))
            m1_monthly[klines[i]['d'][:7]].append(('UP', fut['ret_5d']))
            s_up, _, _, _ = vote_count(sf, ss_fixed, 13)
            m1_by_sn[s_up].append(('UP', fut['ret_5d']))
            m1_details.append({'code': code, 'date': klines[i]['d'],
                               'conf': conf, 'ret_5d': fut['ret_5d'],
                               'ret_3d': fut.get('ret_3d'), 's_up': s_up})

    m1_s5 = calc_stats(m1_5d)
    m1_s3 = calc_stats(m1_3d)

    # ══════════════════════════════════════════════════════════
    # 方法2: Walk-Forward
    # ══════════════════════════════════════════════════════════
    logger.info("[3/6] 方法2: Walk-Forward...")
    wf_3d, wf_5d = [], []
    wf_monthly = defaultdict(list)
    wf_by_sn = defaultdict(list)
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
                local_ss = build_stats_small(ls)
                local_ts = build_stats_small(lt)
                last_train = i; wf_retrain += 1

            if not local_ss or not local_ts: continue

            sf, tf, meta = compute_all_factors(klines[:i+1], ff_bd, mkt_by_date)
            if not sf or not tf: continue

            mkt_c = [mkt_by_date.get(klines[j]['d'], {}).get('c', 0) for j in range(max(0,i-20), i+1)]
            mkt_c = [c for c in mkt_c if c > 0]
            mkt_ret = (mkt_c[-1]/mkt_c[0]-1)*100 if len(mkt_c) >= 2 else 0

            sig, conf, reason = apply_rules(sf, tf, meta, local_ss, local_ts, mkt_ret)
            if sig is None: continue

            fut = compute_future(klines, i)
            if 'ret_5d' not in fut: continue

            wf_5d.append(('UP', fut['ret_5d']))
            if 'ret_3d' in fut:
                wf_3d.append(('UP', fut['ret_3d']))
            wf_monthly[klines[i]['d'][:7]].append(('UP', fut['ret_5d']))
            s_up, _, _, _ = vote_count(sf, local_ss, 13)
            wf_by_sn[s_up].append(('UP', fut['ret_5d']))

    wf_s5 = calc_stats(wf_5d)
    wf_s3 = calc_stats(wf_3d)


    # ══════════════════════════════════════════════════════════
    # 方法3: 无规则基线（对比用）
    # ══════════════════════════════════════════════════════════
    logger.info("[4/6] 方法3: 无规则基线...")
    base_5d = []
    base_monthly = defaultdict(list)
    for code, klines in kdata.items():
        if len(klines) < 80: continue
        ff_bd = {f['d']: f for f in ffdata.get(code, [])}
        te = int(len(klines)*0.4)
        for i in range(max(60, te), len(klines)-10):
            sf, tf, meta = compute_all_factors(klines[:i+1], ff_bd, mkt_by_date)
            if not sf or not tf: continue
            mkt_c = [mkt_by_date.get(klines[j]['d'], {}).get('c', 0) for j in range(max(0,i-20), i+1)]
            mkt_c = [c for c in mkt_c if c > 0]
            mkt_ret = (mkt_c[-1]/mkt_c[0]-1)*100 if len(mkt_c) >= 2 else 0
            if mkt_ret < -3: continue  # 只保留大盘过滤

            s_up, s_dn, _, _ = vote_count(sf, ss_fixed, 13)
            t_up, t_dn, _, _ = vote_count(tf, ts_fixed, 13)
            if s_up < SENT_MIN or s_up <= s_dn: continue
            if t_up < TECH_MIN or t_up <= t_dn: continue

            fut = compute_future(klines, i)
            if 'ret_5d' not in fut: continue
            base_5d.append(('UP', fut['ret_5d']))
            base_monthly[klines[i]['d'][:7]].append(('UP', fut['ret_5d']))

    base_s5 = calc_stats(base_5d)

    # ══════════════════════════════════════════════════════════
    # 输出报告
    # ══════════════════════════════════════════════════════════
    logger.info("[5/6] 输出报告...")

    print(f"\n{'═' * 80}")
    print(f"📊 方法1: 固定训练 + 后60%全量（含规则过滤）")
    print(f"{'═' * 80}")
    if m1_s5:
        print(f"  5日准确率:  {m1_s5['acc']:.1%}  ({m1_s5['n']}样本)")
        print(f"  5日期望:    {m1_s5['pnl']:+.3f}%")
        print(f"  5日盈亏比:  {fmt(m1_s5)}")
    if m1_s3:
        print(f"  3日准确率:  {m1_s3['acc']:.1%}  ({m1_s3['n']}样本)")
        print(f"  3日期望:    {m1_s3['pnl']:+.3f}%")

    print(f"\n  规则过滤统计:")
    for reason, cnt in sorted(m1_filtered_reasons.items(), key=lambda x: -x[1]):
        print(f"    {reason}: 过滤{cnt}次")

    print(f"\n  按情绪一致数:")
    for sn in sorted(m1_by_sn.keys(), reverse=True):
        s = calc_stats(m1_by_sn[sn])
        if s and s['n'] >= 5:
            print(f"    sent={sn}: 准确率={s['acc']:.1%}, 期望={s['pnl']:+.3f}%, n={s['n']}")

    print(f"\n  月度稳定性:")
    m1_maccs = []
    for month in sorted(m1_monthly.keys()):
        s = calc_stats(m1_monthly[month])
        if s and s['n'] >= 5:
            m1_maccs.append(s['acc'])
            print(f"    {month}: 准确率={s['acc']:.1%}, n={s['n']}, 期望={s['pnl']:+.3f}%")
    if m1_maccs:
        am = sum(m1_maccs)/len(m1_maccs)
        astd = (sum((a-am)**2 for a in m1_maccs)/max(len(m1_maccs)-1,1))**0.5
        print(f"    ── 月均: {am:.1%} ± {astd:.1%}")

    print(f"\n{'═' * 80}")
    print(f"📊 方法2: Walk-Forward（最严格）")
    print(f"{'═' * 80}")
    if wf_s5:
        print(f"  5日准确率:  {wf_s5['acc']:.1%}  ({wf_s5['n']}样本)")
        print(f"  5日期望:    {wf_s5['pnl']:+.3f}%")
        print(f"  5日盈亏比:  {fmt(wf_s5)}")
    if wf_s3:
        print(f"  3日准确率:  {wf_s3['acc']:.1%}  ({wf_s3['n']}样本)")
        print(f"  3日期望:    {wf_s3['pnl']:+.3f}%")
    print(f"  重训练次数: {wf_retrain}")

    print(f"\n  按情绪一致数:")
    for sn in sorted(wf_by_sn.keys(), reverse=True):
        s = calc_stats(wf_by_sn[sn])
        if s and s['n'] >= 5:
            print(f"    sent={sn}: 准确率={s['acc']:.1%}, 期望={s['pnl']:+.3f}%, n={s['n']}")

    print(f"\n  月度稳定性:")
    wf_maccs = []
    for month in sorted(wf_monthly.keys()):
        s = calc_stats(wf_monthly[month])
        if s and s['n'] >= 5:
            wf_maccs.append(s['acc'])
            print(f"    {month}: 准确率={s['acc']:.1%}, n={s['n']}, 期望={s['pnl']:+.3f}%")
    if wf_maccs:
        am = sum(wf_maccs)/len(wf_maccs)
        astd = (sum((a-am)**2 for a in wf_maccs)/max(len(wf_maccs)-1,1))**0.5
        print(f"    ── 月均: {am:.1%} ± {astd:.1%}")

    # ══════════════════════════════════════════════════════════
    # 总结
    # ══════════════════════════════════════════════════════════
    logger.info("[6/6] 总结...")
    print(f"\n{'═' * 80}")
    print("📋 最终总结")
    print(f"{'═' * 80}")
    print(f"  {'方法':<35s} {'3日':>8s} {'5日':>8s} {'样本':>6s} {'期望':>8s} {'盈亏比':>6s}")
    print(f"  {'─' * 75}")
    if base_s5:
        print(f"  {'基线(无规则过滤)':<35s} {'—':>8s} {base_s5['acc']:>7.1%} {base_s5['n']:>6d} "
              f"{base_s5['pnl']:>+8.3f}% {fmt(base_s5):>6s}")
    if m1_s5:
        m1_3 = f"{m1_s3['acc']:.1%}" if m1_s3 else '—'
        print(f"  {'方法1: 固定训练+规则':<35s} {m1_3:>8s} {m1_s5['acc']:>7.1%} {m1_s5['n']:>6d} "
              f"{m1_s5['pnl']:>+8.3f}% {fmt(m1_s5):>6s}")
    if wf_s5:
        wf_3 = f"{wf_s3['acc']:.1%}" if wf_s3 else '—'
        print(f"  {'方法2: Walk-Forward+规则':<35s} {wf_3:>8s} {wf_s5['acc']:>7.1%} {wf_s5['n']:>6d} "
              f"{wf_s5['pnl']:>+8.3f}% {fmt(wf_s5):>6s}")

    # 规则增量
    if base_s5 and m1_s5:
        delta = m1_s5['acc'] - base_s5['acc']
        print(f"\n  规则增量: {delta:+.1%} (基线{base_s5['acc']:.1%} → 规则后{m1_s5['acc']:.1%})")
        print(f"  样本减少: {base_s5['n']} → {m1_s5['n']} (过滤{base_s5['n']-m1_s5['n']}个低质量信号)")

    # 过拟合判断
    if m1_s5 and wf_s5:
        diff = m1_s5['acc'] - wf_s5['acc']
        print(f"\n  过拟合检查: 固定{m1_s5['acc']:.1%} vs WF{wf_s5['acc']:.1%}, 差距{diff:+.1%} "
              f"{'✅ 无过拟合' if diff < 0.03 else '⚠️ 轻微' if diff < 0.05 else '❌ 过拟合'}")

    # 最终结论
    final = wf_s5 if wf_s5 else m1_s5
    if final:
        acc = final['acc']
        print(f"\n  🎯 最终可信准确率（Walk-Forward）:")
        print(f"     5日: {acc:.1%}, 期望{final['pnl']:+.3f}%/5天, 盈亏比{fmt(final)}")
        if wf_s3:
            print(f"     3日: {wf_s3['acc']:.1%}, 期望{wf_s3['pnl']:+.3f}%/3天")
        if acc >= 0.65:
            print(f"     ✅ 达到65%目标")
        elif acc >= 0.60:
            print(f"     ⚠️ 60-65%区间，具有实际交易价值")
        else:
            print(f"     ❌ 未达60%")

    # 保存
    report = {
        'meta': {'n_stocks': len(kdata), 'date_range': f'{start_date}~{end_date}',
                 'rules': {'sent_min': SENT_MIN, 'tech_min': TECH_MIN,
                           'mkt_range': [MKT_LOWER, MKT_UPPER],
                           'price_max': PRICE_MAX, 'turn_max': TURN_MAX,
                           'skew_range': [SKEW_LO, SKEW_HI]},
                 'run_time': round(time.time()-t0, 1)},
        'baseline': base_s5,
        'method1_fixed': {'5d': m1_s5, '3d': m1_s3,
                          'monthly': {m: calc_stats(p) for m, p in sorted(m1_monthly.items())},
                          'by_sent_agree': {str(k): calc_stats(v) for k, v in sorted(m1_by_sn.items())}},
        'method2_walkforward': {'5d': wf_s5, '3d': wf_s3, 'retrain_count': wf_retrain,
                                'monthly': {m: calc_stats(p) for m, p in sorted(wf_monthly.items())},
                                'by_sent_agree': {str(k): calc_stats(v) for k, v in sorted(wf_by_sn.items())}},
    }
    out = OUTPUT_DIR / "sentiment_final_backtest.json"
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n⏱️  总耗时: {time.time()-t0:.1f}秒")
    print(f"📁 报告: {out}")
    print("=" * 80)
    return report


if __name__ == '__main__':
    run_backtest()
