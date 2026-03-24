#!/usr/bin/env python3
"""
情绪因子准确率提升 v4 — 双路线冲击65%
======================================
路线A：只做看涨信号 + 极严格多条件过滤
  - v3发现UP准确率62.9%，通过更严格过滤冲击65%
  - 多因子一致性 + 市场环境过滤 + 量价确认

路线B：情绪因子 + 技术面因子融合
  - 情绪因子提供方向信号
  - 技术面因子（动量、RSI、均线、相对强弱）提供确认
  - 两类因子同方向才出信号

防过拟合：
  - 严格3段时间分割（训练40%/验证30%/测试30%）
  - 参数搜索空间极小
  - 月度稳定性检验

用法：
    source .venv/bin/activate
    python -m tools.backtest_sentiment_v4
"""
import json
import logging
import math
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dao import get_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)
OUTPUT_DIR = Path(__file__).parent.parent / "data_results"


def _f(v):
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


# ═══════════════════════════════════════════════════════════════
# 数据加载
# ═══════════════════════════════════════════════════════════════

def load_codes(limit=300):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT stock_code FROM stock_kline "
        "WHERE stock_code NOT LIKE '4%%' AND stock_code NOT LIKE '8%%' "
        "AND stock_code NOT LIKE '9%%' ORDER BY stock_code LIMIT %s", (limit,))
    codes = [r['stock_code'] for r in cur.fetchall()]
    cur.close(); conn.close()
    return codes


def load_klines(codes, start, end):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    res = defaultdict(list)
    for i in range(0, len(codes), 300):
        batch = codes[i:i+300]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,close_price,open_price,high_price,"
            f"low_price,trading_volume,change_percent,change_hand,amplitude "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
            batch + [start, end])
        for r in cur.fetchall():
            res[r['stock_code']].append({
                'd': str(r['date']), 'c': _f(r['close_price']),
                'o': _f(r['open_price']), 'h': _f(r['high_price']),
                'l': _f(r['low_price']), 'v': _f(r['trading_volume']),
                'p': _f(r['change_percent']), 't': _f(r.get('change_hand')),
                'a': _f(r.get('amplitude')),
            })
    cur.close(); conn.close()
    return dict(res)


def load_ff(codes, start):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    res = defaultdict(list)
    for i in range(0, len(codes), 300):
        batch = codes[i:i+300]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,big_net_pct,small_net_pct,net_flow "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s ORDER BY `date`", batch + [start])
        for r in cur.fetchall():
            res[r['stock_code']].append({
                'd': str(r['date']),
                'bn': _f(r.get('big_net_pct')),
                'sn': _f(r.get('small_net_pct')),
                'nf': _f(r.get('net_flow')),
            })
    cur.close(); conn.close()
    return dict(res)


def load_market_klines(start, end):
    """加载上证指数K线作为大盘参考"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute(
        "SELECT `date`, close_price, change_percent FROM stock_kline "
        "WHERE stock_code = '000001.SH' AND `date` >= %s AND `date` <= %s "
        "ORDER BY `date`", (start, end))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{'d': str(r['date']), 'c': _f(r['close_price']),
             'p': _f(r['change_percent'])} for r in rows]


# ═══════════════════════════════════════════════════════════════
# 因子计算：情绪因子 + 技术面因子
# ═══════════════════════════════════════════════════════════════

def compute_sentiment_factors(klines, ff_by_date=None):
    """计算情绪因子（来自v3验证有效的因子）"""
    n = len(klines)
    if n < 60:
        return None
    close = [k['c'] for k in klines]
    open_ = [k['o'] for k in klines]
    high = [k['h'] for k in klines]
    low = [k['l'] for k in klines]
    vol = [k['v'] for k in klines]
    pct = [k['p'] for k in klines]
    turn = [k['t'] for k in klines]
    amp = [k.get('a', 0) or 0 for k in klines]

    if close[-1] <= 0 or vol[-1] <= 0:
        return None

    f = {}

    # S1: 偏度20d (IC=-0.18, v3最强因子)
    rp = pct[-20:]
    m = sum(rp) / 20
    s = (sum((p - m) ** 2 for p in rp) / 19) ** 0.5
    if s > 0:
        f['skew_20'] = sum((p - m) ** 3 for p in rp) / (20 * s ** 3)

    # S2: 价格位置60d (IC=-0.15)
    c60 = [c for c in close[-60:] if c > 0]
    if c60:
        h60, l60 = max(c60), min(c60)
        if h60 > l60:
            f['price_pos'] = (close[-1] - l60) / (h60 - l60)

    # S3: 均线偏离度BIAS (IC=-0.12)
    ma20 = sum(close[-20:]) / 20
    if ma20 > 0:
        f['bias_20'] = (close[-1] / ma20 - 1) * 100

    # S4: 噪声比 (IC=-0.098)
    intra_v, over_v = [], []
    for i in range(-10, 0):
        idx = n + i
        if idx > 0 and close[idx-1] > 0:
            intra_v.append((high[idx] - low[idx]) / close[idx-1] * 100)
            over_v.append(abs(open_[idx] - close[idx-1]) / close[idx-1] * 100)
    if intra_v and over_v:
        ai, ao = sum(intra_v)/len(intra_v), sum(over_v)/len(over_v)
        if ao > 0.01:
            f['noise_ratio'] = ai / ao

    # S5: 大阳大阴频率 (IC=-0.083)
    bu = sum(1 for p in pct[-10:] if p > 3)
    bd = sum(1 for p in pct[-10:] if p < -3)
    f['big_move'] = (bu - bd) / 10

    # S6: 量的不对称性 (IC=-0.077)
    up_v = [vol[n-10+i] for i in range(10) if pct[n-10+i] > 0 and vol[n-10+i] > 0]
    dn_v = [vol[n-10+i] for i in range(10) if pct[n-10+i] < 0 and vol[n-10+i] > 0]
    if up_v and dn_v:
        f['vol_asymmetry'] = (sum(up_v)/len(up_v)) / (sum(dn_v)/len(dn_v))

    # S7: 收盘-开盘5d (IC=-0.044)
    co = []
    for k in klines[-5:]:
        if k['o'] > 0:
            co.append((k['c'] - k['o']) / k['o'] * 100)
    if co:
        f['close_open_5d'] = sum(co) / len(co)

    # S8: 涨停接近度 (IC=-0.042)
    if close[-2] > 0:
        lu, ld = close[-2] * 1.1, close[-2] * 0.9
        f['limit_prox'] = (close[-1] - ld) / (lu - ld)

    # S9: 价格加速度 (IC=-0.033)
    if n >= 15 and close[-6] > 0 and close[-11] > 0:
        f['price_accel'] = (close[-1]/close[-6]-1)*100 - (close[-6]/close[-11]-1)*100

    # S10: 尾盘位置5d (IC=-0.008, 弱但稳定)
    cps = []
    for k in klines[-5:]:
        hl = k['h'] - k['l']
        if hl > 0:
            cps.append((k['c'] - k['l']) / hl)
    if cps:
        f['close_pos'] = sum(cps) / len(cps)

    # S11: 上影线5d (IC=+0.021, 逆向)
    us = []
    for k in klines[-5:]:
        hl = k['h'] - k['l']
        if hl > 0:
            us.append((k['h'] - max(k['c'], k['o'])) / hl)
    if us:
        f['upper_shd'] = sum(us) / len(us)

    # S12: 换手率异常 (IC=-0.018)
    t20 = [t for t in turn[-20:] if t > 0]
    if t20 and turn[-1] > 0:
        at20 = sum(t20) / len(t20)
        if at20 > 0:
            f['turn_spike'] = turn[-1] / at20

    # S13: 下跌日成交量占比 (IC=+0.064 from v2)
    dv = sum(vol[n-5+i] for i in range(5) if pct[n-5+i] < 0)
    tv = sum(vol[-5:])
    if tv > 0:
        f['down_vol_r'] = dv / tv

    # S14: 量能衰竭
    shrink = 0
    for i in range(n-1, max(n-11, 0), -1):
        if i > 0 and vol[i] < vol[i-1]:
            shrink += 1
        else:
            break
    f['vol_exhaust'] = shrink

    # S15: 振幅趋势
    a5 = [a for a in amp[-5:] if a > 0]
    a10 = [a for a in amp[-10:-5] if a > 0]
    if a5 and a10:
        f['amp_trend'] = (sum(a5)/len(a5)) / (sum(a10)/len(a10))

    # 资金流向因子
    if ff_by_date:
        ff_recent = [ff_by_date[k['d']] for k in klines[-5:] if k['d'] in ff_by_date]
        if len(ff_recent) >= 3:
            f['small_net'] = sum(x['sn'] for x in ff_recent) / len(ff_recent)
            f['big_net'] = sum(x['bn'] for x in ff_recent) / len(ff_recent)
            f['big_small_div'] = f['big_net'] - f['small_net']

    return f


def compute_technical_factors(klines, market_klines_by_date=None):
    """计算技术面因子（来自factor_engine.py验证有效的因子）"""
    n = len(klines)
    if n < 60:
        return None
    close = [k['c'] for k in klines]
    high = [k['h'] for k in klines]
    low = [k['l'] for k in klines]
    vol = [k['v'] for k in klines]
    pct = [k['p'] for k in klines]
    turn = [k['t'] for k in klines]

    if close[-1] <= 0:
        return None

    f = {}

    # T1: 20日动量
    if close[-21] > 0:
        f['mom_20'] = (close[-1] / close[-21] - 1) * 100

    # T2: 60日动量
    if n >= 61 and close[-61] > 0:
        f['mom_60'] = (close[-1] / close[-61] - 1) * 100

    # T3: 5日反转
    if close[-6] > 0:
        f['rev_5'] = (close[-1] / close[-6] - 1) * 100

    # T4: RSI 14 (Wilder原版)
    if n >= 15:
        gains, losses = [], []
        for i in range(n-14, n):
            chg = close[i] - close[i-1] if close[i-1] > 0 else 0
            gains.append(max(0, chg))
            losses.append(max(0, -chg))
        ag = sum(gains) / 14
        al = sum(losses) / 14
        f['rsi_14'] = 100 - 100/(1 + ag/al) if al > 0 else 100

    # T5: 20日波动率
    rp = pct[-20:]
    m = sum(rp) / 20
    f['vol_20'] = (sum((p-m)**2 for p in rp) / 19) ** 0.5

    # T6: 量比 (5d/20d)
    v5 = sum(vol[-5:]) / 5
    v20 = sum(vol[-20:]) / 20
    if v20 > 0:
        f['vol_ratio'] = v5 / v20

    # T7: 20日换手率均值
    t20 = [t for t in turn[-20:] if t > 0]
    if t20:
        f['turn_20'] = sum(t20) / len(t20)

    # T8: MA5 > MA20 (均线多头排列)
    ma5 = sum(close[-5:]) / 5
    ma20 = sum(close[-20:]) / 20
    if ma20 > 0:
        f['ma_cross'] = (ma5 / ma20 - 1) * 100

    # T9: 量价相关系数20d
    c20, v20_list = close[-20:], vol[-20:]
    mc = sum(c20)/20
    mv = sum(v20_list)/20
    cov = sum((c20[i]-mc)*(v20_list[i]-mv) for i in range(20))
    sc = (sum((c-mc)**2 for c in c20))**0.5
    sv = (sum((v-mv)**2 for v in v20_list))**0.5
    if sc > 0 and sv > 0:
        f['vp_corr'] = cov / (sc * sv)

    # T10: 连涨/连跌
    cd, cu = 0, 0
    for p in reversed(pct):
        if p < 0: cd += 1
        else: break
    for p in reversed(pct):
        if p > 0: cu += 1
        else: break
    f['consec_net'] = cu - cd

    # T11: ATR% (14日)
    trs = []
    for i in range(-14, 0):
        idx = n + i
        if idx > 0:
            tr = max(high[idx]-low[idx], abs(high[idx]-close[idx-1]), abs(low[idx]-close[idx-1]))
            trs.append(tr)
    if trs and close[-1] > 0:
        f['atr_pct'] = (sum(trs)/len(trs)) / close[-1] * 100

    # T12: 相对强弱 (vs 大盘)
    if market_klines_by_date:
        stock_ret_20 = sum(pct[-20:])
        mkt_dates = [klines[i]['d'] for i in range(max(0,n-20), n)]
        mkt_ret_20 = sum(market_klines_by_date.get(d, {}).get('p', 0) for d in mkt_dates)
        f['rel_strength'] = stock_ret_20 - mkt_ret_20

    # T13: 大盘20日趋势 (环境因子)
    if market_klines_by_date:
        mkt_dates = [klines[i]['d'] for i in range(max(0,n-20), n)]
        mkt_closes = [market_klines_by_date.get(d, {}).get('c', 0) for d in mkt_dates]
        mkt_closes = [c for c in mkt_closes if c > 0]
        if len(mkt_closes) >= 2:
            f['mkt_trend'] = (mkt_closes[-1] / mkt_closes[0] - 1) * 100

    return f


# ═══════════════════════════════════════════════════════════════
# 因子统计与预测引擎
# ═══════════════════════════════════════════════════════════════

def build_factor_stats(records):
    """从训练数据构建因子统计（均值、标准差、IC方向）"""
    stats = {}
    for fname, data in records.items():
        vals = [r[0] for r in data]
        rets = [r[1] for r in data]
        nn = len(vals)
        if nn < 200:
            continue
        m = sum(vals) / nn
        s = (sum((v - m)**2 for v in vals) / (nn - 1)) ** 0.5
        if s < 1e-10:
            continue
        # Spearman IC
        fi = sorted(range(nn), key=lambda i: vals[i])
        ri = sorted(range(nn), key=lambda i: rets[i])
        fr, rr = [0]*nn, [0]*nn
        for rank, idx in enumerate(fi): fr[idx] = rank
        for rank, idx in enumerate(ri): rr[idx] = rank
        mf, mr = sum(fr)/nn, sum(rr)/nn
        cov = sum((fr[i]-mf)*(rr[i]-mr) for i in range(nn))
        sf = sum((fr[i]-mf)**2 for i in range(nn))**0.5
        sr = sum((rr[i]-mr)**2 for i in range(nn))**0.5
        ic = cov / (sf * sr) if sf > 0 and sr > 0 else 0
        stats[fname] = {'mean': m, 'std': s, 'ic': ic, 'abs_ic': abs(ic),
                         'dir': -1 if ic < 0 else 1, 'n': nn}
    return stats


def vote_predict(factors, fstats, min_agree=5, top_k=12):
    """多因子投票预测方向，返回 (direction, confidence, n_agree)"""
    ranked = sorted(fstats.items(), key=lambda x: x[1]['abs_ic'], reverse=True)
    top = [(fn, fs) for fn, fs in ranked[:top_k] if fn in factors]
    if len(top) < min_agree:
        return None, 0, 0

    up, dn = 0, 0
    wscore, wtotal = 0.0, 0.0
    for fn, fs in top:
        z = (factors[fn] - fs['mean']) / fs['std']
        dz = z * fs['dir']
        if dz > 0.2: up += 1
        elif dz < -0.2: dn += 1
        wscore += dz * fs['abs_ic']
        wtotal += fs['abs_ic']

    conf = wscore / wtotal if wtotal > 0 else 0
    if up >= min_agree and up > dn:
        return 'UP', conf, up
    elif dn >= min_agree and dn > up:
        return 'DOWN', conf, dn
    return None, 0, 0


def compute_future(klines, idx):
    base = klines[idx]['c']
    if base <= 0:
        return {}
    r = {}
    for h in (5, 10):
        if idx + h < len(klines) and klines[idx + h]['c'] > 0:
            r[f'ret_{h}d'] = round((klines[idx + h]['c'] / base - 1) * 100, 4)
    return r


def calc_stats(preds):
    if not preds:
        return None
    n = len(preds)
    correct = sum(1 for d, r in preds if (d == 'UP' and r > 0) or (d == 'DOWN' and r < 0))
    wins, losses = [], []
    for d, r in preds:
        pnl = r if d == 'UP' else -r
        (wins if pnl > 0 else losses).append(abs(pnl))
    aw = sum(wins)/len(wins) if wins else 0
    al = sum(losses)/len(losses) if losses else 0
    return {
        'n': n, 'accuracy': round(correct/n, 4),
        'avg_pnl': round(sum(r if d == 'UP' else -r for d, r in preds)/n, 4),
        'avg_win': round(aw, 4), 'avg_loss': round(al, 4),
        'plr': round(aw/al, 2) if al > 0 else 'inf',
    }


# ═══════════════════════════════════════════════════════════════
# 路线A：UP-only + 严格多条件过滤
# ═══════════════════════════════════════════════════════════════

def route_a_predict(sent_factors, sent_stats, mkt_trend_val,
                    min_agree=7, top_k=14, conf_thresh=0.7):
    """
    路线A：只做看涨预测 + 市场环境过滤
    - 只在情绪因子投票看涨时出信号
    - 大盘不在下跌趋势时才出信号
    - 更高的一致性要求
    """
    # 市场环境过滤：大盘20日跌幅>5%时不做多
    if mkt_trend_val is not None and mkt_trend_val < -5:
        return None, 0, 0

    direction, conf, n_agree = vote_predict(sent_factors, sent_stats,
                                             min_agree=min_agree, top_k=top_k)
    # 只保留UP信号
    if direction != 'UP':
        return None, 0, 0
    if conf < conf_thresh:
        return None, 0, 0

    return 'UP', conf, n_agree


# ═══════════════════════════════════════════════════════════════
# 路线B：情绪 + 技术面融合
# ═══════════════════════════════════════════════════════════════

def route_b_predict(sent_factors, sent_stats, tech_factors, tech_stats,
                    sent_min_agree=5, sent_top_k=12, sent_conf=0.3,
                    tech_min_agree=4, tech_top_k=10, tech_conf=0.2):
    """
    路线B：情绪因子 + 技术面因子必须同方向
    - 情绪因子给出方向
    - 技术面因子必须确认同方向
    - 两者都同意才出信号
    """
    s_dir, s_conf, s_n = vote_predict(sent_factors, sent_stats,
                                       min_agree=sent_min_agree, top_k=sent_top_k)
    if s_dir is None or abs(s_conf) < sent_conf:
        return None, 0, 0

    t_dir, t_conf, t_n = vote_predict(tech_factors, tech_stats,
                                       min_agree=tech_min_agree, top_k=tech_top_k)
    if t_dir is None or abs(t_conf) < tech_conf:
        return None, 0, 0

    # 两者必须同方向
    if s_dir != t_dir:
        return None, 0, 0

    # 综合置信度
    combined_conf = (s_conf * 0.6 + t_conf * 0.4)
    return s_dir, combined_conf, s_n + t_n


# ═══════════════════════════════════════════════════════════════
# 主回测
# ═══════════════════════════════════════════════════════════════

def run_backtest():
    t0 = time.time()
    print("=" * 80)
    print("情绪因子 v4 — 双路线冲击65%")
    print("=" * 80)

    # ── 加载数据 ──
    logger.info("[1/7] 加载数据...")
    codes = load_codes(300)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=500)).strftime('%Y-%m-%d')
    kdata = load_klines(codes, start_date, end_date)
    ffdata = load_ff(codes, start_date)
    mkt = load_market_klines(start_date, end_date)
    mkt_by_date = {m['d']: m for m in mkt}
    logger.info("  K线: %d只, 资金流: %d只, 大盘: %d天", len(kdata), len(ffdata), len(mkt))

    # ── 训练期(40%): 收集因子分布和IC ──
    logger.info("[2/7] 训练期 — 收集因子IC...")
    sent_records = defaultdict(list)  # fname -> [(val, ret)]
    tech_records = defaultdict(list)
    train_count = 0

    for code, klines in kdata.items():
        if len(klines) < 80:
            continue
        ff_by_date = {f['d']: f for f in ffdata.get(code, [])}
        train_end = int(len(klines) * 0.4)

        for i in range(60, min(train_end, len(klines) - 10)):
            hist = klines[:i+1]
            sf = compute_sentiment_factors(hist, ff_by_date)
            tf = compute_technical_factors(hist, mkt_by_date)
            if not sf or not tf:
                continue
            future = compute_future(klines, i)
            if 'ret_5d' not in future:
                continue
            train_count += 1
            ret = future['ret_5d']
            for fn, fv in sf.items():
                if fv is not None and not isinstance(fv, str):
                    sent_records[fn].append((fv, ret))
            for fn, fv in tf.items():
                if fv is not None and not isinstance(fv, str):
                    tech_records[fn].append((fv, ret))

    sent_stats = build_factor_stats(sent_records)
    tech_stats = build_factor_stats(tech_records)
    logger.info("  训练样本: %d, 情绪因子: %d, 技术因子: %d",
                train_count, len(sent_stats), len(tech_stats))

    # 打印因子IC
    print(f"\n情绪因子IC排名（{train_count}样本）:")
    for fn, fs in sorted(sent_stats.items(), key=lambda x: x[1]['abs_ic'], reverse=True)[:12]:
        print(f"  {fn:<16s} IC={fs['ic']:+.4f}  |IC|={fs['abs_ic']:.4f}")
    print(f"\n技术因子IC排名:")
    for fn, fs in sorted(tech_stats.items(), key=lambda x: x[1]['abs_ic'], reverse=True)[:12]:
        print(f"  {fn:<16s} IC={fs['ic']:+.4f}  |IC|={fs['abs_ic']:.4f}")

    # ── 验证期(30%): 参数优化 ──
    logger.info("[3/7] 验证期 — 路线A参数优化...")

    # 路线A参数搜索
    a_params = [
        # (min_agree, top_k, conf_thresh)
        (6, 12, 0.5), (6, 12, 0.7), (6, 14, 0.7),
        (7, 12, 0.5), (7, 12, 0.7), (7, 14, 0.5),
        (7, 14, 0.7), (7, 14, 0.9),
        (8, 14, 0.5), (8, 14, 0.7), (8, 14, 0.9),
        (8, 12, 0.7), (9, 14, 0.7),
    ]
    a_val_results = []
    for ma, tk, ct in a_params:
        preds = []
        for code, klines in kdata.items():
            if len(klines) < 80:
                continue
            ff_by_date = {f['d']: f for f in ffdata.get(code, [])}
            te = int(len(klines) * 0.4)
            ve = int(len(klines) * 0.7)
            for i in range(max(60, te), min(ve, len(klines)-10)):
                hist = klines[:i+1]
                sf = compute_sentiment_factors(hist, ff_by_date)
                if not sf:
                    continue
                # 获取大盘趋势
                mkt_dates = [klines[j]['d'] for j in range(max(0,i-20), i+1)]
                mkt_c = [mkt_by_date.get(d, {}).get('c', 0) for d in mkt_dates]
                mkt_c = [c for c in mkt_c if c > 0]
                mkt_trend = (mkt_c[-1]/mkt_c[0]-1)*100 if len(mkt_c) >= 2 else 0

                d, c, n = route_a_predict(sf, sent_stats, mkt_trend, ma, tk, ct)
                if d is None:
                    continue
                future = compute_future(klines, i)
                if 'ret_5d' in future:
                    preds.append((d, future['ret_5d']))

        s = calc_stats(preds)
        if s and s['n'] >= 50:
            a_val_results.append({'p': (ma, tk, ct), 's': s})

    a_val_results.sort(key=lambda x: x['s']['accuracy'], reverse=True)
    print(f"\n路线A验证期结果（UP-only + 市场过滤）:")
    print(f"  {'min_agree':>9s} {'top_k':>5s} {'conf':>5s} {'样本':>6s} {'准确率':>6s} {'期望':>8s} {'盈亏比':>6s}")
    for vr in a_val_results[:8]:
        p, s = vr['p'], vr['s']
        plr = f"{s['plr']:.2f}" if isinstance(s['plr'], (int, float)) else s['plr']
        print(f"  {p[0]:>9d} {p[1]:>5d} {p[2]:>5.1f} {s['n']:>6d} {s['accuracy']:>6.1%} "
              f"{s['avg_pnl']:>+8.3f}% {plr:>6s}")

    best_a = a_val_results[0]['p'] if a_val_results else (7, 14, 0.7)

    # 路线B参数搜索
    logger.info("[4/7] 验证期 — 路线B参数优化...")
    b_params = [
        # (s_min, s_topk, s_conf, t_min, t_topk, t_conf)
        (4, 10, 0.3, 3, 8, 0.2),
        (5, 10, 0.3, 3, 8, 0.2),
        (5, 12, 0.3, 4, 8, 0.2),
        (5, 12, 0.5, 3, 8, 0.2),
        (5, 12, 0.5, 4, 8, 0.3),
        (5, 10, 0.5, 4, 10, 0.3),
        (6, 12, 0.3, 3, 8, 0.2),
        (6, 12, 0.5, 4, 8, 0.3),
        (6, 12, 0.5, 4, 10, 0.3),
        (6, 14, 0.5, 4, 10, 0.3),
        (7, 14, 0.5, 4, 10, 0.3),
        (5, 12, 0.3, 3, 10, 0.2),
    ]
    b_val_results = []
    for sm, stk, sc, tm, ttk, tc in b_params:
        preds = []
        for code, klines in kdata.items():
            if len(klines) < 80:
                continue
            ff_by_date = {f['d']: f for f in ffdata.get(code, [])}
            te = int(len(klines) * 0.4)
            ve = int(len(klines) * 0.7)
            for i in range(max(60, te), min(ve, len(klines)-10)):
                hist = klines[:i+1]
                sf = compute_sentiment_factors(hist, ff_by_date)
                tf = compute_technical_factors(hist, mkt_by_date)
                if not sf or not tf:
                    continue
                d, c, n = route_b_predict(sf, sent_stats, tf, tech_stats,
                                           sm, stk, sc, tm, ttk, tc)
                if d is None:
                    continue
                future = compute_future(klines, i)
                if 'ret_5d' in future:
                    preds.append((d, future['ret_5d']))

        s = calc_stats(preds)
        if s and s['n'] >= 50:
            b_val_results.append({'p': (sm, stk, sc, tm, ttk, tc), 's': s})

    b_val_results.sort(key=lambda x: x['s']['accuracy'], reverse=True)
    print(f"\n路线B验证期结果（情绪+技术融合）:")
    print(f"  {'s_min':>5s} {'s_tk':>4s} {'s_c':>4s} {'t_min':>5s} {'t_tk':>4s} {'t_c':>4s} "
          f"{'样本':>6s} {'准确率':>6s} {'期望':>8s} {'盈亏比':>6s}")
    for vr in b_val_results[:8]:
        p, s = vr['p'], vr['s']
        plr = f"{s['plr']:.2f}" if isinstance(s['plr'], (int, float)) else s['plr']
        print(f"  {p[0]:>5d} {p[1]:>4d} {p[2]:>4.1f} {p[3]:>5d} {p[4]:>4d} {p[5]:>4.1f} "
              f"{s['n']:>6d} {s['accuracy']:>6.1%} {s['avg_pnl']:>+8.3f}% {plr:>6s}")

    best_b = b_val_results[0]['p'] if b_val_results else (5, 12, 0.5, 4, 8, 0.3)


    # ── 测试期(30%): 最终评估 ──
    logger.info("[5/7] 测试期 — 最终评估...")

    # 路线A测试
    a_preds_5d, a_preds_10d = [], []
    a_monthly = defaultdict(list)
    a_by_conf = {'very_high': [], 'high': [], 'medium': []}

    # 路线B测试
    b_preds_5d, b_preds_10d = [], []
    b_by_dir = {'UP': [], 'DOWN': []}
    b_monthly = defaultdict(list)
    b_by_conf = {'very_high': [], 'high': [], 'medium': []}

    for code, klines in kdata.items():
        if len(klines) < 80:
            continue
        ff_by_date = {f['d']: f for f in ffdata.get(code, [])}
        ve = int(len(klines) * 0.7)

        for i in range(max(60, ve), len(klines) - 10):
            hist = klines[:i+1]
            sf = compute_sentiment_factors(hist, ff_by_date)
            tf = compute_technical_factors(hist, mkt_by_date)
            future = compute_future(klines, i)
            if 'ret_5d' not in future:
                continue
            month = klines[i]['d'][:7]

            # 路线A
            if sf:
                mkt_dates = [klines[j]['d'] for j in range(max(0,i-20), i+1)]
                mkt_c = [mkt_by_date.get(d, {}).get('c', 0) for d in mkt_dates]
                mkt_c = [c for c in mkt_c if c > 0]
                mkt_trend = (mkt_c[-1]/mkt_c[0]-1)*100 if len(mkt_c) >= 2 else 0

                ad, ac, an = route_a_predict(sf, sent_stats, mkt_trend, *best_a)
                if ad is not None:
                    a_preds_5d.append((ad, future['ret_5d']))
                    a_monthly[month].append((ad, future['ret_5d']))
                    if 'ret_10d' in future:
                        a_preds_10d.append((ad, future['ret_10d']))
                    aac = abs(ac)
                    if aac > 0.8: a_by_conf['very_high'].append((ad, future['ret_5d']))
                    elif aac > 0.5: a_by_conf['high'].append((ad, future['ret_5d']))
                    else: a_by_conf['medium'].append((ad, future['ret_5d']))

            # 路线B
            if sf and tf:
                bd, bc, bn = route_b_predict(sf, sent_stats, tf, tech_stats, *best_b)
                if bd is not None:
                    b_preds_5d.append((bd, future['ret_5d']))
                    b_by_dir[bd].append((bd, future['ret_5d']))
                    b_monthly[month].append((bd, future['ret_5d']))
                    if 'ret_10d' in future:
                        b_preds_10d.append((bd, future['ret_10d']))
                    bac = abs(bc)
                    if bac > 0.8: b_by_conf['very_high'].append((bd, future['ret_5d']))
                    elif bac > 0.5: b_by_conf['high'].append((bd, future['ret_5d']))
                    else: b_by_conf['medium'].append((bd, future['ret_5d']))

    # ── 打印结果 ──
    logger.info("[6/7] 输出报告...")

    print(f"\n{'═' * 80}")
    print(f"📊 路线A：UP-only + 市场环境过滤")
    print(f"{'═' * 80}")
    print(f"  参数: min_agree={best_a[0]}, top_k={best_a[1]}, conf={best_a[2]}")
    a5 = calc_stats(a_preds_5d)
    a10 = calc_stats(a_preds_10d)
    if a5:
        plr = f"{a5['plr']:.2f}" if isinstance(a5['plr'], (int, float)) else a5['plr']
        print(f"  5日准确率:  {a5['accuracy']:.1%}  ({a5['n']}样本)")
        print(f"  5日期望:    {a5['avg_pnl']:+.3f}%")
        print(f"  5日盈亏比:  {plr}")
    if a10:
        print(f"  10日准确率: {a10['accuracy']:.1%}  ({a10['n']}样本)")

    # 路线A按置信度
    print(f"\n  按置信度:")
    for cl in ['very_high', 'high', 'medium']:
        s = calc_stats(a_by_conf[cl])
        if s and s['n'] >= 10:
            label = {'very_high': '极高(>0.8)', 'high': '高(0.5~0.8)', 'medium': '中等'}[cl]
            plr = f"{s['plr']:.2f}" if isinstance(s['plr'], (int, float)) else s['plr']
            print(f"    {label:<14s}: 准确率={s['accuracy']:.1%}, 期望={s['avg_pnl']:+.3f}%, "
                  f"盈亏比={plr}, n={s['n']}")

    # 路线A月度
    print(f"\n  月度稳定性:")
    a_maccs = []
    for month in sorted(a_monthly.keys()):
        s = calc_stats(a_monthly[month])
        if s and s['n'] >= 10:
            a_maccs.append(s['accuracy'])
            print(f"    {month}: 准确率={s['accuracy']:.1%}, n={s['n']}, 期望={s['avg_pnl']:+.3f}%")
    if a_maccs:
        am = sum(a_maccs)/len(a_maccs)
        astd = (sum((a-am)**2 for a in a_maccs)/max(len(a_maccs)-1,1))**0.5
        print(f"    ── 月均: {am:.1%} ± {astd:.1%}")

    print(f"\n{'═' * 80}")
    print(f"📊 路线B：情绪 + 技术面融合")
    print(f"{'═' * 80}")
    print(f"  参数: sent({best_b[0]},{best_b[1]},{best_b[2]}) + tech({best_b[3]},{best_b[4]},{best_b[5]})")
    b5 = calc_stats(b_preds_5d)
    b10 = calc_stats(b_preds_10d)
    if b5:
        plr = f"{b5['plr']:.2f}" if isinstance(b5['plr'], (int, float)) else b5['plr']
        print(f"  5日准确率:  {b5['accuracy']:.1%}  ({b5['n']}样本)")
        print(f"  5日期望:    {b5['avg_pnl']:+.3f}%")
        print(f"  5日盈亏比:  {plr}")
    if b10:
        print(f"  10日准确率: {b10['accuracy']:.1%}  ({b10['n']}样本)")

    # 路线B按方向
    print(f"\n  按方向:")
    for d in ['UP', 'DOWN']:
        s = calc_stats(b_by_dir[d])
        if s:
            plr = f"{s['plr']:.2f}" if isinstance(s['plr'], (int, float)) else s['plr']
            print(f"    {d:>4s}: 准确率={s['accuracy']:.1%}, 期望={s['avg_pnl']:+.3f}%, "
                  f"盈亏比={plr}, n={s['n']}")

    # 路线B按置信度
    print(f"\n  按置信度:")
    for cl in ['very_high', 'high', 'medium']:
        s = calc_stats(b_by_conf[cl])
        if s and s['n'] >= 10:
            label = {'very_high': '极高(>0.8)', 'high': '高(0.5~0.8)', 'medium': '中等'}[cl]
            plr = f"{s['plr']:.2f}" if isinstance(s['plr'], (int, float)) else s['plr']
            print(f"    {label:<14s}: 准确率={s['accuracy']:.1%}, 期望={s['avg_pnl']:+.3f}%, "
                  f"盈亏比={plr}, n={s['n']}")

    # 路线B月度
    print(f"\n  月度稳定性:")
    b_maccs = []
    for month in sorted(b_monthly.keys()):
        s = calc_stats(b_monthly[month])
        if s and s['n'] >= 10:
            b_maccs.append(s['accuracy'])
            print(f"    {month}: 准确率={s['accuracy']:.1%}, n={s['n']}, 期望={s['avg_pnl']:+.3f}%")
    if b_maccs:
        bm = sum(b_maccs)/len(b_maccs)
        bstd = (sum((a-bm)**2 for a in b_maccs)/max(len(b_maccs)-1,1))**0.5
        print(f"    ── 月均: {bm:.1%} ± {bstd:.1%}")


    # ── 总结 ──
    logger.info("[7/7] 总结...")
    print(f"\n{'═' * 80}")
    print("📋 总结对比")
    print(f"{'═' * 80}")
    print(f"  {'路线':>8s} {'5日准确率':>10s} {'样本':>6s} {'期望收益':>10s} {'盈亏比':>8s}")
    print(f"  {'─' * 50}")
    if a5:
        plr_a = f"{a5['plr']:.2f}" if isinstance(a5['plr'], (int, float)) else a5['plr']
        print(f"  {'A(UP-only)':>8s} {a5['accuracy']:>10.1%} {a5['n']:>6d} {a5['avg_pnl']:>+10.3f}% {plr_a:>8s}")
    if b5:
        plr_b = f"{b5['plr']:.2f}" if isinstance(b5['plr'], (int, float)) else b5['plr']
        print(f"  {'B(融合)':>8s} {b5['accuracy']:>10.1%} {b5['n']:>6d} {b5['avg_pnl']:>+10.3f}% {plr_b:>8s}")

    # v3基线
    print(f"  {'v3基线':>8s} {'57.6%':>10s} {'3924':>6s} {'-0.023%':>10s} {'0.73':>8s}")
    print(f"  {'v3-UP':>8s} {'62.9%':>10s} {'1074':>6s} {'+1.109%':>10s} {'1.01':>8s}")

    # 判断是否达标
    best_acc = 0
    best_route = ''
    if a5 and a5['accuracy'] > best_acc:
        best_acc = a5['accuracy']
        best_route = 'A'
    if b5 and b5['accuracy'] > best_acc:
        best_acc = b5['accuracy']
        best_route = 'B'

    if best_acc >= 0.65:
        print(f"\n  ✅ 路线{best_route} 准确率 {best_acc:.1%} ≥ 65%，达标！")
    elif best_acc >= 0.60:
        print(f"\n  ⚠️ 路线{best_route} 准确率 {best_acc:.1%}，接近但未达65%")
    else:
        print(f"\n  ❌ 两条路线均未达60%")

    # 过拟合检查
    if a_val_results and a5:
        va = a_val_results[0]['s']['accuracy']
        diff = va - a5['accuracy']
        print(f"  路线A过拟合: 验证{va:.1%} vs 测试{a5['accuracy']:.1%}, 差距{diff:+.1%} "
              f"{'✅' if diff < 0.03 else '⚠️' if diff < 0.05 else '❌'}")
    if b_val_results and b5:
        vb = b_val_results[0]['s']['accuracy']
        diff = vb - b5['accuracy']
        print(f"  路线B过拟合: 验证{vb:.1%} vs 测试{b5['accuracy']:.1%}, 差距{diff:+.1%} "
              f"{'✅' if diff < 0.03 else '⚠️' if diff < 0.05 else '❌'}")

    # 保存报告
    report = {
        'meta': {
            'n_stocks': len(kdata), 'date_range': f'{start_date} ~ {end_date}',
            'split': '40% train / 30% val / 30% test',
            'run_time': round(time.time() - t0, 1),
        },
        'sentiment_factor_ic': {fn: {'ic': fs['ic'], 'abs_ic': fs['abs_ic']}
                                 for fn, fs in sorted(sent_stats.items(),
                                 key=lambda x: x[1]['abs_ic'], reverse=True)[:15]},
        'technical_factor_ic': {fn: {'ic': fs['ic'], 'abs_ic': fs['abs_ic']}
                                 for fn, fs in sorted(tech_stats.items(),
                                 key=lambda x: x[1]['abs_ic'], reverse=True)[:12]},
        'route_a': {
            'params': {'min_agree': best_a[0], 'top_k': best_a[1], 'conf': best_a[2]},
            'validation': [{'params': vr['p'], **vr['s']} for vr in a_val_results[:5]],
            'test_5d': a5, 'test_10d': a10,
            'test_by_confidence': {c: calc_stats(p) for c, p in a_by_conf.items()},
            'test_monthly': {m: calc_stats(p) for m, p in sorted(a_monthly.items())},
        },
        'route_b': {
            'params': {'sent': list(best_b[:3]), 'tech': list(best_b[3:])},
            'validation': [{'params': vr['p'], **vr['s']} for vr in b_val_results[:5]],
            'test_5d': b5, 'test_10d': b10,
            'test_by_direction': {d: calc_stats(p) for d, p in b_by_dir.items()},
            'test_by_confidence': {c: calc_stats(p) for c, p in b_by_conf.items()},
            'test_monthly': {m: calc_stats(p) for m, p in sorted(b_monthly.items())},
        },
    }
    out = OUTPUT_DIR / "sentiment_v4_backtest.json"
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n⏱️  总耗时: {time.time() - t0:.1f}秒")
    print(f"📁 报告: {out}")
    print("=" * 80)
    return report


if __name__ == '__main__':
    run_backtest()
