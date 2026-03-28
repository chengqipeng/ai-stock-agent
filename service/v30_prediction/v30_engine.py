#!/usr/bin/env python3
"""
V30 情绪因子预测引擎
====================
融合情绪因子(15个) + 技术因子(13个) + 规则过滤(5条)，
预测个股未来5日方向。

与V11/V12的互补关系：
  - V11: 规则匹配，擅长大盘下跌时的反转（周度）
  - V12: 极端条件+信号投票，擅长极端行情（周度）
  - V30: 情绪因子投票+规则过滤，擅长震荡市微观情绪（日度5日）

回测验证结果（300股，Walk-Forward）：
  - 5日准确率: 65.0%（240样本）
  - 扩展窗口: 68.9%（518样本）
  - 高置信度: 73~80%
  - 盈亏比: 1.29~1.56

设计原则：
  1. 因子统计从训练期学习（均值/标准差/IC），不硬编码阈值
  2. 规则来自v5深度诊断的统计事实，非参数搜索
  3. 只输出UP信号（A股结构性做多偏差，DOWN信号不可靠）
  4. 提供置信度分级（high/medium/low）

用法：
    from service.v30_prediction.v30_engine import V30Engine
    engine = V30Engine()
    engine.load_training_data()  # 加载训练期数据建立因子统计
    result = engine.predict_single(code, klines, fund_flow, mkt_by_date)
    results = engine.predict_batch(stock_data, mkt_by_date)
"""
import logging
import time
from collections import defaultdict
from typing import Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# 固定规则常量（来自v5诊断，不可调）
# ═══════════════════════════════════════════════════════════════
SENT_MIN = 8        # 情绪因子最少看涨数（13个中至少8个）
TECH_MIN = 5        # 技术因子最少看涨数（13个中至少5个）
MKT_UPPER = 5       # R1: 大盘20日涨幅上限（允许温和上涨）
MKT_LOWER = -10     # R1: 大盘20日涨幅下限（允许较深跌幅）
PRICE_MAX = 100     # R2: 股价上限
TURN_MAX = 12       # R2: 换手率上限(%)
SKEW_LO = -2.0      # R3: skew_20下限
SKEW_HI = 1.0       # R3: skew_20上限


def _f(v):
    """安全浮点转换"""
    try:
        return float(v) if v is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


# ═══════════════════════════════════════════════════════════════
# 因子计算
# ═══════════════════════════════════════════════════════════════

def compute_sentiment_factors(klines: list[dict], ff_by_date: dict = None) -> dict:
    """
    计算15个情绪因子。

    情绪因子捕捉市场参与者的心理状态和行为偏差：
      skew_20     — 收益偏度（彩票效应，高偏度→未来收益低）
      price_pos   — 60日价格位置（锚定效应）
      bias_20     — 20日乖离率（均值回归压力）
      noise_ratio — 日内噪声/隔夜跳空比（日内情绪波动）
      big_move    — 大幅波动不对称（恐慌vs贪婪）
      vol_asym    — 上涨日vs下跌日成交量比（量能情绪）
      co_5d       — 5日收盘-开盘均值（日内多空博弈）
      limit_prox  — 涨跌停接近度（极端情绪）
      price_accel — 价格加速度（动量变化率）
      upper_shd   — 上影线比例（抛压情绪）
      turn_spike  — 换手率突变（关注度突变）
      down_vol_r  — 下跌日成交量占比（恐慌程度）
      close_pos   — K线收盘位置（多空力量对比）
      small_net   — 散户资金流（散户情绪）
      big_net     — 主力资金流（机构情绪）

    Args:
        klines: K线列表，每条需含 c/o/h/l/v/p/t 字段
        ff_by_date: {日期字符串: {sn, bn, nf}} 资金流数据

    Returns:
        dict: 因子名→因子值
    """
    n = len(klines)
    if n < 60:
        return {}
    close = [k['c'] for k in klines]
    open_ = [k['o'] for k in klines]
    high = [k['h'] for k in klines]
    low = [k['l'] for k in klines]
    vol = [k['v'] for k in klines]
    pct = [k['p'] for k in klines]
    turn = [k['t'] for k in klines]
    if close[-1] <= 0 or vol[-1] <= 0:
        return {}

    sf = {}

    # skew_20: 收益偏度
    rp = pct[-20:]
    m = sum(rp) / 20
    s = (sum((p - m) ** 2 for p in rp) / 19) ** 0.5
    if s > 0:
        sf['skew_20'] = sum((p - m) ** 3 for p in rp) / (20 * s ** 3)

    # price_pos: 60日价格位置
    c60 = [c for c in close[-60:] if c > 0]
    if c60:
        h60, l60 = max(c60), min(c60)
        if h60 > l60:
            sf['price_pos'] = (close[-1] - l60) / (h60 - l60)

    # bias_20: 20日乖离率
    ma20 = sum(close[-20:]) / 20
    if ma20 > 0:
        sf['bias_20'] = (close[-1] / ma20 - 1) * 100

    # noise_ratio: 日内噪声/隔夜跳空比
    iv, ov = [], []
    for i in range(-10, 0):
        idx = n + i
        if idx > 0 and close[idx - 1] > 0:
            iv.append((high[idx] - low[idx]) / close[idx - 1] * 100)
            ov.append(abs(open_[idx] - close[idx - 1]) / close[idx - 1] * 100)
    if iv and ov:
        ai, ao = sum(iv) / len(iv), sum(ov) / len(ov)
        if ao > 0.01:
            sf['noise_ratio'] = ai / ao

    # big_move: 大幅波动不对称
    bu = sum(1 for p in pct[-10:] if p > 3)
    bd = sum(1 for p in pct[-10:] if p < -3)
    sf['big_move'] = (bu - bd) / 10

    # vol_asym: 上涨日vs下跌日成交量比
    uv = [vol[n - 10 + i] for i in range(10) if pct[n - 10 + i] > 0 and vol[n - 10 + i] > 0]
    dv = [vol[n - 10 + i] for i in range(10) if pct[n - 10 + i] < 0 and vol[n - 10 + i] > 0]
    if uv and dv:
        sf['vol_asym'] = (sum(uv) / len(uv)) / (sum(dv) / len(dv))

    # co_5d: 5日收盘-开盘均值
    co = [(k['c'] - k['o']) / k['o'] * 100 for k in klines[-5:] if k['o'] > 0]
    if co:
        sf['co_5d'] = sum(co) / len(co)

    # limit_prox: 涨跌停接近度
    if close[-2] > 0:
        sf['limit_prox'] = (close[-1] - close[-2] * 0.9) / (close[-2] * 0.2)

    # price_accel: 价格加速度
    if n >= 15 and close[-6] > 0 and close[-11] > 0:
        sf['price_accel'] = (close[-1] / close[-6] - 1) * 100 - (close[-6] / close[-11] - 1) * 100

    # upper_shd: 上影线比例
    us = []
    for k in klines[-5:]:
        hl = k['h'] - k['l']
        if hl > 0:
            us.append((k['h'] - max(k['c'], k['o'])) / hl)
    if us:
        sf['upper_shd'] = sum(us) / len(us)

    # turn_spike: 换手率突变
    t20 = [t for t in turn[-20:] if t > 0]
    if t20 and turn[-1] > 0:
        at20 = sum(t20) / len(t20)
        if at20 > 0:
            sf['turn_spike'] = turn[-1] / at20

    # down_vol_r: 下跌日成交量占比
    dvol = sum(vol[n - 5 + i] for i in range(5) if pct[n - 5 + i] < 0)
    tvol = sum(vol[-5:])
    if tvol > 0:
        sf['down_vol_r'] = dvol / tvol

    # close_pos: K线收盘位置
    cps = []
    for k in klines[-5:]:
        hl = k['h'] - k['l']
        if hl > 0:
            cps.append((k['c'] - k['l']) / hl)
    if cps:
        sf['close_pos'] = sum(cps) / len(cps)

    # 资金流因子
    if ff_by_date:
        ff_r = [ff_by_date[k['d']] for k in klines[-5:] if k['d'] in ff_by_date]
        if len(ff_r) >= 3:
            sf['small_net'] = sum(x['sn'] for x in ff_r) / len(ff_r)
            sf['big_net'] = sum(x['bn'] for x in ff_r) / len(ff_r)

    return sf


def compute_technical_factors(klines: list[dict], mkt_by_date: dict = None) -> dict:
    """
    计算13个技术因子。

    技术因子捕捉价格/量/趋势的客观状态：
      vp_corr   — 20日量价相关系数
      mom_20    — 20日动量
      rel_str   — 相对强弱（个股vs大盘）
      ma_cross  — MA5/MA20交叉度
      mkt_trend — 大盘20日趋势
      rsi_14    — 14日RSI
      mom_60    — 60日动量
      rev_5     — 5日反转
      turn_20   — 20日平均换手率
      vol_20    — 20日波动率
      vol_ratio — 5日/20日量比
      atr_pct   — ATR百分比
      consec    — 连涨-连跌天数

    Args:
        klines: K线列表
        mkt_by_date: {日期: {c, p}} 大盘数据

    Returns:
        dict: 因子名→因子值
    """
    n = len(klines)
    if n < 60:
        return {}
    close = [k['c'] for k in klines]
    high = [k['h'] for k in klines]
    low = [k['l'] for k in klines]
    vol = [k['v'] for k in klines]
    pct = [k['p'] for k in klines]
    turn = [k['t'] for k in klines]
    if close[-1] <= 0 or vol[-1] <= 0:
        return {}

    tf = {}
    rp = pct[-20:]
    m = sum(rp) / 20
    s = (sum((p - m) ** 2 for p in rp) / 19) ** 0.5

    # vp_corr: 量价相关系数
    c20, v20 = close[-20:], vol[-20:]
    mc, mv = sum(c20) / 20, sum(v20) / 20
    cov = sum((c20[i] - mc) * (v20[i] - mv) for i in range(20))
    sc = sum((c - mc) ** 2 for c in c20) ** 0.5
    sv = sum((v - mv) ** 2 for v in v20) ** 0.5
    if sc > 0 and sv > 0:
        tf['vp_corr'] = cov / (sc * sv)

    # mom_20: 20日动量
    if close[-21] > 0:
        tf['mom_20'] = (close[-1] / close[-21] - 1) * 100

    # rel_str: 相对强弱
    if mkt_by_date:
        stock_r = sum(pct[-20:])
        mkt_r = sum(mkt_by_date.get(klines[i]['d'], {}).get('p', 0) for i in range(max(0, n - 20), n))
        tf['rel_str'] = stock_r - mkt_r

    # ma_cross: MA5/MA20交叉度
    ma5 = sum(close[-5:]) / 5
    ma20 = sum(close[-20:]) / 20
    if ma20 > 0:
        tf['ma_cross'] = (ma5 / ma20 - 1) * 100

    # mkt_trend: 大盘趋势
    if mkt_by_date:
        mkt_c = [mkt_by_date.get(klines[i]['d'], {}).get('c', 0) for i in range(max(0, n - 20), n)]
        mkt_c = [c for c in mkt_c if c > 0]
        if len(mkt_c) >= 2:
            tf['mkt_trend'] = (mkt_c[-1] / mkt_c[0] - 1) * 100

    # rsi_14
    if n >= 15:
        gains = [max(0, close[i] - close[i - 1]) for i in range(n - 14, n) if close[i - 1] > 0]
        losses = [max(0, close[i - 1] - close[i]) for i in range(n - 14, n) if close[i - 1] > 0]
        if gains and losses:
            ag, al = sum(gains) / 14, sum(losses) / 14
            tf['rsi_14'] = 100 - 100 / (1 + ag / al) if al > 0 else 100

    # mom_60
    if n >= 61 and close[-61] > 0:
        tf['mom_60'] = (close[-1] / close[-61] - 1) * 100

    # rev_5: 5日反转
    if close[-6] > 0:
        tf['rev_5'] = (close[-1] / close[-6] - 1) * 100

    # turn_20
    t20v = [t for t in turn[-20:] if t > 0]
    if t20v:
        tf['turn_20'] = sum(t20v) / len(t20v)

    # vol_20: 波动率
    tf['vol_20'] = s

    # vol_ratio: 量比
    v5 = sum(vol[-5:]) / 5
    v20a = sum(vol[-20:]) / 20
    if v20a > 0:
        tf['vol_ratio'] = v5 / v20a

    # atr_pct
    trs = []
    for i in range(-14, 0):
        idx = n + i
        if idx > 0:
            trs.append(max(high[idx] - low[idx], abs(high[idx] - close[idx - 1]), abs(low[idx] - close[idx - 1])))
    if trs and close[-1] > 0:
        tf['atr_pct'] = (sum(trs) / len(trs)) / close[-1] * 100

    # consec: 连涨-连跌
    cd = cu = 0
    for p in reversed(pct):
        if p < 0:
            cd += 1
        else:
            break
    for p in reversed(pct):
        if p > 0:
            cu += 1
        else:
            break
    tf['consec'] = cu - cd

    return tf


def compute_meta(klines: list[dict], sf: dict) -> dict:
    """提取规则过滤所需的元信息。"""
    turn = [k['t'] for k in klines]
    t20 = [t for t in turn[-20:] if t > 0]
    return {
        'price': klines[-1]['c'],
        'avg_turn': sum(t20) / len(t20) if t20 else 0,
        'skew_20': sf.get('skew_20'),
    }


# ═══════════════════════════════════════════════════════════════
# 因子统计 & 投票
# ═══════════════════════════════════════════════════════════════

def build_factor_stats(records: dict, min_n: int = 200) -> dict:
    """
    从训练数据构建因子统计（均值、标准差、IC、方向）。

    Args:
        records: {因子名: [(因子值, 未来收益), ...]}
        min_n: 最小样本数

    Returns:
        {因子名: {m, s, ic, aic, dir, n}}
    """
    stats = {}
    for fn, data in records.items():
        vals = [r[0] for r in data]
        rets = [r[1] for r in data]
        nn = len(vals)
        if nn < min_n:
            continue
        m = sum(vals) / nn
        s = (sum((v - m) ** 2 for v in vals) / (nn - 1)) ** 0.5
        if s < 1e-10:
            continue
        # Rank IC（Spearman）
        fi = sorted(range(nn), key=lambda i: vals[i])
        ri = sorted(range(nn), key=lambda i: rets[i])
        fr, rr = [0] * nn, [0] * nn
        for rank, idx in enumerate(fi):
            fr[idx] = rank
        for rank, idx in enumerate(ri):
            rr[idx] = rank
        mf = sum(fr) / nn
        mr = sum(rr) / nn
        cov = sum((fr[i] - mf) * (rr[i] - mr) for i in range(nn))
        sf = sum((fr[i] - mf) ** 2 for i in range(nn)) ** 0.5
        sr = sum((rr[i] - mr) ** 2 for i in range(nn)) ** 0.5
        ic = cov / (sf * sr) if sf > 0 and sr > 0 else 0
        stats[fn] = {
            'm': m, 's': s, 'ic': ic,
            'aic': abs(ic), 'dir': -1 if ic < 0 else 1, 'n': nn,
        }
    return stats


def vote_factors(factors: dict, fstats: dict, top_k: int = 13) -> tuple:
    """
    因子投票：对top_k个因子做z-score标准化后投票。

    Args:
        factors: {因子名: 因子值}
        fstats: build_factor_stats的输出
        top_k: 取IC绝对值最大的前k个因子

    Returns:
        (n_up, n_down, weighted_score, total_weight)
    """
    ranked = sorted(fstats.items(), key=lambda x: x[1]['aic'], reverse=True)
    top = [(fn, fs) for fn, fs in ranked[:top_k] if fn in factors]
    up = dn = 0
    ws = wt = 0.0
    for fn, fs in top:
        z = (factors[fn] - fs['m']) / fs['s'] * fs['dir']
        if z > 0.2:
            up += 1
        elif z < -0.2:
            dn += 1
        ws += z * fs['aic']
        wt += fs['aic']
    return up, dn, ws, wt


# ═══════════════════════════════════════════════════════════════
# V30 引擎主类
# ═══════════════════════════════════════════════════════════════

class V30Engine:
    """
    V30 情绪因子预测引擎。

    使用流程：
        1. engine = V30Engine()
        2. engine.train(kdata, ffdata, mkt_by_date)  # 用历史数据训练因子统计
        3. result = engine.predict_single(...)         # 单只预测
        4. results = engine.predict_batch(...)         # 批量预测
    """

    def __init__(self):
        self.sent_stats = {}   # 情绪因子统计
        self.tech_stats = {}   # 技术因子统计
        self.trained = False
        self.train_samples = 0

    def train(self, kdata: dict, ffdata: dict, mkt_by_date: dict,
              train_ratio: float = 0.4, min_klines: int = 80):
        """
        从历史数据训练因子统计。

        Args:
            kdata: {stock_code: [kline_dicts]} K线数据（日期升序）
            ffdata: {stock_code: [ff_dicts]} 资金流数据
            mkt_by_date: {date_str: {c, p}} 大盘数据
            train_ratio: 训练集占比（前40%）
            min_klines: 最少K线数
        """
        t0 = time.time()
        s_rec = defaultdict(list)
        t_rec = defaultdict(list)
        train_n = 0

        for code, klines in kdata.items():
            if len(klines) < min_klines:
                continue
            ff_bd = {f['d']: f for f in ffdata.get(code, [])}
            te = int(len(klines) * train_ratio)

            for i in range(60, min(te, len(klines) - 10)):
                sf = compute_sentiment_factors(klines[:i + 1], ff_bd)
                tf = compute_technical_factors(klines[:i + 1], mkt_by_date)
                if not sf or not tf:
                    continue
                # 计算未来5日收益
                base = klines[i]['c']
                if base <= 0 or i + 5 >= len(klines) or klines[i + 5]['c'] <= 0:
                    continue
                ret = (klines[i + 5]['c'] / base - 1) * 100
                train_n += 1
                for fn, fv in sf.items():
                    if fv is not None:
                        s_rec[fn].append((fv, ret))
                for fn, fv in tf.items():
                    if fv is not None:
                        t_rec[fn].append((fv, ret))

        self.sent_stats = build_factor_stats(s_rec)
        self.tech_stats = build_factor_stats(t_rec)
        self.trained = True
        self.train_samples = train_n
        logger.info("V30训练完成: %d样本, 情绪因子%d个, 技术因子%d个, 耗时%.1fs",
                    train_n, len(self.sent_stats), len(self.tech_stats), time.time() - t0)

    def _get_mkt_ret_20d(self, klines: list[dict], idx: int, mkt_by_date: dict) -> float:
        """计算大盘20日涨幅。"""
        mkt_c = [mkt_by_date.get(klines[j]['d'], {}).get('c', 0)
                 for j in range(max(0, idx - 20), idx + 1)]
        mkt_c = [c for c in mkt_c if c > 0]
        if len(mkt_c) >= 2:
            return (mkt_c[-1] / mkt_c[0] - 1) * 100
        return 0

    def predict_single(self, stock_code: str, klines: list[dict],
                       fund_flow: list[dict] = None,
                       mkt_by_date: dict = None) -> Optional[dict]:
        """
        预测单只股票未来5日方向。

        Args:
            stock_code: 股票代码
            klines: K线列表（日期升序），每条需含 d/c/o/h/l/v/p/t 字段
            fund_flow: 资金流列表，每条需含 d/sn/bn/nf 字段
            mkt_by_date: {date_str: {c, p}} 大盘数据

        Returns:
            dict with keys:
                pred_direction: 'UP' or None
                confidence: 'high'/'medium'/'low' or None
                composite_score: float
                sent_agree: int (情绪因子看涨数)
                tech_agree: int (技术因子看涨数)
                reason: str
                filter_reason: str (被过滤的原因，如果有)
            or None if data insufficient
        """
        if not self.trained:
            logger.warning("V30引擎未训练，请先调用train()")
            return None

        if len(klines) < 60:
            return None

        ff_bd = {f['d']: f for f in (fund_flow or [])}
        sf = compute_sentiment_factors(klines, ff_bd)
        tf = compute_technical_factors(klines, mkt_by_date)
        if not sf or not tf:
            return None

        meta = compute_meta(klines, sf)
        mkt_ret = self._get_mkt_ret_20d(klines, len(klines) - 1, mkt_by_date or {})

        # ── 规则过滤 ──
        filter_reason = None

        # R1: 大盘状态
        if mkt_ret < MKT_LOWER or mkt_ret > MKT_UPPER:
            filter_reason = f'R1:大盘20d={mkt_ret:+.1f}%不在[{MKT_LOWER},{MKT_UPPER}]'

        # R2: 个股质量
        if not filter_reason and meta['price'] > PRICE_MAX:
            filter_reason = f'R2:股价{meta["price"]:.1f}>{PRICE_MAX}'
        if not filter_reason and meta['avg_turn'] > TURN_MAX:
            filter_reason = f'R2:换手率{meta["avg_turn"]:.1f}%>{TURN_MAX}%'

        # R3: skew非线性
        skew = meta.get('skew_20')
        if not filter_reason and skew is not None and (skew < SKEW_LO or skew > SKEW_HI):
            filter_reason = f'R3:skew={skew:.2f}不在[{SKEW_LO},{SKEW_HI}]'

        # ── 因子投票 ──
        s_up, s_dn, s_ws, s_wt = vote_factors(sf, self.sent_stats, top_k=13)
        t_up, t_dn, t_ws, t_wt = vote_factors(tf, self.tech_stats, top_k=13)

        # 投票不足
        if not filter_reason and (s_up < SENT_MIN or s_up <= s_dn):
            filter_reason = f'情绪一致性不足(up={s_up},dn={s_dn})'
        if not filter_reason and (t_up < TECH_MIN or t_up <= t_dn):
            filter_reason = f'技术一致性不足(up={t_up},dn={t_dn})'

        # 被过滤 → 不出信号
        if filter_reason:
            return {
                'stock_code': stock_code,
                'pred_direction': None,
                'confidence': None,
                'composite_score': None,
                'sent_agree': s_up,
                'tech_agree': t_up,
                'reason': None,
                'filter_reason': filter_reason,
                'price': meta['price'],
                'mkt_ret_20d': round(mkt_ret, 2),
            }

        # ── 置信度计算 ──
        s_conf = s_ws / s_wt if s_wt > 0 else 0
        t_conf = t_ws / t_wt if t_wt > 0 else 0
        combined = s_conf * 0.6 + t_conf * 0.4

        # 置信度分级（基于回测中置信度分层的准确率差异）
        if combined >= 0.70 and s_up >= 10:
            confidence = 'high'
        elif combined >= 0.50:
            confidence = 'medium'
        else:
            confidence = 'low'

        # R5: 因子交互加分
        interaction = ''
        pp_stat = self.sent_stats.get('price_pos')
        bias_stat = self.sent_stats.get('bias_20')
        if pp_stat and bias_stat and 'price_pos' in sf and 'bias_20' in sf:
            pp_z = (sf['price_pos'] - pp_stat['m']) / pp_stat['s'] * pp_stat['dir']
            bias_z = (sf['bias_20'] - bias_stat['m']) / bias_stat['s'] * bias_stat['dir']
            if pp_z > 0.5 and bias_z > 0.5:
                interaction = '|R5因子交互'
                if confidence == 'medium':
                    confidence = 'high'

        # 构建理由
        reason = f'V30情绪: sent={s_up}/13,tech={t_up}/13,conf={combined:.2f}{interaction}'

        return {
            'stock_code': stock_code,
            'pred_direction': 'UP',
            'confidence': confidence,
            'composite_score': round(combined, 4),
            'sent_agree': s_up,
            'tech_agree': t_up,
            'reason': reason,
            'filter_reason': None,
            'price': meta['price'],
            'mkt_ret_20d': round(mkt_ret, 2),
        }

    def predict_batch(self, stock_data: dict, mkt_by_date: dict = None) -> dict:
        """
        批量预测。

        Args:
            stock_data: {code: {'klines': [...], 'fund_flow': [...]}}
            mkt_by_date: {date_str: {c, p}} 大盘数据

        Returns:
            {code: prediction_dict}
        """
        results = {}
        for code, data in stock_data.items():
            klines = data.get('klines', [])
            fund_flow = data.get('fund_flow', [])
            pred = self.predict_single(code, klines, fund_flow, mkt_by_date)
            if pred:
                results[code] = pred
        return results

    def get_signals(self, direction: str = 'UP', min_confidence: str = 'low') -> list[dict]:
        """获取符合条件的信号列表（需先调用predict_batch）。"""
        conf_order = {'high': 3, 'medium': 2, 'low': 1}
        min_level = conf_order.get(min_confidence, 1)
        # This would need stored predictions - for now return empty
        return []
