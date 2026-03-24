#!/usr/bin/env python3
"""
V12 量价关系深度研究
====================
研究量价关系信号能否提高V12在所有市场环境下的预测准确率。

学术理论基础：
  1. Campbell, Grossman & Wang (1993 QJE):
     - 高成交量伴随的价格下跌更可能反转（非信息性交易）
     - 低成交量伴随的价格下跌更可能持续（信息性交易）
     → 核心洞察：成交量区分了"流动性冲击"和"信息驱动"

  2. Llorente, Michaely, Saar & Wang (2002 RFS):
     - 高成交量+正收益 → 信息驱动 → 收益持续
     - 高成交量+负收益 → 非信息驱动 → 收益反转
     → 量价交互项(volume×return)可以预测未来收益方向

  3. Gervais, Kaniel & Mingelgrin (2001 JF):
     - 异常高成交量的股票未来1个月收益更高（High-Volume Return Premium）
     - 在中国A股市场也被验证（Springer 2008）
     → 成交量冲击增加股票可见性，吸引后续买盘

  4. Wyckoff Volume Spread Analysis (1930s):
     - Effort vs Result: 大量成交量+小幅价格变动 = 供需平衡即将打破
     - Selling Climax: 放量暴跌后缩量 = 卖压衰竭
     - No Supply: 缩量回调 = 卖盘枯竭，即将上涨

研究方法：
  不直接修改V12引擎，而是在回测数据上计算量价指标，
  分析它们与预测准确率的关系，找到有效的量价过滤条件。

用法：
    source .venv/bin/activate
    python -m tools.analyze_v12_volume_price
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
from service.v12_prediction.v12_engine import V12PredictionEngine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent.parent / "data_results"


def _to_float(v):
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def load_stock_codes(limit=5000):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT stock_code, COUNT(*) AS cnt
        FROM stock_kline
        WHERE stock_code NOT LIKE '%%.BJ'
        GROUP BY stock_code
        HAVING cnt >= 120
        ORDER BY cnt DESC
        LIMIT %s
    """, (limit,))
    codes = [r['stock_code'] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return codes


def load_kline_data(stock_codes, start_date, end_date):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = defaultdict(list)
    bs = 300
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
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
    cur.close()
    conn.close()
    return dict(result)


def load_fund_flow_data(stock_codes, start_date):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = defaultdict(list)
    bs = 300
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
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
    cur.close()
    conn.close()
    return dict(result)


def load_market_klines(start_date, end_date):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute(
        "SELECT `date`, close_price, change_percent "
        "FROM stock_kline WHERE stock_code = '000001.SH' "
        "AND `date` >= %s AND `date` <= %s ORDER BY `date`",
        (start_date, end_date))
    result = []
    for row in cur.fetchall():
        result.append({
            'date': str(row['date']),
            'close': _to_float(row.get('close_price')),
            'change_percent': _to_float(row['change_percent']),
        })
    cur.close()
    conn.close()
    return result


def group_by_week(klines):
    weeks = defaultdict(list)
    for k in klines:
        d = datetime.strptime(k['date'][:10], '%Y-%m-%d')
        iso = d.isocalendar()
        key = f"{iso[0]}-W{iso[1]:02d}"
        weeks[key].append(k)
    return dict(weeks)


def prepare_backtest_data(stock_codes, n_weeks=100):
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=n_weeks * 7 + 120)).strftime('%Y-%m-%d')
    logger.info("加载数据: %d只股票, %s ~ %s", len(stock_codes), start_date, end_date)
    kline_data = load_kline_data(stock_codes, start_date, end_date)
    fund_flow_data = load_fund_flow_data(stock_codes, start_date)
    market_klines = load_market_klines(start_date, end_date)

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
            week_key = sorted_weeks[wi]
            next_week_key = sorted_weeks[wi + 1]
            week_klines = weekly_groups[week_key]
            next_klines = weekly_groups[next_week_key]
            last_date = week_klines[-1]['date']
            idx = None
            for j, k in enumerate(klines):
                if k['date'] == last_date:
                    idx = j
                    break
            if idx is None or idx < 60:
                continue
            base_close = week_klines[-1].get('close', 0)
            end_close = next_klines[-1].get('close', 0)
            if base_close <= 0 or end_close <= 0:
                continue
            actual_return = (end_close / base_close - 1) * 100
            hist_ff = [f for f in fund_flow if f['date'] <= last_date][:20]
            week_info[week_key] = {
                'hist_klines': klines[:idx + 1],
                'hist_ff': hist_ff,
                'actual_return': actual_return,
                'last_date': last_date,
            }
            all_week_keys.add(week_key)
        if week_info:
            stock_weekly[code] = week_info
    return stock_weekly, sorted(all_week_keys), market_klines


# ═══════════════════════════════════════════════════════════
# 量价指标计算
# ═══════════════════════════════════════════════════════════

def compute_volume_price_features(klines):
    """
    计算量价关系指标。基于学术文献，不做阈值优化。

    返回 dict of features, 如果数据不足返回 None。
    """
    if len(klines) < 25:
        return None

    close = [k.get('close', 0) or 0 for k in klines]
    volume = [k.get('volume', 0) or 0 for k in klines]
    pct = [k.get('change_percent', 0) or 0 for k in klines]
    turnover = [k.get('turnover', 0) or 0 for k in klines]

    if close[-1] <= 0 or volume[-1] <= 0:
        return None

    # ── 指标1: CGW量价交互项 (Campbell, Grossman & Wang 1993) ──
    # 核心思想：高成交量+下跌 → 非信息性(流动性冲击) → 反转
    #          低成交量+下跌 → 信息性(利空) → 持续
    # 计算：近5日的 volume_ratio × return 的符号
    vol_20_avg = sum(volume[-20:]) / 20 if len(volume) >= 20 else sum(volume[-5:]) / 5
    if vol_20_avg <= 0:
        return None

    # 近5日成交量比率（相对20日均量）
    vol_ratios_5d = [v / vol_20_avg for v in volume[-5:]] if vol_20_avg > 0 else [1] * 5
    avg_vol_ratio_5d = sum(vol_ratios_5d) / 5

    # 近5日收益
    ret_5d = (close[-1] / close[-6] - 1) * 100 if len(close) >= 6 and close[-6] > 0 else sum(pct[-5:])

    # CGW交互项：volume_ratio × return
    # 正值 = 放量上涨或缩量下跌（信息性，趋势持续）
    # 负值 = 放量下跌或缩量上涨（非信息性，趋势反转）
    cgw_interaction = avg_vol_ratio_5d * ret_5d

    # CGW分类
    if avg_vol_ratio_5d > 1.2 and ret_5d < -2:
        cgw_type = 'high_vol_decline'  # 放量下跌→非信息性→反转看涨
    elif avg_vol_ratio_5d < 0.8 and ret_5d < -2:
        cgw_type = 'low_vol_decline'   # 缩量下跌→信息性→持续看跌
    elif avg_vol_ratio_5d > 1.2 and ret_5d > 2:
        cgw_type = 'high_vol_rise'     # 放量上涨→信息性→持续看涨
    elif avg_vol_ratio_5d < 0.8 and ret_5d > 2:
        cgw_type = 'low_vol_rise'      # 缩量上涨→非信息性→反转看跌
    else:
        cgw_type = 'neutral'

    # ── 指标2: LMSW动态量价关系 (Llorente et al. 2002) ──
    # 计算每日的 volume×return 交互项，取5日均值
    # 正交互 → 信息驱动（持续），负交互 → 流动性驱动（反转）
    daily_interactions = []
    for i in range(-5, 0):
        idx = len(pct) + i
        if idx >= 0 and idx < len(volume) and vol_20_avg > 0:
            v_ratio = volume[idx] / vol_20_avg
            daily_interactions.append(v_ratio * pct[idx])
    lmsw_score = sum(daily_interactions) / len(daily_interactions) if daily_interactions else 0

    # ── 指标3: Wyckoff Effort vs Result ──
    # 大量成交量 + 小幅价格变动 = 供需即将失衡
    # 计算：5日总成交量 / 5日价格变动幅度
    total_vol_5d = sum(volume[-5:])
    price_range_5d = abs(ret_5d)
    if price_range_5d > 0.1:
        effort_result = (avg_vol_ratio_5d) / (price_range_5d / 5)
    else:
        effort_result = 0  # 价格几乎不动，无法判断

    # ── 指标4: 成交量趋势（5日 vs 前5日）──
    # 成交量递增/递减趋势
    if len(volume) >= 10:
        vol_recent_5 = sum(volume[-5:]) / 5
        vol_prev_5 = sum(volume[-10:-5]) / 5
        vol_trend = (vol_recent_5 / vol_prev_5 - 1) if vol_prev_5 > 0 else 0
    else:
        vol_trend = 0

    # ── 指标5: 量价背离 ──
    bullish_divergence = False
    bearish_divergence = False

    # ── 指标6: 下跌日成交量占比 ──
    # 近5日中下跌日的成交量占总成交量的比例
    # 高占比 = 卖压主导，低占比 = 买压主导
    down_vol = sum(volume[len(volume) - 5 + i] for i in range(5)
                   if pct[len(pct) - 5 + i] < 0)
    total_vol = sum(volume[-5:])
    down_vol_ratio = down_vol / total_vol if total_vol > 0 else 0.5

    # ── 指标7: 尾日量价特征 ──
    # 最后一天的成交量和涨跌幅的关系
    last_vol_ratio = volume[-1] / vol_20_avg if vol_20_avg > 0 else 1
    last_pct = pct[-1]

    # 尾日放量下跌 vs 尾日缩量下跌
    if last_pct < -1:
        if last_vol_ratio > 1.5:
            last_day_type = 'climax_sell'  # 恐慌性抛售（可能是卖压高潮）
        elif last_vol_ratio < 0.7:
            last_day_type = 'quiet_decline'  # 缩量阴跌（信息性下跌）
        else:
            last_day_type = 'normal_decline'
    elif last_pct > 1:
        if last_vol_ratio > 1.5:
            last_day_type = 'climax_buy'  # 放量追涨
        elif last_vol_ratio < 0.7:
            last_day_type = 'quiet_rise'  # 缩量上涨
        else:
            last_day_type = 'normal_rise'
    else:
        last_day_type = 'flat'

    return {
        'avg_vol_ratio_5d': round(avg_vol_ratio_5d, 4),
        'ret_5d': round(ret_5d, 4),
        'cgw_interaction': round(cgw_interaction, 4),
        'cgw_type': cgw_type,
        'lmsw_score': round(lmsw_score, 4),
        'effort_result': round(effort_result, 4),
        'vol_trend': round(vol_trend, 4),
        'bullish_divergence': bullish_divergence,
        'bearish_divergence': bearish_divergence,
        'down_vol_ratio': round(down_vol_ratio, 4),
        'last_vol_ratio': round(last_vol_ratio, 4),
        'last_day_type': last_day_type,
    }


# ═══════════════════════════════════════════════════════════
# 主分析：量价指标与V12预测准确率的关系
# ═══════════════════════════════════════════════════════════

def run_volume_price_analysis():
    t0 = time.time()
    logger.info("=" * 70)
    logger.info("V12 量价关系深度研究")
    logger.info("=" * 70)

    # 加载数据
    logger.info("[0/4] 加载数据...")
    stock_codes = load_stock_codes(5000)
    stock_weekly, all_weeks, market_klines = prepare_backtest_data(stock_codes, n_weeks=100)
    if len(all_weeks) > 100:
        all_weeks = all_weeks[-100:]
    logger.info("  数据准备完成: %d只股票, %d周", len(stock_weekly), len(all_weeks))

    # 计算大盘周涨跌
    mkt_weekly = group_by_week(market_klines)
    mkt_week_chg = {}
    for wk, kls in mkt_weekly.items():
        if len(kls) >= 2:
            fc = kls[0].get('close', 0)
            lc = kls[-1].get('close', 0)
            if fc > 0:
                mkt_week_chg[wk] = (lc / fc - 1) * 100

    # 运行V12预测 + 计算量价指标
    logger.info("[1/4] 运行V12预测 + 计算量价指标...")
    results = []

    for week_key in all_weeks:
        engine = V12PredictionEngine()
        # 截面中位数
        week_vols = []
        week_turns = []
        for code, week_info in stock_weekly.items():
            if week_key not in week_info:
                continue
            kls = week_info[week_key]['hist_klines']
            if len(kls) >= 20:
                pcts = [k.get('change_percent', 0) or 0 for k in kls]
                recent = pcts[-20:]
                m = sum(recent) / 20
                vol = (sum((p - m) ** 2 for p in recent) / 19) ** 0.5
                week_vols.append(vol)
                turnover_vals = [k.get('turnover', 0) or 0 for k in kls]
                avg_turn = sum(turnover_vals[-20:]) / 20
                week_turns.append(avg_turn)
        vol_median = sorted(week_vols)[len(week_vols) // 2] if week_vols else None
        turn_median = sorted(week_turns)[len(week_turns) // 2] if week_turns else None

        for code, week_info in stock_weekly.items():
            if week_key not in week_info:
                continue
            data = week_info[week_key]
            last_date = data.get('last_date', '')
            mkt_hist = [m for m in market_klines if m['date'] <= last_date] if market_klines else None

            pred = engine.predict_single(code, data['hist_klines'], data['hist_ff'],
                                         mkt_hist, vol_median, turn_median)
            if pred is not None:
                actual = data['actual_return']
                is_correct = (pred['pred_direction'] == 'UP') == (actual > 0)

                # 计算量价指标
                vp_features = compute_volume_price_features(data['hist_klines'])

                results.append({
                    'week': week_key,
                    'code': code,
                    'pred': pred,
                    'actual_return': actual,
                    'is_correct': is_correct,
                    'vp': vp_features,
                })

    logger.info("  完成: %d条预测, %d条有量价数据",
                len(results), sum(1 for r in results if r['vp'] is not None))

    # 分析量价指标与准确率的关系
    logger.info("[2/4] 分析量价指标与准确率的关系...")

    def acc(lst):
        if not lst:
            return 0, 0
        c = sum(1 for r in lst if r['is_correct'])
        return round(c / len(lst), 4), len(lst)

    # 分类市场环境
    sideways_weeks = set(wk for wk, chg in mkt_week_chg.items() if -1 <= chg <= 1)
    decline_weeks = set(wk for wk, chg in mkt_week_chg.items() if chg < -1)

    report = {}

    # ── 分析1: CGW量价分类 ──
    logger.info("  CGW量价分类...")
    cgw_analysis = {}
    for label, subset in [
        ('全部', results),
        ('震荡市', [r for r in results if r['week'] in sideways_weeks]),
        ('下跌市', [r for r in results if r['week'] in decline_weeks]),
    ]:
        by_cgw = defaultdict(list)
        for r in subset:
            if r['vp']:
                by_cgw[r['vp']['cgw_type']].append(r)
        cgw_stats = {}
        for cgw_type in sorted(by_cgw.keys()):
            a, n = acc(by_cgw[cgw_type])
            cgw_stats[cgw_type] = {'accuracy': a, 'n': n}
        cgw_analysis[label] = cgw_stats
    report['cgw_classification'] = cgw_analysis

    # ── 分析2: 成交量比率分组 ──
    logger.info("  成交量比率分组...")
    vol_ratio_analysis = {}
    for label, subset in [
        ('全部', results),
        ('震荡市', [r for r in results if r['week'] in sideways_weeks]),
        ('下跌市', [r for r in results if r['week'] in decline_weeks]),
    ]:
        bins = {
            'vol<0.5x': lambda r: r['vp'] and r['vp']['avg_vol_ratio_5d'] < 0.5,
            'vol_0.5-0.8x': lambda r: r['vp'] and 0.5 <= r['vp']['avg_vol_ratio_5d'] < 0.8,
            'vol_0.8-1.2x': lambda r: r['vp'] and 0.8 <= r['vp']['avg_vol_ratio_5d'] < 1.2,
            'vol_1.2-2.0x': lambda r: r['vp'] and 1.2 <= r['vp']['avg_vol_ratio_5d'] < 2.0,
            'vol>2.0x': lambda r: r['vp'] and r['vp']['avg_vol_ratio_5d'] >= 2.0,
        }
        bin_stats = {}
        for bin_name, bin_fn in bins.items():
            matched = [r for r in subset if bin_fn(r)]
            a, n = acc(matched)
            bin_stats[bin_name] = {'accuracy': a, 'n': n}
        vol_ratio_analysis[label] = bin_stats
    report['volume_ratio_bins'] = vol_ratio_analysis

    # ── 分析3: LMSW动态量价关系 ──
    logger.info("  LMSW动态量价关系...")
    lmsw_analysis = {}
    for label, subset in [
        ('全部', results),
        ('震荡市', [r for r in results if r['week'] in sideways_weeks]),
        ('下跌市', [r for r in results if r['week'] in decline_weeks]),
    ]:
        bins = {
            'lmsw_strong_neg(<-5)': lambda r: r['vp'] and r['vp']['lmsw_score'] < -5,
            'lmsw_neg(-5~-1)': lambda r: r['vp'] and -5 <= r['vp']['lmsw_score'] < -1,
            'lmsw_neutral(-1~1)': lambda r: r['vp'] and -1 <= r['vp']['lmsw_score'] <= 1,
            'lmsw_pos(1~5)': lambda r: r['vp'] and 1 < r['vp']['lmsw_score'] <= 5,
            'lmsw_strong_pos(>5)': lambda r: r['vp'] and r['vp']['lmsw_score'] > 5,
        }
        bin_stats = {}
        for bin_name, bin_fn in bins.items():
            matched = [r for r in subset if bin_fn(r)]
            a, n = acc(matched)
            bin_stats[bin_name] = {'accuracy': a, 'n': n}
        lmsw_analysis[label] = bin_stats
    report['lmsw_dynamic'] = lmsw_analysis

    # ── 分析4: 尾日量价类型 ──
    logger.info("  尾日量价类型...")
    last_day_analysis = {}
    for label, subset in [
        ('全部', results),
        ('震荡市', [r for r in results if r['week'] in sideways_weeks]),
        ('下跌市', [r for r in results if r['week'] in decline_weeks]),
    ]:
        by_type = defaultdict(list)
        for r in subset:
            if r['vp']:
                by_type[r['vp']['last_day_type']].append(r)
        type_stats = {}
        for t in sorted(by_type.keys()):
            a, n = acc(by_type[t])
            type_stats[t] = {'accuracy': a, 'n': n}
        last_day_analysis[label] = type_stats
    report['last_day_type'] = last_day_analysis

    # ── 分析5: 下跌日成交量占比 ──
    logger.info("  下跌日成交量占比...")
    down_vol_analysis = {}
    for label, subset in [
        ('全部', results),
        ('震荡市', [r for r in results if r['week'] in sideways_weeks]),
        ('下跌市', [r for r in results if r['week'] in decline_weeks]),
    ]:
        bins = {
            'down_vol<30%': lambda r: r['vp'] and r['vp']['down_vol_ratio'] < 0.3,
            'down_vol_30-50%': lambda r: r['vp'] and 0.3 <= r['vp']['down_vol_ratio'] < 0.5,
            'down_vol_50-70%': lambda r: r['vp'] and 0.5 <= r['vp']['down_vol_ratio'] < 0.7,
            'down_vol>70%': lambda r: r['vp'] and r['vp']['down_vol_ratio'] >= 0.7,
        }
        bin_stats = {}
        for bin_name, bin_fn in bins.items():
            matched = [r for r in subset if bin_fn(r)]
            a, n = acc(matched)
            bin_stats[bin_name] = {'accuracy': a, 'n': n}
        down_vol_analysis[label] = bin_stats
    report['down_volume_ratio'] = down_vol_analysis

    # ── 分析7: 成交量趋势 ──
    logger.info("  成交量趋势...")
    vol_trend_analysis = {}
    for label, subset in [
        ('全部', results),
        ('震荡市', [r for r in results if r['week'] in sideways_weeks]),
        ('下跌市', [r for r in results if r['week'] in decline_weeks]),
    ]:
        bins = {
            'vol_shrinking(<-30%)': lambda r: r['vp'] and r['vp']['vol_trend'] < -0.3,
            'vol_declining(-30%~-10%)': lambda r: r['vp'] and -0.3 <= r['vp']['vol_trend'] < -0.1,
            'vol_stable(-10%~+10%)': lambda r: r['vp'] and -0.1 <= r['vp']['vol_trend'] <= 0.1,
            'vol_rising(+10%~+50%)': lambda r: r['vp'] and 0.1 < r['vp']['vol_trend'] <= 0.5,
            'vol_surging(>+50%)': lambda r: r['vp'] and r['vp']['vol_trend'] > 0.5,
        }
        bin_stats = {}
        for bin_name, bin_fn in bins.items():
            matched = [r for r in subset if bin_fn(r)]
            a, n = acc(matched)
            bin_stats[bin_name] = {'accuracy': a, 'n': n}
        vol_trend_analysis[label] = bin_stats
    report['volume_trend'] = vol_trend_analysis

    # ── 分析8: 量价组合因子（交叉分析）──
    logger.info("[3/4] 量价组合因子交叉分析...")
    combo_analysis = {}

    # 在所有环境下测试组合
    for label, subset in [
        ('全部', results),
        ('震荡市', [r for r in results if r['week'] in sideways_weeks]),
        ('下跌市', [r for r in results if r['week'] in decline_weeks]),
    ]:
        combos = {}

        # 组合A: CGW放量下跌 + V12预测UP
        ca = [r for r in subset if r['vp'] and r['vp']['cgw_type'] == 'high_vol_decline'
              and r['pred']['pred_direction'] == 'UP']
        combos['CGW放量下跌+UP'] = acc(ca)

        # 组合B: CGW缩量下跌 + V12预测UP
        cb = [r for r in subset if r['vp'] and r['vp']['cgw_type'] == 'low_vol_decline'
              and r['pred']['pred_direction'] == 'UP']
        combos['CGW缩量下跌+UP'] = acc(cb)

        # 组合C: 尾日恐慌抛售 + V12预测UP
        cc = [r for r in subset if r['vp'] and r['vp']['last_day_type'] == 'climax_sell'
              and r['pred']['pred_direction'] == 'UP']
        combos['尾日恐慌抛售+UP'] = acc(cc)

        # 组合D: 尾日缩量阴跌 + V12预测UP
        cd = [r for r in subset if r['vp'] and r['vp']['last_day_type'] == 'quiet_decline'
              and r['pred']['pred_direction'] == 'UP']
        combos['尾日缩量阴跌+UP'] = acc(cd)

        # 组合E: 成交量萎缩 + V12预测UP
        ce = [r for r in subset if r['vp'] and r['vp']['vol_trend'] < -0.3
              and r['pred']['pred_direction'] == 'UP']
        combos['成交量萎缩+UP'] = acc(ce)

        # 组合F: 下跌日量占比>70% + V12预测UP（卖压集中释放）
        cg = [r for r in subset if r['vp'] and r['vp']['down_vol_ratio'] > 0.7
              and r['pred']['pred_direction'] == 'UP']
        combos['下跌日量>70%+UP'] = acc(cg)

        # 组合H: CGW放量下跌 + ns>=4
        ch = [r for r in subset if r['vp'] and r['vp']['cgw_type'] == 'high_vol_decline'
              and r['pred'].get('n_supporting', 0) >= 4]
        combos['CGW放量下跌+ns≥4'] = acc(ch)

        # 组合I: CGW放量下跌 + es>=7
        ci = [r for r in subset if r['vp'] and r['vp']['cgw_type'] == 'high_vol_decline'
              and r['pred'].get('extreme_score', 0) >= 7]
        combos['CGW放量下跌+es≥7'] = acc(ci)

        # 组合J: CGW放量下跌 + ns>=4 + es>=7
        cj = [r for r in subset if r['vp'] and r['vp']['cgw_type'] == 'high_vol_decline'
              and r['pred'].get('n_supporting', 0) >= 4
              and r['pred'].get('extreme_score', 0) >= 7]
        combos['CGW放量下跌+ns≥4+es≥7'] = acc(cj)

        # 组合K: LMSW强负值（流动性冲击）+ UP
        ck = [r for r in subset if r['vp'] and r['vp']['lmsw_score'] < -5
              and r['pred']['pred_direction'] == 'UP']
        combos['LMSW强负+UP'] = acc(ck)

        # 组合L: 尾日恐慌 + 成交量萎缩趋势（Wyckoff卖压高潮后缩量）
        cl = [r for r in subset if r['vp']
              and r['vp']['last_day_type'] == 'climax_sell'
              and r['vp']['vol_trend'] < -0.1]
        combos['尾日恐慌+量缩趋势'] = acc(cl)

        # 组合M: CGW缩量下跌 排除（信息性下跌不应预测反转）
        cm_all = [r for r in subset if r['vp']]
        cm_exclude = [r for r in cm_all if r['vp']['cgw_type'] != 'low_vol_decline']
        combos['排除缩量下跌'] = acc(cm_exclude)

        # 基准
        combos['基准(全部)'] = acc(subset)

        combo_analysis[label] = {name: {'accuracy': a, 'n': n} for name, (a, n) in combos.items()}

    report['combo_analysis'] = combo_analysis

    # ── 分析9: 周度一致性验证 ──
    logger.info("[4/4] 量价因子周度一致性...")
    weekly_consistency = {}

    # 测试几个关键量价因子的周胜率
    factor_tests = {
        'CGW放量下跌': lambda r: r['vp'] and r['vp']['cgw_type'] == 'high_vol_decline',
        'CGW缩量下跌': lambda r: r['vp'] and r['vp']['cgw_type'] == 'low_vol_decline',
        '排除缩量下跌': lambda r: r['vp'] and r['vp']['cgw_type'] != 'low_vol_decline',
        '尾日恐慌抛售': lambda r: r['vp'] and r['vp']['last_day_type'] == 'climax_sell',
        'LMSW负值': lambda r: r['vp'] and r['vp']['lmsw_score'] < -1,
        '全部(基准)': lambda r: True,
    }

    for factor_name, factor_fn in factor_tests.items():
        weekly_stats = defaultdict(lambda: {'total': 0, 'correct': 0})
        for r in results:
            if factor_fn(r):
                weekly_stats[r['week']]['total'] += 1
                if r['is_correct']:
                    weekly_stats[r['week']]['correct'] += 1

        week_accs = [v['correct'] / v['total']
                     for v in weekly_stats.values() if v['total'] >= 5]
        if week_accs:
            mean_acc = sum(week_accs) / len(week_accs)
            std_acc = (sum((a - mean_acc)**2 for a in week_accs) / len(week_accs))**0.5
            win_rate = sum(1 for a in week_accs if a > 0.5) / len(week_accs)
            weekly_consistency[factor_name] = {
                'n_valid_weeks': len(week_accs),
                'mean_weekly_acc': round(mean_acc, 4),
                'std': round(std_acc, 4),
                'weekly_win_rate': round(win_rate, 4),
            }
        else:
            weekly_consistency[factor_name] = {'n_valid_weeks': 0}

    report['weekly_consistency'] = weekly_consistency

    # 保存
    output_path = OUTPUT_DIR / "v12_volume_price_analysis.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    logger.info("\n结果已保存: %s", output_path)

    # ═══════════════════════════════════════════════════════════
    # 打印关键发现
    # ═══════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("V12 量价关系深度研究 — 关键发现")
    print("=" * 70)

    print("\n📊 CGW量价分类 (Campbell, Grossman & Wang 1993):")
    for label in ['全部', '震荡市', '下跌市']:
        print(f"\n  [{label}]")
        for cgw_type, stats in sorted(report['cgw_classification'][label].items()):
            if stats['n'] > 0:
                print(f"    {cgw_type:20s}: {stats['accuracy']:.1%} ({stats['n']}条)")

    print("\n📊 成交量比率分组:")
    for label in ['全部', '震荡市', '下跌市']:
        print(f"\n  [{label}]")
        for bin_name, stats in report['volume_ratio_bins'][label].items():
            if stats['n'] > 0:
                print(f"    {bin_name:20s}: {stats['accuracy']:.1%} ({stats['n']}条)")

    print("\n📊 LMSW动态量价关系 (Llorente et al. 2002):")
    for label in ['全部', '震荡市']:
        print(f"\n  [{label}]")
        for bin_name, stats in report['lmsw_dynamic'][label].items():
            if stats['n'] > 0:
                print(f"    {bin_name:30s}: {stats['accuracy']:.1%} ({stats['n']}条)")

    print("\n📊 尾日量价类型:")
    for label in ['全部', '震荡市', '下跌市']:
        print(f"\n  [{label}]")
        for t, stats in sorted(report['last_day_type'][label].items()):
            if stats['n'] > 0:
                print(f"    {t:20s}: {stats['accuracy']:.1%} ({stats['n']}条)")

    print("\n📊 下跌日成交量占比:")
    for label in ['全部', '震荡市']:
        print(f"\n  [{label}]")
        for bin_name, stats in report['down_volume_ratio'][label].items():
            if stats['n'] > 0:
                print(f"    {bin_name:20s}: {stats['accuracy']:.1%} ({stats['n']}条)")

    print("\n📊 成交量趋势:")
    for label in ['全部', '震荡市']:
        print(f"\n  [{label}]")
        for bin_name, stats in report['volume_trend'][label].items():
            if stats['n'] > 0:
                print(f"    {bin_name:30s}: {stats['accuracy']:.1%} ({stats['n']}条)")

    print("\n🎯 量价组合因子:")
    for label in ['全部', '震荡市', '下跌市']:
        print(f"\n  [{label}]")
        base_acc = report['combo_analysis'][label].get('基准(全部)', {}).get('accuracy', 0)
        for name, stats in sorted(report['combo_analysis'][label].items(),
                                   key=lambda x: x[1].get('accuracy', 0), reverse=True):
            if stats['n'] >= 10:
                delta = stats['accuracy'] - base_acc
                marker = '✅' if delta > 0.03 else '  '
                print(f"    {marker} {name:30s}: {stats['accuracy']:.1%} ({stats['n']}条) Δ={delta:+.1%}")

    print("\n📅 周度一致性:")
    for name, stats in sorted(weekly_consistency.items(),
                               key=lambda x: x[1].get('weekly_win_rate', 0), reverse=True):
        if stats.get('n_valid_weeks', 0) > 0:
            print(f"    {name:20s}: 胜率={stats['weekly_win_rate']:.1%} 均准={stats['mean_weekly_acc']:.1%} "
                  f"std={stats['std']:.1%} ({stats['n_valid_weeks']}周)")

    print(f"\n⏱️  总耗时: {time.time() - t0:.1f}秒")
    print("=" * 70)

    return report


if __name__ == '__main__':
    run_volume_price_analysis()
