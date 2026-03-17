#!/usr/bin/env python3
"""
多维度深度选股分析 — 综合数据库全部数据维度
==========================================
结合以下7大维度进行综合评分，筛选未来一个月最有可能上涨的10只股票：

1. 技术面 (K线趋势、均线、动量)     权重 22%
2. 资金面 (主力资金流向)             权重 18%
3. 概念板块强弱势                    权重 13%
4. 周预测信号                        权重 13%
5. 财务基本面                        权重 13%
6. 龙虎榜 + 市场情绪                 权重 8%
7. 成交量形态 (量价关系)             权重 13%

成交量规则来源：service/weekly_prediction_service.py _detect_volume_patterns()
回测验证：142,170样本，量确认70.5% vs 量矛盾52.7%

用法：
    python -m tools.deep_stock_analysis
"""
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal

sys.path.insert(0, '.')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

from dao import get_connection


def _f(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def _parse_pct(val) -> float | None:
    if val is None:
        return None
    if isinstance(val, (int, float, Decimal)):
        return float(val)
    s = str(val).strip().replace('%', '').replace('％', '')
    if s in ('', '-', '--', 'N/A'):
        return None
    try:
        return float(s)
    except ValueError:
        return None


# ═══════════════════════════════════════════════════════════
# Step 1: 用SQL快速筛选候选池 (概念强势 + 周预测看涨 + 资金流入)
# ═══════════════════════════════════════════════════════════

def _get_candidate_pool(cur) -> list[str]:
    """
    从多个维度快速筛选出高质量候选股票池（约200-500只）。
    策略：概念板块强势股 ∪ 周预测看涨高置信 ∪ 近期资金净流入
    """
    logger.info("🔍 Step1: 快速筛选候选股票池...")

    candidates = set()

    # (1) 概念板块强势TOP300 (concept_strength表中stock_code为纯6位数字)
    cur.execute("""
        SELECT stock_code FROM (
            SELECT stock_code, MAX(strength_score) AS ms
            FROM stock_concept_strength
            WHERE score_date = (SELECT MAX(score_date) FROM stock_concept_strength)
            GROUP BY stock_code
            ORDER BY ms DESC
            LIMIT 300
        ) t
    """)
    # 转换为带后缀格式
    concept_top = set()
    for r in cur.fetchall():
        c = r['stock_code']
        if c.startswith('6'):
            concept_top.add(f"{c}.SH")
        elif c.startswith(('0', '3')):
            concept_top.add(f"{c}.SZ")
        elif c.startswith(('4', '8', '9')):
            concept_top.add(f"{c}.BJ")
    # 过滤掉北交所
    concept_top = {c for c in concept_top if not c.endswith('.BJ')}
    candidates.update(concept_top)
    logger.info("  概念强势TOP300: %d 只", len(concept_top))

    # (2) 周预测看涨 + 高/中置信度
    cur.execute("""
        SELECT stock_code FROM stock_weekly_prediction
        WHERE pred_direction = 'UP'
          AND confidence IN ('high', 'medium')
          AND stock_code NOT LIKE '%%BJ'
    """)
    pred_up = {r['stock_code'] for r in cur.fetchall()}
    candidates.update(pred_up)
    logger.info("  周预测看涨(高/中置信): %d 只", len(pred_up))

    # (3) 近5日资金净流入TOP200
    cur.execute("""
        SELECT stock_code FROM (
            SELECT stock_code, SUM(big_net) AS total_big
            FROM stock_fund_flow
            WHERE `date` >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 7 DAY), '%%Y-%%m-%%d')
              AND stock_code NOT LIKE '%%BJ'
            GROUP BY stock_code
            HAVING total_big > 0
            ORDER BY total_big DESC
            LIMIT 200
        ) t
    """)
    fund_top = {r['stock_code'] for r in cur.fetchall()}
    candidates.update(fund_top)
    logger.info("  资金净流入TOP200: %d 只", len(fund_top))

    # (4) 近30天龙虎榜上榜股
    cur.execute("""
        SELECT DISTINCT stock_code FROM stock_dragon_tiger
        WHERE trade_date >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 30 DAY), '%%Y-%%m-%%d')
          AND stock_code NOT LIKE '%%BJ'
    """)
    dragon = {r['stock_code'] for r in cur.fetchall()}
    candidates.update(dragon)
    logger.info("  龙虎榜上榜: %d 只", len(dragon))

    result = sorted(candidates)
    logger.info("  合并去重后候选池: %d 只", len(result))
    return result


# ═══════════════════════════════════════════════════════════
# Step 2: 批量加载各维度数据
# ═══════════════════════════════════════════════════════════

def _load_kline_data(cur, codes: list[str]) -> dict[str, list[dict]]:
    """分批加载K线数据"""
    logger.info("📊 加载K线数据...")
    stock_klines = defaultdict(list)
    batch_size = 100
    for i in range(0, len(codes), batch_size):
        batch = codes[i:i+batch_size]
        placeholders = ','.join(['%s'] * len(batch))
        cur.execute(f"""
            SELECT stock_code, `date`, open_price, close_price, high_price, low_price,
                   change_percent, change_hand, amplitude, trading_volume, trading_amount
            FROM stock_kline
            WHERE stock_code IN ({placeholders})
              AND `date` >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 100 DAY), '%%Y-%%m-%%d')
            ORDER BY stock_code, `date`
        """, batch)
        for r in cur.fetchall():
            stock_klines[r['stock_code']].append(r)
    logger.info("  K线数据加载完成: %d 只股票", len(stock_klines))
    return dict(stock_klines)


def _load_fund_flow_data(cur, codes: list[str]) -> dict[str, list[dict]]:
    """分批加载资金流向数据"""
    logger.info("💰 加载资金流向数据...")
    stock_flows = defaultdict(list)
    batch_size = 100
    for i in range(0, len(codes), batch_size):
        batch = codes[i:i+batch_size]
        placeholders = ','.join(['%s'] * len(batch))
        cur.execute(f"""
            SELECT stock_code, `date`, net_flow, main_net_5day,
                   big_net, big_net_pct, mid_net, small_net
            FROM stock_fund_flow
            WHERE stock_code IN ({placeholders})
              AND `date` >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 30 DAY), '%%Y-%%m-%%d')
            ORDER BY stock_code, `date`
        """, batch)
        for r in cur.fetchall():
            stock_flows[r['stock_code']].append(r)
    logger.info("  资金流向加载完成: %d 只股票", len(stock_flows))
    return dict(stock_flows)


# ═══════════════════════════════════════════════════════════
# Step 3: 各维度评分计算
# ═══════════════════════════════════════════════════════════

def _score_technical(klines: list[dict]) -> tuple[float, dict]:
    """技术面评分"""
    if len(klines) < 30:
        return 0.0, {}

    closes = [_f(k['close_price']) for k in klines]
    changes = [_f(k['change_percent']) for k in klines]
    turnover = [_f(k['change_hand']) for k in klines]
    amounts = [_f(k['trading_amount']) for k in klines]
    highs = [_f(k['high_price']) for k in klines]
    lows = [_f(k['low_price']) for k in klines]

    if closes[-1] <= 0:
        return 0.0, {}

    score = 0.0
    detail = {}

    # (1) 均线多头排列
    ma5 = sum(closes[-5:]) / 5
    ma10 = sum(closes[-10:]) / 10
    ma20 = sum(closes[-20:]) / 20
    ma60 = sum(closes[-60:]) / 60 if len(closes) >= 60 else ma20

    if ma5 > ma10 > ma20 > ma60:
        score += 25
    elif ma5 > ma10 > ma20:
        score += 18
    elif ma5 > ma10:
        score += 10
    elif ma5 < ma10 < ma20:
        score -= 5

    if closes[-1] > ma20:
        score += 5
    if closes[-1] > ma60:
        score += 5
    detail['ma_alignment'] = 'bullish' if ma5 > ma10 > ma20 else 'bearish'

    # (2) 近5日动量
    chg_5d = sum(changes[-5:])
    if 0 < chg_5d <= 8:
        score += 10
    elif chg_5d > 8:
        score += 5
    elif -3 < chg_5d <= 0:
        score += 3
    detail['chg_5d'] = round(chg_5d, 2)

    # (3) 近20日趋势
    chg_20d = sum(changes[-20:])
    if 0 < chg_20d <= 15:
        score += 10
    elif chg_20d > 15:
        score += 3
    detail['chg_20d'] = round(chg_20d, 2)

    # (4) 60日位置
    n = min(60, len(closes))
    high_n = max(highs[-n:])
    low_n = min(lows[-n:])
    pos60 = (closes[-1] - low_n) / (high_n - low_n) if high_n > low_n else 0.5
    if 0.2 <= pos60 <= 0.5:
        score += 10
    elif 0.5 < pos60 <= 0.7:
        score += 5
    elif pos60 > 0.85:
        score -= 5
    detail['pos60'] = round(pos60, 3)

    # (5) 换手率
    avg_turnover = sum(turnover[-5:]) / 5 if turnover[-5:] else 0
    if 2 <= avg_turnover <= 8:
        score += 5
    elif avg_turnover > 15:
        score -= 3
    detail['avg_turnover_5d'] = round(avg_turnover, 2)

    # (6) 成交量趋势
    vol_ratio = 1.0
    if len(amounts) >= 10:
        vol_recent = sum(amounts[-5:])
        vol_prev = sum(amounts[-10:-5])
        if vol_prev > 0:
            vol_ratio = vol_recent / vol_prev
            if 1.2 <= vol_ratio <= 2.5:
                score += 8
            elif vol_ratio > 3:
                score += 2
    detail['vol_ratio'] = round(vol_ratio, 2)

    # (7) 振幅收窄
    if len(changes) >= 10:
        amp_recent = sum(abs(c) for c in changes[-5:]) / 5
        amp_prev = sum(abs(c) for c in changes[-10:-5]) / 5
        if amp_prev > 0 and amp_recent / amp_prev < 0.6:
            score += 5

    score = max(0, min(100, score))
    detail['latest_price'] = closes[-1]
    detail['latest_date'] = klines[-1]['date']
    return round(score, 1), detail


def _score_fund_flow(flows: list[dict]) -> tuple[float, dict]:
    """资金面评分"""
    if len(flows) < 5:
        return 0.0, {}

    score = 0.0
    detail = {}

    recent_5 = flows[-5:]
    net_5d = sum(_f(f['big_net']) for f in recent_5)
    net_5d_pct = sum(_f(f['big_net_pct']) for f in recent_5)

    if net_5d > 0:
        score += 15
        if net_5d_pct > 5:
            score += 10
    elif net_5d < 0:
        score -= 5
    detail['net_5d'] = round(net_5d, 2)
    detail['net_5d_pct'] = round(net_5d_pct, 2)

    net_20d = sum(_f(f['big_net']) for f in flows[-20:])
    if net_20d > 0:
        score += 10
    detail['net_20d'] = round(net_20d, 2)

    latest_main_5d = _f(flows[-1].get('main_net_5day'))
    if latest_main_5d > 0:
        score += 10
    detail['main_net_5day'] = round(latest_main_5d, 2)

    if len(flows) >= 6:
        recent_3 = sum(_f(f['big_net']) for f in flows[-3:])
        prev_3 = sum(_f(f['big_net']) for f in flows[-6:-3])
        if recent_3 > prev_3 and recent_3 > 0:
            score += 10
        detail['flow_accel'] = round(recent_3 - prev_3, 2)

    small_net_5d = sum(_f(f['small_net']) for f in recent_5)
    if net_5d > 0 and small_net_5d < 0:
        score += 10
        detail['smart_money'] = True

    score = max(0, min(100, score))
    return round(score, 1), detail


# ═══════════════════════════════════════════════════════════
# Step 3b: 成交量形态评分 (移植自 weekly_prediction_service._detect_volume_patterns)
# 回测验证: 142,170样本, 量确认70.5% vs 量矛盾52.7%
# ═══════════════════════════════════════════════════════════

def _vol_mean(lst):
    return sum(lst) / len(lst) if lst else 0.0


def _vol_compound_return(pcts):
    r = 1.0
    for p in pcts:
        r *= (1 + p / 100)
    return round((r - 1) * 100, 4)


def _detect_volume_patterns(klines: list[dict]) -> dict:
    """检测成交量形态信号 — 适配 stock_kline 表字段名。

    原始逻辑来自 service/weekly_prediction_service.py _detect_volume_patterns()
    适配差异：
    - stock_kline 字段: open_price, close_price, trading_volume, change_percent
    - 原函数字段: open, close, volume, change_percent
    - 原函数分 week_klines / all_klines，这里用最近5日作为"本周"，之前作为历史
    """
    result = {
        'vol_direction': None,
        'vol_strength': 0.0,
        'panic_bottom': False,
        'sky_vol_bearish': False,
        'price_up_vol_down': False,
        'rush_up_shrink': False,
        'high_pos_down_vol_up': False,
        'price_position': None,
        'vol_ratio_20': None,
    }

    if len(klines) < 25:
        return result

    # 最近5日作为"本周"，之前作为历史
    week_klines = klines[-5:]
    hist = klines[:-5]

    week_vols = [_f(k.get('trading_volume', 0)) for k in week_klines if _f(k.get('trading_volume', 0)) > 0]
    if not week_vols:
        return result

    week_avg_vol = _vol_mean(week_vols)
    week_chg = _vol_compound_return([_f(k['change_percent']) for k in week_klines])

    hist_vols_20 = [_f(k.get('trading_volume', 0)) for k in hist[-20:] if _f(k.get('trading_volume', 0)) > 0]
    hist_vols_60 = [_f(k.get('trading_volume', 0)) for k in hist[-60:] if _f(k.get('trading_volume', 0)) > 0]
    avg_vol_20 = _vol_mean(hist_vols_20) if hist_vols_20 else 0
    avg_vol_60 = _vol_mean(hist_vols_60) if hist_vols_60 else 0
    vol_ratio_20 = week_avg_vol / avg_vol_20 if avg_vol_20 > 0 else None
    result['vol_ratio_20'] = round(vol_ratio_20, 3) if vol_ratio_20 else None

    # 价格位置（相对60日高低点）
    hist_closes = [_f(k['close_price']) for k in hist[-60:] if _f(k.get('close_price', 0)) > 0]
    if hist_closes:
        all_c = hist_closes + [_f(k['close_price']) for k in week_klines if _f(k.get('close_price', 0)) > 0]
        if all_c:
            min_c, max_c = min(all_c), max(all_c)
            latest_c = _f(week_klines[-1].get('close_price', 0))
            if max_c > min_c and latest_c > 0:
                result['price_position'] = round((latest_c - min_c) / (max_c - min_c), 4)

    pp = result['price_position']

    # 个股历史波动率（用于自适应阈值）
    hist_chgs = [abs(_f(k['change_percent'])) for k in hist[-20:] if k.get('change_percent') is not None]
    avg_volatility = _vol_mean(hist_chgs) if hist_chgs else 2.0

    # ── 信号检测（按回测验证的准确率排序）──

    # 1. 恐慌底: 近5日跌 + 放量 + 低位
    panic_chg_th = max(-1.0, -avg_volatility * 0.5)
    if week_chg < panic_chg_th and vol_ratio_20 is not None and vol_ratio_20 > 1.3:
        if pp is not None and pp < 0.25:
            result['panic_bottom'] = True

    # 2. 天量阴线（扫描近5日K线）
    if avg_vol_60 > 0:
        for k in week_klines:
            vol = _f(k.get('trading_volume', 0))
            if vol > avg_vol_60 * 3.0 and _f(k.get('close_price', 0)) < _f(k.get('open_price', 0)):
                result['sky_vol_bearish'] = True

    # 3. 价升量缩（量价背离）
    if week_chg > 0.5 and vol_ratio_20 is not None and vol_ratio_20 < 0.8:
        result['price_up_vol_down'] = True

    # 4. 急涨后缩量（诱多出货形态）
    if len(week_klines) >= 4:
        mid = len(week_klines) // 2
        first_chg = _vol_compound_return([_f(k['change_percent']) for k in week_klines[:mid]])
        first_vol = _vol_mean([_f(k.get('trading_volume', 0)) for k in week_klines[:mid]])
        second_vol = _vol_mean([_f(k.get('trading_volume', 0)) for k in week_klines[mid:]])
        if first_chg > 2.0 and first_vol > 0 and second_vol < first_vol * 0.6:
            result['rush_up_shrink'] = True

    # 5. 高位价跌量增（顶部放量下跌）
    if (week_chg < -1.0 and vol_ratio_20 is not None and vol_ratio_20 > 1.3
            and pp is not None and pp > 0.75):
        result['high_pos_down_vol_up'] = True

    # ── 推断方向 + 信号强度 ──
    if result['panic_bottom']:
        result['vol_direction'] = 'up'
        strength = min(1.0, (vol_ratio_20 - 1.0) * 0.5) if vol_ratio_20 else 0.5
        if pp is not None:
            strength *= (1.0 - pp * 2)
        result['vol_strength'] = max(0.1, min(1.0, strength))
    elif result['sky_vol_bearish']:
        result['vol_direction'] = 'down'
        result['vol_strength'] = 0.7
    elif result['high_pos_down_vol_up']:
        result['vol_direction'] = 'down'
        result['vol_strength'] = 0.6
    elif result['price_up_vol_down']:
        result['vol_direction'] = 'down'
        result['vol_strength'] = 0.4
    elif result['rush_up_shrink']:
        result['vol_direction'] = 'down'
        result['vol_strength'] = 0.5

    return result


def _score_volume_patterns(klines: list[dict]) -> tuple[float, dict]:
    """基于成交量形态信号计算评分。

    评分逻辑：
    - 基础分50（中性）
    - 恐慌底（看涨）: +30~40
    - 量价确认（放量上涨）: +15~25
    - 缩量整理（蓄势）: +10
    - 天量阴线（看跌）: -25
    - 高位价跌量增（看跌）: -20
    - 价升量缩（看跌）: -15
    - 急涨后缩量（看跌）: -18
    """
    if len(klines) < 25:
        return 0.0, {}

    vp = _detect_volume_patterns(klines)
    score = 50.0
    signals = []

    # 看涨信号
    if vp['panic_bottom']:
        bonus = 30 + vp['vol_strength'] * 10
        score += bonus
        signals.append(f"恐慌底✅(+{bonus:.0f})")

    # 看跌信号
    if vp['sky_vol_bearish']:
        score -= 25
        signals.append("天量阴线⚠️(-25)")
    if vp['high_pos_down_vol_up']:
        score -= 20
        signals.append("高位放量下跌⚠️(-20)")
    if vp['price_up_vol_down']:
        score -= 15
        signals.append("价升量缩⚠️(-15)")
    if vp['rush_up_shrink']:
        score -= 18
        signals.append("急涨缩量⚠️(-18)")

    # 量价确认（无特殊信号时，看量价是否一致）
    if not any([vp['panic_bottom'], vp['sky_vol_bearish'], vp['high_pos_down_vol_up'],
                vp['price_up_vol_down'], vp['rush_up_shrink']]):
        # 近5日涨 + 放量 = 量价确认看涨
        week_chg = _vol_compound_return([_f(k['change_percent']) for k in klines[-5:]])
        vr = vp.get('vol_ratio_20')
        if week_chg > 1.0 and vr and vr > 1.2:
            bonus = min(25, 15 + (vr - 1.0) * 10)
            score += bonus
            signals.append(f"放量上涨确认✅(+{bonus:.0f})")
        elif week_chg > 0 and vr and 0.7 <= vr <= 1.0:
            score += 10
            signals.append("缩量整理蓄势(+10)")
        elif week_chg < -1.0 and vr and vr > 1.5:
            score -= 10
            signals.append("放量下跌(-10)")

    score = max(0, min(100, score))

    detail = {
        'vol_direction': vp['vol_direction'],
        'vol_strength': round(vp['vol_strength'], 2),
        'vol_ratio_20': vp.get('vol_ratio_20'),
        'price_position': vp.get('price_position'),
        'signals': signals,
        'signal_summary': ' | '.join(signals) if signals else '无特殊信号',
    }
    return round(score, 1), detail


# ═══════════════════════════════════════════════════════════
# Step 4: 概念板块 + 周预测 + 财务 + 龙虎榜 (直接SQL)
# ═══════════════════════════════════════════════════════════

def _strip_suffix(code: str) -> str:
    """去掉 .SH/.SZ/.BJ 后缀"""
    return code.split('.')[0] if '.' in code else code


def _load_concept_scores(cur, codes: list[str]) -> dict[str, dict]:
    """概念板块强弱势评分 (concept_strength表中stock_code为纯6位数字)"""
    logger.info("🏷️  加载概念板块强弱势...")
    # 转换为纯数字代码
    stripped = [_strip_suffix(c) for c in codes]
    # 建立反向映射: 纯数字 -> 带后缀
    code_map = {_strip_suffix(c): c for c in codes}

    placeholders = ','.join(['%s'] * len(stripped))
    cur.execute(f"""
        SELECT stock_code, stock_name,
               MAX(strength_score) AS max_score,
               AVG(strength_score) AS avg_score,
               MAX(excess_5d) AS best_excess_5d,
               MAX(excess_20d) AS best_excess_20d,
               MAX(win_rate) AS best_win_rate,
               GROUP_CONCAT(
                   CONCAT(board_name, ':', ROUND(strength_score,1))
                   ORDER BY strength_score DESC SEPARATOR ' | '
               ) AS board_scores,
               COUNT(*) AS board_count
        FROM stock_concept_strength
        WHERE score_date = (SELECT MAX(score_date) FROM stock_concept_strength)
          AND stock_code IN ({placeholders})
        GROUP BY stock_code, stock_name
    """, stripped)
    rows = cur.fetchall()

    result = {}
    for r in rows:
        raw_code = r['stock_code']
        full_code = code_map.get(raw_code, raw_code)
        max_s = _f(r['max_score'])
        avg_s = _f(r['avg_score'])
        win_rate = _f(r['best_win_rate'])
        score = max_s * 0.6 + avg_s * 0.2 + win_rate * 20 * 0.2
        score = max(0, min(100, score))
        result[full_code] = {
            'concept_score': round(score, 1),
            'concept_detail': {
                'stock_name': r['stock_name'],
                'max_strength': round(max_s, 1),
                'avg_strength': round(avg_s, 1),
                'excess_5d': round(_f(r['best_excess_5d']), 2),
                'excess_20d': round(_f(r['best_excess_20d']), 2),
                'win_rate': round(win_rate, 3),
                'board_count': r['board_count'],
                'top_boards': (r['board_scores'] or '')[:200],
            }
        }
    logger.info("  概念板块评分: %d 只", len(result))
    return result


def _load_prediction_scores(cur, codes: list[str]) -> dict[str, dict]:
    """周预测信号评分"""
    logger.info("🔮 加载周预测信号...")
    placeholders = ','.join(['%s'] * len(codes))

    cur.execute(f"""
        SELECT stock_code, stock_name, pred_direction, confidence,
               strategy, d3_chg, d4_chg, board_momentum,
               concept_consensus, fund_flow_signal,
               backtest_accuracy, pred_weekly_chg,
               pred_chg_low, pred_chg_high, pred_chg_hit_rate
        FROM stock_weekly_prediction
        WHERE stock_code IN ({placeholders})
    """, codes)
    pred_rows = cur.fetchall()

    # 历史准确率
    cur.execute(f"""
        SELECT stock_code,
               COUNT(*) AS total,
               SUM(CASE WHEN is_correct = 1 THEN 1 ELSE 0 END) AS correct
        FROM stock_weekly_prediction_history
        WHERE is_correct IS NOT NULL AND stock_code IN ({placeholders})
        GROUP BY stock_code
    """, codes)
    hist_acc = {}
    for h in cur.fetchall():
        total = h['total'] or 0
        correct = h['correct'] or 0
        if total >= 3:
            hist_acc[h['stock_code']] = correct / total

    result = {}
    for r in pred_rows:
        code = r['stock_code']
        score = 0.0
        detail = {}

        direction = r['pred_direction']
        confidence = r['confidence']
        pred_chg = _f(r.get('pred_weekly_chg'))

        if direction == 'UP':
            score += 20
            if confidence == 'high':
                score += 15
            elif confidence == 'medium':
                score += 8
        else:
            score -= 10

        if pred_chg > 3:
            score += 15
        elif pred_chg > 1:
            score += 8
        elif pred_chg > 0:
            score += 3

        bt_acc = _f(r.get('backtest_accuracy'))
        if bt_acc > 80:
            score += 15
        elif bt_acc > 70:
            score += 8

        if code in hist_acc:
            ha = hist_acc[code]
            if ha > 0.7:
                score += 10
            elif ha > 0.6:
                score += 5
            detail['hist_accuracy'] = round(ha, 3)

        bm = _f(r.get('board_momentum'))
        cc = _f(r.get('concept_consensus'))
        ffs = _f(r.get('fund_flow_signal'))
        if bm > 0: score += 5
        if cc > 0.5: score += 5
        if ffs > 0: score += 5

        score = max(0, min(100, score))
        detail.update({
            'direction': direction, 'confidence': confidence,
            'pred_weekly_chg': pred_chg, 'backtest_acc': bt_acc,
            'board_momentum': round(bm, 3), 'strategy': r['strategy'],
        })
        result[code] = {
            'pred_score': round(score, 1), 'pred_detail': detail,
            'stock_name': r.get('stock_name', ''),
        }
    logger.info("  周预测评分: %d 只", len(result))
    return result


def _load_financial_scores(cur, codes: list[str]) -> dict[str, dict]:
    """财务基本面评分"""
    logger.info("📈 加载财务数据...")
    placeholders = ','.join(['%s'] * len(codes))
    cur.execute(f"""
        SELECT f.stock_code, f.data_json, f.report_date
        FROM stock_finance f
        INNER JOIN (
            SELECT stock_code, MAX(report_date) AS max_rd
            FROM stock_finance
            WHERE stock_code IN ({placeholders})
              AND report_date <= DATE_FORMAT(CURDATE(), '%%Y-%%m-%%d')
            GROUP BY stock_code
        ) latest ON f.stock_code = latest.stock_code AND f.report_date = latest.max_rd
    """, codes)
    rows = cur.fetchall()

    result = {}
    for r in rows:
        code = r['stock_code']
        try:
            data = json.loads(r['data_json']) if isinstance(r['data_json'], str) else r['data_json']
        except (json.JSONDecodeError, TypeError):
            continue

        score = 0.0
        detail = {}

        rev_growth = _parse_pct(data.get('营业总收入同比增长(%)') or data.get('营业总收入同比增长率') or data.get('营收同比'))
        if rev_growth is not None:
            if rev_growth > 20: score += 20
            elif rev_growth > 10: score += 12
            elif rev_growth > 0: score += 5
            detail['rev_growth'] = rev_growth

        profit_growth = _parse_pct(data.get('归属净利润同比增长(%)') or data.get('归母净利润同比增长率') or data.get('净利润同比'))
        if profit_growth is not None:
            if profit_growth > 30: score += 25
            elif profit_growth > 15: score += 15
            elif profit_growth > 0: score += 5
            elif profit_growth < -20: score -= 10
            detail['profit_growth'] = profit_growth

        roe = _parse_pct(data.get('净资产收益率(%)') or data.get('净资产收益率') or data.get('ROE'))
        if roe is not None:
            if roe > 15: score += 15
            elif roe > 10: score += 10
            elif roe > 5: score += 5
            detail['roe'] = roe

        gross_margin = _parse_pct(data.get('销售毛利率(%)') or data.get('销售毛利率') or data.get('毛利率'))
        if gross_margin is not None:
            if gross_margin > 40: score += 10
            elif gross_margin > 25: score += 5
            detail['gross_margin'] = gross_margin

        debt_ratio = _parse_pct(data.get('资产负债率(%)') or data.get('资产负债率'))
        if debt_ratio is not None:
            if debt_ratio < 40: score += 10
            elif debt_ratio < 60: score += 5
            elif debt_ratio > 80: score -= 5
            detail['debt_ratio'] = debt_ratio

        score = max(0, min(100, score))
        result[code] = {'fin_score': round(score, 1), 'fin_detail': detail}
    logger.info("  财务评分: %d 只", len(result))
    return result


def _load_dragon_tiger_scores(cur, codes: list[str]) -> dict[str, dict]:
    """龙虎榜评分"""
    logger.info("🐉 加载龙虎榜数据...")
    placeholders = ','.join(['%s'] * len(codes))
    cur.execute(f"""
        SELECT stock_code, stock_name,
               COUNT(*) AS appear_count,
               MAX(trade_date) AS latest_date,
               GROUP_CONCAT(reason ORDER BY trade_date DESC SEPARATOR '; ') AS reasons
        FROM stock_dragon_tiger
        WHERE trade_date >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 30 DAY), '%%Y-%%m-%%d')
          AND stock_code IN ({placeholders})
        GROUP BY stock_code, stock_name
    """, codes)
    rows = cur.fetchall()

    result = {}
    for r in rows:
        code = r['stock_code']
        count = r['appear_count'] or 0
        score = 0.0

        if count >= 3: score += 40
        elif count >= 2: score += 25
        elif count >= 1: score += 15

        latest = r['latest_date'] or ''
        if latest:
            try:
                days_ago = (datetime.now() - datetime.strptime(str(latest)[:10], '%Y-%m-%d')).days
                if days_ago <= 3: score += 30
                elif days_ago <= 7: score += 20
                elif days_ago <= 14: score += 10
            except ValueError:
                pass

        score = max(0, min(100, score))
        result[code] = {
            'dragon_score': round(score, 1),
            'dragon_detail': {
                'appear_count': count, 'latest_date': str(latest)[:10],
                'reasons': (r['reasons'] or '')[:200],
            }
        }
    logger.info("  龙虎榜评分: %d 只", len(result))
    return result


# ═══════════════════════════════════════════════════════════
# Step 5: 综合评分 & 排名
# ═══════════════════════════════════════════════════════════

WEIGHTS = {
    'tech': 0.22, 'fund': 0.18, 'concept': 0.13,
    'pred': 0.13, 'fin': 0.13, 'dragon': 0.08, 'volume': 0.13,
}


def _estimate_monthly_change(stock: dict) -> tuple[float, float, float]:
    """基于多维度信号估算未来一个月预期涨跌幅区间"""
    total = stock['total_score']
    if total >= 80: base = 12.0
    elif total >= 70: base = 8.0
    elif total >= 60: base = 5.0
    elif total >= 50: base = 2.0
    else: base = 0.0

    if stock.get('tech_score', 0) >= 70: base += 3.0
    elif stock.get('tech_score', 0) >= 50: base += 1.0
    if stock.get('fund_score', 0) >= 60: base += 2.0

    pd_ = stock.get('pred_detail', {})
    if pd_.get('direction') == 'UP' and pd_.get('confidence') == 'high':
        base += 2.0
    if stock.get('concept_score', 0) >= 70:
        base += 1.5

    # 成交量修正
    vd = stock.get('vol_detail', {})
    vol_dir = vd.get('vol_direction')
    vol_str = vd.get('vol_strength', 0)
    if vol_dir == 'up':
        base += 2.0 * vol_str  # 恐慌底看涨加分
    elif vol_dir == 'down':
        base -= 2.0 * vol_str  # 看跌信号减分

    low = round(base * 0.5, 1)
    high = round(base * 1.6, 1)
    return round(base, 1), low, high


def run_deep_analysis():
    """执行多维度深度分析"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    try:
        # Step 1: 快速筛选候选池
        candidates = _get_candidate_pool(cur)
        if not candidates:
            logger.error("候选池为空，退出")
            return []

        # Step 2: 加载各维度数据
        kline_data = _load_kline_data(cur, candidates)
        fund_data = _load_fund_flow_data(cur, candidates)
        concept_data = _load_concept_scores(cur, candidates)
        pred_data = _load_prediction_scores(cur, candidates)
        fin_data = _load_financial_scores(cur, candidates)
        dragon_data = _load_dragon_tiger_scores(cur, candidates)

        # 获取股票名称
        names = {}
        for code, v in pred_data.items():
            if v.get('stock_name'):
                names[code] = v['stock_name']
        for code, v in concept_data.items():
            if code not in names:
                cd = v.get('concept_detail', {})
                if cd.get('stock_name'):
                    names[code] = cd['stock_name']

        # Step 3: 计算技术面和资金面评分
        logger.info("📊 计算技术面+资金面+成交量评分...")
        tech_scores = {}
        vol_scores = {}
        for code, klines in kline_data.items():
            s, d = _score_technical(klines)
            if s > 0:
                tech_scores[code] = {'tech_score': s, 'tech_detail': d}
            # 成交量形态评分（复用同一份K线数据）
            vs, vd = _score_volume_patterns(klines)
            if vs > 0:
                vol_scores[code] = {'vol_score': vs, 'vol_detail': vd}

        fund_scores = {}
        for code, flows in fund_data.items():
            s, d = _score_fund_flow(flows)
            if s > 0:
                fund_scores[code] = {'fund_score': s, 'fund_detail': d}

        logger.info("  技术面: %d 只, 资金面: %d 只, 成交量: %d 只",
                     len(tech_scores), len(fund_scores), len(vol_scores))

        # Step 4: 综合评分
        logger.info("\n🔄 综合评分...")
        results = []
        for code in candidates:
            tech = tech_scores.get(code, {}).get('tech_score', 0)
            fund = fund_scores.get(code, {}).get('fund_score', 0)
            concept = concept_data.get(code, {}).get('concept_score', 0)
            pred = pred_data.get(code, {}).get('pred_score', 0)
            fin = fin_data.get(code, {}).get('fin_score', 0)
            dragon = dragon_data.get(code, {}).get('dragon_score', 0)
            volume = vol_scores.get(code, {}).get('vol_score', 0)

            dims = sum(1 for s in [tech, fund, concept, pred, fin, dragon, volume] if s > 0)
            if dims < 3:
                continue

            total = (
                tech * WEIGHTS['tech'] + fund * WEIGHTS['fund']
                + concept * WEIGHTS['concept'] + pred * WEIGHTS['pred']
                + fin * WEIGHTS['fin'] + dragon * WEIGHTS['dragon']
                + volume * WEIGHTS['volume']
            )

            name = names.get(code, code)
            stock = {
                'stock_code': code, 'stock_name': name,
                'total_score': round(total, 2),
                'tech_score': tech, 'fund_score': fund,
                'concept_score': concept, 'pred_score': pred,
                'fin_score': fin, 'dragon_score': dragon,
                'vol_score': volume,
                'dim_count': dims,
                'tech_detail': tech_scores.get(code, {}).get('tech_detail', {}),
                'fund_detail': fund_scores.get(code, {}).get('fund_detail', {}),
                'concept_detail': concept_data.get(code, {}).get('concept_detail', {}),
                'pred_detail': pred_data.get(code, {}).get('pred_detail', {}),
                'fin_detail': fin_data.get(code, {}).get('fin_detail', {}),
                'dragon_detail': dragon_data.get(code, {}).get('dragon_detail', {}),
                'vol_detail': vol_scores.get(code, {}).get('vol_detail', {}),
            }

            est_chg, est_low, est_high = _estimate_monthly_change(stock)
            stock['est_monthly_chg'] = est_chg
            stock['est_chg_range'] = f"{est_low}% ~ {est_high}%"
            results.append(stock)

        results.sort(key=lambda x: x['total_score'], reverse=True)
        top10 = results[:10]
        _print_results(top10, len(results))
        return top10

    finally:
        cur.close()
        conn.close()


def _print_results(top10: list[dict], total_count: int):
    """格式化输出"""
    print("\n" + "=" * 100)
    print(f"  📊 多维度深度选股分析结果 — 未来一个月最有可能上涨的TOP10")
    print(f"  分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  候选股票池: {total_count} 只 (至少3个维度有数据)")
    print(f"  评分维度: 技术面(22%) + 资金面(18%) + 概念板块(13%) + 周预测(13%) + 财务(13%) + 龙虎榜(8%) + 成交量(13%)")
    print("=" * 100)

    for i, s in enumerate(top10, 1):
        print(f"\n{'─' * 95}")
        print(f"  🏆 第{i}名: {s['stock_name']} ({s['stock_code']})")
        print(f"  综合评分: {s['total_score']:.1f}/100  |  "
              f"预期月涨幅: {s['est_monthly_chg']}%  |  "
              f"预期区间: {s['est_chg_range']}")
        print(f"  ┌─ 技术面: {s['tech_score']:.0f}  "
              f"│ 资金面: {s['fund_score']:.0f}  "
              f"│ 概念板块: {s['concept_score']:.0f}  "
              f"│ 周预测: {s['pred_score']:.0f}  "
              f"│ 财务: {s['fin_score']:.0f}  "
              f"│ 龙虎榜: {s['dragon_score']:.0f}  "
              f"│ 成交量: {s.get('vol_score', 0):.0f}")

        td = s.get('tech_detail', {})
        if td:
            print(f"  ├─ 技术: 均线={td.get('ma_alignment','?')}  "
                  f"5日涨幅={td.get('chg_5d',0):.1f}%  "
                  f"20日涨幅={td.get('chg_20d',0):.1f}%  "
                  f"60日位置={td.get('pos60',0):.2f}  "
                  f"换手率={td.get('avg_turnover_5d',0):.1f}%")

        fd = s.get('fund_detail', {})
        if fd:
            smart = " 🧠主力吸筹" if fd.get('smart_money') else ""
            print(f"  ├─ 资金: 5日主力净流入={fd.get('net_5d',0):.0f}万  "
                  f"20日累计={fd.get('net_20d',0):.0f}万  "
                  f"5日主力净额={fd.get('main_net_5day',0):.0f}万{smart}")

        cd = s.get('concept_detail', {})
        if cd:
            print(f"  ├─ 板块: 最高强弱分={cd.get('max_strength',0):.0f}  "
                  f"5日超额={cd.get('excess_5d',0):.1f}%  "
                  f"跑赢率={cd.get('win_rate',0):.1%}  "
                  f"所属{cd.get('board_count',0)}个板块")
            if cd.get('top_boards'):
                print(f"  │       热门板块: {cd['top_boards'][:100]}")

        pd_ = s.get('pred_detail', {})
        if pd_:
            print(f"  ├─ 预测: 方向={pd_.get('direction','?')}  "
                  f"置信度={pd_.get('confidence','?')}  "
                  f"预测周涨幅={pd_.get('pred_weekly_chg',0):.1f}%  "
                  f"回测准确率={pd_.get('backtest_acc',0):.1f}%")

        fnd = s.get('fin_detail', {})
        if fnd:
            parts = []
            if 'rev_growth' in fnd: parts.append(f"营收增长={fnd['rev_growth']:.1f}%")
            if 'profit_growth' in fnd: parts.append(f"利润增长={fnd['profit_growth']:.1f}%")
            if 'roe' in fnd: parts.append(f"ROE={fnd['roe']:.1f}%")
            if parts:
                print(f"  ├─ 财务: {' | '.join(parts)}")

        vld = s.get('vol_detail', {})
        if vld:
            vol_dir_str = {'up': '看涨↑', 'down': '看跌↓'}.get(vld.get('vol_direction'), '中性')
            vol_str = vld.get('vol_strength', 0)
            vr20 = vld.get('vol_ratio_20')
            vr_str = f"量比={vr20:.2f}" if vr20 else "量比=N/A"
            sig_summary = vld.get('signal_summary', '无特殊信号')
            print(f"  └─ 成交量: {vol_dir_str}(强度{vol_str:.1f})  {vr_str}  信号: {sig_summary}")

    print(f"\n{'=' * 100}")
    print(f"  ⚠️  免责声明: 以上分析仅基于历史数据的量化模型，不构成投资建议。")
    print(f"  股市有风险，投资需谨慎。预期涨跌幅为模型估算，实际结果可能有较大偏差。")
    print(f"{'=' * 100}\n")


if __name__ == '__main__':
    run_deep_analysis()
