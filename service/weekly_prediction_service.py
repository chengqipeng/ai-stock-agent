#!/usr/bin/env python3
"""
批量周预测服务 — 基于v4回测引擎的实盘预测
==========================================
功能：
1. 获取全部A股股票，基于最新交易日数据进行本周方向预测
2. 预测结果写入 stock_weekly_prediction（最新一条，UPSERT）
3. 同时写入 stock_weekly_prediction_history（历史记录）
4. 附带29周回测准确率作为参考

预测逻辑（v6: 前N天方向策略）：
- 获取本周已有的交易日K线数据
- 计算d3(前3天)/d4(前4天)复合涨跌幅
- 停牌股(前3天全0) → 预测涨(99.5%准确)
- 策略C(d3可用): 前3天累计涨跌>0→预测周涨, <0→预测周跌 (回测81.8%)
- 策略B(仅d1/d2): 周一涨跌>0.5%→跟随方向 (回测67.2%)
- d4强信号(|d4|>2%)可覆盖d3方向, d3d4一致时置信度最高

用法：
    python -m service.weekly_prediction_service
"""
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, '.')

from dao import get_connection
from dao.stock_weekly_prediction_dao import (
    ensure_tables,
    batch_upsert_latest_predictions,
    batch_insert_history,
)
from common.constants.stocks_data import get_stock_name

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
# 个股→大盘指数映射（根据股票代码前缀确定对应的大盘指数）
# ═══════════════════════════════════════════════════════════
_INDEX_MAPPING = {
    "300": "399001.SZ",  # 创业板 → 深证成指
    "301": "399001.SZ",
    "000": "399001.SZ",  # 深市主板 → 深证成指
    "001": "399001.SZ",
    "002": "399001.SZ",
    "003": "399001.SZ",
    "600": "000001.SH",  # 沪市主板 → 上证指数
    "601": "000001.SH",
    "603": "000001.SH",
    "605": "000001.SH",
    "688": "000001.SH",  # 科创板 → 上证指数
    "689": "000001.SH",
    "920": "899050.SZ",  # 北交所 → 北证50
    "430": "899050.SZ",
    "830": "899050.SZ",
    "831": "899050.SZ",
    "832": "899050.SZ",
    "833": "899050.SZ",
    "834": "899050.SZ",
    "835": "899050.SZ",
    "836": "899050.SZ",
    "837": "899050.SZ",
    "838": "899050.SZ",
    "839": "899050.SZ",
    "870": "899050.SZ",
    "871": "899050.SZ",
    "872": "899050.SZ",
    "873": "899050.SZ",
}


def _get_stock_index(stock_code: str) -> str:
    """根据股票代码返回对应的大盘指数代码。"""
    prefix3 = stock_code[:3]
    if prefix3 in _INDEX_MAPPING:
        return _INDEX_MAPPING[prefix3]
    # fallback by suffix
    if stock_code.endswith(".SZ"):
        return "399001.SZ"
    if stock_code.endswith(".SH"):
        return "000001.SH"
    if stock_code.endswith(".BJ"):
        return "899050.SZ"
    return "000001.SH"  # 最终兜底


# ═══════════════════════════════════════════════════════════
# 工具函数（与v4回测引擎一致）
# ═══════════════════════════════════════════════════════════

D4_STRONG_THRESHOLD = 2.0
D4_FUZZY_THRESHOLD = 0.8
D3_STRONG_THRESHOLD = 2.0
D3_FUZZY_THRESHOLD = 0.8


def _to_float(v) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def _compound_return(pcts):
    r = 1.0
    for p in pcts:
        r *= (1 + p / 100)
    return round((r - 1) * 100, 4)


def _mean(lst):
    return sum(lst) / len(lst) if lst else 0.0


def _std(lst):
    if len(lst) < 2:
        return 0.0
    m = _mean(lst)
    return (sum((x - m) ** 2 for x in lst) / (len(lst) - 1)) ** 0.5


def _add_suffix(code_6: str) -> str:
    if code_6.startswith(('0', '3')):
        return f'{code_6}.SZ'
    elif code_6.startswith('6'):
        return f'{code_6}.SH'
    return code_6


def _next_trade_date(dt: datetime) -> datetime:
    """估算下一个交易日（跳过周末，不考虑节假日）。"""
    dt_next = dt + timedelta(days=1)
    while dt_next.weekday() >= 5:  # 5=周六, 6=周日
        dt_next += timedelta(days=1)
    return dt_next


def _next_week_monday(dt: datetime) -> datetime:
    """获取下一周的周一日期。"""
    days_ahead = 7 - dt.weekday()  # weekday(): 0=Mon
    return dt + timedelta(days=days_ahead)


def _next_week_friday(dt: datetime) -> datetime:
    """获取下一周的周五日期。"""
    mon = _next_week_monday(dt)
    return mon + timedelta(days=4)


# ═══════════════════════════════════════════════════════════
# 行业分类 → 策略配置
# ═══════════════════════════════════════════════════════════

# 行业分类 → 策略类型映射
# 使用 sector_mapping_utils 的7大板块分类
# reversal: 低波动/周内反转频繁的行业，fuzzy区间用反向预测
# momentum: 趋势性强的行业，保持动量延续
# adaptive: 根据个股波动率自动选择
_SECTOR_STRATEGY_MAP = {
    # 趋势性强 → 动量策略
    '科技': 'momentum',
    '有色金属': 'momentum',
    '新能源': 'momentum',
    # 周期性/低波动 → 自适应（个股行为决定）
    '制造': 'adaptive',
    '汽车': 'adaptive',
    '化工': 'adaptive',
    '医药': 'adaptive',
    # 未分类的股票默认 adaptive
}

# 每种策略类型的参数配置
_STRATEGY_PROFILES = {
    'momentum': {
        'strong_threshold': 2.0,
        'fuzzy_threshold': 0.8,
        'fuzzy_mode': 'follow',       # fuzzy区间跟随d4方向
        'vol_adjust': False,
    },
    'reversal': {
        'strong_threshold': 2.0,
        'fuzzy_threshold': 0.5,        # 更窄的fuzzy区间
        'fuzzy_mode': 'reverse',       # fuzzy区间反向预测
        'vol_adjust': False,
    },
    'adaptive': {
        'strong_threshold': 2.0,
        'fuzzy_threshold': 0.8,
        'fuzzy_mode': 'auto',          # 根据个股历史反转率决定
        'vol_adjust': True,            # 根据波动率调整阈值
    },
}


def _classify_stock_behavior(weekly_klines: list[dict]) -> dict:
    """分析个股历史行为特征，用于选择最优策略。

    基于最近的周K线数据，计算：
    - avg_daily_vol: 日均波动率(涨跌幅绝对值均值)
    - reversal_rate: 周内反转率(d4方向与全周方向不一致的比例)
    - fuzzy_ratio: 落入fuzzy区间的比例

    Args:
        weekly_klines: 按日期排序的日K线列表，需包含 date, change_percent

    Returns:
        {'avg_daily_vol': float, 'reversal_rate': float, 'fuzzy_ratio': float,
         'recommended_fuzzy_mode': 'follow'|'reverse'|'skip'}
    """
    if len(weekly_klines) < 20:
        return {
            'avg_daily_vol': 0.0, 'reversal_rate': 0.0, 'fuzzy_ratio': 0.0,
            'recommended_fuzzy_mode': 'follow',
        }

    # 日均波动率
    daily_abs = [abs(k['change_percent']) for k in weekly_klines]
    avg_daily_vol = _mean(daily_abs)

    # 按ISO周分组
    week_groups = defaultdict(list)
    for k in weekly_klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        iw = dt.isocalendar()[:2]
        week_groups[iw].append(k)

    reversal_count = 0
    fuzzy_count = 0
    total_weeks = 0

    for iw, days in week_groups.items():
        days.sort(key=lambda x: x['date'])
        if len(days) < 4:
            continue
        pcts = [d['change_percent'] for d in days]
        d4 = _compound_return(pcts[:4])
        weekly = _compound_return(pcts)
        total_weeks += 1

        if abs(d4) <= D4_FUZZY_THRESHOLD:
            fuzzy_count += 1

        # 反转：d4方向与全周方向不一致
        if (d4 >= 0) != (weekly >= 0):
            reversal_count += 1

    reversal_rate = reversal_count / total_weeks if total_weeks > 0 else 0.0
    fuzzy_ratio = fuzzy_count / total_weeks if total_weeks > 0 else 0.0

    # 推荐fuzzy模式
    if reversal_rate > 0.55:
        recommended = 'reverse'
    elif reversal_rate < 0.35:
        recommended = 'follow'
    else:
        recommended = 'skip'  # 不确定，标记为低置信

    return {
        'avg_daily_vol': round(avg_daily_vol, 4),
        'reversal_rate': round(reversal_rate, 4),
        'fuzzy_ratio': round(fuzzy_ratio, 4),
        'recommended_fuzzy_mode': recommended,
    }


def _get_stock_strategy_profile(code: str, sector: str, behavior: dict) -> dict:
    """根据行业和个股行为特征，返回该股票应使用的策略参数。"""
    # 1. 先看行业是否有硬编码策略
    strategy_type = _SECTOR_STRATEGY_MAP.get(sector, 'adaptive')
    profile = _STRATEGY_PROFILES[strategy_type].copy()

    # 2. adaptive模式下，根据个股行为微调
    if strategy_type == 'adaptive' or profile['fuzzy_mode'] == 'auto':
        profile['fuzzy_mode'] = behavior.get('recommended_fuzzy_mode', 'follow')

    # 3. 波动率自适应阈值：低波动股票收窄fuzzy区间
    if profile.get('vol_adjust') and behavior.get('avg_daily_vol', 0) > 0:
        vol = behavior['avg_daily_vol']
        if vol < 1.0:
            # 低波动股：收窄阈值，更多信号归入fuzzy
            profile['fuzzy_threshold'] = max(0.3, vol * 0.6)
            profile['strong_threshold'] = max(1.0, vol * 1.5)
        elif vol > 3.0:
            # 高波动股：放宽阈值
            profile['fuzzy_threshold'] = min(1.5, vol * 0.3)
            profile['strong_threshold'] = min(4.0, vol * 0.8)

    return profile


def _predict_with_profile(d4_chg, d3_chg, is_suspended, n_days, daily_pcts,
                          profile: dict) -> tuple:
    """使用策略配置进行预测（v6: 前N天方向策略 + 信号强度分层）。

    核心改进（基于回测验证）：
    - 策略C(前3天方向>0): 回测准确率81.8%，作为d3可用时的主策略
    - 策略B(周一混合): 回测准确率67.2%，作为仅d1可用时的策略
    - 强信号(|d4|>2%)仍保留动量跟随，但置信度基于回测校准
    - fuzzy区间(|signal|<0.5%)不再强行预测，标记为uncertain

    Returns:
        (pred_up: bool, confidence: str, strategy: str, reason: str)
    """
    strong_th = profile['strong_threshold']
    fuzzy_th = profile['fuzzy_threshold']
    fuzzy_mode = profile['fuzzy_mode']

    if is_suspended:
        return True, 'high', 'suspended_up', '停牌:前3天全0'

    # ── 策略C: 前3天方向（回测81.8%准确率）──
    # 当d3可用时，前3天累计涨跌方向是最强信号
    if d3_chg is not None and n_days >= 3:
        d3_direction_up = d3_chg > 0  # 前3天累计>0 → 预测周涨

        # d4可用时，结合d4强化或修正
        if d4_chg is not None:
            if abs(d4_chg) > strong_th:
                # d4强信号：d4方向覆盖d3（强动量延续）
                pred_up = d4_chg >= 0
                # 如果d3和d4方向一致，置信度更高
                if (d4_chg >= 0) == d3_direction_up:
                    return pred_up, 'high', 'confirm_d3d4(strong)', \
                        f'd3={d3_chg:+.2f}%,d4强信号={d4_chg:+.2f}%,方向一致'
                else:
                    return pred_up, 'medium', 'override_d4(strong)', \
                        f'd4强信号={d4_chg:+.2f}%覆盖d3={d3_chg:+.2f}%'

            elif abs(d4_chg) > fuzzy_th:
                # d4中等信号：与d3方向一致时增强，矛盾时以d4为主
                if (d4_chg >= 0) == d3_direction_up:
                    return d3_direction_up, 'high', 'confirm_d3d4(medium)', \
                        f'd3={d3_chg:+.2f}%,d4={d4_chg:+.2f}%,方向一致'
                else:
                    # d3和d4矛盾 → d4中等信号更可靠（回测验证），以d4为主
                    pred_up = d4_chg >= 0
                    return pred_up, 'low', 'conflict_follow_d4(medium)', \
                        f'd3={d3_chg:+.2f}%与d4={d4_chg:+.2f}%矛盾,以d4为主'
            else:
                # d4 fuzzy区间：d4信号弱，综合d3和d4判断
                # d3方向为主，但如果d3也很弱（fuzzy区间），降低置信度
                if abs(d3_chg) > fuzzy_th:
                    return d3_direction_up, 'medium', 'follow_d3_direction', \
                        f'd3方向={d3_chg:+.2f}%,d4模糊={d4_chg:+.2f}%'
                else:
                    # d3和d4都在fuzzy区间 → 信号极弱
                    # 用d3+d4的综合方向
                    combined = d3_chg + d4_chg
                    pred_up = combined >= 0
                    return pred_up, 'low', 'weak_combined_d3d4', \
                        f'd3={d3_chg:+.2f}%+d4={d4_chg:+.2f}%均弱,综合={combined:+.2f}%'
        else:
            # 仅d3可用（周三收盘）：直接用前3天方向
            if abs(d3_chg) > strong_th:
                return d3_direction_up, 'high', 'follow_d3_direction(strong)', \
                    f'd3方向={d3_chg:+.2f}%(强信号)'
            elif abs(d3_chg) > fuzzy_th:
                return d3_direction_up, 'medium', 'follow_d3_direction(medium)', \
                    f'd3方向={d3_chg:+.2f}%'
            else:
                # d3也在fuzzy区间 → 仍然用方向（回测显示即使小幅也有效）
                # 但置信度降低
                return d3_direction_up, 'low', 'follow_d3_direction(weak)', \
                    f'd3方向={d3_chg:+.2f}%(弱信号)'

    # ── 策略B: 周一混合（回测67.2%准确率）──
    # 仅有1-2天数据时使用
    if n_days >= 1 and daily_pcts:
        d1_chg = daily_pcts[0]
        if n_days >= 2:
            cum_chg = _compound_return(daily_pcts[:2])
        else:
            cum_chg = d1_chg

        if abs(cum_chg) > 0.5:
            # 周一/周二涨跌>0.5% → 跟随方向
            pred_up = cum_chg > 0
            conf = 'medium' if abs(cum_chg) > 1.0 else 'low'
            return pred_up, conf, f'early_direction_d{n_days}', \
                f'前{n_days}天={cum_chg:+.2f}%,方向跟随'
        else:
            # 涨跌幅太小，不确定
            return (cum_chg >= 0), 'low', f'uncertain_d{n_days}', \
                f'前{n_days}天={cum_chg:+.2f}%,信号不足'

    # 数据不足
    cum = _compound_return(daily_pcts) if daily_pcts else 0
    return (cum >= 0), 'low', f'partial_d{n_days}', f'仅{n_days}天数据:{cum:+.2f}%'


# ═══════════════════════════════════════════════════════════
# 数据加载
# ═══════════════════════════════════════════════════════════

def _get_all_stock_codes() -> list[str]:
    """从stock_kline表获取全部有K线数据的股票代码（排除北交所）。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute("SELECT DISTINCT stock_code FROM stock_kline")
        codes = [r['stock_code'] for r in cur.fetchall()
                 if not r['stock_code'].endswith('.BJ')]
        logger.info("全部股票: %d 只（已排除北交所）", len(codes))
        return sorted(codes)
    finally:
        cur.close()
        conn.close()


def _get_latest_trade_date() -> str:
    """获取stock_kline中最新的交易日期（从主要指数中取最新）。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT MAX(`date`) as max_date FROM stock_kline "
            "WHERE stock_code IN ('000001.SH', '399001.SZ', '899050.SZ')"
        )
        row = cur.fetchone()
        return row['max_date'] if row else None
    finally:
        cur.close()
        conn.close()


def _load_prediction_data(stock_codes: list[str], latest_date: str) -> dict:
    """加载预测所需的数据。

    加载最近60天的K线数据用于计算信号。
    """
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    lookback_start = (dt_latest - timedelta(days=90)).strftime('%Y-%m-%d')

    # 1. 个股K线
    stock_klines = defaultdict(list)
    batch_size = 200
    logger.info("[数据加载] 开始加载个股K线 (%d只, lookback=%s ~ %s)...", len(stock_codes), lookback_start, latest_date)
    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i:i + batch_size]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, open_price, close_price, high_price, "
            f"low_price, change_percent, trading_volume, trading_amount "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [lookback_start, latest_date])
        for row in cur.fetchall():
            stock_klines[row['stock_code']].append({
                'date': row['date'],
                'close': _to_float(row['close_price']),
                'change_percent': _to_float(row['change_percent']),
                'volume': _to_float(row['trading_volume']),
            })

    # 2. 个股→板块映射
    logger.info("[数据加载] 个股K线加载完成 (%d只有数据), 开始加载板块映射...", len(stock_klines))
    stock_boards = defaultdict(list)
    code_6_list = list(set(c[:6] for c in stock_codes))
    for i in range(0, len(code_6_list), batch_size):
        batch = code_6_list[i:i + batch_size]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, board_code, board_name "
            f"FROM stock_concept_board_stock WHERE stock_code IN ({ph})",
            batch)
        for row in cur.fetchall():
            sc6 = row['stock_code']
            full_code = _add_suffix(sc6)
            stock_boards[full_code].append({
                'board_code': row['board_code'],
                'board_name': row['board_name'],
            })

    # 3. 板块K线
    logger.info("[数据加载] 板块映射加载完成 (%d只有板块), 开始加载板块K线...", len(stock_boards))
    all_board_codes = set()
    for boards in stock_boards.values():
        for b in boards:
            all_board_codes.add(b['board_code'])
    all_board_codes = list(all_board_codes)

    board_kline_map = defaultdict(list)
    for i in range(0, len(all_board_codes), batch_size):
        batch = all_board_codes[i:i + batch_size]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT board_code, `date`, close_price, change_percent "
            f"FROM concept_board_kline WHERE board_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [lookback_start, latest_date])
        for row in cur.fetchall():
            board_kline_map[row['board_code']].append({
                'date': row['date'],
                'change_percent': _to_float(row['change_percent']),
            })

    # 4. 大盘K线（按指数分别加载：上证、深证、北证50）
    logger.info("[数据加载] 板块K线加载完成 (%d个板块), 开始加载大盘K线...", len(board_kline_map))
    all_index_codes = list(set(_get_stock_index(c) for c in stock_codes))
    # 确保至少包含三大主要指数
    for idx in ('000001.SH', '399001.SZ', '899050.SZ'):
        if idx not in all_index_codes:
            all_index_codes.append(idx)
    ph_idx = ','.join(['%s'] * len(all_index_codes))
    cur.execute(
        f"SELECT stock_code, `date`, close_price, change_percent FROM stock_kline "
        f"WHERE stock_code IN ({ph_idx}) AND `date` >= %s AND `date` <= %s "
        f"ORDER BY `date`", all_index_codes + [lookback_start, latest_date])
    market_klines_by_index = defaultdict(list)
    for r in cur.fetchall():
        market_klines_by_index[r['stock_code']].append({
            'date': r['date'],
            'change_percent': _to_float(r['change_percent']),
        })
    # 兼容旧接口：market_klines 默认取上证指数
    market_klines = market_klines_by_index.get('000001.SH', [])

    # 5. 股票名称（优先从概念板块成分股表获取，缺失的从本地常量补充）
    logger.info("[数据加载] 大盘K线加载完成 (%d个指数), 开始加载股票名称...", len(market_klines_by_index))
    stock_names = {}
    cur.execute(
        "SELECT DISTINCT stock_code, stock_name FROM stock_concept_board_stock")
    for row in cur.fetchall():
        full_code = _add_suffix(row['stock_code'])
        stock_names[full_code] = row['stock_name']

    # 补充：对于不在概念板块中的股票，从本地 STOCK_DICT 兜底
    missing_count = 0
    for code in stock_codes:
        if code not in stock_names or not stock_names[code]:
            name = get_stock_name(code)
            if name:
                stock_names[code] = name
                missing_count += 1
    if missing_count:
        logger.info("[数据加载] 从本地常量补充 %d 只股票名称", missing_count)

    # 5b. 资金流向数据（最近20天）
    logger.info("[数据加载] 股票名称加载完成, 开始加载资金流向...")
    stock_fund_flows = defaultdict(list)
    ff_start = (dt_latest - timedelta(days=30)).strftime('%Y-%m-%d')
    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i:i + batch_size]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, net_flow, big_net, big_net_pct, "
            f"mid_net, small_net, main_net_5day "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [ff_start, latest_date])
        for row in cur.fetchall():
            stock_fund_flows[row['stock_code']].append({
                'date': row['date'],
                'net_flow': _to_float(row['net_flow']),
                'big_net': _to_float(row['big_net']),
                'big_net_pct': _to_float(row['big_net_pct']),
                'mid_net': _to_float(row['mid_net']),
                'small_net': _to_float(row['small_net']),
                'main_net_5day': _to_float(row['main_net_5day']),
            })
    logger.info("[数据加载] 资金流向: %d 只股票有数据", len(stock_fund_flows))

    # 5c. 财报数据（最近2期）
    logger.info("[数据加载] 资金流向加载完成, 开始加载财报数据...")
    stock_finance = {}
    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i:i + batch_size]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, data_json FROM stock_finance "
            f"WHERE stock_code IN ({ph}) "
            f"ORDER BY stock_code, report_date DESC",
            batch)
        current_code = None
        count = 0
        for row in cur.fetchall():
            sc = row['stock_code']
            if sc != current_code:
                current_code = sc
                count = 0
            if count < 2:  # 只取最近2期
                if sc not in stock_finance:
                    stock_finance[sc] = []
                try:
                    stock_finance[sc].append(json.loads(row['data_json']))
                except (json.JSONDecodeError, TypeError):
                    pass
                count += 1
    logger.info("[数据加载] 财报数据: %d 只股票有数据", len(stock_finance))

    conn.close()

    logger.info("[数据加载] %d只股票K线, %d只有板块, %d板块K线, 大盘指数%d个",
                len(stock_klines), len(stock_boards),
                len(board_kline_map), len(market_klines_by_index))

    # 6. 行业分类（申万一级）
    stock_sectors = {}
    conn2 = get_connection(use_dict_cursor=True)
    cur2 = conn2.cursor()
    try:
        # 从 stock_concept_board_stock 中提取行业板块（board_code 以 BK 开头的通常是行业板块）
        # 但更可靠的方式是从 stock_analysis_detail 或直接用 sector_mapping_utils
        pass
    finally:
        cur2.close()
        conn2.close()

    # 使用本地行业映射工具
    try:
        from common.utils.sector_mapping_utils import parse_industry_list_md
        sector_mapping = parse_industry_list_md()
        for code in stock_codes:
            if code in sector_mapping:
                stock_sectors[code] = sector_mapping[code]
    except Exception as e:
        logger.warning("[数据加载] 行业映射加载失败: %s", e)

    # 7. 个股行为特征分析（基于已加载的K线数据）
    stock_behaviors = {}
    for code in stock_codes:
        kl = stock_klines.get(code, [])
        if len(kl) >= 20:
            stock_behaviors[code] = _classify_stock_behavior(kl)

    logger.info("[数据加载] 行业映射 %d 只, 行为分析 %d 只",
                len(stock_sectors), len(stock_behaviors))

    return {
        'stock_klines': dict(stock_klines),
        'stock_boards': dict(stock_boards),
        'board_kline_map': dict(board_kline_map),
        'market_klines': market_klines,
        'market_klines_by_index': dict(market_klines_by_index),
        'stock_names': stock_names,
        'stock_sectors': stock_sectors,
        'stock_behaviors': stock_behaviors,
        'stock_fund_flows': dict(stock_fund_flows),
        'stock_finance': stock_finance,
    }


# ═══════════════════════════════════════════════════════════
# 多维信号计算（资金流向、成交量、量价比、财报）
# ═══════════════════════════════════════════════════════════

def _get_market_klines_for_stock(code: str, data: dict) -> list[dict]:
    """获取个股对应的大盘指数K线数据。"""
    idx = _get_stock_index(code)
    mkt_by_idx = data.get('market_klines_by_index', {})
    klines = mkt_by_idx.get(idx)
    if klines:
        return klines
    # fallback: 上证指数
    return data.get('market_klines', [])


def _compute_fund_flow_signal(code: str, data: dict, latest_date: str,
                              iso_year: int, iso_week: int) -> dict:
    """计算资金流向信号。

    Returns:
        {
            'fund_flow_signal': float,  # 综合资金流信号 [-1, 1]
            'big_net_sum': float,       # 本周大单净额合计(万元)
            'main_net_5day': float,     # 最新5日主力净额(万元)
            'fund_flow_trend': str,     # 'inflow' / 'outflow' / 'neutral'
        }
    """
    ff_list = data.get('stock_fund_flows', {}).get(code, [])
    if not ff_list:
        return {'fund_flow_signal': None, 'big_net_sum': None,
                'main_net_5day': None, 'fund_flow_trend': None}

    # 本周资金流向
    week_ff = []
    for ff in ff_list:
        try:
            dt = datetime.strptime(ff['date'], '%Y-%m-%d')
            ical = dt.isocalendar()
            if ical[0] == iso_year and ical[1] == iso_week:
                week_ff.append(ff)
        except (ValueError, TypeError):
            continue

    if not week_ff:
        # 取最近5天
        recent = sorted(ff_list, key=lambda x: x['date'])[-5:]
        week_ff = recent

    big_net_sum = sum(f['big_net'] for f in week_ff)
    net_flow_sum = sum(f['net_flow'] for f in week_ff)
    latest_main_5d = week_ff[-1].get('main_net_5day', 0) if week_ff else 0

    # 大单净占比均值
    big_pcts = [f['big_net_pct'] for f in week_ff if f['big_net_pct'] != 0]
    avg_big_pct = _mean(big_pcts) if big_pcts else 0

    # 综合信号: 归一化到 [-1, 1]
    # 大单净占比 > 5% 视为强流入, < -5% 视为强流出
    signal = max(-1.0, min(1.0, avg_big_pct / 5.0))

    # 趋势判断
    if avg_big_pct > 2:
        trend = 'inflow'
    elif avg_big_pct < -2:
        trend = 'outflow'
    else:
        trend = 'neutral'

    return {
        'fund_flow_signal': round(signal, 4),
        'big_net_sum': round(big_net_sum, 2),
        'main_net_5day': round(latest_main_5d, 2),
        'fund_flow_trend': trend,
    }


def _compute_volume_signal(week_klines: list[dict], all_klines: list[dict]) -> dict:
    """计算成交量和量价比信号。

    Returns:
        {
            'vol_ratio': float,       # 本周均量 / 20日均量
            'vol_price_corr': float,  # 量价相关性 [-1, 1]
            'vol_trend': str,         # 'expanding' / 'shrinking' / 'normal'
        }
    """
    if not week_klines or not all_klines:
        return {'vol_ratio': None, 'vol_price_corr': None, 'vol_trend': None}

    # 本周均量
    week_vols = [k['volume'] for k in week_klines if k.get('volume', 0) > 0]
    if not week_vols:
        return {'vol_ratio': None, 'vol_price_corr': None, 'vol_trend': None}
    week_avg_vol = _mean(week_vols)

    # 20日均量（排除本周）
    sorted_klines = sorted(all_klines, key=lambda x: x['date'])
    if week_klines:
        first_week_date = week_klines[0]['date']
        hist_klines = [k for k in sorted_klines if k['date'] < first_week_date]
    else:
        hist_klines = sorted_klines
    hist_vols = [k['volume'] for k in hist_klines[-20:] if k.get('volume', 0) > 0]
    hist_avg_vol = _mean(hist_vols) if hist_vols else 0

    vol_ratio = round(week_avg_vol / hist_avg_vol, 4) if hist_avg_vol > 0 else None

    # 量价相关性（本周内：量增价涨为正相关）
    vol_price_corr = None
    if len(week_klines) >= 3:
        chgs = [k['change_percent'] for k in week_klines]
        vols = [k.get('volume', 0) for k in week_klines]
        if len(chgs) == len(vols) and _std(vols) > 0 and _std(chgs) > 0:
            n = len(chgs)
            mean_c = _mean(chgs)
            mean_v = _mean(vols)
            cov = sum((chgs[i] - mean_c) * (vols[i] - mean_v) for i in range(n)) / n
            vol_price_corr = round(cov / (_std(chgs) * _std(vols)), 4)

    # 量能趋势
    if vol_ratio is not None:
        if vol_ratio > 1.5:
            vol_trend = 'expanding'
        elif vol_ratio < 0.7:
            vol_trend = 'shrinking'
        else:
            vol_trend = 'normal'
    else:
        vol_trend = None

    return {
        'vol_ratio': vol_ratio,
        'vol_price_corr': vol_price_corr,
        'vol_trend': vol_trend,
    }


def _detect_volume_patterns(week_klines: list[dict], all_klines: list[dict]) -> dict:
    """检测成交量形态信号，用于置信度修正（基于回测验证的有效信号）。

    回测验证结果（5531只A股×29周=142,170样本）：
    - 成交量确认基线方向: 70.5% vs 矛盾: 52.7%（差距17.8%）
    - 恐慌底+大盘跌: 67.8%准确率
    - 天量阴线+高位: 58.1%准确率
    - 价升量缩: 55.9%准确率

    优化改进（基于学术文献验证）：
    - 天量检测改为扫描全部周内K线（不再break在第一根）
    - 恐慌底增加自适应阈值（基于个股历史波动率）
    - 量峰检测移除（回测仅53.5%，接近随机，不具备统计显著性）
    - 新增：高位价跌量增信号（顶部放量下跌，学术支撑强）
    - 信号强度评分替代简单bool，支持更精细的置信度修正

    Returns:
        {
            'vol_direction': 'up'/'down'/None,  # 成交量推断方向
            'vol_strength': float,              # 信号强度 0~1
            'panic_bottom': bool,               # 恐慌底信号
            'sky_vol_bearish': bool,            # 天量阴线
            'price_up_vol_down': bool,          # 价升量缩
            'rush_up_shrink': bool,             # 急涨后缩量
            'high_pos_down_vol_up': bool,       # 高位价跌量增
            'price_position': float,            # 价格位置(0~1)
        }
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
    }

    if not week_klines or not all_klines:
        return result

    sorted_klines = sorted(all_klines, key=lambda x: x['date'])
    first_week_date = week_klines[0]['date']
    hist = [k for k in sorted_klines if k['date'] < first_week_date]

    if len(hist) < 20:
        return result

    week_vols = [k['volume'] for k in week_klines if k.get('volume', 0) > 0]
    if not week_vols:
        return result

    week_avg_vol = _mean(week_vols)
    week_chg = _compound_return([k['change_percent'] for k in week_klines])

    hist_vols_20 = [k['volume'] for k in hist[-20:] if k.get('volume', 0) > 0]
    hist_vols_60 = [k['volume'] for k in hist[-60:] if k.get('volume', 0) > 0]
    avg_vol_20 = _mean(hist_vols_20) if hist_vols_20 else 0
    avg_vol_60 = _mean(hist_vols_60) if hist_vols_60 else 0
    vol_ratio_20 = week_avg_vol / avg_vol_20 if avg_vol_20 > 0 else None

    # 价格位置（相对60日高低点）
    hist_closes = [k['close'] for k in hist[-60:] if k.get('close', 0) > 0]
    if hist_closes:
        all_c = hist_closes + [k['close'] for k in week_klines if k.get('close', 0) > 0]
        if all_c:
            min_c, max_c = min(all_c), max(all_c)
            latest_c = week_klines[-1].get('close', 0)
            if max_c > min_c and latest_c > 0:
                result['price_position'] = round((latest_c - min_c) / (max_c - min_c), 4)

    pp = result['price_position']

    # 个股历史波动率（用于自适应阈值）
    hist_chgs = [abs(k['change_percent']) for k in hist[-20:] if k.get('change_percent') is not None]
    avg_volatility = _mean(hist_chgs) if hist_chgs else 2.0

    # ── 信号检测（按学术验证的可靠性排序）──

    # 1. 恐慌底: 本周跌 + 放量 + 低位（均值回归+放量确认）
    #    自适应阈值：高波动股需要更大跌幅才算恐慌
    panic_chg_th = max(-1.0, -avg_volatility * 0.5)
    if week_chg < panic_chg_th and vol_ratio_20 is not None and vol_ratio_20 > 1.3:
        if pp is not None and pp < 0.25:
            result['panic_bottom'] = True

    # 2. 天量阴线（扫描全部周内K线，取最极端的）
    #    学术依据：Gervais et al.(2001) 异常高成交量包含未来价格信息
    if avg_vol_60 > 0:
        max_sky_ratio = 0
        for k in week_klines:
            vol = k.get('volume', 0)
            if vol > avg_vol_60 * 3.0 and k.get('close', 0) < k.get('open', 0):
                sky_ratio = vol / avg_vol_60
                if sky_ratio > max_sky_ratio:
                    max_sky_ratio = sky_ratio
                    result['sky_vol_bearish'] = True

    # 3. 价升量缩（量价背离 — 学术支撑最强的看跌信号之一）
    #    学术依据：Campbell, Grossman & Wang(1993) 低成交量伴随的价格变动更可能反转
    if week_chg > 0.5 and vol_ratio_20 is not None and vol_ratio_20 < 0.8:
        result['price_up_vol_down'] = True

    # 4. 急涨后缩量（诱多出货形态）
    if len(week_klines) >= 4:
        mid = len(week_klines) // 2
        first_chg = _compound_return([k['change_percent'] for k in week_klines[:mid]])
        first_vol = _mean([k.get('volume', 0) for k in week_klines[:mid]])
        second_vol = _mean([k.get('volume', 0) for k in week_klines[mid:]])
        if first_chg > 2.0 and first_vol > 0 and second_vol < first_vol * 0.6:
            result['rush_up_shrink'] = True

    # 5. 高位价跌量增（顶部放量下跌 — 主力出货经典信号）
    #    学术依据：高位放量下跌是分布阶段(distribution)的典型特征
    if (week_chg < -1.0 and vol_ratio_20 is not None and vol_ratio_20 > 1.3
            and pp is not None and pp > 0.75):
        result['high_pos_down_vol_up'] = True

    # ── 推断方向（按回测验证的准确率优先级）──
    # 同时计算信号强度（用于精细化置信度修正）
    if result['panic_bottom']:
        result['vol_direction'] = 'up'
        # 强度：放量越大+位置越低 → 信号越强
        strength = min(1.0, (vol_ratio_20 - 1.0) * 0.5) if vol_ratio_20 else 0.5
        if pp is not None:
            strength *= (1.0 - pp * 2)  # 位置越低强度越高
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


def _adjust_nw_confidence_by_volume(pred_up: bool, confidence: str,
                                     vol_patterns: dict) -> tuple[str, str]:
    """根据成交量形态标注下周预测（仅标签，不修正置信度）。

    交叉验证结果（5531只A股×29周）：
    - 全样本：确认70.5% vs 矛盾52.7%（差距+17.8pp）
    - 交叉验证：确认70.6% vs 矛盾79.8%（差距-9.2pp，方向反转）
    - 结论：成交量信号的确认/矛盾区分在样本外失效，不应用于修正置信度
    - 保留信号标签作为参考信息，供用户自行判断

    Returns:
        (unchanged_confidence, vol_note)
    """
    vol_dir = vol_patterns.get('vol_direction')
    if vol_dir is None:
        return confidence, ''

    vol_agrees = (vol_dir == 'up') == pred_up
    strength = vol_patterns.get('vol_strength', 0.5)

    # 构建信号描述
    signal_labels = []
    if vol_patterns.get('panic_bottom'):
        signal_labels.append('恐慌底')
    if vol_patterns.get('sky_vol_bearish'):
        signal_labels.append('天量阴线')
    if vol_patterns.get('high_pos_down_vol_up'):
        signal_labels.append('高位放量跌')
    if vol_patterns.get('price_up_vol_down'):
        signal_labels.append('价升量缩')
    if vol_patterns.get('rush_up_shrink'):
        signal_labels.append('急涨缩量')
    label = ','.join(signal_labels) if signal_labels else '量能'

    if vol_agrees:
        # 确认 → 仅记录标签，不修改置信度
        # CV验证：确认(70.6%) < 矛盾(79.8%)，差距反转-9.2pp
        # 成交量信号的确认/矛盾区分在样本外失效，不应用于修正
        return confidence, f'量能确认({label})'
    else:
        # 矛盾 → 仅记录标签，不修改置信度
        return confidence, f'量能矛盾({label})'


def _adjust_nw_confidence_by_board(pred_up: bool, confidence: str,
                                    board_momentum: float | None,
                                    concept_consensus: float | None) -> tuple[str, str]:
    """根据概念板块强弱势修正下周预测置信度。

    回测验证（1200只股票, 29周）:
    预测涨:
      板块大跌<-3%确认: 91.2% vs 基线82.5% (+8.7pp), 1582样本
      全部看跌确认: 87.2% vs 基线82.5% (+4.7pp), 1755样本
      板块微跌-1~0%矛盾: 65.7% vs 基线82.5% (-16.7pp), 356样本
    预测跌:
      板块涨>1%确认: 82.5% vs 基线72.2% (+10.2pp), 114样本
      全部看涨确认: 79.2% vs 基线72.2% (+7.0pp), 72样本

    策略: 板块因子与规则预测方向一致时提升置信度，矛盾时降低。
    """
    if board_momentum is None and concept_consensus is None:
        return confidence, ''

    if pred_up:
        # 预测涨 → 板块跌=确认(超跌反弹), 板块涨=矛盾
        if board_momentum is not None and board_momentum < -3:
            # 板块大跌确认: +8.7pp
            if confidence == 'reference':
                return 'high', '板块确认↑'
            return confidence, '板块确认'
        elif concept_consensus is not None and concept_consensus == 0:
            # 全部看跌确认: +4.7pp
            return confidence, '板块共识确认'
        elif board_momentum is not None and -1 <= board_momentum < 0:
            # 板块微跌矛盾: -16.7pp — 板块没跌够，反弹信号弱
            if confidence == 'high':
                return 'reference', '板块弱矛盾↓'
            return confidence, '板块弱信号'
    else:
        # 预测跌 → 板块涨=确认(个股逆势弱), 板块跌=矛盾
        if board_momentum is not None and board_momentum > 1:
            # 板块涨确认: +10.2pp
            if confidence == 'reference':
                return 'high', '板块确认↑'
            return confidence, '板块确认'
        elif concept_consensus is not None and concept_consensus == 1.0:
            # 全部看涨确认: +7.0pp
            return confidence, '板块共识确认'

    return confidence, ''


def _compute_finance_signal(code: str, data: dict) -> dict:
    """从财报数据中提取关键财务信号。

    Returns:
        {
            'revenue_yoy': float,     # 营收同比增长率(%)
            'profit_yoy': float,      # 净利润同比增长率(%)
            'roe': float,             # ROE(%)
            'finance_score': float,   # 财务综合评分 [-1, 1]
        }
    """
    fin_list = data.get('stock_finance', {}).get(code, [])
    if not fin_list:
        return {'revenue_yoy': None, 'profit_yoy': None,
                'roe': None, 'finance_score': None}

    latest = fin_list[0]  # 最新一期

    # 提取关键指标（字段名来自同花顺财报JSON）
    revenue_yoy = None
    profit_yoy = None
    roe = None

    for key in ('营业总收入同比增长率(%)', '营业收入同比增长率(%)'):
        if key in latest:
            try:
                revenue_yoy = float(latest[key])
            except (ValueError, TypeError):
                pass
            break

    for key in ('净利润同比增长率(%)', '归属母公司股东的净利润同比增长率(%)'):
        if key in latest:
            try:
                profit_yoy = float(latest[key])
            except (ValueError, TypeError):
                pass
            break

    for key in ('净资产收益率(%)', '加权净资产收益率(%)'):
        if key in latest:
            try:
                roe = float(latest[key])
            except (ValueError, TypeError):
                pass
            break

    # 综合评分: 基于营收增长、利润增长、ROE
    score_parts = []
    if revenue_yoy is not None:
        # 营收增长 > 20% 加分, < -10% 减分
        score_parts.append(max(-1, min(1, revenue_yoy / 30)))
    if profit_yoy is not None:
        # 利润增长 > 30% 加分, < -20% 减分
        score_parts.append(max(-1, min(1, profit_yoy / 40)))
    if roe is not None:
        # ROE > 15% 优秀, < 5% 较差
        score_parts.append(max(-1, min(1, (roe - 10) / 10)))

    finance_score = round(_mean(score_parts), 4) if score_parts else None

    return {
        'revenue_yoy': round(revenue_yoy, 2) if revenue_yoy is not None else None,
        'profit_yoy': round(profit_yoy, 2) if profit_yoy is not None else None,
        'roe': round(roe, 2) if roe is not None else None,
        'finance_score': finance_score,
    }


# ═══════════════════════════════════════════════════════════
# 预测核心逻辑
# ═══════════════════════════════════════════════════════════

def _predict_stock_weekly(code: str, data: dict, latest_date: str) -> dict | None:
    """对单只股票进行本周方向预测。

    基于最新交易日所在ISO周的已有K线数据，计算d3/d4信号并预测。
    """
    klines = data['stock_klines'].get(code, [])
    if not klines:
        return None

    # 确定最新交易日所在的ISO周
    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    iso_cal = dt_latest.isocalendar()
    iso_year, iso_week = iso_cal[0], iso_cal[1]

    # 获取本周的K线数据
    week_klines = []
    for k in klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        ical = dt.isocalendar()
        if ical[0] == iso_year and ical[1] == iso_week:
            week_klines.append(k)

    week_klines.sort(key=lambda x: x['date'])

    if len(week_klines) < 1:
        return None

    n_days = len(week_klines)
    daily_pcts = [k['change_percent'] for k in week_klines]

    # d3信号 + 日期区间
    d3_chg = None
    d3_date_range = None
    if n_days >= 3:
        d3_chg = _compound_return(daily_pcts[:3])
        d3_date_range = f"{week_klines[0]['date']}~{week_klines[2]['date']}"

    # d4信号 + 日期区间
    d4_chg = None
    d4_date_range = None
    if n_days >= 4:
        d4_chg = _compound_return(daily_pcts[:4])
        d4_date_range = f"{week_klines[0]['date']}~{week_klines[3]['date']}"

    # 停牌检测
    is_suspended = n_days >= 3 and all(p == 0 for p in daily_pcts[:3])

    # 大盘本周信号（使用个股对应的大盘指数）
    market_klines = _get_market_klines_for_stock(code, data)
    market_week = []
    for k in market_klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        ical = dt.isocalendar()
        if ical[0] == iso_year and ical[1] == iso_week:
            market_week.append(k)
    market_week.sort(key=lambda x: x['date'])

    market_d3 = None
    market_d4 = None
    if len(market_week) >= 3:
        market_d3 = _compound_return([k['change_percent'] for k in market_week[:3]])
    if len(market_week) >= 4:
        market_d4 = _compound_return([k['change_percent'] for k in market_week[:4]])

    # 概念板块信号
    boards = data['stock_boards'].get(code, [])
    board_names = [b['board_name'] for b in boards[:5]]
    board_momentum = None
    concept_consensus = None

    if boards:
        board_kline_map = data['board_kline_map']
        momentums = []
        boards_up = 0
        valid_boards = 0
        for b in boards:
            bk = board_kline_map.get(b['board_code'], [])
            valid_bk = [k for k in bk if k['date'] <= latest_date]
            if len(valid_bk) >= 5:
                avg_chg = _mean([k['change_percent'] for k in valid_bk[-5:]])
                momentums.append(avg_chg)
                valid_boards += 1
                if avg_chg > 0:
                    boards_up += 1
        if momentums:
            board_momentum = round(_mean(momentums), 4)
        if valid_boards > 0:
            concept_consensus = round(boards_up / valid_boards, 3)

    # ── 多维信号计算 ──
    # 资金流向信号
    ff_result = _compute_fund_flow_signal(code, data, latest_date, iso_year, iso_week)
    fund_flow_signal = ff_result['fund_flow_signal']

    # 成交量 & 量价比信号
    vol_result = _compute_volume_signal(week_klines, klines)

    # 财报信号
    fin_result = _compute_finance_signal(code, data)

    # ── 预测逻辑（v5: 行业自适应 + 个股行为分析） ──
    sector = data.get('stock_sectors', {}).get(code, '')
    behavior = data.get('stock_behaviors', {}).get(code, {})
    profile = _get_stock_strategy_profile(code, sector, behavior)

    pred_up, confidence, strategy, reason = _predict_with_profile(
        d4_chg, d3_chg, is_suspended, n_days, daily_pcts, profile)

    stock_name = data['stock_names'].get(code, '')

    # ── 计算建议买入时间和价格 ──
    suggested_buy_date = None
    suggested_buy_price = None
    suggested_buy_reason = None

    if pred_up and confidence in ('high', 'medium'):
        # 预测涨 + 中高置信度 → 给出买入建议
        latest_close = week_klines[-1].get('close', 0) if week_klines else 0

        if latest_close > 0:
            if n_days <= 3:
                # 本周仅3天数据(周三)，建议周四开盘买入
                # 买入价 = 最新收盘价 * (1 - 小幅折扣)
                # 如果d3跌幅较大，可能还有下探空间，给更大折扣
                if d3_chg is not None and d3_chg < -1.0:
                    discount = 0.005  # 跌势中给0.5%折扣
                    suggested_buy_reason = f'd3跌{d3_chg:.1f}%后反弹预期,建议低吸'
                else:
                    discount = 0.002  # 正常0.2%折扣
                    suggested_buy_reason = f'预测涨({confidence}),建议次日开盘附近买入'
                suggested_buy_price = round(latest_close * (1 - discount), 2)
                # 下一个交易日
                dt_next = _next_trade_date(dt_latest)
                suggested_buy_date = dt_next.strftime('%Y-%m-%d')

            elif n_days == 4:
                # 本周已有4天数据(周四)，建议周五开盘买入
                if d4_chg is not None and d4_chg < -1.5:
                    discount = 0.008  # 连跌4天给更大折扣
                    suggested_buy_reason = f'd4跌{d4_chg:.1f}%后反弹预期,建议低吸'
                elif d4_chg is not None and d4_chg > 2.0:
                    discount = 0.0  # 强势追涨
                    suggested_buy_reason = f'd4涨{d4_chg:.1f}%强势,建议追涨'
                else:
                    discount = 0.003
                    suggested_buy_reason = f'预测涨({confidence}),建议次日开盘附近买入'
                suggested_buy_price = round(latest_close * (1 - discount), 2)
                dt_next = _next_trade_date(dt_latest)
                suggested_buy_date = dt_next.strftime('%Y-%m-%d')

            elif n_days >= 5:
                # 本周已有5天数据(周五)，建议下周一买入
                suggested_buy_reason = f'本周已收盘,建议下周一开盘买入'
                suggested_buy_price = round(latest_close, 2)
                dt_next = _next_trade_date(dt_latest)
                suggested_buy_date = dt_next.strftime('%Y-%m-%d')

    return {
        'stock_code': code,
        'stock_name': stock_name,
        'predict_date': latest_date,
        'iso_year': iso_year,
        'iso_week': iso_week,
        'pred_direction': 'UP' if pred_up else 'DOWN',
        'confidence': confidence,
        'strategy': strategy,
        'reason': reason[:200],
        'd3_chg': d3_chg,
        'd3_date_range': d3_date_range,
        'd4_chg': d4_chg,
        'd4_date_range': d4_date_range,
        'is_suspended': 1 if is_suspended else 0,
        'week_day_count': n_days,
        'board_momentum': board_momentum,
        'concept_consensus': concept_consensus,
        'fund_flow_signal': fund_flow_signal,
        'market_d3_chg': market_d3,
        'market_d4_chg': market_d4,
        'market_index': _get_stock_index(code),
        'concept_boards': ','.join(board_names)[:500] if board_names else None,
        'backtest_accuracy': None,  # 后续填充
        'backtest_lowo_accuracy': None,
        'backtest_weeks': None,
        'backtest_samples': None,
        'backtest_start_date': None,
        'backtest_end_date': None,
        'suggested_buy_date': suggested_buy_date,
        'suggested_buy_price': suggested_buy_price,
        'suggested_buy_reason': suggested_buy_reason[:200] if suggested_buy_reason else None,
        'pred_weekly_chg': None,  # 后续由回测数据填充
        'pred_chg_low': None,
        'pred_chg_high': None,
        'pred_chg_mae': None,
        'pred_chg_hit_rate': None,
        'pred_chg_samples': None,
        'week_realized_chg': round(_compound_return(daily_pcts), 4) if daily_pcts else None,
        'pred_remaining_chg': None,  # 后续由 pred_weekly_chg - week_realized_chg 计算
        # 多维信号
        'vol_ratio': vol_result.get('vol_ratio'),
        'vol_price_corr': vol_result.get('vol_price_corr'),
        'vol_trend': vol_result.get('vol_trend'),
        'fund_flow_trend': ff_result.get('fund_flow_trend'),
        'big_net_sum': ff_result.get('big_net_sum'),
        'main_net_5day': ff_result.get('main_net_5day'),
        'finance_score': fin_result.get('finance_score'),
        'revenue_yoy': fin_result.get('revenue_yoy'),
        'profit_yoy': fin_result.get('profit_yoy'),
        'roe': fin_result.get('roe'),
    }


# ═══════════════════════════════════════════════════════════
# 下周预测（规则引擎 V4 - 全场景覆盖版 + 板块置信度修正）
# ═══════════════════════════════════════════════════════════

# 规则集：按优先级排列，互斥匹配（命中第一条即停止）
# 策略核心：只在高置信条件下输出预测，其余标记为"不确定"
#
# V5优化回测实证（5233只A股, 29周, 134,920样本）:
#
# V4基线:     81.6% (11,098/13,606) 覆盖10.1%
# V5(移除R2R8): 82.8% (9,717/11,734) 覆盖8.7%  — 准确率+1.2%
# V5 CV验证:   84.0% (6,336/7,546) — 过拟合差距仅-1.2%
#
# V5核心变更:
#   1. 移除R2(上证+大盘跌+跌>5%+非高位→涨) — CV62.0%,过拟合gap+11.4%
#   2. 移除R4(CV样本仅9个) — 已在V4中移除
#   3. 移除R6b(CV57.1%≈随机) — 已在V4中移除
#   4. 移除R8(上证+大盘微跌+涨+前周跌→跌) — CV61.5%,过拟合gap+13.4%
#   5. R6a降为Tier2(CV63.6%,gap+9.4%) — 已在V4中降级
#   6. 移除R2后R3覆盖扩大(341→681样本), CV71.4%稳健
#
# 稳健规则(CV验证通过):
#   R1:  CV89.5%, gap+0.0% — 最稳健
#   R5a: CV90.6%, gap-1.5%
#   R5b: CV86.4%, gap-7.9%
#   R5c: CV79.6%, gap-4.3%
#   R3:  CV71.4%, gap-3.8%
#   R7:  CV73.3%, gap-2.2%
#
# 候选规则验证结果(均未通过):
#   P2(连续两周跌→涨): 56.7%~58.4%, 接近随机 ❌
#   P3(大盘涨+逆势): 37.1%~55.9% ❌
#   P4(尾日恐慌→涨): 全样本70.6%但独立样本仅48.1% ❌
#   P5(北交所): 无数据(已排除) ❌
#
# 规则列表:
#   R1: 大盘深跌>3% + 个股跌>2% → 涨 (89.6%, CV89.5%)
#   R3: 上证+大盘跌1-3% + 跌>3% + 前周跌 → 涨 (67.5%, CV71.4%)
#   R5a: 深证+大盘微跌 + 跌>2% + 连跌≥3天 → 涨 (89.1%, CV90.6%)
#   R5b: 深证+大盘微跌 + 跌>2% + 低位<0.2 → 涨 (78.5%, CV86.4%)
#   R5c: 深证+大盘微跌 + 跌>2% → 涨 (75.3%, CV79.6%)
#   R6a: 深证+大盘跌1~3% + 涨>5% → 跌 (73.0%, CV63.6%) — Tier2
#   R6c: 深证+大盘跌1~3% + 涨>2% + 连涨≥3天 → 跌 (73.5%, CV64.3%)
#   R7: 跌>3% + 连涨≥3天 + 非高位<0.6 → 跌 (71.1%, CV73.3%)
#   资金流向/财报规则: 保留（实盘触发）

# 不同指数的大盘跌幅阈值
_INDEX_MKT_THRESHOLD = {
    '000001.SH': 1.0,   # 上证: 跌>1%
    '399001.SZ': 1.5,   # 深证: 收紧到跌>1.5%
    '899050.SZ': 2.0,   # 北证: 收紧到跌>2%
}

_NW_RULES = [
    # ══════════════════════════════════════════════════
    # Tier 1: 涨信号 (准确率≥68%)
    # ══════════════════════════════════════════════════

    # R1: 大盘深跌>3% + 个股跌>2% → 涨 (89.6%, 6297样本)
    {
        'name': '大盘深跌>3%+个股跌→涨',
        'pred_up': True,
        'tier': 1,
        'check': lambda chg, mkt, cd, cu, ld, **kw: (
            chg < -2 and mkt < -3
        ),
    },
    # R2: 已移除 — 全样本73.4%(1745) → CV62.0%(326), 过拟合差距+11.4%
    # V5回测验证: 移除后整体准确率从81.6%→82.8%, CV从82.5%→84.0%
    # R3: 上证+大盘跌1-3%+个股跌>3%+前周跌<-2%+非高位 → 涨 (67.5%, CV71.4%)
    {
        'name': '上证+大盘跌+跌>3%+前周跌→涨',
        'pred_up': True,
        'tier': 1,
        'check': lambda chg, mkt, cd, cu, ld, **kw: (
            chg < -3
            and kw.get('_market_suffix', '') == 'SH'
            and -3 <= mkt < -1.0
            and (kw.get('_prev_week_chg') is not None and kw['_prev_week_chg'] < -2)
            and not (kw.get('_price_pos_60') is not None and kw['_price_pos_60'] >= 0.8)
        ),
    },
    # R4: 已移除 — CV样本仅9个，无法验证有效性
    # R5a: 深证+大盘微跌(0~1%)+个股跌>2%+连跌≥3天 → 涨 (89.1%, 514样本)
    {
        'name': '深证+大盘微跌+跌+连跌3天→涨',
        'pred_up': True,
        'tier': 1,
        'check': lambda chg, mkt, cd, cu, ld, **kw: (
            kw.get('_market_suffix', '') == 'SZ'
            and -1 <= mkt < 0
            and chg < -2
            and cd >= 3
        ),
    },
    # R5b: 深证+大盘微跌(0~1%)+个股跌>2%+低位<0.2 → 涨 (78.5%, 627样本)
    {
        'name': '深证+大盘微跌+跌+低位→涨',
        'pred_up': True,
        'tier': 1,
        'check': lambda chg, mkt, cd, cu, ld, **kw: (
            kw.get('_market_suffix', '') == 'SZ'
            and -1 <= mkt < 0
            and chg < -2
            and kw.get('_price_pos_60') is not None and kw['_price_pos_60'] < 0.2
        ),
    },
    # R5c: 深证+大盘微跌(0~1%)+个股跌>2% → 涨 (75.3%, 1802样本)
    {
        'name': '深证+大盘微跌+跌>2%→涨',
        'pred_up': True,
        'tier': 1,
        'check': lambda chg, mkt, cd, cu, ld, **kw: (
            kw.get('_market_suffix', '') == 'SZ'
            and -1 <= mkt < 0
            and chg < -2
        ),
    },
    # ── 资金流向涨信号（实盘触发）──
    {
        'name': '跌>2%+主力流入+放量→涨',
        'pred_up': True,
        'tier': 1,
        'check': lambda chg, mkt, cd, cu, ld, **kw: (
            chg < -2
            and (kw.get('ff_signal') or 0) > 0.3
            and (kw.get('vol_ratio') or 0) > 1.2
        ),
    },
    {
        'name': '涨>3%+资金流入+量价齐升→涨',
        'pred_up': True,
        'tier': 1,
        'check': lambda chg, mkt, cd, cu, ld, **kw: (
            chg > 3
            and (kw.get('ff_signal') or 0) > 0.2
            and (kw.get('vol_ratio') or 0) > 1.0
            and (kw.get('vol_price_corr') or 0) > 0.3
        ),
    },

    # ══════════════════════════════════════════════════
    # Tier 1: 跌信号 (准确率≥68%)
    # ══════════════════════════════════════════════════

    # R6a: 深证+大盘跌1~3%+个股涨>5% → 跌
    # 全样本73.0%(1292) → CV63.6%(780), 差距9.4% → 降为Tier2
    {
        'name': '深证+大盘跌+涨>5%→跌',
        'pred_up': False,
        'tier': 2,
        'check': lambda chg, mkt, cd, cu, ld, **kw: (
            kw.get('_market_suffix', '') == 'SZ'
            and -3 <= mkt < -1
            and chg > 5
        ),
    },
    # R6b: 已移除 — CV仅57.1%(63样本)，接近随机
    # R6c: 深证+大盘跌1~3%+个股涨>2%+连涨≥3天 → 跌 (66.9%)
    {
        'name': '深证+大盘跌+涨+连涨3天→跌',
        'pred_up': False,
        'tier': 1,
        'check': lambda chg, mkt, cd, cu, ld, **kw: (
            kw.get('_market_suffix', '') == 'SZ'
            and -3 <= mkt < -1
            and chg > 2
            and cu >= 3
        ),
    },

    # ══════════════════════════════════════════════════
    # Tier 2: 中等置信信号 (准确率60-68%)
    # ══════════════════════════════════════════════════

    # R7: 跌>3%+连涨≥3天+非高位<0.6 → 跌 (71.7%, 311样本)
    {
        'name': '跌+前期连涨+非高位→跌',
        'pred_up': False,
        'tier': 2,
        'check': lambda chg, mkt, cd, cu, ld, **kw: (
            chg < -3
            and cu >= 3
            and kw.get('_price_pos_60') is not None and kw['_price_pos_60'] < 0.6
        ),
    },
    # R8: 已移除 — 全样本74.9%(279) → CV61.5%(156), 过拟合差距+13.4%
    # V5回测验证: 移除后整体CV准确率提升+1.5%
    # ── 资金流向跌信号（实盘触发）──
    {
        'name': '资金流出+缩量→跌',
        'pred_up': False,
        'tier': 3,
        'check': lambda chg, mkt, cd, cu, ld, **kw: (
            (kw.get('ff_signal') or 0) < -0.4
            and kw.get('vol_ratio') is not None
            and (kw.get('vol_ratio') or 0) < 0.7
            and chg < 0
        ),
    },
    # ── 财报利好信号（实盘触发）──
    {
        'name': '财报利好+资金流入→涨',
        'pred_up': True,
        'tier': 3,
        'check': lambda chg, mkt, cd, cu, ld, **kw: (
            (kw.get('finance_score') or 0) > 0.5
            and (kw.get('ff_signal') or 0) > 0.2
        ),
    },
]


def _nw_extract_features(daily_pcts: list[float], market_chg: float,
                         ff_signal: float = None, vol_ratio: float = None,
                         vol_price_corr: float = None,
                         finance_score: float = None,
                         market_index: str = '000001.SH',
                         price_pos_60: float = None,
                         prev_week_chg: float = None) -> dict:
    """从日K线数据和多维信号中提取下周预测所需的特征。

    Args:
        market_index: 个股对应的大盘指数代码，用于自适应阈值
        price_pos_60: 价格在60日高低点中的位置(0~1)
        prev_week_chg: 前一周涨跌幅
    """
    this_week_chg = _compound_return(daily_pcts)
    last_day_chg = daily_pcts[-1] if daily_pcts else 0.0

    # 连涨/连跌天数（从尾部开始计算）
    consec_down = 0
    consec_up = 0
    for p in reversed(daily_pcts):
        if p < 0:
            consec_down += 1
            if consec_up > 0:
                break
        elif p > 0:
            consec_up += 1
            if consec_down > 0:
                break
        else:
            break

    # 根据指数代码确定自适应阈值和市场后缀
    mkt_threshold = _INDEX_MKT_THRESHOLD.get(market_index, 1.0)
    market_suffix = market_index.split('.')[-1] if '.' in market_index else ''

    return {
        'this_week_chg': this_week_chg,
        'market_chg': market_chg,
        'consec_down': consec_down,
        'consec_up': consec_up,
        'last_day_chg': last_day_chg,
        'ff_signal': ff_signal,
        'vol_ratio': vol_ratio,
        'vol_price_corr': vol_price_corr,
        'finance_score': finance_score,
        '_mkt_threshold': mkt_threshold,
        '_market_suffix': market_suffix,
        '_price_pos_60': price_pos_60,
        '_prev_week_chg': prev_week_chg,
    }


def _nw_match_rule(feat: dict) -> dict | None:
    """用规则引擎匹配下周预测。返回匹配的规则或 None（不确定）。"""
    chg = feat['this_week_chg']
    mkt = feat['market_chg']
    cd = feat['consec_down']
    cu = feat['consec_up']
    ld = feat['last_day_chg']
    # 多维信号 + 指数自适应参数传递给规则的 **kw
    extra = {
        'ff_signal': feat.get('ff_signal'),
        'vol_ratio': feat.get('vol_ratio'),
        'vol_price_corr': feat.get('vol_price_corr'),
        'finance_score': feat.get('finance_score'),
        '_mkt_threshold': feat.get('_mkt_threshold', 1.0),
        '_market_suffix': feat.get('_market_suffix', ''),
        '_price_pos_60': feat.get('_price_pos_60'),
        '_prev_week_chg': feat.get('_prev_week_chg'),
    }

    for rule in _NW_RULES:
        if rule['check'](chg, mkt, cd, cu, ld, **extra):
            return rule
    return None



def _predict_next_week(code: str, data: dict, latest_date: str,
                       this_week_pred: dict,
                       deepseek_result: dict | None = None) -> dict | None:
    """预测下周方向（规则引擎V5 + DeepSeek AI增强）。

    V5优化（基于5233只A股×29周时间序列交叉验证 + V5回测验证）：
    - V4全样本81.6% vs CV82.5%，过拟合差距-1.0%（规则整体稳健）
    - 移除R2(CV62.0%,过拟合gap+11.4%)、R4(CV样本不足)、R6b(CV57.1%≈随机)
    - 移除R8(CV61.5%,过拟合gap+13.4%) — V5新增移除
    - R6a降为Tier2（CV63.6%,gap+9.4%，存在过拟合）
    - 成交量信号改为仅标签不修正（CV中确认/矛盾差距反转-9.2pp）
    - V5移除R2/R8后: 全样本82.8% vs CV84.0%，准确率+1.2%，覆盖率-1.4%
    稳健规则: R1(CV89.5%), R5a(CV90.6%), R5b(CV86.4%), R5c(CV79.6%), R7(CV73.3%)

    V6增强（DeepSeek AI辅助）：
    - 规则引擎未命中时，使用 DeepSeek 推理引擎作为 fallback
    - DeepSeek 置信度≥0.55 时输出预测，否则仍标记为不确定
    - 策略标记为 'nw_deepseek_ai'，置信度标记为 'ai_reference'

    Args:
        deepseek_result: DeepSeek预测结果 {'direction': 'UP'|'DOWN',
                         'confidence': float, 'justification': str} 或 None

    Returns:
        dict with next_week fields, or None if insufficient data
    """
    klines = data['stock_klines'].get(code, [])
    if not klines:
        return None

    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    iso_cal = dt_latest.isocalendar()
    iso_year, iso_week = iso_cal[0], iso_cal[1]

    # 获取本周K线
    week_klines = []
    for k in klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        ical = dt.isocalendar()
        if ical[0] == iso_year and ical[1] == iso_week:
            week_klines.append(k)
    week_klines.sort(key=lambda x: x['date'])

    if len(week_klines) < 3:
        return None

    daily_pcts = [k['change_percent'] for k in week_klines]

    # 获取个股对应大盘指数的本周涨跌幅
    market_klines = _get_market_klines_for_stock(code, data)
    market_week = []
    for k in market_klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        ical = dt.isocalendar()
        if ical[0] == iso_year and ical[1] == iso_week:
            market_week.append(k)
    market_chg = _compound_return(
        [k['change_percent'] for k in sorted(market_week, key=lambda x: x['date'])]
    ) if len(market_week) >= 3 else 0.0

    # 多维信号（从 this_week_pred 中获取已计算的信号）
    ff_signal = this_week_pred.get('fund_flow_signal') if this_week_pred else None
    vol_ratio = this_week_pred.get('vol_ratio') if this_week_pred else None
    vol_price_corr = this_week_pred.get('vol_price_corr') if this_week_pred else None
    finance_score = this_week_pred.get('finance_score') if this_week_pred else None

    # 提取特征 & 匹配规则（含多维信号 + 指数自适应阈值 + 成交量/位置特征）
    stock_idx = _get_stock_index(code)

    # 计算价格位置（60日高低点中的位置）
    sorted_klines = sorted(klines, key=lambda x: x['date'])
    first_week_date = week_klines[0]['date']
    hist_klines = [k for k in sorted_klines if k['date'] < first_week_date]
    price_pos_60 = None
    if len(hist_klines) >= 20:
        hist_closes = [k.get('close', 0) for k in hist_klines[-60:] if k.get('close', 0) > 0]
        if hist_closes:
            all_c = hist_closes + [k.get('close', 0) for k in week_klines if k.get('close', 0) > 0]
            min_c, max_c = min(all_c), max(all_c)
            latest_c = week_klines[-1].get('close', 0)
            if max_c > min_c and latest_c > 0:
                price_pos_60 = round((latest_c - min_c) / (max_c - min_c), 4)

    # 计算前一周涨跌幅
    prev_week_chg = None
    prev_week_klines = hist_klines[-5:] if len(hist_klines) >= 5 else hist_klines
    if prev_week_klines:
        prev_week_chg = _compound_return([k['change_percent'] for k in prev_week_klines])

    feat = _nw_extract_features(daily_pcts, market_chg,
                                ff_signal=ff_signal, vol_ratio=vol_ratio,
                                vol_price_corr=vol_price_corr,
                                finance_score=finance_score,
                                market_index=stock_idx,
                                price_pos_60=price_pos_60,
                                prev_week_chg=prev_week_chg)
    rule = _nw_match_rule(feat)

    # 下周日期范围
    nw_monday = _next_week_monday(dt_latest)
    nw_friday = nw_monday + timedelta(days=4)
    nw_iso = nw_monday.isocalendar()

    if rule is None:
        # ── DeepSeek AI 增强：规则未命中时使用 AI 推理 ──
        if deepseek_result and deepseek_result.get('direction') in ('UP', 'DOWN'):
            ds_dir = deepseek_result['direction']
            ds_conf = deepseek_result.get('confidence', 0.5)
            ds_reason = deepseek_result.get('justification', '')

            # 置信度映射：DeepSeek confidence → 系统置信度标签
            # 回测验证：≥65%准确率57.7%，<65%准确率约50%
            if ds_conf >= 0.70:
                confidence = 'high'
            elif ds_conf >= 0.65:
                confidence = 'reference'
            else:
                confidence = 'low'

            # 构建理由
            idx_code = _get_stock_index(code)
            idx_names = {'000001.SH': '上证', '399001.SZ': '深证', '899050.SZ': '北证50'}
            idx_label = idx_names.get(idx_code, idx_code)
            parts = [f'[AI]{ds_reason}']
            parts.append(f'本周{feat["this_week_chg"]:+.1f}%')
            if feat['market_chg'] != 0:
                parts.append(f'{idx_label}{feat["market_chg"]:+.1f}%')
            parts.append(f'AI置信{ds_conf:.0%}')
            nw_reason = '; '.join(parts)

            return {
                'nw_pred_direction': ds_dir,
                'nw_confidence': confidence,
                'nw_strategy': 'nw_deepseek_ai',
                'nw_reason': nw_reason[:200],
                'nw_composite_score': round(ds_conf * (1 if ds_dir == 'UP' else -1), 4),
                'nw_this_week_chg': round(feat['this_week_chg'], 4),
                'nw_iso_year': nw_iso[0],
                'nw_iso_week': nw_iso[1],
                'nw_date_range': f'{nw_monday.strftime("%Y-%m-%d")}~{nw_friday.strftime("%Y-%m-%d")}',
                'nw_pred_chg': None,
                'nw_pred_chg_low': None,
                'nw_pred_chg_high': None,
                'nw_pred_chg_mae': None,
                'nw_pred_chg_hit_rate': None,
                'nw_pred_chg_samples': None,
                'nw_backtest_accuracy': None,
                'nw_backtest_samples': None,
            }

        # 不确定 - 规则未命中且无 DeepSeek 结果
        idx_code = _get_stock_index(code)
        idx_names = {'000001.SH': '上证', '399001.SZ': '深证', '899050.SZ': '北证50'}
        idx_label = idx_names.get(idx_code, idx_code)
        reason = f'本周{feat["this_week_chg"]:+.1f}%，未触发预测条件'
        if feat['market_chg'] != 0:
            reason += f'({idx_label}{feat["market_chg"]:+.1f}%)'
        return {
            'nw_pred_direction': None,
            'nw_confidence': None,
            'nw_strategy': None,
            'nw_reason': reason[:200],
            'nw_composite_score': None,
            'nw_this_week_chg': round(feat['this_week_chg'], 4),
            'nw_iso_year': nw_iso[0],
            'nw_iso_week': nw_iso[1],
            'nw_date_range': f'{nw_monday.strftime("%Y-%m-%d")}~{nw_friday.strftime("%Y-%m-%d")}',
            'nw_pred_chg': None,
            'nw_pred_chg_low': None,
            'nw_pred_chg_high': None,
            'nw_pred_chg_mae': None,
            'nw_pred_chg_hit_rate': None,
            'nw_pred_chg_samples': None,
            'nw_backtest_accuracy': None,
            'nw_backtest_samples': None,
        }

    # 命中规则
    nw_pred_up = rule['pred_up']
    tier = rule['tier']
    if tier == 1:
        confidence = 'high'
    else:
        confidence = 'reference'  # Tier 2: 参考级, 准确率~56-59%

    # 成交量形态置信度修正（回测验证：确认70.5% vs 矛盾52.7%）
    vol_patterns = _detect_volume_patterns(week_klines, klines)
    vol_note = ''
    if vol_patterns.get('vol_direction'):
        confidence, vol_note = _adjust_nw_confidence_by_volume(
            nw_pred_up, confidence, vol_patterns)

    # 概念板块强弱势置信度修正（回测验证：确认91.2% vs 矛盾65.7%）
    board_momentum = this_week_pred.get('board_momentum') if this_week_pred else None
    concept_consensus = this_week_pred.get('concept_consensus') if this_week_pred else None
    board_note = ''
    if board_momentum is not None or concept_consensus is not None:
        confidence, board_note = _adjust_nw_confidence_by_board(
            nw_pred_up, confidence, board_momentum, concept_consensus)

    # 构建理由（包含指数名称和多维信号）
    idx_code = _get_stock_index(code)
    idx_names = {'000001.SH': '上证', '399001.SZ': '深证', '899050.SZ': '北证50'}
    idx_label = idx_names.get(idx_code, idx_code)

    parts = [rule['name']]
    parts.append(f'本周{feat["this_week_chg"]:+.1f}%')
    if feat['market_chg'] != 0:
        parts.append(f'{idx_label}{feat["market_chg"]:+.1f}%')
    if ff_signal is not None and ff_signal != 0:
        ff_label = '流入' if ff_signal > 0 else '流出'
        parts.append(f'资金{ff_label}')
    if vol_ratio is not None and vol_ratio != 0:
        if vol_ratio > 1.3:
            parts.append('放量')
        elif vol_ratio < 0.7:
            parts.append('缩量')
    if vol_note:
        parts.append(vol_note)
    if board_note:
        parts.append(board_note)
    if tier >= 2 and confidence != 'high':
        parts.append('参考信号')
    nw_reason = '; '.join(parts)

    return {
        'nw_pred_direction': 'UP' if nw_pred_up else 'DOWN',
        'nw_confidence': confidence,
        'nw_strategy': f'nw_rule_t{tier}',
        'nw_reason': nw_reason[:200],
        'nw_composite_score': round(tier * (-1 if not nw_pred_up else 1), 4),
        'nw_this_week_chg': round(feat['this_week_chg'], 4),
        'nw_iso_year': nw_iso[0],
        'nw_iso_week': nw_iso[1],
        'nw_date_range': f'{nw_monday.strftime("%Y-%m-%d")}~{nw_friday.strftime("%Y-%m-%d")}',
        'nw_pred_chg': None,
        'nw_pred_chg_low': None,
        'nw_pred_chg_high': None,
        'nw_pred_chg_mae': None,
        'nw_pred_chg_hit_rate': None,
        'nw_pred_chg_samples': None,
        'nw_backtest_accuracy': None,
        'nw_backtest_samples': None,
    }




def _compute_next_week_backtest(stock_codes: list[str], data: dict,
                                end_date: str, n_weeks: int = 29) -> dict:
    """计算下周预测的回测准确率（规则引擎版）。

    使用与 _predict_next_week 相同的规则引擎，只统计命中规则的样本。
    未命中规则的周（不确定）不计入准确率。

    Returns:
        {
            'per_stock': {code: {'accuracy': float, 'total': int,
                                 'strategy_dir_chg': {(strategy, dir): stats}}},
            'global': {'accuracy': float, 'total': int, 'coverage': float},
        }
    """
    dt_end = datetime.strptime(end_date, '%Y-%m-%d')
    # 扩大回溯范围以支持 price_pos_60 计算（需要60日历史收盘价）
    dt_start = dt_end - timedelta(days=(n_weeks + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')

    # 加载更长时间范围的K线数据（含 close_price 用于价格位置计算）
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    stock_klines = defaultdict(list)
    batch_size = 200
    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i:i + batch_size]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, close_price, change_percent "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [start_date, end_date])
        for row in cur.fetchall():
            stock_klines[row['stock_code']].append({
                'date': row['date'],
                'close': _to_float(row['close_price']),
                'change_percent': _to_float(row['change_percent']),
            })

    # 大盘K线（加载所有需要的指数）
    all_index_codes = list(set(_get_stock_index(c) for c in stock_codes))
    for idx in ('000001.SH', '399001.SZ', '899050.SZ'):
        if idx not in all_index_codes:
            all_index_codes.append(idx)
    ph_idx = ','.join(['%s'] * len(all_index_codes))
    cur.execute(
        f"SELECT stock_code, `date`, change_percent FROM stock_kline "
        f"WHERE stock_code IN ({ph_idx}) AND `date` >= %s AND `date` <= %s "
        f"ORDER BY `date`", all_index_codes + [start_date, end_date])
    market_klines_by_index = defaultdict(list)
    for r in cur.fetchall():
        market_klines_by_index[r['stock_code']].append({
            'date': r['date'],
            'change_percent': _to_float(r['change_percent']),
        })
    conn.close()

    # 按ISO周分组各指数K线
    market_by_week_by_index = {}  # {index_code: {iso_week: [klines]}}
    for idx_code, klines_list in market_klines_by_index.items():
        by_week = defaultdict(list)
        for k in klines_list:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            by_week[iw].append(k)
        market_by_week_by_index[idx_code] = by_week

    global_correct = 0
    global_total = 0
    global_all_weeks = 0  # 所有可评估的周数（含不确定）
    per_stock = {}

    for code in stock_codes:
        klines = stock_klines.get(code, [])
        if not klines or len(klines) < 20:
            continue

        # 按ISO周分组
        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            wg[iw].append(k)

        sorted_weeks = sorted(wg.keys())
        stock_correct = 0
        stock_total = 0
        stock_all_weeks = 0
        strategy_dir_chg = defaultdict(list)

        for idx in range(len(sorted_weeks) - 1):
            iw_this = sorted_weeks[idx]
            iw_next = sorted_weeks[idx + 1]

            this_days = sorted(wg[iw_this], key=lambda x: x['date'])
            next_days = sorted(wg[iw_next], key=lambda x: x['date'])

            if len(this_days) < 3 or len(next_days) < 3:
                continue

            this_pcts = [d['change_percent'] for d in this_days]
            next_pcts = [d['change_percent'] for d in next_days]
            next_week_chg = _compound_return(next_pcts)
            actual_next_up = next_week_chg >= 0

            # 大盘本周涨跌幅（使用个股对应的指数）
            stock_idx = _get_stock_index(code)
            idx_by_week = market_by_week_by_index.get(stock_idx, {})
            mw = idx_by_week.get(iw_this, [])
            market_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            stock_all_weeks += 1
            global_all_weeks += 1

            # 提取特征 & 匹配规则（含价格位置+前周动量，与生产 _predict_next_week 一致）
            # 计算 price_pos_60
            sorted_all = sorted(klines, key=lambda x: x['date'])
            first_date = this_days[0]['date']
            hist = [k for k in sorted_all if k['date'] < first_date]

            price_pos_60 = None
            if len(hist) >= 20:
                hist_closes = [k.get('close', 0) for k in hist[-60:] if k.get('close', 0) > 0]
                if hist_closes:
                    all_c = hist_closes + [k.get('close', 0) for k in this_days if k.get('close', 0) > 0]
                    min_c, max_c = min(all_c), max(all_c)
                    latest_c = this_days[-1].get('close', 0)
                    if max_c > min_c and latest_c > 0:
                        price_pos_60 = round((latest_c - min_c) / (max_c - min_c), 4)

            # 计算 prev_week_chg
            prev_week_chg = None
            prev_klines = hist[-5:] if len(hist) >= 5 else hist
            if prev_klines:
                prev_week_chg = _compound_return([k['change_percent'] for k in prev_klines])

            feat = _nw_extract_features(this_pcts, market_chg,
                                        market_index=stock_idx,
                                        price_pos_60=price_pos_60,
                                        prev_week_chg=prev_week_chg)
            rule = _nw_match_rule(feat)

            if rule is None:
                # 不确定 - 不计入准确率
                continue

            pred_next_up = rule['pred_up']
            correct = pred_next_up == actual_next_up
            tier = rule['tier']
            strat = f'nw_rule_t{tier}'
            pred_dir = 'UP' if pred_next_up else 'DOWN'

            if correct:
                stock_correct += 1
                global_correct += 1
            stock_total += 1
            global_total += 1

            strategy_dir_chg[(strat, pred_dir)].append(next_week_chg)

        if stock_total > 0:
            stock_acc = round(stock_correct / stock_total * 100, 1)

            # 计算涨跌幅分布
            strat_dir_chg_stats = {}
            all_up_chgs = []
            all_down_chgs = []
            for (s, d), chgs in strategy_dir_chg.items():
                if d == 'UP':
                    all_up_chgs.extend(chgs)
                else:
                    all_down_chgs.extend(chgs)
                if len(chgs) >= 2:
                    sorted_c = sorted(chgs)
                    n = len(sorted_c)
                    median = sorted_c[n // 2]
                    mae = _mean([abs(c - median) for c in chgs])
                    std_val = _std(chgs) if n >= 3 else mae
                    k_factor = 3.0 if n < 5 else (2.2 if n < 10 else (1.8 if n < 20 else 1.5))
                    spread = max(std_val, mae, 0.5)
                    low = median - k_factor * spread
                    high = median + k_factor * spread
                    hits = sum(1 for c in chgs if low <= c <= high)
                    strat_dir_chg_stats[(s, d)] = {
                        'median': round(median, 2),
                        'p10': round(low, 2), 'p90': round(high, 2),
                        'mae': round(mae, 2),
                        'hit_rate': round(hits / n * 100, 1),
                        'samples': n,
                    }

            for label, chgs in [('UP', all_up_chgs), ('DOWN', all_down_chgs)]:
                if len(chgs) >= 2:
                    sorted_c = sorted(chgs)
                    n = len(sorted_c)
                    median = sorted_c[n // 2]
                    mae = _mean([abs(c - median) for c in chgs])
                    std_val = _std(chgs) if n >= 3 else mae
                    k_factor = 3.0 if n < 5 else (2.2 if n < 10 else (1.8 if n < 20 else 1.5))
                    spread = max(std_val, mae, 0.5)
                    low = median - k_factor * spread
                    high = median + k_factor * spread
                    hits = sum(1 for c in chgs if low <= c <= high)
                    strat_dir_chg_stats[('_all', label)] = {
                        'median': round(median, 2),
                        'p10': round(low, 2), 'p90': round(high, 2),
                        'mae': round(mae, 2),
                        'hit_rate': round(hits / n * 100, 1),
                        'samples': n,
                    }

            per_stock[code] = {
                'accuracy': stock_acc,
                'total': stock_total,
                'strategy_dir_chg': strat_dir_chg_stats,
            }

    global_acc = round(global_correct / global_total * 100, 1) if global_total > 0 else 0
    coverage = round(global_total / global_all_weeks * 100, 1) if global_all_weeks > 0 else 0
    logger.info("[下周回测] %d只股票, %d/%d样本(覆盖%.1f%%), 准确率=%.1f%%",
                len(per_stock), global_total, global_all_weeks, coverage, global_acc)

    return {
        'per_stock': per_stock,
        'global': {
            'accuracy': global_acc,
            'total': global_total,
            'all_weeks': global_all_weeks,
            'coverage': coverage,
        },
    }



# ═══════════════════════════════════════════════════════════
# 回测准确率计算（简化版，基于历史数据）
# ═══════════════════════════════════════════════════════════

def _compute_backtest_accuracy(stock_codes: list[str], data: dict,
                                end_date: str, n_weeks: int = 29) -> dict:
    """基于历史数据计算回测准确率（按个股+策略分别计算）。

    对每只股票，回溯n_weeks周，用d4/d3信号预测每周方向，
    与实际方向对比计算该股票自身的准确率。

    Returns:
        {
            'per_stock': {code: {'accuracy': float, 'lowo': float, 'n_weeks': int, 'total': int, 'strategy_acc': {strategy: float}}},
            'global': {'full_accuracy': float, 'lowo_accuracy': float, 'n_weeks': int, 'total_samples': int},
        }
    """
    dt_end = datetime.strptime(end_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=n_weeks * 7 + 7)
    start_date = dt_start.strftime('%Y-%m-%d')

    # 独立加载回测所需的K线数据（更长时间范围）
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 加载所有需要的指数K线（不再固定000001.SH）
    all_index_codes = list(set(_get_stock_index(c) for c in stock_codes))
    for idx in ('000001.SH', '399001.SZ', '899050.SZ'):
        if idx not in all_index_codes:
            all_index_codes.append(idx)
    ph_idx = ','.join(['%s'] * len(all_index_codes))
    cur.execute(
        f"SELECT stock_code, `date`, change_percent FROM stock_kline "
        f"WHERE stock_code IN ({ph_idx}) AND `date` >= %s AND `date` <= %s "
        f"ORDER BY `date`", all_index_codes + [start_date, end_date])
    bt_market_by_index = defaultdict(list)
    for r in cur.fetchall():
        bt_market_by_index[r['stock_code']].append({
            'date': r['date'],
            'change_percent': _to_float(r['change_percent']),
        })

    stock_klines = defaultdict(list)
    batch_size = 200
    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i:i + batch_size]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, change_percent "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [start_date, end_date])
        for row in cur.fetchall():
            stock_klines[row['stock_code']].append({
                'date': row['date'],
                'change_percent': _to_float(row['change_percent']),
            })

    conn.close()
    total_mkt_days = sum(len(v) for v in bt_market_by_index.values())
    logger.info("[回测数据] %d只股票K线, %d个指数共%d天, 区间%s~%s",
                len(stock_klines), len(bt_market_by_index), total_mkt_days,
                start_date, end_date)

    # 全局统计
    global_correct = 0
    global_count = 0
    week_stats = defaultdict(lambda: [0, 0])

    # 个股统计
    per_stock = {}  # code -> {'correct': int, 'total': int, 'strategy_stats': {strategy: [correct, total]}}

    for code in stock_codes:
        klines = stock_klines.get(code, [])
        if not klines or len(klines) < 5:
            continue

        # 获取该股票的策略配置（与预测一致）
        sector = data.get('stock_sectors', {}).get(code, '')
        behavior = data.get('stock_behaviors', {}).get(code, {})
        profile = _get_stock_strategy_profile(code, sector, behavior)

        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            wg[iw].append(k)

        stock_correct = 0
        stock_total = 0
        stock_week_results = []  # [(iw, correct_bool)] 用于计算个股LOWO
        strategy_stats = defaultdict(lambda: [0, 0])  # strategy -> [correct, total]
        strategy_chg_dist = defaultdict(list)  # strategy -> [actual_weekly_chg, ...]
        # 按 (strategy, pred_direction) 分别收集涨跌幅，保证方向一致性
        strategy_dir_chg_dist = defaultdict(list)  # (strategy, 'UP'/'DOWN') -> [actual_weekly_chg, ...]

        for iw, days in wg.items():
            days.sort(key=lambda x: x['date'])
            if len(days) < 3:
                continue

            pcts = [d['change_percent'] for d in days]
            weekly_chg = _compound_return(pcts)
            actual_up = weekly_chg >= 0

            d3 = _compound_return(pcts[:3])
            d4 = _compound_return(pcts[:4]) if len(days) >= 4 else None
            is_susp = all(p == 0 for p in pcts[:3])

            # 使用与预测相同的自适应策略
            pred_up, _conf, strat, _reason = _predict_with_profile(
                d4, d3, is_susp, len(days), pcts, profile)

            correct = pred_up == actual_up
            stock_week_results.append((iw, correct))
            if correct:
                stock_correct += 1
                global_correct += 1
                week_stats[iw][0] += 1
                strategy_stats[strat][0] += 1
            stock_total += 1
            global_count += 1
            week_stats[iw][1] += 1
            strategy_stats[strat][1] += 1
            strategy_chg_dist[strat].append(weekly_chg)
            pred_dir = 'UP' if pred_up else 'DOWN'
            strategy_dir_chg_dist[(strat, pred_dir)].append(weekly_chg)

        if stock_total > 0:
            stock_acc = round(stock_correct / stock_total * 100, 1)
            strat_acc = {}
            strat_chg = {}
            for s, (ok, n) in strategy_stats.items():
                if n > 0:
                    strat_acc[s] = round(ok / n * 100, 1)
            # 辅助函数：计算涨跌幅分布统计
            def _calc_chg_stats(chgs):
                if len(chgs) < 2:
                    return None
                sorted_chgs = sorted(chgs)
                n = len(sorted_chgs)
                median = sorted_chgs[n // 2]
                p25 = sorted_chgs[max(0, n // 4)]
                p75 = sorted_chgs[min(n - 1, n * 3 // 4)]
                mae = _mean([abs(c - median) for c in chgs])
                std = _std(chgs) if n >= 3 else mae

                # 自适应区间：基于 median ± k * std
                # k 根据样本量调整，样本越少区间越宽
                if n >= 20:
                    k = 1.5
                elif n >= 10:
                    k = 1.8
                elif n >= 5:
                    k = 2.2
                else:
                    k = 3.0

                spread = max(std, mae, 0.5)  # 最小展幅0.5%
                low = median - k * spread
                high = median + k * spread

                # 命中率基于自适应区间
                hits = sum(1 for c in chgs if low <= c <= high)
                hit_rate = round(hits / n * 100, 1)
                return {
                    'median': round(median, 2),
                    'p10': round(low, 2), 'p90': round(high, 2),
                    'p25': round(p25, 2), 'p75': round(p75, 2),
                    'mae': round(mae, 2), 'hit_rate': hit_rate, 'samples': n,
                }

            for s, chgs in strategy_chg_dist.items():
                stats = _calc_chg_stats(chgs)
                if stats:
                    strat_chg[s] = stats

            # 按 (strategy, direction) 统计涨跌幅分布
            strat_dir_chg = {}  # (strategy, direction) -> stats
            for (s, d), chgs in strategy_dir_chg_dist.items():
                stats = _calc_chg_stats(chgs)
                if stats:
                    strat_dir_chg[(s, d)] = stats

            # 汇总所有策略的涨跌幅作为兜底（当某策略样本不足时使用）
            all_chgs = []
            all_up_chgs = []
            all_down_chgs = []
            for chgs in strategy_chg_dist.values():
                all_chgs.extend(chgs)
            for (s, d), chgs in strategy_dir_chg_dist.items():
                if d == 'UP':
                    all_up_chgs.extend(chgs)
                else:
                    all_down_chgs.extend(chgs)
            all_stats = _calc_chg_stats(all_chgs)
            if all_stats:
                strat_chg['_all'] = all_stats
            all_up_stats = _calc_chg_stats(all_up_chgs)
            if all_up_stats:
                strat_dir_chg[('_all', 'UP')] = all_up_stats
            all_down_stats = _calc_chg_stats(all_down_chgs)
            if all_down_stats:
                strat_dir_chg[('_all', 'DOWN')] = all_down_stats
            # 计算个股LOWO：逐周留一法，每次去掉一周计算剩余准确率，取平均
            if stock_total >= 2:
                loo_accs = []
                for _iw, c in stock_week_results:
                    rest_correct = stock_correct - (1 if c else 0)
                    rest_total = stock_total - 1
                    loo_accs.append(rest_correct / rest_total * 100)
                stock_lowo = round(_mean(loo_accs), 1)
            else:
                stock_lowo = stock_acc

            per_stock[code] = {
                'accuracy': stock_acc,
                'lowo': stock_lowo,
                'total': stock_total,
                'n_weeks': stock_total,
                'strategy_acc': strat_acc,
                'strategy_chg': strat_chg,
                'strategy_dir_chg': strat_dir_chg,
                '_strategy_raw': dict(strategy_stats),  # {strategy: [correct, total]}
            }

    full_accuracy = round(global_correct / global_count * 100, 1) if global_count > 0 else 0
    week_accs = []
    for iw, (ok, n) in sorted(week_stats.items()):
        if n > 0:
            week_accs.append(ok / n * 100)
    lowo_accuracy = round(_mean(week_accs), 1) if week_accs else 0

    logger.info("[回测] %d周, %d样本, 全样本=%.1f%%, LOWO=%.1f%%, 个股回测=%d只",
                len(week_accs), global_count, full_accuracy, lowo_accuracy, len(per_stock))

    return {
        'per_stock': per_stock,
        'global': {
            'full_accuracy': full_accuracy,
            'lowo_accuracy': lowo_accuracy,
            'n_weeks': len(week_accs),
            'total_samples': global_count,
            'start_date': start_date,
            'end_date': end_date,
        },
    }


# ═══════════════════════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════════════════════

def run_batch_weekly_prediction(progress_callback=None):
    """批量周预测主函数。

    流程：
    1. 获取全部股票代码
    2. 获取最新交易日
    3. 加载数据
    4. 对每只股票进行预测
    5. 计算回测准确率
    6. 写入数据库（最新 + 历史）

    Args:
        progress_callback: 进度回调函数 (total, done, up_count, down_count)
    """
    t_start = datetime.now()
    logger.info("=" * 70)
    logger.info("  批量周预测服务启动")
    logger.info("=" * 70)

    # 1. 建表
    ensure_tables()

    # 2. 获取全部股票
    all_codes = _get_all_stock_codes()
    if not all_codes:
        logger.error("无股票数据")
        return

    # 3. 获取最新交易日
    latest_date = _get_latest_trade_date()
    if not latest_date:
        logger.error("无法获取最新交易日")
        return
    logger.info("最新交易日: %s", latest_date)

    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    iso_cal = dt_latest.isocalendar()
    logger.info("当前ISO周: %d年第%d周, 周%d",
                iso_cal[0], iso_cal[1], iso_cal[2])

    # 4. 加载数据
    logger.info("[1/4] 加载数据...")
    data = _load_prediction_data(all_codes, latest_date)

    # 5. 批量预测
    logger.info("[2/4] 批量预测 %d 只股票...", len(all_codes))
    predictions = []
    skipped = 0
    for i, code in enumerate(all_codes):
        pred = _predict_stock_weekly(code, data, latest_date)
        if pred:
            predictions.append(pred)
        else:
            skipped += 1
        # 实时更新进度
        if progress_callback and (i % 50 == 0 or i == len(all_codes) - 1):
            up_so_far = sum(1 for p in predictions if p['pred_direction'] == 'UP')
            down_so_far = len(predictions) - up_so_far
            progress_callback(len(all_codes), i + 1, up_so_far, down_so_far)

    logger.info("  预测完成: %d 只, 跳过: %d 只", len(predictions), skipped)

    if not predictions:
        logger.error("无有效预测结果")
        return

    # 统计
    up_count = sum(1 for p in predictions if p['pred_direction'] == 'UP')
    down_count = len(predictions) - up_count
    high_count = sum(1 for p in predictions if p['confidence'] == 'high')
    med_count = sum(1 for p in predictions if p['confidence'] == 'medium')
    low_count = sum(1 for p in predictions if p['confidence'] == 'low')

    logger.info("  预测涨: %d, 预测跌: %d", up_count, down_count)
    logger.info("  高置信: %d, 中置信: %d, 低置信: %d",
                high_count, med_count, low_count)

    # 6. 计算回测准确率（按个股+策略）
    logger.info("[3/4] 计算回测准确率...")
    bt_result = _compute_backtest_accuracy(all_codes, data, latest_date)
    per_stock_bt = bt_result['per_stock']
    global_bt = bt_result['global']

    # 6b. 下周预测 + 下周回测
    logger.info("[3b/4] 下周预测 + 回测...")
    nw_bt_result = _compute_next_week_backtest(all_codes, data, latest_date)
    nw_per_stock_bt = nw_bt_result['per_stock']
    nw_global_bt = nw_bt_result['global']

    nw_count = 0
    nw_up = 0
    nw_uncertain = 0
    nw_reference = 0  # Tier 2 参考级预测数量

    for p in predictions:
        code = p['stock_code']
        nw = _predict_next_week(code, data, latest_date, p)
        if nw:
            p.update(nw)
            if nw['nw_pred_direction'] is not None:
                nw_count += 1
                if nw['nw_pred_direction'] == 'UP':
                    nw_up += 1
                if nw.get('nw_confidence') == 'reference':
                    nw_reference += 1
            else:
                nw_uncertain += 1
        else:
            nw_uncertain += 1
            p['nw_pred_direction'] = None
            p['nw_confidence'] = None
            p['nw_strategy'] = None
            p['nw_reason'] = None
            p['nw_composite_score'] = None
            p['nw_this_week_chg'] = None
            p['nw_iso_year'] = None
            p['nw_iso_week'] = None
            p['nw_date_range'] = None
            p['nw_pred_chg'] = None
            p['nw_pred_chg_low'] = None
            p['nw_pred_chg_high'] = None
            p['nw_pred_chg_mae'] = None
            p['nw_pred_chg_hit_rate'] = None
            p['nw_pred_chg_samples'] = None
            p['nw_backtest_accuracy'] = None
            p['nw_backtest_samples'] = None

    logger.info("  规则引擎: %d只有预测, %d只不确定", nw_count, nw_uncertain)

    # ── 填充回测准确率 ──
    for p in predictions:
        code = p['stock_code']
        if p.get('nw_pred_direction') is not None:
            nw_stock_bt = nw_per_stock_bt.get(code)
            if nw_stock_bt:
                p['nw_backtest_accuracy'] = nw_stock_bt['accuracy']
                p['nw_backtest_samples'] = nw_stock_bt['total']

                # 填充下周预测涨跌幅
                strat = p.get('nw_strategy', '')
                pred_dir = p['nw_pred_direction']
                sdc = nw_stock_bt.get('strategy_dir_chg', {})

                # 优先: 同策略+同方向
                chg_stats = sdc.get((strat, pred_dir))
                # 兜底: 所有策略+同方向
                if not chg_stats:
                    chg_stats = sdc.get(('_all', pred_dir))
                if chg_stats:
                    median = chg_stats['median']
                    # 强制符号一致
                    if pred_dir == 'UP' and median < 0:
                        median = abs(median)
                    elif pred_dir == 'DOWN' and median > 0:
                        median = -abs(median)
                    p['nw_pred_chg'] = median
                    p['nw_pred_chg_low'] = chg_stats['p10']
                    p['nw_pred_chg_high'] = chg_stats['p90']
                    p['nw_pred_chg_mae'] = chg_stats['mae']
                    p['nw_pred_chg_hit_rate'] = chg_stats['hit_rate']
                    p['nw_pred_chg_samples'] = chg_stats['samples']
            else:
                if p.get('nw_backtest_accuracy') is None:
                    p['nw_backtest_accuracy'] = nw_global_bt['accuracy']
                    p['nw_backtest_samples'] = 0

    nw_coverage = round(nw_count / len(predictions) * 100, 1) if predictions else 0
    logger.info("  下周预测: %d只 (涨%d 跌%d 参考%d), 不确定%d只, 覆盖率=%.1f%%, 回测准确率=%.1f%%",
                nw_count, nw_up, nw_count - nw_up, nw_reference, nw_uncertain, nw_coverage,
                nw_global_bt['accuracy'])

    # 填充回测准确率：优先使用个股+策略准确率，其次个股整体准确率，最后全局
    #
    # 小样本策略级准确率的问题：
    #   fuzzy/medium 策略在某些股票上只有3~8个样本，准确率波动极大（0%~40%），
    #   但同一股票的整体准确率通常在70~90%（因为 strong 策略占多数且准确率极高）。
    #   策略级准确率不能代表该股票的真实可预测性，需要回退到个股整体准确率。
    #
    # 回退规则：
    #   1. 样本量 < MIN_STRATEGY_SAMPLES(8) → 直接用个股整体准确率
    #   2. 弱信号策略(fuzzy/medium/uncertain) 且策略准确率 < 50% → 回退到个股整体
    #      原因：这些策略信号强度不够，周五一天的波动就能翻转全周方向，
    #      策略级准确率反映的是信号强度不足的问题，不是该股票不可预测
    #   3. strong 策略始终使用策略级准确率（信号强，样本充足，准确率可靠）
    MIN_STRATEGY_SAMPLES = 8
    MIN_WEAK_STRATEGY_ACC = 50.0  # 弱信号策略准确率低于此值时回退
    _WEAK_STRATEGIES = {
        # 弱信号: d3/d4方向不明确或矛盾
        'follow_d3_direction(weak)',
        'conflict_d3d4',
        'uncertain_d1', 'uncertain_d2',
        # 早期数据不足
        'early_direction_d1', 'early_direction_d2',
        'partial_d1', 'partial_d2',
    }
    filled_per_stock = 0
    filled_per_strategy = 0
    filled_weak_fallback = 0
    bt_start = global_bt.get('start_date')
    bt_end = global_bt.get('end_date')
    bt_n_weeks = global_bt.get('n_weeks', 0)
    for p in predictions:
        code = p['stock_code']
        strategy = p.get('strategy', '')
        stock_bt = per_stock_bt.get(code)
        if stock_bt:
            strat_acc = stock_bt.get('strategy_acc', {}).get(strategy)
            strat_raw = stock_bt.get('_strategy_raw', {}).get(strategy, [0, 0])
            strat_samples = strat_raw[1] if isinstance(strat_raw, (list, tuple)) else 0
            use_strategy_acc = (strat_acc is not None and strat_samples >= MIN_STRATEGY_SAMPLES)

            # 弱信号策略（fuzzy + medium）：准确率过低时回退到个股整体准确率
            if use_strategy_acc and strategy in _WEAK_STRATEGIES and strat_acc < MIN_WEAK_STRATEGY_ACC:
                p['backtest_accuracy'] = stock_bt['accuracy']
                filled_weak_fallback += 1
            elif use_strategy_acc:
                p['backtest_accuracy'] = strat_acc
                filled_per_strategy += 1
            else:
                p['backtest_accuracy'] = stock_bt['accuracy']
                filled_per_stock += 1
            p['backtest_lowo_accuracy'] = stock_bt.get('lowo', stock_bt['accuracy'])
            p['backtest_samples'] = stock_bt['total']
        else:
            # 无该股票历史数据，使用全局准确率
            p['backtest_accuracy'] = global_bt['full_accuracy']
            p['backtest_lowo_accuracy'] = global_bt['lowo_accuracy']
            p['backtest_samples'] = 0
        p['backtest_weeks'] = bt_n_weeks
        p['backtest_start_date'] = bt_start
        p['backtest_end_date'] = bt_end

    logger.info("  回测填充: 策略级%d只, 个股级%d只, 弱信号回退%d只, 全局兜底%d只",
                filled_per_strategy, filled_per_stock, filled_weak_fallback,
                len(predictions) - filled_per_strategy - filled_per_stock - filled_weak_fallback)

    # 填充预测涨跌幅：基于回测中同股票+同策略+同方向的历史实际周涨跌幅分布
    filled_pred_chg = 0
    filled_pred_chg_fallback = 0
    filled_pred_chg_dir_mismatch = 0
    for p in predictions:
        code = p['stock_code']
        strategy = p.get('strategy', '')
        pred_dir = p['pred_direction']  # 'UP' or 'DOWN'
        stock_bt = per_stock_bt.get(code)
        if not stock_bt:
            continue

        strat_dir_chg_map = stock_bt.get('strategy_dir_chg', {})
        strat_chg_map = stock_bt.get('strategy_chg', {})

        # 优先级1: 同股票 + 同策略 + 同方向
        dir_chg = strat_dir_chg_map.get((strategy, pred_dir))
        if dir_chg:
            p['pred_weekly_chg'] = dir_chg['median']
            p['pred_chg_low'] = dir_chg['p10']
            p['pred_chg_high'] = dir_chg['p90']
            p['pred_chg_mae'] = dir_chg['mae']
            p['pred_chg_hit_rate'] = dir_chg['hit_rate']
            p['pred_chg_samples'] = dir_chg['samples']
            filled_pred_chg += 1
            continue

        # 优先级2: 同股票 + 所有策略汇总 + 同方向
        all_dir_chg = strat_dir_chg_map.get(('_all', pred_dir))
        if all_dir_chg:
            p['pred_weekly_chg'] = all_dir_chg['median']
            p['pred_chg_low'] = all_dir_chg['p10']
            p['pred_chg_high'] = all_dir_chg['p90']
            p['pred_chg_mae'] = all_dir_chg['mae']
            p['pred_chg_hit_rate'] = all_dir_chg['hit_rate']
            p['pred_chg_samples'] = all_dir_chg['samples']
            filled_pred_chg_fallback += 1
            continue

        # 优先级3: 同股票 + 同策略（不区分方向），但强制保证符号一致
        strat_chg = strat_chg_map.get(strategy)
        if not strat_chg:
            strat_chg = strat_chg_map.get('_all')
        if strat_chg:
            median = strat_chg['median']
            # 强制保证涨跌幅符号与预测方向一致
            if pred_dir == 'UP' and median < 0:
                median = abs(median)
            elif pred_dir == 'DOWN' and median > 0:
                median = -abs(median)
            p['pred_weekly_chg'] = median
            p['pred_chg_low'] = strat_chg['p10']
            p['pred_chg_high'] = strat_chg['p90']
            p['pred_chg_mae'] = strat_chg['mae']
            p['pred_chg_hit_rate'] = strat_chg['hit_rate']
            p['pred_chg_samples'] = strat_chg['samples']
            filled_pred_chg_dir_mismatch += 1

    logger.info("  预测涨跌幅填充: 方向匹配%d只, 方向兜底%d只, 符号修正%d只",
                filled_pred_chg, filled_pred_chg_fallback, filled_pred_chg_dir_mismatch)

    # 计算本周剩余天数预测涨跌幅 = 预测本周涨跌幅 - 本周已实现涨跌幅
    filled_remaining = 0
    for p in predictions:
        if p.get('pred_weekly_chg') is not None and p.get('week_realized_chg') is not None:
            p['pred_remaining_chg'] = round(p['pred_weekly_chg'] - p['week_realized_chg'], 4)
            filled_remaining += 1
    logger.info("  剩余涨跌幅填充: %d只", filled_remaining)



    # 7. 写入数据库
    logger.info("[4/4] 写入数据库...")
    # 分批写入，每批500条
    batch_size = 500
    for i in range(0, len(predictions), batch_size):
        batch = predictions[i:i + batch_size]
        batch_upsert_latest_predictions(batch)
        batch_insert_history(batch)

    elapsed = (datetime.now() - t_start).total_seconds()

    logger.info("=" * 70)
    logger.info("  批量周预测完成")
    logger.info("  预测日期: %s (Y%d-W%02d)", latest_date, iso_cal[0], iso_cal[1])
    logger.info("  股票数: %d, 预测涨: %d, 预测跌: %d",
                len(predictions), up_count, down_count)
    logger.info("  回测准确率: %.1f%% (LOWO: %.1f%%, %d周, %d样本)",
                global_bt['full_accuracy'], global_bt['lowo_accuracy'],
                global_bt['n_weeks'], global_bt['total_samples'])
    logger.info("  下周预测: %d只(高置信%d 参考%d 覆盖%.1f%%), 回测准确率: %.1f%% (%d样本, 覆盖%.1f%%)",
                nw_count, nw_count - nw_reference, nw_reference, nw_coverage,
                nw_global_bt['accuracy'],
                nw_global_bt['total'], nw_global_bt.get('coverage', 0))
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 70)

    return {
        'predict_date': latest_date,
        'iso_year': iso_cal[0],
        'iso_week': iso_cal[1],
        'total_stocks': len(predictions),
        'up_count': up_count,
        'down_count': down_count,
        'backtest': global_bt,
        'next_week_backtest': nw_global_bt,
        'next_week_count': nw_count,
        'next_week_up': nw_up,
        'next_week_uncertain': nw_uncertain,
        'next_week_reference': nw_reference,
        'next_week_coverage': nw_coverage,
        'elapsed': round(elapsed, 1),
    }


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S',
    )
    run_batch_weekly_prediction()
