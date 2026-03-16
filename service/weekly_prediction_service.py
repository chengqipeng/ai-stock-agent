#!/usr/bin/env python3
"""
批量周预测服务 — 基于v4回测引擎的实盘预测
==========================================
功能：
1. 获取全部A股股票，基于最新交易日数据进行本周方向预测
2. 预测结果写入 stock_weekly_prediction（最新一条，UPSERT）
3. 同时写入 stock_weekly_prediction_history（历史记录）
4. 附带29周回测准确率作为参考

预测逻辑（与v4回测一致）：
- 获取本周已有的交易日K线数据
- 计算d3(前3天)/d4(前4天)复合涨跌幅
- 停牌股(前3天全0) → 预测涨(99.5%准确)
- d4可用时: |d4|>2% → 强信号(95%), 0.8-2% → 中等(80%), <0.8% → 模糊(63%)
- d4不可用时: 回退到d3信号

用法：
    python -m service.weekly_prediction_service
"""
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
    """使用策略配置进行预测。

    Returns:
        (pred_up: bool, confidence: str, strategy: str, reason: str)
    """
    strong_th = profile['strong_threshold']
    fuzzy_th = profile['fuzzy_threshold']
    fuzzy_mode = profile['fuzzy_mode']

    if is_suspended:
        return True, 'high', 'suspended_up', '停牌:前3天全0'

    if d4_chg is not None:
        if abs(d4_chg) > strong_th:
            return (d4_chg >= 0), 'high', 'follow_d4(strong)', f'd4强信号:{d4_chg:+.2f}%'
        elif abs(d4_chg) > fuzzy_th:
            return (d4_chg >= 0), 'medium', 'follow_d4(medium)', f'd4中等:{d4_chg:+.2f}%'
        else:
            # fuzzy区间：根据策略配置决定方向
            if fuzzy_mode == 'reverse':
                pred_up = d4_chg < 0  # 反向
                return pred_up, 'low', 'reverse_d4(fuzzy)', f'd4模糊反转:{d4_chg:+.2f}%'
            elif fuzzy_mode == 'skip':
                # 不确定时仍给预测，但标记为极低置信
                return (d4_chg >= 0), 'low', 'uncertain_d4(fuzzy)', f'd4不确定:{d4_chg:+.2f}%'
            else:
                return (d4_chg >= 0), 'low', 'follow_d4(fuzzy)', f'd4模糊:{d4_chg:+.2f}%'

    if d3_chg is not None:
        if abs(d3_chg) > strong_th:
            return (d3_chg > 0), 'high', 'follow_d3(strong)', f'd3强信号:{d3_chg:+.2f}%'
        elif abs(d3_chg) > fuzzy_th:
            return (d3_chg > 0), 'medium', 'follow_d3(medium)', f'd3中等:{d3_chg:+.2f}%'
        else:
            if fuzzy_mode == 'reverse':
                pred_up = d3_chg < 0
                return pred_up, 'low', 'reverse_d3(fuzzy)', f'd3模糊反转:{d3_chg:+.2f}%'
            elif fuzzy_mode == 'skip':
                return (d3_chg >= 0), 'low', 'uncertain_d3(fuzzy)', f'd3不确定:{d3_chg:+.2f}%'
            else:
                return (d3_chg >= 0), 'low', 'follow_d3(fuzzy)', f'd3模糊:{d3_chg:+.2f}%'

    # 数据不足
    cum = _compound_return(daily_pcts)
    return (cum >= 0), 'low', f'partial_d{n_days}', f'仅{n_days}天数据:{cum:+.2f}%'


# ═══════════════════════════════════════════════════════════
# 数据加载
# ═══════════════════════════════════════════════════════════

def _get_all_stock_codes() -> list[str]:
    """从stock_kline表获取全部有K线数据的股票代码。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute("SELECT DISTINCT stock_code FROM stock_kline")
        codes = [r['stock_code'] for r in cur.fetchall()]
        logger.info("全部股票: %d 只", len(codes))
        return sorted(codes)
    finally:
        cur.close()
        conn.close()


def _get_latest_trade_date() -> str:
    """获取stock_kline中最新的交易日期。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT MAX(`date`) as max_date FROM stock_kline "
            "WHERE stock_code = '000001.SH'"
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

    # 4. 大盘K线
    cur.execute(
        "SELECT `date`, close_price, change_percent FROM stock_kline "
        "WHERE stock_code = '000001.SH' AND `date` >= %s AND `date` <= %s "
        "ORDER BY `date`", (lookback_start, latest_date))
    market_klines = [{'date': r['date'],
                      'change_percent': _to_float(r['change_percent'])}
                     for r in cur.fetchall()]

    # 5. 股票名称（优先从概念板块成分股表获取，缺失的从本地常量补充）
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

    conn.close()

    logger.info("[数据加载] %d只股票K线, %d只有板块, %d板块K线, 大盘%d天",
                len(stock_klines), len(stock_boards),
                len(board_kline_map), len(market_klines))

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
        'stock_names': stock_names,
        'stock_sectors': stock_sectors,
        'stock_behaviors': stock_behaviors,
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

    # 大盘本周信号
    market_klines = data['market_klines']
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
    fund_flow_signal = None

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
    }


# ═══════════════════════════════════════════════════════════
# 下周预测（规则引擎 - 选择性高置信预测）
# ═══════════════════════════════════════════════════════════

# 规则集：按优先级排列，互斥匹配（命中第一条即停止）
# 基于 _nw_signal_deep_analysis2.py + _nw_rule_search3.py 的实证分析结果
# 策略核心：只在高置信条件下输出预测，其余标记为"不确定"
#
# 核心发现：跌反转+大盘配合是最强信号
# - 跌>2%+大盘跌>1%→涨: 71.5%准确率 (N=5341)
# - 跌>2%+大盘跌>0.5%+连跌>=2→涨: 73.9%准确率 (N=3356)
# - 跌>2%+大盘跌>0.5%+尾日跌>1%→涨: 77.0%准确率 (N=3545)
# 互斥组合后: ~70.9%准确率, ~10.5%覆盖率
_NW_RULES = [
    # ── 唯一规则: 大盘跌>1% + 个股跌>2% → 下周涨 ──
    # 独立准确率: 71.5% (N=5341/51717, 覆盖10.3%)
    # 核心逻辑: 大盘与个股同步下跌时，下周反弹概率高
    {
        'name': '跌>2%+大盘跌>1%→涨',
        'pred_up': True,
        'tier': 1,
        'check': lambda chg, mkt, cd, cu, ld: chg < -2 and mkt < -1,
    },
]


def _nw_extract_features(daily_pcts: list[float], market_chg: float) -> dict:
    """从日K线数据中提取下周预测所需的特征。"""
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

    return {
        'this_week_chg': this_week_chg,
        'market_chg': market_chg,
        'consec_down': consec_down,
        'consec_up': consec_up,
        'last_day_chg': last_day_chg,
    }


def _nw_match_rule(feat: dict) -> dict | None:
    """用规则引擎匹配下周预测。返回匹配的规则或 None（不确定）。"""
    chg = feat['this_week_chg']
    mkt = feat['market_chg']
    cd = feat['consec_down']
    cu = feat['consec_up']
    ld = feat['last_day_chg']

    for rule in _NW_RULES:
        if rule['check'](chg, mkt, cd, cu, ld):
            return rule
    return None



def _predict_next_week(code: str, data: dict, latest_date: str,
                       this_week_pred: dict) -> dict | None:
    """预测下周方向（规则引擎 - 选择性高置信预测）。

    只在高置信条件下输出预测方向，其余标记为 None（不确定）。
    基于 _nw_signal_deep_analysis2.py 的实证分析结果：
    - 跌反转信号远强于涨反转信号
    - 只预测高置信条件，覆盖率约20-40%，但准确率>=70%

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

    # 获取大盘本周涨跌幅
    market_klines = data['market_klines']
    market_week = []
    for k in market_klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        ical = dt.isocalendar()
        if ical[0] == iso_year and ical[1] == iso_week:
            market_week.append(k)
    market_chg = _compound_return(
        [k['change_percent'] for k in sorted(market_week, key=lambda x: x['date'])]
    ) if len(market_week) >= 3 else 0.0

    # 提取特征 & 匹配规则
    feat = _nw_extract_features(daily_pcts, market_chg)
    rule = _nw_match_rule(feat)

    # 下周日期范围
    nw_monday = _next_week_monday(dt_latest)
    nw_friday = nw_monday + timedelta(days=4)
    nw_iso = nw_monday.isocalendar()

    if rule is None:
        # 不确定 - 无高置信条件匹配
        return {
            'nw_pred_direction': None,
            'nw_confidence': None,
            'nw_strategy': None,
            'nw_reason': '无高置信条件匹配',
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
    confidence = 'high' if tier == 1 else ('medium' if tier == 2 else 'low')

    # 构建理由
    parts = [rule['name']]
    parts.append(f'本周{feat["this_week_chg"]:+.1f}%')
    if feat['market_chg'] != 0:
        parts.append(f'大盘{feat["market_chg"]:+.1f}%')
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
    dt_start = dt_end - timedelta(days=(n_weeks + 2) * 7)
    start_date = dt_start.strftime('%Y-%m-%d')

    # 加载更长时间范围的K线数据
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

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

    # 大盘K线
    cur.execute(
        "SELECT `date`, change_percent FROM stock_kline "
        "WHERE stock_code = '000001.SH' AND `date` >= %s AND `date` <= %s "
        "ORDER BY `date`", (start_date, end_date))
    market_klines = [{'date': r['date'],
                      'change_percent': _to_float(r['change_percent'])}
                     for r in cur.fetchall()]
    conn.close()

    # 按ISO周分组大盘
    market_by_week = defaultdict(list)
    for k in market_klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        iw = dt.isocalendar()[:2]
        market_by_week[iw].append(k)

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

            # 大盘本周涨跌幅
            mw = market_by_week.get(iw_this, [])
            market_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            stock_all_weeks += 1
            global_all_weeks += 1

            # 提取特征 & 匹配规则
            feat = _nw_extract_features(this_pcts, market_chg)
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

    cur.execute(
        "SELECT `date`, change_percent FROM stock_kline "
        "WHERE stock_code = '000001.SH' AND `date` >= %s AND `date` <= %s "
        "ORDER BY `date`", (start_date, end_date))
    market_klines = [{'date': r['date'],
                      'change_percent': _to_float(r['change_percent'])}
                     for r in cur.fetchall()]

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
    logger.info("[回测数据] %d只股票K线, 大盘%d天, 区间%s~%s",
                len(stock_klines), len(market_klines), start_date, end_date)

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
            per_stock[code] = {
                'accuracy': stock_acc,
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

def run_batch_weekly_prediction():
    """批量周预测主函数。

    流程：
    1. 获取全部股票代码
    2. 获取最新交易日
    3. 加载数据
    4. 对每只股票进行预测
    5. 计算回测准确率
    6. 写入数据库（最新 + 历史）
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
    for code in all_codes:
        pred = _predict_stock_weekly(code, data, latest_date)
        if pred:
            predictions.append(pred)
        else:
            skipped += 1

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
    for p in predictions:
        code = p['stock_code']
        nw = _predict_next_week(code, data, latest_date, p)
        if nw:
            # 合并下周预测字段到 prediction dict
            p.update(nw)

            if nw['nw_pred_direction'] is not None:
                # 有明确预测方向
                nw_count += 1
                if nw['nw_pred_direction'] == 'UP':
                    nw_up += 1

                # 填充下周回测准确率
                nw_stock_bt = nw_per_stock_bt.get(code)
                if nw_stock_bt:
                    p['nw_backtest_accuracy'] = nw_stock_bt['accuracy']
                    p['nw_backtest_samples'] = nw_stock_bt['total']

                    # 填充下周预测涨跌幅
                    strat = nw.get('nw_strategy', '')
                    pred_dir = nw['nw_pred_direction']
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
                    p['nw_backtest_accuracy'] = nw_global_bt['accuracy']
                    p['nw_backtest_samples'] = 0
            else:
                # 不确定 - 无高置信条件匹配
                nw_uncertain += 1
        else:
            # 数据不足，填充空值
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

    nw_coverage = round(nw_count / len(predictions) * 100, 1) if predictions else 0
    logger.info("  下周预测: %d只 (涨%d 跌%d), 不确定%d只, 覆盖率=%.1f%%, 回测准确率=%.1f%%",
                nw_count, nw_up, nw_count - nw_up, nw_uncertain, nw_coverage,
                nw_global_bt['accuracy'])

    # 填充回测准确率：优先使用个股+策略准确率，其次个股整体准确率，最后全局
    # 改进：
    #   1. MIN_STRATEGY_SAMPLES 提高到 5，减少小样本极端值
    #   2. 策略级准确率过低（<20%）且为 fuzzy 类弱信号策略时，回退到个股整体准确率
    #      因为 fuzzy 区间信号极弱，策略级准确率不具代表性
    MIN_STRATEGY_SAMPLES = 5
    MIN_FUZZY_STRATEGY_ACC = 20.0  # fuzzy策略准确率低于此值时回退
    _FUZZY_STRATEGIES = {'follow_d4(fuzzy)', 'reverse_d4(fuzzy)', 'uncertain_d4(fuzzy)',
                         'follow_d3(fuzzy)', 'reverse_d3(fuzzy)', 'uncertain_d3(fuzzy)'}
    filled_per_stock = 0
    filled_per_strategy = 0
    filled_fuzzy_fallback = 0
    bt_start = global_bt.get('start_date')
    bt_end = global_bt.get('end_date')
    bt_n_weeks = global_bt.get('n_weeks', 0)
    for p in predictions:
        code = p['stock_code']
        strategy = p.get('strategy', '')
        stock_bt = per_stock_bt.get(code)
        if stock_bt:
            # 优先使用该股票在当前策略下的准确率（需样本量≥5）
            strat_acc = stock_bt.get('strategy_acc', {}).get(strategy)
            strat_raw = stock_bt.get('_strategy_raw', {}).get(strategy, [0, 0])
            strat_samples = strat_raw[1] if isinstance(strat_raw, (list, tuple)) else 0
            use_strategy_acc = (strat_acc is not None and strat_samples >= MIN_STRATEGY_SAMPLES)

            # fuzzy 类弱信号策略：准确率过低时回退到个股整体准确率
            # 原因：fuzzy 区间 d4/d3 信号极弱（接近0），方向预测本质上是随机的，
            # 策略级准确率不能代表该股票的真实可预测性
            if use_strategy_acc and strategy in _FUZZY_STRATEGIES and strat_acc < MIN_FUZZY_STRATEGY_ACC:
                p['backtest_accuracy'] = stock_bt['accuracy']
                p['_fuzzy_fallback'] = True  # 标记：fuzzy策略回退
                p['_strat_acc_original'] = strat_acc  # 保留原始策略准确率供前端展示
                filled_fuzzy_fallback += 1
            elif use_strategy_acc:
                p['backtest_accuracy'] = strat_acc
                filled_per_strategy += 1
            else:
                p['backtest_accuracy'] = stock_bt['accuracy']
                filled_per_stock += 1
            p['backtest_lowo_accuracy'] = stock_bt['accuracy']
            p['backtest_samples'] = stock_bt['total']
        else:
            # 无该股票历史数据，使用全局准确率
            p['backtest_accuracy'] = global_bt['full_accuracy']
            p['backtest_lowo_accuracy'] = global_bt['lowo_accuracy']
            p['backtest_samples'] = 0
        p['backtest_weeks'] = bt_n_weeks
        p['backtest_start_date'] = bt_start
        p['backtest_end_date'] = bt_end

    logger.info("  回测填充: 策略级%d只, 个股级%d只, fuzzy回退%d只, 全局兜底%d只",
                filled_per_strategy, filled_per_stock, filled_fuzzy_fallback,
                len(predictions) - filled_per_strategy - filled_per_stock - filled_fuzzy_fallback)

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
    logger.info("  下周预测: %d只(覆盖%.1f%%), 回测准确率: %.1f%% (%d样本, 覆盖%.1f%%)",
                nw_count, nw_coverage, nw_global_bt['accuracy'],
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
