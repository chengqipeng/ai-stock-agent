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

    # 填充回测准确率：优先使用个股+策略准确率，其次个股整体准确率，最后全局
    # 注意：策略级样本量过小时（<3），统计不可靠，回退到个股整体准确率
    MIN_STRATEGY_SAMPLES = 3
    filled_per_stock = 0
    filled_per_strategy = 0
    bt_start = global_bt.get('start_date')
    bt_end = global_bt.get('end_date')
    bt_n_weeks = global_bt.get('n_weeks', 0)
    for p in predictions:
        code = p['stock_code']
        strategy = p.get('strategy', '')
        stock_bt = per_stock_bt.get(code)
        if stock_bt:
            # 优先使用该股票在当前策略下的准确率（需样本量≥3）
            strat_acc = stock_bt.get('strategy_acc', {}).get(strategy)
            strat_raw = stock_bt.get('_strategy_raw', {}).get(strategy, [0, 0])
            strat_samples = strat_raw[1] if isinstance(strat_raw, (list, tuple)) else 0
            if strat_acc is not None and strat_samples >= MIN_STRATEGY_SAMPLES:
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

    logger.info("  回测填充: 策略级%d只, 个股级%d只, 全局兜底%d只",
                filled_per_strategy, filled_per_stock,
                len(predictions) - filled_per_strategy - filled_per_stock)

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
        'elapsed': round(elapsed, 1),
    }


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S',
    )
    run_batch_weekly_prediction()
