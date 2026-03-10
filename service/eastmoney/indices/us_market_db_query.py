"""
美股市场数据 — 数据库查询接口

从 us_market_data_scheduler.py 对应的数据库表中查询美股数据，
用于 A 股预测回测中的美股隔夜信号因子。

数据表：
1. us_index_kline          — 美股指数日K线（NDX/DJIA/SPX）
2. global_index_realtime   — 全球指数当日行情快照
3. us_stock_ranking        — 涨幅榜快照（中概股/知名美股/互联网中国）

主要查询：
- get_us_overnight_signal()  — 获取美股隔夜信号（核心：用于A股预测）
- get_us_index_kline_range() — 按日期范围查询美股指数K线
- get_china_concept_avg_change() — 中概股平均涨跌幅
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

from dao import get_connection

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# A股板块 → 美股参考指数映射
# 基于 us_market_correlation_analysis.json 相关性分析结果
# ═══════════════════════════════════════════════════════════

# 板块敏感度配置：权重越高表示该板块受美股影响越大
# 基于实际相关性数据校准
SECTOR_US_SENSITIVITY = {
    "科技": {
        "indices": ["NDX"],           # 纳斯达克与科技股相关性最强 (avg_corr=+0.165)
        "weight": 1.5,                # 高敏感度
        "large_move_weight": 2.0,     # 美股大幅波动时权重更高
    },
    "有色金属": {
        "indices": ["SPX", "DJIA"],   # 有色金属与标普/道琼斯弱相关 (avg_corr=+0.093)
        "weight": 0.8,
        "large_move_weight": 1.2,
    },
    "新能源": {
        "indices": ["NDX", "SPX"],    # 新能源与纳斯达克/标普弱相关 (avg_corr=+0.096)
        "weight": 0.8,
        "large_move_weight": 1.5,
    },
    "化工": {
        "indices": ["SPX"],           # 化工与标普弱相关 (avg_corr=+0.097)
        "weight": 0.6,
        "large_move_weight": 1.0,
    },
    "制造": {
        "indices": ["SPX", "DJIA"],   # 制造与标普/道琼斯弱相关 (avg_corr=+0.121)
        "weight": 0.8,
        "large_move_weight": 1.2,
    },
    "汽车": {
        "indices": ["SPX"],           # 汽车与美股相关性很弱 (avg_corr=+0.029)
        "weight": 0.3,
        "large_move_weight": 0.8,
    },
    "医药": {
        "indices": ["SPX"],           # 医药与美股几乎无相关 (avg_corr=+0.013)
        "weight": 0.1,
        "large_move_weight": 0.5,
    },
}


# ═══════════════════════════════════════════════════════════
# 1. 美股指数K线查询（按日期范围）
# ═══════════════════════════════════════════════════════════

def get_us_index_kline_range(
    index_code: str = "NDX",
    start_date: str = None,
    end_date: str = None,
    limit: int = 200,
) -> list[dict]:
    """查询美股指数日K线（按日期范围，由旧到新）。

    Args:
        index_code: 指数代码 NDX/DJIA/SPX
        start_date: 起始日期 YYYY-MM-DD
        end_date: 截止日期 YYYY-MM-DD
        limit: 最大返回条数

    Returns:
        list[dict]: 每条包含 trade_date, close_price, change_pct 等
    """
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        conditions = ["index_code = %s"]
        params = [index_code]

        if start_date:
            conditions.append("trade_date >= %s")
            params.append(start_date)
        if end_date:
            conditions.append("trade_date <= %s")
            params.append(end_date)

        where = " AND ".join(conditions)
        params.append(limit)

        cursor.execute(
            f"SELECT trade_date, open_price, close_price, high_price, low_price, "
            f"       volume, change_pct, amplitude "
            f"FROM us_index_kline "
            f"WHERE {where} ORDER BY trade_date ASC LIMIT %s",
            params,
        )
        rows = cursor.fetchall()
        # trade_date 转字符串
        for r in rows:
            if r.get("trade_date"):
                r["trade_date"] = str(r["trade_date"])
        return rows
    finally:
        cursor.close()
        conn.close()



# ═══════════════════════════════════════════════════════════
# 2. 中概股平均涨跌幅查询
# ═══════════════════════════════════════════════════════════

def get_china_concept_avg_change(trade_date: str, limit: int = 30) -> Optional[float]:
    """查询指定日期中概股涨幅榜的平均涨跌幅。

    Args:
        trade_date: 交易日期 YYYY-MM-DD
        limit: 取前N只股票计算平均

    Returns:
        平均涨跌幅(%)，无数据返回 None
    """
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT AVG(change_pct) as avg_pct, COUNT(*) as cnt "
            "FROM (SELECT change_pct FROM us_stock_ranking "
            "      WHERE trade_date = %s AND category = 'china_concept' "
            "      AND change_pct IS NOT NULL "
            "      ORDER BY rank_order ASC LIMIT %s) t",
            (trade_date, limit),
        )
        row = cursor.fetchone()
        if row and row.get("cnt", 0) > 0 and row.get("avg_pct") is not None:
            return float(row["avg_pct"])
        return None
    finally:
        cursor.close()
        conn.close()


# ═══════════════════════════════════════════════════════════
# 3. 核心：美股隔夜信号计算
# ═══════════════════════════════════════════════════════════

def get_us_overnight_signal(
    a_share_date: str,
    sector: str = None,
    lookback_days: int = 7,
) -> dict:
    """计算A股交易日对应的美股隔夜信号。

    逻辑：A股T日开盘前，美股最近一个交易日（通常是T-1日美东时间）的表现。
    由于时差，A股周一对应美股上周五。

    Args:
        a_share_date: A股交易日期 YYYY-MM-DD
        sector: A股板块名称（用于选择参考指数和权重）
        lookback_days: 往前查找美股交易日的最大天数

    Returns:
        dict: {
            '信号分': float,          # 综合信号（正=看涨，负=看跌）
            '隔夜涨跌(%)': float,     # 美股隔夜涨跌幅
            '参考指数': str,           # 使用的美股指数
            '波动级别': str,           # 小幅/中幅/大幅
            '中概股涨跌(%)': float,    # 中概股平均涨跌（如有）
            '有效': bool,              # 是否有有效数据
        }
    """
    result = {
        "信号分": 0.0,
        "隔夜涨跌(%)": 0.0,
        "参考指数": "",
        "波动级别": "无数据",
        "中概股涨跌(%)": None,
        "有效": False,
    }

    # 确定参考指数和权重
    sensitivity = SECTOR_US_SENSITIVITY.get(sector, {
        "indices": ["SPX"],
        "weight": 0.5,
        "large_move_weight": 1.0,
    })
    ref_indices = sensitivity["indices"]
    base_weight = sensitivity["weight"]
    large_weight = sensitivity["large_move_weight"]

    # 查找A股交易日前一个美股交易日
    dt = datetime.strptime(a_share_date, "%Y-%m-%d")
    search_start = (dt - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    search_end = (dt - timedelta(days=1)).strftime("%Y-%m-%d")

    # 查询各参考指数的隔夜数据
    index_changes = []
    ref_index_used = ""

    for idx_code in ref_indices:
        klines = get_us_index_kline_range(
            index_code=idx_code,
            start_date=search_start,
            end_date=search_end,
            limit=lookback_days,
        )
        if klines:
            # 取最近一个交易日
            latest = klines[-1]
            chg = float(latest.get("change_pct") or 0)
            index_changes.append((idx_code, chg))
            if not ref_index_used:
                ref_index_used = idx_code

    if not index_changes:
        return result

    # 加权平均（第一个指数权重最高）
    if len(index_changes) == 1:
        avg_change = index_changes[0][1]
    else:
        # 主指数70%，副指数30%
        avg_change = index_changes[0][1] * 0.7 + index_changes[1][1] * 0.3

    # 判断波动级别
    abs_chg = abs(avg_change)
    if abs_chg >= 2.0:
        volatility = "大幅"
        effective_weight = large_weight
    elif abs_chg >= 1.0:
        volatility = "中幅"
        effective_weight = (base_weight + large_weight) / 2
    elif abs_chg >= 0.3:
        volatility = "小幅"
        effective_weight = base_weight
    else:
        volatility = "微幅"
        effective_weight = base_weight * 0.5

    # 计算信号分
    # 基础信号：涨跌幅 × 板块敏感度权重
    signal = 0.0
    if avg_change > 2.0:
        signal = 2.0 * effective_weight
    elif avg_change > 1.0:
        signal = 1.0 * effective_weight
    elif avg_change > 0.3:
        signal = 0.5 * effective_weight
    elif avg_change < -2.0:
        signal = -2.0 * effective_weight
    elif avg_change < -1.0:
        signal = -1.0 * effective_weight
    elif avg_change < -0.3:
        signal = -0.5 * effective_weight

    # 中概股信号（对科技板块额外参考）
    china_concept_chg = None
    if sector in ("科技", "新能源"):
        china_concept_chg = get_china_concept_avg_change(search_end)
        if china_concept_chg is not None:
            # 中概股信号作为补充（权重20%）
            if china_concept_chg > 2.0:
                signal += 0.5
            elif china_concept_chg > 1.0:
                signal += 0.2
            elif china_concept_chg < -2.0:
                signal -= 0.5
            elif china_concept_chg < -1.0:
                signal -= 0.2

    # 限幅
    signal = max(-3.0, min(3.0, signal))

    result.update({
        "信号分": round(signal, 3),
        "隔夜涨跌(%)": round(avg_change, 3),
        "参考指数": "/".join(c[0] for c in index_changes),
        "波动级别": volatility,
        "中概股涨跌(%)": round(china_concept_chg, 2) if china_concept_chg is not None else None,
        "有效": True,
    })
    return result


# ═══════════════════════════════════════════════════════════
# 4. 批量预加载美股K线（用于回测加速）
# ═══════════════════════════════════════════════════════════

def preload_us_kline_map(
    start_date: str,
    end_date: str,
    index_codes: list[str] = None,
) -> dict[str, dict[str, float]]:
    """预加载美股指数K线，返回 {index_code: {date_str: change_pct}} 映射。

    用于回测时避免逐日查询数据库。

    Args:
        start_date: 起始日期
        end_date: 截止日期
        index_codes: 指数代码列表，默认 NDX/SPX/DJIA

    Returns:
        {index_code: {trade_date: change_pct}}
    """
    if index_codes is None:
        index_codes = ["NDX", "SPX", "DJIA"]

    result = {}
    for code in index_codes:
        klines = get_us_index_kline_range(
            index_code=code,
            start_date=start_date,
            end_date=end_date,
            limit=500,
        )
        date_map = {}
        for k in klines:
            d = str(k.get("trade_date", ""))
            chg = float(k.get("change_pct") or 0)
            date_map[d] = chg
        result[code] = date_map
        logger.info("预加载美股K线 %s: %d 条", code, len(date_map))

    return result


def get_us_overnight_signal_fast(
    a_share_date: str,
    sector: str,
    us_kline_map: dict[str, dict[str, float]],
    lookback_days: int = 7,
) -> dict:
    """快速版美股隔夜信号（使用预加载数据，避免逐日查DB）。

    Args:
        a_share_date: A股交易日期
        sector: A股板块
        us_kline_map: 预加载的 {index_code: {date: change_pct}}
        lookback_days: 往前查找天数

    Returns:
        同 get_us_overnight_signal
    """
    result = {
        "信号分": 0.0,
        "隔夜涨跌(%)": 0.0,
        "参考指数": "",
        "波动级别": "无数据",
        "有效": False,
    }

    sensitivity = SECTOR_US_SENSITIVITY.get(sector, {
        "indices": ["SPX"],
        "weight": 0.5,
        "large_move_weight": 1.0,
    })
    ref_indices = sensitivity["indices"]
    base_weight = sensitivity["weight"]
    large_weight = sensitivity["large_move_weight"]

    dt = datetime.strptime(a_share_date, "%Y-%m-%d")

    # 查找前一个美股交易日
    index_changes = []
    for idx_code in ref_indices:
        idx_map = us_kline_map.get(idx_code, {})
        for offset in range(1, lookback_days + 1):
            prev_date = (dt - timedelta(days=offset)).strftime("%Y-%m-%d")
            if prev_date in idx_map:
                index_changes.append((idx_code, idx_map[prev_date]))
                break

    if not index_changes:
        return result

    # 加权平均
    if len(index_changes) == 1:
        avg_change = index_changes[0][1]
    else:
        avg_change = index_changes[0][1] * 0.7 + index_changes[1][1] * 0.3

    # 波动级别和有效权重
    abs_chg = abs(avg_change)
    if abs_chg >= 2.0:
        volatility = "大幅"
        effective_weight = large_weight
    elif abs_chg >= 1.0:
        volatility = "中幅"
        effective_weight = (base_weight + large_weight) / 2
    elif abs_chg >= 0.3:
        volatility = "小幅"
        effective_weight = base_weight
    else:
        volatility = "微幅"
        effective_weight = base_weight * 0.5

    # 信号分
    signal = 0.0
    if avg_change > 2.0:
        signal = 2.0 * effective_weight
    elif avg_change > 1.0:
        signal = 1.0 * effective_weight
    elif avg_change > 0.3:
        signal = 0.5 * effective_weight
    elif avg_change < -2.0:
        signal = -2.0 * effective_weight
    elif avg_change < -1.0:
        signal = -1.0 * effective_weight
    elif avg_change < -0.3:
        signal = -0.5 * effective_weight

    signal = max(-3.0, min(3.0, signal))

    result.update({
        "信号分": round(signal, 3),
        "隔夜涨跌(%)": round(avg_change, 3),
        "参考指数": "/".join(c[0] for c in index_changes),
        "波动级别": volatility,
        "有效": True,
    })
    return result
