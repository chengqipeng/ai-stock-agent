"""历史资金流向数据 DAO — stock_fund_flow 表"""
import logging
from dao import get_connection

logger = logging.getLogger(__name__)

TABLE_NAME = "stock_fund_flow"


def create_fund_flow_table(cursor=None):
    """创建历史资金流向数据表（幂等）"""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            stock_code VARCHAR(20) NOT NULL,
            `date` VARCHAR(20) NOT NULL,
            close_price DOUBLE,
            change_pct DOUBLE,
            net_flow DOUBLE COMMENT '资金净流入(万元)',
            main_net_5day DOUBLE COMMENT '5日主力净额(万元)',
            big_net DOUBLE COMMENT '大单(主力)净额(万元)',
            big_net_pct DOUBLE COMMENT '大单净占比(%)',
            mid_net DOUBLE COMMENT '中单净额(万元)',
            mid_net_pct DOUBLE COMMENT '中单净占比(%)',
            small_net DOUBLE COMMENT '小单净额(万元)',
            small_net_pct DOUBLE COMMENT '小单净占比(%)',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_code_date (stock_code, `date`),
            INDEX idx_stock_code (stock_code),
            INDEX idx_date (`date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()
    cursor.execute(ddl)
    if own:
        conn.commit()
        cursor.close()
        conn.close()


def batch_upsert_fund_flow(stock_code: str, data_list: list[dict], cursor=None):
    """
    批量写入资金流向数据（upsert）。

    Args:
        stock_code: 股票代码（如 600183.SH）
        data_list: get_fund_flow_history 返回的原始数据列表（万元单位）
    """
    if not data_list:
        return 0

    sql = f"""
        INSERT INTO {TABLE_NAME}
            (stock_code, `date`, close_price, change_pct,
             net_flow, main_net_5day,
             big_net, big_net_pct, mid_net, mid_net_pct,
             small_net, small_net_pct)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            close_price=VALUES(close_price), change_pct=VALUES(change_pct),
            net_flow=VALUES(net_flow), main_net_5day=VALUES(main_net_5day),
            big_net=VALUES(big_net), big_net_pct=VALUES(big_net_pct),
            mid_net=VALUES(mid_net), mid_net_pct=VALUES(mid_net_pct),
            small_net=VALUES(small_net), small_net_pct=VALUES(small_net_pct)
    """

    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()

    rows = [
        (stock_code, d.get("date", ""), d.get("close_price"),
         d.get("change_pct"), d.get("net_flow"), d.get("main_net_5day"),
         d.get("big_net"), d.get("big_net_pct"),
         d.get("mid_net"), d.get("mid_net_pct"),
         d.get("small_net"), d.get("small_net_pct"))
        for d in data_list
    ]
    cursor.executemany(sql, rows)
    count = cursor.rowcount

    if own:
        conn.commit()
        cursor.close()
        conn.close()

    return count


def get_fund_flow_by_code(stock_code: str, limit: int = 120, cursor=None) -> list[dict]:
    """查询某只股票的历史资金流向数据（按日期倒序）"""
    sql = f"SELECT * FROM {TABLE_NAME} WHERE stock_code = %s ORDER BY `date` DESC LIMIT %s"
    own = cursor is None
    if own:
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
    cursor.execute(sql, (stock_code, limit))
    result = cursor.fetchall()
    if own:
        cursor.close()
        conn.close()
    return result


def get_fund_flow_latest_date(stock_code: str, cursor=None) -> str | None:
    """查询某只股票资金流向的最新日期"""
    sql = f"SELECT MAX(`date`) FROM {TABLE_NAME} WHERE stock_code = %s"
    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()
    cursor.execute(sql, (stock_code,))
    row = cursor.fetchone()
    if own:
        cursor.close()
        conn.close()
    return row[0] if row else None

def get_fund_flow_count(stock_code: str, cursor=None) -> int:
    """查询某只股票的历史资金流向记录条数"""
    sql = f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE stock_code = %s"
    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()
    cursor.execute(sql, (stock_code,))
    row = cursor.fetchone()
    if own:
        cursor.close()
        conn.close()
    return row[0] if row else 0



def sync_price_fields_from_kline(stock_code: str) -> int:
    """
    将资金流向表的 close_price / change_pct 统一从 K线表同步。

    close_price 和 change_pct 本质上是 K线数据，资金流向表中存一份仅为查询方便。
    K线是最权威的价格来源，直接用 K线覆盖可消除所有数据源差异（NULL、mismatch）。

    一条 UPDATE ... JOIN 批量完成，无需逐条比对。

    Returns:
        更新的记录数
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"UPDATE {TABLE_NAME} ff "
            f"JOIN stock_kline k ON ff.stock_code = k.stock_code AND ff.`date` = k.`date` "
            f"SET ff.close_price = k.close_price, ff.change_pct = k.change_percent "
            f"WHERE ff.stock_code = %s "
            f"  AND k.close_price IS NOT NULL "
            f"  AND (ff.close_price IS NULL "
            f"       OR ff.change_pct IS NULL "
            f"       OR ABS(ff.close_price - k.close_price) > 0.001 "
            f"       OR ABS(ff.change_pct - k.change_percent) > 0.001)",
            (stock_code,),
        )
        count = cursor.rowcount
        conn.commit()
        return count
    except Exception as e:
        conn.rollback()
        logger.error("sync_price_fields_from_kline(%s) 异常: %s", stock_code, e)
        return 0
    finally:
        cursor.close()
        conn.close()


def check_fund_flow_db(stock_code: str) -> list[dict]:
    """
    检测资金流向数据的一致性，返回异常列表。
    每条异常: {"type": str, "date": str, "detail": str}

    检测规则：
    1. 关键字段缺失：net_flow 为 NULL
    2. close_price / change_pct 为 NULL 且 K线表中也无对应数据

    注意：
    - 北交所股票直接跳过（数据源不覆盖北交所资金流向）。
    - close_price / change_pct 的 mismatch 不再检测，因为 sync_price_fields_from_kline
      已在修复阶段统一从 K线同步，不可能存在差异。
    - 首日交易数据（K线表最早日期）允许误差，跳过校验。
    - 资金守恒（net_flow = big+mid+small）和占比守恒不作为校验规则，
      因为同花顺数据源的统计口径与东方财富不同：
      · 东方财富：主力+中单+小单=全市场，net_flow ≈ 0，占比之和 ≈ 0
      · 同花顺：各类资金独立统计，net_flow ≠ big+mid+small，占比之和 ≠ 0
      混合数据源下守恒关系不成立是数据源特性，不是数据异常。
    """
    # 北交所股票数据源不覆盖资金流向，跳过校验
    if stock_code.endswith('.BJ'):
        return []

    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    issues: list[dict] = []
    try:
        # 获取该股票K线最早日期（首日交易）
        cursor.execute(
            "SELECT MIN(`date`) AS first_date FROM stock_kline WHERE stock_code = %s",
            (stock_code,),
        )
        row = cursor.fetchone()
        first_kline_date = str(row["first_date"]) if row and row["first_date"] else None

        # 获取资金流向数据
        cursor.execute(
            f"SELECT ff.`date`, ff.close_price, ff.change_pct, ff.net_flow "
            f"FROM {TABLE_NAME} ff "
            f"WHERE ff.stock_code = %s ORDER BY ff.`date`",
            (stock_code,),
        )
        ff_rows = cursor.fetchall()
        if not ff_rows:
            return issues

        for ff in ff_rows:
            d_str = str(ff["date"])

            # 跳过首日交易
            if first_kline_date and d_str == first_kline_date:
                continue

            # 关键字段缺失
            for field in ("close_price", "change_pct", "net_flow"):
                if ff.get(field) is None:
                    issues.append({
                        "type": "ff_null_field",
                        "date": d_str,
                        "detail": f"资金流向 {field} 为 NULL",
                    })
    finally:
        cursor.close()
        conn.close()
    return issues

def evaluate_source_divergence(
    jqka_rows: list[dict],
    em_rows: list[dict],
) -> dict:
    """
    评估同花顺与东方财富两个数据源在重叠日期上的维度差异。

    两个平台的核心差异：
      ┌──────────────┬──────────────────────────┬──────────────────────────┐
      │ 维度         │ 同花顺                    │ 东方财富                  │
      ├──────────────┼──────────────────────────┼──────────────────────────┤
      │ close_price  │ 页面展示值               │ API f[11]                │
      │ change_pct   │ 页面展示值               │ API f[12]                │
      │ big_net      │ "大单(主力)"=超大+大单    │ "主力"(f[1])=超大+大单    │
      │ mid_net      │ 中单                     │ 中单(f[3])               │
      │ small_net    │ 小单                     │ 小单(f[2])               │
      │ net_flow     │ 独立统计，≠ big+mid+small │ big+mid+small ≈ 0       │
      │ big_net_pct  │ 大单(主力)净占比          │ 主力净占比(f[6])          │
      │ mid_net_pct  │ 中单净占比               │ 中单净占比(f[8])          │
      │ small_net_pct│ 小单净占比               │ 小单净占比(f[7])          │
      │ main_net_5day│ 页面直接提供              │ 从主力净流入滚动计算       │
      │ 资金分类阈值  │ 同花顺标准               │ 东方财富标准（不同）       │
      │ 数据覆盖     │ ~30条（近期增量）          │ ~120-150条（全量历史）    │
      └──────────────┴──────────────────────────┴──────────────────────────┘

    评估逻辑：
      对重叠日期逐字段计算偏差，输出各维度的平均偏差和最大偏差，
      以及整体一致性评分，供修复策略决策使用。

    Args:
        jqka_rows: 同花顺数据列表 (dict with date, close_price, change_pct, ...)
        em_rows:   东方财富数据列表 (dict with date, close_price, change_pct, ...)

    Returns:
        {
            "overlap_count": 重叠日期数,
            "jqka_only_count": 仅同花顺有的日期数,
            "em_only_count": 仅东方财富有的日期数,
            "fields": {
                "close_price": {"avg_diff": float, "max_diff": float, "match_rate": float},
                "change_pct":  {"avg_diff": float, "max_diff": float, "match_rate": float},
                "big_net":     {"avg_diff": float, "max_diff": float, "match_rate": float},
                ...
            },
            "recommendation": "jqka_only" | "em_only" | "jqka_recent_em_history" | "em_with_kline_fix",
            "reason": str,
        }
    """
    jqka_map = {r["date"]: r for r in jqka_rows if r.get("date")}
    em_map = {r["date"]: r for r in em_rows if r.get("date")}

    overlap_dates = sorted(set(jqka_map.keys()) & set(em_map.keys()))
    jqka_only = set(jqka_map.keys()) - set(em_map.keys())
    em_only = set(em_map.keys()) - set(jqka_map.keys())

    result = {
        "overlap_count": len(overlap_dates),
        "jqka_only_count": len(jqka_only),
        "em_only_count": len(em_only),
        "fields": {},
        "recommendation": "jqka_recent_em_history",
        "reason": "",
    }

    if not overlap_dates:
        result["recommendation"] = "jqka_recent_em_history"
        result["reason"] = "无重叠日期，无法评估差异，使用默认策略"
        return result

    # 评估各字段差异
    # close_price / change_pct: 两个平台应高度一致（都是收盘价/涨跌幅）
    # 资金流字段: 因统计口径不同，预期有差异
    eval_fields = {
        "close_price":   {"tol": 0.02, "category": "price"},
        "change_pct":    {"tol": 0.5,  "category": "price"},
        "big_net":       {"tol": None, "category": "flow"},
        "mid_net":       {"tol": None, "category": "flow"},
        "small_net":     {"tol": None, "category": "flow"},
        "net_flow":      {"tol": None, "category": "flow"},
        "big_net_pct":   {"tol": None, "category": "pct"},
        "mid_net_pct":   {"tol": None, "category": "pct"},
        "small_net_pct": {"tol": None, "category": "pct"},
    }

    price_mismatch_count = 0

    for field, meta in eval_fields.items():
        diffs = []
        match_count = 0
        for d in overlap_dates:
            jv = jqka_map[d].get(field)
            ev = em_map[d].get(field)
            if jv is None or ev is None:
                continue
            diff = abs(jv - ev)
            diffs.append(diff)
            # 对于价格类字段用绝对容差，资金流字段用相对容差
            if meta["category"] == "price":
                if diff <= (meta["tol"] or 0.5):
                    match_count += 1
                else:
                    price_mismatch_count += 1
            elif meta["category"] == "flow":
                # 资金流字段用相对偏差（相对于两者绝对值的均值）
                base = (abs(jv) + abs(ev)) / 2
                if base < 1:  # 两者都接近0
                    match_count += 1
                elif diff / base <= 0.3:  # 30%以内视为一致
                    match_count += 1
            else:  # pct
                if diff <= 2.0:  # 占比差异2个百分点以内
                    match_count += 1

        total = len(diffs)
        result["fields"][field] = {
            "avg_diff": round(sum(diffs) / total, 4) if total else 0,
            "max_diff": round(max(diffs), 4) if diffs else 0,
            "match_rate": round(match_count / total, 4) if total else 1.0,
            "sample_count": total,
        }

    # 决策逻辑
    price_fields_ok = all(
        result["fields"].get(f, {}).get("match_rate", 1.0) >= 0.9
        for f in ("close_price", "change_pct")
    )
    flow_fields_divergent = any(
        result["fields"].get(f, {}).get("match_rate", 1.0) < 0.5
        for f in ("big_net", "mid_net", "small_net", "net_flow")
    )

    if price_fields_ok and not flow_fields_divergent:
        # 两个数据源高度一致，可以混合使用
        result["recommendation"] = "jqka_recent_em_history"
        result["reason"] = (
            f"价格字段一致性高，资金流字段差异可接受。"
            f"建议：东方财富全量打底 + 同花顺覆盖近期数据"
        )
    elif price_fields_ok and flow_fields_divergent:
        # 价格一致但资金流差异大（统计口径不同导致），避免混合
        result["recommendation"] = "jqka_only"
        result["reason"] = (
            f"价格字段一致，但资金流字段差异显著（统计口径不同）。"
            f"建议：仅使用同花顺数据修复，避免混合不同口径"
        )
    elif not price_fields_ok:
        # 连价格都不一致，说明某个数据源有问题，以K线为准修复价格
        result["recommendation"] = "em_with_kline_fix"
        result["reason"] = (
            f"价格字段存在 {price_mismatch_count} 处不一致，数据源可能有延迟或错误。"
            f"建议：东方财富全量打底 + K线修正价格字段"
        )
    else:
        result["recommendation"] = "jqka_recent_em_history"
        result["reason"] = "默认策略：东方财富全量打底 + 同花顺覆盖近期数据"

    return result







