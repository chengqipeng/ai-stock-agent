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



def check_fund_flow_db(stock_code: str) -> list[dict]:
    """
    检测资金流向数据的强一致性，返回异常列表。
    每条异常: {"type": str, "date": str, "detail": str}

    检测规则：
    1. close_price 与 K线表不一致（容差 0.02）
    2. change_pct 与 K线表不一致（容差 0.5 个百分点）
    3. 关键字段缺失：close_price / change_pct / net_flow 为 NULL

    注意：
    - 首日交易数据（K线表最早日期）允许误差，跳过校验。
    - 资金守恒（net_flow = big+mid+small）和占比守恒不作为校验规则，
      因为同花顺数据源的统计口径与东方财富不同：
      · 东方财富：主力+中单+小单=全市场，net_flow ≈ 0，占比之和 ≈ 0
      · 同花顺：各类资金独立统计，net_flow ≠ big+mid+small，占比之和 ≠ 0
      混合数据源下守恒关系不成立是数据源特性，不是数据异常。
    """
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
            f"SELECT `date`, close_price, change_pct, net_flow, "
            f"big_net, big_net_pct, mid_net, mid_net_pct, small_net, small_net_pct "
            f"FROM {TABLE_NAME} WHERE stock_code = %s ORDER BY `date`",
            (stock_code,),
        )
        ff_rows = cursor.fetchall()
        if not ff_rows:
            return issues

        # 获取K线数据用于交叉比对
        cursor.execute(
            "SELECT `date`, close_price, change_percent "
            "FROM stock_kline WHERE stock_code = %s ORDER BY `date`",
            (stock_code,),
        )
        kline_map = {str(r["date"]): r for r in cursor.fetchall()}

        for ff in ff_rows:
            d_str = str(ff["date"])

            # 跳过首日交易
            if first_kline_date and d_str == first_kline_date:
                continue

            # 规则5：关键字段缺失
            for field in ("close_price", "change_pct", "net_flow"):
                if ff.get(field) is None:
                    issues.append({
                        "type": "ff_null_field",
                        "date": d_str,
                        "detail": f"资金流向 {field} 为 NULL",
                    })

            # 规则1：close_price 与K线交叉比对
            kline = kline_map.get(d_str)
            if kline and ff["close_price"] is not None and kline["close_price"] is not None:
                diff = abs(ff["close_price"] - kline["close_price"])
                if diff > 0.02:
                    issues.append({
                        "type": "ff_price_mismatch",
                        "date": d_str,
                        "detail": (f"资金流向close_price={ff['close_price']} "
                                   f"vs K线close_price={kline['close_price']} 差值={diff:.4f}"),
                    })

            # 规则2：change_pct 与K线交叉比对
            if kline and ff["change_pct"] is not None and kline["change_percent"] is not None:
                diff = abs(ff["change_pct"] - kline["change_percent"])
                if diff > 0.5:
                    issues.append({
                        "type": "ff_chg_pct_mismatch",
                        "date": d_str,
                        "detail": (f"资金流向change_pct={ff['change_pct']} "
                                   f"vs K线change_percent={kline['change_percent']} 差值={diff:.2f}"),
                    })
    finally:
        cursor.close()
        conn.close()
    return issues

