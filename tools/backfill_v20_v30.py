"""
回补历史周的 V20/V30 预测数据

对已有 V11 预测但缺少 V20/V30 的历史周，用当时的 K 线数据重新跑 V20/V30 模型，
将结果写回 stock_weekly_prediction_history 表。

用法: .venv/bin/python tools/backfill_v20_v30.py
"""
import sys
import os
import logging
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _to_float(v):
    try:
        return float(v) if v is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


def find_weeks_to_backfill():
    """找出需要回补 V20/V30 的周"""
    from dao import get_connection
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT iso_year, iso_week, nw_iso_year, nw_iso_week,
                   MAX(predict_date) as predict_date,
                   COUNT(*) as total,
                   SUM(v20_pred_direction IS NOT NULL AND v20_pred_direction != '') as v20_cnt,
                   SUM(v30_pred_direction IS NOT NULL AND v30_pred_direction != '') as v30_cnt
            FROM stock_weekly_prediction_history
            WHERE nw_pred_direction IS NOT NULL AND nw_pred_direction != ''
              AND nw_iso_year IS NOT NULL
            GROUP BY iso_year, iso_week, nw_iso_year, nw_iso_week
            HAVING v20_cnt = 0 OR v30_cnt = 0
            ORDER BY iso_year, iso_week
        """)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def load_stock_klines_for_date(stock_codes, latest_date):
    """加载截止到 latest_date 的 K 线数据（V20 需要至少 60 天）"""
    from dao import get_connection
    from datetime import datetime, timedelta

    start_date = (datetime.strptime(latest_date, '%Y-%m-%d') - timedelta(days=200)).strftime('%Y-%m-%d')

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    stock_klines = defaultdict(list)

    batch_size = 200
    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i:i + batch_size]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, close_price, open_price, high_price, "
            f"low_price, trading_volume, trading_amount, change_percent, change_hand "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [start_date, latest_date])
        for row in cur.fetchall():
            stock_klines[row['stock_code']].append({
                'date': row['date'],
                'close': _to_float(row['close_price']),
                'open': _to_float(row['open_price']),
                'high': _to_float(row['high_price']),
                'low': _to_float(row['low_price']),
                'volume': _to_float(row['trading_volume']),
                'turnover': _to_float(row.get('change_hand')),
                'change_percent': _to_float(row['change_percent']),
            })

    cur.close()
    conn.close()
    return dict(stock_klines)


def backfill_week(iso_year, iso_week, predict_date):
    """回补一个周的 V20/V30 预测"""
    from dao import get_connection
    from service.v20_prediction.v20_engine import V20PredictionEngine
    from service.v30_prediction.v30_predictor import batch_predict_v30

    logger.info("=" * 60)
    logger.info("回补 Y%d-W%02d (predict_date=%s)", iso_year, iso_week, predict_date)
    logger.info("=" * 60)

    # 1. 获取该周所有有 V11 预测的股票
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT stock_code FROM stock_weekly_prediction_history
            WHERE iso_year = %s AND iso_week = %s
              AND nw_pred_direction IS NOT NULL AND nw_pred_direction != ''
        """, (iso_year, iso_week))
        stock_codes = [r['stock_code'] for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()

    if not stock_codes:
        logger.info("  无股票需要回补")
        return

    logger.info("  共 %d 只股票", len(stock_codes))

    # 2. 加载截止到 predict_date 的 K 线数据
    logger.info("  加载K线数据...")
    stock_klines = load_stock_klines_for_date(stock_codes, predict_date)
    logger.info("  %d 只有K线数据", len(stock_klines))

    # 3. 运行 V20
    logger.info("  运行 V20 预测...")
    v20_engine = V20PredictionEngine()
    v20_results = v20_engine.predict_batch(stock_klines)
    logger.info("  V20: %d 只有信号", len(v20_results))

    # 4. 运行 V30
    logger.info("  运行 V30 预测...")
    v30_results = batch_predict_v30(stock_codes, predict_date)
    v30_signal_count = sum(1 for v in v30_results.values() if v.get('v30_pred_direction'))
    logger.info("  V30: %d 只有信号", v30_signal_count)

    # 5. 写回数据库
    logger.info("  写入数据库...")
    conn = get_connection()
    cur = conn.cursor()
    v20_updated = 0
    v30_updated = 0

    try:
        for code in stock_codes:
            updates = []
            params = []

            # V20
            v20 = v20_results.get(code)
            if v20:
                updates.extend([
                    "v20_pred_direction = %s",
                    "v20_confidence = %s",
                    "v20_rule_name = %s",
                    "v20_reason = %s",
                    "v20_backtest_acc = %s",
                    "v20_matched_count = %s",
                    "v20_matched_rules = %s",
                    "v20_pos = %s",
                    "v20_vr5 = %s",
                    "v20_ma20d = %s",
                    "v20_cdn = %s",
                ])
                feat = v20.get('features', {})
                params.extend([
                    v20['pred_direction'],
                    v20['confidence'],
                    v20['rule_name'],
                    (v20.get('reason') or '')[:200],
                    v20['backtest_acc'],
                    v20['matched_count'],
                    ','.join(v20['matched_rules']),
                    feat.get('pos'),
                    feat.get('vr5'),
                    feat.get('ma20d'),
                    feat.get('cdn'),
                ])
                v20_updated += 1

            # V30
            v30 = v30_results.get(code, {})
            if v30.get('v30_pred_direction'):
                updates.extend([
                    "v30_pred_direction = %s",
                    "v30_confidence = %s",
                    "v30_strategy = %s",
                    "v30_reason = %s",
                    "v30_composite_score = %s",
                    "v30_sent_agree = %s",
                    "v30_tech_agree = %s",
                    "v30_mkt_ret_20d = %s",
                ])
                params.extend([
                    v30['v30_pred_direction'],
                    v30['v30_confidence'],
                    v30.get('v30_strategy'),
                    (v30.get('v30_reason') or '')[:200],
                    v30.get('v30_composite_score'),
                    v30.get('v30_sent_agree'),
                    v30.get('v30_tech_agree'),
                    v30.get('v30_mkt_ret_20d'),
                ])
                v30_updated += 1

            if updates:
                sql = (f"UPDATE stock_weekly_prediction_history "
                       f"SET {', '.join(updates)} "
                       f"WHERE stock_code = %s AND iso_year = %s AND iso_week = %s")
                params.extend([code, iso_year, iso_week])
                cur.execute(sql, params)

        conn.commit()
        logger.info("  完成: V20 写入 %d 只, V30 写入 %d 只", v20_updated, v30_updated)
    except Exception as e:
        conn.rollback()
        logger.error("  写入失败: %s", e, exc_info=True)
    finally:
        cur.close()
        conn.close()


def main():
    weeks = find_weeks_to_backfill()
    if not weeks:
        print("所有周的 V20/V30 数据已完整，无需回补")
        return

    print(f"\n需要回补 {len(weeks)} 个周:\n")
    for w in weeks:
        print(f"  预测周 W{w['iso_week']} -> 目标周 W{w['nw_iso_week']} "
              f"(predict_date={w['predict_date']}) "
              f"V20={w['v20_cnt']}条 V30={w['v30_cnt']}条")

    print()
    for w in weeks:
        backfill_week(w['iso_year'], w['iso_week'], str(w['predict_date']))

    # 验证结果
    print("\n" + "=" * 60)
    print("回补完成，验证结果:")
    print("=" * 60)
    from dao import get_connection
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT nw_iso_year, nw_iso_week,
               SUM(nw_pred_direction IS NOT NULL AND nw_pred_direction != '') as v11,
               SUM(v20_pred_direction IS NOT NULL AND v20_pred_direction != '') as v20,
               SUM(v30_pred_direction IS NOT NULL AND v30_pred_direction != '') as v30
        FROM stock_weekly_prediction_history
        WHERE nw_iso_year IS NOT NULL
          AND nw_pred_direction IS NOT NULL AND nw_pred_direction != ''
        GROUP BY nw_iso_year, nw_iso_week
        ORDER BY nw_iso_year DESC, nw_iso_week DESC
    """)
    for r in cur.fetchall():
        print(f"  目标周 W{r['nw_iso_week']}: V11={r['v11']} V20={r['v20']} V30={r['v30']}")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
