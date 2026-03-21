"""
预测验证服务 — 用实际K线数据回填历史预测的 actual_direction / actual_weekly_chg / is_correct
"""
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from dao import get_connection
from dao.stock_weekly_prediction_dao import backfill_actual_results

logger = logging.getLogger(__name__)


def _compound_return(pcts: list[float]) -> float:
    """复合收益率计算。"""
    r = 1.0
    for p in pcts:
        r *= (1 + p / 100)
    return round((r - 1) * 100, 4)


def _get_unverified_weeks(limit: int = 10) -> list[dict]:
    """获取有未验证预测的周列表（is_correct IS NULL 且不是最新周）。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT iso_year, iso_week, COUNT(*) as total,
                   SUM(is_correct IS NULL) as pending
            FROM stock_weekly_prediction_history
            WHERE (stock_code LIKE '6%%.SH' OR stock_code LIKE '0%%.SZ' OR stock_code LIKE '3%%.SZ')
              AND stock_code NOT LIKE '399%%'
              AND stock_code != '000001.SH'
            GROUP BY iso_year, iso_week
            HAVING pending > 0
            ORDER BY iso_year DESC, iso_week DESC
            LIMIT %s
        """, (limit,))
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def _get_week_date_range(iso_year: int, iso_week: int) -> tuple[str, str]:
    """根据 ISO 年+周 计算该周的周一和周五日期。"""
    # ISO week: 周一是第1天
    mon = datetime.strptime(f'{iso_year}-W{iso_week:02d}-1', '%G-W%V-%u')
    fri = mon + timedelta(days=4)
    return mon.strftime('%Y-%m-%d'), fri.strftime('%Y-%m-%d')


def verify_week_predictions(iso_year: int, iso_week: int) -> dict:
    """对指定周的预测进行实际结果验证。

    流程：
    1. 从 history 表获取该周所有 is_correct IS NULL 的预测
    2. 计算该周的日期范围（周一~周五）
    3. 从 stock_kline 批量获取这些股票在该周的 K 线数据
    4. 计算每只股票的实际周涨跌幅
    5. 对比预测方向与实际方向，回填结果

    Returns:
        {'iso_year': ..., 'iso_week': ..., 'verified': N, 'correct': N, 'wrong': N, 'skipped': N}
    """
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    try:
        # 1. 获取该周未验证的预测
        cur.execute("""
            SELECT stock_code, pred_direction
            FROM stock_weekly_prediction_history
            WHERE iso_year = %s AND iso_week = %s AND is_correct IS NULL
              AND (stock_code LIKE '6%%.SH' OR stock_code LIKE '0%%.SZ' OR stock_code LIKE '3%%.SZ')
              AND stock_code NOT LIKE '399%%'
              AND stock_code != '000001.SH'
        """, (iso_year, iso_week))
        predictions = cur.fetchall()

        if not predictions:
            return {'iso_year': iso_year, 'iso_week': iso_week,
                    'verified': 0, 'correct': 0, 'wrong': 0, 'skipped': 0,
                    'message': '该周无待验证预测'}

        stock_codes = [p['stock_code'] for p in predictions]
        pred_map = {p['stock_code']: p['pred_direction'] for p in predictions}

        # 2. 计算该周日期范围
        start_date, end_date = _get_week_date_range(iso_year, iso_week)

        # 3. 批量获取 K 线数据
        kline_map = defaultdict(list)
        batch_size = 200
        for i in range(0, len(stock_codes), batch_size):
            batch = stock_codes[i:i + batch_size]
            ph = ','.join(['%s'] * len(batch))
            cur.execute(f"""
                SELECT stock_code, `date`, change_percent
                FROM stock_kline
                WHERE stock_code IN ({ph})
                  AND `date` >= %s AND `date` <= %s
                ORDER BY `date`
            """, batch + [start_date, end_date])
            for row in cur.fetchall():
                kline_map[row['stock_code']].append(row)

        # 4. 计算实际涨跌幅并对比
        results = []
        skipped = 0
        correct_count = 0
        wrong_count = 0

        for code in stock_codes:
            klines = kline_map.get(code, [])
            if len(klines) < 3:
                # 数据不足（可能停牌或该周无足够交易日），跳过
                skipped += 1
                continue

            pcts = []
            for k in klines:
                cp = k.get('change_percent')
                if cp is not None:
                    pcts.append(float(cp))

            if not pcts:
                skipped += 1
                continue

            actual_chg = _compound_return(pcts)
            actual_dir = 'UP' if actual_chg >= 0 else 'DOWN'
            pred_dir = pred_map.get(code)
            is_correct = 1 if pred_dir == actual_dir else 0

            if is_correct:
                correct_count += 1
            else:
                wrong_count += 1

            results.append({
                'stock_code': code,
                'actual_direction': actual_dir,
                'actual_weekly_chg': actual_chg,
                'is_correct': is_correct,
            })

        # 5. 回填
        if results:
            backfill_actual_results(iso_year, iso_week, results)

        verified = len(results)
        accuracy = round(correct_count / verified * 100, 1) if verified > 0 else None

        logger.info("验证完成 Y%d-W%02d: 验证%d只, 正确%d, 错误%d, 跳过%d, 准确率%s%%",
                    iso_year, iso_week, verified, correct_count, wrong_count, skipped,
                    accuracy if accuracy is not None else '-')

        return {
            'iso_year': iso_year,
            'iso_week': iso_week,
            'date_range': f'{start_date}~{end_date}',
            'verified': verified,
            'correct': correct_count,
            'wrong': wrong_count,
            'skipped': skipped,
            'accuracy': accuracy,
        }

    finally:
        cur.close()
        conn.close()


def _get_nw_unverified_weeks(limit: int = 20) -> list[dict]:
    """获取有未验证下周预测的预测周列表。

    逻辑：找到有 nw_pred_direction 但 nw_is_correct IS NULL 的预测周 W，
    直接在 W 的记录上回填，不依赖 W+1 的 history 记录是否存在。
    """
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT iso_year, iso_week,
                   nw_iso_year, nw_iso_week,
                   COUNT(*) as total,
                   SUM(nw_is_correct IS NULL) as pending
            FROM stock_weekly_prediction_history
            WHERE nw_pred_direction IS NOT NULL
              AND nw_pred_direction != ''
              AND nw_is_correct IS NULL
              AND (stock_code LIKE '6%%.SH' OR stock_code LIKE '0%%.SZ' OR stock_code LIKE '3%%.SZ')
              AND stock_code NOT LIKE '399%%'
              AND stock_code != '000001.SH'
            GROUP BY iso_year, iso_week, nw_iso_year, nw_iso_week
            HAVING pending > 0
            ORDER BY iso_year DESC, iso_week DESC
            LIMIT %s
        """, (limit,))
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def verify_nw_week_predictions(iso_year: int, iso_week: int) -> dict:
    """对指定预测周 W 的下周预测进行实际结果验证。

    直接从 K 线数据计算目标周(nw_iso_year/nw_iso_week)的实际涨跌幅，
    写入 W 的记录的 nw_actual_direction / nw_actual_weekly_chg / nw_is_correct。
    不依赖目标周是否有 history 记录。

    Args:
        iso_year: 做出预测的那一周(W)的 ISO 年
        iso_week: 做出预测的那一周(W)的 ISO 周
    """
    from dao.stock_weekly_prediction_dao import backfill_nw_actual_results

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    try:
        # 1. 获取该预测周所有有下周预测但未验证的记录
        cur.execute("""
            SELECT stock_code, nw_pred_direction, nw_iso_year, nw_iso_week
            FROM stock_weekly_prediction_history
            WHERE iso_year = %s AND iso_week = %s
              AND nw_pred_direction IS NOT NULL AND nw_pred_direction != ''
              AND nw_is_correct IS NULL
              AND (stock_code LIKE '6%%.SH' OR stock_code LIKE '0%%.SZ' OR stock_code LIKE '3%%.SZ')
              AND stock_code NOT LIKE '399%%'
              AND stock_code != '000001.SH'
        """, (iso_year, iso_week))
        predictions = cur.fetchall()

        if not predictions:
            return {'iso_year': iso_year, 'iso_week': iso_week,
                    'verified': 0, 'correct': 0, 'wrong': 0, 'skipped': 0,
                    'message': '该周无待验证下周预测', 'type': 'nw'}

        # 所有预测应指向同一个目标周
        nw_year = predictions[0]['nw_iso_year']
        nw_week = predictions[0]['nw_iso_week']
        target_start, target_end = _get_week_date_range(nw_year, nw_week)

        stock_codes = [p['stock_code'] for p in predictions]
        pred_map = {p['stock_code']: p['nw_pred_direction'] for p in predictions}

        # 2. 批量获取目标周的 K 线数据
        kline_map = defaultdict(list)
        batch_size = 200
        for i in range(0, len(stock_codes), batch_size):
            batch = stock_codes[i:i + batch_size]
            ph = ','.join(['%s'] * len(batch))
            cur.execute(f"""
                SELECT stock_code, `date`, change_percent
                FROM stock_kline
                WHERE stock_code IN ({ph})
                  AND `date` >= %s AND `date` <= %s
                ORDER BY `date`
            """, batch + [target_start, target_end])
            for row in cur.fetchall():
                kline_map[row['stock_code']].append(row)

        # 3. 计算实际涨跌幅并对比
        results = []
        skipped = 0
        correct_count = 0
        wrong_count = 0

        for code in stock_codes:
            klines = kline_map.get(code, [])
            if len(klines) < 3:
                skipped += 1
                continue

            pcts = [float(k['change_percent']) for k in klines
                    if k.get('change_percent') is not None]
            if not pcts:
                skipped += 1
                continue

            actual_chg = _compound_return(pcts)
            actual_dir = 'UP' if actual_chg >= 0 else 'DOWN'
            pred_dir = pred_map.get(code)
            is_correct = 1 if pred_dir == actual_dir else 0

            if is_correct:
                correct_count += 1
            else:
                wrong_count += 1

            results.append({
                'stock_code': code,
                'nw_actual_direction': actual_dir,
                'nw_actual_weekly_chg': actual_chg,
                'nw_is_correct': is_correct,
            })

        # 4. 回填到预测周 W 的记录
        if results:
            backfill_nw_actual_results(iso_year, iso_week, results)

        verified = len(results)
        accuracy = round(correct_count / verified * 100, 1) if verified > 0 else None

        logger.info("NW验证 Y%d-W%02d→Y%d-W%02d: 验证%d只, 正确%d, 错误%d, 跳过%d, 准确率%s%%",
                    iso_year, iso_week, nw_year, nw_week,
                    verified, correct_count, wrong_count, skipped,
                    accuracy if accuracy is not None else '-')

        return {
            'iso_year': iso_year,
            'iso_week': iso_week,
            'nw_target': f'Y{nw_year}-W{nw_week:02d}',
            'date_range': f'{target_start}~{target_end}',
            'verified': verified,
            'correct': correct_count,
            'wrong': wrong_count,
            'skipped': skipped,
            'accuracy': accuracy,
            'type': 'nw',
        }

    finally:
        cur.close()
        conn.close()


def verify_all_pending_weeks() -> list[dict]:
    """验证所有有待验证预测的历史周，包括下周预测的目标周。

    1. 回填本周预测的 actual_direction（跳过最新一周）
    2. 回填下周预测目标周的 actual_direction（确保 nw 验证可用）

    Returns:
        [{'iso_year': ..., 'iso_week': ..., 'verified': N, ...}, ...]
    """
    results = []

    # ── 1. 本周预测验证 ──
    weeks = _get_unverified_weeks(limit=20)
    if weeks:
        # 跳过最新一周（可能本周还没结束）
        weeks = weeks[1:]

    for w in (weeks or []):
        if w['pending'] == 0:
            continue
        try:
            r = verify_week_predictions(w['iso_year'], w['iso_week'])
            results.append(r)
        except Exception as e:
            logger.error("验证 Y%d-W%02d 失败: %s", w['iso_year'], w['iso_week'], e, exc_info=True)
            results.append({
                'iso_year': w['iso_year'],
                'iso_week': w['iso_week'],
                'error': str(e),
            })

    # ── 2. 下周预测验证 ──
    nw_weeks = _get_nw_unverified_weeks(limit=20)
    # 跳过最新预测周（其目标周可能还没结束）
    if nw_weeks:
        nw_weeks = nw_weeks[1:]
    nw_count = 0
    for w in nw_weeks:
        if w['pending'] == 0:
            continue
        try:
            r = verify_nw_week_predictions(w['iso_year'], w['iso_week'])
            if r.get('verified', 0) > 0:
                results.append(r)
                nw_count += 1
        except Exception as e:
            logger.error("NW验证 Y%d-W%02d 失败: %s",
                        w['iso_year'], w['iso_week'], e, exc_info=True)
            results.append({
                'iso_year': w['iso_year'],
                'iso_week': w['iso_week'],
                'error': str(e),
                'type': 'nw',
            })

    if not results:
        logger.info("无待验证的预测周")

    if nw_count > 0:
        logger.info("额外回填了 %d 个下周预测目标周", nw_count)

    return results
