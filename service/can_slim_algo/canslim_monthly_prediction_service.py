#!/usr/bin/env python3
"""
CAN SLIM 月度预测服务 — 替换原有月度预测
==========================================
使用最优算法组合（67.4%胜率）：
  CAN SLIM综合分 >= 60 + 杯柄形态 + 低波动(vol_20<3.0)
  + RSI<65 + 中低换手(avg_hand_5<8)

数据存储：
  - canslim_monthly_prediction: 最新预测（每只股票一条，UPSERT）
  - canslim_monthly_prediction_history: 历史预测（每次执行追加）
"""
import gc
import json
import logging
import time
from calendar import monthrange
from collections import defaultdict
from datetime import datetime, timedelta

from dao import get_connection
from service.can_slim_algo.can_slim_scorer import (
    score_stock, _sf, _mean, _std, _compound_return,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 300
LOOKBACK_DAYS = 400
BUY_THRESHOLD = 60  # CAN SLIM 综合分阈值

# 最优因子过滤条件
OPTIMAL_VOL_20 = 3.0
OPTIMAL_RSI = 65
OPTIMAL_HAND_5 = 8

_INDEX_MAPPING = {
    "300": "399001.SZ", "301": "399001.SZ",
    "000": "399001.SZ", "001": "399001.SZ", "002": "399001.SZ", "003": "399001.SZ",
    "600": "000001.SH", "601": "000001.SH", "603": "000001.SH", "605": "000001.SH",
    "688": "000001.SH", "689": "000001.SH",
}

# ═══════════════════════════════════════════════════════════
# 建表 DDL
# ═══════════════════════════════════════════════════════════

_CREATE_LATEST_TABLE = """
CREATE TABLE IF NOT EXISTS canslim_monthly_prediction (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
    stock_name VARCHAR(100) COMMENT '股票名称',
    predict_date VARCHAR(20) NOT NULL COMMENT '预测执行日期',
    pred_direction VARCHAR(4) NOT NULL DEFAULT 'UP' COMMENT '预测方向',
    confidence VARCHAR(10) COMMENT '置信度: high/medium/low',
    strategy VARCHAR(30) DEFAULT 'canslim_optimal' COMMENT '策略名',
    reason VARCHAR(300) COMMENT '预测理由',
    composite_score DOUBLE COMMENT 'CAN SLIM综合分',
    this_month_chg DOUBLE COMMENT '当月涨跌幅(%)',
    target_year INT NOT NULL COMMENT '预测目标年',
    target_month INT NOT NULL COMMENT '预测目标月',
    date_range VARCHAR(50) COMMENT '目标月日期范围',
    backtest_accuracy DOUBLE COMMENT '回测准确率(%)',
    backtest_samples INT COMMENT '回测样本数',
    dim_scores TEXT COMMENT '维度评分JSON(C/A/N/S/L/I/M+杯柄+技术因子)',
    actual_next_month_chg DOUBLE COMMENT '实际下月涨跌幅(回填)',
    is_correct TINYINT COMMENT '预测是否正确(回填)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_stock_code (stock_code),
    INDEX idx_predict_date (predict_date),
    INDEX idx_target (target_year, target_month),
    INDEX idx_confidence (confidence),
    INDEX idx_composite (composite_score)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='CAN SLIM月度预测最新结果'
"""

_CREATE_HISTORY_TABLE = """
CREATE TABLE IF NOT EXISTS canslim_monthly_prediction_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(100),
    predict_date VARCHAR(20) NOT NULL COMMENT '预测执行日期',
    pred_direction VARCHAR(4) NOT NULL DEFAULT 'UP' COMMENT '预测方向',
    confidence VARCHAR(10) COMMENT '置信度',
    strategy VARCHAR(30) DEFAULT 'canslim_optimal' COMMENT '策略名',
    reason VARCHAR(300) COMMENT '预测理由',
    composite_score DOUBLE COMMENT 'CAN SLIM综合分',
    this_month_chg DOUBLE COMMENT '当月涨跌幅(%)',
    target_year INT NOT NULL COMMENT '预测目标年',
    target_month INT NOT NULL COMMENT '预测目标月',
    date_range VARCHAR(50) COMMENT '目标月日期范围',
    backtest_accuracy DOUBLE COMMENT '回测准确率(%)',
    backtest_samples INT COMMENT '回测样本数',
    dim_scores TEXT COMMENT '维度评分JSON',
    actual_next_month_chg DOUBLE COMMENT '实际下月涨跌幅(回填)',
    is_correct TINYINT COMMENT '预测是否正确(回填)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_stock_code (stock_code),
    INDEX idx_target (target_year, target_month),
    INDEX idx_predict_date (predict_date),
    UNIQUE KEY uk_stock_target (stock_code, target_year, target_month)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='CAN SLIM月度预测历史'
"""


# ═══════════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════════

def _get_stock_index(code: str) -> str:
    return _INDEX_MAPPING.get(code[:3], "399001.SZ" if code.endswith(".SZ") else "000001.SH")


def _next_month(y, m):
    return (y + 1, 1) if m == 12 else (y, m + 1)


def _get_month_date_range(year, month):
    _, last_day = monthrange(year, month)
    return f"{year}-{month:02d}-01~{year}-{month:02d}-{last_day:02d}"


def _calc_quick_factors(klines: list[dict]) -> dict:
    """快速计算技术因子（vol_20, RSI, avg_hand_5）。"""
    if len(klines) < 60:
        return {}
    closes = [k['close_price'] for k in klines if k['close_price'] > 0]
    pcts = [k['change_percent'] for k in klines]
    hands = [_sf(k.get('change_hand', 0)) for k in klines]
    if len(closes) < 60:
        return {}

    vol_20 = _std(pcts[-20:]) if len(pcts) >= 20 else 99
    avg_hand_5 = _mean(hands[-5:]) if hands else 0

    gains = [max(0, p) for p in pcts[-14:]]
    losses = [max(0, -p) for p in pcts[-14:]]
    avg_gain = _mean(gains) if gains else 0
    avg_loss = _mean(losses) if losses else 1
    rsi = 100 - 100 / (1 + avg_gain / avg_loss) if avg_loss > 0 else 50

    return {
        'vol_20': round(vol_20, 2),
        'avg_hand_5': round(avg_hand_5, 2),
        'rsi': round(rsi, 1),
    }


def _get_this_month_chg(klines: list[dict], year: int, month: int) -> float:
    """计算当月涨跌幅。"""
    prefix = f"{year}-{month:02d}"
    pcts = [_sf(k.get('change_percent', 0)) for k in klines if k['date'].startswith(prefix)]
    return _compound_return(pcts) if pcts else 0.0


# ═══════════════════════════════════════════════════════════
# 数据加载
# ═══════════════════════════════════════════════════════════

def _load_market_klines(start_date: str, end_date: str) -> dict:
    """一次性加载指数K线。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    idx_codes = ['000001.SH', '399001.SZ']
    ph = ','.join(['%s'] * len(idx_codes))
    cur.execute(
        f"SELECT stock_code, `date`, close_price, change_percent, trading_volume "
        f"FROM stock_kline WHERE stock_code IN ({ph}) "
        f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
        idx_codes + [start_date, end_date])
    market_klines = defaultdict(list)
    for r in cur.fetchall():
        d = r['date'] if isinstance(r['date'], str) else str(r['date'])
        market_klines[r['stock_code']].append({
            'date': d, 'close_price': _sf(r['close_price']),
            'change_percent': _sf(r['change_percent']),
            'trading_volume': _sf(r['trading_volume']),
        })
    cur.close()
    conn.close()
    return market_klines


def _load_batch(codes: list[str], start_date: str, end_date: str) -> dict:
    """加载一批股票的K线+财报+资金流。"""
    stock_klines = defaultdict(list)
    finance_data = defaultdict(list)
    fund_flow = defaultdict(list)

    sub_bs = 200
    for si in range(0, len(codes), sub_bs):
        sub_codes = codes[si:si + sub_bs]
        conn = get_connection(use_dict_cursor=True)
        cur = conn.cursor()
        ph = ','.join(['%s'] * len(sub_codes))

        cur.execute(
            f"SELECT stock_code, `date`, open_price, close_price, high_price, "
            f"low_price, trading_volume, change_percent, change_hand "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            sub_codes + [start_date, end_date])
        for r in cur.fetchall():
            d = r['date'] if isinstance(r['date'], str) else str(r['date'])
            stock_klines[r['stock_code']].append({
                'date': d, 'close_price': _sf(r['close_price']),
                'open_price': _sf(r['open_price']),
                'high_price': _sf(r['high_price']),
                'low_price': _sf(r['low_price']),
                'trading_volume': _sf(r['trading_volume']),
                'change_percent': _sf(r['change_percent']),
                'change_hand': _sf(r.get('change_hand', 0)),
            })

        cur.execute(
            f"SELECT stock_code, report_date, data_json "
            f"FROM stock_finance WHERE stock_code IN ({ph}) ORDER BY report_date DESC",
            sub_codes)
        for r in cur.fetchall():
            try:
                data = json.loads(r['data_json']) if isinstance(r['data_json'], str) else r['data_json']
                if isinstance(data, dict):
                    data['报告日期'] = r['report_date']
                    finance_data[r['stock_code']].append(data)
            except (json.JSONDecodeError, TypeError):
                pass

        cur.execute(
            f"SELECT stock_code, `date`, big_net, big_net_pct, main_net_5day, net_flow "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date` DESC",
            sub_codes + [start_date, end_date])
        for r in cur.fetchall():
            d = r['date'] if isinstance(r['date'], str) else str(r['date'])
            fund_flow[r['stock_code']].append({
                'date': d, 'big_net': _sf(r['big_net']),
                'big_net_pct': _sf(r['big_net_pct']),
                'main_net_5day': _sf(r['main_net_5day']),
                'net_flow': _sf(r['net_flow']),
            })

        cur.close()
        conn.close()

    return {'stock_klines': stock_klines, 'finance_data': finance_data, 'fund_flow': fund_flow}


def _load_stock_names(codes: list[str]) -> dict:
    """加载股票名称映射（从K线表取最新记录的stock_name，或从weekly_prediction取）。"""
    if not codes:
        return {}
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = {}
    # 先从 weekly_prediction 取
    for si in range(0, len(codes), 500):
        sub = codes[si:si + 500]
        ph = ','.join(['%s'] * len(sub))
        cur.execute(
            f"SELECT DISTINCT stock_code, stock_name FROM stock_weekly_prediction "
            f"WHERE stock_code IN ({ph}) AND stock_name IS NOT NULL", sub)
        for r in cur.fetchall():
            if r['stock_name']:
                result[r['stock_code']] = r['stock_name']
    # 再从自己的表补充
    try:
        for si in range(0, len(codes), 500):
            sub = [c for c in codes[si:si + 500] if c not in result]
            if not sub:
                continue
            ph = ','.join(['%s'] * len(sub))
            cur.execute(
                f"SELECT stock_code, stock_name FROM canslim_monthly_prediction "
                f"WHERE stock_code IN ({ph}) AND stock_name IS NOT NULL", sub)
            for r in cur.fetchall():
                if r['stock_name'] and r['stock_code'] not in result:
                    result[r['stock_code']] = r['stock_name']
    except Exception:
        pass  # 表可能还不存在
    cur.close()
    conn.close()
    return result


# ═══════════════════════════════════════════════════════════
# 评分与过滤
# ═══════════════════════════════════════════════════════════

def _score_and_filter_stock(code: str, klines: list[dict], market_klines: list[dict],
                            finance: list[dict], fund_flow: list[dict]) -> dict | None:
    """对单只股票评分并应用最优过滤条件。返回预测结果或 None。"""
    if len(klines) < 60:
        return None

    try:
        result = score_stock(code, klines, market_klines, finance, fund_flow)
    except Exception:
        return None

    composite = result.get('composite', 0)
    if composite < BUY_THRESHOLD:
        return None

    has_cup_handle = bool(result.get('cup_handle'))
    if not has_cup_handle:
        return None

    tech = _calc_quick_factors(klines)
    if not tech:
        return None

    vol_20 = tech.get('vol_20', 99)
    rsi = tech.get('rsi', 50)
    avg_hand_5 = tech.get('avg_hand_5', 0)

    if vol_20 >= OPTIMAL_VOL_20 or rsi >= OPTIMAL_RSI or avg_hand_5 >= OPTIMAL_HAND_5:
        return None

    cup_handle = result.get('cup_handle', {})
    ch_score = cup_handle.get('pattern_score', 0)
    breakout = cup_handle.get('breakout', False)
    vol_confirm = cup_handle.get('volume_confirm', False)

    # 置信度
    confidence_score = 0
    if composite >= 75:
        confidence_score += 3
    elif composite >= 65:
        confidence_score += 2
    else:
        confidence_score += 1
    if ch_score >= 70:
        confidence_score += 2
    elif ch_score >= 55:
        confidence_score += 1
    if vol_20 < 2.0:
        confidence_score += 1
    if rsi < 50:
        confidence_score += 1
    if breakout and vol_confirm:
        confidence_score += 1

    if confidence_score >= 6:
        confidence = 'high'
    elif confidence_score >= 4:
        confidence = 'medium'
    else:
        confidence = 'low'

    reason_parts = [
        f"CANSLIM={composite:.0f}",
        f"杯柄={ch_score}分",
        f"波动={vol_20:.1f}%",
        f"RSI={rsi:.0f}",
        f"换手={avg_hand_5:.1f}%",
    ]
    if breakout:
        reason_parts.append("已突破")
    if vol_confirm:
        reason_parts.append("放量确认")

    dim_scores = result.get('dim_scores', {})
    dim_json = {
        'C': dim_scores.get('C', 0),
        'A': dim_scores.get('A', 0),
        'N': dim_scores.get('N', 0),
        'S': dim_scores.get('S', 0),
        'L': dim_scores.get('L', 0),
        'I': dim_scores.get('I', 0),
        'M': dim_scores.get('M', 0),
        'composite': composite,
        'cup_handle_score': ch_score,
        'cup_handle_breakout': breakout,
        'cup_handle_vol_confirm': vol_confirm,
        'cup_depth': cup_handle.get('cup_depth', 0),
        'cup_days': cup_handle.get('cup_days', 0),
        'vol_20': vol_20,
        'rsi': rsi,
        'avg_hand_5': avg_hand_5,
    }

    return {
        'composite': composite,
        'confidence': confidence,
        'reason': ', '.join(reason_parts),
        'dim_json': dim_json,
        'tech': tech,
        'cup_handle': cup_handle,
    }


# ═══════════════════════════════════════════════════════════
# 数据库操作
# ═══════════════════════════════════════════════════════════

def ensure_prediction_tables():
    """确保预测表存在，并迁移缺失列。"""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(_CREATE_LATEST_TABLE)
        cur.execute(_CREATE_HISTORY_TABLE)

        # 迁移：为已有的 history 表补齐可能缺失的列
        _history_migrate_cols = [
            ("pred_direction", "VARCHAR(4) NOT NULL DEFAULT 'UP' COMMENT '预测方向'", "predict_date"),
            ("strategy", "VARCHAR(30) DEFAULT 'canslim_optimal' COMMENT '策略名'", "confidence"),
            ("date_range", "VARCHAR(50) COMMENT '目标月日期范围'", "target_month"),
            ("backtest_accuracy", "DOUBLE COMMENT '回测准确率(%)'", "dim_scores"),
            ("backtest_samples", "INT COMMENT '回测样本数'", "backtest_accuracy"),
        ]
        for col_name, col_def, after_col in _history_migrate_cols:
            try:
                cur.execute(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() AND table_name = 'canslim_monthly_prediction_history' "
                    "AND column_name = %s", (col_name,))
                if cur.fetchone()[0] == 0:
                    cur.execute(
                        f"ALTER TABLE canslim_monthly_prediction_history "
                        f"ADD COLUMN {col_name} {col_def} AFTER {after_col}")
                    logger.info("  已补列: canslim_monthly_prediction_history.%s", col_name)
            except Exception as e:
                logger.warning("  补列失败 %s: %s", col_name, e)

        conn.commit()
        logger.info("CAN SLIM月度预测表已就绪")
    finally:
        cur.close()
        conn.close()


def _batch_upsert_predictions(predictions: list[dict]):
    """批量 UPSERT 到 canslim_monthly_prediction 表。"""
    if not predictions:
        return
    conn = get_connection()
    cur = conn.cursor()
    try:
        sql = """
            INSERT INTO canslim_monthly_prediction
                (stock_code, stock_name, predict_date, pred_direction, confidence,
                 strategy, reason, composite_score, this_month_chg,
                 target_year, target_month, date_range,
                 backtest_accuracy, backtest_samples, dim_scores)
            VALUES
                (%(stock_code)s, %(stock_name)s, %(predict_date)s, %(pred_direction)s, %(confidence)s,
                 %(strategy)s, %(reason)s, %(composite_score)s, %(this_month_chg)s,
                 %(target_year)s, %(target_month)s, %(date_range)s,
                 %(backtest_accuracy)s, %(backtest_samples)s, %(dim_scores)s)
            ON DUPLICATE KEY UPDATE
                stock_name = VALUES(stock_name),
                predict_date = VALUES(predict_date),
                pred_direction = VALUES(pred_direction),
                confidence = VALUES(confidence),
                strategy = VALUES(strategy),
                reason = VALUES(reason),
                composite_score = VALUES(composite_score),
                this_month_chg = VALUES(this_month_chg),
                target_year = VALUES(target_year),
                target_month = VALUES(target_month),
                date_range = VALUES(date_range),
                backtest_accuracy = VALUES(backtest_accuracy),
                backtest_samples = VALUES(backtest_samples),
                dim_scores = VALUES(dim_scores),
                actual_next_month_chg = NULL,
                is_correct = NULL
        """
        cur.executemany(sql, predictions)
        conn.commit()
        logger.info("  UPSERT月度预测: %d 条", len(predictions))
    finally:
        cur.close()
        conn.close()


def _batch_insert_history(predictions: list[dict]):
    """批量写入历史表（UPSERT by stock_code+target_year+target_month）。"""
    if not predictions:
        return
    conn = get_connection()
    cur = conn.cursor()
    try:
        sql = """
            INSERT INTO canslim_monthly_prediction_history
                (stock_code, stock_name, predict_date, pred_direction, confidence,
                 strategy, reason, composite_score, this_month_chg,
                 target_year, target_month, date_range,
                 backtest_accuracy, backtest_samples, dim_scores)
            VALUES
                (%(stock_code)s, %(stock_name)s, %(predict_date)s, %(pred_direction)s, %(confidence)s,
                 %(strategy)s, %(reason)s, %(composite_score)s, %(this_month_chg)s,
                 %(target_year)s, %(target_month)s, %(date_range)s,
                 %(backtest_accuracy)s, %(backtest_samples)s, %(dim_scores)s)
            ON DUPLICATE KEY UPDATE
                stock_name = VALUES(stock_name),
                predict_date = VALUES(predict_date),
                pred_direction = VALUES(pred_direction),
                confidence = VALUES(confidence),
                strategy = VALUES(strategy),
                reason = VALUES(reason),
                composite_score = VALUES(composite_score),
                this_month_chg = VALUES(this_month_chg),
                date_range = VALUES(date_range),
                backtest_accuracy = VALUES(backtest_accuracy),
                backtest_samples = VALUES(backtest_samples),
                dim_scores = VALUES(dim_scores)
        """
        cur.executemany(sql, predictions)
        conn.commit()
        logger.info("  写入月度预测历史: %d 条", len(predictions))
    finally:
        cur.close()
        conn.close()


def _clear_stale_predictions(target_year: int, target_month: int):
    """删除最新表中不属于当前预测周期的旧数据。"""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM canslim_monthly_prediction WHERE target_year != %s OR target_month != %s",
            (target_year, target_month))
        deleted = cur.rowcount
        conn.commit()
        if deleted > 0:
            logger.info("  清理旧周期预测: %d 条", deleted)
    finally:
        cur.close()
        conn.close()


# ═══════════════════════════════════════════════════════════
# 批量预测主函数
# ═══════════════════════════════════════════════════════════

def run_batch_canslim_monthly_prediction(progress_callback=None) -> dict:
    """
    批量 CAN SLIM 月度预测主函数。

    流程：
    1. 获取全部股票代码
    2. 分批加载数据 → 评分 → 过滤
    3. 写入 canslim_monthly_prediction 表（UPSERT）
    4. 写入 canslim_monthly_prediction_history 表
    5. 同步更新 stock_weekly_prediction 的 nm_* 列（兼容旧前端）

    Args:
        progress_callback: (total, done, signal_count) 进度回调
    """
    t_start = time.time()
    logger.info("=" * 70)
    logger.info("  CAN SLIM 月度预测服务启动")
    logger.info("  最优算法: CANSLIM>=60 + 杯柄 + vol_20<%.1f + RSI<%d + 换手<%d",
                OPTIMAL_VOL_20, OPTIMAL_RSI, OPTIMAL_HAND_5)
    logger.info("=" * 70)

    ensure_prediction_tables()

    # 获取全部股票代码
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT stock_code FROM stock_kline WHERE stock_code NOT LIKE '%%.BJ'")
    all_codes = sorted([r['stock_code'] for r in cur.fetchall()])
    cur.execute("SELECT MAX(`date`) as d FROM stock_kline WHERE stock_code IN ('000001.SH','399001.SZ')")
    latest_date = cur.fetchone()['d']
    if isinstance(latest_date, datetime):
        latest_date = latest_date.strftime('%Y-%m-%d')
    elif not isinstance(latest_date, str):
        latest_date = str(latest_date)
    cur.close()
    conn.close()

    all_codes = [c for c in all_codes if c not in ('000001.SH', '399001.SZ', '399006.SZ')]

    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=LOOKBACK_DAYS + 90)
    start_date = dt_start.strftime('%Y-%m-%d')

    current_year, current_month = dt_end.year, dt_end.month
    next_year, next_month = _next_month(current_year, current_month)
    next_month_range = _get_month_date_range(next_year, next_month)

    logger.info("  全量股票: %d只, 最新交易日: %s", len(all_codes), latest_date)
    logger.info("  当前月: %d-%02d, 预测目标月: %d-%02d (%s)",
                current_year, current_month, next_year, next_month, next_month_range)

    market_klines = _load_market_klines(start_date, latest_date)
    logger.info("  指数K线加载完成: %d条", sum(len(v) for v in market_klines.values()))

    stock_names = _load_stock_names(all_codes)

    # 分批处理
    predictions = []
    n_batches = (len(all_codes) + BATCH_SIZE - 1) // BATCH_SIZE
    logger.info("  分 %d 批处理（每批 %d 只）", n_batches, BATCH_SIZE)

    processed = 0
    for bi in range(n_batches):
        batch_codes = all_codes[bi * BATCH_SIZE: (bi + 1) * BATCH_SIZE]
        t_batch = time.time()

        batch_data = _load_batch(batch_codes, start_date, latest_date)

        for code in batch_codes:
            klines = batch_data['stock_klines'].get(code, [])
            if len(klines) < 60:
                continue

            idx_code = _get_stock_index(code)
            mkt_kl = market_klines.get(idx_code, [])
            fin = batch_data['finance_data'].get(code, [])
            ff = batch_data['fund_flow'].get(code, [])

            pred = _score_and_filter_stock(code, klines, mkt_kl, fin, ff)
            if pred:
                this_month_chg = _get_this_month_chg(klines, current_year, current_month)
                predictions.append({
                    'stock_code': code,
                    'stock_name': stock_names.get(code),
                    'predict_date': latest_date,
                    'pred_direction': 'UP',
                    'confidence': pred['confidence'],
                    'strategy': 'canslim_optimal',
                    'reason': pred['reason'][:300],
                    'composite_score': pred['composite'],
                    'this_month_chg': round(this_month_chg, 2),
                    'target_year': next_year,
                    'target_month': next_month,
                    'date_range': next_month_range,
                    'backtest_accuracy': 67.4,
                    'backtest_samples': 476,
                    'dim_scores': json.dumps(pred['dim_json'], ensure_ascii=False),
                })

        del batch_data
        gc.collect()

        processed += len(batch_codes)
        elapsed_b = time.time() - t_batch
        if progress_callback:
            progress_callback(len(all_codes), processed, len(predictions))
        logger.info("  批次 %d/%d (%d只): 累计信号=%d, 耗时=%.1fs",
                    bi + 1, n_batches, len(batch_codes), len(predictions), elapsed_b)

    # ── 写入数据库 ──
    if predictions:
        # 1. 清理旧周期数据，写入最新预测表
        _clear_stale_predictions(next_year, next_month)
        logger.info("  写入 %d 条到 canslim_monthly_prediction...", len(predictions))
        for i in range(0, len(predictions), 500):
            _batch_upsert_predictions(predictions[i:i + 500])

        # 2. 写入历史表
        logger.info("  写入 %d 条到 canslim_monthly_prediction_history...", len(predictions))
        for i in range(0, len(predictions), 500):
            _batch_insert_history(predictions[i:i + 500])

        # 3. 同步更新 stock_weekly_prediction 的 nm_* 列（兼容）
        _sync_to_weekly_prediction(predictions)
    else:
        logger.warning("  无有效月度预测结果")

    elapsed = time.time() - t_start

    logger.info("=" * 70)
    logger.info("  CAN SLIM 月度预测完成")
    logger.info("  预测目标: %d-%02d (%s)", next_year, next_month, next_month_range)
    logger.info("  预测涨: %d 只 (全量 %d 只)", len(predictions), len(all_codes))
    logger.info("  回测准确率: 67.4%% (476样本, 5230只股票, 12个月)")
    logger.info("  耗时: %.1fs (%.1f分钟)", elapsed, elapsed / 60)
    logger.info("=" * 70)

    return {
        'target_year': next_year,
        'target_month': next_month,
        'date_range': next_month_range,
        'total_stocks': len(all_codes),
        'total_predicted': len(predictions),
        'backtest_accuracy': 67.4,
        'backtest_samples': 476,
        'strategy': 'canslim_optimal',
        'elapsed': round(elapsed, 1),
    }


def _sync_to_weekly_prediction(predictions: list[dict]):
    """同步更新 stock_weekly_prediction 的 nm_* 列（兼容旧系统）。"""
    conn = get_connection()
    cur = conn.cursor()
    try:
        # 先清空旧的 nm_* 数据
        cur.execute("""
            UPDATE stock_weekly_prediction SET
                nm_pred_direction = NULL, nm_confidence = NULL, nm_strategy = NULL,
                nm_reason = NULL, nm_composite_score = NULL, nm_this_month_chg = NULL,
                nm_target_year = NULL, nm_target_month = NULL, nm_date_range = NULL,
                nm_backtest_accuracy = NULL, nm_backtest_samples = NULL, nm_dim_scores = NULL
            WHERE nm_pred_direction IS NOT NULL
        """)
        # 更新有记录的股票
        sql = """
            UPDATE stock_weekly_prediction SET
                nm_pred_direction = %(pred_direction)s,
                nm_confidence = %(confidence)s,
                nm_strategy = %(strategy)s,
                nm_reason = %(reason)s,
                nm_composite_score = %(composite_score)s,
                nm_this_month_chg = %(this_month_chg)s,
                nm_target_year = %(target_year)s,
                nm_target_month = %(target_month)s,
                nm_date_range = %(date_range)s,
                nm_backtest_accuracy = %(backtest_accuracy)s,
                nm_backtest_samples = %(backtest_samples)s,
                nm_dim_scores = %(dim_scores)s
            WHERE stock_code = %(stock_code)s
        """
        cur.executemany(sql, predictions)
        conn.commit()
        logger.info("  同步nm_*列: %d 条", len(predictions))
    finally:
        cur.close()
        conn.close()


# ═══════════════════════════════════════════════════════════
# 单股预测
# ═══════════════════════════════════════════════════════════

def predict_single_stock(stock_code: str) -> dict:
    """对单只股票进行 CAN SLIM 月度预测。"""
    from datetime import datetime, timedelta

    ensure_prediction_tables()

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("SELECT MAX(`date`) as d FROM stock_kline WHERE stock_code IN ('000001.SH','399001.SZ')")
    latest_date = cur.fetchone()['d']
    if isinstance(latest_date, datetime):
        latest_date = latest_date.strftime('%Y-%m-%d')
    elif not isinstance(latest_date, str):
        latest_date = str(latest_date)
    cur.close()
    conn.close()

    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=LOOKBACK_DAYS + 90)
    start_date = dt_start.strftime('%Y-%m-%d')

    current_year, current_month = dt_end.year, dt_end.month
    next_year, next_month = _next_month(current_year, current_month)
    next_month_range = _get_month_date_range(next_year, next_month)

    # 加载数据
    market_klines = _load_market_klines(start_date, latest_date)
    batch_data = _load_batch([stock_code], start_date, latest_date)

    klines = batch_data['stock_klines'].get(stock_code, [])
    if len(klines) < 60:
        return {'stock_code': stock_code, 'signal': False, 'reason': '数据不足(K线<60条)'}

    idx_code = _get_stock_index(stock_code)
    mkt_kl = market_klines.get(idx_code, [])
    fin = batch_data['finance_data'].get(stock_code, [])
    ff = batch_data['fund_flow'].get(stock_code, [])

    pred = _score_and_filter_stock(stock_code, klines, mkt_kl, fin, ff)
    if not pred:
        # 即使不满足最优条件，也返回评分信息
        try:
            result = score_stock(stock_code, klines, mkt_kl, fin, ff)
            tech = _calc_quick_factors(klines)
            return {
                'stock_code': stock_code,
                'signal': False,
                'reason': '不满足最优因子条件',
                'composite_score': result.get('composite', 0),
                'has_cup_handle': bool(result.get('cup_handle')),
                'dim_scores': result.get('dim_scores', {}),
                'tech_factors': tech,
                'target_month': f"{next_year}-{next_month:02d}",
            }
        except Exception:
            return {'stock_code': stock_code, 'signal': False, 'reason': '评分失败'}

    this_month_chg = _get_this_month_chg(klines, current_year, current_month)
    names = _load_stock_names([stock_code])

    return {
        'stock_code': stock_code,
        'stock_name': names.get(stock_code),
        'signal': True,
        'pred_direction': 'UP',
        'confidence': pred['confidence'],
        'composite_score': pred['composite'],
        'reason': pred['reason'],
        'dim_scores': pred['dim_json'],
        'this_month_chg': round(this_month_chg, 2),
        'target_year': next_year,
        'target_month': next_month,
        'date_range': next_month_range,
        'backtest_accuracy': 67.4,
        'backtest_samples': 476,
    }


# ═══════════════════════════════════════════════════════════
# 查询历史
# ═══════════════════════════════════════════════════════════

def get_monthly_prediction_history(stock_code: str = None,
                                   target_year: int = None,
                                   target_month: int = None,
                                   limit: int = 50) -> list[dict]:
    """查询月度预测历史。"""
    ensure_prediction_tables()
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        where_parts = []
        params = []
        if stock_code:
            where_parts.append("stock_code = %s")
            params.append(stock_code)
        if target_year:
            where_parts.append("target_year = %s")
            params.append(target_year)
        if target_month:
            where_parts.append("target_month = %s")
            params.append(target_month)

        where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
        cur.execute(f"""
            SELECT * FROM canslim_monthly_prediction_history
            {where_sql}
            ORDER BY target_year DESC, target_month DESC, composite_score DESC
            LIMIT %s
        """, params + [limit])
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


# ═══════════════════════════════════════════════════════════
# 从新表查询最新预测（供 API 使用）
# ═══════════════════════════════════════════════════════════

def get_canslim_predictions_page(confidence: str = None,
                                 keywords: list[str] = None,
                                 sort_by: str = 'composite_score',
                                 sort_dir: str = 'desc',
                                 limit: int = 2000,
                                 offset: int = 0) -> tuple[list[dict], int]:
    """从 canslim_monthly_prediction 表分页查询最新预测。"""
    ensure_prediction_tables()
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        where_parts = []
        params = []

        if confidence:
            where_parts.append("confidence = %s")
            params.append(confidence)

        if keywords:
            or_clauses = []
            for term in keywords:
                or_clauses.append("(stock_code LIKE %s OR stock_name LIKE %s)")
                params.extend([f"%{term}%", f"%{term}%"])
            where_parts.append("(" + " OR ".join(or_clauses) + ")")

        where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        allowed_sorts = {
            'stock_code', 'stock_name', 'composite_score', 'confidence',
            'predict_date', 'backtest_accuracy', 'this_month_chg',
            'target_year', 'target_month',
        }
        if sort_by not in allowed_sorts:
            sort_by = 'composite_score'
        order_dir = 'DESC' if sort_dir.lower() == 'desc' else 'ASC'

        cur.execute(f"SELECT COUNT(*) as cnt FROM canslim_monthly_prediction {where_sql}", params)
        total = cur.fetchone()['cnt']

        cur.execute(f"""
            SELECT stock_code, stock_name, predict_date, pred_direction, confidence,
                   strategy, reason, composite_score, this_month_chg,
                   target_year, target_month, date_range,
                   backtest_accuracy, backtest_samples, dim_scores,
                   actual_next_month_chg, is_correct
            FROM canslim_monthly_prediction
            {where_sql}
            ORDER BY {sort_by} {order_dir}
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        rows = cur.fetchall()
        return rows, total
    finally:
        cur.close()
        conn.close()


def get_canslim_prediction_summary() -> dict:
    """从 canslim_monthly_prediction 表获取月度预测汇总统计。"""
    ensure_prediction_tables()
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT
                COUNT(*) as nm_total,
                SUM(pred_direction = 'UP') as nm_up_count,
                ROUND(AVG(backtest_accuracy), 1) as nm_avg_backtest_accuracy,
                ROUND(AVG(backtest_samples), 0) as nm_avg_backtest_samples,
                MAX(date_range) as nm_date_range,
                MAX(target_year) as nm_target_year,
                MAX(target_month) as nm_target_month,
                SUM(confidence = 'high') as nm_high_count,
                SUM(confidence = 'medium') as nm_medium_count,
                SUM(confidence = 'low') as nm_low_count,
                ROUND(AVG(composite_score), 1) as nm_avg_composite_score
            FROM canslim_monthly_prediction
        """)
        return cur.fetchone() or {}
    except Exception:
        return {}
    finally:
        cur.close()
        conn.close()


# ═══════════════════════════════════════════════════════════
# CLI 入口
# ═══════════════════════════════════════════════════════════

if __name__ == '__main__':
    import sys
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    if len(sys.argv) > 1 and sys.argv[1] == 'single':
        code = sys.argv[2] if len(sys.argv) > 2 else '600519.SH'
        result = predict_single_stock(code)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    else:
        result = run_batch_canslim_monthly_prediction()
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
