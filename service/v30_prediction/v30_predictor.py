#!/usr/bin/env python3
"""
V30 预测器 — 对接生产环境
==========================
将V30引擎包装为可被 weekly_prediction_service 调用的接口，
输出格式与 v5_tech_predictor / v12_engine 一致。

输出字段（写入 stock_weekly_prediction 表的 v30_* 列）：
  v30_pred_direction  — 预测方向: UP / None
  v30_confidence      — 置信度: high/medium/low
  v30_strategy        — 策略名: v30_sentiment
  v30_reason          — 预测理由
  v30_composite_score — 综合评分
  v30_sent_agree      — 情绪因子看涨数
  v30_tech_agree      — 技术因子看涨数
  v30_mkt_ret_20d     — 大盘20日涨幅

用法：
    from service.v30_prediction.v30_predictor import batch_predict_v30
    results = batch_predict_v30(stock_codes, latest_date)
    # results: {code: {v30_pred_direction, v30_confidence, ...}}
"""
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta

from dao import get_connection
from service.v30_prediction.v30_engine import V30Engine, _f

logger = logging.getLogger(__name__)

# 全局引擎实例（避免重复训练）
_engine: V30Engine = None
_engine_train_date: str = None


def _load_klines_raw(codes: list[str], start: str, end: str) -> dict:
    """加载K线原始数据，转为V30引擎所需格式。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    res = defaultdict(list)
    for i in range(0, len(codes), 300):
        batch = codes[i:i + 300]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,close_price,open_price,high_price,"
            f"low_price,trading_volume,change_percent,change_hand "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
            batch + [start, end])
        for r in cur.fetchall():
            res[r['stock_code']].append({
                'd': str(r['date']),
                'c': _f(r['close_price']),
                'o': _f(r['open_price']),
                'h': _f(r['high_price']),
                'l': _f(r['low_price']),
                'v': _f(r['trading_volume']),
                'p': _f(r['change_percent']),
                't': _f(r.get('change_hand')),
            })
    cur.close()
    conn.close()
    return dict(res)


def _load_fund_flow_raw(codes: list[str], start: str) -> dict:
    """加载资金流数据。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    res = defaultdict(list)
    for i in range(0, len(codes), 300):
        batch = codes[i:i + 300]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,big_net_pct,small_net_pct,net_flow "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s ORDER BY `date`",
            batch + [start])
        for r in cur.fetchall():
            res[r['stock_code']].append({
                'd': str(r['date']),
                'bn': _f(r.get('big_net_pct')),
                'sn': _f(r.get('small_net_pct')),
                'nf': _f(r.get('net_flow')),
            })
    cur.close()
    conn.close()
    return dict(res)


def _load_market_raw(start: str, end: str) -> dict:
    """加载大盘K线，返回 {date_str: {c, p}}。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute(
        "SELECT `date`,close_price,change_percent FROM stock_kline "
        "WHERE stock_code='000001.SH' AND `date`>=%s AND `date`<=%s ORDER BY `date`",
        (start, end))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {str(r['date']): {'c': _f(r['close_price']), 'p': _f(r['change_percent'])} for r in rows}


def _get_or_train_engine(codes: list[str], latest_date: str,
                        progress_callback=None) -> V30Engine:
    """获取或训练V30引擎（同一天只训练一次）。"""
    global _engine, _engine_train_date

    if _engine is not None and _engine_train_date == latest_date:
        return _engine

    logger.info("V30: 训练引擎 (latest_date=%s)...", latest_date)
    t0 = time.time()

    end_date = latest_date
    start_date = (datetime.strptime(latest_date, '%Y-%m-%d') - timedelta(days=500)).strftime('%Y-%m-%d')

    if progress_callback:
        progress_callback('loading', 0, len(codes))
    kdata = _load_klines_raw(codes, start_date, end_date)
    ffdata = _load_fund_flow_raw(codes, start_date)
    mkt_by_date = _load_market_raw(start_date, end_date)

    if progress_callback:
        progress_callback('training', 0, len(codes))
    engine = V30Engine()
    engine.train(kdata, ffdata, mkt_by_date)

    # 缓存
    _engine = engine
    _engine_train_date = latest_date
    # 同时缓存数据供predict使用
    _engine._cached_kdata = kdata
    _engine._cached_ffdata = ffdata
    _engine._cached_mkt = mkt_by_date

    logger.info("V30: 训练完成, 耗时%.1fs", time.time() - t0)
    return engine


def predict_single_v30(stock_code: str, latest_date: str,
                       engine: V30Engine = None) -> dict:
    """
    预测单只股票，返回v30_*字段字典。

    Args:
        stock_code: 股票代码（如 '000001.SZ'）
        latest_date: 最新交易日（如 '2026-03-24'）
        engine: 已训练的V30引擎（可选，不传则自动获取缓存）

    Returns:
        dict with v30_* keys
    """
    if engine is None:
        engine = _engine
    if engine is None or not engine.trained:
        return _empty_v30_result()

    klines = engine._cached_kdata.get(stock_code, [])
    ff = engine._cached_ffdata.get(stock_code, [])
    mkt = engine._cached_mkt

    if not klines:
        return _empty_v30_result()

    pred = engine.predict_single(stock_code, klines, ff, mkt)
    if pred is None:
        return _empty_v30_result()

    return {
        'v30_pred_direction': pred['pred_direction'],
        'v30_confidence': pred['confidence'],
        'v30_strategy': 'v30_sentiment' if pred['pred_direction'] else None,
        'v30_reason': pred.get('reason') or pred.get('filter_reason'),
        'v30_composite_score': pred.get('composite_score'),
        'v30_sent_agree': pred.get('sent_agree'),
        'v30_tech_agree': pred.get('tech_agree'),
        'v30_mkt_ret_20d': pred.get('mkt_ret_20d'),
    }


def batch_predict_v30(stock_codes: list[str], latest_date: str,
                      progress_callback=None) -> dict:
    """
    批量V30预测，对接 weekly_prediction_service。

    Args:
        stock_codes: 股票代码列表
        latest_date: 最新交易日
        progress_callback: 可选回调函数，接收 (done_count, total_count)

    Returns:
        {stock_code: {v30_pred_direction, v30_confidence, ...}}
    """
    t0 = time.time()
    logger.info("V30: 批量预测 %d 只股票...", len(stock_codes))

    def _train_progress(phase, done, total):
        if progress_callback:
            progress_callback(-1, total)  # -1 表示训练阶段

    engine = _get_or_train_engine(stock_codes, latest_date, progress_callback=_train_progress)
    results = {}
    signal_count = 0
    high_count = 0
    total = len(stock_codes)

    for idx, code in enumerate(stock_codes):
        r = predict_single_v30(code, latest_date, engine)
        results[code] = r
        if r.get('v30_pred_direction'):
            signal_count += 1
            if r.get('v30_confidence') == 'high':
                high_count += 1
        if progress_callback and (idx + 1) % 50 == 0:
            progress_callback(idx + 1, total)

    if progress_callback:
        progress_callback(total, total)

    elapsed = time.time() - t0
    logger.info("V30: 预测完成, %d只有信号(高置信%d), 覆盖率%.1f%%, 耗时%.1fs",
                signal_count, high_count,
                signal_count / total * 100 if total else 0,
                elapsed)
    return results


def _empty_v30_result() -> dict:
    """返回空的V30结果字典。"""
    return {
        'v30_pred_direction': None,
        'v30_confidence': None,
        'v30_strategy': None,
        'v30_reason': None,
        'v30_composite_score': None,
        'v30_sent_agree': None,
        'v30_tech_agree': None,
        'v30_mkt_ret_20d': None,
    }
