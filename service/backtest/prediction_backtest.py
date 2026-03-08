"""
预测回测验证模块

从 stock_kline_screening_history 读取历史预测记录，
与 stock_kline 中的实际K线数据对比，计算预测准确率。

回测维度：
1. 方向准确率：预测"上涨"且实际涨 / 预测"下跌"且实际跌
2. 区间命中率：实际涨跌幅落在预测区间内
3. 按评分区间分组的准确率（用于校准概率模型）
4. 按置信度分组的准确率
"""
import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

from dao import get_connection
from dao.stock_kline_dao import get_kline_data

logger = logging.getLogger(__name__)


# ── 工具函数 ──────────────────────────────────────────────

def _parse_prediction_json(raw) -> Optional[dict]:
    """将数据库中的预测字段解析为 dict"""
    if not raw:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return None
    return None


def _parse_range(range_str: str) -> tuple[Optional[float], Optional[float]]:
    """解析预测涨跌幅区间字符串，如 '+0.5% ~ +2.0%' → (0.5, 2.0)"""
    if not range_str:
        return None, None
    # 匹配形如 +0.5% ~ +2.0% 或 -1.0% ~ +0.5%
    nums = re.findall(r'[+-]?\d+\.?\d*', range_str)
    if len(nums) >= 2:
        try:
            return float(nums[0]), float(nums[1])
        except ValueError:
            return None, None
    return None, None


def _normalize_stock_code(stock_code: str) -> str:
    """统一股票代码格式，保留原始格式（如 '002008.SZ'）。
    stock_kline 表中 stock_code 存储格式为 '002008.SZ'，不能去掉后缀。
    """
    return stock_code.strip()


def _get_next_n_trading_days(stock_code: str, base_date: str, n: int) -> list[dict]:
    """获取 base_date 之后的 n 个交易日K线数据（用于验证预测）"""
    code = _normalize_stock_code(stock_code)
    # 查询 base_date 之后的数据，多取一些以应对停牌
    all_kline = get_kline_data(code, start_date=base_date, limit=n + 10)
    # 过滤掉 base_date 当天及之前的数据，只保留之后的
    future = [k for k in all_kline if k['date'] > base_date]
    # 过滤停牌日（成交量为0）
    trading = [k for k in future if (k.get('trading_volume') or 0) > 0]
    return trading[:n]


def _get_close_on_date(stock_code: str, target_date: str) -> Optional[float]:
    """获取指定日期的收盘价"""
    code = _normalize_stock_code(stock_code)
    rows = get_kline_data(code, start_date=target_date, end_date=target_date, limit=1)
    if rows:
        return rows[0].get('close_price')
    return None


# ── 核心回测逻辑 ──────────────────────────────────────────

def _evaluate_single_prediction(
    prediction: dict,
    base_close: float,
    actual_klines: list[dict],
    prediction_type: str = 'day',
) -> dict:
    """评估单条预测的准确性

    Args:
        prediction: 预测数据 {direction, range, confidence, probability, rationale, ...}
        base_close: 预测日的收盘价（基准价）
        actual_klines: 预测期间的实际K线数据
        prediction_type: 'day'=次日预测, 'week'=一周预测

    Returns:
        评估结果 dict
    """
    if not actual_klines or not base_close:
        return {'状态': '无实际数据', '可评估': False}

    pred_direction = prediction.get('direction', '')
    pred_range_str = prediction.get('range', '')
    pred_confidence = prediction.get('confidence', '')
    pred_probability = prediction.get('probability', '')
    range_low, range_high = _parse_range(pred_range_str)

    if prediction_type == 'day':
        # 次日预测：取第1个交易日
        target_kline = actual_klines[0]
        actual_close = target_kline.get('close_price', 0)
        actual_change_pct = round((actual_close - base_close) / base_close * 100, 2)
        actual_date = target_kline.get('date', '')
    else:
        # 一周预测：取第5个交易日（或最后一个可用的）
        idx = min(4, len(actual_klines) - 1)
        target_kline = actual_klines[idx]
        actual_close = target_kline.get('close_price', 0)
        actual_change_pct = round((actual_close - base_close) / base_close * 100, 2)
        actual_date = target_kline.get('date', '')
        # 一周内的最高/最低涨跌幅
        week_klines = actual_klines[:idx + 1]
        week_highs = [round((k['high_price'] - base_close) / base_close * 100, 2)
                      for k in week_klines if k.get('high_price')]
        week_lows = [round((k['low_price'] - base_close) / base_close * 100, 2)
                     for k in week_klines if k.get('low_price')]

    # 方向判定
    if actual_change_pct > 0.3:
        actual_direction = '上涨'
    elif actual_change_pct < -0.3:
        actual_direction = '下跌'
    else:
        actual_direction = '横盘震荡'

    # 方向是否正确
    direction_correct = False
    if '上涨' in pred_direction and actual_direction == '上涨':
        direction_correct = True
    elif '下跌' in pred_direction and actual_direction == '下跌':
        direction_correct = True
    elif '震荡' in pred_direction and actual_direction == '横盘震荡':
        direction_correct = True

    # 宽松方向判定（预测上涨但实际横盘也算半对）
    direction_loose_correct = direction_correct
    if not direction_correct:
        if '上涨' in pred_direction and actual_change_pct >= 0:
            direction_loose_correct = True
        elif '下跌' in pred_direction and actual_change_pct <= 0:
            direction_loose_correct = True

    # 区间命中
    range_hit = False
    if range_low is not None and range_high is not None:
        range_hit = range_low <= actual_change_pct <= range_high

    result = {
        '可评估': True,
        '预测方向': pred_direction,
        '预测区间': pred_range_str,
        '预测置信度': pred_confidence,
        '预测概率': pred_probability,
        '实际日期': actual_date,
        '实际涨跌幅(%)': actual_change_pct,
        '实际方向': actual_direction,
        '方向正确': direction_correct,
        '方向宽松正确': direction_loose_correct,
        '区间命中': range_hit,
    }

    if prediction_type == 'week':
        result['周内最高涨幅(%)'] = max(week_highs) if week_highs else None
        result['周内最低跌幅(%)'] = min(week_lows) if week_lows else None
        result['实际交易日数'] = len(week_klines)

    return result


# ── 批量回测 ──────────────────────────────────────────────

def fetch_all_predictions(batch_id: int = None, limit: int = 500) -> list[dict]:
    """从数据库获取所有历史预测记录

    Args:
        batch_id: 可选，指定批次ID；None则查询所有批次
        limit: 最大返回条数
    """
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        sql = """
            SELECT h.id, h.batch_id, h.stock_id, h.stock_name, h.stock_code,
                   h.screen_date, h.kline_total_score,
                   h.next_day_prediction, h.next_week_prediction
            FROM stock_kline_screening_history h
            WHERE h.next_day_prediction IS NOT NULL
        """
        params = []
        if batch_id:
            sql += " AND h.batch_id = %s"
            params.append(batch_id)
        sql += " ORDER BY h.screen_date DESC LIMIT %s"
        params.append(limit)

        cursor.execute(sql, params)
        return list(cursor.fetchall())
    except Exception as e:
        logger.error("fetch_all_predictions 失败: %s", e)
        return []
    finally:
        cursor.close()
        conn.close()


def run_backtest(batch_id: int = None, limit: int = 500) -> dict:
    """执行预测回测

    Args:
        batch_id: 可选，指定批次ID
        limit: 最大回测条数

    Returns:
        回测结果汇总
    """
    predictions = fetch_all_predictions(batch_id, limit)
    if not predictions:
        return {'状态': '无历史预测数据', '总记录数': 0}

    day_results = []
    week_results = []
    # 按评分区间分组
    score_buckets = defaultdict(lambda: {'day_correct': 0, 'day_total': 0,
                                          'week_correct': 0, 'week_total': 0})
    # 按置信度分组
    confidence_buckets = defaultdict(lambda: {'day_correct': 0, 'day_total': 0,
                                               'week_correct': 0, 'week_total': 0})

    for record in predictions:
        stock_code = record.get('stock_code', '')
        screen_date = record.get('screen_date', '')
        total_score = record.get('kline_total_score') or 50

        if not stock_code or not screen_date:
            continue

        # 获取预测日的收盘价作为基准
        base_close = _get_close_on_date(stock_code, screen_date)
        if not base_close:
            continue

        # 获取预测日之后的交易日数据
        future_klines = _get_next_n_trading_days(stock_code, screen_date, 6)
        if not future_klines:
            continue

        # 评分区间 bucket
        if total_score >= 70:
            bucket = '70-100'
        elif total_score >= 55:
            bucket = '55-69'
        elif total_score >= 40:
            bucket = '40-54'
        else:
            bucket = '0-39'

        # ── 次日预测回测 ──
        day_pred = _parse_prediction_json(record.get('next_day_prediction'))
        if day_pred and len(future_klines) >= 1:
            day_eval = _evaluate_single_prediction(day_pred, base_close, future_klines, 'day')
            if day_eval.get('可评估'):
                day_eval['stock_code'] = stock_code
                day_eval['stock_name'] = record.get('stock_name', '')
                day_eval['screen_date'] = screen_date
                day_eval['kline_total_score'] = total_score
                day_results.append(day_eval)

                score_buckets[bucket]['day_total'] += 1
                if day_eval['方向正确']:
                    score_buckets[bucket]['day_correct'] += 1

                conf = day_pred.get('confidence', '中')
                confidence_buckets[conf]['day_total'] += 1
                if day_eval['方向正确']:
                    confidence_buckets[conf]['day_correct'] += 1

        # ── 一周预测回测 ──
        week_pred = _parse_prediction_json(record.get('next_week_prediction'))
        if week_pred and len(future_klines) >= 3:
            week_eval = _evaluate_single_prediction(week_pred, base_close, future_klines, 'week')
            if week_eval.get('可评估'):
                week_eval['stock_code'] = stock_code
                week_eval['stock_name'] = record.get('stock_name', '')
                week_eval['screen_date'] = screen_date
                week_eval['kline_total_score'] = total_score
                week_results.append(week_eval)

                score_buckets[bucket]['week_total'] += 1
                if week_eval['方向正确']:
                    score_buckets[bucket]['week_correct'] += 1

                conf = week_pred.get('confidence', '中')
                confidence_buckets[conf]['week_total'] += 1
                if week_eval['方向正确']:
                    confidence_buckets[conf]['week_correct'] += 1

    return _build_backtest_summary(day_results, week_results, score_buckets, confidence_buckets)


def _build_backtest_summary(
    day_results: list[dict],
    week_results: list[dict],
    score_buckets: dict,
    confidence_buckets: dict,
) -> dict:
    """构建回测结果汇总"""

    def _calc_rate(correct, total):
        if total == 0:
            return '无数据'
        return f'{correct}/{total}（{round(correct / total * 100, 1)}%）'

    def _aggregate(results: list[dict]) -> dict:
        if not results:
            return {'总数': 0, '方向准确率': '无数据', '宽松准确率': '无数据', '区间命中率': '无数据'}
        total = len(results)
        dir_correct = sum(1 for r in results if r.get('方向正确'))
        loose_correct = sum(1 for r in results if r.get('方向宽松正确'))
        range_hit = sum(1 for r in results if r.get('区间命中'))
        avg_actual = round(sum(r.get('实际涨跌幅(%)', 0) for r in results) / total, 2)

        # 按预测方向分组统计准确率
        direction_breakdown = defaultdict(lambda: {'correct': 0, 'total': 0})
        for r in results:
            pred_dir = r.get('预测方向', '未知')
            direction_breakdown[pred_dir]['total'] += 1
            if r.get('方向正确'):
                direction_breakdown[pred_dir]['correct'] += 1

        dir_breakdown_summary = {}
        for d, stats in direction_breakdown.items():
            dir_breakdown_summary[d] = _calc_rate(stats['correct'], stats['total'])

        # 预测概率 vs 实际准确率对比（概率校准分析）
        prob_buckets = defaultdict(lambda: {'correct': 0, 'total': 0})
        for r in results:
            prob_str = r.get('预测概率', '')
            try:
                prob_val = float(str(prob_str).replace('%', ''))
                if prob_val >= 70:
                    pb = '≥70%'
                elif prob_val >= 60:
                    pb = '60-69%'
                elif prob_val >= 55:
                    pb = '55-59%'
                else:
                    pb = '<55%'
                prob_buckets[pb]['total'] += 1
                if r.get('方向正确'):
                    prob_buckets[pb]['correct'] += 1
            except (ValueError, TypeError):
                pass

        prob_calibration = {}
        for pb in ['≥70%', '60-69%', '55-59%', '<55%']:
            if pb in prob_buckets:
                prob_calibration[pb] = {
                    '预测准确率': _calc_rate(prob_buckets[pb]['correct'], prob_buckets[pb]['total']),
                    '样本数': prob_buckets[pb]['total'],
                }

        return {
            '总数': total,
            '方向准确率': _calc_rate(dir_correct, total),
            '方向准确率_数值': round(dir_correct / total * 100, 1) if total else 0,
            '宽松准确率': _calc_rate(loose_correct, total),
            '区间命中率': _calc_rate(range_hit, total),
            '平均实际涨跌幅(%)': avg_actual,
            '按预测方向分组准确率': dir_breakdown_summary,
            '预测概率vs实际准确率': prob_calibration,
        }

    # 按评分区间汇总
    score_summary = {}
    for bucket in ['70-100', '55-69', '40-54', '0-39']:
        b = score_buckets.get(bucket, {})
        score_summary[bucket] = {
            '次日方向准确率': _calc_rate(b.get('day_correct', 0), b.get('day_total', 0)),
            '一周方向准确率': _calc_rate(b.get('week_correct', 0), b.get('week_total', 0)),
            '次日样本数': b.get('day_total', 0),
            '一周样本数': b.get('week_total', 0),
        }

    # 按置信度汇总
    conf_summary = {}
    for conf in ['高', '中', '低']:
        b = confidence_buckets.get(conf, {})
        conf_summary[conf] = {
            '次日方向准确率': _calc_rate(b.get('day_correct', 0), b.get('day_total', 0)),
            '一周方向准确率': _calc_rate(b.get('week_correct', 0), b.get('week_total', 0)),
            '次日样本数': b.get('day_total', 0),
            '一周样本数': b.get('week_total', 0),
        }

    return {
        '回测时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        '总预测记录数': len(day_results) + len(week_results),
        '次日预测回测': _aggregate(day_results),
        '一周预测回测': _aggregate(week_results),
        '按评分区间': score_summary,
        '按置信度': conf_summary,
        '校准数据': _build_calibration_data(day_results, week_results, score_buckets),
        '次日预测明细（最近20条）': day_results[:20],
        '一周预测明细（最近10条）': week_results[:10],
    }


def _build_calibration_data(
    day_results: list[dict],
    week_results: list[dict],
    score_buckets: dict,
) -> dict:
    """构建概率校准数据，用于反馈到 _compute_prediction_probability

    将回测实际准确率与模型预估概率对比，输出校准建议。
    增强版：额外分析预测概率与实际准确率的偏差，提供更精确的校准方向。
    """
    calibration = {}

    # 按评分偏离度分组计算实际准确率
    deviation_buckets = defaultdict(lambda: {'correct': 0, 'total': 0})
    for r in day_results:
        score = r.get('kline_total_score', 50)
        dev = abs(score - 50)
        if dev >= 30:
            bucket = '偏离≥30'
        elif dev >= 20:
            bucket = '偏离20-29'
        elif dev >= 10:
            bucket = '偏离10-19'
        elif dev >= 5:
            bucket = '偏离5-9'
        else:
            bucket = '偏离<5'
        deviation_buckets[bucket]['total'] += 1
        if r.get('方向正确'):
            deviation_buckets[bucket]['correct'] += 1

    for bucket_name, data in deviation_buckets.items():
        total = data['total']
        correct = data['correct']
        actual_rate = round(correct / total * 100, 1) if total > 0 else None
        calibration[bucket_name] = {
            '样本数': total,
            '实际准确率': f'{actual_rate}%' if actual_rate is not None else '无数据',
            '实际准确率_数值': actual_rate,
        }

    # 模型预估 vs 实际对比
    model_estimates = {
        '偏离≥30': 70.0,
        '偏离20-29': 65.0,
        '偏离10-19': 60.0,
        '偏离5-9': 55.0,
        '偏离<5': 50.0,
    }
    for bucket_name, est in model_estimates.items():
        if bucket_name in calibration and calibration[bucket_name]['实际准确率_数值'] is not None:
            actual = calibration[bucket_name]['实际准确率_数值']
            diff = round(actual - est, 1)
            calibration[bucket_name]['模型预估'] = f'{est}%'
            calibration[bucket_name]['偏差'] = f'{diff:+.1f}%'
            if abs(diff) > 5:
                calibration[bucket_name]['校准建议'] = f'模型{"偏保守" if diff > 0 else "偏乐观"}，建议调整基准概率{diff:+.1f}%'
            else:
                calibration[bucket_name]['校准建议'] = '模型校准良好，无需调整'

    # ── 增强：按一致性维度分析准确率 ──
    consistency_analysis = defaultdict(lambda: {'correct': 0, 'total': 0})
    for r in day_results:
        # 从预测概率推断一致性等级
        prob_str = r.get('预测概率', '')
        try:
            prob_val = float(str(prob_str).replace('%', ''))
            if prob_val >= 70:
                level = '高置信度(≥70%)'
            elif prob_val >= 60:
                level = '中置信度(60-69%)'
            else:
                level = '低置信度(<60%)'
            consistency_analysis[level]['total'] += 1
            if r.get('方向正确'):
                consistency_analysis[level]['correct'] += 1
        except (ValueError, TypeError):
            pass

    calibration['置信度分层准确率'] = {}
    for level in ['高置信度(≥70%)', '中置信度(60-69%)', '低置信度(<60%)']:
        data = consistency_analysis.get(level, {'correct': 0, 'total': 0})
        if data['total'] > 0:
            rate = round(data['correct'] / data['total'] * 100, 1)
            calibration['置信度分层准确率'][level] = {
                '样本数': data['total'],
                '实际准确率': f'{rate}%',
            }

    return calibration


# ── 单只股票回测 ──────────────────────────────────────────

def run_stock_backtest(stock_code: str, batch_id: int = None, limit: int = 50) -> dict:
    """对单只股票执行预测回测

    Args:
        stock_code: 股票代码
        batch_id: 可选批次ID
        limit: 最大回测条数
    """
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        sql = """
            SELECT h.id, h.batch_id, h.stock_id, h.stock_name, h.stock_code,
                   h.screen_date, h.kline_total_score,
                   h.next_day_prediction, h.next_week_prediction
            FROM stock_kline_screening_history h
            WHERE h.stock_code = %s AND h.next_day_prediction IS NOT NULL
        """
        params = [stock_code]
        if batch_id:
            sql += " AND h.batch_id = %s"
            params.append(batch_id)
        sql += " ORDER BY h.screen_date DESC LIMIT %s"
        params.append(limit)

        cursor.execute(sql, params)
        predictions = list(cursor.fetchall())
    except Exception as e:
        logger.error("run_stock_backtest 查询失败: %s", e)
        predictions = []
    finally:
        cursor.close()
        conn.close()

    if not predictions:
        return {'状态': '该股票无历史预测数据', 'stock_code': stock_code, '总记录数': 0}

    day_results = []
    week_results = []

    for record in predictions:
        screen_date = record.get('screen_date', '')
        total_score = record.get('kline_total_score') or 50

        base_close = _get_close_on_date(stock_code, screen_date)
        if not base_close:
            continue

        future_klines = _get_next_n_trading_days(stock_code, screen_date, 6)
        if not future_klines:
            continue

        day_pred = _parse_prediction_json(record.get('next_day_prediction'))
        if day_pred and len(future_klines) >= 1:
            day_eval = _evaluate_single_prediction(day_pred, base_close, future_klines, 'day')
            if day_eval.get('可评估'):
                day_eval['screen_date'] = screen_date
                day_eval['kline_total_score'] = total_score
                day_results.append(day_eval)

        week_pred = _parse_prediction_json(record.get('next_week_prediction'))
        if week_pred and len(future_klines) >= 3:
            week_eval = _evaluate_single_prediction(week_pred, base_close, future_klines, 'week')
            if week_eval.get('可评估'):
                week_eval['screen_date'] = screen_date
                week_eval['kline_total_score'] = total_score
                week_results.append(week_eval)

    def _agg(results):
        if not results:
            return {'总数': 0, '方向准确率': '无数据'}
        total = len(results)
        correct = sum(1 for r in results if r.get('方向正确'))
        loose = sum(1 for r in results if r.get('方向宽松正确'))
        rng = sum(1 for r in results if r.get('区间命中'))
        return {
            '总数': total,
            '方向准确率': f'{correct}/{total}（{round(correct / total * 100, 1)}%）',
            '宽松准确率': f'{loose}/{total}（{round(loose / total * 100, 1)}%）',
            '区间命中率': f'{rng}/{total}（{round(rng / total * 100, 1)}%）',
        }

    return {
        'stock_code': stock_code,
        'stock_name': predictions[0].get('stock_name', '') if predictions else '',
        '次日预测回测': _agg(day_results),
        '一周预测回测': _agg(week_results),
        '次日明细': day_results,
        '一周明细': week_results,
    }


# ── 获取校准后的概率参数 ──────────────────────────────────

def get_calibrated_probability_params(batch_id: int = None) -> Optional[dict]:
    """基于回测结果，返回校准后的概率参数

    如果回测样本量足够（≥30条），返回校准后的基准概率映射表；
    否则返回 None，使用默认参数。
    """
    result = run_backtest(batch_id, limit=500)
    calibration = result.get('校准数据', {})

    if not calibration:
        return None

    # 检查样本量是否足够
    total_samples = sum(c.get('样本数', 0) for c in calibration.values())
    if total_samples < 30:
        logger.info("回测样本量不足（%d条），使用默认概率参数", total_samples)
        return None

    # 构建校准后的基准概率映射
    calibrated = {}
    default_map = {
        '偏离≥30': 0.70,
        '偏离20-29': 0.65,
        '偏离10-19': 0.60,
        '偏离5-9': 0.55,
        '偏离<5': 0.50,
    }
    for bucket, default_prob in default_map.items():
        if bucket in calibration and calibration[bucket].get('实际准确率_数值') is not None:
            actual = calibration[bucket]['实际准确率_数值'] / 100
            samples = calibration[bucket].get('样本数', 0)
            # 贝叶斯平滑：样本量越大越信任实际数据，样本量小则偏向先验
            weight = min(samples / 50, 1.0)  # 50条样本时完全信任实际数据
            calibrated_prob = round(default_prob * (1 - weight) + actual * weight, 3)
            calibrated[bucket] = calibrated_prob
        else:
            calibrated[bucket] = default_prob

    logger.info("概率校准完成（样本量%d）: %s", total_samples, calibrated)
    return calibrated


# ── CLI 入口 ──────────────────────────────────────────────

if __name__ == '__main__':
    import sys

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    if len(sys.argv) > 1:
        code = sys.argv[1]
        bid = int(sys.argv[2]) if len(sys.argv) > 2 else None
        result = run_stock_backtest(code, bid)
    else:
        result = run_backtest(limit=200)

    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
