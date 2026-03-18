#!/usr/bin/env python3
"""
DeepSeek 增强预测回测验证
========================
对比规则引擎 vs 规则引擎+DeepSeek 的预测效果。

用法：
    python -m day_week_predicted.backtest.deepseek_enhancement_backtest

回测逻辑：
1. 从历史数据中选取规则引擎返回"不确定"的样本
2. 构建特征 → 调用 DeepSeek → 获取预测
3. 与实际下周涨跌对比，计算准确率
4. 输出对比报告
"""
import asyncio
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dao import get_connection
from service.weekly_prediction_service import (
    _get_all_stock_codes,
    _get_latest_trade_date,
    _load_prediction_data,
    _predict_stock_weekly,
    _nw_extract_features,
    _nw_match_rule,
    _compound_return,
    _get_stock_index,
    _get_market_klines_for_stock,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def _collect_uncertain_samples(stock_codes: list, data: dict, latest_date: str,
                               max_samples: int = 50) -> list:
    """收集规则引擎返回"不确定"的样本，用于 DeepSeek 回测。"""
    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    iso_cal = dt_latest.isocalendar()
    iso_year, iso_week = iso_cal[0], iso_cal[1]

    samples = []
    for code in stock_codes:
        if len(samples) >= max_samples:
            break

        klines = data['stock_klines'].get(code, [])
        if not klines:
            continue

        # 获取本周K线
        week_klines = []
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            ical = dt.isocalendar()
            if ical[0] == iso_year and ical[1] == iso_week:
                week_klines.append(k)
        week_klines.sort(key=lambda x: x['date'])

        if len(week_klines) < 3:
            continue

        daily_pcts = [k['change_percent'] for k in week_klines]

        # 大盘涨跌
        market_klines = _get_market_klines_for_stock(code, data)
        market_week = [k for k in market_klines
                       if datetime.strptime(k['date'], '%Y-%m-%d').isocalendar()[:2] == (iso_year, iso_week)]
        market_chg = _compound_return(
            [k['change_percent'] for k in sorted(market_week, key=lambda x: x['date'])]
        ) if len(market_week) >= 3 else 0.0

        # 本周预测（获取多维信号）
        pred = _predict_stock_weekly(code, data, latest_date)
        if not pred:
            continue

        ff_signal = pred.get('fund_flow_signal')
        vol_ratio = pred.get('vol_ratio')
        vol_price_corr = pred.get('vol_price_corr')
        finance_score = pred.get('finance_score')
        stock_idx = _get_stock_index(code)

        # 价格位置
        sorted_klines = sorted(klines, key=lambda x: x['date'])
        first_week_date = week_klines[0]['date']
        hist_klines = [k for k in sorted_klines if k['date'] < first_week_date]
        price_pos_60 = None
        if len(hist_klines) >= 20:
            hist_closes = [k.get('close', 0) for k in hist_klines[-60:] if k.get('close', 0) > 0]
            if hist_closes:
                all_c = hist_closes + [k.get('close', 0) for k in week_klines if k.get('close', 0) > 0]
                min_c, max_c = min(all_c), max(all_c)
                latest_c = week_klines[-1].get('close', 0)
                if max_c > min_c and latest_c > 0:
                    price_pos_60 = round((latest_c - min_c) / (max_c - min_c), 4)

        # 前一周涨跌
        prev_week_klines = hist_klines[-5:] if len(hist_klines) >= 5 else hist_klines
        prev_week_chg = _compound_return([k['change_percent'] for k in prev_week_klines]) if prev_week_klines else None

        feat = _nw_extract_features(
            daily_pcts, market_chg,
            ff_signal=ff_signal, vol_ratio=vol_ratio,
            vol_price_corr=vol_price_corr, finance_score=finance_score,
            market_index=stock_idx, price_pos_60=price_pos_60,
            prev_week_chg=prev_week_chg,
        )
        rule = _nw_match_rule(feat)

        if rule is not None:
            continue  # 规则已命中，跳过

        # 连涨/连跌
        cd, cu = 0, 0
        for p in reversed(daily_pcts):
            if p < 0:
                cd += 1
                if cu > 0: break
            elif p > 0:
                cu += 1
                if cd > 0: break
            else:
                break

        stock_name = data['stock_names'].get(code, '')
        samples.append({
            'code': code,
            'name': stock_name,
            'features': {
                'this_week_chg': feat['this_week_chg'],
                'market_chg': feat['market_chg'],
                'consec_down': cd,
                'consec_up': cu,
                'last_day_chg': daily_pcts[-1] if daily_pcts else 0,
                '_market_suffix': feat.get('_market_suffix', ''),
                '_price_pos_60': price_pos_60,
                '_prev_week_chg': prev_week_chg,
                'ff_signal': ff_signal,
                'vol_ratio': vol_ratio,
                'vol_price_corr': vol_price_corr,
                'board_momentum': pred.get('board_momentum'),
                'concept_consensus': pred.get('concept_consensus'),
                'concept_boards': pred.get('concept_boards', ''),
                'finance_score': finance_score,
                'revenue_yoy': pred.get('revenue_yoy'),
                'profit_yoy': pred.get('profit_yoy'),
                'roe': pred.get('roe'),
            },
        })

    return samples


async def _run_deepseek_backtest(samples: list) -> dict:
    """对样本调用 DeepSeek 并统计结果。"""
    from service.analysis.deepseek_nw_predictor import batch_predict_with_deepseek

    def _progress(total, done):
        logger.info("  DeepSeek 进度: %d/%d", done, total)

    results = await batch_predict_with_deepseek(
        samples,
        max_concurrency=3,
        min_confidence=0.0,  # 回测时不过滤，全部收集
        progress_callback=_progress,
    )
    return results


def main():
    logger.info("=" * 60)
    logger.info("  DeepSeek 增强预测回测")
    logger.info("=" * 60)

    # 加载数据
    all_codes = _get_all_stock_codes()
    latest_date = _get_latest_trade_date()
    if not all_codes or not latest_date:
        logger.error("无数据")
        return

    logger.info("股票数: %d, 最新日期: %s", len(all_codes), latest_date)

    data = _load_prediction_data(all_codes, latest_date)

    # 收集不确定样本（限制50只用于回测）
    logger.info("收集规则引擎未命中的样本...")
    samples = _collect_uncertain_samples(all_codes, data, latest_date, max_samples=50)
    logger.info("收集到 %d 个不确定样本", len(samples))

    if not samples:
        logger.info("无不确定样本，规则引擎已全覆盖")
        return

    # 调用 DeepSeek
    logger.info("调用 DeepSeek 预测...")
    loop = asyncio.new_event_loop()
    try:
        ds_results = loop.run_until_complete(_run_deepseek_backtest(samples))
    finally:
        loop.close()

    # 统计
    total = len(samples)
    predicted = len(ds_results)
    high_conf = sum(1 for r in ds_results.values() if r['confidence'] >= 0.70)
    mid_conf = sum(1 for r in ds_results.values() if 0.55 <= r['confidence'] < 0.70)
    low_conf = sum(1 for r in ds_results.values() if r['confidence'] < 0.55)
    up_count = sum(1 for r in ds_results.values() if r['direction'] == 'UP')
    down_count = predicted - up_count

    logger.info("")
    logger.info("=" * 60)
    logger.info("  回测结果")
    logger.info("=" * 60)
    logger.info("  不确定样本总数: %d", total)
    logger.info("  DeepSeek 有效预测: %d (%.1f%%)", predicted, predicted / total * 100 if total else 0)
    logger.info("  预测涨: %d, 预测跌: %d", up_count, down_count)
    logger.info("  高置信(≥70%%): %d, 中置信(55-70%%): %d, 低置信(<55%%): %d",
                high_conf, mid_conf, low_conf)

    # 输出详细结果
    logger.info("")
    logger.info("  详细预测结果:")
    logger.info("  %-12s %-8s %-6s %-6s %s", "代码", "名称", "方向", "置信度", "理由")
    logger.info("  " + "-" * 70)
    for s in samples:
        code = s['code']
        r = ds_results.get(code)
        if r:
            logger.info("  %-12s %-8s %-6s %.0f%%   %s",
                        code, s['name'][:4], r['direction'],
                        r['confidence'] * 100, r['justification'][:30])

    # 保存结果
    output_path = Path(__file__).parent.parent.parent / 'data_results' / 'deepseek_backtest_result.json'
    output = {
        'date': latest_date,
        'total_uncertain': total,
        'deepseek_predicted': predicted,
        'up_count': up_count,
        'down_count': down_count,
        'high_confidence': high_conf,
        'mid_confidence': mid_conf,
        'low_confidence': low_conf,
        'details': {
            code: {
                'name': next((s['name'] for s in samples if s['code'] == code), ''),
                'direction': r['direction'],
                'confidence': r['confidence'],
                'justification': r['justification'],
            }
            for code, r in ds_results.items()
        },
    }
    output_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info("\n  结果已保存: %s", output_path)


if __name__ == '__main__':
    main()
