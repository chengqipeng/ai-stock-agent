#!/usr/bin/env python3
"""
DeepSeek 多周回测 — 统计显著性验证
===================================
回测多个历史周，每周20只股票，验证DeepSeek预测准确率。

用法：
    .venv/bin/python -m day_week_predicted.backtest.deepseek_multiweek_backtest
"""
import asyncio
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# 40只测试股票（覆盖不同行业和市值）
TEST_STOCKS = [
    ('600519.SH', '贵州茅台'), ('000858.SZ', '五粮液'),
    ('601318.SH', '中国平安'), ('000001.SZ', '平安银行'),
    ('600036.SH', '招商银行'), ('002594.SZ', '比亚迪'),
    ('300750.SZ', '宁德时代'), ('601012.SH', '隆基绿能'),
    ('000333.SZ', '美的集团'), ('600900.SH', '长江电力'),
    ('002475.SZ', '立讯精密'), ('300059.SZ', '东方财富'),
    ('601899.SH', '紫金矿业'), ('600276.SH', '恒瑞医药'),
    ('002714.SZ', '牧原股份'), ('601888.SH', '中国中免'),
    ('300124.SZ', '汇川技术'), ('688981.SH', '中芯国际'),
    ('002049.SZ', '紫光国微'), ('600809.SH', '山西汾酒'),
    ('601398.SH', '工商银行'), ('600030.SH', '中信证券'),
    ('000725.SZ', '京东方A'), ('002415.SZ', '海康威视'),
    ('600887.SH', '伊利股份'), ('000568.SZ', '泸州老窖'),
    ('601166.SH', '兴业银行'), ('300015.SZ', '爱尔眼科'),
    ('002304.SZ', '洋河股份'), ('600585.SH', '海螺水泥'),
    ('601668.SH', '中国建筑'), ('000002.SZ', '万科A'),
    ('002352.SZ', '顺丰控股'), ('600031.SH', '三一重工'),
    ('601919.SH', '中远海控'), ('002371.SZ', '北方华创'),
    ('300274.SZ', '阳光电源'), ('688012.SH', '中微公司'),
    ('600438.SH', '通威股份'), ('002460.SZ', '赣锋锂业'),
]


def _to_float(v) -> float:
    try:
        return float(v) if v is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


def _compound_return(pcts):
    r = 1.0
    for p in pcts:
        r *= (1 + p / 100)
    return (r - 1) * 100


def _load_kline_data():
    """加载测试股票180天K线数据。"""
    from dao import get_connection
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    cur.execute("SELECT MAX(`date`) as d FROM stock_kline WHERE stock_code = '000001.SH'")
    latest_date = cur.fetchone()['d']
    logger.info("最新交易日: %s", latest_date)

    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    lookback = (dt_latest - timedelta(days=180)).strftime('%Y-%m-%d')

    codes = [c for c, _ in TEST_STOCKS]
    all_codes = list(set(codes + ['000001.SH', '399001.SZ']))
    ph = ','.join(['%s'] * len(all_codes))

    cur.execute(
        f"SELECT stock_code, `date`, close_price, change_percent, trading_volume "
        f"FROM stock_kline WHERE stock_code IN ({ph}) "
        f"AND `date` >= %s AND `date` <= %s ORDER BY stock_code, `date`",
        all_codes + [lookback, latest_date]
    )

    klines = defaultdict(list)
    for r in cur.fetchall():
        klines[r['stock_code']].append({
            'date': r['date'],
            'close': _to_float(r['close_price']),
            'change_percent': _to_float(r['change_percent']),
            'volume': _to_float(r['trading_volume']),
        })
    conn.close()
    return klines, latest_date


def _extract_features(code, klines, iso_year, iso_week):
    """提取指定周的特征。返回None如果数据不足。"""
    stock_klines = klines.get(code, [])
    if not stock_klines:
        return None

    week_klines = [k for k in stock_klines
                   if datetime.strptime(k['date'], '%Y-%m-%d').isocalendar()[:2] == (iso_year, iso_week)]
    week_klines.sort(key=lambda x: x['date'])

    if len(week_klines) < 3:
        return None

    daily_pcts = [k['change_percent'] for k in week_klines]
    this_week_chg = _compound_return(daily_pcts)

    # 大盘
    market_code = '000001.SH' if code.endswith('.SH') else '399001.SZ'
    market_klines = klines.get(market_code, [])
    market_week = [k for k in market_klines
                   if datetime.strptime(k['date'], '%Y-%m-%d').isocalendar()[:2] == (iso_year, iso_week)]
    market_chg = _compound_return(
        [k['change_percent'] for k in sorted(market_week, key=lambda x: x['date'])]
    ) if len(market_week) >= 3 else 0.0

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

    # 历史数据（本周之前）
    sorted_k = sorted(stock_klines, key=lambda x: x['date'])
    hist = [k for k in sorted_k if k['date'] < week_klines[0]['date']]

    # 价格位置
    price_pos_60 = None
    if len(hist) >= 20:
        hc = [k['close'] for k in hist[-60:] if k['close'] > 0]
        if hc:
            all_c = hc + [k['close'] for k in week_klines if k['close'] > 0]
            mn, mx = min(all_c), max(all_c)
            lc = week_klines[-1]['close']
            if mx > mn and lc > 0:
                price_pos_60 = round((lc - mn) / (mx - mn), 4)

    # 前一周
    prev_iso_week = iso_week - 1
    prev_iso_year = iso_year
    if prev_iso_week <= 0:
        prev_iso_year -= 1
        prev_iso_week = 52
    prev_week_klines = [k for k in stock_klines
                        if datetime.strptime(k['date'], '%Y-%m-%d').isocalendar()[:2] == (prev_iso_year, prev_iso_week)]
    prev_week_chg = _compound_return(
        [k['change_percent'] for k in sorted(prev_week_klines, key=lambda x: x['date'])]
    ) if len(prev_week_klines) >= 3 else None

    # 大盘前一周
    market_prev_klines = [k for k in market_klines
                          if datetime.strptime(k['date'], '%Y-%m-%d').isocalendar()[:2] == (prev_iso_year, prev_iso_week)]
    market_prev_chg = _compound_return(
        [k['change_percent'] for k in sorted(market_prev_klines, key=lambda x: x['date'])]
    ) if len(market_prev_klines) >= 3 else None

    # 成交量比率
    vol_ratio = None
    if len(hist) >= 20:
        avg_vol = sum(k['volume'] for k in hist[-20:]) / 20
        week_avg_vol = sum(k['volume'] for k in week_klines) / len(week_klines)
        if avg_vol > 0:
            vol_ratio = round(week_avg_vol / avg_vol, 2)

    return {
        'this_week_chg': round(this_week_chg, 2),
        'market_chg': round(market_chg, 2),
        '_market_prev_week_chg': round(market_prev_chg, 2) if market_prev_chg else None,
        'consec_down': cd, 'consec_up': cu,
        'last_day_chg': round(daily_pcts[-1], 2),
        '_market_suffix': 'SH' if code.endswith('.SH') else 'SZ',
        '_price_pos_60': price_pos_60,
        '_prev_week_chg': round(prev_week_chg, 2) if prev_week_chg else None,
        'ff_signal': None, 'vol_ratio': vol_ratio,
        'vol_price_corr': None, 'board_momentum': None,
        'concept_consensus': None, 'concept_boards': '',
        'finance_score': None, 'revenue_yoy': None,
        'profit_yoy': None, 'roe': None,
    }


def _get_actual_return(code, klines, iso_year, iso_week):
    """获取指定周的实际涨跌幅。"""
    stock_klines = klines.get(code, [])
    nw_klines = [k for k in stock_klines
                 if datetime.strptime(k['date'], '%Y-%m-%d').isocalendar()[:2] == (iso_year, iso_week)]
    if len(nw_klines) < 3:
        return None
    nw_klines.sort(key=lambda x: x['date'])
    return round(_compound_return([k['change_percent'] for k in nw_klines]), 2)


async def backtest_one_week(klines, pred_year, pred_week, verify_year, verify_week):
    """回测一周：用pred_week特征预测，verify_week验证。"""
    from service.analysis.deepseek_nw_predictor import batch_predict_with_deepseek

    samples = []
    for code, name in TEST_STOCKS:
        feat = _extract_features(code, klines, pred_year, pred_week)
        if feat:
            samples.append({'code': code, 'name': name, 'features': feat})

    if not samples:
        return None

    # 调用DeepSeek
    results = await batch_predict_with_deepseek(samples, max_concurrency=5, min_confidence=0.0)

    # 获取实际涨跌
    week_results = []
    for s in samples:
        code = s['code']
        pred = results.get(code)
        actual = _get_actual_return(code, klines, verify_year, verify_week)
        if pred and actual is not None:
            actual_up = actual > 0
            pred_up = pred['direction'] == 'UP'
            pred_down = pred['direction'] == 'DOWN'
            is_correct = (actual_up and pred_up) or (not actual_up and pred_down)
            week_results.append({
                'code': code, 'name': s['name'],
                'direction': pred['direction'],
                'confidence': pred['confidence'],
                'actual': actual,
                'correct': is_correct,
            })

    return week_results


async def main():
    logger.info("=" * 60)
    logger.info("  DeepSeek 多周回测")
    logger.info("=" * 60)

    klines, latest_date = _load_kline_data()
    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    current_iso = dt_latest.isocalendar()

    # 回测周5~10（共6周），每周用前一周特征预测，当周验证
    # 当前是第12周，所以第5~10周都有完整数据
    test_weeks = []
    for w in range(5, 11):
        pred_year, pred_week = current_iso[0], w
        verify_year, verify_week = current_iso[0], w + 1
        test_weeks.append((pred_year, pred_week, verify_year, verify_week))

    all_results = []
    for pred_y, pred_w, ver_y, ver_w in test_weeks:
        logger.info("")
        logger.info("回测: 第%d周特征 → 预测第%d周", pred_w, ver_w)
        week_results = await backtest_one_week(klines, pred_y, pred_w, ver_y, ver_w)
        if week_results:
            correct = sum(1 for r in week_results if r['correct'])
            total = len(week_results)
            logger.info("  第%d周: %d/%d = %.1f%%", ver_w, correct, total, correct/total*100)
            all_results.extend([{**r, 'pred_week': pred_w, 'verify_week': ver_w} for r in week_results])
        else:
            logger.warning("  第%d周: 无有效数据", ver_w)

    if not all_results:
        logger.error("无回测结果")
        return

    # 汇总统计
    total = len(all_results)
    correct = sum(1 for r in all_results if r['correct'])
    up_results = [r for r in all_results if r['direction'] == 'UP']
    down_results = [r for r in all_results if r['direction'] == 'DOWN']
    uncertain_results = [r for r in all_results if r['direction'] == 'UNCERTAIN']
    high_conf = [r for r in all_results if r['confidence'] >= 0.65]
    low_conf = [r for r in all_results if r['confidence'] < 0.65]

    logger.info("")
    logger.info("=" * 60)
    logger.info("  多周回测汇总（6周 × 40只）")
    logger.info("=" * 60)
    logger.info("  总样本: %d", total)
    logger.info("  整体准确率: %d/%d = %.1f%%", correct, total, correct/total*100)

    if up_results:
        up_correct = sum(1 for r in up_results if r['correct'])
        logger.info("  预测UP: %d/%d = %.1f%%", up_correct, len(up_results), up_correct/len(up_results)*100)
    if down_results:
        down_correct = sum(1 for r in down_results if r['correct'])
        logger.info("  预测DOWN: %d/%d = %.1f%%", down_correct, len(down_results), down_correct/len(down_results)*100)
    if uncertain_results:
        logger.info("  UNCERTAIN: %d只", len(uncertain_results))
    if high_conf:
        hc_correct = sum(1 for r in high_conf if r['correct'])
        logger.info("  高置信(≥65%%): %d/%d = %.1f%%", hc_correct, len(high_conf), hc_correct/len(high_conf)*100)
    if low_conf:
        lc_correct = sum(1 for r in low_conf if r['correct'])
        logger.info("  低置信(<65%%): %d/%d = %.1f%%", lc_correct, len(low_conf), lc_correct/len(low_conf)*100)

    # 按周统计
    logger.info("")
    logger.info("  按周统计:")
    for pred_y, pred_w, ver_y, ver_w in test_weeks:
        wr = [r for r in all_results if r['verify_week'] == ver_w]
        if wr:
            wc = sum(1 for r in wr if r['correct'])
            logger.info("    第%d周: %d/%d = %.1f%%", ver_w, wc, len(wr), wc/len(wr)*100)

    # 错误模式分析
    logger.info("")
    logger.info("  错误模式分析:")
    errors = [r for r in all_results if not r['correct']]
    # UP预测错误（实际跌了）
    up_errors = [r for r in errors if r['direction'] == 'UP']
    down_errors = [r for r in errors if r['direction'] == 'DOWN']
    logger.info("    预测UP但实际跌: %d次", len(up_errors))
    logger.info("    预测DOWN但实际涨: %d次", len(down_errors))

    # 保存结果
    output = {
        'backtest_weeks': [(pw, vw) for _, pw, _, vw in test_weeks],
        'total_samples': total,
        'overall_accuracy': round(correct/total*100, 1),
        'up_accuracy': round(sum(1 for r in up_results if r['correct'])/len(up_results)*100, 1) if up_results else None,
        'down_accuracy': round(sum(1 for r in down_results if r['correct'])/len(down_results)*100, 1) if down_results else None,
        'high_conf_accuracy': round(sum(1 for r in high_conf if r['correct'])/len(high_conf)*100, 1) if high_conf else None,
        'details': all_results,
    }
    out_path = Path(__file__).parent.parent.parent / 'data_results' / 'deepseek_multiweek_backtest_result.json'
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info("\n  结果已保存: %s", out_path)


if __name__ == '__main__':
    asyncio.run(main())
