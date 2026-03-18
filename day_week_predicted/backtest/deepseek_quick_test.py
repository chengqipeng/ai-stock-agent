#!/usr/bin/env python3
"""
DeepSeek 增强预测 — 快速验证（20只股票）
========================================
直接从数据库取少量股票数据，调用 DeepSeek 验证预测效果。

用法：
    .venv/bin/python -m day_week_predicted.backtest.deepseek_quick_test
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

# ── 测试用股票池（覆盖不同市场和行业）──
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


def _load_test_data():
    """从数据库加载测试股票的K线数据。"""
    from dao import get_connection

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 获取最新交易日
    cur.execute("SELECT MAX(`date`) as d FROM stock_kline WHERE stock_code = '000001.SH'")
    row = cur.fetchone()
    latest_date = row['d'] if row else None
    if not latest_date:
        logger.error("无法获取最新交易日")
        return None, None

    logger.info("最新交易日: %s", latest_date)

    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    lookback = (dt_latest - timedelta(days=90)).strftime('%Y-%m-%d')

    codes = [c for c, _ in TEST_STOCKS]
    # 加上大盘指数
    all_codes = codes + ['000001.SH', '399001.SZ']

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

    # 资金流向
    fund_flows = defaultdict(list)
    code_6_list = [c[:6] for c in codes]
    ph6 = ','.join(['%s'] * len(code_6_list))
    try:
        cur.execute(
            f"SELECT stock_code, `date`, big_net_amount, main_net_5day "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph6}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            code_6_list + [lookback, latest_date]
        )
        for r in cur.fetchall():
            fund_flows[r['stock_code']].append(r)
    except Exception as e:
        logger.warning("资金流向加载失败: %s", e)

    conn.close()
    return klines, latest_date


def _extract_features(code: str, name: str, klines: dict, latest_date: str,
                      target_iso_week: tuple = None) -> dict | None:
    """从K线数据中提取多维特征。
    
    Args:
        target_iso_week: (iso_year, iso_week) 指定目标周，默认为latest_date所在周
    """
    stock_klines = klines.get(code, [])
    if not stock_klines:
        return None

    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    
    if target_iso_week:
        iso_year, iso_week = target_iso_week
    else:
        iso_cal = dt_latest.isocalendar()
        iso_year, iso_week = iso_cal[0], iso_cal[1]

    # 本周K线
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

    # 大盘前一周涨跌
    prev_iso_week = iso_week - 1
    prev_iso_year = iso_year
    if prev_iso_week <= 0:
        prev_iso_year -= 1
        prev_iso_week = 52 + prev_iso_week
    market_prev_week = [k for k in market_klines
                        if datetime.strptime(k['date'], '%Y-%m-%d').isocalendar()[:2] == (prev_iso_year, prev_iso_week)]
    market_prev_chg = _compound_return(
        [k['change_percent'] for k in sorted(market_prev_week, key=lambda x: x['date'])]
    ) if len(market_prev_week) >= 3 else None

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

    # 价格位置
    sorted_k = sorted(stock_klines, key=lambda x: x['date'])
    hist = [k for k in sorted_k if k['date'] < week_klines[0]['date']]
    price_pos_60 = None
    if len(hist) >= 20:
        hc = [k['close'] for k in hist[-60:] if k['close'] > 0]
        if hc:
            all_c = hc + [k['close'] for k in week_klines if k['close'] > 0]
            mn, mx = min(all_c), max(all_c)
            lc = week_klines[-1]['close']
            if mx > mn and lc > 0:
                price_pos_60 = round((lc - mn) / (mx - mn), 4)

    # 前一周涨跌
    prev_week = hist[-5:] if len(hist) >= 5 else hist
    prev_week_chg = _compound_return([k['change_percent'] for k in prev_week]) if prev_week else None

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
        '_market_prev_week_chg': round(market_prev_chg, 2) if market_prev_chg is not None else None,
        'consec_down': cd,
        'consec_up': cu,
        'last_day_chg': round(daily_pcts[-1], 2),
        '_market_suffix': 'SH' if code.endswith('.SH') else 'SZ',
        '_price_pos_60': price_pos_60,
        '_prev_week_chg': round(prev_week_chg, 2) if prev_week_chg else None,
        'ff_signal': None,
        'vol_ratio': vol_ratio,
        'vol_price_corr': None,
        'board_momentum': None,
        'concept_consensus': None,
        'concept_boards': '',
        'finance_score': None,
        'revenue_yoy': None,
        'profit_yoy': None,
        'roe': None,
    }


def _check_next_week_actual(code: str, klines: dict, latest_date: str,
                            target_iso_week: tuple = None) -> float | None:
    """检查下周实际涨跌幅（如果有数据的话）。"""
    stock_klines = klines.get(code, [])
    if not stock_klines:
        return None

    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    
    if target_iso_week:
        iso_year, iso_week = target_iso_week
    else:
        iso_cal = dt_latest.isocalendar()
        iso_year, iso_week = iso_cal[0], iso_cal[1]

    # 下周 = iso_week + 1
    nw_year, nw_week = iso_year, iso_week + 1
    if nw_week > 52:
        nw_year += 1
        nw_week = 1

    nw_klines = [k for k in stock_klines
                 if datetime.strptime(k['date'], '%Y-%m-%d').isocalendar()[:2] == (nw_year, nw_week)]
    if len(nw_klines) < 3:
        return None

    nw_klines.sort(key=lambda x: x['date'])
    return round(_compound_return([k['change_percent'] for k in nw_klines]), 2)


async def main():
    logger.info("=" * 60)
    logger.info("  DeepSeek 快速验证（20只股票 × 多周）")
    logger.info("=" * 60)

    # 加载数据
    klines, latest_date = _load_test_data()
    if not klines:
        return

    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    current_iso = dt_latest.isocalendar()

    # 多周回测：往前回测更多周
    test_weeks = []
    for offset in range(2, 10):  # offset=2~9 → 最多8周回测
        pred_w = (current_iso[0], current_iso[1] - offset)
        verify_w = (current_iso[0], current_iso[1] - offset + 1)
        if pred_w[1] <= 0:
            pred_w = (pred_w[0] - 1, 52 + pred_w[1])
        if verify_w[1] <= 0:
            verify_w = (verify_w[0] - 1, 52 + verify_w[1])
        test_weeks.append((pred_w, verify_w))

    logger.info("回测周数: %d (预测周→验证周: %s)",
                len(test_weeks),
                ', '.join(f"W{p[1]}→W{v[1]}" for p, v in test_weeks))

    from service.analysis.deepseek_nw_predictor import batch_predict_with_deepseek

    all_correct = 0
    all_total = 0
    all_high_conf_correct = 0
    all_high_conf_total = 0
    all_uncertain = 0
    all_details = []

    for pred_week, verify_week in test_weeks:
        logger.info("")
        logger.info("─" * 50)
        logger.info("  预测周 W%d → 验证周 W%d", pred_week[1], verify_week[1])
        logger.info("─" * 50)

        # 提取特征
        samples = []
        for code, name in TEST_STOCKS:
            feat = _extract_features(code, name, klines, latest_date, target_iso_week=pred_week)
            if feat:
                samples.append({'code': code, 'name': name, 'features': feat})

        if not samples:
            logger.warning("  W%d 无有效样本", pred_week[1])
            continue

        logger.info("  有效样本: %d", len(samples))

        # 调用 DeepSeek
        results = await batch_predict_with_deepseek(
            samples, max_concurrency=5, min_confidence=0.0,
        )

        # 检查实际涨跌
        actuals = {}
        for code, _ in TEST_STOCKS:
            actual = _check_next_week_actual(code, klines, latest_date, target_iso_week=pred_week)
            if actual is not None:
                actuals[code] = actual

        # 统计本周
        week_correct = 0
        week_total = 0
        week_hc_correct = 0
        week_hc_total = 0
        week_uncertain = 0

        for s in samples:
            code = s['code']
            r = results.get(code)
            actual = actuals.get(code)

            if not r or actual is None:
                continue

            direction = r['direction']
            conf = r['confidence']

            if direction == 'UNCERTAIN':
                week_uncertain += 1
                all_uncertain += 1
                mark = '⏸️'
            else:
                week_total += 1
                all_total += 1
                actual_up = actual > 0
                pred_up = direction == 'UP'
                is_correct = actual_up == pred_up
                if is_correct:
                    week_correct += 1
                    all_correct += 1
                    mark = '✅'
                else:
                    mark = '❌'
                if conf >= 0.65:
                    week_hc_total += 1
                    all_high_conf_total += 1
                    if is_correct:
                        week_hc_correct += 1
                        all_high_conf_correct += 1

            logger.info("  %-12s %-4s %5.0f%% → 实际%+6.2f%% %s",
                        code, direction, conf * 100, actual, mark)

            all_details.append({
                'week': f"W{pred_week[1]}→W{verify_week[1]}",
                'code': code,
                'name': s['name'],
                'direction': direction,
                'confidence': conf,
                'actual': actual,
                'correct': mark,
            })

        if week_total > 0:
            logger.info("  W%d 准确率: %d/%d = %.1f%% | 不确定: %d | 高置信: %d/%d = %.1f%%",
                        pred_week[1], week_correct, week_total,
                        week_correct / week_total * 100,
                        week_uncertain,
                        week_hc_correct, week_hc_total,
                        week_hc_correct / week_hc_total * 100 if week_hc_total > 0 else 0)

    # 总结
    logger.info("")
    logger.info("=" * 60)
    logger.info("  多周汇总统计")
    logger.info("=" * 60)
    if all_total > 0:
        logger.info("  总有效预测: %d (排除UNCERTAIN %d个)", all_total, all_uncertain)
        logger.info("  整体准确率: %d/%d = %.1f%%",
                    all_correct, all_total, all_correct / all_total * 100)
        if all_high_conf_total > 0:
            logger.info("  高置信(≥65%%)准确率: %d/%d = %.1f%%",
                        all_high_conf_correct, all_high_conf_total,
                        all_high_conf_correct / all_high_conf_total * 100)
        logger.info("  UNCERTAIN占比: %d/%d = %.1f%%",
                    all_uncertain, all_total + all_uncertain,
                    all_uncertain / (all_total + all_uncertain) * 100)
    else:
        logger.info("  无有效验证数据")

    # 保存结果
    output = {
        'date': latest_date,
        'test_weeks': [f"W{p[1]}→W{v[1]}" for p, v in test_weeks],
        'total_predictions': all_total,
        'total_uncertain': all_uncertain,
        'accuracy': round(all_correct / all_total * 100, 1) if all_total > 0 else 0,
        'high_conf_accuracy': round(all_high_conf_correct / all_high_conf_total * 100, 1) if all_high_conf_total > 0 else 0,
        'details': all_details,
    }
    out_path = Path(__file__).parent.parent.parent / 'data_results' / 'deepseek_quick_test_result.json'
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info("\n  结果已保存: %s", out_path)


if __name__ == '__main__':
    asyncio.run(main())
