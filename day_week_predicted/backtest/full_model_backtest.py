"""
完整7维度评分模型回测

直接调用 stock_indicator_all_prompt.py 中的完整数据获取+预计算+评分流程，
对50只股票进行实时评分，然后与最近交易日的实际涨跌对比，
验证 _compute_comprehensive_score 的预测准确性。

与 historical_backtest.py 的区别：
- historical_backtest 使用简化5维度（纯K线），可回测历史任意时段
- 本模块使用完整7维度（含实时API数据），只能验证"当前时刻"的评分准确性
- 本模块的评分逻辑与线上LLM分析完全一致
"""
import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime

from common.utils.stock_info_utils import StockInfo, get_stock_info_by_code
from dao import get_connection
from dao.stock_kline_dao import get_kline_data

logger = logging.getLogger(__name__)


async def _fetch_and_score_single(stock_info: StockInfo) -> dict | None:
    """对单只股票执行完整的数据获取+预计算+评分流程

    复用 stock_indicator_all_prompt.py 中的全部预计算函数和评分逻辑，
    但跳过 prompt 模板拼接，只返回评分结果。
    """
    from common.prompt.strategy_engine.stock_indicator_all_prompt import (
        _filter_valid_trading_days,
        _compute_macd_divergence,
        _compute_macd_bar_trend,
        _compute_golden_cross_quality,
        _compute_kdj_summary,
        _compute_intraday_summary,
        _compute_kline_summary,
        _compute_ma_summary,
        _compute_boll_summary,
        _compute_volume_trend,
        _compute_fund_flow_behavior,
        _compute_northbound_summary,
        _compute_sh_sz_hk_hold_summary,
        _compute_weekly_kline_summary,
        _compute_main_fund_trend_from_10jqka,
        _compute_billboard_summary,
        _compute_margin_trading_summary,
        _compute_comprehensive_score,
        _compute_market_environment,
    )
    from service.eastmoney.stock_info.stock_org_realtime import (
        get_org_realtime_snapshot, compute_org_snapshot_summary,
    )
    from service.web_search.stock_block_trade_search import (
        search_block_trade, compute_block_trade_summary,
    )
    from service.sina.stock_order_book_data import (
        get_order_book, compute_order_book_summary,
    )
    from service.eastmoney.strategy_engine.stock_BOLL_rule import get_boll_rule_boll_only
    from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_kline_cn
    from service.eastmoney.strategy_engine.stock_KDJ_rule import get_kdj_rule_kdj_only
    from service.eastmoney.strategy_engine.stock_MACD_rule import get_macd_signals_macd_only
    from service.eastmoney.technical.stock_day_range_kline import get_moving_averages_json_cn
    from service.eastmoney.stock_info.stock_real_fund_flow import get_real_main_fund_flow
    from service.eastmoney.stock_info.stock_northbound_funds import get_northbound_funds_cn
    from service.jqka10.stock_history_fund_flow_10jqka import get_fund_flow_history
    from service.eastmoney.stock_info.stock_org_hold_by_sh_sz_hk import get_org_hold_by_sh_sz_hk_rank_cn
    from service.jqka10.stock_week_kline_data_10jqka import get_stock_week_kline_list_10jqka
    from service.eastmoney.stock_info.stock_billboard_data import get_billboard_json
    from service.web_search.stock_news_search import search_stock_news
    from service.eastmoney.stock_info.stock_margin_trading import get_margin_trading_json
    from service.jqka10.stock_time_kline_data_10jqka import get_stock_time_kline_cn_10jqka

    from datetime import timedelta
    import chinese_calendar

    data_num = 120
    code = stock_info.stock_code_normalize
    name = stock_info.stock_name

    try:
        # ── 并发获取所有数据 ──
        boll_task = get_boll_rule_boll_only(stock_info)
        kline_task = get_stock_day_kline_cn(stock_info, data_num)
        kdj_task = get_kdj_rule_kdj_only(stock_info, data_num)
        macd_task = get_macd_signals_macd_only(stock_info, data_num)
        time_kline_task = get_stock_time_kline_cn_10jqka(stock_info, 240)
        fund_flow_task = get_real_main_fund_flow(stock_info)
        northbound_task = get_northbound_funds_cn(
            stock_info, ['TRADE_DATE', 'ADD_MARKET_CAP', 'ADD_SHARES_AMP', 'ADD_SHARES_AMP']
        )
        ma_task = get_moving_averages_json_cn(
            stock_info,
            ["date", "close_5_sma", "close_10_ema", "close_20_sma", "close_60_sma",
             "bias_5", "bias_10", "bias_20", "bias_60"],
            120
        )
        org_task = get_org_realtime_snapshot(stock_info)
        hk_task = get_org_hold_by_sh_sz_hk_rank_cn(stock_info, page_size=10)
        weekly_task = get_stock_week_kline_list_10jqka(stock_info, limit=30)
        billboard_task = get_billboard_json(stock_info, days=30)
        news_task = search_stock_news(stock_info, days=7)
        block_task = search_block_trade(stock_info, days=30)
        order_book_task = get_order_book(stock_info)
        margin_task = get_margin_trading_json(stock_info, page_size=5)

        results = await asyncio.gather(
            boll_task, kline_task, kdj_task, macd_task, time_kline_task,
            fund_flow_task, northbound_task, ma_task, org_task, hk_task,
            weekly_task, billboard_task, news_task, block_task, order_book_task,
            margin_task,
            return_exceptions=True,
        )

        # 解包结果，异常的用空值替代
        def _safe(val, default):
            return default if isinstance(val, Exception) else val

        boll_data = _safe(results[0], {})
        kline_data = _safe(results[1], [])
        kdj_data = _safe(results[2], {})
        macd_data = _safe(results[3], {})
        time_kline = _safe(results[4], [])
        fund_flow_realtime = _safe(results[5], {})
        northbound = _safe(results[6], {})
        ma_data = _safe(results[7], [])
        org_snapshot = _safe(results[8], {})
        hk_hold = _safe(results[9], {})
        weekly_kline = _safe(results[10], [])
        billboard = _safe(results[11], [])
        news = _safe(results[12], [])
        block_trade = _safe(results[13], [])
        order_book = _safe(results[14], {})
        margin_data = _safe(results[15], [])

        # 同花顺历史资金流（单独获取，避免影响主流程）
        try:
            history_fund_flow_raw = await get_fund_flow_history(stock_info)
        except Exception:
            history_fund_flow_raw = []

        # ── 预计算 ──
        next_trading_day = datetime.now().date() + timedelta(days=1)
        while next_trading_day.weekday() >= 5 or chinese_calendar.is_holiday(next_trading_day):
            next_trading_day += timedelta(days=1)

        valid_kline = _filter_valid_trading_days(kline_data)
        if not valid_kline:
            return None

        divergence = _compute_macd_divergence(macd_data, valid_kline)
        bar_trend = _compute_macd_bar_trend(macd_data)
        gc_quality = _compute_golden_cross_quality(macd_data, valid_kline)
        kdj_summary = _compute_kdj_summary(kdj_data)
        intraday = _compute_intraday_summary(time_kline)
        kline_summary = _compute_kline_summary(valid_kline)
        ma_summary = _compute_ma_summary(ma_data)
        latest_close = valid_kline[0]['收盘价'] if valid_kline else 0
        boll_summary = _compute_boll_summary(boll_data, latest_close)
        volume_trend = _compute_volume_trend(kline_data)
        fund_flow_behavior = _compute_fund_flow_behavior(fund_flow_realtime)
        org_summary = compute_org_snapshot_summary(org_snapshot)
        northbound_summary = _compute_northbound_summary(northbound)
        hk_summary = _compute_sh_sz_hk_hold_summary(hk_hold)
        weekly_summary = _compute_weekly_kline_summary(weekly_kline)
        main_fund_10jqka = _compute_main_fund_trend_from_10jqka(history_fund_flow_raw)
        billboard_summary = _compute_billboard_summary(billboard)
        block_summary = compute_block_trade_summary(
            block_trade, next_trading_day.strftime('%Y-%m-%d')
        )
        order_book_summary = compute_order_book_summary(order_book)
        margin_summary = _compute_margin_trading_summary(margin_data)
        market_env = await _compute_market_environment(stock_info)

        # ── 获取行业信息用于板块差异化权重 ──
        stock_sector = None
        try:
            from service.eastmoney.stock_info.stock_industry_ranking import get_stock_industry_ranking_json
            from common.prompt.strategy_engine.stock_indicator_all_prompt import classify_stock_sector
            industry_ranking = await get_stock_industry_ranking_json(stock_info)
            industry_name = industry_ranking.get('行业名称', '') if industry_ranking else ''
            stock_sector = classify_stock_sector(industry_name)
        except Exception as e:
            logger.debug("[%s] 获取行业信息失败（使用默认权重）: %s", code, e)

        # ── 综合评分 ──
        score_result = _compute_comprehensive_score(
            macd_data=macd_data,
            macd_bar_trend=bar_trend,
            divergence_result=divergence,
            golden_cross_quality=gc_quality,
            kdj_summary=kdj_summary,
            boll_summary=boll_summary,
            ma_summary=ma_summary,
            kline_summary=kline_summary,
            volume_trend=volume_trend,
            weekly_kline_summary=weekly_summary,
            intraday_summary=intraday,
            fund_flow_behavior=fund_flow_behavior,
            order_book_summary=order_book_summary,
            main_fund_trend_10jqka=main_fund_10jqka,
            org_holder_summary=org_summary,
            billboard_summary=billboard_summary,
            block_trade_summary=block_summary,
            market_env=market_env,
            news_data=news,
            margin_summary=margin_summary,
            calibrated_probability_params=None,
            sector=stock_sector,
        )

        return {
            'stock_code': code,
            'stock_name': name,
            'total_score': score_result['总分'],
            'grade': score_result['评级'],
            'dimensions': score_result['各维度得分'],
            'prediction': score_result.get('预测概率估算', {}),
        }

    except Exception as e:
        logger.error("[%s] 评分失败: %s", code, e, exc_info=True)
        return None


async def run_full_model_backtest(
    stock_codes: list[str] = None,
    max_stocks: int = 50,
    concurrency: int = 5,
) -> dict:
    """对多只股票运行完整7维度评分，与最近交易日实际涨跌对比

    Args:
        stock_codes: 指定股票列表；None则随机抽样
        max_stocks: 最大回测股票数
        concurrency: 并发数（控制API请求速率）

    Returns:
        回测结果汇总
    """
    t_start = time.time()

    # 获取股票列表
    if not stock_codes:
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT stock_code, COUNT(*) cnt
                FROM stock_kline
                WHERE `date` >= '2025-12-01'
                GROUP BY stock_code
                HAVING cnt >= 30
                ORDER BY RAND()
                LIMIT %s
            """, (max_stocks,))
            stock_codes = [r['stock_code'] for r in cursor.fetchall()]
        finally:
            cursor.close()
            conn.close()

    if not stock_codes:
        return {'状态': '无可用股票数据'}

    # 获取 StockInfo 列表
    stock_infos = []
    for code in stock_codes:
        si = get_stock_info_by_code(code)
        if si:
            stock_infos.append(si)

    if not stock_infos:
        return {'状态': '无法获取股票信息'}

    # 并发评分（使用信号量控制并发）
    sem = asyncio.Semaphore(concurrency)
    scored = []
    failed = 0

    async def _score_with_sem(si: StockInfo):
        nonlocal failed
        async with sem:
            try:
                result = await _fetch_and_score_single(si)
                if result:
                    scored.append(result)
                    logger.info("[%s/%s] %s 评分完成: %d分",
                                len(scored) + failed, len(stock_infos),
                                si.stock_name, result['total_score'])
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                logger.error("[%s] 评分异常: %s", si.stock_code_normalize, e)

    tasks = [_score_with_sem(si) for si in stock_infos]
    await asyncio.gather(*tasks)

    if not scored:
        return {'状态': '所有股票评分失败', '失败数': failed}

    # ── 获取最近2个交易日的K线数据，计算实际涨跌 ──
    day_results = []
    week_results = []
    score_buckets = defaultdict(lambda: {'day_ok': 0, 'day_n': 0, 'week_ok': 0, 'week_n': 0})

    for item in scored:
        code = item['stock_code']
        total = item['total_score']
        pred_info = item['prediction']
        pred_dir = pred_info.get('预测方向', '')

        # 获取最近K线
        klines = get_kline_data(code, start_date='2026-02-01', end_date='2026-03-31')
        if len(klines) < 2:
            continue

        # klines 按日期升序，最后一条是最新
        latest = klines[-1]
        prev = klines[-2]
        base_close = prev['close_price']

        if base_close <= 0 or (latest.get('trading_volume') or 0) == 0:
            continue

        # 次日实际涨跌（最新日 vs 前一日）
        actual_chg = round((latest['close_price'] - base_close) / base_close * 100, 2)
        if actual_chg > 0.3:
            actual_dir = '上涨'
        elif actual_chg < -0.3:
            actual_dir = '下跌'
        else:
            actual_dir = '横盘震荡'

        dir_ok = (pred_dir == actual_dir)
        loose_ok = dir_ok
        if not dir_ok:
            if '上涨' in pred_dir and actual_chg >= 0:
                loose_ok = True
            elif '下跌' in pred_dir and actual_chg <= 0:
                loose_ok = True

        # 评分区间
        if total >= 70:
            bucket = '70-100'
        elif total >= 55:
            bucket = '55-69'
        elif total >= 40:
            bucket = '40-54'
        else:
            bucket = '0-39'

        day_results.append({
            'stock_code': code,
            'stock_name': item['stock_name'],
            'total_score': total,
            'grade': item['grade'],
            'pred_direction': pred_dir,
            'actual_change_pct': actual_chg,
            'actual_direction': actual_dir,
            'direction_correct': dir_ok,
            'direction_loose_correct': loose_ok,
            'dimensions': item['dimensions'],
            'latest_date': latest['date'],
        })
        score_buckets[bucket]['day_n'] += 1
        if dir_ok:
            score_buckets[bucket]['day_ok'] += 1

        # 一周验证（如果有足够数据）
        if len(klines) >= 7:
            week_base = klines[-6]  # 5个交易日前
            week_base_close = week_base['close_price']
            if week_base_close > 0:
                week_chg = round((latest['close_price'] - week_base_close) / week_base_close * 100, 2)
                week_dir = '上涨' if week_chg > 0.3 else ('下跌' if week_chg < -0.3 else '横盘震荡')
                week_results.append({
                    'stock_code': code,
                    'stock_name': item['stock_name'],
                    'total_score': total,
                    'pred_direction': pred_dir,
                    'actual_change_pct': week_chg,
                    'actual_direction': week_dir,
                    'direction_correct': pred_dir == week_dir,
                })

    elapsed = time.time() - t_start

    # ── 构建汇总 ──
    def _rate(ok, n):
        return f'{ok}/{n}（{round(ok / n * 100, 1)}%）' if n > 0 else '无数据'

    def _agg(results):
        if not results:
            return {'总数': 0, '方向准确率': '无数据', '宽松准确率': '无数据'}
        n = len(results)
        d_ok = sum(1 for r in results if r['direction_correct'])
        l_ok = sum(1 for r in results if r.get('direction_loose_correct', r['direction_correct']))
        avg_chg = round(sum(r['actual_change_pct'] for r in results) / n, 2)
        return {
            '总数': n,
            '方向准确率': _rate(d_ok, n),
            '方向准确率_数值': round(d_ok / n * 100, 1),
            '宽松准确率': _rate(l_ok, n),
            '宽松准确率_数值': round(l_ok / n * 100, 1),
            '平均实际涨跌幅(%)': avg_chg,
        }

    bucket_summary = {}
    for b in ['70-100', '55-69', '40-54', '0-39']:
        d = score_buckets.get(b, {})
        bucket_summary[b] = {
            '次日方向准确率': _rate(d.get('day_ok', 0), d.get('day_n', 0)),
            '样本数': d.get('day_n', 0),
        }

    # 评分分布
    score_dist = defaultdict(int)
    for r in day_results:
        s = r['total_score']
        if s >= 70: score_dist['≥70'] += 1
        elif s >= 55: score_dist['55-69'] += 1
        elif s >= 40: score_dist['40-54'] += 1
        else: score_dist['<40'] += 1

    # 逐股详情
    stock_details = sorted(day_results, key=lambda x: -x['total_score'])

    return {
        '回测类型': '完整7维度评分模型回测（实时API数据）',
        '回测时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        '耗时(秒)': round(elapsed, 1),
        '评分成功数': len(scored),
        '评分失败数': failed,
        '次日预测回测': _agg(day_results),
        '一周预测回测（参考）': _agg(week_results),
        '按评分区间': bucket_summary,
        '评分分布': dict(score_dist),
        '逐股详情': [{
            '代码': r['stock_code'],
            '名称': r['stock_name'],
            '评分': r['total_score'],
            '评级': r['grade'],
            '预测方向': r['pred_direction'],
            '实际涨跌': f"{r['actual_change_pct']:+.2f}%",
            '实际方向': r['actual_direction'],
            '正确': '✓' if r['direction_correct'] else '✗',
            '维度': r['dimensions'],
            '日期': r['latest_date'],
        } for r in stock_details],
        '说明': (
            '本回测使用完整7维度评分模型（趋势强度20+动能量价20+结构边界15+短线情绪15+资金筹码15+外部环境5+风险收益比10=100分），'
            '数据来源包括东方财富API、同花顺实时数据、新浪盘口、百度新闻等。'
            '评分逻辑与线上LLM分析完全一致。'
            '注意：本回测使用"当前时刻"的实时数据评分，与最近交易日的实际涨跌对比，'
            '因此评分中的分时/盘口/资金流数据反映的是最新状态，而非预测日开盘前的状态。'
        ),
    }
