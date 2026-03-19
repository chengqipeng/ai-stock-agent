"""
DeepSeek 周预测生产服务
========================
为选中的股票提取特征、调用DeepSeek LLM预测、存入数据库。
"""
import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from dao import get_connection
from dao.deepseek_prediction_dao import ensure_table, batch_insert_predictions
from service.analysis.deepseek_nw_predictor import predict_next_week_with_deepseek

logger = logging.getLogger(__name__)


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


def _get_market_code(code: str) -> str:
    prefix3 = code[:3]
    mapping = {
        '600': '000001.SH', '601': '000001.SH', '603': '000001.SH', '605': '000001.SH',
        '688': '000001.SH',
        '000': '399001.SZ', '001': '399001.SZ', '002': '399001.SZ', '003': '399001.SZ',
        '300': '399001.SZ', '301': '399001.SZ',
    }
    if prefix3 in mapping:
        return mapping[prefix3]
    return '000001.SH' if code.endswith('.SH') else '399001.SZ'


def _load_kline_data(stock_codes: list[str]) -> tuple[dict, str]:
    """加载K线数据。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    cur.execute("SELECT MAX(`date`) as d FROM stock_kline WHERE stock_code = '000001.SH'")
    row = cur.fetchone()
    latest_date = row['d'] if row else None
    if not latest_date:
        conn.close()
        return {}, ''

    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    lookback = (dt_latest - timedelta(days=200)).strftime('%Y-%m-%d')

    market_codes = set()
    for c in stock_codes:
        market_codes.add(_get_market_code(c))
    all_codes = list(set(stock_codes) | market_codes)

    klines = defaultdict(list)
    bs = 300
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, close_price, change_percent, trading_volume "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY stock_code, `date`",
            batch + [lookback, latest_date]
        )
        for r in cur.fetchall():
            klines[r['stock_code']].append({
                'date': r['date'],
                'close': _to_float(r['close_price']),
                'change_percent': _to_float(r['change_percent']),
                'volume': _to_float(r['trading_volume']),
            })
    conn.close()
    return klines, latest_date


def _get_week_klines(stock_klines: list, iso_year: int, iso_week: int) -> list:
    result = []
    for k in stock_klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        iy, iw = dt.isocalendar()[:2]
        if iy == iso_year and iw == iso_week:
            result.append(k)
    return sorted(result, key=lambda x: x['date'])


def _extract_features(code: str, klines: dict,
                      iso_year: int, iso_week: int) -> dict | None:
    """提取预测所需的多维特征。"""
    stock_klines = klines.get(code, [])
    if not stock_klines:
        return None

    week_klines = _get_week_klines(stock_klines, iso_year, iso_week)
    if len(week_klines) < 3:
        return None

    daily_pcts = [k['change_percent'] for k in week_klines]
    this_week_chg = _compound_return(daily_pcts)

    market_code = _get_market_code(code)
    market_klines = klines.get(market_code, [])
    market_week = _get_week_klines(market_klines, iso_year, iso_week)
    market_chg = _compound_return(
        [k['change_percent'] for k in market_week]
    ) if len(market_week) >= 3 else 0.0

    # 前一周
    prev_w = iso_week - 1
    prev_y = iso_year
    if prev_w <= 0:
        prev_y -= 1
        dec28 = datetime(prev_y, 12, 28)
        prev_w = dec28.isocalendar()[1]

    market_prev = _get_week_klines(market_klines, prev_y, prev_w)
    market_prev_chg = _compound_return(
        [k['change_percent'] for k in market_prev]
    ) if len(market_prev) >= 3 else None

    prev_stock_week = _get_week_klines(stock_klines, prev_y, prev_w)
    prev_week_chg = _compound_return(
        [k['change_percent'] for k in prev_stock_week]
    ) if len(prev_stock_week) >= 3 else None

    # 连涨/连跌
    cd, cu = 0, 0
    for p in reversed(daily_pcts):
        if p < 0:
            cd += 1
            if cu > 0:
                break
        elif p > 0:
            cu += 1
            if cd > 0:
                break
        else:
            break

    # 60日价格位置
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

    # 量比
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
        'consec_down': cd, 'consec_up': cu,
        'last_day_chg': round(daily_pcts[-1], 2),
        '_price_pos_60': price_pos_60,
        '_prev_week_chg': round(prev_week_chg, 2) if prev_week_chg is not None else None,
        'vol_ratio': vol_ratio,
        # 以下字段在回测中为None，生产环境也暂不填充
        'ff_signal': None, 'vol_price_corr': None,
        'board_momentum': None, 'concept_consensus': None,
        'concept_boards': '', 'finance_score': None,
        'revenue_yoy': None, 'profit_yoy': None, 'roe': None,
        '_market_suffix': _get_market_code(code).split('.')[-1],
        '_market_code': _get_market_code(code),
    }


def _get_stock_names(stock_codes: list[str]) -> dict[str, str]:
    """从数据库获取股票名称。"""
    if not stock_codes:
        return {}
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        ph = ','.join(['%s'] * len(stock_codes))
        cur.execute(
            f"SELECT stock_code, stock_name FROM stock_concept_board_stock "
            f"WHERE CONCAT(stock_code, CASE "
            f"  WHEN stock_code LIKE '6%%' THEN '.SH' "
            f"  WHEN stock_code LIKE '0%%' OR stock_code LIKE '3%%' THEN '.SZ' "
            f"  ELSE '' END) IN ({ph}) "
            f"GROUP BY stock_code, stock_name",
            stock_codes
        )
        result = {}
        for r in cur.fetchall():
            raw = r['stock_code']
            if raw.startswith(('600', '601', '603', '605', '688')):
                full = raw + '.SH'
            elif raw.startswith(('000', '001', '002', '003', '300', '301')):
                full = raw + '.SZ'
            else:
                continue
            result[full] = r['stock_name']
        return result
    finally:
        conn.close()


async def run_deepseek_prediction(stock_codes: list[str],
                                  progress_callback=None) -> dict:
    """
    对选中的股票执行DeepSeek周预测。

    Args:
        stock_codes: 股票代码列表 (如 ['600519.SH', '000001.SZ'])
        progress_callback: 进度回调 (done, total, message)

    Returns:
        {
            'total': int,
            'predicted': int,  # DOWN预测数
            'uncertain': int,
            'failed': int,
            'results': [{'stock_code', 'stock_name', 'direction', ...}],
            'predict_date': str,
            'target_week': str,
        }
    """
    ensure_table()

    if progress_callback:
        progress_callback(0, len(stock_codes), '加载K线数据...')

    # 加载数据
    klines, latest_date = _load_kline_data(stock_codes)
    if not klines or not latest_date:
        return {'total': 0, 'predicted': 0, 'uncertain': 0, 'failed': 0,
                'results': [], 'error': '无法加载K线数据'}

    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    iso_cal = dt_latest.isocalendar()
    iso_year, iso_week = iso_cal[0], iso_cal[1]

    # 下周
    nw_monday = dt_latest + timedelta(days=(7 - dt_latest.weekday()))
    nw_friday = nw_monday + timedelta(days=4)
    nw_iso = nw_monday.isocalendar()

    # 获取股票名称
    name_map = _get_stock_names(stock_codes)

    if progress_callback:
        progress_callback(0, len(stock_codes), '开始预测...')

    # 逐个预测
    results = []
    predicted = 0
    uncertain = 0
    failed = 0
    db_records = []

    sem = asyncio.Semaphore(5)

    async def _predict_one(code):
        nonlocal predicted, uncertain, failed
        async with sem:
            name = name_map.get(code, '')
            feat = _extract_features(code, klines, iso_year, iso_week)

            if not feat:
                failed += 1
                return

            result = await predict_next_week_with_deepseek(code, name, feat)

            if result is None:
                failed += 1
                return

            direction = result['direction']
            is_prefilter_pass = result['confidence'] > 0  # confidence=0 means prefiltered out

            record = {
                'stock_code': code,
                'stock_name': name,
                'predict_date': latest_date,
                'iso_year': iso_year,
                'iso_week': iso_week,
                'target_iso_year': nw_iso[0],
                'target_iso_week': nw_iso[1],
                'target_date_range': f'{nw_monday.strftime("%Y-%m-%d")}~{nw_friday.strftime("%Y-%m-%d")}',
                'pred_direction': direction,
                'confidence': result.get('confidence'),
                'justification': result.get('justification', ''),
                'prefilter_pass': 1 if is_prefilter_pass else 0,
                'prefilter_reason': result.get('justification', '') if not is_prefilter_pass else None,
                'this_week_chg': feat['this_week_chg'],
                'market_chg': feat['market_chg'],
                'price_pos_60': feat.get('_price_pos_60'),
                'vol_ratio': feat.get('vol_ratio'),
                'prev_week_chg': feat.get('_prev_week_chg'),
                'last_day_chg': feat.get('last_day_chg'),
                'consec_up': feat.get('consec_up'),
            }

            if direction == 'DOWN':
                predicted += 1
            else:
                uncertain += 1

            results.append(record)
            db_records.append(record)

    tasks = [_predict_one(c) for c in stock_codes]
    done = 0
    for coro in asyncio.as_completed(tasks):
        await coro
        done += 1
        if progress_callback and done % 5 == 0:
            progress_callback(done, len(stock_codes),
                              f'已完成 {done}/{len(stock_codes)}')

    # 存入数据库
    if db_records:
        try:
            batch_insert_predictions(db_records)
        except Exception as e:
            logger.error("保存预测记录失败: %s", e)

    if progress_callback:
        progress_callback(len(stock_codes), len(stock_codes), '预测完成')

    return {
        'total': len(stock_codes),
        'predicted': predicted,
        'uncertain': uncertain,
        'failed': failed,
        'results': sorted(results, key=lambda r: (
            0 if r['pred_direction'] == 'DOWN' else 1,
            -r.get('this_week_chg', 0)
        )),
        'predict_date': latest_date,
        'target_week': f'W{nw_iso[1]} ({nw_monday.strftime("%m-%d")}~{nw_friday.strftime("%m-%d")})',
    }
