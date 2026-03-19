#!/usr/bin/env python3
"""
三方深度对比回测：V4规则 vs V5规则 vs DeepSeek
============================================================
50只不同概念板块个股，16周滚动回测，深度分析。

用法：
    .venv/bin/python -m day_week_predicted.backtest.deepseek_50stocks_16weeks_backtest
"""
import asyncio
import json
import logging
import random
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

_log_path = Path(__file__).parent.parent.parent / 'data_results' / 'four_way_50stocks_16w_log.txt'
_fh = logging.FileHandler(str(_log_path), mode='w', encoding='utf-8')
_fh.setLevel(logging.INFO)
_fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
logging.getLogger().addHandler(_fh)

NUM_STOCKS = 50
NUM_WEEKS = 16


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


def _get_iso_week(date_str: str) -> tuple:
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    return dt.isocalendar()[:2]


def _prev_iso_week(iso_year: int, iso_week: int) -> tuple:
    if iso_week > 1:
        return iso_year, iso_week - 1
    dec28 = datetime(iso_year - 1, 12, 28)
    return dec28.isocalendar()[0], dec28.isocalendar()[1]


def _next_iso_week(iso_year: int, iso_week: int) -> tuple:
    dec28 = datetime(iso_year, 12, 28)
    max_week = dec28.isocalendar()[1]
    if iso_week < max_week:
        return iso_year, iso_week + 1
    return iso_year + 1, 1


def _get_week_klines(stock_klines: list, iso_year: int, iso_week: int) -> list:
    result = []
    for k in stock_klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        iy, iw = dt.isocalendar()[:2]
        if iy == iso_year and iw == iso_week:
            result.append(k)
    return sorted(result, key=lambda x: x['date'])


def _get_market_code(code: str) -> str:
    prefix3 = code[:3]
    mapping = {
        '600': '000001.SH', '601': '000001.SH', '603': '000001.SH', '605': '000001.SH',
        '688': '000001.SH',
        '000': '399001.SZ', '001': '399001.SZ', '002': '399001.SZ', '003': '399001.SZ',
        '300': '399001.SZ', '301': '399001.SZ',
        '430': '899050.SZ', '830': '899050.SZ', '831': '899050.SZ',
        '832': '899050.SZ', '833': '899050.SZ', '834': '899050.SZ',
        '835': '899050.SZ', '836': '899050.SZ', '837': '899050.SZ',
        '838': '899050.SZ', '839': '899050.SZ', '870': '899050.SZ',
        '871': '899050.SZ', '872': '899050.SZ', '873': '899050.SZ',
    }
    if prefix3 in mapping:
        return mapping[prefix3]
    return '000001.SH' if code.endswith('.SH') else '399001.SZ'


def _select_stocks() -> list[tuple[str, str]]:
    """从概念板块中选取50只股票，每个板块最多取1只。"""
    from dao import get_connection

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    cur.execute("""
        SELECT b.board_name, s.stock_code, s.stock_name
        FROM stock_concept_board_stock s
        JOIN stock_concept_board b ON s.board_code = b.board_code
        WHERE s.stock_code NOT LIKE '4%%'
          AND s.stock_code NOT LIKE '8%%'
          AND s.stock_name NOT LIKE '%%ST%%'
          AND s.stock_name NOT LIKE '%%st%%'
        ORDER BY b.board_name, s.stock_code
    """)
    rows = cur.fetchall()

    board_stocks = defaultdict(list)
    all_raw_codes = set()
    code_name_map = {}
    for r in rows:
        raw_code = r['stock_code']
        name = r['stock_name']
        if raw_code.startswith(('600', '601', '603', '605', '688')):
            code = raw_code + '.SH'
        elif raw_code.startswith(('000', '001', '002', '003', '300', '301')):
            code = raw_code + '.SZ'
        else:
            continue
        board_stocks[r['board_name']].append(code)
        all_raw_codes.add(code)
        code_name_map[code] = name

    # 批量检查K线数据充足性（16周需要更多历史）
    all_codes_list = list(all_raw_codes)
    valid_codes = set()
    bs = 500
    for i in range(0, len(all_codes_list), bs):
        batch = all_codes_list[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code FROM stock_kline "
            f"WHERE stock_code IN ({ph}) "
            f"GROUP BY stock_code HAVING COUNT(*) >= 120",
            batch
        )
        for r in cur.fetchall():
            valid_codes.add(r['stock_code'])

    conn.close()
    logger.info("有效股票(K线≥120): %d / %d", len(valid_codes), len(all_raw_codes))

    selected = {}
    used_codes = set()
    board_names = list(board_stocks.keys())
    random.seed(42)
    random.shuffle(board_names)

    for board_name in board_names:
        if len(selected) >= NUM_STOCKS:
            break
        codes = list(board_stocks[board_name])
        random.shuffle(codes)
        for code in codes:
            if code not in used_codes and code in valid_codes:
                selected[code] = (code, code_name_map[code])
                used_codes.add(code)
                break

    result = list(selected.values())
    logger.info("已选取 %d 只股票（来自 %d 个概念板块）", len(result), len(board_names))
    return result


def _load_kline_data(stock_list: list[tuple[str, str]]) -> tuple[dict, str]:
    from dao import get_connection

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    cur.execute("SELECT MAX(`date`) as d FROM stock_kline WHERE stock_code = '000001.SH'")
    row = cur.fetchone()
    latest_date = row['d'] if row else None
    if not latest_date:
        logger.error("无法获取最新交易日")
        return {}, ''

    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    # 16周≈112天 + 60日位置 + 余量 → 300天
    lookback = (dt_latest - timedelta(days=300)).strftime('%Y-%m-%d')

    codes = [c for c, _ in stock_list]
    all_codes = codes + ['000001.SH', '399001.SZ', '899050.SZ']

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
    logger.info("K线数据加载完成: %d只股票, 最新日期 %s", len(klines), latest_date)
    return klines, latest_date


def _extract_features(code: str, name: str, klines: dict,
                      iso_year: int, iso_week: int) -> dict | None:
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

    prev_y, prev_w = _prev_iso_week(iso_year, iso_week)
    market_prev = _get_week_klines(market_klines, prev_y, prev_w)
    market_prev_chg = _compound_return(
        [k['change_percent'] for k in market_prev]
    ) if len(market_prev) >= 3 else None

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

    prev_stock_week = _get_week_klines(stock_klines, prev_y, prev_w)
    prev_week_chg = _compound_return(
        [k['change_percent'] for k in prev_stock_week]
    ) if len(prev_stock_week) >= 3 else None

    vol_ratio = None
    if len(hist) >= 20:
        avg_vol = sum(k['volume'] for k in hist[-20:]) / 20
        week_avg_vol = sum(k['volume'] for k in week_klines) / len(week_klines)
        if avg_vol > 0:
            vol_ratio = round(week_avg_vol / avg_vol, 2)

    suffix = market_code.split('.')[-1] if '.' in market_code else 'SH'

    return {
        'this_week_chg': round(this_week_chg, 2),
        'market_chg': round(market_chg, 2),
        '_market_prev_week_chg': round(market_prev_chg, 2) if market_prev_chg is not None else None,
        'consec_down': cd, 'consec_up': cu,
        'last_day_chg': round(daily_pcts[-1], 2),
        '_market_suffix': suffix,
        '_price_pos_60': price_pos_60,
        '_prev_week_chg': round(prev_week_chg, 2) if prev_week_chg is not None else None,
        'ff_signal': None, 'vol_ratio': vol_ratio,
        'vol_price_corr': None, 'board_momentum': None,
        'concept_consensus': None, 'concept_boards': '',
        'finance_score': None, 'revenue_yoy': None,
        'profit_yoy': None, 'roe': None,
        '_daily_pcts': daily_pcts, '_market_code': market_code,
    }


def _check_next_week_actual(code: str, klines: dict,
                            iso_year: int, iso_week: int) -> float | None:
    nw_y, nw_w = _next_iso_week(iso_year, iso_week)
    stock_klines = klines.get(code, [])
    nw_klines = _get_week_klines(stock_klines, nw_y, nw_w)
    if len(nw_klines) < 3:
        return None
    return round(_compound_return([k['change_percent'] for k in nw_klines]), 2)


# ═══════════════════════════════════════════════════════════
# V4 / V5 规则引擎
# ═══════════════════════════════════════════════════════════

def _predict_with_v4_rules(features: dict) -> dict:
    feat = _build_rule_feat(features)
    V4_RULES = [
        {'name': 'R1:大盘深跌+个股跌→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f: f['this_chg'] < -2 and f['mkt_chg'] < -3},
        {'name': 'R2:上证+大盘跌+跌>5%+非高位→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f: (f['this_chg'] < -5 and f['suffix'] == 'SH'
                             and -3 <= f['mkt_chg'] < -1
                             and not (f['pos60'] is not None and f['pos60'] >= 0.7))},
        {'name': 'R3:上证+大盘跌+跌>3%+前周跌→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f: (f['this_chg'] < -3 and f['suffix'] == 'SH'
                             and -3 <= f['mkt_chg'] < -1
                             and f['prev_chg'] is not None and f['prev_chg'] < -2
                             and not (f['pos60'] is not None and f['pos60'] >= 0.8))},
        {'name': 'R4:上证+大盘跌+跌>3%+低位→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f: (f['this_chg'] < -3 and f['suffix'] == 'SH'
                             and -3 <= f['mkt_chg'] < -1
                             and f['pos60'] is not None and f['pos60'] < 0.2)},
        {'name': 'R5a:深证+大盘微跌+跌+连跌3天→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                             and f['this_chg'] < -2 and f['cd'] >= 3)},
        {'name': 'R5b:深证+大盘微跌+跌+低位→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                             and f['this_chg'] < -2
                             and f['pos60'] is not None and f['pos60'] < 0.2)},
        {'name': 'R5c:深证+大盘微跌+跌>2%→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                             and f['this_chg'] < -2)},
        {'name': 'R6a:深证+大盘跌+涨>5%→跌', 'pred_up': False, 'tier': 1,
         'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                             and f['this_chg'] > 5)},
        {'name': 'R6b:深证+大盘跌+涨+连涨4天→跌', 'pred_up': False, 'tier': 1,
         'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                             and f['this_chg'] > 2 and f['cu'] >= 4)},
        {'name': 'R6c:深证+大盘跌+涨+连涨3天→跌', 'pred_up': False, 'tier': 1,
         'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                             and f['this_chg'] > 2 and f['cu'] >= 3)},
        {'name': 'R7:跌+前期连涨+非高位→跌', 'pred_up': False, 'tier': 2,
         'check': lambda f: (f['this_chg'] < -3 and f['cu'] >= 3
                             and f['pos60'] is not None and f['pos60'] < 0.6)},
        {'name': 'R8:大涨+尾日回落+前周涨→跌', 'pred_up': False, 'tier': 2,
         'check': lambda f: (f['this_chg'] > 10 and f['last_day'] < -3
                             and f['prev_chg'] is not None and f['prev_chg'] > 3)},
        {'name': 'R9:上证+大盘微跌+涨+前周跌→跌', 'pred_up': False, 'tier': 2,
         'check': lambda f: (f['suffix'] == 'SH' and -1 <= f['mkt_chg'] < 0
                             and f['this_chg'] > 2
                             and f['prev_chg'] is not None and f['prev_chg'] < -3)},
    ]
    for rule in V4_RULES:
        try:
            if rule['check'](feat):
                tier = rule.get('tier', 2)
                conf_map = {1: 0.75, 2: 0.65}
                return {
                    'direction': 'UP' if rule['pred_up'] else 'DOWN',
                    'confidence': conf_map.get(tier, 0.60),
                    'rule_name': rule['name'], 'tier': tier,
                }
        except (TypeError, KeyError):
            continue
    return {'direction': 'UNCERTAIN', 'confidence': 0.0, 'rule_name': '无匹配规则', 'tier': 0}


def _predict_with_v5_rules(features: dict) -> dict:
    from service.weekly_prediction_service import _nw_extract_features, _nw_match_rule

    daily_pcts = features['_daily_pcts']
    market_chg = features['market_chg']
    market_code = features['_market_code']

    feat = _nw_extract_features(
        daily_pcts=daily_pcts, market_chg=market_chg,
        ff_signal=features.get('ff_signal'), vol_ratio=features.get('vol_ratio'),
        vol_price_corr=features.get('vol_price_corr'),
        finance_score=features.get('finance_score'),
        market_index=market_code,
        price_pos_60=features.get('_price_pos_60'),
        prev_week_chg=features.get('_prev_week_chg'),
    )

    rule = _nw_match_rule(feat)
    if rule is None:
        return {'direction': 'UNCERTAIN', 'confidence': 0.0, 'rule_name': '无匹配规则', 'tier': 0}

    tier = rule.get('tier', 3)
    conf_map = {1: 0.75, 2: 0.65, 3: 0.55}
    return {
        'direction': 'UP' if rule['pred_up'] else 'DOWN',
        'confidence': conf_map.get(tier, 0.55),
        'rule_name': rule['name'], 'tier': tier,
    }


def _build_rule_feat(features: dict) -> dict:
    return {
        'this_chg': features['this_week_chg'],
        'mkt_chg': features['market_chg'],
        'pos60': features.get('_price_pos_60'),
        'prev_chg': features.get('_prev_week_chg'),
        'cd': features.get('consec_down', 0),
        'cu': features.get('consec_up', 0),
        'vol_ratio': features.get('vol_ratio'),
        'suffix': features.get('_market_suffix', ''),
        'last_day': features.get('last_day_chg', 0),
    }


# ═══════════════════════════════════════════════════════════
# 统计类
# ═══════════════════════════════════════════════════════════

class MethodStats:
    def __init__(self, name: str):
        self.name = name
        self.correct = 0
        self.total = 0
        self.uncertain = 0
        self.high_conf_correct = 0
        self.high_conf_total = 0
        self.details = []
        self.by_suffix = defaultdict(lambda: {'correct': 0, 'total': 0, 'uncertain': 0})
        self.by_market_regime = defaultdict(lambda: {'correct': 0, 'total': 0, 'uncertain': 0})
        self.by_stock_chg_bin = defaultdict(lambda: {'correct': 0, 'total': 0, 'uncertain': 0})
        self.by_direction = defaultdict(lambda: {'correct': 0, 'total': 0})
        self.by_week = defaultdict(lambda: {'correct': 0, 'total': 0, 'uncertain': 0})

    def add(self, code, name, week_label, direction, confidence,
            actual, market_chg, this_week_chg, suffix, extra_info=''):
        if market_chg < -2:
            regime = '大盘大跌(<-2%)'
        elif market_chg < -0.5:
            regime = '大盘小跌(-2~-0.5%)'
        elif market_chg <= 0.5:
            regime = '大盘震荡(-0.5~0.5%)'
        elif market_chg <= 2:
            regime = '大盘小涨(0.5~2%)'
        else:
            regime = '大盘大涨(>2%)'

        if this_week_chg < -5:
            chg_bin = '暴跌(<-5%)'
        elif this_week_chg < -2:
            chg_bin = '下跌(-5~-2%)'
        elif this_week_chg <= 2:
            chg_bin = '震荡(-2~2%)'
        elif this_week_chg <= 5:
            chg_bin = '上涨(2~5%)'
        else:
            chg_bin = '暴涨(>5%)'

        detail = {
            'week': week_label, 'code': code, 'name': name,
            'direction': direction, 'confidence': confidence,
            'actual': actual, 'market_chg': market_chg,
            'this_week_chg': this_week_chg, 'suffix': suffix,
            'regime': regime, 'chg_bin': chg_bin, 'extra': extra_info,
        }

        if direction == 'UNCERTAIN':
            self.uncertain += 1
            self.by_suffix[suffix]['uncertain'] += 1
            self.by_market_regime[regime]['uncertain'] += 1
            self.by_stock_chg_bin[chg_bin]['uncertain'] += 1
            self.by_week[week_label]['uncertain'] += 1
            detail['mark'] = '⏸️'
            self.details.append(detail)
            return '⏸️'

        self.total += 1
        actual_up = actual > 0
        pred_up = direction == 'UP'
        is_correct = actual_up == pred_up

        if is_correct:
            self.correct += 1
        mark = '✅' if is_correct else '❌'

        if confidence >= 0.65:
            self.high_conf_total += 1
            if is_correct:
                self.high_conf_correct += 1

        for dim_dict, key in [
            (self.by_suffix, suffix),
            (self.by_market_regime, regime),
            (self.by_stock_chg_bin, chg_bin),
            (self.by_week, week_label),
        ]:
            dim_dict[key]['total'] += 1
            if is_correct:
                dim_dict[key]['correct'] += 1

        self.by_direction[direction]['total'] += 1
        if is_correct:
            self.by_direction[direction]['correct'] += 1

        detail['mark'] = mark
        self.details.append(detail)
        return mark

    @property
    def accuracy(self):
        return self.correct / self.total * 100 if self.total > 0 else 0

    @property
    def high_conf_accuracy(self):
        return self.high_conf_correct / self.high_conf_total * 100 if self.high_conf_total > 0 else 0

    @property
    def coverage(self):
        all_count = self.total + self.uncertain
        return self.total / all_count * 100 if all_count > 0 else 0

    def summary_dict(self):
        return {
            'model': self.name,
            'total_predictions': self.total,
            'uncertain': self.uncertain,
            'correct': self.correct,
            'accuracy': round(self.accuracy, 1),
            'coverage': round(self.coverage, 1),
            'high_conf_total': self.high_conf_total,
            'high_conf_accuracy': round(self.high_conf_accuracy, 1),
        }

    def dim_summary(self, dim_dict, dim_name):
        lines = [f"  ── 按{dim_name} ──"]
        _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"
        for key in sorted(dim_dict.keys()):
            s = dim_dict[key]
            t, c, u = s['total'], s['correct'], s['uncertain']
            lines.append(f"    {key:20s} 准确{_p(c,t)}({c}/{t}) 未判{u}")
        return '\n'.join(lines)


# ═══════════════════════════════════════════════════════════
# 深度分析
# ═══════════════════════════════════════════════════════════

def _deep_analysis(stats_list, weekly_all_results):
    v4, v5, ds = stats_list
    logger.info("")
    logger.info("=" * 80)
    logger.info("  深度分析报告")
    logger.info("=" * 80)

    logger.info("")
    logger.info("  ┌─ 总体对比 ─────────────────────────────────────────┐")
    logger.info("  │ %-16s│%8s│%8s│%8s│%8s│%8s│",
                "方法", "准确率", "高置信", "覆盖率", "预测数", "UNCERT")
    logger.info("  │%s│", "─" * 56)
    for s in stats_list:
        logger.info("  │ %-16s│%7.1f%%│%7.1f%%│%7.1f%%│%8d│%8d│",
                     s.name, s.accuracy, s.high_conf_accuracy,
                     s.coverage, s.total, s.uncertain)
    logger.info("  └%s┘", "─" * 57)

    for s in stats_list:
        logger.info("")
        logger.info("  [%s]", s.name)
        logger.info(s.dim_summary(s.by_suffix, '市场'))
        logger.info(s.dim_summary(s.by_market_regime, '大盘行情'))
        logger.info(s.dim_summary(s.by_stock_chg_bin, '个股涨跌'))
        logger.info(s.dim_summary(s.by_week, '周'))
        _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"
        for d in ['UP', 'DOWN']:
            dd = s.by_direction.get(d, {'correct': 0, 'total': 0})
            logger.info("    预测%s: %s (%d/%d)", d,
                        _p(dd['correct'], dd['total']),
                        dd['correct'], dd['total'])

    # 方法间一致性
    logger.info("")
    logger.info("  ┌─ 方法间一致性分析 ──────────────────────────────────┐")
    pred_map = defaultdict(dict)
    actual_map = {}
    for s in stats_list:
        for d in s.details:
            key = (d['code'], d['week'])
            pred_map[key][s.name] = d['direction']
            actual_map[key] = d['actual']

    ds_v5_agree_down = {'correct': 0, 'total': 0}
    ds_v4_agree_down = {'correct': 0, 'total': 0}

    for key, preds in pred_map.items():
        ds_dir = preds.get('DeepSeek')
        v4_dir = preds.get('V4规则')
        v5_dir = preds.get('V5规则')
        actual = actual_map.get(key)
        if not ds_dir or actual is None or ds_dir == 'UNCERTAIN':
            continue
        actual_up = actual > 0
        if v5_dir and v5_dir != 'UNCERTAIN' and ds_dir == 'DOWN' and v5_dir == 'DOWN':
            ds_v5_agree_down['total'] += 1
            if not actual_up:
                ds_v5_agree_down['correct'] += 1
        if v4_dir and v4_dir != 'UNCERTAIN' and ds_dir == 'DOWN' and v4_dir == 'DOWN':
            ds_v4_agree_down['total'] += 1
            if not actual_up:
                ds_v4_agree_down['correct'] += 1

    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"
    logger.info("  │ DeepSeek+V5规则同时看跌: %s (%d/%d)",
                _p(ds_v5_agree_down['correct'], ds_v5_agree_down['total']),
                ds_v5_agree_down['correct'], ds_v5_agree_down['total'])
    logger.info("  │ DeepSeek+V4规则同时看跌: %s (%d/%d)",
                _p(ds_v4_agree_down['correct'], ds_v4_agree_down['total']),
                ds_v4_agree_down['correct'], ds_v4_agree_down['total'])

    rule_hit_analysis = {'v4': defaultdict(int), 'v5': defaultdict(int)}
    for key, preds in pred_map.items():
        actual = actual_map.get(key)
        if actual is None:
            continue
        for rule_name, rule_key in [('V4规则', 'v4'), ('V5规则', 'v5')]:
            r_dir = preds.get(rule_name)
            if r_dir and r_dir != 'UNCERTAIN':
                rule_hit_analysis[rule_key]['hit'] += 1
                l_dir = preds.get('DeepSeek')
                if l_dir and l_dir != 'UNCERTAIN' and l_dir == r_dir:
                    rule_hit_analysis[rule_key]['DeepSeek_agree'] += 1

    for rule_key, label in [('v4', 'V4规则'), ('v5', 'V5规则')]:
        rha = rule_hit_analysis[rule_key]
        hit = rha.get('hit', 0)
        if hit > 0:
            agree = rha.get('DeepSeek_agree', 0)
            logger.info("  │ %s命中%d次, DeepSeek一致: %d/%d = %s",
                        label, hit, agree, hit, _p(agree, hit))
    logger.info("  └%s┘", "─" * 57)

    # 共识信号
    logger.info("")
    logger.info("  ┌─ 共识信号分析 ──────────────────────────────────────┐")
    consensus_3 = {'correct': 0, 'total': 0}
    ds_plus_any_rule = {'correct': 0, 'total': 0}
    for key, preds in pred_map.items():
        actual = actual_map.get(key)
        if actual is None:
            continue
        actual_up = actual > 0
        ds_d = preds.get('DeepSeek')
        v4_d = preds.get('V4规则')
        v5_d = preds.get('V5规则')
        non_unc = [d for d in [ds_d, v4_d, v5_d] if d and d != 'UNCERTAIN']
        if len(non_unc) == 3 and len(set(non_unc)) == 1:
            consensus_3['total'] += 1
            if (non_unc[0] == 'UP') == actual_up:
                consensus_3['correct'] += 1
        if ds_d and ds_d != 'UNCERTAIN':
            for r_d in [v4_d, v5_d]:
                if r_d and r_d != 'UNCERTAIN' and r_d == ds_d:
                    ds_plus_any_rule['total'] += 1
                    if (ds_d == 'UP') == actual_up:
                        ds_plus_any_rule['correct'] += 1
                    break

    logger.info("  │ 三方共识(V4+V5+DS): %s (%d/%d)",
                _p(consensus_3['correct'], consensus_3['total']),
                consensus_3['correct'], consensus_3['total'])
    logger.info("  │ DS+任一规则一致: %s (%d/%d)",
                _p(ds_plus_any_rule['correct'], ds_plus_any_rule['total']),
                ds_plus_any_rule['correct'], ds_plus_any_rule['total'])
    logger.info("  └%s┘", "─" * 57)

    # 错误案例
    logger.info("")
    logger.info("  ┌─ DeepSeek错误案例 ─────────────────────────────────┐")
    ds_wrong = []
    for key, preds in pred_map.items():
        actual = actual_map.get(key)
        if actual is None:
            continue
        actual_up = actual > 0
        ds_d = preds.get('DeepSeek')
        if ds_d and ds_d != 'UNCERTAIN' and (ds_d == 'UP') != actual_up:
            v5_d = preds.get('V5规则', 'N/A')
            ds_wrong.append((key[0], key[1], actual, ds_d, v5_d))

    for code, week, actual, ds_d, v5_d in ds_wrong[:20]:
        logger.info("  │ %s %s: DS=%s V5=%s 实际=%+.2f%%",
                    code, week, ds_d, v5_d, actual)
    if len(ds_wrong) > 20:
        logger.info("  │ ... 共%d个", len(ds_wrong))
    logger.info("  └%s┘", "─" * 57)


# ═══════════════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════════════

async def main():
    t0 = time.time()
    logger.info("=" * 80)
    logger.info("  三方深度对比回测: V4规则 vs V5规则 vs DeepSeek (V14多维预过滤)")
    logger.info("  目标: %d只不同概念板块个股, %d周滚动", NUM_STOCKS, NUM_WEEKS)
    logger.info("=" * 80)

    stock_list = _select_stocks()
    if len(stock_list) < 20:
        logger.error("选股不足20只，退出")
        return

    klines, latest_date = _load_kline_data(stock_list)
    if not klines:
        return

    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    current_iso = dt_latest.isocalendar()

    # 回测周范围：往前 2~(NUM_WEEKS+1) 周（跳过最近1周避免数据不全）
    test_weeks = []
    for offset in range(2, NUM_WEEKS + 2):
        y, w = current_iso[0], current_iso[1] - offset
        while w <= 0:
            y -= 1
            dec28 = datetime(y, 12, 28)
            max_w = dec28.isocalendar()[1]
            w += max_w
        test_weeks.append((y, w))

    logger.info("回测范围: %d周, 股票: %d只, 最大样本: %d",
                len(test_weeks), len(stock_list),
                len(test_weeks) * len(stock_list))

    from service.analysis.deepseek_nw_predictor import predict_next_week_with_deepseek

    v4_stats = MethodStats('V4规则')
    v5_stats = MethodStats('V5规则')
    ds_stats = MethodStats('DeepSeek')
    all_stats = [v4_stats, v5_stats, ds_stats]

    weekly_all_results = []
    total_llm_calls = 0
    total_samples = 0

    for pred_y, pred_w in test_weeks:
        nw_y, nw_w = _next_iso_week(pred_y, pred_w)
        week_label = f"W{pred_w}→W{nw_w}"

        logger.info("")
        logger.info("─" * 80)
        logger.info("  预测周 W%d → 验证周 W%d", pred_w, nw_w)
        logger.info("─" * 80)

        samples = []
        actuals = {}
        for code, name in stock_list:
            feat = _extract_features(code, name, klines, pred_y, pred_w)
            actual = _check_next_week_actual(code, klines, pred_y, pred_w)
            if feat and actual is not None:
                samples.append({'code': code, 'name': name, 'features': feat})
                actuals[code] = actual

        if not samples:
            logger.warning("  W%d 无有效样本", pred_w)
            continue

        logger.info("  有效样本: %d", len(samples))
        total_samples += len(samples)

        v4_results_map = {}
        v5_results_map = {}
        for s in samples:
            v4_results_map[s['code']] = _predict_with_v4_rules(s['features'])
            v5_results_map[s['code']] = _predict_with_v5_rules(s['features'])

        ds_sem = asyncio.Semaphore(5)

        async def _call_ds(s):
            async with ds_sem:
                return await predict_next_week_with_deepseek(
                    s['code'], s['name'], s['features'])

        logger.info("  调用DeepSeek: %d只 ...", len(samples))
        t_llm = time.time()

        ds_results = await asyncio.gather(
            *[_call_ds(s) for s in samples], return_exceptions=True)

        total_llm_calls += len(samples)
        logger.info("  LLM调用完成, 耗时 %.1fs", time.time() - t_llm)

        week_record = {'week': week_label, 'stocks': []}

        for i, s in enumerate(samples):
            code = s['code']
            name = s['name']
            actual = actuals[code]
            feat = s['features']
            mkt_chg = feat['market_chg']
            this_chg = feat['this_week_chg']
            suffix = feat['_market_suffix']

            v4r = v4_results_map[code]
            v4_stats.add(code, name, week_label, v4r['direction'],
                         v4r['confidence'], actual, mkt_chg, this_chg,
                         suffix, v4r.get('rule_name', ''))

            v5r = v5_results_map[code]
            v5_stats.add(code, name, week_label, v5r['direction'],
                         v5r['confidence'], actual, mkt_chg, this_chg,
                         suffix, v5r.get('rule_name', ''))

            dr = ds_results[i] if not isinstance(ds_results[i], Exception) else None
            if dr:
                ds_stats.add(code, name, week_label, dr['direction'],
                             dr['confidence'], actual, mkt_chg, this_chg, suffix)
            else:
                ds_stats.add(code, name, week_label, 'UNCERTAIN',
                             0.0, actual, mkt_chg, this_chg, suffix, 'FAIL')

            week_record['stocks'].append({
                'code': code, 'name': name, 'actual': actual,
                'v4': v4r, 'v5': v5r, 'deepseek': dr,
            })

        weekly_all_results.append(week_record)

        logger.info("  W%d小结:", pred_w)
        for s in all_stats:
            ws = s.by_week.get(week_label, {'correct': 0, 'total': 0, 'uncertain': 0})
            t, c, u = ws['total'], ws['correct'], ws['uncertain']
            acc = f"{c/t*100:.1f}%" if t > 0 else "N/A"
            logger.info("    %-14s 准确%s(%d/%d) 未判%d",
                        s.name, acc, c, t, u)

    _deep_analysis(all_stats, weekly_all_results)

    elapsed = time.time() - t0
    output = {
        'date': latest_date,
        'elapsed_seconds': round(elapsed, 1),
        'total_samples': total_samples,
        'total_llm_calls': total_llm_calls,
        'stock_count': len(stock_list),
        'week_count': len(test_weeks),
        'test_weeks': [f"W{y}-W{w}" for y, w in test_weeks],
        'stocks': [f"{c}({n})" for c, n in stock_list],
        'summary': {s.name: s.summary_dict() for s in all_stats},
    }

    dim_results = {}
    for s in all_stats:
        dim_results[s.name] = {
            'by_suffix': {k: dict(v) for k, v in s.by_suffix.items()},
            'by_market_regime': {k: dict(v) for k, v in s.by_market_regime.items()},
            'by_stock_chg_bin': {k: dict(v) for k, v in s.by_stock_chg_bin.items()},
            'by_direction': {k: dict(v) for k, v in s.by_direction.items()},
            'by_week': {k: dict(v) for k, v in s.by_week.items()},
        }
    output['dimensions'] = dim_results
    output['weekly_results'] = weekly_all_results

    out_path = (Path(__file__).parent.parent.parent
                / 'data_results' / 'four_way_50stocks_16w_result.json')
    out_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2, default=str),
        encoding='utf-8'
    )

    logger.info("")
    logger.info("=" * 80)
    logger.info("  回测完成! 耗时 %.1f分钟", elapsed / 60)
    logger.info("  总样本: %d, LLM调用: %d次", total_samples, total_llm_calls)
    logger.info("  结果已保存: %s", out_path)
    logger.info("=" * 80)


if __name__ == '__main__':
    asyncio.run(main())
