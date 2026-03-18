#!/usr/bin/env python3
"""
三方对比回测：Qwen3-235B vs DeepSeek vs V5规则引擎
===================================================
从数据库取20只不同概念板块的股票，分别用三种方法预测下周涨跌，
与实际涨跌对比，输出三方准确率、高置信准确率、覆盖率等指标。

用法：
    .venv/bin/python -m day_week_predicted.backtest.qwen_vs_deepseek_vs_rules_backtest
"""
import asyncio
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ── 20只股票，覆盖20个不同概念板块 ──
TEST_STOCKS = [
    ('600519.SH', '贵州茅台'),   # 白酒
    ('002594.SZ', '比亚迪'),     # 新能源车
    ('601166.SH', '兴业银行'),   # 银行
    ('688981.SH', '中芯国际'),   # 半导体
    ('300760.SZ', '迈瑞医疗'),   # 医药
    ('000333.SZ', '美的集团'),   # 家电
    ('600893.SH', '航发动力'),   # 军工
    ('601012.SH', '隆基绿能'),   # 光伏
    ('002415.SZ', '海康威视'),   # AI/安防
    ('601688.SH', '华泰证券'),   # 券商
    ('601899.SH', '紫金矿业'),   # 有色金属
    ('001979.SZ', '招商蛇口'),   # 地产
    ('002475.SZ', '立讯精密'),   # 消费电子
    ('601601.SH', '中国太保'),   # 保险
    ('600900.SH', '长江电力'),   # 电力
    ('000998.SZ', '隆平高科'),   # 农业
    ('600309.SH', '万华化学'),   # 化工
    ('600031.SH', '三一重工'),   # 机械
    ('000568.SZ', '泸州老窖'),   # 食品饮料
    ('600050.SH', '中国联通'),   # 通信
]

# ═══════════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════════

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
    return datetime.strptime(date_str, '%Y-%m-%d').isocalendar()[:2]


def _prev_iso_week(iso_year: int, iso_week: int) -> tuple:
    w = iso_week - 1
    y = iso_year
    if w <= 0:
        y -= 1
        w = 52 + w
    return (y, w)


def _next_iso_week(iso_year: int, iso_week: int) -> tuple:
    w = iso_week + 1
    y = iso_year
    if w > 52:
        y += 1
        w = 1
    return (y, w)


def _get_week_klines(stock_klines: list, iso_year: int, iso_week: int) -> list:
    result = [k for k in stock_klines
              if datetime.strptime(k['date'], '%Y-%m-%d').isocalendar()[:2] == (iso_year, iso_week)]
    result.sort(key=lambda x: x['date'])
    return result


# ═══════════════════════════════════════════════════════════
# 数据加载
# ═══════════════════════════════════════════════════════════

def _load_test_data():
    """从数据库加载测试股票 + 大盘指数的K线数据（近120天）。"""
    from dao import get_connection

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    cur.execute("SELECT MAX(`date`) as d FROM stock_kline WHERE stock_code = '000001.SH'")
    row = cur.fetchone()
    latest_date = row['d'] if row else None
    if not latest_date:
        logger.error("无法获取最新交易日")
        return None, None

    logger.info("最新交易日: %s", latest_date)

    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    lookback = (dt_latest - timedelta(days=150)).strftime('%Y-%m-%d')

    codes = [c for c, _ in TEST_STOCKS]
    # 加载三个大盘指数
    all_codes = codes + ['000001.SH', '399001.SZ', '899050.SZ']

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


# ═══════════════════════════════════════════════════════════
# 特征提取（LLM 用 + V5 规则引擎用）
# ═══════════════════════════════════════════════════════════

def _get_market_code(code: str) -> str:
    """根据股票代码返回对应的大盘指数代码（与 weekly_prediction_service._get_stock_index 一致）。"""
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
    if code.endswith('.SZ'):
        return '399001.SZ'
    if code.endswith('.SH'):
        return '000001.SH'
    return '000001.SH'


def _extract_features(code: str, name: str, klines: dict,
                      iso_year: int, iso_week: int) -> dict | None:
    """提取多维特征，同时兼容 LLM 预测器和 V5 规则引擎。"""
    stock_klines = klines.get(code, [])
    if not stock_klines:
        return None

    week_klines = _get_week_klines(stock_klines, iso_year, iso_week)
    if len(week_klines) < 3:
        return None

    daily_pcts = [k['change_percent'] for k in week_klines]
    this_week_chg = _compound_return(daily_pcts)

    # 大盘
    market_code = _get_market_code(code)
    market_klines = klines.get(market_code, [])
    market_week = _get_week_klines(market_klines, iso_year, iso_week)
    market_chg = _compound_return(
        [k['change_percent'] for k in market_week]
    ) if len(market_week) >= 3 else 0.0

    # 大盘前一周
    prev_y, prev_w = _prev_iso_week(iso_year, iso_week)
    market_prev = _get_week_klines(market_klines, prev_y, prev_w)
    market_prev_chg = _compound_return(
        [k['change_percent'] for k in market_prev]
    ) if len(market_prev) >= 3 else None

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

    # 价格位置（60日高低点）
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
    prev_stock_week = _get_week_klines(stock_klines, prev_y, prev_w)
    prev_week_chg = _compound_return(
        [k['change_percent'] for k in prev_stock_week]
    ) if len(prev_stock_week) >= 3 else None

    # 成交量比率
    vol_ratio = None
    if len(hist) >= 20:
        avg_vol = sum(k['volume'] for k in hist[-20:]) / 20
        week_avg_vol = sum(k['volume'] for k in week_klines) / len(week_klines)
        if avg_vol > 0:
            vol_ratio = round(week_avg_vol / avg_vol, 2)

    return {
        # LLM 预测器需要的字段
        'this_week_chg': round(this_week_chg, 2),
        'market_chg': round(market_chg, 2),
        '_market_prev_week_chg': round(market_prev_chg, 2) if market_prev_chg is not None else None,
        'consec_down': cd,
        'consec_up': cu,
        'last_day_chg': round(daily_pcts[-1], 2),
        '_market_suffix': market_code.split('.')[-1] if '.' in market_code else 'SH',
        '_price_pos_60': price_pos_60,
        '_prev_week_chg': round(prev_week_chg, 2) if prev_week_chg is not None else None,
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
        # V5 规则引擎额外需要的字段
        '_daily_pcts': daily_pcts,
        '_market_code': market_code,
    }


def _check_next_week_actual(code: str, klines: dict,
                            iso_year: int, iso_week: int) -> float | None:
    """检查下周实际涨跌幅。"""
    nw_y, nw_w = _next_iso_week(iso_year, iso_week)
    nw_klines = _get_week_klines(klines.get(code, []), nw_y, nw_w)
    if len(nw_klines) < 3:
        return None
    return round(_compound_return([k['change_percent'] for k in nw_klines]), 2)


# ═══════════════════════════════════════════════════════════
# V5 规则引擎本地调用
# ═══════════════════════════════════════════════════════════

# 复用 weekly_prediction_service 的规则引擎
from service.weekly_prediction_service import (
    _nw_extract_features,
    _nw_match_rule,
)


def _predict_with_v5_rules(features: dict) -> dict:
    """用 V5 规则引擎预测。

    Returns:
        {
            'direction': 'UP' | 'DOWN' | 'UNCERTAIN',
            'confidence': float,
            'rule_name': str,
            'tier': int,
        }
    """
    # 从 features 中提取 V5 规则引擎需要的参数
    daily_pcts = features['_daily_pcts']
    market_chg = features['market_chg']
    market_code = features['_market_code']

    feat = _nw_extract_features(
        daily_pcts=daily_pcts,
        market_chg=market_chg,
        ff_signal=features.get('ff_signal'),
        vol_ratio=features.get('vol_ratio'),
        vol_price_corr=features.get('vol_price_corr'),
        finance_score=features.get('finance_score'),
        market_index=market_code,
        price_pos_60=features.get('_price_pos_60'),
        prev_week_chg=features.get('_prev_week_chg'),
    )

    rule = _nw_match_rule(feat)
    if rule is None:
        return {
            'direction': 'UNCERTAIN',
            'confidence': 0.0,
            'rule_name': '无匹配规则',
            'tier': 0,
        }

    tier = rule.get('tier', 3)
    conf_map = {1: 0.75, 2: 0.65, 3: 0.55}
    return {
        'direction': 'UP' if rule['pred_up'] else 'DOWN',
        'confidence': conf_map.get(tier, 0.55),
        'rule_name': rule['name'],
        'tier': tier,
    }


# ═══════════════════════════════════════════════════════════
# 统计类
# ═══════════════════════════════════════════════════════════

class MethodStats:
    """单个预测方法的累计统计。"""

    def __init__(self, name: str):
        self.name = name
        self.correct = 0
        self.total = 0
        self.uncertain = 0
        self.high_conf_correct = 0
        self.high_conf_total = 0
        self.details = []

    def add(self, code: str, name: str, week_label: str,
            direction: str, confidence: float, actual: float,
            extra_info: str = '') -> str:
        if direction == 'UNCERTAIN':
            self.uncertain += 1
            self.details.append({
                'week': week_label, 'code': code, 'name': name,
                'direction': 'UNCERTAIN', 'confidence': confidence,
                'actual': actual, 'mark': '⏸️', 'extra': extra_info,
            })
            return '⏸️'

        self.total += 1
        actual_up = actual > 0
        pred_up = direction == 'UP'
        is_correct = actual_up == pred_up
        if is_correct:
            self.correct += 1
            mark = '✅'
        else:
            mark = '❌'

        if confidence >= 0.65:
            self.high_conf_total += 1
            if is_correct:
                self.high_conf_correct += 1

        self.details.append({
            'week': week_label, 'code': code, 'name': name,
            'direction': direction, 'confidence': confidence,
            'actual': actual, 'mark': mark, 'extra': extra_info,
        })
        return mark

    @property
    def accuracy(self) -> float:
        return self.correct / self.total * 100 if self.total > 0 else 0

    @property
    def high_conf_accuracy(self) -> float:
        return self.high_conf_correct / self.high_conf_total * 100 if self.high_conf_total > 0 else 0

    @property
    def coverage(self) -> float:
        """覆盖率 = 有效预测 / (有效预测 + UNCERTAIN)"""
        all_count = self.total + self.uncertain
        return self.total / all_count * 100 if all_count > 0 else 0

    def summary_dict(self) -> dict:
        all_count = self.total + self.uncertain
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

    def print_summary(self):
        all_count = self.total + self.uncertain
        logger.info("  [%s] 有效预测: %d/%d (覆盖率 %.1f%%) | 准确率: %d/%d = %.1f%%",
                     self.name, self.total, all_count, self.coverage,
                     self.correct, self.total, self.accuracy)
        if self.high_conf_total > 0:
            logger.info("  [%s] 高置信(≥65%%): %d/%d = %.1f%%",
                         self.name, self.high_conf_correct, self.high_conf_total,
                         self.high_conf_accuracy)
        logger.info("  [%s] UNCERTAIN: %d (%.1f%%)",
                     self.name, self.uncertain,
                     self.uncertain / all_count * 100 if all_count > 0 else 0)


# ═══════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════

async def main():
    logger.info("=" * 75)
    logger.info("  三方对比回测: Qwen3-235B vs DeepSeek vs V5规则引擎")
    logger.info("  股票池: %d只 (覆盖%d个概念板块)", len(TEST_STOCKS), len(TEST_STOCKS))
    logger.info("=" * 75)

    # 加载数据
    klines, latest_date = _load_test_data()
    if not klines:
        return

    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    current_iso = dt_latest.isocalendar()

    # 回测周范围：往前 2~9 周（共8周）
    test_weeks = []
    for offset in range(2, 10):
        pred_y, pred_w = current_iso[0], current_iso[1] - offset
        if pred_w <= 0:
            pred_y -= 1
            pred_w = 52 + pred_w
        test_weeks.append((pred_y, pred_w))

    logger.info("回测范围: %d周 (W%s ~ W%s)", len(test_weeks), test_weeks[0][1], test_weeks[-1][1])

    # 导入 LLM 预测器
    from service.analysis.qwen_nw_predictor import predict_next_week_with_qwen
    from service.analysis.deepseek_nw_predictor import predict_next_week_with_deepseek

    qwen_stats = MethodStats('Qwen3-235B')
    ds_stats = MethodStats('DeepSeek')
    rules_stats = MethodStats('V5规则引擎')

    # 每周对比记录
    weekly_results = []

    for pred_y, pred_w in test_weeks:
        nw_y, nw_w = _next_iso_week(pred_y, pred_w)
        week_label = f"W{pred_w}→W{nw_w}"

        logger.info("")
        logger.info("─" * 75)
        logger.info("  预测周 W%d → 验证周 W%d", pred_w, nw_w)
        logger.info("─" * 75)

        # 提取特征 & 获取实际涨跌
        samples = []
        actuals = {}
        for code, name in TEST_STOCKS:
            feat = _extract_features(code, name, klines, pred_y, pred_w)
            actual = _check_next_week_actual(code, klines, pred_y, pred_w)
            if feat and actual is not None:
                samples.append({'code': code, 'name': name, 'features': feat})
                actuals[code] = actual

        if not samples:
            logger.warning("  W%d 无有效样本", pred_w)
            continue

        logger.info("  有效样本: %d", len(samples))

        # ── V5 规则引擎（同步，瞬间完成）──
        rules_results_map = {}
        for s in samples:
            rules_results_map[s['code']] = _predict_with_v5_rules(s['features'])

        # ── 并行调用 Qwen 和 DeepSeek（带并发控制避免 429）──
        qwen_sem = asyncio.Semaphore(3)   # Qwen 限流 3 并发
        ds_sem = asyncio.Semaphore(5)     # DeepSeek 限流 5 并发

        async def _call_qwen(s):
            async with qwen_sem:
                return await predict_next_week_with_qwen(s['code'], s['name'], s['features'])

        async def _call_ds(s):
            async with ds_sem:
                return await predict_next_week_with_deepseek(s['code'], s['name'], s['features'])

        qwen_results = await asyncio.gather(
            *[_call_qwen(s) for s in samples], return_exceptions=True)
        ds_results = await asyncio.gather(
            *[_call_ds(s) for s in samples], return_exceptions=True)

        # ── 逐只对比输出 ──
        logger.info("")
        logger.info("  %-14s │ %-16s │ %-16s │ %-16s │ %s",
                     "股票", "Qwen3-235B", "DeepSeek", "V5规则", "实际")
        logger.info("  %s", "─" * 90)

        week_record = {'week': week_label, 'stocks': []}

        for i, s in enumerate(samples):
            code = s['code']
            name = s['name']
            actual = actuals[code]

            # Qwen
            qr = qwen_results[i] if not isinstance(qwen_results[i], Exception) else None
            if qr:
                q_dir, q_conf = qr['direction'], qr['confidence']
                q_mark = qwen_stats.add(code, name, week_label, q_dir, q_conf, actual)
                q_label = f"{q_dir:4s} {q_conf:.0%} {q_mark}"
            else:
                q_label = "FAIL"

            # DeepSeek
            dr = ds_results[i] if not isinstance(ds_results[i], Exception) else None
            if dr:
                d_dir, d_conf = dr['direction'], dr['confidence']
                d_mark = ds_stats.add(code, name, week_label, d_dir, d_conf, actual)
                d_label = f"{d_dir:4s} {d_conf:.0%} {d_mark}"
            else:
                d_label = "FAIL"

            # V5 规则
            rr = rules_results_map[code]
            r_dir, r_conf = rr['direction'], rr['confidence']
            r_extra = rr.get('rule_name', '')
            r_mark = rules_stats.add(code, name, week_label, r_dir, r_conf, actual, r_extra)
            if r_dir == 'UNCERTAIN':
                r_label = f"UNCR     {r_mark}"
            else:
                r_label = f"{r_dir:4s} T{rr['tier']} {r_mark}"

            logger.info("  %-14s │ %-16s │ %-16s │ %-16s │ %+6.2f%%",
                         f"{code[:6]}({name[:4]})", q_label, d_label, r_label, actual)

            week_record['stocks'].append({
                'code': code, 'name': name, 'actual': actual,
                'qwen': qr if qr else None,
                'deepseek': dr if dr else None,
                'rules': rr,
            })

        weekly_results.append(week_record)

    # ═══════════════════════════════════════════════════════
    # 汇总统计
    # ═══════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 75)
    logger.info("  三方汇总统计（%d周 × %d只 = 最多%d个样本）",
                len(test_weeks), len(TEST_STOCKS),
                len(test_weeks) * len(TEST_STOCKS))
    logger.info("=" * 75)

    for stats in [qwen_stats, ds_stats, rules_stats]:
        stats.print_summary()
        logger.info("")

    # ── 三方对比表 ──
    logger.info("─" * 75)
    logger.info("  三方对比")
    logger.info("─" * 75)
    logger.info("  %-14s │ %8s │ %8s │ %8s │ %8s",
                "方法", "准确率", "高置信", "覆盖率", "UNCERTAIN")
    logger.info("  %s", "─" * 60)
    for stats in [qwen_stats, ds_stats, rules_stats]:
        logger.info("  %-14s │ %7.1f%% │ %7.1f%% │ %7.1f%% │ %8d",
                     stats.name, stats.accuracy, stats.high_conf_accuracy,
                     stats.coverage, stats.uncertain)

    # ── 规则引擎命中时 LLM 是否一致 ──
    agree_qwen, agree_ds, rule_hit = 0, 0, 0
    for wd in weekly_results:
        for sr in wd['stocks']:
            rr = sr['rules']
            if rr['direction'] != 'UNCERTAIN':
                rule_hit += 1
                if sr['qwen'] and sr['qwen']['direction'] == rr['direction']:
                    agree_qwen += 1
                if sr['deepseek'] and sr['deepseek']['direction'] == rr['direction']:
                    agree_ds += 1

    if rule_hit > 0:
        logger.info("")
        logger.info("  规则引擎命中 %d 次时:", rule_hit)
        logger.info("    Qwen 与规则方向一致: %d/%d = %.1f%%",
                     agree_qwen, rule_hit, agree_qwen / rule_hit * 100)
        logger.info("    DeepSeek 与规则方向一致: %d/%d = %.1f%%",
                     agree_ds, rule_hit, agree_ds / rule_hit * 100)

    # ── 保存结果 ──
    output = {
        'date': latest_date,
        'test_weeks': [f"W{y}-W{w}" for y, w in test_weeks],
        'stocks': [f"{c}({n})" for c, n in TEST_STOCKS],
        'summary': {
            'qwen': qwen_stats.summary_dict(),
            'deepseek': ds_stats.summary_dict(),
            'rules': rules_stats.summary_dict(),
        },
        'rule_llm_agreement': {
            'rule_hit_count': rule_hit,
            'qwen_agree': agree_qwen,
            'deepseek_agree': agree_ds,
        },
        'weekly_results': weekly_results,
        'qwen_details': qwen_stats.details,
        'deepseek_details': ds_stats.details,
        'rules_details': rules_stats.details,
    }
    out_path = Path(__file__).parent.parent.parent / 'data_results' / 'qwen_vs_deepseek_vs_rules_result.json'
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    logger.info("\n  结果已保存: %s", out_path)


if __name__ == '__main__':
    asyncio.run(main())
