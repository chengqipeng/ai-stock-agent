#!/usr/bin/env python3
"""
Qwen3-235B 周预测 — 回测验证（10只股票 × 多周）
================================================
从数据库取10只代表性股票的历史K线，调用 Qwen3-235B-Ins 预测，
与实际涨跌对比验证准确率。同时与 DeepSeek 做 A/B 对比。

用法：
    .venv/bin/python -m day_week_predicted.backtest.qwen_quick_test
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

# ── 测试股票池：10只，覆盖沪深主板+创业板+科创板，不同行业 ──
TEST_STOCKS = [
    ('600519.SH', '贵州茅台'),   # 白酒龙头
    ('000858.SZ', '五粮液'),     # 白酒
    ('601318.SH', '中国平安'),   # 保险
    ('002594.SZ', '比亚迪'),     # 新能源车
    ('300750.SZ', '宁德时代'),   # 创业板-电池
    ('000333.SZ', '美的集团'),   # 家电
    ('600036.SH', '招商银行'),   # 银行
    ('300059.SZ', '东方财富'),   # 券商/互联网
    ('601899.SH', '紫金矿业'),  # 有色金属
    ('688981.SH', '中芯国际'),   # 科创板-半导体
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
    """从数据库加载测试股票的K线数据（近90天）。"""
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
    lookback = (dt_latest - timedelta(days=120)).strftime('%Y-%m-%d')

    codes = [c for c, _ in TEST_STOCKS]
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

    conn.close()
    return klines, latest_date


def _get_iso_week(date_str: str) -> tuple:
    """返回 (iso_year, iso_week)。"""
    return datetime.strptime(date_str, '%Y-%m-%d').isocalendar()[:2]


def _prev_iso_week(iso_year: int, iso_week: int) -> tuple:
    """计算前一个ISO周。"""
    w = iso_week - 1
    y = iso_year
    if w <= 0:
        y -= 1
        w = 52 + w
    return (y, w)


def _next_iso_week(iso_year: int, iso_week: int) -> tuple:
    """计算下一个ISO周。"""
    w = iso_week + 1
    y = iso_year
    if w > 52:
        y += 1
        w = 1
    return (y, w)


def _get_week_klines(stock_klines: list, iso_year: int, iso_week: int) -> list:
    """获取指定ISO周的K线。"""
    result = [k for k in stock_klines
              if datetime.strptime(k['date'], '%Y-%m-%d').isocalendar()[:2] == (iso_year, iso_week)]
    result.sort(key=lambda x: x['date'])
    return result


def _extract_features(code: str, name: str, klines: dict,
                      iso_year: int, iso_week: int) -> dict | None:
    """从K线数据中提取多维特征。"""
    stock_klines = klines.get(code, [])
    if not stock_klines:
        return None

    week_klines = _get_week_klines(stock_klines, iso_year, iso_week)
    if len(week_klines) < 3:
        return None

    daily_pcts = [k['change_percent'] for k in week_klines]
    this_week_chg = _compound_return(daily_pcts)

    # 大盘
    market_code = '000001.SH' if code.endswith('.SH') or code.startswith('688') else '399001.SZ'
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
        'this_week_chg': round(this_week_chg, 2),
        'market_chg': round(market_chg, 2),
        '_market_prev_week_chg': round(market_prev_chg, 2) if market_prev_chg is not None else None,
        'consec_down': cd,
        'consec_up': cu,
        'last_day_chg': round(daily_pcts[-1], 2),
        '_market_suffix': 'SH' if code.endswith('.SH') else 'SZ',
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
# 统计辅助类
# ═══════════════════════════════════════════════════════════

class PredictionStats:
    """累计预测统计。"""

    def __init__(self, name: str):
        self.name = name
        self.correct = 0
        self.total = 0
        self.uncertain = 0
        self.high_conf_correct = 0
        self.high_conf_total = 0
        self.low_risk_correct = 0
        self.low_risk_total = 0
        self.details = []

    def add(self, code: str, name: str, week_label: str,
            direction: str, confidence: float, actual: float,
            risk_level: str = 'medium', key_factors: list = None):
        """记录一条预测结果。"""
        if direction == 'UNCERTAIN':
            self.uncertain += 1
            mark = '⏸️'
        else:
            self.total += 1
            actual_up = actual > 0
            pred_up = direction == 'UP'
            is_correct = actual_up == pred_up
            if is_correct:
                self.correct += 1
                mark = '✅'
            else:
                mark = '❌'
            # 高置信统计
            if confidence >= 0.65:
                self.high_conf_total += 1
                if is_correct:
                    self.high_conf_correct += 1
            # 低风险统计（Qwen 特有）
            if risk_level == 'low':
                self.low_risk_total += 1
                if is_correct:
                    self.low_risk_correct += 1

        self.details.append({
            'week': week_label,
            'code': code,
            'name': name,
            'direction': direction,
            'confidence': confidence,
            'risk_level': risk_level,
            'key_factors': key_factors or [],
            'actual': actual,
            'mark': mark,
        })
        return mark

    @property
    def accuracy(self) -> float:
        return self.correct / self.total * 100 if self.total > 0 else 0

    @property
    def high_conf_accuracy(self) -> float:
        return self.high_conf_correct / self.high_conf_total * 100 if self.high_conf_total > 0 else 0

    @property
    def low_risk_accuracy(self) -> float:
        return self.low_risk_correct / self.low_risk_total * 100 if self.low_risk_total > 0 else 0

    def summary_dict(self) -> dict:
        return {
            'model': self.name,
            'total': self.total,
            'correct': self.correct,
            'uncertain': self.uncertain,
            'accuracy': round(self.accuracy, 1),
            'high_conf_total': self.high_conf_total,
            'high_conf_accuracy': round(self.high_conf_accuracy, 1),
            'low_risk_total': self.low_risk_total,
            'low_risk_accuracy': round(self.low_risk_accuracy, 1),
        }

    def print_summary(self):
        logger.info("  [%s] 有效预测: %d | 准确率: %d/%d = %.1f%%",
                     self.name, self.total, self.correct, self.total, self.accuracy)
        if self.high_conf_total > 0:
            logger.info("  [%s] 高置信(≥65%%): %d/%d = %.1f%%",
                         self.name, self.high_conf_correct, self.high_conf_total,
                         self.high_conf_accuracy)
        if self.low_risk_total > 0:
            logger.info("  [%s] 低风险(risk=low): %d/%d = %.1f%%",
                         self.name, self.low_risk_correct, self.low_risk_total,
                         self.low_risk_accuracy)
        logger.info("  [%s] UNCERTAIN: %d (%.1f%%)",
                     self.name, self.uncertain,
                     self.uncertain / (self.total + self.uncertain) * 100
                     if (self.total + self.uncertain) > 0 else 0)


# ═══════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════

async def main():
    logger.info("=" * 70)
    logger.info("  Qwen3-235B vs DeepSeek — 周预测 A/B 回测验证")
    logger.info("  股票池: %d只 | 模式: 单股逐只预测", len(TEST_STOCKS))
    logger.info("=" * 70)

    # 加载数据
    klines, latest_date = _load_test_data()
    if not klines:
        return

    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    current_iso = dt_latest.isocalendar()

    # 确定回测周范围：往前 2~9 周（共8周）
    # offset=2 表示"上上周预测→上周验证"，确保验证周有完整数据
    test_weeks = []
    for offset in range(2, 10):
        pred_y, pred_w = current_iso[0], current_iso[1] - offset
        if pred_w <= 0:
            pred_y -= 1
            pred_w = 52 + pred_w
        test_weeks.append((pred_y, pred_w))

    logger.info("回测范围: %d周 (W%s ~ W%s)",
                len(test_weeks), test_weeks[0][1], test_weeks[-1][1])

    # 导入预测器
    from service.analysis.qwen_nw_predictor import predict_next_week_with_qwen
    from service.analysis.deepseek_nw_predictor import predict_next_week_with_deepseek

    qwen_stats = PredictionStats('Qwen3-235B')
    ds_stats = PredictionStats('DeepSeek')

    for pred_y, pred_w in test_weeks:
        nw_y, nw_w = _next_iso_week(pred_y, pred_w)
        week_label = f"W{pred_w}→W{nw_w}"

        logger.info("")
        logger.info("─" * 60)
        logger.info("  预测周 W%d → 验证周 W%d", pred_w, nw_w)
        logger.info("─" * 60)

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

        # 并行调用 Qwen 和 DeepSeek
        qwen_tasks = []
        ds_tasks = []
        for s in samples:
            qwen_tasks.append(
                predict_next_week_with_qwen(s['code'], s['name'], s['features'])
            )
            ds_tasks.append(
                predict_next_week_with_deepseek(s['code'], s['name'], s['features'])
            )

        qwen_results = await asyncio.gather(*qwen_tasks, return_exceptions=True)
        ds_results = await asyncio.gather(*ds_tasks, return_exceptions=True)

        # 逐只对比
        logger.info("")
        logger.info("  %-12s │ %-20s │ %-20s │ %s",
                     "股票", "Qwen3-235B", "DeepSeek", "实际")
        logger.info("  %s", "─" * 75)

        for i, s in enumerate(samples):
            code = s['code']
            name = s['name']
            actual = actuals[code]

            # Qwen 结果
            qr = qwen_results[i] if not isinstance(qwen_results[i], Exception) else None
            if qr:
                q_dir = qr['direction']
                q_conf = qr['confidence']
                q_risk = qr.get('risk_level', 'medium')
                q_factors = qr.get('key_factors', [])
                q_mark = qwen_stats.add(code, name, week_label, q_dir, q_conf, actual,
                                         q_risk, q_factors)
                q_label = f"{q_dir:4s} {q_conf:.0%} R={q_risk[0]} {q_mark}"
            else:
                q_label = "FAIL"
                if isinstance(qwen_results[i], Exception):
                    logger.debug("  Qwen异常 %s: %s", code, qwen_results[i])

            # DeepSeek 结果
            dr = ds_results[i] if not isinstance(ds_results[i], Exception) else None
            if dr:
                d_dir = dr['direction']
                d_conf = dr['confidence']
                d_mark = ds_stats.add(code, name, week_label, d_dir, d_conf, actual)
                d_label = f"{d_dir:4s} {d_conf:.0%} {d_mark}"
            else:
                d_label = "FAIL"

            logger.info("  %-12s │ %-20s │ %-20s │ %+6.2f%%",
                         f"{code[:6]}({name[:4]})", q_label, d_label, actual)

    # ═══════════════════════════════════════════════════════
    # 汇总统计
    # ═══════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 70)
    logger.info("  多周汇总统计（%d周 × %d只 = 最多%d个样本）",
                len(test_weeks), len(TEST_STOCKS),
                len(test_weeks) * len(TEST_STOCKS))
    logger.info("=" * 70)

    qwen_stats.print_summary()
    logger.info("")
    ds_stats.print_summary()

    # 对比
    logger.info("")
    logger.info("─" * 40)
    logger.info("  A/B 对比")
    logger.info("─" * 40)
    diff = qwen_stats.accuracy - ds_stats.accuracy
    logger.info("  准确率差异: Qwen %.1f%% vs DeepSeek %.1f%% → %+.1f%%",
                qwen_stats.accuracy, ds_stats.accuracy, diff)
    if qwen_stats.high_conf_total > 0 and ds_stats.high_conf_total > 0:
        hc_diff = qwen_stats.high_conf_accuracy - ds_stats.high_conf_accuracy
        logger.info("  高置信差异: Qwen %.1f%% vs DeepSeek %.1f%% → %+.1f%%",
                    qwen_stats.high_conf_accuracy, ds_stats.high_conf_accuracy, hc_diff)
    if qwen_stats.low_risk_total > 0:
        logger.info("  Qwen低风险准确率: %.1f%% (%d样本) — DeepSeek无此维度",
                    qwen_stats.low_risk_accuracy, qwen_stats.low_risk_total)

    # 保存结果
    output = {
        'date': latest_date,
        'test_weeks': [f"W{y}-W{w}" for y, w in test_weeks],
        'stocks': [f"{c}({n})" for c, n in TEST_STOCKS],
        'qwen': qwen_stats.summary_dict(),
        'deepseek': ds_stats.summary_dict(),
        'qwen_details': qwen_stats.details,
        'deepseek_details': ds_stats.details,
    }
    out_path = Path(__file__).parent.parent.parent / 'data_results' / 'qwen_quick_test_result.json'
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info("\n  结果已保存: %s", out_path)


if __name__ == '__main__':
    asyncio.run(main())
