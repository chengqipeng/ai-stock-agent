#!/usr/bin/env python3
"""
混合预测体系 — V11 + 多因子增强回测
=====================================
对比三种模式：
  1. V11 Only（现有系统）
  2. Factor Only（纯因子）
  3. Hybrid（V11 + 因子增强）

验证混合策略是否能提升V11的准确率。

用法：
    python -m tools.run_hybrid_backtest [--weeks 29] [--stocks 200]
"""
import argparse
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dao import get_connection
from service.factor_prediction.factor_engine import (
    compute_price_volume_factors,
    compute_fundamental_factors,
    compute_alternative_factors,
)
from service.factor_prediction.hybrid_predictor import HybridPredictor
from service.weekly_prediction_service import (
    _nw_extract_features,
    _nw_match_rule,
    _get_stock_index,
    _compound_return,
    _mean,
    _std,
    _to_float,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent.parent / "data_results"


def load_data(n_stocks: int, n_weeks: int):
    """加载回测所需的全部数据。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 获取股票列表
    cur.execute("""
        SELECT stock_code, COUNT(*) AS cnt FROM stock_kline
        WHERE stock_code NOT LIKE '%%.BJ'
        GROUP BY stock_code HAVING cnt >= 120
        ORDER BY cnt DESC LIMIT %s
    """, (n_stocks,))
    stock_codes = [r['stock_code'] for r in cur.fetchall()]
    logger.info("加载 %d 只股票", len(stock_codes))

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=n_weeks * 7 + 120)).strftime('%Y-%m-%d')

    # K线数据
    kline_data = defaultdict(list)
    bs = 300
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, close_price, open_price, high_price, "
            f"low_price, trading_volume, change_percent, change_hand "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [start_date, end_date])
        for row in cur.fetchall():
            kline_data[row['stock_code']].append({
                'date': str(row['date']),
                'close': _to_float(row['close_price']),
                'open': _to_float(row['open_price']),
                'high': _to_float(row['high_price']),
                'low': _to_float(row['low_price']),
                'volume': _to_float(row.get('trading_volume')),
                'change_percent': _to_float(row['change_percent']),
                'turnover': _to_float(row.get('change_hand')),
            })

    # 大盘K线
    indices = ['000001.SH', '399001.SZ', '899050.SZ']
    ph = ','.join(['%s'] * len(indices))
    cur.execute(
        f"SELECT stock_code, `date`, change_percent FROM stock_kline "
        f"WHERE stock_code IN ({ph}) AND `date` >= %s AND `date` <= %s ORDER BY `date`",
        indices + [start_date, end_date])
    market_data = defaultdict(list)
    for r in cur.fetchall():
        market_data[r['stock_code']].append({
            'date': str(r['date']),
            'change_percent': _to_float(r['change_percent']),
        })

    # 财报数据
    finance_data = {}
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, data_json FROM stock_finance "
            f"WHERE stock_code IN ({ph}) ORDER BY report_date DESC", batch)
        for row in cur.fetchall():
            code = row['stock_code']
            if code not in finance_data:
                finance_data[code] = []
            try:
                data = json.loads(row['data_json']) if isinstance(row['data_json'], str) else row['data_json']
                finance_data[code].append(data)
            except (json.JSONDecodeError, TypeError):
                pass

    # 资金流向
    fund_flow_data = defaultdict(list)
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, big_net_pct, net_flow "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s ORDER BY `date` DESC",
            batch + [start_date])
        for row in cur.fetchall():
            fund_flow_data[row['stock_code']].append({
                'date': str(row['date']),
                'big_net_pct': _to_float(row.get('big_net_pct')),
                'net_flow': _to_float(row.get('net_flow')),
            })

    cur.close()
    conn.close()

    return {
        'stock_codes': stock_codes,
        'kline_data': dict(kline_data),
        'market_data': dict(market_data),
        'finance_data': finance_data,
        'fund_flow_data': dict(fund_flow_data),
        'start_date': start_date,
        'end_date': end_date,
    }


def run_hybrid_backtest(n_weeks: int = 29, n_stocks: int = 200):
    """执行混合预测回测。"""
    t0 = time.time()
    logger.info("=" * 60)
    logger.info("混合预测体系 — V11 + 多因子增强回测")
    logger.info("=" * 60)

    data = load_data(n_stocks, n_weeks)
    stock_codes = data['stock_codes']
    kline_data = data['kline_data']
    market_data = data['market_data']
    finance_data = data['finance_data']
    fund_flow_data = data['fund_flow_data']

    # 按周分组K线
    def _group_by_week(klines):
        weeks = defaultdict(list)
        for k in klines:
            d = datetime.strptime(k['date'][:10], '%Y-%m-%d')
            iso = d.isocalendar()
            key = f"{iso[0]}-W{iso[1]:02d}"
            weeks[key].append(k)
        return dict(weeks)

    # 统计
    v11_stats = {'total': 0, 'correct': 0, 'covered': 0}
    factor_stats = {'total': 0, 'correct': 0}
    hybrid_stats = {'total': 0, 'correct': 0, 'agree': 0, 'agree_correct': 0,
                    'disagree': 0, 'disagree_correct': 0}
    hybrid_high_stats = {'total': 0, 'correct': 0}

    processed = 0
    for code in stock_codes:
        klines = kline_data.get(code, [])
        if len(klines) < 80:
            continue

        weekly_groups = _group_by_week(klines)
        sorted_weeks = sorted(weekly_groups.keys())
        if len(sorted_weeks) < n_weeks + 2:
            continue

        mkt_idx = _get_stock_index(code)
        mkt_klines = market_data.get(mkt_idx, market_data.get('000001.SH', []))
        mkt_by_week = _group_by_week([{'date': m['date'], 'change_percent': m['change_percent']}
                                       for m in mkt_klines])

        target_weeks = sorted_weeks[-(n_weeks + 1):-1]

        for wi, week_key in enumerate(target_weeks):
            week_klines = weekly_groups[week_key]
            if not week_klines:
                continue

            # 下周实际收益
            next_idx = sorted_weeks.index(week_key) + 1 if week_key in sorted_weeks else None
            if not next_idx or next_idx >= len(sorted_weeks):
                continue
            next_week = sorted_weeks[next_idx]
            next_klines = weekly_groups.get(next_week, [])
            if not next_klines:
                continue

            base_close = week_klines[-1].get('close', 0)
            end_close = next_klines[-1].get('close', 0)
            if base_close <= 0 or end_close <= 0:
                continue
            actual_return = (end_close / base_close - 1) * 100
            actual_up = actual_return > 0

            # ── V11预测 ──
            daily_pcts = [k['change_percent'] for k in week_klines]
            mkt_week = mkt_by_week.get(week_key, [])
            mkt_chg = _compound_return([m['change_percent'] for m in mkt_week]) if mkt_week else 0

            # 计算V11特征
            last_date = week_klines[-1]['date']
            idx_in_klines = None
            for j, k in enumerate(klines):
                if k['date'] == last_date:
                    idx_in_klines = j
                    break

            pos60 = None
            prev_chg = None
            prev2_chg = None
            if idx_in_klines and idx_in_klines >= 60:
                closes_60 = [k['close'] for k in klines[idx_in_klines - 59:idx_in_klines + 1] if k['close'] > 0]
                if closes_60:
                    h60 = max(closes_60)
                    l60 = min(closes_60)
                    if h60 > l60:
                        pos60 = (klines[idx_in_klines]['close'] - l60) / (h60 - l60)

            # 前一周/前两周涨跌
            wi_in_sorted = sorted_weeks.index(week_key)
            if wi_in_sorted >= 1:
                prev_week = sorted_weeks[wi_in_sorted - 1]
                prev_kl = weekly_groups.get(prev_week, [])
                if prev_kl:
                    prev_chg = _compound_return([k['change_percent'] for k in prev_kl])
            if wi_in_sorted >= 2:
                prev2_week = sorted_weeks[wi_in_sorted - 2]
                prev2_kl = weekly_groups.get(prev2_week, [])
                if prev2_kl:
                    prev2_chg = _compound_return([k['change_percent'] for k in prev2_kl])

            mkt_last_day = mkt_week[-1]['change_percent'] if mkt_week else None

            feat = _nw_extract_features(
                daily_pcts, mkt_chg,
                price_pos_60=pos60,
                prev_week_chg=prev_chg,
                prev2_week_chg=prev2_chg,
                mkt_last_day=mkt_last_day,
                market_index=mkt_idx,
                week_klines=[{'high': k['high'], 'low': k['low'], 'close': k['close']} for k in week_klines],
            )

            v11_rule = _nw_match_rule(feat)

            # ── 因子预测 ──
            hist_klines = klines[:idx_in_klines + 1] if idx_in_klines else klines
            factors = compute_price_volume_factors(hist_klines) if len(hist_klines) >= 60 else {}
            fin = finance_data.get(code, [])
            if fin:
                factors.update(compute_fundamental_factors(fin))
            ff = fund_flow_data.get(code, [])
            mkt_hist = [m for m in mkt_klines if m['date'] <= last_date]
            alt = compute_alternative_factors(fund_flow=ff[:20], market_klines=mkt_hist, stock_klines=hist_klines)
            factors.update(alt)

            # 因子投票
            predictor = HybridPredictor()
            factor_vote = predictor._quick_factor_vote(factors)
            factor_up = factor_vote > 0

            # ── 统计 ──
            # V11
            v11_stats['total'] += 1
            if v11_rule:
                v11_stats['covered'] += 1
                v11_pred_up = v11_rule['pred_up']
                if v11_pred_up == actual_up:
                    v11_stats['correct'] += 1

            # Factor
            factor_stats['total'] += 1
            if factor_up == actual_up:
                factor_stats['correct'] += 1

            # Hybrid
            hybrid_stats['total'] += 1
            if v11_rule:
                v11_pred_up = v11_rule['pred_up']
                agreement = (v11_pred_up == factor_up)

                if agreement:
                    hybrid_stats['agree'] += 1
                    # 一致时用V11方向
                    if v11_pred_up == actual_up:
                        hybrid_stats['correct'] += 1
                        hybrid_stats['agree_correct'] += 1
                        hybrid_high_stats['correct'] += 1
                    hybrid_high_stats['total'] += 1
                else:
                    hybrid_stats['disagree'] += 1
                    # 不一致时仍用V11方向但标记低置信
                    if v11_pred_up == actual_up:
                        hybrid_stats['correct'] += 1
                        hybrid_stats['disagree_correct'] += 1
            else:
                # V11无匹配，用因子
                if factor_up == actual_up:
                    hybrid_stats['correct'] += 1

        processed += 1
        if processed % 50 == 0:
            logger.info("已处理 %d/%d", processed, len(stock_codes))

    # 输出结果
    print("\n" + "=" * 60)
    print("混合预测体系 — 回测结果")
    print("=" * 60)

    v11_covered = v11_stats['covered']
    v11_uncovered = v11_stats['total'] - v11_covered
    v11_acc = v11_stats['correct'] / v11_covered * 100 if v11_covered > 0 else 0

    factor_acc = factor_stats['correct'] / factor_stats['total'] * 100 if factor_stats['total'] > 0 else 0
    hybrid_acc = hybrid_stats['correct'] / hybrid_stats['total'] * 100 if hybrid_stats['total'] > 0 else 0

    agree_acc = hybrid_stats['agree_correct'] / hybrid_stats['agree'] * 100 if hybrid_stats['agree'] > 0 else 0
    disagree_acc = hybrid_stats['disagree_correct'] / hybrid_stats['disagree'] * 100 if hybrid_stats['disagree'] > 0 else 0
    high_acc = hybrid_high_stats['correct'] / hybrid_high_stats['total'] * 100 if hybrid_high_stats['total'] > 0 else 0

    print(f"\n📊 样本: {v11_stats['total']}条预测, {len(stock_codes)}只股票, {n_weeks}周")

    print(f"\n🔧 V11规则引擎:")
    print(f"  覆盖率: {v11_covered}/{v11_stats['total']} ({v11_covered / v11_stats['total'] * 100:.1f}%)")
    print(f"  覆盖样本准确率: {v11_acc:.1f}% ({v11_stats['correct']}/{v11_covered})")

    print(f"\n📈 纯因子预测:")
    print(f"  准确率: {factor_acc:.1f}% ({factor_stats['correct']}/{factor_stats['total']})")

    print(f"\n🔀 混合预测 (V11 + 因子增强):")
    print(f"  总准确率: {hybrid_acc:.1f}% ({hybrid_stats['correct']}/{hybrid_stats['total']})")
    print(f"  V11+因子一致: {agree_acc:.1f}% ({hybrid_stats['agree_correct']}/{hybrid_stats['agree']})")
    print(f"  V11+因子矛盾: {disagree_acc:.1f}% ({hybrid_stats['disagree_correct']}/{hybrid_stats['disagree']})")
    print(f"  高置信度(一致): {high_acc:.1f}% ({hybrid_high_stats['correct']}/{hybrid_high_stats['total']})")

    print(f"\n💡 关键发现:")
    if agree_acc > disagree_acc + 3:
        print(f"  ✅ V11+因子一致时准确率({agree_acc:.1f}%)显著高于矛盾时({disagree_acc:.1f}%)")
        print(f"     → 因子可以有效过滤V11的假信号")
    if agree_acc > v11_acc:
        print(f"  ✅ 一致信号准确率({agree_acc:.1f}%)高于V11单独({v11_acc:.1f}%)")
        print(f"     → 混合策略提升了V11的准确率")
    if hybrid_acc > factor_acc:
        print(f"  ✅ 混合准确率({hybrid_acc:.1f}%)高于纯因子({factor_acc:.1f}%)")

    print(f"\n⏱️  耗时: {time.time() - t0:.1f}秒")
    print("=" * 60)

    # 保存结果
    result = {
        'v11_only': {
            'total': v11_stats['total'],
            'covered': v11_covered,
            'correct': v11_stats['correct'],
            'accuracy': round(v11_acc, 2),
            'coverage': round(v11_covered / v11_stats['total'] * 100, 2),
        },
        'factor_only': {
            'total': factor_stats['total'],
            'correct': factor_stats['correct'],
            'accuracy': round(factor_acc, 2),
        },
        'hybrid': {
            'total': hybrid_stats['total'],
            'correct': hybrid_stats['correct'],
            'accuracy': round(hybrid_acc, 2),
            'agree_total': hybrid_stats['agree'],
            'agree_correct': hybrid_stats['agree_correct'],
            'agree_accuracy': round(agree_acc, 2),
            'disagree_total': hybrid_stats['disagree'],
            'disagree_correct': hybrid_stats['disagree_correct'],
            'disagree_accuracy': round(disagree_acc, 2),
            'high_confidence_total': hybrid_high_stats['total'],
            'high_confidence_correct': hybrid_high_stats['correct'],
            'high_confidence_accuracy': round(high_acc, 2),
        },
    }

    output_path = OUTPUT_DIR / "hybrid_prediction_backtest_result.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    logger.info("结果已保存: %s", output_path)

    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='混合预测体系回测')
    parser.add_argument('--weeks', type=int, default=29, help='回测周数')
    parser.add_argument('--stocks', type=int, default=200, help='股票数量')
    args = parser.parse_args()
    run_hybrid_backtest(n_weeks=args.weeks, n_stocks=args.stocks)
