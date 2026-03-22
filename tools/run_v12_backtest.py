#!/usr/bin/env python3
"""
V12 独立预测体系 — 回测验证（两层架构版）
==========================================
完全独立于V11，使用V12引擎对历史数据进行walk-forward回测。

V12两层架构：
  Layer 1: 极端条件过滤 — 只在极端条件下出信号
  Layer 2: 多信号投票 — 确认方向和置信度

验证方法：
  1. 按ISO周滚动：每周用截至周末的数据预测下周方向
  2. 与实际下周涨跌对比，计算准确率
  3. 关键指标：准确率、覆盖率（出信号的比例）、分置信度/极端分数统计

用法：
    source .venv/bin/activate
    python -m tools.run_v12_backtest [--weeks 30] [--stocks 200]
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
from service.v12_prediction.v12_engine import V12PredictionEngine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent.parent / "data_results"


def _to_float(v) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


# ═══════════════════════════════════════════════════════════
# 数据加载（直接SQL，不依赖其他模块）
# ═══════════════════════════════════════════════════════════

def load_stock_codes(limit: int = 200) -> list[str]:
    """获取有足够K线数据的A股代码。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT stock_code, COUNT(*) AS cnt
        FROM stock_kline
        WHERE stock_code NOT LIKE '%%.BJ'
        GROUP BY stock_code
        HAVING cnt >= 120
        ORDER BY cnt DESC
        LIMIT %s
    """, (limit,))
    codes = [r['stock_code'] for r in cur.fetchall()]
    cur.close()
    conn.close()
    logger.info("加载 %d 只股票", len(codes))
    return codes


def load_kline_data(stock_codes: list[str], start_date: str, end_date: str) -> dict:
    """加载K线数据。返回 {code: [{date, close, open, high, low, volume, ...}]}"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = defaultdict(list)
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
            result[row['stock_code']].append({
                'date': str(row['date']),
                'close': _to_float(row['close_price']),
                'open': _to_float(row['open_price']),
                'high': _to_float(row['high_price']),
                'low': _to_float(row['low_price']),
                'volume': _to_float(row['trading_volume']),
                'change_percent': _to_float(row['change_percent']),
                'turnover': _to_float(row.get('change_hand')),
            })
    cur.close()
    conn.close()
    return dict(result)


def load_fund_flow_data(stock_codes: list[str], start_date: str) -> dict:
    """加载资金流向数据。返回 {code: [{date, big_net_pct, net_flow, ...}]}"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = defaultdict(list)
    bs = 300
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, big_net_pct, net_flow, main_net_5day "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s ORDER BY `date` DESC",
            batch + [start_date])
        for row in cur.fetchall():
            result[row['stock_code']].append({
                'date': str(row['date']),
                'big_net_pct': _to_float(row.get('big_net_pct')),
                'net_flow': _to_float(row.get('net_flow')),
                'main_net_5day': _to_float(row.get('main_net_5day')),
            })
    cur.close()
    conn.close()
    return dict(result)


def load_market_klines(start_date: str, end_date: str) -> list[dict]:
    """加载上证指数K线作为大盘参考。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute(
        "SELECT `date`, close_price, change_percent "
        "FROM stock_kline WHERE stock_code = '000001.SH' "
        "AND `date` >= %s AND `date` <= %s ORDER BY `date`",
        (start_date, end_date))
    result = []
    for row in cur.fetchall():
        result.append({
            'date': str(row['date']),
            'close': _to_float(row.get('close_price')),
            'change_percent': _to_float(row['change_percent']),
        })
    cur.close()
    conn.close()
    return result


# ═══════════════════════════════════════════════════════════
# Walk-Forward回测（两层架构版）
# ═══════════════════════════════════════════════════════════

def run_v12_backtest(n_weeks: int = 30, n_stocks: int = 200):
    """执行V12 Walk-Forward回测。"""
    t0 = time.time()
    logger.info("=" * 60)
    logger.info("V12 两层架构预测体系 — Walk-Forward 回测")
    logger.info("参数: weeks=%d, stocks=%d", n_weeks, n_stocks)
    logger.info("=" * 60)

    # 1. 加载数据
    logger.info("[1/3] 加载数据...")
    stock_codes = load_stock_codes(n_stocks)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=n_weeks * 7 + 120)).strftime('%Y-%m-%d')

    kline_data = load_kline_data(stock_codes, start_date, end_date)
    fund_flow_data = load_fund_flow_data(stock_codes, start_date)
    market_klines = load_market_klines(start_date, end_date)
    logger.info("  K线: %d只, 资金流: %d只, 大盘: %d条",
                len(kline_data), len(fund_flow_data), len(market_klines))

    # 2. 按周滚动回测
    logger.info("[2/3] 按周滚动回测...")

    def group_by_week(klines):
        weeks = defaultdict(list)
        for k in klines:
            d = datetime.strptime(k['date'][:10], '%Y-%m-%d')
            iso = d.isocalendar()
            key = f"{iso[0]}-W{iso[1]:02d}"
            weeks[key].append(k)
        return dict(weeks)

    # 预处理：按周组织每只股票的数据
    stock_weekly = {}
    all_week_keys = set()

    for code in stock_codes:
        klines = kline_data.get(code, [])
        if len(klines) < 80:
            continue
        weekly_groups = group_by_week(klines)
        sorted_weeks = sorted(weekly_groups.keys())
        if len(sorted_weeks) < 4:
            continue

        fund_flow = fund_flow_data.get(code, [])
        week_info = {}

        for wi in range(len(sorted_weeks) - 1):
            week_key = sorted_weeks[wi]
            next_week_key = sorted_weeks[wi + 1]
            week_klines = weekly_groups[week_key]
            next_klines = weekly_groups[next_week_key]
            last_date = week_klines[-1]['date']

            idx = None
            for j, k in enumerate(klines):
                if k['date'] == last_date:
                    idx = j
                    break
            if idx is None or idx < 60:
                continue

            base_close = week_klines[-1].get('close', 0)
            end_close = next_klines[-1].get('close', 0)
            if base_close <= 0 or end_close <= 0:
                continue

            actual_return = (end_close / base_close - 1) * 100
            hist_ff = [f for f in fund_flow if f['date'] <= last_date][:20]

            week_info[week_key] = {
                'hist_klines': klines[:idx + 1],
                'hist_ff': hist_ff,
                'actual_return': actual_return,
                'last_date': last_date,
            }
            all_week_keys.add(week_key)

        if week_info:
            stock_weekly[code] = week_info

    logger.info("  %d只股票, %d个周", len(stock_weekly), len(all_week_keys))

    # 统计容器
    total_opportunities = 0  # 总共可预测的股票-周数
    total_pred = 0           # 实际出信号的预测数
    total_correct = 0
    by_confidence = defaultdict(lambda: {'total': 0, 'correct': 0})
    by_extreme_score = defaultdict(lambda: {'total': 0, 'correct': 0})
    by_signal = defaultdict(lambda: {'total': 0, 'correct': 0})
    by_week = defaultdict(lambda: {'total': 0, 'correct': 0, 'opportunities': 0})
    by_direction_agree = {'agree': {'total': 0, 'correct': 0},
                          'disagree': {'total': 0, 'correct': 0}}
    by_market_aligned = {'aligned': {'total': 0, 'correct': 0},
                         'independent': {'total': 0, 'correct': 0}}
    by_quality_factors = defaultdict(lambda: {'total': 0, 'correct': 0})
    weekly_details = []

    # 按周做截面预测
    sorted_all_weeks = sorted(all_week_keys)
    # 只取最后n_weeks周
    if len(sorted_all_weeks) > n_weeks:
        sorted_all_weeks = sorted_all_weeks[-n_weeks:]

    for week_key in sorted_all_weeks:
        engine = V12PredictionEngine()
        week_predictions = {}
        week_actuals = {}
        week_opps = 0

        # 计算本周截面波动率中位数（IVOL过滤的自适应阈值）
        week_vols = []
        week_turns = []
        for code, week_info in stock_weekly.items():
            if week_key not in week_info:
                continue
            kls = week_info[week_key]['hist_klines']
            if len(kls) >= 20:
                pcts = [k.get('change_percent', 0) or 0 for k in kls]
                recent = pcts[-20:]
                m = sum(recent) / 20
                vol = (sum((p - m) ** 2 for p in recent) / 19) ** 0.5
                week_vols.append(vol)
                turnover_vals = [k.get('turnover', 0) or 0 for k in kls]
                avg_turn = sum(turnover_vals[-20:]) / 20
                week_turns.append(avg_turn)
        vol_median = sorted(week_vols)[len(week_vols) // 2] if week_vols else None
        turn_median = sorted(week_turns)[len(week_turns) // 2] if week_turns else None

        for code, week_info in stock_weekly.items():
            if week_key not in week_info:
                continue
            data = week_info[week_key]
            week_opps += 1
            total_opportunities += 1

            last_date = data.get('last_date', '')
            mkt_hist = [m for m in market_klines if m['date'] <= last_date] if market_klines else None

            pred = engine.predict_single(code, data['hist_klines'], data['hist_ff'], mkt_hist, vol_median, turn_median)
            if pred is not None:
                week_predictions[code] = pred
                week_actuals[code] = data['actual_return']

        by_week[week_key]['opportunities'] = week_opps

        for code, pred in week_predictions.items():
            actual = week_actuals.get(code)
            if actual is None:
                continue

            actual_up = actual > 0
            pred_up = pred['pred_direction'] == 'UP'
            is_correct = pred_up == actual_up

            total_pred += 1
            if is_correct:
                total_correct += 1

            # 按置信度
            conf = pred['confidence']
            by_confidence[conf]['total'] += 1
            if is_correct:
                by_confidence[conf]['correct'] += 1

            # 按极端分数
            es = pred.get('extreme_score', 0)
            es_key = f"score_{es}" if es <= 5 else "score_6+"
            by_extreme_score[es_key]['total'] += 1
            if is_correct:
                by_extreme_score[es_key]['correct'] += 1

            # 按周
            by_week[week_key]['total'] += 1
            if is_correct:
                by_week[week_key]['correct'] += 1

            # 按方向一致性
            agree_key = 'agree' if pred.get('direction_agree') else 'disagree'
            by_direction_agree[agree_key]['total'] += 1
            if is_correct:
                by_direction_agree[agree_key]['correct'] += 1

            # 按大盘协同性
            mkt_key = 'aligned' if pred.get('market_aligned') else 'independent'
            by_market_aligned[mkt_key]['total'] += 1
            if is_correct:
                by_market_aligned[mkt_key]['correct'] += 1

            # 各信号独立准确率
            for sig in pred.get('signals', []):
                sig_name = sig['signal']
                sig_up = sig['score'] > 0
                by_signal[sig_name]['total'] += 1
                if sig_up == actual_up:
                    by_signal[sig_name]['correct'] += 1

            # 学术质量因子统计
            vc = pred.get('volume_confirmed', False)
            lt = pred.get('low_turnover_boost', False)
            ls = pred.get('low_salience', False)
            vc_key = 'vol_confirmed' if vc else 'vol_not_confirmed'
            by_quality_factors[vc_key]['total'] += 1
            if is_correct:
                by_quality_factors[vc_key]['correct'] += 1
            lt_key = 'low_turnover' if lt else 'normal_turnover'
            by_quality_factors[lt_key]['total'] += 1
            if is_correct:
                by_quality_factors[lt_key]['correct'] += 1
            ls_key = 'low_salience' if ls else 'high_salience'
            by_quality_factors[ls_key]['total'] += 1
            if is_correct:
                by_quality_factors[ls_key]['correct'] += 1
            # n_supporting统计
            ns = pred.get('n_supporting', 0)
            ns_key = f'n_supporting_{ns}' if ns <= 4 else 'n_supporting_5+'
            by_quality_factors[ns_key]['total'] += 1
            if is_correct:
                by_quality_factors[ns_key]['correct'] += 1

    # 3. 输出结果
    logger.info("[3/3] 生成报告...")

    overall_acc = total_correct / total_pred if total_pred > 0 else 0
    coverage = total_pred / total_opportunities if total_opportunities > 0 else 0

    # 按周准确率趋势
    sorted_week_keys = sorted(by_week.keys())
    week_accs = []
    for wk in sorted_week_keys:
        d = by_week[wk]
        if d['total'] >= 1:
            week_accs.append({
                'week': wk,
                'accuracy': round(d['correct'] / d['total'], 4) if d['total'] > 0 else 0,
                'n_pred': d['total'],
                'n_opportunities': d['opportunities'],
                'coverage': round(d['total'] / d['opportunities'], 4) if d['opportunities'] > 0 else 0,
            })

    # 趋势判断
    trend = 'insufficient_data'
    valid_weeks = [w for w in week_accs if w['n_pred'] >= 5]
    if len(valid_weeks) >= 6:
        first_half = valid_weeks[:len(valid_weeks) // 2]
        second_half = valid_weeks[len(valid_weeks) // 2:]
        acc_first = sum(w['accuracy'] for w in first_half) / len(first_half)
        acc_second = sum(w['accuracy'] for w in second_half) / len(second_half)
        if acc_second - acc_first > 0.03:
            trend = 'improving'
        elif acc_first - acc_second > 0.03:
            trend = 'degrading'
        else:
            trend = 'stable'

    # 高置信度子集
    high_conf = by_confidence.get('high', {'total': 0, 'correct': 0})
    high_acc = high_conf['correct'] / high_conf['total'] if high_conf['total'] > 0 else 0

    # 极端条件方向与信号方向一致的子集
    agree_d = by_direction_agree['agree']
    agree_acc = agree_d['correct'] / agree_d['total'] if agree_d['total'] > 0 else 0
    disagree_d = by_direction_agree['disagree']
    disagree_acc = disagree_d['correct'] / disagree_d['total'] if disagree_d['total'] > 0 else 0

    # 大盘协同性分析
    aligned_d = by_market_aligned['aligned']
    aligned_acc = aligned_d['correct'] / aligned_d['total'] if aligned_d['total'] > 0 else 0
    indep_d = by_market_aligned['independent']
    indep_acc = indep_d['correct'] / indep_d['total'] if indep_d['total'] > 0 else 0

    report = {
        'meta': {
            'algorithm': 'V12-TwoLayer',
            'description': 'Layer1:极端条件过滤 + Layer2:多信号投票',
            'n_stocks': len(stock_codes),
            'n_weeks': n_weeks,
            'date_range': f"{start_date} ~ {end_date}",
            'run_time_sec': round(time.time() - t0, 1),
        },
        'overall': {
            'accuracy': round(overall_acc, 4),
            'total_predictions': total_pred,
            'total_correct': total_correct,
            'total_opportunities': total_opportunities,
            'coverage': round(coverage, 4),
            'trend': trend,
        },
        'high_confidence': {
            'accuracy': round(high_acc, 4),
            'total': high_conf['total'],
            'correct': high_conf['correct'],
        },
        'direction_agreement': {
            'agree': {
                'description': '极端条件方向 = 信号投票方向',
                'accuracy': round(agree_acc, 4),
                'total': agree_d['total'],
            },
            'disagree': {
                'description': '极端条件方向 ≠ 信号投票方向',
                'accuracy': round(disagree_acc, 4),
                'total': disagree_d['total'],
            },
        },
        'market_alignment': {
            'aligned': {
                'description': '大盘方向与极端条件协同（系统性超卖/过热）',
                'accuracy': round(aligned_acc, 4),
                'total': aligned_d['total'],
            },
            'independent': {
                'description': '个股独立极端（大盘未同向）',
                'accuracy': round(indep_acc, 4),
                'total': indep_d['total'],
            },
        },
        'by_confidence': {
            k: {
                'accuracy': round(v['correct'] / v['total'], 4) if v['total'] > 0 else 0,
                'total': v['total'],
                'correct': v['correct'],
            }
            for k, v in sorted(by_confidence.items())
        },
        'by_extreme_score': {
            k: {
                'accuracy': round(v['correct'] / v['total'], 4) if v['total'] > 0 else 0,
                'total': v['total'],
                'correct': v['correct'],
            }
            for k, v in sorted(by_extreme_score.items())
        },
        'by_signal': {
            k: {
                'accuracy': round(v['correct'] / v['total'], 4) if v['total'] > 0 else 0,
                'total': v['total'],
                'correct': v['correct'],
            }
            for k, v in sorted(by_signal.items())
        },
        'by_quality_factors': {
            k: {
                'accuracy': round(v['correct'] / v['total'], 4) if v['total'] > 0 else 0,
                'total': v['total'],
                'correct': v['correct'],
            }
            for k, v in sorted(by_quality_factors.items())
        },
        'weekly_accuracy': week_accs,
    }

    # 保存
    output_path = OUTPUT_DIR / "v12_backtest_result.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    logger.info("结果已保存: %s", output_path)

    # 打印摘要
    print("\n" + "=" * 60)
    print("V12 两层架构预测体系 — 回测结果")
    print("=" * 60)
    print(f"\n📊 数据: {len(stock_codes)}只股票, {n_weeks}周")
    print(f"\n🎯 总体准确率: {overall_acc:.1%} ({total_pred}条预测)")
    print(f"   覆盖率: {coverage:.1%} ({total_pred}/{total_opportunities})")
    print(f"   趋势: {trend}")

    print(f"\n📈 按置信度:")
    for conf in ('high', 'medium', 'low'):
        d = by_confidence.get(conf, {'total': 0, 'correct': 0})
        if d['total'] > 0:
            acc = d['correct'] / d['total']
            print(f"   {conf:8s}: {acc:.1%} ({d['total']}条)")

    print(f"\n🔥 按极端分数:")
    for es_key in sorted(by_extreme_score.keys()):
        d = by_extreme_score[es_key]
        if d['total'] > 0:
            acc = d['correct'] / d['total']
            print(f"   {es_key:12s}: {acc:.1%} ({d['total']}条)")

    print(f"\n🤝 方向一致性:")
    print(f"   极端+信号一致: {agree_acc:.1%} ({agree_d['total']}条)")
    print(f"   极端+信号矛盾: {disagree_acc:.1%} ({disagree_d['total']}条)")

    print(f"\n🌍 大盘协同性:")
    print(f"   大盘协同(系统性): {aligned_acc:.1%} ({aligned_d['total']}条)")
    print(f"   个股独立:         {indep_acc:.1%} ({indep_d['total']}条)")

    print(f"\n📡 各信号独立准确率:")
    for sig_name in sorted(by_signal.keys()):
        d = by_signal[sig_name]
        if d['total'] > 0:
            acc = d['correct'] / d['total']
            print(f"   {sig_name:25s}: {acc:.1%} ({d['total']}条)")

    print(f"\n🔬 学术质量因子:")
    for qf_name in sorted(by_quality_factors.keys()):
        d = by_quality_factors[qf_name]
        if d['total'] > 0:
            acc = d['correct'] / d['total']
            print(f"   {qf_name:25s}: {acc:.1%} ({d['total']}条)")

    if week_accs:
        print(f"\n📅 周度准确率 (最近5周):")
        for wa in week_accs[-5:]:
            print(f"   {wa['week']}: {wa['accuracy']:.1%} (n={wa['n_pred']}, "
                  f"覆盖{wa['coverage']:.0%})")

    print(f"\n⏱️  耗时: {time.time() - t0:.1f}秒")
    print("=" * 60)

    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='V12两层架构预测体系回测')
    parser.add_argument('--weeks', type=int, default=30, help='回测周数')
    parser.add_argument('--stocks', type=int, default=200, help='股票数量')
    args = parser.parse_args()
    run_v12_backtest(n_weeks=args.weeks, n_stocks=args.stocks)
