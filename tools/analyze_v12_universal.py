#!/usr/bin/env python3
"""
V12 通用性预测研究
==================
分析V12在不同市场环境下失败的原因，寻找通用性改进方向。

核心问题：V12在震荡市(53.4%)和上涨市(50.2%)几乎无效，
         只在下跌市(69%)有效。需要找到跨环境稳定的信号特征。

研究方向：
  1. 震荡市失败原因分析：哪些信号/条件在震荡市失效？
  2. 跨环境稳定因子筛选：哪些维度在所有环境下都有区分度？
  3. 信号质量vs数量：是信号太弱还是过滤不够？
  4. 置信度校准失败原因：为什么high在震荡市只有23.4%？
  5. 寻找通用性alpha：不依赖市场方向的个股信号

用法：
    source .venv/bin/activate
    python -m tools.analyze_v12_universal
"""
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


def _to_float(v):
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def load_stock_codes(limit=5000):
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
    return codes


def load_kline_data(stock_codes, start_date, end_date):
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


def load_fund_flow_data(stock_codes, start_date):
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


def load_market_klines(start_date, end_date):
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


def group_by_week(klines):
    weeks = defaultdict(list)
    for k in klines:
        d = datetime.strptime(k['date'][:10], '%Y-%m-%d')
        iso = d.isocalendar()
        key = f"{iso[0]}-W{iso[1]:02d}"
        weeks[key].append(k)
    return dict(weeks)


def prepare_backtest_data(stock_codes, n_weeks=100):
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=n_weeks * 7 + 120)).strftime('%Y-%m-%d')

    logger.info("加载数据: %d只股票, %s ~ %s", len(stock_codes), start_date, end_date)
    kline_data = load_kline_data(stock_codes, start_date, end_date)
    fund_flow_data = load_fund_flow_data(stock_codes, start_date)
    market_klines = load_market_klines(start_date, end_date)

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

    return stock_weekly, sorted(all_week_keys), market_klines


def get_market_week_chg(market_klines):
    """计算每周大盘涨跌幅"""
    mkt_weekly = group_by_week(market_klines)
    mkt_week_chg = {}
    for wk, kls in mkt_weekly.items():
        if len(kls) >= 2:
            first_close = kls[0].get('close', 0)
            last_close = kls[-1].get('close', 0)
            if first_close > 0:
                mkt_week_chg[wk] = (last_close / first_close - 1) * 100
    return mkt_week_chg


def classify_market_regime(chg):
    """分类市场环境"""
    if chg < -3:
        return '暴跌'
    elif chg < -1:
        return '下跌'
    elif chg <= 1:
        return '震荡'
    elif chg <= 3:
        return '上涨'
    else:
        return '暴涨'


def run_full_predictions(stock_weekly, all_weeks, market_klines):
    """运行全量预测，返回详细结果列表"""
    results = []
    for week_key in all_weeks:
        engine = V12PredictionEngine()

        # 计算截面中位数
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
            last_date = data.get('last_date', '')
            mkt_hist = [m for m in market_klines if m['date'] <= last_date] if market_klines else None

            pred = engine.predict_single(code, data['hist_klines'], data['hist_ff'],
                                         mkt_hist, vol_median, turn_median)
            if pred is not None:
                actual = data['actual_return']
                is_correct = (pred['pred_direction'] == 'UP') == (actual > 0)
                results.append({
                    'week': week_key,
                    'code': code,
                    'pred': pred,
                    'actual_return': actual,
                    'is_correct': is_correct,
                })

    return results


# ═══════════════════════════════════════════════════════════
# 分析1: 震荡市失败原因深度拆解
# ═══════════════════════════════════════════════════════════

def analyze_sideways_failure(results, mkt_week_chg):
    """
    深度分析震荡市（大盘-1%~+1%）中V12为什么失效。
    拆解每个维度在震荡市vs下跌市的表现差异。
    """
    sideways_weeks = set(wk for wk, chg in mkt_week_chg.items() if -1 <= chg <= 1)
    decline_weeks = set(wk for wk, chg in mkt_week_chg.items() if chg < -1)

    sideways = [r for r in results if r['week'] in sideways_weeks]
    decline = [r for r in results if r['week'] in decline_weeks]

    def acc(lst):
        if not lst:
            return 0, 0
        c = sum(1 for r in lst if r['is_correct'])
        return c / len(lst), len(lst)

    analysis = {}

    # 按信号数量
    for label, subset in [('震荡市', sideways), ('下跌市', decline)]:
        by_ns = defaultdict(list)
        for r in subset:
            ns = r['pred'].get('n_supporting', 0)
            by_ns[ns].append(r)
        ns_stats = {}
        for ns in sorted(by_ns.keys()):
            a, n = acc(by_ns[ns])
            ns_stats[f'ns={ns}'] = {'accuracy': round(a, 4), 'n': n}
        analysis[f'{label}_by_n_supporting'] = ns_stats

    # 按置信度
    for label, subset in [('震荡市', sideways), ('下跌市', decline)]:
        by_conf = defaultdict(list)
        for r in subset:
            by_conf[r['pred']['confidence']].append(r)
        conf_stats = {}
        for conf in ['high', 'medium', 'low']:
            a, n = acc(by_conf.get(conf, []))
            conf_stats[conf] = {'accuracy': round(a, 4), 'n': n}
        analysis[f'{label}_by_confidence'] = conf_stats

    # 按extreme_score
    for label, subset in [('震荡市', sideways), ('下跌市', decline)]:
        by_es = defaultdict(list)
        for r in subset:
            es = r['pred'].get('extreme_score', 0)
            key = f'es={es}' if es <= 7 else 'es=8+'
            by_es[key].append(r)
        es_stats = {}
        for key in sorted(by_es.keys()):
            a, n = acc(by_es[key])
            es_stats[key] = {'accuracy': round(a, 4), 'n': n}
        analysis[f'{label}_by_extreme_score'] = es_stats

    # 按方向
    for label, subset in [('震荡市', sideways), ('下跌市', decline)]:
        up = [r for r in subset if r['pred']['pred_direction'] == 'UP']
        down = [r for r in subset if r['pred']['pred_direction'] == 'DOWN']
        up_acc, up_n = acc(up)
        down_acc, down_n = acc(down)
        analysis[f'{label}_by_direction'] = {
            'UP': {'accuracy': round(up_acc, 4), 'n': up_n},
            'DOWN': {'accuracy': round(down_acc, 4), 'n': down_n},
        }

    # 按market_aligned
    for label, subset in [('震荡市', sideways), ('下跌市', decline)]:
        aligned = [r for r in subset if r['pred'].get('market_aligned')]
        indep = [r for r in subset if not r['pred'].get('market_aligned')]
        al_acc, al_n = acc(aligned)
        in_acc, in_n = acc(indep)
        analysis[f'{label}_by_market_aligned'] = {
            'aligned': {'accuracy': round(al_acc, 4), 'n': al_n},
            'independent': {'accuracy': round(in_acc, 4), 'n': in_n},
        }

    # 按各信号独立准确率
    for label, subset in [('震荡市', sideways), ('下跌市', decline)]:
        sig_stats = defaultdict(lambda: {'total': 0, 'correct': 0})
        for r in subset:
            actual_up = r['actual_return'] > 0
            for sig in r['pred'].get('signals', []):
                sig_name = sig['signal']
                sig_up = sig['score'] > 0
                sig_stats[sig_name]['total'] += 1
                if sig_up == actual_up:
                    sig_stats[sig_name]['correct'] += 1
        sig_result = {}
        for name in sorted(sig_stats.keys()):
            s = sig_stats[name]
            sig_result[name] = {
                'accuracy': round(s['correct'] / s['total'], 4) if s['total'] > 0 else 0,
                'n': s['total'],
            }
        analysis[f'{label}_signal_accuracy'] = sig_result

    # 按缩量+低换手
    for label, subset in [('震荡市', sideways), ('下跌市', decline)]:
        vol_low = [r for r in subset
                   if r['pred'].get('volume_confirmed') and r['pred'].get('low_turnover_boost')]
        vol_other = [r for r in subset
                     if not (r['pred'].get('volume_confirmed') and r['pred'].get('low_turnover_boost'))]
        vl_acc, vl_n = acc(vol_low)
        vo_acc, vo_n = acc(vol_other)
        analysis[f'{label}_by_vol_turnover'] = {
            '缩量+低换手': {'accuracy': round(vl_acc, 4), 'n': vl_n},
            '其他': {'accuracy': round(vo_acc, 4), 'n': vo_n},
        }

    # 震荡市中实际涨跌分布
    sw_returns = [r['actual_return'] for r in sideways]
    if sw_returns:
        up_pct = sum(1 for r in sw_returns if r > 0) / len(sw_returns)
        avg_ret = sum(sw_returns) / len(sw_returns)
        analysis['震荡市_actual_distribution'] = {
            'n_predictions': len(sw_returns),
            'actual_up_ratio': round(up_pct, 4),
            'avg_actual_return': round(avg_ret, 4),
            'note': '震荡市中被预测的股票实际涨跌分布',
        }

    return analysis


# ═══════════════════════════════════════════════════════════
# 分析2: 跨环境稳定因子筛选
# ═══════════════════════════════════════════════════════════

def find_stable_factors(results, mkt_week_chg):
    """
    在所有市场环境下寻找稳定有区分度的因子组合。
    一个好的因子应该在暴跌/下跌/震荡/上涨/暴涨中都有正向区分度。
    """
    regimes = {
        '暴跌': lambda chg: chg < -3,
        '下跌': lambda chg: -3 <= chg < -1,
        '震荡': lambda chg: -1 <= chg <= 1,
        '上涨': lambda chg: 1 < chg <= 3,
        '暴涨': lambda chg: chg > 3,
    }

    regime_weeks = {}
    for name, fn in regimes.items():
        regime_weeks[name] = set(wk for wk, chg in mkt_week_chg.items() if fn(chg))

    # 定义要测试的因子维度
    factor_tests = {
        'extreme_score_high': lambda r: r['pred'].get('extreme_score', 0) >= 7,
        'extreme_score_low': lambda r: r['pred'].get('extreme_score', 0) <= 5,
        'ns_4plus': lambda r: r['pred'].get('n_supporting', 0) >= 4,
        'ns_3': lambda r: r['pred'].get('n_supporting', 0) == 3,
        'market_aligned': lambda r: r['pred'].get('market_aligned', False),
        'market_independent': lambda r: not r['pred'].get('market_aligned', False),
        'volume_depleted': lambda r: r['pred'].get('volume_confirmed', False),
        'low_turnover': lambda r: r['pred'].get('low_turnover_boost', False),
        'vol_and_turn': lambda r: r['pred'].get('volume_confirmed', False) and r['pred'].get('low_turnover_boost', False),
        'UP_direction': lambda r: r['pred']['pred_direction'] == 'UP',
        'DOWN_direction': lambda r: r['pred']['pred_direction'] == 'DOWN',
        'high_confidence': lambda r: r['pred']['confidence'] == 'high',
        'low_confidence': lambda r: r['pred']['confidence'] == 'low',
        'strong_reversal': lambda r: any(s['signal'] == 'reversal' and abs(s['score']) > 0.3 for s in r['pred'].get('signals', [])),
        'rsi_deep_oversold': lambda r: r['pred'].get('conditions', {}).get('rsi', 50) < 30,
        'rsi_oversold': lambda r: r['pred'].get('conditions', {}).get('rsi', 50) < 35,
        'price_very_low': lambda r: r['pred'].get('conditions', {}).get('price_pos', 0.5) < 0.15,
        'week_chg_lt_neg5': lambda r: r['pred'].get('conditions', {}).get('week_chg', 0) < -5,
        'week_chg_lt_neg7': lambda r: r['pred'].get('conditions', {}).get('week_chg', 0) < -7,
        'consec_down_4plus': lambda r: r['pred'].get('conditions', {}).get('consec_down', 0) >= 4,
        'high_amplitude': lambda r: r['pred'].get('conditions', {}).get('week_amplitude', 0) > 15,
    }

    factor_stability = {}

    for factor_name, factor_fn in factor_tests.items():
        regime_accs = {}
        for regime_name, weeks in regime_weeks.items():
            regime_results = [r for r in results if r['week'] in weeks]
            factor_true = [r for r in regime_results if factor_fn(r)]
            factor_false = [r for r in regime_results if not factor_fn(r)]

            true_correct = sum(1 for r in factor_true if r['is_correct'])
            false_correct = sum(1 for r in factor_false if r['is_correct'])

            true_acc = true_correct / len(factor_true) if factor_true else 0
            false_acc = false_correct / len(factor_false) if factor_false else 0
            lift = true_acc - false_acc

            regime_accs[regime_name] = {
                'true_acc': round(true_acc, 4),
                'true_n': len(factor_true),
                'false_acc': round(false_acc, 4),
                'false_n': len(factor_false),
                'lift': round(lift, 4),
            }

        # 计算跨环境稳定性
        lifts = [v['lift'] for v in regime_accs.values() if v['true_n'] >= 20]
        if lifts:
            avg_lift = sum(lifts) / len(lifts)
            min_lift = min(lifts)
            max_lift = max(lifts)
            # 稳定性 = 所有环境lift都>0的比例
            positive_ratio = sum(1 for l in lifts if l > 0) / len(lifts)
            lift_std = (sum((l - avg_lift)**2 for l in lifts) / len(lifts))**0.5
        else:
            avg_lift = min_lift = max_lift = positive_ratio = lift_std = 0

        factor_stability[factor_name] = {
            'by_regime': regime_accs,
            'stability': {
                'avg_lift': round(avg_lift, 4),
                'min_lift': round(min_lift, 4),
                'max_lift': round(max_lift, 4),
                'positive_ratio': round(positive_ratio, 4),
                'lift_std': round(lift_std, 4),
                'is_universal': positive_ratio >= 0.8 and avg_lift > 0.02,
            }
        }

    # 按稳定性排序
    ranked = sorted(factor_stability.items(),
                    key=lambda x: (x[1]['stability']['positive_ratio'],
                                   x[1]['stability']['avg_lift']),
                    reverse=True)

    return {
        'factors': dict(ranked),
        'universal_factors': [name for name, data in ranked
                              if data['stability']['is_universal']],
        'best_sideways_factors': sorted(
            [(name, data['by_regime'].get('震荡', {}).get('lift', 0))
             for name, data in factor_stability.items()
             if data['by_regime'].get('震荡', {}).get('true_n', 0) >= 20],
            key=lambda x: x[1], reverse=True
        )[:10],
    }


# ═══════════════════════════════════════════════════════════
# 分析3: 置信度校准失败原因
# ═══════════════════════════════════════════════════════════

def analyze_confidence_failure(results, mkt_week_chg):
    """
    分析为什么high置信度在震荡市只有23.4%。
    拆解high置信度的构成：哪些路径产生了high？哪些路径在震荡市失效？
    """
    sideways_weeks = set(wk for wk, chg in mkt_week_chg.items() if -1 <= chg <= 1)

    # high置信度的产生路径：
    # 路径1: extreme_score>=6 + market_aligned (基础high)
    # 路径2: medium升级 → ns>=4 + market_aligned
    # 路径3: medium升级 → UP + vol_depleted + low_turnover
    # 降级: ns==3 → high降为medium

    all_high = [r for r in results if r['pred']['confidence'] == 'high']
    sw_high = [r for r in all_high if r['week'] in sideways_weeks]
    other_high = [r for r in all_high if r['week'] not in sideways_weeks]

    def acc(lst):
        if not lst:
            return 0, 0
        c = sum(1 for r in lst if r['is_correct'])
        return round(c / len(lst), 4), len(lst)

    # 分析high的构成路径
    def classify_high_path(r):
        p = r['pred']
        es = p.get('extreme_score', 0)
        ma = p.get('market_aligned', False)
        ns = p.get('n_supporting', 0)
        vc = p.get('volume_confirmed', False)
        lt = p.get('low_turnover_boost', False)
        direction = p['pred_direction']

        # 基础high: es>=6 + aligned
        base_high = es >= 6 and ma
        # ns升级: ns>=4 + aligned 可以从medium升到high
        ns_upgrade = ns >= 4 and ma
        # vol升级: UP + vol + low_turn 可以从medium升到high
        vol_upgrade = direction == 'UP' and vc and lt

        if base_high and ns_upgrade:
            return 'base_high+ns_upgrade'
        elif base_high and vol_upgrade:
            return 'base_high+vol_upgrade'
        elif base_high:
            return 'base_high_only'
        elif ns_upgrade and vol_upgrade:
            return 'ns_upgrade+vol_upgrade'
        elif ns_upgrade:
            return 'ns_upgrade_only'
        elif vol_upgrade:
            return 'vol_upgrade_only'
        else:
            return 'unknown'

    path_analysis = {}
    for label, subset in [('全部', all_high), ('震荡市', sw_high), ('非震荡市', other_high)]:
        by_path = defaultdict(list)
        for r in subset:
            path = classify_high_path(r)
            by_path[path].append(r)

        path_stats = {}
        for path in sorted(by_path.keys()):
            a, n = acc(by_path[path])
            path_stats[path] = {'accuracy': a, 'n': n}
        path_analysis[label] = path_stats

    # 震荡市中high预测的实际涨跌分布
    sw_high_returns = [r['actual_return'] for r in sw_high]
    sw_high_up_pred = [r for r in sw_high if r['pred']['pred_direction'] == 'UP']
    sw_high_down_pred = [r for r in sw_high if r['pred']['pred_direction'] == 'DOWN']

    return {
        'path_analysis': path_analysis,
        'sideways_high_stats': {
            'total': len(sw_high),
            'accuracy': acc(sw_high)[0],
            'UP_predictions': {'accuracy': acc(sw_high_up_pred)[0], 'n': len(sw_high_up_pred)},
            'DOWN_predictions': {'accuracy': acc(sw_high_down_pred)[0], 'n': len(sw_high_down_pred)},
            'avg_actual_return': round(sum(sw_high_returns) / len(sw_high_returns), 4) if sw_high_returns else 0,
            'actual_up_ratio': round(sum(1 for r in sw_high_returns if r > 0) / len(sw_high_returns), 4) if sw_high_returns else 0,
        },
    }


# ═══════════════════════════════════════════════════════════
# 分析4: 寻找通用性alpha — 不依赖大盘方向的个股信号
# ═══════════════════════════════════════════════════════════

def find_universal_alpha(results, mkt_week_chg):
    """
    寻找在所有市场环境下都有效的信号组合。
    核心思路：如果一个信号只在大盘暴跌后有效，那它捕捉的是β不是α。
    真正的α应该在震荡市也有效。
    """
    sideways_weeks = set(wk for wk, chg in mkt_week_chg.items() if -1 <= chg <= 1)
    sw_results = [r for r in results if r['week'] in sideways_weeks]

    def acc(lst):
        if not lst:
            return 0, 0
        c = sum(1 for r in lst if r['is_correct'])
        return round(c / len(lst), 4), len(lst)

    # 测试多因子组合在震荡市的表现
    combos = {}

    # 组合1: 高extreme_score + 高n_supporting
    c1 = [r for r in sw_results
          if r['pred'].get('extreme_score', 0) >= 7
          and r['pred'].get('n_supporting', 0) >= 4]
    combos['es≥7+ns≥4'] = acc(c1)

    # 组合2: RSI深度超卖 + 缩量
    c2 = [r for r in sw_results
          if r['pred'].get('conditions', {}).get('rsi', 50) < 30
          and r['pred'].get('volume_confirmed', False)]
    combos['RSI<30+缩量'] = acc(c2)

    # 组合3: RSI超卖 + 低换手 + ns>=4
    c3 = [r for r in sw_results
          if r['pred'].get('conditions', {}).get('rsi', 50) < 35
          and r['pred'].get('low_turnover_boost', False)
          and r['pred'].get('n_supporting', 0) >= 4]
    combos['RSI<35+低换手+ns≥4'] = acc(c3)

    # 组合4: 极端暴跌(>7%) + 任意
    c4 = [r for r in sw_results
          if r['pred'].get('conditions', {}).get('week_chg', 0) < -7]
    combos['周跌>7%'] = acc(c4)

    # 组合5: 连跌4天+ + ns>=4
    c5 = [r for r in sw_results
          if r['pred'].get('conditions', {}).get('consec_down', 0) >= 4
          and r['pred'].get('n_supporting', 0) >= 4]
    combos['连跌4天+ns≥4'] = acc(c5)

    # 组合6: 60日极低位 + RSI超卖
    c6 = [r for r in sw_results
          if r['pred'].get('conditions', {}).get('price_pos', 0.5) < 0.15
          and r['pred'].get('conditions', {}).get('rsi', 50) < 35]
    combos['60日极低位+RSI<35'] = acc(c6)

    # 组合7: 缩量+低换手+ns>=4 (三重过滤)
    c7 = [r for r in sw_results
          if r['pred'].get('volume_confirmed', False)
          and r['pred'].get('low_turnover_boost', False)
          and r['pred'].get('n_supporting', 0) >= 4]
    combos['缩量+低换手+ns≥4'] = acc(c7)

    # 组合8: es>=7 + RSI<35
    c8 = [r for r in sw_results
          if r['pred'].get('extreme_score', 0) >= 7
          and r['pred'].get('conditions', {}).get('rsi', 50) < 35]
    combos['es≥7+RSI<35'] = acc(c8)

    # 组合9: 周跌>5% + 连跌3天+ + ns>=4
    c9 = [r for r in sw_results
          if r['pred'].get('conditions', {}).get('week_chg', 0) < -5
          and r['pred'].get('conditions', {}).get('consec_down', 0) >= 3
          and r['pred'].get('n_supporting', 0) >= 4]
    combos['周跌>5%+连跌3天+ns≥4'] = acc(c9)

    # 组合10: UP方向 + es>=7 + ns>=4
    c10 = [r for r in sw_results
           if r['pred']['pred_direction'] == 'UP'
           and r['pred'].get('extreme_score', 0) >= 7
           and r['pred'].get('n_supporting', 0) >= 4]
    combos['UP+es≥7+ns≥4'] = acc(c10)

    # 组合11: 不依赖market_aligned的组合
    c11 = [r for r in sw_results
           if not r['pred'].get('market_aligned', False)
           and r['pred'].get('n_supporting', 0) >= 4
           and r['pred'].get('extreme_score', 0) >= 7]
    combos['独立+es≥7+ns≥4'] = acc(c11)

    # 组合12: 强reversal信号 + RSI超卖
    c12 = [r for r in sw_results
           if any(s['signal'] == 'reversal' and s['score'] > 0.3 for s in r['pred'].get('signals', []))
           and r['pred'].get('conditions', {}).get('rsi', 50) < 35]
    combos['强reversal+RSI<35'] = acc(c12)

    # 全环境验证最佳组合
    best_combos_full = {}
    # 对震荡市准确率>55%且样本>20的组合，在全环境验证
    for name, (a, n) in combos.items():
        if a > 0.55 and n >= 10:
            # 在所有环境下测试
            regime_results = {}
            for regime_name, regime_fn in [
                ('暴跌', lambda chg: chg < -3),
                ('下跌', lambda chg: -3 <= chg < -1),
                ('震荡', lambda chg: -1 <= chg <= 1),
                ('上涨', lambda chg: 1 < chg <= 3),
                ('暴涨', lambda chg: chg > 3),
            ]:
                regime_wks = set(wk for wk, chg in mkt_week_chg.items() if regime_fn(chg))
                # 重新应用同样的过滤条件
                regime_subset = [r for r in results if r['week'] in regime_wks]
                # 需要重新应用组合过滤...这里简化处理
                regime_results[regime_name] = '需要在主函数中验证'

            best_combos_full[name] = {
                'sideways_accuracy': a,
                'sideways_n': n,
            }

    return {
        'sideways_combos': {name: {'accuracy': a, 'n': n} for name, (a, n) in combos.items()},
        'promising_combos': best_combos_full,
        'baseline_sideways': acc(sw_results),
    }


# ═══════════════════════════════════════════════════════════
# 分析5: 周度一致性分析 — 寻找稳定盈利的子集
# ═══════════════════════════════════════════════════════════

def weekly_consistency_analysis(results, mkt_week_chg):
    """
    不按市场环境分，而是直接看哪些因子组合能提高周胜率。
    周胜率比整体准确率更重要——一个策略如果只在少数周大赚，
    大部分周亏损，实际上不可用。
    """
    # 按周统计
    weekly_stats = defaultdict(lambda: defaultdict(lambda: {'total': 0, 'correct': 0}))

    for r in results:
        week = r['week']
        is_correct = r['is_correct']
        pred = r['pred']

        # 全部
        weekly_stats[week]['all']['total'] += 1
        if is_correct:
            weekly_stats[week]['all']['correct'] += 1

        # ns>=4
        if pred.get('n_supporting', 0) >= 4:
            weekly_stats[week]['ns4+']['total'] += 1
            if is_correct:
                weekly_stats[week]['ns4+']['correct'] += 1

        # es>=7
        if pred.get('extreme_score', 0) >= 7:
            weekly_stats[week]['es7+']['total'] += 1
            if is_correct:
                weekly_stats[week]['es7+']['correct'] += 1

        # ns>=4 + es>=7
        if pred.get('n_supporting', 0) >= 4 and pred.get('extreme_score', 0) >= 7:
            weekly_stats[week]['ns4+es7+']['total'] += 1
            if is_correct:
                weekly_stats[week]['ns4+es7+']['correct'] += 1

        # UP only
        if pred['pred_direction'] == 'UP':
            weekly_stats[week]['UP_only']['total'] += 1
            if is_correct:
                weekly_stats[week]['UP_only']['correct'] += 1

        # UP + ns>=4
        if pred['pred_direction'] == 'UP' and pred.get('n_supporting', 0) >= 4:
            weekly_stats[week]['UP+ns4+']['total'] += 1
            if is_correct:
                weekly_stats[week]['UP+ns4+']['correct'] += 1

        # RSI<35
        if pred.get('conditions', {}).get('rsi', 50) < 35:
            weekly_stats[week]['RSI<35']['total'] += 1
            if is_correct:
                weekly_stats[week]['RSI<35']['correct'] += 1

        # 缩量+低换手
        if pred.get('volume_confirmed') and pred.get('low_turnover_boost'):
            weekly_stats[week]['vol+turn']['total'] += 1
            if is_correct:
                weekly_stats[week]['vol+turn']['correct'] += 1

    # 计算每个子集的周胜率
    subset_names = ['all', 'ns4+', 'es7+', 'ns4+es7+', 'UP_only', 'UP+ns4+', 'RSI<35', 'vol+turn']
    consistency = {}

    for subset in subset_names:
        week_accs = []
        for week in sorted(weekly_stats.keys()):
            s = weekly_stats[week][subset]
            if s['total'] >= 5:  # 至少5条预测才算有效周
                week_accs.append(s['correct'] / s['total'])

        if week_accs:
            mean_acc = sum(week_accs) / len(week_accs)
            std_acc = (sum((a - mean_acc)**2 for a in week_accs) / len(week_accs))**0.5
            win_rate = sum(1 for a in week_accs if a > 0.5) / len(week_accs)
            # 稳定盈利指标：周胜率 × 平均准确率 / 波动率
            stability_score = (win_rate * mean_acc / std_acc) if std_acc > 0 else 0

            consistency[subset] = {
                'n_valid_weeks': len(week_accs),
                'mean_weekly_accuracy': round(mean_acc, 4),
                'std_weekly_accuracy': round(std_acc, 4),
                'weekly_win_rate': round(win_rate, 4),
                'stability_score': round(stability_score, 4),
                'cv': round(std_acc / mean_acc, 4) if mean_acc > 0 else 0,
            }
        else:
            consistency[subset] = {
                'n_valid_weeks': 0,
                'note': '有效周数不足',
            }

    return consistency


# ═══════════════════════════════════════════════════════════
# 分析6: 个股特征分析 — 什么样的股票更可预测
# ═══════════════════════════════════════════════════════════

def stock_characteristic_analysis(results, mkt_week_chg):
    """
    分析哪些个股特征与预测准确率相关。
    如果某类股票在所有环境下都更可预测，那就是通用性alpha的来源。
    """
    sideways_weeks = set(wk for wk, chg in mkt_week_chg.items() if -1 <= chg <= 1)

    # 按个股特征分组
    characteristics = {
        # 按换手率水平
        'turn_very_low': lambda r: r['pred'].get('conditions', {}).get('avg_turn', 5) < 1,
        'turn_low': lambda r: 1 <= r['pred'].get('conditions', {}).get('avg_turn', 5) < 3,
        'turn_medium': lambda r: 3 <= r['pred'].get('conditions', {}).get('avg_turn', 5) < 6,
        'turn_high': lambda r: r['pred'].get('conditions', {}).get('avg_turn', 5) >= 6,
        # 按RSI水平
        'rsi_deep_oversold': lambda r: r['pred'].get('conditions', {}).get('rsi', 50) < 25,
        'rsi_oversold': lambda r: 25 <= r['pred'].get('conditions', {}).get('rsi', 50) < 35,
        'rsi_neutral': lambda r: 35 <= r['pred'].get('conditions', {}).get('rsi', 50) < 65,
        'rsi_overbought': lambda r: r['pred'].get('conditions', {}).get('rsi', 50) >= 65,
        # 按价格位置
        'price_very_low': lambda r: r['pred'].get('conditions', {}).get('price_pos', 0.5) < 0.15,
        'price_low': lambda r: 0.15 <= r['pred'].get('conditions', {}).get('price_pos', 0.5) < 0.3,
        'price_mid': lambda r: 0.3 <= r['pred'].get('conditions', {}).get('price_pos', 0.5) < 0.7,
        'price_high': lambda r: r['pred'].get('conditions', {}).get('price_pos', 0.5) >= 0.7,
        # 按周跌幅
        'drop_3_5': lambda r: -5 < r['pred'].get('conditions', {}).get('week_chg', 0) <= -3,
        'drop_5_7': lambda r: -7 < r['pred'].get('conditions', {}).get('week_chg', 0) <= -5,
        'drop_7_10': lambda r: -10 < r['pred'].get('conditions', {}).get('week_chg', 0) <= -7,
        'drop_10plus': lambda r: r['pred'].get('conditions', {}).get('week_chg', 0) <= -10,
        # 按振幅
        'amp_low': lambda r: r['pred'].get('conditions', {}).get('week_amplitude', 0) < 10,
        'amp_medium': lambda r: 10 <= r['pred'].get('conditions', {}).get('week_amplitude', 0) < 20,
        'amp_high': lambda r: r['pred'].get('conditions', {}).get('week_amplitude', 0) >= 20,
    }

    def acc(lst):
        if not lst:
            return 0, 0
        c = sum(1 for r in lst if r['is_correct'])
        return round(c / len(lst), 4), len(lst)

    char_results = {}
    for char_name, char_fn in characteristics.items():
        # 全环境
        all_match = [r for r in results if char_fn(r)]
        all_acc, all_n = acc(all_match)

        # 震荡市
        sw_match = [r for r in results if char_fn(r) and r['week'] in sideways_weeks]
        sw_acc, sw_n = acc(sw_match)

        char_results[char_name] = {
            'all_accuracy': all_acc,
            'all_n': all_n,
            'sideways_accuracy': sw_acc,
            'sideways_n': sw_n,
        }

    return char_results


# ═══════════════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════════════

def run_universal_analysis():
    t0 = time.time()
    logger.info("=" * 70)
    logger.info("V12 通用性预测研究")
    logger.info("=" * 70)

    # 加载数据
    logger.info("[0/6] 加载数据...")
    stock_codes = load_stock_codes(5000)
    stock_weekly, all_weeks, market_klines = prepare_backtest_data(stock_codes, n_weeks=100)
    if len(all_weeks) > 100:
        all_weeks = all_weeks[-100:]
    logger.info("  数据准备完成: %d只股票, %d周", len(stock_weekly), len(all_weeks))

    # 计算大盘周涨跌
    mkt_week_chg = get_market_week_chg(market_klines)

    # 运行全量预测
    logger.info("[1/6] 运行全量预测...")
    results = run_full_predictions(stock_weekly, all_weeks, market_klines)
    logger.info("  完成: %d条预测", len(results))

    report = {}

    # 分析1: 震荡市失败原因
    logger.info("[2/6] 震荡市失败原因分析...")
    report['sideways_failure'] = analyze_sideways_failure(results, mkt_week_chg)
    logger.info("  完成")

    # 分析2: 跨环境稳定因子
    logger.info("[3/6] 跨环境稳定因子筛选...")
    report['stable_factors'] = find_stable_factors(results, mkt_week_chg)
    logger.info("  通用因子: %s", report['stable_factors']['universal_factors'])

    # 分析3: 置信度校准失败
    logger.info("[4/6] 置信度校准失败原因...")
    report['confidence_failure'] = analyze_confidence_failure(results, mkt_week_chg)
    logger.info("  完成")

    # 分析4: 通用性alpha
    logger.info("[5/6] 寻找通用性alpha...")
    report['universal_alpha'] = find_universal_alpha(results, mkt_week_chg)
    logger.info("  完成")

    # 分析5: 周度一致性
    logger.info("[6/6] 周度一致性 + 个股特征分析...")
    report['weekly_consistency'] = weekly_consistency_analysis(results, mkt_week_chg)
    report['stock_characteristics'] = stock_characteristic_analysis(results, mkt_week_chg)
    logger.info("  完成")

    # 保存
    output_path = OUTPUT_DIR / "v12_universal_analysis.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    logger.info("\n结果已保存: %s", output_path)

    # ═══════════════════════════════════════════════════════════
    # 打印关键发现
    # ═══════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("V12 通用性预测研究 — 关键发现")
    print("=" * 70)

    # 震荡市 vs 下跌市对比
    sf = report['sideways_failure']
    print("\n📊 震荡市 vs 下跌市 — 信号准确率对比:")
    for sig_name in sorted(set(
        list(sf.get('震荡市_signal_accuracy', {}).keys()) +
        list(sf.get('下跌市_signal_accuracy', {}).keys())
    )):
        sw = sf.get('震荡市_signal_accuracy', {}).get(sig_name, {})
        dc = sf.get('下跌市_signal_accuracy', {}).get(sig_name, {})
        sw_acc = sw.get('accuracy', 0)
        dc_acc = dc.get('accuracy', 0)
        print(f"   {sig_name:20s}: 震荡={sw_acc:.1%}({sw.get('n',0)}) | 下跌={dc_acc:.1%}({dc.get('n',0)}) | Δ={dc_acc-sw_acc:+.1%}")

    print("\n📊 震荡市 vs 下跌市 — 按n_supporting:")
    for ns_key in sorted(set(
        list(sf.get('震荡市_by_n_supporting', {}).keys()) +
        list(sf.get('下跌市_by_n_supporting', {}).keys())
    )):
        sw = sf.get('震荡市_by_n_supporting', {}).get(ns_key, {})
        dc = sf.get('下跌市_by_n_supporting', {}).get(ns_key, {})
        print(f"   {ns_key}: 震荡={sw.get('accuracy',0):.1%}({sw.get('n',0)}) | 下跌={dc.get('accuracy',0):.1%}({dc.get('n',0)})")

    print("\n📊 震荡市 vs 下跌市 — 按置信度:")
    for conf in ['high', 'medium', 'low']:
        sw = sf.get('震荡市_by_confidence', {}).get(conf, {})
        dc = sf.get('下跌市_by_confidence', {}).get(conf, {})
        print(f"   {conf:8s}: 震荡={sw.get('accuracy',0):.1%}({sw.get('n',0)}) | 下跌={dc.get('accuracy',0):.1%}({dc.get('n',0)})")

    # 通用因子
    print("\n🔍 跨环境通用因子:")
    uf = report['stable_factors']
    if uf['universal_factors']:
        for name in uf['universal_factors']:
            data = uf['factors'][name]['stability']
            print(f"   ✅ {name}: avg_lift={data['avg_lift']:+.1%}, min={data['min_lift']:+.1%}, positive={data['positive_ratio']:.0%}")
    else:
        print("   ❌ 未找到跨所有环境都有效的通用因子")

    print("\n   震荡市最佳因子 (top 10):")
    for name, lift in uf['best_sideways_factors']:
        print(f"   {name:30s}: lift={lift:+.1%}")

    # 通用alpha组合
    print("\n🎯 震荡市alpha组合:")
    ua = report['universal_alpha']
    base_acc = ua['baseline_sideways'][0]
    print(f"   基准(震荡市全部): {base_acc:.1%} ({ua['baseline_sideways'][1]}条)")
    for name, data in sorted(ua['sideways_combos'].items(), key=lambda x: x[1]['accuracy'], reverse=True):
        if data['n'] >= 5:
            marker = '✅' if data['accuracy'] > 0.55 else '  '
            print(f"   {marker} {name:30s}: {data['accuracy']:.1%} ({data['n']}条) Δ={data['accuracy']-base_acc:+.1%}")

    # 置信度失败
    print("\n🔧 置信度校准失败原因:")
    cf = report['confidence_failure']
    print(f"   震荡市high: {cf['sideways_high_stats']['accuracy']:.1%} ({cf['sideways_high_stats']['total']}条)")
    print(f"   震荡市high中UP: {cf['sideways_high_stats']['UP_predictions']['accuracy']:.1%} ({cf['sideways_high_stats']['UP_predictions']['n']}条)")
    print(f"   震荡市high中DOWN: {cf['sideways_high_stats']['DOWN_predictions']['accuracy']:.1%} ({cf['sideways_high_stats']['DOWN_predictions']['n']}条)")
    print(f"   震荡市被预测股票实际上涨比例: {cf['sideways_high_stats']['actual_up_ratio']:.1%}")
    print("\n   High路径分析 (震荡市):")
    for path, stats in sorted(cf['path_analysis'].get('震荡市', {}).items(), key=lambda x: x[1]['n'], reverse=True):
        print(f"   {path:30s}: {stats['accuracy']:.1%} ({stats['n']}条)")

    # 周度一致性
    print("\n📅 周度一致性 (周胜率):")
    wc = report['weekly_consistency']
    for subset in sorted(wc.keys(), key=lambda x: wc[x].get('weekly_win_rate', 0), reverse=True):
        data = wc[subset]
        if data.get('n_valid_weeks', 0) > 0:
            print(f"   {subset:15s}: 胜率={data['weekly_win_rate']:.1%} 均准={data['mean_weekly_accuracy']:.1%} "
                  f"std={data['std_weekly_accuracy']:.1%} CV={data['cv']:.2f} ({data['n_valid_weeks']}周)")

    # 个股特征
    print("\n📈 个股特征 — 震荡市准确率:")
    sc = report['stock_characteristics']
    for name in sorted(sc.keys(), key=lambda x: sc[x].get('sideways_accuracy', 0), reverse=True):
        data = sc[name]
        if data.get('sideways_n', 0) >= 20:
            print(f"   {name:20s}: 震荡={data['sideways_accuracy']:.1%}({data['sideways_n']}) | 全部={data['all_accuracy']:.1%}({data['all_n']})")

    print(f"\n⏱️  总耗时: {time.time() - t0:.1f}秒")
    print("=" * 70)

    return report


if __name__ == '__main__':
    run_universal_analysis()
