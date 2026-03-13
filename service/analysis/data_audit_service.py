#!/usr/bin/env python3
"""
v19 综合数据审计与信号剪枝分析 — Service模块

功能：
1. 审计所有DB数据源的可用性和覆盖率
2. 测试美股个股K线（18只半导体龙头）信号
3. 分析当前17个因子中哪些真正有用、哪些应该剔除
4. 测试"剪枝模型"（移除无用因子）是否提升泛化性能
5. 时间序列验证 + 留一日交叉验证
6. 纯统计基线分析（理论天花板）
"""
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

from dao import get_connection

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
# 常量定义
# ═══════════════════════════════════════════════════════════

SECTORS = ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']

FACTOR_NAMES = [
    'reversion', 'rsi', 'kdj', 'macd', 'boll', 'vp', 'fund', 'market',
    'streak', 'trend_bias', 'us_overnight', 'vol_regime', 'momentum_persist',
    'gap_signal', 'intraday_pos', 'db_fund', 'turnover',
]

FACTOR_LABELS = {
    'reversion': '均值回归', 'rsi': 'RSI', 'kdj': 'KDJ', 'macd': 'MACD',
    'boll': 'BOLL', 'vp': '量价背离', 'fund': '资金流API', 'market': '大盘环境',
    'streak': '连续涨跌', 'trend_bias': 'MA趋势', 'us_overnight': '美股隔夜',
    'vol_regime': '波动率', 'momentum_persist': '动量持续', 'gap_signal': '跳空缺口',
    'intraday_pos': '日内位置', 'db_fund': 'DB资金流', 'turnover': '换手率',
}

# v16b 当前阈值配置
CURRENT_THRESHOLDS = {
    '科技': (1.0, -0.5, False),
    '有色金属': (999, -999, True),      # always 上涨
    '汽车': (0.5, -0.5, False),
    '新能源': (0.5, -1.0, True),
    '医药': (0.5, -0.5, False),
    '化工': (999, -1.0, True),           # 上涨 unless combined < -1.0
    '制造': (0.5, -0.5, True),
}

# DB审计表配置: (table_name, date_col, code_col, label)
AUDIT_TABLE_CONFIGS = [
    ('stock_dragon_tiger', 'trade_date', 'stock_code', '龙虎榜'),
    ('stock_order_book', 'trade_date', 'stock_code', '盘口数据'),
    ('stock_time_data', 'trade_date', 'stock_code', '分时数据'),
    ('stock_fund_flow', 'date', 'stock_code', 'DB资金流'),
]


# ═══════════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════════

def parse_chg(s: str) -> float:
    """解析涨跌幅字符串为浮点数"""
    return float(s.replace('%', '').replace('+', ''))


def check_loose(direction: str, actual: float) -> bool:
    """宽松模式判断: 预测上涨且实际>=0 或 预测下跌且实际<=0"""
    if direction == '上涨' and actual >= 0:
        return True
    if direction == '下跌' and actual <= 0:
        return True
    return False


def _enrich_details(details: list[dict]) -> list[dict]:
    """为回测详情数据添加计算字段"""
    for d in details:
        d['_actual'] = parse_chg(d['实际涨跌'])
        d['_ge0'] = d['_actual'] >= 0
        d['_le0'] = d['_actual'] <= 0
        try:
            d['_wd'] = datetime.strptime(d['评分日'], '%Y-%m-%d').weekday()
        except Exception:
            d['_wd'] = -1
    return details


# ═══════════════════════════════════════════════════════════
# 第1部分: DB数据源可用性审计
# ═══════════════════════════════════════════════════════════

def audit_table(
    table_name: str,
    date_col: str,
    code_col: str,
    bt_codes_list: list[str],
    bt_start: str = '2025-12-10',
    bt_end: str = '2026-03-10',
) -> dict:
    """审计单个DB表在回测期间的数据覆盖情况"""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(f'SELECT COUNT(*) FROM {table_name}')
        total_rows = cur.fetchone()[0]

        cur.execute(f'SELECT MIN({date_col}), MAX({date_col}) FROM {table_name}')
        min_d, max_d = cur.fetchone()

        placeholders = ','.join(['%s'] * len(bt_codes_list))
        cur.execute(f'''
            SELECT COUNT(DISTINCT {code_col}), COUNT(*)
            FROM {table_name}
            WHERE {code_col} IN ({placeholders})
            AND {date_col} >= %s AND {date_col} <= %s
        ''', bt_codes_list + [bt_start, bt_end])
        bt_stocks, bt_rows = cur.fetchone()

        return {
            'total_rows': total_rows,
            'date_range': f'{min_d}~{max_d}',
            'bt_stocks': bt_stocks,
            'bt_rows': bt_rows,
            'usable': bt_stocks > 0 and bt_rows > 10,
        }
    except Exception as e:
        return {'error': str(e)}
    finally:
        cur.close()
        conn.close()


def audit_us_stock_kline(bt_start: str = '2025-12-10', bt_end: str = '2026-03-10') -> dict:
    """审计美股个股K线数据覆盖"""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute('''
            SELECT COUNT(*), COUNT(DISTINCT stock_code), MIN(trade_date), MAX(trade_date)
            FROM us_stock_kline WHERE trade_date >= %s AND trade_date <= %s
        ''', (bt_start, bt_end))
        r = cur.fetchone()
        return {
            'total_rows': r[0], 'stock_count': r[1],
            'date_range': f'{r[2]}~{r[3]}', 'usable': (r[0] or 0) > 10,
        }
    finally:
        cur.close()
        conn.close()


def audit_global_index(bt_start: str = '2025-12-10', bt_end: str = '2026-03-10') -> dict:
    """审计全球指数行情数据覆盖"""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute('''
            SELECT COUNT(*), COUNT(DISTINCT index_code), MIN(trade_date), MAX(trade_date)
            FROM global_index_realtime WHERE trade_date >= %s AND trade_date <= %s
        ''', (bt_start, bt_end))
        r = cur.fetchone()
        return {
            'total_rows': r[0] or 0, 'index_count': r[1] or 0,
            'date_range': f'{r[2] or "N/A"}~{r[3] or "N/A"}',
            'usable': (r[0] or 0) > 10,
        }
    finally:
        cur.close()
        conn.close()


def run_db_audit(bt_codes: list[str], bt_start: str = '2025-12-10', bt_end: str = '2026-03-10') -> dict:
    """运行完整的DB数据源审计，返回所有表的审计结果"""
    results = {}
    for table, date_col, code_col, label in AUDIT_TABLE_CONFIGS:
        results[label] = audit_table(table, date_col, code_col, bt_codes, bt_start, bt_end)

    results['美股个股K线'] = audit_us_stock_kline(bt_start, bt_end)
    results['全球指数行情'] = audit_global_index(bt_start, bt_end)
    return results


# ═══════════════════════════════════════════════════════════
# 第2部分: 美股半导体个股K线信号
# ═══════════════════════════════════════════════════════════

def load_us_kline_map(start_date: str = '2025-10-01', end_date: str = '2026-03-10') -> dict:
    """从DB加载美股个股K线数据，返回 {stock_code: [kline_dict, ...]}"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT stock_code, trade_date, close_price, change_pct, volume
        FROM us_stock_kline WHERE trade_date >= %s AND trade_date <= %s
        ORDER BY stock_code, trade_date
    ''', (start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    kline_map = defaultdict(list)
    for r in rows:
        kline_map[r[0]].append({
            'date': str(r[1]),
            'close': float(r[2] or 0),
            'chg_pct': float(r[3] or 0),
            'volume': int(r[4] or 0),
        })
    return dict(kline_map)


def get_us_semi_signal(
    a_date_str: str,
    us_kline_map: dict,
    lookback: int = 7,
) -> Optional[dict]:
    """
    计算A股某日对应的美股半导体个股信号。

    查找a_date_str之前最近有数据的美股交易日，
    计算平均涨跌幅和上涨比例，生成信号值。
    """
    dt = datetime.strptime(a_date_str, '%Y-%m-%d')
    for offset in range(1, lookback + 7):
        prev_date = (dt - timedelta(days=offset)).strftime('%Y-%m-%d')
        changes = []
        for code, klines in us_kline_map.items():
            for k in klines:
                if k['date'] == prev_date and k['chg_pct'] != 0:
                    changes.append(k['chg_pct'])
                    break
        if len(changes) >= 5:
            avg_chg = sum(changes) / len(changes)
            up_ratio = sum(1 for c in changes if c > 0) / len(changes)
            signal = 0.0
            if avg_chg > 2.0:
                signal = 2.0
            elif avg_chg > 1.0:
                signal = 1.0
            elif avg_chg > 0.3:
                signal = 0.5
            elif avg_chg < -2.0:
                signal = -2.0
            elif avg_chg < -1.0:
                signal = -1.0
            elif avg_chg < -0.3:
                signal = -0.5
            if up_ratio > 0.85 and avg_chg > 0.5:
                signal += 0.5
            elif up_ratio < 0.15 and avg_chg < -0.5:
                signal -= 0.5
            return {
                'avg_chg': avg_chg, 'signal': signal,
                'up_ratio': up_ratio, 'n': len(changes),
            }
    return None


def compute_us_semi_signals(details: list[dict], us_kline_map: dict) -> dict:
    """为所有回测日期计算美股半导体信号，返回 {date_str: signal_dict}"""
    signals = {}
    for d in details:
        date = d['评分日']
        if date not in signals:
            signals[date] = get_us_semi_signal(date, us_kline_map)
    return signals


# ═══════════════════════════════════════════════════════════
# 第3部分: 因子有效性审计
# ═══════════════════════════════════════════════════════════

def analyze_factor_effectiveness(
    factor_analysis: dict,
    sector_factor_weights: dict,
) -> dict:
    """
    分析各板块因子的有效性，返回分类结果。

    返回: {sector: {'effective': [...], 'noise': [...], 'reversal': [...]}}
    """
    results = {}
    for sector in SECTORS:
        sec_fa = factor_analysis.get(sector, {})
        sec_w = sector_factor_weights.get(sector, {})

        effective, noise, reversal = [], [], []
        for fname in FACTOR_NAMES:
            fa = sec_fa.get(fname, {})
            rate_str = fa.get('方向一致率', '0%')
            rate = float(rate_str.replace('%', '')) if rate_str != '0%' else 0
            n = fa.get('样本数', 0)
            w = sec_w.get(fname, 0)

            if n < 10:
                continue
            elif rate > 55:
                effective.append((fname, rate, w))
            elif rate < 45:
                reversal.append((fname, rate, w))
            else:
                if w != 0:
                    noise.append((fname, rate, w))

        results[sector] = {
            'effective': effective,
            'noise': noise,
            'reversal': reversal,
        }
    return results


# ═══════════════════════════════════════════════════════════
# 第4部分: 因子剪枝模拟（阈值搜索）
# ═══════════════════════════════════════════════════════════

def simulate_with_thresholds(
    d: dict,
    bullish_th: float,
    bearish_th: float,
    default_up: bool,
) -> str:
    """用给定阈值模拟方向决策（含星期效应和低分规则）"""
    sector = d['板块']
    combined = d['融合信号']
    confidence = d['置信度']
    score = d['评分']

    if combined > bullish_th:
        direction = '上涨'
    elif combined < bearish_th:
        direction = '下跌'
    else:
        direction = '上涨' if default_up else '下跌'

    # 星期效应
    try:
        wd = datetime.strptime(d['评分日'], '%Y-%m-%d').weekday()
        if sector == '医药' and wd == 4:
            direction = '下跌'
        elif sector == '汽车' and wd == 1 and confidence != 'high':
            direction = '下跌'
        elif sector == '汽车' and wd == 2 and confidence != 'high':
            direction = '下跌'
        elif sector == '有色金属' and wd == 2 and confidence != 'high':
            direction = '下跌'
        elif sector == '科技' and wd == 2 and confidence != 'high':
            direction = '下跌'
        elif sector == '新能源' and wd == 4 and confidence != 'high':
            direction = '下跌'
        elif sector == '新能源' and wd == 0 and confidence != 'high':
            direction = '下跌'
        elif sector == '制造' and wd == 4 and confidence != 'high':
            direction = '下跌'
        elif sector == '化工' and wd == 2 and confidence == 'low':
            direction = '下跌'
        elif sector == '有色金属' and wd == 1:
            direction = '上涨'
        elif sector == '有色金属' and wd == 4:
            direction = '上涨'
        elif sector == '化工' and wd == 4:
            direction = '上涨'
        elif sector == '化工' and wd == 1 and confidence != 'high':
            direction = '上涨'
    except Exception:
        pass

    if sector == '汽车' and score < 35:
        direction = '上涨'
    elif sector == '有色金属' and score < 35:
        direction = '上涨'
    elif sector == '科技' and score < 35:
        direction = '上涨'

    return direction


def simulate_enhanced(
    d: dict,
    bull_th: float,
    bear_th: float,
    def_up: bool,
    us_semi_signals: dict,
    us_weight: float = 0.0,
) -> str:
    """增强模拟：在combined信号上叠加美股半导体信号后决策"""
    sector = d['板块']
    combined = d['融合信号']
    confidence = d['置信度']
    score = d['评分']

    us_semi = us_semi_signals.get(d['评分日'])
    if us_semi and us_weight != 0:
        combined = combined + us_semi['signal'] * us_weight

    if combined > bull_th:
        direction = '上涨'
    elif combined < bear_th:
        direction = '下跌'
    else:
        direction = '上涨' if def_up else '下跌'

    # 星期效应（与 simulate_with_thresholds 相同）
    try:
        wd = datetime.strptime(d['评分日'], '%Y-%m-%d').weekday()
        if sector == '医药' and wd == 4:
            direction = '下跌'
        elif sector == '汽车' and wd == 1 and confidence != 'high':
            direction = '下跌'
        elif sector == '汽车' and wd == 2 and confidence != 'high':
            direction = '下跌'
        elif sector == '有色金属' and wd == 2 and confidence != 'high':
            direction = '下跌'
        elif sector == '科技' and wd == 2 and confidence != 'high':
            direction = '下跌'
        elif sector == '新能源' and wd == 4 and confidence != 'high':
            direction = '下跌'
        elif sector == '新能源' and wd == 0 and confidence != 'high':
            direction = '下跌'
        elif sector == '制造' and wd == 4 and confidence != 'high':
            direction = '下跌'
        elif sector == '化工' and wd == 2 and confidence == 'low':
            direction = '下跌'
        elif sector == '有色金属' and wd == 1:
            direction = '上涨'
        elif sector == '有色金属' and wd == 4:
            direction = '上涨'
        elif sector == '化工' and wd == 4:
            direction = '上涨'
        elif sector == '化工' and wd == 1 and confidence != 'high':
            direction = '上涨'
    except Exception:
        pass

    if sector == '汽车' and score < 35:
        direction = '上涨'
    elif sector == '有色金属' and score < 35:
        direction = '上涨'
    elif sector == '科技' and score < 35:
        direction = '上涨'

    return direction


def search_optimal_thresholds(
    details: list[dict],
    first_half_dates: set[str],
    second_half_dates: set[str],
) -> dict:
    """
    对每个板块进行阈值网格搜索（前半训练→后半测试）。

    返回: {sector: (bull_th, bear_th, default_up, first_ok, second_ok)}
    """
    bull_grid = [-0.5, 0.0, 0.3, 0.5, 0.8, 1.0, 1.5, 2.0, 999]
    bear_grid = [-2.0, -1.5, -1.0, -0.5, -0.3, 0.0, -999]

    best_configs = {}
    for sector in SECTORS:
        sd = [d for d in details if d['板块'] == sector]
        sd_first = [d for d in sd if d['评分日'] in first_half_dates]
        sd_second = [d for d in sd if d['评分日'] in second_half_dates]

        if not sd_first or not sd_second:
            continue

        best_first = 0
        best_second = 0
        best_params = None

        for bull_th in bull_grid:
            for bear_th in bear_grid:
                for def_up in [True, False]:
                    first_ok = sum(
                        1 for d in sd_first
                        if check_loose(simulate_with_thresholds(d, bull_th, bear_th, def_up), d['_actual'])
                    )
                    first_rate = first_ok / len(sd_first) * 100

                    second_ok = sum(
                        1 for d in sd_second
                        if check_loose(simulate_with_thresholds(d, bull_th, bear_th, def_up), d['_actual'])
                    )
                    second_rate = second_ok / len(sd_second) * 100

                    if first_rate > best_first or (first_rate == best_first and second_rate > best_second):
                        best_first = first_rate
                        best_second = second_rate
                        best_params = (bull_th, bear_th, def_up, first_ok, second_ok)

        if best_params:
            best_configs[sector] = best_params

    return best_configs


# ═══════════════════════════════════════════════════════════
# 第5部分: 美股半导体信号增强搜索
# ═══════════════════════════════════════════════════════════

def search_us_semi_weight(
    details: list[dict],
    us_semi_signals: dict,
    first_half_dates: set[str],
    second_half_dates: set[str],
    us_weights: list[float] = None,
) -> list[dict]:
    """
    搜索不同美股半导体权重下的准确率。

    返回: [{weight, total_rate, first_rate, second_rate, diff_vs_baseline}, ...]
    """
    if us_weights is None:
        us_weights = [0.0, 0.1, 0.2, 0.3, -0.1, -0.2, -0.3]

    total = len(details)
    baseline_ok = sum(1 for d in details if d['宽松正确'] == '✓')
    baseline_rate = baseline_ok / total * 100

    results = []
    for us_w in us_weights:
        total_ok = 0
        first_ok, second_ok = 0, 0
        first_n, second_n = 0, 0

        for sector in SECTORS:
            sd = [d for d in details if d['板块'] == sector]
            bt, brt, du = CURRENT_THRESHOLDS[sector]

            for d in sd:
                direction = simulate_enhanced(d, bt, brt, du, us_semi_signals, us_w)
                ok = check_loose(direction, d['_actual'])
                if ok:
                    total_ok += 1
                if d['评分日'] in first_half_dates:
                    first_n += 1
                    if ok:
                        first_ok += 1
                else:
                    second_n += 1
                    if ok:
                        second_ok += 1

        rate = total_ok / total * 100
        fr = first_ok / first_n * 100 if first_n > 0 else 0
        sr = second_ok / second_n * 100 if second_n > 0 else 0

        results.append({
            'weight': us_w,
            'total_rate': rate,
            'first_rate': fr,
            'second_rate': sr,
            'diff_vs_baseline': rate - baseline_rate,
        })

    return results


# ═══════════════════════════════════════════════════════════
# 第6部分: 留一日交叉验证
# ═══════════════════════════════════════════════════════════

def leave_one_day_cv(
    details: list[dict],
    all_dates: list[str],
    best_configs: dict,
) -> dict:
    """
    留一日交叉验证，比较当前模型和优化阈值模型。

    返回: {orig_mean, orig_std, opt_mean, opt_std, diff}
    """
    date_accuracy = {}
    for date in all_dates:
        dd = [d for d in details if d['评分日'] == date]
        if not dd:
            continue
        orig_ok = sum(1 for d in dd if d['宽松正确'] == '✓')

        opt_ok = 0
        for d in dd:
            sector = d['板块']
            if sector in best_configs:
                bt, brt, du, _, _ = best_configs[sector]
            else:
                bt, brt, du = CURRENT_THRESHOLDS.get(sector, (0.5, -0.5, False))
            direction = simulate_with_thresholds(d, bt, brt, du)
            if check_loose(direction, d['_actual']):
                opt_ok += 1

        date_accuracy[date] = (orig_ok, opt_ok, len(dd))

    orig_cv, opt_cv = [], []
    for leave in all_dates:
        o_rest = sum(v[0] for d, v in date_accuracy.items() if d != leave)
        p_rest = sum(v[1] for d, v in date_accuracy.items() if d != leave)
        n_rest = sum(v[2] for d, v in date_accuracy.items() if d != leave)
        if n_rest > 0:
            orig_cv.append(o_rest / n_rest * 100)
            opt_cv.append(p_rest / n_rest * 100)

    orig_mean = sum(orig_cv) / len(orig_cv) if orig_cv else 0
    opt_mean = sum(opt_cv) / len(opt_cv) if opt_cv else 0
    orig_std = (sum((r - orig_mean) ** 2 for r in orig_cv) / len(orig_cv)) ** 0.5 if orig_cv else 0
    opt_std = (sum((r - opt_mean) ** 2 for r in opt_cv) / len(opt_cv)) ** 0.5 if opt_cv else 0

    return {
        'orig_mean': orig_mean, 'orig_std': orig_std,
        'opt_mean': opt_mean, 'opt_std': opt_std,
        'diff': opt_mean - orig_mean,
    }


# ═══════════════════════════════════════════════════════════
# 第7部分: 纯统计基线分析
# ═══════════════════════════════════════════════════════════

def compute_baselines(
    details: list[dict],
    all_dates: list[str],
    first_half_dates: set[str],
    second_half_dates: set[str],
) -> dict:
    """
    计算各种统计基线（理论天花板）。

    返回: {
        sector_fixed: {sector: {all_up, all_dn, best, current}},
        daily_best, sector_daily_best, total,
        base_rate_shift: {sector: {first_up, second_up, diff}},
    }
    """
    total = len(details)

    # 板块固定方向基线
    sector_fixed = {}
    total_best_fixed = 0
    for sector in SECTORS:
        sd = [d for d in details if d['板块'] == sector]
        all_up = sum(1 for d in sd if d['_ge0'])
        all_dn = sum(1 for d in sd if d['_le0'])
        best = max(all_up, all_dn)
        cur = sum(1 for d in sd if d['宽松正确'] == '✓')
        total_best_fixed += best
        sector_fixed[sector] = {
            'all_up': all_up / len(sd) * 100,
            'all_dn': all_dn / len(sd) * 100,
            'best': best / len(sd) * 100,
            'current': cur / len(sd) * 100,
            'n': len(sd),
        }

    # 每日最优固定方向
    daily_best = 0
    for date in all_dates:
        dd = [d for d in details if d['评分日'] == date]
        up_ok = sum(1 for d in dd if d['_ge0'])
        dn_ok = sum(1 for d in dd if d['_le0'])
        daily_best += max(up_ok, dn_ok)

    # 板块×日最优（理论上限）
    sector_daily_best = 0
    for sector in SECTORS:
        for date in all_dates:
            dd = [d for d in details if d['板块'] == sector and d['评分日'] == date]
            if not dd:
                continue
            up_ok = sum(1 for d in dd if d['_ge0'])
            dn_ok = sum(1 for d in dd if d['_le0'])
            sector_daily_best += max(up_ok, dn_ok)

    # 前半 vs 后半涨跌基准率变化
    base_rate_shift = {}
    for sector in SECTORS:
        sd = [d for d in details if d['板块'] == sector]
        f_data = [d for d in sd if d['评分日'] in first_half_dates]
        s_data = [d for d in sd if d['评分日'] in second_half_dates]
        f_up = sum(1 for d in f_data if d['_ge0']) / len(f_data) * 100 if f_data else 0
        s_up = sum(1 for d in s_data if d['_ge0']) / len(s_data) * 100 if s_data else 0
        base_rate_shift[sector] = {
            'first_up': f_up, 'second_up': s_up, 'diff': f_up - s_up,
        }

    return {
        'sector_fixed': sector_fixed,
        'total_best_fixed': total_best_fixed / total * 100,
        'daily_best': daily_best / total * 100,
        'sector_daily_best': sector_daily_best / total * 100,
        'total': total,
        'daily_best_raw': daily_best,
        'sector_daily_best_raw': sector_daily_best,
        'base_rate_shift': base_rate_shift,
    }


# ═══════════════════════════════════════════════════════════
# 第2部分补充: 美股半导体信号方向分析
# ═══════════════════════════════════════════════════════════

def analyze_us_semi_direction(
    details: list[dict],
    us_semi_signals: dict,
    first_half_dates: set[str],
    second_half_dates: set[str],
) -> dict:
    """
    分析美股半导体信号对各板块的方向预测力和时间稳定性。

    返回: {
        direction: {sector: {pos_up, neg_dn, strong_pos_up, strong_neg_dn, dir_rate}},
        stability: {sector: {first_rate, second_rate, diff}},
        reversal: {sector: {pos_dn, neg_up, rev_rate}},
    }
    """
    direction_results = {}
    stability_results = {}
    reversal_results = {}

    for sector in SECTORS:
        sd = [d for d in details if d['板块'] == sector]

        # 方向预测力
        pos = [d for d in sd if us_semi_signals.get(d['评分日']) and us_semi_signals[d['评分日']]['signal'] > 0]
        neg = [d for d in sd if us_semi_signals.get(d['评分日']) and us_semi_signals[d['评分日']]['signal'] < 0]
        sp = [d for d in sd if us_semi_signals.get(d['评分日']) and us_semi_signals[d['评分日']]['signal'] > 1.0]
        sn = [d for d in sd if us_semi_signals.get(d['评分日']) and us_semi_signals[d['评分日']]['signal'] < -1.0]

        pos_up = sum(1 for d in pos if d['_ge0']) / len(pos) * 100 if pos else 0
        neg_dn = sum(1 for d in neg if d['_le0']) / len(neg) * 100 if neg else 0
        sp_up = sum(1 for d in sp if d['_ge0']) / len(sp) * 100 if sp else 0
        sn_dn = sum(1 for d in sn if d['_le0']) / len(sn) * 100 if sn else 0

        all_sig = [d for d in sd if us_semi_signals.get(d['评分日']) and us_semi_signals[d['评分日']]['signal'] != 0]
        dir_ok = sum(1 for d in all_sig if
                     (us_semi_signals[d['评分日']]['signal'] > 0 and d['_ge0']) or
                     (us_semi_signals[d['评分日']]['signal'] < 0 and d['_le0']))
        dir_rate = dir_ok / len(all_sig) * 100 if all_sig else 0

        direction_results[sector] = {
            'pos_up': pos_up, 'pos_n': len(pos),
            'neg_dn': neg_dn, 'neg_n': len(neg),
            'strong_pos_up': sp_up, 'strong_pos_n': len(sp),
            'strong_neg_dn': sn_dn, 'strong_neg_n': len(sn),
            'dir_rate': dir_rate, 'dir_n': len(all_sig),
        }

        # 前半 vs 后半稳定性
        first_sig = [d for d in all_sig if d['评分日'] in first_half_dates]
        second_sig = [d for d in all_sig if d['评分日'] in second_half_dates]
        f_ok = sum(1 for d in first_sig if
                   (us_semi_signals[d['评分日']]['signal'] > 0 and d['_ge0']) or
                   (us_semi_signals[d['评分日']]['signal'] < 0 and d['_le0']))
        s_ok = sum(1 for d in second_sig if
                   (us_semi_signals[d['评分日']]['signal'] > 0 and d['_ge0']) or
                   (us_semi_signals[d['评分日']]['signal'] < 0 and d['_le0']))
        fr = f_ok / len(first_sig) * 100 if first_sig else 0
        sr = s_ok / len(second_sig) * 100 if second_sig else 0
        stability_results[sector] = {
            'first_rate': fr, 'first_n': len(first_sig),
            'second_rate': sr, 'second_n': len(second_sig),
            'diff': fr - sr,
        }

        # 反转测试
        pos_dn = sum(1 for d in pos if d['_le0']) / len(pos) * 100 if pos else 0
        neg_up = sum(1 for d in neg if d['_ge0']) / len(neg) * 100 if neg else 0
        rev_ok = sum(1 for d in all_sig if
                     (us_semi_signals[d['评分日']]['signal'] > 0 and d['_le0']) or
                     (us_semi_signals[d['评分日']]['signal'] < 0 and d['_ge0']))
        rev_rate = rev_ok / len(all_sig) * 100 if all_sig else 0
        reversal_results[sector] = {
            'pos_dn': pos_dn, 'neg_up': neg_up, 'rev_rate': rev_rate,
        }

    return {
        'direction': direction_results,
        'stability': stability_results,
        'reversal': reversal_results,
    }


# ═══════════════════════════════════════════════════════════
# 主入口: 运行完整数据审计
# ═══════════════════════════════════════════════════════════

def run_data_audit(
    backtest_result_path: str = 'data_results/backtest_prediction_enhanced_v9_50stocks_result.json',
) -> dict:
    """
    运行完整的v19数据审计分析（8个维度），返回所有分析结果。

    参数:
        backtest_result_path: 回测结果JSON文件路径

    返回: 包含所有分析维度结果的字典
    """
    from service.backtest.prediction_enhanced_backtest import (
        _SECTOR_FACTOR_WEIGHTS,
    )

    # 加载回测数据
    with open(backtest_result_path) as f:
        bt_data = json.load(f)

    details = _enrich_details(bt_data['逐日详情'])
    total = len(details)
    loose_ok = sum(1 for d in details if d['宽松正确'] == '✓')

    all_dates = sorted(set(d['评分日'] for d in details))
    bt_codes = sorted(set(d['代码'] for d in details))

    # 时间分割
    mid_idx = len(all_dates) // 2
    first_half_dates = set(all_dates[:mid_idx])
    second_half_dates = set(all_dates[mid_idx:])

    print(f"{'=' * 80}")
    print(f"v19 综合数据审计与信号剪枝分析")
    print(f"{'=' * 80}")
    print(f"当前基线: {loose_ok}/{total} ({loose_ok / total * 100:.1f}%)")
    print(f"目标65%: {int(total * 0.65)}/{total}, 差距: {int(total * 0.65) - loose_ok}个样本")
    print(f"时间分割: 前半{len(first_half_dates)}天, 后半{len(second_half_dates)}天\n")

    # ── 第1部分: DB审计 ──
    print(f"{'=' * 80}")
    print(f"第1部分: DB数据源可用性审计")
    print(f"{'=' * 80}")
    db_audit = run_db_audit(bt_codes)
    for label, r in db_audit.items():
        if 'error' in r:
            print(f"  {label}: ERROR - {r['error']}")
        else:
            usable = '✅' if r.get('usable') else '❌'
            print(f"  {label}: {usable} (rows={r.get('total_rows', r.get('total_rows', 0))})")

    # ── 第2部分: 美股半导体信号 ──
    print(f"\n{'=' * 80}")
    print(f"第2部分: 美股半导体个股K线信号分析")
    print(f"{'=' * 80}")
    us_kline_map = load_us_kline_map()
    print(f"美股个股: {len(us_kline_map)}只")
    us_semi_signals = compute_us_semi_signals(details, us_kline_map)
    valid_us = sum(1 for v in us_semi_signals.values() if v is not None)
    print(f"有效信号: {valid_us}/{len(us_semi_signals)} 天")

    us_analysis = analyze_us_semi_direction(
        details, us_semi_signals, first_half_dates, second_half_dates,
    )
    _print_us_semi_analysis(us_analysis)

    # ── 第3部分: 因子有效性 ──
    print(f"\n{'=' * 80}")
    print(f"第3部分: 已有因子有效性审计")
    print(f"{'=' * 80}")
    factor_analysis = bt_data.get('因子有效性分析(按板块)', {})
    factor_eff = analyze_factor_effectiveness(factor_analysis, _SECTOR_FACTOR_WEIGHTS)
    _print_factor_effectiveness(factor_eff, factor_analysis, _SECTOR_FACTOR_WEIGHTS)

    # ── 第4部分: 阈值搜索 ──
    print(f"\n{'=' * 80}")
    print(f"第4部分: 因子剪枝模拟（阈值搜索）")
    print(f"{'=' * 80}")
    best_configs = search_optimal_thresholds(details, first_half_dates, second_half_dates)
    _print_threshold_search(details, best_configs, first_half_dates, second_half_dates)

    # ── 第5部分: 美股半导体增强 ──
    print(f"\n{'=' * 80}")
    print(f"第5部分: 美股半导体信号增强 + 阈值优化")
    print(f"{'=' * 80}")
    us_weight_results = search_us_semi_weight(
        details, us_semi_signals, first_half_dates, second_half_dates,
    )
    _print_us_weight_search(us_weight_results)

    # ── 第6部分: LOO-CV ──
    print(f"\n{'=' * 80}")
    print(f"第6部分: 留一日交叉验证")
    print(f"{'=' * 80}")
    cv_result = leave_one_day_cv(details, all_dates, best_configs)
    print(f"  当前模型: {cv_result['orig_mean']:.2f}% ± {cv_result['orig_std']:.2f}%")
    print(f"  优化阈值: {cv_result['opt_mean']:.2f}% ± {cv_result['opt_std']:.2f}%")
    print(f"  差异: {cv_result['diff']:+.2f}pp")

    # ── 第7部分: 统计基线 ──
    print(f"\n{'=' * 80}")
    print(f"第7部分: 纯统计基线分析（理论天花板）")
    print(f"{'=' * 80}")
    baselines = compute_baselines(details, all_dates, first_half_dates, second_half_dates)
    _print_baselines(baselines)

    # ── 第8部分: 综合结论 ──
    print(f"\n{'=' * 80}")
    print(f"第8部分: 综合结论")
    print(f"{'=' * 80}")
    print(f"当前模型准确率: {loose_ok}/{total} ({loose_ok / total * 100:.1f}%)")
    print(f"LOO-CV: 当前{cv_result['orig_mean']:.2f}% vs 优化{cv_result['opt_mean']:.2f}%")
    print(f"板块×日最优理论上限: {baselines['sector_daily_best']:.1f}%")
    print(f"{'=' * 80}")

    return {
        'baseline': {'loose_ok': loose_ok, 'total': total},
        'db_audit': db_audit,
        'us_semi_signals': us_semi_signals,
        'us_analysis': us_analysis,
        'factor_effectiveness': factor_eff,
        'best_configs': best_configs,
        'us_weight_results': us_weight_results,
        'cv_result': cv_result,
        'baselines': baselines,
    }


# ═══════════════════════════════════════════════════════════
# 打印辅助函数
# ═══════════════════════════════════════════════════════════

def _print_us_semi_analysis(us_analysis: dict):
    """打印美股半导体信号分析结果"""
    print(f"\n维度2a: 美股半导体信号 vs A股次日方向")
    print(f"{'板块':<10} {'信号>0→涨':>14} {'信号<0→跌':>14} {'方向一致率':>12}")
    print('-' * 55)
    for sector in SECTORS:
        d = us_analysis['direction'][sector]
        print(f"{sector:<10} {d['pos_up']:>5.1f}%({d['pos_n']:>3}) "
              f"{d['neg_dn']:>5.1f}%({d['neg_n']:>3}) "
              f"{d['dir_rate']:>5.1f}%({d['dir_n']:>3})")

    print(f"\n维度2b: 前半 vs 后半稳定性")
    print(f"{'板块':<10} {'前半一致率':>12} {'后半一致率':>12} {'差异':>8}")
    print('-' * 50)
    for sector in SECTORS:
        s = us_analysis['stability'][sector]
        print(f"{sector:<10} {s['first_rate']:>5.1f}%({s['first_n']:>3}) "
              f"{s['second_rate']:>5.1f}%({s['second_n']:>3}) {s['diff']:>+6.1f}pp")

    print(f"\n维度2c: 反转测试")
    for sector in SECTORS:
        r = us_analysis['reversal'][sector]
        print(f"  {sector}: 反转一致率 {r['rev_rate']:.1f}%")


def _print_factor_effectiveness(factor_eff: dict, factor_analysis: dict, sector_weights: dict):
    """打印因子有效性审计结果"""
    for sector in SECTORS:
        eff = factor_eff[sector]
        print(f"\n{'─' * 60}")
        print(f"板块: {sector}")
        print(f"  有效因子: {', '.join(FACTOR_LABELS.get(f, f) for f, _, _ in eff['effective']) or '无'}")
        print(f"  噪声因子: {', '.join(FACTOR_LABELS.get(f, f) + f'(w={w})' for f, _, w in eff['noise']) or '无'}")
        print(f"  反转因子: {', '.join(FACTOR_LABELS.get(f, f) for f, _, _ in eff['reversal']) or '无'}")


def _print_threshold_search(details, best_configs, first_half_dates, second_half_dates):
    """打印阈值搜索结果"""
    for sector in SECTORS:
        sd = [d for d in details if d['板块'] == sector]
        sd_first = [d for d in sd if d['评分日'] in first_half_dates]
        sd_second = [d for d in sd if d['评分日'] in second_half_dates]
        cur_ok = sum(1 for d in sd if d['宽松正确'] == '✓')

        if sector in best_configs:
            bt, brt, du, fo, so = best_configs[sector]
            total_ok = fo + so
            print(f"  {sector}: bull>{bt}, bear<{brt}, default={'涨' if du else '跌'} "
                  f"→ {total_ok / len(sd) * 100:.1f}% (当前{cur_ok / len(sd) * 100:.1f}%)")


def _print_us_weight_search(results: list[dict]):
    """打印美股半导体权重搜索结果"""
    print(f"{'权重':>6} {'总准确率':>10} {'前半':>8} {'后半':>8} {'vs当前':>8}")
    print('-' * 50)
    for r in results:
        print(f"{r['weight']:>6.1f} {r['total_rate']:>8.1f}% {r['first_rate']:>6.1f}% "
              f"{r['second_rate']:>6.1f}% {r['diff_vs_baseline']:>+6.1f}pp")


def _print_baselines(baselines: dict):
    """打印统计基线分析结果"""
    print(f"\n板块固定方向基线:")
    print(f"{'板块':<10} {'全涨':>8} {'全跌':>8} {'最优':>8} {'当前':>8}")
    print('-' * 50)
    for sector in SECTORS:
        sf = baselines['sector_fixed'][sector]
        print(f"{sector:<10} {sf['all_up']:>6.1f}% {sf['all_dn']:>6.1f}% "
              f"{sf['best']:>6.1f}% {sf['current']:>6.1f}%")

    print(f"\n总计最优固定: {baselines['total_best_fixed']:.1f}%")
    print(f"每日最优: {baselines['daily_best']:.1f}%")
    print(f"板块×日最优(理论上限): {baselines['sector_daily_best']:.1f}%")

    print(f"\n前半 vs 后半涨跌基准率变化:")
    for sector in SECTORS:
        br = baselines['base_rate_shift'][sector]
        print(f"  {sector}: 前半{br['first_up']:.1f}% → 后半{br['second_up']:.1f}% ({br['diff']:+.1f}pp)")
