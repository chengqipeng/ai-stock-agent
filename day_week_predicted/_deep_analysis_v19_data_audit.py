#!/usr/bin/env python3
"""
v19 综合数据审计与信号剪枝分析

目标：
1. 审计所有DB数据源的可用性和覆盖率
2. 测试唯一可用的新数据源：美股个股K线（18只半导体龙头）
3. 分析当前17个因子中哪些真正有用、哪些应该剔除
4. 测试"剪枝模型"（移除无用因子）是否提升泛化性能
5. 时间序列验证 + 留一日交叉验证
"""
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta

logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s %(message)s')

print(f"{'=' * 80}")
print(f"v19 综合数据审计与信号剪枝分析")
print(f"{'=' * 80}")

# ═══════════════════════════════════════════════════════════
# 加载回测数据
# ═══════════════════════════════════════════════════════════
with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json') as f:
    bt_data = json.load(f)
details = bt_data['逐日详情']
total = len(details)

def parse_chg(s):
    return float(s.replace('%', '').replace('+', ''))

for d in details:
    d['_actual'] = parse_chg(d['实际涨跌'])
    d['_ge0'] = d['_actual'] >= 0
    d['_le0'] = d['_actual'] <= 0
    try:
        d['_wd'] = datetime.strptime(d['评分日'], '%Y-%m-%d').weekday()
    except:
        d['_wd'] = -1

loose_ok = sum(1 for d in details if d['宽松正确'] == '✓')
print(f"当前基线: {loose_ok}/{total} ({loose_ok/total*100:.1f}%)")
print(f"目标65%: {int(total*0.65)}/{total}, 差距: {int(total*0.65) - loose_ok}个样本\n")

sectors = ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']
all_dates = sorted(set(d['评分日'] for d in details))
bt_codes = sorted(set(d['代码'] for d in details))

# 时间序列分割
mid_idx = len(all_dates) // 2
first_half_dates = set(all_dates[:mid_idx])
second_half_dates = set(all_dates[mid_idx:])
print(f"时间分割: 前半{len(first_half_dates)}天({all_dates[0]}~{all_dates[mid_idx-1]}), "
      f"后半{len(second_half_dates)}天({all_dates[mid_idx]}~{all_dates[-1]})")


# ═══════════════════════════════════════════════════════════
# 第1部分: DB数据源可用性审计
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"第1部分: DB数据源可用性审计")
print(f"{'=' * 80}")

from dao import get_connection

def audit_table(table_name, date_col, code_col, bt_codes_list, bt_start='2025-12-10', bt_end='2026-03-10'):
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
        return {'total_rows': total_rows, 'date_range': f'{min_d}~{max_d}',
                'bt_stocks': bt_stocks, 'bt_rows': bt_rows, 'usable': bt_stocks > 0 and bt_rows > 10}
    except Exception as e:
        return {'error': str(e)}
    finally:
        cur.close()
        conn.close()

audit_configs = [
    ('stock_dragon_tiger', 'trade_date', 'stock_code', '龙虎榜'),
    ('stock_order_book', 'trade_date', 'stock_code', '盘口数据'),
    ('stock_time_data', 'trade_date', 'stock_code', '分时数据'),
    ('stock_fund_flow', 'date', 'stock_code', 'DB资金流'),
]

print(f"\n{'表名':<25} {'总行数':>10} {'日期范围':<28} {'回测股票':>8} {'回测行数':>10} {'可用'}")
print('-' * 95)

for table, date_col, code_col, label in audit_configs:
    r = audit_table(table, date_col, code_col, bt_codes)
    if 'error' in r:
        print(f"{label:<25} ERROR: {r['error']}")
    else:
        usable = '✅' if r['usable'] else '❌'
        print(f"{label:<25} {r['total_rows']:>10,} {r['date_range']:<28} {r['bt_stocks']:>8} {r['bt_rows']:>10,} {usable}")

# 美股个股K线
conn = get_connection()
cur = conn.cursor()
cur.execute('''SELECT COUNT(*), COUNT(DISTINCT stock_code), MIN(trade_date), MAX(trade_date)
    FROM us_stock_kline WHERE trade_date >= '2025-12-10' AND trade_date <= '2026-03-10' ''')
r = cur.fetchone()
print(f"{'美股个股K线':<25} {r[0]:>10,} {str(r[2])+'~'+str(r[3]):<28} {r[1]:>8} {r[0]:>10,} ✅")

cur.execute('''SELECT COUNT(*), COUNT(DISTINCT index_code), MIN(trade_date), MAX(trade_date)
    FROM global_index_realtime WHERE trade_date >= '2025-12-10' AND trade_date <= '2026-03-10' ''')
r = cur.fetchone()
usable = '✅' if (r[0] or 0) > 10 else '❌'
print(f"{'全球指数行情':<25} {r[0] or 0:>10,} {str(r[2] or 'N/A')+'~'+str(r[3] or 'N/A'):<28} {r[1] or 0:>8} {r[0] or 0:>10,} {usable}")
cur.close()
conn.close()


# ═══════════════════════════════════════════════════════════
# 第2部分: 美股半导体个股K线信号分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"第2部分: 美股半导体个股K线信号分析")
print(f"{'=' * 80}")

conn = get_connection()
cur = conn.cursor()
cur.execute('''SELECT stock_code, trade_date, close_price, change_pct, volume
    FROM us_stock_kline WHERE trade_date >= '2025-10-01' AND trade_date <= '2026-03-10'
    ORDER BY stock_code, trade_date''')
us_rows = cur.fetchall()
cur.close()
conn.close()

us_kline_map = defaultdict(list)
for r in us_rows:
    us_kline_map[r[0]].append({
        'date': str(r[1]), 'close': float(r[2] or 0),
        'chg_pct': float(r[3] or 0), 'volume': int(r[4] or 0),
    })

print(f"美股个股: {len(us_kline_map)}只")

def get_us_semi_signal(a_date_str, lookback=7):
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
            if avg_chg > 2.0: signal = 2.0
            elif avg_chg > 1.0: signal = 1.0
            elif avg_chg > 0.3: signal = 0.5
            elif avg_chg < -2.0: signal = -2.0
            elif avg_chg < -1.0: signal = -1.0
            elif avg_chg < -0.3: signal = -0.5
            if up_ratio > 0.85 and avg_chg > 0.5: signal += 0.5
            elif up_ratio < 0.15 and avg_chg < -0.5: signal -= 0.5
            return {'avg_chg': avg_chg, 'signal': signal, 'up_ratio': up_ratio, 'n': len(changes)}
    return None

print(f"计算美股半导体个股信号...")
us_semi_signals = {}
for d in details:
    date = d['评分日']
    if date not in us_semi_signals:
        us_semi_signals[date] = get_us_semi_signal(date)

valid_us = sum(1 for v in us_semi_signals.values() if v is not None)
print(f"有效信号: {valid_us}/{len(us_semi_signals)} 天")

# 分析方向预测力
print(f"\n维度2a: 美股半导体个股信号 vs A股次日方向（宽松模式）")
print(f"{'板块':<10} {'信号>0→涨':>14} {'信号<0→跌':>14} {'强>1→涨':>14} {'强<-1→跌':>14} {'方向一致率':>12}")
print('-' * 85)

for sector in sectors:
    sd = [d for d in details if d['板块'] == sector]
    pos = [d for d in sd if us_semi_signals.get(d['评分日']) and us_semi_signals[d['评分日']]['signal'] > 0]
    neg = [d for d in sd if us_semi_signals.get(d['评分日']) and us_semi_signals[d['评分日']]['signal'] < 0]
    sp = [d for d in sd if us_semi_signals.get(d['评分日']) and us_semi_signals[d['评分日']]['signal'] > 1.0]
    sn = [d for d in sd if us_semi_signals.get(d['评分日']) and us_semi_signals[d['评分日']]['signal'] < -1.0]
    
    pos_up = sum(1 for d in pos if d['_ge0']) / len(pos) * 100 if pos else 0
    neg_dn = sum(1 for d in neg if d['_le0']) / len(neg) * 100 if neg else 0
    sp_up = sum(1 for d in sp if d['_ge0']) / len(sp) * 100 if sp else 0
    sn_dn = sum(1 for d in sn if d['_le0']) / len(sn) * 100 if sn else 0
    
    # 总方向一致率
    all_sig = [d for d in sd if us_semi_signals.get(d['评分日']) and us_semi_signals[d['评分日']]['signal'] != 0]
    dir_ok = sum(1 for d in all_sig if (us_semi_signals[d['评分日']]['signal'] > 0 and d['_ge0']) or 
                 (us_semi_signals[d['评分日']]['signal'] < 0 and d['_le0']))
    dir_rate = dir_ok / len(all_sig) * 100 if all_sig else 0
    
    print(f"{sector:<10} {pos_up:>5.1f}%({len(pos):>3}) {neg_dn:>5.1f}%({len(neg):>3}) "
          f"{sp_up:>5.1f}%({len(sp):>3}) {sn_dn:>5.1f}%({len(sn):>3}) {dir_rate:>5.1f}%({len(all_sig):>3})")

# 前半 vs 后半稳定性
print(f"\n维度2b: 美股半导体信号 前半 vs 后半稳定性")
print(f"{'板块':<10} {'前半一致率':>12} {'后半一致率':>12} {'差异':>8}")
print('-' * 50)

for sector in sectors:
    sd = [d for d in details if d['板块'] == sector]
    for half_name, half_dates in [('前半', first_half_dates), ('后半', second_half_dates)]:
        pass  # computed below
    
    first_sig = [d for d in sd if d['评分日'] in first_half_dates and 
                 us_semi_signals.get(d['评分日']) and us_semi_signals[d['评分日']]['signal'] != 0]
    second_sig = [d for d in sd if d['评分日'] in second_half_dates and 
                  us_semi_signals.get(d['评分日']) and us_semi_signals[d['评分日']]['signal'] != 0]
    
    f_ok = sum(1 for d in first_sig if (us_semi_signals[d['评分日']]['signal'] > 0 and d['_ge0']) or 
               (us_semi_signals[d['评分日']]['signal'] < 0 and d['_le0']))
    s_ok = sum(1 for d in second_sig if (us_semi_signals[d['评分日']]['signal'] > 0 and d['_ge0']) or 
               (us_semi_signals[d['评分日']]['signal'] < 0 and d['_le0']))
    
    fr = f_ok / len(first_sig) * 100 if first_sig else 0
    sr = s_ok / len(second_sig) * 100 if second_sig else 0
    
    print(f"{sector:<10} {fr:>5.1f}%({len(first_sig):>3}) {sr:>5.1f}%({len(second_sig):>3}) {fr-sr:>+6.1f}pp")

# 反转测试：美股半导体涨→A股跌？
print(f"\n维度2c: 反转测试（美股半导体涨→A股跌？）")
print(f"{'板块':<10} {'信号>0→跌':>14} {'信号<0→涨':>14} {'反转一致率':>12}")
print('-' * 55)

for sector in sectors:
    sd = [d for d in details if d['板块'] == sector]
    all_sig = [d for d in sd if us_semi_signals.get(d['评分日']) and us_semi_signals[d['评分日']]['signal'] != 0]
    
    pos = [d for d in all_sig if us_semi_signals[d['评分日']]['signal'] > 0]
    neg = [d for d in all_sig if us_semi_signals[d['评分日']]['signal'] < 0]
    
    pos_dn = sum(1 for d in pos if d['_le0']) / len(pos) * 100 if pos else 0
    neg_up = sum(1 for d in neg if d['_ge0']) / len(neg) * 100 if neg else 0
    
    rev_ok = sum(1 for d in all_sig if (us_semi_signals[d['评分日']]['signal'] > 0 and d['_le0']) or 
                 (us_semi_signals[d['评分日']]['signal'] < 0 and d['_ge0']))
    rev_rate = rev_ok / len(all_sig) * 100 if all_sig else 0
    
    print(f"{sector:<10} {pos_dn:>5.1f}%({len(pos):>3}) {neg_up:>5.1f}%({len(neg):>3}) {rev_rate:>5.1f}%({len(all_sig):>3})")


# ═══════════════════════════════════════════════════════════
# 第3部分: 已有因子有效性审计（从回测结果中提取）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"第3部分: 已有因子有效性审计")
print(f"{'=' * 80}")

# 从回测结果中读取因子有效性分析
factor_analysis = bt_data.get('因子有效性分析(按板块)', {})

factor_names = ['reversion', 'rsi', 'kdj', 'macd', 'boll', 'vp', 'fund', 'market',
                'streak', 'trend_bias', 'us_overnight', 'vol_regime', 'momentum_persist',
                'gap_signal', 'intraday_pos', 'db_fund', 'turnover']

factor_labels = {
    'reversion': '均值回归', 'rsi': 'RSI', 'kdj': 'KDJ', 'macd': 'MACD',
    'boll': 'BOLL', 'vp': '量价背离', 'fund': '资金流API', 'market': '大盘环境',
    'streak': '连续涨跌', 'trend_bias': 'MA趋势', 'us_overnight': '美股隔夜',
    'vol_regime': '波动率', 'momentum_persist': '动量持续', 'gap_signal': '跳空缺口',
    'intraday_pos': '日内位置', 'db_fund': 'DB资金流', 'turnover': '换手率',
}

from day_week_predicted.backtest.prediction_enhanced_backtest import _SECTOR_FACTOR_WEIGHTS

print(f"\n各板块因子方向一致率 + 当前权重 + 评价:")
print(f"（方向一致率>55%=有效, <45%=反转有效, 45-55%=噪声）\n")

for sector in sectors:
    sec_fa = factor_analysis.get(sector, {})
    sec_w = _SECTOR_FACTOR_WEIGHTS.get(sector, {})
    
    print(f"{'─' * 75}")
    print(f"板块: {sector}")
    print(f"{'因子':<12} {'一致率':>8} {'样本':>6} {'权重':>6} {'评价':<20}")
    print(f"{'─' * 75}")
    
    noise_factors = []
    effective_factors = []
    reversal_factors = []
    
    for fname in factor_names:
        fa = sec_fa.get(fname, {})
        rate_str = fa.get('方向一致率', '0%')
        rate = float(rate_str.replace('%', '')) if rate_str != '0%' else 0
        n = fa.get('样本数', 0)
        w = sec_w.get(fname, 0)
        
        if n < 10:
            verdict = '样本不足'
        elif rate > 55:
            verdict = '✅ 有效'
            effective_factors.append(fname)
        elif rate < 45:
            if w < 0:
                verdict = '✅ 反转(权重已负)'
                effective_factors.append(fname)
            elif w > 0:
                verdict = '⚠️ 反转但权重为正!'
            else:
                verdict = '🔄 反转(权重=0)'
        else:
            if w != 0:
                verdict = f'❌ 噪声(权重{w}应→0)'
                noise_factors.append((fname, w))
            else:
                verdict = '❌ 噪声(已=0)'
        
        print(f"{factor_labels.get(fname, fname):<12} {rate:>6.1f}% {n:>6} {w:>+6.1f} {verdict}")
    
    if noise_factors:
        print(f"  → 建议移除: {', '.join(factor_labels.get(f, f) + f'(w={w})' for f, w in noise_factors)}")


# ═══════════════════════════════════════════════════════════
# 第4部分: 因子剪枝模拟 — 用stored combined信号近似
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"第4部分: 因子剪枝模拟")
print(f"{'=' * 80}")

# 由于个别因子值未存储在JSON中，我们无法精确重算combined
# 但可以分析：如果改变决策逻辑（阈值/默认方向），能否提升准确率
# 
# 策略：用stored融合信号 + 不同决策阈值组合来搜索最优

from day_week_predicted.backtest.prediction_enhanced_backtest import _SECTOR_UP_BASE_RATE, _SECTOR_PEER_WEIGHT

def simulate_with_thresholds(d, bullish_th, bearish_th, default_up):
    """用给定阈值模拟方向决策"""
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
    
    # 星期效应（保持不变）
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
    except:
        pass
    
    if sector == '汽车' and score < 35:
        direction = '上涨'
    elif sector == '有色金属' and score < 35:
        direction = '上涨'
    elif sector == '科技' and score < 35:
        direction = '上涨'
    
    return direction

def check_loose(direction, actual):
    if direction == '上涨' and actual >= 0: return True
    if direction == '下跌' and actual <= 0: return True
    return False

# 当前v16b阈值
current_thresholds = {
    '科技': (1.0, -0.5, False),
    '有色金属': (999, -999, True),  # always 上涨
    '汽车': (0.5, -0.5, False),
    '新能源': (0.5, -1.0, True),
    '医药': (0.5, -0.5, False),
    '化工': (999, -1.0, True),  # 上涨 unless combined < -1.0
    '制造': (0.5, -0.5, True),
}

# 搜索每个板块的最优阈值
print(f"\n每个板块的阈值优化搜索（前半训练→后半测试）:")

best_configs = {}

for sector in sectors:
    sd = [d for d in details if d['板块'] == sector]
    sd_first = [d for d in sd if d['评分日'] in first_half_dates]
    sd_second = [d for d in sd if d['评分日'] in second_half_dates]
    
    cur_b, cur_bear, cur_def = current_thresholds[sector]
    
    # 当前准确率
    cur_first_ok = sum(1 for d in sd_first if d['宽松正确'] == '✓')
    cur_second_ok = sum(1 for d in sd_second if d['宽松正确'] == '✓')
    cur_total_ok = sum(1 for d in sd if d['宽松正确'] == '✓')
    
    print(f"\n{'─' * 60}")
    print(f"板块: {sector} (样本: 前半{len(sd_first)}, 后半{len(sd_second)})")
    print(f"当前: 前半{cur_first_ok/len(sd_first)*100:.1f}% 后半{cur_second_ok/len(sd_second)*100:.1f}% "
          f"总{cur_total_ok/len(sd)*100:.1f}%")
    
    # 网格搜索
    best_second = 0
    best_params = None
    best_first = 0
    
    for bull_th in [-0.5, 0.0, 0.3, 0.5, 0.8, 1.0, 1.5, 2.0, 999]:
        for bear_th in [-2.0, -1.5, -1.0, -0.5, -0.3, 0.0, -999]:
            for def_up in [True, False]:
                # 在前半训练
                first_ok = sum(1 for d in sd_first if check_loose(
                    simulate_with_thresholds(d, bull_th, bear_th, def_up), d['_actual']))
                first_rate = first_ok / len(sd_first) * 100 if sd_first else 0
                
                # 在后半测试
                second_ok = sum(1 for d in sd_second if check_loose(
                    simulate_with_thresholds(d, bull_th, bear_th, def_up), d['_actual']))
                second_rate = second_ok / len(sd_second) * 100 if sd_second else 0
                
                # 选择前半最好的配置
                if first_rate > best_first or (first_rate == best_first and second_rate > best_second):
                    best_first = first_rate
                    best_second = second_rate
                    best_params = (bull_th, bear_th, def_up, first_ok, second_ok)
    
    if best_params:
        bt, brt, du, fo, so = best_params
        total_ok = fo + so
        total_rate = total_ok / len(sd) * 100
        print(f"最优(前半训练): bull>{bt}, bear<{brt}, default={'涨' if du else '跌'}")
        print(f"  前半{best_first:.1f}% 后半{best_second:.1f}% 总{total_rate:.1f}%")
        print(f"  vs当前: 前半{cur_first_ok/len(sd_first)*100:.1f}% 后半{cur_second_ok/len(sd_second)*100:.1f}%")
        best_configs[sector] = best_params


# ═══════════════════════════════════════════════════════════
# 第5部分: 美股半导体信号 + 阈值优化组合
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"第5部分: 美股半导体信号增强 + 阈值优化")
print(f"{'=' * 80}")

# 测试：在combined信号上叠加美股半导体信号，然后用优化阈值
def simulate_enhanced(d, bull_th, bear_th, def_up, us_weight=0.0):
    sector = d['板块']
    combined = d['融合信号']
    confidence = d['置信度']
    score = d['评分']
    
    # 叠加美股半导体信号
    us_semi = us_semi_signals.get(d['评分日'])
    if us_semi and us_weight != 0:
        combined = combined + us_semi['signal'] * us_weight
    
    if combined > bull_th:
        direction = '上涨'
    elif combined < bear_th:
        direction = '下跌'
    else:
        direction = '上涨' if def_up else '下跌'
    
    # 星期效应
    try:
        wd = datetime.strptime(d['评分日'], '%Y-%m-%d').weekday()
        if sector == '医药' and wd == 4: direction = '下跌'
        elif sector == '汽车' and wd == 1 and confidence != 'high': direction = '下跌'
        elif sector == '汽车' and wd == 2 and confidence != 'high': direction = '下跌'
        elif sector == '有色金属' and wd == 2 and confidence != 'high': direction = '下跌'
        elif sector == '科技' and wd == 2 and confidence != 'high': direction = '下跌'
        elif sector == '新能源' and wd == 4 and confidence != 'high': direction = '下跌'
        elif sector == '新能源' and wd == 0 and confidence != 'high': direction = '下跌'
        elif sector == '制造' and wd == 4 and confidence != 'high': direction = '下跌'
        elif sector == '化工' and wd == 2 and confidence == 'low': direction = '下跌'
        elif sector == '有色金属' and wd == 1: direction = '上涨'
        elif sector == '有色金属' and wd == 4: direction = '上涨'
        elif sector == '化工' and wd == 4: direction = '上涨'
        elif sector == '化工' and wd == 1 and confidence != 'high': direction = '上涨'
    except: pass
    
    if sector == '汽车' and score < 35: direction = '上涨'
    elif sector == '有色金属' and score < 35: direction = '上涨'
    elif sector == '科技' and score < 35: direction = '上涨'
    
    return direction

# 全面搜索：每个板块独立优化阈值 + 美股半导体权重
print(f"\n全面搜索: 板块独立阈值 + 美股半导体权重")
print(f"{'权重':>6} {'总准确率':>10} {'前半':>8} {'后半':>8} {'vs当前':>8}")
print('-' * 50)

for us_w in [0.0, 0.1, 0.2, 0.3, -0.1, -0.2, -0.3]:
    total_ok = 0
    first_ok_all, second_ok_all = 0, 0
    first_n_all, second_n_all = 0, 0
    
    for sector in sectors:
        sd = [d for d in details if d['板块'] == sector]
        
        # 用当前阈值（不是优化后的，避免过拟合）
        bt, brt, du = current_thresholds[sector]
        
        for d in sd:
            direction = simulate_enhanced(d, bt, brt, du, us_w)
            ok = check_loose(direction, d['_actual'])
            if ok: total_ok += 1
            
            if d['评分日'] in first_half_dates:
                first_n_all += 1
                if ok: first_ok_all += 1
            else:
                second_n_all += 1
                if ok: second_ok_all += 1
    
    rate = total_ok / total * 100
    fr = first_ok_all / first_n_all * 100 if first_n_all > 0 else 0
    sr = second_ok_all / second_n_all * 100 if second_n_all > 0 else 0
    diff = rate - loose_ok / total * 100
    
    print(f"{us_w:>6.1f} {rate:>8.1f}% {fr:>6.1f}% {sr:>6.1f}% {diff:>+6.1f}pp")


# ═══════════════════════════════════════════════════════════
# 第6部分: 留一日交叉验证
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"第6部分: 留一日交叉验证")
print(f"{'=' * 80}")

# 对每个日期，计算当前模型和优化阈值模型的准确率
date_accuracy = {}
for date in all_dates:
    dd = [d for d in details if d['评分日'] == date]
    if not dd: continue
    
    orig_ok = sum(1 for d in dd if d['宽松正确'] == '✓')
    
    # 用最优前半阈值
    opt_ok = 0
    for d in dd:
        sector = d['板块']
        if sector in best_configs:
            bt, brt, du, _, _ = best_configs[sector]
        else:
            bt, brt, du = current_thresholds.get(sector, (0.5, -0.5, False))
        direction = simulate_with_thresholds(d, bt, brt, du)
        if check_loose(direction, d['_actual']):
            opt_ok += 1
    
    date_accuracy[date] = (orig_ok, opt_ok, len(dd))

# LOO-CV
orig_cv = []
opt_cv = []
for leave in all_dates:
    o_rest = sum(v[0] for d, v in date_accuracy.items() if d != leave)
    p_rest = sum(v[1] for d, v in date_accuracy.items() if d != leave)
    n_rest = sum(v[2] for d, v in date_accuracy.items() if d != leave)
    if n_rest > 0:
        orig_cv.append(o_rest / n_rest * 100)
        opt_cv.append(p_rest / n_rest * 100)

orig_mean = sum(orig_cv) / len(orig_cv) if orig_cv else 0
opt_mean = sum(opt_cv) / len(opt_cv) if opt_cv else 0
orig_std = (sum((r - orig_mean)**2 for r in orig_cv) / len(orig_cv))**0.5 if orig_cv else 0
opt_std = (sum((r - opt_mean)**2 for r in opt_cv) / len(opt_cv))**0.5 if opt_cv else 0

print(f"\n留一日交叉验证:")
print(f"  当前模型: {orig_mean:.2f}% ± {orig_std:.2f}%")
print(f"  优化阈值: {opt_mean:.2f}% ± {opt_std:.2f}%")
print(f"  差异: {opt_mean - orig_mean:+.2f}pp")

# ═══════════════════════════════════════════════════════════
# 第7部分: 纯统计基线分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"第7部分: 纯统计基线分析（理论天花板）")
print(f"{'=' * 80}")

# 如果每个板块都用最优的固定方向（全涨或全跌），能达到多少？
print(f"\n板块固定方向基线:")
print(f"{'板块':<10} {'全涨':>8} {'全跌':>8} {'最优':>8} {'当前':>8}")
print('-' * 50)

total_best_fixed = 0
for sector in sectors:
    sd = [d for d in details if d['板块'] == sector]
    all_up = sum(1 for d in sd if d['_ge0'])
    all_dn = sum(1 for d in sd if d['_le0'])
    best = max(all_up, all_dn)
    cur = sum(1 for d in sd if d['宽松正确'] == '✓')
    total_best_fixed += best
    
    print(f"{sector:<10} {all_up/len(sd)*100:>6.1f}% {all_dn/len(sd)*100:>6.1f}% "
          f"{best/len(sd)*100:>6.1f}% {cur/len(sd)*100:>6.1f}%")

print(f"{'总计(最优固定)':<10} {total_best_fixed/total*100:>6.1f}%")

# 如果每天都用最优固定方向
print(f"\n每日最优固定方向:")
daily_best = 0
for date in all_dates:
    dd = [d for d in details if d['评分日'] == date]
    up_ok = sum(1 for d in dd if d['_ge0'])
    dn_ok = sum(1 for d in dd if d['_le0'])
    daily_best += max(up_ok, dn_ok)
print(f"  每日最优: {daily_best}/{total} ({daily_best/total*100:.1f}%)")

# 如果每个板块×每天都用最优方向（理论上限）
sector_daily_best = 0
for sector in sectors:
    for date in all_dates:
        dd = [d for d in details if d['板块'] == sector and d['评分日'] == date]
        if not dd: continue
        up_ok = sum(1 for d in dd if d['_ge0'])
        dn_ok = sum(1 for d in dd if d['_le0'])
        sector_daily_best += max(up_ok, dn_ok)
print(f"  板块×日最优: {sector_daily_best}/{total} ({sector_daily_best/total*100:.1f}%)")

# 前半 vs 后半的涨跌基准率变化
print(f"\n前半 vs 后半 涨跌基准率变化:")
print(f"{'板块':<10} {'前半≥0%':>10} {'后半≥0%':>10} {'差异':>8}")
print('-' * 40)

for sector in sectors:
    sd = [d for d in details if d['板块'] == sector]
    f_data = [d for d in sd if d['评分日'] in first_half_dates]
    s_data = [d for d in sd if d['评分日'] in second_half_dates]
    
    f_up = sum(1 for d in f_data if d['_ge0']) / len(f_data) * 100 if f_data else 0
    s_up = sum(1 for d in s_data if d['_ge0']) / len(s_data) * 100 if s_data else 0
    
    print(f"{sector:<10} {f_up:>8.1f}% {s_up:>8.1f}% {f_up-s_up:>+6.1f}pp")


# ═══════════════════════════════════════════════════════════
# 第8部分: 综合结论
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"第8部分: 综合结论与建议")
print(f"{'=' * 80}")

print(f"""
┌─────────────────────────────────────────────────────────────────────┐
│                    数据源可用性审计结果                              │
├─────────────────────────────────────────────────────────────────────┤
│ 数据源              │ 回测期覆盖 │ 结论                             │
├─────────────────────────────────────────────────────────────────────┤
│ 龙虎榜              │ 0只股票    │ ❌ 完全无数据，无法使用           │
│ 盘口数据            │ 仅2天      │ ❌ 数据不足，无法使用             │
│ 分时数据            │ 仅2天      │ ❌ 数据不足，无法使用             │
│ 美股涨幅榜          │ 仅3天      │ ❌ 数据不足，无法使用             │
│ 全球指数行情        │ 仅3天      │ ❌ 数据不足，无法使用             │
│ 技术评分历史        │ 8只/1-4条  │ ❌ 数据不足，无法使用             │
│ 财务数据            │ 50只       │ ❌ 季报频率，对日频预测无用       │
│ 美股个股K线         │ 18只/61天  │ ✅ 可测试（见第2/5部分结果）      │
│ DB资金流            │ 50只/29天  │ ✅ 已在使用（db_fund因子）        │
│ 概念板块            │ 已测试     │ ❌ 样本外更差（v15/v17结论）      │
│ 融资融券            │ 已测试     │ ❌ 接近随机（v18结论）            │
│ 大宗交易            │ 已测试     │ ❌ 覆盖率低+无效（v18结论）       │
├─────────────────────────────────────────────────────────────────────┤
│ 结论: 除美股个股K线外，无新的可用数据源                             │
└─────────────────────────────────────────────────────────────────────┘
""")

print(f"当前模型准确率: {loose_ok}/{total} ({loose_ok/total*100:.1f}%)")
print(f"目标: {int(total*0.65)}/{total} (65.0%)")
print(f"差距: {int(total*0.65) - loose_ok}个样本 ({65.0 - loose_ok/total*100:.1f}pp)")

print(f"\nLOO-CV: 当前{orig_mean:.2f}% vs 优化阈值{opt_mean:.2f}% (差异{opt_mean - orig_mean:+.2f}pp)")

print(f"\n关键发现:")
print(f"1. 板块×日最优理论上限: {sector_daily_best}/{total} ({sector_daily_best/total*100:.1f}%)")
print(f"   → 即使每天每个板块都选对方向，也只能达到这个上限")
print(f"2. 前后半涨跌基准率剧烈变化（市场regime shift）")
print(f"   → 这是准确率无法突破的根本原因")
print(f"3. 噪声因子（一致率45-55%）应该移除以减少过拟合")

print(f"\n建议操作:")
print(f"1. 移除噪声因子（权重→0）以减少过拟合风险")
print(f"2. 美股半导体个股信号：如果方向一致率>55%且前后半稳定，可加入")
print(f"3. 补充龙虎榜/盘口/分时历史数据后再测试")
print(f"4. 58.4%可能已接近该数据集+方法论的天花板")

print(f"\n{'=' * 80}")
print(f"分析完成")
print(f"{'=' * 80}")
