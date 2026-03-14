#!/usr/bin/env python3
"""
v14 全板块深度分析：利用行业板块全部个股走势+资金流向优化预测

核心思路：
  当前模型只用了50只回测股票的同行数据（最多8只peer），
  但DB中7个板块共3750只股票都有kline和fund_flow数据。
  本分析验证：利用全板块所有个股的聚合信号是否能提升预测准确率。

分析维度：
  1. 全板块涨跌比（当日板块内所有股票涨跌比例）vs 回测股票次日涨跌
  2. 全板块资金流向聚合（板块内所有股票大单净额汇总）vs 次日涨跌
  3. 全板块资金流向一致性（>60%股票同向流入/流出）vs 次日涨跌
  4. 板块龙头股走势领先信号（板块内市值最大的N只股票走势）
  5. 全板块连续资金流向趋势（连续3日板块级净流入/流出）
  6. 组合信号：全板块走势+资金流向+美股 三维组合
  7. 与当前模型对比：全板块信号能否修正模型错误预测
"""
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
# 加载回测结果
# ═══════════════════════════════════════════════════════════
with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json') as f:
    data = json.load(f)

details = data['逐日详情']
total = len(details)


def parse_chg(s):
    return float(s.replace('%', '').replace('+', ''))


loose_ok = sum(1 for d in details if d['宽松正确'] == '✓')
print(f"{'=' * 80}")
print(f"v14 全板块深度分析")
print(f"当前基线: {loose_ok}/{total} ({loose_ok / total * 100:.1f}%)")
print(f"目标: 65% = {int(total * 0.65)}/{total}, 差距: {int(total * 0.65) - loose_ok}个样本")
print(f"{'=' * 80}")

sectors = ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']

# ═══════════════════════════════════════════════════════════
# 加载全板块数据
# ═══════════════════════════════════════════════════════════
from common.utils.sector_mapping_utils import parse_industry_list_md
from dao import get_connection

sector_mapping = parse_industry_list_md()
print(f"\n板块映射: {len(sector_mapping)} 只股票")

# 按板块分组
sector_stocks = defaultdict(list)
for code, sector in sector_mapping.items():
    sector_stocks[sector].append(code)

for s in sectors:
    print(f"  {s}: {len(sector_stocks[s])}只")


# ═══════════════════════════════════════════════════════════
# 从DB批量加载全板块K线和资金流向数据
# 使用SQL聚合查询，避免逐只加载（3750只太慢）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"加载全板块聚合数据（SQL批量查询）...")
print(f"{'=' * 80}")

conn = get_connection(use_dict_cursor=True)
cursor = conn.cursor()

# 收集回测涉及的日期范围
all_dates = sorted(set(d['评分日'] for d in details))
date_min = all_dates[0]
date_max = all_dates[-1]
print(f"  回测日期范围: {date_min} ~ {date_max}")

# ── 1. 全板块K线聚合：按(板块, 日期)统计涨跌比 ──
# 查询每只股票每天的涨跌幅，然后按板块聚合
sector_daily_kline = {}  # {(sector, date): {up_count, down_count, flat_count, total, avg_chg}}

for sector in sectors:
    codes = sector_stocks[sector]
    if not codes:
        continue

    placeholders = ','.join(['%s'] * len(codes))
    sql = f"""
        SELECT k.date,
               COUNT(*) as total_stocks,
               SUM(CASE WHEN k.close_price > k.open_price THEN 1 ELSE 0 END) as up_count,
               SUM(CASE WHEN k.close_price < k.open_price THEN 1 ELSE 0 END) as down_count,
               SUM(CASE WHEN k.close_price = k.open_price THEN 1 ELSE 0 END) as flat_count,
               AVG((k.close_price - k.open_price) / NULLIF(k.open_price, 0) * 100) as avg_chg_intraday
        FROM stock_kline k
        WHERE k.stock_code IN ({placeholders})
          AND k.date >= %s AND k.date <= %s
          AND k.trading_volume > 0
        GROUP BY k.date
        ORDER BY k.date
    """
    cursor.execute(sql, codes + [date_min, date_max])
    rows = cursor.fetchall()
    for r in rows:
        key = (sector, str(r['date']))
        sector_daily_kline[key] = {
            'total': r['total_stocks'],
            'up': r['up_count'],
            'down': r['down_count'],
            'flat': r['flat_count'],
            'avg_chg': float(r['avg_chg_intraday'] or 0),
        }

    print(f"  {sector}: {len(rows)}个交易日的K线聚合数据")

# ── 1b. 全板块K线：用前一日收盘价计算日涨跌幅（更准确） ──
# 查询每只股票的日涨跌幅（close vs prev_close）
sector_daily_chg = {}  # {(sector, date): {up_pct, down_pct, avg_chg, median_chg}}

for sector in sectors:
    codes = sector_stocks[sector]
    if not codes:
        continue

    placeholders = ','.join(['%s'] * len(codes))
    # 使用LAG窗口函数计算日涨跌幅
    sql = f"""
        SELECT date, stock_code, close_price,
               change_percent
        FROM stock_kline
        WHERE stock_code IN ({placeholders})
          AND date >= %s AND date <= %s
          AND trading_volume > 0
        ORDER BY stock_code, date
    """
    cursor.execute(sql, codes + [date_min, date_max])
    rows = cursor.fetchall()

    # 按日期聚合
    date_chgs = defaultdict(list)
    for r in rows:
        chg = float(r.get('change_percent') or 0)
        date_chgs[str(r['date'])].append(chg)

    for dt, chgs in date_chgs.items():
        if not chgs:
            continue
        up_count = sum(1 for c in chgs if c > 0.3)
        down_count = sum(1 for c in chgs if c < -0.3)
        total_c = len(chgs)
        avg_c = sum(chgs) / total_c
        sorted_chgs = sorted(chgs)
        median_c = sorted_chgs[total_c // 2]

        key = (sector, dt)
        sector_daily_chg[key] = {
            'total': total_c,
            'up_count': up_count,
            'down_count': down_count,
            'up_pct': up_count / total_c if total_c > 0 else 0,
            'down_pct': down_count / total_c if total_c > 0 else 0,
            'avg_chg': round(avg_c, 3),
            'median_chg': round(median_c, 3),
        }

    print(f"  {sector}: {len(date_chgs)}个交易日的涨跌幅聚合")


# ── 2. 全板块资金流向聚合 ──
sector_daily_fund = {}  # {(sector, date): {total_big_net, avg_big_net_pct, inflow_count, outflow_count, total}}

for sector in sectors:
    codes = sector_stocks[sector]
    if not codes:
        continue

    placeholders = ','.join(['%s'] * len(codes))
    sql = f"""
        SELECT f.date,
               COUNT(*) as total_stocks,
               SUM(f.big_net) as total_big_net,
               AVG(f.big_net_pct) as avg_big_net_pct,
               SUM(CASE WHEN f.big_net > 0 THEN 1 ELSE 0 END) as inflow_count,
               SUM(CASE WHEN f.big_net < 0 THEN 1 ELSE 0 END) as outflow_count,
               SUM(f.big_net) / NULLIF(COUNT(*), 0) as avg_big_net,
               AVG(f.main_net_5day) as avg_main_5d
        FROM stock_fund_flow f
        WHERE f.stock_code IN ({placeholders})
          AND f.date >= %s AND f.date <= %s
        GROUP BY f.date
        ORDER BY f.date
    """
    cursor.execute(sql, codes + [date_min, date_max])
    rows = cursor.fetchall()
    for r in rows:
        key = (sector, str(r['date']))
        total_s = r['total_stocks'] or 1
        sector_daily_fund[key] = {
            'total': total_s,
            'total_big_net': float(r['total_big_net'] or 0),
            'avg_big_net_pct': float(r['avg_big_net_pct'] or 0),
            'inflow_count': r['inflow_count'] or 0,
            'outflow_count': r['outflow_count'] or 0,
            'inflow_pct': (r['inflow_count'] or 0) / total_s,
            'outflow_pct': (r['outflow_count'] or 0) / total_s,
            'avg_big_net': float(r['avg_big_net'] or 0),
            'avg_main_5d': float(r['avg_main_5d'] or 0),
        }

    print(f"  {sector}: {len(rows)}个交易日的资金流向聚合")

cursor.close()
conn.close()
print(f"\n  数据加载完成!")


# ═══════════════════════════════════════════════════════════
# 分析一：全板块涨跌比 vs 回测股票次日涨跌
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"一、全板块涨跌比（评分日当天板块内所有股票涨跌比例）vs 次日涨跌")
print(f"{'=' * 80}")

# 当板块内>60%股票上涨时，回测股票次日涨的概率
for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]

    # 按板块涨跌比分组
    bullish_ok, bullish_n = 0, 0  # 板块>60%上涨
    bearish_ok, bearish_n = 0, 0  # 板块>60%下跌
    neutral_ok, neutral_n = 0, 0  # 中性
    strong_bull_ok, strong_bull_n = 0, 0  # >70%上涨
    strong_bear_ok, strong_bear_n = 0, 0  # >70%下跌

    for d in sec_data:
        key = (sector, d['评分日'])
        chg_data = sector_daily_chg.get(key)
        if not chg_data:
            continue

        actual_chg = parse_chg(d['实际涨跌'])
        up_pct = chg_data['up_pct']
        down_pct = chg_data['down_pct']

        if up_pct > 0.7:
            strong_bull_n += 1
            if actual_chg > 0:
                strong_bull_ok += 1
        elif up_pct > 0.6:
            bullish_n += 1
            if actual_chg > 0:
                bullish_ok += 1
        elif down_pct > 0.7:
            strong_bear_n += 1
            if actual_chg < 0:
                strong_bear_ok += 1
        elif down_pct > 0.6:
            bearish_n += 1
            if actual_chg < 0:
                bearish_ok += 1
        else:
            neutral_n += 1
            if d['宽松正确'] == '✓':
                neutral_ok += 1

    sb = f"{strong_bull_ok}/{strong_bull_n}({strong_bull_ok/strong_bull_n*100:.1f}%)" if strong_bull_n > 0 else "N/A"
    b = f"{bullish_ok}/{bullish_n}({bullish_ok/bullish_n*100:.1f}%)" if bullish_n > 0 else "N/A"
    sbe = f"{strong_bear_ok}/{strong_bear_n}({strong_bear_ok/strong_bear_n*100:.1f}%)" if strong_bear_n > 0 else "N/A"
    be = f"{bearish_ok}/{bearish_n}({bearish_ok/bearish_n*100:.1f}%)" if bearish_n > 0 else "N/A"
    ne = f"{neutral_ok}/{neutral_n}({neutral_ok/neutral_n*100:.1f}%)" if neutral_n > 0 else "N/A"
    print(f"  {sector:6s}: >70%涨→次涨={sb} | 60-70%涨→次涨={b} | >70%跌→次跌={sbe} | 60-70%跌→次跌={be} | 中性模型准确={ne}")

# 1b. 全板块平均涨跌幅方向 vs 次日涨跌
print(f"\n  1b. 全板块平均涨跌幅方向 vs 次日涨跌:")
for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]

    avg_up_ok, avg_up_n = 0, 0
    avg_down_ok, avg_down_n = 0, 0
    big_up_ok, big_up_n = 0, 0  # 板块平均涨>1%
    big_down_ok, big_down_n = 0, 0  # 板块平均跌>1%

    for d in sec_data:
        key = (sector, d['评分日'])
        chg_data = sector_daily_chg.get(key)
        if not chg_data:
            continue

        actual_chg = parse_chg(d['实际涨跌'])
        avg_chg = chg_data['avg_chg']

        if avg_chg > 1.0:
            big_up_n += 1
            if actual_chg > 0:
                big_up_ok += 1
        elif avg_chg > 0:
            avg_up_n += 1
            if actual_chg > 0:
                avg_up_ok += 1
        elif avg_chg < -1.0:
            big_down_n += 1
            if actual_chg < 0:
                big_down_ok += 1
        else:
            avg_down_n += 1
            if actual_chg < 0:
                avg_down_ok += 1

    bu = f"{big_up_ok}/{big_up_n}({big_up_ok/big_up_n*100:.1f}%)" if big_up_n > 0 else "N/A"
    au = f"{avg_up_ok}/{avg_up_n}({avg_up_ok/avg_up_n*100:.1f}%)" if avg_up_n > 0 else "N/A"
    bd = f"{big_down_ok}/{big_down_n}({big_down_ok/big_down_n*100:.1f}%)" if big_down_n > 0 else "N/A"
    ad = f"{avg_down_ok}/{avg_down_n}({avg_down_ok/avg_down_n*100:.1f}%)" if avg_down_n > 0 else "N/A"
    print(f"    {sector:6s}: 板块大涨(>1%)→次涨={bu} | 板块微涨→次涨={au} | 板块大跌(<-1%)→次跌={bd} | 板块微跌→次跌={ad}")


# ═══════════════════════════════════════════════════════════
# 分析二：全板块资金流向聚合 vs 次日涨跌
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"二、全板块资金流向聚合（板块内所有股票大单净额汇总）vs 次日涨跌")
print(f"{'=' * 80}")

for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]

    # 按板块资金流向方向分组
    inflow_ok, inflow_n = 0, 0
    outflow_ok, outflow_n = 0, 0

    # 按资金流向强度分组
    strong_in_ok, strong_in_n = 0, 0
    strong_out_ok, strong_out_n = 0, 0

    # 按资金流向一致性分组（>60%股票同向）
    consensus_in_ok, consensus_in_n = 0, 0
    consensus_out_ok, consensus_out_n = 0, 0
    no_consensus_ok, no_consensus_n = 0, 0

    for d in sec_data:
        key = (sector, d['评分日'])
        fund_data = sector_daily_fund.get(key)
        if not fund_data:
            continue

        actual_chg = parse_chg(d['实际涨跌'])
        total_net = fund_data['total_big_net']
        inflow_pct = fund_data['inflow_pct']
        outflow_pct = fund_data['outflow_pct']
        avg_pct = fund_data['avg_big_net_pct']

        # 方向
        if total_net > 0:
            inflow_n += 1
            if actual_chg > 0:
                inflow_ok += 1
        elif total_net < 0:
            outflow_n += 1
            if actual_chg < 0:
                outflow_ok += 1

        # 强度（平均大单净占比）
        if avg_pct > 1.0:
            strong_in_n += 1
            if actual_chg > 0:
                strong_in_ok += 1
        elif avg_pct < -1.0:
            strong_out_n += 1
            if actual_chg < 0:
                strong_out_ok += 1

        # 一致性
        if inflow_pct > 0.6:
            consensus_in_n += 1
            if actual_chg > 0:
                consensus_in_ok += 1
        elif outflow_pct > 0.6:
            consensus_out_n += 1
            if actual_chg < 0:
                consensus_out_ok += 1
        else:
            no_consensus_n += 1
            if d['宽松正确'] == '✓':
                no_consensus_ok += 1

    inf = f"{inflow_ok}/{inflow_n}({inflow_ok/inflow_n*100:.1f}%)" if inflow_n > 0 else "N/A"
    outf = f"{outflow_ok}/{outflow_n}({outflow_ok/outflow_n*100:.1f}%)" if outflow_n > 0 else "N/A"
    print(f"  {sector:6s}: 全板块净流入→涨={inf} | 全板块净流出→跌={outf}")

    si = f"{strong_in_ok}/{strong_in_n}({strong_in_ok/strong_in_n*100:.1f}%)" if strong_in_n > 0 else "N/A"
    so = f"{strong_out_ok}/{strong_out_n}({strong_out_ok/strong_out_n*100:.1f}%)" if strong_out_n > 0 else "N/A"
    print(f"           强流入(avg>1%)→涨={si} | 强流出(avg<-1%)→跌={so}")

    ci = f"{consensus_in_ok}/{consensus_in_n}({consensus_in_ok/consensus_in_n*100:.1f}%)" if consensus_in_n > 0 else "N/A"
    co = f"{consensus_out_ok}/{consensus_out_n}({consensus_out_ok/consensus_out_n*100:.1f}%)" if consensus_out_n > 0 else "N/A"
    nc = f"{no_consensus_ok}/{no_consensus_n}({no_consensus_ok/no_consensus_n*100:.1f}%)" if no_consensus_n > 0 else "N/A"
    print(f"           >60%流入→涨={ci} | >60%流出→跌={co} | 无共识模型准确={nc}")


# ═══════════════════════════════════════════════════════════
# 分析三：全板块走势+资金流向 vs 仅50只同行（对比当前模型）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"三、全板块信号 vs 当前模型50只同行信号 对比")
print(f"{'=' * 80}")

# 加载50只股票的资金流向（用于对比）
from dao.stock_fund_flow_dao import get_fund_flow_by_code

stock_codes_50 = list(set(d['代码'] for d in details))
fund_flow_50 = {}
for code in stock_codes_50:
    ff = get_fund_flow_by_code(code, limit=200)
    if ff:
        fund_flow_50[code] = {str(r.get('date', '')): r for r in ff}

# 对比：50只同行聚合 vs 全板块聚合
print(f"\n  3a. 资金流向方向一致率对比（50只同行 vs 全板块）:")
for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]

    # 50只同行聚合
    peer50_in_ok, peer50_in_n = 0, 0
    peer50_out_ok, peer50_out_n = 0, 0

    # 全板块聚合
    full_in_ok, full_in_n = 0, 0
    full_out_ok, full_out_n = 0, 0

    for d in sec_data:
        actual_chg = parse_chg(d['实际涨跌'])
        score_date = d['评分日']

        # 50只同行
        peer_net = 0
        peer_count = 0
        for code2 in stock_codes_50:
            if d['板块'] != details[0]['板块']:  # 同板块
                pass
            if code2 in fund_flow_50 and sector_mapping.get(code2) == sector:
                ff = fund_flow_50[code2].get(score_date)
                if ff:
                    peer_net += (ff.get('big_net') or 0)
                    peer_count += 1

        if peer_count > 0:
            if peer_net > 0:
                peer50_in_n += 1
                if actual_chg > 0:
                    peer50_in_ok += 1
            elif peer_net < 0:
                peer50_out_n += 1
                if actual_chg < 0:
                    peer50_out_ok += 1

        # 全板块
        key = (sector, score_date)
        fund_data = sector_daily_fund.get(key)
        if fund_data:
            if fund_data['total_big_net'] > 0:
                full_in_n += 1
                if actual_chg > 0:
                    full_in_ok += 1
            elif fund_data['total_big_net'] < 0:
                full_out_n += 1
                if actual_chg < 0:
                    full_out_ok += 1

    p_in = f"{peer50_in_ok}/{peer50_in_n}({peer50_in_ok/peer50_in_n*100:.1f}%)" if peer50_in_n > 0 else "N/A"
    p_out = f"{peer50_out_ok}/{peer50_out_n}({peer50_out_ok/peer50_out_n*100:.1f}%)" if peer50_out_n > 0 else "N/A"
    f_in = f"{full_in_ok}/{full_in_n}({full_in_ok/full_in_n*100:.1f}%)" if full_in_n > 0 else "N/A"
    f_out = f"{full_out_ok}/{full_out_n}({full_out_ok/full_out_n*100:.1f}%)" if full_out_n > 0 else "N/A"
    print(f"    {sector:6s}: 50只同行 流入→涨={p_in} 流出→跌={p_out}")
    print(f"    {sector:6s}: 全板块   流入→涨={f_in} 流出→跌={f_out}")


# ═══════════════════════════════════════════════════════════
# 分析四：全板块连续资金流向趋势
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"四、全板块连续资金流向趋势（连续N日板块级净流入/流出）vs 次日涨跌")
print(f"{'=' * 80}")

# 构建板块级资金流向时间序列
sector_fund_series = defaultdict(dict)  # {sector: {date: total_big_net}}
for (sector, dt), fund_data in sector_daily_fund.items():
    sector_fund_series[sector][dt] = fund_data['total_big_net']

for n_days in [2, 3, 5]:
    print(f"\n  连续{n_days}日全板块资金流向趋势:")
    for sector in sectors:
        sec_data = [d for d in details if d['板块'] == sector]
        fund_ts = sector_fund_series[sector]
        dates_sorted = sorted(fund_ts.keys())

        consec_in_ok, consec_in_n = 0, 0
        consec_out_ok, consec_out_n = 0, 0
        mixed_ok, mixed_n = 0, 0

        for d in sec_data:
            score_date = d['评分日']
            actual_chg = parse_chg(d['实际涨跌'])

            # 找score_date及之前n_days天的板块资金流向
            recent_dates = [dt for dt in dates_sorted if dt <= score_date]
            if len(recent_dates) < n_days:
                continue

            recent_nets = [fund_ts[dt] for dt in recent_dates[-n_days:]]

            if all(n > 0 for n in recent_nets):
                consec_in_n += 1
                if actual_chg > 0:
                    consec_in_ok += 1
            elif all(n < 0 for n in recent_nets):
                consec_out_n += 1
                if actual_chg < 0:
                    consec_out_ok += 1
            else:
                mixed_n += 1
                if d['宽松正确'] == '✓':
                    mixed_ok += 1

        ci = f"{consec_in_ok}/{consec_in_n}({consec_in_ok/consec_in_n*100:.1f}%)" if consec_in_n > 0 else "N/A"
        co = f"{consec_out_ok}/{consec_out_n}({consec_out_ok/consec_out_n*100:.1f}%)" if consec_out_n > 0 else "N/A"
        mx = f"{mixed_ok}/{mixed_n}({mixed_ok/mixed_n*100:.1f}%)" if mixed_n > 0 else "N/A"
        print(f"    {sector:6s}: 连续{n_days}日净流入→涨={ci} | 连续{n_days}日净流出→跌={co} | 混合模型准确={mx}")


# ═══════════════════════════════════════════════════════════
# 分析五：全板块走势+资金流向+美股 三维组合信号
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"五、三维组合信号：全板块走势 + 全板块资金流向 + 美股隔夜")
print(f"{'=' * 80}")

combo3_stats = defaultdict(lambda: {'ok': 0, 'n': 0})

for d in details:
    sector = d['板块']
    score_date = d['评分日']
    actual_chg = parse_chg(d['实际涨跌'])

    # 信号1: 全板块走势方向
    chg_key = (sector, score_date)
    chg_data = sector_daily_chg.get(chg_key)
    sector_signal = 0
    if chg_data:
        if chg_data['up_pct'] > 0.6:
            sector_signal = 1
        elif chg_data['down_pct'] > 0.6:
            sector_signal = -1

    # 信号2: 全板块资金流向
    fund_key = (sector, score_date)
    fund_data = sector_daily_fund.get(fund_key)
    fund_signal = 0
    if fund_data:
        if fund_data['inflow_pct'] > 0.55:
            fund_signal = 1
        elif fund_data['outflow_pct'] > 0.55:
            fund_signal = -1

    # 信号3: 美股隔夜
    us_chg = d.get('美股涨跌(%)')
    us_signal = 0
    if us_chg is not None:
        if us_chg > 0.5:
            us_signal = 1
        elif us_chg < -0.5:
            us_signal = -1

    # 组合分类
    signals = (sector_signal, fund_signal, us_signal)
    signal_sum = sector_signal + fund_signal + us_signal

    if signal_sum >= 2:
        combo = '多数看涨(≥2)'
    elif signal_sum <= -2:
        combo = '多数看跌(≤-2)'
    elif signal_sum == 0 and sector_signal == 0:
        combo = '全无信号'
    else:
        combo = '信号分歧'

    combo3_stats[combo]['n'] += 1
    if d['宽松正确'] == '✓':
        combo3_stats[combo]['ok'] += 1

for combo_name in ['多数看涨(≥2)', '多数看跌(≤-2)', '信号分歧', '全无信号']:
    s = combo3_stats[combo_name]
    if s['n'] > 0:
        rate = s['ok'] / s['n'] * 100
        print(f"  {combo_name:16s}: {s['ok']}/{s['n']} ({rate:.1f}%)")

# 5b. 按板块的三维组合
print(f"\n  5b. 按板块的三维组合效果:")
for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]

    majority_bull_ok, majority_bull_n = 0, 0
    majority_bear_ok, majority_bear_n = 0, 0

    for d in sec_data:
        score_date = d['评分日']

        chg_data = sector_daily_chg.get((sector, score_date))
        fund_data = sector_daily_fund.get((sector, score_date))

        s1 = 0
        if chg_data:
            if chg_data['up_pct'] > 0.6:
                s1 = 1
            elif chg_data['down_pct'] > 0.6:
                s1 = -1

        s2 = 0
        if fund_data:
            if fund_data['inflow_pct'] > 0.55:
                s2 = 1
            elif fund_data['outflow_pct'] > 0.55:
                s2 = -1

        us_chg = d.get('美股涨跌(%)')
        s3 = 0
        if us_chg is not None:
            if us_chg > 0.5:
                s3 = 1
            elif us_chg < -0.5:
                s3 = -1

        signal_sum = s1 + s2 + s3
        if signal_sum >= 2:
            majority_bull_n += 1
            if d['宽松正确'] == '✓':
                majority_bull_ok += 1
        elif signal_sum <= -2:
            majority_bear_n += 1
            if d['宽松正确'] == '✓':
                majority_bear_ok += 1

    mb = f"{majority_bull_ok}/{majority_bull_n}({majority_bull_ok/majority_bull_n*100:.1f}%)" if majority_bull_n > 0 else "N/A"
    mbe = f"{majority_bear_ok}/{majority_bear_n}({majority_bear_ok/majority_bear_n*100:.1f}%)" if majority_bear_n > 0 else "N/A"
    print(f"    {sector:6s}: 多数看涨模型准确={mb} | 多数看跌模型准确={mbe}")


# ═══════════════════════════════════════════════════════════
# 分析六：全板块信号能否修正模型错误预测
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"六、全板块信号修正模型错误预测的潜力分析")
print(f"{'=' * 80}")

# 找出模型预测错误的样本，看全板块信号是否给出了正确方向
wrong = [d for d in details if d['宽松正确'] == '✗']
print(f"  模型错误样本: {len(wrong)}个")

# 对每个错误样本，检查全板块信号是否能修正
correctable_by_sector_chg = 0
correctable_by_sector_fund = 0
correctable_by_both = 0

for d in wrong:
    sector = d['板块']
    score_date = d['评分日']
    actual_chg = parse_chg(d['实际涨跌'])
    pred = d['预测方向']

    chg_data = sector_daily_chg.get((sector, score_date))
    fund_data = sector_daily_fund.get((sector, score_date))

    # 全板块走势信号是否给出正确方向
    sector_correct = False
    if chg_data:
        if actual_chg > 0 and chg_data['up_pct'] > 0.6:
            sector_correct = True
        elif actual_chg < 0 and chg_data['down_pct'] > 0.6:
            sector_correct = True

    # 全板块资金流向是否给出正确方向
    fund_correct = False
    if fund_data:
        if actual_chg > 0 and fund_data['inflow_pct'] > 0.55:
            fund_correct = True
        elif actual_chg < 0 and fund_data['outflow_pct'] > 0.55:
            fund_correct = True

    if sector_correct:
        correctable_by_sector_chg += 1
    if fund_correct:
        correctable_by_sector_fund += 1
    if sector_correct and fund_correct:
        correctable_by_both += 1

print(f"  全板块走势信号可修正: {correctable_by_sector_chg}/{len(wrong)} ({correctable_by_sector_chg/len(wrong)*100:.1f}%)")
print(f"  全板块资金流向可修正: {correctable_by_sector_fund}/{len(wrong)} ({correctable_by_sector_fund/len(wrong)*100:.1f}%)")
print(f"  两者同时可修正: {correctable_by_both}/{len(wrong)} ({correctable_by_both/len(wrong)*100:.1f}%)")

# 6b. 按板块分析可修正比例
print(f"\n  6b. 按板块的可修正比例:")
for sector in sectors:
    sec_wrong = [d for d in wrong if d['板块'] == sector]
    if not sec_wrong:
        continue

    chg_fix, fund_fix, both_fix = 0, 0, 0
    for d in sec_wrong:
        score_date = d['评分日']
        actual_chg = parse_chg(d['实际涨跌'])

        chg_data = sector_daily_chg.get((sector, score_date))
        fund_data = sector_daily_fund.get((sector, score_date))

        sc = False
        if chg_data:
            if actual_chg > 0 and chg_data['up_pct'] > 0.6:
                sc = True
            elif actual_chg < 0 and chg_data['down_pct'] > 0.6:
                sc = True

        fc = False
        if fund_data:
            if actual_chg > 0 and fund_data['inflow_pct'] > 0.55:
                fc = True
            elif actual_chg < 0 and fund_data['outflow_pct'] > 0.55:
                fc = True

        if sc:
            chg_fix += 1
        if fc:
            fund_fix += 1
        if sc and fc:
            both_fix += 1

    print(f"    {sector:6s}: 错误{len(sec_wrong)}个 | 走势可修正={chg_fix}({chg_fix/len(sec_wrong)*100:.1f}%) | 资金可修正={fund_fix}({fund_fix/len(sec_wrong)*100:.1f}%) | 双修正={both_fix}({both_fix/len(sec_wrong)*100:.1f}%)")


# ═══════════════════════════════════════════════════════════
# 分析七：全板块信号作为反转指标（板块大涨→次日回调？）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"七、全板块走势反转效应（板块大涨→次日回调？板块大跌→次日反弹？）")
print(f"{'=' * 80}")

for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]

    # 板块大涨(avg>1.5%)→次日回调
    big_up_reverse_ok, big_up_reverse_n = 0, 0
    # 板块大跌(avg<-1.5%)→次日反弹
    big_down_reverse_ok, big_down_reverse_n = 0, 0
    # 板块涨→次日继续涨（动量延续）
    momentum_up_ok, momentum_up_n = 0, 0
    # 板块跌→次日继续跌
    momentum_down_ok, momentum_down_n = 0, 0

    for d in sec_data:
        key = (sector, d['评分日'])
        chg_data = sector_daily_chg.get(key)
        if not chg_data:
            continue

        actual_chg = parse_chg(d['实际涨跌'])
        avg_chg = chg_data['avg_chg']

        if avg_chg > 1.5:
            big_up_reverse_n += 1
            if actual_chg < 0:
                big_up_reverse_ok += 1
        elif avg_chg > 0.3:
            momentum_up_n += 1
            if actual_chg > 0:
                momentum_up_ok += 1
        elif avg_chg < -1.5:
            big_down_reverse_n += 1
            if actual_chg > 0:
                big_down_reverse_ok += 1
        elif avg_chg < -0.3:
            momentum_down_n += 1
            if actual_chg < 0:
                momentum_down_ok += 1

    bur = f"{big_up_reverse_ok}/{big_up_reverse_n}({big_up_reverse_ok/big_up_reverse_n*100:.1f}%)" if big_up_reverse_n > 0 else "N/A"
    bdr = f"{big_down_reverse_ok}/{big_down_reverse_n}({big_down_reverse_ok/big_down_reverse_n*100:.1f}%)" if big_down_reverse_n > 0 else "N/A"
    mu = f"{momentum_up_ok}/{momentum_up_n}({momentum_up_ok/momentum_up_n*100:.1f}%)" if momentum_up_n > 0 else "N/A"
    md = f"{momentum_down_ok}/{momentum_down_n}({momentum_down_ok/momentum_down_n*100:.1f}%)" if momentum_down_n > 0 else "N/A"
    print(f"  {sector:6s}: 板块大涨→次跌={bur} | 板块大跌→次涨={bdr} | 板块涨→续涨={mu} | 板块跌→续跌={md}")


# ═══════════════════════════════════════════════════════════
# 分析八：全板块资金流向反转 vs 动量
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"八、全板块资金流向：反转 vs 动量效应")
print(f"{'=' * 80}")

# 板块资金大幅流入→次日个股涨？还是跌？（反转效应）
for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]

    # 资金大幅流入(>70%股票流入)→次日涨 vs 跌
    heavy_in_up, heavy_in_down, heavy_in_n = 0, 0, 0
    # 资金大幅流出(>70%股票流出)→次日涨 vs 跌
    heavy_out_up, heavy_out_down, heavy_out_n = 0, 0, 0

    for d in sec_data:
        key = (sector, d['评分日'])
        fund_data = sector_daily_fund.get(key)
        if not fund_data:
            continue

        actual_chg = parse_chg(d['实际涨跌'])

        if fund_data['inflow_pct'] > 0.7:
            heavy_in_n += 1
            if actual_chg > 0:
                heavy_in_up += 1
            elif actual_chg < 0:
                heavy_in_down += 1
        elif fund_data['outflow_pct'] > 0.7:
            heavy_out_n += 1
            if actual_chg > 0:
                heavy_out_up += 1
            elif actual_chg < 0:
                heavy_out_down += 1

    if heavy_in_n > 0:
        print(f"  {sector:6s}: >70%流入(n={heavy_in_n:3d}) → 次涨={heavy_in_up}({heavy_in_up/heavy_in_n*100:.1f}%) 次跌={heavy_in_down}({heavy_in_down/heavy_in_n*100:.1f}%)")
    if heavy_out_n > 0:
        print(f"  {sector:6s}: >70%流出(n={heavy_out_n:3d}) → 次涨={heavy_out_up}({heavy_out_up/heavy_out_n*100:.1f}%) 次跌={heavy_out_down}({heavy_out_down/heavy_out_n*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 分析九：全板块走势+资金流向 矛盾信号分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"九、全板块走势与资金流向矛盾信号（板块涨但资金流出 / 板块跌但资金流入）")
print(f"{'=' * 80}")

for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]

    # 板块涨+资金流出 → 次日？
    up_outflow_ok, up_outflow_n = 0, 0
    # 板块跌+资金流入 → 次日？
    down_inflow_ok, down_inflow_n = 0, 0
    # 板块涨+资金流入（一致看涨）
    up_inflow_ok, up_inflow_n = 0, 0
    # 板块跌+资金流出（一致看跌）
    down_outflow_ok, down_outflow_n = 0, 0

    for d in sec_data:
        score_date = d['评分日']
        actual_chg = parse_chg(d['实际涨跌'])

        chg_data = sector_daily_chg.get((sector, score_date))
        fund_data = sector_daily_fund.get((sector, score_date))
        if not chg_data or not fund_data:
            continue

        sector_up = chg_data['avg_chg'] > 0.3
        sector_down = chg_data['avg_chg'] < -0.3
        fund_in = fund_data['inflow_pct'] > 0.55
        fund_out = fund_data['outflow_pct'] > 0.55

        if sector_up and fund_out:
            up_outflow_n += 1
            if actual_chg < 0:  # 板块涨+资金流出→次日跌？
                up_outflow_ok += 1
        elif sector_down and fund_in:
            down_inflow_n += 1
            if actual_chg > 0:  # 板块跌+资金流入→次日涨？
                down_inflow_ok += 1
        elif sector_up and fund_in:
            up_inflow_n += 1
            if actual_chg > 0:  # 一致看涨→次日涨？
                up_inflow_ok += 1
        elif sector_down and fund_out:
            down_outflow_n += 1
            if actual_chg < 0:  # 一致看跌→次日跌？
                down_outflow_ok += 1

    uo = f"{up_outflow_ok}/{up_outflow_n}({up_outflow_ok/up_outflow_n*100:.1f}%)" if up_outflow_n > 0 else "N/A"
    di = f"{down_inflow_ok}/{down_inflow_n}({down_inflow_ok/down_inflow_n*100:.1f}%)" if down_inflow_n > 0 else "N/A"
    ui = f"{up_inflow_ok}/{up_inflow_n}({up_inflow_ok/up_inflow_n*100:.1f}%)" if up_inflow_n > 0 else "N/A"
    do = f"{down_outflow_ok}/{down_outflow_n}({down_outflow_ok/down_outflow_n*100:.1f}%)" if down_outflow_n > 0 else "N/A"
    print(f"  {sector:6s}: 涨+流出→次跌={uo} | 跌+流入→次涨={di} | 涨+流入→次涨={ui} | 跌+流出→次跌={do}")


# ═══════════════════════════════════════════════════════════
# 分析十：模拟优化 — 用全板块信号修正模型预测
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"十、模拟优化：用全板块信号修正模型预测的效果估算")
print(f"{'=' * 80}")

# 策略：当全板块信号与模型预测矛盾时，考虑翻转
# 规则1: 板块>65%下跌 + 模型预测上涨 → 翻转为下跌
# 规则2: 板块>65%上涨 + 模型预测下跌 → 翻转为上涨
# 规则3: 板块资金>65%流出 + 模型预测上涨 → 翻转为下跌
# 规则4: 板块资金>65%流入 + 模型预测下跌 → 翻转为上涨

strategies = [
    ('策略A: 仅板块走势翻转(65%)', 0.65, False),
    ('策略B: 仅板块走势翻转(60%)', 0.60, False),
    ('策略C: 板块走势+资金双确认翻转(60%)', 0.60, True),
    ('策略D: 仅板块走势翻转(55%)', 0.55, False),
]

for strategy_name, threshold, require_fund in strategies:
    new_correct = 0
    flipped = 0
    flip_correct = 0
    flip_wrong = 0

    for d in details:
        sector = d['板块']
        score_date = d['评分日']
        pred = d['预测方向']
        actual_chg = parse_chg(d['实际涨跌'])
        original_ok = (d['宽松正确'] == '✓')

        chg_data = sector_daily_chg.get((sector, score_date))
        fund_data = sector_daily_fund.get((sector, score_date))

        should_flip = False

        if chg_data:
            if pred == '上涨' and chg_data['down_pct'] > threshold:
                if require_fund:
                    if fund_data and fund_data['outflow_pct'] > threshold:
                        should_flip = True
                else:
                    should_flip = True
            elif pred == '下跌' and chg_data['up_pct'] > threshold:
                if require_fund:
                    if fund_data and fund_data['inflow_pct'] > threshold:
                        should_flip = True
                else:
                    should_flip = True

        if should_flip:
            flipped += 1
            new_pred = '下跌' if pred == '上涨' else '上涨'
            # 重新判断宽松正确
            if new_pred == '上涨' and actual_chg >= 0:
                new_correct += 1
                flip_correct += 1
            elif new_pred == '下跌' and actual_chg <= 0:
                new_correct += 1
                flip_correct += 1
            else:
                flip_wrong += 1
                # 翻转后错了，但原来可能也错了
        else:
            if original_ok:
                new_correct += 1

    delta = new_correct - loose_ok
    print(f"\n  {strategy_name}:")
    print(f"    翻转样本: {flipped}个")
    print(f"    翻转后正确: {flip_correct}/{flipped} ({flip_correct/flipped*100:.1f}%)" if flipped > 0 else "    翻转样本: 0个")
    print(f"    新准确率: {new_correct}/{total} ({new_correct/total*100:.1f}%)")
    print(f"    变化: {delta:+d}个样本 ({delta/total*100:+.1f}pp)")

# 按板块的最优策略
print(f"\n  按板块的策略C效果:")
threshold_c = 0.60
for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    sec_ok_orig = sum(1 for d in sec_data if d['宽松正确'] == '✓')
    sec_ok_new = 0
    sec_flipped = 0

    for d in sec_data:
        score_date = d['评分日']
        pred = d['预测方向']
        actual_chg = parse_chg(d['实际涨跌'])
        original_ok = (d['宽松正确'] == '✓')

        chg_data = sector_daily_chg.get((sector, score_date))
        fund_data = sector_daily_fund.get((sector, score_date))

        should_flip = False
        if chg_data:
            if pred == '上涨' and chg_data['down_pct'] > threshold_c:
                if fund_data and fund_data['outflow_pct'] > threshold_c:
                    should_flip = True
            elif pred == '下跌' and chg_data['up_pct'] > threshold_c:
                if fund_data and fund_data['inflow_pct'] > threshold_c:
                    should_flip = True

        if should_flip:
            sec_flipped += 1
            new_pred = '下跌' if pred == '上涨' else '上涨'
            if (new_pred == '上涨' and actual_chg >= 0) or (new_pred == '下跌' and actual_chg <= 0):
                sec_ok_new += 1
        else:
            if original_ok:
                sec_ok_new += 1

    delta = sec_ok_new - sec_ok_orig
    print(f"    {sector:6s}: 原{sec_ok_orig}/{len(sec_data)}({sec_ok_orig/len(sec_data)*100:.1f}%) → 新{sec_ok_new}/{len(sec_data)}({sec_ok_new/len(sec_data)*100:.1f}%) 翻转{sec_flipped}个 变化{delta:+d}")

# ═══════════════════════════════════════════════════════════
# 总结
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析完成")
print(f"{'=' * 80}")
