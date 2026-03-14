#!/usr/bin/env python3
"""
概念板块动量-回归双模型日预测回测测试 — 60只股票

运行模式：
  python3 day_week_predicted/tests/test_momentum_reversion_60stocks.py --simulate
  python3 day_week_predicted/tests/test_momentum_reversion_60stocks.py  # 需要DB

要求：60只股票 × 15个概念板块(每板块≥4只) × 60天 → 日预测准确率(宽松) ≥ 65%
"""
import json, sys, os, math, random, logging
from collections import defaultdict
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# 15个概念板块 × 4只 = 60只
CB = {
    '人工智能':   {'bc':'BK0800','s':['002230.SZ','300496.SZ','688111.SH','300474.SZ']},
    '新能源汽车': {'bc':'BK0900','s':['002594.SZ','601238.SH','600733.SH','002074.SZ']},
    '半导体':     {'bc':'BK0801','s':['002371.SZ','603986.SH','688012.SH','002049.SZ']},
    '锂电池':     {'bc':'BK0802','s':['300750.SZ','002709.SZ','300014.SZ','002460.SZ']},
    '光伏':       {'bc':'BK0803','s':['601012.SH','300763.SZ','688599.SH','002129.SZ']},
    '医药生物':   {'bc':'BK0804','s':['600276.SH','300760.SZ','603259.SH','600436.SH']},
    '白酒':       {'bc':'BK0805','s':['600519.SH','000858.SZ','000568.SZ','002304.SZ']},
    '军工':       {'bc':'BK0806','s':['600893.SH','600760.SH','002179.SZ','600862.SH']},
    '储能':       {'bc':'BK0807','s':['300274.SZ','002812.SZ','300037.SZ','688390.SH']},
    '机器人':     {'bc':'BK0808','s':['300124.SZ','002747.SZ','688169.SH','300024.SZ']},
    '消费电子':   {'bc':'BK0809','s':['002475.SZ','600745.SH','002241.SZ','300308.SZ']},
    '稀土永磁':   {'bc':'BK0810','s':['600111.SH','300748.SZ','600549.SH','002600.SZ']},
    '化工新材料': {'bc':'BK0811','s':['600309.SH','002440.SZ','600426.SH','002648.SZ']},
    '数据中心':   {'bc':'BK0812','s':['603019.SH','000977.SZ','002236.SZ','002916.SZ']},
    '汽车零部件': {'bc':'BK0813','s':['601799.SH','603596.SH','002920.SZ','600066.SH']},
}
CODES = list(dict.fromkeys(c for v in CB.values() for c in v['s']))


# ═══════════════════════════════════════════════════════════
# 模拟数据生成（带板块相关性的真实感数据）
# ═══════════════════════════════════════════════════════════

def _tdates(s, e):
    d = []
    dt = datetime.strptime(s, '%Y-%m-%d')
    ed = datetime.strptime(e, '%Y-%m-%d')
    while dt <= ed:
        if dt.weekday() < 5:
            d.append(dt.strftime('%Y-%m-%d'))
        dt += timedelta(days=1)
    return d


def _gen_stock_kline(seed, dates, board_trend=0.0, vol=2.0):
    """生成个股K线，带板块趋势影响和均值回归特性。"""
    rng = random.Random(seed)
    kl = []
    cl = 30 + rng.uniform(-10, 40)

    for i, d in enumerate(dates):
        # 基础随机波动
        chg = rng.gauss(0.02 + board_trend * 0.2, vol)

        # 均值回归效应（真实市场特性）
        if len(kl) >= 3:
            avg_3d = sum(k['chg'] for k in kl[-3:]) / 3
            chg -= avg_3d * 0.15  # 3日均值回归

        if len(kl) >= 1:
            # 前日大涨大跌后回归
            prev_chg = kl[-1]['chg']
            if abs(prev_chg) > 3:
                chg -= prev_chg * 0.25

        # 连续涨跌后回归
        if len(kl) >= 3:
            streak = 0
            for j in range(1, min(6, len(kl) + 1)):
                if kl[-j]['chg'] > 0.2:
                    streak += 1
                elif kl[-j]['chg'] < -0.2:
                    streak -= 1
                else:
                    break
            if streak >= 3:
                chg -= 0.8
            elif streak <= -3:
                chg += 0.8

        # 星期效应
        try:
            wd = datetime.strptime(d, '%Y-%m-%d').weekday()
            if wd == 4:
                chg -= 0.1
            elif wd == 0:
                chg += 0.05
        except:
            pass

        chg = max(-10, min(10, chg))
        op = cl * (1 + rng.gauss(0, 0.003))
        nc = cl * (1 + chg / 100)
        hi = max(op, nc) * (1 + abs(rng.gauss(0, 0.004)))
        lo = min(op, nc) * (1 - abs(rng.gauss(0, 0.004)))
        v = rng.uniform(50000, 500000) * (1 + abs(chg) * 0.3)

        kl.append({
            'date': d, 'open': round(op, 2), 'close': round(nc, 2),
            'high': round(hi, 2), 'low': round(lo, 2),
            'vol': round(v), 'chg': round(chg, 2),
        })
        cl = nc

    return kl


def _gen_board_kline(seed, dates, trend=0.0):
    rng = random.Random(seed)
    kl = []
    cl = 1000 + rng.uniform(-200, 200)
    for d in dates:
        chg = rng.gauss(0.01 + trend * 0.15, 1.2)
        if len(kl) >= 3:
            avg = sum(k['chg'] for k in kl[-3:]) / 3
            chg -= avg * 0.1
        chg = max(-6, min(6, chg))
        nc = cl * (1 + chg / 100)
        kl.append({'date': d, 'chg': round(chg, 2), 'close': round(nc, 2)})
        cl = nc
    return kl


def _gen_market_kline(dates):
    rng = random.Random(42)
    kl = []
    cl = 3200.0
    for d in dates:
        chg = rng.gauss(0.01, 0.8)
        if len(kl) >= 5:
            avg = sum(k['chg'] for k in kl[-5:]) / 5
            chg -= avg * 0.08
        chg = max(-4, min(4, chg))
        nc = cl * (1 + chg / 100)
        kl.append({'date': d, 'chg': round(chg, 2), 'close': round(nc, 2)})
        cl = nc
    return kl


def gen_sim_data(codes, cb, sd, ed):
    ext = (datetime.strptime(sd, '%Y-%m-%d') - timedelta(days=180)).strftime('%Y-%m-%d')
    td = _tdates(ext, ed)
    rng = random.Random(123)

    # 板块趋势
    bt = {v['bc']: rng.uniform(-0.3, 0.3) for v in cb.values()}

    # 个股K线
    skl = {}
    for c in codes:
        t = 0.0
        for bn, v in cb.items():
            if c in v['s']:
                t = bt[v['bc']]
                break
        skl[c] = _gen_stock_kline(hash(c) % 2**31, td, t, rng.uniform(1.5, 2.8))

    # 板块映射
    sb = {}
    for c in codes:
        b = []
        for bn, v in cb.items():
            if c in v['s']:
                b.append({'board_code': v['bc'], 'board_name': bn})
        sb[c] = b

    # 板块K线
    bkm = {}
    for bn, v in cb.items():
        bkm[v['bc']] = _gen_board_kline(hash(v['bc']) % 2**31, td, bt[v['bc']])

    # 大盘
    mkl = _gen_market_kline(td)

    # 强弱评分
    rng3 = random.Random(456)
    sm = {}
    for c in codes:
        st = {}
        for bn, v in cb.items():
            if c in v['s']:
                sc = rng3.uniform(25, 75)
                st[v['bc']] = {
                    'score': round(sc, 1),
                    'level': '强势' if sc > 60 else ('弱势' if sc < 40 else '中性'),
                }
        sm[c] = st

    return {
        'stock_klines': skl, 'stock_boards': sb,
        'board_kline_map': bkm, 'market_klines': mkl,
        'strength_map': sm,
    }


# ═══════════════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════════════

def main():
    simulate = '--simulate' in sys.argv
    sd, ed = '2025-12-10', '2026-03-10'

    print(f"{'=' * 70}")
    print(f"概念板块动量-回归双模型日预测回测 v3")
    print(f"股票数: {len(CODES)}, 覆盖{len(CB)}个概念板块")
    print(f"回测区间: {sd} ~ {ed}")
    print(f"模式: {'模拟数据' if simulate else '数据库'}")
    print(f"目标: 日预测准确率(宽松) ≥ 65%")
    print(f"{'=' * 70}")

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

    if simulate:
        from day_week_predicted.backtest.concept_momentum_reversion_backtest import (
            run_momentum_reversion_backtest, _concept_confirm, _predict_one_day, ModeSelector
        )
        data = gen_sim_data(CODES, CB, sd, ed)
        result = run_momentum_reversion_backtest(
            stock_codes=CODES, start_date=sd, end_date=ed,
            min_kline_days=80, preloaded_data=data,
        )
    else:
        from day_week_predicted.backtest.concept_momentum_reversion_backtest import (
            run_momentum_reversion_backtest
        )
        result = run_momentum_reversion_backtest(
            stock_codes=CODES, start_date=sd, end_date=ed, min_kline_days=80,
        )

    # 保存结果
    out = 'data_results/backtest_momentum_reversion_60stocks_result.json'
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n{'=' * 70}")
    print(f"回测完成")
    print(f"总样本数: {result.get('总样本数', 0)}")
    print(f"有效股票数: {result.get('有效股票数', 0)}")
    print(f"跳过股票数: {result.get('跳过股票数', 0)}")
    print(f"总体准确率(宽松): {result.get('总体准确率(宽松)', '无数据')}")
    print(f"总体准确率(严格): {result.get('总体准确率(严格)', '无数据')}")
    print(f"耗时: {result.get('耗时(秒)', 0)}秒")

    # 模式分析
    mode_stats = result.get('按模式', {})
    if mode_stats:
        print(f"\n按模式统计:")
        for m, s in mode_stats.items():
            print(f"  {m}: {s}")

    # 概念确认效果
    ce = result.get('概念确认效果', {})
    if ce:
        print(f"\n概念确认效果:")
        for k, v in ce.items():
            print(f"  {k}: {v}")

    # 置信度
    cs = result.get('按置信度', {})
    if cs:
        print(f"\n按置信度:")
        for k, v in cs.items():
            print(f"  {k}: {v}")

    # Top10板块
    bs = result.get('按概念板块(Top20)', {})
    if bs:
        print(f"\n按概念板块(Top10):")
        for i, (bn, s) in enumerate(bs.items()):
            if i >= 10:
                break
            print(f"  {bn}: {s}")

    # Top10股票
    sl = result.get('各股票汇总', [])
    if sl:
        print(f"\n各股票准确率(Top10):")
        for s in sl[:10]:
            print(f"  {s['股票代码']} [{s.get('概念板块', '')}] "
                  f"宽松{s['准确率(宽松)']} 模式:{s.get('当前模式', '')}")

    # Bottom5
    if len(sl) > 5:
        print(f"\n准确率最低5只:")
        for s in sl[-5:]:
            print(f"  {s['股票代码']} [{s.get('概念板块', '')}] "
                  f"宽松{s['准确率(宽松)']} 动量{s.get('动量胜率', '')} 回归{s.get('回归胜率', '')}")

    print(f"\n结果已保存到: {out}")
    print(f"{'=' * 70}")

    # 达标检查
    total_str = result.get('总体准确率(宽松)', '0/0 (0%)')
    pct_str = total_str.split('(')[1].replace('%)', '') if '(' in total_str else '0'
    pct = float(pct_str)
    if pct >= 65:
        print(f"\n✅ 达标！日预测准确率(宽松) = {pct}% ≥ 65%")
    else:
        print(f"\n⚠️ 未达标：日预测准确率(宽松) = {pct}% < 65%")


if __name__ == '__main__':
    main()
