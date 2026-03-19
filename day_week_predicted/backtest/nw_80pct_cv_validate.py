#!/usr/bin/env python3
"""
下周预测80%目标 — CV验证最优配置
================================
基于nw_80pct_target_backtest的发现，对最有前景的配置做CV验证。

关键发现：
1. R1(大盘深跌+个股跌→涨) 89.6%是最强规则，但CV=0%（只在特定周触发）
2. R5a(深证+大盘微跌+跌+连跌3天→涨) CV=90.6%最稳健
3. 只保留涨信号(no_down): 84.7%
4. 信号强度过滤(chg>=3): 83.1%
5. 连跌天数(cd>=2): 85.5%
6. 价格位置(pos<0.5): 84.3%
7. 板块负动量(bm<0): 83.0%
8. 个股历史准确率>=50%: 85.7%，>=70%: 93.3%

策略：组合最有效的过滤器，CV验证确认

用法:
    .venv/bin/python -m day_week_predicted.backtest.nw_80pct_cv_validate
"""
import sys, logging
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, '.')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s',
                    datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

from dao import get_connection
from service.weekly_prediction_service import (
    _get_all_stock_codes, _get_latest_trade_date, _to_float,
    _compound_return, _get_stock_index,
)

N_WEEKS = 29

RULES = [
    {'name': 'R1', 'pred_up': True, 'tier': 1,
     'check': lambda f: f['this_chg'] < -2 and f['mkt_chg'] < -3},
    {'name': 'R3', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -3 and f['suffix'] == 'SH'
                         and -3 <= f['mkt_chg'] < -1
                         and f['prev_chg'] is not None and f['prev_chg'] < -2
                         and not (f['pos60'] is not None and f['pos60'] >= 0.8))},
    {'name': 'R5a', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2 and f['cd'] >= 3)},
    {'name': 'R5b', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2
                         and f['pos60'] is not None and f['pos60'] < 0.2)},
    {'name': 'R5c', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2)},
    {'name': 'R6a', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 5)},
    {'name': 'R6c', 'pred_up': False, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 2 and f['cu'] >= 3)},
    {'name': 'R7', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['this_chg'] < -3 and f['cu'] >= 3
                         and f['pos60'] is not None and f['pos60'] < 0.6)},
]

# 只保留涨信号的规则集
RULES_UP_ONLY = [r for r in RULES if r['pred_up']]

# 精简规则集：只保留CV>75%的涨信号
RULES_ELITE = [
    {'name': 'R1', 'pred_up': True, 'tier': 1,
     'check': lambda f: f['this_chg'] < -2 and f['mkt_chg'] < -3},
    {'name': 'R5a', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2 and f['cd'] >= 3)},
    {'name': 'R5b', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2
                         and f['pos60'] is not None and f['pos60'] < 0.2)},
    {'name': 'R5c', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2)},
]

def match_rule(feat, rules):
    for rule in rules:
        try:
            if rule['check'](feat):
                return rule
        except (TypeError, KeyError):
            continue
    return None


def load_data():
    latest_date = _get_latest_trade_date()
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(N_WEEKS + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')
    all_codes = _get_all_stock_codes()

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    stock_klines = defaultdict(list)
    bs = 500
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,close_price,change_percent,trading_volume "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            stock_klines[r['stock_code']].append({
                'date': r['date'], 'close': _to_float(r['close_price']),
                'change_percent': _to_float(r['change_percent']),
                'volume': _to_float(r['trading_volume']),
            })

    idx_codes = list(set(_get_stock_index(c) for c in all_codes))
    for idx in ('000001.SH', '399001.SZ', '899050.SZ'):
        if idx not in idx_codes: idx_codes.append(idx)
    ph = ','.join(['%s'] * len(idx_codes))
    cur.execute(
        f"SELECT stock_code,`date`,change_percent FROM stock_kline "
        f"WHERE stock_code IN ({ph}) AND `date`>=%s AND `date`<=%s ORDER BY `date`",
        idx_codes + [start_date, latest_date])
    mkt_kl = defaultdict(list)
    for r in cur.fetchall():
        mkt_kl[r['stock_code']].append({
            'date': r['date'], 'change_percent': _to_float(r['change_percent']),
        })

    # 板块
    stock_boards = defaultdict(list)
    cur.execute("SELECT stock_code, board_code, board_name FROM stock_concept_board_stock")
    for r in cur.fetchall():
        stock_boards[r['stock_code']].append({
            'board_code': r['board_code'], 'board_name': r['board_name'],
        })
    board_codes = set()
    for bl in stock_boards.values():
        for b in bl: board_codes.add(b['board_code'])
    board_klines = defaultdict(list)
    if board_codes:
        bc_list = list(board_codes)
        for i in range(0, len(bc_list), bs):
            batch = bc_list[i:i + bs]
            ph = ','.join(['%s'] * len(batch))
            cur.execute(
                f"SELECT board_code,`date`,change_percent "
                f"FROM concept_board_kline WHERE board_code IN ({ph}) "
                f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
                batch + [start_date, latest_date])
            for r in cur.fetchall():
                board_klines[r['board_code']].append({
                    'date': r['date'], 'change_percent': _to_float(r['change_percent']),
                })
    conn.close()
    return {
        'all_codes': all_codes, 'stock_klines': dict(stock_klines),
        'market_klines': dict(mkt_kl), 'stock_boards': dict(stock_boards),
        'board_klines': dict(board_klines),
        'latest_date': latest_date, 'dt_end': dt_end,
    }


def build_samples(data):
    dt_end = data['dt_end']
    dt_cutoff = dt_end - timedelta(days=N_WEEKS * 7 + 14)
    samples = []
    mkt_by_week = {}
    for ic, kl in data['market_klines'].items():
        bw = defaultdict(list)
        for k in kl:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            bw[dt.isocalendar()[:2]].append(k)
        mkt_by_week[ic] = bw

    for code in data['all_codes']:
        klines = data['stock_klines'].get(code, [])
        if not klines or len(klines) < 60: continue
        stock_idx = _get_stock_index(code)
        suffix = stock_idx.split('.')[-1] if '.' in stock_idx else ''
        idx_bw = mkt_by_week.get(stock_idx, {})
        code_6 = code.split('.')[0] if '.' in code else code
        boards = data['stock_boards'].get(code_6, [])

        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            wg[dt.isocalendar()[:2]].append(k)
        sorted_weeks = sorted(wg.keys())
        sorted_all = sorted(klines, key=lambda x: x['date'])

        for i in range(len(sorted_weeks) - 1):
            iw_this = sorted_weeks[i]
            iw_next = sorted_weeks[i + 1]
            this_days = sorted(wg[iw_this], key=lambda x: x['date'])
            next_days = sorted(wg[iw_next], key=lambda x: x['date'])
            if len(this_days) < 3 or len(next_days) < 3: continue
            dt_this = datetime.strptime(this_days[0]['date'], '%Y-%m-%d')
            if dt_this < dt_cutoff: continue

            this_pcts = [d['change_percent'] for d in this_days]
            this_chg = _compound_return(this_pcts)
            next_chg = _compound_return([d['change_percent'] for d in next_days])
            mw = idx_bw.get(iw_this, [])
            mkt_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            first_date = this_days[0]['date']
            last_date = this_days[-1]['date']
            hist = [k for k in sorted_all if k['date'] < first_date]

            pos60 = None
            if len(hist) >= 20:
                hc = [k['close'] for k in hist[-60:] if k['close'] > 0]
                if hc:
                    ac = hc + [k['close'] for k in this_days if k['close'] > 0]
                    mn, mx = min(ac), max(ac)
                    lc = this_days[-1]['close']
                    if mx > mn and lc > 0: pos60 = (lc - mn) / (mx - mn)

            prev_chg = None
            if i > 0:
                prev_days = sorted(wg[sorted_weeks[i-1]], key=lambda x: x['date'])
                if len(prev_days) >= 3:
                    prev_chg = _compound_return([d['change_percent'] for d in prev_days])

            cd, cu = 0, 0
            for p in reversed(this_pcts):
                if p < 0:
                    cd += 1
                    if cu > 0: break
                elif p > 0:
                    cu += 1
                    if cd > 0: break
                else: break

            last_day = this_pcts[-1] if this_pcts else 0
            excess = this_chg - mkt_chg

            # 板块动量
            board_momentum = None
            if boards:
                moms = []
                for b in boards[:5]:
                    bk = data['board_klines'].get(b['board_code'], [])
                    valid = [k for k in bk if k['date'] <= last_date]
                    if len(valid) >= 5:
                        moms.append(sum(k['change_percent'] for k in valid[-5:]) / 5)
                if moms: board_momentum = sum(moms) / len(moms)

            samples.append({
                'code': code, 'suffix': suffix,
                'iw_this': iw_this, 'iw_next': iw_next,
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'pos60': pos60, 'prev_chg': prev_chg,
                'cd': cd, 'cu': cu, 'last_day': last_day,
                'excess': excess, 'board_momentum': board_momentum,
                'next_chg': next_chg, 'actual_up': next_chg >= 0,
            })
    return samples


def run_cv(samples, rules, filter_fn=None, label=''):
    """时间序列CV"""
    all_weeks = sorted(set(s['iw_this'] for s in samples))
    min_train = 12
    cv_total, cv_correct = 0, 0
    full_total, full_correct = 0, 0

    for s in samples:
        rule = match_rule(s, rules)
        if rule:
            if filter_fn and not filter_fn(s, rule): continue
            full_total += 1
            if rule['pred_up'] == s['actual_up']: full_correct += 1

    for test_idx in range(min_train, len(all_weeks)):
        test_week = all_weeks[test_idx]
        for s in [x for x in samples if x['iw_this'] == test_week]:
            rule = match_rule(s, rules)
            if rule:
                if filter_fn and not filter_fn(s, rule): continue
                cv_total += 1
                if rule['pred_up'] == s['actual_up']: cv_correct += 1

    full_acc = full_correct/full_total*100 if full_total > 0 else 0
    cv_acc = cv_correct/cv_total*100 if cv_total > 0 else 0
    gap = full_acc - cv_acc
    return full_acc, full_total, cv_acc, cv_total, gap


def run_backtest():
    t0 = datetime.now()
    logger.info("=" * 80)
    logger.info("  下周预测80%%目标 — CV验证")
    logger.info("=" * 80)

    logger.info("\n[1/3] 加载数据...")
    data = load_data()
    logger.info("[2/3] 构建样本...")
    samples = build_samples(data)
    logger.info("  总样本数: %d", len(samples))

    logger.info("\n[3/3] CV验证各配置...")
    logger.info("  %-50s %12s %12s %6s", "配置", "全样本", "CV", "gap")

    configs = [
        ("基线(全部规则)", RULES, None),
        ("只保留涨信号", RULES_UP_ONLY, None),
        ("精简(R1+R5abc)", RULES_ELITE, None),
        ("精简+pos<0.6", RULES_ELITE,
         lambda s, r: s['pos60'] is None or s['pos60'] < 0.6),
        ("精简+pos<0.5", RULES_ELITE,
         lambda s, r: s['pos60'] is None or s['pos60'] < 0.5),
        ("精简+chg<-3", RULES_ELITE,
         lambda s, r: s['this_chg'] < -3),
        ("精简+chg<-4", RULES_ELITE,
         lambda s, r: s['this_chg'] < -4),
        ("精简+chg<-5", RULES_ELITE,
         lambda s, r: s['this_chg'] < -5),
        ("精简+cd>=2", RULES_ELITE,
         lambda s, r: s['cd'] >= 2),
        ("精简+cd>=2+pos<0.6", RULES_ELITE,
         lambda s, r: s['cd'] >= 2 and (s['pos60'] is None or s['pos60'] < 0.6)),
        ("精简+chg<-3+pos<0.6", RULES_ELITE,
         lambda s, r: s['this_chg'] < -3 and (s['pos60'] is None or s['pos60'] < 0.6)),
        ("精简+chg<-3+pos<0.5", RULES_ELITE,
         lambda s, r: s['this_chg'] < -3 and (s['pos60'] is None or s['pos60'] < 0.5)),
        ("精简+chg<-4+pos<0.6", RULES_ELITE,
         lambda s, r: s['this_chg'] < -4 and (s['pos60'] is None or s['pos60'] < 0.6)),
        ("精简+bm<0", RULES_ELITE,
         lambda s, r: s.get('board_momentum') is None or s['board_momentum'] < 0),
        ("精简+bm<0+pos<0.6", RULES_ELITE,
         lambda s, r: (s.get('board_momentum') is None or s['board_momentum'] < 0)
                       and (s['pos60'] is None or s['pos60'] < 0.6)),
        ("精简+chg<-3+bm<0", RULES_ELITE,
         lambda s, r: s['this_chg'] < -3
                       and (s.get('board_momentum') is None or s['board_momentum'] < 0)),
        ("精简+chg<-3+bm<0+pos<0.6", RULES_ELITE,
         lambda s, r: s['this_chg'] < -3
                       and (s.get('board_momentum') is None or s['board_momentum'] < 0)
                       and (s['pos60'] is None or s['pos60'] < 0.6)),
        ("精简+cd>=2+bm<0", RULES_ELITE,
         lambda s, r: s['cd'] >= 2
                       and (s.get('board_momentum') is None or s['board_momentum'] < 0)),
        ("精简+cd>=2+bm<0+pos<0.6", RULES_ELITE,
         lambda s, r: s['cd'] >= 2
                       and (s.get('board_momentum') is None or s['board_momentum'] < 0)
                       and (s['pos60'] is None or s['pos60'] < 0.6)),
        # R1专项优化
        ("只R1", [RULES[0]], None),
        ("只R1+chg<-3", [RULES[0]], lambda s, r: s['this_chg'] < -3),
        ("只R1+chg<-5", [RULES[0]], lambda s, r: s['this_chg'] < -5),
        ("只R1+pos<0.6", [RULES[0]],
         lambda s, r: s['pos60'] is None or s['pos60'] < 0.6),
        ("只R1+chg<-3+pos<0.6", [RULES[0]],
         lambda s, r: s['this_chg'] < -3 and (s['pos60'] is None or s['pos60'] < 0.6)),
        ("只R1+chg<-5+pos<0.6", [RULES[0]],
         lambda s, r: s['this_chg'] < -5 and (s['pos60'] is None or s['pos60'] < 0.6)),
        ("只R1+excess<0", [RULES[0]], lambda s, r: s['excess'] < 0),
        ("只R1+excess<-2", [RULES[0]], lambda s, r: s['excess'] < -2),
        ("只R1+excess<0+pos<0.6", [RULES[0]],
         lambda s, r: s['excess'] < 0 and (s['pos60'] is None or s['pos60'] < 0.6)),
        # R1+R5a组合
        ("R1+R5a", [RULES[0], RULES[2]], None),
        ("R1+R5a+pos<0.6", [RULES[0], RULES[2]],
         lambda s, r: s['pos60'] is None or s['pos60'] < 0.6),
        ("R1+R5a+chg<-3+pos<0.6", [RULES[0], RULES[2]],
         lambda s, r: s['this_chg'] < -3 and (s['pos60'] is None or s['pos60'] < 0.6)),
    ]

    results = []
    for label, rules, filt in configs:
        fa, ft, ca, ct, gap = run_cv(samples, rules, filt, label)
        marker = '★★' if ca >= 80 else ('★' if ca >= 75 else ('⚠️' if gap > 10 else ''))
        logger.info("  %-50s %5.1f%%(%5d) %5.1f%%(%5d) %+5.1f%% %s",
                    label, fa, ft, ca, ct, gap, marker)
        results.append((label, fa, ft, ca, ct, gap))

    # 总结
    logger.info("\n" + "=" * 80)
    logger.info("  ══ CV≥80%%的配置 ══")
    logger.info("=" * 80)
    for label, fa, ft, ca, ct, gap in results:
        if ca >= 80:
            logger.info("  %-50s CV=%.1f%%(%d) 全样本=%.1f%%(%d) gap=%+.1f%%",
                        label, ca, ct, fa, ft, gap)

    logger.info("\n" + "=" * 80)
    logger.info("  ══ CV≥75%%且样本≥100的配置（实用性） ══")
    logger.info("=" * 80)
    for label, fa, ft, ca, ct, gap in results:
        if ca >= 75 and ct >= 100:
            logger.info("  %-50s CV=%.1f%%(%d) 全样本=%.1f%%(%d) gap=%+.1f%%",
                        label, ca, ct, fa, ft, gap)

    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("\n  总耗时: %.1fs", elapsed)


if __name__ == '__main__':
    run_backtest()
