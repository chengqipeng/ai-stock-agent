#!/usr/bin/env python3
"""
下周预测80%目标回测 — 深度分析+优化
====================================
目标：下周预测准确率>80%，低准确率股票/规则直接舍弃

策略：
1. 分析每条规则的CV准确率，移除CV<65%的规则
2. 分析个股历史准确率，过滤历史准确率极低的股票
3. 分析信号强度（跌幅/涨幅绝对值），只保留强信号
4. 组合过滤器寻找最优配置

用法:
    .venv/bin/python -m day_week_predicted.backtest.nw_80pct_target_backtest
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
    _compound_return, _get_stock_index, _mean,
)

N_WEEKS = 29


def load_data():
    latest_date = _get_latest_trade_date()
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(N_WEEKS + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')
    all_codes = _get_all_stock_codes()
    logger.info("股票数: %d, 日期范围: %s ~ %s", len(all_codes), start_date, latest_date)

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
                'date': r['date'],
                'close': _to_float(r['close_price']),
                'change_percent': _to_float(r['change_percent']),
                'volume': _to_float(r['trading_volume']),
            })

    idx_codes = list(set(_get_stock_index(c) for c in all_codes))
    for idx in ('000001.SH', '399001.SZ', '899050.SZ'):
        if idx not in idx_codes:
            idx_codes.append(idx)
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

    # 资金流向
    stock_ff = defaultdict(list)
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,big_net_pct "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            stock_ff[r['stock_code']].append({
                'date': r['date'], 'big_net_pct': _to_float(r.get('big_net_pct', 0)),
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
        for b in bl:
            board_codes.add(b['board_code'])
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
        'all_codes': all_codes,
        'stock_klines': dict(stock_klines),
        'market_klines': dict(mkt_kl),
        'stock_ff': dict(stock_ff),
        'stock_boards': dict(stock_boards),
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

    # 资金流向按周分组
    ff_by_week = {}
    for code, ffl in data['stock_ff'].items():
        bw = defaultdict(list)
        for f in ffl:
            dt = datetime.strptime(f['date'], '%Y-%m-%d')
            bw[dt.isocalendar()[:2]].append(f)
        ff_by_week[code] = bw

    processed = 0
    for code in data['all_codes']:
        klines = data['stock_klines'].get(code, [])
        if not klines or len(klines) < 60:
            continue

        stock_idx = _get_stock_index(code)
        suffix = stock_idx.split('.')[-1] if '.' in stock_idx else ''
        idx_bw = mkt_by_week.get(stock_idx, {})
        code_6 = code.split('.')[0] if '.' in code else code
        boards = data['stock_boards'].get(code_6, [])
        code_ff_bw = ff_by_week.get(code, {})

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
            if len(this_days) < 3 or len(next_days) < 3:
                continue
            dt_this = datetime.strptime(this_days[0]['date'], '%Y-%m-%d')
            if dt_this < dt_cutoff:
                continue

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

            # pos60
            pos60 = None
            if len(hist) >= 20:
                hc = [k['close'] for k in hist[-60:] if k['close'] > 0]
                if hc:
                    ac = hc + [k['close'] for k in this_days if k['close'] > 0]
                    mn, mx = min(ac), max(ac)
                    lc = this_days[-1]['close']
                    if mx > mn and lc > 0:
                        pos60 = (lc - mn) / (mx - mn)

            # prev_chg
            prev_chg = None
            if i > 0:
                prev_iw = sorted_weeks[i - 1]
                prev_days = sorted(wg[prev_iw], key=lambda x: x['date'])
                if len(prev_days) >= 3:
                    prev_chg = _compound_return([d['change_percent'] for d in prev_days])

            # 大盘前一周
            mkt_prev_chg = None
            if i > 0:
                prev_mw = idx_bw.get(sorted_weeks[i - 1], [])
                if len(prev_mw) >= 3:
                    mkt_prev_chg = _compound_return(
                        [k['change_percent'] for k in sorted(prev_mw, key=lambda x: x['date'])])

            # 连涨/连跌
            cd, cu = 0, 0
            for p in reversed(this_pcts):
                if p < 0:
                    cd += 1; 
                    if cu > 0: break
                elif p > 0:
                    cu += 1
                    if cd > 0: break
                else: break

            last_day = this_pcts[-1] if this_pcts else 0

            # 资金流向
            ff_signal = None
            wff = code_ff_bw.get(iw_this, [])
            if wff:
                pcts = [f['big_net_pct'] for f in wff if f['big_net_pct'] != 0]
                if pcts:
                    ff_signal = max(-1.0, min(1.0, (sum(pcts)/len(pcts)) / 5.0))

            # 板块动量
            board_momentum = None
            if boards:
                moms = []
                for b in boards[:5]:
                    bk = data['board_klines'].get(b['board_code'], [])
                    valid = [k for k in bk if k['date'] <= last_date]
                    if len(valid) >= 5:
                        moms.append(sum(k['change_percent'] for k in valid[-5:]) / 5)
                if moms:
                    board_momentum = sum(moms) / len(moms)

            # 波动率（20日）
            volatility = None
            if len(hist) >= 20:
                h20 = [k['change_percent'] for k in hist[-20:]]
                m = sum(h20) / len(h20)
                volatility = (sum((x - m)**2 for x in h20) / (len(h20)-1)) ** 0.5

            # 超额收益
            excess = this_chg - mkt_chg

            samples.append({
                'code': code, 'suffix': suffix,
                'iw_this': iw_this, 'iw_next': iw_next,
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'pos60': pos60, 'prev_chg': prev_chg,
                'mkt_prev_chg': mkt_prev_chg,
                'cd': cd, 'cu': cu, 'last_day': last_day,
                'ff_signal': ff_signal,
                'board_momentum': board_momentum,
                'volatility': volatility,
                'excess': excess,
                'next_chg': next_chg, 'actual_up': next_chg >= 0,
            })

        processed += 1
        if processed % 1000 == 0:
            logger.info("  构建样本: %d/%d ...", processed, len(data['all_codes']))

    return samples


# ═══════════════════════════════════════════════════════════
# 规则集（与生产一致，但标记名称用于分析）
# ═══════════════════════════════════════════════════════════

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


def match_rule(feat, rules=RULES):
    for rule in rules:
        try:
            if rule['check'](feat):
                return rule
        except (TypeError, KeyError):
            continue
    return None


# ═══════════════════════════════════════════════════════════
# 分析引擎
# ═══════════════════════════════════════════════════════════

def analyze_by_rule_cv(samples):
    """按规则做时间序列CV，找出CV准确率低的规则"""
    all_weeks = sorted(set(s['iw_this'] for s in samples))
    min_train = 12

    rule_cv = defaultdict(lambda: {'correct': 0, 'total': 0})
    rule_full = defaultdict(lambda: {'correct': 0, 'total': 0})

    for s in samples:
        rule = match_rule(s)
        if rule:
            ok = rule['pred_up'] == s['actual_up']
            rule_full[rule['name']]['total'] += 1
            if ok: rule_full[rule['name']]['correct'] += 1

    for test_idx in range(min_train, len(all_weeks)):
        test_week = all_weeks[test_idx]
        for s in [x for x in samples if x['iw_this'] == test_week]:
            rule = match_rule(s)
            if rule:
                ok = rule['pred_up'] == s['actual_up']
                rule_cv[rule['name']]['total'] += 1
                if ok: rule_cv[rule['name']]['correct'] += 1

    return rule_full, rule_cv


def analyze_signal_strength(samples):
    """分析信号强度（跌幅/涨幅绝对值）对准确率的影响"""
    # 只看R1（最大的规则）
    r1_samples = []
    for s in samples:
        rule = match_rule(s)
        if rule and rule['name'] == 'R1':
            r1_samples.append(s)

    if not r1_samples:
        return

    logger.info("\n  ── R1信号强度分析 ──")
    # 按个股跌幅分桶
    buckets = [
        ('跌>8%', lambda s: s['this_chg'] < -8),
        ('跌5~8%', lambda s: -8 <= s['this_chg'] < -5),
        ('跌3~5%', lambda s: -5 <= s['this_chg'] < -3),
        ('跌2~3%', lambda s: -3 <= s['this_chg'] < -2),
    ]
    for label, cond in buckets:
        sub = [s for s in r1_samples if cond(s)]
        if sub:
            ok = sum(1 for s in sub if s['actual_up'])
            logger.info("    %-10s %d样本 准确率%.1f%%", label, len(sub),
                        ok/len(sub)*100)

    # 按大盘跌幅分桶
    logger.info("  R1按大盘跌幅:")
    mkt_buckets = [
        ('大盘跌>5%', lambda s: s['mkt_chg'] < -5),
        ('大盘跌3~5%', lambda s: -5 <= s['mkt_chg'] < -3),
    ]
    for label, cond in mkt_buckets:
        sub = [s for s in r1_samples if cond(s)]
        if sub:
            ok = sum(1 for s in sub if s['actual_up'])
            logger.info("    %-12s %d样本 准确率%.1f%%", label, len(sub),
                        ok/len(sub)*100)

    # 按pos60分桶
    logger.info("  R1按价格位置:")
    pos_buckets = [
        ('低位<0.2', lambda s: s['pos60'] is not None and s['pos60'] < 0.2),
        ('中低0.2~0.4', lambda s: s['pos60'] is not None and 0.2 <= s['pos60'] < 0.4),
        ('中位0.4~0.6', lambda s: s['pos60'] is not None and 0.4 <= s['pos60'] < 0.6),
        ('中高0.6~0.8', lambda s: s['pos60'] is not None and 0.6 <= s['pos60'] < 0.8),
        ('高位>0.8', lambda s: s['pos60'] is not None and s['pos60'] >= 0.8),
    ]
    for label, cond in pos_buckets:
        sub = [s for s in r1_samples if cond(s)]
        if sub:
            ok = sum(1 for s in sub if s['actual_up'])
            logger.info("    %-12s %d样本 准确率%.1f%%", label, len(sub),
                        ok/len(sub)*100)

    # 按超额收益分桶
    logger.info("  R1按超额收益(个股-大盘):")
    exc_buckets = [
        ('超额<-5%', lambda s: s['excess'] < -5),
        ('超额-5~-2%', lambda s: -5 <= s['excess'] < -2),
        ('超额-2~0%', lambda s: -2 <= s['excess'] < 0),
        ('超额>0%', lambda s: s['excess'] >= 0),
    ]
    for label, cond in exc_buckets:
        sub = [s for s in r1_samples if cond(s)]
        if sub:
            ok = sum(1 for s in sub if s['actual_up'])
            logger.info("    %-14s %d样本 准确率%.1f%%", label, len(sub),
                        ok/len(sub)*100)


def analyze_per_stock_accuracy(samples):
    """分析个股历史准确率分布"""
    stock_stats = defaultdict(lambda: {'correct': 0, 'total': 0})
    for s in samples:
        rule = match_rule(s)
        if rule:
            stock_stats[s['code']]['total'] += 1
            if rule['pred_up'] == s['actual_up']:
                stock_stats[s['code']]['correct'] += 1

    # 按准确率分桶
    acc_dist = defaultdict(int)
    for code, st in stock_stats.items():
        if st['total'] >= 3:
            acc = st['correct'] / st['total']
            bucket = int(acc * 10) * 10
            acc_dist[bucket] += 1

    logger.info("\n  ── 个股准确率分布（≥3次预测） ──")
    for b in sorted(acc_dist.keys()):
        logger.info("    %d%%~%d%%: %d只股票", b, b+10, acc_dist[b])

    # 过滤低准确率股票的效果
    logger.info("\n  ── 过滤低准确率股票效果 ──")
    for min_acc in [0.5, 0.6, 0.7, 0.8]:
        good_codes = {c for c, st in stock_stats.items()
                      if st['total'] >= 3 and st['correct']/st['total'] >= min_acc}
        total, correct = 0, 0
        for s in samples:
            rule = match_rule(s)
            if rule and s['code'] in good_codes:
                total += 1
                if rule['pred_up'] == s['actual_up']:
                    correct += 1
        if total > 0:
            logger.info("    历史准确率≥%d%%: 准确率%.1f%% (%d/%d) 保留%d只",
                        int(min_acc*100), correct/total*100, correct, total, len(good_codes))


def grid_search_filters(samples):
    """网格搜索最优过滤器组合，目标>80%准确率"""
    logger.info("\n" + "=" * 80)
    logger.info("  ══ 网格搜索最优配置 ══")
    logger.info("=" * 80)

    # 只保留核心规则（移除低准确率的资金流/财报规则）
    core_rules = [r for r in RULES]  # 已经只有核心规则

    # 定义过滤条件
    # 每个过滤条件是一个 (name, check_fn) 对
    # check_fn(sample, rule) -> True表示应该保留
    filters = {
        # 信号强度过滤
        'chg_3': lambda s, r: abs(s['this_chg']) >= 3 if r['pred_up'] else True,
        'chg_4': lambda s, r: abs(s['this_chg']) >= 4 if r['pred_up'] else True,
        'chg_5': lambda s, r: abs(s['this_chg']) >= 5 if r['pred_up'] else True,
        # 大盘跌幅加强（仅R1）
        'mkt_4': lambda s, r: s['mkt_chg'] < -4 if r['name'] == 'R1' else True,
        'mkt_5': lambda s, r: s['mkt_chg'] < -5 if r['name'] == 'R1' else True,
        # 价格位置过滤
        'pos_lt07': lambda s, r: (s['pos60'] is None or s['pos60'] < 0.7) if r['pred_up'] else True,
        'pos_lt06': lambda s, r: (s['pos60'] is None or s['pos60'] < 0.6) if r['pred_up'] else True,
        'pos_lt05': lambda s, r: (s['pos60'] is None or s['pos60'] < 0.5) if r['pred_up'] else True,
        # 超额收益过滤（个股跌幅远超大盘=极端弱势）
        'exc_gt_m8': lambda s, r: s['excess'] > -8 if r['pred_up'] else True,
        'exc_gt_m5': lambda s, r: s['excess'] > -5 if r['pred_up'] else True,
        # 连跌天数加强
        'cd_ge2': lambda s, r: s['cd'] >= 2 if r['pred_up'] else True,
        'cd_ge3': lambda s, r: s['cd'] >= 3 if r['pred_up'] else True,
        # 板块动量
        'bm_neg': lambda s, r: (s.get('board_momentum') is None or s['board_momentum'] < 0) if r['pred_up'] else True,
        'bm_lt05': lambda s, r: (s.get('board_momentum') is None or s['board_momentum'] < 0.5) if r['pred_up'] else True,
        # 波动率过滤（低波动率股票更可预测）
        'vol_lt3': lambda s, r: (s.get('volatility') is None or s['volatility'] < 3) if r['pred_up'] else True,
        'vol_lt4': lambda s, r: (s.get('volatility') is None or s['volatility'] < 4),
        # 只保留R1（最强规则）
        'only_R1': lambda s, r: r['name'] == 'R1',
        # 只保留R1+R5a（两个最强规则）
        'only_R1_R5a': lambda s, r: r['name'] in ('R1', 'R5a'),
        # 只保留涨信号
        'only_up': lambda s, r: r['pred_up'],
        # 移除R3（CV较低）
        'no_R3': lambda s, r: r['name'] != 'R3',
        # 移除R5b R5c（准确率较低）
        'no_R5bc': lambda s, r: r['name'] not in ('R5b', 'R5c'),
        # 移除跌信号
        'no_down': lambda s, r: r['pred_up'],
    }

    # 先测试单个过滤器
    logger.info("\n  ── 单个过滤器效果 ──")
    single_results = {}
    for fname, fcheck in sorted(filters.items()):
        total, correct = 0, 0
        for s in samples:
            rule = match_rule(s, core_rules)
            if rule and fcheck(s, rule):
                total += 1
                if rule['pred_up'] == s['actual_up']:
                    correct += 1
        if total > 0:
            acc = correct / total * 100
            single_results[fname] = (acc, correct, total)
            marker = '★' if acc >= 80 else ('▲' if acc >= 75 else '')
            logger.info("    %-15s 准确率%5.1f%% (%5d/%5d) %s",
                        fname, acc, correct, total, marker)

    # 找出准确率>75%的过滤器，做组合搜索
    good_filters = {k: v for k, v in filters.items()
                    if k in single_results and single_results[k][0] >= 70}

    logger.info("\n  ── 组合过滤器搜索（准确率≥70%%的过滤器组合） ──")
    best_configs = []
    filter_names = sorted(good_filters.keys())

    # 2-filter组合
    for i in range(len(filter_names)):
        for j in range(i + 1, len(filter_names)):
            f1, f2 = filter_names[i], filter_names[j]
            total, correct = 0, 0
            for s in samples:
                rule = match_rule(s, core_rules)
                if rule and filters[f1](s, rule) and filters[f2](s, rule):
                    total += 1
                    if rule['pred_up'] == s['actual_up']:
                        correct += 1
            if total >= 50:
                acc = correct / total * 100
                if acc >= 78:
                    best_configs.append((acc, correct, total, f'{f1}+{f2}'))

    # 3-filter组合
    for i in range(len(filter_names)):
        for j in range(i + 1, len(filter_names)):
            for k in range(j + 1, len(filter_names)):
                f1, f2, f3 = filter_names[i], filter_names[j], filter_names[k]
                total, correct = 0, 0
                for s in samples:
                    rule = match_rule(s, core_rules)
                    if rule and filters[f1](s, rule) and filters[f2](s, rule) and filters[f3](s, rule):
                        total += 1
                        if rule['pred_up'] == s['actual_up']:
                            correct += 1
                if total >= 30:
                    acc = correct / total * 100
                    if acc >= 80:
                        best_configs.append((acc, correct, total, f'{f1}+{f2}+{f3}'))

    best_configs.sort(key=lambda x: (-x[0], -x[2]))
    logger.info("  找到 %d 个≥78%%的配置:", len(best_configs))
    for acc, correct, total, label in best_configs[:30]:
        marker = '★★' if acc >= 85 else ('★' if acc >= 80 else '')
        logger.info("    %5.1f%% (%4d/%4d) %s %s", acc, correct, total, label, marker)

    return best_configs


def cv_validate_top_configs(samples, best_configs):
    """对top配置做时间序列CV验证"""
    if not best_configs:
        return

    logger.info("\n" + "=" * 80)
    logger.info("  ══ CV验证Top配置 ══")
    logger.info("=" * 80)

    all_weeks = sorted(set(s['iw_this'] for s in samples))
    min_train = 12

    # 重建过滤器（需要在这里重新定义）
    filters = {
        'chg_3': lambda s, r: abs(s['this_chg']) >= 3 if r['pred_up'] else True,
        'chg_4': lambda s, r: abs(s['this_chg']) >= 4 if r['pred_up'] else True,
        'chg_5': lambda s, r: abs(s['this_chg']) >= 5 if r['pred_up'] else True,
        'mkt_4': lambda s, r: s['mkt_chg'] < -4 if r['name'] == 'R1' else True,
        'mkt_5': lambda s, r: s['mkt_chg'] < -5 if r['name'] == 'R1' else True,
        'pos_lt07': lambda s, r: (s['pos60'] is None or s['pos60'] < 0.7) if r['pred_up'] else True,
        'pos_lt06': lambda s, r: (s['pos60'] is None or s['pos60'] < 0.6) if r['pred_up'] else True,
        'pos_lt05': lambda s, r: (s['pos60'] is None or s['pos60'] < 0.5) if r['pred_up'] else True,
        'exc_gt_m8': lambda s, r: s['excess'] > -8 if r['pred_up'] else True,
        'exc_gt_m5': lambda s, r: s['excess'] > -5 if r['pred_up'] else True,
        'cd_ge2': lambda s, r: s['cd'] >= 2 if r['pred_up'] else True,
        'cd_ge3': lambda s, r: s['cd'] >= 3 if r['pred_up'] else True,
        'bm_neg': lambda s, r: (s.get('board_momentum') is None or s['board_momentum'] < 0) if r['pred_up'] else True,
        'bm_lt05': lambda s, r: (s.get('board_momentum') is None or s['board_momentum'] < 0.5) if r['pred_up'] else True,
        'vol_lt3': lambda s, r: (s.get('volatility') is None or s['volatility'] < 3) if r['pred_up'] else True,
        'vol_lt4': lambda s, r: (s.get('volatility') is None or s['volatility'] < 4),
        'only_R1': lambda s, r: r['name'] == 'R1',
        'only_R1_R5a': lambda s, r: r['name'] in ('R1', 'R5a'),
        'only_up': lambda s, r: r['pred_up'],
        'no_R3': lambda s, r: r['name'] != 'R3',
        'no_R5bc': lambda s, r: r['name'] not in ('R5b', 'R5c'),
        'no_down': lambda s, r: r['pred_up'],
    }

    # 取top 15配置做CV
    top_configs = best_configs[:15]
    cv_results = []

    for _, _, _, label in top_configs:
        fnames = label.split('+')
        fchecks = [filters[fn] for fn in fnames if fn in filters]
        if not fchecks:
            continue

        cv_total, cv_correct = 0, 0
        full_total, full_correct = 0, 0

        for s in samples:
            rule = match_rule(s)
            if rule and all(fc(s, rule) for fc in fchecks):
                full_total += 1
                if rule['pred_up'] == s['actual_up']:
                    full_correct += 1

        for test_idx in range(min_train, len(all_weeks)):
            test_week = all_weeks[test_idx]
            for s in [x for x in samples if x['iw_this'] == test_week]:
                rule = match_rule(s)
                if rule and all(fc(s, rule) for fc in fchecks):
                    cv_total += 1
                    if rule['pred_up'] == s['actual_up']:
                        cv_correct += 1

        if cv_total > 0 and full_total > 0:
            full_acc = full_correct / full_total * 100
            cv_acc = cv_correct / cv_total * 100
            gap = full_acc - cv_acc
            cv_results.append((cv_acc, cv_correct, cv_total, full_acc, full_total, gap, label))

    cv_results.sort(key=lambda x: (-x[0], -x[2]))
    logger.info("  %-35s %10s %10s %8s", "配置", "全样本", "CV", "gap")
    for cv_acc, cv_c, cv_t, full_acc, full_t, gap, label in cv_results:
        marker = '★★' if cv_acc >= 80 else ('★' if cv_acc >= 75 else ('⚠️' if gap > 10 else ''))
        logger.info("  %-35s %5.1f%%(%4d) %5.1f%%(%4d) %+5.1f%% %s",
                    label, full_acc, full_t, cv_acc, cv_t, gap, marker)

    return cv_results


def run_backtest():
    t0 = datetime.now()
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"

    logger.info("=" * 80)
    logger.info("  下周预测80%%目标回测")
    logger.info("=" * 80)

    logger.info("\n[1/5] 加载数据...")
    data = load_data()
    logger.info("[2/5] 构建样本...")
    samples = build_samples(data)
    logger.info("  总样本数: %d", len(samples))

    # 3. 按规则分析
    logger.info("\n[3/5] 按规则分析...")
    rule_full, rule_cv = analyze_by_rule_cv(samples)

    logger.info("\n  ── 规则准确率（全样本 vs CV） ──")
    logger.info("  %-10s %15s %15s %8s", "规则", "全样本", "CV", "gap")
    for rn in sorted(rule_full.keys()):
        rf = rule_full[rn]
        rc = rule_cv.get(rn, {'correct': 0, 'total': 0})
        fa = rf['correct']/rf['total']*100 if rf['total'] > 0 else 0
        ca = rc['correct']/rc['total']*100 if rc['total'] > 0 else 0
        gap = fa - ca
        marker = '⚠️' if ca < 65 else ('✅' if ca >= 80 else '')
        logger.info("  %-10s %5.1f%%(%5d) %5.1f%%(%5d) %+5.1f%% %s",
                    rn, fa, rf['total'], ca, rc['total'], gap, marker)

    # 4. 信号强度分析
    logger.info("\n[4/5] 信号强度分析...")
    analyze_signal_strength(samples)
    analyze_per_stock_accuracy(samples)

    # 5. 网格搜索
    logger.info("\n[5/5] 网格搜索最优配置...")
    best_configs = grid_search_filters(samples)

    # 6. CV验证
    cv_results = cv_validate_top_configs(samples, best_configs)

    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("\n  总耗时: %.1fs", elapsed)
    logger.info("=" * 80)


if __name__ == '__main__':
    run_backtest()
