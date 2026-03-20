#!/usr/bin/env python3
"""V11 概念板块 × 个股强弱势 深度分析。

分析维度:
  1. V11规则在不同板块类型下的准确率差异
  2. 个股相对强弱势(vs大盘/vs板块)对预测准确率的影响
  3. 板块动量/一致性分层后的规则表现
  4. 是否存在板块特异性规则(某些板块下准确率显著高/低)
  5. 个股强弱势分类后的定制规则空间

目标: 判断V11是否需要按板块/强弱势定制规则，还是通用规则已足够。
"""
import sys, os, json, logging
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from dao import get_connection
from service.weekly_prediction_service import _get_stock_index

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s',
                    datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

N_WEEKS = 29
MIN_TRAIN_WEEKS = 20

def _to_float(v):
    try:
        return float(v) if v is not None else 0.0
    except (ValueError, TypeError):
        return 0.0

def _safe_mean(lst):
    return sum(lst) / len(lst) if lst else 0.0

def _compound_return(pcts):
    r = 1.0
    for p in pcts:
        r *= (1 + p / 100)
    return (r - 1) * 100

def _pct(c, t):
    return f"{c/t*100:.1f}%" if t > 0 else "N/A"


# ═══════════════════════════════════════════════════════════
# 数据加载 (复用V11的load_data)
# ═══════════════════════════════════════════════════════════

def _get_latest_trade_date():
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("SELECT MAX(`date`) as d FROM stock_kline WHERE stock_code='000001.SH'")
    r = cur.fetchone()
    conn.close()
    return r['d']

def _get_all_stock_codes():
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT stock_code FROM stock_kline "
                "WHERE stock_code NOT LIKE '%%.BJ' AND stock_code LIKE '%%.__' "
                "AND stock_code NOT LIKE '899%%'")
    codes = [r['stock_code'] for r in cur.fetchall()
             if not r['stock_code'].startswith('899')]
    conn.close()
    return codes

def load_data(n_weeks):
    latest_date = _get_latest_trade_date()
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(n_weeks + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')
    all_codes = _get_all_stock_codes()
    logger.info("股票数: %d, 日期范围: %s ~ %s", len(all_codes), start_date, latest_date)

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 个股K线
    logger.info("  加载个股K线...")
    stock_klines = defaultdict(list)
    bs = 2000
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,close_price,change_percent,trading_volume,change_hand "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY stock_code,`date`",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            stock_klines[r['stock_code']].append({
                'date': r['date'],
                'close': _to_float(r['close_price']),
                'change_percent': _to_float(r['change_percent']),
                'volume': _to_float(r['trading_volume']),
                'turnover': _to_float(r['change_hand']),
            })
        logger.info("  K线: %d/%d ...", min(i + bs, len(all_codes)), len(all_codes))

    # 大盘指数
    idx_codes = list(set(_get_stock_index(c) for c in all_codes))
    for idx in ('000001.SH', '399001.SZ'):
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

    # 概念板块映射
    stock_boards = defaultdict(list)
    cur.execute("SELECT stock_code, board_code, board_name FROM stock_concept_board_stock")
    for r in cur.fetchall():
        sc6 = r['stock_code']
        if sc6.startswith('6'):
            full = f"{sc6}.SH"
        elif sc6.startswith(('0', '3')):
            full = f"{sc6}.SZ"
        else:
            continue
        stock_boards[full].append({
            'board_code': r['board_code'], 'board_name': r['board_name'],
        })
    logger.info("  概念板块映射: %d 只有板块", len(stock_boards))

    # 板块K线
    board_klines = defaultdict(list)
    cur.execute(
        "SELECT board_code,`date`,change_percent FROM concept_board_kline "
        "WHERE `date`>=%s AND `date`<=%s ORDER BY board_code,`date`",
        [start_date, latest_date])
    for r in cur.fetchall():
        board_klines[r['board_code']].append({
            'date': r['date'], 'change_percent': _to_float(r['change_percent']),
        })
    logger.info("  板块K线: %d 个板块", len(board_klines))

    # 资金流向
    stock_fund_flows = defaultdict(list)
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,big_net_pct FROM stock_fund_flow "
            f"WHERE stock_code IN ({ph}) AND `date`>=%s AND `date`<=%s "
            f"ORDER BY stock_code,`date`",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            stock_fund_flows[r['stock_code']].append({
                'date': r['date'], 'big_net_pct': _to_float(r['big_net_pct']),
            })

    conn.close()
    return {
        'all_codes': all_codes, 'stock_klines': dict(stock_klines),
        'market_klines': dict(mkt_kl), 'stock_boards': dict(stock_boards),
        'board_klines': dict(board_klines), 'stock_fund_flows': dict(stock_fund_flows),
        'latest_date': latest_date, 'dt_end': dt_end,
    }


# ═══════════════════════════════════════════════════════════
# 构建扩展样本 (在V11样本基础上增加板块/强弱势维度)
# ═══════════════════════════════════════════════════════════

def build_extended_samples(data, n_weeks):
    """构建扩展样本，增加板块类型和个股强弱势分类。"""
    dt_end = data['dt_end']
    dt_cutoff = dt_end - timedelta(days=n_weeks * 7 + 14)
    samples = []

    mkt_by_week = {}
    for ic, kl in data['market_klines'].items():
        bw = defaultdict(list)
        for k in kl:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            bw[dt.isocalendar()[:2]].append(k)
        mkt_by_week[ic] = bw

    board_klines = data.get('board_klines', {})
    board_kl_by_week = {}
    for bc, kl in board_klines.items():
        bw = defaultdict(list)
        for k in kl:
            try:
                dt = datetime.strptime(k['date'], '%Y-%m-%d')
                bw[dt.isocalendar()[:2]].append(k)
            except (ValueError, TypeError):
                continue
        board_kl_by_week[bc] = bw

    stock_boards = data.get('stock_boards', {})

    ff_data = data.get('stock_fund_flows', {})

    processed = 0
    for code in data['all_codes']:
        klines = data['stock_klines'].get(code, [])
        if not klines or len(klines) < 60:
            continue

        stock_idx = _get_stock_index(code)
        suffix = stock_idx.split('.')[-1] if '.' in stock_idx else ''
        idx_bw = mkt_by_week.get(stock_idx, {})

        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            wg[dt.isocalendar()[:2]].append(k)

        sorted_weeks = sorted(wg.keys())
        sorted_all = sorted(klines, key=lambda x: x['date'])
        boards = stock_boards.get(code, [])

        ff_list = ff_data.get(code, [])
        ff_by_week = defaultdict(list)
        for ff in ff_list:
            try:
                dt = datetime.strptime(ff['date'], '%Y-%m-%d')
                ff_by_week[dt.isocalendar()[:2]].append(ff)
            except (ValueError, TypeError):
                continue

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

            mkt_last_day = None
            if mw:
                mkt_sorted = sorted(mw, key=lambda x: x['date'])
                mkt_last_day = mkt_sorted[-1]['change_percent']

            first_date = this_days[0]['date']
            hist = [k for k in sorted_all if k['date'] < first_date]

            # 价格位置
            pos60 = None
            if len(hist) >= 20:
                hc = [k['close'] for k in hist[-60:] if k['close'] > 0]
                if hc:
                    ac = hc + [k['close'] for k in this_days if k['close'] > 0]
                    mn, mx = min(ac), max(ac)
                    lc = this_days[-1]['close']
                    if mx > mn and lc > 0:
                        pos60 = (lc - mn) / (mx - mn)

            prev_chg = None
            if i > 0:
                prev_days = sorted(wg[sorted_weeks[i - 1]], key=lambda x: x['date'])
                if len(prev_days) >= 3:
                    prev_chg = _compound_return([d['change_percent'] for d in prev_days])

            # 连涨连跌
            cd, cu = 0, 0
            for p in reversed(this_pcts):
                if p < 0:
                    cd += 1
                    if cu > 0: break
                elif p > 0:
                    cu += 1
                    if cd > 0: break
                else:
                    break

            last_day = this_pcts[-1] if this_pcts else 0

            # 成交量比
            vol_ratio = None
            tv = [d['volume'] for d in this_days if d['volume'] > 0]
            hv = [k['volume'] for k in hist[-20:] if k['volume'] > 0]
            if tv and hv:
                at = _safe_mean(tv)
                ah = _safe_mean(hv)
                if ah > 0:
                    vol_ratio = at / ah

            # 资金流向
            ff_week = ff_by_week.get(iw_this, [])
            big_net_pct_avg = None
            if ff_week:
                pcts = [f['big_net_pct'] for f in ff_week if f['big_net_pct'] != 0]
                if pcts:
                    big_net_pct_avg = _safe_mean(pcts)

            # ── 板块维度(扩展) ──
            board_names = []
            board_codes = []
            board_chgs = []  # 每个板块本周涨跌幅
            board_momentum = None
            concept_consensus = None
            best_board_chg = None
            worst_board_chg = None

            if boards:
                boards_up = 0
                valid_boards = 0
                for b in boards:
                    bkw = board_kl_by_week.get(b['board_code'], {})
                    bk_this = bkw.get(iw_this, [])
                    if bk_this:
                        bk_chg = _safe_mean([k['change_percent'] for k in bk_this])
                        board_chgs.append(bk_chg)
                        board_names.append(b['board_name'])
                        board_codes.append(b['board_code'])
                        valid_boards += 1
                        if bk_chg > 0:
                            boards_up += 1
                if board_chgs:
                    board_momentum = _safe_mean(board_chgs)
                    best_board_chg = max(board_chgs)
                    worst_board_chg = min(board_chgs)
                if valid_boards > 0:
                    concept_consensus = boards_up / valid_boards

            # ── 个股强弱势分类 ──
            relative_strength = this_chg - mkt_chg  # vs大盘
            relative_to_board = this_chg - board_momentum if board_momentum is not None else None

            # 强弱势分类
            if relative_strength > 3:
                stock_strength_vs_mkt = '强势'
            elif relative_strength > 0:
                stock_strength_vs_mkt = '偏强'
            elif relative_strength > -3:
                stock_strength_vs_mkt = '偏弱'
            else:
                stock_strength_vs_mkt = '弱势'

            if relative_to_board is not None:
                if relative_to_board > 3:
                    stock_strength_vs_board = '强于板块'
                elif relative_to_board > 0:
                    stock_strength_vs_board = '略强于板块'
                elif relative_to_board > -3:
                    stock_strength_vs_board = '略弱于板块'
                else:
                    stock_strength_vs_board = '弱于板块'
            else:
                stock_strength_vs_board = '无板块数据'

            # 板块强弱分类
            if board_momentum is not None:
                if board_momentum > 2:
                    board_strength = '强势板块'
                elif board_momentum > 0:
                    board_strength = '偏强板块'
                elif board_momentum > -2:
                    board_strength = '偏弱板块'
                else:
                    board_strength = '弱势板块'
            else:
                board_strength = '无板块数据'

            # 板块一致性分类
            if concept_consensus is not None:
                if concept_consensus >= 0.7:
                    consensus_level = '高一致(≥0.7)'
                elif concept_consensus >= 0.4:
                    consensus_level = '中一致(0.4~0.7)'
                else:
                    consensus_level = '低一致(<0.4)'
            else:
                consensus_level = '无数据'

            samples.append({
                'code': code, 'suffix': suffix,
                'iw_this': iw_this,
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'mkt_last_day': mkt_last_day,
                'pos60': pos60, 'prev_chg': prev_chg,
                'cd': cd, 'cu': cu, 'last_day': last_day,
                'vol_ratio': vol_ratio,
                'big_net_pct_avg': big_net_pct_avg,
                'board_momentum': board_momentum,
                'concept_consensus': concept_consensus,
                'best_board_chg': best_board_chg,
                'worst_board_chg': worst_board_chg,
                'board_names': board_names,
                'board_codes': board_codes,
                'n_boards': len(board_names),
                'relative_strength': relative_strength,
                'relative_to_board': relative_to_board,
                'stock_strength_vs_mkt': stock_strength_vs_mkt,
                'stock_strength_vs_board': stock_strength_vs_board,
                'board_strength': board_strength,
                'consensus_level': consensus_level,
                'next_chg': next_chg, 'actual_up': next_chg >= 0,
            })

        processed += 1
        if processed % 1000 == 0:
            logger.info("  构建样本: %d/%d ...", processed, len(data['all_codes']))

    logger.info("  总样本: %d", len(samples))
    return samples


# ═══════════════════════════════════════════════════════════
# V11 规则匹配 (复用V11最优配置: 骨干+尾日涨only+过滤low)
# ═══════════════════════════════════════════════════════════

def v11_predict(s):
    """V11最优配置预测。返回 (pred_up, rule_name, layer) 或 (None, None, None)。"""
    # ── 层1: backbone ──
    backbone_rules = [
        ('V5_R1:深跌+跌→涨', True,
         lambda s: s['this_chg'] < -2 and s['mkt_chg'] < -3),
        ('V5_R5a:SZ+微跌+连跌3→涨', True,
         lambda s: s['suffix'] == 'SZ' and -1 <= s['mkt_chg'] < 0
                   and s['this_chg'] < -2 and s['cd'] >= 3),
        ('V5_R5b:SZ+微跌+低位→涨', True,
         lambda s: s['suffix'] == 'SZ' and -1 <= s['mkt_chg'] < 0
                   and s['this_chg'] < -2
                   and s['pos60'] is not None and s['pos60'] < 0.2),
        ('V5_R3:SH+跌+前周跌→涨', True,
         lambda s: s['this_chg'] < -3 and s['suffix'] == 'SH'
                   and -3 <= s['mkt_chg'] < -1
                   and s['prev_chg'] is not None and s['prev_chg'] < -2
                   and not (s['pos60'] is not None and s['pos60'] >= 0.8)),
        ('V5_R5c:SZ+微跌+跌>2%→涨', True,
         lambda s: s['suffix'] == 'SZ' and -1 <= s['mkt_chg'] < 0
                   and s['this_chg'] < -2),
        ('V5_R6c:SZ+跌+涨+连涨3→跌', False,
         lambda s: s['suffix'] == 'SZ' and -3 <= s['mkt_chg'] < -1
                   and s['this_chg'] > 2 and s['cu'] >= 3),
        ('V5_R6a:SZ+跌+涨>5%→跌', False,
         lambda s: s['suffix'] == 'SZ' and -3 <= s['mkt_chg'] < -1
                   and s['this_chg'] > 5),
        ('V5_R7:跌+连涨+非高位→跌', False,
         lambda s: s['this_chg'] < -3 and s['cu'] >= 3
                   and s['pos60'] is not None and s['pos60'] < 0.6),
    ]
    for name, pred_up, check in backbone_rules:
        try:
            if check(s):
                return pred_up, name, 'backbone'
        except (TypeError, KeyError):
            continue

    # ── 层2.5: bull涨信号only ──
    bull_up_rules = [
        ('BULL_UP1:尾日跌+跌>2%+低位→涨', True,
         lambda s: s['mkt_chg'] >= 0
                   and s['mkt_last_day'] is not None and s['mkt_last_day'] < -1
                   and s['this_chg'] < -2
                   and s['pos60'] is not None and s['pos60'] < 0.3),
        ('BULL_UP2:尾日跌+跌>3%→涨', True,
         lambda s: s['mkt_chg'] >= 0
                   and s['mkt_last_day'] is not None and s['mkt_last_day'] < -1
                   and s['this_chg'] < -3),
        ('BULL_UP3:尾日跌+跌>2%→涨', True,
         lambda s: s['mkt_chg'] >= 0
                   and s['mkt_last_day'] is not None and s['mkt_last_day'] < -1
                   and s['this_chg'] < -2),
    ]
    for name, pred_up, check in bull_up_rules:
        try:
            if check(s):
                return pred_up, name, 'bull'
        except (TypeError, KeyError):
            continue

    return None, None, None


# ═══════════════════════════════════════════════════════════
# 深度分析
# ═══════════════════════════════════════════════════════════

def analyze(samples):
    """多维度深度分析。"""
    logger.info("\n" + "=" * 100)
    logger.info("  V11 概念板块 × 个股强弱势 深度分析")
    logger.info("=" * 100)

    # 先对所有样本做V11预测
    predicted = []
    for s in samples:
        pred_up, rule_name, layer = v11_predict(s)
        if pred_up is not None:
            s['pred_up'] = pred_up
            s['rule_name'] = rule_name
            s['layer'] = layer
            s['correct'] = pred_up == s['actual_up']
            predicted.append(s)

    total = len(predicted)
    correct = sum(1 for s in predicted if s['correct'])
    logger.info("\n  V11预测总量: %d, 准确率: %s", total, _pct(correct, total))

    results = {}

    # ═══════════════════════════════════════════════════════
    # 分析1: V11规则在不同个股强弱势(vs大盘)下的准确率
    # ═══════════════════════════════════════════════════════
    logger.info("\n" + "─" * 80)
    logger.info("  分析1: V11规则 × 个股强弱势(vs大盘)")
    logger.info("─" * 80)

    strength_stats = defaultdict(lambda: {'correct': 0, 'total': 0})
    for s in predicted:
        k = s['stock_strength_vs_mkt']
        strength_stats[k]['total'] += 1
        if s['correct']:
            strength_stats[k]['correct'] += 1

    logger.info("  %-12s %-10s %-10s %-10s", '强弱势', '准确率', '样本数', '占比')
    logger.info("  " + "-" * 45)
    r1 = {}
    for k in ['强势', '偏强', '偏弱', '弱势']:
        st = strength_stats[k]
        acc = st['correct'] / st['total'] * 100 if st['total'] > 0 else 0
        logger.info("  %-12s %-10s %-10d %-10s", k, f"{acc:.1f}%", st['total'],
                    f"{st['total']/total*100:.1f}%")
        r1[k] = {'acc': round(acc, 1), 'total': st['total']}
    results['个股强弱势vs大盘'] = r1

    # ═══════════════════════════════════════════════════════
    # 分析2: V11规则 × 个股强弱势(vs板块)
    # ═══════════════════════════════════════════════════════
    logger.info("\n" + "─" * 80)
    logger.info("  分析2: V11规则 × 个股强弱势(vs板块)")
    logger.info("─" * 80)

    vs_board_stats = defaultdict(lambda: {'correct': 0, 'total': 0})
    for s in predicted:
        k = s['stock_strength_vs_board']
        vs_board_stats[k]['total'] += 1
        if s['correct']:
            vs_board_stats[k]['correct'] += 1

    logger.info("  %-14s %-10s %-10s", '强弱势vs板块', '准确率', '样本数')
    logger.info("  " + "-" * 40)
    r2 = {}
    for k in ['强于板块', '略强于板块', '略弱于板块', '弱于板块', '无板块数据']:
        st = vs_board_stats[k]
        if st['total'] > 0:
            acc = st['correct'] / st['total'] * 100
            logger.info("  %-14s %-10s %-10d", k, f"{acc:.1f}%", st['total'])
            r2[k] = {'acc': round(acc, 1), 'total': st['total']}
    results['个股强弱势vs板块'] = r2

    # ═══════════════════════════════════════════════════════
    # 分析3: V11规则 × 板块强弱势
    # ═══════════════════════════════════════════════════════
    logger.info("\n" + "─" * 80)
    logger.info("  分析3: V11规则 × 板块强弱势")
    logger.info("─" * 80)

    board_str_stats = defaultdict(lambda: {'correct': 0, 'total': 0})
    for s in predicted:
        k = s['board_strength']
        board_str_stats[k]['total'] += 1
        if s['correct']:
            board_str_stats[k]['correct'] += 1

    logger.info("  %-14s %-10s %-10s", '板块强弱', '准确率', '样本数')
    logger.info("  " + "-" * 40)
    r3 = {}
    for k in ['强势板块', '偏强板块', '偏弱板块', '弱势板块', '无板块数据']:
        st = board_str_stats[k]
        if st['total'] > 0:
            acc = st['correct'] / st['total'] * 100
            logger.info("  %-14s %-10s %-10d", k, f"{acc:.1f}%", st['total'])
            r3[k] = {'acc': round(acc, 1), 'total': st['total']}
    results['板块强弱势'] = r3

    # ═══════════════════════════════════════════════════════
    # 分析4: V11规则 × 板块一致性
    # ═══════════════════════════════════════════════════════
    logger.info("\n" + "─" * 80)
    logger.info("  分析4: V11规则 × 板块一致性")
    logger.info("─" * 80)

    consensus_stats = defaultdict(lambda: {'correct': 0, 'total': 0})
    for s in predicted:
        k = s['consensus_level']
        consensus_stats[k]['total'] += 1
        if s['correct']:
            consensus_stats[k]['correct'] += 1

    logger.info("  %-16s %-10s %-10s", '一致性', '准确率', '样本数')
    logger.info("  " + "-" * 40)
    r4 = {}
    for k in ['高一致(≥0.7)', '中一致(0.4~0.7)', '低一致(<0.4)', '无数据']:
        st = consensus_stats[k]
        if st['total'] > 0:
            acc = st['correct'] / st['total'] * 100
            logger.info("  %-16s %-10s %-10d", k, f"{acc:.1f}%", st['total'])
            r4[k] = {'acc': round(acc, 1), 'total': st['total']}
    results['板块一致性'] = r4

    # ═══════════════════════════════════════════════════════
    # 分析5: 每条V11规则 × 板块强弱势 交叉分析
    # ═══════════════════════════════════════════════════════
    logger.info("\n" + "─" * 80)
    logger.info("  分析5: 每条V11规则 × 板块强弱势 交叉")
    logger.info("─" * 80)

    rule_board_stats = defaultdict(lambda: defaultdict(lambda: {'correct': 0, 'total': 0}))
    for s in predicted:
        rule_board_stats[s['rule_name']][s['board_strength']]['total'] += 1
        if s['correct']:
            rule_board_stats[s['rule_name']][s['board_strength']]['correct'] += 1

    r5 = {}
    for rule_name in sorted(rule_board_stats.keys()):
        logger.info("\n  规则: %s", rule_name)
        rule_data = {}
        for bs in ['强势板块', '偏强板块', '偏弱板块', '弱势板块', '无板块数据']:
            st = rule_board_stats[rule_name][bs]
            if st['total'] >= 10:
                acc = st['correct'] / st['total'] * 100
                logger.info("    %-14s %s (%d)", bs, f"{acc:.1f}%", st['total'])
                rule_data[bs] = {'acc': round(acc, 1), 'total': st['total']}
        r5[rule_name] = rule_data
    results['规则×板块强弱'] = r5

    # ═══════════════════════════════════════════════════════
    # 分析6: 每条V11规则 × 个股强弱势(vs大盘) 交叉
    # ═══════════════════════════════════════════════════════
    logger.info("\n" + "─" * 80)
    logger.info("  分析6: 每条V11规则 × 个股强弱势(vs大盘) 交叉")
    logger.info("─" * 80)

    rule_strength_stats = defaultdict(lambda: defaultdict(lambda: {'correct': 0, 'total': 0}))
    for s in predicted:
        rule_strength_stats[s['rule_name']][s['stock_strength_vs_mkt']]['total'] += 1
        if s['correct']:
            rule_strength_stats[s['rule_name']][s['stock_strength_vs_mkt']]['correct'] += 1

    r6 = {}
    for rule_name in sorted(rule_strength_stats.keys()):
        logger.info("\n  规则: %s", rule_name)
        rule_data = {}
        for ss in ['强势', '偏强', '偏弱', '弱势']:
            st = rule_strength_stats[rule_name][ss]
            if st['total'] >= 10:
                acc = st['correct'] / st['total'] * 100
                logger.info("    %-10s %s (%d)", ss, f"{acc:.1f}%", st['total'])
                rule_data[ss] = {'acc': round(acc, 1), 'total': st['total']}
        r6[rule_name] = rule_data
    results['规则×个股强弱'] = r6

    # ═══════════════════════════════════════════════════════
    # 分析7: 板块一致性 × 个股强弱势 交叉 (寻找定制规则空间)
    # ═══════════════════════════════════════════════════════
    logger.info("\n" + "─" * 80)
    logger.info("  分析7: 板块一致性 × 个股强弱势 交叉 (定制规则空间)")
    logger.info("─" * 80)

    cross_stats = defaultdict(lambda: {'correct': 0, 'total': 0, 'up_correct': 0, 'up_total': 0,
                                        'dn_correct': 0, 'dn_total': 0})
    for s in predicted:
        k = (s['consensus_level'], s['stock_strength_vs_mkt'])
        cross_stats[k]['total'] += 1
        if s['correct']:
            cross_stats[k]['correct'] += 1
        if s['pred_up']:
            cross_stats[k]['up_total'] += 1
            if s['correct']:
                cross_stats[k]['up_correct'] += 1
        else:
            cross_stats[k]['dn_total'] += 1
            if s['correct']:
                cross_stats[k]['dn_correct'] += 1

    logger.info("  %-16s %-10s %-10s %-10s %-12s %-12s",
                '一致性×强弱', '总准确率', '样本', '涨准确率', '跌准确率', '差异')
    logger.info("  " + "-" * 75)
    r7 = {}
    for cl in ['高一致(≥0.7)', '中一致(0.4~0.7)', '低一致(<0.4)']:
        for ss in ['强势', '偏强', '偏弱', '弱势']:
            st = cross_stats[(cl, ss)]
            if st['total'] >= 20:
                acc = st['correct'] / st['total'] * 100
                up_acc = st['up_correct'] / st['up_total'] * 100 if st['up_total'] > 0 else 0
                dn_acc = st['dn_correct'] / st['dn_total'] * 100 if st['dn_total'] > 0 else 0
                diff = abs(acc - (correct / total * 100))
                flag = '★' if diff > 5 else ''
                logger.info("  %s %-14s %-10s %-10d %-12s %-12s %+.1f%% %s",
                            flag, f"{cl[:4]}+{ss}", f"{acc:.1f}%", st['total'],
                            f"{up_acc:.1f}%({st['up_total']})" if st['up_total'] > 0 else "N/A",
                            f"{dn_acc:.1f}%({st['dn_total']})" if st['dn_total'] > 0 else "N/A",
                            acc - (correct / total * 100), flag)
                r7[f"{cl}+{ss}"] = {
                    'acc': round(acc, 1), 'total': st['total'],
                    'up_acc': round(up_acc, 1), 'up_total': st['up_total'],
                    'dn_acc': round(dn_acc, 1), 'dn_total': st['dn_total'],
                }
    results['一致性×强弱交叉'] = r7

    # ═══════════════════════════════════════════════════════
    # 分析8: 未命中样本的板块/强弱势分布 (覆盖率提升空间)
    # ═══════════════════════════════════════════════════════
    logger.info("\n" + "─" * 80)
    logger.info("  分析8: V11未命中样本的板块/强弱势分布")
    logger.info("─" * 80)

    unpredicted = [s for s in samples if v11_predict(s)[0] is None]
    logger.info("  未命中样本: %d (%.1f%%)", len(unpredicted),
                len(unpredicted) / len(samples) * 100)

    # 未命中样本中，按板块强弱势分布
    unp_board = defaultdict(lambda: {'total': 0, 'up': 0})
    for s in unpredicted:
        k = s['board_strength']
        unp_board[k]['total'] += 1
        if s['actual_up']:
            unp_board[k]['up'] += 1

    logger.info("\n  未命中样本 × 板块强弱:")
    logger.info("  %-14s %-10s %-10s", '板块强弱', '样本数', '实际涨比例')
    for k in ['强势板块', '偏强板块', '偏弱板块', '弱势板块', '无板块数据']:
        st = unp_board[k]
        if st['total'] > 0:
            logger.info("  %-14s %-10d %-10s", k, st['total'],
                        f"{st['up']/st['total']*100:.1f}%")

    # 未命中样本中，按大盘场景分布
    unp_mkt = defaultdict(lambda: {'total': 0, 'up': 0})
    for s in unpredicted:
        mkt = s['mkt_chg']
        if mkt < -3:
            regime = '大盘深跌'
        elif mkt < -1:
            regime = '大盘跌'
        elif mkt < 0:
            regime = '大盘微跌'
        elif mkt <= 1:
            regime = '大盘微涨'
        else:
            regime = '大盘涨'
        unp_mkt[regime]['total'] += 1
        if s['actual_up']:
            unp_mkt[regime]['up'] += 1

    logger.info("\n  未命中样本 × 大盘场景:")
    for regime in ['大盘深跌', '大盘跌', '大盘微跌', '大盘微涨', '大盘涨']:
        st = unp_mkt[regime]
        if st['total'] > 0:
            logger.info("  %-10s %-10d 涨%.1f%%", regime, st['total'],
                        st['up'] / st['total'] * 100)

    results['未命中分布'] = {
        'total': len(unpredicted),
        'pct': round(len(unpredicted) / len(samples) * 100, 1),
    }

    # ═══════════════════════════════════════════════════════
    # 分析9: 板块定制规则探索 — 在未命中样本中寻找高准确率规则
    # ═══════════════════════════════════════════════════════
    logger.info("\n" + "─" * 80)
    logger.info("  分析9: 未命中样本中的板块定制规则探索")
    logger.info("─" * 80)

    # 在未命中样本中，按(大盘场景, 板块强弱, 个股强弱)分组，看涨跌比例
    candidate_rules = []
    for mkt_label, mkt_check in [
        ('大盘涨', lambda s: s['mkt_chg'] > 0),
        ('大盘跌', lambda s: s['mkt_chg'] < -1),
        ('大盘微跌', lambda s: -1 <= s['mkt_chg'] < 0),
    ]:
        for bs_label, bs_check in [
            ('强势板块', lambda s: s['board_momentum'] is not None and s['board_momentum'] > 2),
            ('偏强板块', lambda s: s['board_momentum'] is not None and 0 < s['board_momentum'] <= 2),
            ('偏弱板块', lambda s: s['board_momentum'] is not None and -2 < s['board_momentum'] <= 0),
            ('弱势板块', lambda s: s['board_momentum'] is not None and s['board_momentum'] <= -2),
        ]:
            for ss_label, ss_check in [
                ('弱势', lambda s: s['relative_strength'] < -3),
                ('偏弱', lambda s: -3 <= s['relative_strength'] < 0),
                ('偏强', lambda s: 0 <= s['relative_strength'] < 3),
                ('强势', lambda s: s['relative_strength'] >= 3),
            ]:
                matched = [s for s in unpredicted
                           if mkt_check(s) and bs_check(s) and ss_check(s)]
                if len(matched) >= 30:
                    up_pct = sum(1 for s in matched if s['actual_up']) / len(matched) * 100
                    if up_pct >= 65 or up_pct <= 35:
                        pred = '涨' if up_pct >= 65 else '跌'
                        acc = up_pct if up_pct >= 65 else (100 - up_pct)
                        candidate_rules.append({
                            'desc': f"{mkt_label}+{bs_label}+{ss_label}→{pred}",
                            'acc': round(acc, 1),
                            'total': len(matched),
                            'up_pct': round(up_pct, 1),
                        })

    if candidate_rules:
        candidate_rules.sort(key=lambda x: -x['acc'])
        logger.info("\n  发现 %d 条候选板块定制规则(准确率≥65%或≤35%):", len(candidate_rules))
        for r in candidate_rules[:20]:
            flag = '★' if r['acc'] >= 70 else ''
            logger.info("    %s %-50s 准确率%.1f%% (%d样本)", flag, r['desc'], r['acc'], r['total'])
    else:
        logger.info("  未发现显著的板块定制规则候选")

    results['候选板块定制规则'] = candidate_rules

    # ═══════════════════════════════════════════════════════
    # 分析10: 已命中样本中，板块/强弱势是否能进一步过滤提升准确率
    # ═══════════════════════════════════════════════════════
    logger.info("\n" + "─" * 80)
    logger.info("  分析10: 已命中样本中的板块/强弱势过滤效果")
    logger.info("─" * 80)

    # 对已命中样本，测试加入板块/强弱势过滤后的准确率变化
    filters = [
        ('板块一致性≥0.5', lambda s: s['concept_consensus'] is not None and s['concept_consensus'] >= 0.5),
        ('板块一致性<0.5', lambda s: s['concept_consensus'] is not None and s['concept_consensus'] < 0.5),
        ('板块动量>0', lambda s: s['board_momentum'] is not None and s['board_momentum'] > 0),
        ('板块动量<0', lambda s: s['board_momentum'] is not None and s['board_momentum'] < 0),
        ('个股强于大盘', lambda s: s['relative_strength'] > 0),
        ('个股弱于大盘', lambda s: s['relative_strength'] <= 0),
        ('个股强于板块', lambda s: s['relative_to_board'] is not None and s['relative_to_board'] > 0),
        ('个股弱于板块', lambda s: s['relative_to_board'] is not None and s['relative_to_board'] <= 0),
        ('资金净流入', lambda s: s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] > 0),
        ('资金净流出', lambda s: s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < 0),
    ]

    base_acc = correct / total * 100
    logger.info("  基准准确率: %.1f%% (%d样本)", base_acc, total)
    logger.info("\n  %-20s %-10s %-10s %-10s", '过滤条件', '准确率', '样本数', '变化')
    logger.info("  " + "-" * 55)

    r10 = {}
    for fname, fcheck in filters:
        filtered = [s for s in predicted if fcheck(s)]
        if len(filtered) >= 50:
            f_correct = sum(1 for s in filtered if s['correct'])
            f_acc = f_correct / len(filtered) * 100
            diff = f_acc - base_acc
            flag = '★' if abs(diff) > 3 else ''
            logger.info("  %s %-18s %-10s %-10d %+.1f%%", flag, fname,
                        f"{f_acc:.1f}%", len(filtered), diff)
            r10[fname] = {'acc': round(f_acc, 1), 'total': len(filtered), 'diff': round(diff, 1)}
    results['过滤效果'] = r10

    # ═══════════════════════════════════════════════════════
    # 分析11: 按涨/跌信号分别看板块/强弱势影响
    # ═══════════════════════════════════════════════════════
    logger.info("\n" + "─" * 80)
    logger.info("  分析11: 涨信号 vs 跌信号 × 板块/强弱势")
    logger.info("─" * 80)

    for direction, dir_label in [(True, '涨信号'), (False, '跌信号')]:
        dir_samples = [s for s in predicted if s['pred_up'] == direction]
        if not dir_samples:
            continue
        dir_correct = sum(1 for s in dir_samples if s['correct'])
        dir_acc = dir_correct / len(dir_samples) * 100
        logger.info("\n  ── %s (基准%.1f%%, %d样本) ──", dir_label, dir_acc, len(dir_samples))

        for dim_name, dim_key, dim_values in [
            ('板块强弱', 'board_strength', ['强势板块', '偏强板块', '偏弱板块', '弱势板块']),
            ('个股强弱', 'stock_strength_vs_mkt', ['强势', '偏强', '偏弱', '弱势']),
            ('板块一致性', 'consensus_level', ['高一致(≥0.7)', '中一致(0.4~0.7)', '低一致(<0.4)']),
        ]:
            logger.info("    %s:", dim_name)
            for v in dim_values:
                sub = [s for s in dir_samples if s[dim_key] == v]
                if len(sub) >= 20:
                    sub_c = sum(1 for s in sub if s['correct'])
                    sub_acc = sub_c / len(sub) * 100
                    diff = sub_acc - dir_acc
                    flag = '★' if abs(diff) > 5 else ''
                    logger.info("      %s %-14s %s (%d) %+.1f%%", flag, v,
                                f"{sub_acc:.1f}%", len(sub), diff)

    # ═══════════════════════════════════════════════════════
    # 总结
    # ═══════════════════════════════════════════════════════
    logger.info("\n" + "=" * 100)
    logger.info("  总结")
    logger.info("=" * 100)

    # 计算最大差异
    max_diff_dim = ''
    max_diff_val = 0
    for dim_name, dim_data in [
        ('个股强弱势vs大盘', r1),
        ('板块强弱势', r3),
        ('板块一致性', r4),
    ]:
        accs = [v['acc'] for v in dim_data.values() if v['total'] >= 50]
        if len(accs) >= 2:
            diff = max(accs) - min(accs)
            if diff > max_diff_val:
                max_diff_val = diff
                max_diff_dim = dim_name

    logger.info("  最大准确率差异维度: %s (差异%.1f%%)", max_diff_dim, max_diff_val)
    logger.info("  候选板块定制规则数: %d", len(candidate_rules))

    if max_diff_val > 10:
        logger.info("  ★ 建议: 存在显著差异(>10%%), 值得按%s定制规则", max_diff_dim)
    elif max_diff_val > 5:
        logger.info("  ◆ 建议: 存在一定差异(5~10%%), 可考虑作为置信度修正因子")
    else:
        logger.info("  ○ 建议: 差异不大(<5%%), 通用规则已足够，无需板块定制")

    return results


def run():
    logger.info("=" * 100)
    logger.info("  V11 概念板块 × 个股强弱势 深度分析")
    logger.info("  数据: 5233只A股 × %d周", N_WEEKS)
    logger.info("=" * 100)

    data = load_data(N_WEEKS)
    samples = build_extended_samples(data, N_WEEKS)
    results = analyze(samples)

    # 保存结果
    out_path = 'data_results/nw_v11_board_strength_analysis.json'
    # 清理不可序列化的字段
    clean = {}
    for k, v in results.items():
        if isinstance(v, dict):
            clean[k] = {}
            for k2, v2 in v.items():
                if isinstance(v2, dict):
                    clean[k][k2] = v2
                elif isinstance(v2, list):
                    clean[k][k2] = v2
                else:
                    clean[k][k2] = v2
        elif isinstance(v, list):
            clean[k] = v
        else:
            clean[k] = v

    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(clean, f, ensure_ascii=False, indent=2)
    logger.info("\n  结果已保存到 %s", out_path)


if __name__ == '__main__':
    run()
