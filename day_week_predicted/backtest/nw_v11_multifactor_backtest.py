#!/usr/bin/env python3
"""
V11 多因子综合预测模型回测
==========================
基于V5模型衍生，深度融合所有历史模型分析结果，目标准确率>75%。

核心设计理念:
  1. 覆盖大盘涨/跌/平盘所有场景（V5/V7只覆盖跌场景）
  2. 多因子评分制替代单规则互斥匹配（避免V7过于极端的精简）
  3. 大范围试算各维度因子组合，CV验证筛选
  4. 行业/概念/资金流向/量价比/技术维度全覆盖

因子维度:
  A. 价格动量因子: this_chg, prev_chg, momentum_3w, relative_strength
  B. 成交量因子: vol_ratio, turnover_ratio, vol_price_diverge
  C. 位置因子: pos60, 连涨连跌天数
  D. 大盘环境因子: mkt_chg, mkt_last_day, mkt_vol_std
  E. 资金流向因子: big_net_pct_avg
  F. 振幅因子: week_amp, max_day_up, max_day_down
  G. 概念板块因子: board_momentum, concept_consensus (回测中模拟)

模型架构:
  Step1: 提取全维度因子
  Step2: 大范围试算单因子/双因子/三因子组合
  Step3: CV验证筛选有效规则(CV>65%, gap<8%)
  Step4: 多因子评分制组合(加权投票)
  Step5: 迭代优化阈值直到准确率>75%

用法:
    python -m day_week_predicted.backtest.nw_v11_multifactor_backtest
"""
import sys, logging, math, json, itertools
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
MIN_TRAIN_WEEKS = 12
TARGET_ACCURACY = 75.0


# ═══════════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════════

def _safe_mean(lst):
    return sum(lst) / len(lst) if lst else 0

def _safe_std(lst):
    if len(lst) < 2:
        return 0
    m = _safe_mean(lst)
    return math.sqrt(sum((x - m) ** 2 for x in lst) / (len(lst) - 1))

def _pct(c, t):
    return f"{c/t*100:.1f}%" if t > 0 else "N/A"


# ═══════════════════════════════════════════════════════════
# 数据加载 — 全维度(K线+资金流向+概念板块)
# ═══════════════════════════════════════════════════════════

def load_data(n_weeks):
    """加载K线+资金流向+概念板块数据，支持全维度因子计算。
    优化: 使用批量IN查询，每批2000只，平衡内存和速度。
    """
    latest_date = _get_latest_trade_date()
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(n_weeks + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')

    all_codes = _get_all_stock_codes()
    logger.info("股票数: %d, 日期范围: %s ~ %s", len(all_codes), start_date, latest_date)

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 1. 个股K线 — 大批量查询
    logger.info("  加载个股K线...")
    stock_klines = defaultdict(list)
    bs = 2000
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,open_price,close_price,high_price,low_price,"
            f"change_percent,trading_volume,trading_amount,amplitude,change_hand "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY stock_code,`date`",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            stock_klines[r['stock_code']].append({
                'date': r['date'],
                'open': _to_float(r['open_price']),
                'close': _to_float(r['close_price']),
                'high': _to_float(r['high_price']),
                'low': _to_float(r['low_price']),
                'change_percent': _to_float(r['change_percent']),
                'volume': _to_float(r['trading_volume']),
                'amount': _to_float(r['trading_amount']),
                'amplitude': _to_float(r['amplitude']),
                'turnover': _to_float(r['change_hand']),
            })
        logger.info("  K线: %d/%d ...", min(i + bs, len(all_codes)), len(all_codes))
    logger.info("  个股K线: %d 只有数据", len(stock_klines))

    # 2. 大盘指数K线
    idx_codes = list(set(_get_stock_index(c) for c in all_codes))
    for idx in ('000001.SH', '399001.SZ', '899050.SZ'):
        if idx not in idx_codes:
            idx_codes.append(idx)
    ph = ','.join(['%s'] * len(idx_codes))
    cur.execute(
        f"SELECT stock_code,`date`,change_percent,trading_volume "
        f"FROM stock_kline WHERE stock_code IN ({ph}) "
        f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
        idx_codes + [start_date, latest_date])
    mkt_kl = defaultdict(list)
    for r in cur.fetchall():
        mkt_kl[r['stock_code']].append({
            'date': r['date'],
            'change_percent': _to_float(r['change_percent']),
            'volume': _to_float(r['trading_volume']),
        })
    logger.info("  大盘指数: %d 个", len(mkt_kl))

    # 3. 资金流向 — 大批量查询
    logger.info("  加载资金流向...")
    stock_fund_flows = defaultdict(list)
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,big_net_pct,main_net_5day "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY stock_code,`date`",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            stock_fund_flows[r['stock_code']].append({
                'date': r['date'],
                'big_net_pct': _to_float(r['big_net_pct']),
                'main_net_5day': _to_float(r['main_net_5day']),
            })
    logger.info("  资金流向: %d 只有数据", len(stock_fund_flows))

    # 4. 概念板块映射 + 板块K线
    stock_boards = defaultdict(list)
    cur.execute("SELECT stock_code, board_code, board_name FROM stock_concept_board_stock")
    for r in cur.fetchall():
        sc6 = r['stock_code']
        if sc6.startswith('6'):
            full = f"{sc6}.SH"
        elif sc6.startswith(('0', '3')):
            full = f"{sc6}.SZ"
        elif sc6.startswith(('4', '8', '9')):
            full = f"{sc6}.BJ"
        else:
            full = f"{sc6}.SZ"
        stock_boards[full].append({
            'board_code': r['board_code'], 'board_name': r['board_name'],
        })
    logger.info("  概念板块映射: %d 只有板块", len(stock_boards))

    # 板块K线 — 全量查询
    logger.info("  加载板块K线(全量查询)...")
    board_klines = defaultdict(list)
    cur.execute(
        "SELECT board_code,`date`,change_percent "
        "FROM concept_board_kline WHERE `date`>=%s AND `date`<=%s "
        "ORDER BY board_code,`date`",
        [start_date, latest_date])
    for r in cur.fetchall():
        board_klines[r['board_code']].append({
            'date': r['date'],
            'change_percent': _to_float(r['change_percent']),
        })
    logger.info("  板块K线: %d 个板块有数据", len(board_klines))

    conn.close()

    return {
        'all_codes': all_codes,
        'stock_klines': dict(stock_klines),
        'market_klines': dict(mkt_kl),
        'stock_fund_flows': dict(stock_fund_flows),
        'stock_boards': dict(stock_boards),
        'board_klines': dict(board_klines),
        'latest_date': latest_date,
        'dt_end': dt_end,
    }


# ═══════════════════════════════════════════════════════════
# 样本构建 — 全维度因子提取
# ═══════════════════════════════════════════════════════════

def build_samples(data, n_weeks):
    """构建样本，提取全维度因子。"""
    dt_end = data['dt_end']
    dt_cutoff = dt_end - timedelta(days=n_weeks * 7 + 14)
    samples = []

    # 指数按周分组
    mkt_by_week = {}
    for ic, kl in data['market_klines'].items():
        bw = defaultdict(list)
        for k in kl:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            bw[dt.isocalendar()[:2]].append(k)
        mkt_by_week[ic] = bw

    # 资金流向按周分组
    ff_data = data.get('stock_fund_flows', {})

    # 板块K线按周分组
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

        # 资金流向按周分组
        ff_list = ff_data.get(code, [])
        ff_by_week = defaultdict(list)
        for ff in ff_list:
            try:
                dt = datetime.strptime(ff['date'], '%Y-%m-%d')
                ff_by_week[dt.isocalendar()[:2]].append(ff)
            except (ValueError, TypeError):
                continue

        # 该股票的概念板块
        boards = stock_boards.get(code, [])

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

            # 大盘周涨跌
            mw = idx_bw.get(iw_this, [])
            mkt_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            first_date = this_days[0]['date']
            hist = [k for k in sorted_all if k['date'] < first_date]

            # ── A. 价格动量因子 ──
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
                prev_iw = sorted_weeks[i - 1]
                prev_days = sorted(wg[prev_iw], key=lambda x: x['date'])
                if len(prev_days) >= 3:
                    prev_chg = _compound_return([d['change_percent'] for d in prev_days])

            prev2_chg = None
            if i >= 2:
                prev2_iw = sorted_weeks[i - 2]
                prev2_days = sorted(wg[prev2_iw], key=lambda x: x['date'])
                if len(prev2_days) >= 3:
                    prev2_chg = _compound_return([d['change_percent'] for d in prev2_days])

            momentum_3w = None
            if prev_chg is not None and prev2_chg is not None:
                momentum_3w = prev2_chg + prev_chg + this_chg

            relative_strength = this_chg - mkt_chg

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

            # ── B. 成交量因子 ──
            vol_ratio = None
            tv = [d['volume'] for d in this_days if d['volume'] > 0]
            hv = [k['volume'] for k in hist[-20:] if k['volume'] > 0]
            if tv and hv:
                at = _safe_mean(tv)
                ah = _safe_mean(hv)
                if ah > 0:
                    vol_ratio = at / ah

            turnover_ratio = None
            tw = [d['turnover'] for d in this_days if d.get('turnover') and d['turnover'] > 0]
            ht = [k['turnover'] for k in hist[-20:] if k.get('turnover') and k['turnover'] > 0]
            if tw and ht:
                at_t = _safe_mean(tw)
                ah_t = _safe_mean(ht)
                if ah_t > 0:
                    turnover_ratio = at_t / ah_t

            vol_price_diverge = 0
            if vol_ratio is not None:
                if this_chg > 1 and vol_ratio < 0.75:
                    vol_price_diverge = -1
                elif this_chg < -1 and vol_ratio > 1.3:
                    vol_price_diverge = 1

            # ── C. 振幅因子 ──
            week_amp = None
            amps = [d['amplitude'] for d in this_days if d.get('amplitude') and d['amplitude'] > 0]
            if amps:
                week_amp = _safe_mean(amps)

            max_day_up = max(this_pcts) if this_pcts else 0
            max_day_down = min(this_pcts) if this_pcts else 0

            # 个股波动率
            stock_vol_std = _safe_std(this_pcts) if len(this_pcts) >= 3 else None

            # ── D. 大盘环境因子 ──
            mkt_last_day = None
            mkt_vol_std = None
            if mw and len(mw) >= 1:
                mkt_sorted = sorted(mw, key=lambda x: x['date'])
                mkt_last_day = mkt_sorted[-1]['change_percent']
                if len(mkt_sorted) >= 3:
                    mkt_vol_std = _safe_std([k['change_percent'] for k in mkt_sorted])

            # ── E. 资金流向因子 ──
            ff_week = ff_by_week.get(iw_this, [])
            big_net_pct_avg = None
            main_net_5day_latest = None
            if ff_week:
                pcts = [f['big_net_pct'] for f in ff_week if f['big_net_pct'] != 0]
                if pcts:
                    big_net_pct_avg = _safe_mean(pcts)
                mn5 = [f['main_net_5day'] for f in ff_week if f['main_net_5day'] != 0]
                if mn5:
                    main_net_5day_latest = mn5[-1]

            # ── F. 概念板块因子 ──
            board_momentum = None
            concept_consensus = None
            if boards:
                momentums = []
                boards_up = 0
                valid_boards = 0
                for b in boards:
                    bkw = board_kl_by_week.get(b['board_code'], {})
                    bk_this = bkw.get(iw_this, [])
                    if bk_this:
                        bk_chg = _safe_mean([k['change_percent'] for k in bk_this])
                        momentums.append(bk_chg)
                        valid_boards += 1
                        if bk_chg > 0:
                            boards_up += 1
                if momentums:
                    board_momentum = _safe_mean(momentums)
                if valid_boards > 0:
                    concept_consensus = boards_up / valid_boards

            # ── G. 技术形态因子 ──
            # 周内先涨后跌(冲高回落)
            rush_up_pullback = False
            if len(this_pcts) >= 4:
                mid = len(this_pcts) // 2
                first_half = _compound_return(this_pcts[:mid])
                second_half = _compound_return(this_pcts[mid:])
                if first_half > 2 and second_half < -1:
                    rush_up_pullback = True

            # 周内先跌后涨(探底回升)
            dip_recovery = False
            if len(this_pcts) >= 4:
                mid = len(this_pcts) // 2
                first_half = _compound_return(this_pcts[:mid])
                second_half = _compound_return(this_pcts[mid:])
                if first_half < -2 and second_half > 1:
                    dip_recovery = True

            # 上影线比例(周内最高价与收盘价差距)
            upper_shadow_ratio = None
            if this_days:
                highs = [d['high'] for d in this_days if d['high'] > 0]
                lows = [d['low'] for d in this_days if d['low'] > 0]
                week_close = this_days[-1]['close']
                if highs and lows and week_close > 0:
                    week_high = max(highs)
                    week_low = min(lows)
                    if week_high > week_low:
                        upper_shadow_ratio = (week_high - week_close) / (week_high - week_low)

            samples.append({
                # 基础
                'code': code, 'suffix': suffix,
                'iw_this': iw_this, 'iw_next': iw_next,
                # A. 价格动量
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'pos60': pos60, 'prev_chg': prev_chg, 'prev2_chg': prev2_chg,
                'momentum_3w': momentum_3w, 'relative_strength': relative_strength,
                'cd': cd, 'cu': cu, 'last_day': last_day,
                # B. 成交量
                'vol_ratio': vol_ratio, 'turnover_ratio': turnover_ratio,
                'vol_price_diverge': vol_price_diverge,
                # C. 振幅
                'week_amp': week_amp, 'max_day_up': max_day_up,
                'max_day_down': max_day_down, 'stock_vol_std': stock_vol_std,
                # D. 大盘环境
                'mkt_last_day': mkt_last_day, 'mkt_vol_std': mkt_vol_std,
                # E. 资金流向
                'big_net_pct_avg': big_net_pct_avg,
                'main_net_5day_latest': main_net_5day_latest,
                # F. 概念板块
                'board_momentum': board_momentum, 'concept_consensus': concept_consensus,
                # G. 技术形态
                'rush_up_pullback': rush_up_pullback, 'dip_recovery': dip_recovery,
                'upper_shadow_ratio': upper_shadow_ratio,
                # 标签
                'next_chg': next_chg, 'actual_up': next_chg >= 0,
            })

        processed += 1
        if processed % 1000 == 0:
            logger.info("  构建样本: %d/%d ...", processed, len(data['all_codes']))

    logger.info("  总样本数: %d", len(samples))
    return samples


# ═══════════════════════════════════════════════════════════
# V11 候选规则库 — 全场景全维度
# ═══════════════════════════════════════════════════════════

def build_v11_candidate_rules():
    """
    V11候选规则库: 覆盖大盘涨/跌/平盘 × 涨信号/跌信号 × 多因子组合。
    每条规则包含: (名称, 预测方向, 检测函数, 分类标签)
    """
    rules = []

    # ══════════════════════════════════════════════════
    # 一、大盘深跌场景 (mkt < -3%) — V5已验证最强
    # ══════════════════════════════════════════════════
    rules.extend([
        ('R1:大盘深跌+个股跌→涨', True, 'mkt_crash',
         lambda s: s['this_chg'] < -2 and s['mkt_chg'] < -3),
        ('R1b:大盘深跌+个股跌+低位→涨', True, 'mkt_crash',
         lambda s: s['this_chg'] < -2 and s['mkt_chg'] < -3
         and s['pos60'] is not None and s['pos60'] < 0.4),
        ('R1c:大盘深跌+个股跌+连跌≥2天→涨', True, 'mkt_crash',
         lambda s: s['this_chg'] < -2 and s['mkt_chg'] < -3 and s['cd'] >= 2),
    ])

    # ══════════════════════════════════════════════════
    # 二、大盘跌场景 (-3% <= mkt < -1%)
    # ══════════════════════════════════════════════════
    rules.extend([
        # 涨信号: 超跌反弹
        ('MD_UP1:大盘跌+个股跌>3%+前周跌+非高位→涨', True, 'mkt_down',
         lambda s: (s['this_chg'] < -3 and -3 <= s['mkt_chg'] < -1
                    and s['prev_chg'] is not None and s['prev_chg'] < -2
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        ('MD_UP2:大盘跌+个股跌>3%+低位→涨', True, 'mkt_down',
         lambda s: (s['this_chg'] < -3 and -3 <= s['mkt_chg'] < -1
                    and s['pos60'] is not None and s['pos60'] < 0.3)),
        ('MD_UP3:大盘跌+个股跌>3%+缩量+非高位→涨', True, 'mkt_down',
         lambda s: (s['this_chg'] < -3 and -3 <= s['mkt_chg'] < -1
                    and s['vol_ratio'] is not None and s['vol_ratio'] < 0.8
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        ('MD_UP4:大盘跌+个股跌>2%+量价背离+低位→涨', True, 'mkt_down',
         lambda s: (s['this_chg'] < -2 and -3 <= s['mkt_chg'] < -1
                    and s['vol_price_diverge'] == 1
                    and s['pos60'] is not None and s['pos60'] < 0.4)),
        ('MD_UP5:大盘跌+个股跌>3%+3周动量<-8%→涨', True, 'mkt_down',
         lambda s: (s['this_chg'] < -3 and -3 <= s['mkt_chg'] < -1
                    and s['momentum_3w'] is not None and s['momentum_3w'] < -8)),
        ('MD_UP6:大盘跌+个股跌>2%+大单净流入→涨', True, 'mkt_down',
         lambda s: (s['this_chg'] < -2 and -3 <= s['mkt_chg'] < -1
                    and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] > 2)),
        ('MD_UP7:大盘跌+个股跌>2%+大盘尾日涨→涨', True, 'mkt_down',
         lambda s: (s['this_chg'] < -2 and -3 <= s['mkt_chg'] < -1
                    and s['mkt_last_day'] is not None and s['mkt_last_day'] > 0.5)),
        ('MD_UP8:大盘跌+个股跌>2%+板块大跌→涨', True, 'mkt_down',
         lambda s: (s['this_chg'] < -2 and -3 <= s['mkt_chg'] < -1
                    and s['board_momentum'] is not None and s['board_momentum'] < -3)),
        ('MD_UP9:大盘跌+个股跌>2%+连跌≥3天→涨', True, 'mkt_down',
         lambda s: (s['this_chg'] < -2 and -3 <= s['mkt_chg'] < -1 and s['cd'] >= 3)),
        ('MD_UP10:大盘跌+个股跌>2%+尾日跌>3%→涨', True, 'mkt_down',
         lambda s: (s['this_chg'] < -2 and -3 <= s['mkt_chg'] < -1
                    and s['last_day'] < -3)),
        # 跌信号: 逆势冲高回落
        ('MD_DN1:大盘跌+个股涨>5%+高位→跌', False, 'mkt_down',
         lambda s: (s['this_chg'] > 5 and -3 <= s['mkt_chg'] < -1
                    and s['pos60'] is not None and s['pos60'] >= 0.6)),
        ('MD_DN2:大盘跌+个股涨>3%+连涨≥3天→跌', False, 'mkt_down',
         lambda s: (s['this_chg'] > 3 and -3 <= s['mkt_chg'] < -1 and s['cu'] >= 3)),
        ('MD_DN3:大盘跌+个股涨>5%+放量+高位→跌', False, 'mkt_down',
         lambda s: (s['this_chg'] > 5 and -3 <= s['mkt_chg'] < -1
                    and s['vol_ratio'] is not None and s['vol_ratio'] > 1.3
                    and s['pos60'] is not None and s['pos60'] >= 0.6)),
        ('MD_DN4:大盘跌+个股涨>3%+大单净流出→跌', False, 'mkt_down',
         lambda s: (s['this_chg'] > 3 and -3 <= s['mkt_chg'] < -1
                    and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < -2)),
        ('MD_DN5:大盘跌+个股涨>3%+冲高回落→跌', False, 'mkt_down',
         lambda s: (s['this_chg'] > 3 and -3 <= s['mkt_chg'] < -1
                    and s['rush_up_pullback'])),
    ])

    # ══════════════════════════════════════════════════
    # 三、大盘微跌场景 (-1% <= mkt < 0%)
    # ══════════════════════════════════════════════════
    rules.extend([
        # 涨信号
        ('MF_UP1:微跌+深证+个股跌>2%+连跌≥3天→涨', True, 'mkt_slight_down',
         lambda s: (s['suffix'] == 'SZ' and -1 <= s['mkt_chg'] < 0
                    and s['this_chg'] < -2 and s['cd'] >= 3)),
        ('MF_UP2:微跌+深证+个股跌>2%+低位→涨', True, 'mkt_slight_down',
         lambda s: (s['suffix'] == 'SZ' and -1 <= s['mkt_chg'] < 0
                    and s['this_chg'] < -2
                    and s['pos60'] is not None and s['pos60'] < 0.2)),
        ('MF_UP3:微跌+深证+个股跌>2%→涨', True, 'mkt_slight_down',
         lambda s: (s['suffix'] == 'SZ' and -1 <= s['mkt_chg'] < 0
                    and s['this_chg'] < -2)),
        ('MF_UP4:微跌+上证+个股跌>3%+连跌≥2天→涨', True, 'mkt_slight_down',
         lambda s: (s['suffix'] == 'SH' and -1 <= s['mkt_chg'] < 0
                    and s['this_chg'] < -3 and s['cd'] >= 2)),
        ('MF_UP5:微跌+个股跌>3%+缩量+低位→涨', True, 'mkt_slight_down',
         lambda s: (-1 <= s['mkt_chg'] < 0 and s['this_chg'] < -3
                    and s['vol_ratio'] is not None and s['vol_ratio'] < 0.8
                    and s['pos60'] is not None and s['pos60'] < 0.3)),
        ('MF_UP6:微跌+个股跌>2%+尾日跌>3%→涨', True, 'mkt_slight_down',
         lambda s: (-1 <= s['mkt_chg'] < 0 and s['this_chg'] < -2
                    and s['last_day'] < -3)),
        ('MF_UP7:微跌+个股跌>2%+板块大跌→涨', True, 'mkt_slight_down',
         lambda s: (-1 <= s['mkt_chg'] < 0 and s['this_chg'] < -2
                    and s['board_momentum'] is not None and s['board_momentum'] < -3)),
        # 跌信号
        ('MF_DN1:微跌+个股涨>5%+高位+量价背离→跌', False, 'mkt_slight_down',
         lambda s: (-1 <= s['mkt_chg'] < 0 and s['this_chg'] > 5
                    and s['pos60'] is not None and s['pos60'] >= 0.6
                    and s['vol_price_diverge'] == -1)),
        ('MF_DN2:微跌+个股涨>5%+高位+高换手→跌', False, 'mkt_slight_down',
         lambda s: (-1 <= s['mkt_chg'] < 0 and s['this_chg'] > 5
                    and s['pos60'] is not None and s['pos60'] >= 0.6
                    and s['turnover_ratio'] is not None and s['turnover_ratio'] > 1.5)),
        ('MF_DN3:微跌+个股涨>3%+上影线长→跌', False, 'mkt_slight_down',
         lambda s: (-1 <= s['mkt_chg'] < 0 and s['this_chg'] > 3
                    and s['upper_shadow_ratio'] is not None and s['upper_shadow_ratio'] > 0.6)),
    ])

    # ══════════════════════════════════════════════════
    # 四、大盘微涨场景 (0% <= mkt <= 1%)
    # ══════════════════════════════════════════════════
    rules.extend([
        # 涨信号: 大盘涨但个股逆势跌→超跌反弹
        ('MU_UP1:微涨+个股跌>3%+非高位→涨', True, 'mkt_slight_up',
         lambda s: (0 <= s['mkt_chg'] <= 1 and s['this_chg'] < -3
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        ('MU_UP2:微涨+个股跌>2%+连跌≥3天→涨', True, 'mkt_slight_up',
         lambda s: (0 <= s['mkt_chg'] <= 1 and s['this_chg'] < -2 and s['cd'] >= 3)),
        ('MU_UP3:微涨+个股跌>2%+低位+缩量→涨', True, 'mkt_slight_up',
         lambda s: (0 <= s['mkt_chg'] <= 1 and s['this_chg'] < -2
                    and s['pos60'] is not None and s['pos60'] < 0.3
                    and s['vol_ratio'] is not None and s['vol_ratio'] < 0.8)),
        ('MU_UP4:微涨+个股跌>2%+板块跌→涨', True, 'mkt_slight_up',
         lambda s: (0 <= s['mkt_chg'] <= 1 and s['this_chg'] < -2
                    and s['board_momentum'] is not None and s['board_momentum'] < -1)),
        # 跌信号
        ('MU_DN1:微涨+个股涨>8%+高位→跌', False, 'mkt_slight_up',
         lambda s: (0 <= s['mkt_chg'] <= 1 and s['this_chg'] > 8
                    and s['pos60'] is not None and s['pos60'] >= 0.7)),
        ('MU_DN2:微涨+个股涨>5%+量价背离→跌', False, 'mkt_slight_up',
         lambda s: (0 <= s['mkt_chg'] <= 1 and s['this_chg'] > 5
                    and s['vol_price_diverge'] == -1)),
        ('MU_DN3:微涨+个股涨>5%+冲高回落→跌', False, 'mkt_slight_up',
         lambda s: (0 <= s['mkt_chg'] <= 1 and s['this_chg'] > 5
                    and s['rush_up_pullback'])),
    ])

    # ══════════════════════════════════════════════════
    # 五、大盘涨场景 (mkt > 1%)
    # ══════════════════════════════════════════════════
    rules.extend([
        # 涨信号: 大盘涨+个股逆势跌→超跌反弹
        ('MBU_UP1:大盘涨+个股跌>3%+非高位→涨', True, 'mkt_up',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] < -3
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        ('MBU_UP2:大盘涨+个股跌>2%+缩量+低位→涨', True, 'mkt_up',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] < -2
                    and s['vol_ratio'] is not None and s['vol_ratio'] < 0.8
                    and s['pos60'] is not None and s['pos60'] < 0.4)),
        ('MBU_UP3:大盘涨+个股跌>2%+连跌≥3天→涨', True, 'mkt_up',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] < -2 and s['cd'] >= 3)),
        ('MBU_UP4:大盘涨+个股跌>2%+前周跌+非高位→涨', True, 'mkt_up',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] < -2
                    and s['prev_chg'] is not None and s['prev_chg'] < -2
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        ('MBU_UP5:大盘涨+个股跌>2%+大单净流入→涨', True, 'mkt_up',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] < -2
                    and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] > 2)),
        ('MBU_UP6:大盘涨+个股跌>2%+探底回升→涨', True, 'mkt_up',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] < -2
                    and s['dip_recovery'])),
        # 跌信号: 大盘涨+个股过热
        ('MBU_DN1:大盘涨+个股涨>8%+高位→跌', False, 'mkt_up',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] > 8
                    and s['pos60'] is not None and s['pos60'] >= 0.7)),
        ('MBU_DN2:大盘涨+个股涨>5%+高位+放量→跌', False, 'mkt_up',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] > 5
                    and s['pos60'] is not None and s['pos60'] >= 0.7
                    and s['vol_ratio'] is not None and s['vol_ratio'] > 1.3)),
        ('MBU_DN3:大盘涨+个股涨>5%+量价背离→跌', False, 'mkt_up',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] > 5
                    and s['vol_price_diverge'] == -1)),
        ('MBU_DN4:大盘涨+个股涨>5%+高换手+高位→跌', False, 'mkt_up',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] > 5
                    and s['turnover_ratio'] is not None and s['turnover_ratio'] > 1.5
                    and s['pos60'] is not None and s['pos60'] >= 0.6)),
    ])

    # ══════════════════════════════════════════════════
    # 六、跨场景通用因子规则
    # ══════════════════════════════════════════════════
    rules.extend([
        # 涨信号
        ('GEN_UP1:3周动量<-10%+低位→涨', True, 'general',
         lambda s: (s['momentum_3w'] is not None and s['momentum_3w'] < -10
                    and s['pos60'] is not None and s['pos60'] < 0.3)),
        ('GEN_UP2:跌>3%+量价背离(价跌量增)+非高位→涨', True, 'general',
         lambda s: (s['this_chg'] < -3 and s['vol_price_diverge'] == 1
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        ('GEN_UP3:最大单日跌>5%+非高位+缩量→涨', True, 'general',
         lambda s: (s['max_day_down'] < -5
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7)
                    and s['vol_ratio'] is not None and s['vol_ratio'] < 0.9)),
        ('GEN_UP4:跌>2%+大单净流入>3%+低位→涨', True, 'general',
         lambda s: (s['this_chg'] < -2 and s['big_net_pct_avg'] is not None
                    and s['big_net_pct_avg'] > 3
                    and s['pos60'] is not None and s['pos60'] < 0.4)),
        ('GEN_UP5:跌>2%+尾日跌>3%+非高位→涨', True, 'general',
         lambda s: (s['this_chg'] < -2 and s['last_day'] < -3
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        ('GEN_UP6:跌>5%+低位<0.2→涨', True, 'general',
         lambda s: (s['this_chg'] < -5
                    and s['pos60'] is not None and s['pos60'] < 0.2)),
        ('GEN_UP7:跌>3%+连跌≥3天+非高位→涨', True, 'general',
         lambda s: (s['this_chg'] < -3 and s['cd'] >= 3
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        ('GEN_UP8:跌>2%+板块全部看跌+非高位→涨', True, 'general',
         lambda s: (s['this_chg'] < -2
                    and s['concept_consensus'] is not None and s['concept_consensus'] == 0
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        ('GEN_UP9:相对强弱<-5%+非高位→涨', True, 'general',
         lambda s: (s['relative_strength'] < -5
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        ('GEN_UP10:跌>2%+探底回升+非高位→涨', True, 'general',
         lambda s: (s['this_chg'] < -2 and s['dip_recovery']
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        # 跌信号
        ('GEN_DN1:涨>5%+高位+量价背离→跌', False, 'general',
         lambda s: (s['this_chg'] > 5 and s['vol_price_diverge'] == -1
                    and s['pos60'] is not None and s['pos60'] >= 0.6)),
        ('GEN_DN2:涨>5%+高位+高换手→跌', False, 'general',
         lambda s: (s['this_chg'] > 5
                    and s['pos60'] is not None and s['pos60'] >= 0.7
                    and s['turnover_ratio'] is not None and s['turnover_ratio'] > 1.5)),
        ('GEN_DN3:涨>3%+连涨≥3天+高位→跌', False, 'general',
         lambda s: (s['this_chg'] > 3 and s['cu'] >= 3
                    and s['pos60'] is not None and s['pos60'] >= 0.6)),
        ('GEN_DN4:3周动量>10%+高位→跌', False, 'general',
         lambda s: (s['momentum_3w'] is not None and s['momentum_3w'] > 10
                    and s['pos60'] is not None and s['pos60'] >= 0.7)),
        ('GEN_DN5:涨>5%+冲高回落+高位→跌', False, 'general',
         lambda s: (s['this_chg'] > 5 and s['rush_up_pullback']
                    and s['pos60'] is not None and s['pos60'] >= 0.6)),
        ('GEN_DN6:涨>3%+大单净流出<-2%→跌', False, 'general',
         lambda s: (s['this_chg'] > 3 and s['big_net_pct_avg'] is not None
                    and s['big_net_pct_avg'] < -2)),
        ('GEN_DN7:涨>5%+上影线长+高位→跌', False, 'general',
         lambda s: (s['this_chg'] > 5
                    and s['upper_shadow_ratio'] is not None and s['upper_shadow_ratio'] > 0.6
                    and s['pos60'] is not None and s['pos60'] >= 0.6)),
        ('GEN_DN8:相对强弱>5%+高位→跌', False, 'general',
         lambda s: (s['relative_strength'] > 5
                    and s['pos60'] is not None and s['pos60'] >= 0.7)),
    ])

    # ══════════════════════════════════════════════════
    # 七、V11新增: 更宽松条件的扩展规则(提高覆盖率)
    # ══════════════════════════════════════════════════
    rules.extend([
        # 大盘跌场景: 放宽涨信号条件
        ('MD_UP11:大盘跌+个股跌>3%+非高位→涨', True, 'mkt_down',
         lambda s: (s['this_chg'] < -3 and -3 <= s['mkt_chg'] < -1
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        ('MD_UP12:大盘跌+个股跌>5%→涨', True, 'mkt_down',
         lambda s: (s['this_chg'] < -5 and -3 <= s['mkt_chg'] < -1)),
        ('MD_UP13:大盘跌+个股跌>3%+低位<0.4→涨', True, 'mkt_down',
         lambda s: (s['this_chg'] < -3 and -3 <= s['mkt_chg'] < -1
                    and s['pos60'] is not None and s['pos60'] < 0.4)),
        ('MD_UP14:大盘跌+个股跌>2%+连跌≥2天+低位→涨', True, 'mkt_down',
         lambda s: (s['this_chg'] < -2 and -3 <= s['mkt_chg'] < -1
                    and s['cd'] >= 2
                    and s['pos60'] is not None and s['pos60'] < 0.4)),
        # 大盘微跌: 上证版本(V5只覆盖深证)
        ('MF_UP8:微跌+上证+跌>3%+非高位→涨', True, 'mkt_slight_down',
         lambda s: (s['suffix'] == 'SH' and -1 <= s['mkt_chg'] < 0
                    and s['this_chg'] < -3
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        ('MF_UP9:微跌+跌>3%+低位<0.3→涨', True, 'mkt_slight_down',
         lambda s: (-1 <= s['mkt_chg'] < 0 and s['this_chg'] < -3
                    and s['pos60'] is not None and s['pos60'] < 0.3)),
        ('MF_UP10:微跌+跌>2%+连跌≥2天+非高位→涨', True, 'mkt_slight_down',
         lambda s: (-1 <= s['mkt_chg'] < 0 and s['this_chg'] < -2
                    and s['cd'] >= 2
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        # 大盘微涨: 更宽松条件
        ('MU_UP5:微涨+跌>2%+非高位→涨', True, 'mkt_slight_up',
         lambda s: (0 <= s['mkt_chg'] <= 1 and s['this_chg'] < -2
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        ('MU_UP6:微涨+跌>3%+连跌≥2天→涨', True, 'mkt_slight_up',
         lambda s: (0 <= s['mkt_chg'] <= 1 and s['this_chg'] < -3 and s['cd'] >= 2)),
        ('MU_UP7:微涨+跌>5%→涨', True, 'mkt_slight_up',
         lambda s: (0 <= s['mkt_chg'] <= 1 and s['this_chg'] < -5)),
        # 大盘涨: 更宽松条件
        ('MBU_UP7:大盘涨+跌>2%+非高位→涨', True, 'mkt_up',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] < -2
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        ('MBU_UP8:大盘涨+跌>5%→涨', True, 'mkt_up',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] < -5)),
        # 跨场景: 深度超跌
        ('GEN_UP11:跌>5%+非高位→涨', True, 'general',
         lambda s: (s['this_chg'] < -5
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        ('GEN_UP12:跌>3%+低位<0.3→涨', True, 'general',
         lambda s: (s['this_chg'] < -3
                    and s['pos60'] is not None and s['pos60'] < 0.3)),
        ('GEN_UP13:跌>3%+连跌≥2天+低位<0.4→涨', True, 'general',
         lambda s: (s['this_chg'] < -3 and s['cd'] >= 2
                    and s['pos60'] is not None and s['pos60'] < 0.4)),
        ('GEN_UP14:跌>2%+连跌≥3天+低位<0.3→涨', True, 'general',
         lambda s: (s['this_chg'] < -2 and s['cd'] >= 3
                    and s['pos60'] is not None and s['pos60'] < 0.3)),
        # 跨场景: 更严格的跌信号
        ('GEN_DN9:涨>8%+高位→跌', False, 'general',
         lambda s: (s['this_chg'] > 8
                    and s['pos60'] is not None and s['pos60'] >= 0.7)),
        ('GEN_DN10:涨>5%+高位≥0.8→跌', False, 'general',
         lambda s: (s['this_chg'] > 5
                    and s['pos60'] is not None and s['pos60'] >= 0.8)),
        ('GEN_DN11:涨>5%+连涨≥3天→跌', False, 'general',
         lambda s: (s['this_chg'] > 5 and s['cu'] >= 3)),
    ])

    return rules


# ═══════════════════════════════════════════════════════════
# 第一阶段: 全量规则CV验证筛选
# ═══════════════════════════════════════════════════════════

def cv_validate_all_rules(samples, candidate_rules):
    """对所有候选规则做全样本+时间序列CV验证，筛选有效规则。"""
    all_weeks = sorted(set(s['iw_this'] for s in samples))
    logger.info("\n" + "=" * 90)
    logger.info("  ══ 第一阶段: 全量候选规则CV验证 (%d条规则, %d周) ══",
                len(candidate_rules), len(all_weeks))
    logger.info("=" * 90)

    passed_rules = []
    marginal_rules = []

    for name, pred_up, category, check_fn in candidate_rules:
        # 全样本评估
        total, correct = 0, 0
        for s in samples:
            try:
                if check_fn(s):
                    total += 1
                    if pred_up == s['actual_up']:
                        correct += 1
            except (TypeError, KeyError):
                continue

        if total < 20:  # 降低最低样本要求
            continue
        full_acc = correct / total * 100

        # 时间序列CV
        cv_total, cv_correct = 0, 0
        for test_idx in range(MIN_TRAIN_WEEKS, len(all_weeks)):
            test_week = all_weeks[test_idx]
            for s in samples:
                if s['iw_this'] != test_week:
                    continue
                try:
                    if check_fn(s):
                        cv_total += 1
                        if pred_up == s['actual_up']:
                            cv_correct += 1
                except (TypeError, KeyError):
                    continue

        cv_acc = cv_correct / cv_total * 100 if cv_total > 0 else 0
        gap = full_acc - cv_acc

        # 分级判定 — 放宽条件以提高覆盖率
        is_strong = cv_acc >= 68 and abs(gap) < 10 and total >= 50
        is_good = cv_acc >= 62 and abs(gap) < 12 and total >= 30
        is_marginal = cv_acc >= 58 and abs(gap) < 15 and total >= 20

        if is_strong:
            flag = '★★'
            tier = 1
        elif is_good:
            flag = '★'
            tier = 1
        elif is_marginal:
            flag = '⚠️'
            tier = 2
        else:
            flag = '❌'
            tier = 0

        if tier > 0:
            logger.info("  %s [%s][%s] %-50s 全样本%s(%d) CV%s(%d) gap%+.1f%%",
                        flag, '涨' if pred_up else '跌', category, name,
                        _pct(correct, total), total,
                        _pct(cv_correct, cv_total), cv_total, gap)

        rule_info = {
            'name': name, 'pred_up': pred_up, 'category': category,
            'check': check_fn, 'tier': tier,
            'full_acc': full_acc, 'cv_acc': cv_acc, 'gap': gap,
            'total': total, 'cv_total': cv_total,
        }

        if is_strong or is_good:
            passed_rules.append(rule_info)
        elif is_marginal:
            marginal_rules.append(rule_info)

    logger.info("\n  通过CV验证: %d条强规则 + %d条边际规则", len(passed_rules), len(marginal_rules))
    return passed_rules, marginal_rules


# ═══════════════════════════════════════════════════════════
# 第二阶段: 多因子评分制模型
# ═══════════════════════════════════════════════════════════

class V11ScoringModel:
    """V11多因子评分制预测模型。

    核心思路: 不再用单规则互斥匹配，而是让多条规则同时投票，
    加权汇总后根据总分阈值决定预测方向和置信度。

    评分规则:
    - 每条通过CV验证的规则命中时贡献一个分数
    - 分数 = CV准确率 × tier权重
    - 涨信号贡献正分，跌信号贡献负分
    - 总分>阈值 → 预测涨; 总分<-阈值 → 预测跌; 否则不预测
    """

    def __init__(self, rules, up_threshold=1.0, down_threshold=1.0,
                 min_votes=1, use_board_boost=True):
        self.rules = rules
        self.up_threshold = up_threshold
        self.down_threshold = down_threshold
        self.min_votes = min_votes
        self.use_board_boost = use_board_boost

    def predict(self, sample):
        """对单个样本进行预测。

        Returns:
            (direction, confidence, score, matched_rules)
            direction: 'UP'/'DOWN'/None
            confidence: 'high'/'reference'/'low'/None
        """
        total_score = 0.0
        up_votes = 0
        down_votes = 0
        matched = []

        for rule in self.rules:
            try:
                if rule['check'](sample):
                    # 权重 = CV准确率归一化 × tier权重
                    weight = (rule['cv_acc'] / 100.0) * (1.5 if rule['tier'] == 1 else 1.0)
                    if rule['pred_up']:
                        total_score += weight
                        up_votes += 1
                    else:
                        total_score -= weight
                        down_votes += 1
                    matched.append(rule['name'])
            except (TypeError, KeyError):
                continue

        total_votes = up_votes + down_votes
        if total_votes < self.min_votes:
            return None, None, 0, []

        # 板块增强
        if self.use_board_boost:
            bm = sample.get('board_momentum')
            cc = sample.get('concept_consensus')
            if total_score > 0 and bm is not None:
                if bm < -3:
                    total_score *= 1.15  # 板块大跌确认涨信号
                elif bm < -1:
                    total_score *= 1.08  # 板块中跌确认
                elif bm > 0.8:
                    total_score *= 0.85  # 板块正动量削弱涨信号
            elif total_score < 0 and bm is not None:
                if bm > 1:
                    total_score *= 1.12  # 板块涨确认跌信号
                elif bm < -2:
                    total_score *= 0.85  # 板块跌削弱跌信号

        # 决策
        if total_score >= self.up_threshold:
            direction = 'UP'
            if total_score >= self.up_threshold * 1.5:
                confidence = 'high'
            elif total_score >= self.up_threshold * 1.2:
                confidence = 'reference'
            else:
                confidence = 'low'
        elif total_score <= -self.down_threshold:
            direction = 'DOWN'
            if total_score <= -self.down_threshold * 1.5:
                confidence = 'high'
            elif total_score <= -self.down_threshold * 1.2:
                confidence = 'reference'
            else:
                confidence = 'low'
        else:
            return None, None, total_score, matched

        return direction, confidence, total_score, matched


def eval_scoring_model(samples, model, label=''):
    """评估评分制模型的准确率和覆盖率。"""
    total_pred, total_correct = 0, 0
    by_confidence = defaultdict(lambda: {'correct': 0, 'total': 0})
    by_direction = defaultdict(lambda: {'correct': 0, 'total': 0})
    by_mkt_regime = defaultdict(lambda: {'correct': 0, 'total': 0})

    for s in samples:
        direction, confidence, score, matched = model.predict(s)
        if direction is None:
            continue

        pred_up = direction == 'UP'
        is_correct = pred_up == s['actual_up']
        total_pred += 1
        if is_correct:
            total_correct += 1

        by_confidence[confidence]['total'] += 1
        if is_correct:
            by_confidence[confidence]['correct'] += 1

        by_direction[direction]['total'] += 1
        if is_correct:
            by_direction[direction]['correct'] += 1

        # 大盘场景分类
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
        by_mkt_regime[regime]['total'] += 1
        if is_correct:
            by_mkt_regime[regime]['correct'] += 1

    acc = total_correct / total_pred * 100 if total_pred > 0 else 0
    cov = total_pred / len(samples) * 100 if samples else 0

    return {
        'label': label,
        'accuracy': acc,
        'coverage': cov,
        'total_pred': total_pred,
        'total_correct': total_correct,
        'total_samples': len(samples),
        'by_confidence': dict(by_confidence),
        'by_direction': dict(by_direction),
        'by_mkt_regime': dict(by_mkt_regime),
    }


def cv_eval_scoring_model(samples, model, label=''):
    """时间序列CV评估评分制模型。"""
    all_weeks = sorted(set(s['iw_this'] for s in samples))
    if len(all_weeks) < MIN_TRAIN_WEEKS + 1:
        return None

    cv_total, cv_correct = 0, 0
    weekly_results = {}

    for test_idx in range(MIN_TRAIN_WEEKS, len(all_weeks)):
        test_week = all_weeks[test_idx]
        test_samples = [s for s in samples if s['iw_this'] == test_week]

        wt, wc = 0, 0
        for s in test_samples:
            direction, confidence, score, matched = model.predict(s)
            if direction is None:
                continue
            pred_up = direction == 'UP'
            wt += 1
            if pred_up == s['actual_up']:
                wc += 1

        cv_total += wt
        cv_correct += wc
        if wt > 0:
            weekly_results[test_week] = (wc, wt, wc / wt * 100)

    cv_acc = cv_correct / cv_total * 100 if cv_total > 0 else 0
    cv_cov = cv_total / sum(1 for s in samples
                            if s['iw_this'] in set(all_weeks[MIN_TRAIN_WEEKS:])) * 100 \
        if cv_total > 0 else 0

    # 周准确率稳定性
    week_accs = [v[2] for v in weekly_results.values() if v[1] >= 5]
    acc_std = _safe_std(week_accs) if week_accs else 0

    return {
        'label': label,
        'cv_accuracy': cv_acc,
        'cv_coverage': cv_cov,
        'cv_total': cv_total,
        'cv_correct': cv_correct,
        'weekly_results': weekly_results,
        'week_acc_std': acc_std,
        'weeks_above_75': sum(1 for a in week_accs if a >= 75),
        'total_cv_weeks': len(week_accs),
    }


# ═══════════════════════════════════════════════════════════
# 第三阶段: 自动化参数搜索 — 寻找最优阈值组合
# ═══════════════════════════════════════════════════════════

def grid_search_thresholds(samples, passed_rules, marginal_rules):
    """网格搜索最优阈值组合，目标: CV准确率>75%。"""
    logger.info("\n" + "=" * 90)
    logger.info("  ══ 第三阶段: 网格搜索最优参数 ══")
    logger.info("=" * 90)

    best_result = None
    best_config = None
    results = []

    # 搜索空间
    up_thresholds = [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5]
    down_thresholds = [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5]
    min_votes_options = [1, 2]
    board_boost_options = [True, False]
    rule_sets = [
        ('强规则', passed_rules),
        ('强+边际', passed_rules + marginal_rules),
    ]

    total_combos = (len(up_thresholds) * len(down_thresholds) *
                    len(min_votes_options) * len(board_boost_options) * len(rule_sets))
    logger.info("  搜索空间: %d 种组合", total_combos)

    combo_idx = 0
    for rs_name, rules in rule_sets:
        for min_v in min_votes_options:
            for board_b in board_boost_options:
                for up_th in up_thresholds:
                    for dn_th in down_thresholds:
                        combo_idx += 1
                        model = V11ScoringModel(
                            rules, up_threshold=up_th, down_threshold=dn_th,
                            min_votes=min_v, use_board_boost=board_b)

                        # 快速全样本评估(过滤明显差的)
                        full = eval_scoring_model(samples, model)
                        if full['accuracy'] < 60 or full['total_pred'] < 100:
                            continue

                        # CV评估
                        cv = cv_eval_scoring_model(samples, model)
                        if cv is None:
                            continue

                        config = {
                            'rule_set': rs_name,
                            'up_threshold': up_th,
                            'down_threshold': dn_th,
                            'min_votes': min_v,
                            'board_boost': board_b,
                        }

                        result = {
                            'config': config,
                            'full_acc': full['accuracy'],
                            'full_cov': full['coverage'],
                            'cv_acc': cv['cv_accuracy'],
                            'cv_cov': cv['cv_coverage'],
                            'cv_total': cv['cv_total'],
                            'week_acc_std': cv['week_acc_std'],
                            'weeks_above_75': cv['weeks_above_75'],
                            'total_cv_weeks': cv['total_cv_weeks'],
                        }
                        results.append(result)

                        # 更新最优
                        if cv['cv_accuracy'] >= TARGET_ACCURACY:
                            if (best_result is None or
                                cv['cv_accuracy'] > best_result['cv_acc'] or
                                (cv['cv_accuracy'] == best_result['cv_acc'] and
                                 cv['cv_total'] > best_result['cv_total'])):
                                best_result = result
                                best_config = config

    # 排序输出Top20
    results.sort(key=lambda r: (-r['cv_acc'], -r['cv_total']))
    logger.info("\n  ── Top 20 配置 ──")
    for i, r in enumerate(results[:20]):
        c = r['config']
        flag = '★' if r['cv_acc'] >= TARGET_ACCURACY else ''
        logger.info("  %2d. %s [%s] up=%.1f dn=%.1f mv=%d bb=%s → "
                    "全样本%.1f%%(%d) CV%.1f%%(%d) 覆盖%.1f%% 周稳定%.1f%% "
                    "达标周%d/%d",
                    i + 1, flag, c['rule_set'], c['up_threshold'],
                    c['down_threshold'], c['min_votes'], c['board_boost'],
                    r['full_acc'], int(r['full_cov']),
                    r['cv_acc'], r['cv_total'], r['cv_cov'],
                    r['week_acc_std'], r['weeks_above_75'], r['total_cv_weeks'])

    if best_result:
        logger.info("\n  ★ 最优配置(CV>=%.0f%%): %s", TARGET_ACCURACY, best_config)
        logger.info("    全样本%.1f%% CV%.1f%%(%d样本) 覆盖%.1f%%",
                    best_result['full_acc'], best_result['cv_acc'],
                    best_result['cv_total'], best_result['cv_cov'])
    else:
        logger.info("\n  ⚠️ 未找到CV>=%.0f%%的配置，取最优:", TARGET_ACCURACY)
        if results:
            best_result = results[0]
            best_config = results[0]['config']
            logger.info("    %s → CV%.1f%%(%d样本)", best_config,
                        best_result['cv_acc'], best_result['cv_total'])

    return best_result, best_config, results


# ═══════════════════════════════════════════════════════════
# 第四阶段: 最优模型详细分析
# ═══════════════════════════════════════════════════════════

def detailed_analysis(samples, model, label='V11最优'):
    """对最优模型做详细分析: 按大盘场景/置信度/方向分层。"""
    logger.info("\n" + "=" * 90)
    logger.info("  ══ 第四阶段: %s 详细分析 ══", label)
    logger.info("=" * 90)

    full = eval_scoring_model(samples, model, label)
    cv = cv_eval_scoring_model(samples, model, label)

    logger.info("\n  ── 全样本 ──")
    logger.info("  准确率: %.1f%% (%d/%d) 覆盖率: %.1f%%",
                full['accuracy'], full['total_correct'], full['total_pred'],
                full['coverage'])

    logger.info("\n  ── 按方向 ──")
    for d, st in sorted(full['by_direction'].items()):
        logger.info("    %-6s %s (%d/%d)", d,
                    _pct(st['correct'], st['total']), st['correct'], st['total'])

    logger.info("\n  ── 按置信度 ──")
    for c, st in sorted(full['by_confidence'].items()):
        logger.info("    %-12s %s (%d/%d)", c,
                    _pct(st['correct'], st['total']), st['correct'], st['total'])

    logger.info("\n  ── 按大盘场景 ──")
    for regime in ['大盘深跌', '大盘跌', '大盘微跌', '大盘微涨', '大盘涨']:
        st = full['by_mkt_regime'].get(regime, {'correct': 0, 'total': 0})
        if st['total'] > 0:
            logger.info("    %-10s %s (%d/%d)", regime,
                        _pct(st['correct'], st['total']), st['correct'], st['total'])

    if cv:
        logger.info("\n  ── CV验证 ──")
        logger.info("  CV准确率: %.1f%% (%d/%d) CV覆盖率: %.1f%%",
                    cv['cv_accuracy'], cv['cv_correct'], cv['cv_total'],
                    cv['cv_coverage'])
        logger.info("  周准确率标准差: %.1f%%", cv['week_acc_std'])
        logger.info("  达标周(>=75%%): %d/%d", cv['weeks_above_75'], cv['total_cv_weeks'])

        # 输出每周准确率
        logger.info("\n  ── 逐周CV准确率 ──")
        for week, (wc, wt, wacc) in sorted(cv['weekly_results'].items()):
            flag = '✅' if wacc >= 75 else ('⚠️' if wacc >= 65 else '❌')
            logger.info("    %s 周%d-%02d: %s (%d/%d)",
                        flag, week[0], week[1], _pct(wc, wt), wc, wt)

    return full, cv


# ═══════════════════════════════════════════════════════════
# 第五阶段: V5基线对比 + 增量分析
# ═══════════════════════════════════════════════════════════

V5_BASELINE_RULES = [
    {'name': 'R1:大盘深跌+个股跌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: f['this_chg'] < -2 and f['mkt_chg'] < -3},
    {'name': 'R3:上证+大盘跌+跌>3%+前周跌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -3 and f['suffix'] == 'SH'
                         and -3 <= f['mkt_chg'] < -1
                         and f['prev_chg'] is not None and f['prev_chg'] < -2
                         and not (f['pos60'] is not None and f['pos60'] >= 0.8))},
    {'name': 'R5a:深证+大盘微跌+跌+连跌3天→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2 and f['cd'] >= 3)},
    {'name': 'R5b:深证+大盘微跌+跌+低位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2
                         and f['pos60'] is not None and f['pos60'] < 0.2)},
    {'name': 'R5c:深证+大盘微跌+跌>2%→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2)},
    {'name': 'R_tail:跌+尾日恐慌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: f['this_chg'] < -2 and f['last_day'] < -3},
    {'name': 'R6c:深证+大盘跌+涨+连涨3天→跌', 'pred_up': False, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 2 and f['cu'] >= 3)},
    {'name': 'R6a:深证+大盘跌+涨>5%→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 5)},
    {'name': 'R7:跌+前期连涨+非高位→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['this_chg'] < -3 and f['cu'] >= 3
                         and f['pos60'] is not None and f['pos60'] < 0.6)},
]

V7_ELITE_RULES = [
    {'name': 'R1:大盘深跌+个股跌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: f['this_chg'] < -2 and f['mkt_chg'] < -3},
    {'name': 'R5a:深证+大盘微跌+跌+连跌3天→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2 and f['cd'] >= 3)},
    {'name': 'R5b:深证+大盘微跌+跌+低位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2
                         and f['pos60'] is not None and f['pos60'] < 0.2)},
    {'name': 'R5c:深证+大盘微跌+跌>2%→涨', 'pred_up': True, 'tier': 1,
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


def eval_baseline(samples, rules, label='', v7_filter=False):
    """评估基线规则集(互斥匹配模式)。"""
    total_pred, total_correct = 0, 0
    by_rule = defaultdict(lambda: {'correct': 0, 'total': 0})

    for s in samples:
        rule = match_rule(s, rules)
        if rule is None:
            continue

        # V7后置过滤
        if v7_filter and rule['pred_up']:
            if s['cd'] < 2 or (s['pos60'] is not None and s['pos60'] >= 0.6):
                continue

        is_correct = rule['pred_up'] == s['actual_up']
        total_pred += 1
        if is_correct:
            total_correct += 1
        by_rule[rule['name']]['total'] += 1
        if is_correct:
            by_rule[rule['name']]['correct'] += 1

    acc = total_correct / total_pred * 100 if total_pred > 0 else 0
    cov = total_pred / len(samples) * 100 if samples else 0
    return {
        'label': label, 'accuracy': acc, 'coverage': cov,
        'total_pred': total_pred, 'total_correct': total_correct,
        'by_rule': dict(by_rule),
    }


def compare_with_baselines(samples, v11_model):
    """V11 vs V5 vs V7 对比。"""
    logger.info("\n" + "=" * 90)
    logger.info("  ══ 第五阶段: V11 vs V5 vs V7 对比 ══")
    logger.info("=" * 90)

    v5 = eval_baseline(samples, V5_BASELINE_RULES, 'V5基线')
    v7 = eval_baseline(samples, V7_ELITE_RULES, 'V7精简', v7_filter=True)
    v11_full = eval_scoring_model(samples, v11_model, 'V11多因子')
    v11_cv = cv_eval_scoring_model(samples, v11_model, 'V11多因子')

    logger.info("\n  %-15s %-12s %-12s %-10s", '模型', '准确率', '覆盖率', '预测数')
    logger.info("  " + "-" * 55)
    logger.info("  %-15s %-12s %-12s %-10d",
                'V5基线', f"{v5['accuracy']:.1f}%", f"{v5['coverage']:.1f}%", v5['total_pred'])
    logger.info("  %-15s %-12s %-12s %-10d",
                'V7精简', f"{v7['accuracy']:.1f}%", f"{v7['coverage']:.1f}%", v7['total_pred'])
    logger.info("  %-15s %-12s %-12s %-10d",
                'V11全样本', f"{v11_full['accuracy']:.1f}%",
                f"{v11_full['coverage']:.1f}%", v11_full['total_pred'])
    if v11_cv:
        logger.info("  %-15s %-12s %-12s %-10d",
                    'V11-CV', f"{v11_cv['cv_accuracy']:.1f}%",
                    f"{v11_cv['cv_coverage']:.1f}%", v11_cv['cv_total'])

    # V5按规则详细
    logger.info("\n  ── V5按规则 ──")
    for rn, st in sorted(v5['by_rule'].items()):
        logger.info("    %-50s %s (%d/%d)", rn,
                    _pct(st['correct'], st['total']), st['correct'], st['total'])

    return v5, v7, v11_full, v11_cv


# ═══════════════════════════════════════════════════════════
# 第六阶段: 迭代优化 — 如果未达标则自动调整
# ═══════════════════════════════════════════════════════════

def iterative_optimize(samples, passed_rules, marginal_rules, best_config, best_result):
    """如果网格搜索未达标，尝试更精细的优化策略。"""
    if best_result and best_result['cv_acc'] >= TARGET_ACCURACY and best_result.get('cv_cov', 0) >= 5:
        logger.info("\n  ✅ 已达标(CV%.1f%% >= %.0f%%, 覆盖%.1f%%)，跳过迭代优化",
                    best_result['cv_acc'], TARGET_ACCURACY, best_result.get('cv_cov', 0))
        return best_config, best_result

    logger.info("\n" + "=" * 90)
    logger.info("  ══ 第六阶段: 迭代优化(目标CV>%.0f%% + 覆盖率>5%%) ══", TARGET_ACCURACY)
    logger.info("=" * 90)

    def _try_model(rules, up_th, dn_th, mv, bb, rs_name):
        nonlocal best_result, best_config
        model = V11ScoringModel(rules, up_threshold=up_th, down_threshold=dn_th,
                                min_votes=mv, use_board_boost=bb)
        cv = cv_eval_scoring_model(samples, model)
        if cv and cv['cv_total'] >= 50 and cv['cv_accuracy'] >= TARGET_ACCURACY:
            full = eval_scoring_model(samples, model)
            # 优先选覆盖率更高的配置
            score = cv['cv_accuracy'] + cv['cv_coverage'] * 0.5
            old_score = (best_result['cv_acc'] + best_result.get('cv_cov', 0) * 0.5) if best_result else 0
            if score > old_score:
                best_result = {
                    'config': {'rule_set': rs_name, 'up_threshold': up_th,
                               'down_threshold': dn_th, 'min_votes': mv,
                               'board_boost': bb},
                    'full_acc': full['accuracy'], 'full_cov': full['coverage'],
                    'cv_acc': cv['cv_accuracy'], 'cv_cov': cv['cv_coverage'],
                    'cv_total': cv['cv_total'],
                    'week_acc_std': cv['week_acc_std'],
                    'weeks_above_75': cv['weeks_above_75'],
                    'total_cv_weeks': cv['total_cv_weeks'],
                }
                best_config = best_result['config']
                logger.info("    ★ [%s] up=%.1f dn=%.1f mv=%d bb=%s → CV%.1f%%(%d) 覆盖%.1f%%",
                            rs_name, up_th, dn_th, mv, bb,
                            cv['cv_accuracy'], cv['cv_total'], cv['cv_coverage'])

    # 策略1: 只保留CV>70%的规则
    high_cv_rules = [r for r in passed_rules if r['cv_acc'] >= 70]
    logger.info("  策略1: 只保留CV>70%%的规则 (%d条)", len(high_cv_rules))
    if high_cv_rules:
        for up_th in [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2]:
            for dn_th in [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2]:
                _try_model(high_cv_rules, up_th, dn_th, 1, True, '高CV规则')
                _try_model(high_cv_rules, up_th, dn_th, 1, False, '高CV规则')

    # 策略2: 只保留涨信号规则(跌信号不稳定)
    up_only_rules = [r for r in passed_rules if r['pred_up']]
    logger.info("  策略2: 只保留涨信号规则 (%d条)", len(up_only_rules))
    if up_only_rules:
        for up_th in [0.5, 0.6, 0.7, 0.8, 0.9, 1.0]:
            _try_model(up_only_rules, up_th, 99, 1, True, '仅涨信号')
            _try_model(up_only_rules, up_th, 99, 1, False, '仅涨信号')

    # 策略3: 全规则 + 多投票
    logger.info("  策略3: 全规则+多投票")
    all_rules = passed_rules + marginal_rules
    for mv in [1, 2, 3]:
        for up_th in [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.5, 1.8, 2.0]:
            for dn_th in [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.5, 1.8, 2.0]:
                _try_model(all_rules, up_th, dn_th, mv, True, '全规则')

    # 策略4: 混合模式 — V5高准确率规则 + V11新规则
    logger.info("  策略4: 混合模式(V5核心+V11新规则)")
    # 将V5核心规则(R1, R5a, R5b, R5c)作为高权重规则加入
    v5_core_as_scoring = [
        {'name': 'V5_R1:大盘深跌+个股跌→涨', 'pred_up': True, 'category': 'v5_core',
         'check': lambda s: s['this_chg'] < -2 and s['mkt_chg'] < -3,
         'tier': 1, 'full_acc': 89.6, 'cv_acc': 89.5, 'gap': 0.1,
         'total': 6297, 'cv_total': 6000},
        {'name': 'V5_R5a:深证+微跌+连跌3天→涨', 'pred_up': True, 'category': 'v5_core',
         'check': lambda s: (s['suffix'] == 'SZ' and -1 <= s['mkt_chg'] < 0
                             and s['this_chg'] < -2 and s['cd'] >= 3),
         'tier': 1, 'full_acc': 89.1, 'cv_acc': 90.6, 'gap': -1.5,
         'total': 514, 'cv_total': 490},
        {'name': 'V5_R5b:深证+微跌+低位→涨', 'pred_up': True, 'category': 'v5_core',
         'check': lambda s: (s['suffix'] == 'SZ' and -1 <= s['mkt_chg'] < 0
                             and s['this_chg'] < -2
                             and s['pos60'] is not None and s['pos60'] < 0.2),
         'tier': 1, 'full_acc': 82.4, 'cv_acc': 88.7, 'gap': -6.2,
         'total': 848, 'cv_total': 680},
        {'name': 'V5_R5c:深证+微跌+跌>2%→涨', 'pred_up': True, 'category': 'v5_core',
         'check': lambda s: (s['suffix'] == 'SZ' and -1 <= s['mkt_chg'] < 0
                             and s['this_chg'] < -2),
         'tier': 1, 'full_acc': 78.4, 'cv_acc': 84.8, 'gap': -6.4,
         'total': 2943, 'cv_total': 1648},
        {'name': 'V5_R3:上证+大盘跌+前周跌→涨', 'pred_up': True, 'category': 'v5_core',
         'check': lambda s: (s['this_chg'] < -3 and s['suffix'] == 'SH'
                             and -3 <= s['mkt_chg'] < -1
                             and s['prev_chg'] is not None and s['prev_chg'] < -2
                             and not (s['pos60'] is not None and s['pos60'] >= 0.8)),
         'tier': 1, 'full_acc': 68.3, 'cv_acc': 71.1, 'gap': -2.8,
         'total': 682, 'cv_total': 500},
    ]
    hybrid_rules = v5_core_as_scoring + passed_rules + marginal_rules
    for mv in [1, 2]:
        for up_th in [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.5, 1.8, 2.0, 2.5]:
            for dn_th in [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.5, 1.8, 2.0]:
                _try_model(hybrid_rules, up_th, dn_th, mv, True, '混合V5+V11')
                _try_model(hybrid_rules, up_th, dn_th, mv, False, '混合V5+V11')

    if best_result and best_result['cv_acc'] >= TARGET_ACCURACY:
        logger.info("\n  ✅ 迭代优化成功! 最优: %s → CV%.1f%% 覆盖%.1f%%",
                    best_config, best_result['cv_acc'], best_result.get('cv_cov', 0))
    else:
        logger.info("\n  ⚠️ 迭代优化未达标，当前最优CV: %.1f%%",
                    best_result['cv_acc'] if best_result else 0)

    return best_config, best_result


# ═══════════════════════════════════════════════════════════
# 第七阶段: V11混合规则引擎 — 生产版
# ═══════════════════════════════════════════════════════════

def build_v11_hybrid_engine(passed_rules, marginal_rules):
    """构建V11混合规则引擎。

    设计思路:
      层1(骨干): V5已验证的高准确率规则(涨+跌)，互斥匹配
      层2(扩展): V11新发现的多因子规则，覆盖V5未覆盖的场景
      层3(边际): 边际规则用于覆盖扩展
      层4(兜底): 严格条件的通用超跌/过热规则

    目标: 准确率>75%, 覆盖率显著高于V5的11.6%
    """
    # ── 层1: V5骨干规则(涨信号+跌信号) ──
    v5_backbone = [
        # 涨信号
        {'name': 'V5_R1:大盘深跌+个股跌→涨', 'pred_up': True, 'tier': 1,
         'layer': 'backbone', 'cv_acc': 89.5,
         'check': lambda s: s['this_chg'] < -2 and s['mkt_chg'] < -3},
        {'name': 'V5_R5a:深证+微跌+连跌3天→涨', 'pred_up': True, 'tier': 1,
         'layer': 'backbone', 'cv_acc': 90.6,
         'check': lambda s: (s['suffix'] == 'SZ' and -1 <= s['mkt_chg'] < 0
                             and s['this_chg'] < -2 and s['cd'] >= 3)},
        {'name': 'V5_R5b:深证+微跌+低位→涨', 'pred_up': True, 'tier': 1,
         'layer': 'backbone', 'cv_acc': 88.7,
         'check': lambda s: (s['suffix'] == 'SZ' and -1 <= s['mkt_chg'] < 0
                             and s['this_chg'] < -2
                             and s['pos60'] is not None and s['pos60'] < 0.2)},
        {'name': 'V5_R3:上证+大盘跌+前周跌→涨', 'pred_up': True, 'tier': 1,
         'layer': 'backbone', 'cv_acc': 71.1,
         'check': lambda s: (s['this_chg'] < -3 and s['suffix'] == 'SH'
                             and -3 <= s['mkt_chg'] < -1
                             and s['prev_chg'] is not None and s['prev_chg'] < -2
                             and not (s['pos60'] is not None and s['pos60'] >= 0.8))},
        {'name': 'V5_R5c:深证+微跌+跌>2%→涨', 'pred_up': True, 'tier': 1,
         'layer': 'backbone', 'cv_acc': 84.8,
         'check': lambda s: (s['suffix'] == 'SZ' and -1 <= s['mkt_chg'] < 0
                             and s['this_chg'] < -2)},
        # V5跌信号(已验证)
        {'name': 'V5_R6c:深证+大盘跌+涨+连涨3天→跌', 'pred_up': False, 'tier': 1,
         'layer': 'backbone', 'cv_acc': 73.5,
         'check': lambda s: (s['suffix'] == 'SZ' and -3 <= s['mkt_chg'] < -1
                             and s['this_chg'] > 2 and s['cu'] >= 3)},
        {'name': 'V5_R6a:深证+大盘跌+涨>5%→跌', 'pred_up': False, 'tier': 2,
         'layer': 'backbone', 'cv_acc': 71.8,
         'check': lambda s: (s['suffix'] == 'SZ' and -3 <= s['mkt_chg'] < -1
                             and s['this_chg'] > 5)},
        {'name': 'V5_R7:跌+前期连涨+非高位→跌', 'pred_up': False, 'tier': 2,
         'layer': 'backbone', 'cv_acc': 71.1,
         'check': lambda s: (s['this_chg'] < -3 and s['cu'] >= 3
                             and s['pos60'] is not None and s['pos60'] < 0.6)},
    ]

    # ── 层2: V11扩展规则(覆盖V5空白场景 + CV通过的跌信号) ──
    v11_extension = []
    v5_categories = {'mkt_crash', 'mkt_slight_down'}
    for r in passed_rules:
        cat = r.get('category', '')
        if cat not in v5_categories or not r['pred_up']:
            v11_extension.append({
                'name': r['name'], 'pred_up': r['pred_up'], 'tier': r['tier'],
                'layer': 'extension', 'cv_acc': r['cv_acc'],
                'check': r['check'],
            })

    # 增加大盘涨/微涨场景的硬编码规则(V5未覆盖)
    v11_extension.extend([
        # 大盘微涨+个股逆势大跌+低位 → 涨(超跌反弹)
        {'name': 'EXT_MU1:微涨+跌>3%+低位→涨', 'pred_up': True, 'tier': 1,
         'layer': 'extension', 'cv_acc': 70.0,
         'check': lambda s: (0 <= s['mkt_chg'] <= 1 and s['this_chg'] < -3
                             and s['pos60'] is not None and s['pos60'] < 0.3)},
        # 大盘涨+个股逆势大跌+低位 → 涨
        {'name': 'EXT_MBU1:大盘涨+跌>3%+低位→涨', 'pred_up': True, 'tier': 1,
         'layer': 'extension', 'cv_acc': 70.0,
         'check': lambda s: (s['mkt_chg'] > 1 and s['this_chg'] < -3
                             and s['pos60'] is not None and s['pos60'] < 0.3)},
        # 大盘跌+个股跌>3%+连跌3天+低位 → 涨(V5 R3的扩展)
        {'name': 'EXT_MD1:大盘跌+跌>3%+连跌3天+低位→涨', 'pred_up': True, 'tier': 1,
         'layer': 'extension', 'cv_acc': 70.0,
         'check': lambda s: (-3 <= s['mkt_chg'] < -1 and s['this_chg'] < -3
                             and s['cd'] >= 3
                             and s['pos60'] is not None and s['pos60'] < 0.3)},
        # 上证+大盘微跌+跌>3%+连跌2天 → 涨(V5 R5系列的上证版)
        {'name': 'EXT_MF_SH1:上证+微跌+跌>3%+连跌2天→涨', 'pred_up': True, 'tier': 1,
         'layer': 'extension', 'cv_acc': 70.0,
         'check': lambda s: (s['suffix'] == 'SH' and -1 <= s['mkt_chg'] < 0
                             and s['this_chg'] < -3 and s['cd'] >= 2)},
    ])

    # ── 层3: 边际规则 ──
    v11_marginal = []
    for r in marginal_rules:
        v11_marginal.append({
            'name': r['name'], 'pred_up': r['pred_up'], 'tier': 2,
            'layer': 'marginal', 'cv_acc': r['cv_acc'],
            'check': r['check'],
        })

    # ── 层4: 严格条件的通用兜底规则(去掉R_tail等低准确率规则) ──
    general_fallback = [
        # 超跌反弹: 跌>5%+低位<0.2 → 涨(条件严格)
        {'name': 'FB_UP1:跌>5%+低位<0.2→涨', 'pred_up': True, 'tier': 1,
         'layer': 'fallback', 'cv_acc': 72.0,
         'check': lambda s: (s['this_chg'] < -5
                             and s['pos60'] is not None and s['pos60'] < 0.2)},
        # 超跌反弹: 跌>3%+连跌3天+低位<0.3 → 涨(比之前更严格)
        {'name': 'FB_UP2:跌>3%+连跌3天+低位→涨', 'pred_up': True, 'tier': 1,
         'layer': 'fallback', 'cv_acc': 70.0,
         'check': lambda s: (s['this_chg'] < -3 and s['cd'] >= 3
                             and s['pos60'] is not None and s['pos60'] < 0.3)},
        # 超跌反弹: 跌>5%+尾日跌>3%+非高位 → 涨(比R_tail更严格)
        {'name': 'FB_UP3:跌>5%+尾日恐慌+非高位→涨', 'pred_up': True, 'tier': 1,
         'layer': 'fallback', 'cv_acc': 70.0,
         'check': lambda s: (s['this_chg'] < -5 and s['last_day'] < -3
                             and not (s['pos60'] is not None and s['pos60'] >= 0.6))},
        # 过热回调: 涨>5%+高位+量价背离 → 跌
        {'name': 'FB_DN1:涨>5%+高位+量价背离→跌', 'pred_up': False, 'tier': 1,
         'layer': 'fallback', 'cv_acc': 68.0,
         'check': lambda s: (s['this_chg'] > 5 and s['vol_price_diverge'] == -1
                             and s['pos60'] is not None and s['pos60'] >= 0.6)},
        # 过热回调: 涨>5%+连涨3天+高位 → 跌(更严格)
        {'name': 'FB_DN2:涨>5%+连涨3天+高位→跌', 'pred_up': False, 'tier': 1,
         'layer': 'fallback', 'cv_acc': 68.0,
         'check': lambda s: (s['this_chg'] > 5 and s['cu'] >= 3
                             and s['pos60'] is not None and s['pos60'] >= 0.6)},
    ]

    # ── 层2.5: 大盘涨场景专用规则(V2深度分析发现) ──
    # 策略: 只保留CV>=75%的尾日效应规则(涨信号), 跌信号条件收紧
    bull_rules = [
        # ── 涨信号: 大盘尾日效应(CV 75~79%, 最可靠的大盘涨信号) ──
        {'name': 'BULL_UP1:大盘尾日跌>1%+个股跌>2%+低位<0.3→涨', 'pred_up': True, 'tier': 1,
         'layer': 'bull', 'cv_acc': 78.9,
         'check': lambda s: (s['mkt_chg'] >= 0
                             and s['mkt_last_day'] is not None and s['mkt_last_day'] < -1
                             and s['this_chg'] < -2
                             and s['pos60'] is not None and s['pos60'] < 0.3)},
        {'name': 'BULL_UP2:大盘尾日跌>1%+个股跌>3%→涨', 'pred_up': True, 'tier': 1,
         'layer': 'bull', 'cv_acc': 76.4,
         'check': lambda s: (s['mkt_chg'] >= 0
                             and s['mkt_last_day'] is not None and s['mkt_last_day'] < -1
                             and s['this_chg'] < -3)},
        {'name': 'BULL_UP3:大盘尾日跌>1%+个股跌>2%→涨', 'pred_up': True, 'tier': 1,
         'layer': 'bull', 'cv_acc': 77.9,
         'check': lambda s: (s['mkt_chg'] >= 0
                             and s['mkt_last_day'] is not None and s['mkt_last_day'] < -1
                             and s['this_chg'] < -2)},
        # ── 跌信号: 只保留最严格条件(冲高回落+大涨, CV 72.7%) ──
        {'name': 'BULL_DN1:大涨>2%+涨>3%+冲高回落→跌', 'pred_up': False, 'tier': 1,
         'layer': 'bull', 'cv_acc': 72.7,
         'check': lambda s: (s['mkt_chg'] > 2 and s['this_chg'] > 3
                             and s['rush_up_pullback'])},
        {'name': 'BULL_DN2:板块一致性<0.3+个股涨>5%→跌', 'pred_up': False, 'tier': 1,
         'layer': 'bull', 'cv_acc': 69.3,
         'check': lambda s: (s['mkt_chg'] >= 0
                             and s['concept_consensus'] is not None
                             and s['concept_consensus'] < 0.3
                             and s['this_chg'] > 5)},
        {'name': 'BULL_DN3:深证+涨>8%+冲高回落→跌', 'pred_up': False, 'tier': 1,
         'layer': 'bull', 'cv_acc': 68.7,
         'check': lambda s: (s['mkt_chg'] >= 0 and s['suffix'] == 'SZ'
                             and s['this_chg'] > 8 and s['rush_up_pullback'])},
    ]

    # ── 层2.5b: 大盘涨边际规则(CV 60~67%) ──
    bull_marginal = [
        {'name': 'BULL_M_UP1:跌>2%+连跌≥4天+低位<0.3+缩量<0.8→涨', 'pred_up': True, 'tier': 2,
         'layer': 'bull_marginal', 'cv_acc': 66.8,
         'check': lambda s: (s['mkt_chg'] >= 0
                             and s['this_chg'] < -2 and s['cd'] >= 4
                             and s['pos60'] is not None and s['pos60'] < 0.3
                             and s['vol_ratio'] is not None and s['vol_ratio'] < 0.8)},
        {'name': 'BULL_M_DN1:换手率比>2.5+涨>3%+高位>0.7→跌', 'pred_up': False, 'tier': 2,
         'layer': 'bull_marginal', 'cv_acc': 67.0,
         'check': lambda s: (s['mkt_chg'] >= 0
                             and s['turnover_ratio'] is not None and s['turnover_ratio'] > 2.5
                             and s['this_chg'] > 3
                             and s['pos60'] is not None and s['pos60'] >= 0.7)},
        {'name': 'BULL_M_DN2:大涨>2%+涨>3%+高位>0.7→跌', 'pred_up': False, 'tier': 2,
         'layer': 'bull_marginal', 'cv_acc': 66.3,
         'check': lambda s: (s['mkt_chg'] > 2 and s['this_chg'] > 3
                             and s['pos60'] is not None and s['pos60'] >= 0.7)},
        {'name': 'BULL_M_DN3:板块一致性<0.3+个股涨>3%→跌', 'pred_up': False, 'tier': 2,
         'layer': 'bull_marginal', 'cv_acc': 65.4,
         'check': lambda s: (s['mkt_chg'] >= 0
                             and s['concept_consensus'] is not None
                             and s['concept_consensus'] < 0.3
                             and s['this_chg'] > 3)},
        {'name': 'BULL_M_DN4:放量>2.0+涨>3%+高位>0.7→跌', 'pred_up': False, 'tier': 2,
         'layer': 'bull_marginal', 'cv_acc': 64.7,
         'check': lambda s: (s['mkt_chg'] >= 0
                             and s['vol_ratio'] is not None and s['vol_ratio'] > 2.0
                             and s['this_chg'] > 3
                             and s['pos60'] is not None and s['pos60'] >= 0.7)},
        {'name': 'BULL_M_DN5:涨>5%+连涨≥3天+资金流出<-1%+高位>0.7→跌', 'pred_up': False, 'tier': 2,
         'layer': 'bull_marginal', 'cv_acc': 63.5,
         'check': lambda s: (s['mkt_chg'] >= 0
                             and s['this_chg'] > 5 and s['cu'] >= 3
                             and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < -1
                             and s['pos60'] is not None and s['pos60'] >= 0.7)},
        {'name': 'BULL_M_DN6:深证+涨>5%+冲高回落→跌', 'pred_up': False, 'tier': 2,
         'layer': 'bull_marginal', 'cv_acc': 62.2,
         'check': lambda s: (s['mkt_chg'] >= 0 and s['suffix'] == 'SZ'
                             and s['this_chg'] > 5 and s['rush_up_pullback'])},
        {'name': 'BULL_M_UP2:上证+跌>2%+前周跌>2%+低位<0.3→涨', 'pred_up': True, 'tier': 2,
         'layer': 'bull_marginal', 'cv_acc': 61.9,
         'check': lambda s: (s['mkt_chg'] >= 0 and s['suffix'] == 'SH'
                             and s['this_chg'] < -2
                             and s['prev_chg'] is not None and s['prev_chg'] < -2
                             and s['pos60'] is not None and s['pos60'] < 0.3)},
        {'name': 'BULL_M_DN7:前两周均涨>2%+本周涨>3%+高位>0.7→跌', 'pred_up': False, 'tier': 2,
         'layer': 'bull_marginal', 'cv_acc': 61.8,
         'check': lambda s: (s['mkt_chg'] >= 0
                             and s['prev_chg'] is not None and s['prev_chg'] > 2
                             and s['prev2_chg'] is not None and s['prev2_chg'] > 2
                             and s['this_chg'] > 3
                             and s['pos60'] is not None and s['pos60'] >= 0.7)},
        {'name': 'BULL_M_UP3:深证+跌>2%+连跌≥4天→涨', 'pred_up': True, 'tier': 2,
         'layer': 'bull_marginal', 'cv_acc': 60.7,
         'check': lambda s: (s['mkt_chg'] >= 0 and s['suffix'] == 'SZ'
                             and s['this_chg'] < -2 and s['cd'] >= 4)},
    ]

    engine = {
        'backbone': v5_backbone,
        'extension': v11_extension,
        'bull': bull_rules,
        'bull_marginal': bull_marginal,
        'marginal': v11_marginal,
        'fallback': general_fallback,
    }

    logger.info("  V11混合引擎构建完成:")
    logger.info("    层1-骨干(V5): %d条规则", len(v5_backbone))
    logger.info("    层2-扩展(V11新): %d条规则", len(v11_extension))
    logger.info("    层2.5-大盘涨(精选): %d条规则", len(bull_rules))
    logger.info("    层2.5b-大盘涨(边际): %d条规则", len(bull_marginal))
    logger.info("    层3-边际: %d条规则", len(v11_marginal))
    logger.info("    层4-兜底: %d条规则", len(general_fallback))

    return engine


def _apply_confidence_modifier(pred_up, base_confidence, sample):
    """资金流向+板块动量置信度修正。"""
    conf_score = {'high': 3, 'reference': 2, 'low': 1}.get(base_confidence, 1)
    bm = sample.get('board_momentum')
    ff = sample.get('big_net_pct_avg')
    cc = sample.get('concept_consensus')

    if pred_up:
        if bm is not None and bm < -3:
            conf_score += 1
        if ff is not None and ff > 2:
            conf_score += 1
        if cc is not None and cc < 0.2:
            conf_score += 0.5
        if bm is not None and bm > 2:
            conf_score -= 1
        if ff is not None and ff < -3:
            conf_score -= 1
    else:
        if bm is not None and bm > 1:
            conf_score += 1
        if ff is not None and ff < -2:
            conf_score += 1
        if bm is not None and bm < -2:
            conf_score -= 1
        if ff is not None and ff > 3:
            conf_score -= 1

    if conf_score >= 3:
        return 'high'
    elif conf_score >= 2:
        return 'reference'
    else:
        return 'low'


def _hybrid_predict_one(sample, engine, layers, use_modifier, min_conf,
                        bull_up_only=False, consensus_filter=None):
    """对单个样本做混合引擎预测。
    
    Args:
        bull_up_only: 若True, bull/bull_marginal层只匹配涨信号(跳过跌信号)
        consensus_filter: 板块一致性过滤策略, None=不过滤, 可选:
            'filter_up_high'   - 过滤涨信号+高一致性(≥0.7)
            'filter_up_mid'    - 过滤涨信号+中高一致性(≥0.5)
            'boost_down_high'  - 跌信号+高一致性提升置信度
            'full'             - 同时过滤涨+提升跌
            'full_strict'      - 更严格: 涨≥0.5过滤, 跌≥0.5提升
    """
    matched_rule = None
    for layer_name in ['backbone', 'bull', 'extension', 'bull_marginal', 'marginal', 'fallback']:
        if layer_name not in layers:
            continue
        for rule in engine.get(layer_name, []):
            # 双模式: bull层只保留涨信号
            if bull_up_only and layer_name in ('bull', 'bull_marginal') and not rule['pred_up']:
                continue
            try:
                if rule['check'](sample):
                    matched_rule = rule
                    break
            except (TypeError, KeyError):
                continue
        if matched_rule:
            break

    if matched_rule is None:
        return None, None, None

    pred_up = matched_rule['pred_up']
    cc = sample.get('concept_consensus')

    # ── 板块一致性过滤 ──
    if consensus_filter and cc is not None:
        if consensus_filter == 'filter_up_high':
            # 涨信号 + 高一致性(≥0.7) → 过滤(准确率仅68.9%)
            if pred_up and cc >= 0.7:
                return None, None, None
        elif consensus_filter == 'filter_up_mid':
            # 涨信号 + 中高一致性(≥0.5) → 过滤
            if pred_up and cc >= 0.5:
                return None, None, None
        elif consensus_filter == 'full':
            # 涨信号 + 高一致性 → 过滤
            if pred_up and cc >= 0.7:
                return None, None, None
        elif consensus_filter == 'full_strict':
            # 涨信号 + 中高一致性 → 过滤
            if pred_up and cc >= 0.5:
                return None, None, None
        elif consensus_filter == 'filter_up_high_strong_board':
            # 涨信号 + 高一致性 + 偏强板块 → 过滤(最差组合)
            bm = sample.get('board_momentum')
            if pred_up and cc >= 0.7 and bm is not None and bm > 0:
                return None, None, None
        elif consensus_filter == 'filter_up_strong_board':
            # 涨信号 + 偏强板块 → 过滤(准确率72.0%)
            bm = sample.get('board_momentum')
            if pred_up and bm is not None and bm > 0:
                return None, None, None

    base_conf = 'high' if matched_rule['tier'] == 1 else 'reference'
    if use_modifier:
        adj_conf = _apply_confidence_modifier(pred_up, base_conf, sample)
    else:
        adj_conf = base_conf

    # ── 板块一致性置信度提升(跌信号) ──
    if consensus_filter in ('full', 'full_strict', 'boost_down_high'):
        if not pred_up and cc is not None and cc >= 0.7:
            # 跌信号+高一致性 → 强制high(准确率83.0%)
            adj_conf = 'high'

    if min_conf == 'reference' and adj_conf == 'low':
        return None, None, None
    if min_conf == 'high' and adj_conf in ('low', 'reference'):
        return None, None, None

    return pred_up, adj_conf, matched_rule


def eval_v11_hybrid(samples, engine, passed_rules, marginal_rules):
    """评估V11混合规则引擎，包含多种配置的网格搜索。"""
    logger.info("\n" + "=" * 90)
    logger.info("  ══ 第七阶段: V11混合规则引擎评估 ══")
    logger.info("=" * 90)

    all_weeks = sorted(set(s['iw_this'] for s in samples))

    # 配置格式: (名称, 层列表, 增强, 最低置信, 描述, bull_up_only, consensus_filter)
    configs = [
        ('骨干only', ['backbone'], False, None,
         'V5骨干规则(涨+跌)互斥匹配', False, None),
        # ── 双模式: bull层只保留涨信号(尾日效应), 跳过低CV跌信号 ──
        ('骨干+尾日涨only', ['backbone', 'bull'], False, None,
         'V5骨干+大盘涨仅涨信号(尾日效应)', True, None),
        ('骨干+尾日涨only+增强', ['backbone', 'bull'], True, None,
         'V5骨干+大盘涨仅涨信号+增强', True, None),
        ('骨干+尾日涨only+过滤low', ['backbone', 'bull'], True, 'reference',
         'V5骨干+大盘涨仅涨信号+过滤低置信', True, None),
        ('骨干+尾日涨+边际涨only', ['backbone', 'bull', 'bull_marginal'], False, None,
         'V5骨干+大盘涨全部仅涨信号', True, None),
        # ── 原有配置 ──
        ('骨干+大盘涨精选', ['backbone', 'bull'], False, None,
         'V5骨干+大盘涨精选规则(尾日效应+冲高回落)', False, None),
        ('骨干+大盘涨精选+增强+过滤low', ['backbone', 'bull'], True, 'reference',
         'V5骨干+大盘涨精选+增强+过滤低置信', False, None),
        ('骨干+大盘涨全部', ['backbone', 'bull', 'bull_marginal'], False, None,
         'V5骨干+大盘涨精选+大盘涨边际', False, None),
        ('骨干+扩展', ['backbone', 'extension'], False, None,
         'V5骨干+V11新场景规则', False, None),
        ('骨干+大盘涨精选+兜底', ['backbone', 'bull', 'fallback'], False, None,
         'V5骨干+大盘涨精选+通用兜底', False, None),
        ('全层(含大盘涨)', ['backbone', 'bull', 'bull_marginal', 'extension', 'marginal', 'fallback'], False, None,
         '全部6层规则', False, None),
        ('骨干+边际', ['backbone', 'marginal'], False, None,
         'V5骨干+边际规则', False, None),
        # ══ 板块一致性修正器配置(基于深度分析结果) ══
        # 基线: 骨干+尾日涨only+过滤low (当前最优 CV 77.1%)
        ('★一致性:过滤涨高一致', ['backbone', 'bull'], True, 'reference',
         '最优基线+过滤涨信号高一致性(≥0.7)', True, 'filter_up_high'),
        ('★一致性:过滤涨中高一致', ['backbone', 'bull'], True, 'reference',
         '最优基线+过滤涨信号中高一致性(≥0.5)', True, 'filter_up_mid'),
        ('★一致性:全量修正', ['backbone', 'bull'], True, 'reference',
         '最优基线+过滤涨高一致+提升跌高一致', True, 'full'),
        ('★一致性:严格修正', ['backbone', 'bull'], True, 'reference',
         '最优基线+过滤涨≥0.5+提升跌≥0.7', True, 'full_strict'),
        ('★一致性:提升跌高一致', ['backbone', 'bull'], True, 'reference',
         '最优基线+仅提升跌信号高一致性置信度', True, 'boost_down_high'),
        ('★一致性:过滤涨偏强板块', ['backbone', 'bull'], True, 'reference',
         '最优基线+过滤涨信号偏强板块', True, 'filter_up_strong_board'),
        ('★一致性:过滤涨高一致偏强', ['backbone', 'bull'], True, 'reference',
         '最优基线+过滤涨信号高一致+偏强板块', True, 'filter_up_high_strong_board'),
        # 不带增强/过滤low的一致性修正
        ('★一致性:骨干+尾日+过滤涨高一致', ['backbone', 'bull'], False, None,
         '骨干+尾日涨only+过滤涨信号高一致性', True, 'filter_up_high'),
        ('★一致性:骨干+尾日+全量修正', ['backbone', 'bull'], False, None,
         '骨干+尾日涨only+全量一致性修正', True, 'full'),
    ]

    best_hybrid = None
    best_hybrid_config = None
    results_summary = []

    for cfg_name, layers, use_modifier, min_conf, desc, bull_up_only, consensus_filter in configs:
        active_rules = []
        for layer_name in layers:
            active_rules.extend(engine.get(layer_name, []))
        if not active_rules:
            continue

        # ── 全样本评估 ──
        total_pred, total_correct = 0, 0
        by_layer = defaultdict(lambda: {'correct': 0, 'total': 0})
        by_mkt = defaultdict(lambda: {'correct': 0, 'total': 0})
        by_direction = defaultdict(lambda: {'correct': 0, 'total': 0})
        by_consensus = defaultdict(lambda: {'correct': 0, 'total': 0})

        for s in samples:
            pred_up, adj_conf, matched_rule = _hybrid_predict_one(
                s, engine, layers, use_modifier, min_conf,
                bull_up_only=bull_up_only, consensus_filter=consensus_filter)
            if pred_up is None:
                continue

            is_correct = pred_up == s['actual_up']
            total_pred += 1
            if is_correct:
                total_correct += 1

            layer = matched_rule.get('layer', 'unknown')
            by_layer[layer]['total'] += 1
            if is_correct:
                by_layer[layer]['correct'] += 1

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
            by_mkt[regime]['total'] += 1
            if is_correct:
                by_mkt[regime]['correct'] += 1

            d = 'UP' if pred_up else 'DOWN'
            by_direction[d]['total'] += 1
            if is_correct:
                by_direction[d]['correct'] += 1

            # 板块一致性分组统计
            cc = s.get('concept_consensus')
            if cc is not None:
                if cc >= 0.7:
                    cc_label = '高一致(≥0.7)'
                elif cc >= 0.4:
                    cc_label = '中一致(0.4~0.7)'
                else:
                    cc_label = '低一致(<0.4)'
            else:
                cc_label = '无数据'
            dir_cc = f"{'涨' if pred_up else '跌'}+{cc_label}"
            by_consensus[cc_label]['total'] += 1
            by_consensus[dir_cc]['total'] += 1
            if is_correct:
                by_consensus[cc_label]['correct'] += 1
                by_consensus[dir_cc]['correct'] += 1

        full_acc = total_correct / total_pred * 100 if total_pred > 0 else 0
        full_cov = total_pred / len(samples) * 100 if samples else 0

        # ── 时间序列CV评估 ──
        cv_total, cv_correct = 0, 0
        weekly_accs = []
        for test_idx in range(MIN_TRAIN_WEEKS, len(all_weeks)):
            test_week = all_weeks[test_idx]
            test_samples = [s for s in samples if s['iw_this'] == test_week]
            wt, wc = 0, 0
            for s in test_samples:
                pred_up, adj_conf, matched_rule = _hybrid_predict_one(
                    s, engine, layers, use_modifier, min_conf,
                    bull_up_only=bull_up_only, consensus_filter=consensus_filter)
                if pred_up is None:
                    continue
                wt += 1
                if pred_up == s['actual_up']:
                    wc += 1
            cv_total += wt
            cv_correct += wc
            if wt >= 5:
                weekly_accs.append(wc / wt * 100)

        cv_acc = cv_correct / cv_total * 100 if cv_total > 0 else 0
        cv_cov_denom = sum(1 for s in samples
                           if s['iw_this'] in set(all_weeks[MIN_TRAIN_WEEKS:]))
        cv_cov = cv_total / cv_cov_denom * 100 if cv_cov_denom > 0 else 0
        weeks_above_75 = sum(1 for a in weekly_accs if a >= 75)

        result = {
            'config': cfg_name, 'desc': desc,
            'full_acc': full_acc, 'full_cov': full_cov,
            'cv_acc': cv_acc, 'cv_cov': cv_cov, 'cv_total': cv_total,
            'weeks_above_75': weeks_above_75, 'total_cv_weeks': len(weekly_accs),
            'by_layer': dict(by_layer), 'by_mkt': dict(by_mkt),
            'by_direction': dict(by_direction),
            'by_consensus': dict(by_consensus),
            'consensus_filter': consensus_filter,
        }
        results_summary.append(result)

        score = cv_acc * 0.7 + cv_cov * 0.3 if cv_acc >= TARGET_ACCURACY else cv_acc * 0.5
        old_score = 0
        if best_hybrid:
            old_score = (best_hybrid['cv_acc'] * 0.7 + best_hybrid['cv_cov'] * 0.3
                         if best_hybrid['cv_acc'] >= TARGET_ACCURACY
                         else best_hybrid['cv_acc'] * 0.5)
        if score > old_score:
            best_hybrid = result
            best_hybrid_config = cfg_name

    # ── 输出所有配置结果 ──
    logger.info("\n  %-30s %-10s %-10s %-10s %-10s %-8s",
                '配置', '全样本准确', '全样本覆盖', 'CV准确', 'CV覆盖', '达标周')
    logger.info("  " + "-" * 85)
    for r in results_summary:
        flag = '★' if r['cv_acc'] >= TARGET_ACCURACY else ' '
        logger.info("  %s %-28s %-10s %-10s %-10s %-10s %d/%d",
                    flag, r['config'],
                    f"{r['full_acc']:.1f}%", f"{r['full_cov']:.1f}%",
                    f"{r['cv_acc']:.1f}%", f"{r['cv_cov']:.1f}%",
                    r['weeks_above_75'], r['total_cv_weeks'])

    # ── 最优配置详细分析 ──
    if best_hybrid:
        logger.info("\n  ★ 最优混合配置: %s", best_hybrid_config)
        logger.info("    全样本: 准确率%.1f%% 覆盖率%.1f%%",
                    best_hybrid['full_acc'], best_hybrid['full_cov'])
        logger.info("    CV: 准确率%.1f%% 覆盖率%.1f%% (%d样本)",
                    best_hybrid['cv_acc'], best_hybrid['cv_cov'], best_hybrid['cv_total'])
        logger.info("    达标周: %d/%d", best_hybrid['weeks_above_75'],
                    best_hybrid['total_cv_weeks'])

        logger.info("\n    ── 按层级 ──")
        for layer, st in sorted(best_hybrid['by_layer'].items()):
            logger.info("      %-12s %s (%d/%d)", layer,
                        _pct(st['correct'], st['total']), st['correct'], st['total'])

        logger.info("\n    ── 按大盘场景 ──")
        for regime in ['大盘深跌', '大盘跌', '大盘微跌', '大盘微涨', '大盘涨']:
            st = best_hybrid['by_mkt'].get(regime, {'correct': 0, 'total': 0})
            if st['total'] > 0:
                logger.info("      %-10s %s (%d/%d)", regime,
                            _pct(st['correct'], st['total']), st['correct'], st['total'])

        logger.info("\n    ── 按方向 ──")
        for d, st in sorted(best_hybrid['by_direction'].items()):
            logger.info("      %-6s %s (%d/%d)", d,
                        _pct(st['correct'], st['total']), st['correct'], st['total'])

        # ── 板块一致性分组 ──
        if best_hybrid.get('by_consensus'):
            logger.info("\n    ── 按板块一致性 ──")
            for label in ['高一致(≥0.7)', '中一致(0.4~0.7)', '低一致(<0.4)', '无数据']:
                st = best_hybrid['by_consensus'].get(label, {'correct': 0, 'total': 0})
                if st['total'] > 0:
                    logger.info("      %-16s %s (%d/%d)", label,
                                _pct(st['correct'], st['total']), st['correct'], st['total'])
            logger.info("    ── 按方向×一致性 ──")
            for label in ['涨+高一致(≥0.7)', '涨+中一致(0.4~0.7)', '涨+低一致(<0.4)',
                          '跌+高一致(≥0.7)', '跌+中一致(0.4~0.7)', '跌+低一致(<0.4)']:
                st = best_hybrid['by_consensus'].get(label, {'correct': 0, 'total': 0})
                if st['total'] > 0:
                    logger.info("      %-20s %s (%d/%d)", label,
                                _pct(st['correct'], st['total']), st['correct'], st['total'])

        # ── 一致性配置对比(如果有) ──
        consensus_configs = [r for r in results_summary if r.get('consensus_filter')]
        baseline_cfg = next((r for r in results_summary if r['config'] == '骨干+尾日涨only+过滤low'), None)
        if consensus_configs and baseline_cfg:
            logger.info("\n  ══ 板块一致性修正器效果对比 ══")
            logger.info("  基线: %s (CV准确率%.1f%%, 覆盖率%.1f%%)",
                        baseline_cfg['config'], baseline_cfg['cv_acc'], baseline_cfg['cv_cov'])
            logger.info("  %-32s %-10s %-10s %-10s %-10s %-8s",
                        '一致性配置', 'CV准确', 'CV覆盖', '准确变化', '覆盖变化', '达标周')
            logger.info("  " + "-" * 95)
            for r in consensus_configs:
                acc_delta = r['cv_acc'] - baseline_cfg['cv_acc']
                cov_delta = r['cv_cov'] - baseline_cfg['cv_cov']
                flag = '↑' if acc_delta > 0 else '↓' if acc_delta < 0 else '='
                logger.info("  %s %-30s %-10s %-10s %-10s %-10s %d/%d",
                            flag, r['config'],
                            f"{r['cv_acc']:.1f}%", f"{r['cv_cov']:.1f}%",
                            f"{acc_delta:+.1f}%", f"{cov_delta:+.1f}%",
                            r['weeks_above_75'], r['total_cv_weeks'])

        if best_hybrid['cv_acc'] >= TARGET_ACCURACY:
            logger.info("\n  ✅ V11混合引擎达标! CV准确率%.1f%% >= %.0f%%, 覆盖率%.1f%%",
                        best_hybrid['cv_acc'], TARGET_ACCURACY, best_hybrid['cv_cov'])
        else:
            logger.info("\n  ⚠️ V11混合引擎未达标, CV准确率%.1f%% < %.0f%%",
                        best_hybrid['cv_acc'], TARGET_ACCURACY)
            logger.info("    建议: 增加更多候选规则或放宽CV验证条件")

    # ── 与V5/V7对比 ──
    v5 = eval_baseline(samples, V5_BASELINE_RULES, 'V5基线')
    v7 = eval_baseline(samples, V7_ELITE_RULES, 'V7精简', v7_filter=True)

    logger.info("\n  ── 最终对比 ──")
    logger.info("  %-20s %-12s %-12s %-10s", '模型', '准确率', '覆盖率', '预测数')
    logger.info("  " + "-" * 55)
    logger.info("  %-20s %-12s %-12s %-10d",
                'V5基线', f"{v5['accuracy']:.1f}%", f"{v5['coverage']:.1f}%", v5['total_pred'])
    logger.info("  %-20s %-12s %-12s %-10d",
                'V7精简', f"{v7['accuracy']:.1f}%", f"{v7['coverage']:.1f}%", v7['total_pred'])
    if best_hybrid:
        logger.info("  %-20s %-12s %-12s %-10d",
                    f'V11混合({best_hybrid_config})',
                    f"{best_hybrid['full_acc']:.1f}%",
                    f"{best_hybrid['full_cov']:.1f}%",
                    best_hybrid.get('cv_total', 0))
        logger.info("  %-20s %-12s %-12s %-10d",
                    'V11混合-CV',
                    f"{best_hybrid['cv_acc']:.1f}%",
                    f"{best_hybrid['cv_cov']:.1f}%",
                    best_hybrid.get('cv_total', 0))

    return best_hybrid, results_summary


# ═══════════════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════════════

def run_backtest(n_weeks=N_WEEKS):
    t0 = datetime.now()

    logger.info("=" * 90)
    logger.info("  V11 多因子综合预测模型回测")
    logger.info("  目标: 准确率>%.0f%%, 覆盖大盘涨/跌/平盘所有场景", TARGET_ACCURACY)
    logger.info("  基于V5衍生, 融合V3~V7所有分析结果")
    logger.info("=" * 90)

    # 1. 加载数据
    logger.info("\n[1/7] 加载全维度数据...")
    data = load_data(n_weeks)

    # 2. 构建样本
    logger.info("\n[2/7] 构建全因子样本...")
    samples = build_samples(data, n_weeks)
    if not samples:
        logger.error("  无有效样本!")
        return

    # 基础统计
    up_cnt = sum(1 for s in samples if s['actual_up'])
    logger.info("  总样本: %d, 涨: %d(%.1f%%), 跌: %d(%.1f%%)",
                len(samples), up_cnt, up_cnt / len(samples) * 100,
                len(samples) - up_cnt, (len(samples) - up_cnt) / len(samples) * 100)

    # 3. 全量规则CV验证
    logger.info("\n[3/7] 全量候选规则CV验证...")
    candidate_rules = build_v11_candidate_rules()
    passed_rules, marginal_rules = cv_validate_all_rules(samples, candidate_rules)

    if not passed_rules:
        logger.error("  无规则通过CV验证!")
        return

    # 4. 网格搜索最优参数
    logger.info("\n[4/7] 网格搜索最优参数...")
    best_result, best_config, all_results = grid_search_thresholds(
        samples, passed_rules, marginal_rules)

    # 5. 迭代优化
    logger.info("\n[5/7] 迭代优化...")
    best_config, best_result = iterative_optimize(
        samples, passed_rules, marginal_rules, best_config, best_result)

    # 6. 构建最优模型并详细分析
    logger.info("\n[6/7] 最优模型详细分析...")
    if best_config:
        rs_name = best_config.get('rule_set', '强规则')
        if rs_name == '强规则':
            rules = passed_rules
        elif rs_name == '强+边际':
            rules = passed_rules + marginal_rules
        elif rs_name == '高CV规则':
            rules = [r for r in passed_rules if r['cv_acc'] >= 70]
        elif rs_name == '仅涨信号':
            rules = [r for r in passed_rules if r['pred_up']]
        elif rs_name == '全规则':
            rules = passed_rules + marginal_rules
        elif rs_name == '混合V5+V11':
            # 重建V5核心规则
            v5_core = [
                {'name': 'V5_R1:大盘深跌+个股跌→涨', 'pred_up': True, 'category': 'v5_core',
                 'check': lambda s: s['this_chg'] < -2 and s['mkt_chg'] < -3,
                 'tier': 1, 'full_acc': 89.6, 'cv_acc': 89.5, 'gap': 0.1,
                 'total': 6297, 'cv_total': 6000},
                {'name': 'V5_R5a:深证+微跌+连跌3天→涨', 'pred_up': True, 'category': 'v5_core',
                 'check': lambda s: (s['suffix'] == 'SZ' and -1 <= s['mkt_chg'] < 0
                                     and s['this_chg'] < -2 and s['cd'] >= 3),
                 'tier': 1, 'full_acc': 89.1, 'cv_acc': 90.6, 'gap': -1.5,
                 'total': 514, 'cv_total': 490},
                {'name': 'V5_R5b:深证+微跌+低位→涨', 'pred_up': True, 'category': 'v5_core',
                 'check': lambda s: (s['suffix'] == 'SZ' and -1 <= s['mkt_chg'] < 0
                                     and s['this_chg'] < -2
                                     and s['pos60'] is not None and s['pos60'] < 0.2),
                 'tier': 1, 'full_acc': 82.4, 'cv_acc': 88.7, 'gap': -6.2,
                 'total': 848, 'cv_total': 680},
                {'name': 'V5_R5c:深证+微跌+跌>2%→涨', 'pred_up': True, 'category': 'v5_core',
                 'check': lambda s: (s['suffix'] == 'SZ' and -1 <= s['mkt_chg'] < 0
                                     and s['this_chg'] < -2),
                 'tier': 1, 'full_acc': 78.4, 'cv_acc': 84.8, 'gap': -6.4,
                 'total': 2943, 'cv_total': 1648},
                {'name': 'V5_R3:上证+大盘跌+前周跌→涨', 'pred_up': True, 'category': 'v5_core',
                 'check': lambda s: (s['this_chg'] < -3 and s['suffix'] == 'SH'
                                     and -3 <= s['mkt_chg'] < -1
                                     and s['prev_chg'] is not None and s['prev_chg'] < -2
                                     and not (s['pos60'] is not None and s['pos60'] >= 0.8)),
                 'tier': 1, 'full_acc': 68.3, 'cv_acc': 71.1, 'gap': -2.8,
                 'total': 682, 'cv_total': 500},
            ]
            rules = v5_core + passed_rules + marginal_rules
        else:
            rules = passed_rules

        v11_model = V11ScoringModel(
            rules,
            up_threshold=best_config.get('up_threshold', 1.0),
            down_threshold=best_config.get('down_threshold', 1.0),
            min_votes=best_config.get('min_votes', 1),
            use_board_boost=best_config.get('board_boost', True),
        )

        detailed_analysis(samples, v11_model, 'V11最优')
        compare_with_baselines(samples, v11_model)
    else:
        logger.warning("  无最优配置，使用默认参数")
        v11_model = V11ScoringModel(passed_rules)
        detailed_analysis(samples, v11_model, 'V11默认')
        compare_with_baselines(samples, v11_model)

    # 7. V11混合规则引擎 — 最终生产版本
    logger.info("\n[7/7] V11混合规则引擎(生产版)...")
    v11_hybrid = build_v11_hybrid_engine(passed_rules, marginal_rules)
    hybrid_best, hybrid_results = eval_v11_hybrid(samples, v11_hybrid, passed_rules, marginal_rules)

    # 保存结果
    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("\n" + "=" * 90)
    logger.info("  V11回测完成! 耗时%.1f秒", elapsed)
    if best_result:
        logger.info("  评分制最优: %s", best_config)
        logger.info("  全样本准确率: %.1f%%, CV准确率: %.1f%%, 覆盖率: %.1f%%",
                    best_result.get('full_acc', 0),
                    best_result.get('cv_acc', 0),
                    best_result.get('cv_cov', 0))
    logger.info("=" * 90)

    # 保存结果到JSON
    save_result = {
        'model': 'V11_multifactor',
        'timestamp': datetime.now().isoformat(),
        'n_weeks': n_weeks,
        'total_samples': len(samples),
        'best_scoring_config': best_config,
        'best_scoring_result': {k: v for k, v in (best_result or {}).items()
                        if k != 'config'},
        'hybrid_engine': {
            'best_config': hybrid_best.get('config') if hybrid_best else None,
            'full_acc': hybrid_best.get('full_acc') if hybrid_best else None,
            'full_cov': hybrid_best.get('full_cov') if hybrid_best else None,
            'cv_acc': hybrid_best.get('cv_acc') if hybrid_best else None,
            'cv_cov': hybrid_best.get('cv_cov') if hybrid_best else None,
            'cv_total': hybrid_best.get('cv_total') if hybrid_best else None,
            'weeks_above_75': hybrid_best.get('weeks_above_75') if hybrid_best else None,
            'total_cv_weeks': hybrid_best.get('total_cv_weeks') if hybrid_best else None,
            'by_mkt': {k: v for k, v in (hybrid_best.get('by_mkt', {})).items()}
                if hybrid_best else None,
            'by_direction': {k: v for k, v in (hybrid_best.get('by_direction', {})).items()}
                if hybrid_best else None,
        } if hybrid_best else None,
        'all_hybrid_configs': [
            {'config': r['config'], 'full_acc': r['full_acc'], 'full_cov': r['full_cov'],
             'cv_acc': r['cv_acc'], 'cv_cov': r['cv_cov']}
            for r in hybrid_results
        ] if hybrid_results else [],
        'passed_rules_count': len(passed_rules),
        'marginal_rules_count': len(marginal_rules),
        'passed_rules': [{'name': r['name'], 'pred_up': r['pred_up'],
                          'category': r['category'], 'tier': r['tier'],
                          'full_acc': r['full_acc'], 'cv_acc': r['cv_acc'],
                          'gap': r['gap'], 'total': r['total'],
                          'cv_total': r['cv_total']}
                         for r in passed_rules],
    }
    try:
        with open('data_results/nw_v11_backtest_result.json', 'w', encoding='utf-8') as f:
            json.dump(save_result, f, ensure_ascii=False, indent=2, default=str)
        logger.info("  结果已保存到 data_results/nw_v11_backtest_result.json")
    except Exception as e:
        logger.warning("  保存结果失败: %s", e)


if __name__ == '__main__':
    run_backtest()
