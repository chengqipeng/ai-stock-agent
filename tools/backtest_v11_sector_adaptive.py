#!/usr/bin/env python3
"""
V11 下周预测算法 — 行业/概念板块适配性深度回测分析
====================================================
目标：评估V11规则引擎在不同行业和概念板块下的准确率差异，
      发现哪些板块适合当前规则、哪些需要参数适配，
      并量化板块级别参数调优的潜在收益。

分析维度：
  1. 按概念板块分组回测：每个板块内股票的V11准确率
  2. 按行业分组回测：申万一级行业的V11准确率
  3. 规则×板块交叉分析：哪些规则在哪些板块表现好/差
  4. 板块动量/强弱势对准确率的影响
  5. 板块适配参数探索：阈值微调对特定板块的提升空间

不修改任何现有代码，纯只读分析工具。
"""
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

# 添加项目根目录到 path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dao import get_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent.parent / "data_results"

# ═══════════════════════════════════════════════════════════
# 复用 weekly_prediction_service 的核心函数（只读引用）
# ═══════════════════════════════════════════════════════════
from service.weekly_prediction_service import (
    _nw_extract_features,
    _nw_match_rule,
    _get_stock_index,
    _compound_return,
    _mean,
    _std,
    _to_float,
)


# ═══════════════════════════════════════════════════════════
# 数据加载
# ═══════════════════════════════════════════════════════════

def load_stock_board_mapping() -> dict:
    """加载 股票→概念板块 映射。返回 {full_code: [{'board_code','board_name'}, ...]}"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("SELECT stock_code, board_code, board_name FROM stock_concept_board_stock")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    mapping = defaultdict(list)
    for r in rows:
        raw = r['stock_code']
        if '.' not in raw:
            if raw.startswith(('0', '3')):
                full = f"{raw}.SZ"
            elif raw.startswith('6'):
                full = f"{raw}.SH"
            else:
                continue
        else:
            full = raw
        mapping[full].append({'board_code': r['board_code'], 'board_name': r['board_name']})
    return dict(mapping)


def load_industry_mapping() -> dict:
    """加载 股票→行业 映射。返回 {full_code: industry_name}"""
    try:
        from common.utils.sector_mapping_utils import parse_industry_list_md
        return parse_industry_list_md()
    except Exception as e:
        logger.warning("行业映射加载失败: %s", e)
        return {}


def load_backtest_klines(stock_codes: list[str], start_date: str, end_date: str) -> dict:
    """加载回测所需的K线数据（个股+大盘）。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    bs = 300

    stock_klines = defaultdict(list)
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, close_price, change_percent, "
            f"trading_volume, change_hand, high_price, low_price, open_price "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [start_date, end_date])
        for row in cur.fetchall():
            stock_klines[row['stock_code']].append({
                'date': row['date'],
                'close': _to_float(row['close_price']),
                'change_percent': _to_float(row['change_percent']),
                'volume': _to_float(row.get('trading_volume')),
                'turnover': _to_float(row.get('change_hand')),
                'high': _to_float(row.get('high_price')),
                'low': _to_float(row.get('low_price')),
                'open': _to_float(row.get('open_price')),
            })

    # 大盘K线
    all_index_codes = list(set(_get_stock_index(c) for c in stock_codes))
    for idx in ('000001.SH', '399001.SZ', '899050.SZ'):
        if idx not in all_index_codes:
            all_index_codes.append(idx)
    ph_idx = ','.join(['%s'] * len(all_index_codes))
    cur.execute(
        f"SELECT stock_code, `date`, change_percent FROM stock_kline "
        f"WHERE stock_code IN ({ph_idx}) AND `date` >= %s AND `date` <= %s "
        f"ORDER BY `date`", all_index_codes + [start_date, end_date])
    market_klines = defaultdict(list)
    for r in cur.fetchall():
        market_klines[r['stock_code']].append({
            'date': r['date'],
            'change_percent': _to_float(r['change_percent']),
        })

    cur.close()
    conn.close()
    return {'stock_klines': dict(stock_klines), 'market_klines': dict(market_klines)}


def load_board_klines(board_codes: list[str], start_date: str, end_date: str) -> dict:
    """加载概念板块K线。返回 {board_code: [{date, change_percent}, ...]}"""
    if not board_codes:
        return {}
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    bs = 300
    result = defaultdict(list)
    for i in range(0, len(board_codes), bs):
        batch = board_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT board_code, `date`, change_percent FROM concept_board_kline "
            f"WHERE board_code IN ({ph}) AND `date` >= %s AND `date` <= %s "
            f"ORDER BY `date`", batch + [start_date, end_date])
        for r in cur.fetchall():
            result[r['board_code']].append({
                'date': r['date'],
                'change_percent': _to_float(r['change_percent']),
            })
    cur.close()
    conn.close()
    return dict(result)


def get_all_stock_codes() -> list[str]:
    """获取所有有K线数据的A股代码（排除北交所）。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT stock_code FROM stock_kline")
    codes = [r['stock_code'] for r in cur.fetchall()
             if not r['stock_code'].endswith('.BJ')]
    cur.close()
    conn.close()
    return sorted(codes)


# ═══════════════════════════════════════════════════════════
# 核心回测引擎（按周滚动，与生产 _compute_next_week_backtest 一致）
# ═══════════════════════════════════════════════════════════

def run_v11_backtest_per_stock(stock_codes: list[str], data: dict,
                               n_weeks: int = 29) -> list[dict]:
    """对每只股票执行V11下周预测回测，返回逐条预测记录。

    每条记录包含：stock_code, iso_week, rule_name, layer, pred_up,
                  actual_up, correct, next_week_chg, this_week_chg, ...

    Returns:
        list of prediction records (flat, for downstream grouping)
    """
    stock_klines_map = data['stock_klines']
    market_klines_map = data['market_klines']

    # 按ISO周分组大盘K线
    market_by_week = {}
    for idx_code, klines_list in market_klines_map.items():
        by_week = defaultdict(list)
        for k in klines_list:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            by_week[iw].append(k)
        market_by_week[idx_code] = by_week

    records = []

    for code in stock_codes:
        klines = stock_klines_map.get(code, [])
        if not klines or len(klines) < 20:
            continue

        # 按ISO周分组
        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            wg[iw].append(k)

        sorted_weeks = sorted(wg.keys())
        # 只回测最后 n_weeks 周
        if len(sorted_weeks) > n_weeks + 1:
            sorted_weeks = sorted_weeks[-(n_weeks + 1):]

        sorted_all = sorted(klines, key=lambda x: x['date'])

        for idx in range(len(sorted_weeks) - 1):
            iw_this = sorted_weeks[idx]
            iw_next = sorted_weeks[idx + 1]

            this_days = sorted(wg[iw_this], key=lambda x: x['date'])
            next_days = sorted(wg[iw_next], key=lambda x: x['date'])

            if len(this_days) < 3 or len(next_days) < 3:
                continue

            this_pcts = [d['change_percent'] for d in this_days]
            next_pcts = [d['change_percent'] for d in next_days]
            next_week_chg = _compound_return(next_pcts)
            actual_next_up = next_week_chg >= 0

            # 大盘
            stock_idx = _get_stock_index(code)
            idx_by_week = market_by_week.get(stock_idx, {})
            mw = idx_by_week.get(iw_this, [])
            market_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            # 大盘最后一天
            mkt_sorted = sorted(mw, key=lambda x: x['date'])
            mkt_last_day = mkt_sorted[-1]['change_percent'] if mkt_sorted else None

            # 历史K线
            first_date = this_days[0]['date']
            hist = [k for k in sorted_all if k['date'] < first_date]

            # price_pos_60
            price_pos_60 = None
            if len(hist) >= 20:
                hist_closes = [k.get('close', 0) for k in hist[-60:] if k.get('close', 0) > 0]
                if hist_closes:
                    all_c = hist_closes + [k.get('close', 0) for k in this_days if k.get('close', 0) > 0]
                    min_c, max_c = min(all_c), max(all_c)
                    latest_c = this_days[-1].get('close', 0)
                    if max_c > min_c and latest_c > 0:
                        price_pos_60 = round((latest_c - min_c) / (max_c - min_c), 4)

            # prev_week_chg / prev2_week_chg
            prev_week_chg = None
            prev_klines = hist[-5:] if len(hist) >= 5 else hist
            if prev_klines:
                prev_week_chg = _compound_return([k['change_percent'] for k in prev_klines])

            prev2_week_chg = None
            if len(hist) >= 10:
                prev2_klines = hist[-10:-5]
                if prev2_klines:
                    prev2_week_chg = _compound_return([k['change_percent'] for k in prev2_klines])

            # 成交量比
            bt_vol_ratio = None
            tv = [d.get('volume', 0) for d in this_days if d.get('volume', 0) > 0]
            hv = [k.get('volume', 0) for k in hist[-20:] if k.get('volume', 0) > 0]
            if tv and hv:
                avg_tv = _mean(tv)
                avg_hv = _mean(hv)
                if avg_hv > 0:
                    bt_vol_ratio = avg_tv / avg_hv

            # 换手率比
            bt_turnover_ratio = None
            tw = [d.get('turnover', 0) for d in this_days if d.get('turnover') and d['turnover'] > 0]
            ht = [k.get('turnover', 0) for k in hist[-20:] if k.get('turnover') and k['turnover'] > 0]
            if tw and ht:
                avg_tw = _mean(tw)
                avg_ht = _mean(ht)
                if avg_ht > 0:
                    bt_turnover_ratio = avg_tw / avg_ht

            feat = _nw_extract_features(
                this_pcts, market_chg,
                market_index=stock_idx,
                price_pos_60=price_pos_60,
                prev_week_chg=prev_week_chg,
                prev2_week_chg=prev2_week_chg,
                mkt_last_day=mkt_last_day,
                vol_ratio=bt_vol_ratio,
                turnover_ratio=bt_turnover_ratio,
                week_klines=this_days,
                hist_klines=hist)
            rule = _nw_match_rule(feat)

            if rule is None:
                continue  # 未命中规则，不计入

            pred_up = rule['pred_up']
            correct = pred_up == actual_next_up
            layer = rule.get('layer', 'backbone')

            records.append({
                'stock_code': code,
                'iso_week': iw_this,
                'rule_name': rule['name'],
                'layer': layer,
                'tier': rule.get('tier', 0),
                'pred_up': pred_up,
                'actual_up': actual_next_up,
                'correct': correct,
                'next_week_chg': round(next_week_chg, 4),
                'this_week_chg': round(feat['this_chg'], 4),
                'market_chg': round(market_chg, 4),
                'pos60': price_pos_60,
                'vol_ratio': bt_vol_ratio,
            })

    return records


# ═══════════════════════════════════════════════════════════
# 分析维度1：按概念板块分组
# ═══════════════════════════════════════════════════════════

def analyze_by_concept_board(records: list[dict], stock_board_map: dict,
                             min_samples: int = 30) -> dict:
    """按概念板块分组统计V11准确率。

    一只股票可能属于多个板块，每条记录会被计入所有所属板块。

    Returns:
        {board_name: {accuracy, total, correct, avg_return, win_rate,
                      up_accuracy, down_accuracy, layers: {layer: accuracy}}}
    """
    board_records = defaultdict(list)

    for rec in records:
        code = rec['stock_code']
        boards = stock_board_map.get(code, [])
        if not boards:
            board_records['_无板块'].append(rec)
        for b in boards:
            board_records[b['board_name']].append(rec)

    result = {}
    for board_name, recs in board_records.items():
        if len(recs) < min_samples:
            continue

        total = len(recs)
        correct = sum(1 for r in recs if r['correct'])
        accuracy = round(correct / total * 100, 1)

        # 涨/跌分别统计
        up_recs = [r for r in recs if r['pred_up']]
        down_recs = [r for r in recs if not r['pred_up']]
        up_acc = round(sum(1 for r in up_recs if r['correct']) / len(up_recs) * 100, 1) if up_recs else 0
        down_acc = round(sum(1 for r in down_recs if r['correct']) / len(down_recs) * 100, 1) if down_recs else 0

        # 平均收益（预测UP的实际收益）
        up_returns = [r['next_week_chg'] for r in up_recs]
        avg_return = round(_mean(up_returns), 2) if up_returns else 0

        # 按层级分组
        layer_stats = {}
        layer_groups = defaultdict(list)
        for r in recs:
            layer_groups[r['layer']].append(r)
        for layer, lr in layer_groups.items():
            lc = sum(1 for r in lr if r['correct'])
            layer_stats[layer] = {
                'accuracy': round(lc / len(lr) * 100, 1),
                'total': len(lr),
            }

        # 按规则分组
        rule_stats = {}
        rule_groups = defaultdict(list)
        for r in recs:
            rule_groups[r['rule_name']].append(r)
        for rule_name, rr in rule_groups.items():
            if len(rr) >= 5:
                rc = sum(1 for r in rr if r['correct'])
                rule_stats[rule_name] = {
                    'accuracy': round(rc / len(rr) * 100, 1),
                    'total': len(rr),
                }

        result[board_name] = {
            'accuracy': accuracy,
            'total': total,
            'correct': correct,
            'up_count': len(up_recs),
            'down_count': len(down_recs),
            'up_accuracy': up_acc,
            'down_accuracy': down_acc,
            'avg_up_return': avg_return,
            'layers': layer_stats,
            'rules': rule_stats,
        }

    return result


# ═══════════════════════════════════════════════════════════
# 分析维度2：按行业分组
# ═══════════════════════════════════════════════════════════

def analyze_by_industry(records: list[dict], industry_map: dict,
                        min_samples: int = 30) -> dict:
    """按申万一级行业分组统计V11准确率。"""
    industry_records = defaultdict(list)

    for rec in records:
        code = rec['stock_code']
        industry = industry_map.get(code, '_未知行业')
        industry_records[industry].append(rec)

    result = {}
    for industry, recs in industry_records.items():
        if len(recs) < min_samples:
            continue

        total = len(recs)
        correct = sum(1 for r in recs if r['correct'])
        accuracy = round(correct / total * 100, 1)

        up_recs = [r for r in recs if r['pred_up']]
        down_recs = [r for r in recs if not r['pred_up']]
        up_acc = round(sum(1 for r in up_recs if r['correct']) / len(up_recs) * 100, 1) if up_recs else 0
        down_acc = round(sum(1 for r in down_recs if r['correct']) / len(down_recs) * 100, 1) if down_recs else 0

        up_returns = [r['next_week_chg'] for r in up_recs]
        avg_return = round(_mean(up_returns), 2) if up_returns else 0

        # 按层级
        layer_stats = {}
        layer_groups = defaultdict(list)
        for r in recs:
            layer_groups[r['layer']].append(r)
        for layer, lr in layer_groups.items():
            lc = sum(1 for r in lr if r['correct'])
            layer_stats[layer] = {
                'accuracy': round(lc / len(lr) * 100, 1),
                'total': len(lr),
            }

        result[industry] = {
            'accuracy': accuracy,
            'total': total,
            'correct': correct,
            'up_count': len(up_recs),
            'down_count': len(down_recs),
            'up_accuracy': up_acc,
            'down_accuracy': down_acc,
            'avg_up_return': avg_return,
            'layers': layer_stats,
        }

    return result


# ═══════════════════════════════════════════════════════════
# 分析维度3：规则×板块交叉分析
# ═══════════════════════════════════════════════════════════

def analyze_rule_board_cross(records: list[dict], stock_board_map: dict,
                             min_samples: int = 10) -> dict:
    """规则×板块交叉分析：找出每条规则在哪些板块表现最好/最差。

    Returns:
        {rule_name: {
            'global_accuracy': float,
            'best_boards': [(board, acc, n), ...],
            'worst_boards': [(board, acc, n), ...],
            'board_spread': float  # 板块间准确率极差
        }}
    """
    # rule → board → records
    rule_board = defaultdict(lambda: defaultdict(list))

    for rec in records:
        code = rec['stock_code']
        boards = stock_board_map.get(code, [])
        rule = rec['rule_name']
        for b in boards:
            rule_board[rule][b['board_name']].append(rec)

    result = {}
    for rule_name, board_map in rule_board.items():
        # 全局准确率
        all_recs = [r for recs in board_map.values() for r in recs]
        global_acc = round(sum(1 for r in all_recs if r['correct']) / len(all_recs) * 100, 1) if all_recs else 0

        board_accs = []
        for board_name, recs in board_map.items():
            if len(recs) >= min_samples:
                acc = round(sum(1 for r in recs if r['correct']) / len(recs) * 100, 1)
                board_accs.append((board_name, acc, len(recs)))

        if len(board_accs) < 3:
            continue

        board_accs.sort(key=lambda x: x[1], reverse=True)
        spread = board_accs[0][1] - board_accs[-1][1]

        result[rule_name] = {
            'global_accuracy': global_acc,
            'total_boards': len(board_accs),
            'best_boards': board_accs[:5],
            'worst_boards': board_accs[-5:],
            'board_spread': round(spread, 1),
        }

    return result


# ═══════════════════════════════════════════════════════════
# 分析维度4：板块动量对准确率的影响
# ═══════════════════════════════════════════════════════════

def analyze_board_momentum_impact(records: list[dict], stock_board_map: dict,
                                  board_klines: dict) -> dict:
    """分析板块当周动量（涨跌幅）对V11预测准确率的影响。

    将板块按当周涨跌幅分为：强势(>2%), 中性(-2%~2%), 弱势(<-2%)，
    分别统计V11在这三种板块状态下的准确率。

    Returns:
        {
            'strong_boards': {accuracy, total, avg_return},
            'neutral_boards': {...},
            'weak_boards': {...},
            'momentum_correlation': float  # 板块动量与准确率的相关性
        }
    """
    # 板块K线按周分组
    board_week_chg = {}  # {board_code: {iso_week: chg}}
    for bc, klines in board_klines.items():
        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            wg[iw].append(k['change_percent'])
        board_week_chg[bc] = {iw: _compound_return(pcts) for iw, pcts in wg.items()}

    # 为每条记录标注板块动量
    strong_recs, neutral_recs, weak_recs = [], [], []

    for rec in records:
        code = rec['stock_code']
        iw = rec['iso_week']
        boards = stock_board_map.get(code, [])
        if not boards:
            continue

        # 取所有所属板块的平均动量
        momentums = []
        for b in boards:
            bc = b['board_code']
            chg = board_week_chg.get(bc, {}).get(iw)
            if chg is not None:
                momentums.append(chg)

        if not momentums:
            continue

        avg_momentum = _mean(momentums)
        if avg_momentum > 2:
            strong_recs.append(rec)
        elif avg_momentum < -2:
            weak_recs.append(rec)
        else:
            neutral_recs.append(rec)

    def _summarize(recs):
        if not recs:
            return {'accuracy': 0, 'total': 0, 'avg_return': 0}
        total = len(recs)
        correct = sum(1 for r in recs if r['correct'])
        up_returns = [r['next_week_chg'] for r in recs if r['pred_up']]
        return {
            'accuracy': round(correct / total * 100, 1),
            'total': total,
            'correct': correct,
            'avg_up_return': round(_mean(up_returns), 2) if up_returns else 0,
            'up_count': sum(1 for r in recs if r['pred_up']),
            'down_count': sum(1 for r in recs if not r['pred_up']),
        }

    return {
        'strong_boards': _summarize(strong_recs),
        'neutral_boards': _summarize(neutral_recs),
        'weak_boards': _summarize(weak_recs),
    }


# ═══════════════════════════════════════════════════════════
# 分析维度5：板块适配参数探索
# ═══════════════════════════════════════════════════════════

def analyze_adaptive_thresholds(records: list[dict], stock_board_map: dict,
                                min_samples: int = 50) -> dict:
    """探索不同板块是否需要不同的阈值参数。

    对每个板块，模拟调整以下参数的效果：
    - this_chg 阈值（超跌/过热判定）
    - pos60 阈值（高低位判定）
    - vol_ratio 阈值（量能判定）

    Returns:
        {board_name: {
            'current_accuracy': float,
            'best_this_chg_shift': float,  # 最优涨跌幅阈值偏移
            'best_pos60_shift': float,
            'potential_improvement': float,  # 潜在提升空间
            'analysis': str
        }}
    """
    board_records = defaultdict(list)
    for rec in records:
        code = rec['stock_code']
        boards = stock_board_map.get(code, [])
        for b in boards:
            board_records[b['board_name']].append(rec)

    result = {}
    for board_name, recs in board_records.items():
        if len(recs) < min_samples:
            continue

        current_acc = round(sum(1 for r in recs if r['correct']) / len(recs) * 100, 1)

        # 分析错误预测的特征分布
        wrong_recs = [r for r in recs if not r['correct']]
        correct_recs = [r for r in recs if r['correct']]

        if not wrong_recs or not correct_recs:
            continue

        # 错误预测 vs 正确预测的 this_week_chg 分布
        wrong_chgs = [r['this_week_chg'] for r in wrong_recs]
        correct_chgs = [r['this_week_chg'] for r in correct_recs]
        wrong_avg_chg = _mean(wrong_chgs)
        correct_avg_chg = _mean(correct_chgs)

        # 错误预测 vs 正确预测的 pos60 分布
        wrong_pos = [r['pos60'] for r in wrong_recs if r['pos60'] is not None]
        correct_pos = [r['pos60'] for r in correct_recs if r['pos60'] is not None]
        wrong_avg_pos = _mean(wrong_pos) if wrong_pos else 0.5
        correct_avg_pos = _mean(correct_pos) if correct_pos else 0.5

        # 错误预测 vs 正确预测的 vol_ratio 分布
        wrong_vol = [r['vol_ratio'] for r in wrong_recs if r['vol_ratio'] is not None]
        correct_vol = [r['vol_ratio'] for r in correct_recs if r['vol_ratio'] is not None]
        wrong_avg_vol = _mean(wrong_vol) if wrong_vol else 1.0
        correct_avg_vol = _mean(correct_vol) if correct_vol else 1.0

        # 模拟过滤：如果排除 pos60 > 某阈值的预测，准确率变化
        best_filter_acc = current_acc
        best_filter_desc = ''
        best_filter_loss = 0  # 过滤掉的样本比例

        # 尝试不同的 pos60 过滤阈值
        for pos_thresh in [0.8, 0.7, 0.6, 0.5]:
            filtered = [r for r in recs if r['pos60'] is None or r['pos60'] < pos_thresh]
            if len(filtered) >= min_samples * 0.5:
                fc = sum(1 for r in filtered if r['correct'])
                fa = round(fc / len(filtered) * 100, 1)
                if fa > best_filter_acc:
                    best_filter_acc = fa
                    best_filter_desc = f'过滤pos60>{pos_thresh}'
                    best_filter_loss = round((1 - len(filtered) / len(recs)) * 100, 1)

        # 尝试不同的 vol_ratio 过滤
        for vol_thresh in [2.5, 2.0, 1.5]:
            filtered = [r for r in recs if r['vol_ratio'] is None or r['vol_ratio'] < vol_thresh]
            if len(filtered) >= min_samples * 0.5:
                fc = sum(1 for r in filtered if r['correct'])
                fa = round(fc / len(filtered) * 100, 1)
                if fa > best_filter_acc:
                    best_filter_acc = fa
                    best_filter_desc = f'过滤vol_ratio>{vol_thresh}'
                    best_filter_loss = round((1 - len(filtered) / len(recs)) * 100, 1)

        # 尝试只保留特定层级
        for keep_layers in [['backbone'], ['backbone', 'bull'], ['backbone', 'extension']]:
            filtered = [r for r in recs if r['layer'] in keep_layers]
            if len(filtered) >= min_samples * 0.3:
                fc = sum(1 for r in filtered if r['correct'])
                fa = round(fc / len(filtered) * 100, 1)
                if fa > best_filter_acc:
                    best_filter_acc = fa
                    best_filter_desc = f'仅保留{"+".join(keep_layers)}'
                    best_filter_loss = round((1 - len(filtered) / len(recs)) * 100, 1)

        improvement = round(best_filter_acc - current_acc, 1)

        # 生成分析结论
        analysis_parts = []
        if abs(wrong_avg_chg - correct_avg_chg) > 1:
            analysis_parts.append(
                f'错误预测平均周涨跌{wrong_avg_chg:+.1f}% vs 正确{correct_avg_chg:+.1f}%')
        if abs(wrong_avg_pos - correct_avg_pos) > 0.1:
            analysis_parts.append(
                f'错误预测平均pos60={wrong_avg_pos:.2f} vs 正确={correct_avg_pos:.2f}')
        if improvement > 0:
            analysis_parts.append(
                f'最优过滤: {best_filter_desc} → +{improvement}%准确率(损失{best_filter_loss}%覆盖)')

        result[board_name] = {
            'current_accuracy': current_acc,
            'total': len(recs),
            'wrong_avg_chg': round(wrong_avg_chg, 2),
            'correct_avg_chg': round(correct_avg_chg, 2),
            'wrong_avg_pos60': round(wrong_avg_pos, 3),
            'correct_avg_pos60': round(correct_avg_pos, 3),
            'wrong_avg_vol_ratio': round(wrong_avg_vol, 2),
            'correct_avg_vol_ratio': round(correct_avg_vol, 2),
            'best_filter': best_filter_desc,
            'best_filter_accuracy': best_filter_acc,
            'potential_improvement': improvement,
            'filter_coverage_loss': best_filter_loss,
            'analysis': '; '.join(analysis_parts) if analysis_parts else '无显著差异',
        }

    return result


# ═══════════════════════════════════════════════════════════
# 分析维度6：板块波动率分层分析
# ═══════════════════════════════════════════════════════════

def analyze_by_board_volatility(records: list[dict], stock_board_map: dict,
                                board_klines: dict) -> dict:
    """按板块波动率分层分析V11准确率。

    将板块按历史波动率分为：高波动(>3%), 中波动(1.5%~3%), 低波动(<1.5%)，
    分析V11在不同波动率板块中的表现差异。
    """
    # 计算每个板块的历史波动率
    board_vol = {}
    for bc, klines in board_klines.items():
        pcts = [k['change_percent'] for k in klines if k['change_percent'] is not None]
        if len(pcts) >= 20:
            board_vol[bc] = _std(pcts)

    # 板块code→name映射
    board_code_to_name = {}
    for boards in stock_board_map.values():
        for b in boards:
            board_code_to_name[b['board_code']] = b['board_name']

    # 为每条记录标注板块波动率
    high_vol_recs, mid_vol_recs, low_vol_recs = [], [], []

    for rec in records:
        code = rec['stock_code']
        boards = stock_board_map.get(code, [])
        if not boards:
            continue

        vols = [board_vol.get(b['board_code'], 0) for b in boards if b['board_code'] in board_vol]
        if not vols:
            continue

        avg_vol = _mean(vols)
        if avg_vol > 3:
            high_vol_recs.append(rec)
        elif avg_vol < 1.5:
            low_vol_recs.append(rec)
        else:
            mid_vol_recs.append(rec)

    def _summarize(recs, label):
        if not recs:
            return {'label': label, 'accuracy': 0, 'total': 0}
        total = len(recs)
        correct = sum(1 for r in recs if r['correct'])
        up_recs = [r for r in recs if r['pred_up']]
        down_recs = [r for r in recs if not r['pred_up']]
        up_acc = round(sum(1 for r in up_recs if r['correct']) / len(up_recs) * 100, 1) if up_recs else 0
        down_acc = round(sum(1 for r in down_recs if r['correct']) / len(down_recs) * 100, 1) if down_recs else 0
        return {
            'label': label,
            'accuracy': round(correct / total * 100, 1),
            'total': total,
            'up_accuracy': up_acc,
            'down_accuracy': down_acc,
            'up_count': len(up_recs),
            'down_count': len(down_recs),
        }

    return {
        'high_volatility': _summarize(high_vol_recs, '高波动(>3%)'),
        'mid_volatility': _summarize(mid_vol_recs, '中波动(1.5~3%)'),
        'low_volatility': _summarize(low_vol_recs, '低波动(<1.5%)'),
    }


# ═══════════════════════════════════════════════════════════
# 分析维度7：板块内相对强弱对预测的影响
# ═══════════════════════════════════════════════════════════

def analyze_relative_strength_in_board(records: list[dict], stock_board_map: dict,
                                       data: dict) -> dict:
    """分析个股在板块内的相对强弱对V11预测准确率的影响。

    将个股按本周涨跌幅相对板块平均的超额收益分为：
    - 板块领涨股（超额>2%）
    - 板块同步股（超额-2%~2%）
    - 板块落后股（超额<-2%）
    """
    # 按周计算每只股票的板块超额收益
    leader_recs, sync_recs, laggard_recs = [], [], []

    # 简化：用个股本周涨跌 vs 大盘涨跌的超额来近似
    for rec in records:
        excess = rec['this_week_chg'] - rec['market_chg']
        if excess > 2:
            leader_recs.append(rec)
        elif excess < -2:
            laggard_recs.append(rec)
        else:
            sync_recs.append(rec)

    def _summarize(recs, label):
        if not recs:
            return {'label': label, 'accuracy': 0, 'total': 0}
        total = len(recs)
        correct = sum(1 for r in recs if r['correct'])
        up_recs = [r for r in recs if r['pred_up']]
        down_recs = [r for r in recs if not r['pred_up']]
        up_acc = round(sum(1 for r in up_recs if r['correct']) / len(up_recs) * 100, 1) if up_recs else 0
        down_acc = round(sum(1 for r in down_recs if r['correct']) / len(down_recs) * 100, 1) if down_recs else 0
        return {
            'label': label,
            'accuracy': round(correct / total * 100, 1),
            'total': total,
            'up_accuracy': up_acc,
            'down_accuracy': down_acc,
        }

    return {
        'board_leaders': _summarize(leader_recs, '板块领涨(超额>2%)'),
        'board_sync': _summarize(sync_recs, '板块同步(-2%~2%)'),
        'board_laggards': _summarize(laggard_recs, '板块落后(超额<-2%)'),
    }


# ═══════════════════════════════════════════════════════════
# 汇总报告生成
# ═══════════════════════════════════════════════════════════

def generate_summary_report(global_stats: dict, board_analysis: dict,
                            industry_analysis: dict, rule_cross: dict,
                            momentum_impact: dict, adaptive: dict,
                            volatility: dict, relative_strength: dict) -> str:
    """生成可读的汇总报告。"""
    lines = []
    lines.append("=" * 80)
    lines.append("  V11 下周预测算法 — 行业/概念板块适配性深度回测分析报告")
    lines.append("=" * 80)
    lines.append("")

    # 全局统计
    lines.append("【全局统计】")
    lines.append(f"  总预测样本: {global_stats['total']}")
    lines.append(f"  全局准确率: {global_stats['accuracy']}%")
    lines.append(f"  预测UP: {global_stats['up_count']} (准确率 {global_stats['up_accuracy']}%)")
    lines.append(f"  预测DOWN: {global_stats['down_count']} (准确率 {global_stats['down_accuracy']}%)")
    lines.append(f"  覆盖股票: {global_stats['stock_count']} 只")
    lines.append(f"  覆盖周数: {global_stats['week_count']} 周")
    lines.append("")

    # 行业分析 TOP/BOTTOM
    lines.append("【行业准确率排名】(申万一级)")
    sorted_ind = sorted(industry_analysis.items(), key=lambda x: x[1]['accuracy'], reverse=True)
    lines.append(f"  {'行业':<12} {'准确率':>8} {'样本数':>8} {'UP准确率':>10} {'DOWN准确率':>10} {'UP均收益':>10}")
    lines.append("  " + "-" * 62)
    for ind, stats in sorted_ind:
        lines.append(f"  {ind:<12} {stats['accuracy']:>7.1f}% {stats['total']:>7} "
                     f"{stats['up_accuracy']:>9.1f}% {stats['down_accuracy']:>9.1f}% "
                     f"{stats['avg_up_return']:>9.2f}%")
    lines.append("")

    # 概念板块 TOP 20 / BOTTOM 20
    sorted_boards = sorted(board_analysis.items(), key=lambda x: x[1]['accuracy'], reverse=True)
    lines.append(f"【概念板块准确率 TOP 20】(样本≥30)")
    lines.append(f"  {'板块':<16} {'准确率':>8} {'样本数':>8} {'UP准确率':>10} {'DOWN准确率':>10}")
    lines.append("  " + "-" * 56)
    for name, stats in sorted_boards[:20]:
        lines.append(f"  {name:<16} {stats['accuracy']:>7.1f}% {stats['total']:>7} "
                     f"{stats['up_accuracy']:>9.1f}% {stats['down_accuracy']:>9.1f}%")
    lines.append("")

    lines.append(f"【概念板块准确率 BOTTOM 20】")
    lines.append(f"  {'板块':<16} {'准确率':>8} {'样本数':>8} {'UP准确率':>10} {'DOWN准确率':>10}")
    lines.append("  " + "-" * 56)
    for name, stats in sorted_boards[-20:]:
        lines.append(f"  {name:<16} {stats['accuracy']:>7.1f}% {stats['total']:>7} "
                     f"{stats['up_accuracy']:>9.1f}% {stats['down_accuracy']:>9.1f}%")
    lines.append("")

    # 规则×板块交叉
    lines.append("【规则×板块交叉分析】(板块间准确率极差最大的规则)")
    sorted_cross = sorted(rule_cross.items(), key=lambda x: x[1]['board_spread'], reverse=True)
    for rule_name, stats in sorted_cross[:10]:
        lines.append(f"  {rule_name}")
        lines.append(f"    全局准确率: {stats['global_accuracy']}%, 板块极差: {stats['board_spread']}%")
        best = stats['best_boards'][:3]
        worst = stats['worst_boards'][-3:]
        lines.append(f"    最佳板块: {', '.join(f'{b[0]}({b[1]}%,n={b[2]})' for b in best)}")
        lines.append(f"    最差板块: {', '.join(f'{b[0]}({b[1]}%,n={b[2]})' for b in worst)}")
    lines.append("")

    # 板块动量影响
    lines.append("【板块动量对准确率的影响】")
    for key, label in [('strong_boards', '强势板块(周涨>2%)'),
                       ('neutral_boards', '中性板块(-2%~2%)'),
                       ('weak_boards', '弱势板块(周跌>2%)')]:
        s = momentum_impact.get(key, {})
        lines.append(f"  {label}: 准确率={s.get('accuracy', 0)}%, "
                     f"样本={s.get('total', 0)}, "
                     f"UP预测均收益={s.get('avg_up_return', 0)}%")
    lines.append("")

    # 板块波动率分层
    lines.append("【板块波动率分层分析】")
    for key in ['high_volatility', 'mid_volatility', 'low_volatility']:
        s = volatility.get(key, {})
        lines.append(f"  {s.get('label', key)}: 准确率={s.get('accuracy', 0)}%, "
                     f"样本={s.get('total', 0)}, "
                     f"UP准确率={s.get('up_accuracy', 0)}%, "
                     f"DOWN准确率={s.get('down_accuracy', 0)}%")
    lines.append("")

    # 板块内相对强弱
    lines.append("【板块内相对强弱对预测的影响】")
    for key in ['board_leaders', 'board_sync', 'board_laggards']:
        s = relative_strength.get(key, {})
        lines.append(f"  {s.get('label', key)}: 准确率={s.get('accuracy', 0)}%, "
                     f"样本={s.get('total', 0)}, "
                     f"UP准确率={s.get('up_accuracy', 0)}%, "
                     f"DOWN准确率={s.get('down_accuracy', 0)}%")
    lines.append("")

    # 板块适配参数探索 TOP 20
    lines.append("【板块适配参数探索 — 潜在提升空间 TOP 20】")
    sorted_adaptive = sorted(adaptive.items(),
                             key=lambda x: x[1]['potential_improvement'], reverse=True)
    for name, stats in sorted_adaptive[:20]:
        if stats['potential_improvement'] <= 0:
            continue
        lines.append(f"  {name}: 当前{stats['current_accuracy']}% → "
                     f"优化后{stats['best_filter_accuracy']}% "
                     f"(+{stats['potential_improvement']}%, "
                     f"方法: {stats['best_filter']}, "
                     f"覆盖损失: {stats['filter_coverage_loss']}%)")
        if stats['analysis']:
            lines.append(f"    分析: {stats['analysis']}")
    lines.append("")

    # 结论
    lines.append("=" * 80)
    lines.append("【结论与建议】")

    # 计算关键指标
    board_accs = [s['accuracy'] for s in board_analysis.values()]
    if board_accs:
        acc_std = _std(board_accs) if len(board_accs) >= 2 else 0
        acc_range = max(board_accs) - min(board_accs)
        lines.append(f"  1. 板块间准确率标准差: {acc_std:.1f}%, 极差: {acc_range:.1f}%")
        if acc_std > 5:
            lines.append("     → 板块间差异显著，适配有价值")
        else:
            lines.append("     → 板块间差异较小，通用规则已足够")

    improvable = [s for s in adaptive.values() if s['potential_improvement'] > 2]
    lines.append(f"  2. 有{len(improvable)}个板块通过参数适配可提升>2%准确率")

    # 高/低波动率差异
    hv = volatility.get('high_volatility', {}).get('accuracy', 0)
    lv = volatility.get('low_volatility', {}).get('accuracy', 0)
    if abs(hv - lv) > 3:
        lines.append(f"  3. 高波动板块({hv}%) vs 低波动板块({lv}%) 差异{abs(hv-lv):.1f}%，"
                     "建议按波动率分层调参")
    else:
        lines.append(f"  3. 高/低波动板块准确率差异不大({abs(hv-lv):.1f}%)，波动率适配优先级低")

    lines.append("=" * 80)
    return '\n'.join(lines)


# ═══════════════════════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════════════════════

def main():
    t0 = time.time()
    logger.info("=" * 70)
    logger.info("  V11 行业/概念板块适配性深度回测分析")
    logger.info("=" * 70)

    # ── 参数 ──
    N_WEEKS = 29          # 回测周数（与生产一致）
    MIN_BOARD_SAMPLES = 30  # 板块最小样本数

    # ── Step 1: 加载映射数据 ──
    logger.info("[Step 1/6] 加载板块映射和行业映射...")
    stock_board_map = load_stock_board_mapping()
    industry_map = load_industry_mapping()
    logger.info("  板块映射: %d 只股票, 行业映射: %d 只股票",
                len(stock_board_map), len(industry_map))

    # ── Step 2: 获取全量股票代码 ──
    logger.info("[Step 2/6] 获取全量A股代码...")
    all_codes = get_all_stock_codes()
    logger.info("  共 %d 只A股（排除北交所）", len(all_codes))

    # ── Step 3: 加载K线数据 ──
    logger.info("[Step 3/6] 加载K线数据...")
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("SELECT MAX(`date`) FROM stock_kline WHERE stock_code = '000001.SH'")
    row = cur.fetchone()
    cur.close()
    conn.close()
    latest_date = list(row.values())[0] if row else None
    if not latest_date:
        logger.error("无法获取最新交易日")
        return

    dt_end = datetime.strptime(str(latest_date), '%Y-%m-%d')
    # 回溯范围：n_weeks + 额外60天用于 price_pos_60 计算
    dt_start = dt_end - timedelta(days=(N_WEEKS + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')
    end_date = str(latest_date)

    logger.info("  数据范围: %s ~ %s", start_date, end_date)
    data = load_backtest_klines(all_codes, start_date, end_date)
    logger.info("  个股K线: %d 只, 大盘指数: %d 个",
                len(data['stock_klines']), len(data['market_klines']))

    # 加载板块K线
    all_board_codes = set()
    for boards in stock_board_map.values():
        for b in boards:
            all_board_codes.add(b['board_code'])
    board_klines = load_board_klines(list(all_board_codes), start_date, end_date)
    logger.info("  板块K线: %d 个板块", len(board_klines))

    # ── Step 4: 执行V11回测 ──
    logger.info("[Step 4/6] 执行V11规则引擎回测（%d只股票 × %d周）...", len(all_codes), N_WEEKS)
    records = run_v11_backtest_per_stock(all_codes, data, n_weeks=N_WEEKS)
    logger.info("  回测完成: %d 条预测记录", len(records))

    if not records:
        logger.error("回测无结果，退出")
        return

    # 全局统计
    total = len(records)
    correct = sum(1 for r in records if r['correct'])
    up_recs = [r for r in records if r['pred_up']]
    down_recs = [r for r in records if not r['pred_up']]
    global_stats = {
        'total': total,
        'correct': correct,
        'accuracy': round(correct / total * 100, 1),
        'up_count': len(up_recs),
        'down_count': len(down_recs),
        'up_accuracy': round(sum(1 for r in up_recs if r['correct']) / len(up_recs) * 100, 1) if up_recs else 0,
        'down_accuracy': round(sum(1 for r in down_recs if r['correct']) / len(down_recs) * 100, 1) if down_recs else 0,
        'stock_count': len(set(r['stock_code'] for r in records)),
        'week_count': len(set(r['iso_week'] for r in records)),
    }
    logger.info("  全局准确率: %.1f%% (%d/%d)", global_stats['accuracy'], correct, total)

    # ── Step 5: 多维度分析 ──
    logger.info("[Step 5/6] 执行多维度分析...")

    logger.info("  [5a] 按概念板块分组分析...")
    board_analysis = analyze_by_concept_board(records, stock_board_map, min_samples=MIN_BOARD_SAMPLES)
    logger.info("    有效板块: %d 个", len(board_analysis))

    logger.info("  [5b] 按行业分组分析...")
    industry_analysis = analyze_by_industry(records, industry_map, min_samples=MIN_BOARD_SAMPLES)
    logger.info("    有效行业: %d 个", len(industry_analysis))

    logger.info("  [5c] 规则×板块交叉分析...")
    rule_cross = analyze_rule_board_cross(records, stock_board_map, min_samples=10)
    logger.info("    有效规则: %d 条", len(rule_cross))

    logger.info("  [5d] 板块动量影响分析...")
    momentum_impact = analyze_board_momentum_impact(records, stock_board_map, board_klines)

    logger.info("  [5e] 板块波动率分层分析...")
    volatility = analyze_by_board_volatility(records, stock_board_map, board_klines)

    logger.info("  [5f] 板块内相对强弱分析...")
    relative_strength = analyze_relative_strength_in_board(records, stock_board_map, data)

    logger.info("  [5g] 板块适配参数探索...")
    adaptive = analyze_adaptive_thresholds(records, stock_board_map, min_samples=50)
    logger.info("    可优化板块: %d 个", sum(1 for s in adaptive.values() if s['potential_improvement'] > 0))

    # ── Step 6: 生成报告 ──
    logger.info("[Step 6/6] 生成报告...")

    report = generate_summary_report(
        global_stats, board_analysis, industry_analysis, rule_cross,
        momentum_impact, adaptive, volatility, relative_strength)

    # 保存文本报告
    report_path = OUTPUT_DIR / "v11_sector_adaptive_backtest_report.txt"
    report_path.write_text(report, encoding='utf-8')
    logger.info("  文本报告: %s", report_path)

    # 保存JSON详细数据
    json_data = {
        'meta': {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'n_weeks': N_WEEKS,
            'total_stocks': len(all_codes),
            'total_records': len(records),
            'data_range': f'{start_date} ~ {end_date}',
        },
        'global_stats': global_stats,
        'industry_analysis': industry_analysis,
        'board_analysis': {
            k: {kk: vv for kk, vv in v.items() if kk != 'rules'}
            for k, v in board_analysis.items()
        },
        'rule_board_cross': {
            k: {
                'global_accuracy': v['global_accuracy'],
                'board_spread': v['board_spread'],
                'total_boards': v['total_boards'],
                'best_boards': [{'name': b[0], 'accuracy': b[1], 'samples': b[2]} for b in v['best_boards']],
                'worst_boards': [{'name': b[0], 'accuracy': b[1], 'samples': b[2]} for b in v['worst_boards']],
            }
            for k, v in rule_cross.items()
        },
        'momentum_impact': momentum_impact,
        'volatility_analysis': volatility,
        'relative_strength': relative_strength,
        'adaptive_thresholds': adaptive,
    }

    json_path = OUTPUT_DIR / "v11_sector_adaptive_backtest_result.json"
    json_path.write_text(json.dumps(json_data, ensure_ascii=False, indent=2, default=str),
                         encoding='utf-8')
    logger.info("  JSON数据: %s", json_path)

    elapsed = time.time() - t0
    logger.info("=" * 70)
    logger.info("  分析完成，耗时 %.1f 秒", elapsed)
    logger.info("=" * 70)

    # 打印报告
    print("\n" + report)


if __name__ == '__main__':
    main()
