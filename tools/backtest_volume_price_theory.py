#!/usr/bin/env python3
"""
量价关系理论深度验证
====================
针对财经科普视频中的量价关系理论，用A股真实数据进行系统性回测验证。

验证的核心理论：
  一、上涨阶段：低位放量上涨(启动信号)、高位放量上涨(看效率)、
                低位缩量上涨(轻车上阵vs假反弹)、高位缩量上涨(加速vs背离)
  二、下跌阶段：低位放量下跌(暴力换手vs抛压)、高位放量下跌(出货信号)、
                缩量下跌(阴跌)、高位缩量下跌(假摔vs阴跌)
  三、横盘阶段：低位放量横盘(底部换手)、高位放量横盘(分歧)、
                低位缩量横盘(潜伏)、高位缩量横盘(消化分歧)

方法：
  1. 用60日K线判断股票所处"位置"（低位/高位/横盘）
  2. 用近5日量价关系判断当前"状态"
  3. 观察未来5日/10日的实际涨跌幅，统计各场景的胜率和收益

用法：
    source .venv/bin/activate
    python -m tools.backtest_volume_price_theory
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


def load_stock_codes(limit=3000):
    """加载股票代码，排除ST、北交所等"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT stock_code FROM stock_kline "
        "WHERE stock_code NOT LIKE '4%%' AND stock_code NOT LIKE '8%%' "
        "AND stock_code NOT LIKE '9%%' "
        "ORDER BY stock_code LIMIT %s", (limit,))
    codes = [r['stock_code'] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return codes


def load_kline_data(stock_codes, start_date, end_date):
    """批量加载K线数据"""
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


# ═══════════════════════════════════════════════════════════════
# 核心：判断股票"位置"和"量价状态"
# ═══════════════════════════════════════════════════════════════

def classify_position(klines, lookback=60):
    """
    判断股票所处位置：低位 / 高位 / 横盘
    基于近lookback日的价格位置：
      - 当前价在近60日区间的下1/3 → 低位
      - 当前价在近60日区间的上1/3 → 高位
      - 中间 → 横盘
    """
    if len(klines) < lookback:
        return None
    recent = klines[-lookback:]
    highs = [k['high'] for k in recent if k['high'] > 0]
    lows = [k['low'] for k in recent if k['low'] > 0]
    if not highs or not lows:
        return None
    period_high = max(highs)
    period_low = min(lows)
    if period_high <= period_low:
        return 'sideways'
    current = klines[-1]['close']
    if current <= 0:
        return None
    position_pct = (current - period_low) / (period_high - period_low)
    if position_pct <= 0.33:
        return 'low'       # 低位
    elif position_pct >= 0.67:
        return 'high'      # 高位
    else:
        return 'sideways'  # 横盘


def classify_volume(klines, lookback_vol=20, lookback_recent=5):
    """
    判断成交量状态：放量 / 缩量 / 正常
    近5日均量 vs 近20日均量：
      - >1.5倍 → 放量
      - <0.7倍 → 缩量
      - 中间 → 正常量
    """
    if len(klines) < lookback_vol:
        return None, 0
    vol_20 = [k['volume'] for k in klines[-lookback_vol:] if k['volume'] > 0]
    vol_5 = [k['volume'] for k in klines[-lookback_recent:] if k['volume'] > 0]
    if not vol_20 or not vol_5:
        return None, 0
    avg_20 = sum(vol_20) / len(vol_20)
    avg_5 = sum(vol_5) / len(vol_5)
    if avg_20 <= 0:
        return None, 0
    ratio = avg_5 / avg_20
    if ratio >= 1.5:
        return 'high_vol', ratio    # 放量
    elif ratio <= 0.7:
        return 'low_vol', ratio     # 缩量
    else:
        return 'normal_vol', ratio  # 正常量


def classify_price_action(klines, lookback=5):
    """
    判断近期价格走势：上涨 / 下跌 / 横盘
    近5日累计涨跌幅：
      - >3% → 上涨
      - <-3% → 下跌
      - 中间 → 横盘
    """
    if len(klines) < lookback + 1:
        return None, 0
    base = klines[-(lookback + 1)]['close']
    current = klines[-1]['close']
    if base <= 0:
        return None, 0
    ret = (current / base - 1) * 100
    if ret > 3:
        return 'rising', ret
    elif ret < -3:
        return 'falling', ret
    else:
        return 'flat', ret


def compute_efficiency(klines, lookback=5):
    """
    计算量价效率：单位成交量推动的价格变动
    效率 = |价格变动%| / 成交量比率
    高效率 = 少量资金推动大幅涨跌
    低效率 = 大量资金但价格变动小（量价损耗）
    """
    if len(klines) < 20:
        return None
    vol_20 = [k['volume'] for k in klines[-20:] if k['volume'] > 0]
    vol_5 = [k['volume'] for k in klines[-lookback:] if k['volume'] > 0]
    if not vol_20 or not vol_5:
        return None
    avg_20 = sum(vol_20) / len(vol_20)
    avg_5 = sum(vol_5) / len(vol_5)
    if avg_20 <= 0:
        return None
    vol_ratio = avg_5 / avg_20
    base = klines[-(lookback + 1)]['close']
    current = klines[-1]['close']
    if base <= 0:
        return None
    price_change = abs((current / base - 1) * 100)
    if vol_ratio < 0.01:
        return None
    return price_change / vol_ratio


def check_prior_distribution(klines, lookback=20):
    """
    检查前期是否有出货迹象（高位放量滞涨、冲高回落）
    返回 True 如果有出货痕迹
    """
    if len(klines) < lookback + 5:
        return False
    prior = klines[-(lookback + 5):-5]
    if len(prior) < 10:
        return False
    vol_avg = sum(k['volume'] for k in prior if k['volume'] > 0) / max(len(prior), 1)
    # 检查是否有放量滞涨日（量>1.5倍均量，但涨幅<1%）
    stagnation_days = 0
    for k in prior[-10:]:
        if k['volume'] > vol_avg * 1.5 and abs(k['change_percent']) < 1:
            stagnation_days += 1
    # 检查是否有冲高回落（上影线长）
    reversal_days = 0
    for k in prior[-10:]:
        body = abs(k['close'] - k['open'])
        upper_shadow = k['high'] - max(k['close'], k['open'])
        if k['high'] > 0 and upper_shadow > body * 1.5:
            reversal_days += 1
    return stagnation_days >= 2 or reversal_days >= 2


# ═══════════════════════════════════════════════════════════════
# 场景分类：将视频中的12种量价场景映射为可回测的规则
# ═══════════════════════════════════════════════════════════════

def classify_scenario(klines):
    """
    将当前K线状态分类为视频中描述的12种量价场景之一。
    返回 (scenario_name, details_dict) 或 (None, None)
    """
    position = classify_position(klines)
    vol_state, vol_ratio = classify_volume(klines)
    price_action, price_ret = classify_price_action(klines)
    efficiency = compute_efficiency(klines)

    if position is None or vol_state is None or price_action is None:
        return None, None

    details = {
        'position': position,
        'vol_state': vol_state,
        'vol_ratio': round(vol_ratio, 2),
        'price_action': price_action,
        'price_ret': round(price_ret, 2),
        'efficiency': round(efficiency, 2) if efficiency else None,
    }

    # ── 上涨阶段 ──
    if price_action == 'rising':
        if position == 'low' and vol_state == 'high_vol':
            # 低位放量上涨：资金点火，强势启动
            return 'A1_低位放量上涨', details
        elif position == 'high' and vol_state == 'high_vol':
            # 高位放量上涨：看效率
            if efficiency and efficiency > 3:
                return 'A2a_高位放量高效上涨', details  # 强上加强
            else:
                return 'A2b_高位放量低效上涨', details  # 量价损耗
        elif position == 'low' and vol_state == 'low_vol':
            # 低位缩量上涨
            # 判断是否长期阴跌后（抛压耗尽）
            if len(klines) >= 40:
                ret_20 = (klines[-6]['close'] / klines[-26]['close'] - 1) * 100 if klines[-26]['close'] > 0 else 0
                if ret_20 < -10:
                    return 'A3a_低位缩量上涨_抛压耗尽', details
                else:
                    return 'A3b_低位缩量上涨_小反弹', details
            return 'A3b_低位缩量上涨_小反弹', details
        elif position == 'high' and vol_state == 'low_vol':
            # 高位缩量上涨
            if price_ret > 5:
                return 'A4a_高位缩量大涨', details  # 筹码锁定，加速
            else:
                return 'A4b_高位缩量小涨', details  # 背离式上涨

    # ── 下跌阶段 ──
    elif price_action == 'falling':
        if position == 'low' and vol_state == 'high_vol':
            # 低位放量下跌
            # 检查是否有长下影线（暴力换手）
            last_5 = klines[-5:]
            long_lower_shadow = 0
            for k in last_5:
                body = abs(k['close'] - k['open'])
                lower_shadow = min(k['close'], k['open']) - k['low']
                if k['low'] > 0 and lower_shadow > body * 1.5:
                    long_lower_shadow += 1
            if long_lower_shadow >= 2:
                return 'B1a_低位放量下跌_暴力换手', details
            else:
                return 'B1b_低位放量下跌_抛压大', details
        elif position == 'high' and vol_state == 'high_vol':
            # 高位放量下跌：出货信号
            return 'B2_高位放量下跌', details
        elif vol_state == 'low_vol' and position == 'low':
            # 低位缩量下跌：阴跌
            return 'B3_缩量下跌_阴跌', details
        elif position == 'high' and vol_state == 'low_vol':
            # 高位缩量下跌
            has_distribution = check_prior_distribution(klines)
            if has_distribution:
                return 'B4a_高位缩量下跌_出货后阴跌', details
            else:
                return 'B4b_高位缩量下跌_假摔', details

    # ── 横盘阶段 ──
    elif price_action == 'flat':
        if position == 'low' and vol_state == 'high_vol':
            return 'C1_低位放量横盘', details  # 底部换手
        elif position == 'high' and vol_state == 'high_vol':
            return 'C2_高位放量横盘', details  # 分歧大
        elif position == 'low' and vol_state == 'low_vol':
            return 'C3_低位缩量横盘', details  # 潜伏
        elif position == 'high' and vol_state == 'low_vol':
            return 'C4_高位缩量横盘', details  # 消化分歧

    return None, None


def compute_future_returns(klines, idx, horizons=(5, 10)):
    """计算未来N日的收益率"""
    base = klines[idx]['close']
    if base <= 0:
        return {}
    returns = {}
    for h in horizons:
        if idx + h < len(klines):
            future = klines[idx + h]['close']
            if future > 0:
                returns[f'ret_{h}d'] = round((future / base - 1) * 100, 2)
    # 未来最大回撤和最大涨幅
    if idx + 10 < len(klines):
        future_slice = klines[idx + 1: idx + 11]
        valid_highs = [k['high'] for k in future_slice if k.get('high', 0) > 0]
        valid_lows = [k['low'] for k in future_slice if k.get('low', 0) > 0]
        max_high = max(valid_highs) if valid_highs else base
        min_low = min(valid_lows) if valid_lows else base
        returns['max_gain_10d'] = round((max_high / base - 1) * 100, 2)
        returns['max_loss_10d'] = round((min_low / base - 1) * 100, 2)
    return returns


# ═══════════════════════════════════════════════════════════════
# 视频理论预期方向映射
# ═══════════════════════════════════════════════════════════════

THEORY_EXPECTATIONS = {
    # 上涨阶段
    'A1_低位放量上涨':         {'expected': 'UP',   'desc': '资金点火，强势启动信号，持续性强'},
    'A2a_高位放量高效上涨':    {'expected': 'UP',   'desc': '多头力量强，强上加强'},
    'A2b_高位放量低效上涨':    {'expected': 'DOWN', 'desc': '量价损耗，上涨乏力，短线谨慎'},
    'A3a_低位缩量上涨_抛压耗尽': {'expected': 'UP', 'desc': '主力轻推，轻车上阵'},
    'A3b_低位缩量上涨_小反弹': {'expected': 'DOWN', 'desc': '小资金自嗨，后续易回落'},
    'A4a_高位缩量大涨':       {'expected': 'UP',   'desc': '筹码锁定好，加速上涨'},
    'A4b_高位缩量小涨':       {'expected': 'DOWN', 'desc': '背离式上涨，涨势乏力'},

    # 下跌阶段
    'B1a_低位放量下跌_暴力换手': {'expected': 'UP',   'desc': '洗盘，后续可能反包'},
    'B1b_低位放量下跌_抛压大':   {'expected': 'DOWN', 'desc': '抛压大，继续下跌'},
    'B2_高位放量下跌':          {'expected': 'DOWN', 'desc': '资金抢跑出货，果断离场'},
    'B3_缩量下跌_阴跌':        {'expected': 'DOWN', 'desc': '阴跌走势，容易套住抄底者'},
    'B4a_高位缩量下跌_出货后阴跌': {'expected': 'DOWN', 'desc': '无人接盘，持续走弱'},
    'B4b_高位缩量下跌_假摔':    {'expected': 'UP',   'desc': '假摔，可能反弹'},

    # 横盘阶段
    'C1_低位放量横盘':  {'expected': 'UP',   'desc': '底部换手，有上涨潜力'},
    'C2_高位放量横盘':  {'expected': 'DOWN', 'desc': '分歧大，上涨乏力'},
    'C3_低位缩量横盘':  {'expected': 'FLAT', 'desc': '缺乏热度，适合潜伏非短线'},
    'C4_高位缩量横盘':  {'expected': 'UP',   'desc': '消化分歧，均线跟上后可能再启动'},
}


# ═══════════════════════════════════════════════════════════════
# 主回测逻辑
# ═══════════════════════════════════════════════════════════════

def run_backtest():
    t0 = time.time()
    print("=" * 70)
    print("量价关系理论深度验证 — 基于A股真实数据")
    print("=" * 70)

    # 加载数据（用2000只股票、400天做验证）
    logger.info("[1/3] 加载数据...")
    stock_codes = load_stock_codes(2000)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=400)).strftime('%Y-%m-%d')
    kline_data = load_kline_data(stock_codes, start_date, end_date)
    logger.info("  加载完成: %d只股票", len(kline_data))

    # 遍历每只股票的每个交易日，分类场景并记录未来收益
    logger.info("[2/3] 扫描量价场景...")
    scenario_results = defaultdict(list)
    total_scanned = 0
    total_classified = 0

    for code, klines in kline_data.items():
        if len(klines) < 80:
            continue
        # 从第60日开始扫描，确保有足够历史；留出10日计算未来收益
        for i in range(60, len(klines) - 10):
            total_scanned += 1
            hist = klines[:i + 1]
            scenario, details = classify_scenario(hist)
            if scenario is None:
                continue
            total_classified += 1
            future = compute_future_returns(klines, i)
            if not future:
                continue
            scenario_results[scenario].append({
                'code': code,
                'date': klines[i]['date'],
                'details': details,
                'future': future,
            })

    logger.info("  扫描完成: %d个交易日, %d个被分类 (%.1f%%)",
                total_scanned, total_classified,
                total_classified / total_scanned * 100 if total_scanned else 0)

    # 分析每个场景的准确率
    logger.info("[3/3] 统计各场景准确率...")
    report = {}

    for scenario in sorted(THEORY_EXPECTATIONS.keys()):
        records = scenario_results.get(scenario, [])
        if not records:
            report[scenario] = {'n': 0, 'note': '无样本'}
            continue

        expected = THEORY_EXPECTATIONS[scenario]['expected']
        desc = THEORY_EXPECTATIONS[scenario]['desc']

        # 统计未来5日和10日收益
        rets_5d = [r['future']['ret_5d'] for r in records if 'ret_5d' in r['future']]
        rets_10d = [r['future']['ret_10d'] for r in records if 'ret_10d' in r['future']]
        max_gains = [r['future']['max_gain_10d'] for r in records if 'max_gain_10d' in r['future']]
        max_losses = [r['future']['max_loss_10d'] for r in records if 'max_loss_10d' in r['future']]

        n = len(rets_5d)
        if n == 0:
            report[scenario] = {'n': 0, 'note': '无有效样本'}
            continue

        avg_5d = sum(rets_5d) / n
        avg_10d = sum(rets_10d) / len(rets_10d) if rets_10d else 0
        median_5d = sorted(rets_5d)[n // 2]

        # 方向准确率
        if expected == 'UP':
            correct_5d = sum(1 for r in rets_5d if r > 0)
            correct_10d = sum(1 for r in rets_10d if r > 0)
        elif expected == 'DOWN':
            correct_5d = sum(1 for r in rets_5d if r < 0)
            correct_10d = sum(1 for r in rets_10d if r < 0)
        else:  # FLAT
            correct_5d = sum(1 for r in rets_5d if abs(r) < 3)
            correct_10d = sum(1 for r in rets_10d if abs(r) < 5)

        acc_5d = correct_5d / n
        acc_10d = correct_10d / len(rets_10d) if rets_10d else 0

        # 盈亏比（仅对有方向预期的场景）
        if expected == 'UP':
            wins = [r for r in rets_5d if r > 0]
            losses = [r for r in rets_5d if r < 0]
        elif expected == 'DOWN':
            wins = [-r for r in rets_5d if r < 0]  # 做空盈利
            losses = [r for r in rets_5d if r > 0]  # 做空亏损
        else:
            wins = losses = []

        avg_win = sum(wins) / len(wins) if wins else 0
        avg_loss = sum(losses) / len(losses) if losses else 0
        profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else float('inf')

        # 收益分布
        pct_big_up = sum(1 for r in rets_5d if r > 5) / n  # 大涨>5%
        pct_big_down = sum(1 for r in rets_5d if r < -5) / n  # 大跌>5%

        report[scenario] = {
            'n': n,
            'expected_direction': expected,
            'desc': desc,
            'accuracy_5d': round(acc_5d, 4),
            'accuracy_10d': round(acc_10d, 4),
            'avg_return_5d': round(avg_5d, 2),
            'avg_return_10d': round(avg_10d, 2),
            'median_return_5d': round(median_5d, 2),
            'avg_max_gain_10d': round(sum(max_gains) / len(max_gains), 2) if max_gains else 0,
            'avg_max_loss_10d': round(sum(max_losses) / len(max_losses), 2) if max_losses else 0,
            'profit_loss_ratio': round(profit_loss_ratio, 2) if profit_loss_ratio != float('inf') else 'inf',
            'pct_big_up_5d': round(pct_big_up, 4),
            'pct_big_down_5d': round(pct_big_down, 4),
        }

    # ── 额外分析：口诀验证 ──
    # "低位看启动、高位看损耗、横盘看变量"
    mantra_report = {}

    # 低位看启动：低位场景中，放量上涨是否真的是最佳启动信号？
    low_scenarios = {k: v for k, v in report.items() if k.startswith(('A1', 'A3', 'B1', 'B3', 'C1', 'C3'))}
    mantra_report['低位看启动'] = {
        'scenarios': low_scenarios,
        'best': max(low_scenarios.items(), key=lambda x: x[1].get('avg_return_5d', -999))[0] if low_scenarios else None,
    }

    # 高位看损耗：高位场景中，量价效率是否真的是关键？
    high_scenarios = {k: v for k, v in report.items() if k.startswith(('A2', 'A4', 'B2', 'B4', 'C2', 'C4'))}
    mantra_report['高位看损耗'] = {
        'scenarios': high_scenarios,
        'best': max(high_scenarios.items(), key=lambda x: x[1].get('avg_return_5d', -999))[0] if high_scenarios else None,
    }

    # 横盘看变量：横盘场景中，量能变化是否预示方向？
    flat_scenarios = {k: v for k, v in report.items() if k.startswith('C')}
    mantra_report['横盘看变量'] = {
        'scenarios': flat_scenarios,
        'best': max(flat_scenarios.items(), key=lambda x: x[1].get('avg_return_5d', -999))[0] if flat_scenarios else None,
    }

    # 保存完整报告
    full_report = {
        'meta': {
            'total_scanned': total_scanned,
            'total_classified': total_classified,
            'n_stocks': len(kline_data),
            'date_range': f'{start_date} ~ {end_date}',
            'run_time': round(time.time() - t0, 1),
        },
        'scenario_analysis': report,
        'mantra_validation': mantra_report,
    }

    output_path = OUTPUT_DIR / "volume_price_theory_backtest.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(full_report, f, ensure_ascii=False, indent=2, default=str)

    # ═══════════════════════════════════════════════════════════
    # 打印结果
    # ═══════════════════════════════════════════════════════════
    print(f"\n数据范围: {start_date} ~ {end_date}, {len(kline_data)}只股票")
    print(f"扫描: {total_scanned}个交易日, 分类: {total_classified}个")

    # 按大类打印
    categories = [
        ("📈 上涨阶段", [k for k in sorted(report.keys()) if k.startswith('A')]),
        ("📉 下跌阶段", [k for k in sorted(report.keys()) if k.startswith('B')]),
        ("➡️  横盘阶段", [k for k in sorted(report.keys()) if k.startswith('C')]),
    ]

    for cat_name, scenarios in categories:
        print(f"\n{'─' * 70}")
        print(f"{cat_name}")
        print(f"{'─' * 70}")
        print(f"  {'场景':<30s} {'样本':>6s} {'预期':>4s} {'5日准确率':>8s} {'10日准确率':>9s} "
              f"{'均收益5d':>8s} {'均收益10d':>9s} {'盈亏比':>6s} {'判定':>4s}")
        print(f"  {'─' * 95}")

        for s in scenarios:
            r = report.get(s, {})
            if r.get('n', 0) == 0:
                print(f"  {s:<30s} {'无样本':>6s}")
                continue

            # 判定理论是否成立
            acc5 = r['accuracy_5d']
            if r['expected_direction'] == 'FLAT':
                verdict = '✅' if acc5 > 0.4 else '❌'
            else:
                verdict = '✅' if acc5 > 0.55 else ('⚠️' if acc5 > 0.50 else '❌')

            plr = r['profit_loss_ratio']
            plr_str = f"{plr:.1f}" if isinstance(plr, (int, float)) else plr

            print(f"  {s:<30s} {r['n']:>6d} {r['expected_direction']:>4s} "
                  f"{r['accuracy_5d']:>8.1%} {r['accuracy_10d']:>9.1%} "
                  f"{r['avg_return_5d']:>+8.2f}% {r['avg_return_10d']:>+8.2f}% "
                  f"{plr_str:>6s} {verdict:>4s}")

    # 总结
    print(f"\n{'═' * 70}")
    print("📋 理论验证总结")
    print(f"{'═' * 70}")

    verified = []
    partially = []
    failed = []

    for s, r in report.items():
        if r.get('n', 0) < 30:
            continue
        acc5 = r.get('accuracy_5d', 0)
        expected = r.get('expected_direction', '')
        if expected == 'FLAT':
            if acc5 > 0.4:
                verified.append(s)
            else:
                failed.append(s)
        else:
            if acc5 > 0.55:
                verified.append(s)
            elif acc5 > 0.50:
                partially.append(s)
            else:
                failed.append(s)

    print(f"\n  ✅ 验证通过 (准确率>55%): {len(verified)}个场景")
    for s in verified:
        r = report[s]
        print(f"     {s}: {r['accuracy_5d']:.1%} ({r['n']}样本) — {r.get('desc', '')}")

    print(f"\n  ⚠️  部分成立 (50-55%): {len(partially)}个场景")
    for s in partially:
        r = report[s]
        print(f"     {s}: {r['accuracy_5d']:.1%} ({r['n']}样本) — {r.get('desc', '')}")

    print(f"\n  ❌ 未验证 (<50%): {len(failed)}个场景")
    for s in failed:
        r = report[s]
        print(f"     {s}: {r['accuracy_5d']:.1%} ({r['n']}样本) — {r.get('desc', '')}")

    # 口诀验证
    print(f"\n{'─' * 70}")
    print("🔑 口诀验证: 低位看启动 / 高位看损耗 / 横盘看变量")
    print(f"{'─' * 70}")
    for mantra, data in mantra_report.items():
        best = data.get('best')
        if best and best in report:
            r = report[best]
            print(f"  {mantra}: 最佳场景 = {best}")
            print(f"    → 5日均收益 {r['avg_return_5d']:+.2f}%, 准确率 {r['accuracy_5d']:.1%}")

    print(f"\n⏱️  总耗时: {time.time() - t0:.1f}秒")
    print(f"📁 完整报告: {output_path}")
    print("=" * 70)

    return full_report


if __name__ == '__main__':
    run_backtest()
