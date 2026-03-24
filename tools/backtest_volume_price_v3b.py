#!/usr/bin/env python3
"""
量价关系 V3b — 在V3基础上叠加波动率过滤 + 更精细的条件组合
==========================================================
V3发现：
  - 看跌6分=60.5%，看跌5分=57.1%（3折全部>55%）
  - 高波动股票的看跌准确率更高（V2中高波动+放量下跌=62%）
  - 看涨方向折2偏低，需要更强的过滤

V3b策略：
  1. 在V3评分基础上叠加波动率过滤（只看高波动股）
  2. 加入10日/20日收益率动量条件
  3. 加入换手率异常条件
  4. 测试不同评分阈值 × 波动率组合

用法：
    source .venv/bin/activate
    python -m tools.backtest_volume_price_v3b
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)
OUTPUT_DIR = Path(__file__).parent.parent / "data_results"


def _f(v):
    try:
        return float(v) if v is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


def load_all_stock_codes():
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT stock_code FROM stock_kline "
        "WHERE stock_code NOT LIKE '4%%' AND stock_code NOT LIKE '8%%' "
        "AND stock_code NOT LIKE '9%%' ORDER BY stock_code")
    codes = [r['stock_code'] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return codes


def load_kline_batch(stock_codes, start_date, end_date):
    result = defaultdict(list)
    bs = 200  # smaller batch to avoid connection timeout
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        for attempt in range(3):
            try:
                conn = get_connection(use_dict_cursor=True)
                cur = conn.cursor()
                cur.execute(
                    f"SELECT stock_code, `date`, close_price, open_price, high_price, "
                    f"low_price, trading_volume, change_percent, change_hand "
                    f"FROM stock_kline WHERE stock_code IN ({ph}) "
                    f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
                    batch + [start_date, end_date])
                for row in cur.fetchall():
                    result[row['stock_code']].append({
                        'd': str(row['date']),
                        'c': _f(row['close_price']),
                        'o': _f(row['open_price']),
                        'h': _f(row['high_price']),
                        'l': _f(row['low_price']),
                        'v': _f(row['trading_volume']),
                        'p': _f(row['change_percent']),
                        't': _f(row.get('change_hand')),
                    })
                cur.close()
                conn.close()
                break
            except Exception as e:
                logger.warning("  批次%d/%d 第%d次重试: %s", i//bs+1, len(stock_codes)//bs+1, attempt+1, e)
                time.sleep(2)
        if i % 2000 == 0 and i > 0:
            logger.info("  已加载 %d/%d 批次, %d只股票", i//bs, len(stock_codes)//bs+1, len(result))
    return dict(result)


# ═══════════════════════════════════════════════════════════════
# 全量特征计算（一次计算，多次过滤）
# ═══════════════════════════════════════════════════════════════

def compute_all_features(klines, idx):
    """计算idx位置的全部特征，返回dict或None"""
    hist = klines[:idx + 1]
    n = len(hist)
    if n < 60:
        return None

    c = [k['c'] for k in hist]
    v = [k['v'] for k in hist]
    p = [k['p'] for k in hist]
    h = [k['h'] for k in hist]
    l = [k['l'] for k in hist]
    o = [k['o'] for k in hist]
    t = [k['t'] for k in hist]

    if c[-1] <= 0 or v[-1] <= 0:
        return None

    # 位置
    valid_h = [x for x in h[-60:] if x > 0]
    valid_l = [x for x in l[-60:] if x > 0]
    if not valid_h or not valid_l:
        return None
    h60, l60 = max(valid_h), min(valid_l)
    if h60 <= l60:
        return None
    pos_pct = (c[-1] - l60) / (h60 - l60)

    # 均线
    ma5 = sum(c[-5:]) / 5
    ma10 = sum(c[-10:]) / 10
    ma20 = sum(c[-20:]) / 20
    ma60 = sum(c[-60:]) / 60

    # 成交量
    vol_20 = sum(v[-20:]) / 20
    vol_5 = sum(v[-5:]) / 5
    vol_3 = sum(v[-3:]) / 3
    if vol_20 <= 0:
        return None
    vol_ratio_5 = vol_5 / vol_20
    vol_ratio_3 = vol_3 / vol_20

    # 换手率
    turn_5 = sum(t[-5:]) / 5
    turn_20 = sum(t[-20:]) / 20
    turn_ratio = turn_5 / turn_20 if turn_20 > 0 else 1

    # 收益率
    ret_1d = p[-1]
    ret_3d = (c[-1] / c[-4] - 1) * 100 if c[-4] > 0 else 0
    ret_5d = (c[-1] / c[-6] - 1) * 100 if n >= 6 and c[-6] > 0 else 0
    ret_10d = (c[-1] / c[-11] - 1) * 100 if n >= 11 and c[-11] > 0 else 0
    ret_20d = (c[-1] / c[-21] - 1) * 100 if n >= 21 and c[-21] > 0 else 0

    # 波动率
    rets_20 = [(c[i] / c[i-1] - 1) * 100 for i in range(n-20, n) if c[i-1] > 0]
    if rets_20:
        mean_r = sum(rets_20) / len(rets_20)
        volatility = (sum((r - mean_r)**2 for r in rets_20) / len(rets_20)) ** 0.5
    else:
        volatility = 0

    # 连涨/连跌
    consec_up = 0
    consec_down = 0
    for i in range(n - 1, max(n - 15, 0), -1):
        if p[i] > 0:
            if consec_down == 0:
                consec_up += 1
            else:
                break
        elif p[i] < 0:
            if consec_up == 0:
                consec_down += 1
            else:
                break
        else:
            break

    # K线形态
    upper_shadows = 0
    lower_shadows = 0
    for i in range(-5, 0):
        body = abs(c[i] - o[i])
        if body > 0:
            upper = h[i] - max(c[i], o[i]) if h[i] > 0 else 0
            lower = min(c[i], o[i]) - l[i] if l[i] > 0 else 0
            if upper > body * 1.5:
                upper_shadows += 1
            if lower > body * 1.5:
                lower_shadows += 1

    # 量能趋势
    vol_expanding = all(v[-i] >= v[-i-1] * 0.85 for i in range(1, 4) if v[-i-1] > 0)
    vol_contracting = all(v[-i] <= v[-i-1] * 1.15 for i in range(1, 4) if v[-i-1] > 0)

    # 近5日最大单日涨跌
    max_up_1d = max(p[-5:])
    max_dn_1d = min(p[-5:])

    # 价格相对MA20的偏离度
    ma20_dev = (c[-1] / ma20 - 1) * 100 if ma20 > 0 else 0

    return {
        'pos': pos_pct,
        'ma5': ma5, 'ma10': ma10, 'ma20': ma20, 'ma60': ma60,
        'ma_bull': ma5 > ma20 > ma60,
        'ma_bear': ma5 < ma20 < ma60,
        'vr5': vol_ratio_5, 'vr3': vol_ratio_3,
        'tr': turn_ratio, 't5': turn_5,
        'r1': ret_1d, 'r3': ret_3d, 'r5': ret_5d, 'r10': ret_10d, 'r20': ret_20d,
        'vol': volatility,
        'cup': consec_up, 'cdn': consec_down,
        'ush': upper_shadows, 'lsh': lower_shadows,
        'vexp': vol_expanding, 'vcon': vol_contracting,
        'mu1': max_up_1d, 'md1': max_dn_1d,
        'ma20d': ma20_dev,
    }


# ═══════════════════════════════════════════════════════════════
# 信号规则定义（每条规则是一个独立的过滤条件组合）
# ═══════════════════════════════════════════════════════════════

RULES = {
    # ── 看跌规则 ──
    'BEAR_A_高位放量连涨过热': {
        'dir': 'DOWN',
        'filter': lambda f: (
            f['pos'] >= 0.67          # 高位
            and f['vr5'] >= 1.5       # 放量
            and f['r5'] > 3           # 近5日涨
            and f['cup'] >= 3         # 连涨≥3日
        ),
    },
    'BEAR_B_高位放量连涨过热_高波动': {
        'dir': 'DOWN',
        'filter': lambda f: (
            f['pos'] >= 0.67
            and f['vr5'] >= 1.5
            and f['r5'] > 3
            and f['cup'] >= 3
            and f['vol'] > 3          # 高波动
        ),
    },
    'BEAR_C_高位放量连涨_上影线': {
        'dir': 'DOWN',
        'filter': lambda f: (
            f['pos'] >= 0.67
            and f['vr5'] >= 1.5
            and f['r5'] > 3
            and f['cup'] >= 3
            and f['ush'] >= 1         # 有上影线
        ),
    },
    'BEAR_D_高位放量连涨_高波动_上影线': {
        'dir': 'DOWN',
        'filter': lambda f: (
            f['pos'] >= 0.67
            and f['vr5'] >= 1.5
            and f['r5'] > 3
            and f['cup'] >= 3
            and f['vol'] > 3
            and f['ush'] >= 1
        ),
    },
    'BEAR_E_高位放量大涨_高波动': {
        'dir': 'DOWN',
        'filter': lambda f: (
            f['pos'] >= 0.67
            and f['vr5'] >= 1.5
            and f['r5'] > 5           # 大涨>5%
            and f['vol'] > 3
        ),
    },
    'BEAR_F_高位放量_偏离MA20大': {
        'dir': 'DOWN',
        'filter': lambda f: (
            f['pos'] >= 0.67
            and f['vr5'] >= 1.5
            and f['ma20d'] > 10       # 偏离MA20超过10%
        ),
    },
    'BEAR_G_高位放量连涨_换手激增': {
        'dir': 'DOWN',
        'filter': lambda f: (
            f['pos'] >= 0.67
            and f['vr5'] >= 1.5
            and f['cup'] >= 3
            and f['tr'] > 1.5         # 换手率激增
        ),
    },
    'BEAR_H_极端过热': {
        'dir': 'DOWN',
        'filter': lambda f: (
            f['pos'] >= 0.75          # 极高位
            and f['vr5'] >= 2.0       # 大幅放量
            and f['r5'] > 5           # 大涨
            and f['cup'] >= 3
            and f['vol'] > 3
        ),
    },
    'BEAR_I_高位放量连涨_均线空头': {
        'dir': 'DOWN',
        'filter': lambda f: (
            f['pos'] >= 0.67
            and f['vr5'] >= 1.5
            and f['r5'] > 3
            and f['cup'] >= 3
            and f['ma_bear']          # 均线空头排列
        ),
    },
    'BEAR_J_高位放量_单日暴涨': {
        'dir': 'DOWN',
        'filter': lambda f: (
            f['pos'] >= 0.67
            and f['vr5'] >= 1.5
            and f['mu1'] > 7          # 单日暴涨>7%
            and f['vol'] > 2.5
        ),
    },
    'BEAR_K_极端过热_上影线': {
        'dir': 'DOWN',
        'filter': lambda f: (
            f['pos'] >= 0.75
            and f['vr5'] >= 2.0
            and f['r5'] > 5
            and f['cup'] >= 3
            and f['vol'] > 3
            and f['ush'] >= 1
        ),
    },
    'BEAR_L_极端过热_偏离MA20': {
        'dir': 'DOWN',
        'filter': lambda f: (
            f['pos'] >= 0.75
            and f['vr5'] >= 2.0
            and f['r5'] > 5
            and f['cup'] >= 3
            and f['ma20d'] > 10
        ),
    },
    'BEAR_M_高位放量大涨_高波动_上影线': {
        'dir': 'DOWN',
        'filter': lambda f: (
            f['pos'] >= 0.67
            and f['vr5'] >= 1.5
            and f['r5'] > 5
            and f['vol'] > 3
            and f['ush'] >= 1
        ),
    },
    'BEAR_N_高位放量_偏离MA20_高波动': {
        'dir': 'DOWN',
        'filter': lambda f: (
            f['pos'] >= 0.67
            and f['vr5'] >= 1.5
            and f['ma20d'] > 10
            and f['vol'] > 3
        ),
    },
    'BEAR_O_高位放量连涨_偏离MA20': {
        'dir': 'DOWN',
        'filter': lambda f: (
            f['pos'] >= 0.67
            and f['vr5'] >= 1.5
            and f['cup'] >= 3
            and f['ma20d'] > 8
            and f['vol'] > 3
        ),
    },
    'BEAR_P_高位大幅放量_高波动_换手激增': {
        'dir': 'DOWN',
        'filter': lambda f: (
            f['pos'] >= 0.67
            and f['vr5'] >= 2.0
            and f['vol'] > 3
            and f['tr'] > 1.5
        ),
    },

    # ── 看涨规则 ──
    'BULL_A_低位缩量连跌': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.33
            and f['vr5'] <= 0.7
            and f['r5'] < -3
            and f['cdn'] >= 3
        ),
    },
    'BULL_B_低位缩量连跌_低波动': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.33
            and f['vr5'] <= 0.7
            and f['r5'] < -3
            and f['cdn'] >= 3
            and f['vol'] < 2.5        # 低/中波动
        ),
    },
    'BULL_C_低位缩量连跌_下影线': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.33
            and f['vr5'] <= 0.7
            and f['r5'] < -3
            and f['cdn'] >= 3
            and f['lsh'] >= 1
        ),
    },
    'BULL_D_低位缩量连跌_MA20走平': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.33
            and f['vr5'] <= 0.7
            and f['r5'] < -3
            and f['cdn'] >= 3
            and abs(f['ma20d']) < 5   # 接近MA20
        ),
    },
    'BULL_E_低位缩量_20日大跌': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.33
            and f['vr5'] <= 0.7
            and f['r20'] < -10        # 20日跌幅>10%
        ),
    },
    'BULL_F_低位缩量连跌_量能萎缩': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.33
            and f['vr5'] <= 0.7
            and f['cdn'] >= 3
            and f['vcon']             # 量能持续萎缩
        ),
    },
    'BULL_G_极端超跌': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.2           # 极低位
            and f['vr5'] <= 0.6       # 深度缩量
            and f['r5'] < -5          # 大跌
            and f['cdn'] >= 3
        ),
    },
    'BULL_H_低位缩量_偏离MA20大': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.33
            and f['vr5'] <= 0.7
            and f['ma20d'] < -8       # 大幅低于MA20
        ),
    },
    'BULL_I_低位缩量连跌_低换手': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.33
            and f['vr5'] <= 0.7
            and f['cdn'] >= 3
            and f['t5'] < 2           # 低换手率
        ),
    },
    'BULL_J_极端超跌_偏离MA20': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.2
            and f['vr5'] <= 0.6
            and f['r5'] < -5
            and f['ma20d'] < -8
        ),
    },
    'BULL_K_低位缩量_偏离MA20_下影线': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.33
            and f['vr5'] <= 0.7
            and f['ma20d'] < -8
            and f['lsh'] >= 1
        ),
    },
    'BULL_L_低位缩量_20日大跌_下影线': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.33
            and f['vr5'] <= 0.7
            and f['r20'] < -10
            and f['lsh'] >= 1
        ),
    },
    'BULL_M_低位缩量_偏离MA20_低波动': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.33
            and f['vr5'] <= 0.7
            and f['ma20d'] < -8
            and f['vol'] < 2.5
        ),
    },
    'BULL_N_极端超跌_下影线': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.2
            and f['vr5'] <= 0.6
            and f['r5'] < -5
            and f['cdn'] >= 3
            and f['lsh'] >= 1
        ),
    },
    'BULL_O_低位缩量_偏离MA20_连跌': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.33
            and f['vr5'] <= 0.7
            and f['ma20d'] < -8
            and f['cdn'] >= 3
        ),
    },
    'BULL_P_低位深度缩量_20日大跌': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.25
            and f['vr5'] <= 0.6
            and f['r20'] < -15
        ),
    },
}


# ═══════════════════════════════════════════════════════════════
# 统计 + 主回测
# ═══════════════════════════════════════════════════════════════

def calc_stats(rets, direction):
    if not rets:
        return None
    n = len(rets)
    if direction == 'UP':
        correct = sum(1 for r in rets if r > 0)
    else:
        correct = sum(1 for r in rets if r < 0)
    acc = correct / n
    avg = sum(rets) / n
    med = sorted(rets)[n // 2]
    if direction == 'UP':
        wins = [r for r in rets if r > 0]
        losses = [-r for r in rets if r < 0]
    else:
        wins = [-r for r in rets if r < 0]
        losses = [r for r in rets if r > 0]
    aw = sum(wins) / len(wins) if wins else 0
    al = sum(losses) / len(losses) if losses else 0.001
    plr = round(aw / al, 2) if al > 0 else 99
    return {'n': n, 'acc': round(acc, 4), 'avg': round(avg, 2), 'med': round(med, 2), 'plr': plr}


def run_v3b():
    t0 = time.time()
    print("=" * 90)
    print("量价关系 V3b — 精细条件组合 + 全量数据 + 3折交叉验证")
    print("=" * 90)

    logger.info("[1/3] 加载全量数据...")
    all_codes = load_all_stock_codes()
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=450)).strftime('%Y-%m-%d')
    kline_data = load_kline_batch(all_codes, start_date, end_date)
    logger.info("  %d只股票加载完成", len(kline_data))

    logger.info("[2/3] 扫描全部规则...")
    # 为每条规则收集记录
    rule_records = {name: [] for name in RULES}
    total = 0

    for code, klines in kline_data.items():
        if len(klines) < 80:
            continue
        for i in range(60, len(klines) - 10):
            total += 1
            feat = compute_all_features(klines, i)
            if feat is None:
                continue

            # 计算未来收益（只算一次）
            base_c = klines[i]['c']
            if base_c <= 0:
                continue
            r5 = round((klines[i + 5]['c'] / base_c - 1) * 100, 2) if i + 5 < len(klines) and klines[i + 5]['c'] > 0 else None
            r10 = round((klines[i + 10]['c'] / base_c - 1) * 100, 2) if i + 10 < len(klines) and klines[i + 10]['c'] > 0 else None
            if r5 is None:
                continue

            date_str = klines[i]['d']
            month = date_str[:7]

            for name, rule in RULES.items():
                try:
                    if rule['filter'](feat):
                        rule_records[name].append({
                            'date': date_str,
                            'month': month,
                            'code': code,
                            'r5': r5,
                            'r10': r10,
                        })
                except Exception:
                    pass

    logger.info("  扫描%d日完成", total)

    # ── 3折验证 ──
    logger.info("[3/3] 3折交叉验证...")
    all_months = sorted(set(
        r['month'] for recs in rule_records.values() for r in recs
    ))
    nm = len(all_months)
    fs = nm // 3
    folds = [set(all_months[:fs]), set(all_months[fs:2*fs]), set(all_months[2*fs:])]
    fold_labels = [f"{all_months[0]}~{all_months[fs-1]}",
                   f"{all_months[fs]}~{all_months[2*fs-1]}",
                   f"{all_months[2*fs]}~{all_months[-1]}"]

    print(f"\n数据: {len(kline_data)}只股票, {start_date}~{end_date}, 扫描{total}日")
    print(f"3折: {' | '.join(fold_labels)}")

    print(f"\n{'─' * 115}")
    print(f"  {'规则':<40s} {'方向':>4s} {'样本':>7s} {'全量':>6s} "
          f"{'折1':>6s} {'折2':>6s} {'折3':>6s} {'最低':>6s} "
          f"{'均收益':>7s} {'盈亏比':>6s} {'月胜率':>6s} {'判定':>4s}")
    print(f"  {'─' * 112}")

    report = {}
    for name in sorted(RULES.keys()):
        recs = rule_records[name]
        direction = RULES[name]['dir']
        rets_5 = [r['r5'] for r in recs]
        s = calc_stats(rets_5, direction)
        if not s or s['n'] < 30:
            continue

        # 3折
        fold_accs = []
        for fold_months in folds:
            fr = [r['r5'] for r in recs if r['month'] in fold_months]
            fs_stat = calc_stats(fr, direction)
            fold_accs.append(fs_stat['acc'] if fs_stat and fs_stat['n'] >= 10 else None)

        valid_folds = [a for a in fold_accs if a is not None]
        min_fold = min(valid_folds) if valid_folds else 0

        # 月度一致性
        monthly = defaultdict(list)
        for r in recs:
            monthly[r['month']].append(r['r5'])
        m_accs = []
        for month, mrs in monthly.items():
            ms = calc_stats(mrs, direction)
            if ms and ms['n'] >= 10:
                m_accs.append(ms['acc'])
        m_win = sum(1 for a in m_accs if a > 0.5) / len(m_accs) if m_accs else 0

        # 判定
        passed = s['acc'] >= 0.65 and min_fold >= 0.55 and m_win >= 0.6
        good = s['acc'] >= 0.60 and min_fold >= 0.52
        verdict = '✅' if passed else ('⚠️' if good else '❌')

        fa = [f"{a:.1%}" if a is not None else " N/A" for a in fold_accs]

        print(f"  {name:<40s} {direction:>4s} {s['n']:>7d} {s['acc']:>6.1%} "
              f"{fa[0]:>6s} {fa[1]:>6s} {fa[2]:>6s} {min_fold:>6.1%} "
              f"{s['avg']:>+7.2f}% {s['plr']:>6.1f} {m_win:>6.0%} {verdict:>4s}")

        report[name] = {
            'dir': direction, 'n': s['n'], 'acc': s['acc'],
            'avg': s['avg'], 'med': s['med'], 'plr': s['plr'],
            'fold_accs': fold_accs, 'min_fold': round(min_fold, 4),
            'monthly_win_rate': round(m_win, 3),
            'verdict': verdict,
        }

    # ── 最终推荐 ──
    print(f"\n{'═' * 115}")
    print("📋 最终推荐")
    print(f"{'═' * 115}")

    passed_rules = [(n, d) for n, d in report.items() if d['verdict'] == '✅']
    if passed_rules:
        for name, data in sorted(passed_rules, key=lambda x: x[1]['acc'], reverse=True):
            print(f"\n  ✅ {name}")
            print(f"     方向: {data['dir']} | 准确率: {data['acc']:.1%} | 样本: {data['n']} | "
                  f"均收益: {data['avg']:+.2f}% | 盈亏比: {data['plr']:.1f}")
            print(f"     3折: {data['fold_accs']} | 最低折: {data['min_fold']:.1%} | "
                  f"月胜率: {data['monthly_win_rate']:.0%}")
    else:
        print("\n  无规则通过全部条件(≥65%+最低折≥55%+月胜率≥60%)，最接近的：")
        top5 = sorted(report.items(), key=lambda x: x[1]['acc'], reverse=True)[:8]
        for name, data in top5:
            print(f"  {'⚠️' if data['verdict']=='⚠️' else '❌'} {name}: "
                  f"{data['acc']:.1%} (n={data['n']}, 最低折={data['min_fold']:.1%}, "
                  f"月胜率={data['monthly_win_rate']:.0%})")

    output_path = OUTPUT_DIR / "volume_price_v3b_backtest.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump({'meta': report.get('meta', {}), 'rules': report}, f,
                  ensure_ascii=False, indent=2, default=str)

    print(f"\n⏱️  耗时: {time.time() - t0:.1f}秒")
    print(f"📁 报告: {output_path}")
    print("=" * 90)
    return report


if __name__ == '__main__':
    run_v3b()
