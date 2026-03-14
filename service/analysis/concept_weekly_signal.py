"""
概念板块周预测信号服务

基于DB中的概念板块K线数据，为个股周预测提供概念维度的辅助信号。

信号维度：
1. concept_momentum: 个股所属概念板块的近N日平均涨跌幅（动量）
2. concept_consensus: 看涨概念板块占比（共识度）
3. concept_strength: 概念板块相对大盘的强弱（强势度）
4. concept_peer_trend: 概念板块内同行股票的近期走势

使用场景：
- 策略B（周一收盘后）：模糊区（周一涨跌±0.5%）用概念信号辅助判断
- 策略C（周三收盘后）：边界区（前3天涨跌±1%）用概念信号修正方向
"""
import logging
from collections import defaultdict
from decimal import Decimal

from dao import get_connection

logger = logging.getLogger(__name__)


def _to_float(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    return float(v)


def get_stock_concept_boards(stock_code: str) -> list[dict]:
    """查询个股所属的所有概念板块（含板块K线数据可用性）。

    Returns:
        [{"board_code", "board_name"}, ...]
    """
    code_6 = stock_code.split('.')[0] if '.' in stock_code else stock_code
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT board_code, board_name FROM stock_concept_board_stock "
            "WHERE stock_code = %s ORDER BY board_code",
            (code_6,)
        )
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def get_concept_board_kline_map(
    board_codes: list[str],
    start_date: str,
    end_date: str,
) -> dict[str, list[dict]]:
    """批量查询多个概念板块的K线数据。

    Returns:
        {board_code: [{"date", "change_percent", "close_price", ...}, ...]}
        K线按日期升序排列。
    """
    if not board_codes:
        return {}

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        placeholders = ','.join(['%s'] * len(board_codes))
        cur.execute(
            f"SELECT board_code, `date`, change_percent, close_price, "
            f"trading_volume, trading_amount "
            f"FROM concept_board_kline "
            f"WHERE board_code IN ({placeholders}) "
            f"AND `date` >= %s AND `date` <= %s "
            f"ORDER BY board_code, `date` ASC",
            (*board_codes, start_date, end_date),
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    result = defaultdict(list)
    for r in rows:
        result[r['board_code']].append({
            'date': r['date'],
            'change_percent': _to_float(r['change_percent']),
            'close_price': _to_float(r['close_price']),
        })
    return dict(result)


def get_concept_peer_klines(
    board_codes: list[str],
    exclude_code: str,
    start_date: str,
    end_date: str,
    max_peers_per_board: int = 20,
) -> dict[str, dict[str, list]]:
    """查询概念板块内同行股票的K线涨跌数据。

    Returns:
        {board_code: {peer_code: [{"date", "change_percent"}, ...]}}
    """
    if not board_codes:
        return {}

    exclude_6 = exclude_code.split('.')[0] if '.' in exclude_code else exclude_code
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        # 先查每个板块的成分股
        placeholders = ','.join(['%s'] * len(board_codes))
        cur.execute(
            f"SELECT board_code, stock_code FROM stock_concept_board_stock "
            f"WHERE board_code IN ({placeholders}) AND stock_code != %s",
            (*board_codes, exclude_6),
        )
        board_members = defaultdict(list)
        for r in cur.fetchall():
            board_members[r['board_code']].append(r['stock_code'])

        # 限制每个板块的同行数量
        all_peer_codes = set()
        for bc, members in board_members.items():
            members_limited = members[:max_peers_per_board]
            board_members[bc] = members_limited
            all_peer_codes.update(members_limited)

        if not all_peer_codes:
            return {}

        # 批量查询同行K线
        peer_list = list(all_peer_codes)
        # 转换为带后缀的格式查询stock_kline表
        peer_placeholders = ','.join(['%s'] * len(peer_list))
        # stock_kline表中stock_code可能是6位或带后缀，尝试两种
        cur.execute(
            f"SELECT stock_code, `date`, change_percent "
            f"FROM stock_kline "
            f"WHERE stock_code IN ({peer_placeholders}) "
            f"AND `date` >= %s AND `date` <= %s "
            f"ORDER BY stock_code, `date` ASC",
            (*peer_list, start_date, end_date),
        )
        peer_kline_rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    # 组织为 peer_code -> [{date, change_percent}]
    peer_kline_map = defaultdict(list)
    for r in peer_kline_rows:
        code = r['stock_code']
        peer_kline_map[code].append({
            'date': r['date'],
            'change_percent': _to_float(r['change_percent']),
        })

    # 组织为 board_code -> {peer_code -> [...]}
    result = {}
    for bc, members in board_members.items():
        board_peers = {}
        for pc in members:
            if pc in peer_kline_map and len(peer_kline_map[pc]) >= 10:
                board_peers[pc] = peer_kline_map[pc]
        if board_peers:
            result[bc] = board_peers
    return result


def compute_concept_signal_for_date(
    stock_code: str,
    score_date: str,
    board_kline_map: dict[str, list[dict]],
    stock_boards: list[dict],
    lookback: int = 5,
) -> dict | None:
    """计算某只股票在某个日期的概念板块信号。

    基于DB中的概念板块K线数据（而非代理信号），计算：
    - concept_momentum: 所属概念板块近lookback日平均涨跌幅
    - concept_consensus: 看涨概念板块占比
    - concept_strength: 概念板块平均强度（近期累计涨跌）
    - n_boards: 有效概念板块数

    Args:
        stock_code: 股票代码
        score_date: 评分日期
        board_kline_map: {board_code: [{date, change_percent, ...}]}
        stock_boards: [{board_code, board_name}]
        lookback: 回看天数

    Returns:
        信号字典，无数据返回 None
    """
    if not stock_boards:
        return None

    board_momentums = []
    board_strengths = []
    boards_up = 0
    boards_total = 0
    board_details = []

    for board in stock_boards:
        bc = board['board_code']
        klines = board_kline_map.get(bc, [])
        if not klines:
            continue

        # 找到 score_date 及之前的K线
        valid_klines = [k for k in klines if k['date'] <= score_date]
        if len(valid_klines) < 3:
            continue

        # 取最近 lookback 天
        recent = valid_klines[-lookback:]
        if not recent:
            continue

        boards_total += 1
        avg_chg = sum(k['change_percent'] for k in recent) / len(recent)
        cum_chg = sum(k['change_percent'] for k in recent)

        board_momentums.append(avg_chg)
        board_strengths.append(cum_chg)

        if avg_chg > 0:
            boards_up += 1

        board_details.append({
            'board_code': bc,
            'board_name': board['board_name'],
            'momentum': avg_chg,
            'cum_return': cum_chg,
        })

    if boards_total == 0:
        return None

    return {
        'concept_momentum': sum(board_momentums) / len(board_momentums),
        'concept_consensus': boards_up / boards_total,
        'concept_strength': sum(board_strengths) / len(board_strengths),
        'n_boards': boards_total,
        'board_details': board_details,
    }


def batch_preload_concept_data(
    stock_codes: list[str],
    start_date: str,
    end_date: str,
) -> dict:
    """批量预加载所有股票的概念板块数据（单次DB查询优化）。

    Returns:
        {
            "stock_boards": {stock_code: [board_info, ...]},
            "board_kline_map": {board_code: [kline, ...]},
        }
    """
    from datetime import datetime, timedelta

    # 1. 批量查询所有股票的概念板块（单次查询）
    codes_6 = [c.split('.')[0] if '.' in c else c for c in stock_codes]
    code_to_full = {}
    for full, c6 in zip(stock_codes, codes_6):
        code_to_full[c6] = full

    stock_boards = defaultdict(list)
    all_board_codes = set()

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        if codes_6:
            placeholders = ','.join(['%s'] * len(codes_6))
            cur.execute(
                f"SELECT stock_code, board_code, board_name "
                f"FROM stock_concept_board_stock "
                f"WHERE stock_code IN ({placeholders}) "
                f"ORDER BY stock_code, board_code",
                tuple(codes_6),
            )
            for r in cur.fetchall():
                full_code = code_to_full.get(r['stock_code'], r['stock_code'])
                stock_boards[full_code].append({
                    'board_code': r['board_code'],
                    'board_name': r['board_name'],
                })
                all_board_codes.add(r['board_code'])
    finally:
        cur.close()
        conn.close()

    stock_boards = dict(stock_boards)
    logger.info("[概念周信号] %d/%d 只股票有概念板块数据, 涉及 %d 个板块",
                len(stock_boards), len(stock_codes), len(all_board_codes))

    # 2. 批量查询板块K线（扩展start_date往前45天以支持lookback）
    dt = datetime.strptime(start_date, '%Y-%m-%d')
    extended_start = (dt - timedelta(days=45)).strftime('%Y-%m-%d')

    board_kline_map = get_concept_board_kline_map(
        list(all_board_codes), extended_start, end_date
    )
    n_with_kline = sum(1 for bc in all_board_codes if bc in board_kline_map)
    logger.info("[概念周信号] %d/%d 个板块有K线数据",
                n_with_kline, len(all_board_codes))

    return {
        'stock_boards': stock_boards,
        'board_kline_map': board_kline_map,
    }


# ══════════════════════════════════════════════════════════════
# 周预测策略增强函数
# ══════════════════════════════════════════════════════════════

def predict_weekly_B_with_concept(
    mon_actual: float,
    sector_up_rate: float,
    concept_signal: dict | None,
) -> tuple[bool, str]:
    """策略B增强版：周一收盘后 + 概念板块信号。

    决策逻辑：
    1. 强信号区（|周一涨跌| > 0.5%）：跟随周一方向，但概念极端反向时修正
    2. 模糊区（|周一涨跌| ≤ 0.5%）：优先用概念共识度，无概念数据时用板块基准率

    Args:
        mon_actual: 周一实际涨跌幅(%)
        sector_up_rate: 板块历史周涨概率
        concept_signal: 概念信号字典（可为None）

    Returns:
        (pred_up, reason): 预测方向和理由
    """
    has_concept = concept_signal is not None and concept_signal.get('n_boards', 0) >= 2

    # 概念综合评分
    cs = 0
    if has_concept:
        consensus = concept_signal['concept_consensus']
        momentum = concept_signal['concept_momentum']
        strength = concept_signal['concept_strength']

        if consensus > 0.65:
            cs += 1
        elif consensus < 0.35:
            cs -= 1
        if momentum > 0.3:
            cs += 1
        elif momentum < -0.3:
            cs -= 1
        if strength > 1.5:
            cs += 1
        elif strength < -1.5:
            cs -= 1

    # 强看涨区
    if mon_actual > 0.5:
        if has_concept and cs <= -2:
            return False, f"周一涨{mon_actual:+.2f}%但概念极弱(cs={cs})→反转看跌"
        return True, f"周一涨{mon_actual:+.2f}%→看涨"

    # 强看跌区
    if mon_actual < -0.5:
        if has_concept and cs >= 2:
            return True, f"周一跌{mon_actual:+.2f}%但概念极强(cs={cs})→反弹看涨"
        return False, f"周一跌{mon_actual:+.2f}%→看跌"

    # 模糊区：概念信号优先
    if has_concept:
        if cs > 0:
            return True, f"模糊区+概念看涨(cs={cs},共识{concept_signal['concept_consensus']:.0%})"
        elif cs < 0:
            return False, f"模糊区+概念看跌(cs={cs},共识{concept_signal['concept_consensus']:.0%})"
        # cs==0 时用概念共识度
        if concept_signal['concept_consensus'] > 0.5:
            return True, f"模糊区+概念共识偏涨({concept_signal['concept_consensus']:.0%})"
        elif concept_signal['concept_consensus'] < 0.5:
            return False, f"模糊区+概念共识偏跌({concept_signal['concept_consensus']:.0%})"

    # 兜底：板块基准率
    pred_up = sector_up_rate > 0.5
    return pred_up, f"模糊区+板块基准率({sector_up_rate:.0%})→{'涨' if pred_up else '跌'}"


def predict_weekly_C_with_concept(
    d3_chg: float,
    concept_signal_wed: dict | None,
) -> tuple[bool, str]:
    """策略C增强版：周三收盘后 + 概念板块信号。

    决策逻辑：
    1. 强信号区（|前3天涨跌| > 1.5%）：跟随前3天方向
    2. 中等信号区（0.5% < |前3天涨跌| ≤ 1.5%）：概念极端反向时修正
    3. 模糊区（|前3天涨跌| ≤ 0.5%）：用概念信号决定方向

    Args:
        d3_chg: 前3天累计涨跌幅(%)
        concept_signal_wed: 周三的概念信号字典（可为None）

    Returns:
        (pred_up, reason): 预测方向和理由
    """
    has_concept = (concept_signal_wed is not None
                   and concept_signal_wed.get('n_boards', 0) >= 2)

    cs = 0
    if has_concept:
        consensus = concept_signal_wed['concept_consensus']
        momentum = concept_signal_wed['concept_momentum']

        if consensus > 0.6:
            cs += 1
        elif consensus < 0.4:
            cs -= 1
        if momentum > 0.2:
            cs += 1
        elif momentum < -0.2:
            cs -= 1

    # 强信号区
    if abs(d3_chg) > 1.5:
        return d3_chg > 0, f"前3天{d3_chg:+.2f}%(强信号)→{'涨' if d3_chg > 0 else '跌'}"

    # 中等信号区
    if abs(d3_chg) > 0.5:
        pred_up = d3_chg > 0
        if has_concept:
            if pred_up and cs <= -2:
                return False, f"前3天涨{d3_chg:+.2f}%但概念极弱(cs={cs})→反转看跌"
            if not pred_up and cs >= 2:
                return True, f"前3天跌{d3_chg:+.2f}%但概念极强(cs={cs})→反弹看涨"
        return pred_up, f"前3天{d3_chg:+.2f}%→{'涨' if pred_up else '跌'}"

    # 模糊区
    if has_concept:
        if cs > 0:
            return True, f"前3天模糊({d3_chg:+.2f}%)+概念看涨(cs={cs})"
        elif cs < 0:
            return False, f"前3天模糊({d3_chg:+.2f}%)+概念看跌(cs={cs})"

    # 兜底：跟随前3天方向
    return d3_chg > 0, f"前3天{d3_chg:+.2f}%→{'涨' if d3_chg > 0 else '跌'}"
