#!/usr/bin/env python3
"""
概念板块增强日预测回测 v1

在 prediction_enhanced_backtest 基础上，新增概念板块维度因子：
1. concept_board_momentum: 个股所属概念板块的近N日平均动量
2. concept_board_consensus: 看涨概念板块占比（共识度）
3. concept_stock_excess: 个股相对概念板块的超额收益（强弱势）
4. concept_peer_momentum: 概念板块内同行股票的近期走势
5. concept_board_vs_market: 概念板块相对大盘的强弱

核心思路：
- 概念板块是比行业板块更细粒度的分类，一只股票可属于多个概念板块
- 当多个概念板块同时走强时，个股上涨概率更高（共识度信号）
- 个股在概念板块中的相对强弱可以预测短期动量延续/反转
- 概念板块相对大盘的强弱可以判断资金流向

目标：日预测准确率（宽松）≥ 65%
"""

import asyncio
import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal

from dao import get_connection
from dao.stock_kline_dao import get_kline_data
from common.utils.sector_mapping_utils import parse_industry_list_md, get_sector_peers
from service.eastmoney.indices.us_market_db_query import (
    preload_us_kline_map,
    get_us_overnight_signal_fast,
)

logger = logging.getLogger(__name__)


def _to_float(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    return float(v)


# ═══════════════════════════════════════════════════════════
# 概念板块数据预加载
# ═══════════════════════════════════════════════════════════

def preload_concept_board_data(stock_codes: list[str],
                               start_date: str, end_date: str) -> dict:
    """批量预加载所有股票的概念板块数据 + 板块K线 + 大盘K线。

    Returns:
        {
            "stock_boards": {stock_code_6: [{board_code, board_name}, ...]},
            "board_kline_map": {board_code: [{date, change_percent, close_price, ...}, ...]},
            "market_kline_map": {date: change_percent},
        }
    """
    codes_6 = []
    code_map = {}  # 6位 -> 完整代码
    for c in stock_codes:
        c6 = c.split('.')[0] if '.' in c else c
        codes_6.append(c6)
        code_map[c6] = c

    stock_boards = defaultdict(list)
    all_board_codes = set()

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        # 1. 查询所有股票的概念板块
        if codes_6:
            ph = ','.join(['%s'] * len(codes_6))
            cur.execute(
                f"SELECT stock_code, board_code, board_name "
                f"FROM stock_concept_board_stock "
                f"WHERE stock_code IN ({ph}) ORDER BY stock_code, board_code",
                tuple(codes_6),
            )
            for r in cur.fetchall():
                stock_boards[r['stock_code']].append({
                    'board_code': r['board_code'],
                    'board_name': r['board_name'],
                })
                all_board_codes.add(r['board_code'])

        # 2. 查询板块K线（扩展60天以支持lookback）
        dt = datetime.strptime(start_date, '%Y-%m-%d')
        ext_start = (dt - timedelta(days=90)).strftime('%Y-%m-%d')

        board_kline_map = defaultdict(list)
        if all_board_codes:
            bc_list = list(all_board_codes)
            ph2 = ','.join(['%s'] * len(bc_list))
            cur.execute(
                f"SELECT board_code, `date`, change_percent, close_price, "
                f"trading_volume, trading_amount "
                f"FROM concept_board_kline "
                f"WHERE board_code IN ({ph2}) AND `date` >= %s AND `date` <= %s "
                f"ORDER BY board_code, `date` ASC",
                (*bc_list, ext_start, end_date),
            )
            for r in cur.fetchall():
                board_kline_map[r['board_code']].append({
                    'date': r['date'],
                    'change_percent': _to_float(r['change_percent']),
                    'close_price': _to_float(r['close_price']),
                })

        # 3. 大盘K线（上证指数）
        cur.execute(
            "SELECT `date`, change_percent FROM stock_kline "
            "WHERE stock_code = '000001.SH' AND `date` >= %s AND `date` <= %s "
            "ORDER BY `date` ASC",
            (ext_start, end_date),
        )
        market_kline_map = {}
        for r in cur.fetchall():
            market_kline_map[r['date']] = _to_float(r['change_percent'])

    finally:
        cur.close()
        conn.close()

    n_with_boards = sum(1 for c6 in codes_6 if c6 in stock_boards)
    n_with_kline = sum(1 for bc in all_board_codes if bc in board_kline_map)
    logger.info("[概念板块] %d/%d 只股票有概念板块, %d/%d 板块有K线, 大盘%d天",
                n_with_boards, len(codes_6), n_with_kline,
                len(all_board_codes), len(market_kline_map))

    return {
        'stock_boards': dict(stock_boards),
        'board_kline_map': dict(board_kline_map),
        'market_kline_map': market_kline_map,
    }
