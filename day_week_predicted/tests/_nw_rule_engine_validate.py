"""验证规则引擎下周预测的回测准确率。

要求：从至少200个不同概念板块中各选1只个股，确保样本多样性。
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import logging
import random
from collections import defaultdict
from datetime import datetime, timedelta
from dao import get_connection

logger = logging.getLogger(__name__)

def _to_float(v) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0

def _compound_return(pcts):
    r = 1.0
    for p in pcts:
        r *= (1 + p / 100)
    return (r - 1) * 100


def select_stocks_from_boards(min_boards=200):
    """从至少 min_boards 个不同概念板块中各选1只个股。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
