"""
测试 industry_stock_sorter 的排序功能。

运行: python -m pytest tests/test_industry_stock_sorter.py -v
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from service.industry_sort.industry_stock_sorter import sort_and_rewrite
import logging


def test_sort_industry_list():
    """执行排序并输出结果"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
    )
    sort_and_rewrite()


if __name__ == '__main__':
    test_sort_industry_list()
