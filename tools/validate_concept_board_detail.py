"""
概念板块成分股差异详细清单

对10个数量不一致的板块，逐个抓取同花顺完整成分股列表，
与数据库做差集，输出每个板块具体缺少哪些股票。

Usage:
    python -m tools.validate_concept_board_detail
    python -m tools.validate_concept_board_detail --board 300200,300800
"""
import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dao import get_connection
from service.jqka10.concept_board_stocks_10jqka import fetch_board_stocks

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# 上次验证发现的10个不一致板块
DEFAULT_BOARDS = [
    "300200", "300800", "300756", "308761", "301605",
    "301797", "300900", "309090", "308477", "309050",
]


def _get_db_stocks(board_code: str) -> dict[str, str]:
    """从数据库获取板块成分股 {stock_code: stock_name}"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT stock_code, stock_name FROM stock_concept_board_stock "
            "WHERE board_code = %s", (board_code,))
        return {r["stock_code"]: r["stock_name"] for r in cur.fetchall()}
    finally:
        cur.close()
        conn.close()


def _get_board_name(board_code: str) -> str:
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT board_name FROM stock_concept_board "
            "WHERE board_code = %s", (board_code,))
        row = cur.fetchone()
        return row["board_name"] if row else "未知"
    finally:
        cur.close()
        conn.close()


def analyze_board(board_code: str, delay: float = 0.3) -> dict:
    """分析单个板块的差异"""
    board_name = _get_board_name(board_code)
    print(f"\n{'─'*60}")
    print(f"  板块: {board_code} {board_name}")
    print(f"{'─'*60}")

    # 1. DB数据
    db_stocks = _get_db_stocks(board_code)
    print(f"  数据库成分股: {len(db_stocks)} 只")

    # 2. 同花顺数据
    print(f"  正在抓取同花顺成分股...")
    ths_stocks_list = fetch_board_stocks(board_code, delay=delay)
    ths_stocks = {s["stock_code"]: s["stock_name"] for s in ths_stocks_list}
    print(f"  同花顺成分股: {len(ths_stocks)} 只")

    # 3. 差集
    db_codes = set(db_stocks.keys())
    ths_codes = set(ths_stocks.keys())

    missing_in_db = ths_codes - db_codes      # 同花顺有、DB没有
    extra_in_db = db_codes - ths_codes         # DB有、同花顺没有
    common = db_codes & ths_codes

    result = {
        "board_code": board_code,
        "board_name": board_name,
        "db_count": len(db_stocks),
        "ths_count": len(ths_stocks),
        "common": len(common),
        "missing_in_db": [],
        "extra_in_db": [],
    }

    if missing_in_db:
        missing_list = sorted(
            [(c, ths_stocks[c]) for c in missing_in_db],
            key=lambda x: x[0])
        result["missing_in_db"] = missing_list
        print(f"\n  ⚠ 数据库缺少 {len(missing_list)} 只 (同花顺有、DB无):")
        for i, (code, name) in enumerate(missing_list):
            print(f"    {i+1:3d}. {code} {name}")

    if extra_in_db:
        extra_list = sorted(
            [(c, db_stocks[c]) for c in extra_in_db],
            key=lambda x: x[0])
        result["extra_in_db"] = extra_list
        print(f"\n  ⚠ 数据库多余 {len(extra_list)} 只 (DB有、同花顺无):")
        for i, (code, name) in enumerate(extra_list):
            print(f"    {i+1:3d}. {code} {name}")

    if not missing_in_db and not extra_in_db:
        print(f"\n  ✓ 完全一致")

    print(f"\n  汇总: 共同={len(common)}, DB缺少={len(missing_in_db)}, "
          f"DB多余={len(extra_in_db)}")
    return result


def main():
    parser = argparse.ArgumentParser(description="概念板块成分股差异详细清单")
    parser.add_argument("--board", type=str,
                        help="指定板块代码(逗号分隔)，默认检查10个不一致板块")
    parser.add_argument("--delay", type=float, default=0.5,
                        help="抓取页间延迟(秒)")
    args = parser.parse_args()

    board_codes = args.board.split(",") if args.board else DEFAULT_BOARDS

    print(f"\n{'='*60}")
    print(f"  概念板块成分股差异详细清单")
    print(f"  待检查板块: {len(board_codes)} 个")
    print(f"{'='*60}")

    all_results = []
    for i, bc in enumerate(board_codes):
        result = analyze_board(bc, delay=args.delay)
        all_results.append(result)
        if i < len(board_codes) - 1:
            time.sleep(2)

    # 总汇总
    total_missing = sum(len(r["missing_in_db"]) for r in all_results)
    total_extra = sum(len(r["extra_in_db"]) for r in all_results)

    print(f"\n{'='*60}")
    print(f"  总汇总")
    print(f"{'='*60}")
    print(f"  检查板块数: {len(all_results)}")
    print(f"  总计DB缺少: {total_missing} 只成分股")
    print(f"  总计DB多余: {total_extra} 只成分股")

    if total_missing > 0:
        print(f"\n  各板块缺少明细:")
        print(f"  {'板块代码':10s} {'板块名称':12s} {'DB':>5s} {'同花顺':>5s} "
              f"{'缺少':>5s} {'多余':>5s}")
        print(f"  {'-'*48}")
        for r in all_results:
            if r["missing_in_db"] or r["extra_in_db"]:
                print(f"  {r['board_code']:10s} {r['board_name']:12s} "
                      f"{r['db_count']:5d} {r['ths_count']:5d} "
                      f"{len(r['missing_in_db']):5d} {len(r['extra_in_db']):5d}")
    print()


if __name__ == "__main__":
    main()
