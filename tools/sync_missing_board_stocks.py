"""
补全10个缺少成分股的概念板块数据。

从同花顺抓取完整成分股列表，与数据库做差集，仅插入缺少的股票。

Usage:
    python -m tools.sync_missing_board_stocks
    python -m tools.sync_missing_board_stocks --board 300200,300800
    python -m tools.sync_missing_board_stocks --no-sync  # 不同步新股到系统
"""
import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dao import get_connection
from dao.stock_concept_board_dao import batch_upsert_board_stocks
from service.jqka10.concept_board_stocks_10jqka import fetch_board_stocks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# 上次验证发现的10个不一致板块
MISMATCHED_BOARDS = [
    ("300200", "风电"),
    ("300800", "安防"),
    ("300756", "网络安全"),
    ("308761", "虚拟电厂"),
    ("301605", "体育产业"),
    ("301797", "智能物流"),
    ("300900", "融资融券"),
    ("309090", "华为昇腾"),
    ("308477", "电力物联网"),
    ("309050", "数字水印"),
]


def _get_db_stocks(board_code: str) -> dict[str, str]:
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


def sync_board(board_code: str, board_name: str,
               delay: float = 0.3, sync_stocks: bool = True) -> dict:
    """拉取单个板块的缺少成分股并写入数据库。"""
    print(f"\n{'─'*60}")
    print(f"  处理: {board_code} {board_name}")
    print(f"{'─'*60}")

    # 1. 获取DB现有数据
    db_stocks = _get_db_stocks(board_code)
    print(f"  数据库现有: {len(db_stocks)} 只")

    # 2. 从同花顺抓取完整列表
    print(f"  正在抓取同花顺数据...")
    ths_stocks = fetch_board_stocks(board_code, delay=delay)
    print(f"  同花顺返回: {len(ths_stocks)} 只")

    if not ths_stocks:
        print(f"  ⚠ 抓取失败，跳过")
        return {"board_code": board_code, "board_name": board_name,
                "status": "failed", "fetched": 0, "inserted": 0}

    # 3. 计算差集
    ths_map = {s["stock_code"]: s["stock_name"] for s in ths_stocks}
    missing_codes = set(ths_map.keys()) - set(db_stocks.keys())

    if not missing_codes:
        print(f"  ✓ 无缺少数据")
        return {"board_code": board_code, "board_name": board_name,
                "status": "ok", "fetched": len(ths_stocks), "inserted": 0}

    print(f"  需补入: {len(missing_codes)} 只")

    # 4. 全量 upsert（包含已有的，upsert 不会重复）
    count = batch_upsert_board_stocks(board_code, board_name, ths_stocks)
    print(f"  ✓ 写入完成 (affected={count})")

    # 5. 同步新股到 stocks_data / score_list / K线
    if sync_stocks and missing_codes:
        missing_stocks = [{"stock_code": c, "stock_name": ths_map[c]}
                          for c in missing_codes]
        try:
            from service.jqka10.stock_sync_service import sync_new_stocks
            print(f"  正在同步 {len(missing_stocks)} 只新股到系统...")
            asyncio.run(sync_new_stocks(
                missing_stocks, fetch_kline=True, kline_delay=2.0))
            print(f"  ✓ 新股同步完成")
        except Exception as e:
            print(f"  ⚠ 新股同步失败: {e}")

    # 6. 验证
    db_after = _get_db_stocks(board_code)
    print(f"  验证: 写入后DB={len(db_after)} 只 (之前={len(db_stocks)})")

    return {"board_code": board_code, "board_name": board_name,
            "status": "ok", "fetched": len(ths_stocks),
            "inserted": len(missing_codes),
            "db_before": len(db_stocks), "db_after": len(db_after)}


def main():
    parser = argparse.ArgumentParser(description="补全缺少的概念板块成分股")
    parser.add_argument("--board", type=str,
                        help="指定板块代码(逗号分隔)")
    parser.add_argument("--delay", type=float, default=0.3,
                        help="页间延迟(秒)")
    parser.add_argument("--no-sync", action="store_true",
                        help="不同步新股到系统")
    args = parser.parse_args()

    if args.board:
        codes = args.board.split(",")
        boards = []
        for c in codes:
            name = next((n for bc, n in MISMATCHED_BOARDS if bc == c), "")
            boards.append((c, name))
    else:
        boards = MISMATCHED_BOARDS

    print(f"\n{'='*60}")
    print(f"  补全缺少的概念板块成分股")
    print(f"  待处理: {len(boards)} 个板块")
    print(f"  同步新股: {'否' if args.no_sync else '是'}")
    print(f"{'='*60}")

    results = []
    for i, (code, name) in enumerate(boards):
        result = sync_board(code, name, delay=args.delay,
                            sync_stocks=not args.no_sync)
        results.append(result)
        if i < len(boards) - 1:
            print(f"\n  等待3秒后处理下一个板块...")
            time.sleep(3)

    # 汇总
    total_inserted = sum(r.get("inserted", 0) for r in results)
    ok = sum(1 for r in results if r["status"] == "ok")
    failed = sum(1 for r in results if r["status"] == "failed")

    print(f"\n{'='*60}")
    print(f"  补全完成")
    print(f"{'='*60}")
    print(f"  成功: {ok} 个板块")
    print(f"  失败: {failed} 个板块")
    print(f"  总计补入: {total_inserted} 只成分股")

    if any(r.get("db_before") for r in results):
        print(f"\n  {'板块':10s} {'名称':12s} {'之前':>5s} {'之后':>5s} {'补入':>5s}")
        print(f"  {'-'*45}")
        for r in results:
            if r["status"] == "ok" and r.get("inserted", 0) > 0:
                print(f"  {r['board_code']:10s} {r['board_name']:12s} "
                      f"{r.get('db_before',0):5d} {r.get('db_after',0):5d} "
                      f"{r['inserted']:5d}")
    print()


if __name__ == "__main__":
    main()
