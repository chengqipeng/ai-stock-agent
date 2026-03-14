"""
验证北方华创(002371)与所属各板块的走势相关性。

步骤：
1. 通过 stock_block_info_10jqka 获取所属板块列表
2. 拉取个股和各板块的近60日K线（同花顺 v6/line 接口）
3. 计算每日涨跌幅的皮尔逊相关系数
4. 按相关性排序输出

Usage:
    python _verify_block_correlation.py
"""
import json
import re
import time
import random
import urllib.request

from service.jqka10.stock_block_info_10jqka import (
    fetch_stock_blocks, BLOCK_TYPE_INDUSTRY, BLOCK_TYPE_CONCEPT,
)

_HEADERS = {
    "Accept": "*/*",
    "Referer": "https://stockpage.10jqka.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/145.0.0.0 Safari/537.36",
}
_JSONP_RE = re.compile(r'\((\{.*\})\)', re.DOTALL)


def _fetch_kline_60(symbol: str, prefix: str = "hs") -> dict[str, float]:
    """
    获取近60日K线收盘价，返回 {date: close_price}。
    prefix: "hs" 个股, "bk" 板块
    """
    url = f"https://d.10jqka.com.cn/v6/line/{prefix}_{symbol}/01/last60.js"
    req = urllib.request.Request(url, headers=_HEADERS)
    with urllib.request.urlopen(req, timeout=20) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    m = _JSONP_RE.search(raw)
    if not m:
        return {}
    data_str = json.loads(m.group(1)).get("data", "")
    closes = {}
    for record in data_str.split(";"):
        fields = record.split(",")
        if len(fields) >= 4:
            date, close = fields[0], fields[3]
            try:
                closes[date] = float(close)
            except ValueError:
                pass
    return closes


def _calc_daily_returns(closes: dict[str, float]) -> dict[str, float]:
    """将收盘价序列转为日涨跌幅序列。"""
    sorted_dates = sorted(closes.keys())
    returns = {}
    for i in range(1, len(sorted_dates)):
        prev = closes[sorted_dates[i - 1]]
        curr = closes[sorted_dates[i]]
        if prev != 0:
            returns[sorted_dates[i]] = (curr - prev) / prev
    return returns


def _pearson_corr(x: list[float], y: list[float]) -> float:
    """计算皮尔逊相关系数。"""
    n = len(x)
    if n < 5:
        return 0.0
    mean_x = sum(x) / n
    mean_y = sum(y) / n
    cov = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
    std_x = sum((xi - mean_x) ** 2 for xi in x) ** 0.5
    std_y = sum((yi - mean_y) ** 2 for yi in y) ** 0.5
    if std_x == 0 or std_y == 0:
        return 0.0
    return cov / (std_x * std_y)


def main():
    stock_code = "002371"
    print(f"=== 北方华创({stock_code}) 板块走势相关性验证 ===\n")

    # 1. 获取所属板块
    print("[1] 获取所属板块...")
    block_info = fetch_stock_blocks(stock_code)
    if not block_info:
        print("获取板块信息失败")
        return

    # 只分析行业板块和概念板块
    target_blocks = block_info["industry_blocks"] + block_info["concept_blocks"]
    print(f"    行业板块: {len(block_info['industry_blocks'])}个, "
          f"概念板块: {len(block_info['concept_blocks'])}个, "
          f"共分析: {len(target_blocks)}个\n")

    # 2. 获取个股K线
    print("[2] 获取个股近60日K线...")
    stock_closes = _fetch_kline_60(stock_code, prefix="hs")
    stock_returns = _calc_daily_returns(stock_closes)
    print(f"    个股交易日数: {len(stock_closes)}, 涨跌幅数据点: {len(stock_returns)}\n")

    # 3. 逐个获取板块K线并计算相关性
    print("[3] 获取板块K线并计算相关性...")
    results = []
    for i, block in enumerate(target_blocks):
        bid = block["id"]
        bname = block["name"]
        btype = block["type"]
        time.sleep(0.15 + random.uniform(0, 0.1))
        try:
            block_closes = _fetch_kline_60(str(bid), prefix="bk")
            block_returns = _calc_daily_returns(block_closes)
            # 对齐日期
            common_dates = sorted(set(stock_returns.keys()) & set(block_returns.keys()))
            if len(common_dates) < 10:
                print(f"    {i+1}/{len(target_blocks)} {bname}({bid}) -> 数据不足({len(common_dates)}天)")
                continue
            x = [stock_returns[d] for d in common_dates]
            y = [block_returns[d] for d in common_dates]
            corr = _pearson_corr(x, y)
            results.append({
                "id": bid, "name": bname, "type": btype,
                "corr": corr, "days": len(common_dates),
            })
            print(f"    {i+1}/{len(target_blocks)} {bname}({bid}) [{btype}] "
                  f"-> 相关系数: {corr:.4f} ({len(common_dates)}天)")
        except Exception as e:
            print(f"    {i+1}/{len(target_blocks)} {bname}({bid}) -> 异常: {e}")

    # 4. 按相关性排序输出
    results.sort(key=lambda r: r["corr"], reverse=True)
    print(f"\n{'='*70}")
    print(f"北方华创({stock_code}) 板块走势相关性排名 (近60日涨跌幅皮尔逊相关系数)")
    print(f"{'='*70}")
    print(f"{'排名':>4} {'板块名称':<16} {'类型':<4} {'板块ID':>8} {'相关系数':>8} {'天数':>4}")
    print(f"{'-'*70}")
    for rank, r in enumerate(results, 1):
        marker = " ★" if rank <= 3 else ""
        print(f"{rank:>4} {r['name']:<16} {r['type']:<4} {r['id']:>8} "
              f"{r['corr']:>8.4f} {r['days']:>4}{marker}")

    if results:
        top = results[0]
        print(f"\n>>> 走势最相关板块: {top['name']}({top['id']}) [{top['type']}] "
              f"相关系数={top['corr']:.4f}")


if __name__ == "__main__":
    main()
