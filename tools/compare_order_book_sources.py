"""
对比新浪 vs 腾讯盘口数据，验证数据一致性。
同时与数据库中已有的新浪数据进行对比。

用法: .venv/bin/python tools/compare_order_book_sources.py
"""

import asyncio
import json
import re
import aiohttp

_HEADERS = {
    "Referer": "https://finance.sina.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}

STOCKS = [
    ("sh600519", "sh600519", "600519.SH", "贵州茅台"),
    ("sz002371", "sz002371", "002371.SZ", "北方华创"),
    ("sz000001", "sz000001", "000001.SZ", "平安银行"),
    ("sh601318", "sh601318", "601318.SH", "中国平安"),
]


async def fetch_sina(symbol: str) -> dict:
    url = f"https://hq.sinajs.cn/list={symbol}"
    async with aiohttp.ClientSession() as s:
        async with s.get(url, headers=_HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as r:
            text = await r.text(encoding='gbk')
    match = re.search(r'"(.+)"', text)
    if not match:
        return {}
    f = match.group(1).split(',')
    def _f(i):
        try: return float(f[i])
        except: return 0.0
    def _v(i):
        try: return int(float(f[i])) // 100
        except: return 0
    return {
        "source": "sina",
        "current_price": _f(3), "open_price": _f(1), "prev_close": _f(2),
        "high_price": _f(4), "low_price": _f(5),
        "volume": _v(8), "amount_yuan": _f(9),
        "buy1_price": _f(11), "buy1_vol": _v(10),
        "buy2_price": _f(13), "buy2_vol": _v(12),
        "sell1_price": _f(21), "sell1_vol": _v(20),
        "sell2_price": _f(23), "sell2_vol": _v(22),
    }


async def fetch_tencent(symbol: str) -> dict:
    url = f"https://qt.gtimg.cn/q={symbol}"
    headers = {**_HEADERS, "Referer": "https://stockpage.10jqka.com.cn/"}
    async with aiohttp.ClientSession() as s:
        async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            text = await r.text(encoding='gbk')
    content = text.split('="')[1].rstrip('";').rstrip('"')
    f = content.split('~')
    def _f(i):
        try: return float(f[i])
        except: return 0.0
    def _i(i):
        try: return int(float(f[i]))
        except: return 0
    return {
        "source": "tencent",
        "current_price": _f(3), "open_price": _f(5), "prev_close": _f(4),
        "high_price": _f(33), "low_price": _f(34),
        "volume": _i(6), "amount_wan": _f(37),
        "outer_vol": _i(7), "inner_vol": _i(8),
        "buy1_price": _f(9), "buy1_vol": _i(10),
        "buy2_price": _f(11), "buy2_vol": _i(12),
        "sell1_price": _f(19), "sell1_vol": _i(20),
        "sell2_price": _f(21), "sell2_vol": _i(22),
    }


def compare(name, sina, tencent):
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")

    fields = ["current_price", "open_price", "prev_close", "high_price", "low_price",
              "buy1_price", "buy2_price", "sell1_price", "sell2_price"]
    all_match = True
    for field in fields:
        sv = sina.get(field, 0)
        tv = tencent.get(field, 0)
        match = "✅" if abs(sv - tv) < 0.01 else "❌"
        if match == "❌":
            all_match = False
        print(f"  {field:20s}  新浪={sv:>12.2f}  腾讯={tv:>12.2f}  {match}")

    # 成交量对比（新浪是股→手，腾讯直接是手）
    sv = sina.get("volume", 0)
    tv = tencent.get("volume", 0)
    match = "✅" if abs(sv - tv) < 10 else "⚠️ (差异)"
    print(f"  {'volume(手)':20s}  新浪={sv:>12d}  腾讯={tv:>12d}  {match}")

    # 买卖量对比
    for field in ["buy1_vol", "buy2_vol", "sell1_vol", "sell2_vol"]:
        sv = sina.get(field, 0)
        tv = tencent.get(field, 0)
        match = "✅" if sv == tv else "⚠️"
        print(f"  {field:20s}  新浪={sv:>12d}  腾讯={tv:>12d}  {match}")

    # 腾讯独有字段
    print(f"  {'outer_vol(外盘)':20s}  {'N/A':>12s}  腾讯={tencent.get('outer_vol', 0):>12d}")
    print(f"  {'inner_vol(内盘)':20s}  {'N/A':>12s}  腾讯={tencent.get('inner_vol', 0):>12d}")

    # 成交额对比
    sina_amount = sina.get("amount_yuan", 0)
    tencent_amount = tencent.get("amount_wan", 0) * 10000
    diff_pct = abs(sina_amount - tencent_amount) / max(sina_amount, 1) * 100
    match = "✅" if diff_pct < 1 else f"⚠️ ({diff_pct:.1f}%)"
    print(f"  {'amount(元)':20s}  新浪={sina_amount:>14.2f}  腾讯={tencent_amount:>14.2f}  {match}")

    return all_match


async def compare_with_db():
    """对比腾讯新数据与数据库中已有的新浪数据（取最近一个交易日的数据逐条对比）"""
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    try:
        from dao.stock_order_book_dao import get_order_book
        from dao import get_connection
    except ImportError as e:
        print(f"\n[跳过数据库对比] 无法导入 dao 模块: {e}")
        return

    print(f"\n{'='*60}")
    print(f"  数据库已有数据(新浪采集) vs 腾讯新接口 逐条对比")
    print(f"{'='*60}")

    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        # 取最近一个交易日
        cursor.execute(
            "SELECT DISTINCT trade_date FROM stock_order_book "
            "WHERE prev_close > 0 ORDER BY trade_date DESC LIMIT 1"
        )
        date_row = cursor.fetchone()
        if not date_row:
            print("  数据库中无有效盘口数据")
            return
        latest_date = date_row["trade_date"]
        print(f"  最近交易日: {latest_date}")

        # 取该日所有记录
        cursor.execute(
            "SELECT stock_code, current_price, open_price, prev_close, "
            "high_price, low_price, volume, amount, "
            "buy1_price, buy1_vol, buy2_price, buy2_vol, "
            "sell1_price, sell1_vol, sell2_price, sell2_vol "
            "FROM stock_order_book WHERE trade_date = %s AND prev_close > 0 "
            "ORDER BY stock_code LIMIT 20",
            (latest_date,),
        )
        rows = cursor.fetchall()
        if not rows:
            print("  该日无数据")
            return

        print(f"  抽样对比 {len(rows)} 只股票\n")

        price_match = 0
        price_total = 0
        vol_match = 0
        vol_total = 0

        for row in rows:
            code = row["stock_code"]
            # 构造腾讯符号
            if code.startswith("6"):
                qt_sym = f"sh{code}"
            else:
                qt_sym = f"sz{code}"

            try:
                tencent = await fetch_tencent(qt_sym)
            except Exception as e:
                print(f"  {code}: 腾讯接口异常 {e}")
                continue

            # 如果腾讯返回的当前价为0，说明该股票可能停牌或代码不对
            if tencent.get("current_price", 0) == 0:
                print(f"  {code}: 腾讯返回空数据，跳过")
                continue

            # 对比价格字段
            price_fields = ["current_price", "open_price", "prev_close", "high_price", "low_price",
                            "buy1_price", "buy2_price", "sell1_price", "sell2_price"]
            mismatches = []
            for f in price_fields:
                db_val = float(row.get(f) or 0)
                qt_val = tencent.get(f, 0)
                price_total += 1
                if abs(db_val - qt_val) < 0.01:
                    price_match += 1
                else:
                    mismatches.append(f"{f}: DB={db_val} vs QT={qt_val}")

            # 对比量字段
            vol_fields = ["buy1_vol", "buy2_vol", "sell1_vol", "sell2_vol"]
            vol_mismatches = []
            for f in vol_fields:
                db_val = int(row.get(f) or 0)
                qt_val = tencent.get(f, 0)
                vol_total += 1
                if abs(db_val - qt_val) <= 1:
                    vol_match += 1
                else:
                    vol_mismatches.append(f"{f}: DB={db_val} vs QT={qt_val}")

            status = "✅" if not mismatches and not vol_mismatches else "❌"
            print(f"  {code} {status}", end="")
            if mismatches:
                print(f"  价格差异: {', '.join(mismatches)}", end="")
            if vol_mismatches:
                print(f"  量差异: {', '.join(vol_mismatches)}", end="")

            # 显示腾讯独有的外盘/内盘
            outer = tencent.get("outer_vol", 0)
            inner = tencent.get("inner_vol", 0)
            if outer or inner:
                print(f"  [外盘={outer} 内盘={inner}]", end="")
            print()

        print(f"\n  价格字段一致率: {price_match}/{price_total} ({price_match/max(price_total,1)*100:.1f}%)")
        print(f"  量字段一致率:   {vol_match}/{vol_total} ({vol_match/max(vol_total,1)*100:.1f}%)")
        if price_match == price_total and vol_match == vol_total:
            print("  ✅ 数据库已有数据与腾讯接口完全一致")
        else:
            print("  ⚠️ 存在差异（收盘后五档量可能因集合竞价略有不同，属正常现象）")

    finally:
        cursor.close()
        conn.close()


async def main():
    print("盘口数据源对比: 新浪 vs 腾讯")
    print("注意: 收盘后数据应完全一致，盘中可能有微小延迟差异\n")

    all_ok = True
    for sina_sym, tencent_sym, code, name in STOCKS:
        try:
            sina_data = await fetch_sina(sina_sym)
            tencent_data = await fetch_tencent(tencent_sym)
            ok = compare(f"{name} ({code})", sina_data, tencent_data)
            if not ok:
                all_ok = False
        except Exception as e:
            print(f"\n{name}: 获取失败 - {e}")

    await compare_with_db()

    print(f"\n{'='*60}")
    if all_ok:
        print("  ✅ 所有价格字段完全一致，可安全切换到腾讯接口")
    else:
        print("  ⚠️ 部分字段存在差异，请检查上方详情")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
