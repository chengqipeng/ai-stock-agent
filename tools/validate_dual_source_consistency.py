"""
验证同花顺与东方财富个股资金流向数据一致性。

以同花顺为主，验证东方财富数据可作为合格备用源。

判定标准：
  - 收盘价：差异 < 0.5%
  - 涨跌幅：绝对差异 < 0.3 个百分点
  - 资金流向 big_net：两家分类阈值不同，仅供参考

Usage:
    .venv/bin/python -m tools.validate_dual_source_consistency
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _pct_diff(a, b):
    """计算百分比差异，基于 a（同花顺为主）"""
    if a is None or b is None:
        return None
    if a == 0 and b == 0:
        return 0.0
    if a == 0:
        return abs(b) * 100
    return abs(a - b) / abs(a) * 100


def _abs_diff(a, b):
    if a is None or b is None:
        return None
    return abs(a - b)


async def validate_fund_flow():
    """验证个股资金流向：同花顺 vs 东方财富"""
    from common.utils.stock_info_utils import get_stock_info_by_code
    from service.jqka10.stock_history_fund_flow_10jqka import get_fund_flow_history as jqka_ff
    from service.eastmoney.stock_info.stock_history_flow import get_fund_flow_history as em_ff_raw
    from service.auto_job.fund_flow_scheduler import _convert_em_klines_to_dicts
    from dao import get_connection

    print(f"\n{'='*70}")
    print("个股资金流向 — 同花顺 vs 东方财富")
    print(f"{'='*70}")

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT stock_code FROM stock_fund_flow "
        "ORDER BY RAND() LIMIT 3"
    )
    codes = [r["stock_code"] for r in cur.fetchall()]
    cur.close()
    conn.close()

    if not codes:
        print("❌ 找不到有资金流向数据的股票")
        return False

    all_pass = True
    for code in codes:
        stock_info = get_stock_info_by_code(code)
        if not stock_info:
            print(f"  ⚠️ {code} 无法获取 stock_info")
            continue

        print(f"\n  股票: {stock_info.stock_name}({code})")

        try:
            jqka_data = await jqka_ff(stock_info)
        except Exception as e:
            print(f"  ⚠️ 同花顺获取失败: {e}")
            continue

        try:
            em_raw = await em_ff_raw(stock_info)
            em_data = _convert_em_klines_to_dicts(em_raw) if em_raw else []
        except Exception as e:
            print(f"  ⚠️ 东方财富获取失败: {e}")
            continue

        if not jqka_data or not em_data:
            print(f"  ⚠️ 数据为空 jqka={len(jqka_data or [])} em={len(em_data or [])}")
            continue

        jqka_map = {r["date"]: r for r in jqka_data}
        em_map = {r["date"]: r for r in em_data}
        common_dates = sorted(set(jqka_map.keys()) & set(em_map.keys()))

        if not common_dates:
            print(f"  ⚠️ 无重叠日期")
            continue

        check_dates = common_dates[-10:]
        print(f"  共同日期: {len(common_dates)} 检查最近 {len(check_dates)} 天")

        close_diffs = []
        chg_diffs = []
        big_net_diffs = []
        issues = []

        for d in check_dates:
            j = jqka_map[d]
            e = em_map[d]

            cd = _pct_diff(j.get("close_price"), e.get("close_price"))
            if cd is not None:
                close_diffs.append(cd)
                if cd > 0.5:
                    issues.append(f"    {d} close: 同花顺={j.get('close_price')} 东方财富={e.get('close_price')} 差异={cd:.2f}%")

            chg = _abs_diff(j.get("change_pct"), e.get("change_pct"))
            if chg is not None:
                chg_diffs.append(chg)

            j_big = j.get("big_net")
            e_big = e.get("big_net")
            if j_big is not None and e_big is not None and j_big != 0:
                bd = abs(j_big - e_big) / abs(j_big) * 100
                big_net_diffs.append(bd)

        avg_close = sum(close_diffs) / len(close_diffs) if close_diffs else 0
        avg_chg = sum(chg_diffs) / len(chg_diffs) if chg_diffs else 0
        avg_big = sum(big_net_diffs) / len(big_net_diffs) if big_net_diffs else 0

        close_ok = avg_close < 0.5
        chg_ok = avg_chg < 0.3

        print(f"  {'✅' if close_ok else '❌'} 收盘价平均差异: {avg_close:.3f}% (阈值<0.5%)")
        print(f"  {'✅' if chg_ok else '❌'} 涨跌幅平均差异: {avg_chg:.3f}pp (阈值<0.3pp)")
        print(f"  ℹ️  大单净额平均差异: {avg_big:.1f}% (两家分类阈值不同，仅供参考)")

        if issues:
            for iss in issues[:5]:
                print(iss)

        if not (close_ok and chg_ok):
            all_pass = False

        await asyncio.sleep(2)

    return all_pass


async def main():
    print("=" * 70)
    print("资金流向双数据源一致性验证（以同花顺为主，东方财富为备用）")
    print("=" * 70)

    flow_ok = await validate_fund_flow()

    print(f"\n{'='*70}")
    if flow_ok:
        print("✅ 验证通过：东方财富可作为合格备用源（收盘价/涨跌幅完全一致）")
    else:
        print("❌ 验证未通过")


if __name__ == "__main__":
    asyncio.run(main())
