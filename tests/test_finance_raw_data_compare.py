"""
同花顺 vs 东方财富 原始财务数据（英文字段）一致性验证

选取 10 只股票，对比两个数据源 get_financial_raw_data 返回的原始数值是否一致。
对比维度：
  1. 报告期覆盖：重叠报告期数量
  2. 字段结构：两边返回的英文 key 是否一致
  3. 核心指标数值：同一报告期下数值偏差（相对误差 ≤ 5%）
"""
import asyncio
import logging

from common.utils.stock_info_utils import StockInfo
from service.jqka10.stock_finance_data_10jqka import (
    get_financial_raw_data as jqka_raw,
)
from service.eastmoney.stock_info.stock_financial_main import (
    get_financial_raw_data as em_raw,
)


VERIFY_STOCKS = [
    ("600519.SH", "贵州茅台"),
    ("000858.SZ", "五粮液"),
    ("300750.SZ", "宁德时代"),
    ("601318.SH", "中国平安"),
    ("000001.SZ", "平安银行"),
    ("002714.SZ", "牧原股份"),
    ("688981.SH", "中芯国际"),
    ("600036.SH", "招商银行"),
    ("002371.SZ", "北方华创"),
    ("300059.SZ", "东方财富"),
]


def _make_stock_info(code_normalize: str, name: str) -> StockInfo:
    code, suffix = code_normalize.split(".")
    market_prefix = "0" if suffix == "SZ" else "1"
    return StockInfo(
        secid=f"{market_prefix}.{code}",
        stock_code=code,
        stock_code_normalize=code_normalize,
        stock_name=name,
    )


# 核心对比字段（英文 key）
_COMPARE_KEYS = [
    "EPSJB", "BPS", "TOTALOPERATEREVE", "PARENTNETPROFIT", "KCFJCXSYJLR",
    "TOTALOPERATEREVETZ", "PARENTNETPROFITTZ", "KCFJCXSYJLRTZ",
    "ROEJQ", "XSMLL", "XSJLL", "ZCFZL",
    "SINGLE_QUARTER_REVENUE", "SINGLE_QUARTER_PARENTNETPROFIT",
    "SINGLE_QUARTER_KCFJCXSYJLR",
    "EPSKCJB", "ROEKCJQ",
]

_TOLERANCE_PCT = 5.0
_TOLERANCE_ABS = 0.05


def _values_close(v1, v2) -> tuple[bool, str]:
    """比较两个原始数值是否接近"""
    n1 = float(v1) if isinstance(v1, (int, float)) else None
    n2 = float(v2) if isinstance(v2, (int, float)) else None

    if n1 is None and n2 is None:
        return True, "both None"
    if n1 is None or n2 is None:
        return False, f"jqka={v1} vs em={v2} (one is None)"

    diff = abs(n1 - n2)
    base = max(abs(n1), abs(n2))

    if base < _TOLERANCE_ABS:
        ok = diff < _TOLERANCE_ABS
    else:
        ok = (diff / base * 100) <= _TOLERANCE_PCT

    if ok:
        return True, f"jqka={v1} vs em={v2}"
    pct = diff / base * 100 if base else float("inf")
    return False, f"jqka={v1} vs em={v2} (差异{pct:.1f}%)"



async def compare_single_stock(code_normalize: str, name: str) -> dict:
    stock_info = _make_stock_info(code_normalize, name)
    result = {
        "code": code_normalize, "name": name,
        "pass": True, "errors": [], "warnings": [],
        "matched_periods": 0, "field_results": {},
    }

    try:
        jqka_data, em_data = await asyncio.gather(
            jqka_raw(stock_info), em_raw(stock_info),
        )
    except Exception as e:
        result["pass"] = False
        result["errors"].append(f"数据请求失败: {e}")
        return result

    if not jqka_data:
        result["pass"] = False
        result["errors"].append("同花顺返回空数据")
        return result
    if not em_data:
        result["pass"] = False
        result["errors"].append("东方财富返回空数据")
        return result

    # 1. 结构检查：确认两边都用英文 key
    jqka_keys = set(jqka_data[0].keys())
    em_keys = set(em_data[0].keys())
    if "REPORT_DATE" not in jqka_keys:
        result["errors"].append("同花顺缺少 REPORT_DATE 字段")
    if "REPORT_DATE" not in em_keys:
        result["errors"].append("东方财富缺少 REPORT_DATE 字段")

    # 确认没有中文 key（排除 REPORT_DATE_NAME）
    for k in jqka_keys:
        if any('\u4e00' <= c <= '\u9fff' for c in k) and k != "REPORT_DATE_NAME":
            result["errors"].append(f"同花顺存在中文字段: {k}")
            break

    # 2. 按报告日期建索引
    jqka_by_date = {(r.get("REPORT_DATE") or "")[:10]: r for r in jqka_data}
    em_by_date = {(r.get("REPORT_DATE") or "")[:10]: r for r in em_data}

    common_dates = sorted(set(jqka_by_date) & set(em_by_date), reverse=True)
    result["matched_periods"] = len(common_dates)

    if not common_dates:
        result["pass"] = False
        result["errors"].append("无重叠报告期")
        return result

    # 3. 对比最近 5 个报告期的核心字段
    for date in common_dates[:5]:
        jrec = jqka_by_date[date]
        erec = em_by_date[date]

        for key in _COMPARE_KEYS:
            jval = jrec.get(key)
            eval_ = erec.get(key)
            ok, desc = _values_close(jval, eval_)

            field_key = f"{date}|{key}"
            result["field_results"][field_key] = {"ok": ok, "desc": desc}
            if not ok:
                result["warnings"].append(f"[{date}] {key}: {desc}")

    total = len(result["field_results"])
    failed = sum(1 for v in result["field_results"].values() if not v["ok"])
    if total > 0 and failed / total > 0.3:
        result["pass"] = False
        result["errors"].append(f"不一致字段过多: {failed}/{total} ({failed/total*100:.0f}%)")

    return result



async def main():
    print("=" * 75)
    print("  同花顺 vs 东方财富 原始财务数据（英文字段）一致性验证")
    print("=" * 75)
    print(f"\n  对比字段: {len(_COMPARE_KEYS)} 个核心指标（英文 key）")
    print(f"  容差: 相对误差 ≤ {_TOLERANCE_PCT}%，绝对误差兜底 {_TOLERANCE_ABS}")
    print(f"  验证股票: {len(VERIFY_STOCKS)} 只\n")
    print(f"{'─' * 75}")

    results = []
    total_pass = 0
    total_fail = 0

    for code, name in VERIFY_STOCKS:
        print(f"\n  📊 {name}({code}) ... ", end="", flush=True)
        r = await compare_single_stock(code, name)
        results.append(r)

        if r["pass"] and not r["errors"]:
            warn_count = len(r["warnings"])
            if warn_count:
                total_pass += 1
                print(f"⚠️  通过（{warn_count} 个字段有偏差，{r['matched_periods']} 个重叠报告期）")
            else:
                total_pass += 1
                print(f"✅ 通过（{r['matched_periods']} 个重叠报告期，全部字段一致）")
        else:
            total_fail += 1
            print(f"❌ 失败")

        for e in r["errors"]:
            print(f"       └─ ❌ {e}")
        for w in r["warnings"][:5]:
            print(f"       └─ ⚠️  {w}")
        if len(r["warnings"]) > 5:
            print(f"       └─ ... 还有 {len(r['warnings']) - 5} 条偏差")

        fr = r["field_results"]
        if fr:
            ok_count = sum(1 for v in fr.values() if v["ok"])
            print(f"       └─ 一致率: {ok_count}/{len(fr)} ({ok_count/len(fr)*100:.0f}%)")

        # 打印第一条记录的字段样例（仅第一只股票）
        if code == VERIFY_STOCKS[0][0] and r["pass"]:
            print(f"\n       数据样例（{name} 最新一期）:")
            try:
                jqka_data = await jqka_raw(_make_stock_info(code, name))
                if jqka_data:
                    rec = jqka_data[0]
                    sample_keys = ["REPORT_DATE_NAME", "REPORT_DATE", "EPSJB", "BPS",
                                   "TOTALOPERATEREVE", "PARENTNETPROFIT", "ROEJQ", "XSMLL"]
                    for k in sample_keys:
                        print(f"         {k}: {rec.get(k)}")
            except Exception:
                pass

        await asyncio.sleep(0.5)

    # 汇总
    print(f"\n{'=' * 75}")
    print(f"  验证结果汇总")
    print(f"{'=' * 75}")
    print(f"  总计: {len(VERIFY_STOCKS)} 只")
    print(f"  通过: {total_pass} 只")
    print(f"  失败: {total_fail} 只")

    all_fields = sum(len(r["field_results"]) for r in results)
    all_ok = sum(sum(1 for v in r["field_results"].values() if v["ok"]) for r in results)
    if all_fields:
        print(f"  全局字段一致率: {all_ok}/{all_fields} ({all_ok/all_fields*100:.1f}%)")

    all_pass = total_fail == 0
    print(f"\n  最终结论: {'✅ 全部验证通过' if all_pass else '❌ 存在验证失败'}")
    print(f"{'=' * 75}")
    return all_pass


if __name__ == "__main__":
    ok = asyncio.run(main())
    exit(0 if ok else 1)
