"""
同花顺 vs 东方财富 财务数据一致性验证

选取 10 只覆盖不同市场、行业的股票，对比两个数据源返回的主要财务指标是否一致。
对比维度：
  1. 报告期覆盖：两边返回的报告期列表是否有交集
  2. 关键指标数值：同一报告期下核心字段的数值偏差
  3. 格式一致性：字段名、单位格式是否对齐
"""
import asyncio
import logging

from common.utils.stock_info_utils import StockInfo, get_stock_info_by_code
from service.jqka10.stock_finance_data_10jqka import (
    get_financial_data_to_json as jqka_to_json,
)
from service.eastmoney.stock_info.stock_financial_main import (
    get_financial_data_to_json as em_to_json,
)


# 10 只验证股票：沪市/深市/创业板/科创板，大盘/中盘，不同行业
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


def _parse_amount_str(val) -> float | None:
    """将 '893.35亿' / '9852.29万' / 数值 / None 统一转为 float（元）"""
    if val is None or val == "--":
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    try:
        if s.endswith("亿"):
            return float(s[:-1]) * 1e8
        if s.endswith("万"):
            return float(s[:-1]) * 1e4
        return float(s)
    except (ValueError, TypeError):
        return None


# 需要对比的核心指标（中文名, 英文key, 是否金额类）
_COMPARE_FIELDS = [
    ("基本每股收益(元)", "EPSJB", False),
    ("每股净资产(元)", "BPS", False),
    ("营业总收入(元)", "TOTALOPERATEREVE", True),
    ("归母净利润(元)", "PARENTNETPROFIT", True),
    ("扣非净利润(元)", "KCFJCXSYJLR", True),
    ("营业总收入同比增长(%)", "TOTALOPERATEREVETZ", False),
    ("归属净利润同比增长(%)", "PARENTNETPROFITTZ", False),
    ("扣非净利润同比增长(%)", "KCFJCXSYJLRTZ", False),
    ("净资产收益率(加权)(%)", "ROEJQ", False),
    ("毛利率(%)", "XSMLL", False),
    ("净利率(%)", "XSJLL", False),
    ("资产负债率(%)", "ZCFZL", False),
]

# 允许的相对误差（百分比）
_TOLERANCE_PCT = 5.0
# 绝对误差兜底（用于接近 0 的值）
_TOLERANCE_ABS = 0.05



def _values_close(v1, v2, is_amount: bool) -> tuple[bool, str]:
    """
    比较两个值是否接近。
    返回 (是否通过, 描述信息)
    """
    n1 = _parse_amount_str(v1) if is_amount else (float(v1) if isinstance(v1, (int, float)) else None)
    n2 = _parse_amount_str(v2) if is_amount else (float(v2) if isinstance(v2, (int, float)) else None)

    if n1 is None and n2 is None:
        return True, "both None"
    if n1 is None or n2 is None:
        return False, f"jqka={v1} vs em={v2} (one is None)"

    diff = abs(n1 - n2)
    base = max(abs(n1), abs(n2))

    if base < _TOLERANCE_ABS:
        # 两个值都接近 0
        ok = diff < _TOLERANCE_ABS
    else:
        ok = (diff / base * 100) <= _TOLERANCE_PCT

    if ok:
        return True, f"jqka={v1} vs em={v2}"
    else:
        pct = diff / base * 100 if base else float("inf")
        return False, f"jqka={v1} vs em={v2} (差异{pct:.1f}%)"


async def compare_single_stock(code_normalize: str, name: str) -> dict:
    """对比单只股票两个数据源的财务数据"""
    stock_info = _make_stock_info(code_normalize, name)
    result = {
        "code": code_normalize,
        "name": name,
        "pass": True,
        "errors": [],
        "warnings": [],
        "matched_periods": 0,
        "field_results": {},
    }

    # 并发请求两个数据源
    try:
        jqka_data, em_data = await asyncio.gather(
            jqka_to_json(stock_info),
            em_to_json(stock_info),
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

    # 按报告日期建索引（取 YYYY-MM-DD 前10位）
    jqka_by_date = {}
    for rec in jqka_data:
        d = (rec.get("报告日期") or "")[:10]
        if d:
            jqka_by_date[d] = rec

    em_by_date = {}
    for rec in em_data:
        d = (rec.get("报告日期") or "")[:10]
        if d:
            em_by_date[d] = rec

    # 找交集报告期
    common_dates = sorted(set(jqka_by_date) & set(em_by_date), reverse=True)
    result["matched_periods"] = len(common_dates)

    if not common_dates:
        result["pass"] = False
        result["errors"].append(
            f"无重叠报告期 (jqka: {list(jqka_by_date.keys())[:3]}, em: {list(em_by_date.keys())[:3]})"
        )
        return result

    # 只对比最近 5 个报告期
    for date in common_dates[:5]:
        jrec = jqka_by_date[date]
        erec = em_by_date[date]

        for cn_name, en_key, is_amount in _COMPARE_FIELDS:
            jval = jrec.get(cn_name)
            eval_ = erec.get(cn_name)

            ok, desc = _values_close(jval, eval_, is_amount)

            field_key = f"{date}|{cn_name}"
            result["field_results"][field_key] = {"ok": ok, "desc": desc}

            if not ok:
                result["warnings"].append(f"[{date}] {cn_name}: {desc}")

    # 超过 30% 字段不一致则判定失败
    total = len(result["field_results"])
    failed = sum(1 for v in result["field_results"].values() if not v["ok"])
    if total > 0 and failed / total > 0.3:
        result["pass"] = False
        result["errors"].append(f"不一致字段过多: {failed}/{total} ({failed/total*100:.0f}%)")

    return result



async def main():
    print("=" * 75)
    print("  同花顺 vs 东方财富 财务数据一致性验证")
    print("=" * 75)
    print(f"\n  对比字段: {len(_COMPARE_FIELDS)} 个核心指标")
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
            total_pass += 1
            warn_count = len(r["warnings"])
            if warn_count:
                print(f"⚠️  通过（{warn_count} 个字段有偏差，{r['matched_periods']} 个重叠报告期）")
            else:
                print(f"✅ 通过（{r['matched_periods']} 个重叠报告期，全部字段一致）")
        else:
            total_fail += 1
            print(f"❌ 失败")

        for e in r["errors"]:
            print(f"       └─ ❌ {e}")

        # 显示前 5 条偏差详情
        for w in r["warnings"][:5]:
            print(f"       └─ ⚠️  {w}")
        if len(r["warnings"]) > 5:
            print(f"       └─ ... 还有 {len(r['warnings']) - 5} 条偏差")

        # 统计一致率
        fr = r["field_results"]
        if fr:
            ok_count = sum(1 for v in fr.values() if v["ok"])
            print(f"       └─ 一致率: {ok_count}/{len(fr)} ({ok_count/len(fr)*100:.0f}%)")

        await asyncio.sleep(0.5)

    # 汇总
    print(f"\n{'=' * 75}")
    print(f"  验证结果汇总")
    print(f"{'=' * 75}")
    print(f"  总计: {len(VERIFY_STOCKS)} 只")
    print(f"  通过: {total_pass} 只")
    print(f"  失败: {total_fail} 只")

    # 全局一致率
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
