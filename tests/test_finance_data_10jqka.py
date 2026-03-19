"""
同花顺财报数据接口验证测试

选取 10 只覆盖不同市场、行业、市值的股票，验证四种报表接口返回数据的完整性和准确性。
验证维度：
  1. 接口可达性：HTTP 200，返回非空数据
  2. 数据结构：包含"报告期"字段，报告期格式正确
  3. 关键字段完整性：核心财务字段存在且非 None
  4. 数值合理性：营收/净利润/总资产等数值在合理范围
  5. 数据一致性：利润表净利润 ≈ 主要指标净利润（同一报告期）
  6. 单位转换：亿/万/百分比转换正确
"""
import asyncio
import json
import logging

from common.utils.stock_info_utils import get_stock_info_by_code, StockInfo
from service.jqka10.stock_finance_data_10jqka import (
    get_stock_income,
    get_stock_balance,
    get_stock_cashflow,
    get_stock_finance_indicators,
    get_stock_all_finance_data,
    _parse_value,
)

logger = logging.getLogger(__name__)

# 10 只验证股票：覆盖沪市/深市/创业板，大盘/中盘/小盘，不同行业
VERIFY_STOCKS = [
    ("600519.SH", "贵州茅台"),   # 沪市 白酒龙头
    ("000858.SZ", "五粮液"),     # 深市主板 白酒
    ("300750.SZ", "宁德时代"),   # 创业板 新能源
    ("601318.SH", "中国平安"),   # 沪市 保险金融
    ("000001.SZ", "平安银行"),   # 深市 银行
    ("002714.SZ", "牧原股份"),   # 深市 农牧
    ("688981.SH", "中芯国际"),   # 科创板 半导体
    ("600036.SH", "招商银行"),   # 沪市 银行
    ("002371.SZ", "北方华创"),   # 深市 半导体设备
    ("300059.SZ", "东方财富"),   # 创业板 券商
]


def _make_stock_info(code_normalize: str, name: str) -> StockInfo:
    """构造 StockInfo 对象"""
    code, suffix = code_normalize.split(".")
    market_prefix = "0" if suffix == "SZ" else "1"
    return StockInfo(
        secid=f"{market_prefix}.{code}",
        stock_code=code,
        stock_code_normalize=code_normalize,
        stock_name=name,
    )


# ─────────────────── 单位转换验证 ───────────────────

def test_parse_value():
    """验证 _parse_value 单位转换的正确性"""
    cases = [
        ("893.35亿", 89335000000.0),
        ("32.60亿", 3260000000.0),
        ("9852.29万", 98522900.0),
        ("7390.15万", 73901500.0),
        ("52.08%", 52.08),
        ("51.53", 51.53),
        ("-4.87亿", -487000000.0),
        ("0.48", 0.48),
        ("--", None),
        ("", None),
        ("False", None),
        ("不适用", None),
    ]
    print("\n【单位转换验证】")
    all_pass = True
    for raw, expected in cases:
        result = _parse_value(raw)
        if expected is None:
            ok = result is None
        else:
            ok = result is not None and abs(result - expected) < 0.01
        status = "✅" if ok else "❌"
        if not ok:
            all_pass = False
        print(f"  {status} _parse_value('{raw}') = {result}  (期望: {expected})")
    return all_pass


# ─────────────────── 接口验证 ───────────────────

async def verify_single_stock(code_normalize: str, name: str) -> dict:
    """
    验证单只股票的四种报表数据。
    返回验证结果 dict。
    """
    stock_info = _make_stock_info(code_normalize, name)
    result = {
        "code": code_normalize,
        "name": name,
        "pass": True,
        "errors": [],
        "summary": {},
    }

    try:
        all_data = await get_stock_all_finance_data(stock_info)
    except Exception as e:
        result["pass"] = False
        result["errors"].append(f"接口请求失败: {e}")
        return result

    for report_type, cn_name in [
        ("main", "主要财务指标"),
        ("benefit", "利润表"),
        ("debt", "资产负债表"),
        ("cash", "现金流量表"),
    ]:
        data = all_data.get(report_type, [])

        # 1. 非空检查
        if not data:
            result["pass"] = False
            result["errors"].append(f"{cn_name}: 返回空数据")
            continue

        result["summary"][report_type] = {"count": len(data)}

        # 2. 报告期格式检查
        latest = data[0]
        report_date = latest.get("报告期", "")
        if not report_date or len(report_date) != 10 or report_date[4] != "-":
            result["pass"] = False
            result["errors"].append(f"{cn_name}: 报告期格式异常 '{report_date}'")
            continue

        result["summary"][report_type]["latest_date"] = report_date

        # 3. 关键字段检查
        if report_type == "main":
            _check_main_fields(latest, cn_name, result)
        elif report_type == "benefit":
            _check_benefit_fields(latest, cn_name, result)
        elif report_type == "debt":
            _check_debt_fields(latest, cn_name, result)
        elif report_type == "cash":
            _check_cash_fields(latest, cn_name, result)

    # 5. 交叉验证：利润表净利润 vs 主要指标净利润
    _cross_validate(all_data, result)

    return result


def _check_field_exists(record: dict, field: str, cn_name: str, result: dict):
    """检查字段存在且非 None"""
    val = record.get(field)
    if val is None:
        result["errors"].append(f"{cn_name}: 缺少字段 '{field}'")
        return False
    return True


def _check_field_positive(record: dict, field: str, cn_name: str, result: dict):
    """检查字段为正数"""
    val = record.get(field)
    if val is not None and isinstance(val, (int, float)) and val <= 0:
        result["errors"].append(f"{cn_name}: '{field}' 应为正数，实际={val}")
        return False
    return True


def _check_main_fields(latest: dict, cn_name: str, result: dict):
    """验证主要财务指标的关键字段"""
    # 核心必需字段（所有行业通用）
    required = ["净利润", "营业总收入", "基本每股收益", "每股净资产", "净资产收益率"]
    for f in required:
        _check_field_exists(latest, f, cn_name, result)
    # 营收和每股净资产应为正数
    for f in ["营业总收入", "每股净资产"]:
        _check_field_positive(latest, f, cn_name, result)
    # 毛利率：银行/保险等金融行业可能无此字段，仅在存在时校验范围
    gpm = latest.get("销售毛利率")
    if gpm is not None and isinstance(gpm, (int, float)):
        if gpm < -100 or gpm > 100:
            result["errors"].append(f"{cn_name}: 销售毛利率异常 {gpm}%")
    result["summary"]["main"]["净利润"] = latest.get("净利润")
    result["summary"]["main"]["营业总收入"] = latest.get("营业总收入")
    result["summary"]["main"]["基本每股收益"] = latest.get("基本每股收益")


def _check_benefit_fields(latest: dict, cn_name: str, result: dict):
    """验证利润表的关键字段"""
    # 利润表核心字段（不同公司字段名可能略有差异）
    revenue_fields = ["其中：营业收入", "营业收入", "一、营业总收入"]
    has_revenue = any(latest.get(f) is not None for f in revenue_fields)
    if not has_revenue:
        result["errors"].append(f"{cn_name}: 缺少营业收入相关字段")

    profit_fields = ["五、净利润", "净利润"]
    has_profit = any(latest.get(f) is not None for f in profit_fields)
    if not has_profit:
        result["errors"].append(f"{cn_name}: 缺少净利润相关字段")

    # 提取净利润用于交叉验证
    for f in profit_fields:
        if latest.get(f) is not None:
            result["summary"]["benefit"]["净利润"] = latest[f]
            break

    for f in revenue_fields:
        if latest.get(f) is not None:
            result["summary"]["benefit"]["营业收入"] = latest[f]
            break


def _check_debt_fields(latest: dict, cn_name: str, result: dict):
    """验证资产负债表的关键字段"""
    required = ["资产合计", "负债合计"]
    for f in required:
        _check_field_exists(latest, f, cn_name, result)
        _check_field_positive(latest, f, cn_name, result)

    # 资产 = 负债 + 所有者权益（允许 1% 误差）
    total_assets = latest.get("资产合计")
    total_liab = latest.get("负债合计")
    total_equity = latest.get("所有者权益（或股东权益）合计")
    if all(isinstance(v, (int, float)) for v in [total_assets, total_liab, total_equity] if v is not None):
        if total_assets and total_liab and total_equity:
            diff = abs(total_assets - total_liab - total_equity)
            tolerance = abs(total_assets) * 0.01
            if diff > tolerance:
                result["errors"].append(
                    f"{cn_name}: 资产={total_assets:.0f} ≠ 负债{total_liab:.0f} + 权益{total_equity:.0f}，差额={diff:.0f}"
                )

    result["summary"]["debt"]["资产合计"] = total_assets
    result["summary"]["debt"]["负债合计"] = total_liab


def _check_cash_fields(latest: dict, cn_name: str, result: dict):
    """验证现金流量表的关键字段"""
    key_fields = ["经营活动产生的现金流量净额", "投资活动产生的现金流量净额", "筹资活动产生的现金流量净额"]
    found = 0
    for f in key_fields:
        if latest.get(f) is not None:
            found += 1
    if found == 0:
        result["errors"].append(f"现金流量表: 三大现金流字段全部缺失")
    result["summary"]["cash"]["经营现金流"] = latest.get("经营活动产生的现金流量净额")


def _cross_validate(all_data: dict, result: dict):
    """交叉验证：主要指标净利润 vs 利润表归母净利润（同一报告期）
    
    注意：主要指标中的"净利润"实际是归属于母公司所有者的净利润，
    应与利润表中的"归属于母公司所有者的净利润"对比，而非"五、净利润"（合并净利润）。
    """
    main_data = all_data.get("main", [])
    benefit_data = all_data.get("benefit", [])
    if not main_data or not benefit_data:
        return

    main_latest = main_data[0]
    benefit_latest = benefit_data[0]

    # 确保是同一报告期
    if main_latest.get("报告期") != benefit_latest.get("报告期"):
        return

    main_profit = main_latest.get("净利润")
    # 利润表中取归母净利润进行对比
    benefit_profit = benefit_latest.get("归属于母公司所有者的净利润")

    if main_profit is not None and benefit_profit is not None:
        if isinstance(main_profit, (int, float)) and isinstance(benefit_profit, (int, float)):
            if main_profit != 0:
                diff_pct = abs(main_profit - benefit_profit) / abs(main_profit) * 100
                if diff_pct > 5:  # 允许 5% 误差（四舍五入差异）
                    result["errors"].append(
                        f"交叉验证: 主要指标净利润={main_profit:.0f} vs 利润表归母净利润={benefit_profit:.0f}，差异{diff_pct:.1f}%"
                    )


# ─────────────────── 主流程 ───────────────────

async def main():
    print("=" * 70)
    print("  同花顺财报数据接口验证测试")
    print("=" * 70)

    # 1. 单位转换测试
    parse_ok = test_parse_value()
    print(f"\n  单位转换: {'✅ 全部通过' if parse_ok else '❌ 存在失败'}")

    # 2. 逐只股票验证
    print(f"\n{'─' * 70}")
    print(f"  验证 {len(VERIFY_STOCKS)} 只股票的财报数据")
    print(f"{'─' * 70}\n")

    results = []
    total_pass = 0
    total_fail = 0

    for code, name in VERIFY_STOCKS:
        print(f"  📊 {name}({code}) ... ", end="", flush=True)
        r = await verify_single_stock(code, name)
        results.append(r)

        if r["pass"] and not r["errors"]:
            total_pass += 1
            print("✅ 通过")
        elif r["errors"]:
            # 区分严重错误和警告
            serious = [e for e in r["errors"] if "缺少" in e or "失败" in e or "空数据" in e or "格式异常" in e]
            if serious:
                total_fail += 1
                print(f"❌ 失败")
            else:
                total_pass += 1
                print(f"⚠️  通过（有警告）")
            for e in r["errors"]:
                print(f"       └─ {e}")
        else:
            total_pass += 1
            print("✅ 通过")

        # 打印摘要
        summary = r.get("summary", {})
        if summary.get("main"):
            m = summary["main"]
            revenue = m.get("营业总收入")
            profit = m.get("净利润")
            eps = m.get("基本每股收益")
            date = m.get("latest_date", "")
            parts = []
            if date:
                parts.append(f"报告期={date}")
            if revenue is not None:
                parts.append(f"营收={revenue/1e8:.2f}亿")
            if profit is not None:
                parts.append(f"净利润={profit/1e8:.2f}亿")
            if eps is not None:
                parts.append(f"EPS={eps}")
            if parts:
                print(f"       └─ {', '.join(parts)}")

        # 请求间隔，避免被限流
        await asyncio.sleep(0.5)

    # 3. 汇总
    print(f"\n{'=' * 70}")
    print(f"  验证结果汇总")
    print(f"{'=' * 70}")
    print(f"  总计: {len(VERIFY_STOCKS)} 只")
    print(f"  通过: {total_pass} 只")
    print(f"  失败: {total_fail} 只")
    print(f"  单位转换: {'✅' if parse_ok else '❌'}")

    # 4. 详细数据展示（取第一只股票展示完整数据样例）
    print(f"\n{'─' * 70}")
    print(f"  数据样例：{VERIFY_STOCKS[0][1]}({VERIFY_STOCKS[0][0]}) 最新一期")
    print(f"{'─' * 70}")

    sample_info = _make_stock_info(VERIFY_STOCKS[0][0], VERIFY_STOCKS[0][1])
    try:
        sample_data = await get_stock_all_finance_data(sample_info)

        indicators = sample_data["main"]
        if indicators:
            print("\n  【主要财务指标】")
            latest = indicators[0]
            for k, v in latest.items():
                if v is not None:
                    if isinstance(v, float) and abs(v) >= 1e8:
                        print(f"    {k}: {v/1e8:.2f}亿")
                    else:
                        print(f"    {k}: {v}")

        income = sample_data["benefit"]
        if income:
            print("\n  【利润表核心】")
            latest = income[0]
            core_fields = ["报告期", "其中：营业收入", "其中：营业成本", "销售费用",
                           "管理费用", "三、营业利润", "四、利润总额", "五、净利润",
                           "归属于母公司所有者的净利润", "（一）基本每股收益"]
            for f in core_fields:
                v = latest.get(f)
                if v is not None:
                    if isinstance(v, float) and abs(v) >= 1e8:
                        print(f"    {f}: {v/1e8:.2f}亿")
                    else:
                        print(f"    {f}: {v}")
    except Exception as e:
        print(f"  数据样例获取失败: {e}")

    all_ok = total_fail == 0 and parse_ok
    print(f"\n{'=' * 70}")
    print(f"  最终结论: {'✅ 全部验证通过' if all_ok else '❌ 存在验证失败'}")
    print(f"{'=' * 70}")

    return all_ok


if __name__ == "__main__":
    ok = asyncio.run(main())
    exit(0 if ok else 1)
