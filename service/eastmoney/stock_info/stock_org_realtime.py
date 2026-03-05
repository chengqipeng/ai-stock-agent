"""机构持仓实时快照模块。

从东方财富获取最新一期机构持仓数据（RPT_MAIN_ORGHOLD）
以及股东人数变化趋势（RPT_F10_EH_HOLDERNUM），
预计算摘要供提示词直接引用。
"""

import logging
from collections import defaultdict

from common.http.http_utils import fetch_eastmoney_api, EASTMONEY_API_URL
from common.utils.amount_utils import convert_amount_org_holder_1, convert_amount_org_holder_2
from common.utils.cache_utils import get_cache_path, load_cache, save_cache
from common.utils.stock_info_utils import StockInfo

logger = logging.getLogger(__name__)


async def _fetch_latest_org_hold(stock_info: StockInfo, page_size: int = 50) -> list[dict]:
    """获取最新一期机构持仓明细（RPT_MAIN_ORGHOLD）"""
    cache_path = get_cache_path("org_realtime_snapshot", stock_info.stock_code)
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data

    params = {
        "reportName": "RPT_MAIN_ORGHOLD",
        "columns": "ALL",
        "quoteColumns": "",
        "filter": f'(SECURITY_CODE="{stock_info.stock_code}")',
        "pageNumber": "1",
        "pageSize": str(page_size),
        "sortTypes": "-1",
        "sortColumns": "REPORT_DATE",
        "source": "WEB",
        "client": "WEB",
    }

    data = await fetch_eastmoney_api(EASTMONEY_API_URL, params)
    if data.get("result") and data["result"].get("data"):
        result = data["result"]["data"]
        save_cache(cache_path, result)
        return result
    return []


async def _fetch_holder_num_trend(stock_info: StockInfo, page_size: int = 8) -> list[dict]:
    """获取股东人数变化趋势（RPT_F10_EH_HOLDERNUM）"""
    cache_path = get_cache_path("holder_num_trend", stock_info.stock_code)
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data

    params = {
        "reportName": "RPT_F10_EH_HOLDERNUM",
        "columns": "SECUCODE,SECURITY_CODE,END_DATE,HOLDER_TOTAL_NUM,"
                   "TOTAL_NUM_RATIO,AVG_FREE_SHARES,AVG_FREESHARES_RATIO,"
                   "HOLD_FOCUS,PRICE,AVG_HOLD_AMT",
        "quoteColumns": "",
        "filter": f'(SECUCODE="{stock_info.stock_code_normalize}")',
        "pageNumber": "1",
        "pageSize": str(page_size),
        "sortTypes": "-1",
        "sortColumns": "END_DATE",
        "source": "HSF10",
        "client": "PC",
    }

    data = await fetch_eastmoney_api(EASTMONEY_API_URL, params)
    if data.get("result") and data["result"].get("data"):
        result = data["result"]["data"]
        save_cache(cache_path, result)
        return result
    return []


def _safe_hold_focus(val) -> str | None:
    """安全解析筹码集中度，支持数值和文本（如'非常分散'、'集中'）"""
    if val is None:
        return None
    if isinstance(val, str):
        try:
            return f"{round(float(val), 2)}%"
        except ValueError as e:
            logger.debug("_safe_hold_focus 转换失败: val=%s, %s", val, e)
            return val  # 直接返回文本描述
    if isinstance(val, (int, float)):
        return f"{round(float(val), 2)}%"
    return str(val)


async def get_org_realtime_snapshot(stock_info: StockInfo) -> dict:
    """获取机构持仓实时快照。

    返回最新一期机构持仓分类汇总 + 近几期股东人数变化趋势。
    """
    org_data = await _fetch_latest_org_hold(stock_info)
    holder_trend = await _fetch_holder_num_trend(stock_info)

    # 按报告日期分组，取最新两期
    grouped = defaultdict(list)
    for item in org_data:
        rd = (item.get("REPORT_DATE") or "")[:10]
        if rd and item.get("ORG_TYPE_NAME") not in ("机构汇总", "其他"):
            grouped[rd].append(item)

    sorted_dates = sorted(grouped.keys(), reverse=True)
    latest_date = sorted_dates[0] if sorted_dates else None
    prev_date = sorted_dates[1] if len(sorted_dates) > 1 else None

    # 构建最新一期机构持仓
    latest_orgs = []
    if latest_date:
        for item in grouped[latest_date]:
            latest_orgs.append({
                "机构类型": item.get("ORG_TYPE_NAME", ""),
                "持股家数": item.get("HOULD_NUM"),
                "持股总数（万股）": convert_amount_org_holder_1(item.get("FREE_SHARES")) if item.get("FREE_SHARES") else None,
                "持股市值（亿元）": convert_amount_org_holder_2(item.get("FREE_MARKET_CAP")) if item.get("FREE_MARKET_CAP") else None,
                "占流通股比（%）": round(item.get("FREESHARES_RATIO") or 0, 2),
                "持股变化（万股）": convert_amount_org_holder_1(item.get("HOLDCHA_NUM")) if item.get("HOLDCHA_NUM") else None,
            })

    # 构建前一期（用于对比）
    prev_orgs = []
    if prev_date:
        for item in grouped[prev_date]:
            prev_orgs.append({
                "机构类型": item.get("ORG_TYPE_NAME", ""),
                "持股家数": item.get("HOULD_NUM"),
                "占流通股比（%）": round(item.get("FREESHARES_RATIO") or 0, 2),
            })

    # 股东人数趋势（近几期）
    holder_num_list = []
    for item in holder_trend[:6]:
        holder_num_list.append({
            "截止日期": (item.get("END_DATE") or "")[:10],
            "股东总数": item.get("HOLDER_TOTAL_NUM"),
            "较上期变化（%）": round(float(item.get("TOTAL_NUM_RATIO") or 0), 2),
            "筹码集中度": _safe_hold_focus(item.get("HOLD_FOCUS")),
            "人均持股金额（元）": round(float(item.get("AVG_HOLD_AMT") or 0), 2) if item.get("AVG_HOLD_AMT") else None,
        })

    return {
        "最新报告期": latest_date,
        "前一报告期": prev_date,
        "最新机构持仓": latest_orgs,
        "前一期机构持仓": prev_orgs,
        "股东人数趋势": holder_num_list,
    }


def compute_org_snapshot_summary(snapshot: dict) -> dict:
    """预计算机构持仓变化摘要，供提示词直接引用。

    分析维度：
    1. 各类机构增减持方向
    2. 筹码集中度变化
    3. 股东人数变化趋势
    """
    if not snapshot or not snapshot.get("最新机构持仓"):
        return {"状态": "未获取到机构持仓数据"}

    latest_date = snapshot.get("最新报告期", "--")
    latest_orgs = snapshot.get("最新机构持仓", [])
    prev_orgs = snapshot.get("前一期机构持仓", [])
    holder_trend = snapshot.get("股东人数趋势", [])

    # 增持/减持机构统计
    increase_list = []
    decrease_list = []
    for org in latest_orgs:
        change = org.get("持股变化（万股）")
        if change is None:
            continue
        # change 可能是字符串如 "123.45万股"，也可能是数值
        change_val = _parse_change_val(change)
        if change_val > 0:
            increase_list.append(f'{org["机构类型"]}(+{change})')
        elif change_val < 0:
            decrease_list.append(f'{org["机构类型"]}({change})')

    # 机构总持股占比
    total_ratio = sum(org.get("占流通股比（%）", 0) for org in latest_orgs)

    # 与前一期对比
    prev_map = {o["机构类型"]: o.get("占流通股比（%）", 0) for o in prev_orgs}
    ratio_change_desc = ""
    if prev_orgs:
        prev_total = sum(prev_map.values())
        diff = round(total_ratio - prev_total, 2)
        if diff > 0:
            ratio_change_desc = f"机构合计占流通股比从{prev_total}%升至{total_ratio}%（+{diff}pp）"
        elif diff < 0:
            ratio_change_desc = f"机构合计占流通股比从{prev_total}%降至{total_ratio}%（{diff}pp）"
        else:
            ratio_change_desc = f"机构合计占流通股比持平于{total_ratio}%"

    # 股东人数变化趋势
    holder_desc = ""
    if len(holder_trend) >= 2:
        latest_num = holder_trend[0].get("股东总数")
        prev_num = holder_trend[1].get("股东总数")
        change_pct = holder_trend[0].get("较上期变化（%）", 0)
        if latest_num and prev_num:
            if change_pct < -5:
                holder_desc = f"股东人数从{prev_num}降至{latest_num}（{change_pct}%），筹码明显集中"
            elif change_pct < 0:
                holder_desc = f"股东人数从{prev_num}降至{latest_num}（{change_pct}%），筹码小幅集中"
            elif change_pct > 5:
                holder_desc = f"股东人数从{prev_num}升至{latest_num}（+{change_pct}%），筹码明显分散"
            elif change_pct > 0:
                holder_desc = f"股东人数从{prev_num}升至{latest_num}（+{change_pct}%），筹码小幅分散"
            else:
                holder_desc = f"股东人数{latest_num}，基本持平"

    # 筹码集中度
    focus_desc = ""
    if holder_trend and holder_trend[0].get("筹码集中度"):
        focus_desc = f'最新筹码集中度{holder_trend[0]["筹码集中度"]}'

    return {
        "报告期": latest_date,
        "机构合计占流通股比（%）": round(total_ratio, 2),
        "增持机构": increase_list if increase_list else "无增持",
        "减持机构": decrease_list if decrease_list else "无减持",
        "持仓变化趋势": ratio_change_desc if ratio_change_desc else "仅有一期数据，无法对比",
        "股东人数变化": holder_desc if holder_desc else "无股东人数数据",
        "筹码集中度": focus_desc if focus_desc else "无数据",
        "机构持仓明细": latest_orgs,
        "股东人数趋势": holder_trend[:4],
    }


def _parse_change_val(change) -> float:
    """解析持股变化值，支持数值和字符串格式"""
    if isinstance(change, (int, float)):
        return float(change)
    if isinstance(change, str):
        cleaned = change.replace("万股", "").replace(",", "").strip()
        try:
            return float(cleaned)
        except ValueError as e:
            logger.debug("_parse_change_val 转换失败: change=%s, %s", change, e)
            return 0.0
    return 0.0


if __name__ == "__main__":
    import json
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name("生益科技")
        logger.info("=== %s（%s）机构持仓快照 ===\n", stock_info.stock_name, stock_info.stock_code_normalize)

        snapshot = await get_org_realtime_snapshot(stock_info)
        logger.info("原始快照：")
        logger.info(json.dumps(snapshot, ensure_ascii=False, indent=2))

        logger.info("\n--- 预计算摘要 ---")
        summary = compute_org_snapshot_summary(snapshot)
        logger.info(json.dumps(summary, ensure_ascii=False, indent=2))

    asyncio.run(main())
