"""
美股隔夜走势 vs A股次日涨跌 相关性分析

分析逻辑：
1. 从东方财富API获取纳斯达克/标普500/道琼斯 近3个月日线数据
2. 从数据库获取回测用的50只A股同期日线数据
3. 计算美股隔夜涨跌与A股次日涨跌的相关系数
4. 按行业分组分析，找出对美股敏感度高的板块
5. 输出结论：是否值得将美股信号纳入评分体系

使用方法：
    python tests/test_us_market_correlation.py
"""
import sys
import os
import json
import math
import asyncio
import aiohttp
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
logging.basicConfig(level=logging.WARNING)

from dao.stock_kline_dao import get_kline_data
from common.http.http_utils import get_dynamic_headers, clean_jsonp_response


# ═══════════════════════════════════════════════════════════
# 回测用的50只A股（从 backtest_technical_50stocks_3m_result.json 提取）
# ═══════════════════════════════════════════════════════════
BACKTEST_STOCKS = [
    ("002440.SZ", "闰土股份", "化工"),
    ("002497.SZ", "雅化集团", "化工"),
    ("002772.SZ", "众兴菌业", "农业"),
    ("300666.SZ", "江丰电子", "半导体"),
    ("301312.SZ", "智立方", "机器人"),
    ("600686.SH", "金龙汽车", "汽车"),
    ("688336.SH", "三生国健", "医药"),
    ("002850.SZ", "科达利", "锂电"),
    ("002463.SZ", "沪电股份", "PCB/电子"),
    ("601127.SH", "赛力斯", "汽车"),
    ("603629.SH", "利通电子", "电子"),
    ("603979.SH", "金诚信", "矿业"),
    ("002150.SZ", "正泰电源", "电力设备"),
    ("002728.SZ", "特一药业", "医药"),
    ("002759.SZ", "天际股份", "化工"),
    ("002842.SZ", "翔鹭钨业", "有色金属"),
    ("300619.SZ", "金银河", "锂电设备"),
    ("300953.SZ", "震裕科技", "汽车零部件"),
    ("600549.SH", "厦门钨业", "有色金属"),
    ("603596.SH", "伯特利", "汽车零部件"),
    ("688278.SH", "特宝生物", "医药"),
    ("688519.SH", "南亚新材", "PCB/电子"),
    ("688578.SH", "艾力斯", "医药"),
    ("688617.SH", "惠泰医疗", "医药"),
    ("002001.SZ", "新和成", "化工"),
    ("002957.SZ", "科瑞技术", "机器人"),
    ("600066.SH", "宇通客车", "汽车"),
    ("600114.SH", "东睦股份", "金属材料"),
    ("600150.SH", "中国船舶", "军工"),
    ("600160.SH", "巨化股份", "化工"),
    ("300394.SZ", "天孚通信", "光通信"),
    ("002050.SZ", "三花智控", "汽车零部件"),
    ("688668.SH", "鼎通科技", "连接器"),
    ("002155.SZ", "湖南黄金", "有色金属"),
    ("002378.SZ", "章源钨业", "有色金属"),
    ("002545.SZ", "东方铁塔", "通信设备"),
    ("600884.SH", "杉杉股份", "锂电"),
    ("601138.SH", "工业富联", "消费电子"),
    ("688008.SH", "澜起科技", "半导体"),
    ("688025.SH", "杰普特", "激光设备"),
    ("300124.SZ", "汇川技术", "工控"),
    ("002196.SZ", "方正电机", "汽车零部件"),
    ("002250.SZ", "联化科技", "化工"),
    ("002287.SZ", "奇正藏药", "医药"),
    ("002709.SZ", "天赐材料", "锂电"),
    ("600378.SH", "昊华科技", "化工"),
    ("600489.SH", "中金黄金", "有色金属"),
    ("601899.SH", "紫金矿业", "有色金属"),
    ("688019.SH", "安集科技", "半导体"),
]

# 行业分组
INDUSTRY_GROUPS = {
    "半导体/电子": ["半导体", "PCB/电子", "消费电子", "光通信", "连接器", "电子"],
    "有色金属/矿业": ["有色金属", "矿业", "金属材料"],
    "汽车产业链": ["汽车", "汽车零部件"],
    "锂电/新能源": ["锂电", "锂电设备", "电力设备"],
    "医药": ["医药"],
    "化工": ["化工"],
    "其他": ["农业", "机器人", "军工", "通信设备", "激光设备", "工控"],
}

# 回测区间
START_DATE = "2025-12-10"
END_DATE = "2026-03-10"


# ═══════════════════════════════════════════════════════════
# 获取美股三大指数日线数据（东方财富全球指数API）
# ═══════════════════════════════════════════════════════════

async def fetch_us_index_kline(secid: str, index_name: str, start: str, end: str) -> list[dict]:
    """从东方财富获取美股指数日K线数据

    secid 格式：
      100.NDX  = 纳斯达克100
      100.SPX  = 标普500
      100.DJIA = 道琼斯
    """
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",       # 日K
        "fqt": "1",
        "beg": start.replace("-", ""),
        "end": end.replace("-", ""),
        "lmt": "200",
    }
    headers = get_dynamic_headers()
    headers["Referer"] = "https://quote.eastmoney.com/"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=30)) as resp:
            text = await resp.text()
            data = json.loads(clean_jsonp_response(text))

    klines_raw = data.get("data", {}).get("klines", [])
    result = []
    for line in klines_raw:
        parts = line.split(",")
        if len(parts) >= 9:
            result.append({
                "date": parts[0],           # YYYY-MM-DD
                "open": float(parts[1]),
                "close": float(parts[2]),
                "high": float(parts[3]),
                "low": float(parts[4]),
                "volume": float(parts[5]),
                "change_pct": float(parts[8]),  # 涨跌幅%
            })
    print(f"  {index_name}: 获取到 {len(result)} 条日线数据")
    return result


# ═══════════════════════════════════════════════════════════
# 统计工具函数
# ═══════════════════════════════════════════════════════════

def pearson_corr(x: list[float], y: list[float]) -> float:
    """计算皮尔逊相关系数"""
    n = len(x)
    if n < 5:
        return 0.0
    mx = sum(x) / n
    my = sum(y) / n
    sx = math.sqrt(sum((xi - mx) ** 2 for xi in x) / n)
    sy = math.sqrt(sum((yi - my) ** 2 for yi in y) / n)
    if sx == 0 or sy == 0:
        return 0.0
    cov = sum((x[i] - mx) * (y[i] - my) for i in range(n)) / n
    return round(cov / (sx * sy), 4)


def directional_accuracy(us_changes: list[float], a_changes: list[float],
                          threshold: float = 0.0) -> dict:
    """计算方向一致性：美股涨→A股涨 / 美股跌→A股跌 的比例

    threshold: 美股涨跌幅绝对值超过此阈值才计入（过滤小幅波动）
    """
    n = len(us_changes)
    if n == 0:
        return {"样本数": 0}

    same_dir = 0
    us_up_a_up = 0
    us_up_total = 0
    us_down_a_down = 0
    us_down_total = 0
    counted = 0

    for i in range(n):
        us_chg = us_changes[i]
        a_chg = a_changes[i]
        if abs(us_chg) <= threshold:
            continue
        counted += 1
        if us_chg > 0:
            us_up_total += 1
            if a_chg > 0:
                us_up_a_up += 1
                same_dir += 1
        else:
            us_down_total += 1
            if a_chg < 0:
                us_down_a_down += 1
                same_dir += 1

    if counted == 0:
        return {"样本数": 0}

    return {
        "样本数": counted,
        "方向一致率": f"{same_dir}/{counted} ({round(same_dir/counted*100, 1)}%)",
        "美股涨→A股涨": f"{us_up_a_up}/{us_up_total} ({round(us_up_a_up/us_up_total*100,1)}%)" if us_up_total else "N/A",
        "美股跌→A股跌": f"{us_down_a_down}/{us_down_total} ({round(us_down_a_down/us_down_total*100,1)}%)" if us_down_total else "N/A",
    }


def large_move_accuracy(us_changes: list[float], a_changes: list[float],
                         us_threshold: float = 1.0) -> dict:
    """美股大幅波动（>1%）时A股的跟随情况"""
    return directional_accuracy(us_changes, a_changes, threshold=us_threshold)


# ═══════════════════════════════════════════════════════════
# 核心分析逻辑
# ═══════════════════════════════════════════════════════════

def build_us_date_map(us_klines: list[dict]) -> dict[str, float]:
    """构建美股日期→涨跌幅映射"""
    return {k["date"]: k["change_pct"] for k in us_klines}


def find_previous_us_trading_day(a_date: str, us_date_map: dict) -> str | None:
    """找到A股交易日对应的前一个美股交易日

    A股T日开盘前，美股最近一个交易日是T-1日（美东时间）。
    由于时差，A股周一对应的是美股上周五。
    """
    from datetime import datetime, timedelta
    dt = datetime.strptime(a_date, "%Y-%m-%d")
    # 往前找最多7天
    for offset in range(1, 8):
        prev = (dt - timedelta(days=offset)).strftime("%Y-%m-%d")
        if prev in us_date_map:
            return prev
    return None


def analyze_single_stock(stock_code: str, stock_name: str, industry: str,
                          us_date_map: dict, us_index_name: str) -> dict | None:
    """分析单只A股与美股指数的相关性"""
    klines = get_kline_data(stock_code, start_date=START_DATE, end_date=END_DATE)
    if len(klines) < 10:
        return None

    # 过滤停牌日
    klines = [k for k in klines if (k.get("trading_volume") or 0) > 0]

    us_overnight = []  # 美股隔夜涨跌幅
    a_next_day = []    # A股当日涨跌幅

    for k in klines:
        a_date = k["date"]
        a_chg = k.get("change_percent") or 0
        prev_us_date = find_previous_us_trading_day(a_date, us_date_map)
        if prev_us_date is None:
            continue
        us_chg = us_date_map[prev_us_date]
        us_overnight.append(us_chg)
        a_next_day.append(a_chg)

    if len(us_overnight) < 10:
        return None

    corr = pearson_corr(us_overnight, a_next_day)
    dir_acc = directional_accuracy(us_overnight, a_next_day)
    large_acc = large_move_accuracy(us_overnight, a_next_day, us_threshold=1.0)

    return {
        "股票代码": stock_code,
        "股票名称": stock_name,
        "行业": industry,
        "美股指数": us_index_name,
        "样本数": len(us_overnight),
        "相关系数": corr,
        "方向一致性": dir_acc,
        "美股大幅波动时": large_acc,
        "美股平均涨跌(%)": round(sum(us_overnight) / len(us_overnight), 3),
        "A股平均涨跌(%)": round(sum(a_next_day) / len(a_next_day), 3),
    }


# ═══════════════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════════════

async def main():
    print("=" * 70)
    print("美股隔夜走势 vs A股次日涨跌 相关性分析")
    print(f"分析区间: {START_DATE} ~ {END_DATE}")
    print("=" * 70)

    # 1. 获取美股三大指数数据
    print("\n[1] 获取美股三大指数日线数据...")
    # 需要多取一些数据（A股T日对应美股T-1日，所以美股要从更早开始）
    us_start = "2025-11-25"
    us_indices = {
        "纳斯达克": ("100.NDX", []),
        "标普500": ("100.SPX", []),
        "道琼斯": ("100.DJIA", []),
    }
    for name, (secid, _) in us_indices.items():
        try:
            klines = await fetch_us_index_kline(secid, name, us_start, END_DATE)
            us_indices[name] = (secid, klines)
        except Exception as e:
            print(f"  ⚠ {name} 获取失败: {e}")

    # 2. 逐股分析
    print(f"\n[2] 分析 {len(BACKTEST_STOCKS)} 只A股与美股的相关性...")

    # 用纳斯达克作为主要分析对象（科技股联动性最强）
    primary_index = "纳斯达克"
    _, primary_klines = us_indices[primary_index]
    if not primary_klines:
        print("❌ 纳斯达克数据获取失败，无法继续分析")
        return

    primary_map = build_us_date_map(primary_klines)

    all_results = []
    for code, name, industry in BACKTEST_STOCKS:
        result = analyze_single_stock(code, name, industry, primary_map, primary_index)
        if result:
            all_results.append(result)

    if not all_results:
        print("❌ 无有效分析结果")
        return

    # 3. 输出个股结果（按相关系数排序）
    print(f"\n[3] 个股相关性排名（{primary_index}隔夜 vs A股次日）")
    print("-" * 90)
    print(f"{'排名':>3} {'股票':>10} {'行业':>12} {'相关系数':>8} {'方向一致率':>12} {'大幅波动跟随':>14}")
    print("-" * 90)

    sorted_results = sorted(all_results, key=lambda x: abs(x["相关系数"]), reverse=True)
    for i, r in enumerate(sorted_results, 1):
        dir_rate = r["方向一致性"].get("方向一致率", "N/A")
        large_rate = r["美股大幅波动时"].get("方向一致率", "N/A")
        print(f"{i:>3}  {r['股票名称']:>8}  {r['行业']:>10}  {r['相关系数']:>+8.4f}  {dir_rate:>12}  {large_rate:>14}")

    # 4. 按行业分组统计
    print(f"\n[4] 按行业分组统计")
    print("-" * 80)

    industry_corrs = defaultdict(list)
    for r in all_results:
        for group_name, industries in INDUSTRY_GROUPS.items():
            if r["行业"] in industries:
                industry_corrs[group_name].append(r["相关系数"])
                break
        else:
            industry_corrs["其他"].append(r["相关系数"])

    print(f"{'行业分组':>16} {'股票数':>6} {'平均相关系数':>12} {'最大|r|':>10} {'建议':>20}")
    print("-" * 80)
    for group_name in ["半导体/电子", "有色金属/矿业", "汽车产业链", "锂电/新能源", "医药", "化工", "其他"]:
        corrs = industry_corrs.get(group_name, [])
        if not corrs:
            continue
        avg_corr = sum(corrs) / len(corrs)
        max_abs = max(abs(c) for c in corrs)
        if avg_corr > 0.15:
            advice = "✅ 建议纳入美股信号"
        elif avg_corr > 0.08:
            advice = "⚠ 可选择性纳入"
        else:
            advice = "❌ 联动性弱，不建议"
        print(f"{group_name:>14}  {len(corrs):>5}  {avg_corr:>+12.4f}  {max_abs:>10.4f}  {advice}")

    # 5. 多指数对比
    print(f"\n[5] 三大美股指数对比（全部50只A股平均）")
    print("-" * 70)
    for idx_name, (_, idx_klines) in us_indices.items():
        if not idx_klines:
            print(f"  {idx_name}: 数据缺失")
            continue
        idx_map = build_us_date_map(idx_klines)
        corrs = []
        all_us = []
        all_a = []
        for code, name, industry in BACKTEST_STOCKS:
            klines = get_kline_data(code, start_date=START_DATE, end_date=END_DATE)
            klines = [k for k in klines if (k.get("trading_volume") or 0) > 0]
            for k in klines:
                prev_us = find_previous_us_trading_day(k["date"], idx_map)
                if prev_us:
                    all_us.append(idx_map[prev_us])
                    all_a.append(k.get("change_percent") or 0)
            r = analyze_single_stock(code, name, industry, idx_map, idx_name)
            if r:
                corrs.append(r["相关系数"])

        avg_corr = sum(corrs) / len(corrs) if corrs else 0
        pool_corr = pearson_corr(all_us, all_a) if all_us else 0
        pool_dir = directional_accuracy(all_us, all_a)
        pool_large = large_move_accuracy(all_us, all_a, 1.0)

        print(f"\n  {idx_name}:")
        print(f"    个股平均相关系数: {avg_corr:+.4f}")
        print(f"    池化相关系数:     {pool_corr:+.4f}")
        print(f"    方向一致性:       {pool_dir.get('方向一致率', 'N/A')}")
        print(f"    美股涨→A股涨:    {pool_dir.get('美股涨→A股涨', 'N/A')}")
        print(f"    美股跌→A股跌:    {pool_dir.get('美股跌→A股跌', 'N/A')}")
        print(f"    大幅波动(>1%)时:  {pool_large.get('方向一致率', 'N/A')}")

    # 6. 结论
    print("\n" + "=" * 70)
    print("分析结论")
    print("=" * 70)

    avg_all = sum(r["相关系数"] for r in all_results) / len(all_results)
    strong_corr = [r for r in all_results if abs(r["相关系数"]) > 0.15]
    weak_corr = [r for r in all_results if abs(r["相关系数"]) < 0.05]

    print(f"\n  全部50只股票平均相关系数: {avg_all:+.4f}")
    print(f"  强相关(|r|>0.15)股票数:  {len(strong_corr)}/{len(all_results)}")
    print(f"  弱相关(|r|<0.05)股票数:  {len(weak_corr)}/{len(all_results)}")

    if avg_all > 0.12:
        print("\n  ✅ 结论：美股隔夜走势与A股次日涨跌存在显著正相关，")
        print("     建议在外部环境维度中加入美股隔夜信号（权重1-2分）。")
        print("     特别是半导体/电子、有色金属板块，可给更高权重。")
    elif avg_all > 0.06:
        print("\n  ⚠ 结论：美股隔夜走势与A股次日涨跌存在弱正相关，")
        print("     可选择性地对高联动行业（半导体/有色金属）加入美股信号，")
        print("     但对内需消费类股票不建议使用。")
    else:
        print("\n  ❌ 结论：美股隔夜走势与A股次日涨跌相关性很弱，")
        print("     加入美股信号可能只是增加噪声，不建议纳入评分体系。")

    # 输出可直接用于评分系统的行业敏感度配置
    print("\n  行业敏感度配置建议（可用于评分系统）:")
    print("  US_SENSITIVITY = {")
    for group_name in ["半导体/电子", "有色金属/矿业", "汽车产业链", "锂电/新能源", "医药", "化工", "其他"]:
        corrs = industry_corrs.get(group_name, [])
        if not corrs:
            continue
        avg = sum(corrs) / len(corrs)
        if avg > 0.15:
            weight = 2
        elif avg > 0.08:
            weight = 1
        else:
            weight = 0
        print(f'      "{group_name}": {weight},  # avg_corr={avg:+.4f}')
    print("  }")

    # 保存详细结果到JSON
    output = {
        "分析时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "分析区间": f"{START_DATE} ~ {END_DATE}",
        "主要指数": primary_index,
        "全部股票平均相关系数": avg_all,
        "强相关股票数": len(strong_corr),
        "弱相关股票数": len(weak_corr),
        "个股详情": sorted_results,
        "行业分组": {
            g: {"平均相关系数": round(sum(c)/len(c), 4), "股票数": len(c)}
            for g, c in industry_corrs.items() if c
        },
    }
    out_path = "data_results/us_market_correlation_analysis.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n  详细结果已保存到: {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
