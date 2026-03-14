"""
板块差异化评分深度分析

基于回测数据，分析不同板块在7个评分维度上的表现差异，
验证是否需要对不同板块做个性化评分逻辑。

分析维度：
1. 各板块整体准确率差异
2. 各板块在7个评分维度上的得分分布与准确率关系
3. 各板块的波动率特征差异（高波动 vs 低波动）
4. 各板块的趋势维度有效性差异
5. 各板块的资金筹码维度有效性差异
6. 输出个性化评分权重建议

使用方法：
    python tests/test_sector_scoring_analysis.py
"""
import sys
import os
import json
import math
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
logging.basicConfig(level=logging.WARNING)

from dao.stock_kline_dao import get_kline_data

# ═══════════════════════════════════════════════════════════
# 板块分类（与美股相关性分析一致 + 更细分）
# ═══════════════════════════════════════════════════════════

STOCK_SECTOR_MAP = {
    "002440.SZ": ("闰土股份", "化工"),
    "002497.SZ": ("雅化集团", "化工"),
    "002772.SZ": ("众兴菌业", "农业"),
    "300666.SZ": ("江丰电子", "半导体"),
    "301312.SZ": ("智立方", "机器人"),
    "600686.SH": ("金龙汽车", "汽车"),
    "688336.SH": ("三生国健", "医药"),
    "002850.SZ": ("科达利", "锂电"),
    "002463.SZ": ("沪电股份", "电子"),
    "601127.SH": ("赛力斯", "汽车"),
    "603629.SH": ("利通电子", "电子"),
    "603979.SH": ("金诚信", "矿业"),
    "002150.SZ": ("正泰电源", "电力设备"),
    "002728.SZ": ("特一药业", "医药"),
    "002759.SZ": ("天际股份", "化工"),
    "002842.SZ": ("翔鹭钨业", "有色金属"),
    "300619.SZ": ("金银河", "锂电"),
    "300953.SZ": ("震裕科技", "汽车零部件"),
    "600549.SH": ("厦门钨业", "有色金属"),
    "603596.SH": ("伯特利", "汽车零部件"),
    "688278.SH": ("特宝生物", "医药"),
    "688519.SH": ("南亚新材", "电子"),
    "688578.SH": ("艾力斯", "医药"),
    "688617.SH": ("惠泰医疗", "医药"),
    "002001.SZ": ("新和成", "化工"),
    "002957.SZ": ("科瑞技术", "机器人"),
    "600066.SH": ("宇通客车", "汽车"),
    "600114.SH": ("东睦股份", "金属材料"),
    "600150.SH": ("中国船舶", "军工"),
    "600160.SH": ("巨化股份", "化工"),
    "300394.SZ": ("天孚通信", "光通信"),
    "002050.SZ": ("三花智控", "汽车零部件"),
    "688668.SH": ("鼎通科技", "电子"),
    "002155.SZ": ("湖南黄金", "有色金属"),
    "002378.SZ": ("章源钨业", "有色金属"),
    "002545.SZ": ("东方铁塔", "通信"),
    "600884.SH": ("杉杉股份", "锂电"),
    "601138.SH": ("工业富联", "电子"),
    "688008.SH": ("澜起科技", "半导体"),
    "688025.SH": ("杰普特", "激光"),
    "300124.SZ": ("汇川技术", "工控"),
    "002196.SZ": ("方正电机", "汽车零部件"),
    "002250.SZ": ("联化科技", "化工"),
    "002287.SZ": ("奇正藏药", "医药"),
    "002709.SZ": ("天赐材料", "锂电"),
    "600378.SH": ("昊华科技", "化工"),
    "600489.SH": ("中金黄金", "有色金属"),
    "601899.SH": ("紫金矿业", "有色金属"),
    "688019.SH": ("安集科技", "半导体"),
}

# 大板块分组
SECTOR_GROUPS = {
    "科技(半导体/电子/光通信)": ["半导体", "电子", "光通信", "通信", "激光"],
    "有色金属/矿业": ["有色金属", "矿业", "金属材料"],
    "汽车产业链": ["汽车", "汽车零部件"],
    "新能源(锂电/电力)": ["锂电", "电力设备"],
    "医药": ["医药"],
    "化工": ["化工"],
    "制造/其他": ["农业", "机器人", "军工", "工控"],
}

START_DATE = "2025-12-10"
END_DATE = "2026-03-10"


# ═══════════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════════

def parse_dim_score(dim_str: str) -> tuple[int, int]:
    """解析 '5/15' 格式的维度得分，返回 (得分, 满分)"""
    parts = dim_str.split("/")
    return int(parts[0]), int(parts[1])


def get_sector_group(industry: str) -> str:
    """获取行业所属的大板块"""
    for group, industries in SECTOR_GROUPS.items():
        if industry in industries:
            return group
    return "制造/其他"


def calc_volatility(klines: list[dict]) -> dict:
    """计算股票波动率特征"""
    changes = [k.get("change_percent") or 0 for k in klines
               if (k.get("trading_volume") or 0) > 0]
    if len(changes) < 10:
        return {}
    avg = sum(changes) / len(changes)
    std = math.sqrt(sum((c - avg) ** 2 for c in changes) / len(changes))
    abs_changes = [abs(c) for c in changes]
    avg_abs = sum(abs_changes) / len(abs_changes)
    max_up = max(changes)
    max_down = min(changes)
    # 涨跌不对称性：正收益天数占比
    up_days = sum(1 for c in changes if c > 0)
    up_ratio = up_days / len(changes)
    return {
        "日均波动率(%)": round(std, 3),
        "平均绝对涨跌(%)": round(avg_abs, 3),
        "最大单日涨幅(%)": round(max_up, 2),
        "最大单日跌幅(%)": round(max_down, 2),
        "上涨天数占比": round(up_ratio, 3),
        "交易天数": len(changes),
    }


# ═══════════════════════════════════════════════════════════
# 核心分析：从回测JSON提取维度级别数据
# ═══════════════════════════════════════════════════════════

def load_backtest_data() -> dict:
    """加载回测结果JSON"""
    path = "data_results/backtest_technical_50stocks_3m_result.json"
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def analyze_dimension_effectiveness(daily_details: list[dict]) -> dict:
    """分析每个维度得分与预测准确率的关系

    对每个维度，将得分分为高/中/低三档，统计各档的预测准确率。
    如果某维度高分时准确率反而低，说明该维度对该板块是负信号。
    """
    DIM_NAMES = ["趋势强度", "动能与量价", "结构边界", "短线情绪", "资金筹码", "外部环境", "风险收益比"]
    DIM_MAX = {"趋势强度": 15, "动能与量价": 23, "结构边界": 15,
               "短线情绪": 17, "资金筹码": 15, "外部环境": 5, "风险收益比": 10}

    results = {}
    for dim in DIM_NAMES:
        max_score = DIM_MAX[dim]
        high_threshold = max_score * 0.65
        low_threshold = max_score * 0.35

        buckets = {"高分": [], "中分": [], "低分": []}
        for d in daily_details:
            dims = d.get("维度", {})
            dim_str = dims.get(dim, "")
            if not dim_str or "/" not in dim_str:
                continue
            score, _ = parse_dim_score(dim_str)
            is_correct = d.get("宽松正确") == "✓"
            actual_chg_str = d.get("实际涨跌", "0%").replace("%", "").replace("+", "")
            try:
                actual_chg = float(actual_chg_str)
            except ValueError:
                actual_chg = 0

            entry = {"score": score, "correct": is_correct, "actual_chg": actual_chg}
            if score >= high_threshold:
                buckets["高分"].append(entry)
            elif score <= low_threshold:
                buckets["低分"].append(entry)
            else:
                buckets["中分"].append(entry)

        dim_result = {}
        for bucket_name, entries in buckets.items():
            n = len(entries)
            if n == 0:
                dim_result[bucket_name] = {"样本数": 0}
                continue
            correct_n = sum(1 for e in entries if e["correct"])
            avg_score = sum(e["score"] for e in entries) / n
            avg_chg = sum(e["actual_chg"] for e in entries) / n
            dim_result[bucket_name] = {
                "样本数": n,
                "准确率": round(correct_n / n * 100, 1),
                "平均得分": round(avg_score, 1),
                "平均实际涨跌(%)": round(avg_chg, 3),
            }

        # 计算维度有效性指标：高分准确率 - 低分准确率
        # 正值=维度有效（高分确实更准），负值=维度反向（高分反而不准）
        high_acc = dim_result["高分"].get("准确率", 50)
        low_acc = dim_result["低分"].get("准确率", 50)
        high_n = dim_result["高分"].get("样本数", 0)
        low_n = dim_result["低分"].get("样本数", 0)
        if high_n >= 5 and low_n >= 5:
            effectiveness = round(high_acc - low_acc, 1)
        else:
            effectiveness = None

        results[dim] = {
            "分档统计": dim_result,
            "有效性(高分-低分准确率差)": effectiveness,
        }

    return results


def analyze_score_direction_bias(daily_details: list[dict]) -> dict:
    """分析评分系统的方向偏差

    检查：
    1. 预测上涨 vs 预测下跌的比例是否合理
    2. 高分区间是否真的对应上涨
    3. 是否存在系统性偏多或偏空
    """
    total = len(daily_details)
    if total == 0:
        return {}

    pred_up = sum(1 for d in daily_details if d.get("预测方向") == "上涨")
    pred_down = sum(1 for d in daily_details if d.get("预测方向") == "下跌")
    actual_up = sum(1 for d in daily_details
                    if float(d.get("实际涨跌", "0%").replace("%", "").replace("+", "")) > 0.3)
    actual_down = sum(1 for d in daily_details
                      if float(d.get("实际涨跌", "0%").replace("%", "").replace("+", "")) < -0.3)

    # 高分(>=55)预测上涨的准确率
    high_score_up = [d for d in daily_details
                     if d.get("评分", 0) >= 55 and d.get("预测方向") == "上涨"]
    high_score_up_correct = sum(1 for d in high_score_up if d.get("宽松正确") == "✓")

    # 低分(<45)预测下跌的准确率
    low_score_down = [d for d in daily_details
                      if d.get("评分", 0) < 45 and d.get("预测方向") == "下跌"]
    low_score_down_correct = sum(1 for d in low_score_down if d.get("宽松正确") == "✓")

    return {
        "总样本": total,
        "预测上涨占比": round(pred_up / total * 100, 1),
        "预测下跌占比": round(pred_down / total * 100, 1),
        "实际上涨占比": round(actual_up / total * 100, 1),
        "实际下跌占比": round(actual_down / total * 100, 1),
        "预测偏差": "偏空" if pred_down / total > 0.6 else ("偏多" if pred_up / total > 0.6 else "均衡"),
        "高分看涨准确率": f"{high_score_up_correct}/{len(high_score_up)} ({round(high_score_up_correct/len(high_score_up)*100,1)}%)" if high_score_up else "N/A",
        "低分看跌准确率": f"{low_score_down_correct}/{len(low_score_down)} ({round(low_score_down_correct/len(low_score_down)*100,1)}%)" if low_score_down else "N/A",
    }


def analyze_volatility_vs_accuracy(daily_details: list[dict], kline_volatility: dict) -> dict:
    """分析波动率与准确率的关系"""
    vol = kline_volatility.get("日均波动率(%)", 0)
    avg_abs = kline_volatility.get("平均绝对涨跌(%)", 0)

    if not daily_details:
        return {}

    total = len(daily_details)
    correct = sum(1 for d in daily_details if d.get("宽松正确") == "✓")
    strict_correct = sum(1 for d in daily_details if d.get("严格正确") == "✓")

    # 按当日实际波动大小分组
    small_move = [d for d in daily_details
                  if abs(float(d.get("实际涨跌", "0%").replace("%", "").replace("+", ""))) < 1.0]
    large_move = [d for d in daily_details
                  if abs(float(d.get("实际涨跌", "0%").replace("%", "").replace("+", ""))) >= 2.0]

    small_correct = sum(1 for d in small_move if d.get("宽松正确") == "✓")
    large_correct = sum(1 for d in large_move if d.get("宽松正确") == "✓")

    return {
        "日均波动率(%)": vol,
        "平均绝对涨跌(%)": avg_abs,
        "总准确率(宽松)": round(correct / total * 100, 1) if total else 0,
        "总准确率(严格)": round(strict_correct / total * 100, 1) if total else 0,
        "小幅波动(<1%)准确率": round(small_correct / len(small_move) * 100, 1) if small_move else None,
        "大幅波动(>=2%)准确率": round(large_correct / len(large_move) * 100, 1) if large_move else None,
        "小幅波动样本数": len(small_move),
        "大幅波动样本数": len(large_move),
    }


def analyze_consecutive_pattern(daily_details: list[dict]) -> dict:
    """分析连涨/连跌后的预测准确率

    不同板块对连涨/连跌的反应不同：
    - 趋势型板块（有色金属）：连涨后继续涨的概率高
    - 均值回归型板块（医药）：连涨后回调概率高
    """
    if len(daily_details) < 5:
        return {}

    # 按股票分组，找连涨/连跌模式
    by_stock = defaultdict(list)
    for d in daily_details:
        by_stock[d["代码"]].append(d)

    after_3up = []   # 连涨3天后的预测
    after_3down = []  # 连跌3天后的预测
    after_big_up = []  # 单日大涨(>3%)后的预测
    after_big_down = []  # 单日大跌(<-3%)后的预测

    for code, details in by_stock.items():
        details.sort(key=lambda x: x["评分日"])
        for i in range(3, len(details)):
            # 检查前3天是否连涨/连跌
            prev_changes = []
            for j in range(i - 3, i):
                chg_str = details[j].get("实际涨跌", "0%").replace("%", "").replace("+", "")
                try:
                    prev_changes.append(float(chg_str))
                except ValueError:
                    prev_changes.append(0)

            if all(c > 0 for c in prev_changes):
                after_3up.append(details[i])
            elif all(c < 0 for c in prev_changes):
                after_3down.append(details[i])

        # 单日大幅波动后
        for i in range(1, len(details)):
            prev_chg_str = details[i - 1].get("实际涨跌", "0%").replace("%", "").replace("+", "")
            try:
                prev_chg = float(prev_chg_str)
            except ValueError:
                continue
            if prev_chg > 3:
                after_big_up.append(details[i])
            elif prev_chg < -3:
                after_big_down.append(details[i])

    def _acc(items):
        if not items:
            return "N/A"
        n = len(items)
        ok = sum(1 for d in items if d.get("宽松正确") == "✓")
        return f"{ok}/{n} ({round(ok/n*100,1)}%)"

    return {
        "连涨3天后准确率": _acc(after_3up),
        "连涨3天后样本数": len(after_3up),
        "连跌3天后准确率": _acc(after_3down),
        "连跌3天后样本数": len(after_3down),
        "大涨(>3%)后准确率": _acc(after_big_up),
        "大涨后样本数": len(after_big_up),
        "大跌(<-3%)后准确率": _acc(after_big_down),
        "大跌后样本数": len(after_big_down),
    }


# ═══════════════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════════════

def main():
    print("=" * 80)
    print("板块差异化评分深度分析")
    print(f"分析区间: {START_DATE} ~ {END_DATE}")
    print("=" * 80)

    # 1. 加载回测数据
    print("\n[1] 加载回测数据...")
    bt_data = load_backtest_data()
    daily_details = bt_data.get("逐日详情", [])
    stock_summaries = bt_data.get("各股票汇总", [])
    print(f"  总样本数: {len(daily_details)}, 股票数: {len(stock_summaries)}")

    # 2. 按板块分组
    print("\n[2] 按板块分组...")
    sector_daily = defaultdict(list)  # 大板块 -> 逐日详情列表
    stock_sector = {}  # 股票代码 -> 大板块

    for d in daily_details:
        code = d["代码"]
        info = STOCK_SECTOR_MAP.get(code)
        if not info:
            continue
        _, industry = info
        group = get_sector_group(industry)
        sector_daily[group].append(d)
        stock_sector[code] = group

    for group, details in sorted(sector_daily.items(), key=lambda x: -len(x[1])):
        codes = set(d["代码"] for d in details)
        print(f"  {group}: {len(codes)}只股票, {len(details)}条样本")

    # 3. 各板块整体准确率
    print("\n" + "=" * 80)
    print("[3] 各板块整体准确率对比")
    print("-" * 80)
    print(f"{'板块':>22} {'样本数':>6} {'宽松准确率':>12} {'严格准确率':>12} {'平均评分':>8} {'平均实际涨跌':>12}")
    print("-" * 80)

    sector_acc = {}
    for group in SECTOR_GROUPS:
        details = sector_daily.get(group, [])
        if not details:
            continue
        n = len(details)
        loose_ok = sum(1 for d in details if d.get("宽松正确") == "✓")
        strict_ok = sum(1 for d in details if d.get("严格正确") == "✓")
        avg_score = sum(d.get("评分", 0) for d in details) / n
        avg_chg_vals = []
        for d in details:
            try:
                avg_chg_vals.append(float(d.get("实际涨跌", "0%").replace("%", "").replace("+", "")))
            except ValueError:
                pass
        avg_chg = sum(avg_chg_vals) / len(avg_chg_vals) if avg_chg_vals else 0

        loose_pct = round(loose_ok / n * 100, 1)
        strict_pct = round(strict_ok / n * 100, 1)
        sector_acc[group] = {"loose": loose_pct, "strict": strict_pct, "n": n}

        print(f"{group:>20}  {n:>5}  {loose_ok}/{n} ({loose_pct}%)  "
              f"{strict_ok}/{n} ({strict_pct}%)  {avg_score:>6.1f}  {avg_chg:>+10.3f}%")

    # 4. 各板块维度有效性分析
    print("\n" + "=" * 80)
    print("[4] 各板块维度有效性分析（高分准确率 - 低分准确率）")
    print("    正值=维度有效, 负值=维度反向, None=样本不足")
    print("-" * 110)

    DIM_NAMES = ["趋势强度", "动能与量价", "结构边界", "短线情绪", "资金筹码", "外部环境", "风险收益比"]
    header = f"{'板块':>22}"
    for dim in DIM_NAMES:
        header += f" {dim[:4]:>7}"
    print(header)
    print("-" * 110)

    sector_dim_effectiveness = {}
    for group in SECTOR_GROUPS:
        details = sector_daily.get(group, [])
        if not details:
            continue
        dim_eff = analyze_dimension_effectiveness(details)
        sector_dim_effectiveness[group] = dim_eff

        row = f"{group:>20}"
        for dim in DIM_NAMES:
            eff = dim_eff.get(dim, {}).get("有效性(高分-低分准确率差)")
            if eff is None:
                row += f" {'N/A':>7}"
            elif eff > 5:
                row += f" {eff:>+6.1f}✓"
            elif eff < -5:
                row += f" {eff:>+6.1f}✗"
            else:
                row += f" {eff:>+6.1f}~"
        print(row)

    # 5. 各板块维度详细分档数据
    print("\n" + "=" * 80)
    print("[5] 各板块维度分档详细数据")
    for group in SECTOR_GROUPS:
        details = sector_daily.get(group, [])
        if not details:
            continue
        dim_eff = sector_dim_effectiveness.get(group, {})
        print(f"\n  ── {group} ({len(details)}样本) ──")
        for dim in DIM_NAMES:
            de = dim_eff.get(dim, {})
            eff = de.get("有效性(高分-低分准确率差)")
            buckets = de.get("分档统计", {})
            eff_str = f"{eff:+.1f}" if eff is not None else "N/A"
            print(f"    {dim}(有效性:{eff_str}):", end="")
            for bname in ["高分", "中分", "低分"]:
                b = buckets.get(bname, {})
                n = b.get("样本数", 0)
                acc = b.get("准确率", 0)
                avg_s = b.get("平均得分", 0)
                if n > 0:
                    print(f"  {bname}({n}样本,准确率{acc}%,均分{avg_s})", end="")
            print()

    # 6. 波动率特征与准确率关系
    print("\n" + "=" * 80)
    print("[6] 各板块波动率特征与准确率关系")
    print("-" * 100)
    print(f"{'板块':>22} {'日均波动率':>10} {'平均|涨跌|':>10} {'总准确率':>8} "
          f"{'小波动准确率':>12} {'大波动准确率':>12} {'波动率类型':>10}")
    print("-" * 100)

    sector_vol_type = {}
    for group in SECTOR_GROUPS:
        details = sector_daily.get(group, [])
        if not details:
            continue
        # 获取该板块所有股票的K线数据计算波动率
        codes = set(d["代码"] for d in details)
        all_klines = []
        for code in codes:
            klines = get_kline_data(code, start_date=START_DATE, end_date=END_DATE)
            all_klines.extend(klines)

        vol_info = calc_volatility(all_klines)
        vol_acc = analyze_volatility_vs_accuracy(details, vol_info)

        vol_type = "高波动" if vol_info.get("日均波动率(%)", 0) > 2.5 else (
            "中波动" if vol_info.get("日均波动率(%)", 0) > 1.8 else "低波动")
        sector_vol_type[group] = vol_type

        print(f"{group:>20}  {vol_info.get('日均波动率(%)', 0):>8.3f}%  "
              f"{vol_info.get('平均绝对涨跌(%)', 0):>8.3f}%  "
              f"{vol_acc.get('总准确率(宽松)', 0):>6.1f}%  "
              f"{vol_acc.get('小幅波动(<1%)准确率', 'N/A'):>10}  "
              f"{vol_acc.get('大幅波动(>=2%)准确率', 'N/A'):>10}  "
              f"{vol_type:>8}")

    # 7. 连涨/连跌模式分析
    print("\n" + "=" * 80)
    print("[7] 各板块连涨/连跌后预测准确率（趋势型 vs 均值回归型）")
    print("-" * 100)
    print(f"{'板块':>22} {'连涨3天后':>14} {'连跌3天后':>14} {'大涨后':>14} {'大跌后':>14} {'板块特性':>10}")
    print("-" * 100)

    sector_pattern_type = {}
    for group in SECTOR_GROUPS:
        details = sector_daily.get(group, [])
        if not details:
            continue
        pattern = analyze_consecutive_pattern(details)
        if not pattern:
            continue

        # 判断板块特性
        up3_str = pattern.get("连涨3天后准确率", "N/A")
        down3_str = pattern.get("连跌3天后准确率", "N/A")

        # 解析准确率数值
        def _parse_acc(s):
            if "N/A" in str(s) or not s:
                return None
            try:
                return float(s.split("(")[1].replace("%)", ""))
            except (IndexError, ValueError):
                return None

        up3_acc = _parse_acc(up3_str)
        down3_acc = _parse_acc(down3_str)

        if up3_acc is not None and down3_acc is not None:
            # 连涨后准确率高=趋势延续型，连跌后准确率高=趋势延续型
            # 连涨后准确率低=均值回归型
            if up3_acc > 55 and down3_acc > 55:
                ptype = "趋势型"
            elif up3_acc < 45 and down3_acc < 45:
                ptype = "反转型"
            else:
                ptype = "混合型"
        else:
            ptype = "数据不足"
        sector_pattern_type[group] = ptype

        print(f"{group:>20}  {pattern.get('连涨3天后准确率', 'N/A'):>12}  "
              f"{pattern.get('连跌3天后准确率', 'N/A'):>12}  "
              f"{pattern.get('大涨(>3%)后准确率', 'N/A'):>12}  "
              f"{pattern.get('大跌(<-3%)后准确率', 'N/A'):>12}  "
              f"{ptype:>8}")

    # 8. 方向偏差分析
    print("\n" + "=" * 80)
    print("[8] 各板块评分方向偏差分析")
    print("-" * 100)
    print(f"{'板块':>22} {'预测上涨%':>10} {'预测下跌%':>10} {'实际上涨%':>10} {'实际下跌%':>10} "
          f"{'偏差':>6} {'高分看涨准确率':>16} {'低分看跌准确率':>16}")
    print("-" * 100)

    for group in SECTOR_GROUPS:
        details = sector_daily.get(group, [])
        if not details:
            continue
        bias = analyze_score_direction_bias(details)
        if not bias:
            continue
        print(f"{group:>20}  {bias.get('预测上涨占比', 0):>8.1f}%  "
              f"{bias.get('预测下跌占比', 0):>8.1f}%  "
              f"{bias.get('实际上涨占比', 0):>8.1f}%  "
              f"{bias.get('实际下跌占比', 0):>8.1f}%  "
              f"{bias.get('预测偏差', ''):>5}  "
              f"{bias.get('高分看涨准确率', 'N/A'):>14}  "
              f"{bias.get('低分看跌准确率', 'N/A'):>14}")

    # ═══════════════════════════════════════════════════════════
    # 9. 综合结论与个性化权重建议
    # ═══════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("[9] 综合结论与个性化评分权重建议")
    print("=" * 80)

    # 当前默认权重
    DEFAULT_WEIGHTS = {
        "趋势强度": 15, "动能与量价": 23, "结构边界": 15,
        "短线情绪": 17, "资金筹码": 15, "外部环境": 5, "风险收益比": 10,
    }

    for group in SECTOR_GROUPS:
        dim_eff = sector_dim_effectiveness.get(group, {})
        vol_type = sector_vol_type.get(group, "中波动")
        pattern_type = sector_pattern_type.get(group, "混合型")
        acc_info = sector_acc.get(group, {})

        if not dim_eff:
            continue

        print(f"\n  ── {group} ──")
        print(f"    当前准确率: 宽松{acc_info.get('loose', 0)}% / 严格{acc_info.get('strict', 0)}%")
        print(f"    波动率类型: {vol_type}")
        print(f"    板块特性:   {pattern_type}")

        # 生成个性化权重建议
        suggested_weights = dict(DEFAULT_WEIGHTS)
        adjustments = []

        for dim in DIM_NAMES:
            eff = dim_eff.get(dim, {}).get("有效性(高分-低分准确率差)")
            if eff is None:
                continue

            if eff < -10:
                # 维度严重反向，大幅降权
                reduction = min(suggested_weights[dim] // 2, 5)
                suggested_weights[dim] -= reduction
                adjustments.append(f"    ⚠ {dim}: 有效性{eff:+.1f}（反向），建议降权{reduction}分 → {suggested_weights[dim]}分")
            elif eff < -3:
                # 维度轻微反向，小幅降权
                reduction = min(suggested_weights[dim] // 3, 3)
                suggested_weights[dim] -= reduction
                adjustments.append(f"    ⚠ {dim}: 有效性{eff:+.1f}（弱反向），建议降权{reduction}分 → {suggested_weights[dim]}分")
            elif eff > 15:
                # 维度非常有效，加权
                addition = min(5, 100 - sum(suggested_weights.values()))
                if addition > 0:
                    suggested_weights[dim] += addition
                    adjustments.append(f"    ✅ {dim}: 有效性{eff:+.1f}（强有效），建议加权{addition}分 → {suggested_weights[dim]}分")
            elif eff > 8:
                addition = min(3, 100 - sum(suggested_weights.values()))
                if addition > 0:
                    suggested_weights[dim] += addition
                    adjustments.append(f"    ✅ {dim}: 有效性{eff:+.1f}（有效），建议加权{addition}分 → {suggested_weights[dim]}分")

        # 波动率相关调整
        if vol_type == "高波动":
            if suggested_weights["风险收益比"] < 13:
                old = suggested_weights["风险收益比"]
                suggested_weights["风险收益比"] = min(13, old + 3)
                adjustments.append(f"    📊 高波动板块：风险收益比加权 {old}→{suggested_weights['风险收益比']}分")
        elif vol_type == "低波动":
            if suggested_weights["动能与量价"] > 20:
                old = suggested_weights["动能与量价"]
                suggested_weights["动能与量价"] = max(18, old - 3)
                adjustments.append(f"    📊 低波动板块：动能与量价降权 {old}→{suggested_weights['动能与量价']}分（低波动时量价信号弱）")

        # 趋势型 vs 反转型调整
        if pattern_type == "趋势型":
            adjustments.append(f"    📈 趋势型板块：趋势维度保持或加权，超跌反弹修正力度减小")
        elif pattern_type == "反转型":
            adjustments.append(f"    📉 反转型板块：趋势维度降权，超跌反弹修正力度加大")

        if adjustments:
            print("    个性化调整建议:")
            for adj in adjustments:
                print(adj)
        else:
            print("    当前权重基本合理，无需大幅调整")

        # 输出建议权重
        total_w = sum(suggested_weights.values())
        if total_w != 100:
            # 归一化到100
            factor = 100 / total_w
            suggested_weights = {k: round(v * factor) for k, v in suggested_weights.items()}
            # 微调确保总和=100
            diff = 100 - sum(suggested_weights.values())
            if diff != 0:
                max_dim = max(suggested_weights, key=suggested_weights.get)
                suggested_weights[max_dim] += diff

        print(f"    建议权重: {suggested_weights}")
        print(f"    默认权重: {DEFAULT_WEIGHTS}")

    # 10. 是否需要板块划分的最终结论
    print("\n" + "=" * 80)
    print("[10] 最终结论：是否需要板块差异化评分")
    print("=" * 80)

    # 计算板块间准确率方差
    acc_values = [v["loose"] for v in sector_acc.values()]
    if acc_values:
        acc_mean = sum(acc_values) / len(acc_values)
        acc_var = sum((a - acc_mean) ** 2 for a in acc_values) / len(acc_values)
        acc_std = math.sqrt(acc_var)
        acc_range = max(acc_values) - min(acc_values)

        print(f"\n  板块间准确率统计:")
        print(f"    平均准确率: {acc_mean:.1f}%")
        print(f"    标准差:     {acc_std:.1f}%")
        print(f"    极差:       {acc_range:.1f}%")

        # 统计维度有效性差异
        dim_eff_variance = {}
        for dim in DIM_NAMES:
            effs = []
            for group in SECTOR_GROUPS:
                de = sector_dim_effectiveness.get(group, {})
                eff = de.get(dim, {}).get("有效性(高分-低分准确率差)")
                if eff is not None:
                    effs.append(eff)
            if effs:
                eff_range = max(effs) - min(effs)
                dim_eff_variance[dim] = eff_range

        print(f"\n  各维度在不同板块间的有效性极差:")
        for dim, eff_range in sorted(dim_eff_variance.items(), key=lambda x: -x[1]):
            flag = "⚠ 差异大" if eff_range > 20 else ("~ 差异中" if eff_range > 10 else "✓ 差异小")
            print(f"    {dim}: {eff_range:.1f}pp  {flag}")

        # 最终判断
        high_variance_dims = sum(1 for v in dim_eff_variance.values() if v > 15)
        if acc_range > 15 or high_variance_dims >= 3:
            print(f"\n  ✅ 结论：强烈建议进行板块差异化评分")
            print(f"     板块间准确率极差{acc_range:.1f}%，{high_variance_dims}个维度存在显著板块差异。")
            print(f"     建议实现方案：在 _compute_comprehensive_score 中增加 sector 参数，")
            print(f"     根据板块类型调整各维度权重和阈值。")
        elif acc_range > 8 or high_variance_dims >= 2:
            print(f"\n  ⚠ 结论：建议对差异最大的板块做针对性调整")
            print(f"     板块间准确率极差{acc_range:.1f}%，{high_variance_dims}个维度存在中等板块差异。")
            print(f"     建议先对准确率最低的板块做专项优化。")
        else:
            print(f"\n  ❌ 结论：板块差异不显著，当前统一评分逻辑基本可行")
            print(f"     板块间准确率极差仅{acc_range:.1f}%，维度有效性差异不大。")

    # 保存结果
    output = {
        "分析时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "分析区间": f"{START_DATE} ~ {END_DATE}",
        "板块准确率": sector_acc,
        "板块波动率类型": sector_vol_type,
        "板块特性": sector_pattern_type,
        "维度有效性": {
            group: {
                dim: {
                    "有效性": de.get(dim, {}).get("有效性(高分-低分准确率差)"),
                    "分档": de.get(dim, {}).get("分档统计", {}),
                }
                for dim in DIM_NAMES
            }
            for group, de in sector_dim_effectiveness.items()
        },
    }
    out_path = "data_results/sector_scoring_analysis.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n  详细结果已保存到: {out_path}")


if __name__ == "__main__":
    main()
