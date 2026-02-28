import json
from datetime import datetime

from common.utils.stock_info_utils import StockInfo
from service.eastmoney.strategy_engine.stock_MACD_rule import get_macd_signals_macd_only, get_macd_signals_cn, \
    calculate_macd_signals, _build_dataframe
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_kline_cn, \
    get_stock_day_range_kline_by_db_cache


def _pre_compute_risk_metrics(stock_day_kline: list[dict], macd_detail: list[dict]) -> dict:
    """
    预计算风控所需的关键衍生指标，消除 LLM 计算幻觉。
    基于最新交易日往前推算。
    """
    if len(stock_day_kline) < 4 or len(macd_detail) < 4:
        return {}

    latest = stock_day_kline[0]
    prev_3 = stock_day_kline[1:4]

    # 前3日平均成交量
    avg_vol_3 = round(sum(d['成交量（手）'] for d in prev_3) / 3, 2)
    # 前3日最低价（日内最低）
    min_low_3 = min(d['最低价'] for d in prev_3)
    # 量比（最新成交量 / 前3日均量）
    vol_ratio = round(latest['成交量（手）'] / avg_vol_3, 2) if avg_vol_3 > 0 else 0

    # MACD 柱体连续变化方向
    hist_values = [d['MACD柱'] for d in macd_detail[:5]]
    hist_trend = []
    for i in range(len(hist_values) - 1):
        delta = round(hist_values[i] - hist_values[i + 1], 4)
        hist_trend.append(delta)

    # DIF 近5日变化率
    dif_values = [d['DIF'] for d in macd_detail[:5]]
    dif_trend = []
    for i in range(len(dif_values) - 1):
        dif_trend.append(round(dif_values[i] - dif_values[i + 1], 4))

    # DIF 距零轴百分比（相对于股价的比例，衡量零轴距离的实际意义）
    dif_zero_pct = round(macd_detail[0]['DIF'] / latest['收盘价'] * 100, 4) if latest['收盘价'] > 0 else 0

    # 最新交易日是否为大阴线
    is_big_drop = latest['涨跌幅(%)'] <= -3.0
    # 放量判定（量比 > 1.3 视为放量）
    is_vol_amplified = vol_ratio > 1.3
    # 断头破位判定
    is_break_below = latest['收盘价'] < min_low_3

    # 近5日涨跌幅序列（用于判断连续阴线等模式）
    recent_changes = [d['涨跌幅(%)'] for d in stock_day_kline[:5]]

    # ---- 以下为消除 LLM 自行计算而新增的预计算字段 ----
    # 近5日累计涨跌幅（简单求和）
    recent_5d_total_change = round(sum(recent_changes), 2)
    # DIF近4日累计变化量
    dif_4d_total_change = round(sum(dif_trend), 4) if dif_trend else 0
    # DIF近4日下行天数（变化量为负的天数）
    dif_4d_down_days = sum(1 for d in dif_trend if d < 0)
    # 近5日跌幅超2%次数
    recent_5d_big_drop_count = sum(1 for c in recent_changes if c <= -2.0)

    return {
        '最新交易日': latest['日期'],
        '最新收盘价': latest['收盘价'],
        '最新涨跌幅(%)': latest['涨跌幅(%)'],
        '量比（最新/前3日均量）': vol_ratio,
        '前3日最低价（日内低点）': min_low_3,
        '是否大阴线（跌幅>3%）': is_big_drop,
        '是否放量（量比>1.3）': is_vol_amplified,
        '是否断头破位（收盘<前3日最低价）': is_break_below,
        'MACD柱体近4日逐日变化量': hist_trend,
        'DIF近4日逐日变化量': dif_trend,
        'DIF距零轴占股价比(%)': dif_zero_pct,
        '近5日涨跌幅序列': recent_changes,
        '近5日累计涨跌幅(%)': recent_5d_total_change,
        'DIF近4日累计变化量': dif_4d_total_change,
        'DIF近4日下行天数': dif_4d_down_days,
        '近5日跌幅超2%次数': recent_5d_big_drop_count,
    }


def _compute_divergence_summary(stock_info: StockInfo, klines_raw: list[str]) -> dict:
    """
    使用 calculate_macd_signals 的完整背离检测算法，
    提取预计算的顶/底背离信号，避免 LLM 自行判断背离。
    """
    import pandas as pd
    if not klines_raw:
        return {'底背离': [], '顶背离': [], '当前波段状态': '未知'}

    df = _build_dataframe(klines_raw)
    df = calculate_macd_signals(df)

    # 提取最近的底背离
    bottom_div = df[df['Bottom_Divergence']].sort_index(ascending=False).head(3)
    bottom_list = []
    for d, r in bottom_div.iterrows():
        bottom_list.append({
            '日期': d.strftime('%Y-%m-%d'),
            '收盘价': round(r['close'], 2),
            '最低价': round(r['low'], 2),
            'DIF': round(r['DIF'], 4),
        })

    # 提取最近的顶背离
    top_div = df[df['Top_Divergence']].sort_index(ascending=False).head(3)
    top_list = []
    for d, r in top_div.iterrows():
        top_list.append({
            '日期': d.strftime('%Y-%m-%d'),
            '收盘价': round(r['close'], 2),
            '最高价': round(r['high'], 2),
            'DIF': round(r['DIF'], 4),
        })

    # 当前波段状态
    latest = df.iloc[-1]
    if latest['DIF'] > latest['DEA']:
        band_state = '多头波段（DIF > DEA）'
    else:
        band_state = '空头波段（DIF < DEA）'

    # 当前波段内是否正在形成背离（检查最近10日）
    recent = df.tail(10)
    forming_bottom = recent['Bottom_Divergence'].any()
    forming_top = recent['Top_Divergence'].any()

    return {
        '底背离历史（最近3次）': bottom_list,
        '顶背离历史（最近3次）': top_list,
        '当前波段状态': band_state,
        '近10日内触发底背离': forming_bottom,
        '近10日内触发顶背离': forming_top,
    }


def _compute_cross_history_with_outcome(stock_info: StockInfo, klines_raw: list[str]) -> dict:
    """
    回溯近3次金叉和死叉后的实际走势，预计算胜率，
    避免 LLM 自行从明细数据中推算。
    """
    import pandas as pd
    if not klines_raw:
        return {'近3次金叉走势': [], '近3次死叉走势': [], '金叉胜率': '无数据', '死叉胜率': '无数据'}

    df = _build_dataframe(klines_raw)
    df = calculate_macd_signals(df)

    def _evaluate_cross(cross_col, direction='up', lookforward=10):
        """评估交叉后 lookforward 日的走势"""
        crosses = df[df[cross_col]].sort_index(ascending=False).head(3)
        results = []
        win_count = 0
        total = 0
        for cross_date, row in crosses.iterrows():
            idx = df.index.get_loc(cross_date)
            future = df.iloc[idx:idx + lookforward + 1]
            if len(future) < 2:
                continue
            entry_price = future.iloc[0]['close']
            exit_price = future.iloc[-1]['close'] if len(future) > lookforward else future.iloc[-1]['close']
            pnl_pct = round((exit_price - entry_price) / entry_price * 100, 2)

            is_win = bool((pnl_pct > 0) if direction == 'up' else (pnl_pct < 0))
            if is_win:
                win_count += 1
            total += 1

            results.append({
                '交叉日期': cross_date.strftime('%Y-%m-%d'),
                '交叉价格': round(entry_price, 2),
                '0轴位置': '0轴上' if row['DIF'] > 0 and row['DEA'] > 0 else ('弱多区' if row['DIF'] > 0 else '0轴下'),
                f'{lookforward}日后收盘价': round(exit_price, 2),
                f'{lookforward}日收益率(%)': pnl_pct,
                '是否有效': is_win,
            })

        win_rate = f"{win_count}/{total}（{round(win_count / total * 100)}%）" if total > 0 else '无数据'
        return results, win_rate

    golden_results, golden_wr = _evaluate_cross('Golden_Cross', direction='up')
    death_results, death_wr = _evaluate_cross('Death_Cross', direction='down')

    return {
        '近3次金叉走势': golden_results,
        '金叉胜率': golden_wr,
        '近3次死叉走势': death_results,
        '死叉胜率': death_wr,
    }


async def get_stock_indicator_macd_prompt(stock_info: StockInfo):
    data_num = 30
    macd_signals = await get_macd_signals_macd_only(stock_info, data_num)
    macd_detail = macd_signals.get('明细数据', [])

    # 获取完整 K 线数据（预计算函数内部需要全部字段）
    stock_day_kline_full = await get_stock_day_kline_cn(stock_info, data_num)

    # 预计算风控衍生指标（使用完整 K 线）
    risk_metrics = _pre_compute_risk_metrics(stock_day_kline_full, macd_detail)

    # 仅保留 LLM 实际使用的字段输出到 prompt（日期、收盘价、涨跌幅）
    _keep = {'日期', '收盘价', '涨跌幅(%)'}
    stock_day_kline = [{k: v for k, v in r.items() if k in _keep}
                       for r in stock_day_kline_full]

    # 预计算背离信号（使用完整算法，非 LLM 推测）
    klines_raw = await get_stock_day_range_kline_by_db_cache(stock_info, limit=120)
    divergence_summary = _compute_divergence_summary(stock_info, klines_raw)

    # 预计算交叉历史胜率
    cross_history = _compute_cross_history_with_outcome(stock_info, klines_raw)

    # 构建 MACD 摘要（利用 get_macd_signals_macd_only 已计算的结构化信号）
    macd_summary = {k: v for k, v in macd_signals.items() if k != '明细数据'}

    return f"""
# 当前时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

# Role: 资深A股技术面分析师 / MACD专项与量价量化分析师
你拥有20年实战交易经验，精通 MACD 指标的底层逻辑与滞后性缺陷，擅长结合 K 线量价进行"指标去伪存真"。你的分析风格：客观、冷酷、严谨、直击本质、极度风险厌恶、拒绝任何模棱两可和"和稀泥"。

## Task: 股票 {stock_info.stock_name}（{stock_info.stock_code_normalize}） MACD 专项深度与防滞后性分析

---

## 数据使用铁律（最高优先级）

1. **禁止计算幻觉**：所有衍生指标（前3日均量、前3日最低价、量比、DIF变化率、背离信号、交叉胜率）均已在【预计算风控指标】和【预计算背离信号】中提供，**必须直接引用，严禁自行重新计算**。
2. **背离判定必须引用预计算结果**：底背离/顶背离由状态机算法预先检测（基于120日数据、20-60日窗口约束），LLM 仅负责解读其含义，**严禁自行从30日明细中"目测"背离**。
3. **交叉胜率必须引用预计算结果**：金叉/死叉后10日实际走势和胜率已预先回测，**严禁自行编造胜率数据**。

---

## Analysis Framework (核心分析框架)

### 一、MACD 多空与位置定调

**判定规则**：
- DIF > 0 且 DEA > 0 → 强多头
- DIF > 0 且 DEA ≤ 0 → 弱多头
- DIF < 0 → 空头

**分析要求**：
1. 读取【MACD摘要信号】中的市场状态，确认当前多空定位。
2. **零轴引力/斥力评估**：读取【预计算风控指标】中的"DIF距零轴占股价比(%)"，判定 DIF 距零轴的实际距离。
   - 若 DIF 距零轴占股价比 < 2%，视为"贴近零轴"，需高度警惕方向选择。
   - 结合"DIF近4日逐日变化量"和"DIF近4日下行天数"判断 DIF 的运动方向（向上远离/向下逼近零轴）。**严禁自行对变化量序列计数，必须直接引用"DIF近4日下行天数"**。
3. **零轴突破/受阻判定**：当 DIF 从下方靠近 0 轴时，结合最新 K 线涨跌幅：
   - 若最新日为阳线且 DIF 变化量为正 → 有效突破倾向
   - 若最新日为阴线或 DIF 变化量为负 → 零轴受阻倾向

### 二、动能演变与"价标背离"检验（⚠️第一核心预警）

**分析要求**：
1. **柱体趋势**：读取【预计算风控指标】中的"MACD柱体近4日逐日变化量"：
   - 变化量持续为正 → 绿柱收窄或红柱放大（多头动能增强）
   - 变化量持续为负 → 红柱收窄或绿柱放大（空头动能增强）
   - 变化量方向不一致 → 动能震荡，方向不明
2. **价标背离排查（指标滞后性核心检验）**：
   - 对比最新交易日的【涨跌幅】与【MACD柱体变化方向】：
     - 股价阳线 + 绿柱放大 = ⚠️ 价涨标跌背离（多头陷阱风险）
     - 股价阴线 + 红柱放大 = ⚠️ 价跌标涨背离（指标钝化风险）
     - 股价方向与柱体方向一致 = ✅ 无价标背离
   - **进阶检验**：直接读取【预计算风控指标】中的"近5日累计涨跌幅(%)"和"DIF近4日累计变化量"，对比两者方向是否一致。若股价累计上涨但 DIF 累计下降，说明反弹缺乏 MACD 趋势确认，定性为"弱势反弹"。**严禁自行对序列求和计算累计值**。
3. 若最新交易日跌幅 > 3%，标记为大阴线，强制进入第六步防暴跌验证。

### 三、交叉信号有效性与历史胜率

**分析要求**：
1. 读取【MACD摘要信号】确认当前是金叉还是死叉状态，以及零轴位置。
2. **直接引用**【预计算交叉历史胜率】中的数据：
   - 逐一列出近3次同向交叉的日期、价格、0轴位置、10日收益率。
   - 直接引用预计算的胜率数字。
3. **胜率评估铁律**：
   - 胜率 ≥ 67%：信号可靠，正常评分
   - 胜率 33%-66%：信号一般，评分需谨慎
   - 胜率 < 33%：⚠️ 必须拉响警报，定性为"无博弈价值"
4. **零轴位置加权**：
   - 0轴上金叉 → 主升段信号，权重最高
   - 0轴下金叉 → 超跌反弹信号，权重降级
   - 0轴上死叉 → 高位转空信号，杀伤力最大
   - 0轴下死叉 → 弱势延续信号

### 四、背离结构与反弹质量分析

**分析要求**：
1. **直接引用**【预计算背离信号】中的数据，严禁自行判断背离：
   - 若"近10日内触发底背离"为 true → 存在底背离结构，详细说明背离日期和价格
   - 若"近10日内触发顶背离"为 true → 存在顶背离结构，详细说明背离日期和价格
   - 若两者均为 false → 无背离结构
2. **反弹质量判定**（当股价处于反弹阶段时）：
   - 有底背离支撑的反弹 → "有根基反弹"，可信度高
   - 无底背离、且 DIF 与股价方向背离（股价涨 DIF 跌）→ "无根基脉冲"，随时可能夭折
   - 无底背离、但 DIF 与股价同向上行 → "趋势跟随反弹"，需观察持续性
3. **顶背离风险**：若存在顶背离，无论其他指标如何，必须在评分中体现风险。

### 五、综合评分模型（满分25分，⚠️严禁打分幻觉）

必须严格遵守以下对应标准打分，**严禁感情用事、严禁妥协给高分、严禁在缺乏条件的情况下给满分**：

| 评分维度 | 满分 | 严格打分标准 (AI执行铁律) |
| :--- | :--- | :--- |
| 0轴位置 | 8分 | 双线均在0轴上且DIF上行(8分)；双线0轴上但DIF下行(6-7分)；DIF过0轴但DEA未过(4-5分)；双线在0轴下但DIF上行(2-3分)；双线在0轴下且DIF下行(0-1分) |
| 动能趋势 | 7分 | 柱体顺势放大+无价标背离+DIF与股价同向(6-7分)；柱体收窄但方向正确(4-5分)；动能震荡方向不明(2-3分)；出现价标背离或DIF与股价反向(0-1分) |
| 交叉信号 | 5分 | 0轴上金叉+历史胜率≥67%(5分)；0轴上金叉+胜率一般(3-4分)；0轴下金叉(2-3分)；无交叉但趋势延续(1-2分)；死叉运行中(0分)；0轴上高位死叉(0分且触发额外警告) |
| 背离预警 | 5分 | 预计算确认底背离+近10日内触发(4-5分)；无背离但趋势跟随(2-3分)；无背离且为无根基反弹(1分)；预计算确认顶背离(0分) |
| **总分** | **25分** | 必须精确相加。若触发第六步降级机制，需在此基础上强制扣分。 |

**评分交叉验证**：打分完成后，必须回检各维度得分与数据是否自洽。例如：
- 若"交叉信号"给了0分（死叉），则"动能趋势"不应给6-7分（除非绿柱明确收窄）
- 若"0轴位置"给了0-1分（双线0轴下且下行），则总分不应超过14分

### 六、操作建议与评级锁定（⚠️防骗线与防暴跌智能风控机制）

**【常规映射机制】**（根据第五步基础总分映射）：
* **总分 20-25**：[多空定调：强多] -> 未持有：积极买入 / 持有：持股待涨
* **总分 15-19**：[多空定调：偏多] -> 未持有：逢低建仓 / 持有：持股待涨
* **总分 10-14**：[多空定调：中性/弱多] -> 未持有：保持观望 / 持有：逢高减仓
* **总分 0-9**：  [多空定调：偏空/强空] -> 未持有：绝对观望 / 持有：清仓离场

**🚨【防暴跌与防骗线交叉验证】（最高指令）**：
当【预计算风控指标】中"是否大阴线（跌幅>3%）"为 true 时，**严禁一刀切，必须强制执行以下验证**：

**1. 检验"真破位"（满足任意一项即判定）**：
直接读取预计算指标，**严禁自行计算**：
* **放量杀跌**：【预计算】"是否放量（量比>1.3）"为 true
* **断头破位**：【预计算】"是否断头破位（收盘<前3日最低价）"为 true
* **高位衰竭**：MACD 在0轴上方且处于死叉状态，或红柱出现断崖式急缩（柱体变化量连续2日为负且绝对值递增）
**⚡ 真破位执行铁律**：满足任意一项 → 总分强制扣减至 9 分及以下，评级锁定 [偏空/强空]，操作锁定 [未持有：绝对观望 / 持有：清仓离场]。必须明确指出 MACD 已严重滞后失效。

**2. 识别"假摔洗盘"（必须同时满足全部条件）**：
* **明显缩量**：【预计算】"是否放量（量比>1.3）"为 false，且量比 < 0.8
* **结构完好**：【预计算】"是否断头破位"为 false
* **指标未坏**：MACD 柱体仍在收窄（绿柱变短或红柱区间），且 DIF 未加速下行
**🛡️ 假摔执行铁律**：不触发强制清仓，但在原总分基础上强制扣减 3-5 分；评级最高不超过 [中性/弱多]；操作最高限制在 [保持观望 / 逢高减仓]。严禁给出买入建议，并明确指出下方防守底线（取前3日最低价）。

**3. 非大阴线但存在隐性风险的额外检查**：
即使最新日非大阴线，若满足以下条件，也需在报告中发出预警：
* 近5日涨跌幅序列中出现2次以上跌幅 > 2% → "连续杀跌预警"（直接读取预计算"近5日跌幅超2%次数"，**严禁自行计数**）
* DIF 连续4日下行且距零轴 < 2% → "零轴失守预警"（直接读取预计算"DIF近4日下行天数"，**严禁自行计数**）
* 量比持续萎缩（< 0.6）且股价横盘 → "量能枯竭预警"

---

## Output Requirements (输出铁律)
1. **禁止计算幻觉**：所有衍生数据必须直接引用预计算结果，严禁自行捏造。
2. **格式强制**：必须且只能输出清晰的 Markdown 格式文本。使用各级标题（`##`, `###`）、加粗（`**`）、列表（`-` 或 `*`）和表格进行专业排版。
3. **内容结构**：严格包含框架要求的一至六模块，并在文章开头直接给出"股票名称+代码"、"MACD多空状态"、"综合评分"、"持仓与未持仓建议"的核心结论摘要表格。
4. **禁止废话**：不输出任何开场白、问候语或总结套话，直入主题，客观冷酷。
5. **数据溯源**：每个关键结论后必须标注数据来源（如"据预计算：量比=1.45"），增强可验证性。

## Data Input

**MACD 摘要信号**：
{json.dumps(macd_summary, ensure_ascii=False)}

**近{data_num}日交易数据（日期/收盘价/涨跌幅）**：
{json.dumps(stock_day_kline, ensure_ascii=False)}

**预计算风控指标（⚠️必须直接引用，严禁自行计算）**：
{json.dumps(risk_metrics, ensure_ascii=False, default=str)}

**预计算背离信号（⚠️由120日状态机算法检测，必须直接引用）**：
{json.dumps(divergence_summary, ensure_ascii=False, default=str)}

**预计算交叉历史胜率（⚠️基于实际走势回测，必须直接引用）**：
{json.dumps(cross_history, ensure_ascii=False, default=str)}


[最终输出] 只能输出 json 格式数据：
{{
  'stock_code': '<股票代码>',
  'stock_name': '<股票名称>',
  'macd_state': '<MACD多空状态，强多头 / 弱多头 / 空头 / 超跌>',
  'not_hold_grade': '<未持有建议，积极买入 / 逢低建仓 / 保持观望 / 绝对观望>',
  'hold_grade': '<持有建议，持股待涨 / 逢高减仓 / 清仓离场>',
  'content': '<MACD专项深度分析内容，输出 markdown 格式>'
}}
"""


if __name__ == '__main__':
    import asyncio
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name('生益科技')
        prompt = await get_stock_indicator_macd_prompt(stock_info)
        print(prompt)

    asyncio.run(main())
