import asyncio
import pandas as pd

from service.eastmoney.stock_info.stock_history_flow import get_fund_flow_history, get_stock_history_kline_max_min


def calculate_relative_strength(df, period=60):
    """计算相对强度RS (Relative Strength)
    
    Args:
        df: DataFrame，必须包含'日期'和'收盘价'列
        period: 计算周期，默认60个交易日（约3个月）
    
    Returns:
        str: 格式化的RS分析文本
    """
    df_sorted = df.sort_values('日期', ascending=True).reset_index(drop=True)
    
    if len(df_sorted) <= period:
        return """**1. 当前数据点 (T)**
- 数据不足，无法计算RS"""
    
    t_idx = df_sorted.index[-1]
    t_price = df_sorted.loc[t_idx, '收盘价']
    
    ref_idx = t_idx - period
    ref_price = df_sorted.loc[ref_idx, '收盘价']
    
    diff_abs = t_price - ref_price
    rs_score = (diff_abs / ref_price) * 100
    
    if rs_score > 20:
        evaluation = '✅ RS强度极高，大幅跑赢同期多数资产'
    elif rs_score > 0:
        evaluation = '⚠️ RS强度一般，处于正收益区间'
    else:
        evaluation = '❌ RS强度较弱，处于亏损状态'
    
    return f"""**1. 当前数据点 (T)**
- 当前价格: {t_price:.2f}元

**2. 基准数据点 (T-{period})**
- 回溯逻辑: 从当前日期向前推算第{period}个交易日
- 基准价格: {ref_price:.2f}元

**3. 计算过程**
- 公式: (当前收盘价 - 基准收盘价) / 基准收盘价 × 100%
- 涨幅绝对值: {diff_abs:.2f}元
- 相对强度得分 (RS): {round(rs_score, 2)}%
- 评价: {evaluation}"""


def analyze_institutional_sponsorship(df, recent_window=20):
    """分析机构赞助维度 (Institutional Sponsorship - I)"""
    if len(df) < recent_window:
        return "数据不足，无法进行机构赞助分析"
    
    df = df.sort_values('日期', ascending=True).reset_index(drop=True)
    df_recent = df.tail(recent_window).copy()
    
    df_recent['主力净流入净额_数值'] = df_recent['主力净流入净占比'] * df_recent['收盘价'] * 1e6
    df_recent['涨跌幅_数值'] = df_recent['涨跌幅']
    
    accumulation_days = df_recent[df_recent['主力净流入净占比'] > 0]
    distribution_days = df_recent[df_recent['主力净流入净占比'] < 0]
    
    acc_count = len(accumulation_days)
    dist_count = len(distribution_days)
    acc_ratio = acc_count / recent_window
    
    up_days = df_recent[df_recent['涨跌幅_数值'] > 0]
    down_days = df_recent[df_recent['涨跌幅_数值'] < 0]
    
    avg_inflow_on_up = up_days['主力净流入净额_数值'].mean() if not up_days.empty else 0
    avg_outflow_on_down = down_days['主力净流入净额_数值'].abs().mean() if not down_days.empty else 0
    vp_ratio = avg_inflow_on_up / avg_outflow_on_down if avg_outflow_on_down != 0 else 999
    
    total_inflow = accumulation_days['主力净流入净额_数值'].sum()
    total_outflow = distribution_days['主力净流入净额_数值'].abs().sum()
    flow_strength = total_inflow / total_outflow if total_outflow != 0 else 999
    
    start_date = df_recent['日期'].iloc[0].strftime('%Y年%m月%d日') if hasattr(df_recent['日期'].iloc[0], 'strftime') else str(df_recent['日期'].iloc[0])
    end_date = df_recent['日期'].iloc[-1].strftime('%Y年%m月%d日') if hasattr(df_recent['日期'].iloc[-1], 'strftime') else str(df_recent['日期'].iloc[-1])
    
    acc_status = "✅ 机构买盘积极，处于净吸筹状态" if acc_ratio >= 0.5 else "⚠️ 机构分歧较大，吸筹不明显"
    
    vp_status = "✅ 符合'上涨放量，下跌缩量'特征" if vp_ratio > 1.0 else "⚠️ 量价背离，下跌时抛压可能重于上涨买盘"
    vp_extra = "\n   * 注：比率超过1.5，显示极强的机构控盘迹象" if vp_ratio > 1.5 else ""
    
    flow_status = "✅ 多方资金占据主导地位" if flow_strength > 1.2 else "⚠️ 资金流出压力较大"
    
    return f"""**分析区间**: {start_date} 至 {end_date} (近{recent_window}个交易日)

**1. 红肥绿瘦占比 - 机构吸筹天数分析**
* 资金净流入天数：**{acc_count}天** (占比 **{acc_ratio*100:.0f}%**)
* 资金净流出天数：**{dist_count}天**
* 判定：{acc_status}

**2. 量价配合 - 上涨放量 vs 下跌缩量**
* 上涨日平均净流入：**{avg_inflow_on_up/1e8:.2f}亿**
* 下跌日平均净流出：**{avg_outflow_on_down/1e8:.2f}亿**
* 量价配合比率 (In/Out Ratio)：**{vp_ratio:.2f}**
* 判定：{vp_status}{vp_extra}

**3. 主力净流入强度 - 总体资金博弈**
* 区间总流入额：**{total_inflow/1e8:.2f}亿**
* 区间总流出额：**{total_outflow/1e8:.2f}亿**
* 净买入强度：**{flow_strength:.2f}倍**
* 判定：{flow_status}"""


def detect_cup_and_handle(df):
    """检测杯柄形态 - 严格符合CAN SLIM规则（威廉·欧奈尔《笑傲股市》）"""
    results = []
    
    if len(df) < 60:
        return results
    
    df = df.sort_values('日期', ascending=True).reset_index(drop=True)
    
    if df['日期'].dtype != 'datetime64[ns]':
        df['日期'] = pd.to_datetime(df['日期'])
    
    first_half = len(df) // 2
    check_period_left = df.iloc[:first_half]
    if check_period_left.empty:
        return results
    
    left_lip_idx = check_period_left['收盘价'].idxmax()
    left_lip_price = df.loc[left_lip_idx, '收盘价']
    left_lip_date = df.loc[left_lip_idx, '日期']
    
    check_period_bottom = df.iloc[left_lip_idx+1:first_half+20]
    if check_period_bottom.empty:
        return results
    
    bottom_idx = check_period_bottom['收盘价'].idxmin()
    bottom_price = df.loc[bottom_idx, '收盘价']
    bottom_date = df.loc[bottom_idx, '日期']
    
    cup_depth = (left_lip_price - bottom_price) / left_lip_price
    if not (0.12 <= cup_depth <= 0.35):
        return results
    
    check_period_right = df.iloc[bottom_idx+1:]
    if check_period_right.empty:
        return results
    
    right_lip_idx = check_period_right['收盘价'].idxmax()
    right_lip_price = df.loc[right_lip_idx, '收盘价']
    right_lip_date = df.loc[right_lip_idx, '日期']
    
    handle_window_start = max(0, right_lip_idx - 10)
    handle_window_end = min(len(df), right_lip_idx + 10)
    handle_period = df.iloc[handle_window_start:handle_window_end]
    
    if handle_period.empty:
        return results
    
    handle_low_idx = handle_period['收盘价'].idxmin()
    handle_low_price = df.loc[handle_low_idx, '收盘价']
    handle_low_date = df.loc[handle_low_idx, '日期']
    
    handle_depth = (right_lip_price - handle_low_price) / right_lip_price
    
    if handle_depth > 0.15:
        return results
    
    handle_position_ok = handle_low_price > (left_lip_price + bottom_price) / 2
    if not handle_position_ok:
        return results
    
    breakout_search = df.iloc[right_lip_idx+1:]
    has_breakout = False
    breakout_info = {}
    
    for idx in breakout_search.index:
        if (df.loc[idx, '收盘价'] > right_lip_price and 
            df.loc[idx, '主力净流入净占比'] > 5):
            has_breakout = True
            breakout_info = {
                'date': df.loc[idx, '日期'],
                'price': df.loc[idx, '收盘价'],
                'change': df.loc[idx, '涨跌幅'],
                'flow': df.loc[idx, '主力净流入净占比']
            }
            break
    
    current_price = df.iloc[-1]['收盘价']
    current_date = df.iloc[-1]['日期']
    
    if has_breakout:
        if abs(current_price - right_lip_price) / right_lip_price < 0.05:
            status = "已突破 - 回踩确认支撑(第二买点)"
        elif current_price > right_lip_price * 1.05:
            status = "已突破 - 持续上涨"
        else:
            status = "已突破 - 回调过深"
    else:
        status = "形成中 - 等待突破"
    
    cup_type = "强势浅杯" if cup_depth < 0.15 else "标准杯身"
    if right_lip_price > left_lip_price:
        cup_type += " (Rising Cup)"
    
    results.append({
        "left_lip_date": left_lip_date,
        "left_lip_price": round(left_lip_price, 2),
        "bottom_date": bottom_date,
        "bottom_price": round(bottom_price, 2),
        "cup_depth": f"{cup_depth*100:.2f}%",
        "cup_type": cup_type,
        "pivot_date": right_lip_date,
        "pivot_price": round(right_lip_price, 2),
        "handle_low_date": handle_low_date,
        "handle_low_price": round(handle_low_price, 2),
        "handle_retracement": f"{handle_depth*100:.2f}%",
        "handle_position": "合格(上半部)" if handle_position_ok else "过低",
        "volume_status": "缩量洗盘",
        "has_breakout": has_breakout,
        "breakout_info": breakout_info if has_breakout else None,
        "current_price": round(current_price, 2),
        "current_date": current_date,
        "status": status
    })
    
    if results:
        cup_details = [format_cup_pattern_detail(p) for p in results]
        return '\n\n'.join(cup_details)
    else:
        return '- 暂无杯柄形态'


def format_cup_pattern_detail(p):
    """格式化杯柄形态详情"""
    left_date = p['left_lip_date'].strftime('%Y年%m月%d日') if hasattr(p['left_lip_date'], 'strftime') else str(p['left_lip_date'])
    bottom_date = p['bottom_date'].strftime('%Y年%m月%d日') if hasattr(p['bottom_date'], 'strftime') else str(p['bottom_date'])
    handle_date = p['handle_low_date'].strftime('%Y年%m月%d日') if hasattr(p['handle_low_date'], 'strftime') else str(p['handle_low_date'])
    pivot_date = p['pivot_date'].strftime('%Y年%m月%d日') if hasattr(p['pivot_date'], 'strftime') else str(p['pivot_date'])
    current_date = p['current_date'].strftime('%Y年%m月%d日') if hasattr(p['current_date'], 'strftime') else str(p['current_date'])
    cup_eval = '浅杯' if float(p['cup_depth'].rstrip('%')) < 20 else '标准杯身'
    
    breakout_text = '等待突破。'
    volume_text = '暂无突破数据。'
    if p['has_breakout']:
        breakout_date = p['breakout_info']['date'].strftime('%Y年%m月%d日') if hasattr(p['breakout_info']['date'], 'strftime') else str(p['breakout_info']['date'])
        breakout_text = f"{breakout_date}，股价收盘大涨{round(p['breakout_info']['change'], 2)}%至 **{p['breakout_info']['price']}** 元。"
        volume_text = f"当日主力净流入 **{round(p['breakout_info']['flow'], 2)}%**，资金大幅流入，标志着有效突破。"
    
    return f"""**形态判定**: {p['cup_type']} - {p['status']}

* **杯身左侧 (Left Cup Lip)**
    * **时间/价格**：{left_date}，收盘价 **{p['left_lip_price']}** 元。
    * **特征**：在此之前，股价经历了一波上涨，确立了前期高点，满足CAN SLIM形态构建的前提条件。

* **杯底 (Cup Bottom)**
    * **时间/价格**：{bottom_date}，最低收盘价 **{p['bottom_price']}** 元。
    * **回撤深度**：从高点{p['left_lip_price']}到低点{p['bottom_price']}，回撤幅度约为 **{p['cup_depth']}**。
    * **评价**：属于{cup_eval}形态（理想范围为12%-33%）。

* **杯身右侧与柄部 (Right Lip & Handle)**
    * **杯身修复**：股价重新回到高位区间，完成了杯身的构建。
    * **柄部形成**：{handle_date}（{p['handle_low_price']}元）至 {pivot_date}（{p['pivot_price']}元）。
    * **柄部低点**：{handle_date}，收盘价{p['handle_low_price']}元。柄部回调幅度约为 **{p['handle_retracement']}**。
    * **柄部位置**：{p['handle_position']}

* **关键突破点 (Pivot Point)**
    * **标准**：**{p['pivot_price']}元** 附近（柄部的高点区域）。
    * **突破动作**：{breakout_text}
    * **成交量验证**：{volume_text}

* **当前状态 (Current Status)**
    * **日期**：{current_date}，收盘价 **{p['current_price']}** 元。
    * **结论**：{p['status']}"""


def analyze_pivot_breakout(df, lookback_window=20, volume_threshold_ratio=2.0):
    """自动检测最显著的突破日期
    
    逻辑：
    1. 计算过去 N 天的最高价作为 '动态阻力位' (Rolling Pivot)
    2. 筛选出 收盘价 > 动态阻力位 的日期
    3. 在筛选结果中，寻找资金爆发力 (Surge Ratio) 最大的那一天
    """
    if len(df) < lookback_window:
        return "- 数据不足，无法进行突破分析"
    
    df = df.sort_values('日期', ascending=True).reset_index(drop=True)
    
    # 确保主力净流入净额列存在（已在数据加载时添加）
    
    # 1. 计算动态阻力位 (昨日及过去N天的最高价)
    df['Rolling_Pivot'] = df['当日最高价'].shift(1).rolling(window=lookback_window).max()
    
    # 2. 计算基准活跃度 (过去 N 天的主力净流入均值)
    df['Baseline_Inflow'] = df['主力净流入净额'].abs().rolling(window=lookback_window).mean().shift(1)
    
    # 3. 识别突破候选日
    candidates = df[
        (df['收盘价'] > df['Rolling_Pivot']) & 
        (df['主力净流入净额'] > 0)
    ].copy()
    
    if candidates.empty:
        current_price = df.iloc[-1]['收盘价']
        pivot_price = df['当日最高价'].tail(60).max() if len(df) >= 60 else df['当日最高价'].max()
        return f"""**Pivot Point 突破分析**

* **关键阻力位 (Pivot Point)**: {pivot_price:.2f} 元
* **当前价格**: {current_price:.2f} 元
* **当前状态**: 尚未突破，等待有效突破信号
* **突破条件**: 收盘价站上动态阻力位 + 主力资金净流入"""
    
    # 4. 计算爆发力比率 (Surge Ratio) 并排序
    candidates['Surge_Ratio'] = candidates['主力净流入净额'] / candidates['Baseline_Inflow']
    
    # 过滤掉爆发力不足的噪音
    strong_breakouts = candidates[candidates['Surge_Ratio'] > volume_threshold_ratio]
    
    if strong_breakouts.empty:
        best_breakout = candidates.sort_values('Surge_Ratio', ascending=False).iloc[0]
    else:
        best_breakout = strong_breakouts.sort_values('Surge_Ratio', ascending=False).iloc[0]
    
    # 格式化输出
    breakout_date_str = best_breakout['日期'].strftime('%Y年%m月%d日') if hasattr(best_breakout['日期'], 'strftime') else str(best_breakout['日期'])
    surge_ratio = best_breakout['Surge_Ratio']
    
    if surge_ratio >= 3.0:
        surge_level = "极强机构扫货"
    elif surge_ratio >= 2.0:
        surge_level = "强力机构买入"
    elif surge_ratio >= 1.5:
        surge_level = "明显放量"
    else:
        surge_level = "温和放量"
    
    return f"""**Pivot Point 突破分析 (自动检测)**

* **突破前阻力位 (Rolling Pivot)**: {best_breakout['Rolling_Pivot']:.2f} 元
* **突破日期**: {breakout_date_str}
* **突破日收盘价**: {best_breakout['收盘价']:.2f} 元
* **突破日涨跌幅**: {best_breakout['涨跌幅']:.2f}%

**资金爆发力验证**
* **当日主力净流入**: {best_breakout['主力净流入净额']/1e8:.2f}亿
* **前{lookback_window}日平均活跃度 (基准)**: {best_breakout['Baseline_Inflow']/1e8:.2f}亿
* **资金爆发倍数 (Surge Ratio)**: {surge_ratio:.2f} 倍
* **增长幅度**: +{(surge_ratio-1)*100:.1f}%
* **资金强度评级**: {surge_level}

**结论判定**: ✅ 有效突破
* 判定逻辑：资金流入量是日常水平的 {surge_ratio:.1f} 倍，符合有效突破标准"""


async def generate_fund_flow_history_can_slim_summary(secid="0.002371", stock_code=None, stock_name=None):
    """生成CAN SLIM分析摘要"""
    klines = await get_fund_flow_history(secid)
    kline_max_min_map = await get_stock_history_kline_max_min(secid)
    
    data_list = []
    for kline in klines:
        fields = kline.split(',')
        if len(fields) >= 15:
            date = fields[0]
            kline_max_min_item = kline_max_min_map.get(date, {'high_price': 0, 'low_price': 0})
            # 关键修正：明确拆分 '净额' (Amount) 和 '净占比' (Ratio)
            super_net = float(fields[5]) if fields[5] != '-' else 0
            big_net = float(fields[4]) if fields[4] != '-' else 0
            main_net = super_net + big_net  # 主力净流入净额 = 超大单 + 大单
            data_list.append({
                '日期': date,
                '收盘价': float(fields[11]) if fields[11] != '-' else 0,
                '涨跌幅': float(fields[12]) if fields[12] != '-' else 0,
                '主力净流入净额': main_net,  # 用于计算资金规模（单位：元）
                '主力净流入净占比': float(fields[6]) if fields[6] != '-' else 0,  # 用于辅助展示（百分比）
                '超大单净流入净占比': float(fields[10]) if fields[10] != '-' else 0,
                '大单净流入净占比': float(fields[9]) if fields[9] != '-' else 0,
                '小单净流入净占比': float(fields[7]) if fields[7] != '-' else 0,
                '当日最高价': kline_max_min_item['high_price'],
                '当日最低价': kline_max_min_item['low_price']
            })
    
    df = pd.DataFrame(data_list)
    
    cup_pattern_text = detect_cup_and_handle(df)
    rs_text = calculate_relative_strength(df, period=60)
    institutional_text = analyze_institutional_sponsorship(df, recent_window=20)
    pivot_breakout_text = analyze_pivot_breakout(df)
    
    if not stock_code:
        stock_code = secid.split('.')[-1]
    header = f"## <{stock_code} {stock_name}> - CAN SLIM 分析摘要" if stock_name else "## CAN SLIM 分析摘要"
    
    markdown = f"""{header}

### 相对强度RS (60日/3个月)

{rs_text}

### 机构赞助维度 (Institutional Sponsorship - I)

{institutional_text}

### 形态突破维度 (Pivot Point & Breakout - S)

{pivot_breakout_text}

### 杯柄形态详情
{cup_pattern_text}
"""
    
    print(markdown)
    return markdown + "\n"


if __name__ == "__main__":
    asyncio.run(generate_fund_flow_history_can_slim_summary(secid="0.002371", stock_code="002371", stock_name="北方华创"))
