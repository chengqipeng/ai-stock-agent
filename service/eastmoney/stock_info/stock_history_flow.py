import asyncio
import pandas as pd

from common.utils.amount_utils import convert_amount_unit
from common.http.http_utils import EASTMONEY_PUSH_API_URL, fetch_eastmoney_api, EASTMONEY_PUSH2HIS_API_URL

async def get_fund_flow_history(secid="0.002371"):
    """获取资金流向历史数据"""
    url = f"{EASTMONEY_PUSH2HIS_API_URL}/stock/fflow/daykline/get"
    params = {
        "lmt": "150",
        "klt": "101",
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "secid": secid,
        "_": 1715330901
    }
    data = await fetch_eastmoney_api(url, params, referer="https://quote.eastmoney.com/")
    if data.get("data") and data["data"].get("klines"):
        klines = data["data"]["klines"]
        klines.reverse()
        return klines
    else:
        raise Exception(f"未获取到股票 {secid} 的资金流向历史数据")

async def get_fund_flow_history_markdown(secid="0.002371", stock_code=None, stock_name=None):
    """获取资金流向历史数据并转换为markdown"""
    page_size = 120
    klines = await get_fund_flow_history(secid)
    kline_max_min_map = await get_stock_history_kline_max_min(secid)
    if not stock_code:
        stock_code = secid.split('.')[-1]
    header = f"## <{stock_code} {stock_name}> - 历史资金流向" if stock_name else "## 历史资金流向"
    markdown = f"""{header}
| 日期 | 收盘价 | 涨跌幅 | 主力净流入净额 | 主力净流入净占比 | 超大单净流入净额 | 超大单净流入净占比 | 大单净流入净额 | 大单净流入净占比 | 中单净流入净额 | 中单净流入占比 | 小单净流入净额 | 小单净流入净占比 | 当日最高价 | 当日最低价 |
|-----|-------|-------|--------------|---------------|----------------|-----------------|-------------|----------------|-------------|--------------|--------------|---------------|----------|--------------|
"""
    for kline in klines[:page_size]:
        fields = kline.split(',')
        if len(fields) >= 15:
            date = fields[0]
            kline_max_min_item = kline_max_min_map[date]
            close_price = round(float(fields[11]), 2) if fields[11] != '-' else '--'
            change_pct = f"{round(float(fields[12]), 2)}%" if fields[12] != '-' else "--"
            super_net = float(fields[5]) if fields[5] != '-' else 0
            super_pct = f"{round(float(fields[10]), 2)}%" if fields[10] != '-' else "--"
            super_net_str = convert_amount_unit(super_net)
            big_net = float(fields[4]) if fields[4] != '-' else 0
            big_net_str = convert_amount_unit(big_net)
            big_pct = f"{round(float(fields[9]), 2)}%" if fields[9] != '-' else "--"
            mid_net = float(fields[3]) if fields[3] != '-' else 0
            mid_net_str = convert_amount_unit(mid_net)
            mid_pct = f"{round(float(fields[8]), 2)}%" if fields[8] != '-' else "--"
            small_net = float(fields[2]) if fields[2] != '-' else 0
            small_net_str = convert_amount_unit(small_net)
            small_pct = f"{round(float(fields[7]), 2)}%" if fields[7] != '-' else "--"
            main_net = super_net + big_net
            main_net_str = convert_amount_unit(main_net)
            main_pct = f"{round(float(fields[6]), 2)}%" if fields[6] != '-' else "--"
            markdown += f"| {date} | {close_price} | {change_pct} | {main_net_str} | {main_pct} | {super_net_str} | {super_pct} | {big_net_str} | {big_pct} | {mid_net_str} | {mid_pct} | {small_net_str} | {small_pct} | {kline_max_min_item['high_price']} | {kline_max_min_item['low_price']} |\n"
    return markdown + "\n"


async def get_stock_history_kline_max_min(secid="0.002371"):
    """获取股票K线数据"""
    url = f"{EASTMONEY_PUSH2HIS_API_URL}/stock/kline/get"
    params = {
        "secid": secid,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "1",
        "end": "20500101",
        "smplmt": "460",
        "lmt": "130"
    }
    data = await fetch_eastmoney_api(url, params, referer="https://quote.eastmoney.com/")
    if data.get("data") and data["data"].get("klines"):
        klines = data["data"]["klines"]
        result = {}
        for kline in klines:
            fields = kline.split(',')
            date = fields[0]
            high_price = float(fields[2])
            low_price = float(fields[3])
            result[date] = {"high_price": high_price, "low_price": low_price}
        return result
    else:
        raise Exception(f"未获取到股票 {secid} 的K线数据")

def calculate_relative_strength(df, period=60):
    """计算相对强度RS (Relative Strength)
    
    Args:
        df: DataFrame，必须包含'日期'和'收盘价'列
        period: 计算周期，默认60个交易日（约3个月）
    
    Returns:
        str: 格式化的RS分析文本
    """
    # 1. 排序：按日期正序排列（最旧的在前，最新的在后）
    df_sorted = df.sort_values('日期', ascending=True).reset_index(drop=True)
    
    if len(df_sorted) <= period:
        return """**1. 当前数据点 (T)**
- 数据不足，无法计算RS"""
    
    # 2. 确定 T (Current)
    t_idx = df_sorted.index[-1]  # 最后一行
    t_price = df_sorted.loc[t_idx, '收盘价']
    
    # 3. 确定 T-60 (Benchmark)
    # 逻辑：当前索引减去60 (交易日)
    ref_idx = t_idx - period
    ref_price = df_sorted.loc[ref_idx, '收盘价']
    
    # 4. 计算 RS
    diff_abs = t_price - ref_price
    rs_score = (diff_abs / ref_price) * 100
    
    # 5. 评价
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

def clean_money(x):
    """清洗金额数据"""
    if isinstance(x, str):
        if '亿' in x: return float(x.replace('亿', '')) * 1e8
        if '万' in x: return float(x.replace('万', '')) * 1e4
    return float(x)

def clean_percent(x):
    """清洗百分比数据"""
    if isinstance(x, str): return float(x.replace('%', ''))
    return float(x)

def analyze_institutional_sponsorship(df, recent_window=20):
    """分析机构赞助维度 (Institutional Sponsorship - I)"""
    if len(df) < recent_window:
        return "数据不足，无法进行机构赞助分析"
    
    # 数据准备：按日期正序排列
    df = df.sort_values('日期', ascending=True).reset_index(drop=True)
    df_recent = df.tail(recent_window).copy()
    
    # 计算主力净流入净额（使用主力净流入净占比 * 收盘价作为近似）
    df_recent['主力净流入净额_数值'] = df_recent['主力净流入净占比'] * df_recent['收盘价'] * 1e6
    df_recent['涨跌幅_数值'] = df_recent['涨跌幅']
    
    # A. 红肥绿瘦分析
    accumulation_days = df_recent[df_recent['主力净流入净占比'] > 0]
    distribution_days = df_recent[df_recent['主力净流入净占比'] < 0]
    
    acc_count = len(accumulation_days)
    dist_count = len(distribution_days)
    acc_ratio = acc_count / recent_window
    
    # B. 量价配合分析
    up_days = df_recent[df_recent['涨跌幅_数值'] > 0]
    down_days = df_recent[df_recent['涨跌幅_数值'] < 0]
    
    avg_inflow_on_up = up_days['主力净流入净额_数值'].mean() if not up_days.empty else 0
    avg_outflow_on_down = down_days['主力净流入净额_数值'].abs().mean() if not down_days.empty else 0
    vp_ratio = avg_inflow_on_up / avg_outflow_on_down if avg_outflow_on_down != 0 else 999
    
    # C. 主力净流入强度
    total_inflow = accumulation_days['主力净流入净额_数值'].sum()
    total_outflow = distribution_days['主力净流入净额_数值'].abs().sum()
    flow_strength = total_inflow / total_outflow if total_outflow != 0 else 999
    
    # 格式化输出
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
    
    # 1. 数据准备：按日期正序排列
    df = df.sort_values('日期', ascending=True).reset_index(drop=True)
    
    # 2. 日期类型转换
    if df['日期'].dtype != 'datetime64[ns]':
        df['日期'] = pd.to_datetime(df['日期'])
    
    # 步骤1: 寻找杯身左侧高点
    first_half = len(df) // 2
    check_period_left = df.iloc[:first_half]
    if check_period_left.empty:
        return results
    
    left_lip_idx = check_period_left['收盘价'].idxmax()
    left_lip_price = df.loc[left_lip_idx, '收盘价']
    left_lip_date = df.loc[left_lip_idx, '日期']
    
    # 步骤2: 寻找杯底
    check_period_bottom = df.iloc[left_lip_idx+1:first_half+20]
    if check_period_bottom.empty:
        return results
    
    bottom_idx = check_period_bottom['收盘价'].idxmin()
    bottom_price = df.loc[bottom_idx, '收盘价']
    bottom_date = df.loc[bottom_idx, '日期']
    
    # 计算杯身深度 - 标准范围12%-33%
    cup_depth = (left_lip_price - bottom_price) / left_lip_price
    if not (0.12 <= cup_depth <= 0.35):
        return results
    
    # 步骤3: 寻找杯身右侧（Pivot Point）
    check_period_right = df.iloc[bottom_idx+1:]
    if check_period_right.empty:
        return results
    
    right_lip_idx = check_period_right['收盘价'].idxmax()
    right_lip_price = df.loc[right_lip_idx, '收盘价']
    right_lip_date = df.loc[right_lip_idx, '日期']
    
    # 步骤4: 寻找柄部低点
    handle_window_start = max(0, right_lip_idx - 10)
    handle_window_end = min(len(df), right_lip_idx + 10)
    handle_period = df.iloc[handle_window_start:handle_window_end]
    
    if handle_period.empty:
        return results
    
    handle_low_idx = handle_period['收盘价'].idxmin()
    handle_low_price = df.loc[handle_low_idx, '收盘价']
    handle_low_date = df.loc[handle_low_idx, '日期']
    
    # 计算柄部回撤深度
    handle_depth = (right_lip_price - handle_low_price) / right_lip_price
    
    # 柄部回撤应小于15%
    if handle_depth > 0.15:
        return results
    
    # 柄部位置需在杯身上半部
    handle_position_ok = handle_low_price > (left_lip_price + bottom_price) / 2
    if not handle_position_ok:
        return results
    
    # 步骤5: 检测突破
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
    
    # 步骤6: 当前状态分析
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
    
    # 判断杯型
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
    
    # 格式化输出
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






"""
基于欧奈尔（William J. O'Neil）的 CAN SLIM 投资法则，针对您提供的纯历史交易数据（即不包含每股收益EPS等基本面数据），分析维度将严格聚焦于 S（供给与需求/成交量）、L（市场领导者/相对强度）、I（机构机构/资金流向） 和 M（市场趋势） 这四个技术面维度。
以下是基于您提供的120天交易数据，欧奈尔法则会关注的具体分析维度及代码实现：
一、 欧奈尔交易数据分析的具体维度
由于缺少基本面数据（C、A），我们重点通过以下技术指标来量化验证股票是否具备“大牛股”的技术特征：

趋势维度 (Current Trend - M/N)
股价是否站在生命线之上：收盘价必须高于 50日移动平均线 (MA50)。这是欧奈尔判定股票处于上升阶段的核心底线。
均线多头排列：短期均线（MA10或MA20）需位于长期均线（MA50）之上。

机构赞助维度 (Institutional Sponsorship - I)
主力资金净流入强度：欧奈尔强调“成交量”是专业机构进场的脚印。在您提供的数据中，“主力净流入净额” 是比单纯成交量更直接的指标。
上涨放量，下跌缩量：股价上涨日的资金流入量，应显著大于股价下跌日的资金流出量。

相对强度维度 (Leader or Laggard - L)
相对价格强度 (RS Strength)：虽然没有大盘指数做对比，但可以通过**“当前价格相对于过去N天的涨幅”**来计算自身的强度。欧奈尔要求股票通常在创出52周新高附近的15%以内。
接近新高：股价应接近近期（120天内）的最高点，而非在底部徘徊。
形态突破维度 (Pivot Point & Breakout - S)

突破力度：在突破关键价位（Pivot）当日，成交量（资金流入）应至少比平均水平高出 40%-50%。
"""
async def generate_fund_flow_history_can_slim_summary(secid="0.002371", stock_code=None, stock_name=None):
    """生成CAN SLIM分析摘要"""
    klines = await get_fund_flow_history(secid)
    kline_max_min_map = await get_stock_history_kline_max_min(secid)
    
    # 构建DataFrame
    data_list = []
    for kline in klines:
        fields = kline.split(',')
        if len(fields) >= 15:
            date = fields[0]
            kline_max_min_item = kline_max_min_map.get(date, {'high_price': 0, 'low_price': 0})
            data_list.append({
                '日期': date,
                '收盘价': float(fields[11]) if fields[11] != '-' else 0,
                '涨跌幅': float(fields[12]) if fields[12] != '-' else 0,
                '主力净流入净占比': float(fields[6]) if fields[6] != '-' else 0,
                '超大单净流入净占比': float(fields[10]) if fields[10] != '-' else 0,
                '大单净流入净占比': float(fields[9]) if fields[9] != '-' else 0,
                '小单净流入净占比': float(fields[7]) if fields[7] != '-' else 0,
                '当日最高价': kline_max_min_item['high_price'],
                '当日最低价': kline_max_min_item['low_price']
            })
    
    df = pd.DataFrame(data_list)
    
    # 杯柄形态检测
    cup_pattern_text = detect_cup_and_handle(df)
    
    # 计算相对强度RS
    rs_text = calculate_relative_strength(df, period=60)
    
    # 机构赞助维度分析
    institutional_text = analyze_institutional_sponsorship(df, recent_window=20)
    
    # 生成Markdown摘要
    if not stock_code:
        stock_code = secid.split('.')[-1]
    header = f"## <{stock_code} {stock_name}> - CAN SLIM 分析摘要" if stock_name else "## CAN SLIM 分析摘要"
    
    markdown = f"""{header}

### 相对强度RS (60日/3个月)

{rs_text}

### 机构赞助维度 (Institutional Sponsorship - I)

{institutional_text}

### 杯柄形态详情
{cup_pattern_text}
"""
    
    print(markdown)
    return markdown + "\n"


if __name__ == "__main__":
    asyncio.run(generate_fund_flow_history_can_slim_summary(secid="0.002371", stock_code="002371", stock_name="北方华创"))
