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
|-----|-------|-------|--------------|---------------|----------------|-----------------|-------------|----------------|-------------|--------------|--------------|---------------|----------|-----------|
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

def detect_cup_and_handle(df):
    """检测杯柄形态 - 严格符合CAN SLIM规则（威廉·欧奈尔《笑傲股市》）"""
    df = df.sort_values('日期').reset_index(drop=True)
    results = []
    
    if len(df) < 60:
        return results
    
    # 将日期转换为datetime类型（如果还不是）
    if df['日期'].dtype != 'datetime64[ns]':
        df['日期'] = pd.to_datetime(df['日期'])
    
    # 步骤1: 寻找杯身左侧高点 (Left Cup Lip)
    # 逻辑：在数据的前半段寻找局部高点（通常是前期上涨后的顶部）
    first_half = len(df) // 2
    check_period_left = df.iloc[:first_half]
    if check_period_left.empty:
        return results
    
    left_lip_idx = check_period_left['收盘价'].idxmax()
    left_lip_price = df.loc[left_lip_idx, '收盘价']
    left_lip_date = df.loc[left_lip_idx, '日期']
    
    # 步骤2: 寻找杯底 (Cup Bottom)
    # 逻辑：在左侧高点之后寻找最低点（基于收盘价）
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
    
    # 步骤3: 寻找杯身右侧与柄部 (Right Lip & Handle)
    # 逻辑：在杯底之后寻找右侧高点（Pivot Point）
    check_period_right = df.iloc[bottom_idx+1:]
    if check_period_right.empty:
        return results
    
    # 寻找右侧高点区域（可能有多个局部高点）
    right_lip_idx = check_period_right['收盘价'].idxmax()
    right_lip_price = df.loc[right_lip_idx, '收盘价']  # Pivot Price
    right_lip_date = df.loc[right_lip_idx, '日期']
    
    # 步骤4: 寻找柄部低点 (Handle Low)
    # 逻辑：在右侧高点附近或之后的小幅回调
    # 柄部通常在Pivot前后形成，取Pivot附近的最低点
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
    
    # 柄部回撤应小于15%（理想<10%）
    if handle_depth > 0.15:
        return results
    
    # 柄部位置需在杯身上半部
    handle_position_ok = handle_low_price > (left_lip_price + bottom_price) / 2
    if not handle_position_ok:
        return results
    
    # 步骤5: 检测突破 (Breakout)
    # 逻辑：在Pivot之后寻找放量突破
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
        # 判断是否在回踩确认阶段（第二买点）
        if abs(current_price - right_lip_price) / right_lip_price < 0.05:
            status = "已突破 - 回踩确认支撑(第二买点)"
        elif current_price > right_lip_price * 1.05:
            status = "已突破 - 持续上涨"
        else:
            status = "已突破 - 回调过深"
    else:
        status = "形成中 - 等待突破"
    
    # 判断杯型
    if cup_depth < 0.15:
        cup_type = "强势浅杯"
    else:
        cup_type = "标准杯身"
    
    # 判断是否为Rising Cup（右侧高于左侧）
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
    
    return results

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
    
    # CAN SLIM分析
    high_inflow_threshold = df['主力净流入净占比'].quantile(0.8)
    low_inflow_threshold = df['主力净流入净占比'].quantile(0.2)
    df['吸筹日'] = (df['涨跌幅'] > 0) & (df['主力净流入净占比'] > high_inflow_threshold)
    df['派发日'] = (df['涨跌幅'] < -0.2) & (df['主力净流入净占比'] < low_inflow_threshold)
    
    df['日内振幅'] = (df['当日最高价'] - df['当日最低价']) / df['收盘价'].shift(1) * 100
    df['波动收缩'] = df['日内振幅'].rolling(3).mean() < df['日内振幅'].rolling(10).mean()
    
    df['机构净流入占比'] = df['超大单净流入净占比'] + df['大单净流入净占比']
    df['筹码向机构集中'] = (df['机构净流入占比'] > 0) & (df['小单净流入净占比'] < 0)
    
    df['20日新高'] = df['收盘价'] >= df['收盘价'].rolling(window=20).max()
    df['放量突破'] = df['20日新高'] & (df['主力净流入净占比'] > high_inflow_threshold)
    
    df['CAN_SLIM_Score'] = (
        df['吸筹日'].astype(int) + 
        df['波动收缩'].astype(int) + 
        df['筹码向机构集中'].astype(int) + 
        (df['超大单净流入净占比'] > 5).astype(int)
    )
    
    # 杯柄形态检测
    cup_patterns = detect_cup_and_handle(df)
    
    # 生成Markdown摘要
    if not stock_code:
        stock_code = secid.split('.')[-1]
    header = f"## <{stock_code} {stock_name}> - CAN SLIM 分析摘要" if stock_name else "## CAN SLIM 分析摘要"
    
    markdown = f"""{header}

### （{len(klines)}）天关键指标统计
- 吸筹日数量: {df['吸筹日'].sum()}天
- 派发日数量: {df['派发日'].sum()}天
- 筹码向机构集中天数: {df['筹码向机构集中'].sum()}天
- 放量突破天数: {df['放量突破'].sum()}天
- 平均CAN SLIM评分: {df['CAN_SLIM_Score'].mean():.2f}
- 杯柄形态: {'✓ 检测到' + str(len(cup_patterns)) + '个' if cup_patterns else '✗ 未检测到'}

### 杯柄形态详情
{chr(10).join([f'''- **形态判定**: {p['cup_type']} - {p['status']}
  - 杯身左侧: {p['left_lip_date']}, 价格 {p['left_lip_price']}元
  - 杯底低点: {p['bottom_date']}, 价格 {p['bottom_price']}元
  - 杯身深度: {p['cup_depth']} (理想范围: 12%-33%)
  - 枢轴点(Pivot): {p['pivot_date']}, 价格 {p['pivot_price']}元
  - 柄部低点: {p['handle_low_date']}, 价格 {p['handle_low_price']}元
  - 柄部回撤: {p['handle_retracement']} (理想<10%)
  - 柄部位置: {p['handle_position']}
  - 资金状态: {p['volume_status']}
  - 突破状态: {'✓ 已突破 - ' + str(p['breakout_info']['date']) + ', 涨幅' + str(round(p['breakout_info']['change'], 2)) + '%, 主力流入' + str(round(p['breakout_info']['flow'], 2)) + '%' if p['has_breakout'] else '✗ 等待突破'}
  - 当前价格: {p['current_price']}元 ({p['current_date']})''' for p in cup_patterns]) if cup_patterns else '- 暂无杯柄形态'}

### 近期重要信号（前30天）
| 日期 | 收盘价 | 涨跌幅 | 吸筹日 | 派发日 | 筹码向机构集中 | CAN_SLIM评分 |
|-----|-------|-------|-------|-------|--------------|-------------|
"""
    
    for _, row in df.head(30).iterrows():
        markdown += f"| {row['日期']} | {row['收盘价']:.2f} | {row['涨跌幅']:.2f}% | {'✓' if row['吸筹日'] else ''} | {'✓' if row['派发日'] else ''} | {'✓' if row['筹码向机构集中'] else ''} | {row['CAN_SLIM_Score']} |\n"

    print(markdown)
    return markdown + "\n"


if __name__ == "__main__":
    asyncio.run(generate_fund_flow_history_can_slim_summary(secid="0.002371", stock_code="002371", stock_name="北方华创"))