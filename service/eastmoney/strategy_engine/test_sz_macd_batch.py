#!/usr/bin/env python3
"""
批量测试MACD计算 - 测试前50个股票
"""
import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline
from common.utils.stock_info_utils import StockInfo
from service.eastmoney.strategy_engine.stock_MACD_rule import calculate_macd_signals, _build_dataframe
from common.constants.stocks_data import STOCKS
import pandas as pd

async def test_single_stock(stock_data: dict, index: int, total: int) -> dict:
    """测试单个股票"""
    try:
        stock_code_normalize = stock_data['code']
        stock_code, market_suffix = stock_code_normalize.split('.')
        market_prefix = "0" if market_suffix == "SZ" else "1"
        secid = f"{market_prefix}.{stock_code}"
        
        stock_info = StockInfo(
            secid=secid,
            stock_code=stock_code,
            stock_code_normalize=stock_code_normalize,
            stock_name=stock_data['name']
        )
        
        klines = await get_stock_day_range_kline(stock_info, limit=100)
        if not klines:
            result = {'index': index, 'name': stock_data['name'], 'code': stock_data['code'], 'status': 'NO_DATA'}
            print(f"[{index}/{total}] {stock_data['name']} ({stock_data['code']}) - 无数据")
            return result
        
        df = _build_dataframe(klines)
        result_df = calculate_macd_signals(df)
        
        latest = result_df.iloc[-1]
        latest_date = result_df.index[-1].strftime('%Y-%m-%d')
        
        # MACD强弱
        macd_strength = '强势' if latest['Market_State'] == 'Bull_Strong' else ('弱势' if latest['Market_State'] in ['Bull_Weak', 'Bear'] else '中性')
        
        # 交叉类型
        if latest['Zero_Above_GC']:
            cross_type = '零轴上金叉'
        elif latest['Golden_Cross']:
            cross_type = '零轴下金叉'
        elif latest['Zero_Below_DC']:
            cross_type = '零轴下死叉'
        elif latest['Death_Cross']:
            cross_type = '零轴上死叉'
        else:
            cross_type = '无交叉'
        
        result = {
            'index': index,
            'name': stock_data['name'],
            'code': stock_data['code'],
            'status': 'SUCCESS',
            'date': latest_date,
            'close': latest['close'],
            'dif': latest['DIF'],
            'dea': latest['DEA'],
            '市场状态': latest['Market_State'],
            'MACD强弱': macd_strength,
            '交叉类型': cross_type,
            '背离': '底背离' if latest['Bottom_Divergence'] else ('顶背离' if latest['Top_Divergence'] else '无'),
            f'金叉（{latest_date}）': latest['Golden_Cross'] or latest['Zero_Above_GC'],
            f'死叉（{latest_date}）': latest['Death_Cross'] or latest['Zero_Below_DC'],
            'market_state': latest['Market_State'],
            'macd_strength': macd_strength,
            'cross_type': cross_type,
            'golden_cross': latest['Golden_Cross'],
            'death_cross': latest['Death_Cross'],
            'zero_above_gc': latest['Zero_Above_GC'],
            'zero_below_dc': latest['Zero_Below_DC'],
            'bottom_div': latest['Bottom_Divergence'],
            'top_div': latest['Top_Divergence']
        }
        
        div_signal = result['背离']
        print(f"[{index}/{total}] {result['name']:<10} {result['code']:<12} {result['date']} "
              f"收盘:{result['close']:<8.2f} DIF:{result['dif']:<8.4f} DEA:{result['dea']:<8.4f} "
              f"{result['market_state']:<12} {result['macd_strength']:<6} {result['cross_type']:<12} {div_signal}")
        
        return result
    except Exception as e:
        import traceback
        result = {'index': index, 'name': stock_data['name'], 'code': stock_data.get('code', 'N/A'), 'status': 'ERROR', 'error': str(e), 'traceback': traceback.format_exc()}
        print(f"[{index}/{total}] {stock_data['name']} ({stock_data.get('code', 'N/A')}) - 错误: {str(e)[:50]}")
        return result

async def main():
    print("=" * 180)
    print("MACD批量测试 - 所有.SZ股票")
    print("=" * 180)
    
    # 取所有.SZ结尾的股票
    test_stocks = [stock for stock in STOCKS if stock['code'].endswith('.SZ')]
    total = len(test_stocks)
    print(f"\n共{total}只股票，开始逐个测试...\n")
    
    # 顺序测试，每个股票完成后立即输出
    results = []
    for i, stock in enumerate(test_stocks):
        result = await test_single_stock(stock, i+1, total)
        results.append(result)
        await asyncio.sleep(1)
    
    # 统计
    success_count = sum(1 for r in results if r['status'] == 'SUCCESS')
    error_count = sum(1 for r in results if r['status'] == 'ERROR')
    no_data_count = sum(1 for r in results if r['status'] == 'NO_DATA')
    
    # 统计汇总
    print("\n" + "=" * 180)
    print("统计汇总")
    print("=" * 180)
    print(f"总测试数: {len(test_stocks)}")
    print(f"成功: {success_count}")
    print(f"失败: {error_count}")
    print(f"无数据: {no_data_count}")
    
    # 市场状态分布
    if success_count > 0:
        success_results = [r for r in results if r['status'] == 'SUCCESS']
        bull_strong = sum(1 for r in success_results if r['market_state'] == 'Bull_Strong')
        bull_weak = sum(1 for r in success_results if r['market_state'] == 'Bull_Weak')
        bear = sum(1 for r in success_results if r['market_state'] == 'Bear')
        
        print(f"\n市场状态分布:")
        print(f"  强多头(Bull_Strong): {bull_strong} ({bull_strong/success_count*100:.1f}%)")
        print(f"  弱多头(Bull_Weak): {bull_weak} ({bull_weak/success_count*100:.1f}%)")
        print(f"  空头(Bear): {bear} ({bear/success_count*100:.1f}%)")
        
        # 交叉信号统计
        golden_cross = sum(1 for r in success_results if r['golden_cross'])
        death_cross = sum(1 for r in success_results if r['death_cross'])
        zero_above_gc = sum(1 for r in success_results if r['zero_above_gc'])
        zero_below_dc = sum(1 for r in success_results if r['zero_below_dc'])
        
        print(f"\n交叉信号统计:")
        print(f"  金叉: {golden_cross}")
        print(f"  死叉: {death_cross}")
        print(f"  零轴上金叉: {zero_above_gc}")
        print(f"  零轴下死叉: {zero_below_dc}")
        
        # 背离信号统计
        bottom_div = sum(1 for r in success_results if r['bottom_div'])
        top_div = sum(1 for r in success_results if r['top_div'])
        
        print(f"\n背离信号统计:")
        print(f"  底背离: {bottom_div}")
        print(f"  顶背离: {top_div}")
    
    print("\n" + "=" * 180)

if __name__ == '__main__':
    asyncio.run(main())
