import asyncio
import json
import sqlite3
from datetime import datetime
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from common.prompt.strategy_engine.stock_strategy_engine_prompt import get_strategy_engine_prompt
from service.eastmoney.strategy_engine.stock_BOLL_rule import get_boll_rule_cn
from service.eastmoney.strategy_engine.stock_KDJ_rule import get_kdj_rule_cn
from service.eastmoney.strategy_engine.stock_MACD_rule import get_macd_signals_cn
from service.eastmoney.strategy_engine.stock_identify_new_high_signal import get_new_high_signals_cn
from service.eastmoney.strategy_engine.stock_is_high_vol_pillar import get_high_vol_pillars
from service.eastmoney.strategy_engine.stock_unlimited_increase import get_unlimited_increase_cn
from service.eastmoney.strategy_engine.stock_bottom_far_top_volume_indicates import get_bottom_far_top_volume_indicates_cn
from service.eastmoney.strategy_engine.stock_volume_increases_price_remains_stagnant import get_distribution_signal_cn
from service.eastmoney.strategy_engine.stock_volume_reduction_pullback import get_volume_reduction_pullback_cn

DB_PATH = 'stock_strategy.db'


def _init_db(conn: sqlite3.Connection) -> None:
    conn.execute('''
        CREATE TABLE IF NOT EXISTS stock_strategy_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code TEXT NOT NULL,
            stock_name TEXT NOT NULL,
            analysis_date TEXT NOT NULL,
            boll_rule TEXT,
            kdj_rule TEXT,
            macd_rule TEXT,
            new_high_signal TEXT,
            high_vol_pillar TEXT,
            unlimited_increase TEXT,
            bottom_far_top_volume TEXT,
            distribution_signal TEXT,
            volume_reduction_pullback TEXT,
            buy_conclusion TEXT,
            sell_conclusion TEXT,
            created_at TEXT NOT NULL
        )
    ''')
    conn.commit()


def _build_conclusions(signals: dict) -> tuple[str, str]:
    """根据各维度信号生成买入和卖出结论"""
    buy_points = []
    sell_points = []
    latest_date = ''

    boll = signals.get('boll_rule', {})
    if boll:
        latest_date = boll.get('最新交易日', '')
        if boll.get(f'超强势开启信号（{latest_date}）'):
            buy_points.append('BOLL超强势开启（放量突破中轨且中轨向上）')
        elif boll.get(f'强势开启信号（{latest_date}）'):
            buy_points.append('BOLL强势开启（放量突破中轨）')
        if boll.get(f'波段结束信号（{latest_date}）'):
            sell_points.append('BOLL波段结束（跌破中轨）')
        if not boll.get(f'可操作区（{latest_date}）', True):
            sell_points.append('BOLL处于弱势区（收盘低于中轨）')

    kdj = signals.get('kdj_rule', {})
    if kdj:
        latest_date = kdj.get('最新交易日', latest_date)
        latest_signal = kdj.get(f'最新信号（{latest_date}）', '')
        if latest_signal == 'Buy':
            buy_points.append('KDJ超卖区金叉买入信号')
        elif 'Sell' in latest_signal:
            sell_points.append(f'KDJ卖出信号（{latest_signal}）')

    macd = signals.get('macd_rule', {})
    if macd:
        latest_date = macd.get('最新交易日', latest_date)
        if macd.get(f'零轴上金叉（{latest_date}）'):
            buy_points.append('MACD零轴上金叉（主升段信号）')
        elif macd.get(f'金叉（{latest_date}）'):
            buy_points.append('MACD金叉')
        if macd.get(f'零轴下死叉（{latest_date}）'):
            sell_points.append('MACD零轴下死叉（防暴跌）')
        elif macd.get(f'死叉（{latest_date}）'):
            sell_points.append('MACD死叉')
        if macd.get('底背离历史（最近3次）'):
            buy_points.append('MACD底背离（潜在反转）')
        if macd.get('顶背离历史（最近3次）'):
            sell_points.append('MACD顶背离（潜在见顶）')

    new_high = signals.get('new_high_signal', {})
    if new_high:
        latest_date = new_high.get('最新交易日', latest_date)
        if new_high.get(f'新量新价出新高（{latest_date}）'):
            buy_points.append('新量新价出新高（量价协同突破）')

    high_vol = signals.get('high_vol_pillar', [])
    if high_vol:
        buy_points.append(f'近期出现高量柱（共{len(high_vol)}根），主力积极介入')

    unlimited = signals.get('unlimited_increase', {})
    if unlimited:
        latest_date = unlimited.get('最新交易日', latest_date)
        if unlimited.get(f'无量上涨诱多背离（{latest_date}）'):
            sell_points.append('无量上涨诱多背离（警惕出货）')
        if unlimited.get(f'RSI超买背离确认信号（{latest_date}）'):
            sell_points.append('RSI超买背离确认（高置信度卖出）')

    bottom_vol = signals.get('bottom_far_top_volume', [])
    if bottom_vol and bottom_vol[0].get(f'底量远超顶量（{bottom_vol[0].get("交易日", "")}）'):
        buy_points.append('底量远超顶量（威科夫吸筹，主力长期建仓）')

    dist = signals.get('distribution_signal', {})
    if dist:
        latest_date = dist.get('最新交易日', latest_date)
        if dist.get(f'放量滞涨派发（{latest_date}）'):
            sell_points.append('放量滞涨派发（主力出货形态）')

    pullback = signals.get('volume_reduction_pullback', {})
    if pullback:
        latest_date = pullback.get('最新交易日', latest_date)
        if pullback.get(f'缩量回调（{latest_date}）'):
            buy_points.append('缩量回调健康洗盘（可逢低买入）')

    buy_score = len(buy_points)
    sell_score = len(sell_points)

    if buy_score == 0:
        buy_conclusion = '暂无明确买入信号'
    elif buy_score >= 3:
        buy_conclusion = f'强烈买入信号（{buy_score}个维度共振）：' + '；'.join(buy_points)
    else:
        buy_conclusion = f'买入参考信号（{buy_score}个维度）：' + '；'.join(buy_points)

    if sell_score == 0:
        sell_conclusion = '暂无明确卖出信号'
    elif sell_score >= 3:
        sell_conclusion = f'强烈卖出信号（{sell_score}个维度共振）：' + '；'.join(sell_points)
    else:
        sell_conclusion = f'卖出参考信号（{sell_score}个维度）：' + '；'.join(sell_points)

    return buy_conclusion, sell_conclusion


async def analyze_and_save_strategy(stock_info: StockInfo, db_path: str = DB_PATH) -> dict:
    """
    并发获取9个维度的策略信号，存入SQLite，并返回买入/卖出结论。
    """
    (
        boll, kdj, macd, new_high, unlimited, bottom_vol, dist, pullback
    ) = await asyncio.gather(
        get_boll_rule_cn(stock_info),
        get_kdj_rule_cn(stock_info),
        get_macd_signals_cn(stock_info),
        get_new_high_signals_cn(stock_info),
        get_unlimited_increase_cn(stock_info),
        get_bottom_far_top_volume_indicates_cn(stock_info),
        get_distribution_signal_cn(stock_info),
        get_volume_reduction_pullback_cn(stock_info),
    )

    high_vol_df = await get_high_vol_pillars(stock_info)
    high_vol = [
        {
            '日期': date.strftime('%Y-%m-%d'),
            '成交量（万）': round(row['volume'] / 10000, 2),
            '涨幅(%)': row['pct_change'],
        }
        for date, row in high_vol_df.sort_index(ascending=False).head(5).iterrows()
    ]

    signals = {
        'boll_rule': boll,
        'kdj_rule': kdj,
        'macd_rule': macd,
        'new_high_signal': new_high,
        'high_vol_pillar': high_vol,
        'unlimited_increase': unlimited,
        'bottom_far_top_volume': bottom_vol,
        'distribution_signal': dist,
        'volume_reduction_pullback': pullback,
    }

    buy_conclusion, sell_conclusion = _build_conclusions(signals)
    analysis_date = datetime.now().strftime('%Y-%m-%d')
    created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    with sqlite3.connect(db_path) as conn:
        _init_db(conn)
        conn.execute(
            '''
            INSERT INTO stock_strategy_signals
                (stock_code, stock_name, analysis_date,
                 boll_rule, kdj_rule, macd_rule, new_high_signal, high_vol_pillar,
                 unlimited_increase, bottom_far_top_volume, distribution_signal,
                 volume_reduction_pullback, buy_conclusion, sell_conclusion, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ''',
            (
                stock_info.stock_code_normalize,
                stock_info.stock_name,
                analysis_date,
                json.dumps(boll, ensure_ascii=False),
                json.dumps(kdj, ensure_ascii=False),
                json.dumps(macd, ensure_ascii=False),
                json.dumps(new_high, ensure_ascii=False),
                json.dumps(high_vol, ensure_ascii=False),
                json.dumps(unlimited, ensure_ascii=False),
                json.dumps(bottom_vol, ensure_ascii=False),
                json.dumps(dist, ensure_ascii=False),
                json.dumps(pullback, ensure_ascii=False),
                buy_conclusion,
                sell_conclusion,
                created_at,
            ),
        )

    return {
        'stock_code': stock_info.stock_code_normalize,
        'stock_name': stock_info.stock_name,
        'analysis_date': analysis_date,
        'buy_conclusion': buy_conclusion,
        'sell_conclusion': sell_conclusion,
        'signals': signals,
    }


async def get_strategy_engine_analysis(stock_info: StockInfo) -> str:
    return await get_strategy_engine_prompt(stock_info)


if __name__ == '__main__':
    async def main():
        stock_info: StockInfo = get_stock_info_by_name('易天股份')
        result = await get_strategy_engine_analysis(stock_info)
        print(result)
        #print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(main())
