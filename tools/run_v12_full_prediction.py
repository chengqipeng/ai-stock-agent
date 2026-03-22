#!/usr/bin/env python3
"""
V12 全量预测 — 对数据库中所有A股生成下周预测
=============================================
用法：
    source .venv/bin/activate
    python -m tools.run_v12_full_prediction
"""
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dao import get_connection
from service.v12_prediction.v12_engine import V12PredictionEngine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent.parent / "data_results"


def _to_float(v) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def load_all_stock_codes() -> list[str]:
    """获取所有有足够K线数据的A股代码（排除北交所）。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT stock_code, COUNT(*) AS cnt
        FROM stock_kline
        WHERE stock_code NOT LIKE '%%.BJ'
        GROUP BY stock_code
        HAVING cnt >= 80
        ORDER BY stock_code
    """)
    codes = [r['stock_code'] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return codes


def load_kline_data(stock_codes: list[str]) -> dict:
    """加载最近120个交易日的K线数据。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    start_date = (datetime.now() - timedelta(days=250)).strftime('%Y-%m-%d')
    result = defaultdict(list)
    bs = 500
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, close_price, open_price, high_price, "
            f"low_price, trading_volume, change_percent, change_hand "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s ORDER BY `date`",
            batch + [start_date])
        for row in cur.fetchall():
            result[row['stock_code']].append({
                'date': str(row['date']),
                'close': _to_float(row['close_price']),
                'open': _to_float(row['open_price']),
                'high': _to_float(row['high_price']),
                'low': _to_float(row['low_price']),
                'volume': _to_float(row['trading_volume']),
                'change_percent': _to_float(row['change_percent']),
                'turnover': _to_float(row.get('change_hand')),
            })
        if (i // bs) % 5 == 0:
            logger.info("  K线加载进度: %d/%d", min(i + bs, len(stock_codes)), len(stock_codes))
    cur.close()
    conn.close()
    return dict(result)


def load_fund_flow_data(stock_codes: list[str]) -> dict:
    """加载最近30天的资金流向数据。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    start_date = (datetime.now() - timedelta(days=45)).strftime('%Y-%m-%d')
    result = defaultdict(list)
    bs = 500
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, big_net_pct, net_flow, main_net_5day "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s ORDER BY `date` DESC",
            batch + [start_date])
        for row in cur.fetchall():
            result[row['stock_code']].append({
                'date': str(row['date']),
                'big_net_pct': _to_float(row.get('big_net_pct')),
                'net_flow': _to_float(row.get('net_flow')),
                'main_net_5day': _to_float(row.get('main_net_5day')),
            })
    cur.close()
    conn.close()
    return dict(result)


def load_market_klines() -> list[dict]:
    """加载上证指数K线。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    start_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
    cur.execute(
        "SELECT `date`, close_price, change_percent "
        "FROM stock_kline WHERE stock_code = '000001.SH' "
        "AND `date` >= %s ORDER BY `date`",
        (start_date,))
    result = [{'date': str(r['date']),
               'close': _to_float(r.get('close_price')),
               'change_percent': _to_float(r['change_percent'])}
              for r in cur.fetchall()]
    cur.close()
    conn.close()
    return result


def load_stock_names(stock_codes: list[str]) -> dict:
    """尝试从stocks.json加载股票名称映射。"""
    names = {}
    try:
        stocks_file = Path(__file__).parent.parent / "common" / "files" / "stocks.json"
        if stocks_file.exists():
            with open(stocks_file, 'r', encoding='utf-8') as f:
                stocks = json.load(f)
            for s in stocks:
                code = s.get('code', s.get('stock_code', ''))
                name = s.get('name', s.get('stock_name', ''))
                if code and name:
                    names[code] = name
    except Exception:
        pass
    return names


def run_full_prediction():
    t0 = time.time()
    logger.info("=" * 60)
    logger.info("V12 全量预测 — 生成下周预测")
    logger.info("=" * 60)

    # 1. 加载数据
    logger.info("[1/3] 加载数据...")
    stock_codes = load_all_stock_codes()
    logger.info("  共 %d 只A股（排除北交所，K线≥80条）", len(stock_codes))

    kline_data = load_kline_data(stock_codes)
    fund_flow_data = load_fund_flow_data(stock_codes)
    market_klines = load_market_klines()
    logger.info("  K线: %d只, 资金流: %d只, 大盘: %d条",
                len(kline_data), len(fund_flow_data), len(market_klines))

    # 2. 全量预测
    logger.info("[2/3] 执行V12预测...")
    engine = V12PredictionEngine()
    predictions = []
    skipped = 0

    # 计算截面波动率中位数（IVOL过滤的自适应阈值）
    vols = []
    turns = []
    for code in stock_codes:
        klines = kline_data.get(code, [])
        if len(klines) >= 20:
            pcts = [k.get('change_percent', 0) or 0 for k in klines]
            recent = pcts[-20:]
            m = sum(recent) / 20
            vol = (sum((p - m) ** 2 for p in recent) / 19) ** 0.5
            vols.append(vol)
            turnover_vals = [k.get('turnover', 0) or 0 for k in klines]
            avg_turn = sum(turnover_vals[-20:]) / 20
            turns.append(avg_turn)
    vol_median = sorted(vols)[len(vols) // 2] if vols else None
    turn_median = sorted(turns)[len(turns) // 2] if turns else None

    for code in stock_codes:
        klines = kline_data.get(code, [])
        if len(klines) < 60:
            skipped += 1
            continue
        fund_flow = fund_flow_data.get(code, [])
        pred = engine.predict_single(code, klines, fund_flow, market_klines, vol_median, turn_median)
        if pred is not None:
            predictions.append(pred)

    logger.info("  预测完成: %d只出信号, %d只跳过(数据不足), %d只无极端条件",
                len(predictions), skipped,
                len(stock_codes) - len(predictions) - skipped)

    # 3. 整理输出
    logger.info("[3/3] 生成结果...")
    stock_names = load_stock_names(stock_codes)

    # 按置信度和方向分组
    up_high, up_medium, down_high, down_medium = [], [], [], []
    for p in predictions:
        code = p['stock_code']
        klines = kline_data.get(code, [])
        last_date = klines[-1]['date'] if klines else ''
        last_close = klines[-1]['close'] if klines else 0

        entry = {
            'stock_code': code,
            'stock_name': stock_names.get(code, ''),
            'pred_direction': p['pred_direction'],
            'confidence': p['confidence'],
            'extreme_score': p['extreme_score'],
            'composite_score': p['composite_score'],
            'reason': p['reason'],
            'last_date': last_date,
            'last_close': round(last_close, 2),
            'week_chg': round(p['conditions'].get('week_chg', 0), 2),
            'rsi': round(p['conditions'].get('rsi', 50), 1),
            'price_pos_60d': round(p['conditions'].get('price_pos', 0.5), 3),
            'n_signals': p['n_signals'],
            'n_agree': p['n_agree'],
            'signal_details': [
                {'signal': s['signal'], 'score': s['score'], 'reason': s['reason']}
                for s in p.get('signals', [])
            ],
        }

        if p['pred_direction'] == 'UP':
            if p['confidence'] == 'high':
                up_high.append(entry)
            else:
                up_medium.append(entry)
        else:
            if p['confidence'] == 'high':
                down_high.append(entry)
            else:
                down_medium.append(entry)

    # 按extreme_score降序排列
    for lst in [up_high, up_medium, down_high, down_medium]:
        lst.sort(key=lambda x: x['extreme_score'], reverse=True)

    result = {
        'meta': {
            'algorithm': 'V12-TwoLayer',
            'prediction_date': datetime.now().strftime('%Y-%m-%d %H:%M'),
            'data_latest_date': market_klines[-1]['date'] if market_klines else '',
            'total_stocks_scanned': len(stock_codes),
            'total_predictions': len(predictions),
            'coverage': round(len(predictions) / len(stock_codes), 4) if stock_codes else 0,
            'run_time_sec': round(time.time() - t0, 1),
        },
        'summary': {
            'up_high': len(up_high),
            'up_medium': len(up_medium),
            'down_high': len(down_high),
            'down_medium': len(down_medium),
        },
        'predictions_up_high': up_high,
        'predictions_up_medium': up_medium,
        'predictions_down_high': down_high,
        'predictions_down_medium': down_medium,
    }

    output_path = OUTPUT_DIR / "v12_full_prediction_result.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    logger.info("结果已保存: %s", output_path)

    # 打印摘要
    print("\n" + "=" * 60)
    print("V12 全量预测结果")
    print("=" * 60)
    print(f"\n📊 扫描: {len(stock_codes)}只A股")
    print(f"📅 数据截止: {result['meta']['data_latest_date']}")
    print(f"🎯 出信号: {len(predictions)}只 (覆盖率{result['meta']['coverage']:.1%})")
    print(f"\n📈 看涨: {len(up_high) + len(up_medium)}只 (高置信{len(up_high)}, 中置信{len(up_medium)})")
    print(f"📉 看跌: {len(down_high) + len(down_medium)}只 (高置信{len(down_high)}, 中置信{len(down_medium)})")

    if up_high:
        print(f"\n🔥 看涨-高置信度 (Top 20):")
        for e in up_high[:20]:
            name = f"({e['stock_name']})" if e['stock_name'] else ''
            print(f"   {e['stock_code']:12s}{name:10s} "
                  f"极端分={e['extreme_score']} 周跌{e['week_chg']:+.1f}% "
                  f"RSI={e['rsi']:.0f} | {e['reason'][:50]}")

    if down_high:
        print(f"\n🔻 看跌-高置信度 (Top 10):")
        for e in down_high[:10]:
            name = f"({e['stock_name']})" if e['stock_name'] else ''
            print(f"   {e['stock_code']:12s}{name:10s} "
                  f"极端分={e['extreme_score']} 周涨{e['week_chg']:+.1f}% "
                  f"RSI={e['rsi']:.0f} | {e['reason'][:50]}")

    if up_medium:
        print(f"\n📈 看涨-中置信度 (Top 10):")
        for e in up_medium[:10]:
            name = f"({e['stock_name']})" if e['stock_name'] else ''
            print(f"   {e['stock_code']:12s}{name:10s} "
                  f"极端分={e['extreme_score']} 周跌{e['week_chg']:+.1f}% "
                  f"RSI={e['rsi']:.0f} | {e['reason'][:50]}")

    print(f"\n⏱️  耗时: {time.time() - t0:.1f}秒")
    print(f"📁 完整结果: {output_path}")
    print("=" * 60)


if __name__ == '__main__':
    run_full_prediction()
