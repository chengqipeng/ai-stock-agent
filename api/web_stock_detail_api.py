"""个股详情页 API — 聚合K线、预测、财报、概念板块等数据"""
import datetime as _dt
import json
import logging
from decimal import Decimal

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse

from dao import get_connection

# A股板块 → 美股可比细分领域映射
_SECTOR_TO_US_SECTORS = {
    '科技': ['芯片设计', '半导体设备', '半导体材料', '晶圆代工', '消费电子', '连接器', '光通信', '存储'],
    '新能源': [],
    '汽车': [],
    '制造': [],
    '医药': [],
    '化工': [],
    '有色金属': [],
}

logger = logging.getLogger(__name__)

router = APIRouter()

import re as _re

def _parse_amount(v):
    """解析金额字符串: '1309.04亿'->130904000000, 数字原样返回"""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v
    s = str(v)
    m = _re.match(r'^([\-]?[\d.]+)\s*亿$', s)
    if m:
        return float(m.group(1)) * 1e8
    m = _re.match(r'^([\-]?[\d.]+)\s*万$', s)
    if m:
        return float(m.group(1)) * 1e4
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _enrich_finance_rows(finance_rows: list[dict]):
    """补算财报中缺失的字段: 毛利率、营收环比、净利环比、流动比率、速动比率"""
    for i, row in enumerate(finance_rows):
        d = row.get("data")
        if not d:
            continue

        rev = _parse_amount(d.get("营业总收入(元)"))
        prof = _parse_amount(d.get("归母净利润(元)"))
        gross = _parse_amount(d.get("毛利润(元)"))

        # 毛利率: 毛利润/营业总收入*100; 若毛利润缺失但有净利率，用净利率兜底标记
        if d.get("毛利率(%)") is None and gross is not None and rev and rev != 0:
            d["毛利率(%)"] = round(gross / rev * 100, 2)

        # 营收环比 / 净利环比: 用单季度数据与上一期单季度数据对比
        if i + 1 < len(finance_rows):
            prev_d = finance_rows[i + 1].get("data") or {}
            sq_rev = _parse_amount(d.get("单季度营业收入(元)"))
            sq_prof = _parse_amount(d.get("单季归母净利润(元)"))
            prev_sq_rev = _parse_amount(prev_d.get("单季度营业收入(元)"))
            prev_sq_prof = _parse_amount(prev_d.get("单季归母净利润(元)"))

            if d.get("营业总收入环比增长(%)") is None and sq_rev is not None and prev_sq_rev and prev_sq_rev != 0:
                d["营业总收入环比增长(%)"] = round((sq_rev - prev_sq_rev) / abs(prev_sq_rev) * 100, 2)

            if d.get("归属净利润环比增长(%)") is None and sq_prof is not None and prev_sq_prof and prev_sq_prof != 0:
                d["归属净利润环比增长(%)"] = round((sq_prof - prev_sq_prof) / abs(prev_sq_prof) * 100, 2)


def _fill_finance_from_report(prediction: dict, finance_rows: list[dict]):
    """当 prediction 中 finance_score 等字段为空时，从财报原始数据实时计算并回填。"""
    if not prediction or not finance_rows:
        return
    # 如果已有值则跳过
    if prediction.get('finance_score') is not None:
        return
    latest_data = finance_rows[0].get('data') if finance_rows else None
    if not latest_data:
        return

    def _try_float(d, keys):
        for k in keys:
            v = d.get(k)
            if v is not None:
                try:
                    return float(v)
                except (ValueError, TypeError):
                    pass
        return None

    rev_yoy = _try_float(latest_data, [
        '营业总收入同比增长(%)', '营业总收入同比增长率(%)',
        '营业收入同比增长率(%)', 'TOTALOPERATEREVETZ'])
    prof_yoy = _try_float(latest_data, [
        '归属净利润同比增长(%)', '净利润同比增长率(%)',
        '归属母公司股东的净利润同比增长率(%)', 'PARENTNETPROFITTZ'])
    roe = _try_float(latest_data, [
        '净资产收益率(加权)(%)', '净资产收益率(%)',
        '加权净资产收益率(%)', 'ROEJQ'])

    parts = []
    if rev_yoy is not None:
        parts.append(max(-1, min(1, rev_yoy / 30)))
    if prof_yoy is not None:
        parts.append(max(-1, min(1, prof_yoy / 40)))
    if roe is not None:
        parts.append(max(-1, min(1, (roe - 10) / 10)))

    prediction['finance_score'] = round(sum(parts) / len(parts), 4) if parts else None
    prediction['revenue_yoy'] = round(rev_yoy, 2) if rev_yoy is not None else None
    prediction['profit_yoy'] = round(prof_yoy, 2) if prof_yoy is not None else None
    prediction['roe'] = round(roe, 2) if roe is not None else None


def _serialize(obj):
    """将 Decimal / datetime 等转为 JSON 可序列化类型"""
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize(v) for v in obj]
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (_dt.datetime, _dt.date)):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    return obj


@router.get("/stock_detail", response_class=HTMLResponse)
async def stock_detail_page():
    with open("static/stock_detail.html", "r", encoding="utf-8") as f:
        content = f.read()
    return HTMLResponse(content=content, headers={
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache", "Expires": "0",
    })


@router.get("/api/stock_detail/overview")
async def stock_detail_overview(stock_code: str = Query(..., description="股票代码如600519.SH")):
    """个股综合概览 — 一次性返回所有维度数据"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        result = {}

        # 1. 500天K线数据（涨跌幅、量价分析、技术指标）
        cur.execute(
            "SELECT `date`, open_price, close_price, high_price, low_price, "
            "trading_volume AS volume, trading_amount AS amount, "
            "change_percent, change_hand AS turnover_rate "
            "FROM stock_kline WHERE stock_code = %s "
            "ORDER BY `date` DESC LIMIT 500",
            (stock_code,),
        )
        klines = _serialize(cur.fetchall())
        result["klines"] = klines

        # 2. 最新预测数据（本周 + 下周V11）
        cur.execute(
            "SELECT * FROM stock_weekly_prediction WHERE stock_code = %s",
            (stock_code,),
        )
        prediction = _serialize(cur.fetchone())
        result["prediction"] = prediction

        # 3. 本周预测历史（最近20周）
        cur.execute(
            "SELECT predict_date, iso_year, iso_week, pred_direction, confidence, "
            "strategy, reason, d3_chg, d4_chg, actual_direction, actual_weekly_chg, "
            "is_correct, backtest_accuracy "
            "FROM stock_weekly_prediction_history "
            "WHERE stock_code = %s ORDER BY predict_date DESC LIMIT 20",
            (stock_code,),
        )
        result["prediction_history"] = _serialize(cur.fetchall())

        # 4. 下周V11预测历史
        cur.execute(
            "SELECT predict_date, iso_year, iso_week, "
            "nw_pred_direction, nw_confidence, nw_strategy, nw_reason, "
            "nw_composite_score, nw_this_week_chg, nw_pred_chg, "
            "nw_backtest_accuracy, nw_actual_direction, nw_actual_weekly_chg, nw_is_correct "
            "FROM stock_weekly_prediction_history "
            "WHERE stock_code = %s AND nw_pred_direction IS NOT NULL "
            "ORDER BY predict_date DESC LIMIT 20",
            (stock_code,),
        )
        result["nw_history"] = _serialize(cur.fetchall())

        # 5. 资金流向（最近60天）
        cur.execute(
            "SELECT `date`, close_price, change_pct, net_flow, main_net_5day, "
            "big_net, big_net_pct, mid_net, mid_net_pct, small_net, small_net_pct "
            "FROM stock_fund_flow WHERE stock_code = %s "
            "ORDER BY `date` DESC LIMIT 60",
            (stock_code,),
        )
        result["fund_flow"] = _serialize(cur.fetchall())

        # 6. 大单追踪（最近100条）
        stock_code_6 = stock_code.split(".")[0]
        try:
            cur.execute(
                "SELECT trade_date, `time`, stock_code, stock_name, price, volume, "
                "amount, direction, change_pct, turnover_rate "
                "FROM stock_big_order WHERE stock_code = %s "
                "ORDER BY trade_date DESC, `time` DESC LIMIT 100",
                (stock_code_6,),
            )
            result["big_orders"] = _serialize(cur.fetchall())
        except Exception:
            result["big_orders"] = []

        # 7. 财报数据（最近8期）
        cur.execute(
            "SELECT report_date, report_period_name, data_json "
            "FROM stock_finance WHERE stock_code = %s "
            "ORDER BY report_date DESC LIMIT 8",
            (stock_code,),
        )
        finance_rows = cur.fetchall()
        for row in finance_rows:
            if row.get("data_json"):
                try:
                    row["data"] = json.loads(row["data_json"])
                except Exception:
                    row["data"] = {}
                del row["data_json"]
        _enrich_finance_rows(finance_rows)
        result["finance"] = _serialize(finance_rows)

        # 7b. 如果预测数据中财务评分为空，从财报原始数据实时计算
        if prediction:
            _fill_finance_from_report(prediction, finance_rows)
            result["prediction"] = _serialize(prediction)
        elif finance_rows:
            # 即使没有预测记录，也从财报数据计算财务评分
            prediction = {}
            _fill_finance_from_report(prediction, finance_rows)
            if prediction.get('finance_score') is not None:
                result["prediction"] = _serialize(prediction)

        # 8. 所属概念板块 + 板块强弱势
        # stock_concept_board_stock 使用6位纯数字代码（如 600519），需去掉后缀
        stock_code_6 = stock_code.split(".")[0]
        cur.execute(
            "SELECT DISTINCT s.board_code, s.board_name "
            "FROM stock_concept_board_stock s WHERE s.stock_code = %s",
            (stock_code_6,),
        )
        boards = cur.fetchall()
        # 补充板块大盘强弱势评分
        for b in boards:
            cur.execute(
                "SELECT market_strength_score, market_excess_return, market_board_return "
                "FROM stock_concept_board WHERE board_code = %s",
                (b["board_code"],),
            )
            board_info = cur.fetchone()
            if board_info:
                b.update(board_info)
        result["concept_boards"] = _serialize(boards)

        # 9. 个股在概念板块中的强弱势
        cur.execute(
            "SELECT board_code, board_name, strength_score, strength_level, "
            "total_return, excess_5d, excess_20d, excess_total, win_rate, "
            "rank_in_board, board_total_stocks, score_date "
            "FROM stock_concept_strength WHERE stock_code = %s "
            "ORDER BY strength_score DESC",
            (stock_code_6,),
        )
        result["stock_strength"] = _serialize(cur.fetchall())

        # 10. CAN SLIM 最新分析（从 stock_analysis_detail 取最新一条）
        cur.execute(
            "SELECT d.*, b.batch_name FROM stock_analysis_detail d "
            "LEFT JOIN stock_batch_list_info b ON d.batch_id = b.id "
            "WHERE d.stock_code = %s ORDER BY d.completed_at DESC LIMIT 1",
            (stock_code,),
        )
        canslim = _serialize(cur.fetchone())
        result["canslim"] = canslim

        # 11. CAN SLIM 深度分析历史
        stock_name = prediction.get("stock_name", "") if prediction else ""
        if stock_name:
            cur.execute(
                "SELECT id, stock_name, stock_code, is_deep_thinking, "
                "c_score, a_score, n_score, s_score, l_score, i_score, m_score, "
                "overall_analysis, created_at "
                "FROM stock_deep_analysis_history "
                "WHERE stock_code = %s ORDER BY created_at DESC LIMIT 5",
                (stock_code,),
            )
            result["canslim_history"] = _serialize(cur.fetchall())
        else:
            result["canslim_history"] = []

        # 12. DeepSeek 预测历史
        try:
            cur.execute(
                "SELECT predict_date, target_iso_year, target_iso_week, target_date_range, "
                "pred_direction, confidence, justification, prefilter_pass, "
                "this_week_chg, actual_direction, actual_chg, is_correct "
                "FROM deepseek_weekly_prediction "
                "WHERE stock_code = %s ORDER BY predict_date DESC LIMIT 20",
                (stock_code,),
            )
            result["deepseek_history"] = _serialize(cur.fetchall())
        except Exception:
            result["deepseek_history"] = []

        # 13. 海外同类型公司涨跌（美股半导体龙头）
        try:
            from common.utils.sector_mapping_utils import parse_industry_list_md
            mapping = parse_industry_list_md()
            sector = mapping.get(stock_code, '')
            us_sectors = _SECTOR_TO_US_SECTORS.get(sector, [])
            overseas = []
            if us_sectors:
                placeholders = ','.join(['%s'] * len(us_sectors))
                cur.execute(
                    f"SELECT stock_code, stock_name, sector, trade_date, "
                    f"close_price, change_pct "
                    f"FROM us_stock_kline "
                    f"WHERE sector IN ({placeholders}) "
                    f"AND trade_date = (SELECT MAX(trade_date) FROM us_stock_kline) "
                    f"ORDER BY change_pct DESC",
                    us_sectors,
                )
                overseas = cur.fetchall()
            # 同时获取最近20日涨跌幅
            for item in overseas:
                cur.execute(
                    "SELECT trade_date, close_price, change_pct "
                    "FROM us_stock_kline WHERE stock_code = %s "
                    "ORDER BY trade_date DESC LIMIT 20",
                    (item['stock_code'],),
                )
                history = cur.fetchall()
                if len(history) >= 2:
                    first = history[0]['close_price']
                    last = history[-1]['close_price']
                    if last and first:
                        item['chg_20d'] = round(float((first - last) / last * 100), 2)
                if len(history) >= 5:
                    p5 = history[4]['close_price']
                    if p5 and history[0]['close_price']:
                        item['chg_5d'] = round(float((history[0]['close_price'] - p5) / p5 * 100), 2)
            result["overseas"] = _serialize(overseas)
            result["overseas_sector"] = sector
        except Exception as e:
            logger.debug("海外数据查询: %s", e)
            result["overseas"] = []
            result["overseas_sector"] = ''

        return {"success": True, "data": result}
    except Exception as e:
        logger.error("个股详情查询失败 [%s]: %s", stock_code, e, exc_info=True)
        return {"success": False, "error": str(e)}
    finally:
        cur.close()
        conn.close()
