"""个股详情页 API — 聚合K线、预测、财报、概念板块等数据"""
import datetime as _dt
import json
import logging
from decimal import Decimal

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse

from dao import get_connection

logger = logging.getLogger(__name__)

router = APIRouter()


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

        # 1. 120天K线数据（涨跌幅、量价分析）
        cur.execute(
            "SELECT `date`, open_price, close_price, high_price, low_price, "
            "volume, amount, change_percent, turnover_rate "
            "FROM stock_kline WHERE stock_code = %s "
            "ORDER BY `date` DESC LIMIT 120",
            (stock_code,),
        )
        klines = _serialize(cur.fetchall())
        result["klines"] = klines

        # 2. 最新预测数据（本周 + 下周V11 + OBV V5）
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
            "nw_backtest_accuracy, nw_actual_direction, nw_actual_chg, nw_is_correct "
            "FROM stock_weekly_prediction_history "
            "WHERE stock_code = %s AND nw_pred_direction IS NOT NULL "
            "ORDER BY predict_date DESC LIMIT 20",
            (stock_code,),
        )
        result["nw_history"] = _serialize(cur.fetchall())

        # 5. OBV V5预测历史
        cur.execute(
            "SELECT predict_date, iso_year, iso_week, "
            "v5_pred_direction, v5_confidence, v5_strategy, v5_reason, "
            "v5_win_rate, v5_signal_date, v5_actual_direction, "
            "v5_actual_5d_chg, v5_is_correct "
            "FROM stock_weekly_prediction_history "
            "WHERE stock_code = %s AND v5_pred_direction IS NOT NULL "
            "ORDER BY predict_date DESC LIMIT 20",
            (stock_code,),
        )
        result["v5_history"] = _serialize(cur.fetchall())

        # 6. 资金流向（最近60天）
        cur.execute(
            "SELECT `date`, close_price, change_pct, net_flow, main_net_5day, "
            "big_net, big_net_pct, mid_net, mid_net_pct, small_net, small_net_pct "
            "FROM stock_fund_flow WHERE stock_code = %s "
            "ORDER BY `date` DESC LIMIT 60",
            (stock_code,),
        )
        result["fund_flow"] = _serialize(cur.fetchall())

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
        result["finance"] = _serialize(finance_rows)

        # 8. 所属概念板块 + 板块强弱势
        cur.execute(
            "SELECT DISTINCT s.board_code, s.board_name "
            "FROM stock_concept_board_stock s WHERE s.stock_code = %s",
            (stock_code,),
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
            (stock_code,),
        )
        result["stock_strength"] = _serialize(cur.fetchall())

        # 10. CAN SLIM 最新分析（从 stock_analysis_detail 取最新一条）
        cur.execute(
            "SELECT d.*, b.batch_name FROM stock_analysis_detail d "
            "LEFT JOIN stock_analysis_batch b ON d.batch_id = b.id "
            "WHERE d.stock_code = %s ORDER BY d.updated_at DESC LIMIT 1",
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

        return {"success": True, "data": result}
    except Exception as e:
        logger.error("个股详情查询失败 [%s]: %s", stock_code, e, exc_info=True)
        return {"success": False, "error": str(e)}
    finally:
        cur.close()
        conn.close()
