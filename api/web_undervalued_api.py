"""低估值股票筛选 API"""
import json
import re
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse

from dao import get_connection
from dao.stock_undervalued_dao import (
    create_undervalued_table, upsert_pick,
    get_pick_history, get_pick_detail, get_pick_by_date,
)

logger = logging.getLogger(__name__)
_CST = ZoneInfo("Asia/Shanghai")
router = APIRouter()


# ── 行业关键词 ──
_TECH_KW = [
    '芯片','半导体','集成电路','光刻','封装','存储','GPU','AI','人工智能','算力',
    '大模型','机器人','自动驾驶','智能驾驶','激光','光伏','新能源','锂电','储能',
    '风电','5G','通信','物联网','云计算','大数据','网络安全','信创','操作系统',
    '数据库','软件','SaaS','EDA','传感器','MEMS','光学','显示','面板','LED',
    'Mini LED','消费电子','PCB','连接器','被动元件','电子','科技','信息技术',
    '数字经济','卫星','北斗','航天','军工电子','雷达','无人机','低空经济',
]
_MFG_KW = [
    '高端制造','智能制造','工业母机','数控','机床','工业自动化','减速器','伺服',
    '精密制造','模具','轴承','液压','气动','阀门','泵','压缩机','注塑',
    '新材料','碳纤维','复合材料','特种钢','钛合金','稀土','磁材',
    '汽车零部件','汽零','电驱','电控','线束','底盘','制动','转向',
    '航空航天','发动机','航空装备','军工','国防',
    '医疗器械','医疗设备','体外诊断','基因','生物','创新药',
    '锂电设备','光伏设备','风电设备','氢能','核电',
    '工程机械','叉车','起重','矿山','盾构',
]
_ALL_KW = _TECH_KW + _MFG_KW


def _pa(v):
    if v is None: return None
    if isinstance(v, (int, float)): return v
    s = str(v)
    m = re.match(r'^([-\d.]+)\s*亿$', s)
    if m: return float(m.group(1)) * 1e8
    m = re.match(r'^([-\d.]+)\s*万$', s)
    if m: return float(m.group(1)) * 1e4
    try: return float(s)
    except: return None


def _sf(v):
    try: return float(v) if v is not None else None
    except: return None


def _is_tech_mfg(boards_str: str) -> bool:
    t = (boards_str or '').lower()
    return any(kw.lower() in t for kw in _ALL_KW)


def _get_sector(boards_str: str) -> str:
    tags, seen = [], set()
    for kw in _ALL_KW:
        if kw in (boards_str or '') and kw not in seen:
            seen.add(kw); tags.append(kw)
        if len(tags) >= 4: break
    return '/'.join(tags)


def _run_screening(mcap_min: float = 80, mcap_max: float = 500,
                   top_n: int = 20) -> tuple[int, list[dict]]:
    """执行筛选，返回 (筛选池总数, TOP N 列表)"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        # 最新财报
        cur.execute('''
            SELECT f.stock_code, f.report_period_name, f.data_json
            FROM stock_finance f INNER JOIN (
                SELECT stock_code, MAX(report_date) as md
                FROM stock_finance WHERE report_date >= '2024-06-30'
                GROUP BY stock_code
            ) l ON f.stock_code = l.stock_code AND f.report_date = l.md
        ''')
        fmap = {r['stock_code']: r for r in cur.fetchall()}

        # 最新价格
        cur.execute('SELECT MAX(date) as d FROM stock_kline')
        md = cur.fetchone()['d']
        cur.execute('SELECT stock_code, close_price FROM stock_kline WHERE date = %s', (md,))
        pmap = {r['stock_code']: r['close_price'] for r in cur.fetchall()}

        # 股票名称 + 概念板块（从 weekly_prediction 表获取）
        cur.execute(
            'SELECT stock_code, stock_name, concept_boards '
            'FROM stock_weekly_prediction')
        predmap = {r['stock_code']: r for r in cur.fetchall()}

        # 概念板块
        cur.execute(
            'SELECT stock_code, GROUP_CONCAT(board_name) as boards '
            'FROM stock_concept_board_stock GROUP BY stock_code')
        bmap = {r['stock_code']: r['boards'] or '' for r in cur.fetchall()}

        # 120日前价格
        cur.execute('SELECT DISTINCT date FROM stock_kline ORDER BY date DESC LIMIT 120')
        dates = [r['date'] for r in cur.fetchall()]
        d120 = dates[-1] if len(dates) >= 120 else None
        c120map = {}
        if d120:
            cur.execute('SELECT stock_code, close_price FROM stock_kline WHERE date=%s', (d120,))
            for r in cur.fetchall():
                pn = pmap.get(r['stock_code'])
                if pn and r['close_price'] and r['close_price'] > 0:
                    c120map[r['stock_code']] = round((pn - r['close_price']) / r['close_price'] * 100, 2)

        cands = []
        for code, fin in fmap.items():
            if code.endswith('.BJ'): continue
            pr = predmap.get(code, {})
            nm = pr.get('stock_name', '')
            if not nm or 'ST' in nm: continue
            price = pmap.get(code)
            if not price: continue

            code6 = code.split('.')[0]
            boards = bmap.get(code6, '') + ' ' + (pr.get('concept_boards') or '')
            if not _is_tech_mfg(boards): continue

            d = json.loads(fin['data_json']) if fin['data_json'] else {}
            eps = _sf(d.get('基本每股收益(元)'))
            bps = _sf(d.get('每股净资产(元)'))
            profit = _pa(d.get('归母净利润(元)'))
            roe = _sf(d.get('净资产收益率(加权)(%)'))
            gm = _sf(d.get('毛利率(%)'))
            nm2 = _sf(d.get('净利率(%)'))
            ry = _sf(d.get('营业总收入同比增长(%)'))
            py = _sf(d.get('归属净利润同比增长(%)'))
            debt = _sf(d.get('资产负债率(%)'))
            cr = _sf(d.get('流动比率'))
            ocf = _sf(d.get('每股经营现金流(元)'))
            rev = _pa(d.get('营业总收入(元)'))

            if not eps or eps <= 0 or not profit or profit <= 0 or not bps or bps <= 0:
                continue
            mcap = profit / eps * price / 1e8
            if mcap < mcap_min or mcap > mcap_max: continue

            prd = fin.get('report_period_name', '')
            if '三季' in prd: ea = eps * 4 / 3
            elif '中报' in prd or '半年' in prd: ea = eps * 2
            elif '一季' in prd: ea = eps * 4
            else: ea = eps

            pe = price / ea if ea > 0 else None
            pb = price / bps if bps > 0 else None
            if roe is None or roe < 5: continue
            if debt and debt > 65: continue
            if pe is None or pe > 35 or pe < 0: continue
            if py is not None and py < -15: continue

            sc = 0
            if pe < 10: sc += 3
            elif pe < 15: sc += 2
            elif pe < 20: sc += 1
            if pb and pb < 1.5: sc += 3
            elif pb and pb < 2: sc += 2
            elif pb and pb < 3: sc += 1
            if roe > 15: sc += 3
            elif roe > 10: sc += 2
            elif roe > 7: sc += 1
            if gm and gm > 40: sc += 3
            elif gm and gm > 25: sc += 2
            elif gm and gm > 15: sc += 1
            if ry and ry > 20: sc += 2
            elif ry and ry > 5: sc += 1
            if py and py > 30: sc += 2
            elif py and py > 5: sc += 1
            if debt and debt < 25: sc += 2
            elif debt and debt < 45: sc += 1
            if ocf and ocf > 0: sc += 1
            if ocf and eps and ocf > eps * 0.7: sc += 1
            c120 = c120map.get(code)
            if c120 is not None and c120 < -25: sc += 2
            elif c120 is not None and c120 < -10: sc += 1

            sector = _get_sector(boards)

            cands.append({
                'code': code, 'name': nm, 'price': round(price, 2),
                'mcap': round(mcap, 0), 'pe': round(pe, 1) if pe else None,
                'pb': round(pb, 2) if pb else None,
                'roe': round(roe, 1) if roe else None,
                'gm': round(gm, 1) if gm else None,
                'nm': round(nm2, 1) if nm2 else None,
                'ry': round(ry, 1) if ry else None,
                'py': round(py, 1) if py else None,
                'debt': round(debt, 1) if debt else None,
                'cr': round(cr, 1) if cr else None,
                'ocf': round(ocf, 2) if ocf else None,
                'c120': c120, 'sc': sc, 'sector': sector,
            })

        total = len(cands)
        cands.sort(key=lambda x: (-x['sc'], x['pe'] or 999))
        return total, cands[:top_n]
    finally:
        cur.close()
        conn.close()


# ── 页面路由 ──

@router.get("/undervalued", response_class=HTMLResponse)
async def undervalued_page():
    with open("static/undervalued.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read(), headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache", "Expires": "0",
        })


# ── API 路由 ──

@router.post("/api/undervalued/generate")
async def generate_undervalued(
    mcap_min: float = Query(80), mcap_max: float = Query(500), top_n: int = Query(20),
):
    """生成低估值筛选记录（每天同条件只存一次，重复点击覆盖更新）"""
    create_undervalued_table()
    today = datetime.now(_CST).strftime("%Y-%m-%d")

    total, picks = _run_screening(mcap_min, mcap_max, top_n)
    upsert_pick(today, "tech_mfg", mcap_min, mcap_max, total, len(picks), picks)

    return {"success": True, "data": {
        "pick_date": today, "total_screened": total,
        "total_picked": len(picks), "picks": picks,
    }}


@router.get("/api/undervalued/history")
async def undervalued_history(limit: int = Query(30)):
    """获取历史筛选记录列表"""
    create_undervalued_table()
    rows = get_pick_history(limit)
    # 序列化 datetime
    for r in rows:
        if r.get('created_at'):
            r['created_at'] = r['created_at'].isoformat()
    return {"success": True, "data": rows}


@router.get("/api/undervalued/detail")
async def undervalued_detail(id: int = Query(...)):
    """获取某条筛选记录详情"""
    row = get_pick_detail(id)
    if not row:
        return {"success": False, "error": "记录不存在"}
    if row.get('created_at'):
        row['created_at'] = row['created_at'].isoformat()
    return {"success": True, "data": row}
