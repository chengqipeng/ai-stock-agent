"""
从 stock_news(forecast) 提取业绩预告数据写入 stock_finance 表。

规则：
- 只处理 news_type='forecast' 的记录
- 从 title 和 content 中解析报告期、净利润预测、同比增幅、每股收益等
- 如果 stock_finance 中已有该股票该报告期的正式财报数据 → 跳过
- 如果没有正式财报 → 写入预告数据（report_period_name 标记为 "预告"）
- 正式财报拉取时会自动覆盖预告数据（UPSERT on stock_code + report_date）

用法: .venv/bin/python tools/extract_forecast_to_finance.py
"""
import json
import logging
import re
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _parse_amount(text: str) -> float | None:
    """解析金额文本，如 '9033.86万' → 9033.86, '1.80亿' → 18000.0"""
    if not text:
        return None
    text = text.strip().replace(',', '').replace('，', '')
    m = re.match(r'^([+-]?\d+\.?\d*)\s*(万|亿)?', text)
    if not m:
        return None
    val = float(m.group(1))
    unit = m.group(2)
    if unit == '亿':
        val *= 10000  # 转万元
    return val


def _parse_pct(text: str) -> float | None:
    """解析百分比文本，如 '100.00%' → 100.0"""
    if not text or text.strip() == '-':
        return None
    m = re.match(r'^([+-]?\d+\.?\d*)%?', text.strip())
    return float(m.group(1)) if m else None


def parse_forecast_content(title: str, content: str) -> dict | None:
    """从业绩预告的 title + content 中解析结构化数据。

    Returns:
        {
            'report_date': '2026-03-31',
            'report_period_name': '2026一季报预告',
            'net_profit_low': 9033.86,  # 万元
            'net_profit_high': 9937.25,
            'net_profit_yoy_low': 100.0,  # %
            'net_profit_yoy_high': 120.0,
            'eps_low': 0.26,
            'eps_high': 0.28,
            'deducted_profit_low': ...,
            'deducted_profit_high': ...,
            'change_reason': '...',
        }
    """
    if not content:
        return None

    result = {}

    # 1. 报告期
    m = re.search(r'\((\d{4}-\d{2}-\d{2})\)', title or '')
    if m:
        result['report_date'] = m.group(1)
    else:
        m = re.search(r'报告期:\s*(\d{4}-\d{2}-\d{2})', content)
        if m:
            result['report_date'] = m.group(1)

    if not result.get('report_date'):
        return None

    # 2. 推断报告期名称
    rd = result['report_date']
    month = rd[5:7]
    year = rd[:4]
    period_map = {'03': '一季报', '06': '中报', '09': '三季报', '12': '年报'}
    period_name = period_map.get(month, '')
    result['report_period_name'] = f"{year}{period_name}预告"

    # 3. 解析各指标段落
    # 按 [...] 分段
    sections = re.split(r'\[([^\]]+)\]', content)
    # sections: ['前文', '归属于上市公司股东的净利润', '内容...', '扣非净利润', '内容...', ...]

    for i in range(1, len(sections) - 1, 2):
        section_name = sections[i].strip()
        section_body = sections[i + 1]

        # 预测金额
        amt_m = re.search(r'预测金额:\s*(.+?)~\s*(.+?)(?:\n|$)', section_body)
        # 同比增幅
        yoy_m = re.search(r'同比增幅:\s*(.+?)~\s*(.+?)(?:\n|$)', section_body)
        # 变动原因
        reason_m = re.search(r'变动原因:\s*(.+?)(?:\n\[|$)', section_body, re.DOTALL)

        if '归属' in section_name and '净利润' in section_name:
            if amt_m:
                result['net_profit_low'] = _parse_amount(amt_m.group(1))
                result['net_profit_high'] = _parse_amount(amt_m.group(2))
            if yoy_m:
                result['net_profit_yoy_low'] = _parse_pct(yoy_m.group(1))
                result['net_profit_yoy_high'] = _parse_pct(yoy_m.group(2))
            if reason_m:
                result['change_reason'] = reason_m.group(1).strip()[:500]

        elif '扣除' in section_name or '扣非' in section_name:
            if amt_m:
                result['deducted_profit_low'] = _parse_amount(amt_m.group(1))
                result['deducted_profit_high'] = _parse_amount(amt_m.group(2))
            if yoy_m:
                result['deducted_yoy_low'] = _parse_pct(yoy_m.group(1))
                result['deducted_yoy_high'] = _parse_pct(yoy_m.group(2))

        elif '每股收益' in section_name:
            if amt_m:
                result['eps_low'] = _parse_amount(amt_m.group(1))
                result['eps_high'] = _parse_amount(amt_m.group(2))

    # 如果没有分段格式，尝试从 title 解析
    if 'net_profit_low' not in result:
        m = re.search(r'净利润\s*([+-]?\d+[\d,.]*(?:万|亿)?)\s*~\s*([+-]?\d+[\d,.]*(?:万|亿)?)', title or '')
        if m:
            result['net_profit_low'] = _parse_amount(m.group(1))
            result['net_profit_high'] = _parse_amount(m.group(2))
        m2 = re.search(r'同比\s*([+-]?\d+\.?\d*)%?\s*~\s*([+-]?\d+\.?\d*)%', title or '')
        if m2:
            result['net_profit_yoy_low'] = _parse_pct(m2.group(1))
            result['net_profit_yoy_high'] = _parse_pct(m2.group(2))
        # 营收
        m3 = re.search(r'营收\s*([+-]?\d+[\d,.]*(?:万|亿)?)\s*~\s*([+-]?\d+[\d,.]*(?:万|亿)?)', title or '')
        if m3:
            result['revenue_low'] = _parse_amount(m3.group(1))
            result['revenue_high'] = _parse_amount(m3.group(2))

    return result if result.get('report_date') else None


def build_finance_json(forecast: dict, stock_code: str) -> dict:
    """将解析后的预告数据构建为 stock_finance 的 data_json 格式"""
    def _fmt_wan(v):
        """万元格式化为带单位字符串"""
        if v is None:
            return None
        if abs(v) >= 10000:
            return f"{v / 10000:.2f}亿"
        return f"{v:.2f}万"

    data = {
        '报告期': forecast.get('report_period_name', ''),
        '报告日期': forecast.get('report_date', ''),
        '数据来源': '业绩预告',
    }

    # 归母净利润
    low = forecast.get('net_profit_low')
    high = forecast.get('net_profit_high')
    if low is not None and high is not None:
        mid = (low + high) / 2
        data['归母净利润(元)'] = _fmt_wan(mid)
        data['归母净利润_预告下限(万)'] = low
        data['归母净利润_预告上限(万)'] = high
    elif low is not None:
        data['归母净利润(元)'] = _fmt_wan(low)

    # 同比
    yoy_low = forecast.get('net_profit_yoy_low')
    yoy_high = forecast.get('net_profit_yoy_high')
    if yoy_low is not None and yoy_high is not None:
        data['归属净利润同比增长(%)'] = round((yoy_low + yoy_high) / 2, 2)
        data['归属净利润同比_预告下限(%)'] = yoy_low
        data['归属净利润同比_预告上限(%)'] = yoy_high
    elif yoy_low is not None:
        data['归属净利润同比增长(%)'] = yoy_low

    # 扣非净利润
    d_low = forecast.get('deducted_profit_low')
    d_high = forecast.get('deducted_profit_high')
    if d_low is not None and d_high is not None:
        data['扣非净利润(元)'] = _fmt_wan((d_low + d_high) / 2)
    elif d_low is not None:
        data['扣非净利润(元)'] = _fmt_wan(d_low)

    # EPS
    eps_low = forecast.get('eps_low')
    eps_high = forecast.get('eps_high')
    if eps_low is not None and eps_high is not None:
        data['基本每股收益(元)'] = round((eps_low + eps_high) / 2, 4)
    elif eps_low is not None:
        data['基本每股收益(元)'] = eps_low

    # 变动原因
    if forecast.get('change_reason'):
        data['业绩变动原因'] = forecast['change_reason']

    # 营收
    rev_low = forecast.get('revenue_low')
    rev_high = forecast.get('revenue_high')
    if rev_low is not None and rev_high is not None:
        data['营业总收入(元)'] = _fmt_wan((rev_low + rev_high) / 2)
    elif rev_low is not None:
        data['营业总收入(元)'] = _fmt_wan(rev_low)

    return data


def run_extraction(batch_size: int = 500, dry_run: bool = False):
    """主流程：从 stock_news 提取 forecast 写入 stock_finance"""
    from dao import get_connection
    from datetime import datetime
    from zoneinfo import ZoneInfo

    _CST = ZoneInfo("Asia/Shanghai")
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 1. 获取所有 forecast 的 title（轻量查询，content 按需加载）
    logger.info("加载 forecast 标题...")
    forecasts = []
    last_id = 0
    page_size = 10000
    while True:
        cur.execute("""
            SELECT id, stock_code, title
            FROM stock_news
            WHERE news_type = 'forecast' AND id > %s
            ORDER BY id
            LIMIT %s
        """, (last_id, page_size))
        batch = cur.fetchall()
        if not batch:
            break
        forecasts.extend(batch)
        last_id = batch[-1]['id']
        logger.info("  已加载 %d 条", len(forecasts))

    logger.info("共 %d 条 forecast 记录", len(forecasts))

    # 2. 获取 stock_finance 中已有的正式财报（非预告）的 (stock_code, report_date) 集合
    logger.info("加载已有财报记录...")
    cur.execute("""
        SELECT stock_code, report_date, report_period_name
        FROM stock_finance
    """)
    existing = {}  # (code, report_date) -> period_name
    for r in cur.fetchall():
        key = (r['stock_code'], str(r['report_date']))
        existing[key] = r['report_period_name'] or ''

    logger.info("已有 %d 条财报记录", len(existing))
    cur.close()
    conn.close()

    # 3. 解析并筛选
    parsed = 0
    skipped_has_report = 0
    skipped_parse_fail = 0
    to_insert = []  # [(stock_code, report_date, period_name, data_json)]
    seen = set()  # 去重: (stock_code, report_date)

    for f in forecasts:
        code = f['stock_code']
        title = f['title'] or ''

        # 从 title 解析报告期和净利润（title 格式统一，包含所有关键信息）
        forecast = parse_forecast_content(title, title)
        if not forecast or not forecast.get('report_date'):
            skipped_parse_fail += 1
            continue

        parsed += 1
        rd = forecast['report_date']
        key = (code, rd)

        # 去重（同一股票同一报告期只取最新的一条）
        if key in seen:
            continue
        seen.add(key)

        # 检查是否已有正式财报
        existing_period = existing.get(key, None)
        if existing_period is not None and '预告' not in existing_period:
            # 已有正式财报，跳过
            skipped_has_report += 1
            continue

        # 构建 data_json
        data = build_finance_json(forecast, code)
        period_name = forecast.get('report_period_name', '')
        data_json = json.dumps(data, ensure_ascii=False)
        to_insert.append((code, rd, period_name, data_json))

    logger.info("解析完成: 成功%d 解析失败%d 已有正式财报%d 待写入%d",
                parsed, skipped_parse_fail, skipped_has_report, len(to_insert))

    if dry_run:
        logger.info("[DRY RUN] 不写入数据库")
        for code, rd, pn, dj in to_insert[:5]:
            logger.info("  %s %s %s: %s", code, rd, pn, dj[:100])
        return

    # 4. 写入数据库
    if not to_insert:
        logger.info("无需写入")
        return

    conn = get_connection()
    cur = conn.cursor()
    now_str = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
    inserted = 0
    try:
        for i in range(0, len(to_insert), batch_size):
            batch = to_insert[i:i + batch_size]
            cur.executemany(
                """INSERT INTO stock_finance
                   (stock_code, report_date, report_period_name, data_json, updated_at)
                   VALUES (%s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                       report_period_name = CASE
                           WHEN report_period_name IS NULL
                                OR report_period_name LIKE '%%预告%%'
                                OR report_period_name = ''
                           THEN VALUES(report_period_name)
                           ELSE report_period_name
                       END,
                       data_json = CASE
                           WHEN report_period_name IS NULL
                                OR report_period_name LIKE '%%预告%%'
                                OR report_period_name = ''
                           THEN VALUES(data_json)
                           ELSE data_json
                       END,
                       updated_at = CASE
                           WHEN report_period_name IS NULL
                                OR report_period_name LIKE '%%预告%%'
                                OR report_period_name = ''
                           THEN VALUES(updated_at)
                           ELSE updated_at
                       END
                """,
                [(code, rd, pn, dj, now_str) for code, rd, pn, dj in batch]
            )
            inserted += len(batch)
            conn.commit()
            logger.info("  写入 %d/%d", inserted, len(to_insert))

        logger.info("写入完成: %d 条预告数据", inserted)
    except Exception as e:
        conn.rollback()
        logger.error("写入失败: %s", e, exc_info=True)
    finally:
        cur.close()
        conn.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='提取业绩预告到财报表')
    parser.add_argument('--dry-run', action='store_true', help='只解析不写入')
    args = parser.parse_args()

    run_extraction(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
