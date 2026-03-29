"""
同花顺个股新闻公告抓取模块

数据来源：https://stockpage.10jqka.com.cn/{code}/#xwgg
四个维度：
  1. 公司新闻 (news)     — stat="f10_spqk_gsxw"
  2. 公司公告 (notice)   — stat="f10_spqk_gsgg"
  3. 行业资讯 (industry) — stat="f10_spqk_hyzx"
  4. 研究报告 (report)   — stat="f10_spqk_yjbg"

正文抓取策略（按域名区分）：
  - stock.10jqka.com.cn  → .article-content
  - fund.10jqka.com.cn   → .article-content
  - news.10jqka.com.cn/field/sr/ (研报) → .YBText
  - news.10jqka.com.cn   → body 文本（微信转载）
  - notice.10jqka.com.cn → PDF，跳过正文抓取
  - 重定向到微信等外部链接 → 跳过

使用 curl_cffi 模拟浏览器 TLS 指纹绕过反爬。
"""

import asyncio
import logging
import re
import unicodedata
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Literal

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

logger = logging.getLogger(__name__)

IMPERSONATE = "chrome131"
_CST = ZoneInfo("Asia/Shanghai")

NewsType = Literal["news", "notice", "industry", "report"]

TYPE_LABEL: dict[str, str] = {
    "news": "公司新闻",
    "notice": "公司公告",
    "industry": "行业资讯",
    "report": "研究报告",
    "event": "近期重要事件",
    "ranking": "行业排名",
    "forecast": "业绩预告",
}

_STAT_MAP: dict[str, NewsType] = {
    "f10_spqk_gsxw": "news",
    "f10_spqk_gsgg": "notice",
    "f10_spqk_hyzx": "industry",
    "f10_spqk_yjbg": "report",
}


# ── 文本清洗 ──────────────────────────────────────────────

# 需要剔除的无效文本模式
_JUNK_PATTERNS = [
    re.compile(r'加载中\.{0,3}'),
    re.compile(r'点击查看更多.*'),
    re.compile(r'免责声明.*', re.DOTALL),
    re.compile(r'风险提示：.*以上内容仅供参考.*', re.DOTALL),
    re.compile(r'（本文由.*?机器人.*?自动生成）'),
    re.compile(r'数据来源：.*?iFind'),
    re.compile(r'注：以上.*?仅供参考.*'),
    re.compile(r'以上内容由.*?智能生成'),
    re.compile(r'以上内容为.*?自动生成'),
]

# 无效行关键词（整行匹配则丢弃）
_JUNK_LINE_KEYWORDS = [
    '扫码下载', '打开APP', '下载APP', '微信扫码', '关注我们',
    '点击关注', '长按识别', '阅读原文', '返回搜狐', '责任编辑',
    '声明：', '版权归原作者', '如有侵权', '联系删除',
    'Original', '在小说阅读器中沉浸阅读', '融媒体记者',
    '东西问', '理论·评论', '中国侨网', '大湾区', '一带一路',
    '中国新闻周刊', '国是直通车', '中外对话',
    '铸牢中华民族共同体意识', '中国—东盟商贸资讯',
    '举报邮箱', '举报电话', '网上有害信息举报',
    '京ICP', '沪ICP', '粤ICP', '浙ICP', '备案号',
    '客服电话', '投诉建议', '意见反馈',
    '数字报', '网站地图', '设为首页', '加入收藏',
    'APP APP', 'DoNews',
    '极客圈', '帅作文', 'International', '职教院',
    '广角镜', 'Z世代', '洋腔队', '舆论场', '新漫评',
    '思享家', '会客厅', '研究院', '问西东', '中国新观察', '三里河',
    '世界观', '民生',
]


def _clean_content(raw_text: str) -> str:
    """清洗正文内容：去除乱码、广告、无效文本"""
    if not raw_text:
        return ""

    # 1. 替换常见乱码和不可见字符
    text = raw_text
    # 替换零宽字符、BOM等
    text = text.replace('\ufeff', '').replace('\u200b', '').replace('\u200c', '')
    text = text.replace('\u200d', '').replace('\u00a0', ' ').replace('\xa0', ' ')

    # 2. 移除控制字符（保留换行和制表符）
    text = ''.join(
        c for c in text
        if c in ('\n', '\t', '\r') or (not unicodedata.category(c).startswith('C'))
    )

    # 3. 按行处理
    lines = text.split('\n')
    clean_lines = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        # 跳过纯数字行（表格残留）
        if re.match(r'^[\d,.\-\s%]+$', line):
            continue
        # 跳过过短的行（导航残留）
        if len(line) < 3:
            continue
        # 跳过包含无效关键词的行
        if any(kw in line for kw in _JUNK_LINE_KEYWORDS):
            continue
        clean_lines.append(line)

    text = '\n'.join(clean_lines)

    # 4. 应用正则清洗模式
    for pat in _JUNK_PATTERNS:
        text = pat.sub('', text)

    # 5. 合并多余空行
    text = re.sub(r'\n{3,}', '\n\n', text)

    # 6. 检测乱码：如果文本中包含大量替换字符或不可读字符，视为无效
    replacement_chars = text.count('\ufffd')
    if replacement_chars > len(text) * 0.05 and replacement_chars > 10:
        return ""

    return text.strip()


# ── 正文抓取 ──────────────────────────────────────────────

def _clean_pdf_content(raw_text: str) -> str:
    """PDF公告专用清洗：去除页码、勾选框、水印、页眉页脚等"""
    if not raw_text:
        return ""

    text = raw_text

    # 1. 去除孤立的页码（行首或行尾的纯数字，通常是PDF页码）
    text = re.sub(r'(?m)^\s*\d{1,3}\s*$', '', text)
    # 去除紧贴在"证券代码"前面的页码数字
    text = re.sub(r'^\d{1,3}(?=证券代码)', '', text)
    text = re.sub(r'\n\d{1,3}(?=证券代码)', '\n', text)

    # 2. 去除勾选框符号
    text = text.replace('□', '').replace('√', '').replace('■', '')
    text = text.replace('☑', '').replace('☐', '')

    # 3. 去除PDF常见水印/页眉页脚
    text = re.sub(r'(?m)^.*巨潮资讯网.*$', '', text)
    text = re.sub(r'(?m)^.*www\.cninfo\.com\.cn.*$', '', text)
    text = re.sub(r'(?m)^.*仅供参考，请以正式公告为准.*$', '', text)

    # 4. 合并被PDF分页打断的段落（行尾非标点+下一行行首非空格/数字/标点）
    text = re.sub(r'(?<=[^\n。！？；：，、）》\]\s])\n(?=[^\n\s\d（《\[一二三四五六七八九十])', '', text)

    # 5. 去除多余空白
    text = re.sub(r'[ \t]{2,}', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)

    # 6. 通用清洗
    text = _clean_content(text)

    return text


async def _fetch_pdf_content(url: str) -> str:
    """下载PDF并提取文本内容"""
    try:
        from common.utils.pdf_parser import PDFParser
        import os

        txt_path, status = await PDFParser.download_and_parse(url, max_retries=2)
        if not txt_path or status != "success":
            logger.debug("[PDF抓取] %s 失败: %s", url, status)
            return ""

        with open(txt_path, 'r', encoding='utf-8') as f:
            text = f.read()

        # 清理临时文件
        try:
            os.remove(txt_path)
        except Exception:
            pass

        # 清洗PDF文本（PDF专用清洗 + 通用清洗）
        text = _clean_pdf_content(text)

        # 限制长度
        if len(text) > 8000:
            text = text[:8000] + '...'

        return text

    except Exception as e:
        logger.debug("[PDF抓取] %s 异常: %s", url, e)
        return ""


async def _fetch_article_content(url: str, session: AsyncSession) -> str:
    """抓取单篇文章正文

    根据 URL 域名选择不同的解析策略：
    - notice.10jqka.com.cn (PDF) → 下载PDF并提取文本
    - news.10jqka.com.cn/field/sr/ (研报) → .YBText
    - stock/fund.10jqka.com.cn → .article-content
    - news.10jqka.com.cn → body 文本
    - 外部链接/重定向 → 跳过
    """
    if not url:
        return ""

    # PDF公告：下载并解析PDF文本
    if 'notice.10jqka.com.cn' in url or url.endswith('.pdf'):
        return await _fetch_pdf_content(url)

    # 非10jqka链接跳过
    if '10jqka.com.cn' not in url:
        return ""

    try:
        resp = await session.get(url, timeout=15)
        resp.raise_for_status()

        # 处理编码：优先用响应头声明的编码，否则尝试 UTF-8 和 GBK
        html = ""
        raw_bytes = resp.content
        # 尝试从 content-type 获取编码
        ct = resp.headers.get("content-type", "")
        if "gbk" in ct.lower() or "gb2312" in ct.lower():
            html = raw_bytes.decode("gbk", errors="replace")
        else:
            try:
                html = raw_bytes.decode("utf-8")
            except UnicodeDecodeError:
                html = raw_bytes.decode("gbk", errors="replace")

        # 页面太小通常是重定向页
        if len(html) < 500:
            return ""

        # 检测乱码：如果包含大量替换字符，说明编码不对，尝试另一种
        if html.count('\ufffd') > 10:
            try:
                html = raw_bytes.decode("gbk", errors="replace")
            except Exception:
                pass

        soup = BeautifulSoup(html, "html.parser")

        # 检测"无法在线阅读，请下载原文"页面 → 提取PDF链接并解析
        body_text = soup.get_text(strip=True)
        if "请下载原文" in body_text or "无法在线阅读" in body_text:
            # 优先从 <a> 标签找 PDF 链接
            pdf_url = ""
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if ".pdf" in href.lower() or ".PDF" in href:
                    pdf_url = href
                    break
            # 也从 script 中找 notice.10jqka.com.cn 的 PDF
            if not pdf_url:
                for script in soup.find_all("script"):
                    text = script.string or ""
                    m = re.search(r'(https?://notice\.10jqka\.com\.cn/api/pdf/[^\s"\']+\.pdf)', text)
                    if m:
                        pdf_url = m.group(1)
                        break
            if pdf_url:
                return await _fetch_pdf_content(pdf_url)

        # 移除 script/style 标签
        for tag in soup.find_all(['script', 'style', 'iframe', 'noscript']):
            tag.decompose()

        content = ""

        # 策略1: 研究报告 → .YBText
        if '/field/sr/' in url:
            el = soup.select_one('.YBText')
            if el:
                content = el.get_text(strip=True, separator='\n')

        # 策略2: .article-content (stock/fund 域名)
        if not content:
            el = soup.select_one('.article-content')
            if el:
                content = el.get_text(strip=True, separator='\n')

        # 策略3: body 文本兜底（微信转载等）
        if not content:
            # 先尝试更精确的选择器
            for sel in ['.rich_media_content', '#js_content', '.news-content',
                        '.article-body', '.content-article', '.main-content']:
                el = soup.select_one(sel)
                if el:
                    content = el.get_text(strip=True, separator='\n')
                    if len(content) > 50:
                        break

        if not content:
            body = soup.find('body')
            if body:
                # 移除导航、页头页脚等无关区域
                for nav in body.find_all(['nav', 'header', 'footer', 'aside']):
                    nav.decompose()
                for cls in ['nav', 'header', 'footer', 'sidebar', 'breadcrumb',
                            'comment', 'recommend', 'related', 'share', 'toolbar',
                            'topbar', 'bottom', 'copyright', 'menu', 'banner']:
                    for el in body.find_all(class_=lambda c: c and cls in ' '.join(c).lower()):
                        el.decompose()
                # 尝试找到最大的文本块（通常是正文）
                best_block = ""
                for div in body.find_all('div'):
                    # 跳过有太多子div的容器（通常是布局容器）
                    if len(div.find_all('div')) > 5:
                        continue
                    t = div.get_text(strip=True, separator='\n')
                    if len(t) > len(best_block) and len(t) > 100:
                        best_block = t
                if best_block:
                    content = best_block
                else:
                    raw = body.get_text(strip=True, separator='\n')
                    if len(raw) > 100:
                        content = raw

        # 清洗
        content = _clean_content(content)

        # 限制长度（避免存储过大）
        if len(content) > 5000:
            content = content[:5000] + '...'

        return content

    except Exception as e:
        err_str = str(e)
        # Cloudflare 反爬错误（521/522/523）应抛出，让调用方重试
        if any(f'52{c}' in err_str for c in '123'):
            raise
        logger.debug("[正文抓取] %s 失败: %s", url, e)
        return ""


# ── 列表解析 ──────────────────────────────────────────────

def _normalize_date(date_str: str) -> tuple[str, str]:
    """将日期字符串标准化为 (publish_date, publish_time)"""
    date_str = date_str.strip()
    if not date_str:
        return "", ""

    m = re.match(r'^(\d{4}-\d{2}-\d{2})$', date_str)
    if m:
        return m.group(1), ""

    m = re.match(r'^(\d{2}-\d{2})\s+(\d{2}:\d{2})$', date_str)
    if m:
        year = datetime.now(_CST).year
        return f"{year}-{m.group(1)}", m.group(2)

    m = re.match(r'^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2})$', date_str)
    if m:
        return m.group(1), m.group(2)

    return date_str, ""


def _parse_stockpage_html(html: str, stock_code: str) -> dict[str, list[dict]]:
    """解析同花顺个股主页 HTML，提取四类新闻数据"""
    soup = BeautifulSoup(html, "html.parser")
    result: dict[str, list[dict]] = {
        "news": [], "notice": [], "industry": [], "report": [],
    }

    for stat_val, news_type in _STAT_MAP.items():
        ul = soup.find("ul", attrs={"stat": stat_val})
        if not ul:
            continue

        items = []
        for li in ul.find_all("li", class_="clearfix"):
            title_span = li.find("span", class_="news_title")
            if not title_span:
                continue
            a_tag = title_span.find("a")
            if not a_tag:
                continue

            title = a_tag.get_text(strip=True)
            href = a_tag.get("href", "")

            if not title or len(title) < 2:
                continue

            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                href = "https://stockpage.10jqka.com.cn" + href

            date_span = li.find("span", class_="news_date")
            date_text = ""
            if date_span:
                em = date_span.find("em")
                date_text = em.get_text(strip=True) if em else date_span.get_text(strip=True)

            publish_date, publish_time = _normalize_date(date_text)

            if not publish_date:
                url_date = re.search(r'/(\d{4})(\d{2})(\d{2})/', href)
                if url_date:
                    publish_date = f"{url_date.group(1)}-{url_date.group(2)}-{url_date.group(3)}"

            items.append({
                "news_type": news_type,
                "title": title,
                "url": href,
                "publish_date": publish_date,
                "publish_time": publish_time,
                "source": "同花顺",
                "content": "",
            })

        result[news_type] = items

    return result


async def _fetch_industry_ranking(code: str, session: AsyncSession) -> list[dict]:
    """抓取行业排名数据（来自 basic.10jqka.com.cn/field.html）

    数据源：http://basic.10jqka.com.cn/{code}/field.html
    解析 hy3_div（细分行业排名）表格。
    """
    url = f"http://basic.10jqka.com.cn/{code}/field.html"
    try:
        resp = await session.get(url, timeout=20)
        resp.raise_for_status()
        html = resp.content.decode("gbk", errors="replace")
        soup = BeautifulSoup(html, "html.parser")

        # 行业分类
        tip = soup.find("span", class_="tip")
        industry_name = tip.get_text(strip=True) if tip else ""

        today_str = datetime.now(_CST).strftime("%Y-%m-%d")
        items = []

        # 解析 hy3 细分行业排名（与目标股票同一细分行业）
        for table_id, tab_name in [("hy3_table_1", "hy3_1"), ("hy3_table_2", "hy3_2")]:
            table = soup.find("table", id=table_id)
            if not table:
                continue

            # 获取报告期
            tab_li = soup.find("li", attrs={"name": tab_name})
            period = tab_li.get_text(strip=True) if tab_li else ""

            # 解析表头
            headers = []
            thead = table.find("thead")
            if thead:
                headers = [th.get_text(strip=True) for th in thead.find_all("th")]

            # 解析数据行
            tbody = table.find("tbody")
            if not tbody:
                continue

            ranking_rows = []
            target_rank = 0
            total_companies = 0
            for tr in tbody.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) < len(headers):
                    continue
                row_data = {}
                for i, h in enumerate(headers):
                    val = tds[i].get_text(strip=True) if i < len(tds) else ""
                    row_data[h] = val
                ranking_rows.append(row_data)
                total_companies += 1
                # 找到目标股票的排名
                if row_data.get("股票代码", "") == code:
                    target_rank = total_companies

            if not ranking_rows:
                continue

            # 构建内容文本
            content_lines = [
                f"行业分类: {industry_name}",
                f"报告期: {period}",
                f"行业内公司数: {total_companies}",
                f"本股排名: 第{target_rank}名" if target_rank else "本股排名: -",
                "",
            ]

            # 目标股票的数据
            target_data = next((r for r in ranking_rows if r.get("股票代码") == code), None)
            if target_data:
                content_lines.append("本股数据:")
                for k, v in target_data.items():
                    if k not in ("股票代码", "股票简称", "排名"):
                        content_lines.append(f"  {k}: {v}")
                content_lines.append("")

            # 前5名
            content_lines.append("行业前5名:")
            for i, row in enumerate(ranking_rows[:5]):
                name = row.get("股票简称", "")
                eps = row.get("每股收益(元)", "")
                revenue = row.get("营业总收入(元)", "")
                profit = row.get("净利润(元)", "")
                content_lines.append(f"  {i+1}. {name} 每股收益:{eps} 营收:{revenue} 净利润:{profit}")

            content = "\n".join(content_lines)
            content = _clean_content(content)

            title = f"行业排名({period}): {industry_name} 第{target_rank}/{total_companies}名" if target_rank else f"行业排名({period}): {industry_name}"

            items.append({
                "news_type": "ranking",
                "title": title,
                "url": url,
                "publish_date": today_str,
                "publish_time": "",
                "source": "同花顺",
                "content": content,
            })

            # 只取最新一期
            break

        logger.debug("[%s] 解析行业排名: %d 条", code, len(items))
        return items

    except Exception as e:
        logger.error("[%s] 抓取行业排名失败: %s", code, e)
        return []


def _extract_code(stock_code_normalize: str) -> str:
    return stock_code_normalize.split(".")[0]


async def _fetch_events(code: str, session: AsyncSession, fetch_content: bool = False) -> list[dict]:
    """抓取近期重要事件（来自 basic.10jqka.com.cn/event.html）

    数据源：http://basic.10jqka.com.cn/{code}/event.html
    解析 #tableToday 和 #tableList 两个表格。

    原文来源（按优先级）：
      1. 公告链接 news.10jqka.com.cn/field/sn/ → 抓取HTML/PDF原文
      2. 展开详情 <div class="check_else"> → 页面内联文本
      3. 摘要文本 <span> → 行内摘要
    """
    url = f"http://basic.10jqka.com.cn/{code}/event.html"
    try:
        resp = await session.get(url, timeout=20)
        resp.raise_for_status()
        html = resp.content.decode("gbk", errors="replace")

        soup = BeautifulSoup(html, "html.parser")
        today_str = datetime.now(_CST).strftime("%Y-%m-%d")
        events = []

        for table_id in ["tableToday", "tableList"]:
            table = soup.find("table", id=table_id)
            if not table:
                continue
            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) < 2:
                    continue

                date_text = tds[0].get_text(strip=True)
                if date_text == "今天":
                    date_text = today_str

                detail_td = tds[1]
                strong = detail_td.find("strong")
                event_type = strong.get_text(strip=True).rstrip("：:") if strong else ""

                # ── 摘要文本 ──
                direct_spans = [sp for sp in detail_td.find_all("span", recursive=False)]
                if direct_spans:
                    summary = " ".join(sp.get_text(strip=True) for sp in direct_spans)
                else:
                    clone = BeautifulSoup(str(detail_td), "html.parser")
                    for a in clone.find_all("a"):
                        a.decompose()
                    s = clone.find("strong")
                    if s:
                        s.decompose()
                    summary = clone.get_text(strip=True)

                summary = re.sub(r"\s+", " ", summary).strip().replace("\xa0", " ")
                summary = re.sub(r"详细内容▼.*?▲", "", summary)
                summary = re.sub(r"收起▲.*", "", summary)
                summary = re.sub(r"其它参控公司▼▲.*", "", summary)
                summary = re.sub(r"变动原因▼▲.*", "", summary)
                summary = re.sub(r"涨停分析\s*▼收起▲.*", "", summary)
                summary = summary.strip()

                if not summary or len(summary) < 4:
                    continue

                # ── 展开详情（check_else div）──
                detail_div = detail_td.find("div", class_="check_else")
                detail_text = ""
                if detail_div:
                    detail_text = detail_div.get_text(strip=True)
                    detail_text = re.sub(r"\s+", " ", detail_text).strip()

                # ── 公告链接 ──
                announce_url = ""
                for a in detail_td.find_all("a", href=True):
                    href = a.get("href", "")
                    if "field/sn" in href and "10jqka.com.cn" in href:
                        announce_url = href
                        break

                # ── 组装 content ──
                content_parts = []
                if event_type:
                    content_parts.append(f"[{event_type}]")
                content_parts.append(summary)
                if detail_text and detail_text != summary:
                    content_parts.append(f"\n{detail_text}")
                content = " ".join(content_parts[:2])
                if len(content_parts) > 2:
                    content += content_parts[2]

                # 清洗
                content = _clean_content(content)

                events.append({
                    "news_type": "event",
                    "title": f"{event_type}：{summary[:80]}" if event_type else summary[:80],
                    "url": announce_url or url,
                    "publish_date": date_text,
                    "publish_time": "",
                    "source": "同花顺",
                    "content": content,
                    "_announce_url": announce_url,  # 内部用，抓取原文时使用
                })

        # ── 抓取公告原文 ──
        if fetch_content:
            for item in events:
                aurl = item.pop("_announce_url", "")
                if not aurl:
                    continue
                try:
                    full_text = await _fetch_article_content(aurl, session)
                    if full_text and len(full_text) > len(item["content"]):
                        item["content"] = full_text
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logger.debug("[事件原文] %s 抓取失败: %s", aurl, e)
        else:
            for item in events:
                item.pop("_announce_url", None)

        logger.debug("[%s] 解析近期重要事件: %d 条", code, len(events))
        return events

    except Exception as e:
        logger.error("[%s] 抓取近期重要事件失败: %s", code, e)
        return []

async def _fetch_performance_forecast(code: str) -> list[dict]:
    """抓取业绩预告数据（来自东方财富 API）

    数据源：https://datacenter-web.eastmoney.com/api/data/v1/get
    reportName: RPT_PUBLIC_OP_NEWPREDICT
    按个股代码查询最近的业绩预告记录。
    """
    from common.http.http_utils import fetch_eastmoney_api, EASTMONEY_API_URL

    try:
        params = {
            "sortColumns": "NOTICE_DATE",
            "sortTypes": "-1",
            "pageSize": "20",
            "pageNumber": "1",
            "reportName": "RPT_PUBLIC_OP_NEWPREDICT",
            "columns": "ALL",
            "filter": f'(SECURITY_CODE="{code}")',
        }
        data = await fetch_eastmoney_api(
            EASTMONEY_API_URL, params,
            referer="https://data.eastmoney.com/bbsj/yjyg.html",
        )

        if not data or not data.get("result"):
            return []

        rows = data["result"].get("data", [])
        if not rows:
            return []

        # 按报告期+指标去重，合并同一公告日的多个指标
        from collections import defaultdict
        grouped = defaultdict(list)
        for row in rows:
            notice_date = (row.get("NOTICE_DATE") or "")[:10]
            report_date = (row.get("REPORT_DATE") or "")[:10]
            key = f"{notice_date}_{report_date}"
            grouped[key].append(row)

        items = []
        for key, group in grouped.items():
            notice_date = (group[0].get("NOTICE_DATE") or "")[:10]
            report_date = (group[0].get("REPORT_DATE") or "")[:10]
            stock_name = group[0].get("SECURITY_NAME_ABBR", "")

            content_lines = [f"业绩预告 — {stock_name}", f"公告日期: {notice_date}", f"报告期: {report_date}", ""]

            title_parts = []
            for row in group:
                indicator = row.get("PREDICT_FINANCE", "")
                lower = row.get("PREDICT_AMT_LOWER")
                upper = row.get("PREDICT_AMT_UPPER")
                amp_lower = row.get("ADD_AMP_LOWER")
                amp_upper = row.get("ADD_AMP_UPPER")
                predict_text = row.get("PREDICT_CONTENT", "")
                reason = row.get("CHANGE_REASON_EXPLAIN", "")

                # 格式化金额
                def fmt_amt(v):
                    if v is None:
                        return "-"
                    v = float(v)
                    if abs(v) >= 1e8:
                        return f"{v / 1e8:.2f}亿"
                    elif abs(v) >= 1e4:
                        return f"{v / 1e4:.2f}万"
                    return f"{v:.2f}"

                amt_range = f"{fmt_amt(lower)} ~ {fmt_amt(upper)}" if lower and upper else "-"
                amp_range = f"{amp_lower:.2f}% ~ {amp_upper:.2f}%" if amp_lower is not None and amp_upper is not None else "-"

                content_lines.append(f"[{indicator}]")
                content_lines.append(f"  预测金额: {amt_range}")
                content_lines.append(f"  同比增幅: {amp_range}")
                if predict_text:
                    content_lines.append(f"  预测内容: {predict_text}")
                if reason:
                    content_lines.append(f"  变动原因: {reason}")
                content_lines.append("")

                if indicator in ("归属于上市公司股东的净利润", "净利润"):
                    title_parts.append(f"净利润{amt_range} 同比{amp_range}")
                elif indicator == "营业收入":
                    title_parts.append(f"营收{amt_range}")

            content = "\n".join(content_lines)
            content = _clean_content(content)

            title = f"业绩预告({report_date}): " + "; ".join(title_parts) if title_parts else f"业绩预告({report_date})"

            items.append({
                "news_type": "forecast",
                "title": title[:200],
                "url": f"https://data.eastmoney.com/bbsj/yjyg.html",
                "publish_date": notice_date,
                "publish_time": "",
                "source": "东方财富",
                "content": content,
            })

        logger.debug("[%s] 解析业绩预告: %d 条", code, len(items))
        return items

    except Exception as e:
        logger.error("[%s] 抓取业绩预告失败: %s", code, e)
        return []





# ── 主入口 ────────────────────────────────────────────────

async def fetch_stock_news(
    stock_code_normalize: str,
    session: AsyncSession = None,
    fetch_content: bool = False,
) -> dict[str, list[dict]]:
    """抓取个股主页的新闻公告数据

    Args:
        stock_code_normalize: 标准化代码如 002371.SZ
        session: 可选的复用 session
        fetch_content: 是否抓取每条新闻的正文内容

    Returns:
        {"news": [...], "notice": [...], "industry": [...], "report": [...], "event": [...]}
    """
    code = _extract_code(stock_code_normalize)
    url = f"https://stockpage.10jqka.com.cn/{code}/"

    async def _do_fetch(s: AsyncSession) -> str:
        resp = await s.get(url, timeout=20)
        resp.raise_for_status()
        return resp.text

    try:
        own_session = session is None
        if own_session:
            session = AsyncSession(impersonate=IMPERSONATE)

        try:
            html = await _do_fetch(session)
            result = _parse_stockpage_html(html, stock_code_normalize)

            # 抓取近期重要事件
            events = await _fetch_events(code, session, fetch_content=fetch_content)
            result["event"] = events

            # 抓取行业排名
            ranking = await _fetch_industry_ranking(code, session)
            result["ranking"] = ranking

            # 抓取业绩预告（东方财富API，不需要session）
            forecast = await _fetch_performance_forecast(code)
            result["forecast"] = forecast

            # 抓取正文内容（event/ranking 类型已自行处理）
            if fetch_content:
                for news_type, items in result.items():
                    if news_type in ("event", "ranking", "forecast"):
                        continue
                    for item in items:
                        item_url = item.get("url", "")
                        if item_url and '10jqka.com.cn' in item_url:
                            content = await _fetch_article_content(item_url, session)
                            item["content"] = content
                            # 避免请求过快
                            if content:
                                await asyncio.sleep(0.5)

            total = sum(len(v) for v in result.values())
            content_count = sum(
                1 for items in result.values()
                for item in items if item.get("content")
            )
            logger.info("[%s] 抓取新闻公告完成，共 %d 条，正文 %d 条",
                        stock_code_normalize, total, content_count)
            return result
        finally:
            if own_session:
                await session.close()

    except Exception as e:
        logger.error("[%s] 抓取新闻公告失败: %s", stock_code_normalize, e)
        return {"news": [], "notice": [], "industry": [], "report": [], "event": [], "ranking": [], "forecast": []}


async def fetch_stock_news_all_types(stock_code_normalize: str) -> list[dict]:
    """抓取个股所有类型新闻，返回扁平列表"""
    result = await fetch_stock_news(stock_code_normalize)
    all_items = []
    for items in result.values():
        all_items.extend(items)
    return all_items


# ── 测试入口 ──────────────────────────────────────────────

if __name__ == "__main__":
    import json

    async def main():
        code = "002371.SZ"
        result = await fetch_stock_news(code, fetch_content=True)
        for news_type, items in result.items():
            print(f"\n{'=' * 60}")
            print(f"  {TYPE_LABEL.get(news_type, news_type)} ({len(items)} 条)")
            print(f"{'=' * 60}")
            for item in items[:3]:
                print(f"  标题: {item['title']}")
                print(f"  日期: {item['publish_date']} {item['publish_time']}")
                print(f"  正文: {(item.get('content') or '(无)')[:100]}...")
                print()

    asyncio.run(main())
