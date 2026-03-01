"""股票近期新闻/公告/事件搜索模块。

基于百度搜索获取个股近期重要资讯，爬取全文后清洗无效字符，
再通过大模型过滤无关信息，供技术面分析时补充消息面参考。
"""

import asyncio
import html
import json
import logging
import re
import time
from datetime import datetime, timedelta

from common.utils.llm_utils import parse_llm_json
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.llm.volcengine_client import VolcengineClient
from service.web_search.baidu_search import baidu_search
from service.web_search.web_scraper import extract_main_content, extract_content_with_datetime, BUSINESS_TIMEOUT

logger = logging.getLogger(__name__)

# ── 新闻搜索缓存（2小时TTL） ──
_news_cache: dict[str, dict] = {}  # key -> {'data': [...], 'ts': timestamp}
_NEWS_CACHE_TTL = 2 * 60 * 60  # 2小时（秒）

# ── 优先财经网站域名（按权威性排序） ──
_FINANCE_DOMAINS = [
    'eastmoney.com',      # 东方财富
    'cninfo.com.cn',      # 巨潮资讯（官方公告披露）
    'sse.com.cn',         # 上交所
    'szse.cn',            # 深交所
    '10jqka.com.cn',      # 同花顺
    'cls.cn',             # 财联社
    'stcn.com',           # 证券时报
    'cs.com.cn',          # 中证网
    'sina.com.cn',        # 新浪财经
    'hexun.com',          # 和讯网
    'caixin.com',         # 财新网
    'yicai.com',          # 第一财经
    'nbd.com.cn',         # 每日经济新闻
    'securities.com',     # 证券之星
]

# ── 百度搜索私有标记字符（高频出现的 PUA 区段） ──
_BAIDU_MARKER_RE = re.compile(r'[\ue000-\ue099]')

# ── 零宽/不可见 Unicode 字符 ──
_INVISIBLE_RE = re.compile(
    r'[\u200b\u200c\u200d\u200e\u200f'
    r'\u202a-\u202e'
    r'\u2060\u2061\u2062\u2063'
    r'\ufeff\ufffe'
    r'\xad'
    r'\x00-\x08\x0b\x0c\x0e-\x1f]'
)

# ── 残留 HTML 标签 ──
_HTML_TAG_RE = re.compile(r'<[^>]+>')

# ── 版权/备案号噪声 ──
_COPYRIGHT_RE = re.compile(
    r'©.*?(?:Baidu|百度)|'
    r'京ICP[证备]\d+号[—\-]?\d*|'
    r'京公网安备\d+号|'
    r'All Rights Reserved|'
    r'Copyright\s*©?.*?\d{4}|'
    r'版权所有',
    re.IGNORECASE
)

# ── PDF公告模板噪声 ──
_ANNOUNCEMENT_NOISE_RE = re.compile(
    r'本公司董事会及全体董事保证.*?(?:法律责任|完整性)|'
    r'提请投资者注意投资风险|'
    r'单位[:：]\s*人民币[万亿]?元|'
    r'公告编号[:：]?\s*\d{4}[—\-]\d+|'
    r'股票简称[:：]|'
    r'股票代码[:：]|'
    r'增减变动幅'
)

# ── 网页 UI 噪声短语 ──
_UI_NOISE = {
    '首页', '下载APP', '下载app', '登录', '注册', '登录注册',
    '关注', '分享', '点赞', '评论', '转发', '收藏', '举报',
    '展开全文', '查看更多', '阅读全文', '打开APP', '打开app',
    '来源：', '责任编辑', '免责声明', '版权声明', '特别声明',
    '返回搜狐', '进入搜狐首页', '声明：该文观点仅代表作者本人',
}

# ── 连续空白压缩（含全角空格） ──
_MULTI_SPACE_RE = re.compile(r'[\s\u3000]+')
_MULTI_NEWLINE_RE = re.compile(r'\n{3,}')

# ── 中文字符检测 ──
_HAS_CHINESE_RE = re.compile(r'[\u4e00-\u9fff]')

# ── 标题去重：仅保留中文/字母/数字用于相似度比较 ──
_TITLE_NORM_RE = re.compile(r'[^\u4e00-\u9fffa-zA-Z0-9]')


def _clean_text(text: str) -> str:
    """清洗搜索结果中的无效字符和噪声内容。"""
    if not text:
        return ''

    text = html.unescape(text)
    text = _HTML_TAG_RE.sub('', text)
    text = _BAIDU_MARKER_RE.sub('', text)
    text = _INVISIBLE_RE.sub('', text)
    text = text.replace('\u3000', ' ')
    text = _COPYRIGHT_RE.sub('', text)
    text = _ANNOUNCEMENT_NOISE_RE.sub('', text)

    lines = text.split('\n')
    cleaned_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped in _UI_NOISE:
            continue
        if len(stripped) < 20 and any(stripped.startswith(n) for n in _UI_NOISE):
            continue
        if len(stripped) < 6 and not _HAS_CHINESE_RE.search(stripped):
            continue
        cleaned_lines.append(stripped)

    text = '\n'.join(cleaned_lines)
    text = _MULTI_SPACE_RE.sub(' ', text)
    text = _MULTI_NEWLINE_RE.sub('\n\n', text)

    return text.strip()


def _time_precision(publish_time: str) -> int:
    """返回时间精度分：含有效时分秒=2，仅含时分=1，仅日期或空=0。
    注意：00:00:00 视为无精确时间，降级为0。
    """
    if not publish_time:
        return 0
    t = publish_time.strip()
    if len(t) >= 19 and t[16] == ':' and not t[11:].startswith('00:00:00'):
        return 2
    if len(t) >= 16 and t[13] == ':' and not t[11:].startswith('00:00'):
        return 1
    return 0


def _dedup_by_title(items: list[dict]) -> list[dict]:
    """按标题去重，相同/高度相似的标题只保留最优的一条。

    优先级：时间精度高 > 内容更长。
    """
    seen: dict[str, dict] = {}  # norm_title -> best item
    for item in items:
        norm = _TITLE_NORM_RE.sub('', item['title'])
        if not norm:
            continue
        matched_key = None
        for existing_key in seen:
            if norm == existing_key or norm in existing_key or existing_key in norm:
                matched_key = existing_key
                break
        if matched_key is not None:
            existing = seen[matched_key]
            item_prec = _time_precision(item.get('publish_time', ''))
            exist_prec = _time_precision(existing.get('publish_time', ''))
            if item_prec > exist_prec or (
                item_prec == exist_prec and len(item.get('content', '')) > len(existing.get('content', ''))
            ):
                seen[matched_key] = item
        else:
            seen[norm] = item
    return list(seen.values())

def _classify_market_session(publish_time: str) -> str:
    """根据发布时间判定消息属于盘前/盘中/盘后。

    A股交易时段：09:30-15:00
    - 盘中（09:30-15:00）：发布时间在交易时段内
    - 盘后（15:00后）或盘前（09:30前）：看发布时间之后是否有新的交易日
      - 有新交易日 → 盘前（影响下一个交易日开盘）
      - 无新交易日 → 盘后（最新交易日收盘后）
    - 无法解析时间 → 未知
    """
    if not publish_time or len(publish_time) < 11:
        return '未知'
    try:
        import chinese_calendar
        dt = datetime.strptime(publish_time.strip()[:16], '%Y-%m-%d %H:%M')
        hour, minute = dt.hour, dt.minute
        total_minutes = hour * 60 + minute
        is_trading_day = dt.date().weekday() < 5 and not chinese_calendar.is_holiday(dt.date())
        if is_trading_day and (570 <= total_minutes <= 690 or 780 <= total_minutes <= 900):  # 09:30-11:30 or 13:00-15:00
            return '盘中'
        if total_minutes < 570:  # 09:30 之前
            return '盘前'
        return '盘后'
    except (ValueError, IndexError) as e:
        logger.debug("_classify_market_session 解析失败: publish_time=%s, %s", publish_time, e)
        return '未知'

# ── 股票交易数据过滤（标题命中则直接剔除） ──
_TRADING_DATA_RE = re.compile(
    r'(今日|昨日|本周|本月)?'
    r'(股价|收盘|开盘|涨停|跌停|涨幅|跌幅|涨跌|成交量|成交额|换手率|振幅|量比|委比|'
    r'市盈率|市净率|总市值|流通市值|每股收益|每股净资产|'
    r'分时|K线|均线|MACD|KDJ|RSI|布林|技术指标|'
    r'行情|报价|实时|最新价|最高价|最低价)'
)

# ── 融资融券关键词 ──
_MARGIN_KEYWORDS = [
    '融资融券', '融资余额', '融券余额', '融资买入', '融券卖出',
    '融资净买入', '融券净卖出', '融资偿还', '融券偿还',
    '两融', '融资净偿还', '融券净偿还', '融资融券余额',
    '融资融券标的', '融资融券数据', '融资融券变动',
]

def _is_margin_trading_news(title: str, content: str) -> bool:
    """判断新闻是否属于融资融券类别。"""
    text = (title + content).lower()
    return any(kw in text for kw in _MARGIN_KEYWORDS)





async def _fetch_and_replace_content(item: dict) -> dict:
    """爬取网页全文及发布时间，成功且内容更丰富则替换原有 content，同时提取精确发布时间。"""
    try:
        result = await extract_content_with_datetime(item['url'], timeout=BUSINESS_TIMEOUT)
        text = result.get('content', '')
        publish_time = result.get('publish_time')
        if text and len(text[:800]) > len(item.get('content') or ''):
            item['content'] = text[:800]
        # 如果成功提取到精确时间（含时分），优先使用
        if publish_time:
            item['publish_time'] = publish_time
    except Exception as e:
        logger.warning(f"爬取全文失败 [{item.get('url')}]: {e}")
    return item



def _common_prompt_header(stock_info: StockInfo) -> str:
    now_str = datetime.now().strftime('%Y-%m-%d')
    cutoff_str = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    return (
        "# Role\n"
        "你是一位资深证券分析师，擅长从海量信息中快速识别对股价有实质驱动力的核心事件。\n\n"
        f"当前日期：{now_str}\n"
        f"目标公司：{stock_info.stock_name}（{stock_info.stock_code_normalize}）\n\n"
        "# 盘前盘后判定说明\n"
        "A股交易时段为 09:30-15:00，publish_time 格式为 YYYY-MM-DD HH:MM。\n"
        "- 15:00 后发布 → 盘后消息，影响次日开盘\n"
        "- 09:30 前发布 → 盘前消息，影响当日开盘\n"
        "- 09:30-15:00 发布 → 盘中消息，可能已被市场消化\n"
        "优先关注盘后和盘前消息。\n\n"
        "# 时效性硬约束（最高优先级）\n"
        f"只保留发布日期在 {cutoff_str} 至 {now_str} 之间（近7天）的条目，超出或缺少日期一律剔除。\n\n"
        "# 强制去重（必须严格执行）\n"
        "同一事件以不同标题出现多次时，只保留信息最完整的一条；完整度相近时优先保留官方来源（cninfo、sse、szse）。\n\n"
    ), now_str


def _build_ann_filter_prompt(stock_info: StockInfo, ann_results: list[dict]) -> str:
    """构建公告/新闻过滤提示词"""
    header, _ = _common_prompt_header(stock_info)
    results_json = json.dumps(ann_results, ensure_ascii=False)
    return (
        header +
        "# Task\n"
        "从以下公告/新闻搜索结果中，筛选与目标公司**直接相关**且对股价有**实质影响**的条目。\n\n"
        "# 保留准则（符合任一即可）\n"
        "1. 资本运作：并购重组、定增融资、股权激励、大股东增减持\n"
        "2. 业绩变动：财报、业绩预告、重大资产减值\n"
        "3. 经营动态：重大合同、核心高管变动、技术突破、产能扩张\n"
        "4. 外部环境：行业重磅政策、监管处罚、重大诉讼\n"
        "5. 市场关注：机构评级调整、热点题材催化\n\n"
        "# 严格禁选\n"
        "- 股价行情、K线、技术指标、成交量、换手率、涨跌幅等交易数据\n"
        "- 融资融券相关内容（由另一组数据单独处理）\n"
        "- 营销软文、产品介绍、泛行业讨论\n\n"
        "# 搜索结果\n"
        f"{results_json}\n\n"
        "# Output\n"
        "只返回符合要求的 id 列表，JSON 数组格式，按影响面从高到低排序，最多4条最少0条。禁止输出任何解释。\n"
        "示例：[3, 1, 5]\n"
    )


def _build_margin_filter_prompt(stock_info: StockInfo, margin_results: list[dict]) -> str:
    """构建融资融券过滤提示词"""
    header, _ = _common_prompt_header(stock_info)
    results_json = json.dumps(margin_results, ensure_ascii=False)
    return (
        header +
        "# Task\n"
        "从以下融资融券搜索结果中，筛选与目标公司**直接相关**且反映两融资金动向的条目。\n\n"
        "# 保留准则（符合任一即可）\n"
        "1. 融资余额变化：融资余额大幅增加或减少\n"
        "2. 融券余额变化：融券余额异动\n"
        "3. 两融数据异动：融资净买入/净偿还、融券净卖出/净偿还\n"
        "4. 融资融券标的调整：新增或移除标的\n\n"
        "# 严格禁选\n"
        "- 股价行情、K线、技术指标等交易数据\n"
        "- 与融资融券无关的公告、新闻\n"
        "- 泛行业两融数据（非目标公司个股数据）\n\n"
        "# 搜索结果\n"
        f"{results_json}\n\n"
        "# Output\n"
        "只返回符合要求的 id 列表，JSON 数组格式，按影响面从高到低排序，最多2条最少0条。禁止输出任何解释。\n"
        "示例：[2, 1]\n"
    )


async def search_stock_news(stock_info: StockInfo, days: int = 7) -> list[dict]:
    """搜索并过滤个股近期新闻/公告/事件。

    流程：缓存检查 -> 百度搜索 -> 爬取全文 -> 文本清洗 -> 标题去重 -> 大模型过滤 -> 按影响面排序返回
    缓存策略：同一股票2小时内不重复搜索，直接返回缓存结果。
    """
    cache_key = f"{stock_info.stock_code_normalize}_{days}"
    now = time.time()

    # 检查缓存
    cached = _news_cache.get(cache_key)
    if cached and (now - cached['ts']) < _NEWS_CACHE_TTL:
        logger.info(f"命中新闻缓存 [{stock_info.stock_name}]，缓存时间 {datetime.fromtimestamp(cached['ts']).strftime('%H:%M:%S')}")
        return cached['data']

    query = f"{stock_info.stock_name} 公告"
    query_margin = f"{stock_info.stock_name} 融资融券"
    try:
        results_ann, results_margin = await asyncio.gather(
            baidu_search(query=query, days=days, top_k=15, preferred_domains=_FINANCE_DOMAINS),
            baidu_search(query=query_margin, days=days, top_k=10, preferred_domains=_FINANCE_DOMAINS),
        )
        results_ann = results_ann or []
        results_margin = results_margin or []
        if not results_ann and not results_margin:
            _news_cache[cache_key] = {'data': [], 'ts': now}
            return []

        # 并发爬取全文
        results_ann, results_margin = await asyncio.gather(
            asyncio.gather(*[_fetch_and_replace_content(item) for item in results_ann]),
            asyncio.gather(*[_fetch_and_replace_content(item) for item in results_margin]),
        )
        results_ann = list(results_ann)
        results_margin = list(results_margin)

        def _to_search_items(results: list, id_start: int) -> list[dict]:
            items = []
            for i, item in enumerate(results, id_start):
                title = _clean_text(item.get('title') or '')
                content = _clean_text(item.get('content') or '')
                if not title or _TRADING_DATA_RE.search(title):
                    continue
                publish_time = item.get('publish_time') or item.get('date', '')
                items.append({
                    'id': i,
                    'title': title,
                    'publish_time': publish_time,
                    'content': content[:500],
                    'url': item.get('url', ''),
                })
            return _dedup_by_title(items)

        ann_items = _to_search_items(results_ann, 1)
        margin_items = _to_search_items(results_margin, 1000)  # id 区间隔开避免冲突

        if not ann_items and not margin_items:
            _news_cache[cache_key] = {'data': [], 'ts': now}
            return []

        # 并发调用大模型过滤
        client = VolcengineClient()

        async def _filter(items: list[dict], prompt_fn) -> list[dict]:
            if not items:
                return []
            prompt = prompt_fn(stock_info, items)
            resp = await client.chat(
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                thinking=True
            )
            msg = resp['choices'][0]['message']
            resp_content = msg.get('content') or msg.get('reasoning_content') or ''
            ids = parse_llm_json(resp_content) if resp_content else []
            id_map = {item['id']: item for item in items}
            return [id_map[fid] for fid in ids if fid in id_map]

        filtered_ann, filtered_margin = await asyncio.gather(
            _filter(ann_items, _build_ann_filter_prompt),
            _filter(margin_items, _build_margin_filter_prompt),
        )

        def _to_result(item: dict, category: str) -> dict:
            return {
                '标题': item['title'],
                '发布时间': item.get('publish_time', ''),
                '时段': _classify_market_session(item.get('publish_time', '')),
                '摘要': item['content'][:300],
                '来源': item.get('url', ''),
                '类别': category,
            }

        filtered_result = (
            [_to_result(item, '公告') for item in filtered_ann] +
            [_to_result(item, '融资融券') for item in filtered_margin]
        )

        _news_cache[cache_key] = {'data': filtered_result, 'ts': now}
        return filtered_result

    except Exception as e:
        logger.warning(f"搜索股票新闻失败 [{stock_info.stock_name}]: {e}")
        # 搜索失败时，如果有过期缓存也返回（降级策略）
        if cached:
            logger.info(f"搜索失败，降级使用过期缓存 [{stock_info.stock_name}]")
            return cached['data']
        return []



def _assess_news_next_day_impact(publish_time: str, session: str, next_trading_day: str) -> str:
    """根据新闻发布时间和下一个交易日，判断该消息对下一个交易日的影响。

    A股交易时段：09:30-11:30（上午）、13:00-15:00（下午），北京时间。

    判定逻辑：
    - 盘后消息（15:00后）：如果发布日 == 下一个交易日的前一个交易日 → 尚未被市场消化，直接影响次日开盘
    - 盘前消息（09:30前）：如果发布日 == 下一个交易日 → 直接影响当日开盘
    - 盘中消息（09:30-15:00）：已在盘中被部分消化，对次日影响减弱
    - 更早的消息：影响已逐步衰减
    """
    if not publish_time or not next_trading_day or len(publish_time) < 10:
        return '时间不明，无法判断'

    try:
        from datetime import datetime, timedelta
        import chinese_calendar

        next_td = datetime.strptime(next_trading_day, '%Y-%m-%d').date()
        # 找到上一个交易日
        prev_td = next_td - timedelta(days=1)
        while prev_td.weekday() >= 5 or chinese_calendar.is_holiday(prev_td):
            prev_td -= timedelta(days=1)

        pub_date_str = publish_time.strip()[:10]
        pub_date = datetime.strptime(pub_date_str, '%Y-%m-%d').date()

        if session == '盘后' and pub_date == prev_td:
            return f"★ 上一个交易日盘后发布，市场尚未消化，直接影响{next_trading_day}开盘"
        elif session == '盘前' and pub_date == next_td:
            return f"★ 下一个交易日盘前发布，直接影响{next_trading_day}开盘"
        elif session == '盘中' and pub_date == prev_td:
            return f"上一个交易日盘中发布，已被部分消化，对{next_trading_day}影响减弱"
        elif pub_date >= prev_td:
            return f"近期消息，对{next_trading_day}仍有一定参考价值"
        else:
            days_gap = (next_td - pub_date).days
            return f"发布于{days_gap}天前，影响已逐步衰减"
    except (ValueError, ImportError) as e:
        logger.debug("_assess_news_next_day_impact 解析失败: %s", e)
        return '时间不明，无法判断'


def format_news_for_prompt(news_list: list[dict], next_trading_day: str = '') -> str:
    """将新闻列表格式化为提示词中的文本块，按融资融券和其他消息分类展示。

    Args:
        news_list: 新闻列表（每条含 '类别' 字段：'融资融券' 或 '其他'）
        next_trading_day: 下一个交易日日期（YYYY-MM-DD），用于判断消息对次日的影响
    """
    if not news_list:
        return "未获取到近期相关新闻/公告信息。"

    margin_news = [n for n in news_list if n.get('类别') == '融资融券']
    other_news = [n for n in news_list if n.get('类别') != '融资融券']

    def _format_section(items: list[dict]) -> str:
        lines = []
        for i, news in enumerate(items, 1):
            time_label = ''
            publish_time = news.get('发布时间', '')
            session = news.get('时段', '未知')
            if publish_time:
                time_label = f'（{publish_time} [{session}]）'

            impact_label = ''
            if next_trading_day:
                impact = _assess_news_next_day_impact(publish_time, session, next_trading_day)
                impact_label = f'\n   → 对次日影响：{impact}'

            lines.append(f'{i}. {news["标题"]}{time_label}{impact_label}')
            if news.get('摘要'):
                lines.append(f'   摘要：{news["摘要"]}')
        return '\n'.join(lines)

    sections = []

    if margin_news:
        sections.append(f'【融资融券动态】\n{_format_section(margin_news)}')
    else:
        sections.append('【融资融券动态】\n无近期融资融券相关消息。')

    if other_news:
        sections.append(f'【重要公告消息】\n{_format_section(other_news)}')
    else:
        sections.append('【重要公告消息】\n无近期其他重要消息。')

    return '\n\n'.join(sections)



if __name__ == "__main__":
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name('中国卫通')
        news = await search_stock_news(stock_info)
        print(format_news_for_prompt(news))

    asyncio.run(main())
