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
from common.utils.stock_info_utils import StockInfo
from service.llm.deepseek_client import DeepSeekClient
from service.web_search.baidu_search import baidu_search
from service.web_search.web_scraper import extract_main_content, BUSINESS_TIMEOUT

logger = logging.getLogger(__name__)

# ── 新闻搜索缓存（2小时TTL） ──
_news_cache: dict[str, dict] = {}  # key -> {'data': [...], 'ts': timestamp}
_NEWS_CACHE_TTL = 2 * 60 * 60  # 2小时（秒）

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


def _dedup_by_title(items: list[dict]) -> list[dict]:
    """按标题去重，相同/高度相似的标题只保留内容最长的一条。

    判定规则：将标题归一化（只保留中文+字母+数字）后，
    若两条标题归一化结果相同，或其中一个是另一个的子串，视为重复。
    """
    seen: dict[str, dict] = {}  # norm_title -> best item
    for item in items:
        norm = _TITLE_NORM_RE.sub('', item['title'])
        if not norm:
            continue
        # 检查是否与已有标题重复（完全相同或互为子串）
        matched_key = None
        for existing_key in seen:
            if norm == existing_key or norm in existing_key or existing_key in norm:
                matched_key = existing_key
                break
        if matched_key is not None:
            # 保留内容更长的那条
            if len(item.get('content', '')) > len(seen[matched_key].get('content', '')):
                seen[matched_key] = item
        else:
            seen[norm] = item
    return list(seen.values())


async def _fetch_and_replace_content(item: dict) -> dict:
    """爬取网页全文，成功且内容更丰富则替换原有 content，失败则保留原内容。"""
    try:
        text = await extract_main_content(item['url'], timeout=BUSINESS_TIMEOUT)
        if text and len(text[:800]) > len(item.get('content') or ''):
            item['content'] = text[:800]
    except Exception as e:
        logger.warning(f"爬取全文失败 [{item.get('url')}]: {e}")
    return item


def _build_filter_prompt(stock_info: StockInfo, search_results: list[dict]) -> str:
    """构建大模型过滤提示词"""
    now_str = datetime.now().strftime('%Y-%m-%d')
    cutoff_str = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    results_json = json.dumps(search_results, ensure_ascii=False)
    return (
        "# Role\n"
        "你是一位资深证券分析师，擅长从海量信息中快速识别对股价有实质驱动力的核心事件。\n\n"
        f"当前日期：{now_str}\n"
        f"目标公司：{stock_info.stock_name}（{stock_info.stock_code_normalize}）\n\n"
        "# Task\n"
        "对以下网络搜索结果进行筛选，只保留与目标公司**直接相关**且对股价有**实质影响**的信息。\n\n"
        "# 时效性硬约束（最高优先级）\n"
        f"只允许保留发布日期在 {cutoff_str} 至 {now_str} 之间（近7天）的消息。\n"
        "超出该时间范围的条目无论内容多重要，一律剔除。\n"
        "若条目缺少日期字段，视为不符合时效要求，同样剔除。\n\n"
        "# 筛选准则（在满足时效性前提下，符合任一即可保留）\n"
        "1. 资本运作：并购重组、定增融资、股权激励、大股东增减持\n"
        "2. 业绩变动：财报超预期/不及预期、业绩预告、重大资产减值\n"
        "3. 经营动态：重大合同、核心高管变动、技术突破、产能扩张\n"
        "4. 外部环境：行业重磅政策、监管处罚、重大诉讼\n"
        "5. 市场关注：机构评级调整、热点题材催化、行业景气度变化\n\n"
        "# 严格禁选\n"
        "- 实时股价波动、K线走势、技术指标分析\n"
        "- 常规产品介绍、营销软文、用户评价\n"
        "- 与目标公司无直接关联的泛行业讨论\n"
        "- 搜索内容相近的条目只保留最详细的一条\n\n"
        "# 排序要求\n"
        "返回的 id 必须按对股价影响程度从高到低排序（影响最大的排最前面）。\n\n"
        "# 搜索结果\n"
        f"{results_json}\n\n"
        "# Output\n"
        "只返回符合要求的 id 列表，JSON 数组格式，按影响面从高到低排序，最多5条最少0条。禁止输出任何解释。\n"
        "示例：[3, 1, 5]\n"
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
    try:
        results = await baidu_search(query=query, days=days, top_k=20)
        if not results:
            _news_cache[cache_key] = {'data': [], 'ts': now}
            return []

        # 并发爬取全文，成功则替换原有摘要内容
        results = list(await asyncio.gather(
            *[_fetch_and_replace_content(item) for item in results]
        ))

        # 清洗 + 构建带 id 的搜索结果供大模型筛选
        search_items = []
        for i, item in enumerate(results, 1):
            title = _clean_text(item.get('title') or '')
            content = _clean_text(item.get('content') or '')
            if not title:
                continue
            search_items.append({
                'id': i,
                'title': title,
                'date': item.get('date', ''),
                'content': content[:500],
                'url': item.get('url', ''),
            })

        if not search_items:
            _news_cache[cache_key] = {'data': [], 'ts': now}
            return []

        # 标题去重：相似标题只保留内容最长的一条
        search_items = _dedup_by_title(search_items)

        # 大模型过滤
        client = DeepSeekClient()
        prompt = _build_filter_prompt(stock_info, search_items)
        response = await client.chat(
            messages=[{"role": "user", "content": prompt}],
            model="deepseek-chat",
            temperature=0.1
        )
        resp_content = response['choices'][0]['message']['content']
        filtered_ids = parse_llm_json(resp_content)

        # 按大模型返回的顺序（影响面从高到低）提取结果
        id_to_item = {item['id']: item for item in search_items}
        filtered = [id_to_item[fid] for fid in filtered_ids if fid in id_to_item]

        filtered_result = [
            {
                '标题': item['title'],
                '日期': item.get('date', ''),
                '摘要': item['content'][:300],
                '来源': item.get('url', ''),
            }
            for item in filtered
        ][:5]

        _news_cache[cache_key] = {'data': filtered_result, 'ts': now}
        return filtered_result

    except Exception as e:
        logger.warning(f"搜索股票新闻失败 [{stock_info.stock_name}]: {e}")
        # 搜索失败时，如果有过期缓存也返回（降级策略）
        if cached:
            logger.info(f"搜索失败，降级使用过期缓存 [{stock_info.stock_name}]")
            return cached['data']
        return []


def format_news_for_prompt(news_list: list[dict]) -> str:
    """将新闻列表格式化为提示词中的文本块"""
    if not news_list:
        return "未获取到近期相关新闻/公告信息。"

    lines = []
    for i, news in enumerate(news_list, 1):
        date_str = ''
        if news.get('日期'):
            date_str = '（' + news['日期'] + '）'
        lines.append(str(i) + '. ' + news['标题'] + date_str)
        if news.get('摘要'):
            lines.append('   摘要：' + news['摘要'])
    return '\n'.join(lines)


if __name__ == "__main__":
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name('生益科技')
        news = await search_stock_news(stock_info)
        print(format_news_for_prompt(news))

    asyncio.run(main())
