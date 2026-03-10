"""从 stock_industry_list.md 解析股票→板块映射，避免调用外部API。

解析 data_results/industry_analysis/stock_industry_list.md 文件，
将每只股票映射到 _SECTOR_WEIGHTS 中定义的7个板块分组之一。
"""
import os
import re

# stock_industry_list.md 中的大类标题 → _SECTOR_WEIGHTS 板块名称映射
_SECTION_TO_SECTOR = {
    '半导体与集成电路': '科技',
    '电子元器件': '科技',
    '软件与信息技术': '科技',
    '通信': '科技',
    '传媒': '科技',
    '新能源与电力设备': '新能源',
    '汽车与零部件': '汽车',
    '机械装备': '制造',
    '医药与生物科技': '医药',
    '化工': '化工',
    '有色金属与矿业': '有色金属',
    '钢铁': '制造',
    '国防军工': '制造',
    '建筑与工程': '制造',
}

# 股票代码正则
_STOCK_RE = re.compile(r'[（(](\d{6}\.[A-Z]{2})[）)]')
# 大类标题正则：## 一、xxx  或  ## 二、xxx
_SECTION_RE = re.compile(r'^##\s+[一二三四五六七八九十百千]+[、．.]\s*(.+)')


def parse_industry_list_md(md_path: str = None) -> dict[str, str]:
    """解析 stock_industry_list.md，返回 {stock_code: sector_name} 映射。

    sector_name 为 _SECTOR_WEIGHTS 中的键：科技/有色金属/汽车/新能源/医药/化工/制造
    未匹配到板块的股票不会出现在返回结果中。
    """
    if md_path is None:
        md_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'data_results', 'industry_analysis', 'stock_industry_list.md'
        )

    if not os.path.exists(md_path):
        return {}

    mapping = {}
    current_sector = None

    with open(md_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            # 检测大类标题
            m = _SECTION_RE.match(line)
            if m:
                section_name = m.group(1).strip()
                current_sector = _SECTION_TO_SECTOR.get(section_name)
                continue

            # 在当前板块下提取股票代码
            if current_sector:
                for code_match in _STOCK_RE.finditer(line):
                    code = code_match.group(1)
                    mapping[code] = current_sector

    return mapping


def get_sector_peers(mapping: dict[str, str], stock_code: str, max_peers: int = 10) -> list[str]:
    """获取同板块的其他股票代码列表（不含自身）。"""
    sector = mapping.get(stock_code)
    if not sector:
        return []
    peers = [c for c, s in mapping.items() if s == sector and c != stock_code]
    # 返回前 max_peers 个
    return peers[:max_peers]
