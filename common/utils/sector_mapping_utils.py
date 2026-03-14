"""从 stock_industry_list.md 解析股票→板块映射，避免调用外部API。

解析 data_results/industry_analysis/stock_industry_list.md 文件，
将每只股票映射到 _SECTOR_WEIGHTS 中定义的7个板块分组之一。
"""
import os
import re

# stock_industry_list.md 中的大类标题 → _SECTOR_WEIGHTS 板块名称映射
_SECTION_TO_SECTOR = {
    # 科技
    '半导体与集成电路': '科技',
    '电子': '科技',
    '电子元器件': '科技',  # 兼容旧格式
    '计算机': '科技',
    '软件与信息技术': '科技',  # 兼容旧格式
    '软件与信息技术（计算机）': '科技',  # 兼容旧格式
    '通信': '科技',
    '传媒': '科技',
    # 新能源
    '电力设备': '新能源',
    '新能源与电力设备': '新能源',  # 兼容旧格式
    # 汽车
    '汽车': '汽车',
    '汽车与零部件': '汽车',  # 兼容旧格式
    # 制造
    '机械设备': '制造',
    '机械装备': '制造',  # 兼容旧格式
    '钢铁': '制造',
    '国防军工': '制造',
    '建筑装饰': '制造',
    '建筑材料': '制造',
    '建筑与工程': '制造',  # 兼容旧格式
    # 医药
    '医药生物': '医药',
    '医药与生物科技': '医药',  # 兼容旧格式
    # 化工
    '基础化工': '化工',
    '化工': '化工',
    # 有色金属
    '有色金属': '有色金属',
    '有色金属与矿业': '有色金属',  # 兼容旧格式
}

# 回测用硬编码板块映射（覆盖MD文件中分类不准确的股票）
_HARDCODED_SECTOR = {
    # 科技
    '002371.SZ': '科技',  # 北方华创 - 半导体设备
    '300308.SZ': '科技',  # 中际旭创 - 光通信
    '002916.SZ': '科技',  # 深南电路 - PCB
    '603986.SH': '科技',  # 兆易创新 - 芯片设计
    '688981.SH': '科技',  # 中芯国际 - 晶圆代工
    '002475.SZ': '科技',  # 立讯精密 - 消费电子
    '300502.SZ': '科技',  # 新易盛 - 光通信
    '002049.SZ': '科技',  # 紫光国微 - 芯片设计
    # 汽车
    '002594.SZ': '汽车',  # 比亚迪
    '601689.SH': '汽车',  # 拓普集团 - 汽车零部件
    '002920.SZ': '汽车',  # 德赛西威 - 汽车电子
    '002050.SZ': '汽车',  # 三花智控 - 热管理
    '603596.SH': '汽车',  # 伯特利 - 制动系统
    '601127.SH': '汽车',  # 赛力斯 - 新能源汽车
    # 新能源
    '300750.SZ': '新能源',  # 宁德时代 - 锂电池
    '300763.SZ': '新能源',  # 锦浪科技 - 光伏逆变器
    '002709.SZ': '新能源',  # 天赐材料 - 锂电材料
    '002074.SZ': '新能源',  # 国轩高科 - 锂电池
    '300073.SZ': '新能源',  # 当升科技 - 正极材料
    '600406.SH': '新能源',  # 国电南瑞 - 电网设备
    '002202.SZ': '新能源',  # 金风科技 - 风电
    '300450.SZ': '新能源',  # 先导智能 - 锂电设备
    # 医药
    '600276.SH': '医药',  # 恒瑞医药 - 创新药
    '600436.SH': '医药',  # 片仔癀 - 中药
    '603259.SH': '医药',  # 药明康德 - CXO
    '000963.SZ': '医药',  # 华东医药 - 化学制药
    '688271.SH': '医药',  # 联影医疗 - 医疗器械
    '300759.SZ': '医药',  # 康龙化成 - CXO
    '000538.SZ': '医药',  # 云南白药 - 中药
    # 化工
    '002440.SZ': '化工',  # 闰土股份 - 精细化工
    '002497.SZ': '化工',  # 雅化集团 - 农药化肥
    '600426.SH': '化工',  # 华鲁恒升 - 基础化工
    '002648.SZ': '化工',  # 卫星化学 - 基础化工
    '600989.SH': '化工',  # 宝丰能源 - 基础化工
    '002250.SZ': '化工',  # 联化科技 - 精细化工
    # 制造
    '300124.SZ': '制造',  # 汇川技术 - 工业自动化
    '000157.SZ': '制造',  # 中联重科 - 工程机械
    '000425.SZ': '制造',  # 徐工机械 - 工程机械
    '600031.SH': '制造',  # 三一重工 - 工程机械
    '601100.SH': '制造',  # 恒立液压 - 液压设备
    '600150.SH': '制造',  # 中国船舶 - 重型装备
    # 有色金属
    '002155.SZ': '有色金属',  # 湖南黄金
    '601899.SH': '有色金属',  # 紫金矿业
    '600549.SH': '有色金属',  # 厦门钨业
    '600547.SH': '有色金属',  # 山东黄金
    '600489.SH': '有色金属',  # 中金黄金
    '600988.SH': '有色金属',  # 赤峰黄金
    '300748.SZ': '有色金属',  # 金力永磁 - 磁性材料
    # 汽车（补充）
    '600066.SH': '汽车',  # 宇通客车
    '002920.SZ': '汽车',  # 德赛西威 - 汽车电子
    # 化工（补充）
    '600309.SH': '化工',  # 万华化学 - 基础化工
    # 医药（补充）
    '600196.SH': '医药',  # 复星医药 - 综合医药
}

# 股票代码正则
_STOCK_RE = re.compile(r'[（(](\d{6}\.[A-Z]{2})[）)]')
# 大类标题正则：## 一、xxx  或  ## 二、xxx
_SECTION_RE = re.compile(r'^##\s+[一二三四五六七八九十百千]+[、．.]\s*(.+)')


def parse_industry_list_md(md_path: str = None) -> dict[str, str]:
    """解析 stock_industry_list.md，返回 {stock_code: sector_name} 映射。

    sector_name 为 _SECTOR_WEIGHTS 中的键：科技/有色金属/汽车/新能源/医药/化工/制造
    未匹配到板块的股票不会出现在返回结果中。
    优先使用MD文件中的映射，硬编码映射作为补充。
    """
    if md_path is None:
        md_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'data_results', 'industry_analysis', 'stock_industry_list.md'
        )

    # 先从MD文件加载映射
    mapping = {}

    if os.path.exists(md_path):
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

    # 硬编码映射覆盖MD文件（硬编码优先级更高）
    mapping.update(_HARDCODED_SECTOR)

    return mapping


def get_sector_peers(mapping: dict[str, str], stock_code: str, max_peers: int = 10) -> list[str]:
    """获取同板块的其他股票代码列表（不含自身）。"""
    sector = mapping.get(stock_code)
    if not sector:
        return []
    peers = [c for c, s in mapping.items() if s == sector and c != stock_code]
    # 返回前 max_peers 个
    return peers[:max_peers]
