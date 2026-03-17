#!/usr/bin/env python3
"""
从东方财富获取所有行业板块和概念板块的成分股，构建多标签分类。
一只股票可以同时属于多个行业板块和多个概念板块。

输出:
  data_results/industry_analysis/stock_boards_map.json   — 完整JSON数据
  data_results/industry_analysis/stock_industry_list.md  — Markdown格式

Usage:
  python acquire_classify_stocks.py          # 使用缓存数据生成MD
  python acquire_classify_stocks.py --fetch  # 从东方财富API重新获取数据
"""
import json
import logging
import re
import os
import sys
import random
import time
import urllib.request
from collections import defaultdict

from service.jqka10.stock_concept_boards_10jqka import batch_fetch_concept_boards

# ============================================================
# 1. Parse score list
# ============================================================
def parse_score_list(fp):
    stocks = {}
    with open(fp, 'r', encoding='utf-8') as f:
        for line in f:
            m = re.match(r'(.+?) \((\d{6}\.\w{2})\) - 打分：(\d+)', line.strip())
            if m:
                code = m.group(2)
                if code.endswith('.BJ'):
                    continue  # 忽略北交所个股
                stocks[code] = m.group(1).strip()
    return stocks

# ============================================================
# 2. East Money API helpers
# ============================================================
BASE_URL = 'https://push2delay.eastmoney.com/api/qt/clist/get'

def _fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            cv = f'{random.randint(118, 124)}.0.{random.randint(1000, 9999)}.{random.randint(10, 999)}'
            req = urllib.request.Request(url, headers={
                'User-Agent': f'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{cv} Safari/537.36',
                'Referer': 'https://quote.eastmoney.com/',
                'Accept': '*/*',
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                text = resp.read().decode('utf-8')
                json_text = re.sub(r'^\w+\(', '', text)
                json_text = re.sub(r'\);?$', '', json_text)
                return json.loads(json_text)
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 + random.uniform(0, 1))
            else:
                raise

def get_all_boards(board_type):
    """获取所有板块列表。board_type: 2=行业板块, 3=概念板块"""
    boards = []
    page = 1
    while True:
        url = f'{BASE_URL}?pn={page}&pz=500&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f12&fs=m:90+t:{board_type}+f:!50&fields=f12,f14'
        data = _fetch_json(url)
        items = data.get('data', {}).get('diff', [])
        total = data.get('data', {}).get('total', 0)
        if not items:
            break
        boards.extend(items)
        if len(boards) >= total:
            break
        page += 1
        time.sleep(0.15)
    return [(b['f12'], b['f14']) for b in boards]

def get_board_stocks(board_code):
    """获取某个板块的所有成分股"""
    stocks = []
    page = 1
    while True:
        url = f'{BASE_URL}?pn={page}&pz=500&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f12&fs=b:{board_code}+f:!50&fields=f12,f13,f14'
        data = _fetch_json(url)
        items = data.get('data', {}).get('diff', [])
        total = data.get('data', {}).get('total', 0)
        if not items:
            break
        for item in items:
            code_num = item.get('f12', '')
            market = item.get('f13', 0)
            name = item.get('f14', '')
            if code_num:
                suffix = 'SH' if market == 1 else 'SZ'
                stocks.append((f'{code_num}.{suffix}', name))
        if len(stocks) >= total:
            break
        page += 1
        time.sleep(0.1)
    return stocks


# ============================================================
# 3. Fetch all boards and build mapping
# ============================================================
def fetch_and_build_mapping(cache_path):
    """
    获取所有行业板块和概念板块的成分股，构建:
    {
      "industry_boards": {"板块名": ["code1", "code2", ...], ...},
      "concept_boards":  {"板块名": ["code1", "code2", ...], ...},
      "stocks": {
        "code": {
          "name": "股票名",
          "industry_boards": ["行业1", "行业2"],
          "concept_boards": ["概念1", "概念2"]
        }
      }
    }
    """
    print('Step 1/4: 获取行业板块列表...')
    industry_boards = get_all_boards(2)
    print(f'  共 {len(industry_boards)} 个行业板块')

    print('Step 2/4: 获取概念板块列表...')
    concept_boards = get_all_boards(3)
    print(f'  共 {len(concept_boards)} 个概念板块')

    # 构建板块 -> 成分股映射
    industry_map = {}  # board_name -> [(code, name), ...]
    concept_map = {}
    # 反向映射: stock_code -> {industry_boards: [], concept_boards: []}
    stock_map = {}  # code -> {name, industry_boards, concept_boards}

    print(f'Step 3/4: 获取 {len(industry_boards)} 个行业板块成分股...')
    for i, (bcode, bname) in enumerate(industry_boards):
        stocks = get_board_stocks(bcode)
        industry_map[bname] = [s[0] for s in stocks]
        for code, name in stocks:
            if code not in stock_map:
                stock_map[code] = {'name': name, 'industry_boards': [], 'concept_boards': []}
            stock_map[code]['industry_boards'].append(bname)
        if (i + 1) % 50 == 0:
            print(f'  行业板块进度: {i+1}/{len(industry_boards)} ({len(stock_map)} stocks)')
        time.sleep(0.08)

    print(f'  行业板块完成: {len(industry_map)} boards, {len(stock_map)} stocks')

    print(f'Step 4/4: 获取 {len(concept_boards)} 个概念板块成分股...')
    for i, (bcode, bname) in enumerate(concept_boards):
        stocks = get_board_stocks(bcode)
        concept_map[bname] = [s[0] for s in stocks]
        for code, name in stocks:
            if code not in stock_map:
                stock_map[code] = {'name': name, 'industry_boards': [], 'concept_boards': []}
            stock_map[code]['concept_boards'].append(bname)
        if (i + 1) % 50 == 0:
            print(f'  概念板块进度: {i+1}/{len(concept_boards)} ({len(stock_map)} stocks)')
        time.sleep(0.08)

    print(f'  概念板块完成: {len(concept_map)} boards, {len(stock_map)} stocks')

    result = {
        'industry_boards': industry_map,
        'concept_boards': concept_map,
        'stocks': stock_map,
    }

    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f'已保存到 {cache_path}')

    return result


# ============================================================
# 3b. 同花顺概念噪音过滤 + 从同花顺获取概念板块并按相关性排序
# ============================================================

# 同花顺特有的非行业概念标签关键词
_10JQKA_NOISE_KEYWORDS = [
    # 同花顺自有指数/组合
    '同花顺', '中国AI 50',
    # 机构持仓/资金类
    '国家大基金', '证金持股', '国企改革', '央企',
    # 沪深港通/融资融券
    '沪股通', '深股通', '融资融券',
    # MSCI/指数成分
    'MSCI', '标普', '富时',
    # 财报/业绩类
    '预增', '预减', '扭亏', '季报', '年报',
    # 概念股（公司关联而非行业）
    '中芯国际概念', '华为海思概念', '宁德时代概念',
    '比亚迪概念', '茅台概念',
]

def _is_10jqka_noise(concept_name: str) -> bool:
    """判断同花顺概念是否为非行业属性的噪音标签"""
    for kw in _10JQKA_NOISE_KEYWORDS:
        if kw in concept_name:
            return True
    return False


def _enrich_concepts_from_10jqka(data: dict, score_all: dict):
    """
    从同花顺个股页面抓取概念板块，用于:
    1. 按同花顺给出的相关性排序（页面顺序 = 相关性排序）
    2. 标记最相关概念（top_concepts）

    修改 data['stocks'] 中每只股票的数据，添加:
      - concept_boards_10jqka: 同花顺原始概念列表（按相关性排序）
      - concept_boards: 同花顺概念列表（按相关性排序，不再补充东方财富独有概念）
      - top_concepts: 最相关的概念（10jqka排名前3的概念）
    """
    stock_codes = list(score_all.keys())
    print(f'\n=== 从同花顺获取概念板块（{len(stock_codes)}只，单线程顺序抓取）===')

    jqka_results = batch_fetch_concept_boards(stock_codes, delay=0.15)
    print(f'  同花顺概念获取完成: {len(jqka_results)}/{len(stock_codes)} 成功')

    stock_map = data['stocks']
    enriched = 0

    for code in stock_codes:
        jqka_concepts = jqka_results.get(code)
        if jqka_concepts is None:
            continue

        if code not in stock_map:
            stock_map[code] = {
                'name': score_all.get(code, ''),
                'industry_boards': [],
                'concept_boards': [],
            }

        info = stock_map[code]

        # 保存10jqka原始概念列表
        info['concept_boards_10jqka'] = jqka_concepts

        # 标记最相关概念: 同花顺页面概念顺序即为相关性排序，直接取前3个
        info['top_concepts'] = jqka_concepts[:3]

        # 概念板块直接使用同花顺数据（按相关性排序，去重）
        seen = set()
        deduped = []
        for c in jqka_concepts:
            if c not in seen:
                deduped.append(c)
                seen.add(c)

        info['concept_boards'] = deduped
        enriched += 1

    print(f'  已更新概念: {enriched}只股票（仅同花顺概念，不补充东方财富独有概念）')


# ============================================================
# 4. 行业板块归类到大类 (用于MD文件的章节组织)
# ============================================================
# 东方财富行业板块 -> 大类section 映射
# 关键词匹配规则: (section, keywords_in_board_name)
# 顺序重要 — 先匹配更具体的关键词
_SECTION_KEYWORD_RULES = [
    ('半导体与集成电路', ['半导体', '集成电路', '芯片', '封测', 'MCU', 'GPU', 'FPGA', '分立器件']),
    ('电子', ['电子', '光学', '光电', '消费电子', '元件', 'LED', 'OLED', '面板', '传感器', '连接器', 'PCB', '印制电路板', '安防']),
    ('计算机', ['计算机', '软件', 'IT服务', '网络安全', '信息安全', '云计算', '大数据', '人工智能', '数据库', '操作系统', 'ERP', 'SaaS']),
    ('通信', ['通信', '5G', '物联网', '卫星', '光通信', '电信运营']),
    ('传媒', ['传媒', '游戏', '广告', '影视', '出版', '数字媒体', '电视广播', '院线', '动漫', '门户网站', '图片媒体', '文字媒体', '视频媒体']),
    ('电力设备', ['电力设备', '电池', '光伏', '风电', '风力发电', '电网', '电源设备', '电机', '储能', '逆变器', '充电桩', '燃料电池', '锂电', '蓄电池', '输变电', '配电', '火电设备', '硅料硅片', '线缆部件']),
    ('汽车', ['汽车', '乘用车', '商用车', '商用载客车', '商用载货车', '摩托车', '车身', '底盘', '轮胎', '轮毂']),
    ('机械设备', ['机械', '通用设备', '专用设备', '自动化', '工程机械', '轨交', '金属制品', '仪器仪表', '机器人', '机床', '工控', '激光', '印刷包装机', '农用机械', '楼宇设备', '磨具磨料', '能源及重型设备']),
    ('医药生物', ['医药', '制药', '中药', '生物制品', '医疗', '药商', '药店', '原料药', '疫苗', '血液', '诊断', '医院', '医美']),
    ('基础化工', ['化工', '化学', '塑料', '橡胶', '农化', '农药', '非金属材料', '涂料', '日用化学', '膜材料', '氟化工', '磷化工', '民爆', '纺织化学', '纯碱', '氯碱', '氮肥', '钾肥', '磷肥', '复合肥', '炭黑', '有机硅', '聚氨酯', '胶黏剂', '合成树脂', '改性塑料', '氨纶', '涤纶', '粘胶', '锦纶', '化学纤维', '橡胶助剂', '无机盐', '钛白粉', '煤化工']),
    ('有色金属', ['有色金属', '工业金属', '贵金属', '小金属', '能源金属', '金属新材料', '稀土', '钨', '钼', '铜', '铝', '锌', '铅', '锡', '镍', '钴', '锂', '黄金', '白银', '铅锌', '磁性材料', '铁矿石']),
    ('钢铁', ['钢铁', '普钢', '特钢', '冶钢', '钢结构', '钢管', '长材', '板材']),
    ('煤炭', ['煤炭', '焦炭', '焦煤', '动力煤']),
    ('石油石化', ['石油', '油气', '油服', '油田服务', '炼化', '炼油', '石化']),
    ('食品饮料', ['食品', '饮料', '白酒', '啤酒', '乳品', '休闲食品', '调味', '农产品加工', '保健品', '肉制品', '预加工', '烘焙', '零食', '粮油', '软饮料', '食用菌', '熟食', '酒类', '食品及饲料添加剂']),
    ('家用电器', ['家电', '家用电器', '白色家电', '黑色家电', '小家电', '厨卫电器', '照明设备', '家电零部件', '空调', '冰洗', '彩电', '厨房电器', '厨房小家电', '清洁小家电', '个护小家电', '卫浴电器']),
    ('国防军工', ['国防军工', '军工', '航空装备', '航天装备', '地面兵装', '航海装备', '船舶制造']),
    ('建筑装饰', ['建筑装饰', '房屋建设', '基础建设', '装修装饰', '专业工程', '工程咨询', '园林工程', '基建市政', '国际工程']),
    ('建筑材料', ['建筑材料', '水泥', '玻璃', '玻纤', '装修建材', '耐火材料', '管材', '瓷砖', '地板', '防水材料', '其他建材']),
    ('交通运输', ['交通运输', '铁路', '公路', '航运', '港口', '航空', '机场', '物流', '公交', '快递', '仓储', '高速公路', '航空运输', '其他运输设备']),
    ('银行', ['银行', '城商行', '农商行', '股份制银行', '国有大型银行']),
    ('非银金融', ['非银金融', '证券', '保险', '多元金融', '信托', '期货', '金融信息', '金融控股', '资产管理', '租赁']),
    ('房地产', ['房地产', '住宅开发', '商业地产', '产业地产', '物业管理', '房产租赁', '房地产综合', '商业物业经营', '营销代理']),
    ('公用事业', ['公用事业', '电力', '燃气', '水务', '火电', '水电', '绿电', '热电', '核电', '火力发电', '水力发电', '核力发电', '其他能源发电', '热力服务', '电能综合', '综合电力']),
    ('环保', ['环保', '环境治理', '固废', '大气治理', '水务及水治理', '综合环境']),
    ('美容护理', ['美容护理', '化妆品', '医疗美容', '个护用品', '洗护用品', '品牌化妆品', '医美']),
    ('纺织服饰', ['纺织服饰', '纺织', '服装', '饰品', '鞋帽', '辅料', '运动服装', '非运动服装', '家纺', '钟表珠宝', '印染', '棉纺', '纺织鞋类']),
    ('轻工制造', ['轻工制造', '造纸', '包装印刷', '家居用品', '文娱用品', '家具', '定制家居', '成品家居', '文化用品', '娱乐用品', '生活用纸', '大宗用纸', '特种纸', '纸包装', '塑料包装', '金属包装', '综合包装', '印刷', '卫浴制品', '其他家居']),
    ('农林牧渔', ['农林牧渔', '种植', '养殖', '饲料', '林业', '渔业', '动物保健', '农业综合', '果蔬', '粮食', '水产', '生猪', '肉鸡', '宠物食品', '海洋捕捞', '种子', '畜禽饲料', '水产饲料']),
    ('社会服务', ['社会服务', '旅游', '酒店', '餐饮', '教育', '专业服务', '体育', '人力资源', '会展', '检测服务', '培训', '学历教育', '人工景区', '自然景区']),
    ('商贸零售', ['商贸零售', '一般零售', '贸易', '互联网电商', '专业连锁', '百货', '超市', '多业态零售', '电商服务', '跨境电商', '综合电商', '端到端供应链', '原材料供应链']),
    ('综合', ['综合']),
]

def _resolve_board_section(board_name):
    """将行业板块名映射到大类section"""
    for section, keywords in _SECTION_KEYWORD_RULES:
        for kw in keywords:
            if kw in board_name:
                return section
    return None


# ============================================================
# 5. Generate output files
# ============================================================
SECTION_ORDER = [
    '半导体与集成电路', '电子', '计算机', '通信', '传媒',
    '电力设备', '汽车', '机械设备', '医药生物', '基础化工',
    '有色金属', '钢铁', '煤炭', '石油石化',
    '食品饮料', '家用电器', '国防军工',
    '建筑装饰', '建筑材料', '交通运输',
    '银行', '非银金融', '房地产',
    '公用事业', '环保', '美容护理',
    '纺织服饰', '轻工制造', '农林牧渔',
    '社会服务', '商贸零售', '综合',
]

CN_NUMBERS = ['一','二','三','四','五','六','七','八','九','十',
              '十一','十二','十三','十四','十五','十六','十七','十八','十九','二十',
              '二十一','二十二','二十三','二十四','二十五','二十六','二十七','二十八','二十九','三十',
              '三十一','三十二','三十三','三十四','三十五']


# ============================================================
# 5b. 噪音概念板块过滤 — 剔除非行业属性的概念标签
# ============================================================
# 关键词匹配: 概念名包含任一关键词即视为噪音
_NOISE_CONCEPT_KEYWORDS = [
    # 财报类
    '预增', '预减', '扭亏', '中报', '三季报', '年报',
    # 市值/价格/估值类
    '小盘股', '微盘股', '微盘精选', '百元股', '低价股', '微利股',
    '破净股', '长期破净', '红利破净股', '红利股', '价值股', '周期股',
    # 昨日交易信号类
    '昨日涨停', '昨日炸板', '昨日触板', '昨日连板', '昨日首板',
    '昨日高振幅', '昨日高换手', '最近多板', '东方财富热股',
    # 指数成分类
    'HS300_', '上证180_', '上证380', '上证50_', '央视50_',
    '深成500', '深证100R', '中证500', 'MSCI中国', '标准普尔',
    '富时罗素', '创业成份', '创业板综',
    # 机构持仓类
    'QFII重仓', '基金重仓', '机构重仓', '社保重仓', '证金持股',
    # 融资/转债/股权类
    '融资融券', '转债标的', '股权激励', '股权转让',
    # 政策/标签类
    '专精特新',
    # 沪深港通
    '沪股通', '深股通',
    # 其他非行业标签
    '注册制次新股', '次新股', '超级品牌',
    '茅指数', '宁组合', '中字头', '中特估',
    # 地域/自贸区概念
    '西部大开发', '长江三角', '深圳特区', '雄安新区', '京津冀',
    '成渝特区', '海南自贸', '粤港自贸', '上海自贸', '湖北自贸',
    '滨海新区', '东北振兴', '沪企改革',
    # 参股类
    '参股银行', '参股券商', '参股保险', '参股新三板', '参股期货',
    # 过期/疫情概念
    '消毒剂', '病毒防治', '痘病毒防治', '气溶胶检测', '数字哨兵',
    # 过期政策/事件驱动
    '地摊经济', '供销社概念', '举牌', 'IPO受益', '并购重组概念',
    # 股票属性标签
    'ST股', 'AH股', 'AB股', 'GDR', '创投', '独角兽',
    '券商概念', '北交所概念', '科创板做市商', '科创板做市股',
    # 过期/已冷却概念
    'NFT概念', '元宇宙概念', 'Web3.0', 'ChatGPT概念', 'Sora概念',
    '工业大麻', '盲盒经济', '共享经济', '社区团购', 'C2M概念',
    '人脑工程', '可燃冰', '区块链', '数字货币', '虚拟现实',
    '虚拟数字人', '流感', '超级真菌', '培育钻石', '户外露营',
    '预制菜概念', '远程办公', '冰雪经济',
    # 政策/宏观标签（非行业属性）
    '央国企改革', '一带一路', '贬值受益', '内贸流通', '反内卷概念',
    'PPP模式', '乡村振兴', '新型城镇化', '新型工业化', '统一大市场',
    '首发经济', '化债', '中俄贸易概念', '养老金', '知识产权',
    '土地流转', '租售同权',
    # 重复/过于宽泛
    '新能源', '军工', '军民融合', '节能环保', '新材料',
    '电商概念', '大数据', '互联网金融', '互联网服务',
    '智慧城市', '数字经济', '智慧政务',
    # 消费/食品（与行业板块重复）
    '白酒', '酿酒概念', '乳业', '猪肉概念', '鸡肉概念',
    '啤酒概念', '水产概念',
    # 其他无实际分析价值
    '退税商店', '进口博览', '谷子经济', '电子竞技', '彩票概念',
    '数字阅读', '数字水印', '抗菌面料', '食品安全', '网红经济',
    '免税概念', 'REITs概念', '财税数字化', '电子身份证',
    '房屋检测', '海绵城市',
    # 已普及/已冷却/太小众
    '移动支付', 'ETC', '垃圾分类', '在线教育', '人造肉',
    '代糖概念', '电子烟', '短剧互动游戏', '托育服务', '全息技术',
    '智能电视', '3D玻璃', '3D摄像头', '屏下摄像',
    # 已成熟/与行业板块重复
    '超清视频', '无线耳机', '广电', '包装材料', '造纸印刷',
    '工程建设', '旅游酒店', '旅游概念', '影视概念', '网络游戏',
    '快递概念', '调味品概念', '化妆品概念', '职业教育',
]

# 精确匹配黑名单: 需要剔除的公司关联概念（精确匹配，避免误伤白名单）
_NOISE_CONCEPT_EXACT = {
    '华为汽车', '华为海思', '华为昇腾', '华为欧拉',
    '小米概念', '小米汽车',
    '阿里概念', '蚂蚁概念', '百度概念', '腾讯云',
    '京东金融', '商汤概念', '智谱AI', 'Kimi概念',
    '荣耀概念', '拼多多概念', '快手概念', '小红书概念',
    '抖音概念(字节概念)', '抖音小店', '娃哈哈概念', '富士康',
    '中芯概念',  # 公司关联
}

# 白名单: 即使匹配到关键词也保留的概念
_KEEP_CONCEPT_WHITELIST = {
    '苹果概念', '特斯拉', '英伟达概念', '华为概念',
    '新能源车',  # 保留，区别于过于宽泛的"新能源"
}


def _is_noise_concept(concept_name):
    """判断概念板块是否为噪音标签"""
    # 白名单优先
    if concept_name in _KEEP_CONCEPT_WHITELIST:
        return False
    # 精确匹配黑名单
    if concept_name in _NOISE_CONCEPT_EXACT:
        return True
    # 关键词匹配
    for kw in _NOISE_CONCEPT_KEYWORDS:
        if kw in concept_name:
            return True
    return False


def _filter_concept_boards(concept_list):
    """过滤掉噪音概念板块，返回有效概念列表"""
    return [c for c in concept_list if not _is_noise_concept(c)]


def generate_outputs(data, score_all, output_dir):
    """
    生成:
    1. stock_boards_map.json — 完整JSON
    2. stock_industry_list.md — Markdown格式
    """
    stock_map = data['stocks']
    industry_boards = data['industry_boards']

    # ── JSON输出 ──
    json_path = os.path.join(output_dir, 'stock_boards_map.json')
    # 只输出score_all中的股票，并过滤噪音概念
    filtered_stocks = {}
    for code, name in score_all.items():
        if code in stock_map:
            info = stock_map[code]
            filtered_concepts = _filter_concept_boards(info.get('concept_boards', []))
            top_concepts = info.get('top_concepts', [])
            # top_concepts 也需要过滤噪音
            top_concepts = [c for c in top_concepts if c in filtered_concepts]
            filtered_stocks[code] = {
                'name': info.get('name', name),
                'industry_boards': info.get('industry_boards', []),
                'concept_boards': filtered_concepts,
                'top_concepts': top_concepts,
            }
        else:
            filtered_stocks[code] = {
                'name': name,
                'industry_boards': [],
                'concept_boards': [],
                'top_concepts': [],
            }

    # 过滤噪音概念板块（顶层）
    filtered_concept_boards = {
        bname: codes for bname, codes in data['concept_boards'].items()
        if not _is_noise_concept(bname)
    }

    json_output = {
        'update_date': time.strftime('%Y-%m-%d'),
        'total_stocks': len(filtered_stocks),
        'total_industry_boards': len(data['industry_boards']),
        'total_concept_boards': len(filtered_concept_boards),
        'stocks': filtered_stocks,
        'industry_boards': data['industry_boards'],
        'concept_boards': filtered_concept_boards,
    }
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(json_output, f, ensure_ascii=False, indent=2)
    print(f'已生成 {json_path}')

    # ── MD输出 ──
    md_path = os.path.join(output_dir, 'stock_industry_list.md')

    # 按大类组织: section -> subsection(行业板块) -> [(name, code, concept_boards, top_concepts)]
    structure = defaultdict(lambda: defaultdict(list))
    unclassified_boards = set()

    for code, name in score_all.items():
        info = stock_map.get(code, {})
        iboards = info.get('industry_boards', [])
        cboards = _filter_concept_boards(info.get('concept_boards', []))
        top_concepts = set(info.get('top_concepts', []))

        if not iboards:
            # 无行业板块的股票
            if 'ST' in name:
                structure['ST及风险警示']['ST及风险警示'].append((name, code, cboards, top_concepts))
            else:
                structure['其他/待分类']['其他/待分类'].append((name, code, cboards, top_concepts))
            continue

        # 一只股票可以出现在多个行业板块中
        for board_name in iboards:
            section = _resolve_board_section(board_name)
            if section:
                structure[section][board_name].append((name, code, cboards, top_concepts))
            else:
                unclassified_boards.add(board_name)
                structure['其他行业'][board_name].append((name, code, cboards, top_concepts))

    if unclassified_boards:
        print(f'注意: {len(unclassified_boards)} 个行业板块未映射到大类:')
        for b in sorted(unclassified_boards):
            count = sum(len(v) for v in structure['其他行业'].values() if b in structure['其他行业'])
            print(f'  {b}')

    # 排序并写入
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write('# 股票行业分类清单（多板块归属）\n\n')
        f.write('> 基于东方财富行业板块+同花顺概念板块数据，一只股票可同时属于多个板块\n')
        f.write('> 概念板块来源：同花顺个股页面（按相关性排序，★标记最相关概念）\n')
        f.write(f'> 数据来源：东方财富API（{len(industry_boards)}个行业板块） + 同花顺概念（{len(filtered_concept_boards)}个概念板块，已过滤噪音标签）\n')
        f.write(f'> 更新日期：{time.strftime("%Y-%m-%d")}\n')
        f.write(f'> 股票总数：{len(score_all)}（含重复归属）\n\n')
        f.write('---\n\n')

        all_sections = SECTION_ORDER + [s for s in structure if s not in SECTION_ORDER and s not in ('ST及风险警示', '其他/待分类', '其他行业')]
        all_sections += ['其他行业', 'ST及风险警示', '其他/待分类']

        section_idx = 0
        total_entries = 0
        unique_stocks = set()

        for section in all_sections:
            if section not in structure:
                continue

            cn = CN_NUMBERS[section_idx] if section_idx < len(CN_NUMBERS) else str(section_idx + 1)
            f.write(f'## {cn}、{section}\n\n')

            subsections = structure[section]
            sub_keys = sorted(subsections.keys(), key=lambda x: (-len(subsections[x]), x))

            for sub_idx, sub_name in enumerate(sub_keys):
                stocks = subsections[sub_name]
                stocks.sort(key=lambda x: x[1])  # sort by code

                if sub_name != section and len(sub_keys) > 1:
                    f.write(f'### {section_idx + 1}.{sub_idx + 1} {sub_name}（{len(stocks)}只）\n')

                for name, code, cboards, top_concepts in stocks:
                    concept_str = ''
                    if cboards:
                        # 标记最相关概念: top_concepts中的概念加★前缀
                        display = []
                        for c in cboards[:8]:
                            if c in top_concepts:
                                display.append(f'★{c}')
                            else:
                                display.append(c)
                        suffix = '...' if len(cboards) > 8 else ''
                        concept_str = f' 【{", ".join(display)}{suffix}】'
                    f.write(f'- {name} ({code}){concept_str}\n')
                    total_entries += 1
                    unique_stocks.add(code)
                f.write('\n')

            f.write('---\n\n')
            section_idx += 1

    print(f'已生成 {md_path}')
    print(f'  MD条目总数: {total_entries}（含重复归属）')
    print(f'  唯一股票数: {len(unique_stocks)}')
    print(f'  Score list: {len(score_all)}')

    # 检查覆盖率
    missing = set(score_all.keys()) - unique_stocks
    if missing:
        print(f'  ⚠ {len(missing)} 只股票未出现在任何行业板块中')
    else:
        print(f'  ✅ 所有股票均已覆盖')


# ============================================================
# 6. Main
# ============================================================
def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    score_all = parse_score_list('data_results/stock_to_score_list/stock_score_list.md')
    print(f'Score list: {len(score_all)} stocks')

    output_dir = 'data_results/industry_analysis'
    cache_path = os.path.join(output_dir, 'stock_boards_map.json')

    if '--fetch' in sys.argv:
        print('\n=== 从东方财富API获取板块数据 ===')
        data = fetch_and_build_mapping(cache_path)
    else:
        if not os.path.exists(cache_path):
            print(f'缓存文件不存在: {cache_path}')
            print('请使用 --fetch 参数从API获取数据')
            return
        print(f'加载缓存: {cache_path}')
        with open(cache_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

    stock_map = data['stocks']
    print(f'板块数据: {len(data["industry_boards"])} 行业板块, {len(data["concept_boards"])} 概念板块, {len(stock_map)} stocks')

    # 统计
    matched = sum(1 for c in score_all if c in stock_map)
    multi_industry = sum(1 for c in score_all if c in stock_map and len(stock_map[c].get('industry_boards', [])) > 1)
    avg_industry = sum(len(stock_map[c].get('industry_boards', [])) for c in score_all if c in stock_map) / max(matched, 1)
    avg_concept = sum(len(stock_map[c].get('concept_boards', [])) for c in score_all if c in stock_map) / max(matched, 1)

    print(f'\n统计:')
    print(f'  匹配到板块数据: {matched}/{len(score_all)}')
    print(f'  属于多个行业板块: {multi_industry} stocks')
    print(f'  平均行业板块数: {avg_industry:.1f}')
    print(f'  平均概念板块数: {avg_concept:.1f}')

    # 从同花顺获取概念板块并按相关性排序（并行拉取）
    if '--no-10jqka' not in sys.argv:
        _enrich_concepts_from_10jqka(data, score_all)

    print(f'\n=== 生成输出文件 ===')
    generate_outputs(data, score_all, output_dir)


if __name__ == '__main__':
    main()
