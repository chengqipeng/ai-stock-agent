#!/usr/bin/env python3
"""
Qwen3-235B 周预测模块
====================
基于 StockBench 基准测试的洞察，针对 Qwen3-235B-Ins 的特性优化：
- Ins 模式（enable_thinking=False）：风控能力最强，回撤 -11.2%（所有模型最低）
- 严格结构化输出：Ins 版 schema 错误率仅 2%（Think 版 8%）
- 多维信息融合：消融实验证明新闻+基本面缺一不可
- 保守置信度区间：避免过度交易，与 Qwen3-Ins 的低回撤特性一致

架构设计：
- 感知层：多维特征打包（K线、资金流、板块、财报、技术指标）
- 推理层：Qwen3-235B-Ins（关闭思考模式，低温度确定性输出）
- 风控层：双重置信度过滤 + 风险评级 + 规则引擎交叉验证
- 批量层：分组批量分析（利用 Qwen3 对 10~20 只股票的最优处理能力）

用法：
    from service.analysis.qwen_nw_predictor import predict_next_week_with_qwen
    result = await predict_next_week_with_qwen(code, stock_name, features)

    # 批量预测
    from service.analysis.qwen_nw_predictor import batch_predict_with_qwen
    results = await batch_predict_with_qwen(stocks)
"""
import asyncio
import json
import logging
import time
from typing import Optional

from common.utils.llm_utils import parse_llm_json

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# Qwen 客户端（懒加载单例）
# ═══════════════════════════════════════════════════════════

_client = None


def _get_client():
    global _client
    if _client is None:
        from service.llm.qwen_client import QwenClient
        _client = QwenClient()
    return _client


# 默认模型：Qwen3-235B-A22B（Ins 模式）
# StockBench 综合排名第2，风控第1（最大回撤 -11.2%）
_DEFAULT_MODEL = "qwen3-235b-a22b"


# ═══════════════════════════════════════════════════════════
# System Prompt — 针对 Qwen3-Ins 优化的决策框架
# ═══════════════════════════════════════════════════════════
#
# 设计原则（基于 StockBench 洞察）：
# 1. 严格 JSON schema — Qwen3-Ins schema 错误率仅 2%，充分利用其格式遵循能力
# 2. 显式风险评级 — 利用 Qwen3-Ins 最强的风控特性（回撤 -11.2%）
# 3. A股特化规则 — StockBench 测的是美股，A股有涨跌停/T+1/散户占比高等特性
# 4. 反向思维偏重 — A股超跌反弹概率 >50%，这是规则引擎 R1 准确率 89.6% 的核心逻辑

_SYSTEM_PROMPT = """你是A股量化分析引擎（Qwen3-Ins模式）。基于多维市场数据预测个股下周涨跌方向。

## 严格输出规则
1. 只输出一个JSON对象，不要任何其他文字
2. 必须包含以下5个字段，缺一不可：
   - direction: "UP" 或 "DOWN" 或 "UNCERTAIN"
   - confidence: 数值，范围 0.50~0.80
   - risk_level: "low" 或 "medium" 或 "high"
   - key_factors: 数组，1~3个关键因子字符串（每个≤15字）
   - justification: 中文，≤50字的综合判断理由

## A股决策框架

### 核心原则
- A股散户占比高，超跌反弹是最强信号（大盘跌>3%+个股跌 → 下周涨概率~90%）
- 逆势股延续性强：大盘跌但个股涨 → 倾向继续涨
- 单周下跌≠看跌（均值回归效应，下周反弹概率>50%）
- 看跌需要更强证据（至少2个独立看跌信号才输出DOWN）

### 看涨信号（每个+1分）
1. 本周跌幅>2%且大盘也跌>3%（系统性超跌，最强反弹信号）
2. 连续两周上涨（趋势延续）
3. 前周跌>5%但本周企稳（跌幅<1%或微涨）
4. 放量上涨（vol_ratio>1.2且本周涨）
5. 60日低位(<0.25)且本周跌幅收窄
6. 主力资金净流入(ff_signal>0.3)且量价配合
7. 所属板块整体大跌<-3%但个股跌幅小于板块（相对强势）

### 看跌信号（每个+1分，需要更高阈值）
1. 大盘跌1~3%但个股逆势涨>2%且连涨≥3天（过热回调）
2. 连续两周下跌且跌幅加速（趋势恶化）
3. 高位(>0.8)且放量下跌（主力出货）
4. 大盘涨>1%但个股跌>2%（独立弱势，被市场抛弃）
5. 连跌4天+最后一天仍跌（空头动能未衰竭）

### 决策矩阵
- 看涨≥2 且 看跌=0 → UP，confidence=0.60~0.70
- 看涨≥3 且 看跌≤1 → UP，confidence=0.65~0.75
- 看跌≥2 且 看涨=0 → DOWN，confidence=0.58~0.68
- 看跌≥3 且 看涨≤1 → DOWN，confidence=0.63~0.73
- 看涨=1 且 看跌=0 → UP，confidence=0.53~0.58
- 看跌=1 且 看涨=0 → UNCERTAIN（看跌证据不足）
- 信号矛盾或均为0 → UNCERTAIN，confidence=0.50

### 风险评级规则
- low: 大盘深跌后的超跌反弹 或 强趋势延续（信号一致≥3个）
- medium: 2个信号一致 或 有轻微矛盾
- high: 信号矛盾 或 高位放量 或 市场拐点不明

## 输出示例
```json
{"direction":"UP","confidence":0.65,"risk_level":"low","key_factors":["大盘深跌超跌反弹","低位企稳","资金流入"],"justification":"大盘跌3.8%+个股跌5.2%，60日低位企稳，主力资金流入确认反弹"}
```"""


# ═══════════════════════════════════════════════════════════
# 批量分析 System Prompt — 一次分析多只股票
# ═══════════════════════════════════════════════════════════
# StockBench 发现 Qwen3-235B 在 10~20 只股票时表现最优，
# 利用这一特性做分组批量分析，减少 API 调用次数。

_BATCH_SYSTEM_PROMPT = """你是A股量化分析引擎（Qwen3-Ins批量模式）。一次分析多只股票的下周涨跌方向。

## 严格输出规则
1. 输出一个JSON数组，每个元素对应一只股票
2. 每个元素必须包含6个字段：
   - code: 股票代码（与输入一致）
   - direction: "UP" 或 "DOWN" 或 "UNCERTAIN"
   - confidence: 数值，范围 0.50~0.80
   - risk_level: "low" 或 "medium" 或 "high"
   - key_factors: 数组，1~3个关键因子（每个≤15字）
   - justification: 中文≤50字
3. 数组长度必须与输入股票数量一致，不要遗漏

## A股决策框架（同单股模式）
- 超跌反弹是最强信号（大盘跌>3%+个股跌 → 涨概率~90%）
- 看跌需要更强证据（至少2个独立看跌信号）
- 单周下跌≠看跌（均值回归，反弹概率>50%）
- 不确定就UNCERTAIN，但UNCERTAIN不要超过50%

## 输出示例
```json
[{"code":"600519.SH","direction":"UP","confidence":0.65,"risk_level":"low","key_factors":["超跌反弹","资金流入"],"justification":"大盘深跌+个股跌，低位企稳反弹"},{"code":"000001.SZ","direction":"UNCERTAIN","confidence":0.50,"risk_level":"medium","key_factors":["信号矛盾"],"justification":"涨跌信号矛盾，方向不明"}]
```"""


# ═══════════════════════════════════════════════════════════
# User Prompt 构建 — 单股模式
# ═══════════════════════════════════════════════════════════

def _build_user_prompt(code: str, stock_name: str, features: dict) -> str:
    """将多维特征打包为 Qwen3 可理解的 User Prompt。

    相比 DeepSeek 版本的改进：
    - 增加技术指标摘要（利用 Qwen3 更强的数值推理能力）
    - 增加风险提示标签（利用 Qwen3-Ins 的风控优势）
    - 更紧凑的格式（减少 token 消耗，Qwen3 对简洁输入响应更好）
    """
    parts = [f"分析 {stock_name}（{code}）下周涨跌方向。\n"]

    # ── 大盘环境 ──
    mkt_chg = features.get('market_chg', 0)
    parts.append(f"【大盘】本周: {mkt_chg:+.2f}%")

    mkt_prev = features.get('_market_prev_week_chg')
    if mkt_prev is not None:
        parts.append(f"  前周: {mkt_prev:+.2f}%")
        if mkt_prev < -1 and mkt_chg < -1:
            parts.append("  ⚠️ 连续两周下跌")
        elif mkt_prev > 1 and mkt_chg > 1:
            parts.append("  📈 连续两周上涨")
        elif (mkt_prev < -1 and mkt_chg > 0.5) or (mkt_prev > 1 and mkt_chg < -0.5):
            parts.append("  ⚠️ 可能拐点")

    # ── 个股行情 ──
    this_chg = features.get('this_week_chg', 0)
    parts.append(f"\n【个股】本周: {this_chg:+.2f}%")
    parts.append(f"  连涨{features.get('consec_up', 0)}天 连跌{features.get('consec_down', 0)}天")
    parts.append(f"  尾日: {features.get('last_day_chg', 0):+.2f}%")
    parts.append(f"  市场: {features.get('_market_suffix', '未知')}")

    # 价格位置
    pos = features.get('_price_pos_60')
    if pos is not None:
        labels = {0.2: '低位', 0.4: '偏低', 0.6: '中位', 0.8: '偏高', 1.01: '高位'}
        pos_label = next(v for k, v in labels.items() if pos < k)
        parts.append(f"  60日位置: {pos:.2f}({pos_label})")

    # 前一周
    prev = features.get('_prev_week_chg')
    if prev is not None:
        parts.append(f"  前周: {prev:+.2f}%")

    # ── 资金面 ──
    ff = features.get('ff_signal')
    vr = features.get('vol_ratio')
    vpc = features.get('vol_price_corr')
    fund_parts = []
    if ff is not None:
        ff_label = '强流入' if ff > 0.5 else ('流入' if ff > 0 else ('流出' if ff > -0.5 else '强流出'))
        fund_parts.append(f"资金{ff_label}({ff:+.2f})")
    if vr is not None:
        vr_label = '显著放量' if vr > 1.5 else ('放量' if vr > 1.2 else ('正常' if vr > 0.8 else '缩量'))
        fund_parts.append(f"{vr_label}({vr:.2f})")
    if vpc is not None:
        vpc_label = '量价齐升' if vpc > 0.3 else ('量价背离' if vpc < -0.3 else '')
        if vpc_label:
            fund_parts.append(vpc_label)
    if fund_parts:
        parts.append(f"\n【资金】{' | '.join(fund_parts)}")

    # ── 板块面 ──
    bm = features.get('board_momentum')
    cc = features.get('concept_consensus')
    boards = features.get('concept_boards')
    board_parts = []
    if bm is not None:
        board_parts.append(f"动量{bm:+.2f}")
    if cc is not None:
        board_parts.append(f"共识{cc:.0%}")
    if boards:
        board_parts.append(boards[:60])
    if board_parts:
        parts.append(f"\n【板块】{' | '.join(board_parts)}")

    # ── 基本面 ──
    fs = features.get('finance_score')
    rev = features.get('revenue_yoy')
    prof = features.get('profit_yoy')
    roe = features.get('roe')
    fin_parts = []
    if fs is not None:
        fin_parts.append(f"综合{fs:.2f}")
    if rev is not None:
        fin_parts.append(f"营收YoY{rev:+.1f}%")
    if prof is not None:
        fin_parts.append(f"利润YoY{prof:+.1f}%")
    if roe is not None:
        fin_parts.append(f"ROE{roe:.1f}%")
    if fin_parts:
        parts.append(f"\n【基本面】{' | '.join(fin_parts)}")

    parts.append("\n输出JSON。")
    return '\n'.join(parts)


# ═══════════════════════════════════════════════════════════
# User Prompt 构建 — 批量模式
# ═══════════════════════════════════════════════════════════

def _build_batch_user_prompt(stocks: list[dict]) -> str:
    """将多只股票的特征打包为一个批量 User Prompt。

    Args:
        stocks: [{'code': ..., 'name': ..., 'features': {...}}, ...]
                 最多 10 只（Qwen3 在 10~20 只时表现最优）
    """
    parts = [f"请分析以下 {len(stocks)} 只股票下周的涨跌方向。\n"]

    for i, stock in enumerate(stocks, 1):
        code = stock['code']
        name = stock.get('name', '')
        feat = stock.get('features', {})

        mkt_chg = feat.get('market_chg', 0)
        this_chg = feat.get('this_week_chg', 0)
        pos = feat.get('_price_pos_60')
        prev = feat.get('_prev_week_chg')
        ff = feat.get('ff_signal')
        vr = feat.get('vol_ratio')
        bm = feat.get('board_momentum')

        line = f"\n### {i}. {name}（{code}）"
        line += f"\n大盘{mkt_chg:+.1f}% | 个股{this_chg:+.1f}%"
        line += f" | 连涨{feat.get('consec_up', 0)}连跌{feat.get('consec_down', 0)}"
        line += f" | 尾日{feat.get('last_day_chg', 0):+.1f}%"
        if pos is not None:
            line += f" | 位置{pos:.2f}"
        if prev is not None:
            line += f" | 前周{prev:+.1f}%"
        if ff is not None:
            line += f" | 资金{ff:+.2f}"
        if vr is not None:
            line += f" | 量比{vr:.2f}"
        if bm is not None:
            line += f" | 板块{bm:+.2f}"

        parts.append(line)

    parts.append(f"\n输出JSON数组（{len(stocks)}个元素）。")
    return '\n'.join(parts)


# ═══════════════════════════════════════════════════════════
# 响应解析与验证
# ═══════════════════════════════════════════════════════════

def _validate_single_result(result: dict) -> Optional[dict]:
    """验证并规范化单条预测结果。"""
    direction = str(result.get('direction', '')).upper()
    if direction not in ('UP', 'DOWN', 'UNCERTAIN'):
        return None

    confidence = float(result.get('confidence', 0.5))
    confidence = max(0.50, min(0.80, confidence))

    risk_level = str(result.get('risk_level', 'medium')).lower()
    if risk_level not in ('low', 'medium', 'high'):
        risk_level = 'medium'

    key_factors = result.get('key_factors', [])
    if not isinstance(key_factors, list):
        key_factors = [str(key_factors)]
    key_factors = [str(f)[:15] for f in key_factors[:3]]

    justification = str(result.get('justification', ''))[:100]

    return {
        'direction': direction,
        'confidence': round(confidence, 4),
        'risk_level': risk_level,
        'key_factors': key_factors,
        'justification': justification,
    }


# ═══════════════════════════════════════════════════════════
# 核心预测函数 — 单股模式
# ═══════════════════════════════════════════════════════════

async def predict_next_week_with_qwen(
    code: str,
    stock_name: str,
    features: dict,
    model: str = _DEFAULT_MODEL,
    timeout: float = 60.0,
) -> Optional[dict]:
    """调用 Qwen3-235B-Ins 预测单只股票下周方向。

    关键设计决策（基于 StockBench）：
    - enable_thinking=False：Ins 模式回撤 -11.2% vs Think 模式 -14.9%
    - temperature=0.2：比 DeepSeek 版(0.3)更低，利用 Qwen3-Ins 的纪律性
    - max_tokens=300：比 DeepSeek 版(200)多，因为增加了 risk_level 和 key_factors

    Args:
        code: 股票代码 (如 '600519.SH')
        stock_name: 股票名称
        features: 多维特征字典
        model: Qwen 模型名称
        timeout: 超时秒数

    Returns:
        {
            'direction': 'UP' | 'DOWN' | 'UNCERTAIN',
            'confidence': float (0.50~0.80),
            'risk_level': 'low' | 'medium' | 'high',
            'key_factors': list[str],
            'justification': str,
        }
        或 None（调用失败/解析失败）
    """
    client = _get_client()
    user_prompt = _build_user_prompt(code, stock_name, features)

    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]

    try:
        resp = await asyncio.wait_for(
            client.chat(
                messages=messages,
                model=model,
                temperature=0.2,
                max_tokens=300,
                enable_thinking=False,
            ),
            timeout=timeout,
        )

        content = resp.get('choices', [{}])[0].get('message', {}).get('content', '')
        if not content:
            logger.warning("Qwen 返回空内容: %s", code)
            return None

        result = parse_llm_json(content)
        validated = _validate_single_result(result)
        if validated is None:
            logger.warning("Qwen 输出格式无效: %s → %s", code, content[:100])
            return None

        return validated

    except asyncio.TimeoutError:
        logger.warning("Qwen 超时: %s (%.0fs)", code, timeout)
        return None
    except Exception as e:
        logger.warning("Qwen 预测失败: %s → %s", code, e)
        return None


# ═══════════════════════════════════════════════════════════
# 核心预测函数 — 批量模式（分组）
# ═══════════════════════════════════════════════════════════

async def predict_batch_grouped_with_qwen(
    stocks: list[dict],
    group_size: int = 8,
    model: str = _DEFAULT_MODEL,
    timeout: float = 120.0,
) -> dict[str, dict]:
    """分组批量调用 Qwen3 预测多只股票。

    StockBench 发现 Qwen3-235B 在 10~20 只股票时表现最优。
    利用这一特性，将股票分组后一次性分析，减少 API 调用次数。

    Args:
        stocks: [{'code': ..., 'name': ..., 'features': {...}}, ...]
        group_size: 每组股票数（默认8，保守值）
        model: Qwen 模型名称
        timeout: 单组超时秒数

    Returns:
        {code: {'direction': ..., 'confidence': ..., ...}}
    """
    client = _get_client()
    results = {}

    # 分组
    groups = [stocks[i:i + group_size] for i in range(0, len(stocks), group_size)]

    for group_idx, group in enumerate(groups):
        user_prompt = _build_batch_user_prompt(group)
        messages = [
            {"role": "system", "content": _BATCH_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ]

        try:
            resp = await asyncio.wait_for(
                client.chat(
                    messages=messages,
                    model=model,
                    temperature=0.2,
                    max_tokens=group_size * 150,
                    enable_thinking=False,
                ),
                timeout=timeout,
            )

            content = resp.get('choices', [{}])[0].get('message', {}).get('content', '')
            if not content:
                logger.warning("Qwen 批量返回空内容: group %d", group_idx)
                continue

            batch_result = parse_llm_json(content)

            # 处理返回结果
            if isinstance(batch_result, list):
                for item in batch_result:
                    item_code = item.get('code', '')
                    validated = _validate_single_result(item)
                    if validated and item_code:
                        results[item_code] = validated
            elif isinstance(batch_result, dict):
                # 有时模型只返回单个对象而非数组
                item_code = batch_result.get('code', '')
                validated = _validate_single_result(batch_result)
                if validated and item_code:
                    results[item_code] = validated

        except asyncio.TimeoutError:
            logger.warning("Qwen 批量超时: group %d (%.0fs)", group_idx, timeout)
        except Exception as e:
            logger.warning("Qwen 批量预测失败: group %d → %s", group_idx, e)

    return results


# ═══════════════════════════════════════════════════════════
# 批量预测入口（带并发控制和速率限制）
# ═══════════════════════════════════════════════════════════

async def batch_predict_with_qwen(
    stocks: list[dict],
    max_concurrency: int = 3,
    min_confidence: float = 0.55,
    use_batch_mode: bool = True,
    group_size: int = 8,
    model: str = _DEFAULT_MODEL,
    progress_callback=None,
) -> dict[str, dict]:
    """批量调用 Qwen3-235B 预测多只股票。

    支持两种模式：
    1. 批量模式（use_batch_mode=True）：分组一次分析多只，API 调用少但单次耗时长
    2. 单股模式（use_batch_mode=False）：逐只分析，与 DeepSeek 版行为一致

    Args:
        stocks: [{'code': '600519.SH', 'name': '贵州茅台', 'features': {...}}, ...]
        max_concurrency: 最大并发数（Qwen API 并发建议≤3）
        min_confidence: 最低置信度过滤阈值
        use_batch_mode: 是否使用分组批量模式
        group_size: 批量模式每组股票数
        model: Qwen 模型名称
        progress_callback: 进度回调 (total, done)

    Returns:
        {code: {'direction': ..., 'confidence': ..., 'risk_level': ...,
                'key_factors': [...], 'justification': ...}}
    """
    total = len(stocks)
    if total == 0:
        return {}

    logger.info("Qwen 批量预测开始: %d只股票, 模式=%s, 并发=%d",
                total, '批量' if use_batch_mode else '单股', max_concurrency)
    start_time = time.time()

    if use_batch_mode:
        results = await _batch_predict_grouped(
            stocks, max_concurrency, group_size, model, progress_callback
        )
    else:
        results = await _batch_predict_single(
            stocks, max_concurrency, model, progress_callback
        )

    # 置信度过滤
    filtered = {}
    for code, result in results.items():
        if result['direction'] == 'UNCERTAIN':
            continue
        if result['confidence'] >= min_confidence:
            filtered[code] = result
        else:
            logger.debug("Qwen 置信度过低: %s → %.2f < %.2f",
                         code, result['confidence'], min_confidence)

    elapsed = time.time() - start_time
    logger.info("Qwen 批量预测完成: %d/%d 有效结果 (置信度≥%.0f%%), 耗时%.1fs",
                len(filtered), total, min_confidence * 100, elapsed)
    return filtered


async def _batch_predict_grouped(
    stocks: list[dict],
    max_concurrency: int,
    group_size: int,
    model: str,
    progress_callback,
) -> dict[str, dict]:
    """分组批量预测的内部实现。"""
    results = {}
    semaphore = asyncio.Semaphore(max_concurrency)
    groups = [stocks[i:i + group_size] for i in range(0, len(stocks), group_size)]
    total = len(stocks)
    done_count = 0

    async def _predict_group(group: list[dict], group_idx: int):
        nonlocal done_count
        async with semaphore:
            group_results = await predict_batch_grouped_with_qwen(
                group, group_size=len(group), model=model
            )
            results.update(group_results)
            done_count += len(group)
            if progress_callback and done_count % 20 <= group_size:
                progress_callback(total, done_count)

    tasks = [_predict_group(g, i) for i, g in enumerate(groups)]
    await asyncio.gather(*tasks, return_exceptions=True)
    return results


async def _batch_predict_single(
    stocks: list[dict],
    max_concurrency: int,
    model: str,
    progress_callback,
) -> dict[str, dict]:
    """逐只预测的内部实现（与 DeepSeek 版行为一致）。"""
    results = {}
    semaphore = asyncio.Semaphore(max_concurrency)
    total = len(stocks)
    done_count = 0

    async def _predict_one(stock: dict):
        nonlocal done_count
        async with semaphore:
            code = stock['code']
            name = stock.get('name', '')
            features = stock.get('features', {})

            result = await predict_next_week_with_qwen(
                code, name, features, model=model
            )

            done_count += 1
            if progress_callback and done_count % 20 == 0:
                progress_callback(total, done_count)

            if result:
                results[code] = result

    tasks = [_predict_one(s) for s in stocks]
    await asyncio.gather(*tasks, return_exceptions=True)
    return results
