#!/usr/bin/env python3
"""
DeepSeek 增强下周预测模块
========================
借鉴 Alpha Arena 竞赛中 DeepSeek 的量化交易架构，
为规则引擎未命中的 ~90% 股票提供 AI 辅助预测。

核心设计：
- 感知层：多维特征打包（K线、资金流、板块、财报）
- 决策层：DeepSeek 推理引擎（结构化 Prompt + JSON 输出）
- 风控层：置信度过滤 + 规则引擎交叉验证

用法：
    from service.analysis.deepseek_nw_predictor import predict_next_week_with_deepseek
    result = await predict_next_week_with_deepseek(code, features)
"""
import asyncio
import json
import logging
import time
from typing import Optional

from common.utils.llm_utils import parse_llm_json

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
# DeepSeek 客户端（复用项目已有的 client）
# ═══════════════════════════════════════════════════════════

_client = None


def _get_client():
    global _client
    if _client is None:
        from service.llm.deepseek_client import DeepSeekClient
        _client = DeepSeekClient()
    return _client


# ═══════════════════════════════════════════════════════════
# System Prompt — 定义 AI 的角色和决策框架
# ═══════════════════════════════════════════════════════════
#
# 设计哲学（V5 重构）：
# ─────────────────────
# 1. 不给打分规则，让 DeepSeek 用自己的推理能力分析
#    → 之前的打分框架 DeepSeek 无法精确执行，反而成为噪声
# 2. 只给统计事实和思维框架，不给具体阈值
#    → 让 LLM 做它擅长的事：综合推理，而非机械计分
# 3. 强调"先分析再结论"的 Chain-of-Thought
#    → 在 justification 中要求写出推理过程，提高决策质量
# 4. 去除所有方向性暗示，保持中性
#    → 不说"默认偏空"，不说"倾向DOWN"，让数据说话
# 5. 强调 UNCERTAIN 是合理选项
#    → 不确定时不要强行给方向，降低错误率

_SYSTEM_PROMPT = """你是A股量化分析师。根据提供的市场数据，预测个股下周涨跌方向。

## 输出格式
只输出一个JSON对象：
{"direction":"UP/DOWN/UNCERTAIN","confidence":0.50~0.75,"justification":"≤50字推理过程"}

## 核心统计事实（必须牢记）
- A股任意一周：约40%股票上涨，60%下跌。因此默认预期是DOWN
- 均值回归是最强规律：
  · 本周跌>5%的股票 → 下周约63%概率反弹（UP）
  · 本周涨>5%的股票 → 下周约65%概率回调（DOWN）
  · 本周跌>3%且大盘也跌>2% → 下周约70%概率反弹（UP）
- 大盘系统性影响：大盘涨>2%时约55%股票涨；大盘跌>2%时约70%股票跌

## 决策规则（严格按此执行）

### 强信号规则（直接输出，不需要其他信号确认）
R1: 个股本周跌>5% 且 大盘也跌>2% → UP (0.65) [超跌+系统性下跌=强反弹]
R2: 个股本周跌>5% 且 大盘跌0~2% → UP (0.60) [超跌反弹]
R3: 个股本周涨>5% → DOWN (0.63) [均值回归]
R4: 个股本周涨>3% 且 60日高位>80% → DOWN (0.62) [高位回调]

### 中等信号规则（需要至少1个辅助信号确认）
R5: 个股本周跌3~5% 且 大盘跌>2% → UP (0.58)
R6: 个股本周跌3~5% 且 低位<20% → UP (0.57)
R7: 个股本周涨3~5% 且 连涨≥3天 → DOWN (0.58)
R8: 大盘涨>2% 但个股跌>2% → DOWN (0.57) [独立弱势]

### UNCERTAIN规则（必须严格遵守）
以下情况必须输出UNCERTAIN：
U1: 大盘震荡(-1%~1%) 且 个股震荡(-3%~3%) → UNCERTAIN
U2: 大盘小跌(-2%~-0.5%) 且 个股涨跌幅在-2%~2% → UNCERTAIN
U3: 没有命中上述任何R规则 → UNCERTAIN
U4: 看涨和看跌信号各有1个 → UNCERTAIN

## 关键纠偏
- 个股本周跌了 ≠ 下周继续跌！超跌反弹是A股最赚钱的策略
- 个股本周涨了 ≠ 下周继续涨！追涨是散户亏钱的主因
- 当你想预测DOWN时，先检查：个股是否已经跌了很多？如果跌>5%，应该预测UP
- 当你想预测UP时，先检查：个股是否已经涨了很多？如果涨>5%，应该预测DOWN"""


# ═══════════════════════════════════════════════════════════
# User Prompt 构建 — 动态注入实时数据
# ═══════════════════════════════════════════════════════════
#
# V5 重构原则：
# 1. 只提供有值的数据，不展示"无数据"（减少噪声）
# 2. 数据按重要性排序：大盘 > 个股行情 > 成交量 > 其他
# 3. 不添加任何方向性暗示或 emoji
# 4. 用简洁的数字说话，不做过多文字解读

def _build_user_prompt(code: str, stock_name: str, features: dict) -> str:
    """将多维特征打包为 DeepSeek 可理解的 User Prompt。"""
    parts = [f"分析 {stock_name}（{code}）下周涨跌方向。\n"]

    # ── 大盘环境 ──
    mkt_chg = features.get('market_chg', 0)
    parts.append(f"【大盘】本周: {mkt_chg:+.2f}%")

    mkt_prev = features.get('_market_prev_week_chg')
    if mkt_prev is not None:
        parts.append(f"  前一周: {mkt_prev:+.2f}%")

    # ── 个股行情 ──
    this_chg = features.get('this_week_chg', 0)
    parts.append(f"\n【个股】本周: {this_chg:+.2f}%")

    prev_chg = features.get('_prev_week_chg')
    if prev_chg is not None:
        parts.append(f"  前一周: {prev_chg:+.2f}%")

    # 相对强弱（关键信号）
    relative = this_chg - mkt_chg
    if abs(relative) > 1:
        label = "强于大盘" if relative > 0 else "弱于大盘"
        parts.append(f"  相对大盘: {relative:+.2f}%（{label}）")

    cu = features.get('consec_up', 0)
    cd = features.get('consec_down', 0)
    if cu >= 2:
        parts.append(f"  连涨{cu}天")
    elif cd >= 2:
        parts.append(f"  连跌{cd}天")

    last_day = features.get('last_day_chg', 0)
    if abs(last_day) > 1:
        parts.append(f"  最后一天: {last_day:+.2f}%")

    # 价格位置
    pos = features.get('_price_pos_60')
    if pos is not None:
        if pos < 0.2:
            parts.append(f"  60日位置: {pos:.0%}（低位）")
        elif pos > 0.8:
            parts.append(f"  60日位置: {pos:.0%}（高位）")
        # 中间位置不展示，减少噪声

    # ── 成交量（仅有数据时展示）──
    vr = features.get('vol_ratio')
    if vr is not None and abs(vr - 1.0) > 0.2:
        label = '放量' if vr > 1.2 else '缩量'
        parts.append(f"\n【量能】量比: {vr:.2f}（{label}）")

    # ── 资金面（仅有数据时展示）──
    ff = features.get('ff_signal')
    if ff is not None:
        label = '流入' if ff > 0 else '流出'
        parts.append(f"\n【资金】信号: {ff:+.3f}（{label}）")

    vpc = features.get('vol_price_corr')
    if vpc is not None and abs(vpc) > 0.2:
        label = '量价齐升' if vpc > 0.3 else '量价背离'
        parts.append(f"  量价关系: {vpc:.3f}（{label}）")

    # ── 板块（仅有数据时展示）──
    bm = features.get('board_momentum')
    cc = features.get('concept_consensus')
    boards = features.get('concept_boards')
    has_board = bm is not None or cc is not None or boards
    if has_board:
        parts.append("\n【板块】")
        if bm is not None:
            parts.append(f"  动量: {bm:+.3f}")
        if cc is not None:
            parts.append(f"  共识度: {cc:.1%}")
        if boards:
            parts.append(f"  概念: {boards}")

    # ── 基本面（仅有数据时展示）──
    fs = features.get('finance_score')
    rev = features.get('revenue_yoy')
    prof = features.get('profit_yoy')
    roe = features.get('roe')
    has_fin = any(v is not None for v in [fs, rev, prof, roe])
    if has_fin:
        parts.append("\n【基本面】")
        if rev is not None:
            parts.append(f"  营收增长: {rev:.1f}%")
        if prof is not None:
            parts.append(f"  利润增长: {prof:.1f}%")
        if roe is not None:
            parts.append(f"  ROE: {roe:.1f}%")

    parts.append("\n请输出JSON预测。")
    return '\n'.join(parts)


# ═══════════════════════════════════════════════════════════
# 核心预测函数
# ═══════════════════════════════════════════════════════════

async def predict_next_week_with_deepseek(
    code: str,
    stock_name: str,
    features: dict,
    timeout: float = 30.0,
) -> Optional[dict]:
    """调用 DeepSeek 预测单只股票下周方向。

    Args:
        code: 股票代码 (如 '600519.SH')
        stock_name: 股票名称
        features: 多维特征字典（来自 _nw_extract_features + 额外信号）
        timeout: 超时秒数

    Returns:
        {
            'direction': 'UP' | 'DOWN' | 'UNCERTAIN',
            'confidence': float (0~1),
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
                model="deepseek-chat",
                temperature=0.3,
                max_tokens=200,
            ),
            timeout=timeout,
        )

        content = resp.get('choices', [{}])[0].get('message', {}).get('content', '')
        if not content:
            logger.warning("DeepSeek 返回空内容: %s", code)
            return None

        result = parse_llm_json(content)

        # 验证输出格式
        direction = result.get('direction', '').upper()
        if direction not in ('UP', 'DOWN', 'UNCERTAIN'):
            logger.warning("DeepSeek 输出方向无效: %s → %s", code, direction)
            return None

        confidence = float(result.get('confidence', 0.5))
        confidence = max(0.0, min(1.0, confidence))

        justification = str(result.get('justification', ''))[:100]

        return {
            'direction': direction,
            'confidence': confidence,
            'justification': justification,
        }

    except asyncio.TimeoutError:
        logger.warning("DeepSeek 超时: %s (%.0fs)", code, timeout)
        return None
    except Exception as e:
        logger.warning("DeepSeek 预测失败: %s → %s", code, e)
        return None


# ═══════════════════════════════════════════════════════════
# 批量预测（带并发控制和速率限制）
# ═══════════════════════════════════════════════════════════

async def batch_predict_with_deepseek(
    stocks: list[dict],
    max_concurrency: int = 5,
    min_confidence: float = 0.55,
    progress_callback=None,
) -> dict[str, dict]:
    """批量调用 DeepSeek 预测多只股票。

    Args:
        stocks: [{'code': '600519.SH', 'name': '贵州茅台', 'features': {...}}, ...]
        max_concurrency: 最大并发数（控制API速率）
        min_confidence: 最低置信度过滤阈值
        progress_callback: 进度回调 (total, done)

    Returns:
        {code: {'direction': ..., 'confidence': ..., 'justification': ...}}
    """
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

            result = await predict_next_week_with_deepseek(
                code, name, features
            )

            done_count += 1
            if progress_callback and done_count % 20 == 0:
                progress_callback(total, done_count)

            if result and result['confidence'] >= min_confidence:
                results[code] = result
            elif result:
                logger.debug("DeepSeek 置信度过低: %s → %.2f < %.2f",
                             code, result['confidence'], min_confidence)

    tasks = [_predict_one(s) for s in stocks]
    await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("DeepSeek 批量预测完成: %d/%d 有效结果 (置信度≥%.0f%%)",
                len(results), total, min_confidence * 100)
    return results
