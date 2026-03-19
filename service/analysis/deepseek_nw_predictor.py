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

_SYSTEM_PROMPT = """你是A股量化分析师。分析暴涨股下周是否回调。

## 输出格式
只输出JSON：{"direction":"DOWN/UNCERTAIN","confidence":0.50~0.65,"justification":"≤30字"}

## 背景
你收到的都是本周涨≥8%的暴涨股。统计上约64%会回调，但36%会继续涨。你的任务是区分这两类。

## 继续涨的特征（必须输出UNCERTAIN）
- 前一周也大涨(>3%)：连续暴涨说明强趋势，不会轻易回调
- 前一周涨幅>5%：两周连续大涨是主升浪特征
- 量比极高(>5.0)：超级放量说明有大资金持续介入
- 最后一天涨停(涨≥9.5%)：尾盘涨停说明买盘极强，次周大概率继续

## 回调的特征（可以输出DOWN）
- 前一周平稳或小跌(前周涨跌幅在-5%~3%)：突然暴涨缺乏持续性
- 量比适中(1.0~3.0)：正常放量而非疯狂抢筹
- 60日位置极高(>90%)：已在高位，暴涨是最后一波

## 决策流程
1. 先看前一周涨跌幅：前周>3%→大概率UNCERTAIN
2. 再看量比：>5.0→UNCERTAIN
3. 再看最后一天：涨停→UNCERTAIN
4. 以上都没命中，且前周<3%→DOWN (0.62)
5. 有任何犹豫→UNCERTAIN

## 绝对禁止
- 禁止输出UP
- 禁止给出>0.65的置信度"""


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
    else:
        parts.append(f"  前一周: 无数据")

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

    # 价格位置 — 始终展示（V16关键信号）
    pos = features.get('_price_pos_60')
    if pos is not None:
        if pos < 0.2:
            parts.append(f"  60日位置: {pos:.0%}（低位）")
        elif pos > 0.9:
            parts.append(f"  60日位置: {pos:.0%}（极高位）")
        elif pos > 0.7:
            parts.append(f"  60日位置: {pos:.0%}（高位）")
        else:
            parts.append(f"  60日位置: {pos:.0%}（中位）")

    # ── 成交量 — 始终展示（V16关键信号）──
    vr = features.get('vol_ratio')
    if vr is not None:
        if vr > 5.0:
            label = '超级放量'
        elif vr > 1.2:
            label = '放量'
        elif vr < 0.8:
            label = '缩量'
        else:
            label = '正常'
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
    # ── V15 严格预过滤：基于50股×16周回测优化 ──
    # 核心策略：只在"暴涨+高位+放量"时调用LLM，追求80%+准确率
    # D2规则(涨>=5%+连涨>=3天)在16周数据上0%准确率，已删除
    market_chg = features.get('market_chg', 0)
    this_chg = features.get('this_week_chg', 0)
    price_pos = features.get('_price_pos_60')
    vol_ratio = features.get('vol_ratio')

    # 硬性排除：涨幅<8%不调用LLM（D1规则要求涨>=8%）
    if this_chg < 8.0:
        return {
            'direction': 'UNCERTAIN', 'confidence': 0.0,
            'justification': f'预过滤:涨幅{this_chg:+.2f}%<8%',
        }

    # 硬性排除：60日价格位置<70%（低位暴涨往往是趋势启动，不会回调）
    if price_pos is None or price_pos < 0.7:
        return {
            'direction': 'UNCERTAIN', 'confidence': 0.0,
            'justification': f'预过滤:60日位{price_pos}不足0.7',
        }

    # 硬性排除：量比<1.0（缩量暴涨信号不可靠）
    if vol_ratio is None or vol_ratio < 1.0:
        return {
            'direction': 'UNCERTAIN', 'confidence': 0.0,
            'justification': f'预过滤:量比{vol_ratio}不足1.0',
        }

    # 硬性排除：大盘暴跌<-3%时不预测（崩盘周暴涨股往往继续涨）
    if market_chg < -3.0:
        return {
            'direction': 'UNCERTAIN', 'confidence': 0.0,
            'justification': f'预过滤:大盘暴跌{market_chg:+.1f}%',
        }

    # 通过预过滤：涨>=8% + 60日位>=70% + 量比>=1.0 + 大盘>-3%
    pass_reason = f'涨{this_chg:+.1f}%+pos{price_pos:.0%}+vol{vol_ratio:.1f}+mkt{market_chg:+.1f}%'
    logger.debug("预过滤通过: %s %s", code, pass_reason)

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
