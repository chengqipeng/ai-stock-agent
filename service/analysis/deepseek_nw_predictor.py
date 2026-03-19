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

_SYSTEM_PROMPT = """你是A股量化分析引擎。基于市场数据预测个股下周涨跌方向。

## 输出规则
1. 只输出JSON
2. direction: "UP"/"DOWN"/"UNCERTAIN"
3. confidence: 0.50~0.75
4. justification: 中文40字以内

## 核心统计事实（必须牢记）
- A股周度上涨概率约40-45%，下跌概率55-60%，天然偏空
- 单周大涨(>5%)的股票，下周下跌概率约65%（均值回归）
- 单周大跌(<-5%)的股票，下周上涨概率约55%（超跌反弹）
- 大盘涨>2%时，多数个股已透支涨幅，下周回调概率高
- 大盘跌>2%时，超跌个股下周反弹概率较高

## 决策框架

### 看涨信号（每个+1分）
1. 本周跌>5%且大盘跌>2%（系统性超跌反弹）
2. 连续两周下跌且本周跌幅收窄（企稳信号）
3. 60日低位(<0.2)且本周跌幅>3%（深度超跌）
4. 大盘跌>2%但个股跌幅<大盘（相对抗跌）
5. 连跌4天+最后一天收涨（短期见底）

### 看跌信号（每个+1分）
1. 本周涨>3%且大盘涨>1%（涨幅透支，均值回归）
2. 连续两周上涨且涨幅加速（过热回调）
3. 高位(>0.7)且本周涨>2%（高位追涨风险）
4. 大盘涨>2%但个股涨幅>大盘+3%（过度乐观）
5. 大盘跌但个股逆势大涨>5%（独立过热，回调概率高）

### 决策规则
- 看涨分>=2且看跌分=0 → UP
- 看跌分>=2且看涨分=0 → DOWN
- 看涨分=1且看跌分=0 → UP（低置信0.55）
- 看跌分=1且看涨分=0 → DOWN（低置信0.55）
- 信号矛盾或无信号 → UNCERTAIN

### 置信度
- 2个信号一致: 0.60
- 3个信号一致: 0.65
- 4个以上: 0.70
- 信号矛盾或只有1个: 0.55以下

## 重要提醒
- 默认偏空：无明确看涨信号时，倾向DOWN或UNCERTAIN
- 大涨后看跌：本周涨>5%的股票，除非有极强理由，否则预测DOWN
- 大跌后看涨：本周跌>5%且大盘也跌的股票，倾向UP
- 不确定就UNCERTAIN，宁可错过也不要错判

```json
{"direction":"DOWN","confidence":0.62,"justification":"本周涨幅过大+高位+均值回归"}
```"""


# ═══════════════════════════════════════════════════════════
# User Prompt 构建 — 动态注入实时数据
# ═══════════════════════════════════════════════════════════

def _build_user_prompt(code: str, stock_name: str, features: dict) -> str:
    """将多维特征打包为 DeepSeek 可理解的 User Prompt。"""
    parts = [
        f"请分析 {stock_name}（{code}）下周的涨跌方向。\n",
        "## 大盘环境（最重要）",
        f"- 大盘本周涨跌幅: {features.get('market_chg', 0):+.2f}%",
    ]

    # 大盘前一周
    mkt_prev = features.get('_market_prev_week_chg')
    if mkt_prev is not None:
        parts.append(f"- 大盘前一周涨跌幅: {mkt_prev:+.2f}%")
        mkt_this = features.get('market_chg', 0)
        if mkt_prev < -1 and mkt_this < -1:
            parts.append("- 大盘环境: 连续两周下跌，注意超跌反弹可能")
        elif mkt_prev > 1 and mkt_this > 1:
            parts.append("- 大盘环境: 连续两周上涨，注意均值回归风险")
        elif (mkt_prev < -1 and mkt_this > 0.5) or (mkt_prev > 1 and mkt_this < -0.5):
            parts.append("- 大盘环境: 可能处于拐点，方向不明确")
        else:
            parts.append("- 大盘环境: 震荡")

    parts.extend([
        "\n## 个股本周行情",
        f"- 本周涨跌幅: {features.get('this_week_chg', 0):+.2f}%",
        f"- 连涨天数: {features.get('consec_up', 0)}",
        f"- 连跌天数: {features.get('consec_down', 0)}",
        f"- 最后一天涨跌: {features.get('last_day_chg', 0):+.2f}%",
        f"- 市场类型: {features.get('_market_suffix', '未知')}",
    ])

    # 价格位置
    pos = features.get('_price_pos_60')
    if pos is not None:
        pos_label = '低位' if pos < 0.2 else ('偏低' if pos < 0.4 else ('中位' if pos < 0.6 else ('偏高' if pos < 0.8 else '高位')))
        parts.append(f"- 60日价格位置: {pos:.2f} ({pos_label})")

    # 前一周
    prev = features.get('_prev_week_chg')
    if prev is not None:
        parts.append(f"- 前一周涨跌幅: {prev:+.2f}%")
        # 两周趋势信号（中性描述，不暗示方向）
        this_chg = features.get('this_week_chg', 0)
        if prev < -1 and this_chg < -1:
            parts.append(f"- 两周趋势: 连续两周下跌（前周{prev:+.1f}%，本周{this_chg:+.1f}%），注意超跌反弹可能")
        elif prev > 1 and this_chg > 1:
            parts.append(f"- 两周趋势: 连续两周上涨（前周{prev:+.1f}%，本周{this_chg:+.1f}%），注意均值回归风险")
        elif prev < -8:
            parts.append(f"- 前周大跌{prev:+.1f}%，关注是否企稳")

    # 资金流向
    parts.append("\n## 资金面数据")
    ff = features.get('ff_signal')
    if ff is not None:
        ff_label = '强流入' if ff > 0.5 else ('流入' if ff > 0 else ('流出' if ff > -0.5 else '强流出'))
        parts.append(f"- 资金流向信号: {ff:+.3f} ({ff_label})")
    else:
        parts.append("- 资金流向信号: 无数据")

    vr = features.get('vol_ratio')
    if vr is not None:
        vr_label = '显著放量' if vr > 1.5 else ('放量' if vr > 1.2 else ('正常' if vr > 0.8 else '缩量'))
        parts.append(f"- 成交量比率: {vr:.2f} ({vr_label})")
    else:
        parts.append("- 成交量比率: 无数据")

    vpc = features.get('vol_price_corr')
    if vpc is not None:
        vpc_label = '量价齐升' if vpc > 0.3 else ('弱正相关' if vpc > 0 else ('量价背离' if vpc < -0.3 else '弱负相关'))
        parts.append(f"- 量价相关性: {vpc:.3f} ({vpc_label})")

    # 板块信号
    parts.append("\n## 板块面数据")
    bm = features.get('board_momentum')
    if bm is not None:
        parts.append(f"- 板块动量: {bm:+.3f}")
    cc = features.get('concept_consensus')
    if cc is not None:
        parts.append(f"- 板块共识度: {cc:.1%}")
    boards = features.get('concept_boards')
    if boards:
        parts.append(f"- 所属概念: {boards}")

    # 财报信号
    parts.append("\n## 基本面数据")
    fs = features.get('finance_score')
    if fs is not None:
        parts.append(f"- 财报综合评分: {fs:.3f}")
    rev = features.get('revenue_yoy')
    if rev is not None:
        parts.append(f"- 营收同比增长: {rev:.1f}%")
    prof = features.get('profit_yoy')
    if prof is not None:
        parts.append(f"- 利润同比增长: {prof:.1f}%")
    roe = features.get('roe')
    if roe is not None:
        parts.append(f"- ROE: {roe:.1f}%")

    parts.append("\n请基于以上数据，输出JSON格式的预测结果。记住：A股周度上涨概率仅40-45%，大涨后倾向回调，无明确信号时偏向DOWN或UNCERTAIN。")
    return '\n'.join(parts)


# ═══════════════════════════════════════════════════════════
# 后校准层 — 基于统计偏差修正 LLM 输出
# ═══════════════════════════════════════════════════════════

def _calibrate_prediction(result: dict, features: dict) -> dict:
    """基于已知统计偏差对 LLM 预测进行后校准。

    核心修正逻辑：
    1. 本周大涨(>5%)但LLM预测UP → 翻转为DOWN（均值回归）
    2. 低置信UP预测 → 降级为UNCERTAIN（消除看涨偏差）
    3. 本周大跌(<-5%)且大盘也跌但LLM预测DOWN → 翻转为UP（超跌反弹）
    """
    direction = result['direction']
    confidence = result['confidence']
    justification = result['justification']

    this_chg = features.get('this_week_chg', 0)
    market_chg = features.get('market_chg', 0)
    price_pos = features.get('_price_pos_60')

    # 规则1: 本周大涨>5%，LLM预测UP → 翻转DOWN（均值回归，历史65%下跌）
    if direction == 'UP' and this_chg > 5:
        return {
            'direction': 'DOWN',
            'confidence': 0.62,
            'justification': f'校准:周涨{this_chg:+.1f}%均值回归',
        }

    # 规则2: 本周涨>3%且高位(>0.7)，LLM预测UP → 翻转DOWN
    if direction == 'UP' and this_chg > 3 and price_pos is not None and price_pos > 0.7:
        return {
            'direction': 'DOWN',
            'confidence': 0.60,
            'justification': f'校准:高位{price_pos:.1f}+周涨{this_chg:+.1f}%回调',
        }

    # 规则3: 低置信UP(<=0.55) → 降级UNCERTAIN（消除看涨偏差，但不过度）
    if direction == 'UP' and confidence <= 0.55:
        return {
            'direction': 'UNCERTAIN',
            'confidence': confidence,
            'justification': justification,
        }

    # 规则4: 本周大跌<-5%且大盘也跌>-1%，LLM预测DOWN → 翻转UP（超跌反弹）
    if direction == 'DOWN' and this_chg < -5 and market_chg < -1:
        return {
            'direction': 'UP',
            'confidence': 0.60,
            'justification': f'校准:系统性超跌{this_chg:+.1f}%反弹',
        }

    # 规则4b: 连续两周下跌且本周跌>3%，LLM预测DOWN → 降级UNCERTAIN（可能超跌反弹）
    prev_chg = features.get('_prev_week_chg')
    if (direction == 'DOWN' and prev_chg is not None
            and prev_chg < -1 and this_chg < -3):
        return {
            'direction': 'UNCERTAIN',
            'confidence': confidence * 0.9,
            'justification': f'校准:连跌可能反弹',
        }

    # 规则5: 大盘涨>2%且个股涨>3%，LLM预测UP → 降级UNCERTAIN（涨幅透支）
    if direction == 'UP' and market_chg > 2 and this_chg > 3:
        return {
            'direction': 'UNCERTAIN',
            'confidence': confidence * 0.9,
            'justification': f'校准:大盘个股同涨透支',
        }

    return result


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
            'direction': 'UP' | 'DOWN',
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
                temperature=0.15,  # 极低温度 → 更确定性的输出
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

        raw_result = {
            'direction': direction,
            'confidence': confidence,
            'justification': justification,
        }

        # 后校准：基于已知统计偏差修正
        return _calibrate_prediction(raw_result, features)

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
