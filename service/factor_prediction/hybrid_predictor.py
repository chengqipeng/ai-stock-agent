#!/usr/bin/env python3
"""
混合预测引擎 — V11规则引擎 + 多因子增强
==========================================
核心思想：
  V11规则引擎在特定场景下准确率很高（70-90%），但覆盖率有限（只覆盖部分股票）。
  多因子体系覆盖率100%但准确率只有52-53%。

  混合策略：
  1. V11规则能匹配的 → 用V11（高准确率）
  2. V11匹配不到的 → 用多因子（扩大覆盖率）
  3. V11 + 因子方向一致 → 提升置信度
  4. V11 + 因子方向矛盾 → 降低置信度

  这样既保留了V11的高准确率，又用因子填补了V11的覆盖盲区。

理论依据：
  - Ensemble方法（Breiman 1996）：不同模型的组合优于单一模型
  - 但关键是"有条件地组合"而非简单平均
  - V11是规则型（离散信号），因子是统计型（连续信号），互补性强
"""
import logging
from typing import Optional

from service.factor_prediction.factor_engine import (
    compute_price_volume_factors,
    compute_fundamental_factors,
    compute_alternative_factors,
)
from service.factor_prediction.prediction_system import (
    FactorPredictionEngine,
    FACTOR_DIRECTIONS,
)

logger = logging.getLogger(__name__)


class HybridPredictor:
    """
    混合预测器：V11规则 + 多因子增强。

    使用方式：
      1. 先用V11规则引擎匹配
      2. 再用多因子计算综合评分
      3. 根据两者的一致性调整最终预测
    """

    # V11规则和因子的一致性对置信度的影响
    CONFIDENCE_UPGRADE = {
        ('high', True): 'high',       # 高置信+因子一致 → 保持高
        ('high', False): 'medium',    # 高置信+因子矛盾 → 降为中
        ('medium', True): 'high',     # 中置信+因子一致 → 升为高
        ('medium', False): 'low',     # 中置信+因子矛盾 → 降为低
        ('low', True): 'medium',      # 低置信+因子一致 → 升为中
        ('low', False): 'low',        # 低置信+因子矛盾 → 保持低
    }

    def __init__(self):
        self.factor_engine = FactorPredictionEngine()

    def predict(self, code: str,
                v11_result: dict = None,
                klines: list[dict] = None,
                finance_records: list[dict] = None,
                fund_flow: list[dict] = None,
                concept_strength: list[dict] = None,
                market_klines: list[dict] = None) -> dict:
        """
        对单只股票生成混合预测。

        Args:
            code: 股票代码
            v11_result: V11规则引擎的预测结果，包含:
                        {'pred_up': bool, 'confidence': str, 'rule_name': str}
                        如果为None表示V11未匹配到规则
            klines: K线数据（升序）
            其他参数: 同 FactorPredictionEngine.add_stock_factors

        Returns:
            {
                'pred_direction': 'UP'/'DOWN',
                'confidence': 'high'/'medium'/'low',
                'source': 'v11'/'factor'/'hybrid',
                'v11_rule': str or None,
                'factor_score': float or None,
                'factor_direction': 'UP'/'DOWN' or None,
                'agreement': bool,  # V11和因子是否一致
            }
        """
        # 计算因子评分
        factor_result = None
        if klines and len(klines) >= 60:
            factors = compute_price_volume_factors(klines)
            if finance_records:
                factors.update(compute_fundamental_factors(finance_records))
            alt = compute_alternative_factors(
                fund_flow=fund_flow or [],
                concept_strength=concept_strength,
                market_klines=market_klines,
                stock_klines=klines,
            )
            factors.update(alt)

            if factors:
                factor_result = {
                    'raw_factors': factors,
                    'factor_score': None,
                    'factor_direction': None,
                }

        # 场景1: V11有匹配 + 有因子
        if v11_result and factor_result:
            v11_up = v11_result.get('pred_up', True)
            v11_conf = v11_result.get('confidence', 'medium')

            # 因子方向判断（简化版：用关键强因子投票）
            factor_vote = self._quick_factor_vote(factor_result['raw_factors'])
            factor_up = factor_vote > 0

            agreement = (v11_up == factor_up)
            new_conf = self.CONFIDENCE_UPGRADE.get((v11_conf, agreement), v11_conf)

            return {
                'pred_direction': 'UP' if v11_up else 'DOWN',
                'confidence': new_conf,
                'source': 'hybrid',
                'v11_rule': v11_result.get('rule_name'),
                'factor_score': factor_vote,
                'factor_direction': 'UP' if factor_up else 'DOWN',
                'agreement': agreement,
            }

        # 场景2: V11有匹配，无因子
        if v11_result:
            return {
                'pred_direction': 'UP' if v11_result.get('pred_up') else 'DOWN',
                'confidence': v11_result.get('confidence', 'medium'),
                'source': 'v11',
                'v11_rule': v11_result.get('rule_name'),
                'factor_score': None,
                'factor_direction': None,
                'agreement': None,
            }

        # 场景3: V11无匹配，用因子兜底
        if factor_result:
            factor_vote = self._quick_factor_vote(factor_result['raw_factors'])
            # 纯因子预测置信度较低
            conf = 'medium' if abs(factor_vote) > 3 else 'low'
            return {
                'pred_direction': 'UP' if factor_vote > 0 else 'DOWN',
                'confidence': conf,
                'source': 'factor',
                'v11_rule': None,
                'factor_score': factor_vote,
                'factor_direction': 'UP' if factor_vote > 0 else 'DOWN',
                'agreement': None,
            }

        # 场景4: 都没有
        return {
            'pred_direction': 'UP',
            'confidence': 'low',
            'source': 'none',
            'v11_rule': None,
            'factor_score': None,
            'factor_direction': None,
            'agreement': None,
        }

    def _quick_factor_vote(self, factors: dict) -> float:
        """
        快速因子投票：用4个显著因子做方向判断。

        只用IC显著的因子（|IR|>0.3），避免噪声因子干扰：
        - volatility_20d (IR=-0.30): 低波动看涨
        - amplitude_5d (IR=-0.33): 低振幅看涨
        - abnormal_turnover (IR=-0.32): 低异常换手看涨
        - upper_shadow (IR=+0.34): 上影线大看涨（A股特色）
        - revenue_yoy (IR=+0.30): 营收增长看涨

        返回投票分（正=看涨，负=看跌），范围约-5~+5
        """
        vote = 0.0

        # 低波动看涨（IR=-0.30）
        vol = factors.get('volatility_20d')
        if vol is not None:
            if vol < 2.0:
                vote += 1.0
            elif vol > 4.0:
                vote -= 1.0

        # 低振幅看涨（IR=-0.33）
        amp = factors.get('amplitude_5d')
        if amp is not None:
            if amp < 3.0:
                vote += 1.0
            elif amp > 6.0:
                vote -= 1.0

        # 低异常换手看涨（IR=-0.32）
        at = factors.get('abnormal_turnover')
        if at is not None:
            if at < 0.7:
                vote += 1.0
            elif at > 1.5:
                vote -= 1.0

        # 上影线看涨（IR=+0.34，A股特色）
        us = factors.get('upper_shadow')
        if us is not None:
            if us > 0.5:
                vote += 1.0
            elif us < 0.1:
                vote -= 0.5

        # 营收增长看涨（IR=+0.30）
        rev = factors.get('revenue_yoy')
        if rev is not None:
            if rev > 20:
                vote += 1.0
            elif rev < -10:
                vote -= 1.0

        return vote
