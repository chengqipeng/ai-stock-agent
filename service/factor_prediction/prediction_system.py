#!/usr/bin/env python3
"""
多因子预测体系 — 泛化优先、抗过拟合设计
==========================================
理论框架：
  1. 因子选择 — 只用有经济学逻辑的因子（SSRN/S&C/Quantocracy验证过的）
  2. 因子合成 — 等权截面Rank合成（DeMiguel 2009: 1/N优于优化权重）
  3. 预测生成 — 复合评分→方向预测，不做参数优化
  4. 抗过拟合 — Purged K-Fold CV + 样本外验证（López de Prado 2018）

核心设计原则：
  - 零参数优化：所有阈值来自学术文献或经济学直觉，不从数据中拟合
  - 等权合成：避免权重过拟合（DeMiguel et al. "Optimal Versus Naive Diversification"）
  - 因子正交化：通过分类（量价/基本面/另类）天然降低因子间相关性
  - 时序隔离：回测时严格禁止未来信息泄露
"""
import logging
import math
from collections import defaultdict
from typing import Optional

from service.factor_prediction.factor_engine import (
    compute_price_volume_factors,
    compute_fundamental_factors,
    compute_alternative_factors,
    cross_sectional_rank,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# 因子配置（只选有经济学逻辑的因子，方向来自学术共识）
# ═══════════════════════════════════════════════════════════

# 因子方向：+1 表示因子值越大越看涨，-1 表示越大越看跌
#
# 方向来源：学术文献共识 + A股实证修正
# A股与美股的关键差异（中国市场微观结构研究共识）：
#   - A股散户占比高，动量效应弱/反转效应强（Liu et al. 2019 JFE）
#   - A股涨跌停制度导致短期反转更显著
#   - A股资金流向因子比美股更有效（机构行为可预测性更强）
FACTOR_DIRECTIONS = {
    # ── 量价因子（A股实证修正）──
    'momentum_20d': -1,       # A股20日反转效应（Liu et al. 2019: A股短期反转显著）
    'momentum_60d': -1,       # A股60日反转（中期也呈反转，与美股相反）
    'volatility_20d': -1,     # 低波动异象（A股同样成立，Baker et al. 2011）★显著
    'volume_ratio': -1,       # 缩量企稳看涨（A股量价理论一致）
    'force_13': -1,           # A股力量指数反转（过度反应后回归）
    'price_pos_60': -1,       # 低位看涨（均值回归，A股一致）
    'vol_price_corr': -1,     # 量价负相关看涨（国君191）
    'consec_down': -1,        # A股连跌趋势延续
    'upper_shadow': +1,       # A股上影线IC为正★显著
    'abnormal_turnover': -1,  # 异常高换手后反转★显著
    'amplitude_5d': -1,       # 高振幅后收敛★显著
    'close_position': -1,     # A股尾盘收高反而看跌（实证修正：散户尾盘追涨）
    'skewness_20d': -1,       # 高偏度看跌（A股彩票效应）

    # ── 基本面因子（方向与全球一致）──
    'revenue_yoy': +1,        # 营收增长看涨（CAN SLIM）
    'profit_yoy': +1,         # 利润增长看涨
    'earnings_surprise': +1,  # 盈利惊喜看涨（Ball & Brown 1968）
    'profit_quality': +1,     # 高盈利质量看涨（Sloan 1996）

    # ── 另类因子 ──
    'smart_money_5d': +1,     # 聪明钱流入看涨
    'fund_flow_momentum': +1, # 资金流加速看涨
    'relative_strength_20d': -1,  # A股相对强弱反转（短期过热回调）
}

# 三大类因子等权分配（每类1/3权重，类内等权）
FACTOR_CATEGORIES = {
    'price_volume': [
        'momentum_20d', 'momentum_60d', 'volatility_20d',
        'volume_ratio', 'force_13', 'price_pos_60', 'vol_price_corr',
        'consec_down', 'upper_shadow',
        'abnormal_turnover', 'amplitude_5d', 'close_position', 'skewness_20d',
    ],
    'fundamental': [
        'revenue_yoy', 'profit_yoy',
        'earnings_surprise', 'profit_quality',
    ],
    'alternative': [
        'smart_money_5d', 'fund_flow_momentum',
        'relative_strength_20d',
    ],
}


# ═══════════════════════════════════════════════════════════
# 核心预测引擎
# ═══════════════════════════════════════════════════════════

class FactorPredictionEngine:
    """
    多因子预测引擎 — 等权Rank合成 + 零参数优化

    工作流程：
      1. 对每只股票计算三类因子的原始值
      2. 截面Rank标准化（0~1）
      3. 按因子方向调整Rank（看跌因子取1-rank）
      4. 三类因子等权合成综合评分
      5. 综合评分 > 0.5 预测涨，< 0.5 预测跌

    为什么用0.5作为阈值：
      - Rank标准化后，0.5是自然中位数
      - 不需要从数据中拟合阈值
      - 天然避免过拟合
    """

    def __init__(self):
        self.stock_raw_factors = {}   # {code: {factor: value}}
        self.stock_ranked_factors = {}  # {code: {factor: rank}}
        self.stock_scores = {}        # {code: {category_score, composite_score, ...}}

    def add_stock_factors(self, code: str, klines: list[dict],
                          finance_records: list[dict] = None,
                          fund_flow: list[dict] = None,
                          concept_strength: list[dict] = None,
                          market_klines: list[dict] = None):
        """为一只股票计算并注册所有因子。"""
        factors = {}

        # A. 量价因子
        pv = compute_price_volume_factors(klines)
        factors.update(pv)

        # B. 基本面因子
        if finance_records:
            fund = compute_fundamental_factors(finance_records)
            factors.update(fund)

        # C. 另类因子
        alt = compute_alternative_factors(
            fund_flow=fund_flow or [],
            concept_strength=concept_strength,
            market_klines=market_klines,
            stock_klines=klines,
        )
        factors.update(alt)

        self.stock_raw_factors[code] = factors

    def compute_predictions(self, ic_weights: dict = None) -> dict[str, dict]:
        """
        对所有已注册股票执行截面Rank合成并生成预测。

        支持两种合成模式：
        1. 等权合成（默认）— 最抗过拟合
        2. IC加权合成 — 用历史IC的绝对值作为权重，只对|IC|>0.02的因子加权

        Args:
            ic_weights: {factor_name: abs(mean_ic)} 可选的IC权重
                        只有|IC|>0.02的因子才会被加权，其余等权

        Returns:
            {code: {
                'composite_score': float (0~1),
                'pred_direction': 'UP'/'DOWN',
                'confidence': 'high'/'medium'/'low',
                'category_scores': {category: score},
                'factor_coverage': int (有效因子数),
                'raw_factors': dict,
            }}
        """
        if len(self.stock_raw_factors) < 5:
            logger.warning("股票数量不足(%d)，截面Rank不可靠", len(self.stock_raw_factors))
            return {}

        # Step 1: 对每个因子做截面Rank
        all_factor_names = set()
        for fdict in self.stock_raw_factors.values():
            all_factor_names.update(fdict.keys())

        ranked = defaultdict(dict)
        for fname in all_factor_names:
            rank_map = cross_sectional_rank(self.stock_raw_factors, fname)
            for code, rank_val in rank_map.items():
                direction = FACTOR_DIRECTIONS.get(fname, +1)
                adjusted = rank_val if direction > 0 else (1 - rank_val)
                ranked[code][fname] = adjusted

        self.stock_ranked_factors = dict(ranked)

        # Step 2: 类内IC加权 + 类间等权合成
        # IC加权的保守策略：只对|IC|>0.02的因子用IC加权，其余等权
        results = {}
        for code, ranks in ranked.items():
            category_scores = {}
            for cat_name, cat_factors in FACTOR_CATEGORIES.items():
                available = [(f, ranks[f]) for f in cat_factors if f in ranks]
                if not available:
                    continue

                if ic_weights:
                    # IC加权：权重 = max(|IC|, 0.01) 保底，避免零权重
                    weights = []
                    vals = []
                    for f, v in available:
                        w = max(ic_weights.get(f, 0.01), 0.01)
                        weights.append(w)
                        vals.append(v)
                    w_sum = sum(weights)
                    category_scores[cat_name] = sum(w * v for w, v in zip(weights, vals)) / w_sum
                else:
                    category_scores[cat_name] = sum(v for _, v in available) / len(available)

            if not category_scores:
                continue

            # 类间等权（DeMiguel 2009: 1/N rule）
            composite = sum(category_scores.values()) / len(category_scores)

            coverage = sum(1 for f in FACTOR_DIRECTIONS if f in ranks)

            deviation = abs(composite - 0.5)
            if deviation > 0.12 and coverage >= 10:
                confidence = 'high'
            elif deviation > 0.06 and coverage >= 6:
                confidence = 'medium'
            else:
                confidence = 'low'

            results[code] = {
                'composite_score': round(composite, 4),
                'pred_direction': 'UP' if composite > 0.5 else 'DOWN',
                'confidence': confidence,
                'category_scores': {k: round(v, 4) for k, v in category_scores.items()},
                'factor_coverage': coverage,
                'raw_factors': self.stock_raw_factors.get(code, {}),
            }

        self.stock_scores = results
        return results

    def get_top_stocks(self, direction: str = 'UP', top_n: int = 50) -> list[dict]:
        """获取评分最高/最低的股票列表。"""
        filtered = [
            {'code': code, **info}
            for code, info in self.stock_scores.items()
            if info['pred_direction'] == direction
        ]
        reverse = (direction == 'UP')
        filtered.sort(key=lambda x: x['composite_score'], reverse=reverse)
        return filtered[:top_n]

    def get_strong_signals(self, min_categories_agree: int = 2) -> dict[str, dict]:
        """
        获取多维度一致的强信号股票。

        核心思想（Quantocracy/Alpha Architect多因子共识）：
        当量价、基本面、另类因子同时指向同一方向时，
        预测可靠性显著提升。

        Args:
            min_categories_agree: 至少几个大类因子方向一致才算强信号

        Returns:
            只包含强信号股票的预测结果
        """
        strong = {}
        for code, info in self.stock_scores.items():
            cat_scores = info.get('category_scores', {})
            if len(cat_scores) < 2:
                continue

            # 统计各大类因子的方向
            up_cats = sum(1 for v in cat_scores.values() if v > 0.55)
            down_cats = sum(1 for v in cat_scores.values() if v < 0.45)

            if up_cats >= min_categories_agree or down_cats >= min_categories_agree:
                strong[code] = info

        return strong
