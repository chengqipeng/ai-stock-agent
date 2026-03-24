"""
V20 量价超跌反弹预测引擎
========================

基于全量A股回测验证的量价超跌反弹规则。

核心逻辑：
  当股票同时满足以下条件时，预测未来5个交易日上涨：
  1. 60日低位（pos ≤ 阈值）— 股价处于近60日低位区间
  2. 缩量（vr5 ≤ 0.8）— 近5日均量 / 20日均量 ≤ 0.8，卖压衰竭
  3. 偏离MA20（ma20d < -10）— 收盘价低于20日均线10%以上
  4. 连跌（cdn ≥ 2）— 连续下跌天数 ≥ 2

规则分级（按严格程度）：
  FINAL_A: pos≤0.25 — 最严格，准确率72.2%，样本4549
  FINAL_B: pos≤0.30 — 主力规则，准确率71.9%，样本5165（✅可靠）
  FINAL_C: pos≤0.33 — 最宽松，准确率71.5%，样本5540（✅可靠）
  FINAL_D: V3b多规则融合≥3 — 融合规则，准确率66.6%（✅可靠）

验证方法：
  - 5折交叉验证（最低折≥63%）
  - 置换检验 p=0.00（真实预测力）
  - 股票子集稳定性 std<0.7%
  - 月度胜率 83.3%

所有规则仅预测UP方向，持有期5个交易日。
"""
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class V20PredictionEngine:
    """
    V20 量价超跌反弹预测引擎。

    输入：日K线数据（至少60条，日期升序）
    输出：预测方向（仅UP）、置信度、命中规则、理由

    用法：
        engine = V20PredictionEngine()
        pred = engine.predict_single('000001', klines)
        results = engine.predict_batch({'000001': klines, ...})
    """

    def __init__(self):
        self.predictions: dict[str, dict] = {}

    # ═══════════════════════════════════════════════════════════
    # 特征计算
    # ═══════════════════════════════════════════════════════════

    @staticmethod
    def compute_features(klines: list[dict]) -> Optional[dict]:
        """
        从日K线序列计算量价特征。

        Args:
            klines: 日K线列表（日期升序），每条需包含:
                close, open, high, low, volume, turnover, change_percent

        Returns:
            特征字典 或 None（数据不足时）
        """
        n = len(klines)
        if n < 60:
            return None

        c = [k.get('close', 0) or 0 for k in klines]
        v = [k.get('volume', 0) or 0 for k in klines]
        p = [k.get('change_percent', 0) or 0 for k in klines]
        h = [k.get('high', 0) or 0 for k in klines]
        l = [k.get('low', 0) or 0 for k in klines]
        o = [k.get('open', 0) or 0 for k in klines]
        t = [k.get('turnover', 0) or 0 for k in klines]

        if c[-1] <= 0 or v[-1] <= 0:
            return None

        # 60日高低点 → 位置
        vh = [x for x in h[-60:] if x > 0]
        vl = [x for x in l[-60:] if x > 0]
        if not vh or not vl:
            return None
        h60, l60 = max(vh), min(vl)
        if h60 <= l60:
            return None
        pos = (c[-1] - l60) / (h60 - l60)

        # 均线
        ma5 = sum(c[-5:]) / 5
        ma20 = sum(c[-20:]) / 20

        # 量比
        vol_20 = sum(v[-20:]) / 20
        vol_5 = sum(v[-5:]) / 5
        if vol_20 <= 0:
            return None
        vr5 = vol_5 / vol_20

        # MA20偏离度
        ma20d = (c[-1] / ma20 - 1) * 100 if ma20 > 0 else 0

        # 连涨/连跌天数
        cup = cdn = 0
        for i in range(n - 1, max(n - 15, 0), -1):
            if p[i] > 0:
                if cdn == 0:
                    cup += 1
                else:
                    break
            elif p[i] < 0:
                if cup == 0:
                    cdn += 1
                else:
                    break
            else:
                break

        # 下影线计数（近5日）
        lsh = 0
        for i in range(-5, 0):
            body = abs(c[i] - o[i])
            if body > 0:
                lower = min(c[i], o[i]) - l[i] if l[i] > 0 else 0
                if lower > body * 1.5:
                    lsh += 1

        # 5日/20日收益率
        r5 = (c[-1] / c[-6] - 1) * 100 if n >= 6 and c[-6] > 0 else 0
        r20 = (c[-1] / c[-21] - 1) * 100 if n >= 21 and c[-21] > 0 else 0

        return {
            'pos': pos,
            'vr5': vr5,
            'ma20d': ma20d,
            'cdn': cdn,
            'cup': cup,
            'lsh': lsh,
            'r5': r5,
            'r20': r20,
        }

    # ═══════════════════════════════════════════════════════════
    # 规则匹配
    # ═══════════════════════════════════════════════════════════

    @staticmethod
    def _match_rules(feat: dict) -> list[dict]:
        """
        对特征匹配所有V20规则，返回命中的规则列表。

        Returns:
            命中规则列表，每项包含 name, confidence, desc, backtest_acc
        """
        matched = []

        pos = feat['pos']
        vr5 = feat['vr5']
        ma20d = feat['ma20d']
        cdn = feat['cdn']
        lsh = feat['lsh']
        r5 = feat['r5']

        # ── FINAL_A: 最严格（pos≤0.25）──
        if pos <= 0.25 and vr5 <= 0.8 and ma20d < -10 and cdn >= 2:
            matched.append({
                'name': 'FINAL_A',
                'confidence': 'high',
                'desc': '60日极低位(≤25%)+缩量+深度偏离MA20+连跌',
                'backtest_acc': 72.2,
                'h2_acc': 72.2,
            })

        # ── FINAL_B: 主力规则（pos≤0.30）──
        if pos <= 0.30 and vr5 <= 0.8 and ma20d < -10 and cdn >= 2:
            matched.append({
                'name': 'FINAL_B',
                'confidence': 'high',
                'desc': '60日低位(≤30%)+缩量+深度偏离MA20+连跌',
                'backtest_acc': 71.9,
                'h2_acc': 71.9,
            })

        # ── FINAL_C: 宽松（pos≤0.33）──
        if pos <= 0.33 and vr5 <= 0.8 and ma20d < -10 and cdn >= 2:
            matched.append({
                'name': 'FINAL_C',
                'confidence': 'medium',
                'desc': '60日低位(≤33%)+缩量+深度偏离MA20+连跌',
                'backtest_acc': 71.5,
                'h2_acc': 71.5,
            })

        # ── FINAL_D: V3b多规则融合（≥3条同时触发）──
        fusion_count = sum([
            pos <= 0.33 and vr5 <= 0.7 and ma20d < -8 and cdn >= 3,
            pos <= 0.33 and vr5 <= 0.7 and ma20d < -8,
            pos <= 0.33 and vr5 <= 0.7 and ma20d < -8 and lsh >= 1,
            pos <= 0.2 and vr5 <= 0.6 and r5 < -5 and ma20d < -8,
            pos <= 0.2 and vr5 <= 0.6 and r5 < -5 and cdn >= 3,
        ])
        if fusion_count >= 3:
            matched.append({
                'name': 'FINAL_D',
                'confidence': 'medium',
                'desc': f'V3b多规则融合({fusion_count}/5条触发)',
                'backtest_acc': 66.6,
                'h2_acc': 66.6,
            })

        return matched

    # ═══════════════════════════════════════════════════════════
    # 单股预测
    # ═══════════════════════════════════════════════════════════

    def predict_single(self, stock_code: str,
                       klines: list[dict]) -> Optional[dict]:
        """
        对单只股票生成V20预测。

        Args:
            stock_code: 股票代码
            klines: 日K线数据（日期升序），至少60条

        Returns:
            预测字典 或 None（不满足任何规则时）
        """
        if len(klines) < 60:
            return None

        feat = self.compute_features(klines)
        if feat is None:
            return None

        matched = self._match_rules(feat)
        if not matched:
            return None

        # 取最严格（最高准确率）的规则作为主规则
        best = max(matched, key=lambda r: r['backtest_acc'])

        # 置信度：命中FINAL_A或FINAL_B → high，否则 medium
        confidence = best['confidence']
        if len(matched) >= 3:
            # 多规则同时命中，提升置信度
            confidence = 'high'

        # 生成理由
        rule_names = [r['name'] for r in matched]
        reason_parts = [f"{best['name']}:{best['desc']}"]
        if len(matched) > 1:
            others = [r['name'] for r in matched if r['name'] != best['name']]
            reason_parts.append(f"同时命中:{','.join(others)}")

        result = {
            'stock_code': stock_code,
            'pred_direction': 'UP',
            'confidence': confidence,
            'rule_name': best['name'],
            'rule_desc': best['desc'],
            'backtest_acc': best['backtest_acc'],
            'matched_rules': rule_names,
            'matched_count': len(matched),
            'features': {
                'pos': round(feat['pos'], 4),
                'vr5': round(feat['vr5'], 4),
                'ma20d': round(feat['ma20d'], 2),
                'cdn': feat['cdn'],
            },
            'reason': ' | '.join(reason_parts),
            'holding_days': 5,
        }

        self.predictions[stock_code] = result
        return result

    # ═══════════════════════════════════════════════════════════
    # 批量预测
    # ═══════════════════════════════════════════════════════════

    def predict_batch(self, stock_klines: dict[str, list[dict]]) -> dict[str, dict]:
        """
        批量预测。

        Args:
            stock_klines: {stock_code: klines_list}

        Returns:
            {stock_code: prediction_dict}（仅包含有信号的股票）
        """
        results = {}
        for code, klines in stock_klines.items():
            pred = self.predict_single(code, klines)
            if pred:
                results[code] = pred

        self.predictions = results
        logger.info("V20批量预测完成: %d/%d只有信号", len(results), len(stock_klines))
        return results

    def get_signals(self, min_confidence: str = 'medium') -> list[dict]:
        """获取指定置信度以上的预测信号。"""
        conf_order = {'high': 3, 'medium': 2, 'low': 1}
        min_level = conf_order.get(min_confidence, 2)
        results = []
        for code, pred in self.predictions.items():
            if conf_order.get(pred['confidence'], 0) >= min_level:
                results.append(pred)
        results.sort(key=lambda x: x['backtest_acc'], reverse=True)
        return results
