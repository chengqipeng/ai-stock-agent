"""
验证个股概念板块强弱势评分 - 纯逻辑验证
"""
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _normalize_excess(excess, days):
    if days <= 0:
        return 0
    daily_avg = excess / days
    return 50 + daily_avg * 50


def _compound_return(daily_pcts):
    product = 1.0
    for p in daily_pcts:
        product *= (1 + p / 100)
    return (product - 1) * 100


def main():
    logger.info("=== 评分逻辑验证 ===")

    # 1. _normalize_excess
    assert abs(_normalize_excess(0, 10) - 50) < 0.01
    assert abs(_normalize_excess(10, 10) - 100) < 0.01
    assert abs(_normalize_excess(-10, 10) - 0) < 0.01
    assert abs(_normalize_excess(5, 10) - 75) < 0.01
    assert abs(_normalize_excess(-5, 10) - 25) < 0.01
    logger.info("✓ _normalize_excess")

    # 2. _compound_return
    ret = _compound_return([10, 10])
    assert abs(ret - 21) < 0.1
    assert abs(_compound_return([]) - 0) < 0.01
    logger.info("✓ _compound_return")

    # 3. 评分分布模拟
    import random
    random.seed(42)
    scores = []
    for _ in range(200):
        e5 = random.gauss(0, 3)
        e20 = random.gauss(0, 8)
        e60 = random.gauss(0, 15)
        raw = (
            _normalize_excess(e5, 5) * 0.40
            + _normalize_excess(e20, 20) * 0.35
            + _normalize_excess(e60, 60) * 0.25
        )
        scores.append(max(0.0, min(100.0, raw)))

    assert all(0 <= s <= 100 for s in scores)
    scores.sort(reverse=True)
    strong = sum(1 for s in scores if s >= 65)
    neutral = sum(1 for s in scores if 35 <= s < 65)
    weak = sum(1 for s in scores if s < 35)
    logger.info("✓ 分布: 强势=%d 中性=%d 弱势=%d (%.1f~%.1f, avg=%.1f)",
                strong, neutral, weak, min(scores), max(scores),
                sum(scores)/len(scores))

    logger.info("=== 全部通过 ✓ ===")


if __name__ == "__main__":
    main()
