#!/usr/bin/env python3
"""
DeepSeek 多维特征分析：找出高准确率的特征组合
=============================================
目标：不限于大盘涨跌幅，找到所有能让准确率≥75%的特征组合。

方法：
1. 去掉预过滤，跑完整回测（复用 four_way 的基础设施）
2. 对每条 DeepSeek DOWN 预测，记录完整特征
3. 按单维度、双维度、三维度交叉分析准确率
4. 输出所有准确率≥75%且样本≥10的特征组合

用法：
    .venv/bin/python tools/deepseek_multidim_analysis.py
"""
import asyncio
import json
import logging
import random
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from itertools import combinations

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

_log_path = Path(__file__).parent.parent / 'data_results' / 'deepseek_multidim_analysis_log.txt'
_fh = logging.FileHandler(str(_log_path), mode='w', encoding='utf-8')
_fh.setLevel(logging.INFO)
_fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
logging.getLogger().addHandler(_fh)


# 复用 four_way 的基础函数
from day_week_predicted.backtest.four_way_200stocks_backtest import (
    _select_200_stocks, _load_kline_data, _extract_features,
    _check_next_week_actual, _next_iso_week,
)


def _bin_feature(name: str, value) -> str | None:
    """将连续特征离散化为分箱标签。返回 None 表示该特征无数据。"""
    if value is None:
        return None

    if name == 'market_chg':
        if value < -2: return '大盘大跌(<-2%)'
        if value < -0.5: return '大盘小跌(-2~-0.5%)'
        if value <= 0.5: return '大盘震荡(±0.5%)'
        if value <= 2: return '大盘小涨(0.5~2%)'
        return '大盘大涨(>2%)'

    if name == 'this_week_chg':
        if value < 0: return '个股下跌(<0%)'
        if value < 3: return '个股微涨(0~3%)'
        if value < 5: return '个股小涨(3~5%)'
        if value < 8: return '个股中涨(5~8%)'
        return '个股暴涨(≥8%)'

    if name == 'consec_up':
        if value >= 4: return '连涨≥4天'
        if value >= 3: return '连涨3天'
        if value >= 2: return '连涨2天'
        return '连涨<2天'

    if name == 'consec_down':
        if value >= 3: return '连跌≥3天'
        if value >= 2: return '连跌2天'
        return '连跌<2天'

    if name == 'vol_ratio':
        if value is None: return None
        if value > 1.5: return '大幅放量(>1.5)'
        if value > 1.2: return '放量(1.2~1.5)'
        if value >= 0.8: return '正常量(0.8~1.2)'
        return '缩量(<0.8)'

    if name == 'price_pos_60':
        if value is None: return None
        if value > 0.8: return '60日高位(>80%)'
        if value > 0.5: return '60日中高(50~80%)'
        if value > 0.2: return '60日中低(20~50%)'
        return '60日低位(<20%)'

    if name == 'last_day_chg':
        if value > 3: return '尾日大涨(>3%)'
        if value > 0: return '尾日小涨(0~3%)'
        if value > -3: return '尾日小跌(-3~0%)'
        return '尾日大跌(<-3%)'

    if name == 'prev_week_chg':
        if value is None: return None
        if value < -3: return '前周大跌(<-3%)'
        if value < 0: return '前周小跌(-3~0%)'
        if value <= 3: return '前周小涨(0~3%)'
        return '前周大涨(>3%)'

    if name == 'relative_strength':
        # this_week_chg - market_chg
        if value > 5: return '强于大盘>5%'
        if value > 2: return '强于大盘2~5%'
        if value > 0: return '略强于大盘'
        return '弱于大盘'

    if name == 'suffix':
        return f'市场_{value}'

    if name == 'market_prev_chg':
        if value is None: return None
        if value < -1: return '前周大盘跌(>1%)'
        if value <= 1: return '前周大盘震荡(±1%)'
        return '前周大盘涨(>1%)'

    return None



def _extract_all_bins(features: dict) -> dict[str, str]:
    """从特征字典中提取所有离散化维度。"""
    bins = {}
    mapping = {
        'market_chg': features.get('market_chg', 0),
        'this_week_chg': features.get('this_week_chg', 0),
        'consec_up': features.get('consec_up', 0),
        'consec_down': features.get('consec_down', 0),
        'vol_ratio': features.get('vol_ratio'),
        'price_pos_60': features.get('_price_pos_60'),
        'last_day_chg': features.get('last_day_chg', 0),
        'prev_week_chg': features.get('_prev_week_chg'),
        'suffix': features.get('_market_suffix', 'SH'),
        'market_prev_chg': features.get('_market_prev_week_chg'),
    }
    # 相对强弱
    mapping['relative_strength'] = features.get('this_week_chg', 0) - features.get('market_chg', 0)

    for name, value in mapping.items():
        b = _bin_feature(name, value)
        if b is not None:
            bins[name] = b
    return bins


async def main():
    t0 = time.time()
    logger.info("=" * 80)
    logger.info("  DeepSeek 多维特征分析（无预过滤）")
    logger.info("  目标: 找出准确率≥75%的特征组合")
    logger.info("=" * 80)

    # 1. 选股 & 加载K线（复用 four_way 的逻辑）
    stock_list = _select_200_stocks()
    klines, latest_date = _load_kline_data(stock_list)
    if not klines:
        return

    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    current_iso = dt_latest.isocalendar()

    test_weeks = []
    for offset in range(2, 10):
        y, w = current_iso[0], current_iso[1] - offset
        while w <= 0:
            y -= 1
            dec28 = datetime(y, 12, 28)
            max_w = dec28.isocalendar()[1]
            w += max_w
        test_weeks.append((y, w))

    logger.info("回测范围: %d周, 股票: %d只", len(test_weeks), len(stock_list))

    # 2. 临时去掉预过滤，直接调用 DeepSeek
    # 通过 monkey-patch 去掉预过滤
    import service.analysis.deepseek_nw_predictor as ds_mod
    _original_predict = ds_mod.predict_next_week_with_deepseek

    async def _predict_no_prefilter(code, stock_name, features, timeout=30.0):
        """绕过预过滤，直接调用LLM。"""
        # 保存原始值
        orig_market = features.get('market_chg', 0)
        orig_chg = features.get('this_week_chg', 0)
        # 临时设置为能通过预过滤的值
        features['market_chg'] = -5.0  # 假装大盘大跌
        features['this_week_chg'] = 10.0  # 假装个股暴涨
        try:
            result = await _original_predict(code, stock_name, features, timeout)
        finally:
            # 恢复原始值
            features['market_chg'] = orig_market
            features['this_week_chg'] = orig_chg
        return result

    # 不，这样会改变prompt内容。更好的方式是直接调用底层。
    # 让我直接构建prompt并调用。

    from service.analysis.deepseek_nw_predictor import (
        _get_client, _build_user_prompt, _SYSTEM_PROMPT
    )
    from common.utils.llm_utils import parse_llm_json

    async def _call_deepseek_raw(code, stock_name, features, timeout=30.0):
        """直接调用DeepSeek，不经过预过滤。"""
        client = _get_client()
        user_prompt = _build_user_prompt(code, stock_name, features)
        messages = [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ]
        try:
            resp = await asyncio.wait_for(
                client.chat(messages=messages, model="deepseek-chat",
                           temperature=0.3, max_tokens=200),
                timeout=timeout,
            )
            content = resp.get('choices', [{}])[0].get('message', {}).get('content', '')
            if not content:
                return None
            result = parse_llm_json(content)
            direction = result.get('direction', '').upper()
            if direction not in ('UP', 'DOWN', 'UNCERTAIN'):
                return None
            confidence = max(0.0, min(1.0, float(result.get('confidence', 0.5))))
            justification = str(result.get('justification', ''))[:100]
            return {'direction': direction, 'confidence': confidence,
                    'justification': justification}
        except Exception as e:
            logger.debug("DeepSeek调用失败: %s → %s", code, e)
            return None

    # 3. 收集所有预测记录
    records = []  # 每条: {features_bins, direction, confidence, actual, week, code, ...}
    ds_sem = asyncio.Semaphore(5)

    for pred_y, pred_w in test_weeks:
        nw_y, nw_w = _next_iso_week(pred_y, pred_w)
        week_label = f"W{pred_w}→W{nw_w}"

        samples = []
        actuals = {}
        features_map = {}
        for code, name in stock_list:
            feat = _extract_features(code, name, klines, pred_y, pred_w)
            actual = _check_next_week_actual(code, klines, pred_y, pred_w)
            if feat and actual is not None:
                samples.append({'code': code, 'name': name, 'features': feat})
                actuals[code] = actual
                features_map[code] = feat

        if not samples:
            continue

        logger.info("W%d→W%d: %d样本, 调用DeepSeek...", pred_w, nw_w, len(samples))

        async def _call(s):
            async with ds_sem:
                return await _call_deepseek_raw(s['code'], s['name'], s['features'])

        results = await asyncio.gather(*[_call(s) for s in samples], return_exceptions=True)

        for i, s in enumerate(samples):
            code = s['code']
            dr = results[i] if not isinstance(results[i], Exception) else None
            if dr is None:
                dr = {'direction': 'UNCERTAIN', 'confidence': 0.0, 'justification': 'FAIL'}

            feat = features_map[code]
            bins = _extract_all_bins(feat)
            actual = actuals[code]

            records.append({
                'code': code,
                'name': s['name'],
                'week': week_label,
                'direction': dr['direction'],
                'confidence': dr['confidence'],
                'justification': dr.get('justification', ''),
                'actual': actual,
                'actual_down': actual < 0,
                'bins': bins,
                # 原始数值特征（用于精细分析）
                'market_chg': feat.get('market_chg', 0),
                'this_week_chg': feat.get('this_week_chg', 0),
                'consec_up': feat.get('consec_up', 0),
                'vol_ratio': feat.get('vol_ratio'),
                'price_pos_60': feat.get('_price_pos_60'),
                'last_day_chg': feat.get('last_day_chg', 0),
                'prev_week_chg': feat.get('_prev_week_chg'),
            })

        ds_preds = sum(1 for r in records if r['week'] == week_label and r['direction'] == 'DOWN')
        logger.info("  → DeepSeek预测DOWN: %d只", ds_preds)

    logger.info("")
    logger.info("=" * 80)
    logger.info("  数据收集完成: %d条记录", len(records))
    logger.info("=" * 80)

    # 4. 分析 — 只看 DeepSeek 预测 DOWN 的记录
    down_records = [r for r in records if r['direction'] == 'DOWN']
    logger.info("DeepSeek预测DOWN: %d条", len(down_records))
    if not down_records:
        logger.info("无DOWN预测，退出")
        return

    total_correct = sum(1 for r in down_records if r['actual_down'])
    logger.info("总体准确率: %.1f%% (%d/%d)",
                total_correct / len(down_records) * 100,
                total_correct, len(down_records))


    # ═══════════════════════════════════════════════════════
    # 4a. 单维度分析
    # ═══════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  单维度分析（每个特征独立）")
    logger.info("=" * 80)

    all_dims = set()
    for r in down_records:
        all_dims.update(r['bins'].keys())

    dim_results = {}  # {dim_name: {bin_value: {correct, total}}}
    for dim in sorted(all_dims):
        dim_results[dim] = defaultdict(lambda: {'correct': 0, 'total': 0})
        for r in down_records:
            bv = r['bins'].get(dim)
            if bv is None:
                continue
            dim_results[dim][bv]['total'] += 1
            if r['actual_down']:
                dim_results[dim][bv]['correct'] += 1

        logger.info("")
        logger.info("  ── %s ──", dim)
        for bv in sorted(dim_results[dim].keys()):
            s = dim_results[dim][bv]
            t, c = s['total'], s['correct']
            acc = c / t * 100 if t > 0 else 0
            marker = " ★" if acc >= 75 and t >= 5 else ""
            logger.info("    %-28s %6.1f%% (%3d/%3d)%s", bv, acc, c, t, marker)

    # ═══════════════════════════════════════════════════════
    # 4b. 双维度交叉分析
    # ═══════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  双维度交叉分析（准确率≥70%且样本≥8）")
    logger.info("=" * 80)

    high_acc_combos = []  # [(dims, bins, correct, total, accuracy)]

    for d1, d2 in combinations(sorted(all_dims), 2):
        cross = defaultdict(lambda: {'correct': 0, 'total': 0})
        for r in down_records:
            b1 = r['bins'].get(d1)
            b2 = r['bins'].get(d2)
            if b1 is None or b2 is None:
                continue
            key = (b1, b2)
            cross[key]['total'] += 1
            if r['actual_down']:
                cross[key]['correct'] += 1

        for (b1, b2), s in cross.items():
            t, c = s['total'], s['correct']
            if t >= 8:
                acc = c / t * 100
                if acc >= 70:
                    high_acc_combos.append(((d1, d2), (b1, b2), c, t, acc))

    # 按准确率降序排列
    high_acc_combos.sort(key=lambda x: (-x[4], -x[3]))

    for (d1, d2), (b1, b2), c, t, acc in high_acc_combos[:40]:
        marker = " ★★" if acc >= 80 else " ★" if acc >= 75 else ""
        logger.info("  %s=%s + %s=%s → %.1f%% (%d/%d)%s",
                    d1, b1, d2, b2, acc, c, t, marker)

    # ═══════════════════════════════════════════════════════
    # 4c. 三维度交叉分析
    # ═══════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  三维度交叉分析（准确率≥75%且样本≥5）")
    logger.info("=" * 80)

    triple_combos = []
    for d1, d2, d3 in combinations(sorted(all_dims), 3):
        cross = defaultdict(lambda: {'correct': 0, 'total': 0})
        for r in down_records:
            b1 = r['bins'].get(d1)
            b2 = r['bins'].get(d2)
            b3 = r['bins'].get(d3)
            if b1 is None or b2 is None or b3 is None:
                continue
            key = (b1, b2, b3)
            cross[key]['total'] += 1
            if r['actual_down']:
                cross[key]['correct'] += 1

        for (b1, b2, b3), s in cross.items():
            t, c = s['total'], s['correct']
            if t >= 5:
                acc = c / t * 100
                if acc >= 75:
                    triple_combos.append(((d1, d2, d3), (b1, b2, b3), c, t, acc))

    triple_combos.sort(key=lambda x: (-x[4], -x[3]))

    for (d1, d2, d3), (b1, b2, b3), c, t, acc in triple_combos[:30]:
        marker = " ★★" if acc >= 80 else " ★"
        logger.info("  %s=%s + %s=%s + %s=%s → %.1f%% (%d/%d)%s",
                    d1, b1, d2, b2, d3, b3, acc, c, t, marker)

    # ═══════════════════════════════════════════════════════
    # 4d. 连续阈值扫描（不依赖分箱）
    # ═══════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  连续阈值扫描（找最优切分点）")
    logger.info("=" * 80)

    # 对每个连续特征，扫描不同阈值
    continuous_features = {
        'this_week_chg': [3, 4, 5, 6, 7, 8, 10],
        'market_chg_upper': [0, -0.5, -1.0, -1.5, -2.0],  # market_chg < threshold
        'consec_up_min': [2, 3, 4],
        'vol_ratio_min': [1.0, 1.2, 1.5, 2.0],
        'price_pos_60_min': [0.3, 0.5, 0.7, 0.8],
        'last_day_chg_upper': [0, -1, -2, -3],  # 尾日跌
        'last_day_chg_lower': [0, 1, 2, 3],  # 尾日涨
    }

    logger.info("")
    logger.info("  ── 个股涨幅阈值 (this_week_chg ≥ X) ──")
    for thresh in continuous_features['this_week_chg']:
        subset = [r for r in down_records if r['this_week_chg'] >= thresh]
        if len(subset) >= 3:
            c = sum(1 for r in subset if r['actual_down'])
            t = len(subset)
            logger.info("    涨幅≥%d%%: %.1f%% (%d/%d)", thresh, c/t*100, c, t)

    logger.info("")
    logger.info("  ── 大盘阈值 (market_chg < X) ──")
    for thresh in continuous_features['market_chg_upper']:
        subset = [r for r in down_records if r['market_chg'] < thresh]
        if len(subset) >= 3:
            c = sum(1 for r in subset if r['actual_down'])
            t = len(subset)
            logger.info("    大盘<%+.1f%%: %.1f%% (%d/%d)", thresh, c/t*100, c, t)

    logger.info("")
    logger.info("  ── 连涨天数阈值 (consec_up ≥ X) ──")
    for thresh in continuous_features['consec_up_min']:
        subset = [r for r in down_records if r['consec_up'] >= thresh]
        if len(subset) >= 3:
            c = sum(1 for r in subset if r['actual_down'])
            t = len(subset)
            logger.info("    连涨≥%d天: %.1f%% (%d/%d)", thresh, c/t*100, c, t)

    logger.info("")
    logger.info("  ── 量比阈值 (vol_ratio ≥ X) ──")
    for thresh in continuous_features['vol_ratio_min']:
        subset = [r for r in down_records if r.get('vol_ratio') is not None and r['vol_ratio'] >= thresh]
        if len(subset) >= 3:
            c = sum(1 for r in subset if r['actual_down'])
            t = len(subset)
            logger.info("    量比≥%.1f: %.1f%% (%d/%d)", thresh, c/t*100, c, t)

    logger.info("")
    logger.info("  ── 60日价格位置阈值 (price_pos_60 ≥ X) ──")
    for thresh in continuous_features['price_pos_60_min']:
        subset = [r for r in down_records
                  if r.get('price_pos_60') is not None and r['price_pos_60'] >= thresh]
        if len(subset) >= 3:
            c = sum(1 for r in subset if r['actual_down'])
            t = len(subset)
            logger.info("    60日位置≥%.0f%%: %.1f%% (%d/%d)", thresh*100, c/t*100, c, t)

    logger.info("")
    logger.info("  ── 尾日涨跌阈值 ──")
    for thresh in continuous_features['last_day_chg_upper']:
        subset = [r for r in down_records if r['last_day_chg'] < thresh]
        if len(subset) >= 3:
            c = sum(1 for r in subset if r['actual_down'])
            t = len(subset)
            logger.info("    尾日<%+d%%: %.1f%% (%d/%d)", thresh, c/t*100, c, t)
    for thresh in continuous_features['last_day_chg_lower']:
        subset = [r for r in down_records if r['last_day_chg'] > thresh]
        if len(subset) >= 3:
            c = sum(1 for r in subset if r['actual_down'])
            t = len(subset)
            logger.info("    尾日>%+d%%: %.1f%% (%d/%d)", thresh, c/t*100, c, t)

    # ═══════════════════════════════════════════════════════
    # 4e. 组合阈值扫描（双条件）
    # ═══════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  组合阈值扫描（双条件，准确率≥75%且样本≥5）")
    logger.info("=" * 80)

    combo_results = []

    chg_thresholds = [3, 5, 6, 8]
    mkt_thresholds = [999, 0, -0.5, -1.0, -1.5]  # 999 = 不限
    cu_thresholds = [0, 2, 3]  # 0 = 不限
    vr_thresholds = [0, 1.2, 1.5]  # 0 = 不限
    pos_thresholds = [0, 0.5, 0.7]  # 0 = 不限

    for chg_t in chg_thresholds:
        for mkt_t in mkt_thresholds:
            for cu_t in cu_thresholds:
                for vr_t in vr_thresholds:
                    for pos_t in pos_thresholds:
                        # 至少2个条件生效（不能全不限）
                        active = sum([
                            chg_t > 0,
                            mkt_t < 999,
                            cu_t > 0,
                            vr_t > 0,
                            pos_t > 0,
                        ])
                        if active < 2:
                            continue

                        subset = []
                        for r in down_records:
                            if r['this_week_chg'] < chg_t:
                                continue
                            if mkt_t < 999 and r['market_chg'] >= mkt_t:
                                continue
                            if cu_t > 0 and r['consec_up'] < cu_t:
                                continue
                            if vr_t > 0:
                                vr = r.get('vol_ratio')
                                if vr is None or vr < vr_t:
                                    continue
                            if pos_t > 0:
                                pp = r.get('price_pos_60')
                                if pp is None or pp < pos_t:
                                    continue
                            subset.append(r)

                        if len(subset) >= 5:
                            c = sum(1 for r in subset if r['actual_down'])
                            t = len(subset)
                            acc = c / t * 100
                            if acc >= 75:
                                conds = []
                                conds.append(f"涨≥{chg_t}%")
                                if mkt_t < 999:
                                    conds.append(f"大盘<{mkt_t:+.1f}%")
                                if cu_t > 0:
                                    conds.append(f"连涨≥{cu_t}天")
                                if vr_t > 0:
                                    conds.append(f"量比≥{vr_t:.1f}")
                                if pos_t > 0:
                                    conds.append(f"60日位≥{pos_t:.0%}")
                                combo_results.append((' + '.join(conds), c, t, acc))

    combo_results.sort(key=lambda x: (-x[3], -x[2]))
    # 去重（保留准确率最高的）
    seen = set()
    for desc, c, t, acc in combo_results[:50]:
        if desc not in seen:
            seen.add(desc)
            marker = " ★★★" if acc >= 80 and t >= 10 else " ★★" if acc >= 80 else " ★"
            logger.info("  %s → %.1f%% (%d/%d)%s", desc, acc, c, t, marker)

    # ═══════════════════════════════════════════════════════
    # 5. 保存原始数据供进一步分析
    # ═══════════════════════════════════════════════════════
    out_path = Path(__file__).parent.parent / 'data_results' / 'deepseek_multidim_records.json'
    out_data = []
    for r in records:
        out_data.append({
            'code': r['code'], 'name': r['name'], 'week': r['week'],
            'direction': r['direction'], 'confidence': r['confidence'],
            'actual': r['actual'], 'actual_down': r['actual_down'],
            'market_chg': r['market_chg'], 'this_week_chg': r['this_week_chg'],
            'consec_up': r['consec_up'], 'vol_ratio': r['vol_ratio'],
            'price_pos_60': r['price_pos_60'], 'last_day_chg': r['last_day_chg'],
            'prev_week_chg': r['prev_week_chg'],
            'justification': r.get('justification', ''),
        })
    out_path.write_text(json.dumps(out_data, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info("")
    logger.info("原始数据已保存: %s (%d条)", out_path, len(out_data))

    elapsed = time.time() - t0
    logger.info("分析完成，耗时 %.1f分钟", elapsed / 60)


if __name__ == '__main__':
    asyncio.run(main())
