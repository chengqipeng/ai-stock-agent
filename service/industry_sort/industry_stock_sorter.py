"""
根据历史日线数据对 stock_industry_list.md 中各板块个股进行强弱排序。

评分维度（满分100）：
1. 均线多头排列（MA5>MA10>MA20>MA60）  — 30分
2. 近期涨幅动量（20日涨幅 + 5日涨幅）  — 25分
3. 相对强度RS（60日+20日涨幅综合）      — 20分
4. 量价配合（近5日量比 + 放量上涨）     — 15分
5. 波动稳定性（振幅适中、回撤可控）     — 10分

使用方式：
    python -m service.industry_sort.industry_stock_sorter
"""
import logging
import re
import numpy as np
from pathlib import Path
from zoneinfo import ZoneInfo

from dao.stock_kline_dao import get_kline_data

logger = logging.getLogger(__name__)
_CST = ZoneInfo("Asia/Shanghai")

_MD_PATH = Path(__file__).parent.parent.parent / "data_results" / "industry_analysis" / "stock_industry_list.md"

# 匹配股票行: - 名称 (代码.交易所)可选后缀
_STOCK_RE = re.compile(r'^- (.+?)\s*\((\d{6}\.[A-Z]{2})\)(.*)')


# ─────────── 健康强势评分 ───────────

def _calc_ma(closes: list[float], period: int) -> float | None:
    if len(closes) < period:
        return None
    return float(np.mean(closes[-period:]))


def score_stock(klines: list[dict]) -> float:
    """
    根据日线数据计算个股健康强势评分（0~100）。
    klines: get_kline_data 返回的 list[dict]，按日期升序。
    """
    if not klines or len(klines) < 20:
        return -1  # 数据不足，排到最后

    closes = [float(k['close_price']) for k in klines]
    volumes = [float(k['trading_volume']) for k in klines if k['trading_volume']]
    pct_changes = [float(k['change_percent']) for k in klines if k['change_percent'] is not None]

    latest_close = closes[-1]
    score = 0.0

    # ── 1. 均线多头排列（30分）──
    ma5 = _calc_ma(closes, 5)
    ma10 = _calc_ma(closes, 10)
    ma20 = _calc_ma(closes, 20)
    ma60 = _calc_ma(closes, 60) if len(closes) >= 60 else None

    ma_score = 0
    if ma5 and ma10 and ma20:
        if latest_close > ma5:
            ma_score += 6
        if ma5 > ma10:
            ma_score += 6
        if ma10 > ma20:
            ma_score += 6
        if ma60 and ma20 > ma60:
            ma_score += 6
        # MA5斜率向上
        if len(closes) >= 8:
            ma5_prev = float(np.mean(closes[-8:-3]))
            if ma5 > ma5_prev:
                ma_score += 6
    score += min(ma_score, 30)

    # ── 2. 近期涨幅动量（25分）──
    momentum_score = 0
    if len(closes) >= 20 and closes[-20] > 0:
        pct_20d = (latest_close - closes[-20]) / closes[-20] * 100
        momentum_score += max(0, min(15, pct_20d))
    if len(closes) >= 5 and closes[-5] > 0:
        pct_5d = (latest_close - closes[-5]) / closes[-5] * 100
        momentum_score += max(0, min(10, pct_5d * 1.25))
    score += min(momentum_score, 25)

    # ── 3. 相对强度RS（20分）──
    rs_score = 0
    if len(closes) >= 60 and closes[-60] > 0:
        pct_60d = (latest_close - closes[-60]) / closes[-60] * 100
        rs_score += max(0, min(12, pct_60d * 0.4))
    if len(closes) >= 20 and closes[-20] > 0:
        pct_20d = (latest_close - closes[-20]) / closes[-20] * 100
        rs_score += max(0, min(8, pct_20d * 0.53))
    score += min(rs_score, 20)

    # ── 4. 量价配合（15分）──
    vol_score = 0
    if len(volumes) >= 20:
        vol_5d_avg = float(np.mean(volumes[-5:])) if len(volumes) >= 5 else volumes[-1]
        vol_20d_avg = float(np.mean(volumes[-20:]))
        if vol_20d_avg > 0:
            vol_ratio = vol_5d_avg / vol_20d_avg
            if 1.0 <= vol_ratio <= 2.5:
                vol_score += min(10, (vol_ratio - 0.8) * 8)
            elif vol_ratio > 2.5:
                vol_score += 5
            else:
                vol_score += max(0, vol_ratio * 5)
        # 上涨放量、下跌缩量
        recent_klines = klines[-5:]
        up_vol_ok = 0
        for k in recent_klines:
            pct = float(k['change_percent']) if k['change_percent'] else 0
            vol = float(k['trading_volume']) if k['trading_volume'] else 0
            if pct > 0 and vol > vol_20d_avg:
                up_vol_ok += 1
            elif pct < 0 and vol < vol_20d_avg:
                up_vol_ok += 1
        vol_score += up_vol_ok
    score += min(vol_score, 15)

    # ── 5. 波动稳定性（10分）──
    stability_score = 0
    if len(pct_changes) >= 20:
        recent_pct = pct_changes[-20:]
        std_dev = float(np.std(recent_pct))
        if std_dev < 2:
            stability_score = 10
        elif std_dev < 5:
            stability_score = max(0, 10 - (std_dev - 2) * 3.33)
        # 最大回撤惩罚
        max_dd = 0
        peak = closes[-20]
        for c in closes[-20:]:
            if c > peak:
                peak = c
            dd = (peak - c) / peak * 100
            if dd > max_dd:
                max_dd = dd
        if max_dd > 15:
            stability_score *= 0.5
        elif max_dd > 10:
            stability_score *= 0.7
    score += min(stability_score, 10)

    return round(score, 1)


# ─────────── 解析与重写 MD ───────────

def sort_and_rewrite(md_path: Path = None, kline_days: int = 80):
    """
    主函数：逐行解析 MD → 查询日线数据 → 评分 → 按分数排序 → 重写 MD。
    保持文件整体结构不变，仅对每个子分类内的连续股票列表按强弱重排序。
    """
    if md_path is None:
        md_path = _MD_PATH

    logger.info("开始对 stock_industry_list.md 进行强弱排序...")
    content = md_path.read_text(encoding='utf-8')
    lines = content.split('\n')

    # 第一遍：收集所有股票代码
    all_codes = set()
    for line in lines:
        m = _STOCK_RE.match(line.strip())
        if m:
            all_codes.add(m.group(2))

    logger.info("共解析到 %d 只股票，开始查询日线数据并评分...", len(all_codes))

    # 批量评分
    score_map: dict[str, float] = {}
    scored = 0
    for code in sorted(all_codes):
        klines = get_kline_data(code, limit=kline_days)
        score_map[code] = score_stock(klines)
        scored += 1
        if scored % 50 == 0:
            logger.info("已评分 %d / %d ...", scored, len(all_codes))

    logger.info("评分完成，开始重写 MD 文件...")

    # 第二遍：逐行处理，遇到连续股票行时收集并排序后输出
    output_lines: list[str] = []
    stock_buffer: list[tuple[str, str, str]] = []  # (name, code, suffix)

    def flush_stock_buffer():
        """将缓冲区中的股票按分数排序后输出"""
        if not stock_buffer:
            return
        sorted_stocks = sorted(
            stock_buffer,
            key=lambda x: score_map.get(x[1], -999),
            reverse=True,
        )
        for name, code, suffix in sorted_stocks:
            output_lines.append(f"- {name} ({code}){suffix}")
        stock_buffer.clear()

    for line in lines:
        stripped = line.strip()
        m = _STOCK_RE.match(stripped)
        if m:
            # 股票行 → 放入缓冲区
            name = m.group(1).strip()
            code = m.group(2)
            suffix = m.group(3)
            stock_buffer.append((name, code, suffix))
        else:
            # 非股票行 → 先 flush 缓冲区，再原样输出
            flush_stock_buffer()
            output_lines.append(line)

    # 文件末尾可能还有未 flush 的股票
    flush_stock_buffer()

    # 写回文件
    md_path.write_text('\n'.join(output_lines), encoding='utf-8')

    sorted_sections = sum(1 for line in lines if line.strip().startswith('###') or
                          (line.strip().startswith('## ') and not line.strip().startswith('## 附')))
    logger.info("排序完成！共 %d 只股票已按强弱重排序。文件已更新: %s",
                len(all_codes), md_path)

    # 输出 Top 20 概览
    top_stocks = sorted(score_map.items(), key=lambda x: x[1], reverse=True)[:20]
    logger.info("── 全局 Top 20 强势股 ──")
    for code, s in top_stocks:
        logger.info("  %s: %.1f 分", code, s)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    sort_and_rewrite()
