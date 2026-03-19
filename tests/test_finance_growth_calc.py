"""
验证财报弹框中 营收同比/环比、扣非同比/环比 的计算逻辑是否正确。

逻辑说明：
  - 同比：直接取数据库字段 "营业总收入同比增长(%)" / "扣非净利润同比增长(%)"
  - 环比：优先取数据库字段，若为空则用相邻两期绝对值手动计算
          绝对值字段带中文单位（如 "46.17亿"、"3618.73万"），需统一转为元再计算
"""
import logging
import re
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.utils.stock_info_utils import StockInfo
from service.jqka10.stock_finance_data_10jqka import get_financial_data_from_db

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

VERIFY_STOCKS = [
    ("002371.SZ", "北方华创"),
    ("300602.SZ", "飞荣达"),
    ("002463.SZ", "沪电股份"),
    ("300308.SZ", "中际旭创"),
    ("300502.SZ", "新易盛"),
    ("002028.SZ", "思源电气"),
    ("300394.SZ", "天孚通信"),
    ("002050.SZ", "三花智控"),
    ("002916.SZ", "深南电路"),
    ("603986.SH", "兆易创新"),
]


def _make_stock_info(code_normalize: str, name: str) -> StockInfo:
    code, suffix = code_normalize.split(".")
    market_prefix = "0" if suffix == "SZ" else "1"
    return StockInfo(secid=f"{market_prefix}.{code}", stock_code=code,
                     stock_code_normalize=code_normalize, stock_name=name)


def _parse_cn_num(v):
    """解析带中文单位的数值（如 "46.17亿"、"3618.73万"），统一转为元"""
    if v is None or v == "" or v == "None":
        return None
    s = str(v).replace(",", "").strip()
    m = re.match(r'^([+-]?\d+(?:\.\d+)?)\s*(亿|万)?$', s)
    if not m:
        try:
            return float(s)
        except (ValueError, TypeError):
            return None
    num = float(m.group(1))
    unit = m.group(2)
    if unit == "亿":
        num *= 1e8
    elif unit == "万":
        num *= 1e4
    return num


def _parse_num(v):
    if v is None or v == "" or v == "None":
        return None
    try:
        return float(str(v).replace(",", ""))
    except (ValueError, TypeError):
        return None


def _calc_qoq(curr_val, prev_val):
    if curr_val is None or prev_val is None or prev_val == 0:
        return None
    return (curr_val - prev_val) / abs(prev_val) * 100


def verify_stock(code_normalize: str, name: str):
    info = _make_stock_info(code_normalize, name)
    records = get_financial_data_from_db(info)

    if not records:
        logger.warning("  ⚠️  %s(%s): 无财报数据", name, code_normalize)
        return True

    logger.info("  📊 %s(%s): %d 条记录", name, code_normalize, len(records))

    errors = []
    has_qoq = False

    for i, rec in enumerate(records):
        period = rec.get("报告期", f"第{i}条")

        yoy_rev = _parse_num(rec.get("营业总收入同比增长(%)"))
        yoy_profit = _parse_num(rec.get("扣非净利润同比增长(%)"))

        db_qoq_rev = _parse_num(rec.get("营业总收入环比增长(%)"))
        db_qoq_profit = _parse_num(rec.get("扣非净利润环比增长(%)"))

        # 手动计算环比（使用带单位解析）
        if i < len(records) - 1:
            curr_rev = _parse_cn_num(rec.get("营业总收入(元)"))
            prev_rev = _parse_cn_num(records[i + 1].get("营业总收入(元)"))
            calc_qoq_rev = _calc_qoq(curr_rev, prev_rev)

            curr_profit = _parse_cn_num(rec.get("扣非净利润(元)"))
            prev_profit = _parse_cn_num(records[i + 1].get("扣非净利润(元)"))
            calc_qoq_profit = _calc_qoq(curr_profit, prev_profit)
        else:
            calc_qoq_rev = None
            calc_qoq_profit = None

        final_qoq_rev = db_qoq_rev if db_qoq_rev is not None else calc_qoq_rev
        final_qoq_profit = db_qoq_profit if db_qoq_profit is not None else calc_qoq_profit

        if final_qoq_rev is not None or final_qoq_profit is not None:
            has_qoq = True

        # 验证：如果 DB 有环比，手动计算应接近
        if db_qoq_rev is not None and calc_qoq_rev is not None:
            diff = abs(db_qoq_rev - calc_qoq_rev)
            if diff > 1.0:
                errors.append(f"    ❌ {period} 营收环比: DB={db_qoq_rev:.2f}% 计算={calc_qoq_rev:.2f}% 差={diff:.2f}%")

        if db_qoq_profit is not None and calc_qoq_profit is not None:
            diff = abs(db_qoq_profit - calc_qoq_profit)
            if diff > 1.0:
                errors.append(f"    ❌ {period} 扣非环比: DB={db_qoq_profit:.2f}% 计算={calc_qoq_profit:.2f}% 差={diff:.2f}%")

        # 验证：环比计算的合理性（绝对值不应超过 10000%）
        if calc_qoq_rev is not None and abs(calc_qoq_rev) > 10000:
            errors.append(f"    ⚠️  {period} 营收环比异常大: {calc_qoq_rev:.2f}%（可能单位转换错误）")
        if calc_qoq_profit is not None and abs(calc_qoq_profit) > 10000:
            errors.append(f"    ⚠️  {period} 扣非环比异常大: {calc_qoq_profit:.2f}%（可能单位转换错误）")

        if i < 5:
            raw_rev = rec.get("营业总收入(元)", "-")
            raw_profit = rec.get("扣非净利润(元)", "-")
            logger.info(
                "    %s | 营收:%s 同比:%s 环比:%s | 扣非:%s 同比:%s 环比:%s",
                period,
                raw_rev,
                f"{yoy_rev:.2f}%" if yoy_rev is not None else "-",
                f"{final_qoq_rev:.2f}%" if final_qoq_rev is not None else "-",
                raw_profit,
                f"{yoy_profit:.2f}%" if yoy_profit is not None else "-",
                f"{final_qoq_profit:.2f}%" if final_qoq_profit is not None else "-",
            )

    if not has_qoq:
        logger.warning("    ⚠️  所有记录环比均为空（可能绝对值字段缺失）")

    if errors:
        for e in errors:
            logger.error(e)
        return False

    logger.info("    ✅ 通过")
    return True


def main():
    logger.info("=" * 70)
    logger.info("财报增长计算验证 - 10 只股票（含中文单位解析）")
    logger.info("=" * 70)

    passed = 0
    failed = 0

    for code, name in VERIFY_STOCKS:
        ok = verify_stock(code, name)
        if ok:
            passed += 1
        else:
            failed += 1

    logger.info("")
    logger.info("=" * 70)
    logger.info("结果: %d 通过, %d 失败 (共 %d 只)", passed, failed, len(VERIFY_STOCKS))
    logger.info("=" * 70)
    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
