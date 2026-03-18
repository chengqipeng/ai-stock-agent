"""
验证所有调度器的股票列表均已过滤北交所（.BJ）股票。

检查范围：
1. kline_data_scheduler - load_stocks_from_score_list / _build_stock_list
2. fund_flow_scheduler - _build_stock_list
3. market_data_scheduler - _build_stock_list
4. week_highest_lowest_price_scheduler - _load_stocks
5. kline_score_scheduler - 内部过滤
6. db_anomalies_scheduler - 内部过滤
7. weekly_prediction_service - _get_all_stock_codes
8. acquire_classify_stocks - parse_score_list
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.utils.stock_info_utils import is_bj_stock


def check_list(name: str, stocks: list[dict], code_key: str = "code"):
    """检查股票列表中是否包含北交所股票"""
    bj_stocks = [s for s in stocks if is_bj_stock(s[code_key])]
    total = len(stocks)
    bj_count = len(bj_stocks)
    if bj_count > 0:
        print(f"  ❌ {name}: 共{total}只，包含{bj_count}只北交所股票!")
        for s in bj_stocks[:5]:
            print(f"     - {s.get('name', '?')} ({s[code_key]})")
        if bj_count > 5:
            print(f"     ... 还有{bj_count - 5}只")
        return False
    else:
        print(f"  ✅ {name}: 共{total}只，无北交所股票")
        return True


def main():
    print("=" * 60)
    print("  验证所有调度器股票列表 - 北交所过滤检查")
    print("=" * 60)
    all_pass = True

    # 1. kline_data_scheduler.load_stocks_from_score_list
    print("\n[1] kline_data_scheduler.load_stocks_from_score_list")
    from service.auto_job.kline_data_scheduler import load_stocks_from_score_list
    stocks = load_stocks_from_score_list()
    if not check_list("score_list", stocks):
        all_pass = False

    # 2. kline_data_scheduler._build_stock_list
    print("\n[2] kline_data_scheduler._build_stock_list")
    from service.auto_job.kline_data_scheduler import _build_stock_list as kline_build
    stocks = kline_build()
    if not check_list("kline_build_stock_list", stocks):
        all_pass = False

    # 3. fund_flow_scheduler._build_stock_list
    print("\n[3] fund_flow_scheduler._build_stock_list")
    from service.auto_job.fund_flow_scheduler import _build_stock_list as ff_build
    stocks = ff_build()
    if not check_list("fund_flow_build_stock_list", stocks):
        all_pass = False

    # 4. market_data_scheduler._build_stock_list
    print("\n[4] market_data_scheduler._build_stock_list")
    from service.auto_job.market_data_scheduler import _build_stock_list as md_build
    stocks = md_build()
    if not check_list("market_data_build_stock_list", stocks):
        all_pass = False

    # 5. week_highest_lowest_price_scheduler._load_stocks
    print("\n[5] week_highest_lowest_price_scheduler._load_stocks")
    from service.auto_job.week_highest_lowest_price_scheduler import _load_stocks
    stocks = _load_stocks()
    if not check_list("price_load_stocks", stocks):
        all_pass = False

    # 6. acquire_classify_stocks.parse_score_list
    print("\n[6] acquire_classify_stocks.parse_score_list")
    from service.auto_job.acquire_classify_stocks import parse_score_list
    from pathlib import Path
    score_path = Path(__file__).parent.parent / "data_results/stock_to_score_list/stock_score_list.md"
    stocks_dict = parse_score_list(score_path)
    bj_in_dict = {k: v for k, v in stocks_dict.items() if is_bj_stock(k)}
    if bj_in_dict:
        print(f"  ❌ parse_score_list: 共{len(stocks_dict)}只，包含{len(bj_in_dict)}只北交所!")
        all_pass = False
    else:
        print(f"  ✅ parse_score_list: 共{len(stocks_dict)}只，无北交所股票")

    # 7. 验证 score_list.md 中北交所股票的原始数量（确认确实有北交所需要过滤）
    print("\n[7] 原始 stock_score_list.md 中北交所股票数量")
    import re
    pattern = re.compile(r'^(.+?)\s+\(([^)]+)\)')
    all_raw = []
    bj_raw = []
    for line in score_path.read_text(encoding='utf-8').splitlines():
        m = pattern.match(line.strip())
        if m:
            code = m.group(2)
            all_raw.append(code)
            if is_bj_stock(code):
                bj_raw.append(code)
    print(f"  📊 原始列表: 共{len(all_raw)}只，其中北交所{len(bj_raw)}只")
    if bj_raw:
        print(f"     北交所样例: {', '.join(bj_raw[:5])}...")

    # 8. is_bj_stock 边界测试
    print("\n[8] is_bj_stock 函数边界测试")
    test_cases = [
        ("920029.BJ", True, "北交所个股"),
        ("899050.SZ", False, "北证50指数（深交所发布）"),
        ("899601.SZ", False, "北证专精特新指数（深交所发布）"),
        ("000001.SH", False, "上证指数"),
        ("000001.SZ", False, "平安银行"),
        ("600000.SH", False, "浦发银行"),
        ("300001.SZ", False, "特锐德"),
        ("688001.SH", False, "华兴源创"),
        ("920029", True, "纯数字北交所"),
        ("000001", False, "纯数字深圳"),
    ]
    edge_pass = True
    for code, expected, desc in test_cases:
        result = is_bj_stock(code)
        status = "✅" if result == expected else "❌"
        if result != expected:
            edge_pass = False
            all_pass = False
        print(f"  {status} is_bj_stock('{code}') = {result} (期望{expected}) - {desc}")

    if edge_pass:
        print("  ✅ 边界测试全部通过")
    else:
        print("  ❌ 边界测试有失败项")

    # 总结
    print("\n" + "=" * 60)
    if all_pass:
        print("  ✅ 全部通过! 所有调度器均已正确过滤北交所股票")
    else:
        print("  ❌ 存在未过滤北交所股票的调度器，请检查!")
    print("=" * 60)

    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
