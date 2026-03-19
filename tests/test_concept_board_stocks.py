"""
测试 concept_board_stocks_10jqka 模块的解析逻辑。
不依赖网络，使用模拟HTML数据验证。

Usage:
    python -m pytest tests/test_concept_board_stocks.py -v
"""
from unittest.mock import patch, MagicMock
from service.jqka10.concept_board_stocks_10jqka import (
    _parse_stocks, _get_total_pages, fetch_board_stocks,
    _MAX_SSR_PAGES,
)


def _make_stock_row(code: str, name: str) -> str:
    return (
        f'<td><a href="http://stockpage.10jqka.com.cn/{code}/"'
        f' target="_blank">{code}</a></td>\n'
        f'<td><a href="http://stockpage.10jqka.com.cn/{code}"'
        f' target="_blank">{name}</a></td>'
    )


def _make_page_html(stocks: list[tuple[str, str]], cur_page: int = 1,
                    total_pages: int = 1) -> str:
    rows = "\n".join(_make_stock_row(c, n) for c, n in stocks)
    return (
        f'<html><body><table>{rows}</table>'
        f'<span class="page_info">{cur_page}/{total_pages}</span>'
        f'</body></html>'
    )


class TestParseStocks:
    def test_basic(self):
        html = _make_page_html([("000001", "平安银行"), ("600519", "贵州茅台")])
        result = _parse_stocks(html)
        assert len(result) == 2
        assert result[0] == ("000001", "平安银行")

    def test_dedup(self):
        html = _make_page_html([("000001", "平安银行"), ("000001", "平安银行")])
        assert len(_parse_stocks(html)) == 1

    def test_empty(self):
        assert _parse_stocks("<html></html>") == []


class TestGetTotalPages:
    def test_page_info(self):
        assert _get_total_pages('<span class="page_info">1/15</span>') == 15

    def test_last_page(self):
        assert _get_total_pages('<a class="changePage" page="20">尾页</a>') == 20

    def test_no_pagination(self):
        assert _get_total_pages("<html></html>") == 1


class TestFetchBoardStocks:
    """测试两阶段抓取逻辑 (SSR + AJAX)"""

    def test_small_board_single_page(self):
        """成分股≤10只，只需SSR首页，不触发AJAX"""
        stocks = [("000001", "平安银行"), ("600519", "贵州茅台")]
        html = _make_page_html(stocks, 1, 1)

        def mock_ssr(board_code, delay=0.3):
            all_stocks = {c: n for c, n in _parse_stocks(html)}
            return all_stocks, 1

        with patch("service.jqka10.concept_board_stocks_10jqka._ssr_fetch_all",
                   side_effect=mock_ssr):
            result = fetch_board_stocks("308832", delay=0)

        assert len(result) == 2

    def test_large_board_ssr_only(self):
        """SSR阶段 desc+asc 各5页覆盖"""
        stock_pool = {f"{i:06d}": f"股票{i}" for i in range(1, 101)}

        def mock_ssr(board_code, delay=0.3):
            return stock_pool, 5  # total_pages <= _MAX_SSR_PAGES

        with patch("service.jqka10.concept_board_stocks_10jqka._ssr_fetch_all",
                   side_effect=mock_ssr):
            result = fetch_board_stocks("308832", delay=0)

        assert len(result) == 100

    def test_large_board_needs_ajax(self):
        """SSR不够时触发AJAX阶段"""
        ssr_stocks = {f"{i:06d}": f"股票{i}" for i in range(1, 51)}

        def mock_ajax(board_code, total_pages, all_stocks, cookies,
                      start_page=0, delay=0.3):
            for i in range(51, 151):
                all_stocks[f"{i:06d}"] = f"股票{i}"
            return total_pages + 1

        def mock_ssr(board_code, delay=0.3):
            return dict(ssr_stocks), 15

        with patch("service.jqka10.concept_board_stocks_10jqka._ssr_fetch_all",
                   side_effect=mock_ssr), \
             patch("service.jqka10.concept_board_stocks_10jqka._load_cookies",
                   return_value={"v": "fake_v"}), \
             patch("service.jqka10.concept_board_stocks_10jqka._generate_hexin_v",
                   return_value="fake_v"), \
             patch("service.jqka10.concept_board_stocks_10jqka._ajax_fetch_all_pages",
                   side_effect=mock_ajax):
            result = fetch_board_stocks("308832", delay=0)

        assert len(result) == 150

    def test_403_handled_gracefully(self):
        """SSR失败时应优雅降级"""
        ssr_stocks = {"000001": "平安银行"}

        def mock_ssr(board_code, delay=0.3):
            return dict(ssr_stocks), 10

        with patch("service.jqka10.concept_board_stocks_10jqka._ssr_fetch_all",
                   side_effect=mock_ssr), \
             patch("service.jqka10.concept_board_stocks_10jqka._load_cookies",
                   return_value={}), \
             patch("service.jqka10.concept_board_stocks_10jqka._generate_hexin_v",
                   return_value=None):
            result = fetch_board_stocks("308832", delay=0)

        assert len(result) == 1  # 至少拿到SSR数据


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
