"""service/jqka10/stock_sync_service.py 单元测试"""
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch, AsyncMock

import pytest

from service.jqka10.stock_sync_service import (
    _normalize_stock_code,
    _get_index_mapping,
    _append_to_stocks_data,
    _append_to_score_list,
    _load_score_list_codes,
    sync_new_stocks,
)


class TestNormalizeStockCode:
    def test_sz_codes(self):
        assert _normalize_stock_code("300143") == "300143.SZ"
        assert _normalize_stock_code("000001") == "000001.SZ"
        assert _normalize_stock_code("002050") == "002050.SZ"
        assert _normalize_stock_code("301312") == "301312.SZ"

    def test_sh_codes(self):
        assert _normalize_stock_code("600519") == "600519.SH"
        assert _normalize_stock_code("601127") == "601127.SH"
        assert _normalize_stock_code("603979") == "603979.SH"
        assert _normalize_stock_code("605499") == "605499.SH"
        assert _normalize_stock_code("688336") == "688336.SH"

    def test_bj_codes(self):
        assert _normalize_stock_code("920002") == "920002.BJ"
        assert _normalize_stock_code("430047") == "430047.BJ"
        assert _normalize_stock_code("830799") == "830799.BJ"

    def test_invalid(self):
        assert _normalize_stock_code("12345") is None
        assert _normalize_stock_code("abc") is None
        assert _normalize_stock_code("") is None


class TestGetIndexMapping:
    def test_cyb(self):
        codes, names = _get_index_mapping("300143.SZ")
        assert codes == ["399001.SZ", "399006.SZ"]
        assert names == ["深证成指", "创业板指"]

    def test_sz_main(self):
        codes, names = _get_index_mapping("000001.SZ")
        assert codes == ["399001.SZ"]

    def test_sh(self):
        codes, names = _get_index_mapping("600519.SH")
        assert codes == ["000001.SH"]

    def test_kcb(self):
        codes, names = _get_index_mapping("688336.SH")
        assert codes == ["000001.SH", "000680.SH"]

    def test_bj(self):
        codes, names = _get_index_mapping("920002.BJ")
        assert codes == ["899050.SZ"]


class TestAppendToStocksData:
    """测试向 stocks_data.py 追加新股票条目"""

    def setup_method(self):
        self.tmp = Path(tempfile.mktemp(suffix='.py'))
        # 创建一个最小化的 stocks_data.py 副本
        self.tmp.write_text(
            'STOCKS = [\n'
            '  {\n'
            '    "code": "300812.SZ",\n'
            '    "name": "易天股份",\n'
            '    "indices_stock_codes": [\n'
            '      "399001.SZ",\n'
            '      "399006.SZ"\n'
            '    ],\n'
            '    "indices_stock_names": [\n'
            '      "深证成指",\n'
            '      "创业板指"\n'
            '    ]\n'
            '  }\n'
            ']\n'
            '\n'
            'ALL_STOCKS = STOCKS + MAIN_STOCK\n',
            encoding='utf-8',
        )

    def teardown_method(self):
        if self.tmp.exists():
            self.tmp.unlink()

    def test_append_single(self):
        import service.jqka10.stock_sync_service as svc
        orig = svc._STOCKS_DATA_FILE
        svc._STOCKS_DATA_FILE = self.tmp
        try:
            entries = [{
                "code": "999001.SZ",
                "name": "测试A",
                "indices_stock_codes": ["399001.SZ"],
                "indices_stock_names": ["深证成指"],
            }]
            count = _append_to_stocks_data(entries)
            assert count == 1
            content = self.tmp.read_text(encoding='utf-8')
            assert '"999001.SZ"' in content
            assert '"测试A"' in content
            # 确保在 ALL_STOCKS 之前
            assert content.index('"999001.SZ"') < content.index('ALL_STOCKS')
            # 确保语法正确
            import ast
            ast.parse(content)
        finally:
            svc._STOCKS_DATA_FILE = orig

    def test_append_multiple(self):
        import service.jqka10.stock_sync_service as svc
        orig = svc._STOCKS_DATA_FILE
        svc._STOCKS_DATA_FILE = self.tmp
        try:
            entries = [
                {"code": "999001.SZ", "name": "测试A",
                 "indices_stock_codes": ["399001.SZ"], "indices_stock_names": ["深证成指"]},
                {"code": "999002.SH", "name": "测试B",
                 "indices_stock_codes": ["000001.SH"], "indices_stock_names": ["上证指数"]},
            ]
            count = _append_to_stocks_data(entries)
            assert count == 2
            content = self.tmp.read_text(encoding='utf-8')
            assert '"999001.SZ"' in content
            assert '"999002.SH"' in content
            import ast
            ast.parse(content)
        finally:
            svc._STOCKS_DATA_FILE = orig

    def test_append_empty(self):
        assert _append_to_stocks_data([]) == 0


class TestAppendToScoreList:
    def setup_method(self):
        self.tmp = Path(tempfile.mktemp(suffix='.md'))
        self.tmp.write_text("闰土股份 (002440.SZ) - 打分：78\n", encoding='utf-8')

    def teardown_method(self):
        if self.tmp.exists():
            self.tmp.unlink()

    def test_append(self):
        import service.jqka10.stock_sync_service as svc
        orig = svc._SCORE_LIST_FILE
        svc._SCORE_LIST_FILE = self.tmp
        try:
            entries = [
                {"code": "999001.SZ", "name": "测试A"},
                {"code": "999002.SH", "name": "测试B"},
            ]
            count = _append_to_score_list(entries)
            assert count == 2
            content = self.tmp.read_text(encoding='utf-8')
            assert "测试A (999001.SZ) - 打分：0" in content
            assert "测试B (999002.SH) - 打分：0" in content
        finally:
            svc._SCORE_LIST_FILE = orig

    def test_append_empty(self):
        assert _append_to_score_list([]) == 0


class TestLoadScoreListCodes:
    def test_load(self):
        codes = _load_score_list_codes()
        assert "002440.SZ" in codes
        assert len(codes) > 100


class TestSyncNewStocks:
    def test_no_new_stocks(self):
        """已存在的股票不应触发任何写入"""
        from service.jqka10.stock_sync_service import sync_new_stocks_sync
        stocks = [
            {"stock_code": "300812", "stock_name": "易天股份"},
        ]
        result = sync_new_stocks_sync(stocks, fetch_kline=False)
        assert result["added_to_stocks_data"] == 0
        assert result["added_to_score_list"] == 0
        assert result["total_new"] == 0

    def test_empty_input(self):
        from service.jqka10.stock_sync_service import sync_new_stocks_sync
        result = sync_new_stocks_sync([], fetch_kline=False)
        assert result["total_new"] == 0
