import aiohttp
import json
from typing import List, Dict, Any, Optional


class HistoryQuotationService:
    """历史行情数据服务"""
    
    # 英文到中文的字段映射
    FIELD_MAPPING = {
        'preClose': '前收盘价', 'open': '开盘价', 'high': '最高价', 'low': '最低价', 'close': '收盘价',
        'avgPrice': '均价', 'change': '涨跌', 'changeRatio': '涨跌幅', 'volume': '成交量', 'amount': '成交额',
        'turnoverRatio': '换手率', 'transactionAmount': '成交笔数', 'totalShares': '总股本', 'totalCapital': '总市值',
        'floatSharesOfAShares': 'A股流通股本',
        #'floatSharesOfBShares': 'B股流通股本',
        'floatCapitalOfAShares': 'A股流通市值',
        #'floatCapitalOfBShares': 'B股流通市值',
        'pe_ttm': '市盈率TTM', 'pe': 'PE市盈率', 'pb': 'PB市净率', 'ps': 'PS市销率', 'pcf': 'PCF市现率',
        #'ths_trading_status_stock': '交易状态',
        'ths_up_and_down_status_stock': '涨跌停状态',
        'ths_af_stock': '复权因子', 'ths_vaild_turnover_stock': '有效换手率',
        #'ths_vol_after_trading_stock': '盘后成交量', 'ths_trans_num_after_trading_stock': '盘后成交笔数',
        #'ths_amt_after_trading_stock': '盘后成交额',
        #'netAssetValue': '单位净值', 'adjustedNAV': '复权单位净值', 'accumulatedNAV': '累计单位净值',
        #'premium': '贴水', 'premiumRatio': '贴水率', 'estimatedPosition': '估算仓位',
        #'floatCapital': '流通市值', 'pe_ttm_index': 'PE_TTM', 'pb_mrq': 'PB_MRQ', 'pe_indexPublisher': 'PE指数发布方',
        #'yieldMaturity': '到期收益率', 'remainingTerm': '剩余期限', 'maxwellDuration': '麦氏久期',
        #'modifiedDuration': '修正久期', 'convexity': '凸性', 'close_2330': '收盘价23:30',
        #'preSettlement': '前结算价', 'settlement': '结算价', 'change_settlement': '涨跌结算价',
        #'chg_settlement': '涨跌幅结算价', 'openInterest': '持仓量', 'positionChange': '持仓变动', 'amplitude': '振幅',
        'thscode': '股票代码', 'time': '时间', 'date': '日期'
    }
    
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.url = "https://quantapi.51ifind.com/api/v1/cmd_history_quotation"
    
    def translate_to_chinese(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """将返回结果中的英文字段转换为中文"""
        if 'tables' not in data:
            return data
        
        translated_tables = []
        for table in data['tables']:
            translated_table = {}
            for key, value in table.items():
                if key == 'table':
                    translated_table['数据'] = {self.FIELD_MAPPING[k]: v for k, v in value.items() if k in self.FIELD_MAPPING}
                elif key in self.FIELD_MAPPING:
                    translated_table[self.FIELD_MAPPING[key]] = value
            translated_tables.append(translated_table)
        
        return {**data, 'tables': translated_tables}
    
    @staticmethod
    def parse_tables(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """解析tables数据，只保留股票代码、时间和数据"""
        tables = data.get('tables', [])
        result = []
        for table in tables:
            parsed = {
                '股票代码': table.get('股票代码', table.get('thscode')),
                '时间': table.get('时间', table.get('time', table.get('date'))),
                '数据': table.get('数据', table.get('table', {}))
            }
            result.append(parsed)
        return result
    
    async def get_history_quotation(
        self,
        codes: List[str],
        start_date: str,
        end_date: str,
        indicators: List[str] = None,
        function_para: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        获取历史行情数据
        
        Args:
            codes: 股票代码列表，如["300033.SZ", "600030.SH"]
            indicators: 指标列表，支持的指标包括：
                基础行情: preClose(前收盘价), open(开盘价), high(最高价), low(最低价), 
                         close(收盘价), avgPrice(均价), change(涨跌), changeRatio(涨跌幅),
                         volume(成交量), amount(成交额), turnoverRatio(换手率), transactionAmount(成交笔数)
                市值相关: totalShares(总股本), totalCapital(总市值), 
                         floatSharesOfAShares(A股流通股本), floatSharesOfBShares(B股流通股本),
                         floatCapitalOfAShares(A股流通市值), floatCapitalOfBShares(B股流通市值)
                估值指标: pe_ttm(市盈率TTM), pe(PE市盈率), pb(PB市净率), ps(PS市销率), pcf(PCF市现率)
                状态指标: ths_trading_status_stock(交易状态), ths_up_and_down_status_stock(涨跌停状态),
                         ths_af_stock(复权因子), ths_vaild_turnover_stock(有效换手率)
                盘后数据: ths_vol_after_trading_stock(盘后成交量), ths_trans_num_after_trading_stock(盘后成交笔数),
                         ths_amt_after_trading_stock(盘后成交额)
                基金专用: netAssetValue(单位净值), adjustedNAV(复权单位净值), accumulatedNAV(累计单位净值),
                         premium(贴水), premiumRatio(贴水率), estimatedPosition(估算仓位)
                指数专用: floatCapital(流通市值), pe_ttm_index(PE_TTM), pb_mrq(PB_MRQ), pe_indexPublisher(PE指数发布方)
                债券专用: yieldMaturity(到期收益率), remainingTerm(剩余期限), maxwellDuration(麦氏久期),
                         modifiedDuration(修正久期), convexity(凸性), close_2330(收盘价23:30)
                期货专用: preSettlement(前结算价), settlement(结算价), change_settlement(涨跌结算价),
                         chg_settlement(涨跌幅结算价), openInterest(持仓量), positionChange(持仓变动), amplitude(振幅)
                期权专用: openInterest(持仓量), positionChange(持仓变动)
            start_date: 开始日期，支持"YYYYMMDD"、"YYYY-MM-DD"、"YYYY/MM/DD"格式
            end_date: 结束日期，支持"YYYYMMDD"、"YYYY-MM-DD"、"YYYY/MM/DD"格式
            function_para: 可选参数，包括：
                Interval: 时间周期 D-日 W-周 M-月 Q-季 S-半年 Y-年（默认D）
                SampleInterval: 抽样周期 D-日 W-周 M-月 Q-季 S-半年 Y-年（默认D）
                CPS: 复权方式 1-不复权 2-前复权(分红再投) 3-后复权(分红再投) 4-全流通前复权 
                     5-全流通后复权 6-前复权(现金分红) 7-后复权(现金分红)（默认1）
                PriceType: 报价类型 1-全价 2-净价（仅债券，默认1）
                Fill: 非交易间隔处理 Previous-沿用之前 Blank-空值 Omit-缺省值（默认Previous）
                BaseDate: 复权基点日期 "YYYY-MM-DD"
                Currency: 货币 MHB-美元 GHB-港元 RMB-人民币 YSHB-原始货币（默认YSHB）
        
        Returns:
            Dict包含历史行情数据
        """
        if indicators is None:
            indicators = [
                "preClose", "open", "high", "low", "close", "avgPrice", "change", "changeRatio",
                "volume", "amount", "turnoverRatio", "transactionAmount",
                "totalShares", "totalCapital", "floatSharesOfAShares", "floatSharesOfBShares",
                "floatCapitalOfAShares", "floatCapitalOfBShares",
                "pe_ttm", "pe", "pb", "ps", "pcf",
                "ths_trading_status_stock", "ths_up_and_down_status_stock", "ths_af_stock", "ths_vaild_turnover_stock",
                "ths_vol_after_trading_stock", "ths_trans_num_after_trading_stock", "ths_amt_after_trading_stock",
                "netAssetValue", "adjustedNAV", "accumulatedNAV", "premium", "premiumRatio", "estimatedPosition",
                "floatCapital", "pe_ttm_index", "pb_mrq", "pe_indexPublisher",
                "yieldMaturity", "remainingTerm", "maxwellDuration", "modifiedDuration", "convexity", "close_2330",
                "preSettlement", "settlement", "change_settlement", "chg_settlement", "openInterest", "positionChange", "amplitude"
            ]
        
        headers = {
            "access_token": self.access_token,
            "Content-Type": "application/json"
        }
        
        payload = {
            "codes": ",".join(codes),
            "indicators": ",".join(indicators),
            "startdate": start_date,
            "enddate": end_date
        }
        
        if function_para:
            payload["functionpara"] = function_para
        
        async with aiohttp.ClientSession() as session:
            async with session.post(self.url, headers=headers, data=json.dumps(payload)) as response:
                result = json.loads(await response.text())
                return self.translate_to_chinese(result)
