import aiohttp
import json
from typing import Dict, Any, List, Optional

class RealTimeQuotation:
    """实时行情接口"""
    
    # 英文到中文的字段映射
    FIELD_MAPPING = {
        'tradeDate': '交易日期', 'tradeTime': '交易时间', 'preClose': '前收盘价', 'open': '开盘价',
        'high': '最高价', 'low': '最低价', 'latest': '最新价', 'latestAmount': '现额', 'latestVolume': '现量',
        'avgPrice': '均价', 'change': '涨跌', 'changeRatio': '涨跌幅', 'upperLimit': '涨停价', 'downLimit': '跌停价',
        'amount': '成交额', 'volume': '成交量', 'turnoverRatio': '换手率', 'sellVolume': '内盘', 'buyVolume': '外盘',
        'totalBidVol': '委买十档总量', 'totalAskVol': '委卖十档总量', 'totalShares': '总股本', 'totalCapital': '总市值',
        'pb': '市净率', 'riseDayCount': '连涨天数', 'suspensionFlag': '停牌标志', 'tradeStatus': '交易状态',
        'chg_1min': '1分钟涨跌幅', 'chg_3min': '3分钟涨跌幅', 'chg_5min': '5分钟涨跌幅',
        'chg_5d': '5日涨跌幅', 'chg_10d': '10日涨跌幅', 'chg_20d': '20日涨跌幅', 'chg_60d': '60日涨跌幅',
        'chg_120d': '120日涨跌幅', 'chg_250d': '250日涨跌幅', 'chg_year': '年初至今涨跌幅',
        'mv': '流通市值', 'vol_ratio': '量比', 'committee': '委比', 'commission_diff': '委差',
        'pe_ttm': '市盈率TTM', 'pbr_lf': '市净率LF', 'swing': '振幅', 'lastest_price': '最新成交价',
        'af_backward': '后复权因子', 'bid1': '买1价', 'bid2': '买2价', 'bid3': '买3价', 'bid4': '买4价', 'bid5': '买5价',
        'bid6': '买6价', 'bid7': '买7价', 'bid8': '买8价', 'bid9': '买9价', 'bid10': '买10价',
        'ask1': '卖1价', 'ask2': '卖2价', 'ask3': '卖3价', 'ask4': '卖4价', 'ask5': '卖5价',
        'ask6': '卖6价', 'ask7': '卖7价', 'ask8': '卖8价', 'ask9': '卖9价', 'ask10': '卖10价',
        'bidSize1': '买1量', 'bidSize2': '买2量', 'bidSize3': '买3量', 'bidSize4': '买4量', 'bidSize5': '买5量',
        'bidSize6': '买6量', 'bidSize7': '买7量', 'bidSize8': '买8量', 'bidSize9': '买9量', 'bidSize10': '买10量',
        'askSize1': '卖1量', 'askSize2': '卖2量', 'askSize3': '卖3量', 'askSize4': '卖4量', 'askSize5': '卖5量',
        'askSize6': '卖6量', 'askSize7': '卖7量', 'askSize8': '卖8量', 'askSize9': '卖9量', 'askSize10': '卖10量',
        'avgBuyPrice': '均买价', 'avgSellPrice': '均卖价', 'totalBuyVolume': '总买量', 'totalSellVolume': '总卖量',
        'transClassification': '成交分类', 'transTimes': '成交次数',
        'mainInflow': '主力流入金额', 'mainOutflow': '主力流出金额', 'mainNetInflow': '主力净流入金额',
        'retailInflow': '散户流入金额', 'retailOutflow': '散户流出金额', 'retailNetInflow': '散户净流入金额',
        'largeInflow': '超大单流入金额', 'largeOutflow': '超大单流出金额', 'largeNetInflow': '超大单净流入金额',
        'bigInflow': '大单流入金额', 'bigOutflow': '大单流出金额', 'bigNetInflow': '大单净流入金额',
        'middleInflow': '中单流入金额', 'middleOutflow': '中单流出金额', 'middleNetInflow': '中单净流入金额',
        'smallInflow': '小单流入金额', 'smallOutflow': '小单流出金额', 'smallNetInflow': '小单净流入金额',
        'activeBuyLargeAmt': '主动买入特大单金额', 'activeSellLargeAmt': '主动卖出特大单金额',
        'activeBuyMainAmt': '主动买入大单金额', 'activeSellMainAmt': '主动卖出大单金额',
        'activeBuyMiddleAmt': '主动买入中单金额', 'activeSellMiddleAmt': '主动卖出中单金额',
        'activeBuySmallAmt': '主动买入小单金额', 'activeSellSmallAmt': '主动卖出小单金额',
        'possitiveBuyLargeAmt': '被动买入特大单金额', 'possitiveSellLargeAmt': '被动卖出特大单金额',
        'possitiveBuyMainAmt': '被动买入大单金额', 'possitiveSellMainAmt': '被动卖出大单金额',
        'possitiveBuyMiddleAmt': '被动买入中单金额', 'possitiveSellMiddleAmt': '被动卖出中单金额',
        'possitiveBuySmallAmt': '被动买入小单金额', 'possitiveSellSmallAmt': '被动卖出小单金额',
        'activeBuyLargeVol': '主动买入特大单量', 'activeSellLargeVol': '主动卖出特大单量',
        'activeBuyMainVol': '主动买入大单量', 'activeSellMainVol': '主动卖出大单量',
        'activeBuyMiddleVol': '主动买入中单量', 'activeSellMiddleVol': '主动卖出中单量',
        'activeBuySmallVol': '主动买入小单量', 'activeSellSmallVol': '主动卖出小单量',
        'possitiveBuyLargeVol': '被动买入特大单量', 'possitiveSellLargeVol': '被动卖出特大单量',
        'possitiveBuyMainVol': '被动买入大单量', 'possitiveSellMainVol': '被动卖出大单量',
        'possitiveBuyMiddleVol': '被动买入中单量', 'possitiveSellMiddleVol': '被动卖出中单量',
        'possitiveBuySmallVol': '被动买入小单量', 'possitiveSellSmallVol': '被动卖出小单量',
        'activebuy_volume': '主买总量', 'activesell_volume': '主卖总量',
        'activebuy_amt': '主买总额', 'activesell_amt': '主卖总额',
        'post_lastest': '盘后最新成交价', 'post_latestVolume': '盘后现量',
        #'post_volume': '盘后成交量', 'post_amt': '盘后成交额', 'post_dealnum': '盘后成交笔数',
        #'priceDiff': '买卖价差', 'sharesPerHand': '每手股数', 'expiryDate': '到期日',
        #'iopv': 'IOPV净值估值', 'premium': '折价',
        #'riseCount': '上涨家数', 'fallCount': '下跌家数', 'upLimitCount': '涨停家数',
        #'downLimitCount': '跌停家数', 'suspensionCount': '停牌家数',
        #'pure_bond_value_cb': '纯债价值', 'surplus_term': '剩余期限天',
        #'dealDirection': '成交方向', 'dealtype': '成交性质',
        #'impliedVolatility': '隐含波动率', 'historyVolatility': '历史波动率',
        #'delta': 'Delta', 'gamma': 'Gamma', 'vega': 'Vega', 'theta': 'Theta', 'rho': 'Rho',
        #'pre_open_interest': '前持仓量', 'pre_implied_volatility': '前隐含波动率',
        #'volume_pcr_total': '成交量pcr品种', 'volume_pcr_month': '成交量pcr同月',
        'thscode': '股票代码', 'marketCategory': '市场类别', 'pricetype': '价格类型', 'time': '时间'
    }
    
    def __init__(self, access_token: str):
        self.url = "https://quantapi.51ifind.com/api/v1/real_time_quotation"
        self.access_token = access_token
    
    def translate_to_chinese(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """将返回结果中的英文字段转换为中文，不在映射中的字段不返回"""
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
                '时间': table.get('时间', table.get('time')),
                '数据': table.get('数据', table.get('table', {}))
            }
            result.append(parsed)
        return result
    
    async def get_quotation(
        self, 
        codes: List[str], 
        indicators: List[str] = None,
        function_para: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        获取实时行情数据
        
        Args:
            codes: 股票代码列表，如["300033.SZ", "600030.SH"]
            indicators: 指标列表，可选指标包括：
                通用指标:
                  tradeDate(交易日期), tradeTime(交易时间), preClose(前收盘价), open(开盘价),
                  high(最高价), low(最低价), latest(最新价), latestAmount(现额), latestVolume(现量),
                  avgPrice(均价), change(涨跌), changeRatio(涨跌幅), upperLimit(涨停价), downLimit(跌停价),
                  amount(成交额), volume(成交量), turnoverRatio(换手率), sellVolume(内盘), buyVolume(外盘)
                
                股票指标:
                  totalBidVol(委买十档总量), totalAskVol(委卖十档总量), totalShares(总股本), totalCapital(总市值),
                  pb(市净率), riseDayCount(连涨天数), suspensionFlag(停牌标志), tradeStatus(交易状态),
                  chg_1min(1分钟涨跌幅), chg_3min(3分钟涨跌幅), chg_5min(5分钟涨跌幅),
                  chg_5d(5日涨跌幅), chg_10d(10日涨跌幅), chg_20d(20日涨跌幅), chg_60d(60日涨跌幅),
                  chg_120d(120日涨跌幅), chg_250d(250日涨跌幅), chg_year(年初至今涨跌幅),
                  mv(流通市值), vol_ratio(量比), committee(委比), commission_diff(委差),
                  pe_ttm(市盈率TTM), pbr_lf(市净率LF), swing(振幅), lastest_price(最新成交价),
                  af_backward(后复权因子), bid1-bid10(买1-10价), ask1-ask10(卖1-10价),
                  bidSize1-bidSize10(买1-10量), askSize1-askSize10(卖1-10量),
                  avgBuyPrice(均买价), avgSellPrice(均卖价), totalBuyVolume(总买量), totalSellVolume(总卖量),
                  transClassification(成交分类), transTimes(成交次数),
                  mainInflow(主力流入金额), mainOutflow(主力流出金额), mainNetInflow(主力净流入金额),
                  retailInflow(散户流入金额), retailOutflow(散户流出金额), retailNetInflow(散户净流入金额),
                  largeInflow(超大单流入金额), largeOutflow(超大单流出金额), largeNetInflow(超大单净流入金额),
                  bigInflow(大单流入金额), bigOutflow(大单流出金额), bigNetInflow(大单净流入金额),
                  middleInflow(中单流入金额), middleOutflow(中单流出金额), middleNetInflow(中单净流入金额),
                  smallInflow(小单流入金额), smallOutflow(小单流出金额), smallNetInflow(小单净流入金额),
                  activeBuyLargeAmt(主动买入特大单金额), activeSellLargeAmt(主动卖出特大单金额),
                  activeBuyMainAmt(主动买入大单金额), activeSellMainAmt(主动卖出大单金额),
                  activeBuyMiddleAmt(主动买入中单金额), activeSellMiddleAmt(主动卖出中单金额),
                  activeBuySmallAmt(主动买入小单金额), activeSellSmallAmt(主动卖出小单金额),
                  possitiveBuyLargeAmt(被动买入特大单金额), possitiveSellLargeAmt(被动卖出特大单金额),
                  possitiveBuyMainAmt(被动买入大单金额), possitiveSellMainAmt(被动卖出大单金额),
                  possitiveBuyMiddleAmt(被动买入中单金额), possitiveSellMiddleAmt(被动卖出中单金额),
                  possitiveBuySmallAmt(被动买入小单金额), possitiveSellSmallAmt(被动卖出小单金额),
                  activeBuyLargeVol(主动买入特大单量), activeSellLargeVol(主动卖出特大单量),
                  activeBuyMainVol(主动买入大单量), activeSellMainVol(主动卖出大单量),
                  activeBuyMiddleVol(主动买入中单量), activeSellMiddleVol(主动卖出中单量),
                  activeBuySmallVol(主动买入小单量), activeSellSmallVol(主动卖出小单量),
                  possitiveBuyLargeVol(被动买入特大单量), possitiveSellLargeVol(被动卖出特大单量),
                  possitiveBuyMainVol(被动买入大单量), possitiveSellMainVol(被动卖出大单量),
                  possitiveBuyMiddleVol(被动买入中单量), possitiveSellMiddleVol(被动卖出中单量),
                  possitiveBuySmallVol(被动买入小单量), possitiveSellSmallVol(被动卖出小单量),
                  activebuy_volume(主买总量), activesell_volume(主卖总量),
                  activebuy_amt(主买总额), activesell_amt(主卖总额),
                  post_lastest(盘后最新成交价), post_latestVolume(盘后现量),
                  post_volume(盘后成交量), post_amt(盘后成交额), post_dealnum(盘后成交笔数)
                
                港股专用: priceDiff(买卖价差), sharesPerHand(每手股数), expiryDate(到期日), tradeStatus(交易状态)
                基金专用: iopv(IOPV净值估值), premium(折价)
                指数专用: riseCount(上涨家数), fallCount(下跌家数), upLimitCount(涨停家数),
                         downLimitCount(跌停家数), suspensionCount(停牌家数),
                         pure_bond_value_cb(纯债价值), surplus_term(剩余期限天)
                期货期权专用: dealDirection(成交方向), dealtype(成交性质)
                期权专用: impliedVolatility(隐含波动率), historyVolatility(历史波动率),
                         delta(Delta), gamma(Gamma), vega(Vega), theta(Theta), rho(Rho),
                         pre_open_interest(前持仓量), pre_implied_volatility(前隐含波动率),
                         volume_pcr_total(成交量pcr品种), volume_pcr_month(成交量pcr同月)
                
                默认: ["open", "high", "low", "latest"]
            function_para: 可选参数，key-value格式，如债券报价方式
            
        Returns:
            Dict包含实时行情数据
        """
        if indicators is None:
            indicators = [
                # 通用指标
                "tradeDate", "tradeTime", "preClose", "open", "high", "low", "latest",
                "latestAmount", "latestVolume", "avgPrice", "change", "changeRatio",
                "upperLimit", "downLimit", "amount", "volume", "turnoverRatio",
                "sellVolume", "buyVolume",
                # 股票指标
                "totalBidVol", "totalAskVol", "totalShares", "totalCapital", "pb",
                "riseDayCount", "suspensionFlag", "tradeStatus",
                "chg_1min", "chg_3min", "chg_5min", "chg_5d", "chg_10d", "chg_20d",
                "chg_60d", "chg_120d", "chg_250d", "chg_year",
                "mv", "vol_ratio", "committee", "commission_diff",
                "pe_ttm", "pbr_lf", "swing", "lastest_price", "af_backward",
                "bid1", "bid2", "bid3", "bid4", "bid5", "bid6", "bid7", "bid8", "bid9", "bid10",
                "ask1", "ask2", "ask3", "ask4", "ask5", "ask6", "ask7", "ask8", "ask9", "ask10",
                "bidSize1", "bidSize2", "bidSize3", "bidSize4", "bidSize5",
                "bidSize6", "bidSize7", "bidSize8", "bidSize9", "bidSize10",
                "askSize1", "askSize2", "askSize3", "askSize4", "askSize5",
                "askSize6", "askSize7", "askSize8", "askSize9", "askSize10",
                "avgBuyPrice", "avgSellPrice", "totalBuyVolume", "totalSellVolume",
                "transClassification", "transTimes",
                "mainInflow", "mainOutflow", "mainNetInflow",
                "retailInflow", "retailOutflow", "retailNetInflow",
                "largeInflow", "largeOutflow", "largeNetInflow",
                "bigInflow", "bigOutflow", "bigNetInflow",
                "middleInflow", "middleOutflow", "middleNetInflow",
                "smallInflow", "smallOutflow", "smallNetInflow",
                "activeBuyLargeAmt", "activeSellLargeAmt",
                "activeBuyMainAmt", "activeSellMainAmt",
                "activeBuyMiddleAmt", "activeSellMiddleAmt",
                "activeBuySmallAmt", "activeSellSmallAmt",
                "possitiveBuyLargeAmt", "possitiveSellLargeAmt",
                "possitiveBuyMainAmt", "possitiveSellMainAmt",
                "possitiveBuyMiddleAmt", "possitiveSellMiddleAmt",
                "possitiveBuySmallAmt", "possitiveSellSmallAmt",
                "activeBuyLargeVol", "activeSellLargeVol",
                "activeBuyMainVol", "activeSellMainVol",
                "activeBuyMiddleVol", "activeSellMiddleVol",
                "activeBuySmallVol", "activeSellSmallVol",
                "possitiveBuyLargeVol", "possitiveSellLargeVol",
                "possitiveBuyMainVol", "possitiveSellMainVol",
                "possitiveBuyMiddleVol", "possitiveSellMiddleVol",
                "possitiveBuySmallVol", "possitiveSellSmallVol",
                "activebuy_volume", "activesell_volume",
                "activebuy_amt", "activesell_amt",
                "post_lastest", "post_latestVolume", "post_volume", "post_amt", "post_dealnum",
                # 港股专用
                "priceDiff", "sharesPerHand", "expiryDate",
                # 基金专用
                "iopv", "premium",
                # 指数专用
                "riseCount", "fallCount", "upLimitCount", "downLimitCount",
                "suspensionCount", "pure_bond_value_cb", "surplus_term",
                # 期货期权专用
                "dealDirection", "dealtype",
                # 期权专用
                "impliedVolatility", "historyVolatility", "delta", "gamma", "vega",
                "theta", "rho", "pre_open_interest", "pre_implied_volatility",
                "volume_pcr_total", "volume_pcr_month"
            ]
        
        headers = {
            "access_token": self.access_token,
            "Content-Type": "application/json"
        }
        
        payload = {
            "codes": ",".join(codes),
            "indicators": ",".join(indicators)
        }
        
        if function_para:
            payload["functionpara"] = function_para
        
        async with aiohttp.ClientSession() as session:
            async with session.post(self.url, headers=headers, data=json.dumps(payload)) as response:
                result = json.loads(await response.text())
                return self.translate_to_chinese(result)

