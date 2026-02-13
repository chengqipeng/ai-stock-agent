from datetime import datetime
from service.eastmoney.stock_info.stock_financial_main import get_financial_data_to_json
from service.eastmoney.stock_info.stock_financial_main_with_total_share import get_equity_data_to_json


async def calculate_eps_from_deducted_profit(secucode: str) -> list:
    """计算扣非每股收益（扣非净利润/总股本）"""
    financial_data = await get_financial_data_to_json(secucode, indicator_keys=['KCFJCXSYJLR', 'REPORT_DATE'])
    equity_data = await get_equity_data_to_json(secucode)

    if not financial_data or not equity_data:
        return []

    def parse_amount(amount_str):
        """解析金额字符串，如'51.0208亿'转换为数值"""
        if not amount_str or amount_str == '-':
            return None
        try:
            if '亿' in amount_str:
                return float(amount_str.replace('亿', '')) * 100000000
            elif '万' in amount_str:
                return float(amount_str.replace('万', '')) * 10000
            else:
                return float(amount_str)
        except:
            return None

    result = []
    for fin in financial_data:
        report_date = fin.get('报告日期', '')
        deduct_profit_str = fin.get('扣非净利润(元)')
        deduct_profit = parse_amount(deduct_profit_str)

        equity_match = next((eq for eq in equity_data if eq.get('变动日期', '').startswith(report_date[:7])), None) if report_date else None
        
        if not equity_match and report_date:
            try:
                target_date = datetime.strptime(report_date[:10], '%Y-%m-%d')
                closest_eq = min(
                    (eq for eq in equity_data if eq.get('变动日期')),
                    key=lambda eq: abs((datetime.strptime(eq['变动日期'][:10], '%Y-%m-%d') - target_date).days),
                    default=None
                )
                equity_match = closest_eq
            except (ValueError, KeyError):
                pass
        
        total_shares = equity_match.get('总股本(股)') if equity_match else None

        if deduct_profit and total_shares and total_shares != 0:
            eps = round(deduct_profit / total_shares, 4)
        else:
            eps = None

        result.append({
            '报告期': fin.get('报告期', ''),
            '报告日期': fin.get('报告日期', ''),
            '扣非净利润(元)': deduct_profit_str,
            '总股本(万股)': round(total_shares / 10000, 4) if total_shares else None,
            '扣非每股收益(元)': eps
        })

    return result
