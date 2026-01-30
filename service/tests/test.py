import aiohttp
import asyncio
import json


# 1. 数值格式化函数：亿/万单位转换
def cal_num(num):
    if num is None:
        return "--"
    # 判断是否达到亿级
    if abs(num / 100000000) >= 1:  # 原逻辑微调为 >=1 更准确
        return str(round(num / 100000000, 3)) + "亿"
    else:
        return str(round(num / 10000, 3)) + "万"


# 2. 组装参数函数
def compose_params(code):
    # 构造过滤条件，限制证券代码和起始日期
    prm = f"(SECURITY_CODE={code})(TRADE_DATE>='2025-10-29')"
    prms = {
        "sortColumns": "TRADE_DATE",
        "sortTypes": "-1",
        "pageSize": "100",
        "pageNumber": "1",
        "reportName": "RPT_MUTUAL_HOLDSTOCKNORTH_STA",
        "columns": "ALL",
        "filter": prm,
    }

    # 将字典拼接为 URL 参数字符串
    result = ""
    for key, val in prms.items():
        result = result + key + "=" + str(val) + "&"
    result = result[:-1]
    return result


# 3. 异步查询主函数
async def query_north_hold_detail(code):
    params_str = compose_params(code)
    server = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    url = f"{server}?{params_str}"

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    resp_text = await response.text()
                    json_body = json.loads(resp_text)

                    json_result = json_body.get("result")
                    if not json_result:
                        print(f"未能获取到代码 {code} 的数据。")
                        return

                    data_list = json_result.get("data", [])

                    # 定义表头
                    title_list = ["日期", "代码", "名称", "价格", "涨跌幅", "持股数", "持股市值", "比例", "1日变",
                                  "5日变", "10日变"]
                    print("\t".join(title_list))
                    print("-" * 120)

                    for node in data_list:
                        # 提取并转换字段
                        date = node["TRADE_DATE"].replace(" 00:00:00", "")
                        name = node["SECURITY_NAME"]
                        price = node["CLOSE_PRICE"]
                        rate = f"{round(node['CHANGE_RATE'], 2)}%"

                        # 调用你提供的 cal_num 进行格式化
                        share = cal_num(node["HOLD_SHARES"])
                        cap = cal_num(node["HOLD_MARKET_CAP"])
                        ratio = f"{node['A_SHARES_RATIO']}%"
                        chg1 = cal_num(node["HOLD_MARKETCAP_CHG1"])
                        chg5 = cal_num(node["HOLD_MARKETCAP_CHG5"])
                        chg10 = cal_num(node["HOLD_MARKETCAP_CHG10"])

                        row = [date, code, name, price, rate, share, cap, ratio, chg1, chg5, chg10]
                        # 普通打印处理
                        print("\t".join(map(str, row)))
                else:
                    print(f"接口请求失败，状态码：{response.status}")
        except Exception as e:
            print(f"运行出错：{e}")


# 4. 执行异步任务
if __name__ == "__main__":
    # 以北向资金重仓股为例
    target_code = "600519"
    asyncio.run(query_north_hold_detail(target_code))