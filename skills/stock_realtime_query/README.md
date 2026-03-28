# stock_realtime_query — 个股盘口与分时数据查询

纯数据抓取模块，无数据库依赖。

## 依赖

```
aiohttp
```

## 使用

```python
import asyncio
from skills.stock_realtime_query import fetch_order_book, fetch_time_data

async def main():
    # 五档盘口 + 外盘/内盘（腾讯财经）
    ob = await fetch_order_book("600519.SH")
    print(ob)
    # {'current_price': 1416.02, 'outer_vol': 16815, 'inner_vol': 13272,
    #  'buy1_price': 1416.02, 'sell1_price': 1416.10, ...}

    # 当日分时数据（同花顺）
    td = await fetch_time_data("600519.SH")
    print(f"共 {len(td)} 条")

asyncio.run(main())
```

## 模块结构

```
skills/stock_realtime_query/
├── __init__.py              # 包入口
├── order_book_fetcher.py    # 五档盘口实时抓取（腾讯财经 qt.gtimg.cn）
├── time_data_fetcher.py     # 分时数据实时抓取（同花顺 d.10jqka.com.cn）
└── README.md
```

## 字段说明

### 盘口数据 (fetch_order_book)

| 字段 | 说明 |
|------|------|
| current_price | 当前价 |
| open_price | 开盘价 |
| prev_close | 昨收价 |
| high_price / low_price | 最高/最低价 |
| volume | 成交量（手） |
| amount | 成交额（自动转换单位） |
| outer_vol | 外盘（手） |
| inner_vol | 内盘（手） |
| buy1_price ~ buy5_price | 买一~买五价格 |
| buy1_vol ~ buy5_vol | 买一~买五量（手） |
| sell1_price ~ sell5_price | 卖一~卖五价格 |
| sell1_vol ~ sell5_vol | 卖一~卖五量（手） |

### 分时数据 (fetch_time_data)

| 字段 | 说明 |
|------|------|
| time | 时间 (HH:MM) |
| close_price | 当前价格 |
| trading_amount | 成交额 |
| avg_price | 均价 |
| trading_volume | 成交量 |
| change_percent | 涨跌幅 (%) |
