import pandas as pd
import json

sh_df = pd.read_excel('上海A股列表.xls')
sz_df = pd.read_excel('深圳A股列表.xlsx')

stocks = []

for _, row in sh_df.iterrows():
    code = str(row['A股代码']).zfill(6)
    name = row['证券简称']
    stocks.append({"code": f"{code}.SH", "name": name})

for _, row in sz_df.iterrows():
    code = str(row['A股代码']).zfill(6)
    name = row['A股简称']
    stocks.append({"code": f"{code}.SZ", "name": name})

with open('stocks.json', 'w', encoding='utf-8') as f:
    json.dump(stocks, f, ensure_ascii=False, indent=2)

print(f"成功转换 {len(stocks)} 只股票")
