import urllib.request, re, json

headers = {
    'Accept': '*/*',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    'Referer': 'http://q.10jqka.com.cn/',
}

# Try the board detail page first to find kline data source
url = 'http://q.10jqka.com.cn/gn/detail/code/309264/'
req = urllib.request.Request(url, headers=headers)
with urllib.request.urlopen(req, timeout=20) as resp:
    raw = resp.read()
    html = raw.decode('gbk', errors='replace')

# Look for chart/kline related scripts or data
for pattern in ['kline', 'chart', 'ifind', 'd.10jqka', 'hexin-v', 'hxc3', 'board_code', 'gnid', 'boardid']:
    idx = html.lower().find(pattern)
    if idx >= 0:
        print(f'Found "{pattern}" at {idx}:')
        print(repr(html[max(0,idx-80):idx+200]))
        print()
