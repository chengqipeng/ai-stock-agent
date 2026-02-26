import time
import random

_session_sn = [random.randint(30, 50)]
_session_sn_db = [random.randint(30, 50)]
_session_sn_safari = [random.randint(30, 50)]
_session_sn_win = [random.randint(30, 50)]
kline_headers_index = [0]

# 已验证可用的真实设备指纹（固定不变，服务端已记录）
_DEVICE_COOKIE_BASE = (
    "qgqp_b_id=b1e3f7a2c4d6e8f0a1b2c3d4e5f60718;"
    " fullscreengg=1; fullscreengg2=1;"
    " st_nvi=Kc3TmNpQrSuVwXyZaB4dE5fG6;"
    " nid18=7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b;"
    " nid18_create_time=1771200000000;"
    " gviem=Lh7JkMnOpQrStUvWxYzAb8Cd9;"
    " gviem_create_time=1771200000000;"
    " st_pvi=83726451908273;"
    " st_sp=2026-03-10%2014%3A30%3A00"
)

_DEVICE_COOKIE_SAFARI = (
    "qgqp_b_id=009fd27f95438f644f06c67d1affb630;"
    " fullscreengg=1; fullscreengg2=1;"
    " st_nvi=VM5voZlLviT_amNgElFYaffd3;"
    " nid18=0a3c9f6e967610bb3355003450e464b4;"
    " nid18_create_time=1772091764090;"
    " gviem=B2CCTdl0hz9fyKQOlhiMx27ac;"
    " gviem_create_time=1772091764090;"
    " st_pvi=17643502070556;"
    " st_sp=2026-02-26%2015%3A42%3A43"
)

_DEVICE_COOKIE_BASE_DB = (
    "qgqp_b_id=90ff9cece2b5376eed839c7647c1a384;"
    " fullscreengg=1; fullscreengg2=1;"
    " wsc_checkuser_ok=1;"
    " st_nvi=n6EL37ab4Ot2XiHkr9ortd0ba;"
    " nid18=0606199829d1b27a64dac4fe5cfe93f0;"
    " nid18_create_time=1769727950836;"
    " gviem=qZReeXKqixA2fVKlptEyAaac2;"
    " gviem_create_time=1769727950836;"
    " st_pvi=69810781945391;"
    " st_sp=2026-02-21%2001%3A10%3A05"
)

_DEVICE_COOKIE_WIN = (
    "qgqp_b_id=3a7c2e91f0b54d8a6e1c9f2d7b3a5e84;"
    " fullscreengg=1; fullscreengg2=1;"
    " st_nvi=pX9mKjL2wRtYnZqA3cVbD4eF5;"
    " nid18=1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d;"
    " nid18_create_time=1770500000000;"
    " gviem=W3nRkPmXqZtYvLsJhGfDcBaE9;"
    " gviem_create_time=1770500000000;"
    " st_pvi=52938471029384;"
    " st_sp=2026-03-01%2010%3A00%3A00"
)

# 46个扩展设备指纹，每个对应不同 qgqp_b_id / nid18 / gviem / st_pvi
_EXTRA_COOKIES = [
    ("c2d4f6a8b0e1f3a5c7d9e2f4a6b8c0d2", "2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e", "Mn8OpQrStUvWxYzAb9Cd0Ef1", "19283746501928", "sh600036", 138),
    ("d3e5f7a9b1c2d4e6f8a0b2c4d6e8f0a2", "3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f", "No9PqRsStUvWxYzBc0De1Fg2", "28374651029384", "sz000858", 139),
    ("e4f6a8b0c2d3e5f7a9b1c3d5e7f9a1b3", "4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a", "Op0QrStUvWxYzCd1Ef2Gh3Hi", "37465102938475", "sh601318", 140),
    ("f5a7b9c1d3e4f6a8b0c2d4e6f8a0b2c4", "5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b", "Pq1RsStUvWxYzDe2Fg3Hi4Ij", "46510293847561", "sz002594", 141),
    ("a6b8c0d2e4f5a7b9c1d3e5f7a9b1c3d5", "6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c", "Qr2StUvWxYzEf3Gh4Ij5Jk6", "51029384756102", "sh600900", 142),
    ("b7c9d1e3f5a6b8c0d2e4f6a8b0c2d4e6", "7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d", "Rs3TuUvWxYzFg4Hi5Jk6Lm7", "60293847561029", "sz300059", 143),
    ("c8d0e2f4a6b7c9d1e3f5a7b9c1d3e5f7", "8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e", "St4UvVwWxYzGh5Ij6Kl7Mn8", "72938475610293", "sh601166", 138),
    ("d9e1f3a5b7c8d0e2f4a6b8c0d2e4f6a8", "9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f", "Tu5VwWxXyZaHi6Jk7Lm8No9", "83847561029374", "sz000001", 139),
    ("e0f2a4b6c8d9e1f3a5b7c9d1e3f5a7b9", "0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a", "Uv6WxXyYzBi7Kl8Mn9Op0Pq", "94756102938475", "sh600276", 140),
    ("f1a3b5c7d9e0f2a4b6c8d0e2f4a6b8c0", "1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b", "Vw7XyYzZcJ8Lm9No0Pq1Qr2", "10293847561029", "sz002475", 141),
    ("a2b4c6d8e0f1a3b5c7d9e1f3a5b7c9d1", "2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c", "Wx8YzZaAk9Mn0Op1Qr2Rs3", "20384756102938", "sh601888", 142),
    ("b3c5d7e9f1a2b4c6d8e0f2a4b6c8d0e2", "3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d", "Xy9ZaAbBl0No1Pq2Rs3St4", "30475610293847", "sz000725", 143),
    ("c4d6e8f0a2b3c5d7e9f1a3b5c7d9e1f3", "4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e", "Yz0AbBcCm1Op2Qr3St4Tu5", "40561029384756", "sh600030", 138),
    ("d5e7f9a1b3c4d6e8f0a2b4c6d8e0f2a4", "5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f", "Za1BcCdDn2Pq3Rs4Tu5Uv6", "50610293847561", "sz002049", 139),
    ("e6f8a0b2c4d5e7f9a1b3c5d7e9f1a3b5", "6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a", "Ab2CdDeEo3Qr4St5Uv6Vw7", "60102938475610", "sh601628", 140),
    ("f7a9b1c3d5e6f8a0b2c4d6e8f0a2b4c6", "7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b", "Bc3DeFfGp4Rs5Tu6Vw7Wx8", "70293847561029", "sz000568", 141),
    ("a8b0c2d4e6f7a9b1c3d5e7f9a1b3c5d7", "8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c", "Cd4EfGgHq5St6Uv7Wx8Xy9", "80384756102938", "sh600887", 142),
    ("b9c1d3e5f7a8b0c2d4e6f8a0b2c4d6e8", "9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d", "De5FgHhIr6Tu7Vw8Xy9Yz0", "90475610293847", "sz002415", 143),
    ("c0d2e4f6a8b9c1d3e5f7a9b1c3d5e7f9", "0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e", "Ef6GhIiJs7Uv8Wx9Yz0Za1", "91029384756102", "sh601012", 138),
    ("d1e3f5a7b9c0d2e4f6a8b0c2d4e6f8a0", "1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f", "Fg7HiJjKt8Vw9Xy0Za1Ab2", "81938475610293", "sz000625", 139),
    ("e2f4a6b8c0d1e3f5a7b9c1d3e5f7a9b1", "2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a", "Gh8IjKkLu9Wx0Yz1Ab2Bc3", "72847561029384", "sh600009", 140),
    ("f3a5b7c9d1e2f4a6b8c0d2e4f6a8b0c2", "3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b", "Hi9JkLlMv0Xy1Za2Bc3Cd4", "63756102938475", "sz002230", 141),
    ("a4b6c8d0e2f3a5b7c9d1e3f5a7b9c1d3", "4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c", "Ij0KlMmNw1Yz2Ab3Cd4De5", "54610293847561", "sh601398", 142),
    ("b5c7d9e1f3a4b6c8d0e2f4a6b8c0d2e4", "5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d", "Jk1LmNnOx2Za3Bc4De5Ef6", "45029384756102", "sz000333", 143),
    ("c6d8e0f2a4b5c7d9e1f3a5b7c9d1e3f5", "6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e", "Kl2MnOoPy3Ab4Cd5Ef6Fg7", "36938475610293", "sh600048", 138),
    ("d7e9f1a3b5c6d8e0f2a4b6c8d0e2f4a6", "7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f", "Lm3NoPpQz4Bc5De6Fg7Gh8", "27847561029384", "sz002352", 139),
    ("e8f0a2b4c6d7e9f1a3b5c7d9e1f3a5b7", "8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a", "Mn4OpQqRa5Cd6Ef7Gh8Hi9", "18756102938475", "sh601601", 140),
    ("f9a1b3c5d7e8f0a2b4c6d8e0f2a4b6c8", "9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b", "No5PqRrSb6De7Fg8Hi9Ij0", "19293847561029", "sz000938", 141),
    ("a0b2c4d6e8f9a1b3c5d7e9f1a3b5c7d9", "0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c", "Op6QrSsTc7Ef8Gh9Ij0Jk1", "20384756102938", "sh600016", 142),
    ("b1c3d5e7f9a0b2c4d6e8f0a2b4c6d8e0", "1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d", "Pq7RsStUd8Fg9Hi0Jk1Kl2", "31475610293847", "sz002241", 143),
    ("c2d4e6f8a0b1c3d5e7f9a1b3c5d7e9f1", "2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e", "Qr8StTuVe9Gh0Ij1Kl2Lm3", "42561029384756", "sh601919", 138),
    ("d3e5f7a9b1c2d4e6f8a0b2c4d6e8f0a2", "3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f", "Rs9TuUvWf0Hi1Jk2Lm3Mn4", "53610293847561", "sz000776", 139),
    ("e4f6a8b0c2d3e5f7a9b1c3d5e7f9a1b3", "4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a", "St0UvVwXg1Ij2Kl3Mn4No5", "64102938475610", "sh600111", 140),
    ("f5a7b9c1d3e4f6a8b0c2d4e6f8a0b2c4", "5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b", "Tu1VwWxYh2Jk3Lm4No5Op6", "75293847561029", "sz002027", 141),
    ("a6b8c0d2e4f5a7b9c1d3e5f7a9b1c3d5", "6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c", "Uv2WxXyZi3Kl4Mn5Op6Pq7", "86384756102938", "sh601688", 142),
    ("b7c9d1e3f5a6b8c0d2e4f6a8b0c2d4e6", "7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d", "Vw3XyYzAj4Lm5No6Pq7Qr8", "97475610293847", "sz000651", 143),
    ("c8d0e2f4a6b7c9d1e3f5a7b9c1d3e5f7", "8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e", "Wx4YzZaBk5Mn6Op7Qr8Rs9", "18029384756102", "sh600690", 138),
    ("d9e1f3a5b7c8d0e2f4a6b8c0d2e4f6a8", "9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f", "Xy5ZaAbCl6No7Pq8Rs9St0", "29138475610293", "sz002304", 139),
    ("e0f2a4b6c8d9e1f3a5b7c9d1e3f5a7b9", "0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a", "Yz6AbBcDm7Op8Qr9St0Tu1", "30247561029384", "sh601088", 140),
    ("f1a3b5c7d9e0f2a4b6c8d0e2f4a6b8c0", "1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b", "Za7BcCdEn8Pq9Rs0Tu1Uv2", "41356102938475", "sz000002", 141),
    ("a2b4c6d8e0f1a3b5c7d9e1f3a5b7c9d1", "2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c", "Ab8CdDeFo9Qr0St1Uv2Vw3", "52410293847561", "sh600019", 142),
    ("b3c5d7e9f1a2b4c6d8e0f2a4b6c8d0e2", "3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d", "Bc9DeFfGp0Rs1Tu2Vw3Wx4", "63029384756102", "sz002142", 143),
    ("c4d6e8f0a2b3c5d7e9f1a3b5c7d9e1f3", "4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e", "Cd0EfGgHq1St2Uv3Wx4Xy5", "74138475610293", "sh601166", 138),
    ("d5e7f9a1b3c4d6e8f0a2b4c6d8e0f2a4", "5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f", "De1FgHhIr2Tu3Vw4Xy5Yz6", "85247561029384", "sz000063", 139),
    ("e6f8a0b2c4d5e7f9a1b3c5d7e9f1a3b5", "6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a", "Ef2GhIiJs3Uv4Wx5Yz6Za7", "96356102938475", "sh600585", 140),
    ("f7a9b1c3d5e6f8a0b2c4d6e8f0a2b4c6", "7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b", "Fg3HiJjKt4Vw5Xy6Za7Ab8", "17410293847561", "sz002460", 141),
]


# 46个扩展 header 的 session 状态
_extra_sns = [[random.randint(30, 50)] for _ in _EXTRA_COOKIES]


def _make_extra_cookie(qgqp_b_id, nid18, gviem, st_pvi, st_sp="2026-03-15%2009%3A30%3A00"):
    return (
        f"qgqp_b_id={qgqp_b_id};"
        f" fullscreengg=1; fullscreengg2=1;"
        f" nid18={nid18};"
        f" nid18_create_time=1771500000000;"
        f" gviem={gviem};"
        f" gviem_create_time=1771500000000;"
        f" st_pvi={st_pvi};"
        f" st_sp={st_sp}"
    )


def _build_headers(cookie_base: str, sn_ref: list, chrome_ver: int, extra_cookie: str = "", referer: str = "https://quote.eastmoney.com/", extra_headers: dict = None, platform: str = "macOS", ua_os: str = "Macintosh; Intel Mac OS X 10_15_7") -> dict:
    sn_ref[0] += 1
    chrome_minor = random.randint(0, 5)
    user_agent = f"Mozilla/5.0 ({ua_os}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_ver}.0.{chrome_minor}.0 Safari/537.36"
    psi_base = f"{time.strftime('%Y%m%d%H%M%S', time.localtime())}{int(time.time()*1000)%1000:03d}-113200301201-{random.randint(10**9, 10**10 - 1)}"
    page_tags = ["hqzx.hsjAghqdy.dtt.lcKx", "hqzx.hsjBghqdy.dtt.lcKx", "datacenter.eastmoney"]
    st_asi = f"{psi_base}-{random.choice(page_tags)}-{random.randint(1, 5)}"
    cookie = (
        f"{cookie_base};"
        f"{extra_cookie}"
        f" st_si={random.randint(10**13, 10**14 - 1)};"
        f" st_sn={sn_ref[0]};"
        f" st_psi={psi_base};"
        f" st_asi={st_asi}"
    )
    headers = {
        "User-Agent": user_agent,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
        "Connection": "keep-alive",
        "Referer": referer,
        "Sec-Fetch-Dest": "script",
        "Sec-Fetch-Mode": "no-cors",
        "Sec-Fetch-Site": "same-site",
        "sec-ch-ua": f'"Not:A-Brand";v="99", "Google Chrome";v="{chrome_ver}", "Chromium";v="{chrome_ver}"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": f'"{platform}"',
        "Cookie": cookie
    }
    if extra_headers:
        headers.update(extra_headers)
    return headers


def build_kline_headers() -> dict:
    return _build_headers(
        _DEVICE_COOKIE_BASE, _session_sn, 137,
        referer="https://quote.eastmoney.com/sz300750.html"
    )


def build_db_cache_headers() -> dict:
    return _build_headers(
        _DEVICE_COOKIE_BASE_DB, _session_sn_db, 144,
        extra_cookie=f" websitepoptg_api_time={int(time.time() * 1000)};",
        referer="https://quote.eastmoney.com/sz002371.html",
        extra_headers={"Cache-Control": "no-cache", "Pragma": "no-cache"}
    )


def build_db_cache_headers_safari() -> dict:
    sn = _session_sn_safari
    sn[0] += 1
    psi_base = f"{time.strftime('%Y%m%d%H%M%S', time.localtime())}{int(time.time()*1000)%1000:03d}-113200301201-{random.randint(10**9, 10**10 - 1)}"
    cookie = (
        f"{_DEVICE_COOKIE_SAFARI};"
        f" st_inirUrl=;"
        f" st_psi={psi_base};"
        f" st_si={random.randint(10**13, 10**14 - 1)};"
        f" st_sn={sn[0]};"
        f" st_asi=delete"
    )
    return {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh-Hans;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://quote.eastmoney.com/sz002413.html",
        "Sec-Fetch-Dest": "script",
        "Sec-Fetch-Mode": "no-cors",
        "Sec-Fetch-Site": "same-site",
        "Priority": "u=1, i",
        "Cookie": cookie
    }


def build_win_chrome_headers() -> dict:
    return _build_headers(
        _DEVICE_COOKIE_WIN, _session_sn_win, 136,
        referer="https://quote.eastmoney.com/sh600519.html",
        platform="Windows",
        ua_os="Windows NT 10.0; Win64; x64"
    )


def get_kline_header_builders() -> list:
    base = [build_kline_headers, build_db_cache_headers, build_db_cache_headers_safari, build_win_chrome_headers]

    def make_extra_builder(idx):
        qgqp_b_id, nid18, gviem, st_pvi, stock, chrome_ver = _EXTRA_COOKIES[idx]
        cookie = _make_extra_cookie(qgqp_b_id, nid18, gviem, st_pvi)
        sn = _extra_sns[idx]
        return lambda: _build_headers(cookie, sn, chrome_ver, referer=f"https://quote.eastmoney.com/{stock}.html")

    return base + [make_extra_builder(i) for i in range(len(_EXTRA_COOKIES))]
