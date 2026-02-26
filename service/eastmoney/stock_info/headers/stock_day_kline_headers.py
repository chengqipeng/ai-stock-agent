import time
import random

_session_sn = [random.randint(30, 50)]
_session_sn_db = [random.randint(30, 50)]
_session_sn_safari = [random.randint(30, 50)]
_session_sn_win = [random.randint(30, 50)]
kline_headers_index = [0]

# 已验证可用的真实设备指纹（固定不变，服务端已记录）
_DEVICE_COOKIE_BASE = (
    "qgqp_b_id=f4748f77325434072983eb6c8d3b1787;"
    " websitepoptg_api_time=1771929823568;"
    " st_nvi=mGKfIoG14uDZGoXVC5f25e1e4;"
    " nid18=0f512d6ee90e691d53d979bde12a1561;"
    " nid18_create_time=1771929823775;"
    " gviem=HLIMP8z85-dn3-VQzTHLLcfbb;"
    " gviem_create_time=1771929823775;"
    " fullscreengg=1; fullscreengg2=1;"
    " st_pvi=37471974443836;"
    " st_sp=2026-02-24%2018%3A43%3A43"
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
    return _build_headers(_DEVICE_COOKIE_BASE, _session_sn, 145)


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
    return [build_kline_headers, build_db_cache_headers, build_db_cache_headers_safari, build_win_chrome_headers]
