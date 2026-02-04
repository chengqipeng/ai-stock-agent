import asyncio
from service.gemini.stream_generate import GeminiService
from service.gemini.parser import parse_gemini_stream_response

async def main():
    cookies = "_gcl_au=1.1.1516393348.1763729168; _ga=GA1.1.949038794.1763729169; SEARCH_SAMESITE=CgQIwZ8B; __Secure-BUCKET=CLkC; __Secure-ENID=30.SE=g3T85coHgVfq8Q_a0UyET_BFZ6G78c8Jgn1BLNpKNbO2T3WqmfFXjjNkJYcLNr_gW2zOlAjCcKceXPPflvFafRohA-zknxP-gph6M7Pk6PNsjzHlRkK35NLGthcrLAcpEhOaZzlGm-WForlZCE_jdqGAaQE7LgWM2HV8p9ZKZ2EFiJJxTjjVVLdq2QH6FF7jsp5-R44ZF8uP44MHR7CogxUwbwN9Q44OgJUvzDReZiz1937NYecidRVZOg; SID=g.a0006AjGdoJMV9YoY6xLNi39p65Ho4Y8J64iU6DSS97xS9-hjoXLcCb6spxTNMF8W3HVOtAkoQACgYKAWMSARESFQHGX2MibsPeQwLKHFYsqz0vPOrXtRoVAUF8yKq7llDeTOSjAoTwf9UEXZjm0076; __Secure-1PSID=g.a0006AjGdoJMV9YoY6xLNi39p65Ho4Y8J64iU6DSS97xS9-hjoXLZF75FhNUVdGxFcYYhBlJOAACgYKAc4SARESFQHGX2MifDB8wfXteZLheMopbsIBsBoVAUF8yKovlzg67k66LnqvfD-R-Zml0076; __Secure-3PSID=g.a0006AjGdoJMV9YoY6xLNi39p65Ho4Y8J64iU6DSS97xS9-hjoXLvfNrwI8KkYV46rPwfk09xgACgYKAY8SARESFQHGX2MiJvcZYNXsFqcPozTJ7QvCOBoVAUF8yKpbejFIL1B2wifKlVEaEz8G0076; HSID=A5XSQMdKvnG8uRc1m; SSID=AF0AJotjZ4lzDKDFD; APISID=wzkpoCMZ_TZdlISv/AuVlwm6ezW29PRg72; SAPISID=5f_p7D_9QEy3ETNA/AYsUb_sJ1OZFo4R6o; __Secure-1PAPISID=5f_p7D_9QEy3ETNA/AYsUb_sJ1OZFo4R6o; __Secure-3PAPISID=5f_p7D_9QEy3ETNA/AYsUb_sJ1OZFo4R6o; S=billing-ui-v3=59Id8PcXojqcFfPTpjYbe3UD-LdEFmhM:billing-ui-v3-efe=59Id8PcXojqcFfPTpjYbe3UD-LdEFmhM; COMPASS=gemini-pd=CjwACWuJV93jFYb_b6k1ZbZc5AVi75OXfwVJx6huPFdJgLZgT-iphNSBtyIyTho-2Gurv4U86El7hPmdVFUQuuiLzAYaXQAJa4lXnqJx4gXk9zvhy1q10WQrRMnG8G9fTHk2jvKIu0mTZmOiCuvFDsXH12Ir-E8p3oGWds0RuYp643WmILSrRIwMqKEtz4d2gTbBdp9qS6_WVZWX0zhA5o5i1yABMAE:gemini-hl=CkkACWuJV4Jq7gXnYGXm-CCWRGf1MNczIJ0yMsen8R98zb0fdd_v1HDcw_-Y0Gxw7WZu_GGVl89NUAGecp6EG6tM_DjudIlkdiK-EKep7csGGmoACWuJV7Lg1UsZQy6wrea7RbcYXrgIBMhT7j2dA3F7F8d80C4l18yb6WVs8qlK7QSQjj7jmPGvzUn5cebPhh4efvhlaRKsIx8742fzS6iZJ-tQOZfmiFBBZBLolITGCafRJlBZ9-u5gJGrIAEwAQ; AEC=AaJma5utKdDnuMMO6KUGnAsMcVqFXxM_BCn-w2iJaez685RdOzZg9as3xg; NID=528=hBiY1C1iKVEoE1vifRADly2ojdptOMBatZS51vheNHMU2iKFQncoC9OSaf0_kyw51HdgK_W7qrI5Owq7qQZ-3hI92aRuaQ2T3aKalLwkN7a0q6TKkD1rJ78Z3tPvD_iBHQ09Z_V7FbDeNQEs1MZTyWx0ChLTYsKrr00zdBDAxxqwU4ehZq5igYeAcwIOF3gcsoOaNIIYZxaeXBnYj1342h45cnu94AAsfmfVq7N_0kgL_KWAgoi3BGfHLV9e8dGI6cidYnjGWDGyULJXwQOsOXgo1lFuVsLHt2D1AAfmZLrxiTVVBe3PdxUJ-sS6mVUM_6v4-l5T_yJoiwzbEOEtPuEc8IWJXMJjsghyi-Ppe4SxNlAQCtkLmi9MTkZosIpqaLTDlWau9VCcY0NIP8QbCjbs6VGvIASBQsANFMnYiZqZTWmkrT5Zsju51574tGL5eb0DsZZXX24sPJWgsWrt4mATolJbRfdH6hblXGlaiO9Tt5LcwMnAAntfeM7NrVq7tO_95w73okfgRZwVpTz3W5lBxNn3HbNbPTGZ0Jbz9DTVgMPvVT5F18-n_SyEnTKDKxbpTrajezjae4p4Wv05gmNHSxXod0XYytlS7E6aCGUjUYIHr-49KqshSyLYqaTfBTJfZI8FtESlOXJg9oRmqzYE524a0ub5FxJoBGmPsWzcmFqVvGoFD1phXLADesz1nk6IigroZeBp-Alby7-yIXMzBGMj83gfwkeYagZeJXIgeMSny-MlbP3JLReSgQvptThnCz656I_1xU5y; __Secure-1PSIDTS=sidts-CjIB7I_69MojPruvrRDcVpmSM2l2z5b9Mfm0qRkDG2jqsfSUkn_7Bfww5Zp8u9nMvAwX6RAA; __Secure-3PSIDTS=sidts-CjIB7I_69MojPruvrRDcVpmSM2l2z5b9Mfm0qRkDG2jqsfSUkn_7Bfww5Zp8u9nMvAwX6RAA; _ga_BF8Q35BMLM=GS2.1.s1770193017$o87$g1$t1770196651$j60$l0$h0; _ga_WC57KJ50ZZ=GS2.1.s1770193017$o98$g1$t1770196651$j60$l0$h0; SIDCC=AKEyXzW_U3PB7amK5HdfRANxh8MWX4nhZdjWCYiI-b7V6IGDH_DxkCuZLaI91GxMSJL5-P-sX_g; __Secure-1PSIDCC=AKEyXzVhghU1D9opb0PBwEkEqrZEECQtvf8Yj9vpDGghZ6Hcd97-3GfyY2HBCfI8NKKBBx76KQ; __Secure-3PSIDCC=AKEyXzWC2dEfrd_N-ziYiLX65QpCN7mnbdxVDy7KwgfEqfOuRGrp9px6DCAIlH-P2VNhn-b0LA"
    
    service = GeminiService(
        cookies=cookies,
        proxy=None,
        timeout=120,
        trust_env=False
    )
    
    raw_result = await service.stream_generate(prompt="CRM系统在中国最知名的前3加企业前3个")
    
    # with open("raw_result.txt", "w", encoding="utf-8") as f:
    #     f.write(raw_result)
    # print("原始结果已保存到 raw_result.txt")
    
    result = parse_gemini_stream_response(raw_result)
    print("\n解析后的文本:")
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
