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
    # 新增50个
    ("a1c3e5f7b9d2e4f6a8c0b2d4f6a8c0e2", "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6", "Gh4IjKlMn5Wx6Yz7Ab8Bc9", "11223344556677", "sh600000", 138),
    ("b2d4f6a8c0e1f3a5c7d9b1e3f5a7c9e1", "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7", "Hi5JkLmNo6Xy7Za8Bc9Cd0", "22334455667788", "sz002001", 139),
    ("c3e5a7b9d1f2a4c6e8b0d2f4a6c8e0b2", "c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8", "Ij6KlMnOp7Yz8Za9Cd0De1", "33445566778899", "sh601006", 140),
    ("d4f6b8c0e2a3c5e7b9d1f3a5c7e9b1d3", "d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9", "Jk7LmNoPq8Za9Ab0De1Ef2", "44556677889900", "sz000100", 141),
    ("e5a7c9d1f3b4d6f8a0c2e4b6d8f0a2c4", "e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0", "Kl8MnOpQr9Ab0Bc1Ef2Fg3", "55667788990011", "sh600104", 142),
    ("f6b8d0e2a4c5e7a9b1d3f5a7c9e1b3d5", "f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1", "Lm9NoPqRs0Bc1Cd2Fg3Gh4", "66778899001122", "sz002202", 143),
    ("a7c9e1f3b5d6f8b0c2e4a6d8f0b2c4e6", "a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2", "Mn0OpQrSt1Cd2De3Gh4Hi5", "77889900112233", "sh601857", 138),
    ("b8d0f2a4c6e7a9b1d3f5b7c9e1a3d5f7", "b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3", "No1PqRsTu2De3Ef4Hi5Ij6", "88990011223344", "sz000938", 139),
    ("c9e1a3b5d7f8b0c2e4a6d8f0c2e4a6b8", "c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4", "Op2QrStUv3Ef4Fg5Ij6Jk7", "99001122334455", "sh600150", 140),
    ("d0f2b4c6e8a9b1d3f5a7c9e1d3f5b7c9", "d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5", "Pq3RsStVw4Fg5Gh6Jk7Kl8", "10112233445566", "sz002311", 141),
    ("e1a3c5d7f9b0c2e4a6d8f0e2a4c6d8f0", "e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6", "Qr4StTuWx5Gh6Hi7Kl8Lm9", "21223344556677", "sh601211", 142),
    ("f2b4d6e8a0c1d3f5b7e9a1c3e5b7d9f1", "f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7", "Rs5TuUvXy6Hi7Ij8Lm9Mn0", "32334455667788", "sz000625", 143),
    ("a3c5e7f9b1d2e4f6a8c0d2f4b6e8a0c2", "a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8", "St6UvVwYz7Ij8Jk9Mn0No1", "43445566778899", "sh600196", 138),
    ("b4d6f8a0c2e3f5a7b9d1e3f5c7f9b1d3", "b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9", "Tu7VwWxZa8Jk9Kl0No1Op2", "54556677889900", "sz002714", 139),
    ("c5e7a9b1d3f4a6b8d0e2f4a6c8e0c2e4", "c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0", "Uv8WxXyAb9Kl0Lm1Op2Pq3", "65667788990011", "sh601390", 140),
    ("d6f8b0c2e4a5b7c9e1f3a5c7d9f1d3f5", "d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1", "Vw9XyYzBc0Lm1Mn2Pq3Qr4", "76778899001122", "sz000776", 141),
    ("e7a9c1d3f5b6c8e0f2a4b6d8e0a2e4a6", "e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2", "Wx0YzZaCd1Mn2No3Qr4Rs5", "87889900112233", "sh600309", 142),
    ("f8b0d2e4a6c7d9f1a3b5c7e9f1b3f5b7", "f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3", "Xy1ZaAbDe2No3Op4Rs5St6", "98990011223344", "sz002129", 143),
    ("a9c1e3f5b7d8e0f2a4c6d8f0a2c4f6c8", "a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4", "Yz2AbBcEf3Op4Pq5St6Tu7", "19001122334455", "sh601699", 138),
    ("b0d2f4a6c8e9f1a3b5d7e9a1b3d5a7d9", "b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5", "Za3BcCdFg4Pq5Qr6Tu7Uv8", "20112233445566", "sz000568", 139),
    ("c1e3a5b7d9f0a2b4d6f8b0c2d4b8e0e1", "c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6", "Ab4CdDeGh5Qr6Rs7Uv8Vw9", "31223344556677", "sh600406", 140),
    ("d2f4b6c8e0a1b3c5e7a9d1e3e5c9f1f2", "d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7", "Bc5DeFfHi6Rs7St8Vw9Wx0", "42334455667788", "sz002236", 141),
    ("e3a5c7d9f1b2c4d6f8b0e2f4f6d0a2a3", "e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8", "Cd6EfGgIj7St8Tu9Wx0Xy1", "53445566778899", "sh601800", 142),
    ("f4b6d8e0a2c3d5e7a9c1f3a5a7e1b3b4", "f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9", "De7FgHhJk8Tu9Uv0Xy1Yz2", "64556677889900", "sz000333", 143),
    ("a5c7e9f1b3d4e6f8b0d2a4b6b8f2c4c5", "a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0", "Ef8GhIiKl9Uv0Vw1Yz2Za3", "75667788990011", "sh600547", 138),
    ("b6d8f0a2c4e5f7a9c1e3b5c7c9a3d5d6", "b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1", "Fg9HiJjLm0Vw1Wx2Za3Ab4", "86778899001122", "sz002555", 139),
    ("c7e9a1b3d5f6a8b0d2f4c6d8d0b4e6e7", "c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2", "Gh0IjKkMn1Wx2Xy3Ab4Bc5", "97889900112233", "sh601668", 140),
    ("d8f0b2c4e6a7b9c1e3a5d7e9e1c5f7f8", "d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3", "Hi1JkLlNo2Xy3Yz4Bc5Cd6", "18990011223344", "sz000651", 141),
    ("e9a1c3d5f7b8c0d2f4b6e8f0f2d6a8a9", "e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4", "Ij2KlMmOp3Yz4Za5Cd6De7", "29001122334455", "sh600660", 142),
    ("f0b2d4e6a8c9d1e3a5c7f9a1a3e7b9b0", "f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5", "Jk3LmNnPq4Za5Ab6De7Ef8", "30112233445566", "sz002352", 143),
    ("a1c3e5f7b9d0e2f4b6d8a0b2b4f8c0c1", "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6", "Kl4MnOoQr5Ab6Bc7Ef8Fg9", "41223344556677", "sh601333", 138),
    ("b2d4f6a8c0e1f3c5e7b9c1c3a9d1d2",  "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7", "Lm5NoPpRs6Bc7Cd8Fg9Gh0", "52334455667788", "sz000002", 139),
    ("c3e5a7b9d1f2d4f6c0d8d2d4b0e2e3",  "c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8", "Mn6OpQqSt7Cd8De9Gh0Hi1", "63445566778899", "sh600372", 140),
    ("d4f6b8c0e2a3e5a7d1e9e3e5c1f3f4",  "d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9", "No7PqRrTu8De9Ef0Hi1Ij2", "74556677889900", "sz002352", 141),
    ("e5a7c9d1f3b4f6b8e2f0f4f6d2a4a5",  "e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0", "Op8QrSsTv9Ef0Fg1Ij2Jk3", "85667788990011", "sh601238", 142),
    ("f6b8d0e2a4c5a7c9f3a1a5a7e3b5b6",  "f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1", "Pq9RsStUw0Fg1Gh2Jk3Kl4", "96778899001122", "sz000858", 143),
    ("a7c9e1f3b5d6b8d0a4b2b6b8f4c6c7",  "a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2", "Qr0StTuVx1Gh2Hi3Kl4Lm5", "17889900112233", "sh600426", 138),
    ("b8d0f2a4c6e7c9e1b5c3c7c9a5d7d8",  "b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3", "Rs1TuUvWy2Hi3Ij4Lm5Mn6", "28990011223344", "sz002352", 139),
    ("c9e1a3b5d7f8d0f2c6d4d8d0b6e8e9",  "c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4", "St2UvVwXz3Ij4Jk5Mn6No7", "39001122334455", "sh601717", 140),
    ("d0f2b4c6e8a9e1a3d7e5e9e1c7f9f0",  "d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5", "Tu3VwWxYa4Jk5Kl6No7Op8", "40112233445566", "sz000725", 141),
    ("e1a3c5d7f9b0f2b4e8f6f0f2d8a0a1",  "e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6", "Uv4WxXyZb5Kl6Lm7Op8Pq9", "51223344556677", "sh600352", 142),
    ("f2b4d6e8a0c1a3c5f9a7a1a3e9b1b2",  "f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7", "Vw5XyYzAc6Lm7Mn8Pq9Qr0", "62334455667788", "sz002352", 143),
    ("a3c5e7f9b1d2b4d6a0b8b2b4f0c2c3",  "a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8", "Wx6YzZaBd7Mn8No9Qr0Rs1", "73445566778899", "sh601727", 138),
    ("b4d6f8a0c2e3c5e7b1c9c3c5a1d3d4",  "b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9", "Xy7ZaAbCe8No9Op0Rs1St2", "84556677889900", "sz000100", 139),
    ("c5e7a9b1d3f4d6f8c2d0d4d6b2e4e5",  "c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0", "Yz8AbBcDf9Op0Pq1St2Tu3", "95667788990011", "sh600893", 140),
    ("d6f8b0c2e4a5e7a9d3e1e5e7c3f5f6",  "d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1", "Za9BcCdEg0Pq1Qr2Tu3Uv4", "16778899001122", "sz002352", 141),
    ("e7a9c1d3f5b6f8b0e4f2f6f8d4a6a7",  "e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2", "Ab0CdDeFh1Qr2Rs3Uv4Vw5", "27889900112233", "sh601100", 142),
    ("f8b0d2e4a6c7a9c1f5a3a7a9e5b7b8",  "f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3", "Bc1DeFfGi2Rs3St4Vw5Wx6", "38990011223344", "sz000063", 143),
    ("a9c1e3f5b7d8b0d2a6b4b8b0f6c8c9",  "a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4", "Cd2EfGgHj3St4Tu5Wx6Xy7", "49001122334455", "sh600760", 138),
    ("b0d2f4a6c8e9c1e3b7c5c9c1a7d9d0",  "b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5", "De3FgHhIk4Tu5Uv6Xy7Yz8", "50112233445566", "sz002352", 139),
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
