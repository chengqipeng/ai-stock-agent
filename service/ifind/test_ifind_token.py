import asyncio

from iFinD import refresh_token
from token_client import THSTokenClient

async def main():
    # 替换为您的refresh_token

    client = THSTokenClient(refresh_token)
    
    try:
        # 获取当前有效的access_token
        print("获取当前access_token...")
        result = await client.get_access_token()
        print(f"结果: {result}")
        
        # 如果需要获取新的access_token（会使旧token失效）
        # print("获取新的access_token...")
        # result = await client.update_access_token()
        # print(f"结果: {result}")
        
    except Exception as e:
        print(f"请求失败: {e}")

if __name__ == "__main__":
    asyncio.run(main())