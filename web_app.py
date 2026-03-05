from common.logger import setup_logging, UVICORN_LOG_CONFIG

# 在任何业务模块导入之前初始化日志
setup_logging()

from api.web_batch_api import app  # noqa: E402

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=80,
        log_config=UVICORN_LOG_CONFIG,
    )
