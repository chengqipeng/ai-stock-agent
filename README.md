# ai-stock-agent

AI驱动的股票分析智能助手，提供CAN SLIM分析和技术指标分析。

## 功能特性

- **CAN SLIM 分析**：基于威廉·欧奈尔的CAN SLIM投资策略进行股票分析
- **技术指标分析**：提供详细的技术指标分析和市场趋势判断
- **批量分析**：支持批量选择股票进行分析，并发执行，实时显示进度
- **Web界面**：友好的Web界面，支持实时查询和Markdown格式展示
- **RESTful API**：提供标准的API接口，方便集成

## 快速开始

### 安装依赖

```bash
poetry install
```

### 启动Web服务

方式1：使用启动脚本
```bash
./start_web.sh
```

方式2：直接运行Python文件
```bash
python web_app.py
```

服务启动后，访问 http://localhost:8080 即可使用Web界面。

### 批量分析

访问 http://localhost:8080/batch 使用批量分析功能。

详细使用说明请查看 [BATCH_FEATURE.md](BATCH_FEATURE.md)

## API接口

### 1. CAN SLIM 分析

**接口地址**：`POST /api/can-slim`

**请求参数**：
```json
{
  "stock_name": "北方华创",
  "advice_type": 1,
  "holding_price": null
}
```

**响应示例**：
```json
{
  "success": true,
  "data": "# 股票分析结果\n..."
}
```

### 2. 技术指标分析

**接口地址**：`POST /api/technical`

**请求参数**：
```json
{
  "stock_name": "北方华创",
  "advice_type": 2,
  "holding_price": null
}
```

**响应示例**：
```json
{
  "success": true,
  "data": "# 技术指标分析\n..."
}
```

## 参数说明

- `stock_name`：股票名称（必填）
- `advice_type`：操作建议类型，取值1-4（必填）
- `holding_price`：持仓价格，当advice_type为3或4时使用（可选）

## 项目结构

```
ai-stock-agent/
├── api/                   # API接口目录
│   └── web_api.py        # Web API主文件
├── static/                # 静态文件目录
│   └── index.html        # 前端页面
├── service/              # 服务层
│   ├── eastmoney/       # 东方财富数据服务
│   ├── processor/       # 数据处理器
│   └── tests/           # 测试脚本
├── common/              # 公共模块
│   ├── constants/      # 常量定义
│   └── utils/          # 工具函数
└── start_web.sh        # 启动脚本
```

## 技术栈

- **后端**：FastAPI + Python 3.10+
- **前端**：HTML5 + JavaScript + Marked.js
- **数据源**：东方财富网
- **异步处理**：asyncio + aiohttp

## 开发说明

原有的命令行脚本仍然可以使用：
- `service/tests/generate_stock_can_slim_prompt.py`：CAN SLIM分析脚本
- `service/tests/generate_stock_technical_prompt.py`：技术指标分析脚本

## License

MIT
