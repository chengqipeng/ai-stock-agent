# 批量处理功能说明

## 功能概述

批量处理功能允许用户一次性分析多只股票，系统会自动并发执行（最多5个线程），并记录每个批次的执行结果。

## 主要特性

### 1. 股票列表管理
- 从 `data_results/stock_to_score_list/stock_score_list.md` 读取待分析股票
- 支持在界面上勾选需要分析的股票
- 显示股票名称、代码和初始分数

### 2. 批量执行
- 并发执行（最多5个线程同时运行）
- 实时显示进度：已完成 xx/总共 xxx
- 自动调用 DeepSeek 进行 CAN SLIM 分析
- 自动提取分析结果中的分数

### 3. 批次管理
- 批次命名格式：`deepseek_{股票数量}_{时间YYYYMMDDHHMM}`
- 记录批次创建时间、总数量、完成数量、状态
- 支持查看历史批次列表

### 4. 结果查询
- 按批次查看股票分析结果
- 结果按分数倒序排列
- 点击股票可查看完整的分析报告和提示词

## 使用方法

### 访问批量分析页面
```
http://localhost:8080/batch
```

### 操作流程
1. 页面加载后自动显示待分析股票列表
2. 勾选需要分析的股票（可使用全选/取消全选）
3. 点击"开始批量分析"按钮
4. 实时查看分析进度
5. 完成后在"批次记录"中查看结果
6. 点击批次可查看该批次下所有股票的分析结果
7. 点击具体股票可查看详细分析报告

## API 接口

### 1. 获取股票列表
```
GET /api/stock_list
```

### 2. 创建批量分析任务
```
POST /api/batch_analysis
Body: {
  "stock_codes": ["北方华创 (002371.SZ)", "圣农发展 (002299.SZ)"]
}
```

### 3. 执行批量分析（SSE流式）
```
GET /api/batch_execute/{batch_id}
```

### 4. 获取批次列表
```
GET /api/batches
```

### 5. 获取批次股票列表
```
GET /api/batch/{batch_id}/stocks
```

### 6. 获取股票详细信息
```
GET /api/batch/stock/{stock_id}
```

## 数据库表结构

### batch_records（批次记录表）
- id: 批次ID
- batch_name: 批次名称
- total_count: 总股票数
- completed_count: 已完成数
- status: 状态（running/completed）
- created_at: 创建时间

### batch_stock_records（批次股票记录表）
- id: 记录ID
- batch_id: 批次ID
- stock_name: 股票名称
- stock_code: 股票代码
- prompt: 分析提示词
- result: 分析结果
- score: 评分
- status: 状态（pending/completed）
- created_at: 创建时间
- completed_at: 完成时间

## 技术实现

### 后端
- FastAPI SSE（Server-Sent Events）实现实时进度推送
- asyncio.Semaphore 控制并发数为5
- SQLite 存储批次和分析结果
- 正则表达式自动提取分析结果中的分数

### 前端
- EventSource 接收 SSE 事件流
- 实时更新进度条和详细信息
- 模态框展示完整分析报告
- 自动刷新批次列表（每5秒）

## 注意事项

1. 批量分析会消耗较多 API 调用次数，请合理使用
2. 并发数限制为5，避免对 API 造成过大压力
3. 分析结果会永久保存在数据库中
4. 建议定期清理历史批次数据
