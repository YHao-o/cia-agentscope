# 大模型服务探测接口

## 接口说明

探测大模型服务是否可用。接口收到请求后，会向当前配置的默认模型（`models.default`）发送一次最小请求（`ping`，10 秒超时），并根据调用结果返回状态。

- **URL**：`GET /health/llm`
- **鉴权**：无需登录，无需携带任何 Header

## 请求示例

```bash
curl -s http://localhost:7010/health/llm
```

## 成功响应

服务可用时返回 HTTP **200**：

```json
{
  "status": "ok",
  "model": "deepseek-v4-flash",
  "base_url": "https://geekai.co/api/v1",
  "latency_ms": 823,
  "reply": "pong"
}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `status` | string | 固定为 `"ok"`，表示大模型服务可用 |
| `model` | string | 探测的模型名称（来自 `model_config.yml` 的 `models.default` 配置） |
| `base_url` | string | 模型服务端点地址 |
| `latency_ms` | int | 本次探测请求的往返耗时（毫秒） |
| `reply` | string | 模型对探测消息 `ping` 的回复文本（内容不固定，仅证明模型正常出话） |

## 失败响应

服务不可用时返回 HTTP **非 200**，**状态码原样透传大模型服务返回的状态码**；无法建立连接/超时等场景返回 502/504：

```json
{
  "status": "error",
  "model": "deepseek-v4-flash",
  "base_url": "https://geekai.co/api/v1",
  "error": {
    "status_code": 401,
    "type": "AuthenticationError",
    "message": "Error code: 401 - invalid api key"
  }
}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `status` | string | 固定为 `"error"` |
| `model` | string | 同成功响应 |
| `base_url` | string | 同成功响应 |
| `error.status_code` | int | 错误状态码，与 HTTP 响应状态码一致 |
| `error.type` | string | 错误类型标识，见下表 |
| `error.message` | string | 报错详情，为上游大模型服务返回的原始报错信息 |

## 状态码汇总

| HTTP 状态码 | `error.type` | 场景 |
|---|---|---|
| 200 | - | 大模型服务可用 |
| 400 / 401 / 404 / 429 / 500 等 | SDK 异常类名（如 `AuthenticationError`、`RateLimitError`、`InternalServerError`） | 上游大模型服务返回错误，状态码与报错信息原样透传 |
| 502 | `connection_error` | 无法连接模型服务（网络不通、DNS 失败等） |
| 504 | `timeout` | 探测请求超过 10 秒未响应 |
| 500 | `config_error` | 后端模型配置未加载（`models.default` 缺失） |

## 判定建议

- 只需判断 HTTP 状态码是否为 `200` 即可（`200` = 可用，非 `200` = 不可用）
- 需要排查原因时读取 `error.status_code` 与 `error.message`
