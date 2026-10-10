# 方案：agent_access 元素携带 mcpClientConfig → 为对应用户配置 MCP 工具

## 一、背景与目标

mng 管理中心下发的 `agent_access`（即 `permissions.agent_whitelist`）部分元素将新增 `mcpClientConfig` 字段。当该字段非 null 时，包含：

```json
{
  "authorization": "...",   // 完整 Authorization 请求头值（已确认，原样使用）
  "code": "...",            // MCP 服务标识（用作 MCPClient name）
  "query_params": {...},    // 追加到 URL 的查询参数
  "transport_type": "...",  // 传输类型（sse / streamable_http）
  "transport_url": "..."    // MCP 服务地址
}
```

目标：登录时把 `mcpClientConfig` 沿现有"权限融合 → Redis 缓存 → 会话装配"链路传递，最终在**该白名单项对应的智能体**（id 匹配）的 Toolkit 中注册为 AgentScope MCP 工具（工具名形如 `mcp__{code}__{tool}`）。

参考：AgentScope 2.x 官方文档 building-blocks/tool 的 MCP 章节。

## 二、现状链路（已探明）

```
登录: mng /api/auth/user/login → permissions.agent_whitelist
      + mng /api/intents → 外部意图
      → merge_external_into_memory()（白名单过滤外部 agent）
      → fused{merged_intents, merged_agents, merged_skills} 写 Redis user_config:{uid}
会话: load_config_bundle() 读 Redis → assemble_workspace_components()
      → AgentDefinition(**merged_agent) → AgentRegistry
      → _build_toolkit_for() → Toolkit(tools=..., skills_or_loaders=...)
      → create_agent → Agent(toolkit=...)
```

AgentScope MCP API（已验证安装版本 `agentscope.mcp`）：

```python
from agentscope.mcp import MCPClient, HttpMCPConfig
client = MCPClient(
    name="xxx",                 # 必须匹配 ^[a-zA-Z0-9_-]+$（用于工具名 mcp__{name}__tool）
    is_stateful=False,          # stateless HTTP：无需 connect()/close()
    mcp_config=HttpMCPConfig(   # SSE 自动识别：URL path 以 /sse 或 /messages/ 结尾
        url="https://...",
        headers={"Authorization": "..."},  # 可选
        timeout=30.0,                       # 可选，默认 30
    ),
)
toolkit = Toolkit(tools=[...], skills_or_loaders=[...], mcps=[client])
```

关键框架行为（源码已核实）：
- stateless 模式无需生命周期管理；Toolkit 无 close 钩子，**stateful 会泄漏连接**，故一律用 stateless
- Toolkit 列举 MCP 工具失败（服务不可达/token 过期）时**自动跳过并 warning，不影响对话**
- `list_tools` 为真实 HTTP 调用，MCP 不可用时该 agent 只是少这些工具

## 三、改动内容（5 个文件）

### 1. `app/agents/base.py` — AgentDefinition 增加字段

pydantic v2 默认**忽略**未知键，`AgentDefinition(**merged_agent)` 会静默丢弃新键，必须显式声明：

```python
class AgentDefinition(BaseModel):
    ...
    # mng 下发的 MCP 客户端配置（原样透传 dict，含 authorization/code/
    # query_params/transport_type/transport_url），None 表示该智能体无 MCP
    mcp_client_config: Optional[dict] = None
```

### 2. `app/services/mng_service.py` — 融合时附加配置

**(a)** 新增辅助函数，从白名单提取 id → mcpClientConfig 映射：

```python
def _build_whitelist_mcp_map(agent_whitelist: list) -> dict:
    """{"id": mcpClientConfig, ...}，仅收集 mcpClientConfig 非 null 的项。"""
```

**(b)** `merge_external_into_memory()` 中，外部 agent 通过白名单检查后，把配置附加到 merged agent dict（键名用 snake_case `mcp_client_config`）：

```python
whitelist_mcp_map = _build_whitelist_mcp_map(
    permissions.get("agent_whitelist", []) if permissions else []
)
...
merged_agents.append({
    "id": agent_code,
    "name": agent_name,
    "skills": skill_codes,
    "system_prompt": agent_prompt,
    **({"mcp_client_config": whitelist_mcp_map[agent_id]}
       if agent_id in whitelist_mcp_map else {}),
})
```

说明：只在 agent 首次创建时附加（现有去重分支 `agent_code in known_agent_ids` 保持不变）；dict 可 JSON 序列化，Redis 缓存链路无需改动。

**(c)** `fetch_external_intents()` 开头增加 AUTH_MOCK 分支：`AUTH_MOCK=true` 时返回内置 `_MOCK_EXTERNAL_INTENTS`（含一个 agent id=999 的意图，与 mock 白名单匹配），使本地无 mng 时也能全链路联调。mock 意图带 `definition` 字段可顺带验证 description 注入。

### 3. `app/agents/registry.py` — Toolkit 注册 MCP

**(a)** 新增 `_build_mcp_clients(definition) -> list[MCPClient]`：

```python
def _build_mcp_clients(self, definition) -> list:
    cfg = getattr(definition, "mcp_client_config", None)
    if not isinstance(cfg, dict) or not cfg:
        return []
    transport_url = str(cfg.get("transport_url") or "")
    if not transport_url:
        logger.warning(f"[AgentRegistry] agent={definition.id} mcpClientConfig 缺 transport_url，跳过")
        return []
    # query_params 兼容 dict / str 两种格式
    query_params = cfg.get("query_params")
    if isinstance(query_params, dict) and query_params:
        sep = "&" if "?" in transport_url else "?"
        transport_url = f"{transport_url}{sep}{urlencode(query_params)}"
    elif isinstance(query_params, str) and query_params.strip():
        sep = "&" if "?" in transport_url else "?"
        transport_url = f"{transport_url}{sep}{query_params.lstrip('?')}"
    # authorization = 完整请求头值，原样放入（已确认）
    headers = {}
    if cfg.get("authorization"):
        headers["Authorization"] = cfg["authorization"]
    # MCPClient name 要求 ^[a-zA-Z0-9_-]+$，非法字符替换为下划线
    name = re.sub(r"[^a-zA-Z0-9_-]", "_", str(cfg.get("code") or definition.id)) or "mcp_server"
    try:
        from agentscope.mcp import MCPClient, HttpMCPConfig
        return [MCPClient(
            name=name,
            is_stateful=False,   # stateless：无生命周期管理，不可达自动跳过
            mcp_config=HttpMCPConfig(url=transport_url, headers=headers or None),
        )]
    except Exception:
        logger.exception(f"[AgentRegistry] agent={definition.id} MCP 客户端构建失败，跳过")
        return []
```

`transport_type` 仅用于日志：值为 sse 而 URL path 不以 `/sse`/`/messages/` 结尾时记 warning（AgentScope 按 URL 自动判定传输方式，不强行改造 URL）。

**(b)** `_build_toolkit_for()` 两处 Toolkit 构造均传 `mcps=`（含"无绑定技能"的 early-return 分支）：

```python
mcps = self._build_mcp_clients(definition)
if not bound_skill_names:
    return Toolkit(tools=[], skills_or_loaders=[], mcps=mcps)
...
return Toolkit(tools=self._all_tools, skills_or_loaders=bound_loaders, mcps=mcps)
```

Toolkit 在 `self._toolkits` 中按 agent_id 缓存（registry 为请求级实例，MCPClient 不跨请求复用，无状态残留）。

### 4. `app/routes/auth.py` — 前端响应剥离凭证

`_normalize_agent_access()` 目前只删 `show`；`mcpClientConfig` 含 `authorization` 凭证，不应下发前端，一并剥离（Redis 中的原始 permissions 保持不动）：

```python
cleaned = {k: v for k, v in item.items() if k not in ("show", "mcpClientConfig")}
```

`_merge_regulations_agents` / `_enrich_agent_access` / `_build_update_response` / `/refresh` 均经由 `_normalize_agent_access`，一处修改全覆盖。

### 5. `app/dao/user_dao.py` — Mock 白名单示例

`_MOCK_USERS` 中 admin 与 zhangsan 的白名单项 `{"id": "999", "name": "生成PPT智能体", ...}` 追加示例（URL 为占位，联调时替换为真实 MCP 服务地址）：

```python
"mcpClientConfig": {
    "code": "ppt_mcp",
    "transport_type": "streamable_http",
    "transport_url": "http://localhost:8000/mcp",
    "query_params": {"demo": "1"},
    "authorization": "Bearer demo-token",
},
```

配合 (2c) 的 mock 外部意图（agent id=999 → code `agent_ppt`），登录 admin 后 merge 会生成带 `mcp_client_config` 的 `agent_ppt`。

## 四、假设与决策

| 决策 | 结论 | 依据 |
|---|---|---|
| 挂载范围 | 只挂 id 匹配的那个智能体 | 用户确认 |
| authorization | 完整请求头值，原样放入 `headers["Authorization"]` | 用户确认 |
| 连接模式 | 一律 stateless（`is_stateful=False`） | Toolkit 无 close 钩子，stateful 会泄漏；stateless 失败自动跳过 |
| query_params | 兼容 dict（urlencode）与 str（原样拼接） | mng 格式未明，防御式处理 |
| transport_type | 不参与 URL 构造，仅用于不匹配时告警 | AgentScope 按 URL path 自动判定 SSE/Streamable HTTP |
| MCP name | `code` 非法字符替换 `_`，兜底 `definition.id` | MCPClient 构造校验 `^[a-zA-Z0-9_-]+$` |
| 前端响应 | 剥离 `mcpClientConfig`（防凭证泄露） | authorization 是敏感凭证 |
| 基础智能体 | 不处理（白名单 id 与基础 agent id 无交集，MCP 仅随外部智能体生效） | 现有白名单过滤逻辑只作用于外部 agent |
| 生效时机 | 需**重新登录**（或删除 Redis `user_config:{uid}`）后生效 | 融合配置登录时缓存，TTL 7 天 |

## 五、验证步骤

1. **单元验证（无需起服务）**：临时脚本调 `merge_external_into_memory`（喂 mock 意图 + 带 mcpClientConfig 的权限），断言 merged agent 含 `mcp_client_config`；再实例化 `AgentRegistry` 调 `get_toolkit("agent_ppt")`，断言 Toolkit 的 basic 组含 1 个 MCPClient（name=ppt_mcp）
2. **登录验证**：重启应用 → `POST /login`（admin）→ 响应 `agent_access` 各项**不含** `mcpClientConfig`，Redis `user_config:1` 中 merged_agents 的 agent_ppt **含** `mcp_client_config`
3. **会话验证（MCP 不可达降级）**：`POST /chat`（`agent_id="agent_ppt"`，或直接提问命中该意图）→ 日志出现 `Skipping MCP 'ppt_mcp' ...`（占位 URL 连不上），对话正常返回 —— 证明降级路径 OK
4. **会话验证（真实 MCP）**：本地起一个 MCP 服务（如 `mcp-server-time` 的 HTTP 版或任意 streamable_http 服务），替换 mock `transport_url` → 重新登录 → chat → 事件流中模型可调用 `mcp__ppt_mcp__*` 工具并返回结果
5. **回归**：`search_enabled=false`、无 MCP 的普通用户路径（zhangsan 原有权限）行为不变
