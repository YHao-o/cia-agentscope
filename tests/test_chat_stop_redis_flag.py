"""K8s 多副本：/chat/stop Redis 停止标志与流清理测试（纯 mock）。

覆盖多副本中断链路新增逻辑（redis 一律 AsyncMock，无真实连接）：
1. /chat/stop 无条件写 chat_stop:{session_id}（"1"，EX 300）——会话可能
   运行在其他 Pod，本地注册表查不到也照样写；Redis 未配置/异常仅告警，
   不影响本地停止与返回值
2. /chat 流结束（finally）删除 chat_stop:{session_id}，本地注册表同步清理
3. generate_response._cancelled（闭包不可单独导入，采用与
   test_message_pair_and_citations.py 相同的最小化驱动）：
   - Redis 轮询按 monotonic 节流：同一秒内多个取消检查点只真实查询一次
   - Redis 标志存在 → _cancelled() 为 True → 主动中断（user_abort）
   - 本地 cancel_event 已置位 → 直接中断，不查 Redis（exists 不被调用）

说明：节流窗口依赖真实 monotonic 时钟，无法在单测中把窗口拨过 1 秒后
观察第二次真实查询（不 patch 全局 time.monotonic，避免干扰事件循环），
故"exists=1 时两次连续调用只查一次"的理想场景拆为：
节流（exists=0 连续多检查点只查 1 次）+ 命中即中断（exists=1 只查 1 次）。
"""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import app.services.chat_service as chat_service_module
from app.models.chat import ChatRequest
from app.routes.chat import StopRequest, chat, stop_chat
from app.services.chat_service import generate_response

_SAFE_SENS = {
    "blocked": False, "reason": "", "hit_sources": [],
    "sensitive_words": [], "source": "server", "raw": {},
}


def _patch_sensitive(monkeypatch):
    """屏蔽真实敏感检测外呼（.env 可能配置了真实服务地址）。"""
    async def fake_dict(text, stage="input"):
        return dict(_SAFE_SENS)

    async def fake_check(text, stage="output"):
        return dict(_SAFE_SENS)

    monkeypatch.setattr(chat_service_module, "dict_check_sensitive", fake_dict)
    monkeypatch.setattr(chat_service_module, "check_sensitive", fake_check)


def _patch_recommended_questions(monkeypatch):
    """屏蔽推荐问题真实 LLM 外呼。"""
    async def fake_emit_rq(*args, **kwargs):
        yield "data: {\"type\": \"recommended_questions\", \"questions\": [\"q1\"]}\n\n"

    monkeypatch.setattr(chat_service_module, "_emit_recommended_questions", fake_emit_rq)


def _make_orch(events):
    """orchestrator mock：run 为依次 yield 指定事件的 async 生成器。

    _input_probe_client 置 None → 输入 403 预检无客户端直接放行，不外呼。
    """
    orch = MagicMock()
    orch._input_probe_client = None
    orch._intent_client = None

    async def fake_run(*args, **kwargs):
        for ev in events:
            yield ev

    orch.run = fake_run
    return orch


def _make_session_service():
    svc = MagicMock()
    svc.load_messages = AsyncMock(return_value=[])
    svc.append_messages = AsyncMock(return_value={
        "user_message_id": 101, "assistant_message_id": 102,
    })
    svc.save_latest_trace_id = AsyncMock()
    svc.append_session_files = AsyncMock()
    svc.mark_last_assistant_failed = AsyncMock()
    return svc


def _make_request(redis=None):
    """generate_response 的 request mock：redis_client 必须显式赋值
    （MagicMock 自动属性非 None 会误入 Redis 分支）。"""
    request = MagicMock()
    request.headers = {}
    request.app.state.redis_client = redis
    request.app.state.upload_file_dao = None
    return request


def _parse_types(events):
    return [
        json.loads(e[6:].strip())["type"]
        for e in events
        if e.startswith("data: ")
    ]


def _drive(orch, redis, cancel_event=None, session_service=None):
    """最小化驱动 generate_response：mock 编排流 / 会话服务 / 敏感检测，
    收集全部 SSE 事件。"""
    return generate_response(
        orchestrator_service=orch,
        messages=[{"role": "user", "content": "你好"}],
        session_id="sess-1",
        user_id="u1",
        session_service=session_service or _make_session_service(),
        langfuse_service=None,
        request=_make_request(redis),
        cancel_event=cancel_event,
        workspace_manager=None,
    )


# ---- /chat/stop：无条件写 Redis 停止标志 ----

def _make_stop_request(redis, chat_tasks):
    request = MagicMock()
    request.app = SimpleNamespace(
        state=SimpleNamespace(redis_client=redis, chat_tasks=chat_tasks)
    )
    return request


@pytest.mark.asyncio
async def test_stop_writes_redis_flag():
    """会话在本地运行时：无条件写 Redis 停止标志（key/value/EX 300），
    同时照常触发本地 cancel_event。"""
    redis = AsyncMock()
    cancel_event = asyncio.Event()
    request = _make_stop_request(redis, {"sess-1": cancel_event})

    resp = await stop_chat(request, StopRequest(session_id="sess-1"), {"user_id": "u1"})

    # 无条件写标志：会话可能运行在其他 Pod（本地注册表查不到也得写）
    redis.set.assert_awaited_once_with("chat_stop:sess-1", "1", ex=300)
    assert cancel_event.is_set()  # 本地停止逻辑不受影响
    assert resp["ok"] is True


@pytest.mark.asyncio
async def test_stop_redis_failure_degrades():
    """Redis 写标志抛异常 → 仅告警，本地停止与返回值不受影响。"""
    redis = AsyncMock()
    redis.set.side_effect = RuntimeError("redis down")
    cancel_event = asyncio.Event()
    request = _make_stop_request(redis, {"sess-1": cancel_event})

    resp = await stop_chat(request, StopRequest(session_id="sess-1"), {"user_id": "u1"})

    assert resp["ok"] is True
    assert cancel_event.is_set()


@pytest.mark.asyncio
async def test_stop_without_redis_client():
    """redis_client 未配置 → 跳过写标志，不抛异常，本地停止照常。"""
    cancel_event = asyncio.Event()
    request = _make_stop_request(None, {"sess-1": cancel_event})

    resp = await stop_chat(request, StopRequest(session_id="sess-1"), {"user_id": "u1"})

    assert resp["ok"] is True
    assert cancel_event.is_set()


# ---- /chat 流结束：finally 清理 Redis 停止标志 ----

def _make_chat_request(redis, session_service):
    request = MagicMock()
    request.headers = {}
    request.app.state.chat_tasks = {}
    request.app.state.redis_client = redis
    request.app.state.session_service = session_service
    request.app.state.orchestrator_service = MagicMock()
    request.app.state.langfuse_service = None
    request.app.state.workspace_manager = None
    return request


@pytest.mark.asyncio
async def test_stream_finally_deletes_flag():
    """流正常结束（finally）：删除 chat_stop:{session_id}（避免残留影响
    同 session 后续请求），本地注册表同步清理。"""
    redis = AsyncMock()
    session_service = MagicMock()
    session_service.get_or_create_session = AsyncMock(return_value="sess-1")
    request = _make_chat_request(redis, session_service)
    body = ChatRequest(
        messages=[{"role": "user", "content": "你好"}], session_id="sess-1"
    )

    async def fake_generate_response(**kwargs):
        yield 'data: {"type": "summary", "content": "hi"}\n\n'

    with patch("app.routes.chat.generate_response",
               side_effect=fake_generate_response):
        resp = await chat(request, body, {"user_id": "u1"})
        events = [chunk async for chunk in resp.body_iterator]

    redis.delete.assert_awaited_once_with("chat_stop:sess-1")
    assert request.app.state.chat_tasks == {}  # 注册表清理
    types = _parse_types(events)
    assert types[0] == "session_ready"
    assert "summary" in types


# ---- generate_response._cancelled：本地优先 / Redis 节流轮询 / 跨 Pod 中断 ----

@pytest.mark.asyncio
async def test_cancelled_throttled_redis_poll(monkeypatch):
    """Redis 轮询节流：2 个事件检查点 + 编排流结束检查点共 3 次 _cancelled()，
    同一秒（节流窗口）内只真实查询 Redis 一次，流正常收尾不中断。"""
    _patch_sensitive(monkeypatch)
    _patch_recommended_questions(monkeypatch)

    redis = AsyncMock()
    redis.exists.return_value = 0  # 停止标志不存在

    orch = _make_orch([
        'data: {"type": "summary", "content": "片段1"}\n\n',
        'data: {"type": "summary", "content": "片段2"}\n\n',
    ])
    events = [ev async for ev in _drive(orch, redis)]

    # 3 个取消检查点命中同一节流窗口 → exists 只被真实调用一次
    redis.exists.assert_awaited_once_with("chat_stop:sess-1")
    types = _parse_types(events)
    assert "user_abort" not in types        # 未中断
    assert types.count("summary") == 2      # 两个事件均正常透传


@pytest.mark.asyncio
async def test_cancelled_redis_flag_aborts_stream(monkeypatch):
    """Redis 停止标志存在（其他 Pod /chat/stop 写入）→ _cancelled() 为 True
    → 主动 CancelledError：编排事件不再消费，收尾仍落库并标记失败，
    exists 首查即命中（只查一次）。"""
    _patch_sensitive(monkeypatch)
    _patch_recommended_questions(monkeypatch)

    redis = AsyncMock()
    redis.exists.return_value = 1

    orch = _make_orch([
        'data: {"type": "summary", "content": "片段1"}\n\n',
        'data: {"type": "summary", "content": "不应被消费"}\n\n',
    ])
    session_service = _make_session_service()
    events = [ev async for ev in _drive(orch, redis, session_service=session_service)]

    redis.exists.assert_awaited_once_with("chat_stop:sess-1")
    types = _parse_types(events)
    assert "user_abort" in types
    assert types.count("summary") == 0  # 命中检查点后事件不再透传
    # 中断轮收尾：仍落库 + 强制标记最新 assistant 消息失败
    session_service.append_messages.assert_awaited_once()
    session_service.mark_last_assistant_failed.assert_awaited_once_with("sess-1", "u1")


@pytest.mark.asyncio
async def test_cancelled_local_event_skips_redis(monkeypatch):
    """本地 cancel_event 已置位（同 Pod /chat/stop）→ _cancelled 直接 True，
    不查 Redis（exists 不被调用）。"""
    _patch_sensitive(monkeypatch)
    _patch_recommended_questions(monkeypatch)

    redis = AsyncMock()
    cancel_event = asyncio.Event()
    cancel_event.set()

    orch = _make_orch(['data: {"type": "summary", "content": "x"}\n\n'])
    session_service = _make_session_service()
    events = [ev async for ev in _drive(orch, redis, cancel_event=cancel_event,
                                        session_service=session_service)]

    redis.exists.assert_not_called()
    types = _parse_types(events)
    assert "user_abort" in types
    session_service.mark_last_assistant_failed.assert_awaited_once_with("sess-1", "u1")
