"""Full generate_response cancellation integration tests (requires app SDKs).

Per-round Redis keys prevent a stale stop from aborting a later round.
Route ownership, cross-Pod stop, cleanup and duplicate-request behavior are
covered by test_chat_multireplica.py; this file exercises event throttling
and interrupted conversation persistence through the real generator.
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
        run_id="run-1",
    )


# Route, ownership and cleanup regressions live in test_chat_multireplica.py.
# This file keeps full generate_response integration tests with app SDKs installed.

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
    redis.exists.assert_awaited_once_with("chat_stop:sess-1:run-1")
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

    redis.exists.assert_awaited_once_with("chat_stop:sess-1:run-1")
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
