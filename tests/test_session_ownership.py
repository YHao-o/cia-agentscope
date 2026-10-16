"""会话归属校验测试：他人会话 ID 不得用于 /chat（越权修复）。"""
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from app.services.session_service import SessionService


def _svc(meta_return):
    dao = AsyncMock()
    dao.get_session_meta = AsyncMock(return_value=meta_return)
    return SessionService(dao), dao


@pytest.mark.asyncio
async def test_foreign_session_id_rejected():
    """session 属于他人 → 403（修复前直接透传）。"""
    svc, _ = _svc({"session_id": "s1", "user_id": "owner_a"})
    with pytest.raises(HTTPException) as exc:
        await svc.get_or_create_session("s1", "attacker_b")
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_own_session_id_allowed():
    svc, _ = _svc({"session_id": "s1", "user_id": "u1"})
    assert await svc.get_or_create_session("s1", "u1") == "s1"


@pytest.mark.asyncio
async def test_unknown_session_id_allowed():
    """sessions 表无记录（前端本地生成的新 ID / 首次对话）→ 保持兼容放行。"""
    svc, dao = _svc(None)
    assert await svc.get_or_create_session("fresh_id", "u1") == "fresh_id"
    dao.get_session_meta.assert_awaited_once_with("fresh_id")


@pytest.mark.asyncio
async def test_no_session_id_creates_new():
    svc, dao = _svc(None)
    new_id = await svc.get_or_create_session(None, "u1")
    assert new_id and isinstance(new_id, str) and len(new_id) == 32
    dao.get_session_meta.assert_not_awaited()
