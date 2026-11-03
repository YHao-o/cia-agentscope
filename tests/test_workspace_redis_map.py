"""OpenSandboxWorkspaceManager 多副本（K8s）Redis 逻辑测试（纯 mock）。

不依赖真实 Redis / opensandbox-server：Sandbox 用 MagicMock 替身
（create / connect 可断言），redis 用 AsyncMock（get / set / delete 可编程）。
mock 沙箱 commands.run 返回 exit_code==0 的结果对象，满足
_touch_session_dir 的探活契约（mkdir + echo 一次往返）。

覆盖：
1. 新建沙箱后写 sandbox_map:{wid} 映射（ex=int(ttl)），分布式锁用毕释放
2. Redis 有映射 → 跨 Pod Sandbox.connect 复用（不再新建）
3. connect 失败 → 删映射 → 回退新建并写新映射
4. 淘汰（_evict_locked）→ 同步删映射
5. 分布式锁被他 Pod 持有（SET NX False）→ 轮询中对方写好映射 → connect 复用
6. redis_client=None → 退化为单进程行为（直接新建）
7. Redis 方法抛异常 → 全程降级不抛出，沙箱仍创建成功
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import app.services.opensandbox_workspace_manager as ws_manager_module
from app.services.opensandbox_workspace_manager import (
    OpenSandboxWorkspaceManager,
    _Entry,
)

BASEDIR = "/data/workspaces"
TTL = 3600.0
WID = "user-u1"
MAP_KEY = f"sandbox_map:{WID}"
LOCK_KEY = f"sandbox_lock:{WID}"


class _CmdResult:
    """commands.run 的最小返回值：_touch_session_dir 只看 exit_code。"""

    def __init__(self, exit_code: int = 0):
        self.exit_code = exit_code


def _fake_sandbox(sid: str, exit_code: int = 0) -> MagicMock:
    """mock 沙箱：探活成功（exit_code==0），id / destroy 可断言。"""
    sbx = MagicMock()
    sbx.id = sid
    sbx.commands.run = AsyncMock(return_value=_CmdResult(exit_code))
    sbx.destroy = AsyncMock()
    return sbx


def _make_redis() -> AsyncMock:
    """AsyncMock redis：默认无映射、SET NX 拿锁成功。"""
    redis = AsyncMock()
    redis.get.return_value = None
    redis.set.return_value = True
    redis.delete.return_value = 1
    redis.exists.return_value = 0
    return redis


def _make_manager(redis) -> OpenSandboxWorkspaceManager:
    return OpenSandboxWorkspaceManager(
        connection_config=MagicMock(),
        base_image="base-img",
        basedir=BASEDIR,
        ttl=TTL,
        redis_client=redis,
    )


@pytest.mark.asyncio
async def test_create_workspace_writes_redis_map_after_create():
    """本地无沙箱且 Redis 无映射 → 新建后写 sandbox_map:{wid}（ex=int(ttl)），
    锁用毕释放，返回的沙箱即新建沙箱。"""
    redis = _make_redis()
    mgr = _make_manager(redis)
    new_sbx = _fake_sandbox("sbx-new")

    with patch.object(ws_manager_module, "Sandbox") as sbx_cls:
        sbx_cls.create = AsyncMock(return_value=new_sbx)
        sbx_cls.connect = AsyncMock(return_value=_fake_sandbox("sbx-remote"))
        result = await mgr.create_workspace("u1", "sess-1")

    assert result is new_sbx
    # 新建成功 → 写 Redis 映射，TTL 与沙箱 TTL 一致
    redis.set.assert_any_call(MAP_KEY, "sbx-new", ex=int(TTL))
    # 无映射 → 不尝试跨 Pod connect
    sbx_cls.connect.assert_not_called()
    # 本地 cache 已登记（后续同 Pod 请求走本地复用）
    assert mgr._cache[WID].sandbox is new_sbx
    # 分布式锁在 finally 中释放
    redis.delete.assert_any_call(LOCK_KEY)


@pytest.mark.asyncio
async def test_create_workspace_connects_from_redis_map():
    """Redis 已有映射（其他 Pod 创建的沙箱）→ Sandbox.connect 以该 id 跨 Pod
    复用，探活成功后不再新建，映射 TTL 刷新。"""
    redis = _make_redis()
    redis.get.return_value = "sbx-remote"
    mgr = _make_manager(redis)
    remote = _fake_sandbox("sbx-remote")

    with patch.object(ws_manager_module, "Sandbox") as sbx_cls:
        sbx_cls.create = AsyncMock(return_value=_fake_sandbox("sbx-local-new"))
        sbx_cls.connect = AsyncMock(return_value=remote)
        result = await mgr.create_workspace("u1", "sess-1")

    # 以 Redis 映射中的 sandbox_id 调用 connect
    sbx_cls.connect.assert_awaited_once_with(
        "sbx-remote", connection_config=mgr._config
    )
    sbx_cls.create.assert_not_called()
    assert result is remote
    # 复用后写回本地 cache，并刷新 Redis 映射 TTL
    assert mgr._cache[WID].sandbox is remote
    redis.set.assert_any_call(MAP_KEY, "sbx-remote", ex=int(TTL))
    redis.delete.assert_not_called()  # 无淘汰、无锁释放


@pytest.mark.asyncio
async def test_connect_failure_falls_back_to_create():
    """映射存在但 Sandbox.connect 失败（沙箱已销毁）→ 删映射 → 回退新建
    并写入新映射。"""
    redis = _make_redis()
    # 首查命中失效映射；删除后（double-check 复查）无映射
    redis.get.side_effect = ["sbx-gone", None]
    mgr = _make_manager(redis)
    new_sbx = _fake_sandbox("sbx-new")

    with patch.object(ws_manager_module, "Sandbox") as sbx_cls:
        sbx_cls.connect = AsyncMock(side_effect=RuntimeError("connection refused"))
        sbx_cls.create = AsyncMock(return_value=new_sbx)
        result = await mgr.create_workspace("u1", "sess-1")

    # connect 只尝试一次（失效的 sandbox_id），失败后删除映射
    sbx_cls.connect.assert_awaited_once_with(
        "sbx-gone", connection_config=mgr._config
    )
    redis.delete.assert_any_call(MAP_KEY)
    # 回退新建成功，并写入新映射
    sbx_cls.create.assert_awaited_once()
    assert result is new_sbx
    redis.set.assert_any_call(MAP_KEY, "sbx-new", ex=int(TTL))


@pytest.mark.asyncio
async def test_evict_deletes_redis_map():
    """淘汰沙箱时同步删除 Redis 映射，避免其他 Pod connect 到已销毁沙箱。"""
    redis = _make_redis()
    mgr = _make_manager(redis)
    sbx = _fake_sandbox("sbx-old")
    mgr._cache[WID] = _Entry(sbx, "u1", session_ids={"s0"})

    await mgr._evict_locked(WID)

    sbx.destroy.assert_awaited_once()
    redis.delete.assert_any_call(MAP_KEY)
    assert WID not in mgr._cache


@pytest.mark.asyncio
async def test_dist_lock_double_check():
    """另一 Pod 持锁（首次 SET NX 返回 False）：轮询等待期间对方建好沙箱并
    写入映射 → 本 Pod connect 复用，不再新建、不释放对方的锁。"""
    redis = _make_redis()
    redis.get.side_effect = [None, "sbx-other"]  # 首查无映射；轮询中出现对方映射
    redis.set.side_effect = [False, True]         # 锁 SET NX 失败；映射刷新成功
    mgr = _make_manager(redis)
    other = _fake_sandbox("sbx-other")

    with patch.object(ws_manager_module, "_DIST_LOCK_POLL_INTERVAL", 0), \
         patch.object(ws_manager_module, "Sandbox") as sbx_cls:
        sbx_cls.connect = AsyncMock(return_value=other)
        sbx_cls.create = AsyncMock(return_value=_fake_sandbox("sbx-must-not-create"))
        result = await mgr.create_workspace("u1", "sess-1")

    assert result is other
    # 轮询中查到对方写入的映射 → 跨 Pod connect 复用，未新建
    sbx_cls.connect.assert_awaited_once_with(
        "sbx-other", connection_config=mgr._config
    )
    sbx_cls.create.assert_not_called()
    assert mgr._cache[WID].sandbox is other
    # 首次拿锁是 SET NX EX（被其他 Pod 持有 → False → 进入轮询等待）
    first_set = redis.set.call_args_list[0]
    assert first_set.args[0] == LOCK_KEY
    assert first_set.args[1]  # lock token 非空
    assert first_set.kwargs == {
        "ex": ws_manager_module._DIST_LOCK_TTL,
        "nx": True,
    }
    # 未拿到锁 → finally 不释放；映射保留（沙箱仍存活，供后续复用）
    redis.delete.assert_not_called()


@pytest.mark.asyncio
async def test_redis_none_fallback():
    """redis_client=None → 退化为单进程行为：跳过映射/锁，直接新建，不抛异常。"""
    mgr = _make_manager(None)
    new_sbx = _fake_sandbox("sbx-new")

    with patch.object(ws_manager_module, "Sandbox") as sbx_cls:
        sbx_cls.create = AsyncMock(return_value=new_sbx)
        sbx_cls.connect = AsyncMock(return_value=_fake_sandbox("sbx-remote"))
        result = await mgr.create_workspace("u1", "sess-1")

    assert result is new_sbx
    sbx_cls.create.assert_awaited_once()
    sbx_cls.connect.assert_not_called()
    assert mgr._cache[WID].sandbox is new_sbx


@pytest.mark.asyncio
async def test_redis_exception_degraded():
    """Redis 全挂（get/set/delete 均抛异常）→ 读映射/拿锁/写映射全部降级，
    仅告警不抛出，沙箱仍创建成功并登记本地 cache。"""
    redis = _make_redis()
    redis.get.side_effect = RuntimeError("redis down")
    redis.set.side_effect = RuntimeError("redis down")
    redis.delete.side_effect = RuntimeError("redis down")
    mgr = _make_manager(redis)
    new_sbx = _fake_sandbox("sbx-new")

    with patch.object(ws_manager_module, "Sandbox") as sbx_cls:
        sbx_cls.create = AsyncMock(return_value=new_sbx)
        sbx_cls.connect = AsyncMock(return_value=_fake_sandbox("sbx-remote"))
        result = await mgr.create_workspace("u1", "sess-1")

    assert result is new_sbx
    sbx_cls.create.assert_awaited_once()
    sbx_cls.connect.assert_not_called()
    assert mgr._cache[WID].sandbox is new_sbx
