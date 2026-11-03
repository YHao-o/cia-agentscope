"""Bounded Redis recovery, real sandbox expiration and safe replacement."""
import asyncio
from datetime import datetime, timedelta, timezone
import json
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from app.services import chat_runtime
from app.services.redis_lease import CoordinationUnavailable, RedisLease
from tests.test_workspace_redis_map import ws, MemoryRedis, FakeSandbox, manager, MAP_KEY, LOCK_KEY, WID


class Redis(MemoryRedis):
    async def exists(self, key):
        return await self.get(key) is not None

    async def scan_iter(self, match=None):
        import fnmatch
        for key in list(self.data):
            if match is None or fnmatch.fnmatch(key, match):
                yield key

    async def eval(self, script, count, *args):
        if "workspace_retire" in script:
            self._check()
            if self.data.get(args[0]) != args[2] or self.data.get(args[1]) != args[3]:
                return 0
            await self.delete(args[1])
            return 1
        return await super().eval(script, count, *args)


class ExpiringSandbox(FakeSandbox):
    def __init__(self, sid, remaining=3600, broken=False):
        super().__init__(sid)
        self.expires_at = datetime.now(timezone.utc) + timedelta(seconds=remaining)
        self.broken = broken

    async def get_info(self):
        return SimpleNamespace(expires_at=self.expires_at, status=SimpleNamespace(state="Running"))

    async def run(self, command):
        self.calls.append(command)
        return SimpleNamespace(exit_code=1 if self.broken else 0)


class LeaseRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_watch_stops_on_third_failure_but_not_on_first(self):
        redis, calls = Redis(), []
        async def exists(key):
            calls.append(key)
            raise TimeoutError()
        redis.exists = exists
        run = chat_runtime.ChatRun(redis, "s", "u")
        with patch.object(chat_runtime, "_STOP_POLL_INTERVAL", 0.001):
            await asyncio.wait_for(run.watch_stop(), 1)
        self.assertEqual(len(calls), 3)

    async def test_confirmed_owner_loss_is_not_retried(self):
        redis, attempts = Redis(), []
        original = redis.eval
        async def zero(script, count, *args):
            if "lease_renew" in script:
                attempts.append(script)
                return 0
            return await original(script, count, *args)
        redis.eval = zero
        lease = RedisLease(redis, "run", ttl=0.12)
        await lease.acquire()
        try:
            await asyncio.wait_for(lease.lost.wait(), 0.3)
            self.assertEqual(len(attempts), 1)
        finally:
            await lease.release()

    async def test_one_transient_renewal_failure_recovers_before_deadline(self):
        redis = Redis()
        calls = []
        original = redis.eval

        async def flaky(script, count, *args):
            if "lease_renew" in script:
                calls.append(time.monotonic())
                if len(calls) == 1:
                    raise TimeoutError("one transient timeout")
            return await original(script, count, *args)

        redis.eval = flaky
        lease = RedisLease(redis, "run", ttl=0.3)
        await lease.acquire()
        try:
            await asyncio.sleep(0.18)
            self.assertFalse(lease.lost.is_set())
            self.assertGreaterEqual(len(calls), 2)
            await lease.assert_owned()
        finally:
            await lease.release()

    async def test_repeated_renewal_errors_stop_before_original_expiry(self):
        redis = Redis()
        original = redis.eval
        count = 0

        async def broken(script, n, *args):
            nonlocal count
            if "lease_renew" in script:
                count += 1
                raise ConnectionError("offline")
            return await original(script, n, *args)

        redis.eval = broken
        # Leave enough real time for Windows' scheduler tick while retaining a
        # deadline-based assertion (the old one-error behavior still fails count).
        lease = RedisLease(redis, "run", ttl=0.6)
        await lease.acquire()
        try:
            await asyncio.wait_for(lease.lost.wait(), 0.9)
            self.assertGreaterEqual(count, 2)
            self.assertIsNotNone(await redis.get("run"))
        finally:
            await lease.release()

    async def test_stop_watcher_tolerates_two_transient_failures(self):
        redis = Redis()
        outcomes = [TimeoutError(), ConnectionError(), False, True]
        reads = 0

        async def exists(key):
            nonlocal reads
            result = outcomes[reads]
            reads += 1
            if isinstance(result, Exception):
                raise result
            return result

        redis.exists = exists
        run = chat_runtime.ChatRun(redis, "s", "u")
        with patch.object(chat_runtime, "_STOP_POLL_INTERVAL", 0.001, create=True):
            await asyncio.wait_for(run.watch_stop(), 2)
        self.assertEqual(reads, 4)

    async def test_new_run_cannot_enter_during_workspace_retirement(self):
        redis = Redis()
        await redis.set("sandbox_lock:user-u", "retiring", ex=30)
        run = chat_runtime.ChatRun(redis, "s", "u")
        try:
            with patch.object(chat_runtime, "_WORKSPACE_GATE_WAIT", 0, create=True):
                with self.assertRaises((chat_runtime.RunBusy, CoordinationUnavailable)):
                    await run.open()
            self.assertIsNone(await redis.get("chat_run:s"))
        finally:
            await run.close()


class WorkspaceRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_retirement_reuses_existing_creation_lease(self):
        redis, bad = Redis(), ExpiringSandbox("bad", broken=True)
        await redis.set(MAP_KEY, "bad", ex=3600)
        mgr, lease = manager(redis), RedisLease(redis, LOCK_KEY)
        await lease.acquire()
        try:
            with patch.object(ws.Sandbox, "connect", AsyncMock(return_value=bad), create=True), \
                 patch.object(ws, "_PREPARE_RETRY_DELAY", 0):
                result = await asyncio.wait_for(mgr._connect_from_redis(WID, "u1", "s1", lease=lease), 1)
            self.assertIsNone(result)
            self.assertNotIn(MAP_KEY, redis.data)
            await lease.assert_owned()
        finally:
            await lease.release()

    async def test_actual_request_can_replace_its_own_bad_sandbox(self):
        redis, bad, good = Redis(), ExpiringSandbox("bad", broken=True), ExpiringSandbox("good")
        await redis.set(MAP_KEY, "bad", ex=3600)
        run, mgr = chat_runtime.ChatRun(redis, "s1", "u1"), manager(redis)
        await run.open()
        try:
            with patch.object(ws.Sandbox, "connect", AsyncMock(return_value=bad), create=True), \
                 patch.object(ws.Sandbox, "create", AsyncMock(return_value=good), create=True), \
                 patch.object(ws, "_PREPARE_RETRY_DELAY", 0):
                self.assertIs(await mgr.create_workspace("u1", "s1"), good)
            self.assertFalse(bad.destroyed)
        finally:
            await run.close()

    async def test_short_ttl_never_gets_a_sixty_second_floor(self):
        redis, sbx = Redis(), ExpiringSandbox("short", remaining=20)
        mgr = ws.OpenSandboxWorkspaceManager(object(), "image", "/data/workspaces", 30, redis_client=redis)
        with patch.object(ws.Sandbox, "create", AsyncMock(return_value=sbx), create=True):
            await mgr.create_workspace("u1", "s1")
        self.assertLess(redis.expires[MAP_KEY] - time.monotonic(), 20)

    async def test_map_expiry_does_not_close_sdk_used_by_active_session(self):
        redis, old = Redis(), ExpiringSandbox("old")
        mgr = manager(redis)
        mgr._cache[WID] = ws._Entry(old, "u1", session_ids={"other"})
        await redis.set("chat_run:other", json.dumps({"user_id": "u1", "run_id": "r"}), ex=30)
        await mgr._evict_locked(WID)
        self.assertFalse(old.closed)
        self.assertFalse(old.destroyed)
        await redis.delete("chat_run:other")
        await mgr._drain_deferred_clients()
        self.assertTrue(old.closed)
        self.assertEqual(mgr._deferred_clients, [])

    async def test_connect_health_failure_can_retire_without_repeating_health_check(self):
        class Unhealthy(Exception):
            error = SimpleNamespace(code="UNHEALTHY")
        redis, bad, good = Redis(), ExpiringSandbox("bad", broken=True), ExpiringSandbox("good")
        await redis.set(MAP_KEY, "bad", ex=3600)
        mgr = manager(redis)
        connect = AsyncMock(side_effect=[Unhealthy("execd unhealthy"), bad])
        with patch.object(ws.Sandbox, "connect", connect, create=True), \
             patch.object(ws.Sandbox, "create", AsyncMock(return_value=good), create=True), \
             patch.object(ws, "_PREPARE_RETRY_DELAY", 0):
            self.assertIs(await mgr.create_workspace("u1", "s1"), good)
        self.assertTrue(connect.await_args_list[1].kwargs["skip_health_check"])
        self.assertFalse(bad.destroyed)

    async def test_transient_prepare_failure_reuses_the_same_sandbox(self):
        class Transient(ExpiringSandbox):
            def __init__(self):
                super().__init__("same")
                self.attempts = 0
            async def run(self, command):
                self.attempts += 1
                return SimpleNamespace(exit_code=1 if self.attempts == 1 else 0)
        redis, sandbox = Redis(), Transient()
        await redis.set(MAP_KEY, "same", ex=3600)
        mgr, create = manager(redis), AsyncMock()
        with patch.object(ws.Sandbox, "connect", AsyncMock(return_value=sandbox), create=True), \
             patch.object(ws.Sandbox, "create", create, create=True), \
             patch.object(ws, "_PREPARE_RETRY_DELAY", 0):
            self.assertIs(await mgr.create_workspace("u1", "s1"), sandbox)
        self.assertEqual(sandbox.attempts, 2)
        create.assert_not_awaited()

    async def test_old_retirement_cannot_delete_a_new_owner_mapping(self):
        redis, old = Redis(), ExpiringSandbox("old", broken=True)
        await redis.set(MAP_KEY, "old", ex=3600)
        mgr = manager(redis)
        async def stale_probe(sbx):
            await redis.set(LOCK_KEY, "new-owner", ex=30)
            await redis.set(MAP_KEY, "new-generation", ex=3600)
            return True
        with patch.object(mgr, "_global_fault_confirmed", stale_probe):
            with self.assertRaises(ws.LeaseLost):
                await mgr._retire_shared_sandbox(WID, "old", "s1", sbx=old)
        self.assertEqual(redis.data[MAP_KEY], "new-generation")
        self.assertEqual(redis.data[LOCK_KEY], "new-owner")
        self.assertFalse(old.destroyed)

    async def test_no_redis_can_not_prove_idle_and_does_not_retire(self):
        redis, bad = Redis(), ExpiringSandbox("bad", broken=True)
        await redis.set(MAP_KEY, "bad", ex=3600)
        mgr = manager(redis)
        async def failing_scan(match=None):
            raise ConnectionError("scan unavailable")
            yield
        redis.scan_iter = failing_scan
        with self.assertRaises(CoordinationUnavailable):
            await mgr._retire_shared_sandbox(WID, "bad", "s1", sbx=bad)
        self.assertEqual(redis.data[MAP_KEY], "bad")
        self.assertFalse(bad.destroyed)

    async def test_mapping_expiry_uses_actual_remaining_lifetime(self):
        redis, sbx = Redis(), ExpiringSandbox("new", remaining=180)
        mgr = manager(redis)
        with patch.object(ws.Sandbox, "create", AsyncMock(return_value=sbx), create=True):
            await mgr.create_workspace("u1", "s1")
        remaining = redis.expires[MAP_KEY] - time.monotonic()
        self.assertGreater(remaining, 100)
        self.assertLess(remaining, 125)

    async def test_old_pool_sandbox_is_not_assigned_a_fresh_mapping_ttl(self):
        redis, old, fresh = Redis(), ExpiringSandbox("old", remaining=10), ExpiringSandbox("fresh")
        mgr = manager(redis)
        mgr._warm_pool = [old]
        mgr._pool_refill = False
        with patch.object(ws.Sandbox, "create", AsyncMock(return_value=fresh), create=True):
            result = await mgr.create_workspace("u1", "s1")
        self.assertIs(result, fresh)
        self.assertTrue(old.destroyed)

    async def test_bad_sandbox_is_replaced_when_no_other_run_uses_it(self):
        redis, bad, good = Redis(), ExpiringSandbox("bad", broken=True), ExpiringSandbox("good")
        await redis.set(MAP_KEY, "bad", ex=3600)
        mgr = manager(redis)
        with patch.object(ws.Sandbox, "connect", AsyncMock(return_value=bad), create=True), \
             patch.object(ws.Sandbox, "create", AsyncMock(return_value=good), create=True), \
             patch.object(ws, "_PREPARE_RETRY_DELAY", 0, create=True):
            result = await mgr.create_workspace("u1", "s1")
        self.assertIs(result, good)
        self.assertEqual(redis.data[MAP_KEY], "good")
        self.assertFalse(bad.destroyed)  # Preserve old data until server TTL.

    async def test_bad_sandbox_is_not_replaced_under_another_live_session(self):
        redis, bad = Redis(), ExpiringSandbox("bad", broken=True)
        await redis.set(MAP_KEY, "bad", ex=3600)
        await redis.set("chat_run:other", json.dumps({"user_id": "u1", "run_id": "r"}), ex=30)
        mgr = manager(redis)
        create = AsyncMock()
        with patch.object(ws.Sandbox, "connect", AsyncMock(return_value=bad), create=True), \
             patch.object(ws.Sandbox, "create", create, create=True), \
             patch.object(ws, "_PREPARE_RETRY_DELAY", 0, create=True):
            with self.assertRaises(RuntimeError):
                await mgr.create_workspace("u1", "s1")
        create.assert_not_awaited()
        self.assertFalse(bad.destroyed)
        self.assertEqual(redis.data[MAP_KEY], "bad")

    async def test_single_directory_error_is_not_a_sandbox_fault(self):
        class BadDirectory(ExpiringSandbox):
            async def run(self, command):
                return SimpleNamespace(exit_code=1 if "mkdir" in command else 0)
        redis, bad = Redis(), BadDirectory("bad")
        await redis.set(MAP_KEY, "bad", ex=3600)
        mgr = manager(redis)
        with patch.object(ws.Sandbox, "connect", AsyncMock(return_value=bad), create=True), \
             patch.object(ws, "_PREPARE_RETRY_DELAY", 0, create=True):
            with self.assertRaises(RuntimeError):
                await mgr.create_workspace("u1", "s1")
        self.assertFalse(bad.destroyed)
        self.assertEqual(redis.data[MAP_KEY], "bad")


if __name__ == "__main__":
    unittest.main()
