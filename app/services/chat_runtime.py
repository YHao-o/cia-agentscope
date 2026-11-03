"""Ownership, cancellation and single-flight control for one chat round."""
import asyncio
import json
import uuid
from contextvars import ContextVar

from app.services.redis_lease import CoordinationUnavailable, LeaseLost, RedisLease, decode, transient_io_error

_STOP_POLL_INTERVAL = 0.5
_WORKSPACE_GATE_WAIT = 5
# Inherited by the SSE pump and workspace-preparation tasks, never global run state.
current_chat_run = ContextVar("current_chat_run", default=None)

_STOP_RUN = """-- chat_stop_run
if redis.call('get', KEYS[1]) == ARGV[1] then
  redis.call('set', KEYS[2], '1', 'EX', 300)
  return 1
end
return 0
"""


class RunBusy(RuntimeError):
    pass


class ChatRun:
    def __init__(self, redis, session_id, user_id):
        self.redis, self.session_id, self.user_id = redis, session_id, str(user_id)
        self.run_id = uuid.uuid4().hex
        self.cancel_event = asyncio.Event()
        self.key = f"chat_run:{session_id}"
        self.stop_key = f"chat_stop:{session_id}:{self.run_id}"
        self.token = json.dumps({"run_id": self.run_id, "user_id": self.user_id}, sort_keys=True)
        self.lease = RedisLease(redis, self.key, token=self.token, on_lost=self.cancel_event.set) if redis is not None else None
        self._closed = False
        self._close_task = None

    async def open(self):
        if self.lease:
            # A retire operation holds this user-level gate while checking active
            # sessions. New sessions must register under the same gate.
            gate = RedisLease(self.redis, f"sandbox_lock:user-{self.user_id}",
                              wait_timeout=_WORKSPACE_GATE_WAIT)
            try:
                if not await gate.acquire():
                    raise RunBusy("该用户工作区正在准备，请稍后重试")
                if not await self.lease.acquire():
                    raise RunBusy("该会话正在生成，请等待当前轮结束")
                await gate.assert_owned()
            except BaseException:
                await self.lease.release()
                raise
            finally:
                await gate.release()
        current_chat_run.set((self.session_id, self.token))

    async def watch_stop(self):
        if self.redis is None:
            await self.cancel_event.wait()
            return
        failures = 0
        while not self.cancel_event.is_set():
            if self.lease is not None and self.lease.lost.is_set():
                self.cancel_event.set()
                return
            try:
                if await asyncio.wait_for(self.redis.exists(self.stop_key), timeout=5):
                    self.cancel_event.set()
                    return
                failures = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failures += 1
                if not transient_io_error(exc) or failures >= 3:
                    self.cancel_event.set()
                    return
            await asyncio.sleep(_STOP_POLL_INTERVAL)

    async def close(self):
        # ASGI disconnect cancellation must not strand a renewing lease forever.
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)
        if current_chat_run.get() == (self.session_id, self.token):
            current_chat_run.set(None)

    async def _close(self):
        if self._closed:
            return
        self._closed = True
        try:
            if self.redis is not None:
                try:
                    await asyncio.wait_for(self.redis.delete(self.stop_key), timeout=5)
                except Exception:
                    pass  # The round-specific key expires and cannot stop a later round.
        finally:
            if self.lease:
                await self.lease.release()


async def authorize_active_run(redis, session_id, user_id, local_run=None):
    if redis is None:
        if local_run is None:
            return None
        data = {"run_id": local_run.run_id, "user_id": local_run.user_id}
    else:
        try:
            raw = decode(await asyncio.wait_for(redis.get(f"chat_run:{session_id}"), timeout=5))
            if raw is None:
                return None
            data = json.loads(raw)
            if not isinstance(data, dict) or not data.get("run_id") or "user_id" not in data:
                raise ValueError("invalid active run")
        except Exception as exc:
            raise CoordinationUnavailable("无法读取会话运行状态") from exc
    if str(data["user_id"]) != str(user_id):
        raise PermissionError("无权操作该会话")
    return data


async def request_stop(redis, session_id, user_id, local_run=None):
    data = await authorize_active_run(redis, session_id, user_id, local_run)
    if data is None:
        return False
    if redis is not None:
        token = json.dumps(data, sort_keys=True)
        try:
            stopped = await asyncio.wait_for(redis.eval(
                _STOP_RUN, 2, f"chat_run:{session_id}",
                f"chat_stop:{session_id}:{data['run_id']}", token), timeout=5)
        except Exception as exc:
            raise CoordinationUnavailable("无法发送停止信号") from exc
        if not stopped:
            return False
    if local_run is not None and local_run.run_id == data["run_id"]:
        local_run.cancel_event.set()
    return True
