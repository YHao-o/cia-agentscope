"""Renewable Redis lease. Configured Redis failures never grant ownership."""
import asyncio
import logging
import time
import uuid

logger = logging.getLogger(__name__)

COMPARE_DELETE = """-- lease_release
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
end
return 0
"""
COMPARE_EXPIRE = """-- lease_renew
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('pexpire', KEYS[1], ARGV[2])
end
return 0
"""


class CoordinationUnavailable(RuntimeError):
    pass


class LeaseLost(CoordinationUnavailable):
    pass


def decode(value):
    return value.decode("utf-8") if isinstance(value, bytes) else value


def transient_io_error(exc):
    """Redis has optional dependencies here; classify transport failures by MRO."""
    names = {cls.__name__ for cls in type(exc).__mro__}
    if names & {"AuthenticationError", "AuthorizationError", "PermissionError"}:
        return False
    return isinstance(exc, (OSError, TimeoutError)) or bool(names & {
        "TimeoutError", "ConnectionError", "BusyLoadingError",
    })


class RedisLease:
    def __init__(self, redis, key, *, ttl=30, wait_timeout=0,
                 poll_interval=0.25, on_lost=None, token=None):
        self.redis, self.key = redis, key
        self.ttl, self.wait_timeout = ttl, wait_timeout
        self.poll_interval, self.on_lost = poll_interval, on_lost
        self.io_timeout = max(0.01, min(5.0, ttl / 3))
        self.token = token or uuid.uuid4().hex
        self.lost = asyncio.Event()
        self._renew_task = None
        self._release_task = None
        self._owned = False
        self._valid_until = 0.0
        self._renew_at = 0.0
        self._safety_margin = min(1.0, ttl / 10)

    async def acquire(self):
        deadline = time.monotonic() + self.wait_timeout
        while True:
            sent_at = time.monotonic()
            try:
                acquired = await asyncio.wait_for(
                    self.redis.set(self.key, self.token, nx=True, px=max(1, int(self.ttl * 1000))),
                    timeout=self.io_timeout,
                )
            except Exception as exc:
                raise CoordinationUnavailable("Redis 协调服务不可用") from exc
            if acquired:
                self._owned = True
                # The server could execute SET immediately and its reply arrive late.
                self._valid_until = sent_at + self.ttl - self._safety_margin
                self._renew_at = sent_at + self.ttl / 3
                if time.monotonic() >= self._valid_until:
                    self._mark_lost()
                    await self.release()
                    raise LeaseLost("获取租约的响应已超出有效期，请重试")
                self._renew_task = asyncio.create_task(self._renew())
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(min(self.poll_interval, max(0, deadline - time.monotonic())))

    def _mark_lost(self):
        if self.lost.is_set():
            return
        logger.warning("Redis lease ownership lost key=%s", self.key)
        self.lost.set()
        if self.on_lost:
            self.on_lost()

    async def _renew(self):
        try:
            while True:
                await asyncio.sleep(max(0, self._renew_at - time.monotonic()))
                sent_at = time.monotonic()
                remaining = self._valid_until - sent_at
                if remaining <= 0:
                    self._mark_lost()
                    return
                try:
                    renewed = await asyncio.wait_for(
                        self.redis.eval(COMPARE_EXPIRE, 1, self.key, self.token, max(1, int(self.ttl * 1000))),
                        timeout=min(self.io_timeout, remaining),
                    )
                except Exception as exc:
                    if not transient_io_error(exc) or time.monotonic() >= self._valid_until:
                        self._mark_lost()
                        return
                    # Retry promptly inside the OLD confirmed validity window.
                    # An ambiguous timeout never extends that window.
                    self._renew_at = min(self._valid_until, time.monotonic() + min(0.05, self.ttl / 100))
                    continue
                if not renewed:
                    self._mark_lost()
                    return
                self._valid_until = sent_at + self.ttl - self._safety_margin
                self._renew_at = sent_at + self.ttl / 3
                if time.monotonic() >= self._valid_until:
                    self._mark_lost()
                    return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("Redis lease renewal failed key=%s", self.key, exc_info=True)
            self._mark_lost()

    async def assert_owned(self):
        if not self._owned or self.lost.is_set() or time.monotonic() >= self._valid_until:
            self._mark_lost()
            raise LeaseLost("运行锁已失效，请重试")
        try:
            owner = decode(await asyncio.wait_for(self.redis.get(self.key), timeout=self.io_timeout))
        except Exception as exc:
            self._mark_lost()
            raise CoordinationUnavailable("Redis 协调服务不可用") from exc
        if owner != self.token or time.monotonic() >= self._valid_until:
            self._mark_lost()
            raise LeaseLost("运行锁已失效，请重试")

    async def release(self):
        if self._release_task is None:
            self._release_task = asyncio.create_task(self._release())
        await asyncio.shield(self._release_task)

    async def _release(self):
        if self._renew_task is not None:
            self._renew_task.cancel()
            try:
                await self._renew_task
            except asyncio.CancelledError:
                pass
            self._renew_task = None
        if self._owned:
            self._owned = False
            try:
                await asyncio.wait_for(self.redis.eval(COMPARE_DELETE, 1, self.key, self.token), timeout=self.io_timeout)
            except Exception:
                # TTL remains the final cleanup boundary; never delete someone else's lease.
                logger.warning("Redis lease release failed key=%s", self.key, exc_info=True)
