import asyncio
import json
import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from starlette.background import BackgroundTask

from app.dependencies import current_user
from app.config import SSE_HEARTBEAT_INTERVAL
from app.models.chat import ChatRequest
from app.services.chat_service import generate_response
from app.services.feedback_service import (
    FeedbackBackendError, cancel_feedback_shared, resolve_feedback_shared,
)
from app.services.chat_runtime import (
    ChatRun, RunBusy, CoordinationUnavailable, authorize_active_run, request_stop,
)
from app.dao.user_dao import fire_notify_mng_active

logger = logging.getLogger(__name__)
router = APIRouter()
_SSE_HEARTBEAT = ": ping\n\n"
_DONE = object()


async def _with_heartbeat(source, cancel_event=None):
    """Pump SSE without cancelling an upstream await on heartbeat timeout.

    A separate cancellation watcher can interrupt silent LLM/tool/feedback waits;
    generate_response still owns partial-history persistence and user_abort events.
    """
    if SSE_HEARTBEAT_INTERVAL <= 0 and cancel_event is None:
        async for ev in source:
            yield ev
        return
    queue = asyncio.Queue()

    async def pump():
        try:
            async for ev in source:
                await queue.put(ev)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            await queue.put(exc)
        finally:
            await queue.put(_DONE)

    pump_task = asyncio.create_task(pump())

    async def stop_pump():
        await cancel_event.wait()
        pump_task.cancel()
        await asyncio.gather(pump_task, return_exceptions=True)
        # Cover cancellation before pump() first executes as well.
        await queue.put(_DONE)

    stopper = asyncio.create_task(stop_pump()) if cancel_event is not None else None
    try:
        while True:
            try:
                if SSE_HEARTBEAT_INTERVAL > 0:
                    item = await asyncio.wait_for(queue.get(), timeout=SSE_HEARTBEAT_INTERVAL)
                else:
                    item = await queue.get()
            except asyncio.TimeoutError:
                yield _SSE_HEARTBEAT
                continue
            if item is _DONE:
                return
            if isinstance(item, Exception):
                raise item
            yield item
    finally:
        if stopper is not None:
            stopper.cancel()
            await asyncio.gather(stopper, return_exceptions=True)
        pump_task.cancel()
        await asyncio.gather(pump_task, return_exceptions=True)


class StopRequest(BaseModel):
    session_id: str


class FeedbackRequest(BaseModel):
    session_id: str
    feedback_id: str
    action: str
    payload: dict = {}


@router.post("/chat")
async def chat(request: Request, body: ChatRequest, user: dict = Depends(current_user)):
    authorization = request.headers.get("authorization", "")
    if authorization.lower().startswith("bearer "):
        jwt_token = authorization.split(" ", 1)[1].strip()
        if jwt_token:
            fire_notify_mng_active(jwt_token)
    session_service = request.app.state.session_service
    user_id = user.get("user_id")
    session_id = await session_service.get_or_create_session(body.session_id, user_id)
    tasks = request.app.state.chat_tasks
    if session_id in tasks:
        raise HTTPException(status_code=409, detail="该会话正在生成，请等待当前轮结束")
    run = ChatRun(getattr(request.app.state, "redis_client", None), session_id, user_id)
    try:
        await run.open()
    except RunBusy as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except CoordinationUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    tasks[session_id] = run

    async def cleanup():
        try:
            await cancel_feedback_shared(session_id)
        except Exception:
            logger.warning("[chat] 清理反馈失败", exc_info=True)
        finally:
            if tasks.get(session_id) is run:
                tasks.pop(session_id, None)
            await run.close()

    async def stream():
        watcher = asyncio.create_task(run.watch_stop())
        try:
            yield f"data: {json.dumps({'type': 'session_ready', 'session_id': session_id})}\n\n"
            async for event in _with_heartbeat(generate_response(
                orchestrator_service=request.app.state.orchestrator_service,
                messages=body.messages, session_id=session_id, user_id=user_id,
                session_service=session_service,
                langfuse_service=request.app.state.langfuse_service,
                agent_id=body.agent_id, request=request,
                search_enabled=body.search_enabled, skills=body.skills,
                knowledge_bases=body.knowledge_bases,
                cancel_event=run.cancel_event, run_id=run.run_id,
                workspace_manager=getattr(request.app.state, "workspace_manager", None),
            ), cancel_event=run.cancel_event):
                yield event
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)
            await asyncio.shield(asyncio.create_task(cleanup()))

    try:
        return StreamingResponse(
            stream(), media_type="text/event-stream",
            headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
            background=BackgroundTask(cleanup),
        )
    except Exception:
        if tasks.get(session_id) is run:
            tasks.pop(session_id, None)
        await run.close()
        raise


@router.post("/chat/stop")
async def stop_chat(request: Request, body: StopRequest, user: dict = Depends(current_user)):
    """Stop only the caller's currently active round, including on another Pod."""
    try:
        stopped = await request_stop(
            getattr(request.app.state, "redis_client", None), body.session_id,
            user.get("user_id"), getattr(request.app.state, "chat_tasks", {}).get(body.session_id),
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except CoordinationUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if not stopped:
        return {"ok": False, "msg": "会话不在运行中或已结束"}
    return {"ok": True, "msg": "已发送停止信号"}


@router.post("/chat/feedback")
async def submit_feedback(request: Request, body: FeedbackRequest, user: dict = Depends(current_user)):
    try:
        active = await authorize_active_run(
            getattr(request.app.state, "redis_client", None), body.session_id,
            user.get("user_id"), getattr(request.app.state, "chat_tasks", {}).get(body.session_id),
        )
        ok = active is not None and await resolve_feedback_shared(
            body.session_id, body.feedback_id, {"action": body.action, "payload": body.payload},
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except (CoordinationUnavailable, FeedbackBackendError) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if not ok:
        raise HTTPException(status_code=404, detail="反馈已失效（超时、已处理或会话已结束）")
    return {"ok": True}
