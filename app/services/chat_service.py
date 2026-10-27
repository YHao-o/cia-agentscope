"""聊天服务：模型创建 + 编排服务适配层。

重构后的职责：
- 保留 create_model_from_config / load_model_config（被 orchestrator_service 依赖）
- generate_response 作为适配层：转调 OrchestratorService.run()，
  并集成 session 历史持久化、Langfuse 追踪、CUSTOM_COMPONENT 事件检测

多智能体编排的核心逻辑下沉到 app.services.orchestrator_service。
"""
import json
import logging
import os
import asyncio
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator, Dict, Iterator, List, Optional, Tuple

import yaml
from agentscope.credential import OpenAICredential
from agentscope.model import OpenAIChatModel

from app.config import MODEL_CONFIG_PATH, SESSION_FILES_PERSIST_DIR
from app.services.file_change_detector import diff, filter_allowed
from app.services.langfuse_service import LangfuseService, derive_trace_id
from app.services.round_state import RoundState
from app.services.sensitive_service import (
    build_message_replace_event,
    check_sensitive,
    dict_check_sensitive,
)
from app.intent.llm_client import chat_complete, extract_json
from app.utils.event_serialization import sanitize_surrogates
from tools.bocha_search_tools import merge_bocha_snapshots

logger = logging.getLogger(__name__)

# 东八区（Asia/Shanghai）：消息时间戳统一按北京时间生成
_BEIJING_TZ = timezone(timedelta(hours=8))


@contextmanager
def _noop_ctx() -> Iterator[None]:
    """空 context manager，langfuse 未启用时作为 span 占位，yield None。"""
    yield None


def _estimate_tokens(content: str) -> int:
    """按内容长度估算 token 数（deepseek-v4，中英文分别计系数，粗略估算）。

    经验值：中文 1 字 ≈ 1.5 token，英文 4 字符 ≈ 1 token（0.25/字符）。
    """
    if not content:
        return 0
    chinese_count = sum(1 for c in content if ord(c) > 127)
    ascii_count = len(content) - chinese_count
    return int(chinese_count * 1.5 + ascii_count * 0.25)


def _compute_ttft_marker(event_type: str) -> bool:
    """判断某事件类型是否是首个用户可见内容事件（用于 TTFT 计时）。"""
    return event_type in ("TEXT_BLOCK_DELTA", "summary")


# 编排链路 error 事件的统一前端兜底文案（原始错误信息只进日志）
_FRIENDLY_ERROR_MESSAGE = "抱歉，暂时无法生成回复。请重试并联系管理员"


async def _generate_recommended_questions(
    orchestrator_service,
    user_input: str,
    final_output: str,
) -> Tuple[List[str], dict]:
    """调用 LLM 根据本轮问答生成 3 个推荐问题。

    复用 orchestrator 的 _intent_client / _intent_model_cfg。
    任何异常都吞掉返回空列表，确保不影响主流程。

    Returns:
        (推荐问题列表, {"system_prompt": ..., "user_prompt": ...})；
        失败时返回 ([], {})。
    """
    try:
        client = getattr(orchestrator_service, "_intent_client", None)
        model_config = getattr(orchestrator_service, "_intent_model_cfg", None)
        if not client or not model_config or not final_output:
            return [], {}

        system_prompt = (
            "你是一个推荐问题生成助手。根据用户的提问和助手的回答，"
            "生成 3 个用户可能想继续追问的相关问题。"
            "只输出 JSON，格式为 {\"questions\": [\"问题1\", \"问题2\", \"问题3\"]}，"
            "不要有任何额外说明或 markdown 标记。"
        )
        user_prompt = f"用户提问：{user_input}\n\n助手回答：{final_output}"

        text = await chat_complete(client, model_config, system_prompt, user_prompt, stage="llm-recommended-questions")
        data = extract_json(text)
        if not isinstance(data, dict):
            return [], {}
        questions = data.get("questions", [])
        if not isinstance(questions, list):
            return [], {}
        # 最多 3 个，过滤空白与非字符串
        cleaned = [str(q).strip() for q in questions if q and str(q).strip()]
        return cleaned[:3], {"system_prompt": system_prompt, "user_prompt": user_prompt}
    except Exception:
        logger.debug("[chat_service] 生成推荐问题失败", exc_info=True)
        return [], {}


def _extract_user_input(messages: List[Dict[str, Any]]) -> str:
    """从 messages 提取最后一条 user 消息的文本（支持 str / content block 列表）。

    统一复用，消除推荐问题与持久化两处重复提取逻辑。
    """
    for msg in reversed(messages):
        if msg.get("role") == "user":
            content = msg.get("content", "")
            if isinstance(content, list):
                return "\n".join(
                    b.get("text", "")
                    for b in content
                    if isinstance(b, dict) and b.get("type") == "text"
                )
            return str(content)
    return ""


def _safe_update_span(span: Any, output_dict: dict) -> None:
    """安全更新 span 的 output 字段，span 为 None 静默忽略，更新异常打告警日志。"""
    if not span:
        return
    try:
        span.update(output=output_dict)
    except Exception:
        # 根 span 的 output 回填失败会让 trace 缺 output，必须留下排查线索
        logger.warning("[chat_service] 更新 span output 失败", exc_info=True)


async def _load_history_messages(session_service: Any, session_id: Optional[str]) -> List[dict]:
    """从 session 加载历史消息并截断为最近 3 轮（6 条）。

    加载失败返回空列表，不影响主流程。
    """
    if not (session_service and session_id):
        return []
    try:
        saved = await session_service.load_messages(session_id)
        history = [
            {"role": m.get("role", "user"), "content": m.get("content", "")}
            for m in saved
            if m.get("content")
        ]
        # 限制历史为最近 3 轮（6 条消息），避免超出模型 20K token 上下文
        return history[-6:]
    except Exception:
        logger.exception("[chat_service] 加载会话历史失败")
        return []


async def _emit_recommended_questions(
    orchestrator_service,
    messages: List[Dict[str, Any]],
    final_output: str,
    langfuse_service: LangfuseService,
) -> AsyncGenerator[str, None]:
    """编排流结束后立即生成推荐问题并 yield recommended_questions 事件。

    生成失败静默跳过，不影响后续持久化与 trace 收尾。
    """
    try:
        user_input = _extract_user_input(messages)

        root_span_ctx = (
            langfuse_service.start_span(
                # 信息精简：reply 与根 span 输出重复，不重复上传
                "recommended-questions",
                input={"user_input": user_input},
            )
            if langfuse_service and langfuse_service.enabled
            else _noop_ctx()
        )
        with root_span_ctx as rec_obs:
            questions, rq_prompts = await _generate_recommended_questions(
                orchestrator_service, user_input, final_output
            )
            _safe_update_span(rec_obs, {"questions": questions, "prompts": rq_prompts})

        rec_event = json.dumps(
            {"type": "recommended_questions", "questions": questions},
            ensure_ascii=False,
        )
        yield f"data: {rec_event}\n\n"
    except Exception:
        logger.debug("[chat_service] 推荐问题事件发送失败", exc_info=True)


async def _persist_conversation_history(
    session_service: Any,
    session_id: Optional[str],
    user_id: Optional[str],
    messages: List[Dict[str, Any]],
    final_output: str,
    upload_file_dao: Any = None,
    message_pair_id: Optional[str] = None,
    citations: Optional[List[dict]] = None,
    bocha_sum: Optional[List[dict]] = None,
    content_parts: Optional[List[dict]] = None,
    round_state: Optional[RoundState] = None,
) -> Optional[dict]:
    """持久化本轮对话历史（用户输入 + 智能体输出）。

    本轮参与的 agent_id 列表、成功标志、真实 token 与消费的上传文件 id
    均取自 round_state（每请求独立对象，见 round_state.py；替代原先挂在
    orchestrator 单例上的 last_* 属性——单例会被并发请求互相覆盖，典型
    症状是上传文件绑定被清零、旧文件下一轮重复注入），与 user/assistant
    消息一同写入 messages 表。任何异常均静默吞掉。
    message_pair_id 为本轮对话配对 id（user/assistant 共享）；
    citations 为制度问答引用、bocha_sum 为博查搜索来源摘要
    （均随 assistant 消息落库，空则不写）。
    落库成功后把本轮 user 消息 id 回填到本轮消费的上传文件
    （upload_files.message_id），避免下一轮重复注入解析内容。

    Returns:
        {"user_message_id": int|None, "assistant_message_id": int|None}，失败为 None。
    """
    if not (session_service and session_id and user_id):
        missing = []
        if not session_service:
            missing.append("session_service")
        if not session_id:
            missing.append("session_id")
        if not user_id:
            missing.append("user_id")
        logger.warning(f"[chat_service] 跳过持久化：{', '.join(missing)} 为空")
        return None

    persist_result: Optional[dict] = None
    try:
        now_str = datetime.now(_BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        user_input = _extract_user_input(messages)
        # 落库前清洗孤立代理字符：流式 chunk 切开的 emoji 半字符会导致
        # pymysql UTF-8 编码失败，整轮对话历史（user+assistant）全部丢失
        user_input = sanitize_surrogates(user_input)
        final_output = sanitize_surrogates(final_output)
        citations = sanitize_surrogates(citations)
        bocha_sum = sanitize_surrogates(bocha_sum)
        content_parts = sanitize_surrogates(content_parts)

        new_messages = []
        if user_input:
            new_messages.append({
                "role": "user", "content": user_input, "timestamp": now_str,
                "agent_ids": [],
                "user_id": user_id,
                "success": True,
                "tokens": _estimate_tokens(user_input),
                "message_pair_id": message_pair_id,
            })
        if final_output:
            # 取本轮参与的 agent_id 列表（单 agent 路径=[agent_id]，多 agent 路径=编排汇总）
            involved_agent_ids = (
                list(round_state.agent_ids) if round_state else []
            )
            # 取本轮编排是否成功（基于 TaskResult.success，异常/超时/失败为 False）
            last_success = round_state.success if round_state else True
            # 真实 token 消耗：MODEL_CALL_END 事件累积的 input+output 之和；
            # 为 0（如自有 LLM 链路未发事件、异常降级）时回退文本估算
            real_tokens = (
                (int(round_state.input_tokens or 0) + int(round_state.output_tokens or 0))
                if round_state else 0
            )
            new_messages.append({
                "role": "assistant", "content": final_output, "timestamp": now_str,
                "agent_ids": involved_agent_ids,
                "user_id": user_id,
                "success": last_success,
                "tokens": real_tokens if real_tokens > 0 else _estimate_tokens(final_output),
                "message_pair_id": message_pair_id,
                "citations": citations if citations else None,
                "bocha_sum": bocha_sum if bocha_sum else None,
                "content_parts": content_parts if content_parts else None,
            })
        if new_messages:
            persist_result = await session_service.append_messages(
                session_id, user_id, new_messages
            )
    except Exception:
        logger.exception("[chat_service] 持久化对话历史失败")

    # 回填上传文件的 message_id（失败仅告警，不影响主流程）
    # isinstance 兜底：session_service 为 duck-typed 注入，非 dict 返回值不打断主流程
    user_message_id = (
        persist_result.get("user_message_id")
        if isinstance(persist_result, dict)
        else None
    )
    # 只绑定本轮实际消费过的文件（orchestrator 在 persist 阶段记录到
    # round_state）：解析中超时与流式期间新上传的文件不绑定，留给下一轮
    # 消费，避免绑定了却从未注入的"永久孤儿"（用户可见症状：模型说
    # 工作区没有该文件）
    consumed_ids = (
        list(round_state.consumed_upload_ids) if round_state else []
    )
    if user_message_id and upload_file_dao and consumed_ids:
        try:
            bound = await upload_file_dao.bind_message_id(
                session_id, user_message_id, message_pair_id, consumed_ids
            )
            if bound:
                logger.info(
                    "[chat_service] 已绑定 %s 个上传文件到消息 %s",
                    bound, user_message_id,
                )
        except Exception:
            logger.warning("[chat_service] 回填上传文件 message_id 失败", exc_info=True)

    return persist_result


def _persist_file_bytes(persist_dir: str, session_id: str, rel_path: str, content: bytes) -> bool:
    """把沙箱文件字节流写入持久化目录 {persist_dir}/{session_id}/{rel_path}。

    保留 rel_path 的子目录结构；任何异常记 warning 并返回 False（调用方回退沙箱 url）。
    """
    try:
        full_path = os.path.join(persist_dir, session_id, rel_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "wb") as f:
            f.write(content)
        return True
    except Exception:
        logger.warning(
            "[chat_service] 写入持久化目录失败 session=%s rel=%s",
            session_id, rel_path, exc_info=True,
        )
        return False


async def _detect_and_emit_files(
    session_id: Optional[str],
    before_files: set,
    session_service: Any,
    workspace_manager: Any = None,
    user_id: str = "",
    message_pair_id: Optional[str] = None,
    assistant_message_id: Optional[int] = None,
) -> AsyncGenerator[str, None]:
    """检测本轮新文件并 yield files_generated 事件，随后持久化文件元信息。

    通过 workspace_manager.list_session_files/stat_session_file 读取沙箱内
    会话目录的文件列表与大小。检测出的新文件会把字节流搬运到
    SESSION_FILES_PERSIST_DIR/{session_id}/（配置了且成功时 url 用
    /persist-files/...，否则回退 /files/...）；files_payload 每项携带
    message_id（本轮 assistant 消息）与 message_pair_id。
    """
    import mimetypes

    files_payload = []
    try:
        if workspace_manager is not None and session_id:
            after_files = await workspace_manager.list_session_files(user_id, session_id)
            # 白名单过滤：常规文档/表格/演示/图片及指定代码文件下发前端并落库，
            # svg/log 等过程文件不进入 files_generated 事件与 session_files
            new_files = filter_allowed(
                diff(before_files, after_files, exclude_prefixes=("uploads/",))
            )
            for rel_path in new_files:
                size = await workspace_manager.stat_session_file(user_id, session_id, rel_path)
                if size is None:
                    continue
                media_type = mimetypes.guess_type(rel_path)[0] or "application/octet-stream"
                # 字节流持久化：配置了持久化目录时读取沙箱文件并落盘，
                # url 指向持久化下载接口；未配置/失败回退沙箱下载地址
                url = f"/files/{session_id}/{rel_path}"
                if SESSION_FILES_PERSIST_DIR:
                    content = await workspace_manager.read_session_file(
                        user_id, session_id, rel_path
                    )
                    if content is not None and _persist_file_bytes(
                        SESSION_FILES_PERSIST_DIR, session_id, rel_path, content
                    ):
                        url = f"/persist-files/{session_id}/{rel_path}"
                files_payload.append({
                    "name": os.path.basename(rel_path),
                    "path": rel_path,
                    "url": url,
                    "size": size,
                    "media_type": media_type,
                    "message_id": assistant_message_id,
                    "message_pair_id": message_pair_id,
                })
    except Exception:
        logger.warning("[chat_service] 检测新文件失败", exc_info=True)
    yield f"data: {json.dumps({'type': 'files_generated', 'files': files_payload, 'message_pair_id':message_pair_id}, ensure_ascii=False)}\n\n"

    # 持久化本轮生成的文件元信息（供 /sessions/{session_id} 回看）
    if files_payload and session_service and session_id:
        try:
            await session_service.append_session_files(session_id, files_payload)
        except Exception:
            logger.warning("[chat_service] 持久化生成文件元信息失败", exc_info=True)


async def _finalize_trace(
    langfuse_service: LangfuseService,
    root_obs: Any,
    session_service: Any,
    session_id: Optional[str],
    already_emitted: bool = False,
) -> AsyncGenerator[str, None]:
    """Langfuse trace 收尾：flush + （可选）yield trace_ready + 保存 trace_id。

    在根 span context manager 之外执行，保证 span 已 end。
    already_emitted=True 时跳过 trace_ready 下发与 trace_id 保存（已在流首处理），
    但仍执行 flush。
    """
    if langfuse_service and langfuse_service.enabled:
        try:
            # flush 为同步上传，丢线程池执行避免阻塞事件循环影响并发用户
            await asyncio.to_thread(langfuse_service.flush)
        except Exception:
            pass

    if not already_emitted:
        trace_id = None
        try:
            trace_id = root_obs.trace_id if root_obs else None
        except Exception:
            pass
        trace_event = json.dumps({"type": "trace_ready", "trace_id": trace_id})
        yield f"data: {trace_event}\n\n"

        # 保存 trace_id 到 Redis 元信息
        if session_service and session_id and trace_id:
            await session_service.save_latest_trace_id(session_id, trace_id)


def load_model_config(config_path: str = MODEL_CONFIG_PATH) -> dict:
    """加载模型配置"""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class _PatchedOpenAIChatModel(OpenAIChatModel):
    """OpenAIChatModel 补丁：结构化输出降级名单纳入泛型 openai.APIError。

    私有推理引擎（SGLang/vLLM 系）对 forced tool_choice 的 JSON schema
    编译 grammar 失败时，可能以非标准形态（如 200 + error body）抛
    openai.APIError 而非标准 400 BadRequestError，导致 agentscope 内置
    的四级 structured-output 降级链在第一级即中断，上下文压缩等
    结构化调用硬失败。将 APIError（BadRequestError 的父类）纳入降级
    名单后，forced 级失败会自动降级到无 tool_choice 的 "none" 级
    （不触发引擎 grammar 编译，靠提示词输出 JSON），绕开引擎 bug。
    """

    @classmethod
    def _get_structured_output_fallback_exceptions(cls):
        import openai
        return (openai.APIError,)

    def _call_api(self, model_name, messages, tools=None, tool_choice=None, **kwargs):
        # 请求出口清洗孤立代理：上下文中残留的孤立代理（如流式输出被
        # chunk 切半的 emoji）若进入请求体，openai 客户端会转义为
        # \uXXXX 形式发出，Python 3.13 上游 JSON 解析器对孤立代理转义
        # 报 "Unterminated string" 并返回 400，整轮对话中断。此处
        # 原地清洗 Msg.content 与文本块，既保住本轮请求也顺带清理内存
        # 上下文，后续轮次不再复发
        try:
            for msg in messages or []:
                content = getattr(msg, "content", None)
                if isinstance(content, str):
                    cleaned = sanitize_surrogates(content)
                    if cleaned != content:
                        msg.content = cleaned
                elif isinstance(content, list):
                    for block in content:
                        text = getattr(block, "text", None)
                        if isinstance(text, str):
                            cleaned = sanitize_surrogates(text)
                            if cleaned != text:
                                block.text = cleaned
        except Exception:
            logger.debug("[chat_service] 模型请求前代理清洗跳过", exc_info=True)
        return super()._call_api(model_name, messages, tools, tool_choice, **kwargs)


def create_model_from_config(model_config: dict, message_pair_id: str = ""):
    """根据配置创建模型实例（业务智能体流式对话用）。

    被 AgentRegistry 的 create_model_fn 调用，每次返回新实例。
    message_pair_id 非空时通过 client_kwargs 注入请求头
    app_serial_number: OIA-AGENTSCOPE-{message_pair_id}（意图识别/改写/编排
    走独立 AsyncOpenAI 客户端，不经此路径，天然不带该头）。
    """
    provider = model_config.get("provider", "openai")
    base_url = model_config.get("base_url", "https://api.deepseek.com/v1")
    model_name = model_config.get("model_name", "deepseek-chat")
    api_key = model_config.get("api_key", "OPENAI_API_KEY")
    # 必须拷贝：model_config 是全局共享 dict（启动时读一次），
    # 直接引用后 pop 会把 context_size 从全局配置里永久删掉，
    # 导致第二次创建 model 起全部走 max_tokens 回退（阈值错乱）
    parameters = dict(model_config.get("parameters", {}))

    if not api_key:
        raise ValueError("环境变量未设置")

    if provider == "openai":
        credential = OpenAICredential(api_key=api_key, base_url=base_url)
        # 上下文窗口独立配置：优先 context_size（模型真实窗口，决定压缩阈值），
        # 缺省回退 max_tokens（兼容旧配置）。两者语义不同：
        # max_tokens 是单次输出上限，会作为请求参数发给 API；
        # context_size 只用于本地 token 估算与压缩触发判断。
        raw_ctx = parameters.pop("context_size", None) or model_config.get(
            "context_size",
        )
        max_tokens = parameters.get("max_tokens", 0)
        context_size = (
            int(raw_ctx)
            if raw_ctx
            else (int(max_tokens) if max_tokens else None)
        )
        # 每轮对话附加请求头：app_serial_number = OIA-AGENTSCOPE-{message_pair_id}
        client_kwargs = (
            {"default_headers": {
                "app_serial_number": f"OIA-AGENTSCOPE-{message_pair_id}"
            }}
            if message_pair_id else None
        )
        model = _PatchedOpenAIChatModel(
            credential=credential,
            model=model_name,
            stream=True,
            parameters=OpenAIChatModel.Parameters(**parameters),
            context_size=context_size,
            # vLLM 部署的 Qwen3 等模型默认开启 thinking，显式关闭，
            # 通过 extra_body 透传 chat_template_kwargs 到请求体
            extra_body={"chat_template_kwargs": {"enable_thinking": True,"reasoning_effort":"low"}},
            **({"client_kwargs": client_kwargs} if client_kwargs else {}),
        )
    else:
        raise ValueError(f"不支持的 provider: {provider}")

    return model


# 需要拦截并转发为 CUSTOM_COMPONENT 事件的组件类型
_COMPONENT_TYPES = {"chart", "volume_chart", "selectable_list", "confirm_action","feedback_chart"}


def _extract_components_from_delta(delta: str):
    """从 TOOL_RESULT_TEXT_DELTA 的 delta 文本中提取组件 JSON。

    沿用原有逻辑：通过花括号匹配提取嵌套 JSON 对象，
    若 type 属于 _COMPONENT_TYPES 则生成 CUSTOM_COMPONENT 事件。
    """
    if not delta:
        return
    i = 0
    while i < len(delta):
        if delta[i] == "{":
            depth = 1
            j = i + 1
            while j < len(delta) and depth > 0:
                if delta[j] == "{":
                    depth += 1
                elif delta[j] == "}":
                    depth -= 1
                j += 1
            if depth == 0:
                try:
                    data = json.loads(delta[i:j])
                    if isinstance(data, dict) and data.get("type") == "feedback_chart":
                        yield {
                            "type": "FEEDBACK_COMPONENT",
                            "component": data,
                        }
                    elif isinstance(data, dict) and data.get("type") in _COMPONENT_TYPES:
                        yield {
                            "type": "CUSTOM_COMPONENT",
                            "component": data,
                        }
                except json.JSONDecodeError:
                    pass
                i = j
            else:
                i += 1
        else:
            i += 1


async def generate_response(
    orchestrator_service,
    messages: List[Dict[str, Any]],
    session_id: str = None,
    user_id: str = None,
    session_service=None,
    langfuse_service: LangfuseService = None,
    agent_id: Optional[str] = None,
    request=None,
    search_enabled: bool = True,
    skills: List[str] = None,
    knowledge_bases: List[str] = None,
    cancel_event: Optional[asyncio.Event] = None,
    workspace_manager: Any = None,
) -> AsyncGenerator[str, None]:
    """根据消息列表生成流式回复（多智能体编排版本）。

    主流程：加载历史 → 快照 → 根span → 输入敏感检测 → 执行编排 → 输出敏感检测 →
    推荐问题 → 持久化 → 文件检测 → trace收尾。
    中断处理：cancel_event 被 set 时主动 raise CancelledError，进入 finally 保证
    落库（success=False）+ flush trace + yield user_abort 事件。
    敏感拦截：输入为纯词典检测（在根 span 内执行，span 嵌套在本轮会话 trace 中），
    命中时 yield message_replace 事件并结束本轮、不持久化；final_output 命中敏感时
    yield message_replace 事件并跳过推荐问题，持久化与文件检测仍执行。
    """
    def _cancelled() -> bool:
        return cancel_event is not None and cancel_event.is_set()

    # ① 加载历史（限 3 轮）并合并当前输入
    history_messages = await _load_history_messages(session_service, session_id)
    full_messages = history_messages + messages
    final_output_parts: List[str] = []
    # 制度问答引用（从编排流 policy_qa_citations 事件累积，随 assistant 消息落库）
    collected_citations: List[dict] = []
    # 博查搜索来源统一总表：bocha_sum 事件按 citation_id 幂等合并、升序；
    # 仅出现新编号时向发送完整快照，落库与 SSE 共用这张总表
    collected_bocha_sum: List[dict] = []
    # 有序内容片段（随 assistant 消息落库 content_parts JSON 列）：
    # text part 来自正文流式 TEXT_BLOCK_DELTA（与组件保持真实穿插顺序），
    # custom_component part 来自工具结果流提取的组件；
    # 整轮无正文流（降级链路只发 summary）时结束时用 summary 兜底补 text
    collected_content_parts: List[dict] = []
    # 本轮是否收到过正文流式 delta（TEXT_BLOCK_DELTA）
    text_delta_seen = False
    # 本轮编排流是否出现过 error 事件（前端文案兜底 + 落库兜底的触发标志）
    error_occurred = False
    # 兜底输出：当无 summary 时（如 pipeline 失败终止、react 无 action 终止），
    # 用 pipeline_intercept.message / react_final.conclusion 作为 final_output，
    # 保证失败轮次也能持久化 assistant 消息
    final_fallback_parts: List[str] = []
    final_output = ""
    aborted = False
    t0 = time.perf_counter()
    ttft_ms = None
    aborted_at_ms = None

    # ② 快照工作目录（用于结束后检测新文件）
    before_files: set = set()
    # 本轮对话配对 id：user/assistant 消息与生成/上传文件共享
    message_pair_id = uuid.uuid4().hex
    # 本轮运行时状态（agent_ids / success / token / 消费的上传文件 id）：
    # 每请求独立对象，替代 orchestrator 单例上的 last_* 属性，
    # 并发请求互不覆盖（修复上传文件绑定被竞态清零的问题）
    round_state = RoundState()
    if session_id:
        try:
            if workspace_manager is not None:
                before_files = await workspace_manager.list_session_files(user_id, session_id)
        except Exception:
            logger.warning("[chat_service] 快照 session 工作目录失败", exc_info=True)

    # ③ 根 span 包裹主体（保持活跃使子 span 自动嵌套）
    # 信息精简：历史仅留最近 2 条×500 字符（全文在 messages 表落库、
    # 模型视角上下文在 llm span 留底），当前轮输入保留全文
    # trace_id 由 message_pair_id 确定性派生（对齐 kb_search 模式）：
    # 显式 TraceContext 让 SDK 标 AS_ROOT，根 span 直接映射为 trace 记录
    # 本体（name/input/output 落库），不依赖服务端对裸根 span 的推断
    lang_trace_id = (
        derive_trace_id(message_pair_id)
        if (langfuse_service and langfuse_service.enabled)
        else None
    )
    root_span_ctx = (
        langfuse_service.start_span(
            "chat-response",
            input={
                "messages": [
                    {"role": m["role"], "content": str(m["content"])[:500]}
                    for m in history_messages[-2:]
                ] + messages,
                "session_id": session_id,
                "user_id": user_id,
            },
            trace_id=lang_trace_id,
        )
        if langfuse_service and langfuse_service.enabled
        else _noop_ctx()
    )
    root_obs = None
    trace_emitted = False
    try:
        with root_span_ctx as root_obs:
            # P0-3: 流首即下发 trace_id（确定性派生，span 活跃即必然存在），
            # 前端可立即关联反馈
            if lang_trace_id and root_obs is not None:
                trace_emitted = True
                yield f"data: {json.dumps({'type': 'trace_ready', 'trace_id': lang_trace_id})}\n\n"
                # 流首保存 trace_id（替代原 finalize 阶段保存，供反馈关联）
                if session_service and session_id:
                    try:
                        await session_service.save_latest_trace_id(session_id, lang_trace_id)
                    except Exception:
                        logger.warning("[chat_service] 流首保存 trace_id 失败", exc_info=True)
            # ③.5 用户输入敏感检测（纯词典，低延迟）。
            # 置于根 span 内执行，使 sensitive-dict-check span 嵌套在本轮会话
            # trace 中（而非形成独立 trace）；命中则发 message_replace 并结束本轮，
            # 不进入编排主流程，也不持久化消息
            sens_input = await dict_check_sensitive(
                _extract_user_input(messages), stage="input"
            )
            if sens_input["blocked"]:
                replace_event = build_message_replace_event(sens_input, stage="input")
                yield f"data: {json.dumps(replace_event, ensure_ascii=False)}\n\n"
                _safe_update_span(root_obs, {
                    "reply": "",
                    "session_id": session_id,
                    "user_id": user_id,
                    "sensitiveBlocked": True,
                    "blockedStage": "input",
                })
                return

            # ④ 执行编排主流程，实时透传 SSE 事件
            async for event_str in orchestrator_service.run(
                full_messages,
                session_id=session_id,
                user_id=user_id,
                session_service=session_service,
                agent_id=agent_id,
                request=request,
                search_enabled=search_enabled,
                skills=skills or [],
                knowledge_bases=knowledge_bases or [],
                langfuse_service=langfuse_service,
                message_pair_id=message_pair_id,
                round_state=round_state,
            ):
                # 取消检查点：每个事件之间检测一次（LLM chunk 之间会回到本循环）
                if _cancelled():
                    aborted = True
                    aborted_at_ms = int((time.perf_counter() - t0) * 1000)
                    raise asyncio.CancelledError()

                # 解析提前：error 事件需替换为友好文案后再透传
                payload = None
                if event_str.startswith("data: ") and event_str.endswith("\n\n"):
                    try:
                        payload = json.loads(event_str[6:].strip())
                    except Exception:
                        payload = None
                event_type = payload.get("type", "") if payload else ""

                if event_type == "error":
                    error_occurred = True
                    logger.warning(
                        "[chat_service] 编排错误（前端已兜底）: %s",
                        payload.get("message", ""),
                    )
                    yield (
                        "data: "
                        + json.dumps(
                            {
                                "type": "error",
                                "message": _FRIENDLY_ERROR_MESSAGE,
                            },
                            ensure_ascii=False,
                        )
                        + "\n\n"
                    )
                elif event_type == "bocha_sum":
                    # 统一出口：bocha_sum 事件不原样透传。按 citation_id 合并到
                    # 本轮总表（collected_bocha_sum），仅出现新编号时发送完整
                    # 快照；落库与 SSE 共用这张总表
                    merged_bocha = merge_bocha_snapshots(
                        collected_bocha_sum, payload.get("bocha_sum") or []
                    )
                    # 来源条目登记后不可变、编号只增不减，
                    # 长度增长 <=> 出现新 citation_id，故用长度比较做变更门控
                    if len(merged_bocha) != len(collected_bocha_sum):
                        collected_bocha_sum = merged_bocha
                        yield (
                            "data: "
                            + json.dumps(
                                {
                                    "type": "bocha_sum",
                                    "bocha_sum": collected_bocha_sum,
                                },
                                ensure_ascii=False,
                            )
                            + "\n\n"
                        )
                    else:
                        # 较旧/无变化快照：丢弃，避免前端覆盖式渲染回退
                        collected_bocha_sum = merged_bocha
                else:
                    yield event_str

                # 解析事件：提取 summary 收集输出 + 检测 CUSTOM_COMPONENT 并转发
                if payload is not None:
                    try:
                        if ttft_ms is None and _compute_ttft_marker(event_type):
                            ttft_ms = int((time.perf_counter() - t0) * 1000)

                        if event_type == "summary":
                            final_output_parts.append(payload.get("content", ""))

                        if event_type == "TEXT_BLOCK_DELTA":
                            # 正文流式增量收为 text part：追加到尾部 text part，
                            # 组件 part 会自然把前后正文切成两段，保持真实穿插顺序
                            _text_delta = payload.get("delta", "")
                            if _text_delta:
                                text_delta_seen = True
                                if (
                                    collected_content_parts
                                    and collected_content_parts[-1]["type"] == "text"
                                ):
                                    collected_content_parts[-1]["content"] += _text_delta
                                else:
                                    collected_content_parts.append({
                                        "type": "text",
                                        "content": _text_delta,
                                    })

                        if event_type == "policy_qa_citations":
                            collected_citations.extend(payload.get("citations") or [])

                        # 兜底收集失败终止事件的文本（仅当无 summary 时才启用）
                        if event_type == "pipeline_intercept":
                            final_fallback_parts.append(payload.get("message", ""))
                        elif event_type == "react_final":
                            final_fallback_parts.append(payload.get("conclusion", ""))

                        if event_type == "TOOL_RESULT_TEXT_DELTA":
                            delta = payload.get("delta", "")
                            for component in _extract_components_from_delta(delta):
                                yield f"data: {json.dumps(component, ensure_ascii=False)}\n\n"
                                # 组件按 SSE 发生顺序记入 content_parts（type 区分
                                # custom_component / feedback_component，component 为本体）
                                collected_content_parts.append({
                                    "type": (
                                        "feedback_component"
                                        if component.get("type") == "FEEDBACK_COMPONENT"
                                        else "custom_component"
                                    ),
                                    "component": component.get("component"),
                                })
                    except Exception:
                        logger.debug("[chat_service] 事件解析跳过", exc_info=True)

            final_output = "\n".join(final_output_parts).strip()
            # 无 summary 时用失败终止事件文本兜底，保证失败轮次也有 assistant 记录
            if not final_output:
                final_output = "\n".join(p for p in final_fallback_parts if p).strip()
            # 报错轮兜底：无任何输出时用友好文案落库，保证会话历史该轮不空白
            # （success=False 由编排层派生）
            if not final_output and error_occurred:
                final_output = _FRIENDLY_ERROR_MESSAGE

            # content_parts 兜底：整轮没收到正文流式 delta（降级链路只发
            # summary）时，用 final_output 补一个 text part，保证不缺正文
            if not text_delta_seen and final_output:
                collected_content_parts.append({
                    "type": "text",
                    "content": final_output,
                })

            # 编排流结束后的取消检查（避免后续步骤继续执行）
            if _cancelled():
                aborted = True
                aborted_at_ms = int((time.perf_counter() - t0) * 1000)
                raise asyncio.CancelledError()

            # 安全敏感检测：编排输出 final_output（推荐问题生成之前）
            # 命中则发送 message_replace 事件并跳过推荐问题（停止后续流式输出）；
            # 持久化与文件检测仍执行，保证会话数据完整；
            # 服务未配置或异常时兜底放行，不影响主流程
            output_blocked = False
            if final_output:
                sens_result = await check_sensitive(final_output, stage="output")
                if sens_result["blocked"]:
                    output_blocked = True
                    replace_event = build_message_replace_event(sens_result, stage="output")
                    yield f"data: {json.dumps(replace_event, ensure_ascii=False)}\n\n"

            # ⑤ 编排流结束立即生成推荐问题（前置，让前端尽快拿到）
            if not output_blocked:
                async for ev in _emit_recommended_questions(
                    orchestrator_service, messages, final_output, langfuse_service,
                ):
                    yield ev

            # ⑥ 持久化对话历史（用户输入 + 智能体输出）+ 回填上传文件 message_id
            persist_ids = await _persist_conversation_history(
                session_service, session_id, user_id,
                messages, final_output,
                upload_file_dao=getattr(
                    request.app.state, "upload_file_dao", None
                ) if request is not None else None,
                message_pair_id=message_pair_id,
                citations=collected_citations,
                bocha_sum=collected_bocha_sum,
                content_parts=collected_content_parts,
                round_state=round_state,
            )

            # ⑦ 检测本轮新文件
            async for ev in _detect_and_emit_files(
                session_id, before_files, session_service,
                workspace_manager=workspace_manager,
                user_id=user_id or "",
                message_pair_id=message_pair_id,
                assistant_message_id=(persist_ids or {}).get("assistant_message_id"),
            ):
                yield ev

            # 更新根 span 输出（context manager 退出时自动 end）
            _safe_update_span(root_obs, {
                "reply": final_output,
                "session_id": session_id,
                "user_id": user_id,
                "ttftMs": ttft_ms,
                "totalMs": int((time.perf_counter() - t0) * 1000),
                "aborted": aborted,
                "abortedAtMs": aborted_at_ms,
                "sensitiveBlocked": output_blocked,
            })
    except asyncio.CancelledError:
        # 中断时吞掉 CancelledError，进入 finally 做清理（不重新 raise，让流优雅结束）
        aborted = True
        if aborted_at_ms is None:
            aborted_at_ms = int((time.perf_counter() - t0) * 1000)
        logger.info(f"[chat_service] 会话被用户中断 session={session_id}")
    finally:
        # 中断时也落库：success 强制 False（round_state 为每请求独立对象，
        # 但中断轮答案被截断，语义上应为失败，仍补 UPDATE 强制标记）
        if aborted:
            if not final_output:
                final_output = "\n".join(p for p in final_fallback_parts if p).strip()
            await _persist_conversation_history(
                session_service, session_id, user_id,
                messages, final_output,
                # 中断轮同样回填上传文件 message_id：本轮已注入消费的文件
                # 若不绑定，下一轮会与新上传文件一起重复注入
                # （用户可见症状：后一次提问用的是前一个文件）
                upload_file_dao=getattr(
                    request.app.state, "upload_file_dao", None
                ) if request is not None else None,
                message_pair_id=message_pair_id,
                citations=collected_citations,
                bocha_sum=collected_bocha_sum,
                content_parts=collected_content_parts,
                round_state=round_state,
            )
            # 中断轮答案被截断，语义上应为失败：补 UPDATE 强制标记
            if session_service and session_id and user_id:
                try:
                    await session_service.mark_last_assistant_failed(session_id, user_id)
                except Exception:
                    logger.warning("[chat_service] 标记中断消息失败状态失败", exc_info=True)

        # ⑧ trace 收尾（保证 span 已 end + flush）
        async for ev in _finalize_trace(
            langfuse_service, root_obs, session_service, session_id,
            already_emitted=trace_emitted,
        ):
            yield ev

        # 中断时给前端发送 user_abort 事件
        if aborted:
            abort_event = {"type": "user_abort", "session_id": session_id, "message": "用户已停止"}
            yield f"data: {json.dumps(abort_event, ensure_ascii=False)}\n\n"
