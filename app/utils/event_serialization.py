"""Agent 事件的 JSON 序列化，仅对代理字符编码异常提供传输/落库兜底。"""

import json
import logging

from pydantic import BaseModel
from pydantic_core import PydanticSerializationError

logger = logging.getLogger(__name__)


def _fix_str_surrogates(s: str) -> str:
    """还原代理对为完整字符，孤立代理替换为 U+FFFD。

    流式 chunk 边界可能把 emoji 的 UTF-16 代理对切成两半，各自独立
    解码后成为孤立代理字符，无法编码为 UTF-8（pymysql 落库、JSON
    序列化均会抛 UnicodeEncodeError）。绕道 UTF-16 往返让相邻的
    代理对（高代理 + 低代理）重组为完整 emoji 字符，残余孤立代理
    降级为 U+FFFD。

    注意：本 docstring 及任何源码字符串中禁止书写反斜杠加 u 开头
    的代理转义示例（如反斜杠uD83D）——普通字符串里转义会实际生效，
    Python 3.13 编译期序列化常量时直接抛 UnicodeEncodeError 导致
    模块无法 import。测试数据统一用 chr(0xD83D) 运行时构造。
    """
    try:
        s.encode("utf-8")
        return s
    except UnicodeEncodeError:
        return s.encode("utf-16-le", "surrogatepass").decode("utf-16-le", "replace")


def sanitize_surrogates(obj):
    """递归清洗 str/list/dict 中的孤立代理字符，其余类型原样返回。"""
    if isinstance(obj, str):
        return _fix_str_surrogates(obj)
    if isinstance(obj, list):
        return [sanitize_surrogates(v) for v in obj]
    if isinstance(obj, dict):
        return {k: sanitize_surrogates(v) for k, v in obj.items()}
    return obj


def serialize_agent_event(event: BaseModel) -> str:
    try:
        return event.model_dump_json()
    except PydanticSerializationError as exc:
        # Pydantic 将 UnicodeEncodeError 包装为序列化异常；其他错误仍向上传递。
        if "UnicodeEncodeError" not in str(exc) or "surrogates not allowed" not in str(exc):
            raise
        # 保留 JSON 模式的字段序列化规则，将代理项转义，避免删除跨 chunk 字符。
        payload = json.dumps(event.model_dump(mode="json"), ensure_ascii=True, separators=(",", ":"))
        logger.warning("Agent event Unicode escape fallback: event_class=%s", type(event).__name__)
        return payload
