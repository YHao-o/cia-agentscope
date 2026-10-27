"""文件变更检测：对比前后快照，计算本轮新生成的文件。"""

import os

# 生成文件返回白名单：常规文档/表格/演示/图片及指定代码类型会下发前端并落库，
# svg/log/json 等过程文件被屏蔽，不进入 files_generated 事件与 session_files
ALLOWED_EXTENSIONS: frozenset[str] = frozenset({
    # 文档
    "doc", "docx", "pdf", "md", "txt",
    # 表格
    "xls", "xlsx", "csv",
    # 演示
    "ppt", "pptx",
    # 图片
    "png", "jpg", "jpeg",
    # 网页与代码
    "html", "css", "js", "py","json","yml","yaml","bak",
})


def is_allowed_file(rel_path: str) -> bool:
    """判断扩展名（大小写不敏感）是否在返回白名单内；无扩展名视为不允许。"""
    _, ext = os.path.splitext(rel_path)
    return ext.lstrip(".").lower() in ALLOWED_EXTENSIONS


def filter_allowed(paths: list[str]) -> list[str]:
    """过滤路径列表，仅保留白名单扩展名的文件，保持原顺序。"""
    return [p for p in paths if is_allowed_file(p)]


def diff(
    before: set[str], after: set[str], exclude_prefixes: tuple[str, ...] = ()
) -> list[str]:
    """返回 after - before 的新相对路径列表，按路径字符串升序排序。

    exclude_prefixes：排除的相对路径前缀（平台写入目录，非模型产物）。
    如 uploads/——上传文件解析内容由 persist_upload_files_to_workspace
    在 before 快照之后写入沙箱，不排除会被误判为"本轮生成的文件"。
    """
    added = after - before
    if exclude_prefixes:
        added = {p for p in added if not p.startswith(exclude_prefixes)}
    return sorted(added)
