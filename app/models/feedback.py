from pydantic import BaseModel
from typing import Optional


class FeedbackRequest(BaseModel):
    # 消息定位（落库 messages.feedback，必填）
    session_id: str
    message_pair_id: str
    # Langfuse 评分用（双写保留）
    trace_id: str
    # None 表示取消评价：清除 feedback 与 feedback_comment（回到未评价）
    liked: Optional[bool] = None
    comment: Optional[str] = None


class FeedbackResponse(BaseModel):
    code: int = 200
    msg: str = "success"
    data: dict = {}