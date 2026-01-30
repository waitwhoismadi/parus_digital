from pydantic import BaseModel
from typing import Optional, Any

class AnalyticsRequest(BaseModel):
    query: str
    session_id: str

class AnalyticsResponse(BaseModel):
    answer_text: str
    plot_base64: Optional[str] = None # Если был построен график
    executed_code: str
    is_error: bool = False