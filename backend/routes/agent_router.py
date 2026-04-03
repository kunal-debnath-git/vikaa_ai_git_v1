# File: agent_router.py (updated)

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from fastapi.responses import HTMLResponse

from models.LLM_LangChain import *
from backend.services.access_guard import require_whitelisted_user

router = APIRouter()

class Attachment(BaseModel):
    filename: str
    dataUrl: str

class AgentRequest(BaseModel):
    session_id: str
    query: str
    model: str = "gemini"
    temperature: float = 0.6  # ✅ ADD THIS FIELD (default: Balanced)
    attachments: list[Attachment] = []

@router.get("/", response_class=HTMLResponse)
def get_client_info(request: Request):
    client_ip = request.client.host
    return client_ip

@router.post("/agent/message")
async def handle_message(agent_request: AgentRequest, request: Request, _acl=Depends(require_whitelisted_user)):
    caller_ip = request.client.host
    try:
        response = invoke_langchain(agent_request)
        return {"response": response}
    except Exception as e:
        error_message = f"Error from **'agent_router.py//handle_message Exception'** >>\n  {e}"
        return {"response": error_message}
