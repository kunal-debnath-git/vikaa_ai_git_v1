"""
agent_router.py

POST /agent/message  — accepts query + attachments, starts a background job,
                       returns { job_id, status: "pending" } immediately.
GET  /agent/message/status/{job_id} — poll until status "done" or "error".

This async job pattern decouples LLM latency from Render's 30-second HTTP
timeout. The frontend polls every 2.5 seconds for the result.
"""
import asyncio
import logging
import time
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from models.LLM_LangChain import invoke_langchain
from backend.services.access_guard import require_whitelisted_user

logger = logging.getLogger(__name__)
router = APIRouter()

# ── In-memory job store ──────────────────────────────────────────────────────
_jobs: dict[str, dict] = {}
_JOB_TTL = 300          # seconds — clean up completed jobs after 5 min

# ── Per-user rate limit ──────────────────────────────────────────────────────
_user_last_call: dict[str, float] = {}
_COOLDOWN_SECONDS = 30

# ── Input caps ───────────────────────────────────────────────────────────────
_MAX_QUERY_CHARS = 4000


# ── Pydantic models ──────────────────────────────────────────────────────────
class Attachment(BaseModel):
    filename: str
    dataUrl: str


class AgentRequest(BaseModel):
    session_id: str
    query: str
    model: str = "gemini"
    temperature: float = 0.6
    attachments: list[Attachment] = []


# ── Background worker ────────────────────────────────────────────────────────
async def _run_job(job_id: str, agent_request: AgentRequest) -> None:
    loop = asyncio.get_event_loop()
    try:
        response = await asyncio.wait_for(
            loop.run_in_executor(None, invoke_langchain, agent_request),
            timeout=25.0,
        )
        _jobs[job_id].update({"status": "done", "response": response})
    except asyncio.TimeoutError:
        logger.warning("Agent job %s timed out", job_id)
        _jobs[job_id].update({
            "status": "error",
            "response": "⚠️ Request timed out (25 s). Try a shorter query or smaller attachment.",
        })
    except Exception as exc:
        logger.exception("Agent job %s failed: %s", job_id, exc)
        _jobs[job_id].update({"status": "error", "response": f"⚠️ Agent error: {exc}"})


def _cleanup_old_jobs() -> None:
    cutoff = time.time() - _JOB_TTL
    stale = [jid for jid, j in list(_jobs.items()) if j.get("created", 0) < cutoff]
    for jid in stale:
        _jobs.pop(jid, None)


# ── Routes ───────────────────────────────────────────────────────────────────
@router.get("/", response_class=HTMLResponse)
def get_client_info(request: Request):
    return request.client.host


@router.post("/agent/message")
async def handle_message(
    agent_request: AgentRequest,
    request: Request,
    user: dict = Depends(require_whitelisted_user),
):
    # ── Rate limit (skip for local-dev) ─────────────────────────────────────
    if user.get("acl_status") != "local-dev":
        uid = user.get("email") or request.client.host
        now = time.time()
        last = _user_last_call.get(uid, 0)
        if now - last < _COOLDOWN_SECONDS:
            remaining = int(_COOLDOWN_SECONDS - (now - last))
            raise HTTPException(
                status_code=429,
                detail=f"Please wait {remaining}s before sending again.",
            )
        _user_last_call[uid] = now

    # ── Query length cap ─────────────────────────────────────────────────────
    if len(agent_request.query) > _MAX_QUERY_CHARS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Query too long ({len(agent_request.query)} chars). "
                f"Please keep it under {_MAX_QUERY_CHARS} characters."
            ),
        )

    _cleanup_old_jobs()
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "pending", "response": None, "created": time.time()}
    asyncio.create_task(_run_job(job_id, agent_request))

    return {"job_id": job_id, "status": "pending"}


@router.get("/agent/message/status/{job_id}")
async def get_job_status(
    job_id: str,
    _acl: dict = Depends(require_whitelisted_user),
):
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found or expired.")
    return {"job_id": job_id, "status": job["status"], "response": job.get("response")}
