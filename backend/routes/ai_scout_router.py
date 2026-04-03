"""
AI Scout Router
===============
POST /tools/ai-scout/digest               → start digest job; returns job_id or cached result
GET  /tools/ai-scout/digest/status/{id}  → poll job status (pending/done/error)
POST /tools/ai-scout/send-email   → email digest to one or more recipients
GET  /tools/ai-scout/status       → check API keys
GET  /tools/ai-scout/companies    → company dropdown list
GET  /tools/ai-scout/recipients   → email recipient list
"""

import os
import time
import json
import uuid
import asyncio
import logging
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from dataclasses import asdict
from backend.services.access_guard import require_whitelisted_user

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/tools/ai-scout", tags=["AI Scout"])

# ── In-memory digest cache ─────────────────────────────────────────────────────
_cache: dict = {}           # key: "{company}_{period}" → {"result": dict, "ts": float}
_CACHE_TTL = 4 * 3600       # 4 hours

# ── Per-user rate limit (prevents runaway Tavily + LLM calls) ─────────────────
_last_digest_time: dict[str, float] = {}   # email → last generate timestamp
_RATE_LIMIT_SECONDS = 30

# ── Async job store (solves Render 30s request timeout) ───────────────────────
# POST /digest returns a job_id immediately; GET /digest/status/{job_id} polls.
_jobs: dict[str, dict] = {}         # job_id → {status, result?, error?, ts}
_pending_by_key: dict[str, str] = {}  # cache_key → job_id (dedup concurrent reqs)
_JOB_TTL = 600                      # clean up jobs older than 10 min


def _cache_key(period: str, company: str) -> str:
    return f"{company.strip().lower()}_{period}"


def _get_cached(period: str, company: str) -> dict | None:
    key = _cache_key(period, company)
    entry = _cache.get(key)
    if entry and (time.time() - entry["ts"]) < _CACHE_TTL:
        logger.info(f"AI Scout cache HIT for {key}")
        return entry["result"]
    return None


def _set_cached(period: str, company: str, result: dict) -> None:
    _cache[_cache_key(period, company)] = {"result": result, "ts": time.time()}


# ── Recipients config ──────────────────────────────────────────────────────────
def _load_recipients() -> list[dict]:
    path = os.path.join(os.path.dirname(__file__), "..", "..", "frontend", "ai_scout_recipients.json")
    with open(os.path.normpath(path), "r", encoding="utf-8") as f:
        return json.load(f)["recipients"]


# ── Pydantic models ────────────────────────────────────────────────────────────

class DigestRequest(BaseModel):
    period: str = "weekly"
    company_context: str = "Any enterprise"
    force_refresh: bool = False          # bypass cache when True


class EmailRequest(BaseModel):
    digest: dict
    to_emails: list[str] = []            # list of recipient addresses


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("/companies")
def get_companies():
    """Return the ordered list of company options from ai_scout_companies.json."""
    from backend.services.ai_scout_service import get_companies_list
    return {"companies": get_companies_list()}


@router.get("/recipients")
def get_recipients():
    """Return email recipients from ai_scout_recipients.json."""
    try:
        return {"recipients": _load_recipients()}
    except Exception as e:
        logger.error(f"Failed to load recipients: {e}")
        return {"recipients": []}


@router.get("/status")
def ai_scout_status():
    """Check whether Tavily, Gemini/Anthropic keys and SMTP are configured."""
    has_tavily    = bool(os.getenv("TAVILY_API_KEY", "").strip())
    has_gemini    = bool((os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY", "")).strip())
    has_anthropic = bool(os.getenv("ANTHROPIC_API_KEY", "").strip())
    has_smtp      = bool(os.getenv("SMTP_USER", "").strip())
    llm_provider  = "Gemini" if has_gemini else ("Anthropic" if has_anthropic else None)
    return {
        "tavily_configured":    has_tavily,
        "gemini_configured":    has_gemini,
        "anthropic_configured": has_anthropic,
        "llm_provider":         llm_provider,
        "smtp_configured":      has_smtp,
        "ready":                has_tavily and (has_gemini or has_anthropic),
    }


async def _run_digest_job(job_id: str, period: str, company: str) -> None:
    """Background task: runs digest generation in a thread, stores result in _jobs."""
    cache_key = _cache_key(period, company)
    try:
        from backend.services.ai_scout_service import generate_digest as svc_generate, digest_to_markdown
        loop = asyncio.get_running_loop()
        digest = await loop.run_in_executor(
            None, lambda: svc_generate(period=period, company_context=company)
        )
        result = asdict(digest)
        result["markdown"] = digest_to_markdown(digest)
        result["from_cache"] = False
        _set_cached(period, company, result)
        _jobs[job_id] = {"status": "done", "result": result, "ts": time.time()}
    except RuntimeError as e:
        _jobs[job_id] = {"status": "error", "error": str(e), "ts": time.time()}
    except Exception as e:
        logger.error(f"AI Scout job {job_id} failed: {e}", exc_info=True)
        _jobs[job_id] = {"status": "error", "error": "Digest generation failed.", "ts": time.time()}
    finally:
        _pending_by_key.pop(cache_key, None)


@router.post("/digest")
async def handle_generate_digest(req: DigestRequest, _acl=Depends(require_whitelisted_user)):
    """
    Start digest generation. Returns immediately with either:
    - the cached result (if available), or
    - {"job_id": "...", "status": "pending"} — poll GET /digest/status/{job_id}
    """
    if req.period not in ("weekly", "monthly"):
        raise HTTPException(status_code=400, detail="period must be 'weekly' or 'monthly'")

    company = req.company_context or "Any enterprise"
    key = _cache_key(req.period, company)

    # ── Cache hit → return immediately, no job needed ──
    if not req.force_refresh:
        cached = _get_cached(req.period, company)
        if cached:
            cached["from_cache"] = True
            return cached

    # ── Already a pending job for this key → reuse it ──
    if not req.force_refresh and key in _pending_by_key:
        job_id = _pending_by_key[key]
        return {"job_id": job_id, "status": "pending"}

    # ── Rate limit check ──
    user_key = _acl.get("email", "unknown")
    now = time.time()
    last_time = _last_digest_time.get(user_key, 0)
    if not req.force_refresh and (now - last_time) < _RATE_LIMIT_SECONDS:
        wait = int(_RATE_LIMIT_SECONDS - (now - last_time))
        raise HTTPException(status_code=429, detail=f"Please wait {wait}s before generating another digest.")

    # ── Launch background job ──
    job_id = uuid.uuid4().hex[:12]
    _jobs[job_id] = {"status": "pending", "ts": time.time()}
    _pending_by_key[key] = job_id
    _last_digest_time[user_key] = time.time()
    asyncio.create_task(_run_digest_job(job_id, req.period, company))
    return {"job_id": job_id, "status": "pending"}


@router.get("/digest/status/{job_id}")
async def get_digest_status(job_id: str, _acl=Depends(require_whitelisted_user)):
    """Poll for async digest job result."""
    # Lazy cleanup of expired jobs
    cutoff = time.time() - _JOB_TTL
    expired = [k for k, v in _jobs.items() if v["ts"] < cutoff]
    for k in expired:
        _jobs.pop(k, None)

    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found or expired.")
    return job


@router.post("/send-email")
async def send_email(req: EmailRequest, _acl=Depends(require_whitelisted_user)):
    """Send digest to one or more email addresses."""
    from backend.services.ai_scout_service import send_digest_email, digest_from_api_dict

    recipients = req.to_emails
    if not recipients:
        raise HTTPException(status_code=400, detail="No recipients selected.")

    try:
        digest = digest_from_api_dict(req.digest)
        sent, failed = [], []
        for email in recipients:
            try:
                send_digest_email(digest, email)
                sent.append(email)
            except Exception as e:
                logger.error(f"Failed to send to {email}: {e}")
                failed.append(email)

        return {"status": "done", "sent": sent, "failed": failed}

    except Exception as e:
        logger.error(f"AI Scout email error: {e}")
        raise HTTPException(status_code=500, detail=f"Email failed: {e}")
