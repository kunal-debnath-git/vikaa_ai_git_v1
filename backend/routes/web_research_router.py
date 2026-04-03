import os
import json
import uuid
import asyncio
import logging
import time
from threading import Lock
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from tavily import TavilyClient
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage
from backend.services.access_guard import require_whitelisted_user

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/tools", tags=["ReAct Research"])

_tavily: TavilyClient | None = None
_llm: ChatGoogleGenerativeAI | None = None

_TAVILY_CACHE_TTL_SEC = int(os.getenv("TAVILY_CACHE_TTL_SEC", "600"))
_tavily_cache_lock = Lock()
_tavily_cache: dict[tuple, tuple[float, dict]] = {}

# ── Monthly Tavily quota guard ─────────────────────────────────────────────────
# Tracks actual API calls (cache hits don't count). Resets automatically each month.
_tavily_monthly: dict[str, int] = {}   # {"2026-04": 42}
_tavily_counter_lock = Lock()
_TAVILY_MONTHLY_LIMIT = int(os.getenv("TAVILY_MONTHLY_LIMIT", "800"))

# ── Per-user rate limit on /quick (entry point for all new searches) ───────────
_user_last_quick: dict[str, float] = {}
_RATE_LIMIT_SECONDS = 15
_rate_limit_lock = Lock()

# ── Async job store (solves Render 30s timeout for Deep mode) ─────────────────
# POST /web-research returns job_id immediately.
# GET  /web-research/status/{job_id} polls until done/error.
_jobs: dict[str, dict] = {}          # job_id → {status, result?, error?, ts}
_pending_by_query: dict[tuple, str] = {}  # query cache key → job_id (dedup)
_JOB_TTL = 600                       # clean up jobs older than 10 min


def _get_tavily() -> TavilyClient:
    global _tavily
    if _tavily is None:
        key = os.getenv("TAVILY_API_KEY")
        if not key:
            raise RuntimeError("TAVILY_API_KEY is not set.")
        _tavily = TavilyClient(api_key=key)
    return _tavily


def _get_llm() -> ChatGoogleGenerativeAI:
    """Reuse the already-probed model from gemini_resolver — result is cached."""
    global _llm
    if _llm is None:
        from models.gemini_resolver import _resolve_gemini_model
        model = _resolve_gemini_model()
        key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
        _llm = ChatGoogleGenerativeAI(
            model=model,
            google_api_key=key,
            temperature=0.3,
            request_options={"timeout": 60},
        )
        logger.info("Web research using model: %s", model)
    return _llm


# ── Pydantic models ────────────────────────────────────────────────────────────

class ResearchContextItem(BaseModel):
    query: str
    headline: str | None = None
    key_takeaways: list[str] | None = None


class ResearchRequest(BaseModel):
    query: str
    max_results: int = 6
    depth_level: int = 2  # 1=Quick, 2=Standard, 3=Deep
    research_context: list[ResearchContextItem] | None = None
    refine_mode: str | None = None


class SourceItem(BaseModel):
    title: str
    url: str
    snippet: str


class QuickResult(BaseModel):
    quick_answer: str | None
    sources: list[SourceItem]
    low_results_warning: str | None = None


class ResearchResponse(BaseModel):
    headline: str
    tldr: str
    body: list[str]
    key_takeaways: list[str]
    follow_up_questions: list[str]
    quick_answer: str | None = None
    sources: list[SourceItem]
    low_results_warning: str | None = None


# ── Depth-aware JSON schemas ────────────────────────────────────────────────────

_SCHEMA_QUICK = """{
  "headline": "Punchy headline, max 10 words",
  "tldr": "One sentence answer [n]",
  "body": [],
  "key_takeaways": ["point 1 [n]", "point 2 [n]", "point 3 [n]"],
  "follow_up_questions": []
}"""

_SCHEMA_STANDARD = """{
  "headline": "Punchy headline, max 12 words",
  "tldr": "One sentence answer — the essential lede [n]",
  "body": ["Paragraph 1 — context [n]", "Paragraph 2 — main findings [n]"],
  "key_takeaways": ["point 1 [n]", "point 2 [n]", "point 3 [n]", "point 4 [n]"],
  "follow_up_questions": ["<specific search query to go deeper on a gap in the above [n]>", "<specific search query [n]>", "<specific search query [n]>"]
}"""

_SCHEMA_DEEP = """{
  "headline": "Punchy journalistic headline, max 14 words",
  "tldr": "One sentence essential answer [n]",
  "body": [
    "Paragraph 1 — background and context [n]",
    "Paragraph 2 — main findings [n]",
    "Paragraph 3 — implications / what this means going forward [n]"
  ],
  "key_takeaways": ["point 1 [n]", "point 2 [n]", "point 3 [n]", "point 4 [n]", "point 5 [n]"],
  "follow_up_questions": ["<specific search query to go deeper on a gap in the above [n]>", "<specific search query [n]>", "<specific search query [n]>"]
}"""

_SCHEMAS = {1: _SCHEMA_QUICK, 2: _SCHEMA_STANDARD, 3: _SCHEMA_DEEP}

_DEPTH_INSTRUCTIONS = {
    1: (
        "Be extremely concise. Provide headline+tldr if possible, but keep the rest minimal. "
        "Return body=[] and follow_up_questions=[]; key_takeaways should be short and cited."
    ),
    2: "Be concise. Two short paragraphs and four takeaways. Ensure every paragraph includes citations.",
    3: "Be thorough. Three well-developed paragraphs and five takeaways.",
}

_REFINE_MODE_INSTRUCTIONS: dict[str, str] = {
    "assumption_audit": (
        "Decision mode: ASSUMPTION AUDIT. Surface implicit assumptions behind common claims about this topic. "
        "For each assumption, label it using only the evidence: supported / weak / contradicted / unstated. "
        "Every label must reference source numbers [n]. End with gaps where evidence is missing. "
        "Format in body[]: one paragraph per assumption. Start each with "
        "'[ASSUMPTION: <claim>] [VERDICT: supported/weak/contradicted/unstated — source [n]]' then explain."
    ),
    "compare_options": (
        "Decision mode: COMPARE OPTIONS. If the sources describe competing views, products, or approaches, "
        "summarise them in a compact comparison (columns for each option, rows for criteria). "
        "Cite [n] for each cell. Close with neutral decision criteria (no new speculation)."
    ),
    "unknowns_gaps": (
        "Decision mode: UNKNOWNS / GAPS. Emphasise what cannot be concluded from these sources. "
        "List open questions and what additional verification would be needed. Still cite what IS known with [n]."
    ),
    "executive_brief": (
        "Decision mode: EXECUTIVE BRIEF. Structure output for a busy reader: exactly five bullets — "
        "(1) Bottom line (2) Evidence strength (3) Key risks (4) Recommended next step (5) Source pattern [which types dominate]. "
        "Use [n] citations inside bullets where possible. Keep tldr as a one-line elevator version. "
        "Place the five bullets in the body array, one item per bullet. Set key_takeaways to [] and follow_up_questions to []."
    ),
    "devils_advocate": (
        "Decision mode: DEVIL'S ADVOCATE. State the strongest counter-narrative to the mainstream view in these results. "
        "Steel-man it with citations [n], then briefly note what would falsify that counter-view using the same sources."
    ),
}


def _format_research_context(items: list[ResearchContextItem] | None) -> str:
    if not items:
        return ""
    lines: list[str] = []
    for i, it in enumerate(items, 1):
        lines.append(f"Prior run {i}: Q: {it.query}")
        if it.headline:
            lines.append(f"  Headline: {it.headline}")
        if it.key_takeaways:
            for t in it.key_takeaways or []:
                lines.append(f"  - {t}")
    return "\n".join(lines)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _clean_json(raw: str) -> dict:
    """Strip markdown fences and parse JSON; fall back to outer {…} extraction."""
    raw = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        start, end = raw.find("{"), raw.rfind("}") + 1
        if start != -1 and end > start:
            return json.loads(raw[start:end])
        raise


def _check_tavily_quota() -> None:
    """Increment monthly Tavily call counter. Raises RuntimeError if limit reached."""
    month_key = time.strftime("%Y-%m")
    with _tavily_counter_lock:
        # Clean up old month keys on month rollover
        for k in list(_tavily_monthly.keys()):
            if k != month_key:
                del _tavily_monthly[k]
        count = _tavily_monthly.get(month_key, 0)
        if count >= _TAVILY_MONTHLY_LIMIT:
            raise RuntimeError(
                f"Monthly search quota reached ({_TAVILY_MONTHLY_LIMIT} searches used). "
                "Quota resets at the start of next month."
            )
        _tavily_monthly[month_key] = count + 1


def _do_tavily_search(query: str, max_results: int, depth_level: int) -> dict:
    search_depth = "advanced" if depth_level == 3 else "basic"
    cache_key = (query.strip().lower(), max_results, depth_level, search_depth)
    now = time.time()
    with _tavily_cache_lock:
        cached = _tavily_cache.get(cache_key)
        if cached:
            ts, payload = cached
            if now - ts <= _TAVILY_CACHE_TTL_SEC:
                return payload

    # Only count actual API calls, not cache hits.
    _check_tavily_quota()

    payload = _get_tavily().search(
        query=query,
        max_results=max_results,
        search_depth=search_depth,
        include_answer=True,
    )

    with _tavily_cache_lock:
        _tavily_cache[cache_key] = (now, payload)
        if len(_tavily_cache) > 128:
            for k in list(_tavily_cache.keys())[:32]:
                del _tavily_cache[k]

    return payload


def _build_sources(raw_results: list) -> list[SourceItem]:
    return [
        SourceItem(
            title=r.get("title", "Untitled"),
            url=r.get("url", ""),
            snippet=r.get("content", "")[:400],
        )
        for r in raw_results
    ]


def _call_llm(
    query: str,
    sources: list[SourceItem],
    quick_answer: str | None,
    depth_level: int,
    *,
    refine_mode: str | None = None,
    research_context: list[ResearchContextItem] | None = None,
) -> ResearchResponse:
    context = "\n\n".join(
        f"[{i+1}] {s.title}\n{s.url}\n{s.snippet}" for i, s in enumerate(sources)
    )
    schema = _SCHEMAS.get(depth_level, _SCHEMA_STANDARD)
    instructions = _DEPTH_INSTRUCTIONS.get(depth_level, "")

    mode_extra = ""
    if refine_mode and refine_mode in _REFINE_MODE_INSTRUCTIONS:
        mode_extra = _REFINE_MODE_INSTRUCTIONS[refine_mode] + "\n\n"

    prior_block = ""
    rc_text = _format_research_context(research_context)
    if rc_text:
        prior_block = (
            "The user may be continuing earlier research. Use this prior context only to disambiguate intent; "
            "every factual claim in your JSON must still be supported by the search results below.\n"
            f"Prior context:\n{rc_text}\n\n"
        )

    prompt = (
        f"You are a journalist. {instructions}\n"
        f"{mode_extra}"
        f"{prior_block}"
        f"\nCITATION RULES (strict):\n"
        f"- Use source citations in the form [n] where n is the 1-based index in the Search results list.\n"
        f"- Every string in `key_takeaways` must include at least one [n].\n"
        f"- If `tldr` is non-empty, it must include at least one [n].\n"
        f"- Every paragraph in `body` must include at least one [n].\n"
        f"- Every item in `follow_up_questions` must be a concrete search query (not a rhetorical question) and reference a specific gap in the evidence with [n].\n"
        f"- Do not invent facts without citations. If evidence is missing, soften claims and cite the closest supporting sources.\n\n"
        f"BASE EVERY CLAIM on the numbered search results above. If a claim has no supporting source, omit it — prefer omission over speculation.\n\n"
        f"Query: {query}\n"
        + (f"Quick answer: {quick_answer}\n\n" if quick_answer else "\n")
        + f"Search results:\n{context}\n\n"
        f"Return ONLY valid JSON (no markdown, no fences) matching:\n{schema}"
    )

    llm = _get_llm()
    response = llm.invoke([HumanMessage(content=prompt)])
    parsed = _clean_json(response.content)

    return ResearchResponse(
        headline=parsed.get("headline", query),
        tldr=parsed.get("tldr", quick_answer or ""),
        body=parsed.get("body", []),
        key_takeaways=parsed.get("key_takeaways", []),
        follow_up_questions=parsed.get("follow_up_questions", []),
        quick_answer=quick_answer,
        sources=sources,
    )


def _do_research_sync(req: ResearchRequest) -> dict:
    """
    Synchronous research execution — intended to run in a thread pool executor.
    Raises ValueError for user-facing errors (no results, quota), other exceptions
    for unexpected failures.
    """
    try:
        result = _do_tavily_search(req.query, req.max_results, req.depth_level)
    except RuntimeError as e:
        raise ValueError(str(e))

    raw_results = result.get("results", [])
    quick_answer = result.get("answer") or None

    if not raw_results:
        raise ValueError("No results found for this query.")

    warning = None
    if len(raw_results) < 3:
        warning = f"Only {len(raw_results)} source(s) found — analysis may be limited."

    sources = _build_sources(raw_results)

    try:
        resp = _call_llm(
            req.query, sources, quick_answer, req.depth_level,
            refine_mode=req.refine_mode,
            research_context=req.research_context,
        )
        resp.low_results_warning = warning
        return resp.model_dump()
    except (json.JSONDecodeError, KeyError) as e:
        logger.warning("LLM JSON parse failed: %s", e)
        return ResearchResponse(
            headline=req.query, tldr=quick_answer or "Research complete.",
            body=[], key_takeaways=[], follow_up_questions=[],
            quick_answer=quick_answer, sources=sources, low_results_warning=warning,
        ).model_dump()
    except Exception as e:
        logger.error("LLM call failed: %s", e)
        return ResearchResponse(
            headline=req.query, tldr=quick_answer or "Search complete. Summarisation unavailable.",
            body=[], key_takeaways=[], follow_up_questions=[],
            quick_answer=quick_answer, sources=sources, low_results_warning=warning,
        ).model_dump()


async def _run_research_job(job_id: str, req: ResearchRequest) -> None:
    """Background task: runs research in a thread pool, stores result in _jobs."""
    query_key = (req.query.strip().lower(), req.max_results, req.depth_level, req.refine_mode)
    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, lambda: _do_research_sync(req))
        _jobs[job_id] = {"status": "done", "result": result, "ts": time.time()}
    except ValueError as e:
        _jobs[job_id] = {"status": "error", "error": str(e), "ts": time.time()}
    except Exception as e:
        logger.error("Research job %s failed: %s", job_id, e, exc_info=True)
        _jobs[job_id] = {"status": "error", "error": "Research failed. Please try again.", "ts": time.time()}
    finally:
        _pending_by_query.pop(query_key, None)


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.post("/web-research/quick", response_model=QuickResult)
async def web_research_quick(req: ResearchRequest, _acl: dict = Depends(require_whitelisted_user)):
    """Phase 1 — returns Tavily results immediately with no LLM call (~1-2s)."""
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty.")

    # Rate limit: entry point for all new searches — protects Tavily quota.
    user_key = _acl.get("email", "unknown")
    now = time.time()
    with _rate_limit_lock:
        last = _user_last_quick.get(user_key, 0)
        if (now - last) < _RATE_LIMIT_SECONDS:
            wait = int(_RATE_LIMIT_SECONDS - (now - last))
            raise HTTPException(
                status_code=429,
                detail=f"Please wait {wait}s before starting a new search.",
            )
        _user_last_quick[user_key] = now

    try:
        result = _do_tavily_search(req.query, req.max_results, req.depth_level)
    except RuntimeError as e:
        raise HTTPException(status_code=429, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Search failed: {e}")

    raw_results = result.get("results", [])
    warning = None
    if len(raw_results) == 0:
        warning = "No sources found for this query — results may be unreliable."
    elif len(raw_results) < 3:
        warning = f"Only {len(raw_results)} source(s) found — results may be limited."

    return QuickResult(
        quick_answer=result.get("answer") or None,
        sources=_build_sources(raw_results),
        low_results_warning=warning,
    )


@router.post("/web-research")
async def web_research(req: ResearchRequest, _acl: dict = Depends(require_whitelisted_user)):
    """
    Phase 2 — launches an async background job and returns immediately.
    Returns: {"job_id": "...", "status": "pending"}
    Poll GET /web-research/status/{job_id} for the result.
    """
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty.")

    query_key = (req.query.strip().lower(), req.max_results, req.depth_level, req.refine_mode)

    # Reuse an in-flight job for the same query (dedup concurrent requests).
    if query_key in _pending_by_query:
        return {"job_id": _pending_by_query[query_key], "status": "pending"}

    job_id = uuid.uuid4().hex[:12]
    _jobs[job_id] = {"status": "pending", "ts": time.time()}
    _pending_by_query[query_key] = job_id
    asyncio.create_task(_run_research_job(job_id, req))
    return {"job_id": job_id, "status": "pending"}


@router.get("/web-research/status/{job_id}")
async def web_research_status(job_id: str, _acl: dict = Depends(require_whitelisted_user)):
    """Poll for async research job result."""
    # Lazy cleanup of expired jobs
    cutoff = time.time() - _JOB_TTL
    for k in [k for k, v in _jobs.items() if v["ts"] < cutoff]:
        _jobs.pop(k, None)

    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found or expired.")
    return job


@router.get("/web-research/quota")
async def web_research_quota(_acl: dict = Depends(require_whitelisted_user)):
    """Return current monthly Tavily call count."""
    month_key = time.strftime("%Y-%m")
    with _tavily_counter_lock:
        count = _tavily_monthly.get(month_key, 0)
    return {"month": month_key, "used": count, "limit": _TAVILY_MONTHLY_LIMIT}
