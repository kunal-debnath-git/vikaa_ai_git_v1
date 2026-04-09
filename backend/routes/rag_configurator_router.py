"""
RAG Configurator API — Ingestion + Retrieval tabs backend.

Endpoints (all three prefix variants registered via include_router_triplet):
  GET  /tools/rag-configurator/health
  POST /tools/rag-configurator/provision      — create Delta table + VS index
  POST /tools/rag-configurator/ingest         — upload file, chunk, insert, sync
  POST /tools/rag-configurator/ingest-text    — inline text ingest (testing)
  GET  /tools/rag-configurator/index-status   — poll VS index state
  POST /tools/rag-configurator/retrieve       — run retrieval pipeline (query)

Requires env vars:
  DATABRICKS_HOST  (or _STORY)
  DATABRICKS_TOKEN (or _STORY / _ANJALI)
  DATABRICKS_SQL_WAREHOUSE_ID (or DATABRICKS_WAREHOUSE_ID)
"""

from __future__ import annotations

import logging
import os
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile
from pydantic import BaseModel

from backend.services.access_guard import require_whitelisted_user
from backend.services.rag_ingestion_service import provision, run_ingestion
from backend.services.rag_retrieval_service import retrieve, synthesize_answer
from backend.services.rag_crag_service import run_crag
from backend.services.rag_generation_service import generate
from backend.services.rag_observability_service import observability_health, run_ragas_evaluation
from backend.services.rag_summary_service import validate_config, save_config, load_config
from backend.integrations.databricks.vector_search import get_index_status
from backend.integrations.databricks.read_unity_catalog import (
    _normalize_host,
    _resolve_host,
    _resolve_token,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tools", tags=["RAG Configurator"])
router_api_alias = APIRouter(prefix="/api/tools", tags=["RAG Configurator"])
router_root = APIRouter(tags=["RAG Configurator"])

_ALL_ROUTERS = (router, router_api_alias, router_root)

_ROUTE = "/rag-configurator"


# ── Env helpers ───────────────────────────────────────────────────────────────

def _get_databricks_creds() -> tuple[str, str, str]:
    # Mirror the fallback chain used by all other Databricks tools in this project
    host  = _resolve_host()   # DATABRICKS_HOST → DATABRICKS_HOST_STORY
    token = _resolve_token()  # DATABRICKS_TOKEN → DATABRICKS_TOKEN_STORY → DATABRICKS_TOKEN_ANJALI
    warehouse_id = next(
        (os.getenv(k, "").strip() for k in ("DATABRICKS_SQL_WAREHOUSE_ID", "DATABRICKS_WAREHOUSE_ID") if os.getenv(k, "").strip()),
        "",
    )
    if not host or not token:
        raise HTTPException(
            status_code=503,
            detail="Databricks host/token not configured. Set DATABRICKS_HOST + DATABRICKS_TOKEN (or _STORY variants) in .env",
        )
    if not warehouse_id:
        raise HTTPException(
            status_code=503,
            detail="SQL warehouse not configured. Set DATABRICKS_SQL_WAREHOUSE_ID (or DATABRICKS_WAREHOUSE_ID) in .env",
        )
    return host, token, warehouse_id


# ── Pydantic models ───────────────────────────────────────────────────────────

class ProvisionRequest(BaseModel):
    ingestion: dict[str, Any]


class IngestTextRequest(BaseModel):
    ingestion: dict[str, Any]
    text: str
    filename: str = "inline_text.txt"
    mode: str = "check"   # "check" | "replace" | "append" | "skip"


class RetrieveRequest(BaseModel):
    query: str
    retrieval: dict[str, Any]
    ingestion: dict[str, Any] | None = None
    is_retry: bool = False
    confidence: float | None = None
    synthesize: bool = False          # if True, synthesize chunks into an answer
    synthesis_model: str = "gemini-2.0-flash"


class CragRunRequest(BaseModel):
    query: str
    crag: dict[str, Any]
    retrieval: dict[str, Any]
    ingestion: dict[str, Any] | None = None
    synthesize: bool = False
    synthesis_model: str = "gemini-2.0-flash"


class GenerateRequest(BaseModel):
    query: str
    generation: dict[str, Any]
    retrieval: dict[str, Any]
    ingestion: dict[str, Any] | None = None
    # Optional: pass pre-retrieved chunks (e.g. from CRAG) to skip retrieval
    chunks: list[dict[str, Any]] | None = None


class EvalSample(BaseModel):
    question: str
    answer: str
    contexts: list[str]
    ground_truth: str = ""


class ObsEvalRequest(BaseModel):
    samples: list[EvalSample]
    observability: dict[str, Any]


class SummaryValidateRequest(BaseModel):
    config: dict[str, Any]
    check_index: bool = True   # if True, calls index-status API too


class SummarySaveRequest(BaseModel):
    config: dict[str, Any]


# ── Health ────────────────────────────────────────────────────────────────────

def _health_payload() -> dict[str, Any]:
    host  = _resolve_host()
    token = _resolve_token()
    wh = next(
        (os.getenv(k, "").strip() for k in ("DATABRICKS_SQL_WAREHOUSE_ID", "DATABRICKS_WAREHOUSE_ID") if os.getenv(k, "").strip()),
        "",
    )
    return {
        "status": "ok",
        "service": "rag-configurator",
        "databricks_host_configured": bool(host),
        "databricks_token_configured": bool(token),
        "warehouse_configured": bool(wh),
        "host_preview": (host[:40] + "…") if host and len(host) > 40 else (host or None),
        "paths": [
            "/tools/rag-configurator/health",
            "/tools/rag-configurator/provision",
            "/tools/rag-configurator/ingest",
            "/tools/rag-configurator/ingest-text",
            "/tools/rag-configurator/index-status",
        ],
    }


async def _health(request: Request) -> dict[str, Any]:
    return _health_payload()


# ── Provision ─────────────────────────────────────────────────────────────────

async def _provision(
    request: Request,
    body: ProvisionRequest,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    """
    Create (or verify) the Delta table and Vector Search index
    defined in the ingestion config section.
    """
    host, token, warehouse_id = _get_databricks_creds()
    try:
        result = provision(host, token, warehouse_id, body.ingestion)
    except Exception as exc:
        logger.exception("Provision failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result


# ── Ingest (file upload) ──────────────────────────────────────────────────────

async def _ingest_file(
    request: Request,
    file: UploadFile = File(...),
    ingestion_config: str = Form(...),
    mode: str = Form("check"),
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    """
    Ingest an uploaded document (multipart/form-data).
    Fields:
      file             — the document
      ingestion_config — JSON string of the ingestion config section
      mode             — "check" (default) | "replace" | "append" | "skip"

    When mode="check" and a duplicate is found the response contains
      { action_required: true, duplicate_type: "...", ... }
    with HTTP 200 — the frontend should present the user with options
    and re-submit with mode="replace" or "append".
    """
    import json as _json
    host, token, warehouse_id = _get_databricks_creds()

    try:
        ingestion_cfg = _json.loads(ingestion_config)
    except Exception:
        raise HTTPException(status_code=422, detail="ingestion_config must be valid JSON")

    if mode not in ("check", "replace", "append", "skip"):
        raise HTTPException(status_code=422, detail="mode must be check | replace | append | skip")

    file_bytes = await file.read()
    if not file_bytes:
        raise HTTPException(status_code=422, detail="Uploaded file is empty")

    try:
        result = run_ingestion(
            host, token, warehouse_id,
            ingestion_cfg,
            file_bytes,
            filename=file.filename or "upload",
            mode=mode,
        )
    except Exception as exc:
        logger.exception("Ingestion failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    # action_required is NOT an error — return 200 so the frontend can handle it
    if not result.get("ok") and not result.get("action_required"):
        raise HTTPException(status_code=500, detail=result.get("error", "Ingestion failed"))
    return result


# ── Ingest (inline text) ──────────────────────────────────────────────────────

async def _ingest_text(
    request: Request,
    body: IngestTextRequest,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    """
    Ingest a plain-text string directly (useful for testing without a file).
    """
    host, token, warehouse_id = _get_databricks_creds()
    if not body.text.strip():
        raise HTTPException(status_code=422, detail="text field is empty")
    if body.mode not in ("check", "replace", "append", "skip"):
        raise HTTPException(status_code=422, detail="mode must be check | replace | append | skip")
    try:
        result = run_ingestion(
            host, token, warehouse_id,
            body.ingestion,
            body.text.encode("utf-8"),
            filename=body.filename,
            mode=body.mode,
        )
    except Exception as exc:
        logger.exception("Ingestion failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if not result.get("ok") and not result.get("action_required"):
        raise HTTPException(status_code=500, detail=result.get("error", "Ingestion failed"))
    return result


# ── Index status ──────────────────────────────────────────────────────────────

async def _index_status(
    request: Request,
    catalog: str = Query("workspace"),
    schema: str = Query("agentic_rag"),
    index: str = Query("rag_index"),
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    """Return current VS index state."""
    host, token, _ = _get_databricks_creds()
    index_full_name = f"{catalog}.{schema}.{index}"
    try:
        status = get_index_status(host, token, index_full_name)
    except Exception as exc:
        logger.exception("Index status check failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return status


# ── Retrieve ─────────────────────────────────────────────────────────────────

async def _retrieve(
    request: Request,
    body: RetrieveRequest,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    """
    Run the retrieval pipeline for a single query.

    Pipeline: query → HyDE (optional) → VS search → cross-encoder re-rank (optional)

    Returns ranked chunks with scores.
    """
    if not body.query.strip():
        raise HTTPException(status_code=422, detail="query must not be empty")

    host, token, _ = _get_databricks_creds()

    try:
        result = retrieve(
            host, token,
            body.query.strip(),
            body.retrieval,
            ingestion_cfg=body.ingestion,
            is_retry=body.is_retry,
            confidence=body.confidence,
        )
    except Exception as exc:
        logger.exception("Retrieval failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if not result.get("ok"):
        raise HTTPException(status_code=500, detail=result.get("error", "Retrieval failed"))

    # Optional synthesis step
    if body.synthesize and result.get("results"):
        try:
            result["answer"] = synthesize_answer(
                host, token,
                body.query,
                result["results"],
                llm_model=body.synthesis_model,
            )
        except Exception as exc:
            result["answer"] = None
            result["synthesis_error"] = str(exc)

    return result


# ── CRAG run ──────────────────────────────────────────────────────────────────

async def _crag_run(
    request: Request,
    body: CragRunRequest,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    """
    Run the CRAG corrective loop for a single query.

    Pipeline: retrieve → grade → re-query (up to max_iterations) → web fallback
    Returns decision, grade_score, final chunks, optional answer, and trace.
    """
    if not body.query.strip():
        raise HTTPException(status_code=422, detail="query must not be empty")

    host, token, _ = _get_databricks_creds()

    try:
        result = run_crag(
            host, token,
            body.query.strip(),
            body.crag,
            body.retrieval,
            ingestion_cfg=body.ingestion,
            synthesize=body.synthesize,
            synthesis_model=body.synthesis_model,
        )
    except Exception as exc:
        logger.exception("CRAG run failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if not result.get("ok"):
        raise HTTPException(status_code=500, detail=result.get("error", "CRAG failed"))

    return result


# ── Generate ──────────────────────────────────────────────────────────────────

async def _generate(
    request: Request,
    body: GenerateRequest,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    """
    Full generation pipeline: retrieve → LLM answer → citation validation → NLI faithfulness.
    Pass `chunks` in the body to skip internal retrieval (e.g. forward CRAG results).
    """
    if not body.query.strip():
        raise HTTPException(status_code=422, detail="query must not be empty")

    host, token, _ = _get_databricks_creds()

    try:
        result = generate(
            host, token,
            body.query.strip(),
            body.generation,
            body.retrieval,
            ingestion_cfg=body.ingestion,
            pre_retrieved_chunks=body.chunks,
        )
    except Exception as exc:
        logger.exception("Generation failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if not result.get("ok"):
        raise HTTPException(status_code=500, detail=result.get("error", "Generation failed"))

    return result


# ── Observability health ──────────────────────────────────────────────────────

async def _obs_health(
    request: Request,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    """Check which observability backends are configured."""
    host, token, _ = _get_databricks_creds()
    return observability_health(host, token)


# ── Observability evaluate ────────────────────────────────────────────────────

async def _obs_evaluate(
    request: Request,
    body: ObsEvalRequest,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    """
    Run Ragas evaluation on provided Q&A samples.
    Optionally logs aggregate scores to MLflow.
    """
    if not body.samples:
        raise HTTPException(status_code=422, detail="samples list is empty")

    host, token, _ = _get_databricks_creds()

    samples = [s.model_dump() for s in body.samples]
    try:
        result = run_ragas_evaluation(
            samples,
            body.observability,
            host=host,
            token=token,
        )
    except Exception as exc:
        logger.exception("Ragas evaluation failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if not result.get("ok"):
        raise HTTPException(status_code=500, detail=result.get("error", "Evaluation failed"))

    return result


# ── Summary validate ─────────────────────────────────────────────────────────

async def _summary_validate(
    request: Request,
    body: SummaryValidateRequest,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    """
    Run the pre-production checklist against the supplied config.
    Optionally fetches the real VS index state to verify it's ONLINE.
    """
    host, token, _ = _get_databricks_creds()

    index_status = None
    if body.check_index:
        db = body.config.get("ingestion", {}).get("databricks", {})
        catalog    = db.get("catalog", "workspace")
        schema     = db.get("schema", "agentic_rag")
        index_name = db.get("index_name", "rag_index")
        full_name  = f"{catalog}.{schema}.{index_name}"
        try:
            index_status = get_index_status(host, token, full_name)
        except Exception as exc:
            logger.warning("Index status fetch for summary failed: %s", exc)

    return validate_config(body.config, index_status=index_status)


# ── Summary save / load ───────────────────────────────────────────────────────

async def _summary_save(
    request: Request,
    body: SummarySaveRequest,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    """Persist the pipeline config JSON to disk."""
    result = save_config(body.config)
    if not result.get("ok"):
        raise HTTPException(status_code=500, detail=result.get("error", "Save failed"))
    return result


async def _summary_load(
    request: Request,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    """Load the last-saved pipeline config from disk."""
    result = load_config()
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error", "No saved config"))
    return result


# ── Register on all three routers ─────────────────────────────────────────────

for _r in _ALL_ROUTERS:
    _r.add_api_route(f"{_ROUTE}/health",                  _health,         methods=["GET"])
    _r.add_api_route(f"{_ROUTE}/provision",               _provision,      methods=["POST"])
    _r.add_api_route(f"{_ROUTE}/ingest",                  _ingest_file,    methods=["POST"])
    _r.add_api_route(f"{_ROUTE}/ingest-text",             _ingest_text,    methods=["POST"])
    _r.add_api_route(f"{_ROUTE}/index-status",            _index_status,   methods=["GET"])
    _r.add_api_route(f"{_ROUTE}/retrieve",                _retrieve,       methods=["POST"])
    _r.add_api_route(f"{_ROUTE}/crag/run",                _crag_run,       methods=["POST"])
    _r.add_api_route(f"{_ROUTE}/generate",                _generate,       methods=["POST"])
    _r.add_api_route(f"{_ROUTE}/observability/health",    _obs_health,      methods=["GET"])
    _r.add_api_route(f"{_ROUTE}/observability/evaluate",  _obs_evaluate,   methods=["POST"])
    _r.add_api_route(f"{_ROUTE}/summary/validate",        _summary_validate, methods=["POST"])
    _r.add_api_route(f"{_ROUTE}/summary/save",            _summary_save,    methods=["POST"])
    _r.add_api_route(f"{_ROUTE}/summary/load",            _summary_load,    methods=["GET"])
