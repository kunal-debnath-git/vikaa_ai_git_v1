"""
RAG Retrieval Service — query → (HyDE) → VS search → (re-rank) → top-N results.

Pipeline stages
───────────────
1. HyDE  (optional)
   Rewrites the raw query into a hypothetical answer document using a Databricks
   LLM endpoint.  The hypothetical text embeds more like a real answer than a
   question, improving dense-vector alignment.
   apply_on controls when HyDE runs:
     "always"         — every query
     "retry"          — only on CRAG re-query (caller passes is_retry=True)
     "low_confidence" — caller passes confidence score below threshold

2. Vector Search query
   Calls the Databricks VS REST API with query_type HYBRID or SIMILARITY.
   Returns top_k chunks.

3. Cross-encoder re-ranking  (optional)
   Loads a sentence-transformers CrossEncoder locally, scores every
   (query, chunk_content) pair, and returns the top_n by score.
   Model is cached in process memory after first load.

All three stages are controlled entirely by the retrieval config dict
that mirrors the frontend JSON contract.
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Any

import requests

from backend.integrations.databricks.vector_search_query import query_index
from backend.integrations.databricks.vector_search import get_index_status
from backend.integrations.databricks.model_serving import invoke_serving_chat

logger = logging.getLogger(__name__)

# ── Defaults ──────────────────────────────────────────────────────────────────

CATALOG = "workspace"
SCHEMA  = "agentic_rag"
DEFAULT_INDEX        = "rag_index"
DEFAULT_TOP_K        = 10
DEFAULT_TOP_N        = 5
DEFAULT_HYDE_MODEL   = "databricks-meta-llama-3-3-70b-instruct"
DEFAULT_RERANKER     = "BAAI/bge-reranker-large"
DEFAULT_RERANKER_BATCH = 32

_HYDE_SYSTEM = (
    "You are a helpful assistant. Given the user's question, write a single concise "
    "paragraph that DIRECTLY ANSWERS it as if you already know the answer. "
    "Do not hedge. Do not say 'I think'. Output the hypothetical answer only."
)

# ── CrossEncoder cache ────────────────────────────────────────────────────────

@lru_cache(maxsize=4)
def _load_cross_encoder(model_name: str):
    """Load and cache a CrossEncoder model. Returns None if dependencies are broken."""
    try:
        from sentence_transformers import CrossEncoder
        logger.info("Loading CrossEncoder model '%s' (first use)", model_name)
        return CrossEncoder(model_name, max_length=512)
    except (ImportError, Exception) as exc:
        logger.warning("CrossEncoder load failed (%s) — re-ranking disabled", exc)
        return None


# ── Stage 1 — HyDE ────────────────────────────────────────────────────────────

def _apply_hyde(
    host: str,
    token: str,
    query: str,
    hyde_cfg: dict[str, Any],
    *,
    is_retry: bool = False,
    confidence: float | None = None,
    relevance_threshold: float = 0.6,
) -> str:
    """
    Return the hypothetical document string to use as the VS query,
    or the original query if HyDE should not run.
    """
    apply_on = (hyde_cfg.get("apply_on") or "retry").lower()
    llm_model = (hyde_cfg.get("llm") or DEFAULT_HYDE_MODEL).strip()

    should_run = (
        apply_on == "always"
        or (apply_on == "retry" and is_retry)
        or (apply_on == "low_confidence"
            and confidence is not None
            and confidence < relevance_threshold)
    )

    if not should_run:
        return query

    try:
        hypothetical = invoke_serving_chat(
            host, token, llm_model,
            system_prompt=_HYDE_SYSTEM,
            user_prompt=query,
            max_tokens=256,
            temperature=0.3,
        )
        logger.info("HyDE expanded query (model=%s, apply_on=%s)", llm_model, apply_on)
        return hypothetical.strip() or query
    except Exception as exc:
        logger.warning("HyDE failed (%s) — using original query", exc)
        return query


# ── Stage 2 — Vector Search ───────────────────────────────────────────────────

def _run_vs_search(
    host: str,
    token: str,
    index_full_name: str,
    query_text: str,
    top_k: int,
    query_type: str,
    retrieve_columns: list[str],
) -> list[dict[str, Any]]:
    vs_query_type = "HYBRID" if query_type == "hybrid" else "SIMILARITY"
    result = query_index(
        host, token, index_full_name,
        query_text,
        num_results=top_k,
        query_type=vs_query_type,
        columns=retrieve_columns or None,
    )
    if not result["ok"]:
        raise RuntimeError(f"Vector Search query failed: {result.get('error')}")
    return result["results"]


# ── Stage 3 — Cross-encoder re-ranking ────────────────────────────────────────

def _rerank(
    query: str,
    chunks: list[dict[str, Any]],
    top_n: int,
    model_name: str,
    batch_size: int,
    content_col: str = "content",
) -> list[dict[str, Any]]:
    if not chunks:
        return chunks

    model = _load_cross_encoder(model_name)
    if model is None:
        return chunks[:top_n]

    pairs = [(query, str(c.get(content_col) or "")) for c in chunks]
    try:
        scores = model.predict(pairs, batch_size=batch_size, show_progress_bar=False)
    except Exception as exc:
        logger.warning("CrossEncoder predict failed (%s) — returning VS order", exc)
        return chunks[:top_n]

    ranked = sorted(
        zip(scores, chunks),
        key=lambda x: float(x[0]),
        reverse=True,
    )
    results = []
    for score, chunk in ranked[:top_n]:
        chunk = dict(chunk)
        chunk["rerank_score"] = round(float(score), 6)
        results.append(chunk)
    return results


# ── Public entry point ────────────────────────────────────────────────────────

def retrieve(
    host: str,
    token: str,
    query: str,
    retrieval_cfg: dict[str, Any],
    ingestion_cfg: dict[str, Any] | None = None,
    *,
    is_retry: bool = False,
    confidence: float | None = None,
) -> dict[str, Any]:
    """
    Run the full retrieval pipeline for one query.

    Returns:
      {
        ok: bool,
        query_used: str,          # original or HyDE-expanded
        hyde_applied: bool,
        results: [ { chunk fields + score + rerank_score? }, ... ],
        count: int,
        reranked: bool,
        index: str,
      }
    """
    # ── Config resolution ──────────────────────────────────────────────────
    db = (ingestion_cfg or {}).get("databricks", {})
    catalog    = db.get("catalog")    or CATALOG
    schema     = db.get("schema")     or SCHEMA
    index_name = db.get("index_name") or DEFAULT_INDEX
    index_full_name = f"{catalog}.{schema}.{index_name}"

    top_k      = int(retrieval_cfg.get("top_k")         or DEFAULT_TOP_K)
    top_n      = int(retrieval_cfg.get("rerank_top_n")  or DEFAULT_TOP_N)
    query_type = (retrieval_cfg.get("query_type")        or "hybrid").lower()
    retrieve_columns: list[str] = retrieval_cfg.get("retrieve_columns") or []

    hyde_cfg     = retrieval_cfg.get("hyde")    or {}
    reranker_cfg = retrieval_cfg.get("reranker") or {}

    hyde_enabled     = bool(hyde_cfg.get("enabled",     True))
    reranker_enabled = bool(reranker_cfg.get("enabled", True))

    # ── Index state pre-check ─────────────────────────────────────────────
    try:
        status = get_index_status(host, token, index_full_name)
        if not status["ready"]:
            state = status.get("state", "UNKNOWN")
            return {
                "ok": False,
                "error": (
                    f"VS index '{index_full_name}' is not ready (state={state}). "
                    "Wait for it to reach ONLINE — check Index Status on Tab 1."
                ),
            }
    except Exception as exc:
        logger.warning("Index state pre-check failed (%s) — proceeding anyway", exc)

    # ── Stage 1 — HyDE ────────────────────────────────────────────────────
    query_used = query
    hyde_applied = False
    if hyde_enabled:
        expanded = _apply_hyde(
            host, token, query, hyde_cfg,
            is_retry=is_retry,
            confidence=confidence,
        )
        if expanded != query:
            query_used = expanded
            hyde_applied = True

    # ── Stage 2 — VS search ───────────────────────────────────────────────
    raw_chunks = _run_vs_search(
        host, token, index_full_name,
        query_used, top_k, query_type, retrieve_columns,
    )

    # ── Stage 3 — Re-rank ─────────────────────────────────────────────────
    reranked = False
    if reranker_enabled and raw_chunks:
        model_name = (reranker_cfg.get("model") or DEFAULT_RERANKER).strip()
        batch_size = int(reranker_cfg.get("batch_size") or DEFAULT_RERANKER_BATCH)
        try:
            final_chunks = _rerank(query, raw_chunks, top_n, model_name, batch_size)
            reranked = True
        except Exception as exc:
            logger.warning("Re-ranking failed (%s) — returning VS order truncated to top_n", exc)
            final_chunks = raw_chunks[:top_n]
    else:
        final_chunks = raw_chunks[:top_n]

    return {
        "ok": True,
        "query_original": query,
        "query_used": query_used,
        "hyde_applied": hyde_applied,
        "results": final_chunks,
        "count": len(final_chunks),
        "reranked": reranked,
        "index": index_full_name,
        "query_type": query_type,
        "answer": None,           # filled by synthesize_answer() if requested
    }


# ── Synthesis (preview of Generation tab) ────────────────────────────────────

_SYNTH_SYSTEM = (
    "You are a helpful assistant. Using ONLY the provided context chunks, "
    "write a clear, concise, and well-structured answer to the question. "
    "Do not make up any information not present in the context. "
    "Speak in flowing prose — no bullet dump, no 'chunk 1 says / chunk 2 says'. "
    "Each chunk is prefixed with [N][filename] — use the filename to attribute facts. "
    "At the very end of your answer add one line: 'Sources: [file1, file2]' listing "
    "only the filenames that directly contributed facts. No duplicates in the list."
)

DEFAULT_GEMINI_MODEL = "gemini-2.0-flash"


def _gemini_key() -> str:
    return (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()


def _gemini_synthesize(
    query: str,
    context: str,
    *,
    model: str = DEFAULT_GEMINI_MODEL,
    max_tokens: int = 512,
) -> str:
    """Call Gemini REST API to synthesize a prose answer from retrieved chunks."""
    key = _gemini_key()
    if not key:
        raise RuntimeError("No Gemini API key found (GEMINI_API_KEY / GOOGLE_API_KEY)")

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
    user_prompt = f"Question: {query}\n\nContext:\n{context}"
    payload = {
        "system_instruction": {"parts": [{"text": _SYNTH_SYSTEM}]},
        "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
        "generationConfig": {
            "maxOutputTokens": max_tokens,
            "temperature": 0.1,
        },
    }
    r = requests.post(url, json=payload, timeout=30)
    if not r.ok:
        raise RuntimeError(f"Gemini API error {r.status_code}: {r.text[:300]}")
    data = r.json()
    try:
        parts = data["candidates"][0]["content"].get("parts", [])
        texts = [p["text"] for p in parts if p.get("text", "").strip()]
        if not texts:
            raise RuntimeError("Gemini returned no text parts")
        return texts[-1].strip()
    except (KeyError, IndexError) as exc:
        raise RuntimeError(f"Unexpected Gemini response shape: {data}") from exc


def synthesize_answer(
    host: str,
    token: str,
    query: str,
    chunks: list[dict[str, Any]],
    llm_model: str = DEFAULT_GEMINI_MODEL,
    content_col: str = "content",
    max_tokens: int = 512,
) -> str:
    """
    Concatenate retrieved chunks and synthesize a prose answer.

    Tries Gemini first (GEMINI_API_KEY / GOOGLE_API_KEY).
    Falls back to Databricks model serving if Gemini key is absent.
    """
    if not chunks:
        return "No relevant context found to answer this question."

    context_parts = []
    for i, c in enumerate(chunks, 1):
        text = str(c.get(content_col) or "").strip()
        if not text:
            continue
        # Extract just the filename from the source path for clean citation
        raw_source = c.get("source") or ""
        filename = raw_source.replace("\\", "/").split("/")[-1] or raw_source
        label = f"[{i}][{filename}]" if filename else f"[{i}]"
        context_parts.append(f"{label} {text}")
    context = "\n\n".join(context_parts)

    # ── Gemini path (preferred — low cost) ───────────────────────────────────
    if _gemini_key():
        gemini_model = llm_model if llm_model.startswith("gemini") else DEFAULT_GEMINI_MODEL
        try:
            answer = _gemini_synthesize(query, context, model=gemini_model, max_tokens=max_tokens)
            logger.info("Synthesis via Gemini (%s)", gemini_model)
            return answer
        except Exception as exc:
            logger.warning("Gemini synthesis failed (%s) — falling back to Databricks LLM", exc)

    # ── Databricks fallback ───────────────────────────────────────────────────
    db_model = llm_model if not llm_model.startswith("gemini") else DEFAULT_HYDE_MODEL
    user_prompt = f"Question: {query}\n\nContext:\n{context}"
    try:
        answer = invoke_serving_chat(
            host, token, db_model,
            system_prompt=_SYNTH_SYSTEM,
            user_prompt=user_prompt,
            max_tokens=max_tokens,
            temperature=0.1,
        )
        logger.info("Synthesis via Databricks LLM (%s)", db_model)
        return answer.strip()
    except Exception as exc:
        logger.warning("Databricks synthesis LLM call failed (%s)", exc)
        raise
