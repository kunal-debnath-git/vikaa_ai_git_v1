"""
RAG Generation Service — Tab 4 of the RAG Configurator.

Pipeline
────────
1. Retrieve context chunks (calls rag_retrieval_service.retrieve)
2. Generate answer via configured LLM with system prompt + citation instruction
3. Extract & validate citations  [SOURCE: chunk_id]
4. NLI faithfulness check — CrossEncoder entailment per sentence
   If score < threshold → regenerate (up to max_regen attempts)
5. Return answer, citations, faithfulness score, regeneration count

LLM Providers
─────────────
  gemini      — Gemini REST API (GEMINI_API_KEY / GOOGLE_API_KEY)
  databricks  — Databricks Model Serving (existing integration)
  openai      — OpenAI SDK (OPENAI_API_KEY)
  anthropic   — Anthropic SDK (ANTHROPIC_API_KEY)
  azure_openai— OpenAI SDK with Azure endpoint
"""

from __future__ import annotations

import logging
import os
import re
from functools import lru_cache
from typing import Any

import requests as _requests

from backend.integrations.databricks.model_serving import invoke_serving_chat
from backend.services.rag_retrieval_service import retrieve

logger = logging.getLogger(__name__)

# ── Defaults ──────────────────────────────────────────────────────────────────

DEFAULT_LLM_PROVIDER     = "gemini"
DEFAULT_GEMINI_MODEL     = "gemini-2.0-flash"
DEFAULT_DB_MODEL         = "databricks-meta-llama-3-3-70b-instruct"
DEFAULT_TEMPERATURE      = 0.2
DEFAULT_MAX_TOKENS       = 2048
DEFAULT_NLI_MODEL        = "cross-encoder/nli-deberta-v3-base"
DEFAULT_FAITH_THRESHOLD  = 0.7
DEFAULT_MAX_REGEN        = 2

_CITATION_SYSTEM_SUFFIX = (
    "\n\nFor every factual claim you make, append an inline citation in the exact format "
    "[SOURCE: <chunk_id>] referencing the relevant chunk. "
    "Use only chunk_ids from the provided context. Do not invent citations."
)

_DEFAULT_SYSTEM_PROMPT = (
    "You are an expert assistant. Answer using ONLY the provided context. "
    "Be concise and precise."
)


# ── NLI model cache ───────────────────────────────────────────────────────────

@lru_cache(maxsize=4)
def _load_nli_model(model_name: str):
    from sentence_transformers import CrossEncoder
    logger.info("Loading NLI model '%s' (first use)", model_name)
    return CrossEncoder(model_name, max_length=512)


# ── LLM dispatchers ───────────────────────────────────────────────────────────

def _gemini_call(system: str, user: str, *, model: str, max_tokens: int, temperature: float) -> str:
    key = (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()
    if not key:
        raise RuntimeError("No Gemini key (GEMINI_API_KEY / GOOGLE_API_KEY)")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
    payload = {
        "system_instruction": {"parts": [{"text": system}]},
        "contents": [{"role": "user", "parts": [{"text": user}]}],
        "generationConfig": {"maxOutputTokens": max_tokens, "temperature": temperature},
    }
    r = _requests.post(url, json=payload, timeout=60)
    if not r.ok:
        raise RuntimeError(f"Gemini {r.status_code}: {r.text[:300]}")
    data = r.json()
    parts = data["candidates"][0]["content"].get("parts", [])
    texts = [p["text"] for p in parts if p.get("text", "").strip()]
    if not texts:
        raise RuntimeError(f"Gemini returned no text parts: {data}")
    return texts[-1].strip()


def _openai_call(system: str, user: str, *, model: str, max_tokens: int, temperature: float,
                 api_key: str | None = None, base_url: str | None = None) -> str:
    from openai import OpenAI
    key = api_key or os.getenv("OPENAI_API_KEY", "")
    client = OpenAI(api_key=key, base_url=base_url) if base_url else OpenAI(api_key=key)
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        max_tokens=max_tokens,
        temperature=temperature,
    )
    return resp.choices[0].message.content.strip()


def _anthropic_call(system: str, user: str, *, model: str, max_tokens: int, temperature: float) -> str:
    import anthropic
    key = os.getenv("ANTHROPIC_API_KEY", "")
    client = anthropic.Anthropic(api_key=key)
    msg = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return msg.content[0].text.strip()


def _call_llm(
    host: str,
    token: str,
    system: str,
    user: str,
    generation_cfg: dict[str, Any],
) -> str:
    provider    = (generation_cfg.get("llm_provider") or DEFAULT_LLM_PROVIDER).lower()
    model       = (generation_cfg.get("llm_model") or "").strip()
    max_tokens  = int(generation_cfg.get("max_tokens") or DEFAULT_MAX_TOKENS)
    temperature = float(generation_cfg.get("temperature") or DEFAULT_TEMPERATURE)

    if provider == "gemini":
        m = model if model.startswith("gemini") else DEFAULT_GEMINI_MODEL
        return _gemini_call(system, user, model=m, max_tokens=max_tokens, temperature=temperature)

    if provider == "databricks":
        m = model or DEFAULT_DB_MODEL
        return invoke_serving_chat(host, token, m, system_prompt=system, user_prompt=user,
                                   max_tokens=max_tokens, temperature=temperature)

    if provider == "openai":
        m = model or "gpt-4o"
        return _openai_call(system, user, model=m, max_tokens=max_tokens, temperature=temperature)

    if provider == "anthropic":
        m = model or "claude-opus-4-6"
        return _anthropic_call(system, user, model=m, max_tokens=max_tokens, temperature=temperature)

    if provider == "azure_openai":
        m = model or "gpt-4o"
        base_url   = os.getenv("AZURE_OPENAI_ENDPOINT", "")
        api_key    = os.getenv("AZURE_OPENAI_API_KEY", "")
        api_ver    = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01")
        full_base  = f"{base_url.rstrip('/')}/openai/deployments/{m}?api-version={api_ver}"
        return _openai_call(system, user, model=m, max_tokens=max_tokens, temperature=temperature,
                            api_key=api_key, base_url=full_base)

    # Fallback — try Gemini
    logger.warning("Unknown provider '%s' — falling back to Gemini", provider)
    return _gemini_call(system, user, model=DEFAULT_GEMINI_MODEL,
                        max_tokens=max_tokens, temperature=temperature)


# ── Citation helpers ──────────────────────────────────────────────────────────

_CITATION_RE = re.compile(r"\[SOURCE:\s*([^\]]+)\]", re.IGNORECASE)


def _extract_citations(text: str) -> list[str]:
    return [m.strip() for m in _CITATION_RE.findall(text)]


def _validate_citations(
    cited_ids: list[str],
    chunks: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Check each cited chunk_id against the actual chunks.
    Returns { valid: [...], fabricated: [...], resolution_map: {chunk_id: source} }
    """
    available = {str(c.get("chunk_id", "")): c.get("source", "") for c in chunks if c.get("chunk_id")}
    valid, fabricated = [], []
    resolution: dict[str, str] = {}
    for cid in cited_ids:
        if cid in available:
            valid.append(cid)
            resolution[cid] = available[cid]
        else:
            fabricated.append(cid)
    return {"valid": valid, "fabricated": fabricated, "resolution_map": resolution}


# ── NLI faithfulness ──────────────────────────────────────────────────────────

def _split_sentences(text: str) -> list[str]:
    """Rough sentence splitter — good enough for faithfulness scoring."""
    parts = re.split(r"(?<=[.!?])\s+", text.strip())
    return [p for p in parts if len(p.split()) >= 4]  # skip very short fragments


def check_faithfulness(
    answer: str,
    chunks: list[dict[str, Any]],
    *,
    nli_model: str = DEFAULT_NLI_MODEL,
    threshold: float = DEFAULT_FAITH_THRESHOLD,
    content_col: str = "content",
) -> dict[str, Any]:
    """
    Score each sentence in `answer` against the retrieved context.
    A sentence is considered faithful if the max entailment score across all chunks >= threshold.
    Returns { score: float, sentence_scores: [...], faithful: bool }
    """
    sentences = _split_sentences(answer)
    if not sentences:
        return {"score": 1.0, "sentence_scores": [], "faithful": True}

    context_texts = [str(c.get(content_col) or "")[:800] for c in chunks if c.get(content_col)]
    if not context_texts:
        return {"score": 0.0, "sentence_scores": [], "faithful": False}

    try:
        model = _load_nli_model(nli_model)
        sentence_scores = []
        for sent in sentences:
            pairs = [(ctx, sent) for ctx in context_texts]
            # CrossEncoder NLI returns [contradiction, neutral, entailment] per pair
            raw_scores = model.predict(pairs, show_progress_bar=False)
            # Take max entailment score across all context chunks
            if hasattr(raw_scores[0], "__len__"):
                entailment_scores = [float(s[2]) for s in raw_scores]
            else:
                # Single-score NLI models return a single float (entailment probability)
                entailment_scores = [float(s) for s in raw_scores]
            best = max(entailment_scores)
            sentence_scores.append({"sentence": sent[:120], "score": round(best, 4)})
        passing = sum(1 for s in sentence_scores if s["score"] >= threshold)
        overall = round(passing / len(sentence_scores), 4)
        return {
            "score":           overall,
            "sentence_scores": sentence_scores,
            "faithful":        overall >= threshold,
        }
    except Exception as exc:
        logger.warning("NLI faithfulness check failed (%s) — skipping", exc)
        return {"score": None, "sentence_scores": [], "faithful": True, "error": str(exc)}


# ── Context builder ───────────────────────────────────────────────────────────

def _build_context(chunks: list[dict[str, Any]], content_col: str = "content") -> str:
    parts = []
    for i, c in enumerate(chunks, 1):
        chunk_id = c.get("chunk_id", f"chunk_{i}")
        text = str(c.get(content_col) or "").strip()
        if text:
            parts.append(f"[chunk_id: {chunk_id}]\n{text}")
    return "\n\n---\n\n".join(parts)


# ── Public entry point ────────────────────────────────────────────────────────

def generate(
    host: str,
    token: str,
    query: str,
    generation_cfg: dict[str, Any],
    retrieval_cfg: dict[str, Any],
    ingestion_cfg: dict[str, Any] | None = None,
    *,
    pre_retrieved_chunks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Full generation pipeline for one query.

    If pre_retrieved_chunks is provided, skips retrieval.
    Otherwise runs retrieval internally.

    Returns:
    {
      ok: bool,
      query: str,
      answer: str,
      citations: { valid, fabricated, resolution_map },
      faithfulness: { score, sentence_scores, faithful },
      regenerations: int,
      provider: str,
      model: str,
      chunks_used: int,
      chunks: [ ... ]    # chunks used for generation
    }
    """
    provider   = (generation_cfg.get("llm_provider") or DEFAULT_LLM_PROVIDER).lower()
    model      = (generation_cfg.get("llm_model") or "").strip()
    system_raw = (generation_cfg.get("system_prompt") or _DEFAULT_SYSTEM_PROMPT).strip()

    citations_cfg   = generation_cfg.get("citations") or {}
    citations_on    = bool(citations_cfg.get("enabled", True))
    resolve_check   = bool(citations_cfg.get("resolve_check", True))

    faith_cfg       = generation_cfg.get("faithfulness") or {}
    faith_on        = bool(faith_cfg.get("enabled", True))
    nli_model       = (faith_cfg.get("nli_model") or DEFAULT_NLI_MODEL).strip()
    faith_threshold = float(faith_cfg.get("threshold") or DEFAULT_FAITH_THRESHOLD)
    max_regen       = int(faith_cfg.get("max_regeneration") or DEFAULT_MAX_REGEN)

    # ── Retrieve (or use pre-retrieved) ──────────────────────────────────────
    if pre_retrieved_chunks is not None:
        chunks = pre_retrieved_chunks
    else:
        ret = retrieve(host, token, query, retrieval_cfg, ingestion_cfg=ingestion_cfg)
        if not ret.get("ok"):
            return {"ok": False, "error": ret.get("error", "Retrieval failed")}
        chunks = ret.get("results", [])

    if not chunks:
        return {
            "ok": False,
            "error": "No context chunks available for generation. Run retrieval/CRAG first.",
        }

    # ── Build context + system prompt ─────────────────────────────────────────
    context = _build_context(chunks)
    system_prompt = system_raw
    if citations_on:
        system_prompt += _CITATION_SYSTEM_SUFFIX
    user_prompt = f"Question: {query}\n\nContext:\n{context}"

    # ── Generation loop (with faithfulness re-try) ────────────────────────────
    answer = ""
    regenerations = 0
    faithfulness_result: dict[str, Any] = {}
    citations_result: dict[str, Any] = {}

    for attempt in range(max_regen + 1):
        try:
            answer = _call_llm(host, token, system_prompt, user_prompt, generation_cfg)
        except Exception as exc:
            logger.exception("LLM generation failed (attempt %d)", attempt)
            return {"ok": False, "error": f"LLM call failed: {exc}"}

        # Citations
        if citations_on:
            cited = _extract_citations(answer)
            citations_result = _validate_citations(cited, chunks) if resolve_check else {
                "valid": cited, "fabricated": [], "resolution_map": {}
            }
        else:
            citations_result = {"valid": [], "fabricated": [], "resolution_map": {}}

        # Faithfulness
        if faith_on:
            faithfulness_result = check_faithfulness(
                answer, chunks,
                nli_model=nli_model,
                threshold=faith_threshold,
            )
            if faithfulness_result.get("faithful", True):
                break  # passed
            if attempt < max_regen:
                logger.info(
                    "Faithfulness check failed (score=%.2f, attempt=%d) — regenerating",
                    faithfulness_result.get("score", 0), attempt,
                )
                regenerations += 1
                # Add a nudge to the system prompt for regeneration
                system_prompt = system_raw + (
                    "\n\nIMPORTANT: Previous attempt was flagged as potentially unfaithful. "
                    "Stick strictly to the provided context. Do not add any information not in the chunks."
                ) + (_CITATION_SYSTEM_SUFFIX if citations_on else "")
        else:
            faithfulness_result = {"score": None, "sentence_scores": [], "faithful": True}
            break

    resolved_model = model or {
        "gemini":       DEFAULT_GEMINI_MODEL,
        "databricks":   DEFAULT_DB_MODEL,
        "openai":       "gpt-4o",
        "anthropic":    "claude-opus-4-6",
        "azure_openai": "gpt-4o",
    }.get(provider, DEFAULT_GEMINI_MODEL)

    return {
        "ok":             True,
        "query":          query,
        "answer":         answer,
        "citations":      citations_result,
        "faithfulness":   faithfulness_result,
        "regenerations":  regenerations,
        "provider":       provider,
        "model":          resolved_model,
        "chunks_used":    len(chunks),
        "chunks":         chunks,
    }
