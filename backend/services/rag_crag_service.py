"""
Agentic CRAG Service — True Agentic RAG, all 3 points.

Pipeline (run_crag)
───────────────────
Stage 1 — Self-Directed Retrieval
  LLM classifies the query: 'direct' or 'retrieve'.
  'direct'   → answer immediately from general knowledge — retriever never called.
  'retrieve' → proceed to Stage 2.

Stage 2 — Goal-Persistent Query Planning
  LLM plans: single-hop or multi-hop?
  Single  → _run_crag_single (standard CRAG loop below).
  Multi   → decompose into 2–3 sub-questions, run _run_crag_single for each,
            collect partial answers, synthesize into one unified response.

Stage 3 — Self-Correction on Failure  (_run_crag_single)
  retrieve → grade (LLM scores chunks 0–1) → decide:
    score >= relevance_threshold              → PASS
    score <  threshold AND retries remain     → RE-QUERY (hyde/rephrase/decompose)
    score <  web_fallback.trigger_score
      AND retries exhausted AND web enabled   → WEB FALLBACK
    else                                      → FAIL (best effort)

Grader LLM
──────────
  Prefers Gemini (GEMINI_API_KEY / GOOGLE_API_KEY), falls back to Databricks serving.
  Prompt asks for a single float 0.0–1.0 per chunk.  Average across graded chunks.
"""

from __future__ import annotations

import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests as _requests

# Persistent session — reuses TCP/TLS connections across Gemini calls (saves ~200ms/call)
_session = _requests.Session()

from backend.services.rag_retrieval_service import retrieve
from backend.integrations.web_search import web_search
from backend.integrations.databricks.model_serving import invoke_serving_chat

logger = logging.getLogger(__name__)

# ── Defaults ──────────────────────────────────────────────────────────────────

DEFAULT_RELEVANCE_THRESHOLD = 0.65
DEFAULT_MAX_ITERATIONS      = 2
DEFAULT_GRADER_LLM          = "gemini-2.0-flash"
DEFAULT_GRADER_TOP_DOCS     = 3
DEFAULT_GRADER_TEMP         = 0.0
DEFAULT_DB_GRADER_LLM       = "databricks-meta-llama-3-3-70b-instruct"

_GRADER_SYSTEM = (
    "You are a relevance grader. Given a question and a document chunk, "
    "output a single decimal number between 0.0 and 1.0 representing how relevant "
    "the chunk is to answering the question. "
    "0.0 = completely irrelevant, 1.0 = highly relevant. "
    "Output ONLY the number — no explanation, no extra text."
)

_REPHRASE_SYSTEM = (
    "You are a query optimizer. Rewrite the user's search query to improve recall "
    "from a dense vector store. Make it more specific and information-rich. "
    "Output ONLY the rewritten query — no explanation."
)

_DECOMPOSE_SYSTEM = (
    "You are a query decomposer. Break the user's complex question into 2–3 "
    "focused sub-questions that together cover the original intent. "
    "Output ONLY the sub-questions, one per line, no numbering."
)

_ROUTE_AND_PLAN_SYSTEM = (
    "You are a query router and planner. Analyse the user's message and respond with "
    "a single JSON object — no markdown, no explanation, nothing else.\n\n"
    "Rules:\n"
    "1. If the message is a greeting, opinion, small-talk, or answerable purely from "
    "   general knowledge without any stored document → "
    '   {"intent": "direct"}\n'
    "2. If the message is a factual/domain-specific question answerable in ONE lookup → "
    '   {"intent": "retrieve", "sub_questions": []}\n'
    "3. If the message is complex and needs 2–3 independent lookups (multi-hop, "
    "   comparison, aggregation) → "
    '   {"intent": "retrieve", "sub_questions": ["atomic q1", "atomic q2"]}\n\n'
    "Maximum 3 sub_questions. When in doubt use intent=retrieve with empty sub_questions."
)

_DIRECT_SYSTEM = (
    "You are a helpful assistant. Answer the user's question concisely from general "
    "knowledge. Do not mention documents or a knowledge base."
)

_MULTI_STEP_SYNTH_SYSTEM = (
    "You are a synthesis assistant. You have received partial answers to several "
    "sub-questions that together address one original question. "
    "Each partial answer includes the source document(s) it was retrieved from. "
    "Combine them into a single coherent, well-structured prose response. "
    "Do not repeat the sub-questions. Do not say 'based on the above'. "
    "Speak naturally as if answering the original question directly. "
    "Use ONLY information present in the partial answers — do not add outside knowledge. "
    "At the very end add one line: 'Sources: [file1, file2]' listing only the document "
    "filenames that directly contributed facts. No duplicates."
)


# ── Gemini helper ─────────────────────────────────────────────────────────────

def _gemini_key() -> str:
    return (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()


def _gemini_call(system: str, user: str, *, max_tokens: int = 64, temperature: float = 0.0) -> str:
    key = _gemini_key()
    if not key:
        raise RuntimeError("No Gemini key")
    model = "gemini-2.0-flash"
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
    payload = {
        "system_instruction": {"parts": [{"text": system}]},
        "contents": [{"role": "user", "parts": [{"text": user}]}],
        "generationConfig": {"maxOutputTokens": max_tokens, "temperature": temperature},
    }
    r = _session.post(url, json=payload, timeout=20)
    if not r.ok:
        raise RuntimeError(f"Gemini {r.status_code}: {r.text[:200]}")
    data = r.json()
    parts = data["candidates"][0]["content"].get("parts", [])
    # Use last non-empty text part — handles thinking-model multi-part responses
    texts = [p["text"] for p in parts if p.get("text", "").strip()]
    if not texts:
        raise RuntimeError("Gemini returned no text parts")
    return texts[-1].strip()


def _llm_call(
    host: str, token: str, system: str, user: str,
    *, max_tokens: int = 64, temperature: float = 0.0,
    db_model: str = DEFAULT_DB_GRADER_LLM,
) -> str:
    """Try Gemini first, fall back to Databricks."""
    if _gemini_key():
        try:
            return _gemini_call(system, user, max_tokens=max_tokens, temperature=temperature)
        except Exception as exc:
            logger.warning("Gemini call failed (%s) — using Databricks LLM", exc)
    return invoke_serving_chat(
        host, token, db_model,
        system_prompt=system, user_prompt=user,
        max_tokens=max_tokens, temperature=temperature,
    )


# ── Stage 2 — Grader ─────────────────────────────────────────────────────────

def _grade_chunk(
    host: str, token: str,
    query: str, chunk_text: str,
    db_model: str,
) -> float:
    user_prompt = f"Question: {query}\n\nChunk:\n{chunk_text[:1500]}"
    try:
        raw = _llm_call(host, token, _GRADER_SYSTEM, user_prompt,
                        max_tokens=16, temperature=0.0, db_model=db_model)
        # Extract the first float from the response
        match = re.search(r"[0-9]+(?:\.[0-9]+)?", raw)
        if match:
            score = float(match.group())
            return max(0.0, min(1.0, score))
    except Exception as exc:
        logger.warning("Grader LLM failed for chunk (%s) — defaulting to 0.5", exc)
    return 0.5


def grade_chunks(
    host: str, token: str,
    query: str,
    chunks: list[dict[str, Any]],
    *,
    top_docs: int = DEFAULT_GRADER_TOP_DOCS,
    db_model: str = DEFAULT_DB_GRADER_LLM,
    content_col: str = "content",
) -> tuple[float, list[dict[str, Any]]]:
    """
    Grade the top_docs chunks.  Returns (avg_score, chunks_with_grade_field).
    """
    graded = chunks[:top_docs]
    if not graded:
        return 0.0, []

    # Grade all top_docs chunks in parallel — each chunk is an independent LLM call
    def _grade_one(idx_chunk: tuple[int, dict]) -> tuple[int, dict, float]:
        idx, chunk = idx_chunk
        text = str(chunk.get(content_col) or "").strip()
        score = _grade_chunk(host, token, query, text, db_model)
        return idx, chunk, score

    workers = min(len(graded), 4)  # cap at 4 to stay within Gemini rate limits
    results_map: dict[int, tuple[dict, float]] = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_grade_one, (i, c)): i for i, c in enumerate(graded)}
        for fut in as_completed(futs):
            try:
                idx, chunk, score = fut.result()
                results_map[idx] = (chunk, score)
            except Exception as exc:
                orig_idx = futs[fut]
                logger.warning("Parallel grade failed for chunk %d (%s) — defaulting 0.5", orig_idx, exc)
                results_map[orig_idx] = (graded[orig_idx], 0.5)

    scored: list[dict[str, Any]] = []
    total = 0.0
    for i in range(len(graded)):
        chunk, score = results_map[i]
        c = dict(chunk)
        c["grade_score"] = round(score, 4)
        scored.append(c)
        total += score

    avg = total / len(graded)
    # Carry grade scores to remaining (un-graded) chunks too
    rest = [dict(c) for c in chunks[top_docs:]]
    return round(avg, 4), scored + rest


# ── Re-query strategies ───────────────────────────────────────────────────────

def _rephrase_query(host: str, token: str, query: str) -> str:
    try:
        return _llm_call(host, token, _REPHRASE_SYSTEM, query, max_tokens=128, temperature=0.3)
    except Exception as exc:
        logger.warning("Rephrase failed (%s) — using original query", exc)
        return query


def _decompose_query(host: str, token: str, query: str) -> list[str]:
    try:
        raw = _llm_call(host, token, _DECOMPOSE_SYSTEM, query, max_tokens=256, temperature=0.3)
        lines = [l.strip(" -•·") for l in raw.splitlines() if l.strip()]
        return [l for l in lines if l][:3] or [query]
    except Exception as exc:
        logger.warning("Decompose failed (%s) — using original query", exc)
        return [query]


# ── Web fallback — chunks from web results ────────────────────────────────────

def _web_results_to_chunks(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for i, r in enumerate(results):
        out.append({
            "chunk_id":    f"web_{i}",
            "source":      r.get("url", ""),
            "source_type": "web",
            "content":     r.get("content", ""),
            "title":       r.get("title", ""),
            "score":       r.get("score"),
            "grade_score": None,
        })
    return out


# ── Points 1 + 3 — Combined route-and-plan (single LLM call) ─────────────────

def _route_and_plan(host: str, token: str, query: str) -> dict[str, Any]:
    """
    Single LLM call that both classifies the query AND plans sub-questions if needed.
    Returns one of:
      {"intent": "direct"}
      {"intent": "retrieve", "sub_questions": []}          # single-hop
      {"intent": "retrieve", "sub_questions": ["q1", ...]} # multi-hop
    Defaults to {"intent": "retrieve", "sub_questions": []} on any failure.
    """
    import json as _json
    _default = {"intent": "retrieve", "sub_questions": []}
    try:
        raw = _llm_call(
            host, token, _ROUTE_AND_PLAN_SYSTEM, query,
            max_tokens=256, temperature=0.0,
        )
        # Strip markdown fences if present
        raw = re.sub(r"```[a-z]*", "", raw).strip().strip("`").strip()
        parsed = _json.loads(raw)
        intent = str(parsed.get("intent", "retrieve")).lower()
        if intent == "direct":
            return {"intent": "direct"}
        subs = parsed.get("sub_questions") or []
        subs = [s.strip() for s in subs if isinstance(s, str) and s.strip()][:3]
        return {"intent": "retrieve", "sub_questions": subs}
    except Exception as exc:
        logger.warning("Route-and-plan failed (%s) — defaulting to single retrieve", exc)
        return _default


def _direct_answer(host: str, token: str, query: str) -> str:
    """Answer a query directly without any retrieval."""
    try:
        return _llm_call(host, token, _DIRECT_SYSTEM, query, max_tokens=512, temperature=0.3)
    except Exception as exc:
        logger.warning("Direct answer LLM call failed (%s)", exc)
        return "I'm sorry, I couldn't generate a response right now."


def _synthesize_multi_step(
    host: str,
    token: str,
    original_query: str,
    sub_answers: list[dict[str, Any]],
) -> str:
    """Combine partial sub-answers into one unified response for the original question."""
    parts = []
    all_source_files: list[str] = []

    for i, sa in enumerate(sub_answers, 1):
        q   = sa.get("question", "")
        a   = sa.get("answer") or ""
        dec = sa.get("decision", "")
        files = sa.get("source_files") or []

        # Skip sub-answers where retrieval failed — don't let weak answers poison synthesis
        if dec == "fail" or not a:
            continue

        file_label = f" [Sources: {', '.join(files)}]" if files else ""
        parts.append(f"Sub-question {i}: {q}{file_label}\nAnswer: {a}")
        for f in files:
            if f not in all_source_files:
                all_source_files.append(f)

    if not parts:
        return (
            "I could not find sufficiently relevant information across the knowledge base "
            "to answer this question. Try rephrasing or enabling web fallback."
        )

    # Cross-document coherence warning — detect when sub-questions pulled from
    # clearly different document domains (> 1 unique source file)
    coherence_note = ""
    if len(all_source_files) > 1:
        coherence_note = (
            f"\n\nNote: This answer draws from multiple documents "
            f"({', '.join(all_source_files)}). Verify that all cited facts "
            f"are relevant to your question."
        )

    combined = "\n\n".join(parts)
    user_prompt = f"Original question: {original_query}\n\nPartial answers:\n{combined}"
    try:
        answer = _llm_call(
            host, token, _MULTI_STEP_SYNTH_SYSTEM, user_prompt,
            max_tokens=768, temperature=0.2,
        )
        return answer + coherence_note
    except Exception as exc:
        logger.warning("Multi-step synthesis failed (%s) — joining partial answers", exc)
        fallback = "\n\n".join(sa.get("answer", "") for sa in sub_answers if sa.get("answer") and sa.get("decision") != "fail")
        return (fallback or "No relevant information found.") + coherence_note


# ── Core single-query CRAG loop (internal) ────────────────────────────────────

def _run_crag_single(
    host: str,
    token: str,
    query: str,
    crag_cfg: dict[str, Any],
    retrieval_cfg: dict[str, Any],
    ingestion_cfg: dict[str, Any] | None = None,
    *,
    synthesize: bool = False,
    synthesis_model: str = "gemini-2.0-flash",
) -> dict[str, Any]:
    """
    Run the CRAG corrective loop for a single query (no planning, no classification).
    Called by run_crag after routing decisions are made.
    """
    relevance_threshold = float(crag_cfg.get("relevance_threshold") or DEFAULT_RELEVANCE_THRESHOLD)
    max_iterations      = min(int(crag_cfg.get("max_iterations") or DEFAULT_MAX_ITERATIONS), 5)
    grader_top_docs     = int(crag_cfg.get("grader_top_docs") or DEFAULT_GRADER_TOP_DOCS)
    grader_temp         = float(crag_cfg.get("grader_temperature") or DEFAULT_GRADER_TEMP)
    requery_strategy    = (crag_cfg.get("requery_strategy") or "hyde").lower()
    db_model            = (crag_cfg.get("grader_llm") or DEFAULT_DB_GRADER_LLM).strip()
    if db_model.startswith("gpt-"):
        db_model = DEFAULT_DB_GRADER_LLM

    web_cfg        = crag_cfg.get("web_fallback") or {}
    web_enabled    = bool(web_cfg.get("enabled", True))
    web_provider   = (web_cfg.get("provider") or "tavily").lower()
    web_max        = int(web_cfg.get("max_results") or 5)
    web_trigger    = float(web_cfg.get("trigger_score") or 0.4)

    trace: list[dict[str, Any]] = []
    current_query = query
    iterations = 0
    final_chunks: list[dict[str, Any]] = []
    grade_score = 0.0
    decision = "fail"
    source = "vs"

    while iterations <= max_iterations:
        is_retry = iterations > 0

        ret = retrieve(
            host, token, current_query, retrieval_cfg,
            ingestion_cfg=ingestion_cfg,
            is_retry=is_retry,
            confidence=grade_score if is_retry else None,
        )
        if not ret.get("ok"):
            return {"ok": False, "error": ret.get("error", "Retrieval failed"), "trace": trace}

        chunks = ret.get("results", [])

        grade_score, chunks_graded = grade_chunks(
            host, token, current_query, chunks,
            top_docs=grader_top_docs,
            db_model=db_model,
        )

        trace.append({
            "iteration":   iterations,
            "query":       current_query,
            "grade_score": grade_score,
            "chunks":      len(chunks_graded),
        })

        if grade_score >= relevance_threshold:
            decision = "pass"
            final_chunks = chunks_graded
            break

        if iterations >= max_iterations:
            if web_enabled and grade_score < web_trigger:
                decision = "web_fallback"
            else:
                decision = "fail"
                final_chunks = chunks_graded
            break

        decision = "requery"
        if requery_strategy == "rephrase":
            current_query = _rephrase_query(host, token, current_query)
        elif requery_strategy == "decompose":
            sub_queries = _decompose_query(host, token, current_query)
            merged: dict[str, dict] = {}
            best_score = 0.0
            for sq in sub_queries:
                sub_ret = retrieve(
                    host, token, sq, retrieval_cfg,
                    ingestion_cfg=ingestion_cfg, is_retry=True,
                )
                if sub_ret.get("ok"):
                    sub_grade, sub_chunks = grade_chunks(
                        host, token, query, sub_ret.get("results", []),
                        top_docs=grader_top_docs, db_model=db_model,
                    )
                    best_score = max(best_score, sub_grade)
                    for c in sub_chunks:
                        cid = c.get("chunk_id") or c.get("source", "") + str(c.get("score", ""))
                        if cid not in merged:
                            merged[cid] = c
            merged_chunks = list(merged.values())
            if merged_chunks:
                grade_score = best_score
                trace.append({
                    "iteration":   f"{iterations}.decompose",
                    "sub_queries": sub_queries,
                    "grade_score": grade_score,
                    "chunks":      len(merged_chunks),
                })
                if grade_score >= relevance_threshold:
                    decision = "pass"
                    final_chunks = merged_chunks
                    break
                final_chunks = merged_chunks

        iterations += 1
        if not final_chunks:
            final_chunks = chunks_graded

    if decision == "web_fallback":
        try:
            web_results = web_search(current_query, provider=web_provider, max_results=web_max)
            web_chunks  = _web_results_to_chunks(web_results)
            web_grade, web_chunks_graded = grade_chunks(
                host, token, query, web_chunks,
                top_docs=grader_top_docs, db_model=db_model,
            )
            source = "web"
            final_chunks = web_chunks_graded
            grade_score  = web_grade
            trace.append({
                "iteration":   "web_fallback",
                "query":       current_query,
                "grade_score": grade_score,
                "chunks":      len(final_chunks),
                "provider":    web_provider,
            })
        except Exception as exc:
            logger.warning("Web fallback failed (%s)", exc)
            source = "vs"
            decision = "fail"

    # Extract unique source filenames from final chunks for citation tracking
    source_files: list[str] = []
    for c in final_chunks:
        raw = c.get("source") or ""
        fname = raw.replace("\\", "/").split("/")[-1]
        if fname and fname not in source_files:
            source_files.append(fname)

    result: dict[str, Any] = {
        "ok":             True,
        "query_original": query,
        "query_used":     current_query,
        "decision":       decision,
        "grade_score":    grade_score,
        "iterations":     iterations,
        "source":         source,
        "results":        final_chunks,
        "count":          len(final_chunks),
        "source_files":   source_files,
        "answer":         None,
        "trace":          trace,
    }

    if synthesize:
        # ── Hallucination guard ───────────────────────────────────────────────
        # Do not synthesize from weak chunks when retrieval confidence is too low.
        # Fabricating an answer from irrelevant chunks is worse than admitting failure.
        if decision == "fail":
            result["answer"] = (
                f"I could not find sufficiently relevant information in the knowledge base "
                f"to answer this question confidently "
                f"(best retrieval score: {grade_score:.2f}, required: {relevance_threshold:.2f}). "
                f"Try rephrasing the question, enabling web fallback, or ingesting more relevant documents."
            )
        elif final_chunks:
            try:
                from backend.services.rag_retrieval_service import synthesize_answer
                result["answer"] = synthesize_answer(
                    host, token, query, final_chunks,
                    llm_model=synthesis_model,
                )
            except Exception as exc:
                result["synthesis_error"] = str(exc)

    return result


# ── Public entry point ────────────────────────────────────────────────────────

def run_crag(
    host: str,
    token: str,
    query: str,
    crag_cfg: dict[str, Any],
    retrieval_cfg: dict[str, Any],
    ingestion_cfg: dict[str, Any] | None = None,
    *,
    synthesize: bool = False,
    synthesis_model: str = "gemini-2.0-flash",
) -> dict[str, Any]:
    """
    Agentic CRAG orchestrator — three-stage pipeline:

    Stage 1 — Self-Directed Retrieval (Point 1)
      LLM classifies the query as 'direct' or 'retrieve'.
      'direct' → answer immediately, no retrieval at all.
      'retrieve' → proceed to Stage 2.

    Stage 2 — Goal-Persistent Planning (Point 3)
      LLM plans whether the query is simple (single-hop) or complex (multi-hop).
      Simple  → single _run_crag_single call.
      Complex → each sub-question runs through _run_crag_single independently,
                partial answers are collected, then synthesized into one response.

    Stage 3 — Self-Correction on Failure (Point 2)
      Each _run_crag_single call runs the full CRAG corrective loop:
      grade → requery/rephrase/decompose → web fallback if exhausted.

    Returns a shape compatible with the existing CragRunRequest router response,
    with additional fields for multi-step and direct-answer decisions:
    {
      ok: bool,
      query_original: str,
      query_used: str,
      decision: "direct_answer"|"multi_step"|"pass"|"requery"|"web_fallback"|"fail",
      grade_score: float | None,
      iterations: int,
      source: "direct"|"vs"|"web"|"mixed",
      results: [ chunk dicts ],
      count: int,
      answer: str | None,
      sub_questions: [ str ],          # present for multi_step only
      sub_answers: [ { ... } ],        # present for multi_step only
      trace: [ ... ],
    }
    """
    query = query.strip()

    # ── Stage 1 + 2: Self-Directed Retrieval + Query Planning (single LLM call) ─
    route = _route_and_plan(host, token, query)
    logger.info("Route-and-plan for '%s': %s", query[:80], route)

    if route["intent"] == "direct":
        answer = _direct_answer(host, token, query)
        return {
            "ok":             True,
            "query_original": query,
            "query_used":     query,
            "decision":       "direct_answer",
            "grade_score":    None,
            "iterations":     0,
            "source":         "direct",
            "results":        [],
            "count":          0,
            "answer":         answer,
            "trace":          [{"iteration": 0, "query": query, "classification": "direct"}],
        }

    sub_questions = route.get("sub_questions") or []
    # Single-hop: empty list OR one sub-question that matches original → standard CRAG loop
    is_multi = len(sub_questions) >= 2
    logger.info("Query plan: %s for '%s'", f"{len(sub_questions)}-hop" if is_multi else "single-hop", query[:80])

    if not is_multi:
        # Simple path — run the standard CRAG loop directly
        return _run_crag_single(
            host, token, query, crag_cfg, retrieval_cfg,
            ingestion_cfg=ingestion_cfg,
            synthesize=synthesize,
            synthesis_model=synthesis_model,
        )

    # ── Multi-step path — run CRAG for each sub-question IN PARALLEL ─────────
    sub_answers_map: dict[int, dict[str, Any]] = {}

    def _run_sub(idx_sq: tuple[int, str]) -> tuple[int, dict[str, Any]]:
        idx, sq = idx_sq
        logger.info("Multi-step: running sub-question %d/%d: '%s'", idx + 1, len(sub_questions), sq[:80])
        sr = _run_crag_single(
            host, token, sq, crag_cfg, retrieval_cfg,
            ingestion_cfg=ingestion_cfg,
            synthesize=True,
            synthesis_model=synthesis_model,
        )
        return idx, sr

    workers = min(len(sub_questions), 3)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_run_sub, (i, sq)): i for i, sq in enumerate(sub_questions)}
        for fut in as_completed(futs):
            try:
                idx, sr = fut.result()
                sub_answers_map[idx] = sr
            except Exception as exc:
                orig_idx = futs[fut]
                logger.warning("Parallel sub-question %d failed (%s)", orig_idx, exc)
                sub_answers_map[orig_idx] = {"ok": False, "answer": "", "decision": "fail",
                                             "grade_score": 0.0, "iterations": 0,
                                             "source": "vs", "count": 0,
                                             "source_files": [], "results": [], "trace": []}

    sub_answers: list[dict[str, Any]] = []
    all_chunks: list[dict[str, Any]] = []
    total_iterations = 0
    sources_seen: set[str] = set()
    combined_trace: list[dict[str, Any]] = []

    for i, sq in enumerate(sub_questions):
        sr = sub_answers_map.get(i, {})
        sub_answers.append({
            "question":     sq,
            "answer":       sr.get("answer") or "",
            "grade_score":  sr.get("grade_score"),
            "decision":     sr.get("decision"),
            "iterations":   sr.get("iterations", 0),
            "source":       sr.get("source", "vs"),
            "chunks":       sr.get("count", 0),
            "source_files": sr.get("source_files", []),
        })
        all_chunks.extend(sr.get("results", []))
        total_iterations += sr.get("iterations", 0)
        sources_seen.add(sr.get("source", "vs"))
        for entry in sr.get("trace", []):
            combined_trace.append({**entry, "sub_question_index": i})

    # Synthesize all partial answers into one unified response
    final_answer = _synthesize_multi_step(host, token, query, sub_answers)

    # Aggregate grade: average of non-None sub-question grades
    grades = [sa["grade_score"] for sa in sub_answers if sa["grade_score"] is not None]
    avg_grade = round(sum(grades) / len(grades), 4) if grades else None

    # Source label
    if len(sources_seen) > 1:
        agg_source = "mixed"
    else:
        agg_source = next(iter(sources_seen), "vs")

    return {
        "ok":             True,
        "query_original": query,
        "query_used":     query,
        "decision":       "multi_step",
        "grade_score":    avg_grade,
        "iterations":     total_iterations,
        "source":         agg_source,
        "results":        all_chunks,
        "count":          len(all_chunks),
        "answer":         final_answer,
        "sub_questions":  sub_questions,
        "sub_answers":    sub_answers,
        "trace":          combined_trace,
    }
