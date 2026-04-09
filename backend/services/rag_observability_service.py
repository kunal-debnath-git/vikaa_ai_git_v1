"""
RAG Observability Service — Tab 5 of the RAG Configurator.

Capabilities
────────────
1. MLflow  — log pipeline run metrics + params to a Databricks or local MLflow experiment
2. LangSmith — optional execution graph tracing (LANGSMITH_API_KEY required)
3. Ragas  — automated evaluation of faithfulness / answer relevancy / context recall / precision

Health check
────────────
/observability/health — reports which backends are reachable & configured.

Evaluation
──────────
/observability/evaluate — accepts a list of { question, answer, contexts, ground_truth? }
                          records, runs Ragas metrics, optionally logs to MLflow.
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from typing import Any

logger = logging.getLogger(__name__)

# ── Env helpers ───────────────────────────────────────────────────────────────

def _mlflow_uri() -> str:
    return (
        os.getenv("MLFLOW_TRACKING_URI")
        or os.getenv("DATABRICKS_HOST", "")   # Databricks MLflow uses the workspace host
        or "databricks"
    ).strip()

def _langsmith_key() -> str:
    return (os.getenv("LANGSMITH_API_KEY") or os.getenv("LANGCHAIN_API_KEY") or "").strip()

def _gemini_key() -> str:
    return (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()


# ── Health check ──────────────────────────────────────────────────────────────

def observability_health(host: str, token: str) -> dict[str, Any]:
    """
    Probe which observability backends are configured and reachable.
    Does NOT make expensive remote calls — just checks env/imports.
    """
    result: dict[str, Any] = {"ok": True, "backends": {}}

    # MLflow
    try:
        import mlflow  # noqa: F401
        uri = _mlflow_uri()
        result["backends"]["mlflow"] = {
            "configured": True,
            "tracking_uri": uri[:60] + ("…" if len(uri) > 60 else ""),
        }
    except ImportError:
        result["backends"]["mlflow"] = {"configured": False, "error": "mlflow not installed"}

    # LangSmith
    ls_key = _langsmith_key()
    result["backends"]["langsmith"] = {
        "configured": bool(ls_key),
        "note": "Set LANGSMITH_API_KEY to enable" if not ls_key else None,
    }

    # Ragas
    try:
        import ragas  # noqa: F401
        result["backends"]["ragas"] = {"configured": True}
    except ImportError:
        result["backends"]["ragas"] = {"configured": False, "error": "ragas not installed"}

    return result


# ── MLflow logger ─────────────────────────────────────────────────────────────

def log_to_mlflow(
    experiment: str,
    run_name: str,
    params: dict[str, Any],
    metrics: dict[str, float],
    *,
    tracking_uri: str | None = None,
    tags: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Log a single pipeline run to MLflow. Returns run_id."""
    try:
        import mlflow
        uri = tracking_uri or _mlflow_uri()
        mlflow.set_tracking_uri(uri)
        mlflow.set_experiment(experiment)
        with mlflow.start_run(run_name=run_name, tags=tags or {}) as run:
            for k, v in params.items():
                try:
                    mlflow.log_param(k, v)
                except Exception:
                    pass
            for k, v in metrics.items():
                if v is not None:
                    try:
                        mlflow.log_metric(k, float(v))
                    except Exception:
                        pass
            return {"ok": True, "run_id": run.info.run_id, "experiment": experiment}
    except Exception as exc:
        logger.warning("MLflow logging failed: %s", exc)
        return {"ok": False, "error": str(exc)}


# ── Ragas evaluation ──────────────────────────────────────────────────────────

def _build_ragas_llm():
    """Return a LangChain-wrapped LLM for Ragas scoring (Gemini preferred)."""
    if _gemini_key():
        try:
            from langchain_google_genai import ChatGoogleGenerativeAI
            return ChatGoogleGenerativeAI(
                model="gemini-2.0-flash",
                google_api_key=_gemini_key(),
                temperature=0.0,
            )
        except Exception as exc:
            logger.warning("Gemini LLM for Ragas failed (%s)", exc)

    # Fallback: OpenAI
    openai_key = os.getenv("OPENAI_API_KEY", "")
    if openai_key:
        try:
            from langchain_openai import ChatOpenAI
            return ChatOpenAI(model="gpt-4o-mini", api_key=openai_key, temperature=0.0)
        except Exception as exc:
            logger.warning("OpenAI LLM for Ragas failed (%s)", exc)

    return None


def _build_ragas_embeddings():
    """Return a LangChain embeddings model for Ragas (Gemini preferred)."""
    if _gemini_key():
        try:
            from langchain_google_genai import GoogleGenerativeAIEmbeddings
            return GoogleGenerativeAIEmbeddings(
                model="models/text-embedding-004",
                google_api_key=_gemini_key(),
            )
        except Exception as exc:
            logger.warning("Gemini embeddings for Ragas failed (%s)", exc)

    openai_key = os.getenv("OPENAI_API_KEY", "")
    if openai_key:
        try:
            from langchain_openai import OpenAIEmbeddings
            return OpenAIEmbeddings(api_key=openai_key)
        except Exception as exc:
            logger.warning("OpenAI embeddings for Ragas failed (%s)", exc)

    return None


def run_ragas_evaluation(
    samples: list[dict[str, Any]],
    obs_cfg: dict[str, Any],
    *,
    host: str = "",
    token: str = "",
) -> dict[str, Any]:
    """
    Run Ragas evaluation on a list of Q&A samples.

    Each sample:
      { question: str, answer: str, contexts: [str, ...], ground_truth?: str }

    Returns:
      { ok, scores: { faithfulness, answer_relevancy, context_recall, context_precision },
        per_sample: [...], mlflow_run_id?: str }
    """
    if not samples:
        return {"ok": False, "error": "No evaluation samples provided"}

    metrics_cfg = (obs_cfg.get("ragas") or {}).get("metrics") or {}
    want_faith   = bool(metrics_cfg.get("faithfulness", True))
    want_ans_rel = bool(metrics_cfg.get("answer_relevancy", True))
    want_recall  = bool(metrics_cfg.get("context_recall", True))
    want_prec    = bool(metrics_cfg.get("context_precision", True))
    min_faith    = float((obs_cfg.get("ragas") or {}).get("min_faithfulness", 0.7))
    min_rel      = float((obs_cfg.get("ragas") or {}).get("min_answer_relevancy", 0.75))
    log_mlflow   = (obs_cfg.get("ragas") or {}).get("log_to_mlflow", True)

    mlflow_cfg   = obs_cfg.get("mlflow") or {}
    experiment   = mlflow_cfg.get("experiment") or "agentic-rag-eval"

    try:
        from ragas import evaluate
        from ragas.metrics import (
            faithfulness,
            answer_relevancy,
            context_recall,
            context_precision,
        )
        from datasets import Dataset
    except ImportError as exc:
        return {"ok": False, "error": f"ragas/datasets not installed: {exc}"}

    # Build metric list
    selected_metrics = []
    if want_faith:   selected_metrics.append(faithfulness)
    if want_ans_rel: selected_metrics.append(answer_relevancy)
    if want_recall:  selected_metrics.append(context_recall)
    if want_prec:    selected_metrics.append(context_precision)

    if not selected_metrics:
        return {"ok": False, "error": "No metrics selected"}

    # Validate samples
    data: dict[str, list] = {"question": [], "answer": [], "contexts": [], "ground_truth": []}
    for s in samples:
        data["question"].append(str(s.get("question") or ""))
        data["answer"].append(str(s.get("answer") or ""))
        ctx = s.get("contexts") or []
        data["contexts"].append([str(c) for c in ctx] if ctx else [""])
        data["ground_truth"].append(str(s.get("ground_truth") or ""))

    dataset = Dataset.from_dict(data)

    llm        = _build_ragas_llm()
    embeddings = _build_ragas_embeddings()

    try:
        t0 = time.time()
        result_ds = evaluate(
            dataset,
            metrics=selected_metrics,
            llm=llm,
            embeddings=embeddings,
            raise_exceptions=False,
        )
        elapsed = round(time.time() - t0, 2)
    except Exception as exc:
        logger.exception("Ragas evaluate() failed")
        return {"ok": False, "error": str(exc)}

    # Aggregate scores
    scores: dict[str, Any] = {}
    try:
        df = result_ds.to_pandas()
        for col in ["faithfulness", "answer_relevancy", "context_recall", "context_precision"]:
            if col in df.columns:
                scores[col] = round(float(df[col].mean(skipna=True)), 4)
        per_sample = df.to_dict(orient="records")
    except Exception as exc:
        logger.warning("Ragas result parsing failed: %s", exc)
        scores = {}
        per_sample = []

    # Pass/fail flags
    flags = {}
    if "faithfulness" in scores:
        flags["faithfulness_ok"] = scores["faithfulness"] >= min_faith
    if "answer_relevancy" in scores:
        flags["relevancy_ok"] = scores["answer_relevancy"] >= min_rel

    out: dict[str, Any] = {
        "ok":         True,
        "scores":     scores,
        "flags":      flags,
        "per_sample": per_sample,
        "sample_count": len(samples),
        "elapsed_s":  elapsed,
    }

    # Log to MLflow
    if log_mlflow and bool((obs_cfg.get("mlflow") or {}).get("enabled", True)):
        ml_result = log_to_mlflow(
            experiment=experiment,
            run_name=f"ragas-eval-{uuid.uuid4().hex[:8]}",
            params={"sample_count": len(samples), "metrics": list(scores.keys())},
            metrics=scores,
            tracking_uri=mlflow_cfg.get("tracking_uri") or None,
        )
        out["mlflow"] = ml_result

    return out


# ── Pipeline run logger (called from other services) ─────────────────────────

def log_pipeline_run(
    obs_cfg: dict[str, Any],
    run_data: dict[str, Any],
) -> None:
    """
    Fire-and-forget MLflow log for a single pipeline execution.
    Swallows all exceptions — observability must never break the main path.

    run_data expected keys (all optional):
      query, decision, grade_score, faithfulness_score, latency_ms,
      chunks_used, iterations, source, provider
    """
    try:
        if not bool((obs_cfg.get("mlflow") or {}).get("enabled", True)):
            return
        mlflow_cfg = obs_cfg.get("mlflow") or {}
        experiment = mlflow_cfg.get("experiment") or "agentic-rag-prod"
        alert_ms   = int(mlflow_cfg.get("latency_alert_ms") or 5000)

        params = {
            k: str(v) for k, v in run_data.items()
            if k in ("query", "decision", "source", "provider", "model") and v is not None
        }
        metrics: dict[str, float] = {}
        for k in ("grade_score", "faithfulness_score", "chunks_used", "iterations", "latency_ms"):
            v = run_data.get(k)
            if v is not None:
                metrics[k] = float(v)

        tags: dict[str, str] = {}
        if "latency_ms" in metrics and metrics["latency_ms"] > alert_ms:
            tags["latency_alert"] = "true"

        log_to_mlflow(
            experiment=experiment,
            run_name="pipeline-run",
            params=params,
            metrics=metrics,
            tracking_uri=mlflow_cfg.get("tracking_uri") or None,
            tags=tags,
        )
    except Exception:
        pass  # never raise from here
