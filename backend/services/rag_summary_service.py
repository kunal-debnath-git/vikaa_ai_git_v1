"""
RAG Summary Service — Tab 6 of the RAG Configurator.

validate_config  — runs the pre-production checklist rules against the full config JSON.
save_config      — persists the config JSON to disk (configs/rag_pipeline_config.json).
load_config      — loads the last-saved config from disk.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_CONFIG_DIR  = Path(__file__).resolve().parents[2] / "configs"
_CONFIG_FILE = _CONFIG_DIR / "rag_pipeline_config.json"


# ── Checklist rules ───────────────────────────────────────────────────────────

def _check(label: str, passed: bool, detail: str = "") -> dict[str, Any]:
    return {"label": label, "passed": passed, "detail": detail}


def validate_config(
    config: dict[str, Any],
    *,
    index_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Run all pre-production checklist rules.
    `index_status` is the result of get_index_status() — pass it in from the router
    so we can report the real VS state without making duplicate HTTP calls.

    Returns:
      { ok: bool, passed: int, total: int, items: [ { label, passed, detail } ] }
    """
    items: list[dict[str, Any]] = []

    ing  = config.get("ingestion", {})
    ret  = config.get("retrieval", {})
    crag = config.get("crag", {})
    gen  = config.get("generation", {})
    obs  = config.get("observability", {})

    db = ing.get("databricks", {})

    # 1. VS index configured
    vs_endpoint  = (db.get("vs_endpoint") or "").strip()
    index_name   = (db.get("index_name") or "").strip()
    catalog      = (db.get("catalog") or "workspace").strip()
    schema       = (db.get("schema") or "agentic_rag").strip()
    vs_ok = bool(vs_endpoint and index_name and catalog and schema)
    vs_detail = ""
    if vs_ok and index_status:
        state = index_status.get("state", "UNKNOWN")
        ready = index_status.get("ready", False)
        vs_ok = ready
        vs_detail = f"Index state: {state}"
    elif not vs_ok:
        vs_detail = "vs_endpoint or index_name is empty"
    items.append(_check("Databricks Vector Search index configured", vs_ok, vs_detail))

    # 2. CRAG max iterations ≤ 3
    max_iter = int(crag.get("max_iterations") or 0)
    items.append(_check(
        "CRAG loop max iterations ≤ 3",
        max_iter <= 3,
        f"max_iterations = {max_iter}" if max_iter > 3 else "",
    ))

    # 3. Web fallback trigger < relevance threshold
    wf = crag.get("web_fallback") or {}
    wf_enabled = bool(wf.get("enabled", True))
    rel_thresh  = float(crag.get("relevance_threshold") or 0.6)
    wf_trigger  = float(wf.get("trigger_score") or 0.4)
    if wf_enabled:
        wf_ok = wf_trigger < rel_thresh
        wf_detail = f"trigger={wf_trigger:.2f} vs threshold={rel_thresh:.2f}"
    else:
        wf_ok = True
        wf_detail = "Web fallback disabled"
    items.append(_check("Web fallback trigger score < relevance threshold", wf_ok, wf_detail))

    # 4. Citation injection enabled
    cit_on = bool((gen.get("citations") or {}).get("enabled", True))
    items.append(_check("Citation injection enabled", cit_on,
                         "" if cit_on else "Enable citations in Tab 4 → Advanced"))

    # 5. Faithfulness guardrail threshold ≥ 0.70
    faith_cfg   = gen.get("faithfulness") or {}
    faith_on    = bool(faith_cfg.get("enabled", True))
    faith_thresh = float(faith_cfg.get("threshold") or 0.0)
    faith_ok    = faith_on and faith_thresh >= 0.70
    faith_detail = f"threshold={faith_thresh:.2f}" if faith_on else "Faithfulness check disabled"
    items.append(_check("Faithfulness guardrail threshold ≥ 0.70", faith_ok, faith_detail))

    # 6. Answer relevancy target ≥ 0.75 (Ragas)
    ragas_cfg   = (obs.get("ragas") or {})
    ragas_on    = bool(ragas_cfg.get("enabled", True))
    min_rel     = float(ragas_cfg.get("min_answer_relevancy") or 0.0)
    rel_ok      = ragas_on and min_rel >= 0.75
    rel_detail  = f"min_answer_relevancy={min_rel:.2f}" if ragas_on else "Ragas evaluation disabled"
    items.append(_check("Answer relevancy target ≥ 0.75 (Ragas)", rel_ok, rel_detail))

    # 7. MLflow tracing enabled
    mlflow_on = bool((obs.get("mlflow") or {}).get("enabled", True))
    items.append(_check("MLflow tracing enabled", mlflow_on,
                         "" if mlflow_on else "Enable MLflow in Tab 5"))

    # 8. Re-ranker enabled
    reranker_on = bool((ret.get("reranker") or {}).get("enabled", True))
    items.append(_check("Cross-encoder re-ranker enabled", reranker_on,
                         "" if reranker_on else "Enable re-ranker in Tab 2 → Advanced"))

    # 9. LLM model configured
    llm_model = (gen.get("llm_model") or "").strip()
    items.append(_check("LLM model ID configured", bool(llm_model),
                         "" if llm_model else "Set LLM Model ID in Tab 4"))

    passed = sum(1 for i in items if i["passed"])
    all_ok = passed == len(items)

    return {
        "ok":     True,
        "passed": passed,
        "total":  len(items),
        "all_ok": all_ok,
        "items":  items,
    }


# ── Persist config ────────────────────────────────────────────────────────────

def save_config(config: dict[str, Any]) -> dict[str, Any]:
    """Save config JSON to disk. Returns { ok, path }."""
    try:
        _CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        _CONFIG_FILE.write_text(json.dumps(config, indent=2), encoding="utf-8")
        logger.info("RAG config saved to %s", _CONFIG_FILE)
        return {"ok": True, "path": str(_CONFIG_FILE)}
    except Exception as exc:
        logger.error("Failed to save config: %s", exc)
        return {"ok": False, "error": str(exc)}


def load_config() -> dict[str, Any]:
    """Load last-saved config from disk. Returns { ok, config } or { ok: False }."""
    try:
        if not _CONFIG_FILE.exists():
            return {"ok": False, "error": "No saved config found"}
        config = json.loads(_CONFIG_FILE.read_text(encoding="utf-8"))
        return {"ok": True, "config": config}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
