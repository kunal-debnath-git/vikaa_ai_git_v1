"""
Databricks Model Serving — chat-style invocation for planner JSON.

Uses POST ``/serving-endpoints/{endpoint}/invocations`` (OpenAI-compatible messages body).
Endpoint name matches UI values such as ``databricks-mixtral-8x7b-instruct``.
"""

from __future__ import annotations

import json
import re
from typing import Any

import requests

from backend.integrations.databricks.read_unity_catalog import _normalize_host


def invoke_serving_chat(
    host: str,
    token: str,
    endpoint_name: str,
    *,
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 2048,
    temperature: float = 0.2,
    timeout_s: int = 120,
) -> str:
    """Returns assistant text (may contain JSON only or fenced JSON)."""
    base = _normalize_host(host).rstrip("/")
    name = (endpoint_name or "").strip()
    if not name:
        raise ValueError("endpoint_name is empty")
    url = f"{base}/serving-endpoints/{name}/invocations"
    headers = {
        "Authorization": f"Bearer {token.strip()}",
        "Content-Type": "application/json",
    }
    body: dict[str, Any] = {
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": max(256, min(max_tokens, 8192)),
        "temperature": temperature,
    }
    r = requests.post(url, headers=headers, json=body, timeout=timeout_s)
    if not r.ok:
        raise RuntimeError(f"serving invocations HTTP {r.status_code}: {r.text[:1200]}")
    data = r.json()
    # OpenAI-style
    choices = data.get("choices")
    if isinstance(choices, list) and choices:
        msg = (choices[0] or {}).get("message") or {}
        content = msg.get("content")
        if isinstance(content, str) and content.strip():
            return content.strip()
    # Databricks alternate: output / predictions
    out = data.get("output") or data.get("predictions")
    if isinstance(out, str) and out.strip():
        return out.strip()
    if isinstance(out, list) and out:
        first = out[0]
        if isinstance(first, dict):
            for k in ("content", "text", "generated_text"):
                v = first.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
        if isinstance(first, str):
            return first.strip()
    raise RuntimeError(f"Unrecognized serving response shape: keys={list(data.keys())[:12]}")


def extract_json_object(text: str) -> dict[str, Any]:
    """Parse first JSON object from model output (strip markdown fences)."""
    s = (text or "").strip()
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", s, re.IGNORECASE)
    if fence:
        s = fence.group(1).strip()
    start = s.find("{")
    end = s.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("No JSON object found in model output.")
    obj = json.loads(s[start : end + 1])
    if not isinstance(obj, dict):
        raise ValueError("Model JSON root must be an object.")
    return obj
