"""
Web search integrations for CRAG web fallback.

Providers:
  tavily      — TavilyClient (requires TAVILY_API_KEY)
  serpapi     — SerpAPI REST  (requires SERPAPI_KEY)
  duckduckgo  — duckduckgo-search, no key required

Each provider returns a normalised list:
  [{ "title": str, "url": str, "content": str, "score": float|None }, ...]
"""

from __future__ import annotations

import logging
import os
from typing import Any

import requests

logger = logging.getLogger(__name__)


# ── Tavily ────────────────────────────────────────────────────────────────────

def _tavily_search(query: str, max_results: int) -> list[dict[str, Any]]:
    from tavily import TavilyClient
    key = os.getenv("TAVILY_API_KEY", "").strip()
    if not key:
        raise RuntimeError("TAVILY_API_KEY not set")
    client = TavilyClient(api_key=key)
    resp = client.search(query, max_results=max_results, include_raw_content=False)
    out = []
    for r in resp.get("results", []):
        out.append({
            "title":   r.get("title", ""),
            "url":     r.get("url", ""),
            "content": r.get("content", ""),
            "score":   r.get("score"),
        })
    return out


# ── SerpAPI ───────────────────────────────────────────────────────────────────

def _serpapi_search(query: str, max_results: int) -> list[dict[str, Any]]:
    key = os.getenv("SERPAPI_KEY", "").strip()
    if not key:
        raise RuntimeError("SERPAPI_KEY not set")
    params = {
        "q": query,
        "api_key": key,
        "num": max_results,
        "engine": "google",
    }
    r = requests.get("https://serpapi.com/search", params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    out = []
    for item in data.get("organic_results", [])[:max_results]:
        out.append({
            "title":   item.get("title", ""),
            "url":     item.get("link", ""),
            "content": item.get("snippet", ""),
            "score":   None,
        })
    return out


# ── DuckDuckGo ────────────────────────────────────────────────────────────────

def _ddg_search(query: str, max_results: int) -> list[dict[str, Any]]:
    try:
        from duckduckgo_search import DDGS
    except ImportError:
        raise RuntimeError("duckduckgo-search package not installed. Run: pip install duckduckgo-search")
    out = []
    with DDGS() as ddgs:
        for r in ddgs.text(query, max_results=max_results):
            out.append({
                "title":   r.get("title", ""),
                "url":     r.get("href", ""),
                "content": r.get("body", ""),
                "score":   None,
            })
    return out


# ── Public entry point ────────────────────────────────────────────────────────

def web_search(
    query: str,
    provider: str = "tavily",
    max_results: int = 5,
) -> list[dict[str, Any]]:
    """
    Run a web search and return normalised results.
    provider: "tavily" | "serpapi" | "duckduckgo"
    """
    provider = (provider or "tavily").lower()
    try:
        if provider == "tavily":
            results = _tavily_search(query, max_results)
        elif provider == "serpapi":
            results = _serpapi_search(query, max_results)
        elif provider == "duckduckgo":
            results = _ddg_search(query, max_results)
        else:
            raise ValueError(f"Unknown provider: {provider}")
        logger.info("Web search (%s) returned %d results", provider, len(results))
        return results
    except Exception as exc:
        logger.warning("Web search failed (%s): %s", provider, exc)
        raise
