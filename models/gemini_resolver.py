# ╔══════════════════════════════════════════════════════════════════╗
# ║  VIKAA.AI — PUBLIC SHOWCASE                                      ║
# ║  This file requires environment credentials to run.              ║
# ║  Architecture and API signatures are shown for reference.        ║
# ║  See README.md and docs/ for setup guide.                        ║
# ╚══════════════════════════════════════════════════════════════════╝

"""
Minimal Gemini model resolver — no heavy imports (no moviepy, PIL, etc.).
Imported by web_research_router and LLM_GEMINI alike so the probe runs once
per process and the result is shared.
"""
import os
import threading
import logging

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage

logger = logging.getLogger(__name__)

_PREFERRED_MODELS = [
    "gemini-2.5-flash",
    "gemini-2.5-pro",
    "gemini-2.0-flash-001",
    "gemini-2.0-flash-lite-001",
    "gemini-2.0-flash-lite",
    "gemini-2.0-flash",
    "gemini-1.5-flash-002",
    "gemini-1.5-flash-001",
    "gemini-1.5-flash",
    "gemini-1.5-pro-002",
    "gemini-1.5-pro-001",
    "gemini-1.5-pro",
    "gemini-pro",
]

_resolved_model: str | None = None
_resolve_lock = threading.Lock()


def _resolve_gemini_model() -> str:
    """
    Return the best working Gemini model for this API key.

    Iterates _PREFERRED_MODELS and sends a minimal probe to each until one
    succeeds. Result is cached for the lifetime of the process.
    """
    ...
