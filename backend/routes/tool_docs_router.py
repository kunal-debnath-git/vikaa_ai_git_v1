import os
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse

router = APIRouter(prefix="/tools/docs", tags=["Tool Docs"])

_DOCS_DIR = Path(__file__).resolve().parents[2] / "docs" / "tools"

# Explicit allowlist prevents path traversal and accidental file exposure.
_ALLOWED_DOCS = {
    "Gmail_Intelligence.md",
    "ReAct_Research_Agent.md",
    "AI_Scout.md",
    "Databricks_Intelligence_Agent.md",
    "Synthetic_Data_Generation_Databricks.md",
    "RAG_Configurator.md",
    "Twitter_Assistant.md",
    "Chat_with_Files.md",
}


@router.get("/{doc_name}", response_class=PlainTextResponse)
async def get_tool_doc(doc_name: str):
    name = os.path.basename(doc_name)
    if name not in _ALLOWED_DOCS:
        raise HTTPException(status_code=404, detail="Tool doc not found.")
    path = _DOCS_DIR / name
    if not path.exists():
        raise HTTPException(status_code=404, detail="Tool doc missing on server.")
    try:
        return path.read_text(encoding="utf-8")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read tool doc: {exc}") from exc

