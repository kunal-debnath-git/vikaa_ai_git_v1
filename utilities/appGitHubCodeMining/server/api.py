"""vikaa.ai Deep GitHub Miner API

Endpoints:
- GET /api/repo_search: repository-level search via the shared GitHub-Search module.
- GET /api/deep_search: simple agentic deep code search (semantic/structural/contextual stubs).

Static UI is served from /web.
"""

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from typing import List, Optional
import os, importlib.util, time, datetime
from typing import Dict, Any
import sys

# Ensure project root is on sys.path for 'modules' imports when started via uvicorn
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.append(ROOT)

# Local modules for deep search (import from project root when running `uvicorn server.api:app`)
from modules.planner import Planner
from modules.semantic import Semantic
from modules.structural import Structural
from modules.contextual import Contextual
from modules.scoring import blend_score

# Import GitHub-Search functions for repo-level search (avoid code duplication)
# Resolve path to Utilities/GitHub-Search/search_github_agentic_ai.py from workspace
UTILS_SEARCH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'GitHub-Search', 'search_github_agentic_ai.py'))
spec = importlib.util.spec_from_file_location("search_github_agentic_ai", UTILS_SEARCH)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)  # type: ignore
search_github_agentic_ai = getattr(mod, 'search_github_agentic_ai')

app = FastAPI(title="vikaa.ai Deep GitHub Miner")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
WEB_DIR = os.path.join(os.path.dirname(THIS_DIR), 'web')
app.mount("/web", StaticFiles(directory=WEB_DIR, html=True), name="web")

class Repo(BaseModel):
    """Subset of repo fields expected by the UI cards."""
    name: str
    full_name: Optional[str] = None
    url: str
    stars: int
    description: Optional[str] = None
    language: Optional[str] = None
    updated_at: Optional[str] = None
    relevance: Optional[float] = None
    score: Optional[float] = None

class SearchResponse(BaseModel):
    results: List[Repo]

class CodeItem(BaseModel):
    """Deep-search snippet with optional metadata (repo/file/etc)."""
    source: str
    snippet: str
    score: float
    meta: Dict[str, Any] | None = None

class DeepSearchResponse(BaseModel):
    agent_plan: List[str]
    results: List[CodeItem]

@app.get("/")
async def root():
    return RedirectResponse(url="/web/")

@app.get("/api/health")
async def health():
    return {"status": "ok"}

@app.get("/api/repo_search", response_model=SearchResponse)
async def repo_search(
    prompt: str = Query("agentic ai"),
    language: str = Query("python"),
    months: int = Query(3, ge=1, le=36),
    max: int = Query(50, ge=1, le=200),
    min_stars: int = Query(0, ge=0),
    llm_expand: bool = Query(False),
    llm_provider: str = Query("gpt"),
    llm_model: str = Query("gpt-4o-mini"),
    llm_temperature: float = Query(0.2, ge=0, le=1),
    llm_rerank_top: int = Query(0, ge=0),
    llm_filter: bool = Query(False),
    llm_filter_top: int = Query(0, ge=0),
):
    """Repository-level search across GitHub using robust logic from GitHub-Search.

    Supports LLM-powered expansion, reranking, and filtering via query params.
    """
    repos = search_github_agentic_ai(
        since_months=months, max_results=max, base_query=prompt, language=language,
        min_stars=min_stars, llm_expand=llm_expand, llm_provider=llm_provider,
        llm_model=llm_model, llm_temperature=llm_temperature, llm_rerank_top=llm_rerank_top,
        llm_filter=llm_filter, llm_filter_top=llm_filter_top,
    )
    payload = [{
        "name": r.get("name"), "full_name": r.get("full_name"), "url": r.get("url"),
        "stars": r.get("stars", 0), "description": r.get("description"),
        "language": r.get("language"), "updated_at": r.get("updated_at"),
        "relevance": r.get("relevance"), "score": r.get("score"),
    } for r in repos]
    return {"results": payload}


# --- Deep Search (semantic + structural + contextual) ---
planner = Planner()
semantic = None  # lazy init to avoid heavy startup
structural = Structural()

@app.get("/api/deep_search", response_model=DeepSearchResponse)
async def deep_search(
    prompt: str = Query(..., description="What to search in code"),
    github_token: str | None = Query(None, description="Optional GitHub token for higher rate limits"),
    top_k: int = Query(5, ge=1, le=20),
):
    """Prototype deep code search.

    Plan steps with a minimal planner; search demo semantic index; run a tiny
    structural heuristic; optionally pull a README. Replace stubs with live
    repo+file hydration in next iterations.
    """
    try:
        plan = planner.decide(prompt)
        results: List[Dict[str, Any]] = []

        # Seed semantic index with a tiny demo corpus (replace with repo/file fetch in future)
        global semantic
        if semantic is None:
            semantic = Semantic()
        if not getattr(semantic, "_seeded", False):
            samples = [
                ("def vector_search(query, index):\n    return index.similarity(query)", {"repo": "demo/semantic", "path": "search.py"}),
                ("def parse_ast(code):\n    import ast\n    return ast.parse(code)", {"repo": "demo/struct", "path": "ast_utils.py"}),
                ("class RAG:\n    def retrieve(self, q): ...\n    def generate(self, ctx): ...", {"repo": "demo/rag", "path": "rag.py"}),
            ]
            for text, meta in samples:
                semantic.add(text, meta)
            semantic._seeded = True  # type: ignore

        # Semantic — search the in-memory demo index
        if "semantic" in plan:
            for hit in semantic.search_with_scores(prompt, top_k=top_k):
                item = hit["item"]
                score = float(hit["score"])
                meta = {k: v for k, v in item.items() if k != "text"}
                # Construct helpful links when repo/path metadata exists
                repo = meta.get("repo")
                path = meta.get("path")
                if repo:
                    meta["repo_url"] = f"https://github.com/{repo}"
                    if path:
                        # Best-effort blob URL guess (branch unknown)
                        meta["blob_url_guess"] = f"https://github.com/{repo}/blob/main/{path}"
                        meta["url"] = meta["blob_url_guess"]
                    else:
                        meta["url"] = meta["repo_url"]
                results.append({
                    "source": "semantic",
                    "snippet": item.get("text", ""),
                    "score": score,
                    "meta": meta,
                })

        # Structural — simple heuristic: find funcs with 2 args in a demo file
        if "structural" in plan:
            code = """\
def add(a, b):
    return a + b

def single(a):
    return a
"""
            names = structural.find_functions_with_args(code, 2)
            for n in names:
                results.append({
                    "source": "structural",
                    "snippet": f"Function with 2 args: {n}",
                    "score": 0.5,
                    "meta": {"file": "demo.py"},
                })

        # Contextual — optionally pull README of a well-known repo as a stub
        if "contextual" in plan:
            ctx = Contextual(github_token)
            readme = ctx.readme("psf/requests") or ""
            if readme:
                results.append({
                    "source": "contextual",
                    "snippet": readme[:1200],
                    "score": 0.4,
                    "meta": {"repo": "psf/requests", "type": "readme", "url": "https://github.com/psf/requests"},
                })

        # Blend/normalize scores lightly (placeholder for unified blend)
        final = []
        for r in results:
            final.append(CodeItem(
                source=r["source"],
                snippet=r["snippet"],
                score=round(float(r.get("score", 0.0)), 4),
                meta=r.get("meta"),
            ))

        # Sort by score desc
        final.sort(key=lambda x: x.score, reverse=True)
        return {"agent_plan": plan, "results": final}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
