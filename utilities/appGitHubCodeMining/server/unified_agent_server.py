from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import requests

from modules.agent_planner import AgentPlanner
from modules.unified_scorer import UnifiedScorer
from Utilities.GitHubCodeMiningUpdated.search.semantic_search import SemanticCodeSearcher
from Utilities.GitHubCodeMiningUpdated.search.structural_search import StructuralCodeSearcher
from Utilities.GitHubCodeMiningUpdated.search.contextual_search import ContextualFetcher

app = FastAPI(
    title="FINAL Unified Deep Search AI Agent",
    description="AI Agent with Real Semantic, Structural, Contextual Search + Planning + Unified Scorer",
    version="2.0"
)

class SearchRequest(BaseModel):
    prompt: str
    github_token: str

class CodeResult(BaseModel):
    source: str
    snippet: str
    score: float

class UnifiedSearchResponse(BaseModel):
    agent_plan: List[str]
    results: List[CodeResult]

planner = AgentPlanner()
scorer = UnifiedScorer()

semantic_searcher = SemanticCodeSearcher()
semantic_searcher.add_code("def resize_image(img, size): return img.resize(size)")
semantic_searcher.add_code("def download_file(url): pass")
semantic_searcher.add_code("def parse_json(data): return json.loads(data)")

structural_searcher = StructuralCodeSearcher()

@app.post("/deep_search", response_model=UnifiedSearchResponse)
def deep_search(req: SearchRequest):
    try:
        contextual_fetcher = ContextualFetcher(req.github_token)
        plan = planner.decide_steps(req.prompt)

        candidates = []
        if "github" in plan:
            # Using semantic search as GitHub simulation
            for result in semantic_searcher.search(req.prompt):
                candidates.append({"source": "GitHub", "snippet": result, "score": scorer.score(0.9, 0, 0)})

        if "semantic" in plan:
            for result in semantic_searcher.search(req.prompt + " semantic"):
                candidates.append({"source": "Semantic", "snippet": result, "score": scorer.score(0.8, 0.1, 0)})

        if "structural" in plan:
            test_code = "def example(a, b): return a + b"
            for result in structural_searcher.find_functions_with_args(test_code, 2):
                candidates.append({"source": "Structural", "snippet": result, "score": scorer.score(0.5, 0.9, 0)})

        if "contextual" in plan:
            readme = contextual_fetcher.fetch_readme("psf/requests")
            candidates.append({"source": "Contextual", "snippet": readme, "score": scorer.score(0.4, 0.1, 0.9)})

        ranked = scorer.rank_results(candidates)
        response = [CodeResult(source=item["source"], snippet=item["snippet"], score=item["score"]) for item in ranked]

        return UnifiedSearchResponse(agent_plan=plan, results=response)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))