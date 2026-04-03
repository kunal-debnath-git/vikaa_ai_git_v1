"""
GitHub Code Mining — agentic retrieval over GitHub (ReAct + tools).

Purpose: Given a natural-language goal, the agent chooses which GitHub API
operations to run (repo search, code search, README, commits, file contents),
retrieves evidence, then synthesises an answer — Agentic RAG (retrieve via tools,
generate with Gemini), aligned with the main FastAPI gateway + LangChain stack.

Outcome: Actionable answers with repo/file references, not generic LLM guesses.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import sys
from typing import Any, Sequence

import requests
from fastapi import APIRouter, Depends, HTTPException
from langchain.agents import AgentExecutor, create_react_agent
from langchain import hub
from langchain_core.messages import HumanMessage
from langchain_core.tools import Tool
from langchain_google_genai import ChatGoogleGenerativeAI
from pydantic import BaseModel, Field
from backend.services.access_guard import require_whitelisted_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tools", tags=["GitHub Code Mining"])
# Aliases so older gateways, proxies, or mistaken paths still hit the same handler (reduces “404” confusion).
router_api_alias = APIRouter(prefix="/api/tools", tags=["GitHub Code Mining"])
router_root = APIRouter(tags=["GitHub Code Mining"])

_GH_MINING_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "utilities", "appGitHubCodeMining")
)
if _GH_MINING_ROOT not in sys.path:
    sys.path.insert(0, _GH_MINING_ROOT)

from modules.planner import Planner  # noqa: E402

_LLM: ChatGoogleGenerativeAI | None = None
_REACT_PROMPT: Any = None


def _resolve_token(request_token: str | None) -> str | None:
    return (request_token or "").strip() or os.getenv("GITHUB_TOKEN") or os.getenv("GITHUB_API_KEY")


def _normalize_react_tool_input(raw: str) -> str:
    """Strip common LLM wrappers so ReAct 'Action Input' still works (JSON blobs, key: prefixes)."""
    q = (raw or "").strip()
    if not q:
        return q
    if q.startswith("{") and "}" in q:
        try:
            d = json.loads(q)
            if isinstance(d, dict):
                if len(d) == 1:
                    return str(next(iter(d.values()))).strip()
                for k in ("search_query", "code_query", "repo_full_name", "path_spec", "user_goal_snippet", "spec", "query"):
                    if k in d and d[k] is not None:
                        return str(d[k]).strip()
        except (json.JSONDecodeError, TypeError):
            pass
    lower = q.lower()
    for prefix in (
        "search_query:", "search_query=", "code_query:", "input:", "query:", "action input:",
    ):
        if lower.startswith(prefix):
            return q[len(prefix) :].strip().strip("'\"")
    return q


def _github_headers(token: str | None, *, raw_readme: bool = False) -> dict[str, str]:
    if raw_readme:
        h = {
            "Accept": "application/vnd.github.v3.raw",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    else:
        h = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def _github_get(
    url: str,
    *,
    token: str | None,
    params: dict | None = None,
    raw_readme: bool = False,
) -> requests.Response:
    """GET with optional token; on 401, retry once without auth (invalid .env PAT should not block public API)."""
    r = requests.get(
        url,
        headers=_github_headers(token, raw_readme=raw_readme),
        params=params or {},
        timeout=30,
    )
    if r.status_code == 401 and token:
        logger.warning(
            "GitHub returned 401 with a configured token; retrying unauthenticated for public data."
        )
        r = requests.get(
            url,
            headers=_github_headers(None, raw_readme=raw_readme),
            params=params or {},
            timeout=30,
        )
    return r


def _get_llm() -> ChatGoogleGenerativeAI:
    global _LLM
    if _LLM is None:
        from models.LLM_GEMINI import _resolve_gemini_model

        key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
        _LLM = ChatGoogleGenerativeAI(
            model=_resolve_gemini_model(),
            google_api_key=key,
            temperature=0.15,
        )
        logger.info("GitHub mining agent using Gemini model: %s", _resolve_gemini_model())
    return _LLM


def _query_tokens(q: str) -> set[str]:
    return {t for t in re.findall(r"[a-z0-9]{2,}", (q or "").lower()) if len(t) > 1}


def _rerank_repo_items(items: list[dict], query: str) -> list[dict]:
    """Blend GitHub's star sort with lexical overlap on name, description, topics."""
    if not items:
        return items
    toks = _query_tokens(query)
    if not toks:
        return items

    def key(it: dict) -> tuple:
        blob = " ".join(
            [
                it.get("full_name", "") or "",
                it.get("description", "") or "",
                " ".join(it.get("topics") or []),
            ]
        ).lower()
        overlap = sum(1 for t in toks if t in blob)
        stars = it.get("stargazers_count") or 0
        return (overlap, stars)

    return sorted(items, key=key, reverse=True)


def _rerank_code_items(items: list[dict], query: str) -> list[dict]:
    """Boost API relevance score when path/repo overlap the user's language."""
    if not items:
        return items
    toks = _query_tokens(query)

    def key(it: dict) -> float:
        repo = (it.get("repository") or {}).get("full_name", "") or ""
        path = it.get("path", "") or ""
        blob = f"{repo}/{path}".lower()
        bonus = sum(2 for t in toks if t in blob)
        return bonus + float(it.get("score") or 0.0)

    return sorted(items, key=key, reverse=True)


def _search_repositories(q: str, token: str | None) -> str:
    url = "https://api.github.com/search/repositories"
    r = _github_get(
        url,
        token=token,
        params={"q": q, "per_page": "12", "sort": "stars", "order": "desc"},
    )
    if r.status_code != 200:
        return f"GitHub API error {r.status_code}: {r.text[:500]}"
    data = r.json()
    items = data.get("items") or []
    if not items:
        return "No repositories found for this query."
    items = _rerank_repo_items(items, q)[:7]
    lines: list[str] = []
    for rank, it in enumerate(items, start=1):
        fn = it.get("full_name", "")
        stars = it.get("stargazers_count", 0)
        desc = (it.get("description") or "").replace("\n", " ")[:220]
        lang = it.get("language") or "—"
        gh_url = it.get("html_url") or ""
        upd = (it.get("updated_at") or "")[:10]
        topics = ", ".join((it.get("topics") or [])[:5])
        topics_s = f"\n  topics: {topics}" if topics else ""
        fork = " (fork)" if it.get("fork") else ""
        url_line = f"\n  url: {gh_url}" if gh_url else ""
        lines.append(
            f"{rank}. {fn}{fork} — {stars}★ [{lang}] updated {upd}{url_line}\n"
            f"  {desc}{topics_s}"
        )
    lines.append(
        "\n(Ranking: query overlap on name/description/topics, tie-broken by stars — "
        "still validate with README/code before betting a design on one repo.)"
    )
    return "\n".join(lines)


def _search_code(q: str, token: str | None) -> str:
    if not token:
        return (
            "GitHub code search usually requires authentication. "
            "Add GITHUB_TOKEN to the server .env or pass a token from the UI."
        )
    url = "https://api.github.com/search/code"
    r = _github_get(
        url,
        token=token,
        params={"q": q, "per_page": "12"},
    )
    if r.status_code != 200:
        return f"GitHub code search error {r.status_code}: {r.text[:500]}"
    data = r.json()
    items = data.get("items") or []
    if not items:
        return "No code hits. Try broadening the query or specifying repo:owner/name."
    items = _rerank_code_items(items, q)[:8]
    lines: list[str] = []
    for rank, it in enumerate(items, start=1):
        repo = (it.get("repository") or {}).get("full_name", "?")
        path = it.get("path", "")
        score = it.get("score", 0)
        hit_url = it.get("html_url") or ""
        url_line = f"\n  url: {hit_url}" if hit_url else ""
        lines.append(f"{rank}. {repo} — {path} (api_score {score}){url_line}")
    lines.append(
        "\n(Ranking: GitHub score + extra weight when path/repo matches query tokens.)"
    )
    return "\n".join(lines)


def _read_file_text(owner: str, repo: str, path: str, ref: str, token: str | None) -> str:
    path = path.lstrip("/")
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    r = _github_get(
        url,
        token=token,
        params={"ref": ref} if ref else {},
    )
    if r.status_code != 200:
        return f"Could not read file {path}: HTTP {r.status_code} — {r.text[:400]}"
    data = r.json()
    if isinstance(data, list):
        names = ", ".join(str(x.get("name", "")) for x in data[:25])
        more = " …" if len(data) > 25 else ""
        return f"Path is a directory. Entries: {names}{more}"
    if data.get("type") != "file":
        return f"Unsupported content type: {data.get('type')}"
    enc = data.get("encoding")
    if enc != "base64":
        return f"Unexpected encoding: {enc}"
    raw = base64.b64decode(data.get("content", "")).decode("utf-8", errors="replace")
    if len(raw) > 14000:
        return raw[:14000] + "\n\n[truncated — file too large for one observation]"
    return raw


def _heuristic_plan(user_text: str) -> str:
    p = Planner()
    steps = p.decide(user_text)
    return ", ".join(steps)


def _build_tools(token: str | None) -> list[Tool]:
    """String-only Tool API — best fit for text ReAct + Gemini."""

    def search_repositories(arg: str) -> str:
        q = _normalize_react_tool_input(arg)
        return _search_repositories(q, token) if q else "Empty query."

    def fetch_readme(arg: str) -> str:
        repo_full_name = _normalize_react_tool_input(arg).replace("https://github.com/", "").strip("/")
        if not repo_full_name:
            return "Provide owner/repo"
        url = f"https://api.github.com/repos/{repo_full_name}/readme"
        r = _github_get(url, token=token, raw_readme=True)
        if r.status_code != 200:
            return f"No README or not accessible for {repo_full_name} (HTTP {r.status_code})."
        text = r.text or ""
        if len(text) > 12000:
            return text[:12000] + "\n[README truncated]"
        return text

    def list_recent_commits(arg: str) -> str:
        spec = _normalize_react_tool_input(arg)
        lim = 10
        repo_full_name = spec
        if "|" in spec:
            repo_full_name, lim_s = spec.rsplit("|", 1)
            repo_full_name = repo_full_name.strip().replace("https://github.com/", "").strip("/")
            try:
                lim = max(1, min(30, int(lim_s.strip())))
            except ValueError:
                repo_full_name = spec.replace("https://github.com/", "").strip("/")
        else:
            repo_full_name = repo_full_name.replace("https://github.com/", "").strip("/")
        if not repo_full_name:
            return "Provide owner/repo or owner/repo|15"
        url = f"https://api.github.com/repos/{repo_full_name}/commits"
        r = _github_get(url, token=token, params={"per_page": str(lim)})
        if r.status_code != 200:
            return f"No commits for {repo_full_name} (HTTP {r.status_code}); private repos need a valid token."
        data = r.json()
        if not isinstance(data, list):
            return f"Unexpected response for commits on {repo_full_name}."
        msgs = [c.get("commit", {}).get("message") for c in data]
        if not any(msgs):
            return f"No commits returned for {repo_full_name}."
        return "\n".join(f"- {m}" for m in msgs if m)

    def read_repository_file(spec: str) -> str:
        spec = _normalize_react_tool_input(spec)
        if "|" not in spec:
            return "Invalid: owner/repo|path/to/file.py OR owner/repo|path/to/file|branch"
        i = spec.index("|")
        left = spec[:i].strip().replace("https://github.com/", "").strip("/")
        right = spec[i + 1 :].strip()
        if "|" in right:
            path_part, ref = right.rsplit("|", 1)
            path = path_part.strip().lstrip("/")
            ref = ref.strip() or "main"
        else:
            path, ref = right.strip().lstrip("/"), "main"
        if "/" not in left:
            return "Invalid repo; use owner/repo|path"
        owner, repo = left.split("/", 1)
        return _read_file_text(owner, repo, path, ref, token)

    def search_code(arg: str) -> str:
        return _search_code(_normalize_react_tool_input(arg), token)

    def suggest_mining_strategy(arg: str) -> str:
        steps = _heuristic_plan(_normalize_react_tool_input(arg))
        return (
            f"Heuristic plan (use tools as needed): {steps}. "
            "Prefer repo search to discover candidates, then README/commits/file reads to verify."
        )

    return [
        Tool(
            name="github_search_repositories",
            func=search_repositories,
            description=(
                "Search GitHub repositories. Input: ONE string using GitHub query syntax "
                "(e.g. fastapi in:name, language:python stars:>1000). "
                "Results are re-ranked for query overlap (topics/description) after retrieval."
            ),
        ),
        Tool(
            name="github_search_code",
            func=search_code,
            description=(
                "Search code (needs PAT). Input: ONE string "
                "(e.g. create_react_agent language:python repo:langchain-ai/langchain). "
                "Hits include direct html URLs; results are re-ranked with query token overlap."
            ),
        ),
        Tool(
            name="github_fetch_readme",
            func=fetch_readme,
            description="Fetch README. Input: ONE string owner/repo e.g. tiangolo/fastapi",
        ),
        Tool(
            name="github_list_recent_commits",
            func=list_recent_commits,
            description="Recent commits. Input: owner/repo OR owner/repo|20",
        ),
        Tool(
            name="github_read_file",
            func=read_repository_file,
            description="Read file. Input: owner/repo|path/to/file.py OR owner/repo|path|branch",
        ),
        Tool(
            name="github_mining_strategy_hint",
            func=suggest_mining_strategy,
            description="Heuristic mining plan for a goal; input is a short goal snippet.",
        ),
    ]




def _get_react_prompt() -> Any:
    global _REACT_PROMPT
    if _REACT_PROMPT is None:
        try:
            _REACT_PROMPT = hub.pull("hwchase17/react")
        except Exception as exc:  # pragma: no cover
            logger.warning("hub.pull(hwchase17/react) failed: %s", exc)
            raise HTTPException(
                status_code=503,
                detail="ReAct prompt unavailable (offline?). Try again later.",
            ) from exc
    return _REACT_PROMPT


def _make_executor(tools: Sequence[Tool]) -> AgentExecutor:
    agent = create_react_agent(_get_llm(), list(tools), _get_react_prompt())
    return AgentExecutor(
        agent=agent,
        tools=list(tools),
        verbose=False,
        max_iterations=12,
        max_execution_time=150,
        handle_parsing_errors=(
            "Format error — reply with exactly one Action/Action Input pair, then wait for Observation."
        ),
        return_intermediate_steps=True,
    )


def _lc_message_text(resp: Any) -> str:
    """Normalize LangChain AIMessage.content (str or multimodal list) to plain text."""
    c = getattr(resp, "content", None)
    if c is None:
        return ""
    if isinstance(c, str):
        return c.strip()
    if isinstance(c, list):
        parts: list[str] = []
        for block in c:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                t = block.get("text")
                if t:
                    parts.append(str(t))
        return "".join(parts).strip()
    return str(c).strip()


async def _synthesize_github_report(
    goal: str,
    focus: str | None,
    raw_agent_answer: str,
    intermediate_steps: list | None,
) -> str | None:
    """
    Second-pass LLM: turn messy ReAct output + observations into structured Markdown.
    Grounding: must not invent URLs/repos not present in inputs.
    """
    if not (os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")):
        return None
    brief_lines: list[str] = []
    if intermediate_steps:
        for i, (action, observation) in enumerate(intermediate_steps[:14]):
            tool = getattr(action, "tool", "?")
            obs = str(observation)
            if len(obs) > 1000:
                obs = obs[:1000] + "…"
            brief_lines.append(f"[{i + 1}] tool={tool}\n{obs}")
    trace_blob = "\n\n".join(brief_lines)
    prompt = (
        "You are a strict technical editor. You will receive (1) a draft answer from a GitHub research agent "
        "and (2) truncated tool observations. The observations are the primary ground truth for links and paths.\n\n"
        f"User goal:\n{goal.strip()}\n\n"
        f"Focus repository (optional): {focus or 'none'}\n\n"
        "Draft agent answer:\n---\n"
        f"{raw_agent_answer[:10000]}\n---\n\n"
        "Tool observations (truncated):\n---\n"
        f"{trace_blob[:16000]}\n---\n\n"
        "Write a polished Markdown report for a senior engineer. Rules:\n"
        "- Use ONLY facts supported by the draft or observations. Do not invent repositories, paths, or URLs.\n"
        "- Copy URLs verbatim from observations when you include them.\n"
        "- If evidence is weak, say exactly what to search, which repo to clone, or which file path to open.\n"
        "- No hype, no emoji spam. Professional, concise.\n\n"
        "Use this structure:\n"
        "## TL;DR\n"
        "- 2–4 bullets\n\n"
        "## Ranked findings\n"
        "Numbered list: each item names owner/repo or file path, why it matters, and link(s) when available from observations.\n\n"
        "## Evidence pointers\n"
        "Bullets: the strongest URLs / owner/repo paths to open (from observations only).\n\n"
        "## Next steps\n"
        "Concrete verification or comparison steps.\n"
    )
    try:
        llm = _get_llm()
        resp = await llm.ainvoke([HumanMessage(content=prompt)])
        text = _lc_message_text(resp)
        return text or None
    except Exception as exc:
        logger.warning("GitHub mining synthesis failed: %s", exc)
        return None


class GitHubMiningRequest(BaseModel):
    query: str = Field(..., min_length=3, description="Goal or question about code / repos on GitHub.")
    focus_repo: str | None = Field(
        None,
        description="Optional owner/repo to prioritise, e.g. langchain-ai/langchain",
    )
    github_token: str | None = Field(None, description="Optional PAT; falls back to GITHUB_TOKEN env.")
    include_tool_trace: bool = Field(False, description="Return intermediate ReAct steps for the UI.")
    synthesize_report: bool = Field(
        True,
        description="If true, run a second LLM pass to restructure the answer as grounded Markdown.",
    )


class ToolStepOut(BaseModel):
    tool: str | None = None
    input: Any = None
    observation_preview: str | None = None


class GitHubMiningResponse(BaseModel):
    answer: str = Field(..., description="Final answer; Markdown when synthesis succeeded.")
    answer_raw: str | None = Field(
        None,
        description="Original ReAct agent text when synthesis replaced the user-facing answer.",
    )
    focus_repo: str | None = None
    heuristic_plan: str | None = None
    tool_trace: list[ToolStepOut] | None = None
    synthesized: bool = Field(False, description="True when the answer came from the second-pass editor LLM.")


def _serialize_steps(steps: list) -> list[ToolStepOut]:
    out: list[ToolStepOut] = []
    for action, observation in steps:
        obs_s = str(observation)
        if len(obs_s) > 1800:
            obs_s = obs_s[:1800] + "…"
        tool = getattr(action, "tool", None)
        inp = getattr(action, "tool_input", None)
        out.append(ToolStepOut(tool=str(tool) if tool else None, input=inp, observation_preview=obs_s))
    return out


def _health_payload() -> dict[str, Any]:
    tok = _resolve_token(None)
    return {
        "status": "ok",
        "service": "github-code-mining",
        "post_urls": [
            "/tools/github-code-mining",
            "/api/tools/github-code-mining",
            "/github-code-mining",
        ],
        "github_token_configured": bool(tok),
        "gemini_configured": bool(os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")),
        "mining_root_present": os.path.isdir(_GH_MINING_ROOT),
    }


async def _execute_github_mining(body: GitHubMiningRequest) -> GitHubMiningResponse:
    token = _resolve_token(body.github_token)
    tools = _build_tools(token)
    executor = _make_executor(tools)

    focus = (body.focus_repo or "").strip() or None
    if focus:
        focus = focus.replace("https://github.com/", "").strip("/")

    plan_hint = _heuristic_plan(body.query)
    augmented = (
        "You are Vikaa's GitHub Code Mining agent (Agentic RAG). Use tools to retrieve real GitHub data; "
        "do not invent file paths or URLs. Prefer fewer, higher-signal repos/files over long undifferentiated lists.\n"
        "When repo/code search returns ranked results, explain why the top 1–3 matter for the user's goal.\n"
        "IMPORTANT: Every Action Input must be a single plain string only — no JSON objects, no key: prefixes.\n"
        f"Heuristic mining modes suggested for this goal: {plan_hint}.\n"
        f"Focus repository (prioritise if relevant): {focus or 'none — discover via search if needed'}.\n\n"
        f"User goal:\n{body.query.strip()}\n"
    )

    try:
        result = await executor.ainvoke({"input": augmented})
    except Exception as exc:
        logger.exception("GitHub mining agent failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    output = (result.get("output") or "").strip()
    if not output:
        output = "No final answer produced. Try a more specific repository or enable a GitHub token for code search."

    trace = None
    steps = result.get("intermediate_steps")
    if body.include_tool_trace and steps:
        trace = _serialize_steps(steps)

    answer_raw: str | None = None
    synthesized = False
    final_answer = output
    if body.synthesize_report:
        synth = await _synthesize_github_report(
            body.query.strip(),
            focus,
            output,
            steps if isinstance(steps, list) else None,
        )
        if synth:
            answer_raw = output
            final_answer = synth
            synthesized = True

    return GitHubMiningResponse(
        answer=final_answer,
        answer_raw=answer_raw,
        focus_repo=focus,
        heuristic_plan=plan_hint,
        tool_trace=trace,
        synthesized=synthesized,
    )


@router.post("/github-code-mining", response_model=GitHubMiningResponse)
async def github_code_mining_run(
    body: GitHubMiningRequest, _acl=Depends(require_whitelisted_user)
) -> GitHubMiningResponse:
    """Run the GitHub mining ReAct agent (primary path: /tools/github-code-mining)."""
    return await _execute_github_mining(body)


@router.get("/github-code-mining/health")
async def github_mining_health():
    return _health_payload()


@router_api_alias.post("/github-code-mining", response_model=GitHubMiningResponse)
async def github_code_mining_api_alias(
    body: GitHubMiningRequest, _acl=Depends(require_whitelisted_user)
) -> GitHubMiningResponse:
    """Alias: /api/tools/github-code-mining (some gateways expect /api prefix)."""
    return await _execute_github_mining(body)


@router_api_alias.get("/github-code-mining/health")
async def github_mining_health_api_alias():
    return _health_payload()


@router_root.post("/github-code-mining", response_model=GitHubMiningResponse)
async def github_code_mining_root(
    body: GitHubMiningRequest, _acl=Depends(require_whitelisted_user)
) -> GitHubMiningResponse:
    """Alias: /github-code-mining (short path)."""
    return await _execute_github_mining(body)


@router_root.get("/github-code-mining/health")
async def github_mining_health_root():
    return _health_payload()
