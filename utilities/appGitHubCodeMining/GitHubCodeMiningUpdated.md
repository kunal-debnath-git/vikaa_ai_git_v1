# GitHubCodeMiningUpdated: What, How, and What's Next

## 1) What we built

A unified utility that combines:

- Repository search (UI + API) via the existing GitHub-Search engine.
- Deep code search stubs (semantic, structural, contextual) with an agent planner.
- A modern web UI with a mode toggle (Repository vs Deep Code) served by FastAPI.

Endpoints:

- GET /api/repo_search — repo-level search with LLM options.
- GET /api/deep_search — prototype deep code search returning snippet cards.
- GET /web — static web UI.

## 2) How it works (high level)

- Server: `server/api.py` mounts the web app and exposes two endpoints.
  - Repo search delegates to `Utilities/GitHub-Search/search_github_agentic_ai.py` to avoid duplication.
  - Deep search wires a minimal Planner + Semantic/Structural/Contextual modules.
- UI: `web/index.html` + `web/app.js` + `web/styles.css`
  - Adds a Mode selector and optional GitHub token field.
  - Calls `/api/repo_search` for Repository mode; `/api/deep_search` for Deep mode.
- Semantic: `modules/semantic.py` wraps SentenceTransformers with an in-memory index.
- Structural: `modules/structural.py` provides a tiny AST-based search.
- Contextual: `modules/contextual.py` fetches README/commits via GitHub API.

## 3) Scope for further enhancement

- Hydrate deep search from real code:
  - Use repo_search results to fetch repo files (by path, language filters, size caps).
  - Embed snippets (sliding window) into the semantic index; include file path + line ranges.
- Scoring and ranking:
  - Blend LLM relevance + stars + recency + semantic similarity (use `modules/scoring.py`).
  - Optional LLM rerank for deep results using selected provider/model.
- Performance and robustness:
  - ETag caching for README/files, 429 backoff, configurable concurrency.
  - Persist embedding index to disk; rehydrate on server start.
- UX polish:
  - Show source (repo/file/line), quick copy, and expand/collapse for long snippets.
  - Filters: language, path globs, max file size, code vs docs.
- Testing and docs:
  - Add unit/integration tests; examples and screenshots in README.

## 4) Try it

- Install deps for the updated utility:
  - See `Utilities/GitHubCodeMiningUpdated/requirements.txt`.
- Start the server:
  - Run `uvicorn Utilities.GitHubCodeMiningUpdated.server.api:app --reload` from the repo root, then open `/web`.
- Use the Mode selector to switch between Repository and Deep Code.
- Provide a GitHub token (optional) for higher rate limits in Deep mode.
