# vikaa-ai-v1 (Vikaa.AI)

Unified GenAI / agentic platform: FastAPI backend, static frontend tool workspace, RAG pipelines, and integrations (Supabase auth, Databricks tools, MCP-oriented surfaces).

## Repository layout

| Area | Role |
|------|------|
| `backend/` | FastAPI app, routers (auth, agent, tools, RAG config, lead, etc.) |
| `frontend/` | Static HTML/JS/CSS: landing, agents, **Tool Workspace** (`ToolIndex.html`) |
| `agentic_rag/` | RAG store/retrieve pipelines, embeddings, controllers |
| `models/` | LLM helpers (e.g. Gemini / LangChain) |
| `datastore/` | DB scripts and clients (Supabase / Mongo) |
| `docs/` | Design notes, tool specs, ASK workflow |
| `main.py` | Application entry (uvicorn `main:app`) |

## Tool workspace (`frontend/`)

The **Tools** hub is [`frontend/ToolIndex.html`](frontend/ToolIndex.html). Tools are declared in [`frontend/ToolConfig.json`](frontend/ToolConfig.json):

- **`enabled`** — whether the tool appears in the sidebar  
- **`default`** — which tool is selected on load  
- **`group`** — sidebar column: `personalized` \| `enterprise` \| `experimental` (invalid or missing values default to `experimental`)

Changing grouping or visibility is done by editing `ToolConfig.json`; no code change required for those fields.

## Quick start (local)

1. Create/activate a Python venv (see [`INFORMATION.txt`](INFORMATION.txt) for Windows commands).  
2. Install dependencies (project root), then run the API:

   ```bash
   uvicorn main:app --host 0.0.0.0 --port 10000
   ```

3. Open frontend pages from disk or via a static server (e.g. Live Server). Example entry: `frontend/vikaa_ai_agent.html` or `frontend/ToolIndex.html`.

4. API docs: `http://localhost:10000/docs`

## Documentation

- **[`project.md`](project.md)** — project overview, modules, endpoints map, doc index  
- **`INFORMATION.txt`** — local run notes and tool-specific hints  
- **`docs/`** — deeper specs (RAG, tools, ASK)
