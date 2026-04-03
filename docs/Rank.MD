# COMING SOON tools — ease ranking (easy → hard)

Ranking is **overall** from a delivery perspective: **setup**, **configuration**, **API cost**, and **time to build & test**. Lower position = start here if you want velocity and predictable spend.

| Rank | Tool id | Doc |
|------|---------|-----|
| 1 (easiest) | `sql-agent` *(LIVE as Conversational SQL)* | [`tools/Databricks_Conversational_SQL.md`](tools/Databricks_Conversational_SQL.md) · stub: [`SQL_PySpark_Coding_Agent.md`](tools/SQL_PySpark_Coding_Agent.md) |
| 2 | `meeting-intelligence` | [`tools/Meeting_Intelligence_Agent.md`](tools/Meeting_Intelligence_Agent.md) |
| 3 | `architecture-advisor` | [`tools/Data_Architecture_Advisor.md`](tools/Data_Architecture_Advisor.md) |
| 4 | `pipeline-designer` | [`tools/Multi_Agent_Pipeline_Designer.md`](tools/Multi_Agent_Pipeline_Designer.md) |
| 5 | `databricks-agent` | [`tools/Databricks_Intelligence_Agent.md`](tools/Databricks_Intelligence_Agent.md) |
| 6 (hardest) | `orchestrator` | [`tools/Personal_AI_Orchestrator.md`](tools/Personal_AI_Orchestrator.md) |

---

## Why this order?

### 1. `sql-agent` — SQL & PySpark Coding Agent

- **Setup**: FastAPI + LLM keys; no new cloud product or MCP server (`ToolConfig.json`: *no dependencies*).
- **Configuration**: Mostly prompts, safety rules, and optional lint/sandbox flags — bounded surface.
- **API cost**: Dominated by **LLM tokens**; no warehouse or third-party search meter running in the background.
- **Build & test**: Deterministic checks (syntax lint, dry-run) are easier than multi-step agent evals.

### 2. `meeting-intelligence` — Meeting Intelligence Agent

- **Setup**: Depends on a **Google Calendar MCP server** — extra moving part, but same *family* as existing Gmail/Google OAuth patterns once MCP exists.
- **Configuration**: OAuth scopes, user consent, tool allowlist; not as open-ended as a full data platform.
- **API cost**: **Calendar API** is typically cheap vs LLM-heavy agents; LLM use is scoped to prep/summary per session.
- **Build & test**: Fewer failure modes than Databricks or multi-agent graphs if read-only calendar is v1.

### 3. `architecture-advisor` — Data Architecture Advisor

- **Setup**: RAG stack (embeddings + vector store + ingestion) — moderate infra, well-understood patterns.
- **Configuration**: **High ongoing** effort — chunking, metadata, and **curated KB quality** drive product value (human + pipeline work).
- **API cost**: **Embeddings** (index rebuilds) + **LLM** per query; predictable compared to accidental warehouse spend, but not negligible at scale.
- **Build & test**: Easy to ship a **thin MVP**; **hard** to test “good advice” without golden Q&A sets and content governance.

### 4. `pipeline-designer` — Multi-Agent Pipeline Designer

- **Setup**: Stays inside your repo — no mandatory external tenant (unlike Databricks).
- **Configuration**: Medium — pipeline DSL, agent roles, prompts; must stay aligned with **real** tools you actually expose.
- **API cost**: **High LLM spend** per user goal — multiple agent passes (design + critique + merge) add up quickly in dev and demos.
- **Build & test**: **Hardest software problem** in this list — non-determinism, loop limits, regression on “plan quality,” and edge cases when tools change.

### 5. `databricks-agent` — Databricks Intelligence Agent

- **Setup**: **Heavy** — workspace access, auth (PAT/OAuth), **Databricks MCP server**, networking, and least-privilege roles.
- **Configuration**: **Heavy** — jobs, SQL warehouse, Unity Catalog, environments; many knobs and org-specific policies.
- **API cost**: **Highest risk of surprise spend** — APIs are “cheap,” but **compute** (warehouses, jobs) triggered by mistakes or eager agents is not.
- **Build & test**: Integration grind + safety reviews; easier to *define* acceptance tests than for free-form multi-agent design, but environment cost slows iteration.

### 6. `orchestrator` — Personal AI Orchestrator

- **Setup**: Must **compose every other tool** — routers, auth, error shapes, and feature flags across the stack.
- **Configuration**: **Maximum** — routing policy, cost caps, tool registry, degradation behaviour, observability.
- **API cost**: **Sum of downstream tools** plus planner model; one user request can fan out to several billable calls.
- **Build & test**: **Largest surface** — end-to-end scenarios only work when subsystems are stable (`ToolConfig.json`: *build LAST*).

---

## Caveats

- **Calendar MCP**: If you must **author** the MCP server yourself (not consume an existing one), `meeting-intelligence` moves **down** toward `databricks-agent` on setup and calendar time.
- **Architecture advisor MVP vs production**: A small doc corpus gets you a demo fast; **production-grade** advisory moves this item **down** on configuration and test effort.
- **Pipeline vs Databricks** is the tightest trade-off: pipeline is **cheaper infra** but **messier evals**; Databricks is **expensive and ops-heavy** but **more objective** integration testing. Here, **API cost and enterprise setup** push Databricks to **5** and **multi-agent complexity** to **4**.

---

## Quick reference — relative scores (1 = easiest, 6 = hardest)

| Tool | Setup | Configuration | API cost | Build & test |
|------|-------|---------------|----------|--------------|
| `sql-agent` | 1 | 1 | 2 | 2 |
| `meeting-intelligence` | 3 | 2 | 1 | 3 |
| `architecture-advisor` | 2 | 5 | 3 | 4 |
| `pipeline-designer` | 2 | 3 | 5 | 6 |
| `databricks-agent` | 6 | 6 | 6 | 5 |
| `orchestrator` | 5 | 6 | 4 | 6 |

Scores are **ordinal** for comparison only, not additive weights.
