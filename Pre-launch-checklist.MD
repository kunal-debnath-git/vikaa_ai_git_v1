# Vikaa.AI — Pre-Launch Checklist (MVP)

**Goal:** Get vikaa.ai production-ready for public audience.
**Stack:** FastAPI + Supabase + Gemini + Anthropic | Hosted on Render.com
**Dev tools:** Claude Code + Cursor.AI
**Last updated:** 2026-04-04

> **HOW TO USE THIS FILE:**
> - `[x]` = Done
> - `[ ]` = Pending
> - **KUNAL** = manual action only you can do (console, browser, account)
> - **CC/CUR** = Claude Code or Cursor.AI can build this
> - **BOTH** = Claude Code builds it, you verify/deploy

---

## PHASE 1 — Security (Fix before ANY public traffic)

These are not optional. One exploited XSS on a public tool ends the credibility.

### 1.1 Vikaa Chat — XSS Fix
- [ ] **CC/CUR** — Add DOMPurify CDN to `vikaa_ai_agent.html`
- [ ] **CC/CUR** — Wrap `div.innerHTML = inner` with `DOMPurify.sanitize()` in `vikaa_ai_agent.js:317`
- [ ] **CC/CUR** — Audit all other `innerHTML` assignments in the same file for the same pattern

### 1.2 Vikaa Chat — Rate Limiting
- [ ] **CC/CUR** — Add 30s per-user cooldown on `POST /agent/message` in `agent_router.py` (same pattern as AI Scout `/digest`)

### 1.3 Vikaa Chat — Query Length Cap
- [ ] **CC/CUR** — Add `MAX_QUERY_CHARS = 4000` guard at top of `/agent/message` handler

### 1.4 Vikaa Chat — Attachment Size Check (Move BEFORE processing)
- [ ] **CC/CUR** — In `LLM_LangChain.py`, move the `total_attachment_bytes > MAX_COMBINED_ATTACHMENT_MB` check to BEFORE the decode loop (currently it checks AFTER — wastes compute on oversized files)

### 1.5 Render Config Hardening
- [ ] **KUNAL** — GCP: Restrict Gemini API key to `generativelanguage.googleapis.com` only (GCP Console → APIs → Credentials)
- [ ] **KUNAL** — GCP: Set daily quota to 200 requests/day on Generative Language API
- [ ] **KUNAL** — Anthropic: Set spending limit $5/month at `console.anthropic.com → Settings → Limits`
- [ ] **KUNAL** — GCP: Set billing alert at $10/month (GCP Console → Billing → Budgets & Alerts)

---

## PHASE 2 — Infrastructure (Render Upgrade)

### 2.1 Upgrade Render Plan
- [ ] **KUNAL** — Upgrade from Free to **Starter ($7/mo)** in Render dashboard → vikaa-ai-platform → Settings → Plan
- [ ] **BOTH** — Update `render.yaml` line 7: change `plan: free` → `plan: starter`

### 2.2 Fix render.yaml — Missing Health Check
- [ ] **CC/CUR** — Add `healthCheckPath: /health` to `render.yaml` service block (without this, Render cannot auto-restart on hang)

### 2.3 Environment Variables on Render
- [ ] **KUNAL** — Verify ALL keys from `.env` are set in Render dashboard → Environment (not just the ones in `render.yaml`)
- [ ] **KUNAL** — Add `SUPABASE_SERVICE_ROLE_KEY` to Render env vars (currently only `SUPABASE_KEY` is listed in render.yaml — confirm which key the backend actually uses)
- [ ] **KUNAL** — Add `SERPAPI_API_KEY` / `SERP_API_KEY` to Render env vars

### 2.4 Keep-Alive (Even on Paid — good practice)
- [ ] **KUNAL** — Set up free UptimeRobot monitor on `https://app-wtiw.onrender.com/health` every 5 minutes. Alert to `kunal.debnath@vikaa.ai` on failure. (uptimerobot.com — free account)

---

## PHASE 3 — Critical UX Fixes (First impression matters)

### 3.1 Vikaa Chat — Async Job Pattern (MOST IMPORTANT UX FIX)
- [ ] **CC/CUR** — Refactor `POST /agent/message` to return `{ job_id }` immediately
- [ ] **CC/CUR** — Add `GET /agent/message/status/{job_id}` polling endpoint
- [ ] **CC/CUR** — Update `vikaa_ai_agent.js` to poll every 2–3s (same pattern as AI Scout digest)
- **Why:** PDF/video queries take 20–40s. Without this, users on Render see a frozen UI and leave.

### 3.2 Vikaa Chat — Typing / Loading Indicator
- [ ] **CC/CUR** — Add 3-dot animated bubble in chat while awaiting response

### 3.3 Vikaa Chat — System Prompt
- [ ] **CC/CUR** — Add `VIKAA_SYSTEM_PROMPT` to `handle_gemini()` in `LLM_GEMINI.py` (full draft already written in `vikaa-chat-Audit-Action.MD` Section 6)
- **Why:** Without a system prompt the agent has no persona, no brand voice, no guardrails. This is the biggest "feels amateur" signal.

### 3.4 Vikaa Chat — Remove Fake Model Buttons
- [ ] **CC/CUR** — Remove or grey-out GPT and Auto-Pilot options in `vikaa_ai_agent.html`. Don't show locked/fake features to a new user.

### 3.5 Vikaa Chat — Session ID Security
- [ ] **CC/CUR** — Replace `Math.random()` session ID with `crypto.randomUUID()` in `vikaa_ai_agent.js:148`

### 3.6 Vikaa Chat — Duplicate Function Cleanup
- [ ] **CC/CUR** — Remove duplicate `clearMessages()` at line 175 in `vikaa_ai_agent.js`
- [ ] **CC/CUR** — Import `history_manager` from `LLM_LangChain` into `LLM_GEMINI.py` (currently two separate in-memory stores)

---

## PHASE 4 — Tool-Level Readiness

Go through each LIVE tool. Verify it works end-to-end on the deployed URL before promoting it.

### 4.1 Gmail Intelligence Agent
- [ ] **BOTH** — Test full OAuth flow on Render (not just local). Gmail OAuth needs a pre-generated token or service account on server.
- [ ] **KUNAL** — Confirm `credentials.json` path is accessible on Render (it's in `etc/secrets/` — this folder must be deployed or env var used)

### 4.2 ReAct Research Agent
- [ ] **BOTH** — Test full research flow end-to-end on prod URL. Verify async job polling works.
- [ ] **KUNAL** — Confirm `SERPAPI_API_KEY` and `TAVILY_API_KEY` are set in Render env vars

### 4.3 AI Scout — Intelligence Digest
- [ ] **BOTH** — Run full digest on prod URL. Verify 7.5/10 audit fixes are deployed and working.
- [ ] **KUNAL** — Complete the 4 pending manual items from `docs/Claude-Code-Tool-Audit.MD` (GCP quota, Anthropic limit, API key restriction, billing alert)

### 4.4 Databricks Tools (Unity Browser, Conversational SQL, Synthetic Data) Databricks Tools (Unity Browser, Conversational SQL, Synthetic Data)
- [ ] **KUNAL** — Confirm `DATABRICKS_HOST_STORY` and `DATABRICKS_TOKEN_STORY` are set in Render env vars
- [ ] **BOTH** — Smoke test each tool on prod URL

### 4.6 RAG Configurator — Hide or Disable
- [ ] **CC/CUR** — In `ToolConfig.json`, set `"enabled": false` for `rag-config` — it's parked and has no page wired. Don't surface it to public users.

### 4.7 Autonomous FnO PaperTrade — Hide
- [ ] **CC/CUR** — Set `"enabled": false` for `autonomous-fno-papertrade` — shell only, no page wired

### 4.8 LinkedIn Assistant — Decide
- [ ] **KUNAL** — Currently `enabled: true` but marked as PARKED. Either wire it up or set `enabled: false`. Don't leave a dead link for new users.

---

## PHASE 5 — Content & Public Readiness

### 5.1 Landing Page / Home Page
- [ ] **KUNAL** — Does `vikaa.ai` homepage clearly explain what the platform does in 1 sentence? First-time visitors need context in under 5 seconds.
- [ ] **CC/CUR** — Add a short tagline/description if missing

### 5.2 Access Control — Who Gets In?
- [ ] **KUNAL** — Decide: Is this invite-only or open signup? Currently it's Supabase whitelist (ACL-gated). Define your access model before promoting.
- [ ] **KUNAL** — If invite-only: prepare a simple waitlist or "Request Access" email flow
- [ ] **KUNAL** — If open: test the full Supabase sign-up flow as a brand new user on prod URL

### 5.3 Error States & Edge Cases
- [ ] **BOTH** — What does a new user see if Gemini API is down? Test with a bad API key temporarily. Should show a friendly error, not a stack trace.
- [ ] **BOTH** — What does a user see if they hit rate limit? Should be a clear message, not a 429 HTTP error.

### 5.4 Mobile Responsiveness
- [ ] **KUNAL** — Open `https://app-wtiw.onrender.com` on your phone. Does the chat UI and tool index look acceptable?

---

## PHASE 6 — Monitoring & Ops (Set up BEFORE launch, not after)

- [ ] **KUNAL** — UptimeRobot: monitor `/health` every 5 min (free tier) → `uptimerobot.com`
- [ ] **KUNAL** — Anthropic console: set $5/month spending cap
- [ ] **KUNAL** — GCP console: set $10/month billing alert
- [ ] **KUNAL** — Render dashboard: enable email notifications on deploy failure
- [ ] **KUNAL** — Bookmark your Render logs URL — first place to look when something breaks in prod

---

## PHASE 7 — Post-Launch Backlog (Parked, not pre-launch)

These are valuable but not blockers. Build after you're live.

| # | Item | Files | Effort |
|---|------|-------|--------|
| 1 | **Scheduled Monday digests** (AI Scout) via APScheduler | `ai_scout_service.py` | 1 afternoon |
| 2 | **Store digests in Supabase** for trend comparison | `ai_scout_service.py` | 1 day |
| 3 | **Persist chat sessions to Supabase** | `agent_router.py`, `LLM_LangChain.py` | 1 day |
| 4 | **Suggested starter prompts** on empty chat | `vikaa_ai_agent.js` | 2 hours |
| 5 | **Claude Haiku fallback** if Gemini quota exceeded | `LLM_GEMINI.py` | half day |
| 6 | **Tool routing** — detect intent and suggest right tool | `agent_router.py` | 1 week |
| 7 | **Meeting Intelligence Agent** | New | Multi-day |
| 8 | **Multi-Agent Pipeline Designer** | New | Complex |

---

## Summary — Launch Gate

| Phase | Owner | Status |
|-------|-------|--------|
| Phase 1 — Security | CC/CUR + KUNAL | Not started |
| Phase 2 — Infrastructure | KUNAL + BOTH | Not started (render.yaml still on `free`) |
| Phase 3 — UX Fixes | CC/CUR | Not started |
| Phase 4 — Tool Readiness | BOTH | Not started |
| Phase 5 — Content | KUNAL | Not started |
| Phase 6 — Monitoring | KUNAL | Not started |

> **You are ready to launch when Phases 1–3 are complete and Phase 4 passes smoke tests.**
> Phases 5 and 6 should be done in parallel, not after.

---

*This is an MVP checklist. It will evolve as tools are added and the platform matures.*
*Maintained by: Kunal Debnath | Assisted by: Claude Code + Cursor.AI*
