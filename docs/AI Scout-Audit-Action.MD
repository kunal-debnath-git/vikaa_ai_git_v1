# AI Scout — Planning Agent: Audit Report

**Audited by:** Claude Code (Opus 4.6)
**Date:** 2026-04-03
**Last updated:** 2026-04-04 (implementation pass 2)
**Scope:** Functionality, technical soundness, security, deployment readiness, documentation, Gmail token longevity, reliability, and cost control.

---

## 1. Functionality — Is it doing justice?

**Verdict: Yes, with caveats.**

The tool delivers on its core promise: a company-contextual AI industry digest with seven well-defined sections. The pipeline (Tavily search -> LLM analysis -> structured JSON -> multi-channel delivery) is sound and the output schema is rich.

### What works well
- **Focused vs. global query strategy** is a smart retrieval design that avoids "giant vendor noise" diluting company-specific digests.
- **Config-driven companies/recipients** — zero-code additions via JSON files.
- **Multi-channel delivery** (UI render, email, markdown download, print-to-PDF) covers real use cases.
- **LinkedIn pack + Agent Idea sections** are genuinely differentiating — they turn passive reading into actionable output.

### Improvement suggestions
| # | Suggestion | Impact |
|---|-----------|--------|
| 1 | **Add a "Confidence / Freshness" indicator** — show how many Tavily results were found and their average recency. If only 3 articles were found for a focused company, the digest quality is lower and the user should know. | Trust |
| 2 | **Source attribution per section** — currently `source_url` is only on `signal_of_week` and `player_moves`. Add it to `monetization_radar` items so readers can verify claims. | Credibility |
| 3 | **"Planning Agent" naming is aspirational** — the tool is a deterministic pipeline, not a multi-step planner with backtracking. Rename to "AI Scout — Intelligence Digest" or keep the name but add a note in docs. Already noted in your doc (line 80), but the frontend title still says "Planning Agent" in the audit request. | Clarity |
| 4 | **Add a "Compare to last week" diff** — when a user generates a new weekly digest, highlight what changed from the previous one (new entrants that appeared/disappeared, player moves that escalated). Store the previous digest hash/key in localStorage. | Stickiness |
| 5 | **Digest quality scoring** — after LLM returns JSON, run a lightweight validation: are all required fields non-empty? Are source_urls actually from the provided articles? Log a quality score. | Reliability |

---

## 2. Technical Soundness

**Verdict: Solid for a single-instance deployment. A few loose ends.**

### Good
- Gemini direct REST avoids SDK version conflicts — deliberate and documented.
- `_clean_json()` handles markdown fences and partial JSON robustly.
- Dataclass schema is well-structured with sensible defaults.
- Fallback LLM pattern (Gemini -> Anthropic) is clean.
- Deduplication by URL in `_gather_news` prevents duplicate articles.

### Issues to address

| # | Issue | Severity | File:Line | Fix |
|---|-------|----------|-----------|-----|
| T1 | **`html` module shadowed by local variable** — `import html` at top of service, then `html = f"""...` on line 652 creates a local variable named `html` that shadows the module. The `html.escape()` calls inside the f-string on lines 667-706 work only because they're evaluated during the f-string construction where the module is still in scope. This is fragile. | Medium | `ai_scout_service.py:652` | Rename the local variable to `html_body` or `email_html`. | **✅ DONE** — renamed to `email_html` |
| T2 | **No timeout on Anthropic fallback** — Gemini has `timeout=120` on `urlopen`, but the Anthropic SDK call has no explicit timeout. A hung Anthropic call blocks the request indefinitely. | Medium | `ai_scout_service.py:419` | Add `timeout=httpx.Timeout(120)` to the Anthropic client or use `client.messages.create(..., timeout=120)`. | **✅ DONE** — `anthropic.Anthropic(api_key=key, timeout=120.0)` |
| T3 | **`generate_digest` function name collision in router** — Line 117 imports `generate_digest` from the service, but the endpoint function is also named `generate_digest` (line 103). The import shadows the function. This works because the import is inside the function body, but it's confusing. | Low | `ai_scout_router.py:103,117` | Rename the endpoint function to `handle_generate_digest` or use `from ... import generate_digest as run_digest`. | Deferred — low risk (import is inside function body) |
| T4 | **In-memory cache lost on process restart** — documented and acceptable for single-instance, but Render's free tier restarts frequently (spins down after 15 min inactivity). Every first request after spin-up hits Tavily + LLM. | Medium | `ai_scout_router.py:24` | For Render: use a simple file-based cache (`/tmp/ai_scout_cache.json`) or Supabase table as cache backend. Redis is overkill for this scale. | Deferred — acceptable until reliability phase |
| T5 | **No rate limiting on `/digest` endpoint** — a misbehaving client (or someone testing) could trigger many Tavily + LLM calls rapidly. | Medium | `ai_scout_router.py:102` | Add a simple in-memory rate limit (1 request per 30 seconds per user) or use FastAPI's `slowapi`. | **✅ DONE** — per-user 30s cooldown added to router |
| T6 | **`new_entrants` parsing doesn't check `isinstance(e, dict)`** — unlike `player_moves` and `monetization_radar`, the `new_entrants` loop on line 498-505 doesn't guard against non-dict items. If LLM returns a malformed list, this will crash. | Low | `ai_scout_service.py:498` | Add `if isinstance(e, dict)` filter (like the other sections). | **✅ DONE** — isinstance guard added |
| T7 | **Tavily client re-created on every query** — `_tavily_search` creates a new `TavilyClient` on each call. With 2-5 queries per digest, that's unnecessary. | Low | `ai_scout_service.py:149` | Create client once in `_gather_news` and pass it, or use a module-level lazy singleton. | Deferred — micro-optimisation, not blocking |

---

## 3. Security

**Verdict: Reasonably secure for a personal/small-team tool. One XSS concern.**

### XSS in Frontend (CRITICAL) — ✅ DONE

**The `renderDigest()` function in `Tool_AIScout.html` directly injects LLM-generated text into `innerHTML` without escaping.**

Lines 706-822: Every field (`d.signal_of_week.headline`, `p.player`, `p.headline`, `p.detail`, `m.how_to_act`, etc.) is inserted raw into HTML. If the LLM returns text containing `<script>` or `<img onerror=...>`, it will execute in the user's browser.

**Risk:** The LLM output is derived from Tavily search results (web content). A poisoned article title could contain XSS payload that flows through: article title -> LLM prompt -> LLM output -> `innerHTML`.

**Fix implemented:** Added three helpers (`esc()`, `safeUrl()`, `safeFlag()`) and applied them throughout `renderDigest()` and the history panel. All LLM-generated text now goes through `esc()`. URL `href` values go through `safeUrl()` (blocks `javascript:` URLs). CSS flag class goes through `safeFlag()` (whitelists Opportunity/Threat/Watch).

### Other security items

| # | Item | Status | Notes |
|---|------|--------|-------|
User Comment> well manages so far >>> | S1 | **API keys in `.env`** — not committed to git (check `.gitignore`) | CHECK | Verify `.env` is in `.gitignore`. If it's committed, rotate ALL keys immediately. |
| S2 | **SMTP credentials in env vars** | OK | Standard practice for Render. |
| S3 | **ACL enforcement on `/digest` and `/send-email`** | OK | `require_whitelisted_user` properly protects costly endpoints. |
| S4 | **Trusted dev bypass** — localhost/LAN/Cursor hosts skip ACL | OK for dev | Ensure this doesn't activate on Render (it won't — server host is not loopback). |
| S5 | **Gemini API key in URL** — `_run_gemini` puts the key in the query string (`?key=...`). This is Google's standard for REST, but the key appears in server logs if you log request URLs. | Low risk | Don't log the full Gemini URL. |
| S6 | **`/status` endpoint exposes key availability** | OK | Only tells if keys are configured (bool), not the keys themselves. |
| S7 | **Email recipient list served without auth** | Low risk | `/recipients` is GET without ACL — anyone can see the 3 email addresses. Add `require_whitelisted_user` if you want it private. |
| S8 | **JSON config files in `/frontend/`** — `ai_scout_companies.json` and `ai_scout_recipients.json` are served as static files. Recipients contain email addresses. | Low risk | If frontend is publicly served, these are accessible. Move recipients to backend-only if concerned. |

---

## 4. Render.com Deployment Readiness

**Verdict: Ready for deployment with minor adjustments.**

| # | Check | Status | Action needed |
|---|-------|--------|--------------|
| D1 | **`requirements.txt` complete** | OK | All dependencies listed. |
| D2 | **Entry point** (`main.py` with uvicorn) | OK | Standard FastAPI deployment. |
| D3 | **Environment variables** | OK | All secrets via env vars, not hardcoded. |
| D4 | **CORS** | CHECK | Verify `main.py` CORS config allows your production domain. |
| D5 | **Process timeout** — Render free tier has 30s request timeout. Digest generation (Tavily + LLM) takes 20-50s per the UI. | ✅ DONE | Async job pattern implemented: `POST /digest` launches background task and returns `job_id` instantly; frontend polls `GET /digest/status/{job_id}` every 3s. |
| D6 | **Cold start** — Render free tier spins down after 15 min. First request takes 30-60s (startup + digest generation). | RISK | For public release: use Render paid tier ($7/mo) for always-on, or add a health-check ping to keep it warm. |
| D7 | **Static files** — Frontend HTML served from `/frontend/`. Render serves via FastAPI's `StaticFiles`. | CHECK | Verify `StaticFiles` mount path in `main.py`. |
| D8 | **Memory** — Single digest generation uses moderate memory (articles + LLM prompt/response). No concern for Render's 512MB free tier. | OK | |

---

## 5. Documentation Review (`docs/tools/AI_Scout.md`)

**Verdict: Well-structured. A few drafting improvements needed.**

| # | Section | Issue | Suggested fix |
|---|---------|-------|--------------|
| Doc1 | Title | "Planning Agent" is misleading (see point 3 in section 1) | Change to "AI Scout — Intelligence Digest" or add "(Pipeline)" qualifier |
| Doc2 | Section 1, "Story" | Good one-liner, but doesn't mention the **company-specific lens** as the primary differentiator up front | Lead with: "A **company-lens AI industry digest** that..." |
| Doc3 | Section 2, "Why" | Three bullet points are solid but could use a **"Who is this for?"** line | Add: "Audience: tech leads, senior consultants, practice leads who need weekly AI awareness without the noise." |
| Doc4 | Section 3, "Components" | Lists libraries but doesn't mention **Supabase** for auth/ACL, which is a key dependency | Add Supabase to the Libraries/APIs list |
| Doc5 | Section 3, "Information flow" | Step 5 "Optional: SMTP" is vague | Change to: "Optional: SMTP email to configured recipients via Hostinger (not Gmail API)." |
| Doc6 | Section 4, "Key consideration" | Fourth bullet mentions "planning agent upgrade pending" — this should either be a clear roadmap item or removed if not planned | Either add a "Roadmap" section or change to: "Current architecture is a pipeline; multi-step planning is a potential future enhancement." |
| Doc7 | Section 5, "Interview prep" | Missing a point on **cost control** — interviewers will ask "how much does this cost to run?" | Add: "Cost model: Tavily (1000 free/mo), Gemini (free tier generous), Anthropic fallback (pay-per-token), SMTP (Hostinger plan). Cache reduces redundant LLM calls." |
| Doc8 | General | No **version history** or **last updated** date | Add a "Last updated: YYYY-MM-DD" at the top |

---

## 6. Gmail Token Longevity

**This tool does NOT use Gmail API / Gmail tokens.** It uses SMTP (Hostinger) for email delivery.

However, since you asked about Gmail tokens broadly (likely for your Gmail Intelligence tool), here's the assessment of `gmail_service.py`:

| # | Finding | Risk | Fix |
|---|---------|------|-----|
| G1 | **Google OAuth refresh tokens** — stored in `tokens/<email>.json`. These have **no expiry** as long as the app stays in "Testing" status with < 100 users. Once you move to "Production" in Google Cloud Console, refresh tokens issued to users outside the test-user list expire in 7 days. | HIGH if you publish | Keep the app in "Testing" mode if < 100 users. For production: implement token refresh logic (the `google-auth-oauthlib` library handles this automatically if `Credentials` object is properly persisted after refresh). |
| G2 | **Refresh token revocation** — Google revokes refresh tokens if: (a) user changes password, (b) user revokes access in Google Account settings, (c) app is idle for 6 months, (d) user has 50+ outstanding tokens (oldest gets revoked). | Medium | Add a `/gmail/token-health` endpoint that checks if the stored token is still valid and alerts if refresh fails. |
| G3 | **Token storage on Render** — Production tokens at `/etc/secrets/token*.json`. Render secret files are persistent across deploys but NOT across service rebuilds (if you delete and recreate the service). | Medium | Back up tokens to Supabase or an encrypted blob store. |
| G4 | **No automatic re-auth flow** — if a token expires/revokes, there's no server-side way to re-authenticate (OAuth requires browser interaction). The user must manually re-auth. | Medium | Add clear error messaging: "Gmail token expired — please re-authenticate at /gmail/auth". Consider a scheduled token health check that emails you before expiry. |

---

## 7. Enhancement Suggestions for Long-term Value

| # | Enhancement | Effort | Value |
|---|------------|--------|-------|
User Comment> NOT NOW >>> | E1 | **Scheduled digests** — Use APScheduler (already in requirements.txt) to auto-generate digests on Monday mornings and email them. No manual button press needed. | Medium | High — transforms from on-demand tool to passive intelligence feed. |
User Comment> NOT NOW >>> | E2 | **Digest comparison / trend tracking** — Store digests in Supabase. Show "This player was in last 3 digests" or "New entrant from 2 weeks ago is now a Player Move." | Medium | High — turns snapshots into longitudinal intelligence. |
User Comment> NOT NOW >>> | E3 | **User-configurable companies** — Let whitelisted users add their own companies via UI (currently requires JSON edit + deploy). | Low | Medium |
User Comment> NOT NOW >>> | E4 | **Slack delivery channel** — Many teams live in Slack. Add a webhook-based Slack delivery option alongside email. | Low | Medium |
User Comment> NOT NOW >>> | E5 | **Digest rating/feedback** — Add thumbs up/down per section. Store in Supabase. Use to refine prompts over time. | Low | High — creates a feedback loop for prompt quality. |
User Comment> NOT NOW >>> | E6 | **Embedding-based dedup** — Two articles about the same story from different outlets currently both appear. Use embeddings to detect semantic duplicates. | Medium | Medium — already partially handled by URL dedup. |
User Comment> NOT NOW >>> | E7 | **Multi-language support** — Add a `language` parameter to generate digests in other languages (the LLM can handle this with a simple prompt instruction). | Low | Niche |

---

## 8. Reliability and Continuity (MUST)

**Goal: Run uninterrupted month after month.**

### Current failure modes and mitigations

| Failure mode | Current behavior | Recommended fix |
|-------------|-----------------|-----------------|
| **Render cold start** | 30-60s delay, request may timeout | Paid tier ($7/mo) for always-on, OR cron ping every 10 min to keep warm |
| **Tavily API down** | `_tavily_search` returns empty list, digest has no articles -> LLM hallucinates | Add minimum article threshold: if < 5 articles found, return error "Insufficient data" instead of running LLM | **✅ DONE** — `RuntimeError` raised if < 5 articles; returns 503 to frontend |
| **Gemini API down** | Falls back to Anthropic | Already handled. Good. |
| **Anthropic API down** | 500 error to user | Add a third fallback or graceful error: "Both LLMs unavailable, try again in 5 minutes" |
| **SMTP down** | Email send fails, returns `failed` list | Already handled per-recipient. Add retry (1 retry with 5s delay). |
| **Tavily API key quota** | RuntimeError -> 503 | Monitor Tavily usage. Free tier = 1000 searches/month. Each digest = 2-5 searches. ~200 digests/month max. |
| **Render service restart** | Cache lost, in-flight requests dropped | File-based cache in `/tmp/` survives within a deploy. For cross-restart: Supabase cache table. |
| **LLM returns malformed JSON** | `_clean_json` tries to recover, may still fail | Add one retry: if JSON parse fails, re-prompt with "Your previous response was not valid JSON. Return ONLY JSON:" |

### Recommended reliability stack

1. **Health check endpoint** (already exists: `/health` in `main.py`) — wire to Render's health check.
2. **UptimeRobot or Render cron** — ping `/health` every 10 min to prevent cold starts.
3. **Alerting** — Add a simple Slack/email alert if `/tools/ai-scout/status` returns `ready: false` (means a key is missing/expired).
4. **Digest auto-generation** — APScheduler cron job every Monday 6 AM UTC. Store result in Supabase. Frontend loads from Supabase if available, falls back to live generation.

---

## 9. Usage Budget and Cost Control (MUST)

### Current cost breakdown (estimated monthly)

| Service | Free tier | Your estimated usage | Monthly cost |
|---------|-----------|---------------------|-------------|
| **Tavily** | 1,000 searches/mo | ~100-300 searches (2-5 per digest x 20-60 digests) | $0 (within free tier) |
| **Gemini 2.5 Pro** | Free tier: generous (varies) | ~20-60 calls/mo | $0-5 (likely free) |
| **Anthropic (fallback)** | No free tier | Only when Gemini fails, ~1-5 calls/mo | $0.01-0.10 |
| **Render** | Free tier (750 hrs) | 1 service always-on | $0 (free) or $7 (paid) |
| **Hostinger SMTP** | Included in hosting plan | ~20-60 emails/mo | $0 (included) |
| **Supabase** | Free tier (50K rows, 500MB) | Auth + ACL only | $0 |
| **Total** | | | **$0-12/mo** |

### Cost control measures to implement

| # | Measure | Status | Action |
|---|---------|--------|--------|
| C1 | **4-hour cache** — prevents duplicate LLM calls | DONE | Good. |
| C2 | **Tavily `search_depth: "basic"`** — cheaper than "advanced" | DONE | Good. |
| C3 | **Article cap at 40** — limits token input to LLM | DONE | Good. `articles[:40]` on line 191. |
| C4 | **`maxOutputTokens: 8192`** for Gemini | DONE | Reasonable cap. |
| C5 | **Monitor Tavily usage** | NOT DONE | Add a counter in the cache dict or Supabase: increment on each Tavily call. Add a `/tools/ai-scout/usage` endpoint. |
| C6 | **Daily/monthly call cap** | NOT DONE | Add a guard: if `_tavily_call_count > 800` this month, refuse new digests with "Monthly search quota reached". |
| C7 | **Gemini model selection** | PARTIAL | Currently uses `gemini-2.5-pro` (most expensive Gemini). For cost savings, consider `gemini-2.0-flash` for weekly digests (cheaper, faster) and reserve Pro for monthly deep-dives. |
| C8 | **Google Cloud billing alerts** | NOT DONE | If using Gemini via Google Cloud (API key), set a billing alert at $5/mo in Google Cloud Console. This is the #1 protection against surprise bills. |
| C9 | **Anthropic spending limit** | NOT DONE | Set a monthly spending limit in Anthropic Console (Settings > Limits). $5/mo is more than enough for fallback use. |

### Google Cloud specific cost management

1. **API key restrictions** — In Google Cloud Console, restrict your Gemini API key to only the `generativelanguage.googleapis.com` API. This prevents the key from being used for other (potentially expensive) Google APIs if leaked.
2. **Quotas** — Set a per-minute and per-day quota on the Generative Language API in Google Cloud Console (APIs & Services > Quotas). Recommended: 10 requests/minute, 200 requests/day.
3. **Billing budget** — Create a budget alert at $10/mo with notifications at 50%, 80%, 100%.
4. **Project isolation** — If this API key is in a shared Google Cloud project, create a dedicated project for Vikaa AI tools. This isolates billing and prevents cross-tool cost bleed.

---

## Summary: Priority Action Items

### Must fix (before public release)
1. ✅ **XSS in frontend** — `esc()`, `safeUrl()`, `safeFlag()` added; all LLM output escaped (Section 3)
2. ✅ **Request timeout on Render** — async job pattern: `POST /digest` returns `job_id` immediately; frontend polls `GET /digest/status/{job_id}` every 3s. No request can block past Render's 30s limit.
3. ✅ **Add rate limiting** on `/digest` endpoint — 30s per-user cooldown (Section 2, T5)
4. ⚠️ **Set Google Cloud billing alert at $10/mo** — TODO (manual): GCP Console → Billing → Budgets & Alerts

### Should fix (reliability)
5. ✅ **Minimum article threshold** — `RuntimeError` if < 5 articles, prevents LLM hallucination (Section 8)
6. ✅ **Anthropic timeout** — `timeout=120.0` on Anthropic client (Section 2, T2)
7. **File-based cache** for Render restarts (Section 2, T4) — deferred
8. **Tavily usage counter** with monthly cap (Section 9, C5-C6) — deferred

### Manual actions pending (do when ready)
- ⚠️ **Google Cloud billing alert** — GCP Console → Billing → Budgets & Alerts → New Budget → set $10/mo alert at 50%, 80%, 100%
- ⚠️ **Anthropic spending limit** — console.anthropic.com → Settings → Limits → set $5/mo cap
- ⚠️ **GCP API key restriction** — GCP Console → APIs & Services → Credentials → restrict key to `generativelanguage.googleapis.com` only
- ⚠️ **GCP daily quota** — GCP Console → APIs & Services → Quotas → Generative Language API → set 200 requests/day

### To reach 9/10 — two high-impact items (parked, do when ready)
9. ⭐ **Scheduled Monday digests** via APScheduler (Section 7, E1) — APScheduler already in `requirements.txt`. One afternoon of work. Shifts tool from "on-demand" to "passive intelligence feed landing in inbox."
10. ⭐ **Store one digest in Supabase for comparison** (Section 7, E2) — save last digest per company/period to a Supabase table. Frontend can then show "last week this was Tactical, now Strategic." Shifts from tool → platform with memory. These two together take it from 7.5/10 to 9/10.

### Other enhancements (lower priority — parked)
11. **Token health endpoint** for Gmail tokens (Section 6, G4)
12. **Slack delivery channel** — webhook alongside email (Section 7, E4)
13. **Digest rating/feedback** — thumbs up/down per section stored in Supabase; creates prompt improvement loop (Section 7, E5)

### Framing note for tech showcase
- Present as: *"A production AI digest pipeline with company-contextual retrieval"* — not a planning agent
- The "agent" name is aspirational; a technical audience will probe it and find a pipeline — own the pipeline confidently
- Defensible strengths: RAG retrieval design, multi-provider LLM fallback, structured JSON schema enforcement, ACL-gated FastAPI on cloud, real product UX (not a notebook)

### Documentation
12. ✅ **`docs/tools/AI_Scout.md`** — fully updated: audience line, Supabase in stack, SMTP clarified, cost model added to interview prep, last updated date, pipeline vs planner clarified

---

*Audit complete. No code changes were made — this document is for review and prioritization.*
