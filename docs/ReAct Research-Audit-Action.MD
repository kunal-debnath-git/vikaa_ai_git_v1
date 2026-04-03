# ReAct Research Agent — Audit Report

**Audited by:** Claude Code (Sonnet 4.6)
**Date:** 2026-04-04
**Scope:** Functionality, technical soundness, security (XSS), Render deployment readiness, documentation, prompt quality, enhancement suggestions, reliability, and cost control.

---

## 1. Functionality — Is it doing justice?

**Verdict: Yes for the core use case. Two gaps hold it back.**

The pipeline is genuinely useful: Tavily search → structured JSON → two-phase UX (quick sources first, analysis second). The refine modes (`assumption_audit`, `compare_options`, `executive_brief`, etc.) are a real differentiator — they turn a generic search summarizer into a decision-support tool.

### What works well
- **Two-phase UX** — `/quick` returns sources in ~1–2s; full analysis loads after. Excellent perceived performance.
- **Refine modes** — five distinct synthesis lenses with citation requirements. Well thought-out prompting.
- **`research_context`** — multi-turn continuity without a persistent session. Smart design for stateless API.
- **Depth levels** — 3 tiers (Quick / Standard / Deep) each with different schema shape, Tavily depth, and LLM instruction. Clean.
- **Source quality scoring** (frontend) — `.gov`/`.edu` domain bonus, HTTPS check, snippet length weighting. Nice UX touch.

### Gaps
| # | Gap | Impact |
|---|-----|--------|
| F1 | **The "ReAct" label is misleading** — The tool is a one-shot retrieval + synthesis pipeline. The "ReAct Trace" card in the UI is hardcoded labels, not a real Reason-Act-Observe loop. A technical interviewer or senior user will probe this and find it hollow. | Credibility |
| F2 | **No minimum result guard** — AI Scout raises a `RuntimeError` if < 5 articles found. This tool passes zero results to the LLM (which then hallucinates politely). | Reliability |
| F3 | **`executive_brief` mode conflicts with the output schema** — The refine instruction says "exactly five bullets", but the schema has `body: []`. The LLM may put them in `body`, `key_takeaways`, or both — rendering is inconsistent. | Quality |

### Improvement suggestions
| # | Suggestion | Impact |
|---|-----------|--------|
| I1 | **Source freshness indicator** — Add a "days ago" tag on each source using Tavily's `published_date` field if available. Users making decisions need to know if sources are 2 days old vs 2 years. | Trust |
| I2 | **"No results" guard** — Mirror AI Scout: if `len(raw_results) == 0`, return a clean 404; if `< 3`, warn the user ("only N sources found — results may be limited"). | Reliability |
| I3 | **Fix `executive_brief` schema collision** — Add a depth-level override or a mode-specific renderer in the frontend that handles the five-bullet format. | Quality |
| I4 | **Query suggestion** — When context is active, offer 2–3 follow-up questions as pre-fill chips before the user types. Currently follow-up chips appear after results, not before. | UX |

---

## 2. Technical Soundness

**Verdict: Functional but missing two critical safety rails (timeout, rate limit).**

### Good
- Lazy singleton `_get_tavily()` and `_get_llm()` — no re-initialization on every request.
- Tavily in-memory cache (10-min TTL, thread-safe with `Lock`) avoids duplicate API hits on repeat queries.
- JSON fallback in `/web-research` endpoint returns partial response with sources even if LLM fails — users get something.
- Pydantic models enforce request shape cleanly.
- `_resolve_gemini_model()` probe-on-start with lock is solid resilience pattern.

### Issues to fix

| # | Issue | Severity | Location | Fix |
|---|-------|----------|----------|-----|
| T1 | **No timeout on LangChain Gemini call** — `llm.invoke([HumanMessage(...)])` has no timeout. A hung Gemini call blocks the worker thread indefinitely. Render will kill it at 30s, but the thread is still waiting, and queued requests stack up. | **HIGH** | `web_research_router.py:256` | Pass `request_options={"timeout": 60}` to `ChatGoogleGenerativeAI` constructor, or wrap in `asyncio.wait_for`. |
| T2 | **No rate limiting on either endpoint** — `/web-research/quick` and `/web-research` are unprotected against rapid fire. A misbehaving client could exhaust Tavily's monthly 1,000 free searches in minutes. AI Scout has a 30s per-user cooldown; this tool has none. | **HIGH** | `web_research_router.py:273,289` | Add per-user 15s cooldown (same pattern as AI Scout). |
| T3 | **Tavily cache eviction is all-or-nothing** — `_tavily_cache.clear()` when `len > 128` clears every entry. The oldest entry triggered the eviction but all 128 cached results are lost too. | Low | `web_research_router.py:192` | Evict only the oldest 32 entries: `for k in list(_tavily_cache.keys())[:32]: del _tavily_cache[k]`. |
| T4 | **Duplicate Tavily call between `/quick` and `/web-research`** — The two-phase UX calls `/quick` first, then `/web-research`. Both call `_do_tavily_search` with the same parameters. If the 10-min cache catches it, this is fine. But the first call from `/quick` writes to cache, so the second call (within 10 min) hits cache — OK in practice. Worth documenting as an assumption. | Low | Design | No action needed if user always calls /quick before /web-research within 10 min. |
| T5 | **`_get_llm()` calls `_resolve_gemini_model()` twice** — once to init the LLM object (line 42) and once for the log message (line 45). Minor inefficiency. | Low | `web_research_router.py:40-45` | Cache result: `model = _resolve_gemini_model(); _llm = ChatGoogleGenerativeAI(model=model, ...); logger.info(f"...{model}")`. |
| T6 | **`LLM_GEMINI.py` drags in `moviepy`, `PIL`, `attachment_handlers`** — heavy imports at module level, all loaded just to get `_resolve_gemini_model()`. On Render cold start, this adds seconds to startup time for an unrelated tool. | Medium | `models/LLM_GEMINI.py` | Extract `_resolve_gemini_model()` into a minimal `models/gemini_resolver.py` with no heavy imports. |
| T7 | **JSON fence stripping is minimal** — `raw.lstrip("```json").lstrip("```").rstrip("```")` does not handle mid-string fences, nested code blocks, or `data:application/json` prefixes. AI Scout's `_clean_json()` is more robust (finds outer `{...}`). | Low | `web_research_router.py:257` | Reuse/import AI Scout's `_clean_json()` or copy the pattern. |
| T8 | **Render 30s timeout on Deep mode** — Deep mode (`depth_level=3`) uses Tavily `advanced` search (slower) + longer LLM output. Combined latency can exceed 30s. AI Scout solved this with async jobs. | Medium | `web_research_router.py:289` | Apply same async job pattern as AI Scout for `/web-research` (not `/quick` — that's already fast). |

---

## 3. Security

**Verdict: CRITICAL XSS vulnerability in frontend. Same class of issue that was fixed in AI Scout.**

### XSS in `buildNewsCard()` and `openShare()` — CRITICAL

The `escapeHtml()` helper IS defined in the frontend (line 872) but is almost entirely unused for LLM-generated content.

**Unescaped LLM output injected directly into `innerHTML`:**

| Variable | Location | Risk |
|----------|----------|------|
| `headline` | `buildNewsCard:1344`, `openShare:1496` | Full XSS via `<script>` or `<img onerror=...>` |
| `tldr` | `buildNewsCard:1351`, `openShare:1497` | Full XSS |
| `body[]` paragraphs | `buildNewsCard:1298`, `openShare:1498` | Full XSS |
| `key_takeaways[]` | `buildNewsCard:1305`, `openShare:1501` | Full XSS |
| `follow_up_questions[]` | `buildNewsCard:1311-1314` | Full XSS + `data-q` attribute injection |
| `sources[].title` | `buildNewsCard:1322`, `openShare:1504` | Full XSS |
| `sources[].url` | `buildNewsCard:1322`, `openShare:1504` | `javascript:` URL execution |

**Threat path:** poisoned article title (via SEO or a malicious site) → Tavily snippet → LLM output field → `innerHTML` → executes in browser.

**Fix required:** Apply `escapeHtml()` to every LLM-generated string before inserting into `innerHTML`. Apply `safeUrl()` (block non-`http(s)`) to all `href` attributes from sources. This is exactly what was done in `Tool_AIScout.html` with `esc()`, `safeUrl()`, `safeFlag()`.

### Other security items

| # | Item | Status | Notes |
|---|------|--------|-------|
| S1 | **ACL enforcement** | OK | Both endpoints use `require_whitelisted_user`. |
| S2 | **`refine_mode` injection** | OK | Validated against `_REFINE_MODE_INSTRUCTIONS` dict before use — unknown values silently ignored. |
| S3 | **`research_context` prompt injection** | Low risk | Prior queries/headlines are appended to the prompt. A malicious user could craft a query designed to override prompt instructions. Mitigation: the existing "every factual claim must be supported by search results" rule limits impact. |
| S4 | **`query` prompt injection** | Low risk | Query is inserted verbatim into the prompt (`f"Query: {query}"`). Same concern as S3. Low risk given it's a whitelisted-user-only endpoint. |
| S5 | **Source URLs in frontend links** | CRITICAL (see above) | No `safeUrl()` guard — `javascript:` URLs from poisoned articles will execute. |

---

## 4. Render.com Deployment Readiness

**Verdict: Quick mode is safe. Full mode has timeout risk on Deep level.**

| # | Check | Status | Action needed |
|---|-------|--------|--------------|
| D1 | `requirements.txt` complete | OK | Tavily, LangChain, FastAPI present. |
| D2 | Entry point (`main.py` + uvicorn) | OK | Standard. |
| D3 | Environment variables | OK | `TAVILY_API_KEY`, `GOOGLE_API_KEY`/`GEMINI_API_KEY` via env vars. |
| D4 | **Request timeout — `/web-research/quick`** | OK | Tavily only, ~1–3s. Safe. |
| D5 | **Request timeout — `/web-research` (Standard)** | LOW RISK | ~5–20s typically. Within Render free tier 30s. Monitor. |
| D6 | **Request timeout — `/web-research` (Deep)**| RISK | Tavily `advanced` + longer LLM output can exceed 30s. Apply async job pattern (see T8). |
| D7 | **Cold start** | RISK | `LLM_GEMINI.py` imports `moviepy` + `PIL` — heavy. Cold start adds 10-20s. Extract `_resolve_gemini_model()` (see T6). |
| D8 | **Memory** | OK | No large in-memory structures. 128-entry Tavily cache is bounded. |

---

## 5. Documentation Review (`docs/tools/ReAct_Research_Agent.md`)

**Verdict: Decent structure. Two things that will hurt in a technical review.**

| # | Issue | Fix |
|---|-------|-----|
| Doc1 | **"ReAct" naming is not defended** — The doc mentions "upgrade pending" but doesn't explain what the current architecture actually is. A tech leader reading this will ask "where's the ReAct loop?" and find no answer. | Add a clear sentence: "Current backend is a **one-shot retrieval + synthesis pipeline** (Tavily → Gemini → JSON). The ReAct trace in the UI visualises the pipeline steps — it is **not** a live multi-step agent loop." |
| Doc2 | **No failure modes section** | Add a failure modes table (same format as AI Scout doc). |
| Doc3 | **Security posture not mentioned** | After the XSS fix, add a security note similar to AI Scout's. |
| Doc4 | **No last-updated date** | Add metadata. |
| Doc5 | **Interview prep is thin** — 7 bullets but they're vague. "Roadmap one-liner" is good. | Add: citation enforcement as a prompt engineering technique; why two-phase UX matters for latency; `research_context` as stateless multi-turn without a session store. |

---

## 6. ⭐ Prompt Improvements (Priority)

The current prompt is solid but has three fixable gaps.

### Gap 1 — `executive_brief` schema collision (HIGH)
The `executive_brief` refine instruction says "exactly five bullets in `body`" but `_SCHEMA_QUICK` (depth=1) and `_SCHEMA_STANDARD` have `"body": []` with no examples. The LLM sometimes puts the five bullets in `key_takeaways` instead, making rendering inconsistent.

**Fix:** Add a note to the `executive_brief` instruction: `"Place the five bullets in the body array, one per item. Set key_takeaways to [] and follow_up_questions to []."` This is a one-line prompt change.

### Gap 2 — `follow_up_questions` are too generic
The current schema example is `"question 1?"` — the LLM generates vague questions like "What is the future of AI?". They should be follow-up *search queries* the user can immediately run.

**Current:**
```
"follow_up_questions": ["question 1?", "question 2?", "question 3?"]
```
**Improved:**
```
"follow_up_questions": ["<specific search query the reader should run next to go deeper on a gap in the above>"]
```
Add to citation rules: `"Every follow_up_question must be a concrete search query, not a rhetorical question. Reference a specific gap in the evidence with [n]."`

### Gap 3 — Grounding rule needs teeth
The current grounding rule `"Based ONLY on these search results"` is stated but the LLM still sometimes adds general knowledge. Tighten it:

**Current:**
```
"Based ONLY on these search results, answer the query."
```
**Improved:**
```
"BASE EVERY CLAIM on the numbered search results above. If a claim has no supporting source, do not include it. Prefer omission over speculation."
```

### Gap 4 — `assumption_audit` mode has no output example
The assumption audit instruction is well-written but the labels ("supported / weak / contradicted / unstated") are buried in prose. The LLM doesn't know the format to use in the JSON body. Add a format hint:

**Add to assumption_audit instruction:**
```
"Format in body[]: one paragraph per assumption. Start each with '[ASSUMPTION: <claim>] [VERDICT: supported/weak/contradicted/unstated — source [n]]' then explain."
```

---

## 7. Enhancement Suggestions (Long-term Value)

| # | Enhancement | Effort | Value |
|---|------------|--------|-------|
| E1 | ⭐ **True multi-step ReAct loop** — After first synthesis, ask the LLM: "What gaps remain? What should I search next?" Run a second Tavily query on the gap, re-synthesize. Cap at 2 extra rounds. This earns the "ReAct" name and meaningfully improves deep research quality. | High | Very High |
| E2 | ⭐ **Streaming LLM response** — LangChain supports `.stream()`. Show the analysis appearing word-by-word instead of a 20s blank wait then a pop. Transforms the "AI is thinking" experience entirely. | Medium | High |
| E3 | **Save to Supabase** — Let users save research reports to a `web_research_history` table. Currently localStorage only — lost when browser data is cleared. | Medium | High |
| E4 | **PDF/Word export** — Print-to-PDF exists but a formatted export (like AI Scout's `.MD` download) with a proper layout and sources would be more useful. | Low | Medium |
| E5 | **Source domain filter** — Let users exclude known low-quality domains (e.g., medium.com aggregators) or only include `.gov`/`.edu`. Tavily doesn't support this natively, but post-search filtering is easy. | Low | Medium |
| E6 | **Confidence calibration feedback** — The frontend source quality score is heuristic. Let users thumbs-up/down a result. Store in Supabase. Use to weight future scoring. | Medium | High (long-run) |
| E7 | **Research session export** — Export the full multi-turn session (all queries + reports) as one document. Today each result is independent. | Low | Medium |

---

## 8. Reliability and Continuity (MUST)

**Goal: Run uninterrupted month after month.**

| Failure mode | Current behaviour | Fix |
|-------------|-----------------|-----|
| **Gemini API hangs** | Worker thread blocked indefinitely | Add 60s timeout to `ChatGoogleGenerativeAI` (T1) |
| **Tavily quota exhausted** | `RuntimeError` → 502 with raw error string | Add monthly usage counter; return friendly "Search quota reached" message |
| **Deep mode Render timeout** | Request killed at 30s | Async job pattern for `/web-research` (T8) |
| **Zero search results** | LLM receives empty context, hallucinates | Add minimum results guard (F2) |
| **LLM returns malformed JSON** | Caught, returns partial payload with sources | Good. Add one retry with "Return ONLY JSON" prompt for empty body responses |
| **Render cold start** | 10-20s delay from `moviepy`/`PIL` imports | Extract `_resolve_gemini_model()` (T6) |
| **`research_context` grows large** | Context payload grows without bound in long sessions | Already limited to `slice(-2)` — good. Document this limit. |

---

## 9. Usage Budget and Cost Control (MUST)

### Current cost breakdown (estimated monthly)

| Service | Free tier | Estimated usage | Monthly cost |
|---------|-----------|-----------------|-------------|
| **Tavily** | 1,000 searches/mo | ~100–400 searches (1–2 per session × depth, no cap) | **$0 (risk: exhausted early without a cap)** |
| **Gemini (LangChain)** | Free tier generous | ~50–200 calls/mo (full mode only; quick has no LLM) | $0–3 |
| **Render** | Free tier (750 hrs) | Shared with other tools | $0 or $7 (paid) |
| **Total** | | | **$0–10/mo** |

### The main risk: Tavily, not Gemini
AI Scout caps articles at 40 and uses `search_depth: "basic"`. This tool uses `search_depth: "advanced"` for Deep mode (counts as 2–5 Tavily credits per call on some plans) and up to 10 results. Without a rate limit, a single enthusiastic user could exhaust the 1,000 free monthly searches in a day.

### Cost control measures

| # | Measure | Status | Action |
|---|---------|--------|--------|
| C1 | **10-min Tavily cache** | DONE | Good. `/quick` + `/web-research` share the same cache key, so two-phase only calls Tavily once. |
| C2 | **Rate limiting** | NOT DONE | Add 15s per-user cooldown on both endpoints (T2). |
| C3 | **Monthly Tavily call counter** | NOT DONE | Add a counter in the cache dict or Supabase. Refuse new requests if > 800 this month with a clear message. |
| C4 | **`advanced` search only on depth=3** | DONE | `search_depth = "advanced" if depth_level == 3 else "basic"`. Good — Deep mode is explicit user choice. |
| C5 | **`max_results` scales with depth** | DONE | 3/6/10 results by depth level. |
| C6 | **Google Cloud billing alert** | NOT DONE | GCP Console → Billing → Budgets & Alerts → set $5/mo alert. LangChain uses Google AI (not Vertex by default) so billing may be on Google AI Studio quota, which is free-tier — verify. |
| C7 | **No Anthropic fallback** | ACCEPTED | Unlike AI Scout, no fallback if Gemini fails. Either add Anthropic fallback or add a clear error message: "AI provider unavailable — please try again." |

---

## Summary: Priority Action Items

### Must fix (before public release)
1. ⚠️ **XSS in `buildNewsCard()` and `openShare()`** — apply `escapeHtml()` to all LLM-generated text; `safeUrl()` to all source href attributes (Section 3)
2. ⚠️ **Add LangChain Gemini timeout** — 60s on `ChatGoogleGenerativeAI` (Section 2, T1)
3. ⚠️ **Add rate limiting** — 15s per-user cooldown on `/web-research` and `/web-research/quick` (Section 2, T2)
4. ⚠️ **Minimum results guard** — return 404 if `len(raw_results) == 0`, warn if < 3 (Section 1, F2)

### Should fix (reliability + quality)
5. **Prompt: fix `executive_brief` schema collision** (Section 6, Gap 1) — one-line change
6. **Prompt: improve `follow_up_questions` to be search queries** (Section 6, Gap 2)
7. **Extract `_resolve_gemini_model()`** from `LLM_GEMINI.py` to reduce cold start (Section 2, T6)
8. **Better Tavily cache eviction** — evict oldest 32 entries, not all 128 (Section 2, T3)

### Manual actions pending
- ⚠️ **Verify Google AI Studio vs GCP billing** — check if Gemini calls via LangChain hit the free AI Studio quota or a GCP billing account
- ⚠️ **Set monthly Tavily call limit** — add a simple counter before calling Tavily; alert at 800 calls

### To reach 9/10 (do when ready)
9. ⭐ **True multi-step ReAct loop** — two search rounds with gap-filling (Section 7, E1). Earns the "ReAct" label properly.
10. ⭐ **Streaming LLM response** — `.stream()` via LangChain (Section 7, E2). Eliminates the "blank wait" UX problem.

### Documentation
11. Update `docs/tools/ReAct_Research_Agent.md` — add failure modes table, "current architecture is a pipeline" honesty, security posture, last-updated date (Section 5)

---

## Framing note for tech showcase
- Present as: *"A grounded two-phase web research pipeline with structured output and decision-mode synthesis lenses"*
- The "ReAct" name is aspirational — own the pipeline confidently; the refine modes and citation enforcement are the genuine engineering story
- Defensible strengths: two-phase UX design, depth-aware schema, five synthesis modes, multi-turn context without sessions, source quality scoring

---

*Audit complete. No code changes were made — this document is for review and prioritisation.*
