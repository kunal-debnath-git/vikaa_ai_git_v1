# Vikaa Chat Tool — Audit & Action Plan

**Tool:** `vikaa_ai_agent.html` + `vikaa_ai_agent.js` + `backend/routes/agent_router.py` + `models/LLM_LangChain.py` + `models/LLM_GEMINI.py`  
**Audit date:** 2026-04-04  
**Auditor:** Claude Code  
**Status:** Functional prototype — production-ready after fixing items marked CRITICAL and HIGH

---

## 1. Functionality — Is it doing justice?

### What it does (correctly)
- Multi-session chat UI with sidebar history grouped by Today / Yesterday / This Week / Earlier
- Messages routed to Gemini via FastAPI with Bearer token auth
- Attachment support: images (vision), PDF, CSV, XLSX, JSON, XML, DOCX, PPTX, audio (Whisper), video (frame extraction), ZIP archives
- YouTube URL detection → special handler
- Voice input (SpeechRecognition API)
- Camera snapshot (getUserMedia)
- Download/copy agent responses (auto-detects JSON, CSV, XML, base64 binary)
- Temperature control: Creative (0.9) / Balanced (0.6) / Precise (0.2)
- Access control: ACL-gated via Supabase whitelist; read-only vs execute mode
- Session uptime timer in sidebar

### Is it doing justice? **YES — for a general-purpose chat agent, the feature set is strong.**

### What it should also do (gaps)
| Gap | Impact |
|-----|--------|
| No system prompt sent to Gemini | Responses have no persona, no brand tone, no guardrails |
| GPT and Auto-Pilot model options are fake (locked + placeholder) | Misleads users; should either implement or remove |
| No loading / typing indicator while waiting for Gemini | Users wait in silence — feels broken, especially on Render cold starts |
| Chat history is browser-only (localStorage) | Lost on device switch, cleared cache, or incognito |
| No suggested prompts / quick-start hints on new chat | Empty chat page is not welcoming |

### Suggested improvements
1. **Add a system prompt** — give the agent a name, role, and tone (see Section 6)
2. Kunal >>> Lets keep this for now... We will do it for Next Version. Leave a remarks to provision this later >>> **Remove or implement GPT / Auto-Pilot** — do not show locked options in a live tool
3. **Add a typing indicator** — 3-dot animated bubble while awaiting response
4. **Persist sessions to Supabase** — so users retain history across devices and restarts
5. **Add 3–5 suggested starter prompts** — shown on empty new chat screen

---

## 2. Technical Flaws & Loose Ends

### BUG (Real code defect)

#### `clearMessages()` defined twice in vikaa_ai_agent.js
```js
// Line 154
function clearMessages() { document.getElementById("messages").innerHTML = ""; }
// Line 175 — DUPLICATE, overwrites silently
function clearMessages() { document.getElementById("messages").innerHTML = ""; }
```
JavaScript hoists the second definition. In this case both are identical so it doesn't break — but it's a code smell and will cause confusion during future edits. **Remove the duplicate (line 175).**

#### `InMemoryHistoryManager` defined in two places
- `LLM_LangChain.py` line 30: creates `history_manager` (used by `invoke_langchain()`)
- `LLM_GEMINI.py` line 33: creates a separate `history_manager` (used only by `analyze_video_frames()`)

These are **two separate in-memory stores**. Video frame analysis sessions are tracked independently from conversation sessions. This means a user's conversation context is invisible to frame analysis and vice versa. Not a crash bug but a logical inconsistency — video analysis results don't feed into the conversation memory. **Consolidate: import `history_manager` from `LLM_LangChain` into `LLM_GEMINI`.**

#### Attachment size check is AFTER processing, not before
```python
# LLM_LangChain.py line 76–130
for att in attachments:        # ← full decoding happens here
    decoded_bytes = b64decode(...)
    ...handle_pdf / handle_video / etc...

if total_attachment_bytes > MAX_COMBINED_ATTACHMENT_MB:   # ← check AFTER all processing
    return "⚠️ exceeded..."
```
A 30 MB video will be fully decoded and processed before the limit is enforced. **Move the size check to before the loop.**

### Technical loose ends

| Issue | File | Risk |
|-------|------|------|
| No timeout on `llm.invoke()` call | `LLM_GEMINI.py:154` | Gemini hang → Render 30s HTTP timeout → silent failure to user |
| `window.onload` (line 818) AND `window.addEventListener("load")` (line 106) both set | `vikaa_ai_agent.js` | Both fire at load — works but is redundant and confusing; consolidate into one |
| `URL.createObjectURL(blob)` in `addDownloadIfNeeded()` never revoked unless download is clicked | `vikaa_ai_agent.js:775` | Minor memory leak per agent response |
| `lucide.createIcons()` called at script parse time (line 381) before `defer` completes for all scripts | `vikaa_ai_agent.js` | Safe in practice but should be inside DOMContentLoaded |
| Session ID generated with `Math.random()` | `vikaa_ai_agent.js:148` | Weak, predictable — not cryptographically random |
| No validation that `session.messages` items have `text` field before rendering | `vikaa_ai_agent.js:453` | Stale/corrupt localStorage entry → TypeError crash |

---

## 3. Security Audit

### CRITICAL

#### XSS via `marked.parse()` → `innerHTML`
```js
// vikaa_ai_agent.js line 317
const formattedText = marked.parse(text);
inner = `<strong>${sender}:</strong><div class="agent-response">${formattedText}</div>`;
// ...
div.innerHTML = inner;   // ← direct assignment
```
If the LLM ever produces `<script>alert(1)</script>` or a `<img onerror=...>` in its response, it will execute. `marked` does **not** sanitize HTML by default — it passes raw HTML through unchanged.

**Fix:** Add [DOMPurify](https://github.com/cure53/DOMPurify) before setting innerHTML:
```js
div.innerHTML = DOMPurify.sanitize(inner);
```

#### Auth token in `localStorage`
```js
const raw = localStorage.getItem("authData");  // Supabase JWT here
```
`localStorage` is accessible to any JavaScript on the page. An XSS attack can exfiltrate the token. httpOnly cookies are the correct solution; localStorage is the acceptable fallback only if XSS is fully mitigated (which it is not currently — see above).

### HIGH

| Issue | Detail |
|-------|--------|
| No rate limiting on `POST /agent/message` | Any whitelisted user can spam requests → unbounded Gemini cost |
| Supabase URL hardcoded in `doLogout()` (line 462) | Should come from `config.js` or env variable |
| Client-generated session ID (`Math.random`) | Use `crypto.randomUUID()` — available in all modern browsers |
| No MIME type validation on attachments server-side | Backend trusts filename extension; a renamed `.exe` sent as `.pdf` will fail gracefully but could be exploited in future |

### MEDIUM

| Issue | Detail |
|-------|--------|
| User message set via `innerHTML` with `isHtml=true` | Attachment preview (`<img src="...">`) goes through `innerHTML` without sanitization — only safe because dataUrls are self-generated |
| `contextmenu` and `Ctrl+U/S` disabled | Minor UX friction for legitimate users; does not prevent View Source — remove or be transparent about intent |
| Alert used for access denied response | `alert()` is blocking and poor UX; replace with inline error message |

---

## 4. Render.com — Is it safe and ready for public release?

### Ready
- `render.yaml` correctly configured (uvicorn, PORT env var, build.sh)
- Health endpoint exists at `/health`
- CORS allows production domain
- Supabase JWT validation gates execution

### NOT ready — fix before public release

#### No async job pattern → Render 30s timeout risk
`POST /agent/message` is a **synchronous blocking call** to Gemini. A complex query with a PDF attachment can easily take 15–40 seconds. Render free tier kills HTTP requests at 30 seconds.

**Required fix:** Follow the AI Scout pattern:
- `POST /agent/message` → starts background task, returns `{ job_id }` immediately (< 500ms)
- `GET /agent/message/status/{job_id}` → returns `pending` | `done: { response }` | `error`
- Frontend polls every 2–3 seconds (as AI Scout does)

This is the single most important deployment fix.

#### No rate limiting
Add 30-second per-user cooldown on `/agent/message` (same as AI Scout's `/digest` endpoint). Use in-memory dict keyed by `user_id` from validated JWT.

#### `healthCheckPath` missing from `render.yaml`
```yaml
# Add this to render.yaml service definition:
healthCheckPath: /health
```
Without this, Render cannot auto-restart the service on hang.

#### In-memory chat history resets on every deploy/restart
Render restarts on every deploy. All `InMemoryHistoryManager` sessions are lost. Add Supabase persistence for conversation threads.

---

## 5. Tool Documentation
See `docs/tools/vikaa-chat-tool.md` — created as part of this audit.

---

## 6. Prompt — Current State & Improvements

### Current state
There is **no system prompt**. `handle_gemini()` in `LLM_GEMINI.py` sends either:
- Raw `memory.chat_memory.messages` (text mode), or
- A `HumanMessage` with image payloads + query text (vision mode)

The LLM has no instructions about who it is, how to behave, or what domain it operates in.

### Recommended system prompt
Add as the first message in `handle_gemini()` before invoking the model:

```python
VIKAA_SYSTEM_PROMPT = """You are Vikaa.AI, a smart, professional, and concise AI assistant.

Guidelines:
- Be direct and structured. Use bullet points and markdown for clarity.
- For data files (CSV, Excel, JSON): always start with a schema summary, then insights.
- For documents (PDF, DOCX, PPTX): start with a 2-line summary, then key points.
- For images/camera snapshots: describe what you see, then answer the user's question.
- For code: explain what it does first, then suggest improvements if asked.
- For YouTube videos: give title, content type (song/lecture/tutorial), then answer.
- If you cannot determine the answer from context, say so clearly — do not hallucinate.
- Keep responses under 400 words unless the user asks for more detail.
- End analytical responses with: "Need deeper analysis? Just ask."
"""
```

This prompt:
- Establishes brand identity
- Sets output format expectations per attachment type
- Controls response length (cost control)
- Prevents hallucination in ambiguous cases
- Adds a natural conversation hook at the end

### Prompt improvements for specific modes

| Mode | Current | Improved |
|------|---------|---------|
| Precise (temp=0.2) | Same prompt as Balanced | Add: "Respond with structured output. If data analysis, output clean markdown tables." |
| Creative (temp=0.9) | Same prompt as Balanced | Add: "Be imaginative and explore multiple angles. Use analogies." |
| Vision/Camera | No guidance | Add: "The user has shared an image. Describe it briefly first, then address their question." |
| YouTube | Hardcoded 6 categories | Extend to: song, lecture, tutorial, documentary, vlog, interview, podcast, cooking, fitness, other |

---

## 7. Enhancement Suggestions for Long-Term Value

| # | Enhancement | Why | Effort |
|---|-------------|-----|--------|
| 1 | **Async job pattern** (POST → poll) | Fixes Render 30s timeout; essential for production | Medium |
| 2 | **System prompt** | Gives agent persona, controls output quality and format | Low |
| 3 | **Typing indicator** | User feedback during 5–30s wait | Low |
| 4 | **Supabase chat persistence** | Survive restarts, multi-device access | Medium |
| 5 | **Suggested starter prompts** | Improves onboarding and engagement | Low |
| 6 | **Claude Haiku fallback** | If Gemini fails or quota exceeded, fall to Haiku | Medium |
| 7 | **Token budget warning** | Warn user before large document is sent ("This PDF will use ~50K tokens") | Medium |
| 8 | **Response regeneration** | "Regenerate" button on agent bubble | Low |
| 9 | **Conversation export to PDF** | Currently only Markdown download | Medium |
| 10 | **Tool routing** | Detect intent: if query is about stocks/news → route to AI Scout; if about data → suggest SQL Agent | High |

---

## 8. Reliability & Continuity — Month After Month

### Risks to uptime

| Risk | Probability | Mitigation |
|------|-------------|------------|
| Render free tier cold start (50s spin-up) | High | Keep-alive ping every 10 min (free UptimeRobot / BetterStack) |
| Gemini API model deprecated | Medium | `gemini_resolver.py` already handles this — probes model list in order |
| Supabase JWT validation fails (rate limit) | Low | Cache validation result per token for 5 min in-memory |
| In-memory history lost on restart | High | Persist to Supabase `agent_sessions` table |
| Render deployment wipes sessions | Certain on every deploy | Same fix as above |
| `marked.js` CDN outage | Low | Pin version; consider self-hosting or fallback |
| Font Awesome / Google Fonts CDN outage | Low | Add `font-display: swap` and local fallback font stack |

### Keep-alive strategy (zero cost)
Use [UptimeRobot](https://uptimerobot.com) free tier:
- Monitor `https://app-wtiw.onrender.com/health` every 5 minutes
- This prevents Render free tier from sleeping after 15 minutes of inactivity
- Set alert to your email if health check fails

### Deployment stability checklist
- [ ] Pin all dependency versions in `requirements.txt` (no `>=` for core packages)
- [ ] Test `build.sh` locally before every deploy
- [ ] Add Render deploy hook to notify via email/Slack on failure
- [ ] Never use `--no-verify` on commits

---

## 9. Usage Budget — Google Cloud / Gemini Cost Control

### Current cost exposure (no controls in place)

| Scenario | Tokens consumed | Daily cost est. |
|----------|----------------|-----------------|
| 1 user, 50 text queries, avg 500 tokens each | 25K tokens | ~$0.002 (free tier) |
| 1 user, 5 PDF uploads (10 pages each) | ~200K tokens | ~$0.015 |
| 1 user, 2 video uploads (5 frames each × Gemini Vision) | 10 Gemini calls | ~$0.05 |
| 10 users, mixed usage above | ~500K tokens | ~$0.05–0.15 |

Gemini free tier: **1 million tokens/day** (Flash). You're safe at low user count.

### What breaks the budget
1. **No rate limiting** — a single user could loop 1,000 calls in an hour
2. **Video frame analysis** = 5 separate Gemini Vision calls per video upload
3. **No caching** — same question answered fresh every time

### Controls to implement (in priority order)

#### Rate limit per user (HIGH — do first)
```python
# agent_router.py — add in-memory rate limiter
_user_last_call: dict[str, float] = {}
COOLDOWN_SECONDS = 30

def check_rate_limit(user_id: str):
    now = time.time()
    last = _user_last_call.get(user_id, 0)
    if now - last < COOLDOWN_SECONDS:
        raise HTTPException(429, f"Please wait {int(COOLDOWN_SECONDS - (now-last))}s before sending again.")
    _user_last_call[user_id] = now
```

#### Query length limit (LOW effort, HIGH protection)
```python
MAX_QUERY_CHARS = 4000
if len(agent_request.query) > MAX_QUERY_CHARS:
    return f"⚠️ Query too long ({len(agent_request.query)} chars). Please keep it under {MAX_QUERY_CHARS} characters."
```

#### Attachment size check BEFORE processing (already flagged as bug)
Prevents decoding 30MB video just to reject it.

#### Supabase JWT validation caching (MEDIUM)
Each message currently calls Supabase REST to validate the JWT. Cache per token for 5 minutes:
```python
_token_cache: dict[str, tuple[dict, float]] = {}
CACHE_TTL = 300  # seconds
```

#### Video frame limit warning
Before processing video, estimate cost and warn the user via a UI message. Keep max frames at 5 (already set in code — good).

#### Monthly budget alarm
Set a Google Cloud budget alert at $5/month on the Gemini API key. You will be notified by email if approaching limit, giving time to react before it costs money.

### Cost summary with controls in place

| Control | Monthly saving est. |
|---------|-------------------|
| Rate limit (30s cooldown) | Prevents runaway; saves 80%+ in abuse scenarios |
| Query length cap | Saves ~10–20% on token cost |
| JWT caching | Saves Supabase quota, not direct money |
| Budget alert | Safety net only |

**Realistic monthly cost with controls and ~10 active users: $0–$3/month.** Free tier handles this comfortably.

---

## Summary — Action Priority Matrix

| Priority | Item | Files affected |
|----------|------|----------------|
| CRITICAL | Fix XSS: add DOMPurify before `innerHTML` | `vikaa_ai_agent.js` |
| CRITICAL | Add async job pattern (`job_id` polling) | `agent_router.py`, `vikaa_ai_agent.js` |
| CRITICAL | Add `healthCheckPath: /health` to render.yaml | `render.yaml` |
| HIGH | Add system prompt to `handle_gemini()` | `LLM_GEMINI.py` |
| HIGH | Add rate limiting to `/agent/message` | `agent_router.py` |
| HIGH | Add query length cap (4000 chars) | `agent_router.py` |
| HIGH | Move attachment size check BEFORE processing loop | `LLM_LangChain.py` |
| HIGH | Add typing indicator in UI | `vikaa_ai_agent.js` |
| HIGH | Set up UptimeRobot keep-alive ping | External (free) |
| MEDIUM | Add Gemini call timeout (25s via asyncio) | `LLM_GEMINI.py` |
| MEDIUM | Add DOMPurify CDN to HTML | `vikaa_ai_agent.html` |
| MEDIUM | Cache Supabase JWT validation (5 min TTL) | `access_guard.py` |
| MEDIUM | Persist chat history to Supabase | `agent_router.py`, `LLM_LangChain.py` |
| MEDIUM | Remove duplicate `clearMessages()` function | `vikaa_ai_agent.js` |
| MEDIUM | Consolidate `InMemoryHistoryManager` to one instance | `LLM_GEMINI.py` |
| MEDIUM | Replace `Math.random()` session ID with `crypto.randomUUID()` | `vikaa_ai_agent.js` |
| MEDIUM | Remove/implement GPT + Auto-Pilot model options | `vikaa_ai_agent.html` |
| LOW | Replace `alert()` for access denied with inline message | `vikaa_ai_agent.js` |
| LOW | Add Google Cloud budget alert at $5/month | Google Cloud console |
| LOW | Add suggested starter prompts on new chat | `vikaa_ai_agent.js` |
| LOW | Add "Regenerate" button on agent responses | `vikaa_ai_agent.js` |
