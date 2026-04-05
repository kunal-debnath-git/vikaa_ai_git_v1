# License & API Key Management — Vikaa.AI Platform

> **PROMPT FOR NEXT USE:**
> Claude, read `.env` and `etc/secrets/` to verify keys are still current.
> Check: (1) Any new keys added since last update? (2) Any expired tokens (GitHub PAT ~90d, LinkedIn ~60d, Databricks)?
> Update this file if anything changed. Cross-reference with sections below.


## 1. API Keys — What They Are & What They Do

### AI / LLM Models

| Key | Used By | Purpose |
|-----|---------|---------|
| `ANTHROPIC_API_KEY` | Claude API (Anthropic) | Core LLM — AI Scout, ReAct Research Agent, agentic pipelines |
| `OPENAI_API_KEY` | OpenAI GPT-4 | LLM fallback + embeddings (`EMBEDDING_PROVIDER=gpt`). GPT-4 is the default model |
| `GEMINI_API_KEY` / `GOOGLE_API_KEY` | Google Gemini 1.5 Flash | Alternative LLM, Google AI features. Created from **Story360 Google account** |

### Search & Web Retrieval

| Key | Used By | Purpose |
|-----|---------|---------|
| `SERPAPI_API_KEY` / `SERP_API_KEY` | SerpAPI | Web search results for AI agents & research pipelines |
| `TAVILY_API_KEY` | Tavily | AI-native web search for ReAct agent / agentic RAG. Dev plan key (`tvly-dev-...`) |

### Vector Database

| Key | Used By | Purpose |
|-----|---------|---------|
| `PINECONE_API_KEY` | Pinecone | Vector store for embeddings. Index: `chat-with-files` |

### Databases & Storage

| Key | Used By | Purpose |
|-----|---------|---------|
| `SUPABASE_URL` + `SUPABASE_SERVICE_ROLE_KEY` + `SUPABASE_ANON_KEY` | Supabase | Primary RDBMS (PostgreSQL). Service role = admin access; Anon = public/frontend |
| `JWT_SECRET` | Supabase | JWT signing secret for auth tokens |
| `MONGO_URI` / `MONGODB_ATLAS_URI` | MongoDB Atlas | Vector chunks + logs. DB: `agentic_rag`, Cluster: `ClusterAI` |
| `MONGODB_ATLAS_PUBLIC_KEY` + `MONGODB_ATLAS_PRIVATE_KEY` | MongoDB Atlas Admin API | Cluster management (not query access — this is the Atlas API key pair) |

### Data Platform

| Key | Account | Purpose |
|-----|---------|---------|
| `DATABRICKS_TOKEN_STORY` | Story360 Databricks workspace | PySpark notebooks, data pipelines. Host: `dbc-08c4f29e-4559` |
| `DATABRICKS_TOKEN_ANJALI` | Anjali's Databricks workspace | Secondary/backup data workspace |

### Social / Communication APIs

| Key | Used By | Purpose |
|-----|---------|---------|
| `TWILIO_ACCOUNT_SID` + `TWILIO_AUTH_TOKEN` | Twilio | SMS & WhatsApp messaging. Sandbox WhatsApp: `+14155238886`, SMS: `+16196483671` |
| `TWITTER/X API_KEY`, `API_KEY_SECRET`, `ACCESS_TOKEN`, `BEARER_TOKEN` | Twitter/X API v2 | Post tweets, read timeline. Handle: `@KunalDebIND` |
| `REDDIT_CLIENT_ID` + `REDDIT_CLIENT_SECRET` | Reddit PRAW | Reddit read/post. Account: `PlaneAct4865` |
| `LINKEDIN_ACCESS_TOKEN` + `LINKEDIN_REFRESH_TOKEN` | LinkedIn API | Post to LinkedIn. URN: `uu50n7Oq-3` (Story360 / dutta.cloud.engineering@outlook.com) |

### Developer & Infrastructure

| Key | Used By | Purpose |
|-----|---------|---------|
| `GITHUB_TOKEN` | GitHub REST API | Repo access, automation, code operations |
| `LangFLow_API_KEY` | LangFlow | LangFlow platform API access (self-hosted or cloud) |
| `RECAPTCHA_SECRET_KEY` | Google reCAPTCHA | Contact Us form bot protection |
| `SMTP_HOST` + `SMTP_USER` + `SMTP_PASS` | Hostinger SMTP | Outbound email — Contact form + Agent notifications. Host: `smtp.hostinger.com:587` |


## 2. Paid vs Free Quota

| Service | Billing Model | Notes |
|---------|--------------|-------|
| **Anthropic (Claude)** | **PAID — wallet balance required** | Pay per token. No free tier for API. Keep $5+ in wallet |
| **OpenAI (GPT-4)** | **PAID — wallet balance required** | GPT-4 is expensive (~$0.03/1K tokens). Also used for embeddings |
| **Google Gemini 1.5 Flash** | **Free + Paid** | Generous free tier (15 RPM, 1M TPM). Paid beyond limits |
| **Pinecone** | **Free + Paid** | Starter (free): 1 index, 100K vectors. Paid if scaling beyond |
| **MongoDB Atlas** | **Free + Paid** | M0 free tier (512MB). Paid for production scale |
| **Supabase** | **Free + Paid** | Free: 500MB DB, 1GB storage, 50K MAU. Paid beyond |
| **SerpAPI** | **Free + Paid** | Free: 100 searches/month. Paid beyond that |
| **Tavily** | **Free (dev plan) + Paid** | Dev key (`tvly-dev-...`) has limited credits. Upgrade for production |
| **Twilio** | **PAID — wallet balance required** | Pay per SMS/WhatsApp message. No meaningful free tier |
| **Twitter/X API** | **Free Basic + Paid** | Free: 1,500 tweets/month write. Paid for more |
| **Reddit API** | **Free** | Personal use, rate-limited. No wallet needed |
| **LinkedIn API** | **Free (limited)** | Community tier free. Paid for marketing APIs |
| **GitHub** | **Free** | PAT-based, no cost for current usage |
| **Databricks** | **PAID** | Cloud compute cost on the Databricks workspace account |
| **Hostinger SMTP** | **PAID** | Part of hosting plan |
| **LangFlow** | **Free** (if self-hosted) | No extra cost if running locally |
| **Google reCAPTCHA** | **Free** | v2/v3 free for standard usage |


## 3. Services Requiring Wallet Balance (Top Priority)

- **Anthropic** — Claude API calls. Keep minimum **$5** balance. Monitor at `console.anthropic.com`
- **OpenAI** — GPT-4 calls + embeddings. Keep **$10+** balance. Monitor at `platform.openai.com`
- **Twilio** — SMS + WhatsApp. Pay-as-you-go. Monitor at `console.twilio.com`
- **Databricks** — Cloud compute charges billed to the workspace account. Monitor usage in Databricks UI
- **Hostinger** — Annual hosting renewal covers SMTP + domain email


## 4. Token & JSON Files (OAuth / Service Accounts)

| File / Variable | Type | Purpose |
|----------------|------|---------|
| `etc/secrets/gemini-service-account.json` | Google Service Account JSON | Auth for Google Cloud / Gemini API in server environments. Path set via `GOOGLE_APPLICATION_CREDENTIALS` |
| `etc/secrets/credentials.json` | Gmail OAuth2 Client Credentials | Enables Gmail API access (read/send mail). Used by Gmail Assistant feature. Path: `GMAIL_OAUTH_CREDENTIALS_PATH` |
| `SUPABASE_SERVICE_ROLE_KEY` | JWT Token | Supabase admin JWT. Encoded. Expiry: **~2035** (long-lived, check `exp` field) |
| `SUPABASE_ANON_KEY` | JWT Token | Supabase public/anon JWT. Same long expiry ~2035 |
| `JWT_SECRET` | Signing Secret | Used to sign/verify app-level JWTs for Supabase auth |
| `LINKEDIN_ACCESS_TOKEN` | OAuth2 Bearer Token | LinkedIn posting. **Expires ~60 days** — needs manual refresh |
| `LINKEDIN_REFRESH_TOKEN` | OAuth2 Refresh Token | LinkedIn refresh. **Expires ~12 months** |

> Note: Gmail OAuth will generate a local `token.json` or `token.pickle` file at runtime after first auth. This is not committed to git.


## 5. Keys Requiring Periodic Maintenance (Rotation / Renewal)

| Key / Token | Expiry / Rotation | Action Required |
|-------------|------------------|-----------------|
| `GITHUB_TOKEN` | ~90 days (PAT classic) | Regenerate at `github.com > Settings > Developer Settings > PAT`. Update `.env` |
| `LINKEDIN_ACCESS_TOKEN` | ~60 days | Re-authorize LinkedIn OAuth flow. Update `LINKEDIN_ACCESS_TOKEN` in `.env` |
| `LINKEDIN_REFRESH_TOKEN` | ~12 months | When access token refresh fails — full re-auth needed |
| `DATABRICKS_TOKEN_STORY` | Depends on workspace policy (usually 90d) | Regenerate in Databricks UI > User Settings > Access Tokens |
| `DATABRICKS_TOKEN_ANJALI` | Same as above | Same process on Anjali's workspace |
| `Gmail OAuth credentials.json` | Client credentials are stable, but `token.json` (runtime) can expire | Re-run OAuth flow if Gmail stops working. `credentials.json` itself is long-lived |
| `GOOGLE_APPLICATION_CREDENTIALS` (service account JSON) | Does not expire | But if key is revoked/rotated in GCP console, download new JSON |
| `TWILIO_AUTH_TOKEN` | No auto-expiry | Rotate manually if compromised. Do in Twilio Console |
| `OPENAI_API_KEY` | No auto-expiry | Rotate if leaked. Monitor usage for unexpected charges |
| `ANTHROPIC_API_KEY` | No auto-expiry | Rotate if leaked. Monitor wallet balance regularly |
| `PINECONE_API_KEY` | No auto-expiry | Rotate if leaked in Pinecone console |


## 6. Account Summary — Where Each Key Lives

| Service | Account / Login | Console URL |
|---------|----------------|-------------|
| Anthropic | `kunal.debnath@vikaa.ai` | console.anthropic.com |
| OpenAI | Check OpenAI account | platform.openai.com |
| Google / Gemini | Story360 Google account | console.cloud.google.com |
| GitHub | `kunal-debnath-git` | github.com/settings/tokens |
| Pinecone | Check Pinecone account | app.pinecone.io |
| Supabase | Check account | app.supabase.com |
| MongoDB Atlas | `duttacloudengineering` | cloud.mongodb.com |
| Databricks | Story360 / Anjali accounts | As per workspace URLs in `.env` |
| Twilio | Check Twilio account | console.twilio.com |
| Twitter/X | `@KunalDebIND` | developer.twitter.com |
| Reddit | `PlaneAct4865` | reddit.com/prefs/apps |
| LinkedIn | `dutta.cloud.engineering@outlook.com` | linkedin.com/developers |
| Hostinger | `kunal.debnath@vikaa.ai` | hpanel.hostinger.com |
| SerpAPI | Check account | serpapi.com/dashboard |
| Tavily | Check account | app.tavily.com |


*Last updated: 2026-04-04 | Based on `.env` audit by Claude Code*
