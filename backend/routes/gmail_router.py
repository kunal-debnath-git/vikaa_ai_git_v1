"""
Gmail Intelligence Router — True MCP via Anthropic tool_use

Architecture:
  POST /tools/gmail/daily-briefing  → today's inbox ranked + draft replies
  POST /tools/gmail/weekly-report   → 7-day inbox health + insights
  GET  /tools/gmail/status          → credential check (demo vs live)

How the MCP loop works:
  1. Backend fetches emails via Gmail API (gmail_service.py)
  2. We define Gmail tools following MCP tool schema format
  3. Claude (via anthropic SDK) receives the task + tools
  4. Claude calls tools as needed (tool_use blocks)
  5. Backend executes each tool call and feeds results back
  6. Claude produces the final structured analysis
  7. Backend returns clean JSON to the frontend
"""

import os
import json
import logging
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from backend.services.gmail_service import fetch_emails, _credentials_available, list_accounts
from backend.services.access_guard import (
    is_trusted_dev_execution_context,
    require_gmail_intelligence_user,
    require_google_oauth_user,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/tools/gmail", tags=["Gmail Intelligence"])

# Local testing only: sole mailbox offered in /accounts on trusted dev. Production never lists this by default.
_LOCAL_GMAIL_TEST_ACCOUNT = "story360degree@gmail.com"


def _production_gmail_account_or_raise(session: dict, requested: str | None) -> str | None:
    """
    Production: only the signed-in Google user's mailbox, and only if a token exists for it on the server.
    """
    raw = (session.get("email") or "").strip()
    user_lower = raw.lower()
    if user_lower in ("local-anon", "local-dev", ""):
        raise HTTPException(
            status_code=403,
            detail="Gmail Intelligence requires a valid Google sign-in session.",
        )
    configured = list_accounts()
    by_lower = {a.lower(): a for a in configured}
    canonical = by_lower.get(user_lower)
    if not canonical:
        raise HTTPException(
            status_code=403,
            detail=(
                f"No Gmail API credentials are stored for {raw}. "
                "Ask an administrator to add a token for this Google account on the server, "
                "or sign in with a Google user that matches a configured mailbox."
            ),
        )
    req_part = (requested or "").strip()
    if req_part and req_part.lower() != user_lower:
        raise HTTPException(
            status_code=403,
            detail="You can only use Gmail Intelligence for the Google account you signed in with on Vikaa.AI.",
        )
    return canonical

# ── Lazy Anthropic client ──────────────────────────────────────────────────────

_anthropic_client = None

def _get_anthropic():
    global _anthropic_client
    if _anthropic_client is None:
        import anthropic
        key = os.getenv("ANTHROPIC_API_KEY", "")
        if not key or key == "your-anthropic-api-key-here":
            raise RuntimeError("ANTHROPIC_API_KEY is not configured.")
        _anthropic_client = anthropic.Anthropic(api_key=key)
    return _anthropic_client


# ── MCP-style tool definitions (Gmail tools Claude can call) ───────────────────

GMAIL_TOOLS = [
    {
        "name": "get_inbox_emails",
        "description": (
            "Retrieves emails from the Gmail inbox for a given time window. "
            "Returns subject, sender, date, and body snippet for each message."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "since_days": {
                    "type": "integer",
                    "description": "How many days back to fetch emails (1 = today, 7 = last week).",
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum number of emails to return (default 50).",
                },
            },
            "required": ["since_days"],
        },
    },
]


def _execute_tool(tool_name: str, tool_input: dict, cached_emails: dict, account: str | None = None) -> str:
    """Execute a Claude tool_use call and return result as a string."""
    if tool_name == "get_inbox_emails":
        since_days  = tool_input.get("since_days", 1)
        max_results = tool_input.get("max_results", 50)
        cache_key   = (since_days, max_results)

        if cache_key not in cached_emails:
            cached_emails[cache_key] = fetch_emails(since_days, max_results, account=account)

        emails = cached_emails[cache_key]
        return json.dumps(emails, ensure_ascii=False)

    return json.dumps({"error": f"Unknown tool: {tool_name}"})


def _run_mcp_loop(system_prompt: str, user_message: str, account: str | None = None) -> str:
    """
    Run the Anthropic tool_use agentic loop.
    Returns Claude's final text response after all tool calls are resolved.
    """
    client  = _get_anthropic()
    messages = [{"role": "user", "content": user_message}]
    cached_emails: dict = {}

    while True:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=4096,
            system=system_prompt,
            tools=GMAIL_TOOLS,
            messages=messages,
        )

        # ── Append assistant turn ──
        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            for block in response.content:
                if hasattr(block, "text"):
                    return block.text
            return ""

        if response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    logger.info(f"Claude calling tool: {block.name} with {block.input}")
                    result_str = _execute_tool(block.name, block.input, cached_emails, account)
                    tool_results.append({
                        "type":        "tool_result",
                        "tool_use_id": block.id,
                        "content":     result_str,
                    })

            messages.append({"role": "user", "content": tool_results})
            continue

        for block in response.content:
            if hasattr(block, "text"):
                return block.text
        return ""


# ── Pydantic models ────────────────────────────────────────────────────────────

class BriefingRequest(BaseModel):
    date_hint: str = ""
    account: str = ""   # Gmail account to use; empty = first available


class EmailTier(BaseModel):
    tier: str          # "act_now" | "bill_invoice" | "read_today" | "can_wait" | "spam"
    subject: str
    sender: str
    summary: str
    draft_reply: str | None = None


class DailyBriefingResponse(BaseModel):
    date: str
    total_emails: int
    demo_mode: bool
    tiers: dict[str, list[EmailTier]]   # keys: act_now, bill_invoice, read_today, can_wait, spam
    action_items: list[str]


class WeeklyReportResponse(BaseModel):
    week_ending: str
    total_emails: int
    demo_mode: bool
    health_score: int          # 0-100
    health_label: str
    unresolved_threads: int
    top_senders: list[dict]
    spam_candidates: list[str]
    action_backlog: list[str]
    insights: list[str]


# ── System prompts ─────────────────────────────────────────────────────────────

_DAILY_SYSTEM = """You are an expert email triage assistant. Your job is to fetch and analyse today's inbox.

Steps:
1. Call get_inbox_emails with since_days=1 to fetch today's emails.
2. Rank EVERY email into exactly one of these tiers:
   - act_now      : deadlines, direct asks needing a reply, urgent flags, payment/charge issues that require a response
   - bill_invoice : invoices, bills, payment due reminders, statements, receipts, order confirmations related to payment
   - read_today   : newsletters worth reading, FYIs from colleagues, useful updates
   - can_wait     : automated notifications, CC-only threads, low-signal updates
   - spam         : promotional blasts, unknown senders, unsubscribe candidates

3. For every act_now email, write a concise draft reply (2-3 sentences max).
   - For bill_invoice emails, do NOT write draft replies. Instead, write a short summary and include a corresponding action item like "Review bill/invoice: <subject>".
4. Ensure "action_items" includes one entry per act_now email (reply) and one entry per bill_invoice email (review).
5. Produce a JSON object — no markdown, no explanation, pure JSON — with this exact structure:

{
  "tiers": {
    "act_now":    [{"subject":"...","sender":"...","summary":"...","draft_reply":"..."}],
    "bill_invoice":[{"subject":"...","sender":"...","summary":"..."}],
    "read_today": [{"subject":"...","sender":"...","summary":"..."}],
    "can_wait":   [{"subject":"...","sender":"...","summary":"..."}],
    "spam":       [{"subject":"...","sender":"...","summary":"..."}]
  },
  "action_items": ["short actionable sentence 1", "..."]
}"""


_WEEKLY_SYSTEM = """You are an expert inbox analytics assistant. Your job is to analyse the last 7 days of email.

Steps:
1. Call get_inbox_emails with since_days=7 to fetch the week's emails.
2. Analyse the full dataset and return a JSON object — no markdown, pure JSON — with this exact structure:

{
  "total_emails": <total count of emails fetched>,
  "health_score": <integer 0-100 reflecting inbox hygiene and responsiveness>,
  "health_label": <"Excellent"|"Good"|"Needs Attention"|"Critical">,
  "unresolved_threads": <count of threads that need a reply from the user>,
  "top_senders": [{"name":"...","email":"...","count":<n>}],
  "spam_candidates": ["email address or domain 1", "..."],
  "action_backlog": ["overdue item 1", "..."],
  "insights": ["observation 1", "observation 2", "observation 3"]
}"""


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("/status")
def gmail_status():
    """Check whether real Gmail credentials are configured."""
    has_gmail  = _credentials_available()
    has_claude = bool(
        os.getenv("ANTHROPIC_API_KEY", "").strip() and
        os.getenv("ANTHROPIC_API_KEY") != "your-anthropic-api-key-here"
    )
    return {
        "gmail_configured":  has_gmail,
        "claude_configured": has_claude,
        "demo_mode":         not (has_gmail and has_claude),
    }


@router.get("/accounts")
async def gmail_accounts(
    request: Request,
    _user: dict = Depends(require_google_oauth_user),
):
    """
    Trusted dev: only the fixed local test mailbox (no other tokens listed).
    Production (e.g. Render): only the signed-in user's Google email, if a server token exists for it;
    otherwise an empty list and no_gmail_token_for_user for a polite UI message.
    """
    if is_trusted_dev_execution_context(request):
        return {
            "accounts": [_LOCAL_GMAIL_TEST_ACCOUNT],
            "local_default_account": _LOCAL_GMAIL_TEST_ACCOUNT,
        }

    raw = (_user.get("email") or "").strip()
    user_lower = raw.lower()
    if user_lower in ("local-anon", ""):
        raise HTTPException(
            status_code=403,
            detail=(
                "Gmail Intelligence is only available when you sign in to Vikaa.AI with Google (Gmail OAuth)."
            ),
        )

    configured = list_accounts()
    by_lower = {a.lower(): a for a in configured}
    canonical = by_lower.get(user_lower)
    if canonical:
        return {"accounts": [canonical]}

    return {
        "accounts": [],
        "no_gmail_token_for_user": True,
        "message": (
            f"There is no Gmail API token on the server for {raw}. "
            "Your Google login is valid, but this mailbox must be authorised on the backend "
            "(or use a Google account that matches a configured token). Contact your administrator if needed."
        ),
    }


@router.post("/daily-briefing", response_model=DailyBriefingResponse)
async def daily_briefing(
    request: Request,
    req: BriefingRequest,
    _acl: dict = Depends(require_gmail_intelligence_user),
):
    """
    Fetch today's inbox and return a Claude-ranked email briefing.
    Returns demo data when credentials are not configured.
    """
    demo_mode = not (_credentials_available() and
                     os.getenv("ANTHROPIC_API_KEY", "") not in ("", "your-anthropic-api-key-here"))

    account = (req.account or "").strip() or None
    if not is_trusted_dev_execution_context(request) and not demo_mode:
        account = _production_gmail_account_or_raise(_acl, account)
    today_str = req.date_hint or datetime.now(timezone.utc).strftime("%A, %d %B %Y")

    if demo_mode:
        emails = fetch_emails(since_days=1)
        raw_json = _mock_briefing_analysis(emails)
    else:
        user_msg = f"Today is {today_str}. Please fetch and triage my inbox."
        try:
            raw = _run_mcp_loop(_DAILY_SYSTEM, user_msg, account=account)
            raw_json = _extract_json(raw)
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))
        except Exception as e:
            logger.error(f"Daily briefing error: {e}")
            raise HTTPException(status_code=500, detail="Analysis failed.")

    tiers_raw = raw_json.get("tiers", {})
    tiers: dict[str, list[EmailTier]] = {}
    for tier_key in ("act_now", "bill_invoice", "read_today", "can_wait", "spam"):
        tiers[tier_key] = [
            EmailTier(
                tier=tier_key,
                subject=e.get("subject", ""),
                sender=e.get("sender", ""),
                summary=e.get("summary", ""),
                draft_reply=e.get("draft_reply"),
            )
            for e in tiers_raw.get(tier_key, [])
        ]

    total = sum(len(v) for v in tiers.values())
    return DailyBriefingResponse(
        date=today_str,
        total_emails=total,
        demo_mode=demo_mode,
        tiers=tiers,
        action_items=raw_json.get("action_items", []),
    )


@router.post("/weekly-report", response_model=WeeklyReportResponse)
async def weekly_report(
    request: Request,
    req: BriefingRequest,
    _acl: dict = Depends(require_gmail_intelligence_user),
):
    """
    Fetch the last 7 days of inbox and return a Claude-generated health report.
    Returns demo data when credentials are not configured.
    """
    demo_mode = not (_credentials_available() and
                     os.getenv("ANTHROPIC_API_KEY", "") not in ("", "your-anthropic-api-key-here"))

    week_ending = datetime.now(timezone.utc).strftime("%A, %d %B %Y")

    account = (req.account or "").strip() or None
    if not is_trusted_dev_execution_context(request) and not demo_mode:
        account = _production_gmail_account_or_raise(_acl, account)

    if demo_mode:
        emails = fetch_emails(since_days=7)
        raw_json = _mock_weekly_analysis(emails)
        total_emails = len(emails)
    else:
        user_msg = f"Week ending {week_ending}. Please analyse my inbox for the past 7 days."
        try:
            raw = _run_mcp_loop(_WEEKLY_SYSTEM, user_msg, account=account)
            raw_json = _extract_json(raw)
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))
        except Exception as e:
            logger.error(f"Weekly report error: {e}")
            raise HTTPException(status_code=500, detail="Analysis failed.")
        # Claude includes total_emails in JSON; fall back to a direct count if missing
        if not raw_json.get("total_emails"):
            raw_json["total_emails"] = len(fetch_emails(since_days=7, account=account))
        total_emails = raw_json["total_emails"]

    return WeeklyReportResponse(
        week_ending=week_ending,
        total_emails=total_emails,
        demo_mode=demo_mode,
        health_score=raw_json.get("health_score", 0),
        health_label=raw_json.get("health_label", "Unknown"),
        unresolved_threads=raw_json.get("unresolved_threads", 0),
        top_senders=raw_json.get("top_senders", []),
        spam_candidates=raw_json.get("spam_candidates", []),
        action_backlog=raw_json.get("action_backlog", []),
        insights=raw_json.get("insights", []),
    )


# ── Helpers ────────────────────────────────────────────────────────────────────

def _extract_json(text: str) -> dict:
    """Strip markdown fences and parse JSON from Claude's response."""
    clean = text.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        # Try to find the first { ... } block
        start = clean.find("{")
        end   = clean.rfind("}") + 1
        if start != -1 and end > start:
            return json.loads(clean[start:end])
        raise


def _mock_briefing_analysis(emails: list[dict]) -> dict:
    """Rule-based mock triage used in demo mode — no Claude call needed."""
    tiers: dict = {"act_now": [], "bill_invoice": [], "read_today": [], "can_wait": [], "spam": []}
    action_items = []

    keywords_bill_invoice = [
        "invoice", "bill", "billing", "statement", "receipt", "amount due",
        "payment due", "due amount", "accounts payable", "remittance",
    ]
    keywords_urgent = ["overdue", "action required", "deadline", "urgent"]
    keywords_spam   = ["sale", "off", "discount", "flash", "promo", "deals"]
    keywords_notify = ["github", "noreply", "notification", "automated"]

    for e in emails:
        text = (e["subject"] + " " + e["snippet"]).lower()
        item = {"subject": e["subject"], "sender": e["from"], "summary": e["snippet"][:120]}

        if any(k in text for k in keywords_spam):
            tiers["spam"].append(item)
        elif any(k in text for k in keywords_bill_invoice):
            tiers["bill_invoice"].append(item)
            action_items.append(f"Review bill/invoice: {e['subject']}")
        elif any(k in text for k in keywords_urgent):
            item["draft_reply"] = (
                f"Thank you for your message regarding '{e['subject']}'. "
                "I'll look into this and get back to you shortly."
            )
            tiers["act_now"].append(item)
            action_items.append(f"Reply to: {e['subject']}")
        elif any(k in text for k in keywords_notify):
            tiers["can_wait"].append(item)
        else:
            tiers["read_today"].append(item)

    return {"tiers": tiers, "action_items": action_items}


def _mock_weekly_analysis(emails: list[dict]) -> dict:
    """Rule-based mock weekly analysis used in demo mode."""
    from collections import Counter
    senders = Counter(e["from"] for e in emails)
    top_senders = [
        {"name": addr.split("<")[0].strip() or addr, "email": addr, "count": cnt}
        for addr, cnt in senders.most_common(5)
    ]
    spam_domains = [e["from"] for e in emails if any(
        k in (e["subject"] + e["snippet"]).lower()
        for k in ["sale", "off", "discount", "deals", "promo"]
    )]
    return {
        "health_score": 68,
        "health_label": "Needs Attention",
        "unresolved_threads": 2,
        "top_senders": top_senders,
        "spam_candidates": spam_domains,
        "action_backlog": [
            "Reply to invoice payment request from accounts@supplier.com",
            "Complete Q1 self-assessment by Friday EOD",
        ],
        "insights": [
            f"You received {len(emails)} emails this week.",
            "2 emails require immediate action.",
            "Consider unsubscribing from promotional senders to reduce noise.",
        ],
    }
