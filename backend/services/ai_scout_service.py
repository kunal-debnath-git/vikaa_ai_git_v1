# ╔══════════════════════════════════════════════════════════════════╗
# ║  VIKAA.AI — PUBLIC SHOWCASE                                      ║
# ║  This file requires environment credentials to run.              ║
# ║  Architecture and API signatures are shown for reference.        ║
# ║  See README.md and docs/ for setup guide.                        ║
# ╚══════════════════════════════════════════════════════════════════╝

"""
AI Scout Service
================
1. Build context-aware Tavily search queries (broad for "All Enterprise", focused for a specific company)
2. Aggregate & deduplicate results
3. Send to Gemini 2.5 Pro (Anthropic fallback) for structured digest generation
4. Return a DigestResult dataclass

Period:
  weekly  → days=7
  monthly → days=30
"""

import os
import json
import html
import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# ── Load company config from JSON (single source of truth) ────────────────────
def _load_company_config() -> dict:
    ...


def _get_config() -> dict:
    """Always read fresh from ai_scout_companies.json — no restart needed after edits."""
    ...


# ── Public accessors used by the router ───────────────────────────────────────
def get_companies_list() -> list[dict]:
    """Return the companies list for the dropdown endpoint."""
    ...


def _build_queries(company_context: str) -> list[str]:
    """
    Return Tavily queries for the selected company context.

    Uses the 'focused' flag in ai_scout_companies.json:
    - focused=false  → base + broad queries (full landscape sweep)
    - focused=true   → ONLY that company's queries (no dilution from unrelated news)
    - Unknown value  → falls back to broad sweep
    """
    ...


# ── Output schema ──────────────────────────────────────────────────────────────

@dataclass
class PlayerMove:
    player: str
    headline: str
    detail: str
    source_url: str = ""
    # Strategic = direct competitive/platform impact; Tactical = product/pricing/partnership;
    # Market context = macro/regulatory/segment shift that still changes decisions for {company_context}
    materiality: str = ""
    angle_for_focus: str = ""


@dataclass
class NewEntrant:
    name: str
    what_they_do: str
    flag: str  # "Opportunity" | "Threat" | "Watch"


@dataclass
class MonetizationItem:
    opportunity: str
    how_to_act: str
    who_benefits: str
    why_now: str = ""
    caveat: str = ""


@dataclass
class LinkedInPack:
    hook: str = ""
    body_paragraphs: list[str] = field(default_factory=list)
    insight_line: str = ""
    cta: str = ""
    hashtags: list[str] = field(default_factory=list)


@dataclass
class AgentIdea:
    title: str
    what_it_does: str
    tech_stack: list[str]
    why_leadership_cares: str
    build_time: str
    best_for: str


@dataclass
class SignalOfWeek:
    headline: str
    what: str
    why_it_matters: str
    your_move: str
    source_url: str = ""


@dataclass
class DigestResult:
    period: str
    generated_at: str
    signal_of_week: SignalOfWeek
    player_moves: list[PlayerMove]
    new_entrants: list[NewEntrant]
    monetization_radar: list[MonetizationItem]
    agent_idea: AgentIdea
    linkedin_draft: str
    technologist_edge: str
    technologist_edge_bullets: list[str] = field(default_factory=list)
    linkedin_pack: LinkedInPack | None = None
    model_used: str = "Unknown"
    sources: list[dict] = field(default_factory=list)


# ── Tavily search ──────────────────────────────────────────────────────────────

def _tavily_search(query: str, days: int, max_results: int = 6) -> list[dict]:
    ...


def _gather_news(days: int, company_context: str = "") -> list[dict]:
    """Run context-aware search queries and return deduplicated articles."""
    ...


# ── Claude analysis ────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """You are a senior AI practice lead and strategy analyst (Big-4 / cloud SI calibre).
Your audience: experienced architects, tech leads, and senior consultants — not beginners.

Writing quality bar:
- Specific, decision-useful, and sourced to the provided articles only.
- No hype, no filler, no platitudes (avoid phrases like "excited to share", "game-changer", "in today's world").
- Prefer concrete nouns (products, benchmarks, pricing motions, partnerships, regulation) over vague trend talk.
- Write in a confident first-person practitioner voice — as if you are the analyst, not an AI summarising.

Return ONLY valid JSON — no markdown, no explanation, no code fences.

The Agent Idea section is CRITICAL: it does NOT need to be a novel idea.
It can be a PROVEN AI use case reimplemented using THIS PERIOD's newest models/APIs/frameworks.
The value is execution clarity — not novelty theater.

URL rule (strict): source_url values MUST be copied character-for-character from the article list.
If no matching URL exists for a claim, use an empty string "". Never fabricate or guess a URL."""

_USER_TEMPLATE = """Today's date: {today}
Period: {period} (last {days} days)
Focus company / lens: {company_context}

{focus_instruction}

News articles gathered ({count} total):
{articles_text}

Produce ONE JSON object with this exact structure (keys and nesting must match):
{{
  "signal_of_week": {{
    "headline": "Max 12-word punchy headline",
    "what": "1-2 sentences: what happened (fact-dense)",
    "why_it_matters": "2-3 sentences: practitioner impact specifically for {company_context}",
    "your_move": "1 sentence: concrete next action for someone accountable to outcomes",
    "source_url": "URL copied verbatim from a provided article"
  }},
  "player_moves": [
    {{
      "player": "Company or institution name",
      "materiality": "Strategic|Tactical|Market context",
      "headline": "One-line factual headline of the move",
      "angle_for_focus": "One sentence: why this matters for {company_context} specifically (not generic industry)",
      "detail": "2-3 sentences: mechanics + implication; name the artifact (product tier, release, policy, deal type) when known from sources",
      "source_url": "URL from provided articles"
    }}
  ],
  "new_entrants": [
    {{"name": "Company/model", "what_they_do": "1 sentence", "flag": "Opportunity|Threat|Watch"}}
  ],
  "monetization_radar": [
    {{
      "opportunity": "Short title (outcome-oriented)",
      "why_now": "2-3 sentences: market signal + why buyers/users care this period (grounded in sources)",
      "how_to_act": "4-6 sentences: a concrete playbook — offerings, delivery pattern, pricing/packaging angle, and 1-2 proof points tied to {company_context}",
      "who_benefits": "Primary buyer/user persona + secondary beneficiary",
      "caveat": "1-2 sentences: main risk, dependency, or 'what would invalidate this read'"
    }}
  ],
  "agent_idea": {{
    "title": "Agent name",
    "what_it_does": "2-3 sentences describing the agent built using {company_context} technology",
    "tech_stack": ["<primary {company_context} tool or API>", "<supporting tool>"],
    "why_leadership_cares": "2 sentences: KPI / risk / cost lever",
    "build_time": "e.g. 5-10 days for a solo developer",
    "best_for": "Teams using {company_context}"
  }},
  "linkedin_pack": {{
    "hook": "First 1-2 lines: professional hook with a sharp point of view (no emoji spam, no clichés)",
    "body_paragraphs": ["<paragraph 1 — evidence-backed, ~60 words>", "<paragraph 2>", "<optional paragraph 3>"],
    "insight_line": "One line: the single takeaway you want the reader to remember",
    "cta": "One line CTA: comment, follow-up question, or poll-style ask (professional)",
    "hashtags": ["#Tag1", "#Tag2", "#Tag3"]
  }},
  "technologist_edge_bullets": [
    "<one crisp line: concrete advantage, threat, capability gap, or architecture implication for {company_context}>",
    "<second bullet — use a strong verb; no filler>"
  ]
}}

Rules (strict):
- Every factual claim must be traceable to the provided articles; prefer paraphrase + the provided URL on player_moves and signal_of_week.
- player_moves: include 3-6 items MAX. Exclude low-signal PR unless it changes a buyer/procurement/capability decision for {company_context}.
  Each row must pass the "so what for {company_context}?" test via angle_for_focus.
  Use materiality honestly:
    Strategic = directly affects {company_context}'s core platform/product competitive position.
    Tactical = pricing/packaging/partnership/release that changes near-term execution.
    Market context = macro/regulatory/supply/investment shift that should change prioritisation.
- monetization_radar: 2-4 items. how_to_act MUST read like a consulting-style mini-playbook (not a bullet list inside the string is OK).
- linkedin_pack: must sound like a senior practitioner or practice lead — structured narrative (hook → evidence-backed paragraphs → one-line insight → CTA → hashtags). Avoid generic AI-bro tone.
- technologist_edge_bullets: minimum 2 bullets unless sources truly support only one; never pad with speculation.
- new_entrants: only include when genuinely new/emerging this period per sources.
- agent_idea tech_stack must use {company_context} tools/APIs as primary building blocks.
"""


def _build_prompt(articles: list[dict], period: str, days: int, company_context: str) -> str:
    ...


def _clean_json(raw: str) -> dict:
    """Strip markdown fences and parse JSON."""
    ...


def _linkedin_pack_from_dict(d: dict | None) -> LinkedInPack | None:
    ...


def _linkedin_full_text(pack: LinkedInPack) -> str:
    ...


def _resolve_linkedin(raw: dict) -> tuple[str, LinkedInPack | None]:
    ...


def _resolve_technologist(raw: dict) -> tuple[list[str], str]:
    ...


def _run_gemini(prompt: str) -> tuple[dict, str]:
    """Call Gemini 2.5 Pro via REST API. Returns (parsed_dict, model_name)."""
    ...


def _run_anthropic(prompt: str) -> tuple[dict, str]:
    """Try Anthropic Claude. Returns (parsed_dict, model_name)."""
    ...


def _run_llm(articles: list[dict], period: str, days: int, company_context: str) -> tuple[dict, str]:
    """
    Run LLM analysis with Gemini as primary, Anthropic as fallback.
    Returns (result_dict, model_used_name).
    """
    ...


# ── Public entry point ─────────────────────────────────────────────────────────

def generate_digest(period: str = "weekly", company_context: str = "Any enterprise") -> DigestResult:
    """
    Generate an AI Scout digest.

    Args:
        period: "weekly" (7 days) or "monthly" (30 days)
        company_context: Target company for the Agent Idea section
    """
    ...


def digest_from_api_dict(d: dict) -> DigestResult:
    """Rebuild DigestResult from JSON (e.g. email resend payload)."""
    ...


# ── Email delivery ─────────────────────────────────────────────────────────────

def send_digest_email(digest: DigestResult, to_email: str) -> None:
    """Send digest as formatted HTML email."""
    ...


# ── Markdown export ────────────────────────────────────────────────────────────

def digest_to_markdown(digest: DigestResult) -> str:
    """Convert a DigestResult to a Markdown string for download."""
    ...
