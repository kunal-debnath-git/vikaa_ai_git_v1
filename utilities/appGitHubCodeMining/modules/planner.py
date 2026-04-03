# ╔══════════════════════════════════════════════════════════════════╗
# ║  VIKAA.AI — PUBLIC SHOWCASE                                      ║
# ║  This file requires environment credentials to run.              ║
# ║  Architecture and API signatures are shown for reference.        ║
# ║  See README.md and docs/ for setup guide.                        ║
# ╚══════════════════════════════════════════════════════════════════╝

"""Minimal agent planner.

Decides which search strategies to run based on prompt intent heuristics.
Future: use LLM or a learned policy; consider user-set preferences.
"""

class Planner:
    """Return a list of steps like ['repo_search','semantic','structural','contextual']."""
    def __init__(self):
        ...

    def decide(self, prompt: str):
        """Heuristic planner: include contextual when prompt hints at docs/commits."""
        ...
