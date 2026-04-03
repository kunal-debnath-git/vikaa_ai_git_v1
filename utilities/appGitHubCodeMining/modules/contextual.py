"""Context fetcher for README and commits via GitHub REST API.

Notes:
- Provide a token to avoid low rate limits for unauthenticated requests.
- Consider adding ETag caching and retry/backoff when integrating at scale.
"""
import requests

class Contextual:
    def __init__(self, github_token: str|None = None):
        self.token = github_token

    def _headers(self, raw=False):
        h = {"Accept": "application/vnd.github+json"}
        if raw:
            h["Accept"] = "application/vnd.github.v3.raw"
        if self.token:
            h["Authorization"] = f"token {self.token}"
        return h

    def readme(self, repo_full: str):
        """Return README content as text for owner/repo (or None)."""
        url = f"https://api.github.com/repos/{repo_full}/readme"
        r = requests.get(url, headers=self._headers(raw=True), timeout=20)
        return r.text if r.status_code == 200 else None

    def commits(self, repo_full: str, limit: int = 20):
        """Return up to `limit` commit messages for owner/repo (empty on error)."""
        url = f"https://api.github.com/repos/{repo_full}/commits?per_page={limit}"
        r = requests.get(url, headers=self._headers(), timeout=20)
        if r.status_code == 200:
            data = r.json()
            return [c.get('commit',{}).get('message') for c in data]
        return []
