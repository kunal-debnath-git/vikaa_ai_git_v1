# Contextual Search Module (Repo and file context fetcher)

import requests

class ContextualFetcher:
    def __init__(self, github_token):
        self.github_token = github_token

    def fetch_readme(self, repo_full_name):
        url = f"https://api.github.com/repos/{repo_full_name}/readme"
        headers = {"Authorization": f"token {self.github_token}", "Accept": "application/vnd.github.v3.raw"}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.text
        return "No README found."

    def fetch_commit_messages(self, repo_full_name):
        url = f"https://api.github.com/repos/{repo_full_name}/commits"
        headers = {"Authorization": f"token {self.github_token}"}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            commits = response.json()
            return [commit['commit']['message'] for commit in commits]
        return []

# Example Usage
if __name__ == "__main__":
    fetcher = ContextualFetcher("your-github-token")
    print("README:", fetcher.fetch_readme("psf/requests"))
    print("Commits:", fetcher.fetch_commit_messages("psf/requests"))