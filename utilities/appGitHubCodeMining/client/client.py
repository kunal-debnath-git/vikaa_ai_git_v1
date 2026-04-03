"""Simple CLI client for Deep Search (GET /api/deep_search).

Usage: run while the server is up. Provides prompt and optional token.
"""
import requests
from urllib.parse import urlencode

BASE = "http://127.0.0.1:8000/api/deep_search"

prompt = input("Enter your code need: ")
github_token = input("Enter your GitHub Token (optional): ")

params = {"prompt": prompt, "top_k": 5}
if github_token.strip():
    params["github_token"] = github_token.strip()

url = f"{BASE}?{urlencode(params)}"
resp = requests.get(url, timeout=30)

if resp.ok:
    data = resp.json()
    print(f"\nAgent Plan: {data.get('agent_plan')}")
    for i, item in enumerate(data.get('results', []), 1):
        print(f"\n[{i}] {item.get('source')} • score={item.get('score')}")
        print(item.get('snippet', '')[:400])
else:
    print("❌ Error:", resp.status_code, resp.text)