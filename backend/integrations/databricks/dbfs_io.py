"""
Write/read small files on DBFS / Unity Catalog volume paths via the Databricks REST API.

- Unity Catalog volumes: ``PUT/GET /api/2.0/fs/files{path}`` (path like ``/Volumes/cat/sch/vol/file.json``).
  ``/api/2.0/dbfs/put`` often does *not* work for ``/Volumes/...``; use Files API first.
- Classic DBFS (e.g. ``/FileStore/...``): ``POST /api/2.0/dbfs/put`` and ``dbfs/read``.

Same workspace PAT as UC / SQL.
"""

from __future__ import annotations

import base64
import json
from typing import Any
from urllib.parse import quote

import requests

from backend.integrations.databricks.read_unity_catalog import _normalize_host

_DBFS_PUT_MAX_BYTES = 1024 * 1024
_DBFS_READ_CHUNK = 1024 * 1024


def dbfs_uri_to_api_path(dbfs_uri: str) -> str:
    """dbfs:/Volumes/.../file -> /Volumes/.../file"""
    p = (dbfs_uri or "").strip()
    if not p.startswith("dbfs:/"):
        raise ValueError("Expected dbfs:/… URI")
    p = p.replace("dbfs:/", "/", 1)
    if not p.startswith("/"):
        p = "/" + p
    return p


def _fs_files_url(base: str, api_path: str, *, query: str = "") -> str:
    """Build /api/2.0/fs/files + percent-encoded absolute path."""
    enc = quote(api_path, safe="/")
    q = f"?{query}" if query else ""
    return f"{base.rstrip('/')}/api/2.0/fs/files{enc}{q}"


def _put_uc_volume_file(host: str, token: str, api_path: str, data: bytes) -> None:
    base = _normalize_host(host).rstrip("/")
    url = _fs_files_url(base, api_path, query="overwrite=true")
    r = requests.put(
        url,
        headers={
            "Authorization": f"Bearer {token.strip()}",
            "Content-Type": "application/json; charset=utf-8",
        },
        data=data,
        timeout=120,
    )
    if not r.ok:
        raise RuntimeError(f"fs/files PUT HTTP {r.status_code}: {r.text[:800]}")


def _get_uc_volume_file(host: str, token: str, api_path: str) -> bytes:
    base = _normalize_host(host).rstrip("/")
    url = _fs_files_url(base, api_path)
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {token.strip()}"},
        timeout=120,
    )
    if not r.ok:
        raise RuntimeError(f"fs/files GET HTTP {r.status_code}: {r.text[:800]}")
    return r.content


def _dbfs_put_bytes(host: str, token: str, api_path: str, data: bytes) -> None:
    base = _normalize_host(host).rstrip("/")
    headers = {
        "Authorization": f"Bearer {token.strip()}",
        "Content-Type": "application/json",
    }
    b64 = base64.standard_b64encode(data).decode("ascii")
    r = requests.post(
        f"{base}/api/2.0/dbfs/put",
        headers=headers,
        json={"path": api_path, "contents": b64, "overwrite": True},
        timeout=120,
    )
    if not r.ok:
        raise RuntimeError(f"dbfs/put HTTP {r.status_code}: {r.text[:800]}")


def write_json_to_dbfs_uri(
    host: str,
    token: str,
    dbfs_uri: str,
    payload: dict[str, Any],
    *,
    indent: int = 2,
) -> None:
    text = json.dumps(payload, indent=indent, default=str, ensure_ascii=False)
    data = text.encode("utf-8")
    if len(data) > _DBFS_PUT_MAX_BYTES:
        raise ValueError(
            f"JSON size {len(data)} exceeds single-request limit ({_DBFS_PUT_MAX_BYTES})."
        )
    api_path = dbfs_uri_to_api_path(dbfs_uri)
    if api_path.startswith("/Volumes/"):
        try:
            _put_uc_volume_file(host, token, api_path, data)
            return
        except RuntimeError as exc_uc:
            try:
                _dbfs_put_bytes(host, token, api_path, data)
                return
            except RuntimeError as exc_dbfs:
                raise RuntimeError(
                    f"UC volume write failed — fs/files: {exc_uc}; dbfs/put: {exc_dbfs}"
                ) from exc_dbfs
    _dbfs_put_bytes(host, token, api_path, data)


def read_json_from_dbfs_uri(host: str, token: str, dbfs_uri: str) -> dict[str, Any]:
    raw = read_bytes_from_dbfs_uri(host, token, dbfs_uri)
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Summary file is not a JSON object.")
    return data


def read_bytes_from_dbfs_uri(host: str, token: str, dbfs_uri: str) -> bytes:
    api_path = dbfs_uri_to_api_path(dbfs_uri)
    if api_path.startswith("/Volumes/"):
        try:
            return _get_uc_volume_file(host, token, api_path)
        except RuntimeError as exc:
            try:
                return _dbfs_read_all_chunks(host, token, api_path)
            except RuntimeError as exc2:
                raise RuntimeError(
                    f"fs/files GET failed ({exc}); dbfs/read fallback also failed ({exc2})."
                ) from exc2
    return _dbfs_read_all_chunks(host, token, api_path)


def _dbfs_read_all_chunks(host: str, token: str, api_path: str) -> bytes:
    base = _normalize_host(host).rstrip("/")
    headers = {
        "Authorization": f"Bearer {token.strip()}",
        "Content-Type": "application/json",
    }
    out = bytearray()
    offset = 0
    while True:
        r = requests.post(
            f"{base}/api/2.0/dbfs/read",
            headers=headers,
            json={"path": api_path, "offset": offset, "length": _DBFS_READ_CHUNK},
            timeout=120,
        )
        if not r.ok:
            raise RuntimeError(f"dbfs/read HTTP {r.status_code}: {r.text[:800]}")
        body = r.json()
        chunk_b64 = body.get("data") or ""
        if chunk_b64:
            out.extend(base64.standard_b64decode(chunk_b64))
        br = int(body.get("bytes_read") or 0)
        if br < _DBFS_READ_CHUNK:
            break
        offset += br
    return bytes(out)
