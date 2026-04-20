#!/usr/bin/env python3
"""Live validation for issue #43 — provider HTTP failure → stable API response.

Exercises the full API → job_runner → provider → httpx chain against a fake
LinkedIn served by httpx.MockTransport. Uses a real httpx.Client so the
provider's retry loop runs against actual HTTP response parsing.
Exit 0 iff all scenarios produce the intended responses.
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import httpx


def _setup_paths() -> None:
    root = Path(__file__).resolve().parent.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def _make_client() -> tuple[object, int]:
    from fastapi.testclient import TestClient
    from libs.core.storage import Storage
    from apps.api.main import app
    import apps.api.main as api_mod

    tmp = tempfile.mkdtemp()
    db_path = Path(tmp) / "validate.sqlite"
    os.environ["DESEARCH_DB_PATH"] = str(db_path)
    storage = Storage(db_path=db_path)
    storage.migrate()
    api_mod.storage = storage
    client = TestClient(app)
    resp = client.post(
        "/accounts",
        json={"label": "validate", "li_at": "AQEDAWx0Y29va2llXXX", "jsessionid": "ajax:csrf123"},
    )
    assert resp.status_code == 200, f"account creation failed: {resp.status_code} {resp.text}"
    return client, resp.json()["account_id"]


_REAL_CLIENT = httpx.Client


def _client_factory_with_transport(transport: httpx.MockTransport):
    def _factory(*args, **kwargs):
        kwargs["transport"] = transport
        kwargs.pop("proxy", None)
        return _REAL_CLIENT(*args, **kwargs)
    return _factory


def _profile_ok_response() -> httpx.Response:
    return httpx.Response(
        200,
        json={"entityUrn": "urn:li:fsd_profile:ABC123"},
        headers={"content-type": "application/json"},
    )


def scenario_sync_redirect_302(client, account_id: int) -> tuple[bool, str]:
    def handler(request: httpx.Request) -> httpx.Response:
        if "/voyager/api/me" in str(request.url):
            return _profile_ok_response()
        return httpx.Response(302, headers={"Location": "https://www.linkedin.com/login"})

    transport = httpx.MockTransport(handler)
    with patch(
        "libs.providers.linkedin.provider.httpx.Client",
        side_effect=_client_factory_with_transport(transport),
    ), patch("libs.providers.linkedin.provider.time.sleep"), patch(
        "libs.providers.linkedin.provider.LinkedInProvider._harvest_and_cache_cookies",
        return_value={},
    ):
        resp = client.post("/sync", json={"account_id": account_id})
    ok = resp.status_code == 401
    return ok, f"status={resp.status_code} detail={resp.json().get('detail')}"


def scenario_sync_expired_session_on_me(client, account_id: int) -> tuple[bool, str]:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"Location": "https://www.linkedin.com/login"})

    transport = httpx.MockTransport(handler)
    with patch(
        "libs.providers.linkedin.provider.httpx.Client",
        side_effect=_client_factory_with_transport(transport),
    ), patch("libs.providers.linkedin.provider.time.sleep"):
        resp = client.post("/sync", json={"account_id": account_id})
    ok = resp.status_code == 401
    return ok, f"status={resp.status_code} detail={resp.json().get('detail')}"


def scenario_send_repeated_429(client, account_id: int) -> tuple[bool, str]:
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        return httpx.Response(429)

    transport = httpx.MockTransport(handler)
    with patch(
        "libs.providers.linkedin.provider.httpx.Client",
        side_effect=_client_factory_with_transport(transport),
    ), patch("libs.providers.linkedin.provider.time.sleep"):
        resp = client.post(
            "/send",
            json={"account_id": account_id, "recipient": "urn:li:fs_miniProfile:abc", "text": "hi"},
        )
    ok = resp.status_code == 429 and call_count["n"] == 6
    return ok, (
        f"status={resp.status_code} detail={resp.json().get('detail')} "
        f"post_calls={call_count['n']}"
    )


def scenario_sync_network_error(client, account_id: int) -> tuple[bool, str]:
    def handler(request: httpx.Request) -> httpx.Response:
        if "/voyager/api/me" in str(request.url):
            return _profile_ok_response()
        raise httpx.ConnectError("network unreachable")

    transport = httpx.MockTransport(handler)
    with patch(
        "libs.providers.linkedin.provider.httpx.Client",
        side_effect=_client_factory_with_transport(transport),
    ), patch("libs.providers.linkedin.provider.time.sleep"):
        resp = client.post("/sync", json={"account_id": account_id})
    ok = resp.status_code == 502
    return ok, f"status={resp.status_code} detail={resp.json().get('detail')}"


def main() -> int:
    _setup_paths()
    client, account_id = _make_client()
    scenarios = [
        ("sync: LinkedIn 302 on GraphQL (post-profile) → 401", 401, scenario_sync_redirect_302),
        ("sync: expired session, 302 on /me (real session expiry) → 401", 401, scenario_sync_expired_session_on_me),
        ("send: repeated 429 rate-limit → 429", 429, scenario_send_repeated_429),
        ("sync: network ConnectError → 502", 502, scenario_sync_network_error),
    ]
    print("=" * 70)
    print("Issue #43 live validation (httpx.MockTransport against real httpx.Client)")
    print("=" * 70)
    failed = 0
    for name, expected, fn in scenarios:
        ok, detail = fn(client, account_id)
        status = "PASS" if ok else "FAIL"
        print(f"\n[{status}] {name}")
        print(f"        expected_status={expected}")
        print(f"        {detail}")
        if not ok:
            failed += 1
    print("\n" + "=" * 70)
    if failed:
        print(f"FAIL: {failed}/{len(scenarios)} scenarios did not produce the intended response")
        return 1
    print(f"PASS: all {len(scenarios)} scenarios produced the intended API responses")
    return 0


if __name__ == "__main__":
    sys.exit(main())
