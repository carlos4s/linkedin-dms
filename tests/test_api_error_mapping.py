"""Tests for provider error mapping in /sync and /send endpoints."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest
from fastapi.testclient import TestClient

from libs.core import crypto


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DESEARCH_DB_PATH", str(tmp_path / "test.sqlite"))
    monkeypatch.delenv("DESEARCH_ENCRYPTION_KEY", raising=False)
    crypto._warned_no_key = False


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DESEARCH_DB_PATH", str(tmp_path / "test.sqlite"))
    from libs.core.storage import Storage

    storage = Storage(db_path=tmp_path / "test.sqlite")
    storage.migrate()

    from apps.api.main import app
    import apps.api.main as api_mod

    original_storage = api_mod.storage
    api_mod.storage = storage
    yield TestClient(app)
    api_mod.storage = original_storage
    storage.close()


def _create_account(client) -> int:
    resp = client.post(
        "/accounts",
        json={"label": "test", "li_at": "AQEDAWx0Y29va2llXXX"},
    )
    assert resp.status_code == 200
    return resp.json()["account_id"]


def _make_http_status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://www.linkedin.com/api")
    response = httpx.Response(status_code=status_code, request=request)
    return httpx.HTTPStatusError(
        str(status_code), request=request, response=response,
    )


@patch("apps.api.main.run_sync")
def test_sync_redirect_302_returns_401(mock_run_sync, client):
    account_id = _create_account(client)
    mock_run_sync.side_effect = PermissionError("LinkedIn redirected to login (HTTP 302)")
    resp = client.post("/sync", json={"account_id": account_id})
    assert resp.status_code == 401
    assert "re-authenticate" in resp.json()["detail"].lower()


@patch("apps.api.main.run_sync")
def test_sync_redirect_303_returns_401(mock_run_sync, client):
    account_id = _create_account(client)
    mock_run_sync.side_effect = PermissionError("LinkedIn redirected to login (HTTP 303)")
    resp = client.post("/sync", json={"account_id": account_id})
    assert resp.status_code == 401
    assert "re-authenticate" in resp.json()["detail"].lower()


@patch("apps.api.main.run_send")
def test_send_redirect_302_returns_401(mock_run_send, client):
    account_id = _create_account(client)
    mock_run_send.side_effect = PermissionError("LinkedIn redirected to login (HTTP 302)")
    resp = client.post(
        "/send",
        json={"account_id": account_id, "recipient": "urn:li:fs_miniProfile:abc", "text": "hi"},
    )
    assert resp.status_code == 401
    assert "re-authenticate" in resp.json()["detail"].lower()


@patch("apps.api.main.run_send")
def test_send_redirect_303_returns_401(mock_run_send, client):
    account_id = _create_account(client)
    mock_run_send.side_effect = PermissionError("LinkedIn redirected to login (HTTP 303)")
    resp = client.post(
        "/send",
        json={"account_id": account_id, "recipient": "urn:li:fs_miniProfile:abc", "text": "hi"},
    )
    assert resp.status_code == 401
    assert "re-authenticate" in resp.json()["detail"].lower()


@patch("apps.api.main.run_send")
def test_send_rate_limit_429_returns_429(mock_run_send, client):
    account_id = _create_account(client)
    mock_run_send.side_effect = _make_http_status_error(429)
    resp = client.post(
        "/send",
        json={"account_id": account_id, "recipient": "urn:li:fs_miniProfile:abc", "text": "hi"},
    )
    assert resp.status_code == 429
    assert "rate limit" in resp.json()["detail"].lower()


@patch("apps.api.main.run_send")
def test_send_rate_limit_999_returns_429(mock_run_send, client):
    account_id = _create_account(client)
    mock_run_send.side_effect = _make_http_status_error(999)
    resp = client.post(
        "/send",
        json={"account_id": account_id, "recipient": "urn:li:fs_miniProfile:abc", "text": "hi"},
    )
    assert resp.status_code == 429
    assert "rate limit" in resp.json()["detail"].lower()


def _mock_httpx_response(status_code: int) -> httpx.Response:
    request = httpx.Request("GET", "https://www.linkedin.com/voyager/api/test")
    return httpx.Response(status_code=status_code, request=request)


def _create_account_with_jsessionid(client) -> int:
    resp = client.post(
        "/accounts",
        json={"label": "test", "li_at": "AQEDAWx0Y29va2llXXX", "jsessionid": "ajax:csrf123"},
    )
    assert resp.status_code == 200
    return resp.json()["account_id"]


@patch("libs.providers.linkedin.provider.LinkedInProvider._get_profile_id", return_value="urn:li:fsd_profile:ABC")
@patch("libs.providers.linkedin.provider.LinkedInProvider._harvest_and_cache_cookies", return_value={})
@patch("libs.providers.linkedin.provider.time.sleep")
@patch("libs.providers.linkedin.provider.httpx.Client")
def test_sync_linkedin_redirect_302_end_to_end(MockClient, _sleep, _harvest, _profile, client):
    mock_client = MagicMock()
    mock_client.is_closed = False
    mock_client.get.return_value = _mock_httpx_response(302)
    MockClient.return_value = mock_client

    account_id = _create_account_with_jsessionid(client)
    resp = client.post("/sync", json={"account_id": account_id})

    assert resp.status_code == 401
    assert "re-authenticate" in resp.json()["detail"].lower()


@patch("libs.providers.linkedin.provider.time")
@patch("libs.providers.linkedin.provider.httpx.Client")
def test_send_repeated_429_end_to_end(MockClient, mock_time, client):
    mock_time.monotonic.return_value = 1000.0
    post_client = MagicMock()
    post_client.post.return_value = _mock_httpx_response(429)
    MockClient.return_value.__enter__ = MagicMock(return_value=post_client)
    MockClient.return_value.__exit__ = MagicMock(return_value=False)

    account_id = _create_account_with_jsessionid(client)
    resp = client.post(
        "/send",
        json={"account_id": account_id, "recipient": "urn:li:fs_miniProfile:abc", "text": "hi"},
    )

    assert resp.status_code == 429
    assert "rate limit" in resp.json()["detail"].lower()
    assert post_client.post.call_count == 6
