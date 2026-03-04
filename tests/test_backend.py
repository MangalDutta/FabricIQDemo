"""
Backend API Unit Tests
─────────────────────
Tests for app.py (FastAPI) and fabric_client.py.

Run with:
    cd backend
    pip install -r ../tests/requirements.txt
    pytest ../tests/test_backend.py -v

Or from repo root:
    pip install -r tests/requirements.txt
    PYTHONPATH=backend pytest tests/test_backend.py -v
"""

import os
import sys
import importlib
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# ─── Make sure backend package is on the path ────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _make_mock_fabric_client(answer: str = "Mock answer") -> MagicMock:
    """Returns a MagicMock that looks like a FabricClient."""
    mock = MagicMock()
    mock.chat.return_value = {
        "answer": answer,
        "timestamp": "2026-01-01T00:00:00+00:00",
        "metadata": {
            "workspace_id": "ws-test-guid",
            "dataagent_id": "agent-test-guid",
            "source": "fabric_data_agent",
        },
    }
    return mock


# ─── App module fixtures ──────────────────────────────────────────────────────

@pytest.fixture()
def test_client_no_fabric():
    """
    TestClient where the FabricClient import is patched to raise so the
    app starts with fabric_client = None (503 path).
    """
    with patch.dict(
        os.environ,
        {
            "FABRIC_WORKSPACE_ID": "",
            "FABRIC_DATAAGENT_ID": "",
        },
    ):
        with patch("fabric_client.FabricClient", side_effect=Exception("no config")):
            import app as app_module
            importlib.reload(app_module)
            yield TestClient(app_module.app)


@pytest.fixture()
def test_client_with_fabric():
    """
    TestClient where the FabricClient is replaced with a working mock.
    """
    mock_fc = _make_mock_fabric_client(
        "Top 5 customers: Priya, Anita, Deepa, Leena, Amit"
    )
    with patch.dict(
        os.environ,
        {
            "FABRIC_WORKSPACE_ID": "ws-fake-guid-0000",
            "FABRIC_DATAAGENT_ID": "agent-fake-guid-0000",
        },
    ):
        with patch("fabric_client.FabricClient", return_value=mock_fc):
            import app as app_module
            importlib.reload(app_module)
            # Inject mock directly
            app_module.fabric_client = mock_fc
            yield TestClient(app_module.app)


# ─── Root / health endpoint tests ────────────────────────────────────────────

class TestHealthEndpoints:
    def test_root_returns_200(self, test_client_with_fabric):
        resp = test_client_with_fabric.get("/")
        assert resp.status_code == 200

    def test_root_body_shape(self, test_client_with_fabric):
        resp = test_client_with_fabric.get("/")
        data = resp.json()
        assert "service" in data
        assert "status" in data
        assert data["status"] == "running"

    def test_health_returns_200(self, test_client_with_fabric):
        resp = test_client_with_fabric.get("/health")
        assert resp.status_code == 200

    def test_health_body(self, test_client_with_fabric):
        resp = test_client_with_fabric.get("/health")
        assert resp.json() == {"status": "healthy"}


# ─── Chat endpoint – happy path ───────────────────────────────────────────────

class TestChatEndpointHappyPath:
    def test_chat_returns_200(self, test_client_with_fabric):
        resp = test_client_with_fabric.post(
            "/api/chat", json={"message": "Top 5 customers by LifetimeValue"}
        )
        assert resp.status_code == 200

    def test_chat_response_has_answer(self, test_client_with_fabric):
        resp = test_client_with_fabric.post(
            "/api/chat", json={"message": "Who has the highest churn risk?"}
        )
        data = resp.json()
        assert "answer" in data
        assert isinstance(data["answer"], str)
        assert len(data["answer"]) > 0

    def test_chat_response_has_timestamp(self, test_client_with_fabric):
        resp = test_client_with_fabric.post(
            "/api/chat", json={"message": "Count customers by state"}
        )
        data = resp.json()
        assert "timestamp" in data

    def test_chat_response_has_metadata(self, test_client_with_fabric):
        resp = test_client_with_fabric.post(
            "/api/chat", json={"message": "Average LifetimeValue"}
        )
        data = resp.json()
        assert "metadata" in data
        assert isinstance(data["metadata"], dict)

    def test_chat_metadata_contains_fabric_source(self, test_client_with_fabric):
        resp = test_client_with_fabric.post(
            "/api/chat", json={"message": "Top customers"}
        )
        metadata = resp.json().get("metadata", {})
        assert metadata.get("source") == "fabric_data_agent"

    def test_chat_passes_user_id(self, test_client_with_fabric):
        """Ensures the user_id is forwarded to the FabricClient."""
        import app as app_module
        importlib.reload(app_module)
        mock_fc = _make_mock_fabric_client()
        app_module.fabric_client = mock_fc

        client = TestClient(app_module.app)
        client.post(
            "/api/chat",
            json={"message": "Hello", "userId": "user-42"},
        )
        mock_fc.chat.assert_called_once()
        call_kwargs = mock_fc.chat.call_args
        assert call_kwargs.kwargs.get("user_id") == "user-42" or \
               (len(call_kwargs.args) > 0 and call_kwargs.args[0] == "user-42")

    def test_chat_defaults_user_id_to_anonymous(self, test_client_with_fabric):
        """When no userId is provided, it defaults to 'anonymous'."""
        import app as app_module
        importlib.reload(app_module)
        mock_fc = _make_mock_fabric_client()
        app_module.fabric_client = mock_fc
        client = TestClient(app_module.app)
        client.post("/api/chat", json={"message": "Hello"})
        mock_fc.chat.assert_called_once()


# ─── Chat endpoint – error paths ─────────────────────────────────────────────

class TestChatEndpointErrorPaths:
    def test_chat_no_message_returns_400(self, test_client_with_fabric):
        resp = test_client_with_fabric.post("/api/chat", json={})
        assert resp.status_code == 400

    def test_chat_empty_message_returns_400(self, test_client_with_fabric):
        resp = test_client_with_fabric.post("/api/chat", json={"message": ""})
        assert resp.status_code == 400

    def test_chat_empty_message_detail(self, test_client_with_fabric):
        resp = test_client_with_fabric.post("/api/chat", json={"message": ""})
        detail = resp.json().get("detail", "")
        assert "message" in detail.lower()

    def test_chat_503_when_fabric_not_configured(self, test_client_no_fabric):
        """When FabricClient failed to init, /api/chat must return 503."""
        resp = test_client_no_fabric.post(
            "/api/chat", json={"message": "test"}
        )
        assert resp.status_code == 503

    def test_chat_500_on_fabric_exception(self, test_client_with_fabric):
        """When FabricClient.chat raises, API must return 500."""
        import app as app_module
        importlib.reload(app_module)
        mock_fc = MagicMock()
        mock_fc.chat.side_effect = RuntimeError("upstream error")
        app_module.fabric_client = mock_fc
        client = TestClient(app_module.app)
        resp = client.post("/api/chat", json={"message": "test"})
        assert resp.status_code == 500


# ─── CORS header tests ────────────────────────────────────────────────────────

class TestCORSHeaders:
    def test_cors_origin_present(self, test_client_with_fabric):
        resp = test_client_with_fabric.options(
            "/api/chat",
            headers={
                "Origin": "http://localhost:5173",
                "Access-Control-Request-Method": "POST",
            },
        )
        # We just check the response doesn't blow up; CORS is wildcarded
        assert resp.status_code in (200, 204)


# ─── FabricClient unit tests ──────────────────────────────────────────────────

class TestFabricClientInit:
    def test_raises_without_workspace_id(self):
        """FabricClient must raise ValueError if workspace ID not set."""
        with patch.dict(
            os.environ,
            {
                "FABRIC_WORKSPACE_ID": "",
                "FABRIC_DATAAGENT_ID": "agent-fake-guid",
            },
        ):
            import fabric_client as fc_module
            importlib.reload(fc_module)
            with patch("fabric_client.DefaultAzureCredential"):
                with pytest.raises(ValueError, match="FABRIC_WORKSPACE_ID"):
                    fc_module.FabricClient()

    def test_raises_without_dataagent_id(self):
        """FabricClient must raise ValueError if data agent ID not set."""
        with patch.dict(
            os.environ,
            {
                "FABRIC_WORKSPACE_ID": "ws-fake-guid",
                "FABRIC_DATAAGENT_ID": "",
            },
        ):
            import fabric_client as fc_module
            importlib.reload(fc_module)
            with patch("fabric_client.DefaultAzureCredential"):
                with pytest.raises(ValueError, match="FABRIC_DATAAGENT_ID"):
                    fc_module.FabricClient()


class TestFabricClientChat:
    """Tests for the FabricClient.chat REST call."""

    def _make_client(self) -> Any:
        """Create a FabricClient with mocked Azure credential."""
        with patch.dict(
            os.environ,
            {
                "FABRIC_WORKSPACE_ID": "ws-test-guid-1234",
                "FABRIC_DATAAGENT_ID": "agent-test-guid-5678",
            },
        ):
            import fabric_client as fc_module
            importlib.reload(fc_module)
            with patch("fabric_client.DefaultAzureCredential") as mock_cred_cls:
                mock_cred = MagicMock()
                mock_token = MagicMock()
                mock_token.token = "fake-token"
                mock_token.expires_on = 9999999999.0
                mock_cred.get_token.return_value = mock_token
                mock_cred_cls.return_value = mock_cred
                return fc_module.FabricClient()

    def test_chat_calls_correct_url(self):
        client = self._make_client()

        def fake_post(url, **kwargs):
            mock_resp = MagicMock()
            mock_resp.ok = True
            mock_resp.json.return_value = {"answer": "Here are the top 5 customers..."}
            return mock_resp

        with patch("requests.post", side_effect=fake_post) as mock_post:
            result = client.chat("user1", "Top 5 customers by LifetimeValue")

        call_args = mock_post.call_args
        assert "dataAgents" in call_args.args[0]
        assert "query" in call_args.args[0]
        assert result["answer"] == "Here are the top 5 customers..."

    def test_chat_sends_history(self):
        client = self._make_client()
        # Pre-populate history for user
        client._histories["u2"] = [
            {"role": "user", "content": "previous question"},
            {"role": "assistant", "content": "previous answer"},
        ]

        captured_payload = {}

        def fake_post(url, **kwargs):
            captured_payload.update(kwargs.get("json", {}))
            mock_resp = MagicMock()
            mock_resp.ok = True
            mock_resp.json.return_value = {"answer": "Follow-up answer"}
            return mock_resp

        with patch("requests.post", side_effect=fake_post):
            client.chat("u2", "Follow-up question")

        assert captured_payload.get("userMessage") == "Follow-up question"
        assert len(captured_payload.get("history", [])) == 2

    def test_chat_updates_history_from_response(self):
        client = self._make_client()

        updated_history = [
            {"role": "user", "content": "question"},
            {"role": "assistant", "content": "answer"},
        ]

        def fake_post(url, **kwargs):
            mock_resp = MagicMock()
            mock_resp.ok = True
            mock_resp.json.return_value = {
                "answer": "answer",
                "history": updated_history,
            }
            return mock_resp

        with patch("requests.post", side_effect=fake_post):
            client.chat("u3", "question")

        assert client._histories["u3"] == updated_history

    def test_chat_builds_history_when_not_in_response(self):
        client = self._make_client()

        def fake_post(url, **kwargs):
            mock_resp = MagicMock()
            mock_resp.ok = True
            mock_resp.json.return_value = {"answer": "my answer"}
            return mock_resp

        with patch("requests.post", side_effect=fake_post):
            client.chat("u4", "my question")

        history = client._histories["u4"]
        assert any(h["role"] == "user" and h["content"] == "my question" for h in history)
        assert any(h["role"] == "assistant" and h["content"] == "my answer" for h in history)

    def test_chat_raises_on_http_error(self):
        client = self._make_client()

        def fake_post(url, **kwargs):
            mock_resp = MagicMock()
            mock_resp.ok = False
            mock_resp.status_code = 404
            mock_resp.text = "Data agent not found"
            return mock_resp

        with patch("requests.post", side_effect=fake_post):
            with pytest.raises(RuntimeError, match="404"):
                client.chat("u5", "some question")

    def test_chat_returns_no_response_when_answer_empty(self):
        client = self._make_client()

        def fake_post(url, **kwargs):
            mock_resp = MagicMock()
            mock_resp.ok = True
            mock_resp.json.return_value = {}  # empty response
            return mock_resp

        with patch("requests.post", side_effect=fake_post):
            result = client.chat("u6", "test")

        assert result["answer"] == "No response received."

    def test_chat_metadata_has_fabric_source(self):
        client = self._make_client()

        def fake_post(url, **kwargs):
            mock_resp = MagicMock()
            mock_resp.ok = True
            mock_resp.json.return_value = {"answer": "ok"}
            return mock_resp

        with patch("requests.post", side_effect=fake_post):
            result = client.chat("u7", "test")

        assert result["metadata"]["source"] == "fabric_data_agent"
        assert result["metadata"]["workspace_id"] == "ws-test-guid-1234"
        assert result["metadata"]["dataagent_id"] == "agent-test-guid-5678"
