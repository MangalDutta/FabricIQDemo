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


# ─── Config endpoint tests ────────────────────────────────────────────────────

class TestConfigEndpoint:
    def test_config_returns_200(self, test_client_with_fabric):
        resp = test_client_with_fabric.get("/api/config")
        assert resp.status_code == 200

    def test_config_returns_powerbi_url(self, test_client_with_fabric):
        resp = test_client_with_fabric.get("/api/config")
        data = resp.json()
        assert "powerbi_report_url" in data
        assert isinstance(data["powerbi_report_url"], str)

    def test_config_reads_env_var(self):
        """Config endpoint should return POWERBI_REPORT_URL from environment."""
        with patch.dict(
            os.environ,
            {
                "FABRIC_WORKSPACE_ID": "ws-fake",
                "FABRIC_DATAAGENT_ID": "agent-fake",
                "POWERBI_REPORT_URL": "https://app.powerbi.com/test-report",
            },
        ):
            with patch("fabric_client.FabricClient", side_effect=Exception("no config")):
                import app as app_module
                importlib.reload(app_module)
                client = TestClient(app_module.app)
                resp = client.get("/api/config")
                assert resp.status_code == 200
                assert resp.json()["powerbi_report_url"] == "https://app.powerbi.com/test-report"


# ─── Status endpoint tests ────────────────────────────────────────────────────

class TestStatusEndpoint:
    def test_status_returns_200(self, test_client_with_fabric):
        resp = test_client_with_fabric.get("/api/status")
        assert resp.status_code == 200

    def test_status_response_has_required_keys(self, test_client_with_fabric):
        resp = test_client_with_fabric.get("/api/status")
        data = resp.json()
        assert "ready" in data
        assert "message" in data
        assert "troubleshooting" in data

    def test_status_not_ready_when_fabric_not_configured(self, test_client_no_fabric):
        """When FabricClient failed to init, /api/status must return ready=False."""
        resp = test_client_no_fabric.get("/api/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ready"] is False
        assert isinstance(data["troubleshooting"], list)
        assert len(data["troubleshooting"]) > 0

    def test_status_ready_when_agent_published(self):
        """When the Fabric agent is in a published state, status returns ready=True."""
        mock_fc = _make_mock_fabric_client()
        mock_fc.workspace_id = "ws-test-guid"
        mock_fc.dataagent_id = "agent-test-guid"
        mock_fc.dataagent_name = "TestAgent"
        mock_fc._get_token.return_value = "fake-token"

        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {"state": "Published"}

        with patch.dict(
            os.environ,
            {
                "FABRIC_WORKSPACE_ID": "ws-test-guid",
                "FABRIC_DATAAGENT_ID": "agent-test-guid",
            },
        ):
            with patch("fabric_client.FabricClient", return_value=mock_fc):
                import app as app_module
                importlib.reload(app_module)
                app_module.fabric_client = mock_fc
                with patch("app._requests.get", return_value=mock_resp):
                    client = TestClient(app_module.app)
                    resp = client.get("/api/status")
                    assert resp.status_code == 200
                    data = resp.json()
                    assert data["ready"] is True
                    assert len(data["troubleshooting"]) == 0

    def test_status_not_ready_when_agent_draft(self):
        """When agent state is Draft, status returns ready=False with troubleshooting."""
        mock_fc = _make_mock_fabric_client()
        mock_fc.workspace_id = "ws-test-guid"
        mock_fc.dataagent_id = "agent-test-guid"
        mock_fc.dataagent_name = "TestAgent"
        mock_fc._get_token.return_value = "fake-token"

        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {"state": "Draft"}

        with patch.dict(
            os.environ,
            {
                "FABRIC_WORKSPACE_ID": "ws-test-guid",
                "FABRIC_DATAAGENT_ID": "agent-test-guid",
            },
        ):
            with patch("fabric_client.FabricClient", return_value=mock_fc):
                import app as app_module
                importlib.reload(app_module)
                app_module.fabric_client = mock_fc
                with patch("app._requests.get", return_value=mock_resp):
                    client = TestClient(app_module.app)
                    resp = client.get("/api/status")
                    assert resp.status_code == 200
                    data = resp.json()
                    assert data["ready"] is False
                    assert len(data["troubleshooting"]) > 0
                    assert "Draft" in data["message"]

    def test_status_not_ready_when_agent_unreachable(self):
        """When agent returns non-200, status returns ready=False."""
        mock_fc = _make_mock_fabric_client()
        mock_fc.workspace_id = "ws-test-guid"
        mock_fc.dataagent_id = "agent-test-guid"
        mock_fc.dataagent_name = "TestAgent"
        mock_fc._get_token.return_value = "fake-token"

        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 404
        mock_resp.text = "Not Found"

        with patch.dict(
            os.environ,
            {
                "FABRIC_WORKSPACE_ID": "ws-test-guid",
                "FABRIC_DATAAGENT_ID": "agent-test-guid",
            },
        ):
            with patch("fabric_client.FabricClient", return_value=mock_fc):
                import app as app_module
                importlib.reload(app_module)
                app_module.fabric_client = mock_fc
                with patch("app._requests.get", return_value=mock_resp):
                    client = TestClient(app_module.app)
                    resp = client.get("/api/status")
                    assert resp.status_code == 200
                    data = resp.json()
                    assert data["ready"] is False
                    assert len(data["troubleshooting"]) > 0

    def test_status_ready_when_query_404_but_assistants_api_works(self):
        """When /query returns 404 but Assistants API is reachable, status is ready."""
        mock_fc = _make_mock_fabric_client()
        mock_fc.workspace_id = "ws-test-guid"
        mock_fc.dataagent_id = "agent-test-guid"
        mock_fc.dataagent_name = "TestAgent"
        mock_fc._get_token.return_value = "fake-token"

        # Metadata GET returns 200 with no explicit state (common for Published agents)
        mock_metadata_resp = MagicMock()
        mock_metadata_resp.ok = True
        mock_metadata_resp.json.return_value = {}

        # /query POST returns 404
        mock_query_resp = MagicMock()
        mock_query_resp.status_code = 404

        # Assistants API GET returns 200 (agent is reachable via Assistants API)
        mock_assistants_resp = MagicMock()
        mock_assistants_resp.status_code = 200

        def mock_get(url, **kwargs):
            if "/aiassistant/openai/assistants" in url:
                return mock_assistants_resp
            return mock_metadata_resp

        with patch.dict(
            os.environ,
            {
                "FABRIC_WORKSPACE_ID": "ws-test-guid",
                "FABRIC_DATAAGENT_ID": "agent-test-guid",
            },
        ):
            with patch("fabric_client.FabricClient", return_value=mock_fc):
                import app as app_module
                importlib.reload(app_module)
                app_module.fabric_client = mock_fc
                with patch("app._requests.get", side_effect=mock_get):
                    with patch("app._requests.post", return_value=mock_query_resp):
                        client = TestClient(app_module.app)
                        resp = client.get("/api/status")
                        assert resp.status_code == 200
                        data = resp.json()
                        assert data["ready"] is True
                        assert len(data["troubleshooting"]) == 0

    def test_status_not_ready_when_both_query_and_assistants_return_404(self):
        """When both /query and Assistants API return 404, status is not ready."""
        mock_fc = _make_mock_fabric_client()
        mock_fc.workspace_id = "ws-test-guid"
        mock_fc.dataagent_id = "agent-test-guid"
        mock_fc.dataagent_name = "TestAgent"
        mock_fc._get_token.return_value = "fake-token"

        # Metadata GET returns 200 with no explicit state
        mock_metadata_resp = MagicMock()
        mock_metadata_resp.ok = True
        mock_metadata_resp.json.return_value = {}

        # Both probes return 404
        mock_404_resp = MagicMock()
        mock_404_resp.status_code = 404

        def mock_get(url, **kwargs):
            if "/aiassistant/openai/assistants" in url:
                return mock_404_resp
            return mock_metadata_resp

        with patch.dict(
            os.environ,
            {
                "FABRIC_WORKSPACE_ID": "ws-test-guid",
                "FABRIC_DATAAGENT_ID": "agent-test-guid",
            },
        ):
            with patch("fabric_client.FabricClient", return_value=mock_fc):
                import app as app_module
                importlib.reload(app_module)
                app_module.fabric_client = mock_fc
                with patch("app._requests.get", side_effect=mock_get):
                    with patch("app._requests.post", return_value=mock_404_resp):
                        client = TestClient(app_module.app)
                        resp = client.get("/api/status")
                        assert resp.status_code == 200
                        data = resp.json()
                        assert data["ready"] is False
                        assert len(data["troubleshooting"]) > 0

    def test_status_ready_when_assistants_post_with_api_version_works(self):
        """When POST to /assistants?api-version returns non-404, status is ready.

        This is the real-world scenario where the agent only supports the
        OpenAI Assistants API (not the legacy /query endpoint). The FabricAgentClient
        calls POST /aiassistant/openai/assistants?api-version=2024-05-01-preview which
        must be probed by the status check.
        """
        mock_fc = _make_mock_fabric_client()
        mock_fc.workspace_id = "ws-test-guid"
        mock_fc.dataagent_id = "agent-test-guid"
        mock_fc.dataagent_name = "TestAgent"
        mock_fc._get_token.return_value = "fake-token"

        # Metadata GET returns 200 with no explicit state
        mock_metadata_resp = MagicMock()
        mock_metadata_resp.ok = True
        mock_metadata_resp.json.return_value = {}

        # /query POST returns 404, but POST to /assistants returns 200
        mock_query_resp = MagicMock()
        mock_query_resp.status_code = 404
        mock_assistants_post_resp = MagicMock()
        mock_assistants_post_resp.status_code = 200

        def mock_post(url, **kwargs):
            if "/aiassistant/openai/assistants" in url:
                return mock_assistants_post_resp  # 200 - matches FabricAgentClient behavior
            return mock_query_resp  # 404 for /query

        with patch.dict(
            os.environ,
            {
                "FABRIC_WORKSPACE_ID": "ws-test-guid",
                "FABRIC_DATAAGENT_ID": "agent-test-guid",
            },
        ):
            with patch("fabric_client.FabricClient", return_value=mock_fc):
                import app as app_module
                importlib.reload(app_module)
                app_module.fabric_client = mock_fc
                with patch("app._requests.get", return_value=mock_metadata_resp):
                    with patch("app._requests.post", side_effect=mock_post):
                        client = TestClient(app_module.app)
                        resp = client.get("/api/status")
                        assert resp.status_code == 200
                        data = resp.json()
                        assert data["ready"] is True
                        assert len(data["troubleshooting"]) == 0



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

    def test_chat_falls_back_to_assistants_api_when_query_not_found(self):
        """When fabric_client.chat raises AgentNotReadyError, /api/chat falls back to agent_client.ask()."""
        from fabric_client import AgentNotReadyError

        import app as app_module
        importlib.reload(app_module)

        mock_fc = MagicMock()
        mock_fc.workspace_id = "ws-test-guid"
        mock_fc.dataagent_id = "agent-test-guid"
        mock_fc.chat.side_effect = AgentNotReadyError(
            "Agent in Draft state",
            workspace_id="ws-test-guid",
            agent_id="agent-test-guid",
        )

        mock_ac = MagicMock()
        mock_ac.ask.return_value = "Fallback answer from Assistants API"

        app_module.fabric_client = mock_fc
        app_module.agent_client = mock_ac

        client = TestClient(app_module.app)
        resp = client.post("/api/chat", json={"message": "Top customers"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["answer"] == "Fallback answer from Assistants API"
        assert data["metadata"]["endpoint"] == "assistants-api-fallback"

    def test_chat_503_when_fallback_also_fails(self):
        """When both fabric_client.chat and agent_client.ask fail, returns 503."""
        from fabric_client import AgentNotReadyError

        import app as app_module
        importlib.reload(app_module)

        mock_fc = MagicMock()
        mock_fc.workspace_id = "ws-test-guid"
        mock_fc.dataagent_id = "agent-test-guid"
        mock_fc.chat.side_effect = AgentNotReadyError(
            "Agent not ready",
            workspace_id="ws-test-guid",
            agent_id="agent-test-guid",
        )

        mock_ac = MagicMock()
        mock_ac.ask.side_effect = RuntimeError("Assistants API also unavailable")

        app_module.fabric_client = mock_fc
        app_module.agent_client = mock_ac

        client = TestClient(app_module.app)
        resp = client.post("/api/chat", json={"message": "Top customers"})
        assert resp.status_code == 503
        detail = resp.json().get("detail", {})
        assert detail.get("error") == "agent_not_ready"

    def test_chat_503_when_agent_not_ready_and_no_fallback_client(self):
        """When AgentNotReadyError is raised and agent_client is None, returns 503."""
        from fabric_client import AgentNotReadyError

        import app as app_module
        importlib.reload(app_module)

        mock_fc = MagicMock()
        mock_fc.workspace_id = "ws-test-guid"
        mock_fc.dataagent_id = "agent-test-guid"
        mock_fc.chat.side_effect = AgentNotReadyError(
            "Agent not ready",
            workspace_id="ws-test-guid",
            agent_id="agent-test-guid",
        )

        app_module.fabric_client = mock_fc
        app_module.agent_client = None

        client = TestClient(app_module.app)
        resp = client.post("/api/chat", json={"message": "Top customers"})
        assert resp.status_code == 503
        detail = resp.json().get("detail", {})
        assert detail.get("error") == "agent_not_ready"


class TestResetEndpoint:
    def test_reset_returns_200(self, test_client_with_fabric):
        resp = test_client_with_fabric.post(
            "/api/reset", json={"userId": "web-user"}
        )
        assert resp.status_code == 200

    def test_reset_response_body(self, test_client_with_fabric):
        resp = test_client_with_fabric.post(
            "/api/reset", json={"userId": "web-user"}
        )
        data = resp.json()
        assert data["status"] == "ok"
        assert "reset" in data["message"].lower()

    def test_reset_defaults_user_id(self, test_client_with_fabric):
        """When no userId is provided, it defaults to 'anonymous'."""
        resp = test_client_with_fabric.post("/api/reset", json={})
        assert resp.status_code == 200

    def test_reset_503_when_fabric_not_configured(self, test_client_no_fabric):
        """When FabricClient failed to init, /api/reset must return 503."""
        resp = test_client_no_fabric.post(
            "/api/reset", json={"userId": "test"}
        )
        assert resp.status_code == 503

    def test_reset_calls_fabric_client(self, test_client_with_fabric):
        """Ensures reset_conversation is called on the FabricClient."""
        import app as app_module
        importlib.reload(app_module)
        mock_fc = _make_mock_fabric_client()
        app_module.fabric_client = mock_fc
        client = TestClient(app_module.app)
        client.post("/api/reset", json={"userId": "user-42"})
        mock_fc.reset_conversation.assert_called_once()


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
            mock_resp.json.return_value = {"errorCode": "EntityNotFound"}
            return mock_resp

        with patch("requests.post", side_effect=fake_post):
            from fabric_client import AgentNotReadyError
            with pytest.raises(AgentNotReadyError, match="404"):
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

    def test_chat_handles_no_http_response(self):
        client = self._make_client()

        with (
            patch.object(client, "_call_primary", return_value=None) as mock_primary,
            patch.object(client, "_call_openai_compat", return_value=None),
            patch("fabric_client.time.sleep", return_value=None),
        ):
            with pytest.raises(
                RuntimeError,
                match="no HTTP response from primary and fallback endpoints",
            ):
                client.chat("u8", "test")

        assert mock_primary.call_count == 3

    def test_openai_compat_url_uses_dataagents_path(self):
        """_openai_compat_url must use the /dataAgents/ path (not /aiskills/)."""
        client = self._make_client()
        url = client._openai_compat_url()
        assert "/dataAgents/" in url
        assert "/aiskills/" not in url
        assert url.endswith("/aiassistant/openai")

    def test_openai_compat_url_legacy_uses_aiskills_path(self):
        """_openai_compat_url_legacy must use the /aiskills/ path."""
        client = self._make_client()
        url = client._openai_compat_url_legacy()
        assert "/aiskills/" in url
        assert url.endswith("/aiassistant/openai")

    def test_fallback_tries_dataagents_before_aiskills(self):
        """OpenAI-compat fallback should try /dataAgents/ first, then /aiskills/."""
        client = self._make_client()
        called_urls = []

        def fake_post(url, **kwargs):
            called_urls.append(url)
            mock_resp = MagicMock()
            if "/dataAgents/" in url and "/aiassistant/openai" in url:
                mock_resp.ok = True
                mock_resp.status_code = 200
                mock_resp.json.return_value = {"choices": [{"message": {"content": "fallback ok"}}]}
            else:
                mock_resp.ok = False
                mock_resp.status_code = 404
            return mock_resp

        with patch("requests.post", side_effect=fake_post):
            result = client._call_openai_compat("test", None)
        assert result is not None
        assert result.ok
        # Should have hit the dataAgents URL first
        assert any("/dataAgents/" in u and "/aiassistant/openai" in u for u in called_urls)

    def test_fallback_falls_through_to_aiskills_on_404(self):
        """When /dataAgents/ returns 404, fallback should try /aiskills/."""
        client = self._make_client()
        called_urls = []

        def fake_post(url, **kwargs):
            called_urls.append(url)
            mock_resp = MagicMock()
            if "/aiskills/" in url:
                mock_resp.ok = True
                mock_resp.status_code = 200
                mock_resp.json.return_value = {"choices": [{"message": {"content": "legacy ok"}}]}
            else:
                mock_resp.ok = False
                mock_resp.status_code = 404
            return mock_resp

        with patch("requests.post", side_effect=fake_post):
            result = client._call_openai_compat("test", None)
        assert result is not None
        assert result.ok
        # Both URLs should have been tried
        assert len(called_urls) == 2
        assert any("/dataAgents/" in u for u in called_urls)
        assert any("/aiskills/" in u for u in called_urls)

    def test_chat_succeeds_via_openai_compat_when_query_returns_404(self):
        """When /query returns 404 but OpenAI-compat works, chat should succeed."""
        client = self._make_client()
        call_count = {"primary": 0}

        def fake_post(url, **kwargs):
            mock_resp = MagicMock()
            if "/query" in url:
                call_count["primary"] += 1
                mock_resp.ok = False
                mock_resp.status_code = 404
                mock_resp.text = "not found"
                mock_resp.json.return_value = {"errorCode": "EntityNotFound"}
                return mock_resp
            elif "/aiassistant/openai" in url:
                mock_resp.ok = True
                mock_resp.status_code = 200
                mock_resp.json.return_value = {
                    "choices": [{"message": {"content": "Fallback answer"}}]
                }
                return mock_resp
            mock_resp.ok = False
            mock_resp.status_code = 500
            mock_resp.text = "unexpected"
            return mock_resp

        def fake_get(url, **kwargs):
            mock_resp = MagicMock()
            # Pre-flight returns state=unknown (agent looks OK in metadata)
            mock_resp.ok = True
            mock_resp.json.return_value = {}
            return mock_resp

        with (
            patch("requests.post", side_effect=fake_post),
            patch("requests.get", side_effect=fake_get),
            patch("fabric_client.time.sleep", return_value=None),
        ):
            result = client.chat("u-fb", "Count customers by Segment")

        assert result["answer"] == "Fallback answer"
        assert result["metadata"]["endpoint"] == "openai-compat"

    def test_chat_post_discovery_retry_succeeds_when_same_id_confirmed(self):
        """When auto-discovery confirms the same agent ID, one extra retry is
        performed with a delay.  This covers the warm-up scenario where the
        listing API succeeds before the query endpoint is ready."""
        client = self._make_client()
        call_count = {"primary": 0}

        def fake_post(url, **kwargs):
            mock_resp = MagicMock()
            if "/query" in url:
                call_count["primary"] += 1
                if call_count["primary"] <= 3:
                    # First 3 retries: still warming up
                    mock_resp.ok = False
                    mock_resp.status_code = 404
                    mock_resp.text = "Entity not found"
                    mock_resp.json.return_value = {"errorCode": "EntityNotFound"}
                else:
                    # Post-discovery retry: agent is now ready
                    mock_resp.ok = True
                    mock_resp.status_code = 200
                    mock_resp.json.return_value = {"answer": "Post-discovery answer"}
            else:
                mock_resp.ok = False
                mock_resp.status_code = 404
                mock_resp.text = "not found"
                mock_resp.json.return_value = {}
            return mock_resp

        def fake_get(url, **kwargs):
            mock_resp = MagicMock()
            mock_resp.ok = True
            # Simulate the listing API returning the same agent ID as the client
            mock_resp.json.return_value = {
                "value": [
                    {
                        "displayName": "Customer360Agent",
                        "id": client.dataagent_id,
                    }
                ]
            }
            return mock_resp

        with (
            patch("requests.post", side_effect=fake_post),
            patch("requests.get", side_effect=fake_get),
            patch("fabric_client.time.sleep", return_value=None),
        ):
            result = client.chat("u-disc", "Top 5 customers")

        assert result["answer"] == "Post-discovery answer"
        # 3 warm-up retries + 1 post-discovery retry
        assert call_count["primary"] == 4

    def test_fallback_returns_last_404_when_all_urls_fail(self):
        """_call_openai_compat must return the last 404 response (not None) when
        every URL returns 404, so callers see the actual HTTP status code."""
        client = self._make_client()

        def fake_post(url, **kwargs):
            mock_resp = MagicMock()
            mock_resp.ok = False
            mock_resp.status_code = 404
            mock_resp.text = "Entity not found"
            return mock_resp

        with patch("requests.post", side_effect=fake_post):
            result = client._call_openai_compat("test message", None)

        assert result is not None, "Expected last 404 response, got None"
        assert result.status_code == 404


# ─── AgentNotReadyError tests ─────────────────────────────────────────────────

class TestAgentNotReadyError:
    """Tests for the AgentNotReadyError and the 503 handling path."""

    def test_agent_not_ready_is_runtime_error(self):
        """AgentNotReadyError must be a subclass of RuntimeError."""
        from fabric_client import AgentNotReadyError
        err = AgentNotReadyError("test", workspace_id="ws-1", agent_id="ag-1")
        assert isinstance(err, RuntimeError)
        assert err.workspace_id == "ws-1"
        assert err.agent_id == "ag-1"

    def test_draft_state_raises_agent_not_ready(self):
        """Pre-flight detecting Draft state must raise AgentNotReadyError early."""
        from fabric_client import AgentNotReadyError

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
                client = fc_module.FabricClient()

        # Mock the pre-flight GET to return Draft state
        def fake_get(url, **kwargs):
            mock_resp = MagicMock()
            mock_resp.ok = True
            mock_resp.json.return_value = {"state": "Draft"}
            return mock_resp

        with patch("requests.get", side_effect=fake_get):
            with pytest.raises(fc_module.AgentNotReadyError, match="Draft"):
                client.chat("user1", "test question")

    def test_chat_503_on_agent_not_ready(self):
        """When FabricClient raises AgentNotReadyError, API returns 503 with structured detail."""
        from fabric_client import AgentNotReadyError

        mock_fc = MagicMock()
        mock_fc.chat.side_effect = AgentNotReadyError(
            "Agent in Draft state",
            workspace_id="ws-id",
            agent_id="ag-id",
        )

        with patch.dict(
            os.environ,
            {
                "FABRIC_WORKSPACE_ID": "ws-fake",
                "FABRIC_DATAAGENT_ID": "agent-fake",
            },
        ):
            with patch("fabric_client.FabricClient", return_value=mock_fc):
                import app as app_module
                importlib.reload(app_module)
                app_module.fabric_client = mock_fc
                client = TestClient(app_module.app)

                resp = client.post("/api/chat", json={"message": "test"})
                assert resp.status_code == 503
                data = resp.json()
                assert data["detail"]["error"] == "agent_not_ready"
                assert isinstance(data["detail"]["troubleshooting"], list)
                assert len(data["detail"]["troubleshooting"]) > 0


# ─── FabricAgentClient unit tests ─────────────────────────────────────────────

class TestFabricAgentClientInit:
    """Tests for FabricAgentClient initialisation and validation."""

    def test_raises_without_workspace_id(self):
        """Must raise ValueError when workspace_id is missing."""
        with patch.dict(os.environ, {"FABRIC_WORKSPACE_ID": "", "FABRIC_DATAAGENT_ID": "agent-id"}):
            with patch("fabric_agent_client.DefaultAzureCredential"):
                from fabric_agent_client import FabricAgentClient
                importlib.reload(sys.modules["fabric_agent_client"])
                from fabric_agent_client import FabricAgentClient
                with pytest.raises(ValueError, match="workspace_id"):
                    FabricAgentClient()

    def test_raises_without_agent_id(self):
        """Must raise ValueError when agent_id is missing."""
        with patch.dict(os.environ, {"FABRIC_WORKSPACE_ID": "ws-id", "FABRIC_DATAAGENT_ID": ""}):
            with patch("fabric_agent_client.DefaultAzureCredential"):
                import fabric_agent_client as fac
                importlib.reload(fac)
                with pytest.raises(ValueError, match="agent_id"):
                    fac.FabricAgentClient()

    def test_explicit_params_override_env(self):
        """Explicit constructor parameters must override environment variables."""
        with patch.dict(os.environ, {"FABRIC_WORKSPACE_ID": "env-ws", "FABRIC_DATAAGENT_ID": "env-ag"}):
            with patch("fabric_agent_client.DefaultAzureCredential"):
                import fabric_agent_client as fac
                importlib.reload(fac)
                client = fac.FabricAgentClient(
                    workspace_id="override-ws",
                    agent_id="override-ag",
                    agent_name="TestAgent",
                )
                assert client.workspace_id == "override-ws"
                assert client.agent_id == "override-ag"
                assert client.agent_name == "TestAgent"

    def test_data_agent_url_is_built_correctly(self):
        """The OpenAI-compatible URL must be built from workspace and agent IDs."""
        with patch.dict(os.environ, {"FABRIC_WORKSPACE_ID": "ws-123", "FABRIC_DATAAGENT_ID": "ag-456"}):
            with patch("fabric_agent_client.DefaultAzureCredential"):
                import fabric_agent_client as fac
                importlib.reload(fac)
                client = fac.FabricAgentClient()
                assert "ws-123" in client._data_agent_url
                assert "ag-456" in client._data_agent_url
                assert client._data_agent_url.endswith("/aiassistant/openai")


class TestFabricAgentClientHelpers:
    """Tests for SQL extraction and data parsing helpers."""

    def test_sql_from_function_args_extracts_sql(self):
        """Should extract SQL from tool call function arguments."""
        with patch.dict(os.environ, {"FABRIC_WORKSPACE_ID": "ws", "FABRIC_DATAAGENT_ID": "ag"}):
            with patch("fabric_agent_client.DefaultAzureCredential"):
                import fabric_agent_client as fac
                importlib.reload(fac)

                tc = MagicMock()
                tc.function.arguments = '{"sql": "SELECT * FROM customers WHERE id > 10"}'
                result = fac.FabricAgentClient._sql_from_function_args(tc)
                assert len(result) == 1
                assert "SELECT" in result[0]

    def test_sql_from_function_args_empty_on_no_sql(self):
        """Should return empty list when no SQL keys are present."""
        with patch.dict(os.environ, {"FABRIC_WORKSPACE_ID": "ws", "FABRIC_DATAAGENT_ID": "ag"}):
            with patch("fabric_agent_client.DefaultAzureCredential"):
                import fabric_agent_client as fac
                importlib.reload(fac)

                tc = MagicMock()
                tc.function.arguments = '{"key": "value"}'
                result = fac.FabricAgentClient._sql_from_function_args(tc)
                assert result == []

    def test_data_from_output_formats_table(self):
        """Should format JSON list of dicts into markdown table lines."""
        with patch.dict(os.environ, {"FABRIC_WORKSPACE_ID": "ws", "FABRIC_DATAAGENT_ID": "ag"}):
            with patch("fabric_agent_client.DefaultAzureCredential"):
                import fabric_agent_client as fac
                importlib.reload(fac)

                tc = MagicMock()
                tc.output = '[{"name": "Alice", "age": "30"}, {"name": "Bob", "age": "25"}]'
                result = fac.FabricAgentClient._data_from_output(tc)
                assert len(result) == 4  # header + separator + 2 data rows
                assert "name" in result[0]
                assert "Alice" in result[2]

    def test_extract_data_from_text_markdown_table(self):
        """Should extract a markdown table from text content."""
        with patch.dict(os.environ, {"FABRIC_WORKSPACE_ID": "ws", "FABRIC_DATAAGENT_ID": "ag"}):
            with patch("fabric_agent_client.DefaultAzureCredential"):
                import fabric_agent_client as fac
                importlib.reload(fac)

                text = "Here are the results:\n| Name | Age |\n|---|---|\n| Alice | 30 |\n| Bob | 25 |"
                result = fac.FabricAgentClient._extract_data_from_text(text)
                assert len(result) == 1  # raw markdown table as single item
                assert "Alice" in result[0]

    def test_extract_data_from_text_numbered_list(self):
        """Should extract numbered list items when no table is present."""
        with patch.dict(os.environ, {"FABRIC_WORKSPACE_ID": "ws", "FABRIC_DATAAGENT_ID": "ag"}):
            with patch("fabric_agent_client.DefaultAzureCredential"):
                import fabric_agent_client as fac
                importlib.reload(fac)

                text = "Results:\n1. Alice is 30\n2. Bob is 25\n"
                result = fac.FabricAgentClient._extract_data_from_text(text)
                assert len(result) == 2
                assert "Alice" in result[0]

    def test_sql_from_output_extracts_from_json(self):
        """Should extract SQL from JSON output."""
        with patch.dict(os.environ, {"FABRIC_WORKSPACE_ID": "ws", "FABRIC_DATAAGENT_ID": "ag"}):
            with patch("fabric_agent_client.DefaultAzureCredential"):
                import fabric_agent_client as fac
                importlib.reload(fac)

                tc = MagicMock()
                tc.output = '{"generated_code": "SELECT COUNT(*) FROM orders WHERE amount > 100"}'
                result = fac.FabricAgentClient._sql_from_output(tc)
                assert len(result) >= 1
                assert "SELECT" in result[0]


# ─── Agent API endpoint tests ────────────────────────────────────────────────

def _make_mock_agent_client():
    """Returns a MagicMock that looks like a FabricAgentClient."""
    mock = MagicMock()
    mock.ask.return_value = "Mock agent answer via Assistants API"
    mock.get_run_details.return_value = {
        "question": "test",
        "answer": "Detailed mock answer",
        "run_status": "completed",
        "run_steps": {"data": []},
        "messages": {"data": []},
        "timestamp": 1700000000.0,
        "thread": {"id": "thread-123", "name": "test-thread"},
        "sql_queries": ["SELECT * FROM customers"],
        "sql_data_previews": [["| name | age |", "|---|---|", "| Alice | 30 |"]],
        "data_retrieval_query": "SELECT * FROM customers",
    }
    mock.get_raw_run_response.return_value = {
        "question": "test",
        "run": {},
        "steps": {},
        "messages": {},
        "timestamp": 1700000000.0,
        "success": True,
        "thread": {"id": "thread-123", "name": "test-thread"},
    }
    mock.compare_draft_vs_production.return_value = {
        "question": "test",
        "draft": {"answer": "draft answer", "run_status": "completed", "sql_queries": [], "error": None},
        "production": {"answer": "prod answer", "run_status": "completed", "sql_queries": [], "error": None},
        "match": False,
        "timestamp": 1700000000.0,
    }
    return mock


@pytest.fixture()
def test_client_with_agent():
    """TestClient where both FabricClient and FabricAgentClient are mocked."""
    mock_fc = _make_mock_fabric_client("simple answer")
    mock_ac = _make_mock_agent_client()

    with patch.dict(
        os.environ,
        {
            "FABRIC_WORKSPACE_ID": "ws-fake-guid-0000",
            "FABRIC_DATAAGENT_ID": "agent-fake-guid-0000",
        },
    ):
        with patch("fabric_client.FabricClient", return_value=mock_fc):
            with patch("fabric_agent_client.FabricAgentClient", return_value=mock_ac):
                import app as app_module
                importlib.reload(app_module)
                app_module.fabric_client = mock_fc
                app_module.agent_client = mock_ac
                yield TestClient(app_module.app)


@pytest.fixture()
def test_client_no_agent():
    """TestClient where FabricAgentClient is None (not configured)."""
    mock_fc = _make_mock_fabric_client("simple answer")

    with patch.dict(
        os.environ,
        {
            "FABRIC_WORKSPACE_ID": "ws-fake-guid-0000",
            "FABRIC_DATAAGENT_ID": "agent-fake-guid-0000",
        },
    ):
        with patch("fabric_client.FabricClient", return_value=mock_fc):
            with patch("fabric_agent_client.FabricAgentClient", side_effect=Exception("no config")):
                import app as app_module
                importlib.reload(app_module)
                app_module.fabric_client = mock_fc
                app_module.agent_client = None
                yield TestClient(app_module.app)


class TestAgentAskEndpoint:
    """Tests for POST /api/agent/ask."""

    def test_ask_returns_200(self, test_client_with_agent):
        resp = test_client_with_agent.post("/api/agent/ask", json={"question": "test"})
        assert resp.status_code == 200

    def test_ask_returns_answer(self, test_client_with_agent):
        resp = test_client_with_agent.post("/api/agent/ask", json={"question": "test"})
        data = resp.json()
        assert "answer" in data
        assert data["answer"] == "Mock agent answer via Assistants API"

    def test_ask_requires_question(self, test_client_with_agent):
        resp = test_client_with_agent.post("/api/agent/ask", json={})
        assert resp.status_code == 400

    def test_ask_empty_question_returns_400(self, test_client_with_agent):
        resp = test_client_with_agent.post("/api/agent/ask", json={"question": "  "})
        assert resp.status_code == 400

    def test_ask_503_when_agent_not_configured(self, test_client_no_agent):
        resp = test_client_no_agent.post("/api/agent/ask", json={"question": "test"})
        assert resp.status_code == 503

    def test_ask_passes_thread_name(self, test_client_with_agent):
        resp = test_client_with_agent.post(
            "/api/agent/ask",
            json={"question": "test", "thread_name": "my-thread"},
        )
        assert resp.status_code == 200


class TestAgentRunDetailsEndpoint:
    """Tests for POST /api/agent/run-details."""

    def test_run_details_returns_200(self, test_client_with_agent):
        resp = test_client_with_agent.post("/api/agent/run-details", json={"question": "test"})
        assert resp.status_code == 200

    def test_run_details_has_sql_queries(self, test_client_with_agent):
        resp = test_client_with_agent.post("/api/agent/run-details", json={"question": "test"})
        data = resp.json()
        assert "sql_queries" in data
        assert len(data["sql_queries"]) > 0

    def test_run_details_has_run_status(self, test_client_with_agent):
        resp = test_client_with_agent.post("/api/agent/run-details", json={"question": "test"})
        data = resp.json()
        assert data["run_status"] == "completed"

    def test_run_details_requires_question(self, test_client_with_agent):
        resp = test_client_with_agent.post("/api/agent/run-details", json={})
        assert resp.status_code == 400

    def test_run_details_503_when_agent_not_configured(self, test_client_no_agent):
        resp = test_client_no_agent.post("/api/agent/run-details", json={"question": "test"})
        assert resp.status_code == 503


class TestAgentCompareEndpoint:
    """Tests for POST /api/agent/compare (draft vs production)."""

    def test_compare_returns_200(self, test_client_with_agent):
        resp = test_client_with_agent.post(
            "/api/agent/compare",
            json={
                "question": "test",
                "draft_agent_id": "draft-id",
                "production_agent_id": "prod-id",
            },
        )
        assert resp.status_code == 200

    def test_compare_response_shape(self, test_client_with_agent):
        resp = test_client_with_agent.post(
            "/api/agent/compare",
            json={
                "question": "test",
                "draft_agent_id": "draft-id",
                "production_agent_id": "prod-id",
            },
        )
        data = resp.json()
        assert "draft" in data
        assert "production" in data
        assert "match" in data
        assert isinstance(data["match"], bool)

    def test_compare_requires_both_agent_ids(self, test_client_with_agent):
        resp = test_client_with_agent.post(
            "/api/agent/compare",
            json={"question": "test", "draft_agent_id": "draft-id"},
        )
        assert resp.status_code == 400

    def test_compare_requires_question(self, test_client_with_agent):
        resp = test_client_with_agent.post(
            "/api/agent/compare",
            json={"draft_agent_id": "draft-id", "production_agent_id": "prod-id"},
        )
        assert resp.status_code == 400

    def test_compare_503_when_agent_not_configured(self, test_client_no_agent):
        resp = test_client_no_agent.post(
            "/api/agent/compare",
            json={
                "question": "test",
                "draft_agent_id": "draft-id",
                "production_agent_id": "prod-id",
            },
        )
        assert resp.status_code == 503
