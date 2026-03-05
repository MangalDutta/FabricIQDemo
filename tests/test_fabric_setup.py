"""
Fabric Setup Script Unit Tests
───────────────────────────────
Tests for scripts/fabric_setup.py using mocked HTTP calls
(no real Fabric credentials needed).

Run with:
    pip install -r tests/requirements.txt
    PYTHONPATH=scripts pytest tests/test_fabric_setup.py -v
"""

import json
import os
import sys
import importlib
from unittest.mock import MagicMock, patch, call
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import fabric_setup as fs


# ─── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _patch_credentials(monkeypatch):
    """Prevent real Azure auth calls in every test."""
    mock_cred = MagicMock()
    mock_cred.get_token.return_value = MagicMock(token="fake-token-abc")
    with patch("fabric_setup.DefaultAzureCredential", return_value=mock_cred):
        yield


def _ok_response(body: dict, status: int = 200) -> MagicMock:
    """Helper to build a mock requests.Response."""
    m = MagicMock()
    m.ok = status < 400
    m.status_code = status
    m.json.return_value = body
    m.headers = {}
    m.text = json.dumps(body)
    return m


def _error_response(status: int, text: str = "error") -> MagicMock:
    m = MagicMock()
    m.ok = False
    m.status_code = status
    m.json.return_value = {}
    m.headers = {}
    m.text = text
    return m


# ─── get_fabric_token ────────────────────────────────────────────────────────

class TestGetFabricToken:
    def test_returns_string(self):
        token = fs.get_fabric_token()
        assert isinstance(token, str)
        assert len(token) > 0

    def test_uses_fabric_scope(self):
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token="tok")
        with patch("fabric_setup.DefaultAzureCredential", return_value=mock_cred):
            fs.get_fabric_token()
        mock_cred.get_token.assert_called_once_with(fs.FABRIC_SCOPE)


class TestGetStorageToken:
    def test_returns_string(self):
        token = fs.get_storage_token()
        assert isinstance(token, str)

    def test_uses_storage_scope(self):
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token="tok2")
        with patch("fabric_setup.DefaultAzureCredential", return_value=mock_cred):
            fs.get_storage_token()
        mock_cred.get_token.assert_called_once_with(fs.STORAGE_SCOPE)


# ─── fabric_request ──────────────────────────────────────────────────────────

class TestFabricRequest:
    def test_adds_bearer_auth(self):
        resp = _ok_response({"value": []})
        with patch("requests.request", return_value=resp) as mock_req:
            fs.fabric_request("GET", "/workspaces", "my-token")
        _, kwargs = mock_req.call_args
        assert kwargs["headers"]["Authorization"] == "Bearer my-token"

    def test_constructs_full_url(self):
        resp = _ok_response({})
        with patch("requests.request", return_value=resp) as mock_req:
            fs.fabric_request("GET", "/workspaces", "tok")
        args, _ = mock_req.call_args
        assert args[1].startswith(fs.FABRIC_BASE_URL)

    def test_raises_on_error_status(self):
        resp = _error_response(403, "Forbidden")
        with patch("requests.request", return_value=resp):
            with pytest.raises(RuntimeError, match="403"):
                fs.fabric_request("GET", "/workspaces", "tok")

    def test_no_raise_when_ok(self):
        resp = _ok_response({"id": "ws1"}, 200)
        with patch("requests.request", return_value=resp):
            result = fs.fabric_request("GET", "/workspaces/ws1", "tok")
        assert result.status_code == 200


# ─── list_workspaces ─────────────────────────────────────────────────────────

class TestListWorkspaces:
    def test_returns_list(self):
        body = {"value": [{"id": "ws1", "displayName": "Test"}]}
        with patch("requests.get", return_value=_ok_response(body)):
            result = fs.list_workspaces("tok")
        assert isinstance(result, list)
        assert result[0]["id"] == "ws1"

    def test_returns_empty_list_when_no_value(self):
        with patch("requests.get", return_value=_ok_response({})):
            result = fs.list_workspaces("tok")
        assert result == []


# ─── get_or_create_workspace ─────────────────────────────────────────────────

class TestGetOrCreateWorkspace:
    def test_returns_existing_workspace_id(self):
        list_body = {
            "value": [{"id": "ws-exists", "displayName": "MyWorkspace", "capacityId": "cap1"}]
        }
        with patch("requests.get", return_value=_ok_response(list_body)):
            ws_id = fs.get_or_create_workspace("MyWorkspace", "cap1", "tok")
        assert ws_id == "ws-exists"

    def test_reassigns_workspace_to_new_capacity(self):
        list_body = {
            "value": [{"id": "ws-1", "displayName": "WS", "capacityId": "old-cap"}]
        }
        assign_body = {}

        with patch("requests.get", return_value=_ok_response(list_body)), \
             patch("requests.request", return_value=_ok_response(assign_body)):
            ws_id = fs.get_or_create_workspace("WS", "new-cap", "tok")
        assert ws_id == "ws-1"

    def test_creates_workspace_when_not_found(self):
        list_body = {"value": []}  # no matching workspace

        create_resp = MagicMock()
        create_resp.ok = True
        create_resp.status_code = 201
        create_resp.json.return_value = {}
        create_resp.headers = {
            "Location": "https://api.fabric.microsoft.com/v1/workspaces/new-ws-id"
        }
        create_resp.text = "{}"

        with patch("requests.get", return_value=_ok_response(list_body)), \
             patch("requests.post", return_value=create_resp):
            ws_id = fs.get_or_create_workspace("NewWS", "cap1", "tok")
        assert ws_id == "new-ws-id"

    def test_raises_when_workspace_id_unavailable(self):
        list_body = {"value": []}
        create_resp = MagicMock()
        create_resp.ok = True
        create_resp.status_code = 201
        create_resp.json.return_value = {}   # no id
        create_resp.headers = {}              # no Location
        create_resp.text = "{}"
        with patch("requests.get", return_value=_ok_response(list_body)), \
             patch("requests.post", return_value=create_resp):
            with pytest.raises(RuntimeError, match="Location"):
                fs.get_or_create_workspace("NewWS", "cap1", "tok")


# ─── get_or_create_lakehouse ─────────────────────────────────────────────────

class TestGetOrCreateLakehouse:
    def test_returns_existing_lakehouse(self):
        body = {"value": [{"id": "lh-1", "displayName": "MyLakehouse"}]}
        with patch("requests.request", return_value=_ok_response(body)):
            lh_id = fs.get_or_create_lakehouse("ws1", "MyLakehouse", "tok")
        assert lh_id == "lh-1"

    def test_creates_lakehouse_when_missing(self):
        list_body = {"value": []}
        create_body = {"id": "lh-new"}
        responses = [
            _ok_response(list_body),    # GET items?type=Lakehouse
            _ok_response(create_body),  # POST lakehouses
        ]
        with patch("requests.request", side_effect=responses):
            lh_id = fs.get_or_create_lakehouse("ws1", "NewLH", "tok")
        assert lh_id == "lh-new"

    def test_raises_when_create_returns_no_id(self):
        list_body = {"value": []}
        create_body = {}   # no id
        responses = [
            _ok_response(list_body),
            _ok_response(create_body),
        ]
        with patch("requests.request", side_effect=responses):
            with pytest.raises(RuntimeError, match="'id'"):
                fs.get_or_create_lakehouse("ws1", "NewLH", "tok")


# ─── upload_csv_to_onelake ────────────────────────────────────────────────────

class TestUploadCsvToOneLake:
    def _put_resp(self) -> MagicMock:
        m = MagicMock()
        m.ok = True
        m.status_code = 201
        m.headers = {}
        m.text = ""
        return m

    def _patch_resp(self) -> MagicMock:
        m = MagicMock()
        m.ok = True
        m.status_code = 202
        m.headers = {}
        m.text = ""
        return m

    def test_uploads_file_successfully(self, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n")

        with patch("requests.put", return_value=self._put_resp()) as mock_put, \
             patch("requests.patch", return_value=self._patch_resp()) as mock_patch:
            filename = fs.upload_csv_to_onelake(
                "ws1", "lh1", str(csv_file), "fake-storage-token"
            )

        assert filename == "data.csv"
        assert mock_put.call_count == 1
        assert mock_patch.call_count == 2  # append + flush

    def test_returns_correct_filename(self, tmp_path):
        csv_file = tmp_path / "customer360.csv"
        csv_file.write_text("a,b\n1,2\n")

        with patch("requests.put", return_value=self._put_resp()), \
             patch("requests.patch", return_value=self._patch_resp()):
            filename = fs.upload_csv_to_onelake(
                "ws1", "lh1", str(csv_file), "tok"
            )
        assert filename == "customer360.csv"

    def test_raises_on_put_failure(self, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n")

        bad_resp = MagicMock()
        bad_resp.ok = False
        bad_resp.status_code = 403
        bad_resp.text = "Forbidden"

        with patch("requests.put", return_value=bad_resp):
            with pytest.raises(RuntimeError, match="403"):
                fs.upload_csv_to_onelake("ws1", "lh1", str(csv_file), "tok")

    def test_raises_on_append_failure(self, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n")

        ok_put = self._put_resp()
        bad_patch = MagicMock()
        bad_patch.ok = False
        bad_patch.status_code = 500
        bad_patch.text = "Server error"

        with patch("requests.put", return_value=ok_put), \
             patch("requests.patch", return_value=bad_patch):
            with pytest.raises(RuntimeError, match="500"):
                fs.upload_csv_to_onelake("ws1", "lh1", str(csv_file), "tok")

    def test_raises_when_csv_file_missing(self, tmp_path):
        """test_main catches missing file, but upload function should raise too."""
        with pytest.raises(FileNotFoundError):
            # open() inside the function will raise
            fs.upload_csv_to_onelake(
                "ws1", "lh1", str(tmp_path / "nonexistent.csv"), "tok"
            )


# ─── load_table_from_file ─────────────────────────────────────────────────────

class TestLoadTableFromFile:
    def test_succeeds_on_200(self):
        resp = _ok_response({"status": "Succeeded"}, 200)
        with patch("requests.post", return_value=resp):
            # Should not raise
            fs.load_table_from_file("ws1", "lh1", "MyTable", "data.csv", "tok")

    def test_succeeds_on_202_with_operation_id(self):
        accept_resp = MagicMock()
        accept_resp.status_code = 202
        accept_resp.json.return_value = {}
        accept_resp.headers = {"x-ms-operation-id": "op-123"}
        accept_resp.text = "{}"

        with patch("requests.post", return_value=accept_resp), \
             patch("fabric_setup.poll_operation") as mock_poll:
            mock_poll.return_value = {"status": "Succeeded"}
            fs.load_table_from_file("ws1", "lh1", "T", "f.csv", "tok")

        mock_poll.assert_called_once_with("op-123", "tok", "load table 'T'")

    def test_raises_on_400(self):
        resp = _error_response(400, "Bad request")
        with patch("requests.post", return_value=resp):
            with pytest.raises(RuntimeError, match="400"):
                fs.load_table_from_file("ws1", "lh1", "T", "f.csv", "tok")


# ─── poll_operation ───────────────────────────────────────────────────────────

class TestPollOperation:
    def test_returns_on_succeeded(self):
        with patch("requests.request", return_value=_ok_response({"status": "Succeeded"})):
            result = fs.poll_operation("op-1", "tok")
        assert result["status"] == "Succeeded"

    def test_raises_on_failed(self):
        error_body = {"status": "Failed", "error": {"message": "out of memory"}}
        with patch("requests.request", return_value=_ok_response(error_body)):
            with pytest.raises(RuntimeError, match="out of memory"):
                fs.poll_operation("op-2", "tok")

    def test_raises_on_cancelled(self):
        with patch(
            "requests.request",
            return_value=_ok_response({"status": "Cancelled"}),
        ):
            with pytest.raises(RuntimeError):
                fs.poll_operation("op-3", "tok")

    def test_polls_until_succeeded(self):
        responses = [
            _ok_response({"status": "Running"}),
            _ok_response({"status": "Running"}),
            _ok_response({"status": "Succeeded"}),
        ]
        with patch("requests.request", side_effect=responses), \
             patch("time.sleep"):  # skip actual waits
            result = fs.poll_operation("op-4", "tok")
        assert result["status"] == "Succeeded"


# ─── main() argument parsing ──────────────────────────────────────────────────

class TestMain:
    def test_missing_csv_exits_1(self, tmp_path, capsys):
        args = [
            "--workspace_name", "ws",
            "--lakehouse_name", "lh",
            "--csv_path", str(tmp_path / "nonexistent.csv"),
            "--table_name", "T",
            "--dataagent_name", "Agent",
            "--capacity_id", "cap-guid",
        ]
        with pytest.raises(SystemExit) as exc_info:
            fs.main(args)
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "not found" in captured.err

    def test_full_run_success(self, tmp_path):
        csv_file = tmp_path / "customer360.csv"
        csv_file.write_text("CustomerId,FullName\nC1,Alice\n")

        with patch("fabric_setup.get_fabric_token", return_value="fab-tok"), \
             patch("fabric_setup.get_storage_token", return_value="sto-tok"), \
             patch("fabric_setup.get_or_create_workspace", return_value="ws-id"), \
             patch("fabric_setup.add_workspace_member"), \
             patch("fabric_setup.get_or_create_lakehouse", return_value="lh-id"), \
             patch("fabric_setup.upload_csv_to_onelake", return_value="customer360.csv"), \
             patch("fabric_setup.load_table_from_file"), \
             patch("fabric_setup.get_or_create_dataagent", return_value="da-id"), \
             patch("fabric_setup.configure_dataagent"), \
             patch("fabric_setup.publish_dataagent"), \
             patch("fabric_setup.get_default_semantic_model", return_value=None):
            fs.main([
                "--workspace_name", "ws",
                "--lakehouse_name", "lh",
                "--csv_path", str(csv_file),
                "--table_name", "Customer360",
                "--dataagent_name", "Agent",
                "--capacity_id", "cap-guid",
            ])
        # No exception means success

    def test_skip_data_upload_flag(self, tmp_path):
        csv_file = tmp_path / "customer360.csv"
        csv_file.write_text("CustomerId,FullName\nC1,Alice\n")

        with patch("fabric_setup.get_fabric_token", return_value="tok"), \
             patch("fabric_setup.get_storage_token") as mock_sto, \
             patch("fabric_setup.get_or_create_workspace", return_value="ws-id"), \
             patch("fabric_setup.add_workspace_member"), \
             patch("fabric_setup.get_or_create_lakehouse", return_value="lh-id"), \
             patch("fabric_setup.upload_csv_to_onelake") as mock_upload, \
             patch("fabric_setup.load_table_from_file") as mock_load, \
             patch("fabric_setup.get_or_create_dataagent", return_value="da-id"), \
             patch("fabric_setup.configure_dataagent"), \
             patch("fabric_setup.publish_dataagent"), \
             patch("fabric_setup.get_default_semantic_model", return_value=None):
            fs.main([
                "--workspace_name", "ws",
                "--lakehouse_name", "lh",
                "--csv_path", str(csv_file),
                "--table_name", "Customer360",
                "--dataagent_name", "Agent",
                "--capacity_id", "cap-guid",
                "--skip_data_upload",
            ])

        mock_upload.assert_not_called()
        mock_load.assert_not_called()
        mock_sto.assert_not_called()


# ─── configure_dataagent ──────────────────────────────────────────────────────

class TestConfigureDataagent:
    def test_succeeds_with_patch_method(self):
        """configure_dataagent should succeed on PATCH 200 (first attempt)."""
        ok_resp = _ok_response({}, 200)
        with patch("requests.request", return_value=ok_resp) as mock_req:
            fs.configure_dataagent("ws1", "ag1", "Agent", "lh1", "Customer360", "tok")
        # PATCH should be the first method tried
        first_call = mock_req.call_args_list[0]
        assert first_call[0][0] == "PATCH"

    def test_falls_back_to_put_when_patch_returns_404(self):
        """configure_dataagent should fall back from PATCH→PUT on 404."""
        not_found = _error_response(404, '{"errorCode":"EntityNotFound"}')
        ok_resp = _ok_response({}, 200)
        # First PATCH payload returns 404, next method PUT payload returns 200
        responses = [not_found, ok_resp]
        with patch("requests.request", side_effect=responses):
            # Should not raise
            fs.configure_dataagent("ws1", "ag1", "Agent", "lh1", "Customer360", "tok")

    def test_succeeds_on_204(self):
        resp = MagicMock()
        resp.status_code = 204
        resp.headers = {}
        resp.text = ""
        with patch("requests.request", return_value=resp):
            fs.configure_dataagent("ws1", "ag1", "Agent", "lh1", "Customer360", "tok")

    def test_non_fatal_on_all_failures(self):
        """configure_dataagent must not raise even when all attempts fail."""
        err_resp = _error_response(500, "Server error")
        with patch("requests.request", return_value=err_resp):
            # Should not raise – it logs a warning and returns
            fs.configure_dataagent("ws1", "ag1", "Agent", "lh1", "Customer360", "tok")

    def test_includes_object_type_in_selected_objects(self):
        """selectedObjects must include objectType:'Table' for Fabric API compatibility."""
        ok_resp = _ok_response({}, 200)
        with patch("requests.request", return_value=ok_resp) as mock_req:
            fs.configure_dataagent("ws1", "ag1", "Agent", "lh1", "Customer360", "tok")
        # Inspect the JSON body of the first call
        first_call_kwargs = mock_req.call_args_list[0][1]
        body = first_call_kwargs.get("json", {})
        selected = (
            body.get("configuration", {})
                .get("dataSources", [{}])[0]
                .get("selectedObjects", [{}])[0]
        )
        assert selected.get("objectType") == "Table"

    def test_succeeds_on_202_polls_operation(self):
        accept_resp = MagicMock()
        accept_resp.status_code = 202
        accept_resp.headers = {"x-ms-operation-id": "op-cfg-1"}
        accept_resp.text = "{}"
        with patch("requests.request", return_value=accept_resp), \
             patch("fabric_setup.poll_operation") as mock_poll:
            mock_poll.return_value = {"status": "Succeeded"}
            fs.configure_dataagent("ws1", "ag1", "Agent", "lh1", "T", "tok")
        mock_poll.assert_called_once_with("op-cfg-1", "tok", "data agent configuration")


# ─── trigger_default_semantic_model ──────────────────────────────────────────

class TestTriggerDefaultSemanticModel:
    def test_succeeds_on_200(self):
        with patch("requests.post", return_value=_ok_response({}, 200)):
            # Should not raise
            fs.trigger_default_semantic_model("ws1", "lh1", "tok")

    def test_succeeds_on_201(self):
        with patch("requests.post", return_value=_ok_response({"id": "sm1"}, 201)):
            fs.trigger_default_semantic_model("ws1", "lh1", "tok")

    def test_handles_409_gracefully(self):
        """409 Conflict means the model already exists — should not raise."""
        conflict = _error_response(409, '{"errorCode":"Conflict"}')
        with patch("requests.post", return_value=conflict):
            fs.trigger_default_semantic_model("ws1", "lh1", "tok")

    def test_handles_error_gracefully(self):
        """Non-2xx/409 response must be treated as non-fatal."""
        err = _error_response(500, "Internal error")
        with patch("requests.post", return_value=err):
            fs.trigger_default_semantic_model("ws1", "lh1", "tok")

    def test_handles_exception_gracefully(self):
        """Network exception must be treated as non-fatal."""
        with patch("requests.post", side_effect=ConnectionError("no network")):
            fs.trigger_default_semantic_model("ws1", "lh1", "tok")

    def test_calls_correct_endpoint(self):
        with patch("requests.post", return_value=_ok_response({}, 200)) as mock_post:
            fs.trigger_default_semantic_model("ws-abc", "lh-xyz", "tok")
        url = mock_post.call_args[0][0]
        assert "ws-abc" in url
        assert "lh-xyz" in url
        assert "createDefaultSemanticModel" in url

    def test_polls_on_202(self):
        accept_resp = MagicMock()
        accept_resp.status_code = 202
        accept_resp.headers = {"x-ms-operation-id": "op-sm-1"}
        accept_resp.text = "{}"
        with patch("requests.post", return_value=accept_resp), \
             patch("fabric_setup.poll_operation") as mock_poll:
            mock_poll.return_value = {"status": "Succeeded"}
            fs.trigger_default_semantic_model("ws1", "lh1", "tok")
        mock_poll.assert_called_once_with("op-sm-1", "tok", "default semantic model creation")


# ─── get_default_semantic_model (updated signature) ──────────────────────────

class TestGetDefaultSemanticModel:
    def test_returns_id_when_found_immediately(self):
        sm_body = {"value": [{"id": "sm-1", "displayName": "MyLH", "type": "SemanticModel"}]}
        with patch("fabric_setup.trigger_default_semantic_model"), \
             patch("requests.request", return_value=_ok_response(sm_body)):
            result = fs.get_default_semantic_model("ws1", "MyLH", "lh1", "tok", retries=1)
        assert result == "sm-1"

    def test_returns_none_when_not_found(self):
        empty_body = {"value": []}
        with patch("fabric_setup.trigger_default_semantic_model"), \
             patch("requests.request", return_value=_ok_response(empty_body)), \
             patch("fabric_setup._find_semantic_model_via_powerbi_api", return_value=None), \
             patch("time.sleep"):
            result = fs.get_default_semantic_model("ws1", "MyLH", "lh1", "tok", retries=1)
        assert result is None

    def test_calls_trigger_before_polling(self):
        empty_body = {"value": []}
        with patch("fabric_setup.trigger_default_semantic_model") as mock_trigger, \
             patch("requests.request", return_value=_ok_response(empty_body)), \
             patch("fabric_setup._find_semantic_model_via_powerbi_api", return_value=None), \
             patch("time.sleep"):
            fs.get_default_semantic_model("ws1", "MyLH", "lh1", "tok", retries=1)
        mock_trigger.assert_called_once_with("ws1", "lh1", "tok")
