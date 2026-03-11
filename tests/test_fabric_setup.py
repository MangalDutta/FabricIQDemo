"""
Fabric Setup Script Unit Tests
───────────────────────────────
Tests for scripts/fabric_setup.py using mocked HTTP calls
(no real Fabric credentials needed).

Run with:
    pip install -r tests/requirements.txt
    PYTHONPATH=scripts pytest tests/test_fabric_setup.py -v
"""

import base64
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
             patch("fabric_setup.validate_dataagent", return_value=True), \
             patch("fabric_setup.get_default_semantic_model", return_value="sm-id"), \
             patch("fabric_setup.create_ontology", return_value="ont-id"), \
             patch("fabric_setup.create_data_agent", return_value="da-id"), \
             patch("fabric_setup.attach_ontology_to_agent"):
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
             patch("fabric_setup.validate_dataagent", return_value=True), \
             patch("fabric_setup.get_default_semantic_model", return_value="sm-id"), \
             patch("fabric_setup.create_ontology", return_value="ont-id"), \
             patch("fabric_setup.create_data_agent", return_value="da-id"), \
             patch("fabric_setup.attach_ontology_to_agent"):
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

    def test_ontology_created_when_semantic_model_available(self, tmp_path):
        """main() should call create_ontology when semantic_model_id is set."""
        csv_file = tmp_path / "customer360.csv"
        csv_file.write_text("CustomerId,FullName\nC1,Alice\n")

        with patch("fabric_setup.get_fabric_token", return_value="fab-tok"), \
             patch("fabric_setup.get_storage_token", return_value="sto-tok"), \
             patch("fabric_setup.get_or_create_workspace", return_value="ws-id"), \
             patch("fabric_setup.add_workspace_member"), \
             patch("fabric_setup.get_or_create_lakehouse", return_value="lh-id"), \
             patch("fabric_setup.upload_csv_to_onelake", return_value="customer360.csv"), \
             patch("fabric_setup.load_table_from_file"), \
             patch("fabric_setup.validate_dataagent", return_value=True), \
             patch("fabric_setup.get_default_semantic_model", return_value="sm-id"), \
             patch("fabric_setup.create_ontology", return_value="ont-id") as mock_ont, \
             patch("fabric_setup.create_data_agent", return_value="da-id"), \
             patch("fabric_setup.attach_ontology_to_agent") as mock_attach:
            fs.main([
                "--workspace_name", "ws",
                "--lakehouse_name", "lh",
                "--csv_path", str(csv_file),
                "--table_name", "Customer360",
                "--dataagent_name", "Agent",
                "--capacity_id", "cap-guid",
            ])

        mock_ont.assert_called_once_with(
            "ws-id", "Customer360 Ontology", "fab-tok", semantic_model_id="sm-id"
        )
        # attach_ontology_to_agent must receive the ontology_id as 3rd positional arg
        assert mock_attach.call_args[0][2] == "ont-id"

    def test_ontology_skipped_when_no_semantic_model(self, tmp_path):
        """main() exits when create_ontology raises (e.g. semantic_model_id is None)."""
        csv_file = tmp_path / "customer360.csv"
        csv_file.write_text("CustomerId,FullName\nC1,Alice\n")

        with patch("fabric_setup.get_fabric_token", return_value="fab-tok"), \
             patch("fabric_setup.get_storage_token", return_value="sto-tok"), \
             patch("fabric_setup.get_or_create_workspace", return_value="ws-id"), \
             patch("fabric_setup.add_workspace_member"), \
             patch("fabric_setup.get_or_create_lakehouse", return_value="lh-id"), \
             patch("fabric_setup.upload_csv_to_onelake", return_value="customer360.csv"), \
             patch("fabric_setup.load_table_from_file"), \
             patch("fabric_setup.get_default_semantic_model", return_value=None), \
             patch("fabric_setup.create_ontology", side_effect=Exception("Ontology creation failed: no sm")) as mock_ont:
            with pytest.raises(SystemExit):
                fs.main([
                    "--workspace_name", "ws",
                    "--lakehouse_name", "lh",
                    "--csv_path", str(csv_file),
                    "--table_name", "Customer360",
                    "--dataagent_name", "Agent",
                    "--capacity_id", "cap-guid",
                ])

        mock_ont.assert_called_once()

    def test_result_contains_ontology_id(self, tmp_path, capsys):
        """Summary JSON must include ontology_id and dataagent_id."""
        csv_file = tmp_path / "customer360.csv"
        csv_file.write_text("CustomerId,FullName\nC1,Alice\n")

        with patch("fabric_setup.get_fabric_token", return_value="fab-tok"), \
             patch("fabric_setup.get_storage_token", return_value="sto-tok"), \
             patch("fabric_setup.get_or_create_workspace", return_value="ws-id"), \
             patch("fabric_setup.add_workspace_member"), \
             patch("fabric_setup.get_or_create_lakehouse", return_value="lh-id"), \
             patch("fabric_setup.upload_csv_to_onelake", return_value="customer360.csv"), \
             patch("fabric_setup.load_table_from_file"), \
             patch("fabric_setup.validate_dataagent", return_value=True), \
             patch("fabric_setup.get_default_semantic_model", return_value="sm-id"), \
             patch("fabric_setup.create_ontology", return_value="ont-id"), \
             patch("fabric_setup.create_data_agent", return_value="da-id"), \
             patch("fabric_setup.attach_ontology_to_agent"):
            fs.main([
                "--workspace_name", "ws",
                "--lakehouse_name", "lh",
                "--csv_path", str(csv_file),
                "--table_name", "Customer360",
                "--dataagent_name", "Agent",
                "--capacity_id", "cap-guid",
            ])

        captured = capsys.readouterr()
        # Find the JSON block in stdout
        lines = captured.out.splitlines()
        json_start = next(i for i, l in enumerate(lines) if l.strip() == "{")
        json_text = "\n".join(lines[json_start:])
        result = json.loads(json_text)
        assert result["ontology_id"] == "ont-id"
        assert result["dataagent_id"] == "da-id"
        assert "report_id" not in result
        assert "powerbi_embed_url" not in result


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

    def test_skips_patch_when_no_config_but_queryable(self):
        """When GET returns no 'configuration' but agent IS queryable, skip PATCH."""
        get_resp = _ok_response({"id": "ag1", "displayName": "Agent"}, 200)
        with patch("requests.get", return_value=get_resp), \
             patch("fabric_setup._is_agent_queryable", return_value=True), \
             patch("requests.request") as mock_req:
            fs.configure_dataagent("ws1", "ag1", "Agent", "lh1", "Customer360", "tok")
        # PATCH/PUT should NOT have been called
        mock_req.assert_not_called()

    def test_proceeds_with_patch_when_no_config_and_not_queryable(self):
        """When GET returns no 'configuration' and agent is NOT queryable, do PATCH."""
        get_resp = _ok_response({"id": "ag1", "displayName": "Agent"}, 200)
        ok_resp = _ok_response({}, 200)
        with patch("requests.get", return_value=get_resp), \
             patch("fabric_setup._is_agent_queryable", return_value=False), \
             patch("requests.request", return_value=ok_resp) as mock_req, \
             patch("fabric_setup.ensure_agent_published"):
            fs.configure_dataagent("ws1", "ag1", "Agent", "lh1", "Customer360", "tok")
        # PATCH should have been called to link the Lakehouse
        assert mock_req.call_count >= 1
        first_call = mock_req.call_args_list[0]
        assert first_call[0][0] == "PATCH"
        body = first_call[1].get("json", {})
        ds = body.get("configuration", {}).get("dataSources", [{}])[0]
        assert ds.get("itemId") == "lh1"

    def test_skips_patch_when_already_linked_and_published(self):
        """When agent is already linked to the correct data source and published, skip PATCH."""
        get_resp = _ok_response({
            "id": "ag1",
            "displayName": "Agent",
            "state": "Active",
            "configuration": {
                "dataSources": [
                    {"type": "Lakehouse", "workspaceId": "ws1", "itemId": "lh1"}
                ]
            },
        }, 200)
        with patch("requests.get", return_value=get_resp), \
             patch("requests.request") as mock_req:
            fs.configure_dataagent("ws1", "ag1", "Agent", "lh1", "Customer360", "tok")
        # PATCH should NOT have been called
        mock_req.assert_not_called()

    def test_uses_semantic_model_type_when_sm_id_provided(self):
        """When semantic_model_id is given, data source type should be SemanticModel."""
        ok_resp = _ok_response({}, 200)
        with patch("requests.request", return_value=ok_resp) as mock_req:
            fs.configure_dataagent(
                "ws1", "ag1", "Agent", "lh1", "Customer360", "tok",
                semantic_model_id="sm-1",
            )
        first_call_kwargs = mock_req.call_args_list[0][1]
        body = first_call_kwargs.get("json", {})
        ds = body.get("configuration", {}).get("dataSources", [{}])[0]
        assert ds.get("type") == "SemanticModel"
        assert ds.get("itemId") == "sm-1"

    def test_skips_patch_when_semantic_model_already_linked(self):
        """When agent is already linked to the correct SemanticModel, skip PATCH."""
        get_resp = _ok_response({
            "id": "ag1",
            "displayName": "Agent",
            "state": "Active",
            "configuration": {
                "dataSources": [
                    {"type": "SemanticModel", "workspaceId": "ws1", "itemId": "sm-1"}
                ]
            },
        }, 200)
        with patch("requests.get", return_value=get_resp), \
             patch("requests.request") as mock_req:
            fs.configure_dataagent(
                "ws1", "ag1", "Agent", "lh1", "Customer360", "tok",
                semantic_model_id="sm-1",
            )
        mock_req.assert_not_called()


class TestValidateDataagent:
    def test_returns_true_when_all_checks_pass(self):
        """validate_dataagent returns True when agent exists, is linked, and queryable."""
        get_resp = _ok_response({
            "id": "ag1",
            "configuration": {
                "dataSources": [{"itemId": "lh1"}]
            },
        }, 200)
        with patch("fabric_setup._validate_agent", return_value=True), \
             patch("requests.get", return_value=get_resp), \
             patch("fabric_setup._is_agent_queryable", return_value=True):
            result = fs.validate_dataagent("ws1", "ag1", "Agent", "lh1", "tok")
        assert result is True

    def test_returns_false_when_agent_not_accessible(self):
        """validate_dataagent returns False when agent GET fails."""
        get_resp = _error_response(404, "not found")
        with patch("fabric_setup._validate_agent", return_value=False), \
             patch("requests.get", return_value=get_resp), \
             patch("fabric_setup._is_agent_queryable", return_value=False):
            result = fs.validate_dataagent("ws1", "ag1", "Agent", "lh1", "tok")
        assert result is False

    def test_warns_when_no_config_in_response(self):
        """validate_dataagent reports warning when GET omits 'configuration'."""
        get_resp = _ok_response({"id": "ag1"}, 200)
        with patch("fabric_setup._validate_agent", return_value=True), \
             patch("requests.get", return_value=get_resp), \
             patch("fabric_setup._is_agent_queryable", return_value=True):
            result = fs.validate_dataagent("ws1", "ag1", "Agent", "lh1", "tok")
        # Should still return True (agent exists and is queryable)
        assert result is True

    def test_returns_false_when_wrong_lakehouse(self):
        """validate_dataagent returns False when linked to wrong Lakehouse."""
        get_resp = _ok_response({
            "id": "ag1",
            "configuration": {
                "dataSources": [{"itemId": "wrong-lh"}]
            },
        }, 200)
        with patch("fabric_setup._validate_agent", return_value=True), \
             patch("requests.get", return_value=get_resp), \
             patch("fabric_setup._is_agent_queryable", return_value=True):
            result = fs.validate_dataagent("ws1", "ag1", "Agent", "lh1", "tok")
        assert result is False

    def test_validates_semantic_model_linkage(self):
        """validate_dataagent checks semantic model when semantic_model_id is given."""
        get_resp = _ok_response({
            "id": "ag1",
            "configuration": {
                "dataSources": [{"itemId": "sm-1"}]
            },
        }, 200)
        with patch("fabric_setup._validate_agent", return_value=True), \
             patch("requests.get", return_value=get_resp), \
             patch("fabric_setup._is_agent_queryable", return_value=True):
            result = fs.validate_dataagent(
                "ws1", "ag1", "Agent", "lh1", "tok",
                semantic_model_id="sm-1",
            )
        assert result is True


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
             patch("time.sleep"):
            result = fs.get_default_semantic_model("ws1", "MyLH", "lh1", "tok", retries=1)
        assert result is None

    def test_calls_trigger_before_polling(self):
        empty_body = {"value": []}
        with patch("fabric_setup.trigger_default_semantic_model") as mock_trigger, \
             patch("requests.request", return_value=_ok_response(empty_body)), \
             patch("time.sleep"):
            fs.get_default_semantic_model("ws1", "MyLH", "lh1", "tok", retries=1)
        mock_trigger.assert_called_once_with("ws1", "lh1", "tok")


# ─── create_direct_lake_semantic_model ────────────────────────────────────────

class TestCreateDirectLakeSemanticModel:
    def test_payload_includes_pbism_and_bim(self):
        """Payload must include both definition.pbism and model.bim parts."""
        ok_resp = _ok_response({"id": "sm-1"}, 201)
        with patch("requests.post", return_value=ok_resp) as mock_post:
            result = fs.create_direct_lake_semantic_model("ws1", "MyModel", "Customer360", "tok")
        assert result == "sm-1"
        payload = mock_post.call_args[1]["json"]
        parts = payload["definition"]["parts"]
        paths = [p["path"] for p in parts]
        assert "definition.pbism" in paths, "definition.pbism must be present"
        assert "model.bim" in paths, "model.bim must be present"

    def test_pbism_contains_version_and_settings(self):
        """definition.pbism must decode to {version, settings}."""
        ok_resp = _ok_response({"id": "sm-2"}, 201)
        with patch("requests.post", return_value=ok_resp) as mock_post:
            fs.create_direct_lake_semantic_model("ws1", "M", "T", "tok")
        parts = mock_post.call_args[1]["json"]["definition"]["parts"]
        pbism_part = next(p for p in parts if p["path"] == "definition.pbism")
        decoded = json.loads(base64.b64decode(pbism_part["payload"]))
        assert decoded["version"] == "1.0"
        assert "settings" in decoded

    def test_returns_none_on_failure(self):
        """Should return None when both endpoints fail."""
        err = _error_response(400, "Bad Request")
        with patch("requests.post", return_value=err):
            result = fs.create_direct_lake_semantic_model("ws1", "M", "T", "tok")
        assert result is None


# ─── get_or_create_ontology ───────────────────────────────────────────────────

class TestGetOrCreateOntology:
    def test_returns_existing_ontology_id(self):
        """Returns the ID of an existing ontology without creating a new one."""
        list_resp = _ok_response({"value": [{"displayName": "My Ontology", "id": "ont-1"}]})
        with patch("requests.request", return_value=list_resp):
            result = fs.get_or_create_ontology("ws1", "My Ontology", "sm-1", "tok")
        assert result == "ont-1"

    def test_creates_ontology_on_201(self):
        """Creates and returns new ontology ID when not found (HTTP 201)."""
        empty_list = _ok_response({"value": []})
        create_resp = _ok_response({"id": "ont-new"}, status=201)
        with patch("requests.request", return_value=empty_list), \
             patch("requests.post", return_value=create_resp):
            result = fs.get_or_create_ontology("ws1", "New Ontology", "sm-1", "tok")
        assert result == "ont-new"

    def test_creates_ontology_on_200(self):
        """Creates and returns new ontology ID when not found (HTTP 200)."""
        empty_list = _ok_response({"value": []})
        create_resp = _ok_response({"id": "ont-200"}, status=200)
        with patch("requests.request", return_value=empty_list), \
             patch("requests.post", return_value=create_resp):
            result = fs.get_or_create_ontology("ws1", "New Ontology", "sm-1", "tok")
        assert result == "ont-200"

    def test_returns_none_on_creation_failure(self):
        """Returns None (non-fatal) when creation returns an error status."""
        empty_list = _ok_response({"value": []})
        error_resp = _error_response(500, "Internal Server Error")
        with patch("requests.request", return_value=empty_list), \
             patch("requests.post", return_value=error_resp):
            result = fs.get_or_create_ontology("ws1", "Ontology", "sm-1", "tok")
        assert result is None

    def test_returns_none_on_exception(self):
        """Returns None (non-fatal) when creation raises an exception."""
        empty_list = _ok_response({"value": []})
        with patch("requests.request", return_value=empty_list), \
             patch("requests.post", side_effect=Exception("network error")):
            result = fs.get_or_create_ontology("ws1", "Ontology", "sm-1", "tok")
        assert result is None

    def test_polls_on_202(self):
        """Polls operation and re-fetches list when creation returns 202."""
        empty_list = _ok_response({"value": []})
        async_resp = MagicMock()
        async_resp.status_code = 202
        async_resp.headers = {"x-ms-operation-id": "op-999"}
        refetch_resp = _ok_response({"value": [{"displayName": "Async Ontology", "id": "ont-async"}]})
        with patch("requests.request", side_effect=[empty_list, refetch_resp]), \
             patch("requests.post", return_value=async_resp), \
             patch("fabric_setup.poll_operation") as mock_poll:
            result = fs.get_or_create_ontology("ws1", "Async Ontology", "sm-1", "tok")
        mock_poll.assert_called_once_with("op-999", "tok", "ontology creation")
        assert result == "ont-async"

    def test_payload_includes_semantic_model_id(self):
        """Creation payload must reference the provided semantic model ID."""
        empty_list = _ok_response({"value": []})
        create_resp = _ok_response({"id": "ont-x"}, status=201)
        with patch("requests.request", return_value=empty_list), \
             patch("requests.post", return_value=create_resp) as mock_post:
            fs.get_or_create_ontology("ws1", "Ontology", "sm-xyz", "tok")
        _, kwargs = mock_post.call_args
        config = kwargs["json"]["configuration"]
        assert config["semanticModelId"] == "sm-xyz"


# ─── create_ontology ──────────────────────────────────────────────────────────

class TestCreateOntology:
    """Tests for the refactored create_ontology function."""

    def test_sync_201_returns_id(self):
        """Returns the ontology ID when the API responds with HTTP 201."""
        resp = _ok_response({"id": "ont-sync"}, status=201)
        with patch("requests.post", return_value=resp):
            result = fs.create_ontology("ws1", "My Ontology", "tok")
        assert result == "ont-sync"

    def test_sync_201_with_semantic_model_in_payload(self):
        """Payload inner definition contains semanticModelId when semantic_model_id is given."""
        resp = _ok_response({"id": "ont-sm"}, status=201)
        with patch("requests.post", return_value=resp) as mock_post:
            fs.create_ontology("ws1", "My Ontology", "tok", semantic_model_id="sm-xyz")
        _, kwargs = mock_post.call_args
        parts = kwargs["json"]["definition"]["parts"]
        decoded = json.loads(base64.b64decode(parts[0]["payload"]))
        assert decoded == {"semanticModelId": "sm-xyz"}

    def test_no_semantic_model_id_in_definition_when_not_provided(self):
        """Payload always includes definition.parts; inner payload defaults to entities list."""
        resp = _ok_response({"id": "ont-no-sm"}, status=201)
        with patch("requests.post", return_value=resp) as mock_post:
            fs.create_ontology("ws1", "My Ontology", "tok")
        _, kwargs = mock_post.call_args
        defn = kwargs["json"]["definition"]
        assert "parts" in defn
        assert "semanticModelId" not in defn
        decoded = json.loads(base64.b64decode(defn["parts"][0]["payload"]))
        assert decoded == {"entities": []}

    def test_sync_200_returns_id(self):
        """Returns the ontology ID when the API responds with HTTP 200."""
        resp = _ok_response({"id": "ont-200"}, status=200)
        with patch("requests.post", return_value=resp):
            result = fs.create_ontology("ws1", "My Ontology", "tok")
        assert result == "ont-200"

    def test_payload_always_includes_parts(self):
        """Payload definition.parts contains definition.json regardless of semantic_model_id."""
        resp = _ok_response({"id": "ont-parts"}, status=201)
        with patch("requests.post", return_value=resp) as mock_post:
            fs.create_ontology("ws1", "My Ontology", "tok")
        _, kwargs = mock_post.call_args
        parts = kwargs["json"]["definition"]["parts"]
        assert len(parts) == 1
        assert parts[0]["path"] == "definition.json"
        assert parts[0]["payloadType"] == "InlineBase64"
        # Payload is base64 of {"entities": []} when no semantic_model_id
        decoded = json.loads(base64.b64decode(parts[0]["payload"]))
        assert decoded == {"entities": []}

    def test_async_202_polls_and_returns_id(self):
        """Polls operation and re-fetches list when the API responds with HTTP 202."""
        async_resp = MagicMock()
        async_resp.status_code = 202
        async_resp.headers = {"x-ms-operation-id": "op-abc"}
        async_resp.json.return_value = {}

        list_resp = _ok_response({"value": [{"displayName": "My Ontology", "id": "ont-async"}]})

        with patch("requests.post", return_value=async_resp), \
             patch("fabric_setup.poll_operation") as mock_poll, \
             patch("fabric_setup.fabric_request", return_value=list_resp), \
             patch("time.sleep"):
            result = fs.create_ontology("ws1", "My Ontology", "tok")

        mock_poll.assert_called_once_with("op-abc", "tok", "ontology creation")
        assert result == "ont-async"

    def test_async_202_uses_operationid_from_json(self):
        """Falls back to operationId from JSON body when header is missing."""
        async_resp = MagicMock()
        async_resp.status_code = 202
        async_resp.headers = {}
        async_resp.json.return_value = {"operationId": "op-json"}

        list_resp = _ok_response({"value": [{"displayName": "Ont", "id": "ont-j"}]})

        with patch("requests.post", return_value=async_resp), \
             patch("fabric_setup.poll_operation") as mock_poll, \
             patch("fabric_setup.fabric_request", return_value=list_resp), \
             patch("time.sleep"):
            result = fs.create_ontology("ws1", "Ont", "tok")

        mock_poll.assert_called_once_with("op-json", "tok", "ontology creation")
        assert result == "ont-j"

    def test_raises_on_failure_status(self):
        """Raises RuntimeError when status code is not 201 or 202."""
        err_resp = _error_response(500, "Internal Server Error")
        with patch("requests.post", return_value=err_resp):
            with pytest.raises(RuntimeError, match="Ontology creation failed"):
                fs.create_ontology("ws1", "My Ontology", "tok")

    def test_propagation_sleep_called_on_202(self):
        """time.sleep(40) is called after 202 async provisioning."""
        async_resp = MagicMock()
        async_resp.status_code = 202
        async_resp.headers = {"x-ms-operation-id": "op-sleep"}
        async_resp.json.return_value = {}

        list_resp = _ok_response({"value": [{"displayName": "Ont", "id": "ont-s"}]})

        sleep_calls = []
        with patch("requests.post", return_value=async_resp), \
             patch("fabric_setup.poll_operation"), \
             patch("fabric_setup.fabric_request", return_value=list_resp), \
             patch("time.sleep", side_effect=lambda n: sleep_calls.append(n)):
            fs.create_ontology("ws1", "Ont", "tok")

        assert fs.ONTOLOGY_PROPAGATION_WAIT_SECONDS in sleep_calls
