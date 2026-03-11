#!/usr/bin/env python3
"""
Fabric Customer360 Setup Script
--------------------------------
Creates/finds Fabric workspace, binds to capacity, creates Lakehouse,
uploads CSV to OneLake Files, loads CSV as a Delta table, creates the
Fabric Data Agent connected to that Lakehouse, locates the default
Semantic Model, creates a Fabric IQ Ontology, and attaches the ontology
to the Data Agent.

Usage:
    python fabric_setup.py \
        --workspace_name fabricagentdemo \
        --lakehouse_name Customer360Lakehouse \
        --csv_path sample-data/customer360.csv \
        --table_name Customer360 \
        --dataagent_name Customer360Agent \
        --capacity_id <GUID>
"""

import argparse
import base64
import json
import os
import re
import sys
import time
import uuid
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests
from azure.identity import DefaultAzureCredential

# ─── Fabric / OneLake endpoints ──────────────────────────────────────────────
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
STORAGE_SCOPE = "https://storage.azure.com/.default"   # OneLake ADLS Gen2 upload
FABRIC_BASE_URL = "https://api.fabric.microsoft.com/v1"
ONELAKE_DFS_URL = "https://onelake.dfs.fabric.microsoft.com"

# ─── Polling config ──────────────────────────────────────────────────────────
POLL_INTERVAL_SECONDS = 5
POLL_MAX_ATTEMPTS = 60   # 5 min max

# ─── Retry config for ItemDisplayNameNotAvailableYet (400 + isRetriable) ─────
NAME_RETRY_MAX = 10        # up to 10 attempts (~5 min)
NAME_RETRY_WAIT = 30       # seconds between retries

# ─── Ontology provisioning ────────────────────────────────────────────────────
# Fabric creates 3 backend resources (Ontology, Graph Model, Ontology Lakehouse)
# after an async ontology creation.  Allow this long for all of them to appear.
ONTOLOGY_PROPAGATION_WAIT_SECONDS = 40


def sanitize_name(name: str) -> str:
    """Return a Fabric-safe display name (alphanumeric / underscores, max 90 chars)."""
    name = re.sub(r'[^A-Za-z0-9_]', '_', name)
    if not name or not name[0].isalpha():
        name = "O_" + name
    return name[:90]


def get_fabric_token() -> str:
    credential = DefaultAzureCredential()
    return credential.get_token(FABRIC_SCOPE).token


def get_storage_token() -> str:
    """Token for OneLake ADLS Gen2 file uploads (storage.azure.com scope)."""
    credential = DefaultAzureCredential()
    return credential.get_token(STORAGE_SCOPE).token


def _is_name_not_available_yet(resp: requests.Response) -> bool:
    """Return True when Fabric signals the display name is temporarily unavailable."""
    if resp.status_code != 400:
        return False
    try:
        body = resp.json()
        return (
            body.get("errorCode") == "ItemDisplayNameNotAvailableYet"
            and body.get("isRetriable") is True
        )
    except Exception:
        return False


# ─── Generic Fabric REST helper ──────────────────────────────────────────────

def fabric_request(
    method: str,
    path: str,
    token: str,
    *,
    expected_status: Optional[int] = None,
    **kwargs,
) -> requests.Response:
    """Makes a request to the Fabric REST API, raising on unexpected status."""
    url = f"{FABRIC_BASE_URL}{path}"
    headers = kwargs.pop("headers", {}) or {}
    headers["Authorization"] = f"Bearer {token}"
    headers.setdefault("Content-Type", "application/json")

    resp = requests.request(method, url, headers=headers, **kwargs)

    if expected_status is not None:
        ok = resp.status_code == expected_status
    else:
        ok = resp.ok  # 2xx

    if not ok:
        print(f"❌ Fabric API error: {method} {path}")
        print(f"   Status : {resp.status_code}")
        print(f"   Response: {resp.text[:500]}")
        raise RuntimeError(
            f"Fabric API failed [{resp.status_code}]: {resp.text[:200]}"
        )
    return resp


# ─── Long-running operation poller ───────────────────────────────────────────

def poll_operation(operation_id: str, token: str, description: str = "operation") -> Dict:
    """
    Polls GET /operations/{operationId} until the operation reaches a terminal
    state (Succeeded / Failed / Cancelled).  Returns the final status object.
    """
    print(f"⏳ Polling {description} (operationId={operation_id})...")
    for attempt in range(1, POLL_MAX_ATTEMPTS + 1):
        resp = fabric_request("GET", f"/operations/{operation_id}", token)
        data = resp.json()
        status = data.get("status", "")

        if status == "Succeeded":
            print(f"   ✅ {description} succeeded (attempt {attempt})")
            return data
        if status in ("Failed", "Cancelled"):
            error = data.get("error", {})
            raise RuntimeError(
                f"{description} {status}: {error.get('message', json.dumps(data))}"
            )

        print(
            f"   ↻ [{attempt}/{POLL_MAX_ATTEMPTS}] Status: {status} — "
            f"waiting {POLL_INTERVAL_SECONDS}s..."
        )
        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError(
        f"{description} did not complete within "
        f"{POLL_MAX_ATTEMPTS * POLL_INTERVAL_SECONDS}s."
    )


# ─── Workspace ───────────────────────────────────────────────────────────────

def list_workspaces(token: str) -> List[Dict[str, Any]]:
    """List all workspaces accessible to the caller, handling pagination."""
    all_workspaces: List[Dict[str, Any]] = []
    url = f"{FABRIC_BASE_URL}/workspaces"
    headers = {"Authorization": f"Bearer {token}"}

    while url:
        resp = requests.get(url, headers=headers, timeout=30)
        if not resp.ok:
            print(f"   ⚠️  Workspace list failed [{resp.status_code}]: {resp.text[:200]}")
            break
        data = resp.json()
        all_workspaces.extend(data.get("value", []) or [])
        # Handle pagination via continuationToken or @odata.nextLink
        continuation = data.get("continuationToken") or data.get("continuationUri")
        next_link = data.get("@odata.nextLink")
        if continuation and not next_link:
            url = f"{FABRIC_BASE_URL}/workspaces?continuationToken={continuation}"
        elif next_link:
            url = next_link
        else:
            url = None

    return all_workspaces


def find_workspace_by_name_admin(workspace_name: str, token: str) -> Optional[str]:
    """
    Try the Admin API to find a workspace by name.
    This works even if the SP is not a member of the workspace.
    Requires Fabric Admin permissions on the service principal.
    """
    try:
        resp = requests.get(
            f"{FABRIC_BASE_URL}/admin/workspaces",
            headers={"Authorization": f"Bearer {token}"},
            params={"nameContains": workspace_name},
            timeout=30,
        )
        if resp.ok:
            for ws in resp.json().get("workspaces", []) or []:
                if ws.get("name") == workspace_name or ws.get("displayName") == workspace_name:
                    return ws.get("id")
    except Exception as exc:
        print(f"   Admin workspace lookup failed (non-fatal): {exc}")
    return None


def _assign_capacity(ws_id: str, capacity_id: str, token: str) -> None:
    """Assign workspace to a Fabric capacity, ignoring errors gracefully."""
    try:
        fabric_request(
            "POST",
            f"/workspaces/{ws_id}/assignToCapacity",
            token,
            json={"capacityId": capacity_id},
        )
        print("   ✓ Workspace assigned to Fabric capacity!")
    except Exception as exc:
        print(f"   ⚠️  Capacity assignment failed (non-fatal): {exc}")


def ensure_capacity_active(capacity_id: str, token: str) -> bool:
    """
    Checks the Fabric capacity state via GET /v1/capacities.
    If Paused, resumes it and polls until Active (up to 4 min).
    Returns True if the capacity is (or becomes) Active, False otherwise.
    """
    if not capacity_id:
        return False
    try:
        resp = requests.get(
            f"{FABRIC_BASE_URL}/capacities",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if not resp.ok:
            print(f"   [WARN] GET /capacities returned HTTP {resp.status_code} - cannot verify capacity state")
            return False

        target = None
        for cap in resp.json().get("value", []) or []:
            if cap.get("id", "").lower() == capacity_id.lower():
                target = cap
                break

        if not target:
            print(f"   [WARN] Capacity {capacity_id} not found in /capacities listing")
            return False

        state = target.get("state", "Unknown")
        display_name = target.get("displayName", capacity_id)
        print(f"   Capacity '{display_name}' (ID: {capacity_id}) state: {state}")

        if state == "Active":
            return True

        if state == "Paused":
            print(f"   Resuming paused capacity '{display_name}'...")
            resume_resp = requests.post(
                f"{FABRIC_BASE_URL}/capacities/{capacity_id}/resume",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                timeout=60,
            )
            if not resume_resp.ok:
                print(f"   [WARN] Resume returned HTTP {resume_resp.status_code}: {resume_resp.text[:200]}")
                return False
            print("   Polling until capacity is Active (up to 4 min)...")
            for attempt in range(1, 25):  # 24 × 10 s = 4 min
                time.sleep(10)
                check = requests.get(
                    f"{FABRIC_BASE_URL}/capacities",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=30,
                )
                if check.ok:
                    for cap in check.json().get("value", []) or []:
                        if cap.get("id", "").lower() == capacity_id.lower():
                            new_state = cap.get("state", "Unknown")
                            print(f"   [{attempt}] Capacity state: {new_state}")
                            if new_state == "Active":
                                print("   ✓ Capacity is now Active")
                                return True
                            break
            print("   [WARN] Capacity did not become Active within 4 minutes")
            return False

        print(f"   [WARN] Unexpected capacity state: {state}")
        return False
    except Exception as exc:
        print(f"   [WARN] Capacity state check failed (non-fatal): {exc}")
        return False


def add_workspace_member(
    workspace_id: str,
    principal_id: str,
    token: str,
    role: str = "Contributor",
) -> None:
    """
    Adds a service principal to the Fabric workspace via
    POST /v1/workspaces/{workspaceId}/roleAssignments

    This is CRITICAL: the App Service Managed Identity (MI) must be a
    workspace member for the Fabric Data Agent query API to return data.
    Without workspace membership, Fabric returns HTTP 404 EntityNotFound
    on ALL resource requests from that MI (not 401/403 -- it hides resources
    from non-members as a security measure).

    Silently succeeds if the principal is already a member (400 errorCode
    PrincipalAlreadyExists is treated as success).
    """
    if not principal_id:
        print("   ⚠️  No principal_id provided -- skipping workspace member assignment")
        return

    print(f"   Adding service principal '{principal_id}' to workspace as {role}...")
    payload = {
        "principal": {
            "id": principal_id,
            "type": "ServicePrincipal",
        },
        "role": role,
    }
    try:
        resp = requests.post(
            f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/roleAssignments",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            print(f"   ✓ Service principal added to workspace as {role}.")
            return
        if resp.status_code in (400, 409):
            try:
                body = resp.json()
                err_code = body.get("errorCode", "")
                err_msg = body.get("message", "")
            except Exception:
                err_code, err_msg = "", ""
            if (
                "AlreadyExists" in err_code
                or "AlreadyHas" in err_code
                or "already" in err_msg.lower()
            ):
                print(f"   ✓ Service principal is already a workspace member ({err_code or 'OK'}).")
                return
            print(f"   ⚠️  roleAssignments returned {resp.status_code}: {err_code} – {err_msg[:200]}")
            return
        print(
            f"   ⚠️  roleAssignments returned HTTP {resp.status_code}: {resp.text[:300]} "
            "(non-fatal — you may need to add the App Service managed identity to the "
            "Fabric workspace manually in the Fabric portal.)"
        )
    except Exception as exc:
        print(
            f"   ⚠️  add_workspace_member failed (non-fatal): {exc}\n"
            "   Grant the App Service managed identity access manually:\n"
            f"   Fabric portal -> Workspace '{workspace_id}' -> Manage access -> "
            f"Add '{principal_id}' as Contributor."
        )


def get_or_create_workspace(
    workspace_name: str,
    capacity_id: Optional[str],
    token: str,
) -> str:
    print(f"🔍 Looking for workspace: {workspace_name}")

    # --- Pass 1: list workspaces the SP has access to (with pagination) ---
    workspaces = list_workspaces(token)
    for ws in workspaces:
        if ws.get("displayName") == workspace_name:
            ws_id = ws["id"]
            print(f"   ✓ Found existing workspace: {workspace_name} (ID: {ws_id})")
            current_capacity = ws.get("capacityId", "")
            if capacity_id and (not current_capacity or current_capacity.lower() != capacity_id.lower()):
                print(f"   Reassigning to capacityId: {capacity_id}")
                _assign_capacity(ws_id, capacity_id, token)
            return ws_id

    # --- Pass 2: try creating, handle 409 (already exists) gracefully ---
    print(f"   Workspace not found in listing. Attempting to create: {workspace_name}")
    payload: Dict[str, Any] = {"displayName": workspace_name}
    if capacity_id:
        payload["capacityId"] = capacity_id

    create_resp = requests.post(
        f"{FABRIC_BASE_URL}/workspaces",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload,
        timeout=60,
    )

    if create_resp.status_code in (200, 201):
        location = create_resp.headers.get("Location", "")
        workspace_id = urlparse(location).path.rstrip("/").split("/")[-1] if location else ""
        if not workspace_id:
            workspace_id = create_resp.json().get("id", "")
        if not workspace_id:
            raise RuntimeError(
                "Workspace created but could not determine workspace ID: "
                "neither the Location header nor the response body contained an ID."
            )
        print(f"   ✓ Created workspace: {workspace_name} (ID: {workspace_id})")
        return workspace_id

    if create_resp.status_code == 409:
        # Workspace already exists but SP isn't a member — try to find it
        print(f"   Workspace '{workspace_name}' already exists (409). Searching via Admin API...")
        ws_id = find_workspace_by_name_admin(workspace_name, token)
        if ws_id:
            print(f"   ✓ Found via Admin API: {workspace_name} (ID: {ws_id})")
            if capacity_id:
                _assign_capacity(ws_id, capacity_id, token)
            return ws_id

        # Last resort: add SP as member then re-list
        print("   Admin API did not find workspace. Re-listing after short wait...")
        time.sleep(5)
        workspaces2 = list_workspaces(token)
        for ws in workspaces2:
            if ws.get("displayName") == workspace_name:
                ws_id = ws["id"]
                print(f"   ✓ Found on retry: {workspace_name} (ID: {ws_id})")
                if capacity_id:
                    _assign_capacity(ws_id, capacity_id, token)
                return ws_id

        raise RuntimeError(
            f"Workspace '{workspace_name}' exists (409) but could not be located.\n"
            f"Please add the service principal as a Workspace Member/Admin in the Fabric portal\n"
            f"(Workspace '{workspace_name}' -> Manage access -> Add your SP by client ID),\n"
            f"then re-run the workflow."
        )

    # Any other error - surface it
    raise RuntimeError(
        f"Workspace creation failed [{create_resp.status_code}]: {create_resp.text[:300]}"
    )


# ─── Lakehouse ────────────────────────────────────────────────────────────────

def get_or_create_lakehouse(
    workspace_id: str,
    lakehouse_name: str,
    token: str,
) -> str:
    print(f"🏗️  Checking lakehouse: {lakehouse_name}")
    resp = fabric_request(
        "GET", f"/workspaces/{workspace_id}/items?type=Lakehouse", token
    )
    for item in resp.json().get("value", []) or []:
        if item.get("displayName") == lakehouse_name:
            lh_id = item["id"]
            print(f"✓ Found existing lakehouse: {lakehouse_name} (ID: {lh_id})")
            return lh_id

    print(f"📦 Creating lakehouse: {lakehouse_name}")
    resp = fabric_request(
        "POST",
        f"/workspaces/{workspace_id}/lakehouses",
        token,
        json={
            "displayName": lakehouse_name,
            "description": "Customer360 Lakehouse – created by accelerator deploy",
        },
    )
    # Creation may be synchronous (200/201) or long-running (202)
    if resp.status_code == 202:
        operation_id = resp.json().get("operationId") or resp.headers.get(
            "x-ms-operation-id"
        )
        if operation_id:
            poll_operation(operation_id, token, "lakehouse creation")

        # Re-fetch after creation
        resp2 = fabric_request(
            "GET", f"/workspaces/{workspace_id}/items?type=Lakehouse", token
        )
        for item in resp2.json().get("value", []) or []:
            if item.get("displayName") == lakehouse_name:
                lh_id = item["id"]
                print(f"✓ Lakehouse ready: {lakehouse_name} (ID: {lh_id})")
                return lh_id
        raise RuntimeError("Lakehouse not found after creation.")
    else:
        lh_id = resp.json().get("id")
        if not lh_id:
            raise RuntimeError("Lakehouse creation did not return an 'id' field.")
        print(f"✓ Created lakehouse: {lakehouse_name} (ID: {lh_id})")
        return lh_id


# ─── OneLake CSV upload (ADLS Gen2) ──────────────────────────────────────────

def upload_csv_to_onelake(
    workspace_id: str,
    lakehouse_id: str,
    csv_path: str,
    storage_token: str,
) -> str:
    """
    Uploads a local CSV file to the Lakehouse Files/ section in OneLake
    using the ADLS Gen2 multi-step API (create → append → flush).
    Returns the filename stored in OneLake (e.g. 'customer360.csv').
    """
    filename = os.path.basename(csv_path)
    base_url = f"{ONELAKE_DFS_URL}/{workspace_id}/{lakehouse_id}/Files/{filename}"
    headers = {
        "Authorization": f"Bearer {storage_token}",
        "x-ms-version": "2020-06-12",
    }

    print(f"📤 Uploading {csv_path} → OneLake Files/{filename}")

    with open(csv_path, "rb") as fh:
        file_bytes = fh.read()
    file_size = len(file_bytes)

    # 1. Create / overwrite the file resource
    create_resp = requests.put(
        base_url,
        headers={**headers, "Content-Length": "0"},
        params={"resource": "file", "overwrite": "true"},
        timeout=30,
    )
    if not create_resp.ok:
        raise RuntimeError(
            f"OneLake file create failed [{create_resp.status_code}]: "
            f"{create_resp.text[:300]}"
        )

    # 2. Append (upload) the content
    append_resp = requests.patch(
        base_url,
        headers={**headers, "Content-Length": str(file_size), "Content-Type": "text/plain"},
        params={"action": "append", "position": "0"},
        data=file_bytes,
        timeout=120,
    )
    if not append_resp.ok:
        raise RuntimeError(
            f"OneLake file append failed [{append_resp.status_code}]: "
            f"{append_resp.text[:300]}"
        )

    # 3. Flush / commit
    flush_resp = requests.patch(
        base_url,
        headers={**headers, "Content-Length": "0"},
        params={"action": "flush", "position": str(file_size)},
        timeout=30,
    )
    if not flush_resp.ok:
        raise RuntimeError(
            f"OneLake file flush failed [{flush_resp.status_code}]: "
            f"{flush_resp.text[:300]}"
        )

    print(f"✓ File uploaded to OneLake: Files/{filename} ({file_size} bytes)")
    return filename


# ─── Load table ──────────────────────────────────────────────────────────────

def _do_load_table_request(
    workspace_id: str,
    lakehouse_id: str,
    table_name: str,
    payload: Dict[str, Any],
    fabric_token: str,
) -> requests.Response:
    """POST to the Load Table endpoint and return the response (does not raise)."""
    url = (
        f"{FABRIC_BASE_URL}/workspaces/{workspace_id}"
        f"/lakehouses/{lakehouse_id}/tables/{table_name}/load"
    )
    return requests.post(
        url,
        headers={
            "Authorization": f"Bearer {fabric_token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=120,
    )


def _handle_load_response(resp: requests.Response, table_name: str, fabric_token: str) -> bool:
    """
    Handle a Load Table response.
    Returns True if load succeeded/accepted, False if failed.
    """
    if resp.status_code == 200:
        print(f"   ✓ Table '{table_name}' loaded (synchronous).")
        return True

    if resp.status_code == 202:
        # Async operation - extract operation id
        operation_id = (
            resp.headers.get("x-ms-operation-id")
            or resp.headers.get("x-ms-operationid")
            or (resp.json().get("operationId") if resp.text else None)
        )
        if not operation_id:
            location = resp.headers.get("Location", "")
            operation_id = location.rstrip("/").split("/")[-1] if location else None

        if operation_id:
            poll_operation(operation_id, fabric_token, f"load table '{table_name}'")
            print(f"   ✓ Table '{table_name}' loaded as Delta table.")
        else:
            print(
                f"   ⚠️  Load accepted (202) but no operation ID to poll — "
                "check Fabric portal to confirm table creation."
            )
        return True

    return False


def load_table_from_file(
    workspace_id: str,
    lakehouse_id: str,
    table_name: str,
    onelake_filename: str,
    fabric_token: str,
) -> None:
    """
    Triggers the Fabric Load Table API to convert an uploaded CSV in OneLake
    Files/ into a managed Delta table.

    Tries multiple payload variations to handle differences across Fabric
    API preview versions (different required fields, casing, etc.).
    """
    print(f"   Loading table '{table_name}' from Files/{onelake_filename}...")

    # Payload variations to try in order (most complete → most minimal)
    payloads = [
        # Variation 1: standard documented format
        {
            "relativePath": f"Files/{onelake_filename}",
            "pathType": "File",
            "format": "Csv",
            "formatOptions": {"header": "true", "inferSchema": "true"},
            "mode": "Overwrite",
        },
        # Variation 2: without mode (some preview versions don't accept it)
        {
            "relativePath": f"Files/{onelake_filename}",
            "pathType": "File",
            "format": "Csv",
            "formatOptions": {"header": "true", "inferSchema": "true"},
        },
        # Variation 3: just the filename, no Files/ prefix
        {
            "relativePath": onelake_filename,
            "pathType": "File",
            "format": "Csv",
            "formatOptions": {"header": "true", "inferSchema": "true"},
            "mode": "Overwrite",
        },
        # Variation 4: minimal - only required fields
        {
            "relativePath": f"Files/{onelake_filename}",
            "pathType": "File",
            "format": "Csv",
        },
    ]

    last_error = ""
    for i, payload in enumerate(payloads, 1):
        print(f"   Attempt {i}: {list(payload.keys())}")
        resp = _do_load_table_request(
            workspace_id, lakehouse_id, table_name, payload, fabric_token
        )
        if _handle_load_response(resp, table_name, fabric_token):
            return
        last_error = f"HTTP {resp.status_code}: {resp.text[:300]}"
        print(f"   Attempt {i} failed ({resp.status_code}) — trying next variation...")

    raise RuntimeError(f"Load table API failed after all attempts. Last error: {last_error}")


# ─── Fabric Data Agent helpers ────────────────────────────────────────────────

def _validate_agent(workspace_id: str, agent_id: str, token: str) -> bool:
    """
    Returns True if the agent metadata is accessible (GET /dataAgents/{id}
    returns HTTP 2xx).

    This is an *existence* check only — a Draft agent passes this check.
    Use _is_agent_queryable() to verify the agent can actually answer queries.
    We intentionally separate these two concerns so we never delete a Draft
    agent: a Draft agent keeps the same ID in the App Service config and
    becomes functional as soon as the user publishes it in the Fabric portal.
    Deleting + recreating a Draft agent just creates a new Draft agent with
    a different ID (breaking the App Service config) and gains nothing.
    """
    try:
        resp = requests.get(
            f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/dataAgents/{agent_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        return resp.ok
    except Exception:
        return False


def _is_agent_queryable(workspace_id: str, agent_id: str, token: str) -> bool:
    """
    Returns True if the agent's /query endpoint returns a non-404 response.

    The Fabric metadata GET returns HTTP 200 regardless of whether the agent
    is Published or Draft.  Probing /query is the only reliable way to tell
    whether the agent can actually answer natural-language questions.

    Any status other than 404 (including 200, 400, 500) means the endpoint
    is reachable and the agent is queryable.
    """
    try:
        probe = requests.post(
            f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/dataAgents/{agent_id}/query",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"userMessage": "ping"},
            timeout=20,
        )
        return probe.status_code != 404
    except Exception:
        # Network error / timeout — assume not queryable
        return False


def ensure_agent_published(
    workspace_id: str,
    agent_id: str,
    agent_name: str,
    token: str,
) -> bool:
    """
    Check queryability and publish if needed.  Returns True if the agent is
    (or becomes) queryable, False if manual action is still required.

    Call this AFTER every create or configure step instead of _try_publish_dataagent
    so that:
      - Already-published agents are detected immediately (no extra API calls)
      - Draft agents get a full set of publish attempts
      - If all publish attempts fail, the user gets a clear, actionable message
        with the workspace URL rather than a cryptic 404 in the smoke test
    """
    # Fast path: already queryable — nothing to do
    if _is_agent_queryable(workspace_id, agent_id, token):
        print(f"   ✓ Agent '{agent_name}' is already queryable — skipping publish step.")
        return True

    print(f"   Agent '{agent_name}' is not yet queryable (query endpoint → 404).")
    print(f"   Attempting to publish via all known API methods...")
    if _try_publish_dataagent(workspace_id, agent_id, agent_name, token):
        # Verify queryability after publish
        time.sleep(5)
        if _is_agent_queryable(workspace_id, agent_id, token):
            print(f"   ✓ Agent '{agent_name}' is now queryable after publish.")
            return True
        print(f"   ⚠️  Publish API returned success but agent still not queryable — "
              f"may still be activating.  Will retry in 15s...")
        time.sleep(15)
        if _is_agent_queryable(workspace_id, agent_id, token):
            print(f"   ✓ Agent '{agent_name}' became queryable after short delay.")
            return True

    # All publish attempts failed or agent still not queryable after publish
    print(
        f"\n"
        f"   ╔══════════════════════════════════════════════════════════════════╗\n"
        f"   ║  ⚠️  MANUAL PUBLISH REQUIRED                                    ║\n"
        f"   ╠══════════════════════════════════════════════════════════════════╣\n"
        f"   ║  The Fabric publish API is not yet available on this tenant.    ║\n"
        f"   ║  Do this ONE-TIME manual step (takes ~1 min):                   ║\n"
        f"   ║                                                                  ║\n"
        f"   ║  1. Open: https://app.fabric.microsoft.com/groups/{workspace_id[:8]}...  ║\n"
        f"   ║     Full URL: https://app.fabric.microsoft.com/groups/{workspace_id}\n"
        f"   ║  2. Click '{agent_name}'                                         ║\n"
        f"   ║  3. Click 'Publish' in the top ribbon                            ║\n"
        f"   ║  4. Look for the green 'Published' badge                         ║\n"
        f"   ║                                                                  ║\n"
        f"   ║  After publishing ONCE, all future re-deploys will keep it       ║\n"
        f"   ║  published — you will NEVER need to do this again.               ║\n"
        f"   ╚══════════════════════════════════════════════════════════════════╝\n"
    )
    return False


def _delete_agent(workspace_id: str, agent_id: str, token: str) -> None:
    """
    Attempts to delete a broken Data Agent so it can be recreated cleanly.
    Tries the dedicated /dataAgents endpoint first, then the generic /items
    endpoint.  Logs but does not raise on failure.
    """
    for path in (
        f"/workspaces/{workspace_id}/dataAgents/{agent_id}",
        f"/workspaces/{workspace_id}/items/{agent_id}",
    ):
        try:
            resp = requests.delete(
                f"{FABRIC_BASE_URL}{path}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=30,
            )
            if resp.ok or resp.status_code == 404:
                print(f"   🗑️  Deleted broken agent (ID: {agent_id}) via {path}")
                return
        except Exception as exc:
            print(f"   ⚠️  Delete attempt via {path} failed (non-fatal): {exc}")
    print(f"   ⚠️  Could not delete agent {agent_id} — re-creation may fail with name conflict")


# ─── Fabric Data Agent ────────────────────────────────────────────────────────

def get_or_create_dataagent(
    workspace_id: str,
    dataagent_name: str,
    lakehouse_id: str,
    token: str,
    table_name: str = "",
    semantic_model_id: str = "",
    ontology_id: str = "",
) -> str:
    """
    Finds an existing Fabric Data Agent by name, or creates one if absent.

    When *semantic_model_id* is provided the agent is linked to the semantic
    model (type ``SemanticModel``) instead of the raw Lakehouse.  This is the
    recommended approach — linking via semantic model makes the data source
    visible in the Fabric portal "Explorer → Data" pane.

    The data source (``dataSources``) and, when provided, the ontology
    (``ontologies``) are **always attached at creation time** in the
    ``configuration`` field of the create payload.  This applies to both the
    primary dedicated ``dataAgents`` endpoint and the generic ``items``
    fallback.  The correct provisioning order is:
        Create Ontology → Create Agent (with dataSources + ontologies) → Publish

    KEY DESIGN DECISION — we never delete a Draft agent:
    ─────────────────────────────────────────────────────
    A Draft agent has the same ID as it would have after being published.
    Deleting it and recreating just produces a NEW Draft agent with a
    different ID — which breaks the App Service FABRIC_DATAAGENT_ID setting
    and gains nothing.  Instead we:

      1. Find by name → return its ID (Draft or Published)
      2. Caller (`configure_dataagent` + `ensure_agent_published`) handles
         any re-linking and publish attempts
      3. If all publish attempts fail, the user publishes once manually;
         from that point on, all future re-deploys find a Published agent
         and touch nothing (no PATCH, no publish call)

    Returns the Data Agent item ID.
    """
    print(f"🤖 Checking for Data Agent: {dataagent_name}")

    # ── Build creation payload (shared by both the primary and fallback paths) ─
    # Data sources and ontology are attached at creation time so that Fabric
    # follows the correct internal provisioning order:
    #   Create Ontology → Create Agent (with dataSources + ontologies) → Publish
    # This avoids the need for a post-creation PATCH to link the data source.
    data_source: Dict[str, Any]
    if semantic_model_id:
        data_source = {
            "type": "SemanticModel",
            "workspaceId": workspace_id,
            "itemId": semantic_model_id,
        }
    else:
        data_source = {
            "type": "Lakehouse",
            "workspaceId": workspace_id,
            "itemId": lakehouse_id,
        }
        if table_name:
            data_source["selectedObjects"] = [
                {"schema": "dbo", "name": table_name, "objectType": "Table"}
            ]
    create_config: Dict[str, Any] = {
        "dataSources": [data_source],
    }
    if ontology_id:
        create_config["ontologies"] = [{"id": ontology_id}]
    if table_name:
        create_config["instructions"] = (
            f"You are a customer analytics assistant. "
            f"Answer questions about the '{table_name}' table in the "
            f"Customer360 Lakehouse. Provide insights about customer "
            f"segments, churn risk, lifetime value, and revenue."
        )
    create_payload: Dict[str, Any] = {
        "displayName": dataagent_name,
        "description": "Customer360 conversational analytics agent",
        "configuration": create_config,
    }

    # ── Try dedicated dataAgents endpoint first ──────────────────────────────
    try:
        list_resp = requests.get(
            f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/dataAgents",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if list_resp.status_code == 200:
            for agent in list_resp.json().get("value", []) or []:
                if agent.get("displayName") == dataagent_name:
                    agent_id = agent["id"]
                    if _validate_agent(workspace_id, agent_id, token):
                        # Agent exists — return its ID regardless of Draft/Published state.
                        # configure_dataagent + ensure_agent_published (called by the
                        # main flow) will handle any re-linking and publishing.
                        print(f"✓ Found existing Data Agent: {dataagent_name} (ID: {agent_id})")
                        return agent_id
                    # GET itself failed — agent record is truly broken (e.g. workspace
                    # was deleted and recreated with a different ID space).  Safe to delete.
                    print(
                        f"⚠️  Agent '{dataagent_name}' (ID: {agent_id}) is listed but "
                        f"GET returned non-2xx — record appears corrupt, deleting..."
                    )
                    _delete_agent(workspace_id, agent_id, token)
                    break  # Fall through to creation below

            print(f"📦 Creating Data Agent: {dataagent_name}")
            create_resp = None
            for _attempt in range(1, NAME_RETRY_MAX + 1):
                create_resp = requests.post(
                    f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/dataAgents",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    },
                    json=create_payload,
                    timeout=60,
                )
                if not _is_name_not_available_yet(create_resp):
                    break
                print(
                    f"   ↻ [{_attempt}/{NAME_RETRY_MAX}] Name '{dataagent_name}' not yet "
                    f"available — retrying in {NAME_RETRY_WAIT}s..."
                )
                time.sleep(NAME_RETRY_WAIT)
            if create_resp.status_code in (200, 201):
                agent_id = create_resp.json().get("id")
                if agent_id:
                    print(f"✓ Created Data Agent: {dataagent_name} (ID: {agent_id})")
                    # Newly created agents start in Draft state.  Wait for Fabric
                    # to finish internal provisioning before attempting publish.
                    print("   Waiting 20s for Fabric to finish provisioning the agent...")
                    time.sleep(20)
                    ensure_agent_published(workspace_id, agent_id, dataagent_name, token)
                    return agent_id
            elif create_resp.status_code == 202:
                op_id = create_resp.headers.get("x-ms-operation-id")
                if op_id:
                    poll_operation(op_id, token, "data agent creation")
                # Re-fetch after async creation
                list_resp2 = requests.get(
                    f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/dataAgents",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=30,
                )
                for agent in list_resp2.json().get("value", []) or []:
                    if agent.get("displayName") == dataagent_name:
                        agent_id = agent["id"]
                        print(f"✓ Data Agent ready: {dataagent_name} (ID: {agent_id})")
                        print("   Waiting 20s for Fabric to finish provisioning the agent...")
                        time.sleep(20)
                        ensure_agent_published(workspace_id, agent_id, dataagent_name, token)
                        return agent_id
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️  Dedicated dataAgents endpoint failed: {exc}")

    # ── Fallback: create as generic workspace item ────────────────────────────
    print("🔄 Falling back to generic Items API for Data Agent...")

    # Check existing items of type DataAgent
    items_resp = fabric_request(
        "GET", f"/workspaces/{workspace_id}/items?type=DataAgent", token
    )
    for item in items_resp.json().get("value", []) or []:
        if item.get("displayName") == dataagent_name:
            agent_id = item["id"]
            if _validate_agent(workspace_id, agent_id, token):
                print(f"✓ Found existing Data Agent item: {dataagent_name} (ID: {agent_id})")
                return agent_id
            # Item exists but isn't queryable via the dedicated endpoint —
            # delete it so we can create a fresh, properly initialised agent.
            print(
                f"⚠️  Agent item '{dataagent_name}' (ID: {agent_id}) is not queryable — "
                f"deleting and recreating..."
            )
            _delete_agent(workspace_id, agent_id, token)
            break  # Exit search loop; fall through to creation below

    # Create generic item (retry on ItemDisplayNameNotAvailableYet).
    # The same create_payload (with configuration.dataSources and, when present,
    # configuration.ontologies) is used here so the data source is attached at
    # creation time — consistent with the dedicated dataAgents endpoint above.
    fallback_payload = {
        "type": "DataAgent",
        **create_payload,
    }
    item_resp = None
    for _attempt in range(1, NAME_RETRY_MAX + 1):
        item_resp = requests.post(
            f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/items",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=fallback_payload,
            timeout=60,
        )
        if not _is_name_not_available_yet(item_resp):
            break
        print(
            f"   ↻ [{_attempt}/{NAME_RETRY_MAX}] Name '{dataagent_name}' not yet "
            f"available — retrying in {NAME_RETRY_WAIT}s..."
        )
        time.sleep(NAME_RETRY_WAIT)
    if not item_resp.ok:
        print(f"❌ Fabric API error: POST /workspaces/{workspace_id}/items")
        print(f"   Status : {item_resp.status_code}")
        print(f"   Response: {item_resp.text[:500]}")
        raise RuntimeError(
            f"Fabric API failed [{item_resp.status_code}]: {item_resp.text[:200]}"
        )
    if item_resp.status_code == 202:
        op_id = item_resp.headers.get("x-ms-operation-id")
        if op_id:
            poll_operation(op_id, token, "data agent item creation")
        # Re-fetch
        items_resp2 = fabric_request(
            "GET", f"/workspaces/{workspace_id}/items?type=DataAgent", token
        )
        for item in items_resp2.json().get("value", []) or []:
            if item.get("displayName") == dataagent_name:
                agent_id = item["id"]
                print(f"✓ Data Agent created: {dataagent_name} (ID: {agent_id})")
                return agent_id
    else:
        agent_id = item_resp.json().get("id")
        if agent_id:
            print(
                f"✓ Data Agent created via Items API: {dataagent_name} (ID: {agent_id})"
            )
            # Data source and ontology were included in the create payload.
            # Wait for Fabric to finish provisioning before any publish attempt.
            print("   Waiting 20s for Fabric to finish provisioning the agent...")
            time.sleep(20)
            ensure_agent_published(workspace_id, agent_id, dataagent_name, token)
            return agent_id

    raise RuntimeError(
        f"Data Agent creation failed: could not obtain ID from any API path."
    )


# ─── Fabric Data Agent – configure (link Lakehouse + tables) ─────────────────

def configure_dataagent(
    workspace_id: str,
    agent_id: str,
    agent_name: str,
    lakehouse_id: str,
    table_name: str,
    token: str,
    semantic_model_id: str = "",
    ontology_id: str = "",
) -> None:
    """
    Updates the Data Agent configuration to link it to a data source.

    When *semantic_model_id* is provided the agent is linked to the semantic
    model (recommended — this makes the data source visible in the Fabric
    portal Explorer pane).  Otherwise falls back to Lakehouse linkage.

    NOTE: Ontology attachment is handled at agent *creation* time via
    ``get_or_create_dataagent(ontology_id=...)``, not here.  The
    ``ontology_id`` parameter is accepted for backwards compatibility but
    is intentionally ignored.  Configuration here only handles:
      - dataSources
      - instructions

    This is the step that makes the agent aware of *which* data it should query.
    Without this step the agent has no data source and will return empty answers.

    IMPORTANT: In Fabric preview, PATCH/PUT on a Published agent moves it back
    to Draft state.  The publish endpoint (/publish, /activate) currently returns
    HTTP 404, so there is no API way to re-publish after a PATCH.  Therefore this
    function SKIPS the PATCH if the agent already has the data source linked
    and is in Published (or unknown) state.  Only agents that are clearly
    mis-configured (no data source) are patched.

    Tries PATCH first, then PUT to handle differences across preview versions.
    """
    ds_item_id = semantic_model_id or lakehouse_id
    ds_type = "SemanticModel" if semantic_model_id else "Lakehouse"
    print(f"   Configuring Data Agent '{agent_name}' → {ds_type} '{ds_item_id}' ...")

    agent_url = f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/dataAgents/{agent_id}"
    req_headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # ── Step 0: GET the current agent state and configuration ─────────────────
    # If the agent is already Published AND the correct Lakehouse is already a
    # data source, skip the PATCH entirely.  Patching a Published agent moves it
    # to Draft state, and the publish endpoint is not yet available in this
    # preview API version.
    try:
        get_resp = requests.get(agent_url, headers=req_headers, timeout=30)
        if get_resp.ok:
            current = get_resp.json()
            # Fabric may use "state", "status", or "publishState" depending on
            # the preview version.  Treat any truthy value other than
            # "Draft"/"Unpublished"/"Inactive" as Published.
            raw_state = (
                current.get("state")
                or current.get("status")
                or current.get("publishState")
                or ""
            )
            state = str(raw_state).lower()
            is_draft = state in ("draft", "unpublished", "inactive", "creating")
            config = current.get("configuration") or {}
            data_sources = config.get("dataSources") or []
            already_linked = any(
                str(ds.get("itemId", "")).lower() == ds_item_id.lower()
                for ds in data_sources
            )
            # Check whether the API actually returned configuration data.
            # The Fabric preview GET /dataAgents/{id} often omits the
            # 'configuration' field entirely, making it impossible to tell
            # whether the data source is already linked.
            has_config_data = "configuration" in current

            print(
                f"   Agent current state: '{raw_state or 'unknown'}', "
                f"data sources: {len(data_sources)}, "
                f"{ds_type} already linked: {already_linked}, "
                f"config in response: {has_config_data}"
            )
            if already_linked and not is_draft:
                print(
                    f"   ✓ Agent is published and {ds_type} is already configured — "
                    "skipping PATCH to avoid Draft-state regression."
                )
                return
            if already_linked and is_draft:
                print(
                    f"   Agent is in Draft state with {ds_type} already linked — "
                    "will attempt to publish without re-patching."
                )
                # Skip to publish-only attempt below
                _try_publish_dataagent(workspace_id, agent_id, agent_name, token)
                return

            # In Fabric preview, GET /dataAgents/{id} often omits the
            # 'configuration' field — so has_config_data is frequently False.
            # We must distinguish between:
            #   (a) Agent already properly configured → skip PATCH
            #   (b) Newly created agent with no data source → must PATCH
            # Use the /query endpoint as the authoritative check: if the
            # agent is queryable it is already linked and published.
            if not has_config_data:
                if _is_agent_queryable(workspace_id, agent_id, token):
                    print(
                        "   API did not return 'configuration' but agent IS queryable — "
                        "skipping PATCH to avoid Draft-state regression."
                    )
                    return
                print(
                    "   API did not return 'configuration' and agent is NOT queryable — "
                    f"proceeding with PATCH to link {ds_type}."
                )
        else:
            print(
                f"   [WARN] GET agent returned HTTP {get_resp.status_code} — "
                f"proceeding with configure anyway."
            )
    except Exception as exc:
        print(f"   [WARN] GET agent for state check failed (non-fatal): {exc}")

    # Build data source payloads (try progressively simpler variants).
    # When semantic_model_id is provided, use SemanticModel type; otherwise
    # fall back to Lakehouse.
    if semantic_model_id:
        data_source_primary: Dict[str, Any] = {
            "type": "SemanticModel",
            "workspaceId": workspace_id,
            "itemId": semantic_model_id,
        }
        data_source_basic = data_source_primary
    else:
        data_source_primary = {
            "type": "Lakehouse",
            "workspaceId": workspace_id,
            "itemId": lakehouse_id,
            "selectedObjects": [
                {"schema": "dbo", "name": table_name, "objectType": "Table"}
            ],
        }
        data_source_basic = {
            "type": "Lakehouse",
            "workspaceId": workspace_id,
            "itemId": lakehouse_id,
        }

    def _make_config(ds: Dict[str, Any], include_instructions: bool) -> Dict[str, Any]:
        cfg: Dict[str, Any] = {"dataSources": [ds]}
        if include_instructions:
            cfg["instructions"] = (
                f"You are a customer analytics assistant. "
                f"Answer questions about the '{table_name}' table in the Customer360 Lakehouse. "
                "Provide insights about customer segments, churn risk, lifetime value, and revenue."
            )
        # Ontology is intentionally NOT set here — it is attached at agent
        # creation time in get_or_create_dataagent().
        return cfg

    payloads = [
        # Attempt 1: full payload with data source + instructions
        {
            "displayName": agent_name,
            "description": "Customer360 conversational analytics agent",
            "configuration": _make_config(data_source_primary, include_instructions=True),
        },
        # Attempt 2: data source without instructions
        {
            "displayName": agent_name,
            "description": "Customer360 conversational analytics agent",
            "configuration": _make_config(data_source_primary, include_instructions=False),
        },
        # Attempt 3: basic linkage, no explicit table selection
        {
            "displayName": agent_name,
            "description": "Customer360 conversational analytics agent",
            "configuration": _make_config(data_source_basic, include_instructions=False),
        },
    ]

    last_err = ""
    attempt = 0
    # Try PATCH first (partial update), then PUT (full replace) for each payload.
    # PATCH is preferred because some Fabric API preview versions return HTTP 404
    # for PUT on the dataAgents endpoint while correctly handling PATCH.
    for method in ("PATCH", "PUT"):
        for i, payload in enumerate(payloads, 1):
            attempt += 1
            ds_keys = list(payload.get("configuration", {}).get("dataSources", [{}])[0].keys())
            print(f"   Configure attempt {attempt} ({method}): {ds_keys}")
            try:
                resp = requests.request(
                    method,
                    agent_url,
                    headers=req_headers,
                    json=payload,
                    timeout=60,
                )
                if resp.status_code in (200, 201, 204):
                    print(f"   OK  Data Agent configured successfully (HTTP {resp.status_code}).")
                    # After a successful PATCH/PUT the agent is in Draft state.
                    # Use ensure_agent_published so we get the fast-path check
                    # and clear manual instructions if the publish API is unavailable.
                    ensure_agent_published(workspace_id, agent_id, agent_name, token)
                    return
                if resp.status_code == 202:
                    # Long-running — poll if we have an operation ID
                    op_id = resp.headers.get("x-ms-operation-id") or resp.headers.get("x-ms-operationid")
                    if op_id:
                        poll_operation(op_id, token, "data agent configuration")
                    print("   OK  Data Agent configuration accepted (202).")
                    return
                if resp.status_code == 404:
                    # This HTTP method is not available for this endpoint — breaking
                    # from the inner (payloads) loop naturally advances to the next
                    # method in the outer loop (e.g. PATCH → PUT).
                    last_err = f"HTTP {resp.status_code}: {resp.text[:300]}"
                    print(f"   Attempt {attempt} ({method}) returned 404 — trying next method/payload...")
                    break  # Break inner (payloads) loop; continue outer (methods) loop
                last_err = f"HTTP {resp.status_code}: {resp.text[:300]}"
                print(f"   Attempt {attempt} ({method}) failed ({resp.status_code}): {resp.text[:200]}")
            except Exception as exc:
                last_err = str(exc)
                print(f"   Attempt {attempt} ({method}) exception: {exc}")

    # Non-fatal: log a warning and continue — the agent may still answer queries
    # if it was already configured correctly from a prior run.
    print(
        f"   WARNING: Data Agent configuration failed after {attempt} attempts. "
        f"Last error: {last_err}\n"
        f"   The agent may not have a data source linked. "
        f"   You can link it manually in the Fabric portal:\n"
        f"   https://app.fabric.microsoft.com/groups/{workspace_id} "
        f"-> Open '{agent_name}' -> Add data source"
    )


# ─── Fabric Data Agent – publish / activate ───────────────────────────────────


def _try_publish_dataagent(
    workspace_id: str,
    agent_id: str,
    agent_name: str,
    token: str,
) -> bool:
    """
    Attempts to publish the Data Agent via every known API path.
    Returns True if any method succeeds, False otherwise.  Non-fatal.

    Tries (in order):
      1. POST  /dataAgents/{id}/publish
      2. POST  /dataAgents/{id}/activate
      3. POST  /items/{id}/publish          (generic items publish)
      4. POST  /items/{id}/deploy
      5. PATCH /dataAgents/{id}  {"publishState": "Published"}
      6. PATCH /dataAgents/{id}  {"state": "Published"}
    Each attempt is retried once after a 10s delay to handle transient
    "agent still provisioning" 404s.
    """
    req_headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    base = f"{FABRIC_BASE_URL}/workspaces/{workspace_id}"

    # ── POST-based publish endpoints ─────────────────────────────────────────
    post_endpoints = [
        f"{base}/dataAgents/{agent_id}/publish",
        f"{base}/dataAgents/{agent_id}/activate",
        f"{base}/items/{agent_id}/publish",
        f"{base}/items/{agent_id}/deploy",
    ]
    for url in post_endpoints:
        for attempt in (1, 2):   # retry once after 10s
            try:
                resp = requests.post(url, headers=req_headers, json={}, timeout=60)
                if resp.status_code in (200, 201, 204):
                    print(f"   ✓ Agent published via POST {url.split('/')[-1]} (HTTP {resp.status_code}).")
                    return True
                if resp.status_code == 202:
                    op_id = (
                        resp.headers.get("x-ms-operation-id")
                        or resp.headers.get("x-ms-operationid")
                    )
                    if op_id:
                        try:
                            poll_operation(op_id, token, "data agent publish")
                        except Exception:
                            pass
                    print(f"   ✓ Agent publish accepted (202) via POST {url.split('/')[-1]}.")
                    return True
                if resp.status_code == 409:
                    print("   Agent already published (409 Conflict) — treating as success.")
                    return True
                if resp.status_code == 404 and attempt == 1:
                    # Agent may still be provisioning — wait and retry once
                    print(f"   404 from {url.split('/')[-1]}, retrying in 10s...")
                    time.sleep(10)
                    continue
                # Any other error — try next endpoint
                print(f"   [INFO] POST {url.split('/')[-1]} → HTTP {resp.status_code}: {resp.text[:120]}")
                break
            except Exception as exc:
                print(f"   [WARN] POST {url.split('/')[-1]} exception: {exc}")
                break

    # ── PATCH-based publish (set publishState / state field directly) ─────────
    patch_payloads = [
        {"publishState": "Published"},
        {"state": "Published"},
        {"status": "Published"},
    ]
    patch_url = f"{base}/dataAgents/{agent_id}"
    for payload in patch_payloads:
        field = list(payload.keys())[0]
        try:
            resp = requests.patch(patch_url, headers=req_headers, json=payload, timeout=60)
            if resp.status_code in (200, 201, 204):
                print(f"   ✓ Agent published via PATCH {field}=Published (HTTP {resp.status_code}).")
                return True
            if resp.status_code == 202:
                print(f"   ✓ Agent PATCH publish accepted (202) via {field}.")
                return True
            # 400/404/405 = field not accepted — try next
            print(f"   [INFO] PATCH {field}=Published → HTTP {resp.status_code}: {resp.text[:100]}")
        except Exception as exc:
            print(f"   [WARN] PATCH {field}=Published exception: {exc}")

    print(
        f"   [INFO] All publish attempts returned 404/400/405 — "
        f"the Fabric publish API may not be available on this tenant yet.\n"
        f"   Manual action required: open the Fabric portal, find '{agent_name}',\n"
        f"   and click 'Publish' in the top ribbon.\n"
        f"   Direct link: https://app.fabric.microsoft.com/groups/{workspace_id}"
    )
    return False


def publish_dataagent(
    workspace_id: str,
    agent_id: str,
    agent_name: str,
    token: str,
) -> None:
    """
    Ensures the Data Agent is published and queryable.

    Delegates entirely to ensure_agent_published which:
      - Fast-paths if the agent is already queryable (no extra API calls)
      - Tries all known publish endpoints if needed
      - Prints clear manual instructions with the workspace URL if the
        Fabric publish API is unavailable on this tenant

    This is called from the main flow after configure_dataagent.
    configure_dataagent itself already calls ensure_agent_published after
    a successful PATCH, so this call handles the case where PATCH was
    skipped (agent already correctly configured and published).
    """
    print(f"   Verifying Data Agent '{agent_name}' is published and queryable...")
    ensure_agent_published(workspace_id, agent_id, agent_name, token)


def validate_dataagent(
    workspace_id: str,
    agent_id: str,
    agent_name: str,
    lakehouse_id: str,
    token: str,
    semantic_model_id: str = "",
    ontology_id: str = "",
) -> bool:
    """
    Post-setup validation for the Data Agent.

    Checks:
      1. Agent metadata is accessible (GET returns 2xx)
      2. Agent is linked to the correct data source (semantic model or Lakehouse)
      3. Agent is queryable (query endpoint is reachable)

    Returns True if all checks pass, False otherwise.
    Prints detailed status for each check.
    """
    ds_item_id = semantic_model_id or lakehouse_id
    ds_type = "SemanticModel" if semantic_model_id else "Lakehouse"
    print(f"\n   🔍 Validating Data Agent '{agent_name}' (ID: {agent_id})...")
    all_ok = True

    # ── Check 1: Agent exists ────────────────────────────────────────────────
    agent_exists = _validate_agent(workspace_id, agent_id, token)
    if agent_exists:
        print("   ✅ Check 1/3: Agent metadata is accessible (GET returned 2xx)")
    else:
        print("   ❌ Check 1/3: Agent metadata NOT accessible (GET returned non-2xx)")
        all_ok = False

    # ── Check 2: Data source linkage ────────────────────────────────────────
    try:
        agent_url = f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/dataAgents/{agent_id}"
        resp = requests.get(
            agent_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if resp.ok:
            current = resp.json()
            config = current.get("configuration") or {}
            data_sources = config.get("dataSources") or []
            if "configuration" not in current:
                print(
                    "   ⚠️  Check 2/3: Data source linkage — API did not return "
                    "'configuration' field (normal for Fabric preview, cannot verify)"
                )
            elif any(
                str(ds.get("itemId", "")).lower() == ds_item_id.lower()
                for ds in data_sources
            ):
                print(f"   ✅ Check 2/3: Agent is linked to the correct {ds_type}")
                if ontology_id:
                    ontologies = config.get("ontologies") or []
                    if any(
                        str(ont.get("id", "")).lower() == ontology_id.lower()
                        for ont in ontologies
                    ):
                        print("   ✅ Check 2/3 (extra): Agent is linked to the Ontology")
                    else:
                        print(
                            "   ⚠️  Check 2/3 (extra): Agent ontology linkage could not be "
                            "verified (normal for Fabric preview)"
                        )
            else:
                print(
                    f"   ❌ Check 2/3: Agent is NOT linked to {ds_type} '{ds_item_id}'. "
                    f"Found data sources: {[ds.get('itemId') for ds in data_sources]}"
                )
                all_ok = False
        else:
            print(f"   ⚠️  Check 2/3: Could not fetch agent details (HTTP {resp.status_code})")
    except Exception as exc:
        print(f"   ⚠️  Check 2/3: Data source linkage check failed: {exc}")

    # ── Check 3: Agent is queryable ──────────────────────────────────────────
    queryable = _is_agent_queryable(workspace_id, agent_id, token)
    if queryable:
        print("   ✅ Check 3/3: Agent is queryable (query endpoint reachable)")
    else:
        print(
            "   ⚠️  Check 3/3: Agent is NOT yet queryable (query endpoint not reachable). "
            "Manual publish may be required."
        )
        # Not a hard failure — user may need to publish manually
        # but we still flag it

    if all_ok and queryable:
        print(f"   ✅ All validation checks passed for Data Agent '{agent_name}'")
    elif all_ok:
        print(
            f"   ⚠️  Data Agent '{agent_name}' exists and is configured, but is not yet queryable.\n"
            f"      Manual publish may be required in the Fabric portal."
        )
    else:
        print(
            f"   ❌ Some validation checks failed for Data Agent '{agent_name}'.\n"
            f"      Check the messages above and resolve in the Fabric portal."
        )

    return all_ok


# ─── Semantic Model (default from Lakehouse) ─────────────────────────────────


def _get_lakehouse_sm_id(workspace_id: str, lakehouse_id: str, token: str) -> Optional[str]:
    """
    Reads the default semantic model ID directly from the Lakehouse's API properties.

    GET /v1/workspaces/{workspaceId}/lakehouses/{lakehouseId} returns:
      properties.defaultSemanticModel.id

    This is the most reliable discovery path because:
    - Uses Fabric REST API token — no additional tenant settings required.
    - Authoritative: the ID is stored directly in the Lakehouse metadata.
    - Works even if the semantic model is not yet visible in the Items API.

    Returns the semantic model ID string, or None if unavailable.
    """
    if not lakehouse_id:
        return None
    try:
        resp = requests.get(
            f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if not resp.ok:
            print(
                f"   [WARN] GET /lakehouses/{lakehouse_id} returned HTTP {resp.status_code}: "
                f"{resp.text[:200]}"
            )
            return None
        data = resp.json()
        props = data.get("properties") or {}
        sm = props.get("defaultSemanticModel") or {}
        sm_id = sm.get("id")
        if sm_id:
            print(f"   ✓ Found semantic model ID in Lakehouse properties: {sm_id}")
            return sm_id
        # Print full properties for diagnostics when SM is absent
        print(f"   Lakehouse properties (defaultSemanticModel absent): {props}")
    except Exception as exc:
        print(f"   [WARN] Lakehouse properties lookup failed (non-fatal): {exc}")
    return None


def _get_sql_endpoint_info(
    workspace_id: str, lakehouse_id: str, token: str
) -> Optional[str]:
    """
    Reads the SQL Analytics endpoint connection string from the Lakehouse
    properties.

    Returns the server hostname (e.g.
    ``xxx.datawarehouse.fabric.microsoft.com``) or *None* if unavailable.
    """
    if not lakehouse_id:
        return None
    try:
        resp = requests.get(
            f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if not resp.ok:
            return None
        props = resp.json().get("properties") or {}
        sql_props = props.get("sqlEndpointProperties") or {}
        conn_str = sql_props.get("connectionString")
        if conn_str and sql_props.get("provisioningStatus") == "Success":
            return conn_str
    except Exception:
        pass
    return None


def _build_direct_lake_bim(
    model_name: str,
    table_name: str,
    sql_endpoint_server: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a minimal BIM (JSON) for a Direct Lake semantic model that connects
    to a Fabric Lakehouse via entity-based Direct Lake partitions.

    When *sql_endpoint_server* is provided the M expression uses
    ``Sql.Database(server, database)`` which Fabric recognises as a valid
    Direct Lake data source.  Without it the function falls back to
    ``Lakehouse.Contents(null)`` (may fail on some Fabric API versions).

    The Customer360 column schema is hardcoded because this accelerator
    always loads the same table; change if you adapt the project.
    """
    columns = [
        {
            "name": "CustomerId", "dataType": "string",
            "sourceColumn": "CustomerId", "lineageTag": str(uuid.uuid4()),
            "summarizeBy": "none",
        },
        {
            "name": "FullName", "dataType": "string",
            "sourceColumn": "FullName", "lineageTag": str(uuid.uuid4()),
            "summarizeBy": "none",
        },
        {
            "name": "State", "dataType": "string",
            "sourceColumn": "State", "lineageTag": str(uuid.uuid4()),
            "summarizeBy": "none",
        },
        {
            "name": "City", "dataType": "string",
            "sourceColumn": "City", "lineageTag": str(uuid.uuid4()),
            "summarizeBy": "none",
        },
        {
            "name": "Segment", "dataType": "string",
            "sourceColumn": "Segment", "lineageTag": str(uuid.uuid4()),
            "summarizeBy": "none",
        },
        {
            "name": "LifetimeValue", "dataType": "decimal",
            "sourceColumn": "LifetimeValue", "lineageTag": str(uuid.uuid4()),
            "summarizeBy": "sum", "formatString": "#,0.00",
        },
        {
            "name": "MonthlyRevenue", "dataType": "decimal",
            "sourceColumn": "MonthlyRevenue", "lineageTag": str(uuid.uuid4()),
            "summarizeBy": "sum", "formatString": "#,0.00",
        },
        {
            "name": "ChurnRiskScore", "dataType": "decimal",
            "sourceColumn": "ChurnRiskScore", "lineageTag": str(uuid.uuid4()),
            "summarizeBy": "average", "formatString": "0.0",
        },
        {
            "name": "LastPurchaseDate", "dataType": "dateTime",
            "sourceColumn": "LastPurchaseDate", "lineageTag": str(uuid.uuid4()),
            "summarizeBy": "none", "formatString": "Short Date",
        },
    ]
    return {
        "name": model_name,
        "compatibilityLevel": 1604,
        "model": {
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "defaultMode": "directLake",
            "expressions": [
                {
                    "name": "DatabaseQuery",
                    "kind": "m",
                    "expression": (
                        (
                            "let\n"
                            f'    database = Sql.Database("{sql_endpoint_server}", "{model_name}")\n'
                            "in\n"
                            "    database"
                        )
                        if sql_endpoint_server
                        else (
                            "let\n"
                            "    database = Lakehouse.Contents(null)\n"
                            "in\n"
                            "    database"
                        )
                    ),
                }
            ],
            "annotations": [
                {"name": "PBI_QueryOrder", "value": f"[\"{table_name}\"]"}
            ],
            "tables": [
                {
                    "name": table_name,
                    "lineageTag": str(uuid.uuid4()),
                    "columns": columns,
                    "partitions": [
                        {
                            "name": table_name,
                            "mode": "directLake",
                            "source": {
                                "type": "entity",
                                "schemaName": "dbo",
                                "entityName": table_name,
                                "expressionSource": "DatabaseQuery",
                            },
                        }
                    ],
                    "annotations": [
                        {"name": "PBI_NavigationStepName", "value": "Navigation"},
                        {"name": "PBI_ResultType", "value": "Table"},
                    ],
                }
            ],
            "roles": [],
        },
    }


def create_direct_lake_semantic_model(
    workspace_id: str,
    model_name: str,
    table_name: str,
    token: str,
    sql_endpoint_server: Optional[str] = None,
) -> Optional[str]:
    """
    Creates a Direct Lake SemanticModel Fabric item via the Fabric Items API.

    This is the PRIMARY path for semantic model creation.  It does not rely
    on createDefaultSemanticModel (which may return HTTP 404 for certain
    service principal configurations).

    Strategy:
      1. POST /v1/workspaces/{wid}/items  (type=SemanticModel, with BIM)
      2. If that returns 4xx, retry with /semanticModels endpoint.
      3. If resp is 202 Accepted, poll the long-running operation.
      4. If resp is 409 Conflict, look up and return the existing model ID.

    Returns the semantic model item ID, or None on failure.
    """
    print(f"   Creating Direct Lake semantic model via Fabric Items API: {model_name}")

    bim = _build_direct_lake_bim(model_name, table_name, sql_endpoint_server=sql_endpoint_server)
    bim_b64 = base64.b64encode(json.dumps(bim, indent=2).encode()).decode()

    # definition.pbism is a required connection descriptor for Fabric
    # semantic models — omitting it causes HTTP 400 "Required artifact is
    # missing in 'definition.pbism'".
    pbism = {"version": "1.0", "settings": {}}
    pbism_b64 = base64.b64encode(json.dumps(pbism).encode()).decode()

    payload: Dict[str, Any] = {
        "displayName": model_name,
        "type": "SemanticModel",
        "definition": {
            "parts": [
                {
                    "path": "definition.pbism",
                    "payload": pbism_b64,
                    "payloadType": "InlineBase64",
                },
                {
                    "path": "model.bim",
                    "payload": bim_b64,
                    "payloadType": "InlineBase64",
                },
            ]
        },
    }

    # Try the general items endpoint first, then the type-specific endpoint.
    for endpoint in (
        f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/items",
        f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/semanticModels",
    ):
        try:
            resp = requests.post(
                endpoint,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=120,
            )

            if resp.status_code in (200, 201):
                sm_id = resp.json().get("id")
                if sm_id:
                    print(f"   ✓ Direct Lake semantic model created (ID: {sm_id})")
                    return sm_id

            elif resp.status_code == 202:
                op_id = (
                    resp.headers.get("x-ms-operation-id")
                    or resp.headers.get("x-ms-operationid")
                    or resp.headers.get("Location", "")
                )
                print(f"   Async creation accepted — polling operation...")
                if op_id:
                    try:
                        poll_operation(op_id, token, "Direct Lake semantic model creation")
                    except Exception as poll_exc:
                        print(f"   [WARN] Async polling failed (non-fatal): {poll_exc}")
                # Re-fetch from workspace items after async step
                time.sleep(5)
                try:
                    ir = fabric_request(
                        "GET",
                        f"/workspaces/{workspace_id}/items?type=SemanticModel",
                        token,
                    )
                    for item in ir.json().get("value", []) or []:
                        if item.get("displayName") == model_name:
                            sm_id = item["id"]
                            print(f"   ✓ Direct Lake semantic model ready (ID: {sm_id})")
                            return sm_id
                except Exception as fetch_exc:
                    print(f"   [WARN] Re-fetch after async creation failed: {fetch_exc}")

            elif resp.status_code == 409:
                # Already exists — look it up
                print(f"   Semantic model '{model_name}' already exists (409) — looking up ID...")
                try:
                    ir = fabric_request(
                        "GET",
                        f"/workspaces/{workspace_id}/items?type=SemanticModel",
                        token,
                    )
                    for item in ir.json().get("value", []) or []:
                        if item.get("displayName") == model_name:
                            sm_id = item["id"]
                            print(f"   ✓ Found existing semantic model (ID: {sm_id})")
                            return sm_id
                except Exception as lu_exc:
                    print(f"   [WARN] Lookup after 409 failed: {lu_exc}")

            else:
                print(
                    f"   [WARN] {endpoint.split('/')[-1]} endpoint returned "
                    f"HTTP {resp.status_code}: {resp.text[:300]}"
                )

        except Exception as exc:
            print(f"   [WARN] {endpoint}: {exc}")

    return None


def trigger_default_semantic_model(
    workspace_id: str,
    lakehouse_id: str,
    token: str,
) -> None:
    """
    Secondary trigger: calls the Fabric Lakehouse-specific
    createDefaultSemanticModel endpoint as a fallback after the primary
    direct-creation path (create_direct_lake_semantic_model) has already
    been attempted.

    This endpoint may return 404 for certain service principal configurations
    — which is non-fatal since the Items API path is tried first.
    """
    url = (
        f"{FABRIC_BASE_URL}/workspaces/{workspace_id}"
        f"/lakehouses/{lakehouse_id}/createDefaultSemanticModel"
    )
    print("   Trying createDefaultSemanticModel endpoint as secondary trigger...")
    try:
        resp = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={},
            timeout=60,
        )
        if resp.status_code in (200, 201, 204):
            print("   ✓ createDefaultSemanticModel succeeded.")
        elif resp.status_code == 202:
            op_id = (
                resp.headers.get("x-ms-operation-id")
                or resp.headers.get("x-ms-operationid")
            )
            if op_id:
                try:
                    poll_operation(op_id, token, "default semantic model creation")
                    print("   ✓ Semantic model creation completed.")
                except Exception as poll_exc:
                    print(f"   [WARN] Polling failed (non-fatal): {poll_exc}")
            else:
                print("   Accepted (202) — no operation ID to poll.")
        elif resp.status_code == 409:
            print("   Semantic model already exists (409) — skipping.")
        else:
            print(
                f"   [WARN] createDefaultSemanticModel returned HTTP {resp.status_code}: "
                f"{resp.text[:200]} (non-fatal — primary creation path already attempted)"
            )
    except Exception as exc:
        print(f"   [WARN] createDefaultSemanticModel call failed (non-fatal): {exc}")


def get_default_semantic_model(
    workspace_id: str,
    lakehouse_name: str,
    lakehouse_id: str,
    token: str,
    table_name: str = "Customer360",
    retries: int = 16,
) -> Optional[str]:
    """
    Finds or creates the Fabric Semantic Model for the Lakehouse.

    Creation strategy (in order — each is tried before falling back to polling):
      0. Read SM ID from Lakehouse properties  (fastest path; works when Fabric
         already auto-created the model)
      1. Create a Direct Lake SemanticModel item via Fabric Items API  (primary
         creation path)
      2. Call createDefaultSemanticModel as a secondary trigger  (fallback)

    Discovery poll (up to `retries` × 30 s after creation attempts):
      a. Fabric Items API  type=SemanticModel
      b. Every 4th attempt: all-items dump + name-agnostic match
      c. Every 4th attempt: re-read Lakehouse properties

    Note: 'Dataset' is NOT a valid Fabric Items API type (HTTP 400
    InvalidItemType) and is deliberately excluded.

    Returns the semantic model item ID, or None if not found/created.
    """
    print(f"Looking for default semantic model: {lakehouse_name}")

    # ── Strategy 0: Read SM ID from Lakehouse properties ─────────────────────
    sm_id = _get_lakehouse_sm_id(workspace_id, lakehouse_id, token)
    if sm_id:
        print(f"[OK] Semantic model found via Lakehouse properties (no retry needed): {sm_id}")
        return sm_id

    # ── Strategy 1: Create Direct Lake SemanticModel via Fabric Items API ────
    # This is the PRIMARY creation path.
    sql_endpoint_server = _get_sql_endpoint_info(workspace_id, lakehouse_id, token)
    if sql_endpoint_server:
        print(f"   Using SQL endpoint for Direct Lake: {sql_endpoint_server}")
    sm_id = create_direct_lake_semantic_model(
        workspace_id, lakehouse_name, table_name, token,
        sql_endpoint_server=sql_endpoint_server,
    )
    if sm_id:
        return sm_id

    # ── Strategy 2: createDefaultSemanticModel secondary trigger ─────────────
    trigger_default_semantic_model(workspace_id, lakehouse_id, token)

    for attempt in range(1, retries + 1):
        # 1. Fabric Items API -- type=SemanticModel only.
        # NOTE: 'Dataset' is NOT a valid type (returns 400 InvalidItemType).
        try:
            resp = fabric_request(
                "GET",
                f"/workspaces/{workspace_id}/items?type=SemanticModel",
                token,
            )
            for item in resp.json().get("value", []) or []:
                if item.get("displayName") == lakehouse_name:
                    sm_id = item["id"]
                    print(
                        f"[OK] Found default semantic model: {lakehouse_name} "
                        f"(ID: {sm_id})"
                    )
                    return sm_id
        except Exception as exc:
            print(f"   [WARN] SemanticModel lookup failed (non-fatal): {exc}")

        # 2. Every 4th attempt: dump all workspace items
        if attempt % 4 == 0:
            # 2a. All-items diagnostic dump + type-agnostic name match
            try:
                all_resp = fabric_request(
                    "GET", f"/workspaces/{workspace_id}/items", token
                )
                all_items = all_resp.json().get("value", []) or []
                print(
                    "   Workspace items visible so far: "
                    + (", ".join(
                        f"{i.get('displayName')} ({i.get('type')})"
                        for i in all_items
                    ) or "(none)")
                )
                for item in all_items:
                    name = item.get("displayName", "")
                    itype = item.get("type", "")
                    if name == lakehouse_name and itype not in (
                        "Lakehouse", "SQLEndpoint", "MirroredDatabase"
                    ):
                        sm_id = item["id"]
                        print(
                            f"[OK] Found semantic model via all-items search: "
                            f"{name} (ID: {sm_id}, type: {itype})"
                        )
                        return sm_id
            except Exception as exc:
                print(f"   [WARN] All-items fallback failed (non-fatal): {exc}")

            # 2b. Re-check Lakehouse properties (SM may now be populated after trigger)
            sm_id = _get_lakehouse_sm_id(workspace_id, lakehouse_id, token)
            if sm_id:
                print(f"[OK] Semantic model appeared in Lakehouse properties: {sm_id}")
                return sm_id

        if attempt < retries:
            print(
                f"   [{attempt}/{retries}] Semantic model not ready yet -- "
                f"waiting 30s for Fabric to materialise it..."
            )
            time.sleep(30)

    # Final attempt: Lakehouse properties one last time before giving up.
    print("   Final fallback: checking Lakehouse properties...")
    sm_id = _get_lakehouse_sm_id(workspace_id, lakehouse_id, token)
    if sm_id:
        print(f"[OK] Semantic model found in final Lakehouse properties check: {sm_id}")
        return sm_id

    print(
        f"\n   [WARN] Default semantic model '{lakehouse_name}' not found after "
        f"{retries} poll attempts ({retries * 30 // 60} min) + direct creation attempts.\n"
        "\n"
        "   Checklist to fix this:\n"
        "   1. Provide 'fabric_capacity_id' in the workflow inputs (most common cause).\n"
        "      Without an F-capacity, Fabric does NOT auto-create the Semantic Model.\n"
        "\n"
        "   2. Ensure your SP is a Workspace Admin:\n"
        f"      https://app.fabric.microsoft.com/groups/{workspace_id} ->\n"
        "      Manage access -> verify your app is listed as Admin.\n"
        "\n"
        "   Re-run the workflow with skip_data_upload=true + fabric_capacity_id set.\n"
    )
    return None


# ─── Fabric IQ Ontology ───────────────────────────────────────────────────────

def get_or_create_ontology(
    workspace_id: str,
    ontology_name: str,
    semantic_model_id: str,
    token: str,
) -> Optional[str]:
    """
    Finds or creates a Fabric IQ Ontology in the workspace linked to the given
    semantic model.  Returns the ontology item ID, or None on failure.

    The ontology provides a semantic layer over the data model that improves the
    Data Agent's ability to answer natural-language queries correctly.
    """
    print(f"🧠 Checking for Fabric IQ Ontology: {ontology_name}")

    req_headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # ── Check for an existing ontology ───────────────────────────────────
    try:
        list_resp = fabric_request(
            "GET", f"/workspaces/{workspace_id}/items?type=Ontology", token
        )
        for item in list_resp.json().get("value", []) or []:
            if item.get("displayName") == ontology_name:
                ontology_id = item["id"]
                print(f"✓ Found existing Ontology: {ontology_name} (ID: {ontology_id})")
                return ontology_id
    except Exception as exc:
        print(f"   [WARN] Listing ontologies failed (non-fatal): {exc}")

    # ── Create the ontology linked to the semantic model ──────────────────
    print(f"📦 Creating Fabric IQ Ontology: {ontology_name}")
    create_payload: Dict[str, Any] = {
        "displayName": ontology_name,
        "type": "Ontology",
        "description": "Customer360 ontology for Fabric IQ Data Agent",
        "configuration": {
            "semanticModelId": semantic_model_id,
            "workspaceId": workspace_id,
        },
    }
    try:
        create_resp = requests.post(
            f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/items",
            headers=req_headers,
            json=create_payload,
            timeout=60,
        )

        if create_resp.status_code in (200, 201):
            ontology_id = create_resp.json().get("id")
            if ontology_id:
                print(f"✓ Created Ontology: {ontology_name} (ID: {ontology_id})")
                return ontology_id

        elif create_resp.status_code == 202:
            op_id = (
                create_resp.headers.get("x-ms-operation-id")
                or create_resp.headers.get("x-ms-operationid")
            )
            if op_id:
                poll_operation(op_id, token, "ontology creation")
            # Re-fetch after async creation
            list_resp2 = fabric_request(
                "GET", f"/workspaces/{workspace_id}/items?type=Ontology", token
            )
            for item in list_resp2.json().get("value", []) or []:
                if item.get("displayName") == ontology_name:
                    ontology_id = item["id"]
                    print(f"✓ Ontology ready: {ontology_name} (ID: {ontology_id})")
                    return ontology_id
        else:
            print(
                f"   ⚠️  Ontology creation returned HTTP {create_resp.status_code}: "
                f"{create_resp.text[:300]}\n"
                "   The Data Agent will continue without an ontology."
            )
    except Exception as exc:
        print(
            f"   ⚠️  Ontology creation failed (non-fatal): {exc}\n"
            "   The Data Agent will continue without an ontology."
        )
    return None


# ─── Fabric IQ Ontology (simplified) ─────────────────────────────────────────

def build_customer360_ontology(
    workspace_id: str,
    lakehouse_id: str,
) -> Dict[str, Any]:
    """Builds the Customer360 ontology definition with full LakehouseTable entity bindings.

    The returned dict encodes a ``Customer`` entity backed by the ``Customer360``
    Delta table in the specified Lakehouse.  Each attribute carries the correct
    Fabric ontology type (``"string"`` or ``"decimal"``) so that the Ontology UI
    shows the binding tab pointing at the Lakehouse table.

    Args:
        workspace_id: Fabric workspace GUID.
        lakehouse_id: Fabric Lakehouse item GUID (the ``itemId``).

    Returns:
        A dict ready to be JSON-serialised and base64-encoded as the
        ``definition.json`` payload sent to the Fabric ontologies API.
    """
    return {
        "entities": [
            {
                "name": "Customer",
                "key": "CustomerId",
                "attributes": [
                    {"name": "CustomerId", "type": "string"},
                    {"name": "FullName", "type": "string"},
                    {"name": "State", "type": "string"},
                    {"name": "City", "type": "string"},
                    {"name": "Segment", "type": "string"},
                    {"name": "LifetimeValue", "type": "decimal"},
                    {"name": "MonthlyRevenue", "type": "decimal"},
                    {"name": "ChurnRiskScore", "type": "decimal"},
                ],
                "source": {
                    "type": "LakehouseTable",
                    "workspaceId": workspace_id,
                    "itemId": lakehouse_id,
                    "schema": "dbo",
                    "table": "Customer360",
                },
            }
        ]
    }


def build_ontology_definition(
    entity_name: str,
    table_name: str,
    columns: List[Dict[str, str]],
) -> Dict[str, Any]:
    """Builds an ontology definition dict with entity schema for definition.json.

    Args:
        entity_name: Conceptual entity name (e.g. ``"Customer"``).
        table_name:  Source table name in the Lakehouse (e.g. ``"Customer360"``).
        columns:     List of ``{"name": ..., "valueType": ...}`` dicts describing
                     each attribute.  Supported valueTypes: ``"String"``,
                     ``"Double"``, ``"Int64"``, ``"Boolean"``, ``"DateTime"``.

    Returns:
        A dict ready to be JSON-serialised and base64-encoded as the
        ``definition.json`` payload sent to the Fabric ontologies API.
    """
    return {
        "entities": [
            {
                "name": entity_name,
                "source": {"table": table_name},
                "attributes": columns,
            }
        ]
    }


def infer_columns_from_csv(csv_path: str) -> List[Dict[str, str]]:
    """Infers column definitions from a CSV file header row.

    Each column receives a ``valueType`` of ``"Double"`` when the first data
    row contains a parseable float, or ``"String"`` otherwise.  Only these two
    value types are used; no attempt is made to distinguish integers, booleans,
    or date strings from general strings.

    Args:
        csv_path: Absolute or relative path to the CSV file.

    Returns:
        List of ``{"name": <header>, "valueType": <"String"|"Double">}`` dicts
        in the order the columns appear in the file.
    """
    import csv as _csv

    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = _csv.DictReader(fh)
        headers: List[str] = list(reader.fieldnames or [])
        first_row: Dict[str, str] = next(iter(reader), {})

    columns: List[Dict[str, str]] = []
    for header in headers:
        value_type = "String"
        raw = first_row.get(header, "")
        try:
            float(raw)
            value_type = "Double"
        except (ValueError, TypeError):
            pass
        columns.append({"name": header, "valueType": value_type})
    return columns


def create_ontology(
    workspace_id: str,
    ontology_name: str,
    token: str,
    semantic_model_id: str = "",
    lakehouse_id: str = "",
    table_name: str = "",
    columns: Optional[List[Dict[str, str]]] = None,
) -> str:
    """Creates a Fabric IQ Ontology, optionally linked to a semantic model.

    Handles both synchronous (HTTP 201) and asynchronous (HTTP 202) provisioning.
    For the async case the long-running operation is polled until it succeeds and
    then the ontology ID is fetched by listing workspace ontologies.

    Definition priority order:
      1. *semantic_model_id* provided → Fabric IQ derives schema automatically.
      2. *lakehouse_id* provided → full Customer360 entity with LakehouseTable
         binding via :func:`build_customer360_ontology`.
      3. *table_name* + *columns* provided → generic entity definition via
         :func:`build_ontology_definition`.
      4. Neither provided → empty ``{"entities": []}`` fallback.

    Raises RuntimeError when creation fails.
    """
    print("🧠 Step: Creating Fabric IQ ontology")

    # Check whether the ontology already exists to avoid failures on reruns.
    try:
        list_resp = fabric_request(
            "GET",
            f"/workspaces/{workspace_id}/items?type=Ontology",
            token,
        )
        for item in list_resp.json().get("value", []):
            if item.get("displayName") == ontology_name:
                ontology_id = item["id"]
                print(f"✓ Found existing ontology: {ontology_name} (ID: {ontology_id})")
                return ontology_id
    except Exception as exc:
        print(f"   [WARN] Could not list ontologies (non-fatal): {exc}. Proceeding to attempt creation.")

    url = f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/ontologies"

    # Build the base64-encoded ontology definition (required by the API).
    # Priority order:
    #   1. Semantic model reference  → Fabric IQ derives schema automatically.
    #   2. Lakehouse binding         → Customer360 entity with LakehouseTable source.
    #   3. Explicit table + columns  → full entity definition with attributes.
    #   4. Neither provided          → empty entities list (fallback).
    if semantic_model_id:
        ontology_definition: Dict[str, Any] = {"semanticModelId": semantic_model_id}
    elif lakehouse_id:
        ontology_definition = build_customer360_ontology(workspace_id, lakehouse_id)
    elif table_name and columns:
        ontology_definition = build_ontology_definition(table_name, table_name, columns)
    else:
        ontology_definition = {"entities": []}

    ontology_def = base64.b64encode(
        json.dumps(ontology_definition).encode()
    ).decode()

    definition: Dict[str, Any] = {
        "parts": [
            {
                "path": "definition.json",
                "payload": ontology_def,
                "payloadType": "InlineBase64",
            }
        ]
    }

    payload: Dict[str, Any] = {
        "displayName": ontology_name,
        "description": "Customer360 ontology",
        "definition": definition,
    }

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=60,
    )

    # SUCCESS CASE (sync)
    if resp.status_code in (200, 201):
        ontology_id = resp.json()["id"]
        print(f"✓ Ontology created: {ontology_id}")
        return ontology_id

    # ASYNC CASE (most common in Fabric preview)
    if resp.status_code == 202:
        operation_id = (
            resp.headers.get("x-ms-operation-id")
            or resp.headers.get("x-ms-operationid")
            or resp.json().get("operationId")
        )

        print("Ontology provisioning started...")
        if operation_id:
            poll_operation(operation_id, token, "ontology creation")

        # Fabric creates 3 backend resources (Ontology, Graph Model, Ontology
        # Lakehouse) — wait for them to propagate before fetching the ID.
        print("Waiting for ontology backend provisioning...")
        time.sleep(ONTOLOGY_PROPAGATION_WAIT_SECONDS)

        # Fetch ontology ID after provisioning completes.
        list_resp = fabric_request(
            "GET",
            f"/workspaces/{workspace_id}/items?type=Ontology",
            token,
        )
        for item in list_resp.json().get("value", []):
            if item.get("displayName") == ontology_name:
                ontology_id = item["id"]
                print(f"✓ Ontology ready: {ontology_id}")
                return ontology_id

        raise RuntimeError(
            f"Ontology '{ontology_name}' not found after async provisioning completed."
        )

    raise RuntimeError(
        f"Ontology creation failed: {resp.status_code} {resp.text}"
    )


# ─── Fabric Data Agent (simplified) ──────────────────────────────────────────

def create_data_agent(workspace_id: str, semantic_model_id: str, token: str) -> str:
    """Creates a Fabric Data Agent linked to the given semantic model."""
    print("🤖 Step: Creating Data Agent")

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataAgents"

    payload = {
        "displayName": "Customer360Agent",
        "semanticModelId": semantic_model_id,
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    r = requests.post(url, headers=headers, json=payload, timeout=60)

    if r.status_code not in [200, 201]:
        raise Exception(f"Data agent creation failed with status {r.status_code}: {r.text}")

    agent_id = r.json()["id"]

    print(f"✓ Data agent created: {agent_id}")

    return agent_id


def attach_ontology_to_agent(
    workspace_id: str, agent_id: str, ontology_id: str, token: str
) -> None:
    """Attaches a Fabric IQ Ontology to a Data Agent."""
    print("🔗 Step: Attaching ontology to agent")

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataAgents/{agent_id}"

    payload = {
        "ontologyIds": [ontology_id],
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    r = requests.patch(url, headers=headers, json=payload, timeout=60)

    if r.status_code not in [200, 204]:
        raise Exception(f"Failed attaching ontology with status {r.status_code}: {r.text}")

    print("✓ Ontology attached to agent")


# ─── Pre-flight cleanup ───────────────────────────────────────────────────────

def _delete_fabric_item(
    workspace_id: str,
    item_id: str,
    item_type: str,
    display_name: str,
    token: str,
) -> None:
    """Delete a single Fabric workspace item by ID.

    Tries a type-specific REST endpoint first (where available), then falls
    back to the generic ``/items/{id}`` endpoint.  Failures are logged but
    never raised.

    DataAgent and Lakehouse each have a dedicated DELETE endpoint in addition
    to the generic items endpoint.  Ontology and SemanticModel only support the
    generic endpoint, so they go straight to ``/items/{id}``.
    """
    type_path_map: Dict[str, str] = {
        "DataAgent": f"/workspaces/{workspace_id}/dataAgents/{item_id}",
        "Lakehouse": f"/workspaces/{workspace_id}/lakehouses/{item_id}",
    }
    paths: List[str] = []
    if item_type in type_path_map:
        paths.append(type_path_map[item_type])
    paths.append(f"/workspaces/{workspace_id}/items/{item_id}")

    auth_headers = {"Authorization": f"Bearer {token}"}
    for path in paths:
        try:
            resp = requests.delete(
                f"{FABRIC_BASE_URL}{path}",
                headers=auth_headers,
                timeout=30,
            )
            if resp.ok or resp.status_code == 404:
                print(f"   🗑️  Deleted {item_type} '{display_name}' (ID: {item_id})")
                return
        except Exception as exc:
            print(f"   ⚠️  Delete {item_type} '{display_name}' via {path} failed: {exc}")
    print(f"   ⚠️  Could not delete {item_type} '{display_name}' — proceeding anyway")


def delete_workspace_items(
    workspace_id: str,
    token: str,
    lakehouse_name: str = "",
    ontology_name: str = "",
    dataagent_name: str = "",
) -> None:
    """
    Deletes existing Fabric items (Lakehouse, Ontology, DataAgent, SemanticModel)
    from the workspace before a fresh deployment.

    This ensures a clean-slate re-deploy when ``--force_recreate`` is passed.
    Each item type is listed and any item whose ``displayName`` matches the
    supplied name is deleted.  Failures are logged but never fatal.

    Fabric's internal order requires deletion in reverse-dependency order:
      DataAgent → Ontology → SemanticModel → Lakehouse
    """
    print("🧹 Pre-flight cleanup: checking for existing Fabric items to delete...")

    auth_headers = {"Authorization": f"Bearer {token}"}

    # ── 1. Delete DataAgent ───────────────────────────────────────────────────
    # The dedicated /dataAgents endpoint is tried first because it returns richer
    # metadata; the generic /items?type=DataAgent endpoint is the fallback.
    if dataagent_name:
        try:
            for path in (
                f"/workspaces/{workspace_id}/dataAgents",
                f"/workspaces/{workspace_id}/items?type=DataAgent",
            ):
                resp = requests.get(
                    f"{FABRIC_BASE_URL}{path}", headers=auth_headers, timeout=30
                )
                if resp.status_code == 200:
                    for item in resp.json().get("value", []) or []:
                        if item.get("displayName") == dataagent_name:
                            _delete_fabric_item(
                                workspace_id, item["id"], "DataAgent", dataagent_name, token
                            )
                    break
        except Exception as exc:
            print(f"   ⚠️  Could not list DataAgents (non-fatal): {exc}")

    # ── 2. Delete Ontology ────────────────────────────────────────────────────
    if ontology_name:
        try:
            resp = requests.get(
                f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/items?type=Ontology",
                headers=auth_headers,
                timeout=30,
            )
            if resp.status_code == 200:
                for item in resp.json().get("value", []) or []:
                    if item.get("displayName") == ontology_name:
                        _delete_fabric_item(
                            workspace_id, item["id"], "Ontology", ontology_name, token
                        )
        except Exception as exc:
            print(f"   ⚠️  Could not list Ontologies (non-fatal): {exc}")

    # ── 3. Delete SemanticModel (default model auto-created by Fabric) ────────
    if lakehouse_name:
        try:
            resp = requests.get(
                f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/items?type=SemanticModel",
                headers=auth_headers,
                timeout=30,
            )
            if resp.status_code == 200:
                for item in resp.json().get("value", []) or []:
                    if item.get("displayName") == lakehouse_name:
                        _delete_fabric_item(
                            workspace_id, item["id"], "SemanticModel", item["displayName"], token
                        )
        except Exception as exc:
            print(f"   ⚠️  Could not list SemanticModels (non-fatal): {exc}")

    # ── 4. Delete Lakehouse ───────────────────────────────────────────────────
    if lakehouse_name:
        try:
            resp = requests.get(
                f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/items?type=Lakehouse",
                headers=auth_headers,
                timeout=30,
            )
            if resp.status_code == 200:
                for item in resp.json().get("value", []) or []:
                    if item.get("displayName") == lakehouse_name:
                        _delete_fabric_item(
                            workspace_id, item["id"], "Lakehouse", lakehouse_name, token
                        )
        except Exception as exc:
            print(f"   ⚠️  Could not list Lakehouses (non-fatal): {exc}")

    print("   ✓ Pre-flight cleanup complete.")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fabric Customer360 setup: workspace → capacity binding → lakehouse "
            "→ CSV upload → table load → Semantic Model → Fabric IQ Ontology "
            "→ Data Agent"
        )
    )
    parser.add_argument("--workspace_name", required=True, help="Fabric workspace display name")
    parser.add_argument("--lakehouse_name", required=True, help="Lakehouse display name")
    parser.add_argument("--csv_path", required=True, help="Local path to customer360.csv")
    parser.add_argument("--table_name", required=True, help="Delta table name in Lakehouse")
    parser.add_argument("--dataagent_name", required=True, help="Fabric Data Agent display name")
    parser.add_argument(
        "--capacity_id",
        required=False,
        default="",
        help="Fabric capacityId GUID (from Fabric Admin → Capacity settings)",
    )
    parser.add_argument(
        "--skip_data_upload",
        action="store_true",
        default=False,
        help="Skip CSV upload and table load (useful if table already exists)",
    )
    parser.add_argument(
        "--ontology_name",
        required=False,
        default="Customer360Ontology",
        help="Display name for the Fabric IQ Ontology to create",
    )
    parser.add_argument(
        "--app_service_principal_id",
        required=False,
        default="",
        help=(
            "Object ID (principal ID) of the App Service's System-Assigned Managed Identity. "
            "When set, this principal is added to the Fabric workspace as Contributor so the "
            "backend can call the Fabric Data Agent query API. "
            "Get it via: az webapp identity show --name <app> --resource-group <rg> "
            "--query principalId -o tsv"
        ),
    )
    parser.add_argument(
        "--force_recreate",
        action="store_true",
        default=False,
        help=(
            "Delete existing Fabric items (Lakehouse, Ontology, DataAgent, SemanticModel) "
            "before running deployment.  Use this for a clean-slate re-deploy."
        ),
    )

    args = parser.parse_args(argv)
    args.ontology_name = sanitize_name(args.ontology_name)

    if not os.path.isfile(args.csv_path):
        print(f"❌ CSV file not found: {args.csv_path}", file=sys.stderr)
        sys.exit(1)

    print("=" * 65)
    print("🚀 Fabric Customer360 Setup  –  Starting")
    print("=" * 65)

    try:
        # ── 1. Authenticate ───────────────────────────────────────────────
        print("\n🔐 Step 1: Authenticating...")
        fabric_token = get_fabric_token()
        print("   ✓ Fabric token acquired")

        storage_token = None
        if not args.skip_data_upload:
            storage_token = get_storage_token()
            print("   ✓ Storage (OneLake) token acquired")

        # ── 2. Workspace ──────────────────────────────────────────────────
        print("\n📁 Step 2: Workspace")
        workspace_id = get_or_create_workspace(
            args.workspace_name, args.capacity_id, fabric_token
        )

        # ── 2a. Grant App Service MI workspace access ──────────────────────
        # The backend App Service uses its System-Assigned Managed Identity to
        # call the Fabric Data Agent query API.  Without workspace membership
        # the MI gets HTTP 404 EntityNotFound on all Fabric resource requests
        # (Fabric hides resources from non-members as a security measure).
        if args.app_service_principal_id:
            print(f"\n🔐 Step 2a: Granting App Service MI access to workspace")
            add_workspace_member(
                workspace_id,
                args.app_service_principal_id,
                fabric_token,
                role="Contributor",
            )
        else:
            print(
                "\n⚠️  Step 2a: --app_service_principal_id not provided.\n"
                "   The App Service Managed Identity may not have access to the Fabric\n"
                "   workspace, causing HTTP 404 errors when the backend queries the Data Agent.\n"
                "   To fix, re-run with --app_service_principal_id <MI-object-id>, or add\n"
                "   the MI manually via: Fabric portal -> Workspace -> Manage access."
            )

        # ── 2b. Pre-flight cleanup (--force_recreate) ──────────────────────
        if args.force_recreate:
            print("\n🧹 Step 2b: Pre-flight cleanup (--force_recreate)")
            delete_workspace_items(
                workspace_id,
                fabric_token,
                lakehouse_name=args.lakehouse_name,
                ontology_name=args.ontology_name,
                dataagent_name=args.dataagent_name,
            )

        # ── 3. Lakehouse ──────────────────────────────────────────────────
        print("\n🏗️  Step 3: Lakehouse")
        lakehouse_id = get_or_create_lakehouse(
            workspace_id, args.lakehouse_name, fabric_token
        )

        # ── 4. CSV upload + table load ────────────────────────────────────
        if args.skip_data_upload:
            print("\n⏭️  Step 4: Skipping CSV upload (--skip_data_upload set)")
        else:
            print("\n📤 Step 4: Upload CSV to OneLake")
            onelake_filename = upload_csv_to_onelake(
                workspace_id, lakehouse_id, args.csv_path, storage_token
            )

            print("\n📊 Step 5: Load Delta table")
            load_table_from_file(
                workspace_id,
                lakehouse_id,
                args.table_name,
                onelake_filename,
                fabric_token,
            )

        # ── 5 / 6. Semantic Model (default from Lakehouse) ─────────────────
        # Moved BEFORE Data Agent so the agent can link to the semantic model
        # (linking via SemanticModel makes the data source visible in the
        # Fabric portal "Explorer → Data" pane).
        step_label = "Step 5" if args.skip_data_upload else "Step 6"
        print(f"\n📐 {step_label}: Semantic Model")
        # Re-acquire token in case it expired during data upload
        fabric_token = get_fabric_token()

        # Ensure the Fabric capacity is Active before searching for the SM.
        # If the capacity was Paused when the Lakehouse was provisioned, Fabric
        # would not have auto-created the default semantic model.
        # Force-reassigning the workspace to an Active capacity triggers that
        # retroactive creation (it may still take 60-90 s to materialise).
        if args.capacity_id:
            print("   Checking Fabric capacity state...")
            capacity_active = ensure_capacity_active(args.capacity_id, fabric_token)
            if capacity_active:
                print(
                    "   Re-assigning workspace to active capacity "
                    "(triggers default semantic model creation if missing)..."
                )
                _assign_capacity(workspace_id, args.capacity_id, fabric_token)
                print("   Waiting 90 s for Fabric to materialise the default semantic model...")
                time.sleep(90)
            else:
                print(
                    "   [WARN] Could not confirm capacity is Active — "
                    "proceeding with semantic model search anyway"
                )

        semantic_model_id = get_default_semantic_model(
            workspace_id, args.lakehouse_name, lakehouse_id, fabric_token,
            table_name=args.table_name,
        )

        # ── Step 6: Create Ontology ───────────────────────────────────────
        print("\n🧠 Step 6: Create Ontology")
        # Infer column schema from the CSV so the ontology definition.json
        # contains a proper entity definition when no semantic model is available.
        csv_columns: List[Dict[str, str]] = []
        if not args.skip_data_upload and os.path.isfile(args.csv_path):
            csv_columns = infer_columns_from_csv(args.csv_path)
        ontology_id = create_ontology(
            workspace_id,
            args.ontology_name,
            fabric_token,
            semantic_model_id=semantic_model_id or "",
            lakehouse_id=lakehouse_id,
            table_name=args.table_name,
            columns=csv_columns or None,
        )

        # ── Step 7: Create Data Agent (with ontology attached at creation) ─
        # The ontology is included in the create_config so Fabric follows the
        # correct internal order: Ontology → Agent creation → Published.
        print("\n🤖 Step 7: Create Data Agent")
        dataagent_id = get_or_create_dataagent(
            workspace_id,
            args.dataagent_name,
            lakehouse_id,
            fabric_token,
            table_name=args.table_name,
            semantic_model_id=semantic_model_id or "",
            ontology_id=ontology_id or "",
        )

        # ── Step 8: Configure agent datasource ───────────────────────────
        # Ontology is NOT included here — it was already attached at creation.
        print("\n⚙️  Step 8: Configure agent datasource")
        configure_dataagent(
            workspace_id,
            dataagent_id,
            args.dataagent_name,
            lakehouse_id,
            args.table_name,
            fabric_token,
            semantic_model_id=semantic_model_id or "",
        )

        # ── Step 9: Validate Data Agent ───────────────────────────────────
        print("\n✅ Step 9: Validate Data Agent")
        fabric_token = get_fabric_token()
        validate_dataagent(
            workspace_id, dataagent_id, args.dataagent_name,
            lakehouse_id, fabric_token,
            semantic_model_id=semantic_model_id or "",
            ontology_id=ontology_id or "",
        )

        workspace_url = f"https://app.fabric.microsoft.com/groups/{workspace_id}"

        # ── Summary ───────────────────────────────────────────────────────
        print("\n" + "=" * 65)
        print("✅ Fabric Customer360 Setup Complete!")
        print("=" * 65)
        result = {
            "workspace_id":       workspace_id,
            "lakehouse_id":       lakehouse_id,
            "table_name":         args.table_name,
            "dataagent_id":       dataagent_id,
            "semantic_model_id":  semantic_model_id or "",
            "ontology_id":        ontology_id or "",
            "workspace_url":      workspace_url,
            "capacity_id":        args.capacity_id,
        }
        print(json.dumps(result, indent=2))

        # Emit GitHub Actions outputs if running in CI
        github_output = os.environ.get("GITHUB_OUTPUT")
        if github_output:
            with open(github_output, "a") as f:
                for k, v in result.items():
                    f.write(f"{k}={v}\n")

    except Exception as ex:
        print(f"\n❌ Error: {ex}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
