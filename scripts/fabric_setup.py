#!/usr/bin/env python3
"""
Fabric Customer360 Setup Script
--------------------------------
Creates/finds Fabric workspace, binds to capacity, creates Lakehouse,
uploads CSV to OneLake Files, loads CSV as a Delta table, creates the
Fabric Data Agent connected to that Lakehouse, and locates the default
Semantic Model + Power BI Report for embedding.

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
import sys
import time
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
    Returns True if GET /dataAgents/{agent_id} succeeds (agent is queryable).
    A listed agent can be in a broken/incomplete state (e.g. created via the
    generic Items API without proper initialisation); validating before using
    the ID prevents stale IDs propagating into the App Service setting.
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
) -> str:
    """
    Creates (or finds) a Fabric Data Agent in the workspace and links it to the
    specified Lakehouse so it can answer natural-language queries.

    The Fabric Data Agent REST API (`/v1/workspaces/{id}/dataAgents`) is in preview
    as of 2026-03.  If the endpoint returns 404 the function falls back to the
    generic Items API and logs a manual-configuration reminder.

    Returns the Data Agent item ID.
    """
    print(f"🤖 Checking for Data Agent: {dataagent_name}")

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
                        print(f"✓ Found existing Data Agent: {dataagent_name} (ID: {agent_id})")
                        return agent_id
                    # Agent is listed but not queryable — delete and recreate
                    print(
                        f"⚠️  Agent '{dataagent_name}' (ID: {agent_id}) exists but is not "
                        f"queryable — deleting and recreating..."
                    )
                    _delete_agent(workspace_id, agent_id, token)
                    break  # Exit search loop; fall through to creation below

            print(f"📦 Creating Data Agent: {dataagent_name}")
            create_payload = {
                "displayName": dataagent_name,
                "description": "Customer360 conversational analytics agent",
                "configuration": {
                    "dataSources": [
                        {
                            "type": "Lakehouse",
                            "workspaceId": workspace_id,
                            "itemId": lakehouse_id,
                        }
                    ]
                },
            }
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

    # Create generic item (retry on ItemDisplayNameNotAvailableYet)
    item_resp = None
    for _attempt in range(1, NAME_RETRY_MAX + 1):
        item_resp = requests.post(
            f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/items",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={
                "displayName": dataagent_name,
                "type": "DataAgent",
                "description": "Customer360 conversational analytics agent",
            },
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
            print(
                "   ℹ️  NOTE: Link this agent to the Lakehouse manually in the Fabric "
                "portal if the API did not auto-configure the data source."
            )
            return agent_id

    raise RuntimeError(
        f"Data Agent creation failed: could not obtain ID from any API path."
    )


# ─── Semantic Model (default Power BI dataset from Lakehouse) ────────────────

def get_default_semantic_model(
    workspace_id: str,
    lakehouse_name: str,
    token: str,
    retries: int = 24,
) -> Optional[str]:
    """
    Finds the default Power BI Semantic Model that Fabric auto-creates when
    a Lakehouse is provisioned.  Its display name matches the Lakehouse name.

    Retries up to `retries` times (30-second intervals, ~12 min total) because
    Fabric can take several minutes to materialise the default semantic model
    after a table load — especially on first deploy.

    Searches both 'SemanticModel' and 'Dataset' item types (the type name
    varies across Fabric API preview versions).  Falls back to listing all
    workspace items if neither typed search finds it.

    Returns the semantic model item ID, or None if not found.
    """
    print(f"📐 Looking for default semantic model: {lakehouse_name}")

    for attempt in range(1, retries + 1):
        # 1. Try SemanticModel type (current Fabric API name)
        for item_type in ("SemanticModel", "Dataset"):
            try:
                resp = fabric_request(
                    "GET",
                    f"/workspaces/{workspace_id}/items?type={item_type}",
                    token,
                )
                for item in resp.json().get("value", []) or []:
                    if item.get("displayName") == lakehouse_name:
                        sm_id = item["id"]
                        print(
                            f"✓ Found default semantic model: {lakehouse_name} "
                            f"(ID: {sm_id}, type: {item_type})"
                        )
                        return sm_id
            except Exception as exc:
                print(f"   ⚠️  type={item_type} lookup failed (non-fatal): {exc}")

        # 2. Every 4th attempt: list ALL items and dump names for diagnosis
        if attempt % 4 == 0:
            try:
                all_resp = fabric_request(
                    "GET", f"/workspaces/{workspace_id}/items", token
                )
                all_items = all_resp.json().get("value", []) or []
                print(
                    f"   Workspace items visible so far: "
                    + ", ".join(
                        f"{i.get('displayName')} ({i.get('type')})"
                        for i in all_items
                    ) or "(none)"
                )
                # Also try a partial-name / type-agnostic match
                for item in all_items:
                    name = item.get("displayName", "")
                    itype = item.get("type", "")
                    if name == lakehouse_name and itype not in (
                        "Lakehouse", "SQLEndpoint", "MirroredDatabase"
                    ):
                        sm_id = item["id"]
                        print(
                            f"✓ Found semantic model via all-items search: "
                            f"{name} (ID: {sm_id}, type: {itype})"
                        )
                        return sm_id
            except Exception as exc:
                print(f"   ⚠️  All-items fallback failed (non-fatal): {exc}")

        if attempt < retries:
            print(
                f"   ↻ [{attempt}/{retries}] Semantic model not ready yet — "
                f"waiting 30s for Fabric to materialise it..."
            )
            time.sleep(30)

    print(
        f"   ⚠️  Default semantic model '{lakehouse_name}' not found after "
        f"{retries} attempts ({retries * 30 // 60} min).\n"
        "   You can create a Power BI report manually in the Fabric portal later."
    )
    return None


# ─── Power BI Report ──────────────────────────────────────────────────────────

def _build_report_definition(semantic_model_id: str) -> Dict[str, Any]:
    """
    Build a minimal PBIR-Legacy report definition (base64-encoded parts) that
    creates a blank report with a live connection to the given semantic model.

    The Fabric Reports API (POST /v1/workspaces/{id}/reports) requires a full
    'definition' object — passing only 'semanticModelId' is not supported.

    Parts produced:
      definition.pbir  — XMLA-style live connection to the semantic model
      report.json      — Minimal single-page blank report layout
    """
    pbir = {
        "version": "1.0",
        "datasetReference": {
            "byConnection": {
                "connectionString": None,
                "pbiServiceModelId": None,
                "pbiModelVirtualServerName": "sobe_wowvirtualserver",
                "pbiModelDatabaseName": semantic_model_id,
                "name": "EntityDataSource",
                "connectionType": "pbiServiceXmlaStyleLive",
            }
        },
    }
    report_json = {
        "id": "00000000-0000-0000-0000-000000000000",
        "config": json.dumps({
            "version": "5.54",
            "themeCollection": {
                "baseTheme": {"name": "CY24SU06", "version": "5.54", "type": 2}
            },
        }),
        "layoutOptimization": 0,
        "publicCustomVisuals": [],
        "pods": [],
        "resourcePackages": [],
        "sections": [
            {
                "id": "ReportSection",
                "name": "ReportSection",
                "displayName": "Page 1",
                "filters": "[]",
                "ordinal": 0,
                "visualContainers": [],
                "config": json.dumps({"relationships": []}),
                "height": 720,
                "width": 1280,
                "type": 20,
            }
        ],
    }

    def _b64(obj: Any) -> str:
        return base64.b64encode(json.dumps(obj).encode()).decode()

    return {
        "format": "PBIR-Legacy",
        "parts": [
            {
                "path": "definition.pbir",
                "payload": _b64(pbir),
                "payloadType": "InlineBase64",
            },
            {
                "path": "report.json",
                "payload": _b64(report_json),
                "payloadType": "InlineBase64",
            },
        ],
    }


def get_or_create_report(
    workspace_id: str,
    report_name: str,
    semantic_model_id: str,
    token: str,
) -> Optional[str]:
    """
    Finds or creates a Power BI report in the workspace linked to the given
    semantic model.  Returns the report item ID, or None on failure.
    """
    print(f"📊 Checking for Power BI report: {report_name}")

    # ── Check for an existing report ─────────────────────────────────────
    resp = fabric_request(
        "GET", f"/workspaces/{workspace_id}/items?type=Report", token
    )
    for item in resp.json().get("value", []) or []:
        if item.get("displayName") == report_name:
            report_id = item["id"]
            print(f"✓ Found existing report: {report_name} (ID: {report_id})")
            return report_id

    # ── Create via Fabric Reports API with PBIR-Legacy definition ─────────
    print(f"📦 Creating Power BI report: {report_name}")
    try:
        definition = _build_report_definition(semantic_model_id)
        create_resp = requests.post(
            f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/reports",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={
                "displayName": report_name,
                "definition": definition,
            },
            timeout=60,
        )

        if create_resp.status_code in (200, 201):
            report_id = create_resp.json().get("id")
            if report_id:
                print(f"✓ Created report: {report_name} (ID: {report_id})")
                return report_id

        elif create_resp.status_code == 202:
            op_id = (
                create_resp.headers.get("x-ms-operation-id")
                or create_resp.headers.get("x-ms-operationid")
            )
            if op_id:
                poll_operation(op_id, token, "report creation")
            # Re-fetch after async creation
            resp2 = fabric_request(
                "GET", f"/workspaces/{workspace_id}/items?type=Report", token
            )
            for item in resp2.json().get("value", []) or []:
                if item.get("displayName") == report_name:
                    report_id = item["id"]
                    print(f"✓ Report ready: {report_name} (ID: {report_id})")
                    return report_id
        else:
            print(
                f"   ⚠️  Report creation returned HTTP {create_resp.status_code}: "
                f"{create_resp.text[:300]}"
            )
            print("   Trying generic Items API as fallback...")
            # Fallback: create as a generic item (no definition required)
            item_resp = requests.post(
                f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/items",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json={"displayName": report_name, "type": "Report"},
                timeout=60,
            )
            if item_resp.status_code in (200, 201):
                report_id = item_resp.json().get("id")
                if report_id:
                    print(f"✓ Created report via Items API: {report_name} (ID: {report_id})")
                    return report_id
            print(
                f"   ⚠️  Items API fallback also failed ({item_resp.status_code}). "
                "Create the report manually in the Fabric portal."
            )
    except Exception as exc:
        print(
            f"   ⚠️  Report creation failed: {exc}\n"
            "   Create the report manually in the Fabric portal."
        )
    return None


def build_powerbi_embed_url(workspace_id: str, report_id: Optional[str]) -> str:
    """
    Returns an embed URL for the Power BI report.
    autoAuth=true enables SSO when the viewer is already logged into Microsoft.
    """
    if not report_id:
        return ""
    return (
        f"https://app.powerbi.com/reportEmbed"
        f"?reportId={report_id}&groupId={workspace_id}&autoAuth=true"
    )


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fabric Customer360 setup: workspace → capacity binding → lakehouse "
            "→ CSV upload → table load → Data Agent → Semantic Model → PBI Report"
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
        "--report_name",
        required=False,
        default="Customer360 Report",
        help="Display name for the Power BI report to create",
    )

    args = parser.parse_args(argv)

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

        # ── 5 / 6. Data Agent ─────────────────────────────────────────────
        step_label = "Step 5" if args.skip_data_upload else "Step 6"
        print(f"\n🤖 {step_label}: Fabric Data Agent")
        dataagent_id = get_or_create_dataagent(
            workspace_id, args.dataagent_name, lakehouse_id, fabric_token
        )

        # ── 6 / 7. Semantic Model (default from Lakehouse) ────────────────
        next_step = 6 if args.skip_data_upload else 7
        print(f"\n📐 Step {next_step}: Semantic Model")
        # Re-acquire token in case it expired during data upload
        fabric_token = get_fabric_token()
        semantic_model_id = get_default_semantic_model(
            workspace_id, args.lakehouse_name, fabric_token
        )

        # ── 7 / 8. Power BI Report ────────────────────────────────────────
        next_step += 1
        print(f"\n📊 Step {next_step}: Power BI Report")
        report_id: Optional[str] = None
        if semantic_model_id:
            report_id = get_or_create_report(
                workspace_id, args.report_name, semantic_model_id, fabric_token
            )

        powerbi_embed_url = build_powerbi_embed_url(workspace_id, report_id)
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
            "report_id":          report_id or "",
            "powerbi_embed_url":  powerbi_embed_url,
            "workspace_url":      workspace_url,
            "capacity_id":        args.capacity_id,
        }
        print(json.dumps(result, indent=2))

        if not powerbi_embed_url:
            print(
                "\n💡 Power BI report not created automatically.\n"
                "   To embed a Power BI report in the frontend:\n"
                f"   1. Open your Fabric workspace: {workspace_url}\n"
                f"   2. Open '{args.lakehouse_name}' → click 'New report' in the ribbon\n"
                "   3. Add visuals (e.g. bar chart: State vs LifetimeValue, table of customers)\n"
                "   4. Save the report\n"
                "   5. In the report, click File → Embed report → Website or portal\n"
                "   6. Copy the embed URL\n"
                "   7. Re-run this GitHub Actions workflow with:\n"
                "        powerbi_report_url = <copied URL>\n"
                "        skip_data_upload = true\n"
            )

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
