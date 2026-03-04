#!/usr/bin/env python3
"""
Fabric Customer360 Setup Script
--------------------------------
Creates/finds Fabric workspace, binds to capacity, creates Lakehouse,
uploads CSV to OneLake Files, loads CSV as a Delta table, and creates
the Fabric Data Agent connected to that Lakehouse.

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


def get_fabric_token() -> str:
    credential = DefaultAzureCredential()
    return credential.get_token(FABRIC_SCOPE).token


def get_storage_token() -> str:
    """Token for OneLake ADLS Gen2 file uploads (storage.azure.com scope)."""
    credential = DefaultAzureCredential()
    return credential.get_token(STORAGE_SCOPE).token


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
    resp = fabric_request("GET", "/workspaces", token)
    return resp.json().get("value", []) or []


def get_or_create_workspace(
    workspace_name: str,
    capacity_id: Optional[str],
    token: str,
) -> str:
    print(f"🔍 Looking for workspace: {workspace_name}")
    workspaces = list_workspaces(token)

    for ws in workspaces:
        if ws.get("displayName") == workspace_name:
            ws_id = ws["id"]
            print(f"✓ Found existing workspace: {workspace_name} (ID: {ws_id})")
            current_capacity = ws.get("capacityId")
            if current_capacity:
                print(f"   Current capacityId: {current_capacity}")
            else:
                print("   ⚠️  No capacity assigned")

            if capacity_id and (
                not current_capacity or current_capacity.lower() != capacity_id.lower()
            ):
                print(f"🔄 Re-assigning to capacityId: {capacity_id}")
                fabric_request(
                    "POST",
                    f"/workspaces/{ws_id}/assignToCapacity",
                    token,
                    json={"capacityId": capacity_id},
                )
                print("✓ Workspace re-assigned to Fabric capacity!")
            elif capacity_id:
                print("   ✓ Already assigned to correct capacity")
            return ws_id

    print(f"📦 Workspace not found. Creating: {workspace_name}")
    payload: Dict[str, Any] = {"displayName": workspace_name}
    if capacity_id:
        payload["capacityId"] = capacity_id

    resp = fabric_request("POST", "/workspaces", token, json=payload)
    location = resp.headers.get("Location", "")
    if not location:
        raise RuntimeError("Workspace created but Location header missing")

    workspace_id = urlparse(location).path.rstrip("/").split("/")[-1]
    print(f"✓ Created workspace: {workspace_name} (ID: {workspace_id})")
    return workspace_id


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

def load_table_from_file(
    workspace_id: str,
    lakehouse_id: str,
    table_name: str,
    onelake_filename: str,
    fabric_token: str,
) -> None:
    """
    Triggers the Fabric Load Table API which reads the CSV from OneLake Files/
    and writes it as a managed Delta table in the Lakehouse Tables/ section.
    This is an async operation; we poll until it completes.
    """
    print(f"📊 Loading table '{table_name}' from Files/{onelake_filename}...")

    payload = {
        "relativePath": f"Files/{onelake_filename}",
        "pathType": "File",
        "format": "Csv",
        "formatOptions": {
            "header": "true",
            "inferSchema": "true",
        },
        "mode": "Overwrite",
    }

    resp = requests.post(
        f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
        f"/tables/{table_name}/load",
        headers={
            "Authorization": f"Bearer {fabric_token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=60,
    )

    if resp.status_code == 200:
        print(f"✓ Table '{table_name}' loaded (synchronous).")
        return

    if resp.status_code == 202:
        # Async – get operation id from header or body
        operation_id = resp.headers.get("x-ms-operation-id") or resp.json().get(
            "operationId"
        )
        if not operation_id:
            # Some versions return Location header
            location = resp.headers.get("Location", "")
            operation_id = location.rstrip("/").split("/")[-1] if location else None

        if operation_id:
            poll_operation(operation_id, fabric_token, f"load table '{table_name}'")
            print(f"✓ Table '{table_name}' loaded successfully as Delta table.")
        else:
            print(
                "⚠️  Load table accepted (202) but no operation ID found to poll. "
                "Check Fabric portal to confirm table creation."
            )
        return

    raise RuntimeError(
        f"Load table API failed [{resp.status_code}]: {resp.text[:300]}"
    )


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
                    print(f"✓ Found existing Data Agent: {dataagent_name} (ID: {agent_id})")
                    return agent_id

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
            create_resp = requests.post(
                f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/dataAgents",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json=create_payload,
                timeout=60,
            )
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
            print(f"✓ Found existing Data Agent item: {dataagent_name} (ID: {agent_id})")
            return agent_id

    # Create generic item
    item_resp = fabric_request(
        "POST",
        f"/workspaces/{workspace_id}/items",
        token,
        json={
            "displayName": dataagent_name,
            "type": "DataAgent",
            "description": "Customer360 conversational analytics agent",
        },
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


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fabric Customer360 setup: workspace → capacity binding → lakehouse "
            "→ CSV upload → table load → Data Agent"
        )
    )
    parser.add_argument("--workspace_name", required=True, help="Fabric workspace display name")
    parser.add_argument("--lakehouse_name", required=True, help="Lakehouse display name")
    parser.add_argument("--csv_path", required=True, help="Local path to customer360.csv")
    parser.add_argument("--table_name", required=True, help="Delta table name in Lakehouse")
    parser.add_argument("--dataagent_name", required=True, help="Fabric Data Agent display name")
    parser.add_argument(
        "--capacity_id",
        required=True,
        help="Fabric capacityId GUID (from Fabric Admin → Capacity settings)",
    )
    parser.add_argument(
        "--skip_data_upload",
        action="store_true",
        default=False,
        help="Skip CSV upload and table load (useful if table already exists)",
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
        print(f"\n🤖 {step_label}: Data Agent")
        dataagent_id = get_or_create_dataagent(
            workspace_id, args.dataagent_name, lakehouse_id, fabric_token
        )

        # ── Summary ───────────────────────────────────────────────────────
        print("\n" + "=" * 65)
        print("✅ Fabric Customer360 Setup Complete!")
        print("=" * 65)
        result = {
            "workspace_id": workspace_id,
            "lakehouse_id": lakehouse_id,
            "table_name": args.table_name,
            "dataagent_id": dataagent_id,
            "capacity_id": args.capacity_id,
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
