#!/usr/bin/env python3
import argparse
import json
import sys
from typing import Any, Dict, List
from urllib.parse import urlparse

import requests
from azure.identity import DefaultAzureCredential

FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
FABRIC_BASE_URL = "https://api.fabric.microsoft.com/v1"


def get_fabric_token() -> str:
    """Get a Fabric access token using DefaultAzureCredential."""
    credential = DefaultAzureCredential()
    token = credential.get_token(FABRIC_SCOPE)
    return token.token


def fabric_request(method: str, path: str, token: str, **kwargs) -> requests.Response:
    """
    Helper to call the Fabric REST API with the right base URL and Authorization header.
    """
    url = f"{FABRIC_BASE_URL}{path}"
    headers = kwargs.pop("headers", {}) or {}
    headers["Authorization"] = f"Bearer {token}"
    headers.setdefault("Content-Type", "application/json")

    resp = requests.request(method, url, headers=headers, **kwargs)
    if not resp.ok:
        print(f"❌ Fabric API error: {method} {path}")
        print(f"   Status: {resp.status_code}")
        print(f"   Response: {resp.text}")
        raise RuntimeError(f"Fabric API failed: {resp.status_code}")
    return resp


def list_workspaces(token: str) -> List[Dict[str, Any]]:
    """Return all workspaces visible to this identity."""
    resp = fabric_request("GET", "/workspaces", token)
    data = resp.json()
    return data.get("value", []) or []


def get_or_create_workspace(
    workspace_name: str,
    capacity_id: str | None,
    token: str,
) -> str:
    """
    If a workspace with displayName == workspace_name exists, return its id.
    Otherwise create it, passing capacityId in the payload when provided.[web:316]
    """
    print(f"🔍 Looking for workspace: {workspace_name}")
    workspaces = list_workspaces(token)

    for ws in workspaces:
        if ws.get("displayName") == workspace_name:
            ws_id = ws["id"]
            print(f"✓ Found existing workspace: {workspace_name} (ID: {ws_id})")
            if "capacityId" in ws:
                print(f"   Existing capacityId: {ws['capacityId']}")
            return ws_id

    print(f"📦 Workspace not found. Creating new workspace: {workspace_name}")
    payload: Dict[str, Any] = {
        "displayName": workspace_name,
    }
    # Use capacityId if provided, as shown in the REST docs and blog examples.[web:316][page:1]
    if capacity_id:
        payload["capacityId"] = capacity_id

    # POST /workspaces
    resp = fabric_request("POST", "/workspaces", token, json=payload)

    # Official pattern: read the new workspace id from the Location header.[web:316][page:1]
    location_header = resp.headers.get("Location")
    if not location_header:
        raise RuntimeError(
            "Workspace created but Location header missing; cannot determine workspace id."
        )

    parsed_url = urlparse(location_header)
    path_parts = parsed_url.path.rstrip("/").split("/")
    workspace_id = path_parts[-1]

    print(f"✓ Created workspace: {workspace_name} (ID: {workspace_id})")
    print(f"   Location: {location_header}")
    return workspace_id


def get_or_create_lakehouse(
    workspace_id: str,
    lakehouse_name: str,
    token: str,
) -> str:
    """
    Check if a lakehouse already exists in the workspace; if not, create it.
    Uses the Lakehouse Items API.[web:1][web:313]
    """
    print(f"🏗️  Checking lakehouse: {lakehouse_name}")

    # List lakehouses in this workspace
    resp = fabric_request(
        "GET",
        f"/workspaces/{workspace_id}/items?type=Lakehouse",
        token,
    )
    items = resp.json().get("value", []) or []

    for item in items:
        if item.get("displayName") == lakehouse_name:
            lh_id = item["id"]
            print(f"✓ Found existing lakehouse: {lakehouse_name} (ID: {lh_id})")
            return lh_id

    print(f"📦 Creating lakehouse: {lakehouse_name}")
    payload = {
        "displayName": lakehouse_name,
        "description": "Lakehouse created by Fabric Customer360 deployment",
    }

    # POST /workspaces/{workspaceId}/lakehouses[web:1][web:319]
    resp = fabric_request(
        "POST",
        f"/workspaces/{workspace_id}/lakehouses",
        token,
        json=payload,
    )

    data = resp.json()
    lakehouse_id = data.get("id")
    if not lakehouse_id:
        raise RuntimeError("Lakehouse creation did not return an 'id' field.")

    print(f"✓ Created lakehouse: {lakehouse_name} (ID: {lakehouse_id})")
    return lakehouse_id


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fabric Customer360 setup "
            "(create or reuse workspace with capacityId, then lakehouse)"
        )
    )
    parser.add_argument("--workspace_name", required=True)
    parser.add_argument("--lakehouse_name", required=True)
    parser.add_argument("--csv_path", required=True)
    parser.add_argument("--table_name", required=True)
    parser.add_argument("--dataagent_name", required=True)
    parser.add_argument(
        "--capacity_id",
        required=False,
        help=(
            "Optional Fabric capacityId to bind the workspace to when creating it. "
            "You can copy this from Fabric Admin → Capacity settings."
        ),
    )

    args = parser.parse_args(argv)

    print("=" * 60)
    print("🚀 Fabric Customer360 Setup (Workspace with capacityId) Starting")
    print("=" * 60)

    try:
        print("🔐 Authenticating with Fabric...")
        token = get_fabric_token()
        print("✓ Authentication successful")

        # 1) Get or create workspace, binding to capacityId if provided.
        workspace_id = get_or_create_workspace(
            args.workspace_name,
            args.capacity_id,
            token,
        )

        # 2) Get or create lakehouse in that workspace.
        lakehouse_id = get_or_create_lakehouse(
            workspace_id,
            args.lakehouse_name,
            token,
        )

        # TODO: Add CSV upload, table creation, and data agent setup here,
        # using the lakehouse REST APIs and your csv_path/table_name/dataagent_name.

        print("\n" + "=" * 60)
        print("✅ Fabric Customer360 Setup Complete!")
        print("=" * 60)
        result = {
            "workspace_id": workspace_id,
            "lakehouse_id": lakehouse_id,
            "table_name": args.table_name,
        }
        print(json.dumps(result, indent=2))

    except Exception as ex:
        print(f"\n❌ Error: {ex}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
