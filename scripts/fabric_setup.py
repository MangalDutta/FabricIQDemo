#!/usr/bin/env python3
import argparse
import json
import sys
from typing import Any, Dict
import requests
from azure.identity import DefaultAzureCredential

FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
FABRIC_BASE_URL = "https://api.fabric.microsoft.com/v1"


def get_fabric_token() -> str:
    credential = DefaultAzureCredential()
    token = credential.get_token(FABRIC_SCOPE)
    return token.token


def fabric_request(method: str, path: str, token: str, **kwargs) -> requests.Response:
    url = f"{FABRIC_BASE_URL}{path}"
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"
    headers["Content-Type"] = "application/json"

    resp = requests.request(method, url, headers=headers, **kwargs)
    if not resp.ok:
        print(f"❌ Fabric API error: {method} {path}")
        print(f"   Status: {resp.status_code}")
        print(f"   Response: {resp.text}")
        raise RuntimeError(f"Fabric API failed: {resp.status_code}")
    return resp


def find_or_create_workspace(workspace_name: str, token: str) -> str:
    print(f"🔍 Looking for workspace: {workspace_name}")
    resp = fabric_request("GET", "/workspaces", token)
    data = resp.json()

    for workspace in data.get("value", []):
        if workspace.get("displayName") == workspace_name:
            ws_id = workspace["id"]
            print(f"✓ Found existing workspace: {workspace_name} (ID: {ws_id})")
            return ws_id

    print(f"📦 Workspace not found. Creating new workspace: {workspace_name}")
    payload = {"displayName": workspace_name}
    resp = fabric_request("POST", "/workspaces", token, json=payload)
    workspace = resp.json()
    ws_id = workspace["id"]
    print(f"✓ Created workspace: {workspace_name} (ID: {ws_id})")
    return ws_id


def ensure_lakehouse(workspace_id: str, lakehouse_name: str, token: str) -> str:
    print(f"🏗️  Checking lakehouse: {lakehouse_name}")
    resp = fabric_request(
        "GET",
        f"/workspaces/{workspace_id}/items?type=Lakehouse",
        token,
    )
    items = resp.json().get("value", [])

    for item in items:
        if item.get("displayName") == lakehouse_name:
            lh_id = item["id"]
            print(f"✓ Lakehouse exists: {lakehouse_name}")
            return lh_id

    print(f"📦 Creating lakehouse: {lakehouse_name}")
    payload = {"displayName": lakehouse_name}
    resp = fabric_request(
        "POST",
        f"/workspaces/{workspace_id}/lakehouses",
        token,
        json=payload,
    )
    lakehouse = resp.json()
    print(f"✓ Created lakehouse: {lakehouse_name}")
    return lakehouse["id"]


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Fabric Customer360 setup automation")
    parser.add_argument("--workspace_name", required=True)
    parser.add_argument("--lakehouse_name", required=True)
    parser.add_argument("--csv_path", required=True)
    parser.add_argument("--table_name", required=True)
    parser.add_argument("--dataagent_name", required=True)

    args = parser.parse_args(argv)

    print("=" * 60)
    print("🚀 Fabric Customer360 Setup Starting")
    print("=" * 60)

    try:
        print("🔐 Authenticating with Fabric...")
        token = get_fabric_token()
        print("✓ Authentication successful")

        workspace_id = find_or_create_workspace(args.workspace_name, token)
        lakehouse_id = ensure_lakehouse(workspace_id, args.lakehouse_name, token)

        print()
        print("=" * 60)
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
