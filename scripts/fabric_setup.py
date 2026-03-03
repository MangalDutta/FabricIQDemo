#!/usr/bin/env python3
import argparse
import json
import sys
from typing import Any, Dict, List

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


def list_capacities(token: str) -> List[Dict[str, Any]]:
    """
    Uses GET /v1/capacities to list capacities the principal can access.[web:259]
    """
    print("🔎 Discovering Fabric capacities available to this identity...")
    resp = fabric_request("GET", "/capacities", token)
    data = resp.json()
    caps = data.get("value", []) or []

    if not caps:
        raise RuntimeError(
            "No Fabric capacities visible to this identity. "
            "You need at least one active Fabric or Premium capacity."
        )

    print("📋 Capacities found:")
    for c in caps:
        print(
            f"  - {c.get('displayName')} "
            f"(sku={c.get('sku')}, region={c.get('region')}, state={c.get('state')})"
        )
    return caps


def choose_capacity(
    token: str, desired_display_name: str | None = None
) -> Dict[str, Any]:
    caps = list_capacities(token)

    # If user specified a capacity name, honor that first.
    if desired_display_name:
        print(f"\n🎯 Looking for requested capacity: {desired_display_name}")
        for c in caps:
            if c.get("displayName") == desired_display_name:
                if c.get("state") != "Active":
                    raise RuntimeError(
                        f"Capacity '{desired_display_name}' is not Active "
                        f"(state={c.get('state')})."
                    )
                print(
                    f"✓ Using capacity '{desired_display_name}' "
                    f"(sku={c.get('sku')}, region={c.get('region')})"
                )
                return c
        raise RuntimeError(
            f"Requested capacity '{desired_display_name}' not found. "
            "Check the name in Fabric Admin portal → Capacity settings."
        )

    # Otherwise, pick the first active, Fabric‑capable capacity.
    print("\n🎯 No capacity name provided. Selecting an active Fabric‑capable capacity...")

    def is_fabric_capable(cap: Dict[str, Any]) -> bool:
        sku = (cap.get("sku") or "").upper()
        state = cap.get("state")
        # F‑SKUs = Fabric capacity, P‑SKUs = Premium per capacity that supports Fabric items.[web:259][web:262]
        return state == "Active" and (sku.startswith("F") or sku.startswith("P"))

    fabric_caps = [c for c in caps if is_fabric_cap(c)]

    if not fabric_caps:
        raise RuntimeError(
            "No active Fabric‑capable capacity found. "
            "You need an F‑SKU or Premium P‑SKU capacity with Fabric enabled."
        )

    chosen = fabric_caps[0]
    print(
        f"✓ Auto‑selected capacity '{chosen.get('displayName')}' "
        f"(sku={chosen.get('sku')}, region={chosen.get('region')})"
    )
    return chosen


def find_workspace_by_name(workspace_name: str, token: str) -> str:
    print(f"\n🔍 Looking for workspace: {workspace_name}")
    resp = fabric_request("GET", "/workspaces", token)
    data = resp.json()

    for ws in data.get("value", []):
        if ws.get("displayName") == workspace_name:
            ws_id = ws["id"]
            print(f"✓ Found existing workspace: {workspace_name} (ID: {ws_id})")
            # Optional: show capacity binding if present in payload.
            if "capacityId" in ws:
                print(f"   Workspace capacityId: {ws['capacityId']}")
            return ws_id

    raise RuntimeError(
        f"Workspace '{workspace_name}' not found. "
        "Create it in Fabric portal and bind it to a Fabric capacity."
    )


def find_lakehouse_by_name(workspace_id: str, lakehouse_name: str, token: str) -> str:
    print(f"\n🏗️  Checking lakehouse: {lakehouse_name}")
    resp = fabric_request(
        "GET", f"/workspaces/{workspace_id}/items?type=Lakehouse", token
    )
    items = resp.json().get("value", []) or []

    for item in items:
        if item.get("displayName") == lakehouse_name:
            lh_id = item["id"]
            print(f"✓ Found existing lakehouse: {lakehouse_name} (ID: {lh_id})")
            return lh_id

    raise RuntimeError(
        f"Lakehouse '{lakehouse_name}' not found in workspace. "
        "Create it manually in Fabric UI (same workspace & capacity) "
        "or add creation logic here once capacity is confirmed."
    )


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description="Fabric Customer360 setup (use existing capacity, workspace & lakehouse)"
    )
    parser.add_argument("--workspace_name", required=True)
    parser.add_argument("--lakehouse_name", required=True)
    parser.add_argument("--csv_path", required=True)
    parser.add_argument("--table_name", required=True)
    parser.add_argument("--dataagent_name", required=True)
    parser.add_argument(
        "--capacity_display_name",
        required=False,
        help=(
            "Optional Fabric capacity displayName to use. "
            "If omitted, the script auto‑selects an active Fabric‑capable capacity."
        ),
    )

    args = parser.parse_args(argv)

    print("=" * 60)
    print("🚀 Fabric Customer360 Setup (Existing Capacity & Workspace) Starting")
    print("=" * 60)

    try:
        print("🔐 Authenticating with Fabric...")
        token = get_fabric_token()
        print("✓ Authentication successful")

        # 1) Ensure we have a Fabric‑capable capacity and pick one.
        capacity = choose_capacity(token, args.capacity_display_name)

        # 2) Use existing workspace & lakehouse (no creation).
        workspace_id = find_workspace_by_name(args.workspace_name, token)
        lakehouse_id = find_lakehouse_by_name(workspace_id, args.lakehouse_name, token)

        print("\n" + "=" * 60)
        print("✅ Fabric Customer360 Setup Complete (Existing Resources Used)!")
        print("=" * 60)
        result = {
            "capacity_id": capacity.get("id"),
            "capacity_display_name": capacity.get("displayName"),
            "capacity_sku": capacity.get("sku"),
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
