#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path
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

    # Check if workspace exists
    for workspace in data.get("value", []):
        if workspace.get("displayName") == workspace_name:
            ws_id = workspace["id"]
            print(f"✓ Found existing workspace: {workspace_name} (ID: {ws_id})")
            return ws_id

    # Create new workspace
    print(f"📦 Workspace not found. Creating new workspace: {workspace_name}")
    payload = {"displayName": workspace_name}
    resp = fabric_request("POST", "/workspaces", token, json=payload)
    workspace = resp.json()
    ws_id = workspace["id"]
    print(f"✓ Created workspace: {workspace_name} (ID: {ws_id})")
    return ws_id

def ensure_lakehouse(workspace_id: str, lakehouse_name: str, token: str) -> str:
    print(f"🏗️  Checking lakehouse: {lakehouse_nam
