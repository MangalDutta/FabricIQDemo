#!/usr/bin/env python3
"""
test_fabric_agent.py
--------------------
Local test script for the Fabric Data Agent integration.

Usage (from repo root):
  python scripts/test_fabric_agent.py \
      --workspace_id <GUID> \
      --agent_id     <GUID> \
      --question     "How many customers do we have?"

Authentication:
  Uses DefaultAzureCredential (az login / env vars / MI).
  Run `az login` first if testing locally.

What this tests:
  1. Token acquisition from DefaultAzureCredential
  2. Agent metadata fetch (GET /dataAgents/{id})
  3. Native query endpoint  (POST /dataAgents/{id}/query)
  4. OpenAI-compat endpoint (POST /aiskills/{id}/aiassistant/openai) [fallback]
  5. Auto-discovery by name if the supplied agent_id returns 404

Exit code: 0 = at least one query path succeeded, 1 = all failed.
"""

import argparse
import json
import sys
import time

import requests
from azure.identity import DefaultAzureCredential

FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
FABRIC_BASE_URL = "https://api.fabric.microsoft.com/v1"


def get_token() -> str:
    credential = DefaultAzureCredential()
    return credential.get_token(FABRIC_SCOPE).token


def hdr(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def check_agent_metadata(ws_id: str, agent_id: str, token: str) -> bool:
    """GET /dataAgents/{id} — confirms the agent exists and is accessible."""
    url = f"{FABRIC_BASE_URL}/workspaces/{ws_id}/dataAgents/{agent_id}"
    print(f"\n[1] GET agent metadata: {url}")
    resp = requests.get(url, headers=hdr(token), timeout=30)
    print(f"    Status: {resp.status_code}")
    if resp.ok:
        data = resp.json()
        print(f"    Name : {data.get('displayName')}")
        print(f"    Type : {data.get('type')}")
        print(f"    State: {data.get('state')}")
        return True
    print(f"    Error: {resp.text[:300]}")
    if resp.status_code == 404:
        print(
            "\n    *** 404 EntityNotFound ***\n"
            "    This usually means the App Service Managed Identity is NOT a member\n"
            "    of the Fabric workspace.  Fix:\n"
            "    1. Re-run the deploy workflow (it adds the MI via --app_service_principal_id)\n"
            "    2. Or manually: Fabric portal -> Workspace -> Manage access -> Add MI as Contributor"
        )
    return False


def list_agents(ws_id: str, token: str) -> list:
    """List all Data Agents in the workspace for discovery."""
    url = f"{FABRIC_BASE_URL}/workspaces/{ws_id}/dataAgents"
    print(f"\n[2] GET agent list: {url}")
    resp = requests.get(url, headers=hdr(token), timeout=30)
    print(f"    Status: {resp.status_code}")
    if resp.ok:
        agents = resp.json().get("value", [])
        print(f"    Found {len(agents)} agent(s):")
        for a in agents:
            print(f"      - {a.get('displayName')} (ID: {a.get('id')})")
        return agents
    print(f"    Error: {resp.text[:300]}")
    return []


def query_native(ws_id: str, agent_id: str, question: str, token: str) -> bool:
    """POST /dataAgents/{id}/query — native Fabric Data Agent query endpoint."""
    url = f"{FABRIC_BASE_URL}/workspaces/{ws_id}/dataAgents/{agent_id}/query"
    payload = {"userMessage": question}
    print(f"\n[3] POST native query: {url}")
    print(f"    Payload: {json.dumps(payload)}")
    t0 = time.time()
    resp = requests.post(url, headers=hdr(token), json=payload, timeout=120)
    elapsed = time.time() - t0
    print(f"    Status : {resp.status_code}  ({elapsed:.1f}s)")
    if resp.ok:
        data = resp.json()
        print(f"    Response keys: {list(data.keys())}")
        answer = data.get("response") or data.get("answer") or str(data)
        print(f"\n    *** ANSWER ***\n    {answer}\n")
        conv_id = data.get("conversationId")
        if conv_id:
            print(f"    conversationId: {conv_id}")
        return True
    print(f"    Error: {resp.text[:400]}")
    return False


def query_openai_compat(ws_id: str, agent_id: str, question: str, token: str) -> bool:
    """POST /aiskills/{id}/aiassistant/openai — OpenAI-compatible fallback."""
    url = f"{FABRIC_BASE_URL}/workspaces/{ws_id}/aiskills/{agent_id}/aiassistant/openai"
    payload = {"messages": [{"role": "user", "content": question}]}
    print(f"\n[4] POST OpenAI-compat fallback: {url}")
    print(f"    Payload: {json.dumps(payload)}")
    t0 = time.time()
    resp = requests.post(url, headers=hdr(token), json=payload, timeout=120)
    elapsed = time.time() - t0
    print(f"    Status : {resp.status_code}  ({elapsed:.1f}s)")
    if resp.ok:
        data = resp.json()
        print(f"    Response keys: {list(data.keys())}")
        choices = data.get("choices", [])
        if choices:
            answer = choices[0].get("message", {}).get("content", "")
            print(f"\n    *** ANSWER (OpenAI format) ***\n    {answer}\n")
        else:
            print(f"    Raw response: {json.dumps(data, indent=2)[:500]}")
        return True
    if resp.status_code == 404:
        print("    404 — this endpoint does not exist for this agent (expected for newer agents)")
    else:
        print(f"    Error: {resp.text[:400]}")
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Test Fabric Data Agent connectivity")
    parser.add_argument("--workspace_id", required=True)
    parser.add_argument("--agent_id", required=True)
    parser.add_argument("--question", default="How many customers do we have by state?")
    args = parser.parse_args()

    print("=" * 60)
    print("Fabric Data Agent – Connectivity Test")
    print("=" * 60)
    print(f"Workspace : {args.workspace_id}")
    print(f"Agent ID  : {args.agent_id}")
    print(f"Question  : {args.question}")

    print("\n[0] Acquiring Fabric token via DefaultAzureCredential...")
    try:
        token = get_token()
        print("    ✓ Token acquired")
    except Exception as exc:
        print(f"    ✗ Token acquisition failed: {exc}")
        print("    Run 'az login' first or set AZURE_CLIENT_ID/AZURE_CLIENT_SECRET/AZURE_TENANT_ID")
        sys.exit(1)

    # Step 1: Check agent metadata
    agent_ok = check_agent_metadata(args.workspace_id, args.agent_id, token)

    # Step 2: List agents (for diagnostics)
    if not agent_ok:
        agents = list_agents(args.workspace_id, token)

    # Step 3: Try native query
    native_ok = query_native(args.workspace_id, args.agent_id, args.question, token)

    # Step 4: Try OpenAI-compat (always try even if native worked)
    openai_ok = query_openai_compat(args.workspace_id, args.agent_id, args.question, token)

    print("\n" + "=" * 60)
    print("Test Summary:")
    print(f"  Agent metadata GET : {'✓ OK' if agent_ok else '✗ FAILED'}")
    print(f"  Native /query      : {'✓ OK' if native_ok else '✗ FAILED'}")
    print(f"  OpenAI-compat      : {'✓ OK' if openai_ok else '✗ FAILED / not available'}")
    print("=" * 60)

    if not native_ok and not openai_ok:
        print("\n✗ Both query paths failed. Check the error messages above.")
        print("Most common fix: ensure the caller identity is a Fabric workspace Contributor.")
        sys.exit(1)
    else:
        print("\n✓ At least one query path succeeded!")
        sys.exit(0)


if __name__ == "__main__":
    main()
