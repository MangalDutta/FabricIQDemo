from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import base64
import json
import logging
import os
from urllib.parse import urlparse, parse_qs
from typing import Any, Dict

import requests as _requests
from azure.identity import DefaultAzureCredential

from fabric_client import AgentNotReadyError, FabricClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("customer360-backend")

app = FastAPI(
    title="Customer360 Conversational Analytics Backend",
    description="Backend API for AI-powered customer analytics",
    version="1.0.0"
)

allowed_origins = os.environ.get("CORS_ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if "*" in allowed_origins else allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

try:
    fabric_client = FabricClient()
    logger.info("✓ Fabric Data Agent client initialized")
except Exception as ex:
    logger.error(f"Failed to initialize Fabric client: {ex}")
    fabric_client = None

# Shared credential for Power BI token endpoint (reuses the same Managed Identity)
try:
    _pbi_credential = DefaultAzureCredential()
except Exception as ex:
    logger.warning(f"DefaultAzureCredential init for Power BI failed: {ex}")
    _pbi_credential = None

@app.get("/")
async def root() -> Dict[str, str]:
    return {
        "service": "Customer360 Conversational Analytics",
        "status": "running",
        "version": "1.0.0"
    }

@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "healthy"}

@app.get("/api/config")
async def config() -> Dict[str, str]:
    """
    Returns runtime configuration for the frontend.
    Exposes POWERBI_REPORT_URL as a backend App Service setting so the Power BI
    embed URL can be updated without rebuilding the frontend Docker image.
    """
    return {
        "powerbi_report_url": os.environ.get("POWERBI_REPORT_URL", ""),
    }


@app.get("/api/powerbi-token")
async def get_powerbi_token() -> Dict[str, Any]:
    """
    Generate a Power BI embed token using the backend's Managed Identity.

    This allows the frontend to embed the report without requiring end-users
    to be signed into Power BI in their browser — fixing the
    "app.powerbi.com refused to connect" iframe auth-redirect failure.

    Requirements:
      1. Backend Managed Identity must be a Fabric workspace Contributor
         (handled automatically by fabric_setup.py during deployment).
      2. Azure AD tenant must allow service principals to use Power BI APIs:
         Power BI Admin Portal → Tenant Settings →
         "Allow service principals to use Power BI APIs" → Enabled.
    """
    report_url = os.environ.get("POWERBI_REPORT_URL", "")
    if not report_url:
        raise HTTPException(
            status_code=503,
            detail="POWERBI_REPORT_URL is not configured on the backend App Service.",
        )

    # Parse reportId and groupId from the embed URL
    parsed = urlparse(report_url)
    params = parse_qs(parsed.query)
    report_id = params.get("reportId", [""])[0]
    group_id = params.get("groupId", [""])[0]

    if not report_id or not group_id:
        raise HTTPException(
            status_code=503,
            detail=f"Could not parse reportId/groupId from POWERBI_REPORT_URL: {report_url[:120]}",
        )

    if not _pbi_credential:
        raise HTTPException(
            status_code=503,
            detail="Managed Identity credential not available. Ensure the App Service has a system-assigned identity.",
        )

    # Step 1: Acquire AAD access token for the Power BI API scope
    try:
        token_obj = _pbi_credential.get_token(
            "https://analysis.windows.net/powerbi/api/.default"
        )
        access_token = token_obj.token
    except Exception as ex:
        logger.error("Power BI AAD token acquisition failed: %s", ex)
        raise HTTPException(
            status_code=503,
            detail=(
                f"Could not acquire Power BI access token via Managed Identity: {str(ex)[:250]} "
                "— Ensure the backend App Service has a system-assigned managed identity enabled."
            ),
        )

    # Step 2: Call Power BI REST API to generate a short-lived embed token
    generate_url = (
        f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}"
        f"/reports/{report_id}/GenerateToken"
    )
    try:
        resp = _requests.post(
            generate_url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json={"accessLevel": "View"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except _requests.exceptions.HTTPError as ex:
        http_status = ex.response.status_code if ex.response else 503
        body = ex.response.text[:300] if ex.response else str(ex)
        logger.error("GenerateToken HTTP %s: %s", http_status, body)
        hints = {
            401: " Ensure the backend Managed Identity is added to the Power BI workspace as Contributor.",
            403: (
                " Enable 'Allow service principals to use Power BI APIs' in "
                "Power BI Admin Portal → Tenant Settings."
            ),
        }
        raise HTTPException(
            status_code=503,
            detail=f"Power BI embed token generation failed (HTTP {http_status}).{hints.get(http_status, '')}",
        )
    except Exception as ex:
        logger.error("GenerateToken request failed: %s", ex)
        raise HTTPException(
            status_code=503,
            detail=f"Failed to call Power BI GenerateToken API: {str(ex)[:250]}",
        )

    embed_url = (
        f"https://app.powerbi.com/reportEmbed"
        f"?reportId={report_id}&groupId={group_id}"
    )
    return {
        "embed_token": data.get("token", ""),
        "embed_url": embed_url,
        "report_id": report_id,
        "group_id": group_id,
        "expiry": data.get("expiration", ""),
    }


@app.get("/api/status")
async def status() -> Dict[str, Any]:
    """
    Lightweight readiness probe for the Fabric Data Agent.

    The frontend calls this on page load to show a proactive status banner
    instead of letting the user discover failures only after sending a message.

    Returns:
        {
            "ready": bool,
            "message": str,           # human-readable status summary
            "agent_name": str | None,
            "troubleshooting": [str]   # empty when ready
        }
    """
    if not fabric_client:
        return {
            "ready": False,
            "message": "Backend is not connected to the Fabric Data Agent. Check server configuration.",
            "agent_name": None,
            "troubleshooting": [
                "Ensure FABRIC_WORKSPACE_ID and FABRIC_DATAAGENT_ID environment variables are set on the backend App Service.",
                "Visit the /api/debug endpoint on the backend for full diagnostics.",
            ],
        }

    workspace_id = fabric_client.workspace_id
    agent_name = fabric_client.dataagent_name

    # Quick GET to see if the agent is reachable and in a queryable state
    try:
        token = fabric_client._get_token()
        agent_url = (
            f"https://api.fabric.microsoft.com/v1"
            f"/workspaces/{workspace_id}"
            f"/dataAgents/{fabric_client.dataagent_id}"
        )
        resp = _requests.get(
            agent_url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            timeout=15,
        )

        if resp.ok:
            data = resp.json()
            raw_state = (
                data.get("state")
                or data.get("status")
                or data.get("publishState")
                or "unknown"
            )
            if str(raw_state).lower() in ("draft", "unpublished", "inactive"):
                return {
                    "ready": False,
                    "message": f"The AI Agent '{agent_name}' is in '{raw_state}' state and must be published before it can answer queries.",
                    "agent_name": agent_name,
                    "troubleshooting": [
                        f"Open the Fabric portal and publish the agent: https://app.fabric.microsoft.com/groups/{workspace_id}",
                        "Ensure the App Service Managed Identity is a workspace Contributor (Fabric portal → Workspace → Manage access → Add as Contributor).",
                        "Visit the /api/debug endpoint on the backend for full diagnostics.",
                    ],
                }
            return {
                "ready": True,
                "message": f"AI Agent '{agent_name}' is ready.",
                "agent_name": agent_name,
                "troubleshooting": [],
            }
        else:
            return {
                "ready": False,
                "message": f"Cannot reach the AI Agent (HTTP {resp.status_code}). The agent may not be published or the backend identity lacks workspace access.",
                "agent_name": agent_name,
                "troubleshooting": [
                    f"Open the Fabric portal and publish the agent: https://app.fabric.microsoft.com/groups/{workspace_id}",
                    "Ensure the App Service Managed Identity is a workspace Contributor (Fabric portal → Workspace → Manage access → Add as Contributor).",
                    "Visit the /api/debug endpoint on the backend for full diagnostics.",
                ],
            }
    except Exception as exc:
        logger.warning("Status check failed: %s", exc)
        return {
            "ready": False,
            "message": f"Could not verify AI Agent status: {str(exc)[:200]}",
            "agent_name": agent_name,
            "troubleshooting": [
                "The backend could not connect to the Fabric API. Check network connectivity and credentials.",
                "Visit the /api/debug endpoint on the backend for full diagnostics.",
            ],
        }

@app.get("/api/debug")
async def debug() -> Dict[str, Any]:
    """
    Diagnostic endpoint — shows which Managed Identity the backend is using,
    current Fabric agent state, and step-by-step fix instructions.

    Browse to /api/debug on the backend App Service URL to see:
      • managed_identity.object_id  — the OID that must be added to the Fabric workspace
      • agent_check.agent_state     — Published (queryable) or Draft (will 404)
    """
    result: Dict[str, Any] = {
        "env": {
            "FABRIC_WORKSPACE_ID": os.environ.get("FABRIC_WORKSPACE_ID", "(not set)"),
            "FABRIC_DATAAGENT_ID": os.environ.get("FABRIC_DATAAGENT_ID", "(not set)"),
            "FABRIC_DATAAGENT_NAME": os.environ.get("FABRIC_DATAAGENT_NAME", "(not set)"),
        },
        "managed_identity": {},
        "agent_check": {},
        "instructions": {},
    }

    if not fabric_client:
        result["error"] = "Fabric client not initialized — check FABRIC_WORKSPACE_ID / FABRIC_DATAAGENT_ID env vars."
        return result

    # ── Decode JWT to find the Managed Identity Object ID ────────────────────
    try:
        token = fabric_client._get_token()
        parts = token.split(".")
        if len(parts) >= 2:
            padding = "=" * (4 - len(parts[1]) % 4)
            payload = json.loads(base64.urlsafe_b64decode(parts[1] + padding))
            result["managed_identity"] = {
                "object_id": payload.get("oid", "(not found)"),
                "app_id": payload.get("appid", payload.get("azp", "(not found)")),
                "identity_type": payload.get("idtyp", "(not found)"),
                "note": (
                    "This object_id is the Backend App Service Managed Identity. "
                    "It must be added to the Fabric workspace as Contributor or higher."
                ),
            }
    except Exception as exc:
        result["managed_identity"] = {"error": str(exc)}

    # ── Test GET /dataAgents/{id} ─────────────────────────────────────────────
    try:
        agent_url = (
            f"https://api.fabric.microsoft.com/v1"
            f"/workspaces/{fabric_client.workspace_id}"
            f"/dataAgents/{fabric_client.dataagent_id}"
        )
        agent_resp = _requests.get(agent_url, headers=fabric_client._headers(), timeout=20)
        if agent_resp.ok:
            agent_data = agent_resp.json()
            raw_state = (
                agent_data.get("state")
                or agent_data.get("status")
                or agent_data.get("publishState")
                or "unknown"
            )
            result["agent_check"] = {
                "reachable": True,
                "http_status": agent_resp.status_code,
                "agent_state": raw_state,
                "queryable": str(raw_state).lower() not in ("draft", "unpublished", "inactive"),
            }
        else:
            result["agent_check"] = {
                "reachable": False,
                "http_status": agent_resp.status_code,
                "error": agent_resp.text[:400],
                "likely_cause": (
                    "HTTP 404 means the Backend Managed Identity is NOT a Fabric workspace member. "
                    "Add the object_id shown in managed_identity to the workspace."
                    if agent_resp.status_code == 404
                    else f"HTTP {agent_resp.status_code} error."
                ),
            }
    except Exception as exc:
        result["agent_check"] = {"error": str(exc)}

    # ── Actionable instructions ───────────────────────────────────────────────
    mi_oid = result.get("managed_identity", {}).get("object_id", "<see managed_identity.object_id above>")
    ws_id = fabric_client.workspace_id
    agent_state = result.get("agent_check", {}).get("agent_state", "unknown")
    result["instructions"] = {
        "step1_add_mi_to_workspace": (
            f"In Fabric portal -> Workspace -> Manage access -> Add member -> "
            f"paste Object ID: {mi_oid} -> role: Contributor"
        ),
        "step2_check_agent_state": (
            "Agent state is already Published — no action needed."
            if result.get("agent_check", {}).get("queryable")
            else (
                f"Agent is in '{agent_state}' state. Open the Fabric portal, open the agent, "
                f"click 'Publish'. URL: https://app.fabric.microsoft.com/groups/{ws_id}"
            )
        ),
        "fabric_workspace_url": f"https://app.fabric.microsoft.com/groups/{ws_id}",
    }

    return result


@app.post("/api/publish-agent")
async def publish_agent() -> Dict[str, Any]:
    """
    Attempt to programmatically publish the Fabric Data Agent using the
    backend's Managed Identity (the same identity that created the agent).

    The Fabric publish API is in preview and may return 404 on some tenants.
    If every endpoint fails this returns a clear manual-publish URL so the
    user can do it in one click from the Fabric portal.

    Returns:
        {
            "published":  bool,
            "method":     str,   # which API endpoint succeeded, or "manual_required"
            "agent_state": str,  # state after attempt
            "portal_url": str,   # direct link to open the agent in Fabric
            "message":    str
        }
    """
    if not fabric_client:
        raise HTTPException(status_code=503, detail="Fabric client not configured.")

    workspace_id = fabric_client.workspace_id
    agent_id = fabric_client.dataagent_id
    agent_name = fabric_client.dataagent_name
    portal_url = f"https://app.fabric.microsoft.com/groups/{workspace_id}"

    try:
        token = fabric_client._get_token()
    except Exception as ex:
        raise HTTPException(
            status_code=503,
            detail=f"Could not acquire Fabric token: {str(ex)[:200]}",
        )

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Try every known publish endpoint variation (Fabric API is in preview and
    # the correct endpoint varies by API version / tenant).
    publish_endpoints = [
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataAgents/{agent_id}/publish",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataAgents/{agent_id}/activate",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{agent_id}/publish",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{agent_id}/deploy",
    ]

    published_via = None
    for url in publish_endpoints:
        try:
            resp = _requests.post(url, headers=headers, json={}, timeout=60)
            if resp.status_code in (200, 201, 204):
                published_via = url.split("/")[-1]
                break
            if resp.status_code == 202:
                published_via = url.split("/")[-1] + " (async)"
                break
            if resp.status_code == 409:
                # Already published
                published_via = "already_published"
                break
            # 404 = endpoint not available in this API version — try next
        except Exception as exc:
            logger.warning("Publish attempt via %s failed: %s", url, exc)

    # Verify the actual agent state after the attempt
    agent_state = "unknown"
    try:
        check = _requests.get(
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataAgents/{agent_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=20,
        )
        if check.ok:
            d = check.json()
            agent_state = (
                d.get("state") or d.get("status") or d.get("publishState") or "unknown"
            )
        else:
            logger.warning("Post-publish state check returned HTTP %d", check.status_code)
    except Exception as exc:
        logger.warning("Post-publish state check failed: %s", exc)

    state_lower = str(agent_state).lower()
    is_queryable = state_lower not in ("draft", "unpublished", "inactive", "unknown")

    if is_queryable:
        return {
            "published": True,
            "method": published_via or "already_active",
            "agent_state": agent_state,
            "portal_url": portal_url,
            "message": f"Agent '{agent_name}' is now in '{agent_state}' state and ready to answer queries.",
        }

    # API publish didn't work — manual action required
    return {
        "published": False,
        "method": "manual_required",
        "agent_state": agent_state,
        "portal_url": portal_url,
        "message": (
            f"Could not publish the agent via API (Fabric publish endpoint is in preview "
            f"and not yet available on this tenant). "
            f"Please publish manually: open {portal_url}, find '{agent_name}', "
            f"and click 'Publish' in the top ribbon."
        ),
    }


@app.post("/api/reset")
async def reset_conversation(request: Request) -> Dict[str, str]:
    """Reset the conversation history for a user so the next message starts fresh."""
    if not fabric_client:
        raise HTTPException(status_code=503, detail="Fabric client not configured")

    try:
        body = await request.json()
        user_id = body.get("userId", "anonymous")
        fabric_client.reset_conversation(user_id=user_id)
        logger.info("Conversation reset for user_id=%s", user_id[:64])
        return {"status": "ok", "message": "Conversation reset"}
    except HTTPException:
        raise
    except Exception as ex:
        logger.exception(f"Reset error: {ex}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(ex)}")


@app.post("/api/chat")
async def chat(request: Request) -> Dict[str, Any]:
    if not fabric_client:
        raise HTTPException(status_code=503, detail="Fabric client not configured")

    try:
        body = await request.json()
        message = body.get("message")
        user_id = body.get("userId", "anonymous")

        if not message:
            raise HTTPException(status_code=400, detail="'message' field is required")

        logger.info(f"Chat request from {user_id}: {message[:100]}")
        result = fabric_client.chat(user_id=user_id, message=message)
        logger.info(f"Chat response received for {user_id}")

        return {
            "answer": result.get("answer", ""),
            "timestamp": result.get("timestamp"),
            "metadata": result.get("metadata", {})
        }
    except HTTPException:
        raise
    except AgentNotReadyError as ex:
        logger.warning(f"Agent not ready: {ex}")
        raise HTTPException(
            status_code=503,
            detail={
                "error": "agent_not_ready",
                "message": str(ex),
                "troubleshooting": [
                    "Open the Fabric portal and publish the agent: "
                    f"https://app.fabric.microsoft.com/groups/{ex.workspace_id}",
                    "Ensure the App Service Managed Identity is a workspace Contributor "
                    "(Fabric portal → Workspace → Manage access → Add as Contributor)",
                    "Visit the /api/debug endpoint on the backend for full diagnostics",
                ],
            },
        )
    except Exception as ex:
        logger.exception(f"Chat error: {ex}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(ex)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
