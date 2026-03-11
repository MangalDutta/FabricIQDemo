from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import base64
from datetime import datetime, timezone
import json
import logging
import os
from urllib.parse import urlparse, parse_qs
from typing import Any, Dict

import requests as _requests
from azure.identity import DefaultAzureCredential

from fabric_client import AgentNotReadyError, FabricClient
from fabric_agent_client import FabricAgentClient

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

# OpenAI-Assistants-API client for advanced interactions (threads, runs, SQL
# extraction, draft-vs-production comparison).  Uses the same Managed Identity
# and workspace/agent env vars as fabric_client.
try:
    agent_client = FabricAgentClient()
    logger.info("✓ Fabric Agent (Assistants API) client initialized")
except Exception as ex:
    logger.warning(f"FabricAgentClient init failed (non-fatal): {ex}")
    agent_client = None

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
    Generate a Power BI embed token following the Microsoft AppOwnsData sample pattern.
    See: https://github.com/microsoft/PowerBI-Developer-Samples

    Flow (mirrors pbiembedservice.py from the MS Python sample):
      1. Read report_id + group_id from discrete env vars set by the workflow
         (POWERBI_REPORT_ID / POWERBI_GROUP_ID), with fallback to parsing
         POWERBI_REPORT_URL for backwards compatibility.
      2. Acquire AAD access token via Managed Identity (App Owns Data pattern).
      3. GET /v1.0/myorg/groups/{group_id}/reports/{report_id}
         → fetch authoritative embedUrl and datasetId from the Power BI API.
      4. POST https://api.powerbi.com/v1.0/myorg/GenerateToken
         with {datasets, reports, targetWorkspaces} — the multi-resource
         token endpoint recommended by Microsoft.
      5. Return {tokenId, accessToken, tokenExpiry, reportConfig} matching
         the EmbedConfig model in the MS sample so the frontend can use
         the standard powerbi-client-react SDK directly.

    Requirements:
      - Backend Managed Identity added to the Power BI workspace as Contributor.
      - Azure AD tenant: "Allow service principals to use Power BI APIs" enabled
        in Power BI Admin Portal → Tenant Settings.
    """
    # ── Resolve report_id and group_id ────────────────────────────────────────
    report_id = os.environ.get("POWERBI_REPORT_ID", "")
    group_id = os.environ.get("POWERBI_GROUP_ID", "")

    # Fallback: parse from POWERBI_REPORT_URL (backwards compat)
    if not report_id or not group_id:
        report_url = os.environ.get("POWERBI_REPORT_URL", "")
        if report_url:
            parsed = urlparse(report_url)
            params = parse_qs(parsed.query)
            report_id = report_id or params.get("reportId", [""])[0]
            group_id = group_id or params.get("groupId", [""])[0]

    if not report_id or not group_id:
        raise HTTPException(
            status_code=503,
            detail=(
                "Power BI report not configured. Set POWERBI_REPORT_ID and "
                "POWERBI_GROUP_ID (or POWERBI_REPORT_URL) on the App Service."
            ),
        )

    if not _pbi_credential:
        raise HTTPException(
            status_code=503,
            detail="Managed Identity credential not available. Ensure the App Service has a system-assigned identity.",
        )

    # ── Step 1: Acquire AAD access token (App Owns Data) ──────────────────────
    try:
        token_obj = _pbi_credential.get_token(
            "https://analysis.windows.net/powerbi/api/.default"
        )
        aad_token = token_obj.token
    except Exception as ex:
        logger.error("Power BI AAD token acquisition failed: %s", ex)
        raise HTTPException(
            status_code=503,
            detail=(
                f"Could not acquire Power BI access token via Managed Identity: {str(ex)[:250]} "
                "— Ensure the App Service has a system-assigned managed identity enabled."
            ),
        )

    request_headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {aad_token}",
    }

    # ── Step 2: GET report details — fetch embedUrl and datasetId ─────────────
    # Mirrors: pbiembedservice.get_embed_params_for_single_report()
    report_api_url = (
        f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/reports/{report_id}"
    )
    try:
        report_resp = _requests.get(report_api_url, headers=request_headers, timeout=15)
        report_resp.raise_for_status()
        report_data = report_resp.json()
    except _requests.exceptions.HTTPError as ex:
        http_status = ex.response.status_code if ex.response else 503
        body = ex.response.text[:300] if ex.response else str(ex)
        logger.error("GET report HTTP %s: %s", http_status, body)
        hints = {
            401: " Ensure the Managed Identity is added to the Power BI workspace as Contributor.",
            403: " Enable 'Allow service principals to use Power BI APIs' in Power BI Admin Portal → Tenant Settings.",
            404: f" Report {report_id} not found in workspace {group_id}. Check POWERBI_REPORT_ID and POWERBI_GROUP_ID.",
        }
        raise HTTPException(
            status_code=503,
            detail=f"Error retrieving report details (HTTP {http_status}).{hints.get(http_status, '')}",
        )
    except Exception as ex:
        logger.error("GET report request failed: %s", ex)
        raise HTTPException(
            status_code=503,
            detail=f"Failed to retrieve Power BI report details: {str(ex)[:250]}",
        )

    embed_url = report_data.get("embedUrl", "")
    report_name = report_data.get("name", "")
    dataset_id = report_data.get("datasetId", "")

    # ── Step 3: POST GenerateToken — multi-resource endpoint (MS recommended) ──
    # Mirrors: pbiembedservice.get_embed_token_for_single_report_single_workspace()
    # See: https://aka.ms/MultiResourceEmbedToken
    generate_token_request = {
        "datasets": [{"id": dataset_id}],
        "reports": [{"id": report_id}],
        "targetWorkspaces": [{"id": group_id}],
    }
    try:
        token_resp = _requests.post(
            "https://api.powerbi.com/v1.0/myorg/GenerateToken",
            headers=request_headers,
            json=generate_token_request,
            timeout=15,
        )
        token_resp.raise_for_status()
        token_data = token_resp.json()
    except _requests.exceptions.HTTPError as ex:
        http_status = ex.response.status_code if ex.response else 503
        body = ex.response.text[:300] if ex.response else str(ex)
        logger.error("GenerateToken HTTP %s: %s", http_status, body)
        hints = {
            401: " Ensure the Managed Identity is added to the Power BI workspace as Contributor.",
            403: " Enable 'Allow service principals to use Power BI APIs' in Power BI Admin Portal → Tenant Settings.",
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

    # ── Return EmbedConfig matching the MS sample model ───────────────────────
    # Mirrors: EmbedConfig(token_id, access_token, token_expiry, report_config)
    return {
        "tokenId": token_data.get("tokenId", ""),
        "accessToken": token_data.get("token", ""),
        "tokenExpiry": token_data.get("expiration", ""),
        "reportConfig": [
            {
                "reportId": report_id,
                "reportName": report_name,
                "embedUrl": embed_url,
            }
        ],
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

    # ── Step 1: GET agent metadata to check state field ──────────────────────
    # NOTE: The Fabric Data Agent API often omits the state field entirely when
    # the agent is Published.  An absent state field does NOT mean "ready" —
    # we must probe the query endpoint to know for sure.
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

        if not resp.ok:
            return {
                "ready": False,
                "message": (
                    f"Cannot reach the AI Agent (HTTP {resp.status_code}). "
                    "The agent may not be published or the backend identity lacks workspace access."
                ),
                "agent_name": agent_name,
                "troubleshooting": [
                    f"Open the Fabric portal and publish the agent: https://app.fabric.microsoft.com/groups/{workspace_id}",
                    "Ensure the App Service Managed Identity is a workspace Contributor (Fabric portal → Workspace → Manage access → Add as Contributor).",
                    "Visit the /api/debug endpoint on the backend for full diagnostics.",
                ],
            }

        data = resp.json()
        raw_state = (
            data.get("state")
            or data.get("status")
            or data.get("publishState")
            or data.get("lifecycleState")
            or data.get("publishedState")
            or ""
        )
        state_lower = str(raw_state).lower()

        # Explicit Draft/Inactive — fail immediately without probing query endpoint
        if state_lower in ("draft", "unpublished", "inactive"):
            return {
                "ready": False,
                "message": (
                    f"The AI Agent '{agent_name}' is in '{raw_state}' state "
                    "and must be published before it can answer queries."
                ),
                "agent_name": agent_name,
                "troubleshooting": [
                    f"Open the Fabric portal and publish the agent: https://app.fabric.microsoft.com/groups/{workspace_id}",
                    "Ensure the App Service Managed Identity is a workspace Contributor (Fabric portal → Workspace → Manage access → Add as Contributor).",
                    "Visit the /api/debug endpoint on the backend for full diagnostics.",
                ],
            }

        # ── Step 2: Probe endpoints to verify real queryability ────────────────
        # The metadata API often returns 200 with no state field whether the agent
        # is Published OR Draft.  We probe the actual endpoints to know for sure.
        #
        # The application uses two APIs to interact with the agent:
        #   1. REST /query endpoint (legacy / simple query)
        #   2. OpenAI Assistants API at /aiassistant/openai (threads, runs, etc.)
        #
        # The /query endpoint may return 404 even when the Assistants API works
        # perfectly (e.g. for agents that only expose the Assistants interface).
        # We probe both and consider the agent ready if EITHER is reachable.
        query_url = (
            f"https://api.fabric.microsoft.com/v1"
            f"/workspaces/{workspace_id}"
            f"/dataAgents/{fabric_client.dataagent_id}/query"
        )
        # Probe URLs for the Assistants API.  The primary probe is a POST to
        # /assistants with the required api-version parameter — this mirrors
        # the exact call FabricAgentClient makes when creating an assistant
        # (client.beta.assistants.create).  A GET to the same path is kept
        # as a secondary fallback for future API changes.
        assistants_probe_urls = [
            (
                "POST",
                f"https://api.fabric.microsoft.com/v1"
                f"/workspaces/{workspace_id}"
                f"/dataAgents/{fabric_client.dataagent_id}"
                f"/aiassistant/openai/assistants",
            ),
            (
                "GET",
                f"https://api.fabric.microsoft.com/v1"
                f"/workspaces/{workspace_id}"
                f"/dataAgents/{fabric_client.dataagent_id}"
                f"/aiassistant/openai/assistants",
            ),
        ]
        agent_reachable = False
        try:
            probe = _requests.post(
                query_url,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"userMessage": "status check"},
                timeout=12,
            )
            if probe.status_code != 404:
                # Any non-404 response (200, 400, 500) means the endpoint exists
                logger.info(
                    "Status probe: query endpoint returned HTTP %d — agent is reachable.",
                    probe.status_code,
                )
                agent_reachable = True
        except Exception as probe_exc:
            logger.warning("Status query probe failed (non-fatal): %s", probe_exc)

        # ── Fallback: probe the Assistants API endpoint(s) ───────────────────
        # This is the endpoint the application actually uses for chat queries.
        # Try multiple paths because not every agent exposes every sub-path.
        if not agent_reachable:
            for method, assistants_url in assistants_probe_urls:
                try:
                    if method == "POST":
                        assistants_probe = _requests.post(
                            assistants_url,
                            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                            json={"model": "not-used"},
                            params={"api-version": "2024-05-01-preview"},
                            timeout=12,
                        )
                    else:
                        assistants_probe = _requests.get(
                            assistants_url,
                            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                            params={"api-version": "2024-05-01-preview"},
                            timeout=12,
                        )
                    if assistants_probe.status_code != 404:
                        logger.info(
                            "Status probe: Assistants API (%s) returned HTTP %d — agent is reachable.",
                            method,
                            assistants_probe.status_code,
                        )
                        agent_reachable = True
                        break
                    else:
                        logger.debug(
                            "Status probe: %s returned 404 — trying next variant.",
                            method,
                        )
                except Exception as asst_exc:
                    logger.warning("Assistants API probe failed (non-fatal): %s", asst_exc)

        if not agent_reachable:
            logger.warning(
                "Status probe: query and all Assistants API probe URLs returned 404 "
                "— agent is NOT queryable (likely Draft state). workspace=%s agent=%s",
                workspace_id, fabric_client.dataagent_id,
            )
            return {
                "ready": False,
                "message": (
                    f"The AI Agent '{agent_name}' is not queryable yet "
                    "(both query and Assistants API endpoints returned 404, "
                    "typically meaning the agent is in Draft state). "
                    "Please publish the agent in the Fabric portal."
                ),
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
        # The /query endpoint returned 404. Try Assistants API (FabricAgentClient) as fallback.
        # This handles agents that only expose the OpenAI Assistants API interface.
        if agent_client:
            logger.info(
                "fabric_client.chat raised AgentNotReadyError — trying Assistants API fallback. user=%s",
                user_id,
            )
            try:
                answer = agent_client.ask(question=message, timeout=120)
                return {
                    "answer": answer,
                    "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                    "metadata": {
                        "workspace_id": fabric_client.workspace_id,
                        "dataagent_id": fabric_client.dataagent_id,
                        "source": "fabric_data_agent",
                        "endpoint": "assistants-api-fallback",
                    },
                }
            except Exception as ask_ex:
                logger.error("Assistants API fallback also failed: %s", ask_ex)
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

# ─── Advanced Agent endpoints (OpenAI Assistants API) ─────────────────────────
# These endpoints use the FabricAgentClient which provides richer capabilities
# than the simple REST wrapper: persistent threads, run-step introspection,
# SQL query extraction, and draft-vs-production comparison.


@app.post("/api/agent/ask")
async def agent_ask(request: Request) -> Dict[str, Any]:
    """Ask a question via the OpenAI Assistants API, with optional thread persistence.

    Request body::

        {
            "question": str,            # required
            "thread_name": str | null,  # optional – reuse a named thread
            "timeout": int              # optional – seconds (default 120)
        }
    """
    if not agent_client:
        raise HTTPException(
            status_code=503,
            detail="Fabric Agent (Assistants API) client not configured. "
            "Ensure FABRIC_WORKSPACE_ID and FABRIC_DATAAGENT_ID are set.",
        )

    body = await request.json()
    question = body.get("question", "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="'question' field is required")

    thread_name = body.get("thread_name")
    timeout = int(body.get("timeout", 120))

    try:
        answer = agent_client.ask(
            question=question, timeout=timeout, thread_name=thread_name
        )
        return {"answer": answer, "question": question}
    except Exception as ex:
        logger.exception("agent/ask error: %s", ex)
        raise HTTPException(status_code=500, detail=str(ex))


@app.post("/api/agent/run-details")
async def agent_run_details(request: Request) -> Dict[str, Any]:
    """Ask a question and return detailed run info (steps, SQL queries, data previews).

    Request body::

        {
            "question": str,
            "thread_name": str | null,
            "timeout": int
        }
    """
    if not agent_client:
        raise HTTPException(
            status_code=503,
            detail="Fabric Agent (Assistants API) client not configured.",
        )

    body = await request.json()
    question = body.get("question", "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="'question' field is required")

    thread_name = body.get("thread_name")
    timeout = int(body.get("timeout", 120))

    try:
        details = agent_client.get_run_details(
            question=question, timeout=timeout, thread_name=thread_name
        )
        return details
    except Exception as ex:
        logger.exception("agent/run-details error: %s", ex)
        raise HTTPException(status_code=500, detail=str(ex))


@app.post("/api/agent/compare")
async def agent_compare(request: Request) -> Dict[str, Any]:
    """Compare responses from a draft agent and a production agent.

    This implements the pattern described in the Fabric.guru article
    *"Programmatically comparing draft vs production Fabric Data Agent
    responses"*.

    Request body::

        {
            "question": str,                  # required
            "draft_agent_id": str,            # required — Fabric item GUID
            "production_agent_id": str,       # required — Fabric item GUID
            "timeout": int                    # optional (default 120)
        }

    Returns::

        {
            "question": str,
            "draft":      { "answer", "run_status", "sql_queries", "error" },
            "production": { "answer", "run_status", "sql_queries", "error" },
            "match": bool,
            "timestamp": float
        }
    """
    if not agent_client:
        raise HTTPException(
            status_code=503,
            detail="Fabric Agent (Assistants API) client not configured.",
        )

    body = await request.json()
    question = body.get("question", "").strip()
    draft_id = body.get("draft_agent_id", "").strip()
    production_id = body.get("production_agent_id", "").strip()
    timeout = int(body.get("timeout", 120))

    if not question:
        raise HTTPException(status_code=400, detail="'question' is required")
    if not draft_id or not production_id:
        raise HTTPException(
            status_code=400,
            detail="Both 'draft_agent_id' and 'production_agent_id' are required",
        )

    try:
        result = agent_client.compare_draft_vs_production(
            question=question,
            draft_agent_id=draft_id,
            production_agent_id=production_id,
            timeout=timeout,
        )
        return result
    except Exception as ex:
        logger.exception("agent/compare error: %s", ex)
        raise HTTPException(status_code=500, detail=str(ex))


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
