from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import base64
import json
import logging
import os
from typing import Any, Dict

import requests as _requests

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
