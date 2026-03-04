"""
FabricClient
─────────────
Directly wraps the Microsoft Fabric Data Agent REST API for the
Customer360 chat backend.  No Azure AI Foundry dependency.

Authentication:
  Uses DefaultAzureCredential (Managed Identity on App Service, or
  CLI/env credentials locally).  Tokens are cached and auto-refreshed.

Conversation history:
  Per-user history is kept in memory so follow-up questions maintain
  context within a single app-service instance.  For production you
  would persist this in Redis or a database.

Environment variables required:
  FABRIC_WORKSPACE_ID   – Fabric workspace GUID
                          (output from fabric_setup.py as 'workspace_id')
  FABRIC_DATAAGENT_ID   – Fabric Data Agent item GUID
                          (output from fabric_setup.py as 'dataagent_id')
"""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from azure.identity import DefaultAzureCredential

logger = logging.getLogger("customer360-fabric")

# Scope required to call Fabric REST APIs
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
FABRIC_BASE_URL = "https://api.fabric.microsoft.com/v1"

# Seconds before token expiry to pro-actively refresh
_TOKEN_REFRESH_MARGIN = 60

# Retry config for EntityNotFound on /query — newly provisioned agents need
# a short warm-up period before they become queryable.
_AGENT_NOT_READY_RETRIES = 4
_AGENT_NOT_READY_WAIT = 15  # seconds between retries (up to ~1 min total)


class FabricClient:
    """
    Chat client that calls the Fabric Data Agent query API directly.

    API endpoint used:
        POST /v1/workspaces/{workspaceId}/dataAgents/{dataAgentId}/query

    Request body:
        {
            "userMessage": "<natural language question>",
            "history": [
                {"role": "user",      "content": "..."},
                {"role": "assistant", "content": "..."}
            ]
        }

    Response shape (approximate – the API is in preview; multiple field
    names are tried for forward-compatibility):
        {
            "answer": "<text answer>",
            "history": [...]           // optional, updated turn list
        }
    """

    def __init__(self) -> None:
        self.workspace_id: str = os.environ.get("FABRIC_WORKSPACE_ID", "").strip()
        self.dataagent_id: str = os.environ.get("FABRIC_DATAAGENT_ID", "").strip()

        if not self.workspace_id:
            raise ValueError(
                "FABRIC_WORKSPACE_ID environment variable is required."
            )
        if not self.dataagent_id:
            raise ValueError(
                "FABRIC_DATAAGENT_ID environment variable is required."
            )

        self._credential = DefaultAzureCredential()

        # Token cache
        self._token: Optional[str] = None
        self._token_expires_on: float = 0.0

        # Per-user conversation history: {user_id: [{"role": ..., "content": ...}]}
        self._histories: Dict[str, List[Dict[str, str]]] = {}

        logger.info(
            "FabricClient initialized (workspace=%s, agent=%s)",
            self.workspace_id,
            self.dataagent_id,
        )

    # ─── Authentication ──────────────────────────────────────────────────────

    def _get_token(self) -> str:
        """Return a valid Fabric bearer token, refreshing before expiry."""
        now = time.time()
        if self._token and now < self._token_expires_on - _TOKEN_REFRESH_MARGIN:
            return self._token

        token_resp = self._credential.get_token(FABRIC_SCOPE)
        self._token = token_resp.token
        self._token_expires_on = float(token_resp.expires_on)
        logger.debug("Acquired new Fabric token (expires_on=%s)", token_resp.expires_on)
        return self._token

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

    # ─── API endpoint ────────────────────────────────────────────────────────

    def _query_url(self) -> str:
        return (
            f"{FABRIC_BASE_URL}/workspaces/{self.workspace_id}"
            f"/dataAgents/{self.dataagent_id}/query"
        )

    # ─── Public chat method ──────────────────────────────────────────────────

    def chat(self, user_id: str, message: str) -> Dict[str, Any]:
        """
        Send *message* from *user_id* to the Fabric Data Agent and return::

            {
                "answer":    str,
                "timestamp": str,    # ISO 8601 UTC
                "metadata":  dict
            }

        Raises RuntimeError if the Fabric API returns a non-2xx status.
        """
        history = self._histories.get(user_id, [])

        payload: Dict[str, Any] = {
            "userMessage": message,
            "history": history,
        }

        logger.info(
            "Fabric Data Agent query  user=%s  message=%.80s",
            user_id,
            message,
        )

        resp = None
        for _attempt in range(1, _AGENT_NOT_READY_RETRIES + 1):
            resp = requests.post(
                self._query_url(),
                headers=self._headers(),
                json=payload,
                timeout=120,
            )
            if resp.ok:
                break

            # On auth errors, clear the cached token so the next call retries
            if resp.status_code in (401, 403):
                self._token = None

            # Retry on 404 EntityNotFound — newly provisioned agents need
            # a brief warm-up period before the /query endpoint is live.
            if resp.status_code == 404:
                try:
                    error_code = resp.json().get("errorCode", "")
                except Exception:
                    error_code = ""
                if error_code == "EntityNotFound" and _attempt < _AGENT_NOT_READY_RETRIES:
                    logger.warning(
                        "Data Agent not ready yet (attempt %d/%d) — retrying in %ds",
                        _attempt,
                        _AGENT_NOT_READY_RETRIES,
                        _AGENT_NOT_READY_WAIT,
                    )
                    time.sleep(_AGENT_NOT_READY_WAIT)
                    continue

            # Any other non-2xx (or exhausted retries) — surface the error
            error_detail = resp.text[:400]
            logger.error(
                "Fabric Data Agent API error [%d]: %s",
                resp.status_code,
                error_detail,
            )
            raise RuntimeError(
                f"Fabric Data Agent returned HTTP {resp.status_code}: {error_detail}"
            )

        data = resp.json()
        logger.debug("Fabric API response keys: %s", list(data.keys()))

        # ── Extract answer (handle preview API field-name variations) ────────
        answer: str = (
            data.get("answer")
            or data.get("response")
            or data.get("message")
            or data.get("text")
            or ""
        )

        # ── Update stored conversation history ───────────────────────────────
        returned_history = data.get("history") or data.get("conversationHistory")
        if returned_history is not None:
            # API returned updated history — use it directly
            self._histories[user_id] = returned_history
        else:
            # API did not return history — build it manually
            updated = list(history)
            updated.append({"role": "user", "content": message})
            if answer:
                updated.append({"role": "assistant", "content": answer})
            self._histories[user_id] = updated

        timestamp = datetime.now(tz=timezone.utc).isoformat()

        return {
            "answer": answer or "No response received.",
            "timestamp": timestamp,
            "metadata": {
                "workspace_id": self.workspace_id,
                "dataagent_id": self.dataagent_id,
                "source": "fabric_data_agent",
            },
        }
