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
  FABRIC_WORKSPACE_ID   - Fabric workspace GUID
                          (output from fabric_setup.py as 'workspace_id')
  FABRIC_DATAAGENT_ID   - Fabric Data Agent item GUID
                          (output from fabric_setup.py as 'dataagent_id')
  FABRIC_DATAAGENT_NAME - Display name of the Data Agent (default: Customer360Agent)
                          Used to auto-rediscover the agent when the stored ID is stale.
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
        self.dataagent_name: str = os.environ.get(
            "FABRIC_DATAAGENT_NAME", "Customer360Agent"
        ).strip()

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
            "FabricClient initialized (workspace=%s, agent=%s, agent_name=%s)",
            self.workspace_id,
            self.dataagent_id,
            self.dataagent_name,
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

    # ─── Auto-discovery ──────────────────────────────────────────────────────

    def _discover_agent_id(self) -> Optional[str]:
        """
        Scan the workspace for a Data Agent whose displayName matches
        self.dataagent_name.  Called automatically when the stored
        FABRIC_DATAAGENT_ID returns 404, so the backend self-heals after
        a re-deploy recreated the agent with a new GUID.

        Tries two endpoints:
          1. GET /workspaces/{id}/dataAgents        (dedicated Data Agent API)
          2. GET /workspaces/{id}/items?type=DataAgent  (generic Items API fallback,
             used when the agent was created via the Items API path)

        Returns the discovered agent ID, or None if not found.
        """
        target = self.dataagent_name.lower()

        # --- Path 1: dedicated /dataAgents endpoint ---
        try:
            list_url = (
                f"{FABRIC_BASE_URL}/workspaces/{self.workspace_id}/dataAgents"
            )
            resp = requests.get(list_url, headers=self._headers(), timeout=30)
            if resp.ok:
                agents = resp.json().get("value", [])
                for agent in agents:
                    if agent.get("displayName", "").lower() == target:
                        discovered_id: str = agent["id"]
                        logger.info(
                            "Agent discovery (/dataAgents): found '%s' with ID %s (was %s)",
                            self.dataagent_name,
                            discovered_id,
                            self.dataagent_id,
                        )
                        return discovered_id
                logger.warning(
                    "Agent discovery (/dataAgents): '%s' not in workspace %s "
                    "(%d agents listed) - trying /items fallback",
                    self.dataagent_name,
                    self.workspace_id,
                    len(agents),
                )
            else:
                logger.warning(
                    "Agent discovery: GET /dataAgents returned %d - trying /items fallback",
                    resp.status_code,
                )
        except Exception as exc:
            logger.warning("Agent discovery /dataAgents failed (non-fatal): %s", exc)

        # --- Path 2: generic /items?type=DataAgent endpoint ---
        try:
            items_url = (
                f"{FABRIC_BASE_URL}/workspaces/{self.workspace_id}"
                f"/items?type=DataAgent"
            )
            resp2 = requests.get(items_url, headers=self._headers(), timeout=30)
            if resp2.ok:
                items = resp2.json().get("value", [])
                for item in items:
                    if item.get("displayName", "").lower() == target:
                        discovered_id = item["id"]
                        logger.info(
                            "Agent discovery (/items): found '%s' with ID %s (was %s)",
                            self.dataagent_name,
                            discovered_id,
                            self.dataagent_id,
                        )
                        return discovered_id
                logger.warning(
                    "Agent discovery (/items): no DataAgent named '%s' in workspace %s "
                    "(%d items listed)",
                    self.dataagent_name,
                    self.workspace_id,
                    len(items),
                )
            else:
                logger.warning(
                    "Agent discovery: GET /items?type=DataAgent returned %d",
                    resp2.status_code,
                )
        except Exception as exc:
            logger.warning("Agent discovery /items failed (non-fatal): %s", exc)

        return None

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

            # Retry on 404 EntityNotFound:
            #   - If this is an early attempt, the agent may just need warm-up
            #     time after a fresh deploy — wait and retry.
            #   - After all warm-up retries, attempt auto-discovery: search the
            #     workspace for an agent named self.dataagent_name. If found,
            #     update self.dataagent_id and retry once with the new ID.
            #     This self-heals the backend when a re-deploy created a new
            #     agent GUID without restarting the App Service.
            if resp.status_code == 404:
                try:
                    error_code = resp.json().get("errorCode", "")
                except Exception:
                    error_code = ""

                if error_code == "EntityNotFound":
                    if _attempt < _AGENT_NOT_READY_RETRIES:
                        logger.warning(
                            "Data Agent not ready yet (attempt %d/%d) - retrying in %ds",
                            _attempt,
                            _AGENT_NOT_READY_RETRIES,
                            _AGENT_NOT_READY_WAIT,
                        )
                        time.sleep(_AGENT_NOT_READY_WAIT)
                        continue

                    # Warm-up retries exhausted — try auto-discovery
                    logger.warning(
                        "Stored agent ID '%s' returned 404 after %d retries. "
                        "Attempting auto-discovery by name '%s'...",
                        self.dataagent_id,
                        _AGENT_NOT_READY_RETRIES,
                        self.dataagent_name,
                    )
                    discovered = self._discover_agent_id()
                    if discovered and discovered != self.dataagent_id:
                        self.dataagent_id = discovered
                        # Retry once with the discovered ID
                        resp = requests.post(
                            self._query_url(),
                            headers=self._headers(),
                            json=payload,
                            timeout=120,
                        )
                        if resp.ok:
                            break
                        logger.error(
                            "Discovered agent ID %s also returned %d",
                            discovered,
                            resp.status_code,
                        )

            # Any other non-2xx (or unrecoverable 404) — surface the error
            error_detail = resp.text[:400]
            logger.error(
                "Fabric Data Agent API error [%d]: %s",
                resp.status_code,
                error_detail,
            )
            if resp.status_code == 404:
                raise RuntimeError(
                    f"Fabric Data Agent returned HTTP 404 (EntityNotFound). "
                    f"Auto-discovery for '{self.dataagent_name}' also failed. "
                    f"Go to https://app.fabric.microsoft.com, open the workspace, "
                    f"confirm '{self.dataagent_name}' exists, then re-run the "
                    f"deploy workflow to refresh FABRIC_DATAAGENT_ID."
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
