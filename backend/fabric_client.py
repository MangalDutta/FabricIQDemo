"""
FabricClient
─────────────
Directly wraps the Microsoft Fabric Data Agent REST API for the
Customer360 chat backend.  No Azure AI Foundry dependency.

Authentication:
  Uses DefaultAzureCredential (Managed Identity on App Service, or
  CLI/env credentials locally).  Tokens are cached and auto-refreshed.

  IMPORTANT: The App Service Managed Identity MUST be a member of the
  Fabric workspace (Contributor or higher).  Without workspace membership
  Fabric returns HTTP 404 EntityNotFound on all resource requests from
  that identity (it hides resources from non-members as a security measure,
  not 401/403).  The deploy workflow adds the MI automatically via
  fabric_setup.py --app_service_principal_id.

Conversation context:
  The Fabric Data Agent maintains conversation history server-side using
  a conversationId string.  The backend tracks one conversationId per
  user (in memory) and includes it on follow-up requests.

Environment variables required:
  FABRIC_WORKSPACE_ID   - Fabric workspace GUID
  FABRIC_DATAAGENT_ID   - Fabric Data Agent item GUID
  FABRIC_DATAAGENT_NAME - Display name of the Data Agent (default: Customer360Agent)
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

# Retry settings for transient errors on a freshly deployed agent
_QUERY_MAX_RETRIES = 3
_QUERY_RETRY_WAIT = 10   # seconds


class AgentNotReadyError(RuntimeError):
    """Raised when the Fabric Data Agent exists but is not in a queryable state.

    Typically means the agent is in Draft/Unpublished state and needs to be
    published manually via the Fabric portal, or the App Service Managed
    Identity has not been added to the workspace.
    """

    def __init__(self, message: str, *, workspace_id: str = "", agent_id: str = ""):
        super().__init__(message)
        self.workspace_id = workspace_id
        self.agent_id = agent_id


class FabricClient:
    """
    Chat client that calls the Fabric Data Agent query API directly.

    Primary API endpoint:
        POST /v1/workspaces/{workspaceId}/dataAgents/{dataAgentId}/query

    Request body:
        {
            "userMessage": "<natural language question>",
            "conversationId": "<string, omit on first turn>"
        }

    Response shape (Fabric Data Agent preview):
        {
            "response":       "<text answer>",
            "conversationId": "<string for next turn>",
            "type":           "answer",
            "citations":      []
        }

    Fallback endpoint (OpenAI-compatible, used when primary returns 404/405):
        POST /v1/workspaces/{workspaceId}/aiskills/{agentId}/aiassistant/openai
        Body: {"messages": [{"role": "user", "content": "..."}]}
        Response: standard OpenAI chat-completion format
    """

    def __init__(self) -> None:
        self.workspace_id: str = os.environ.get("FABRIC_WORKSPACE_ID", "").strip()
        self.dataagent_id: str = os.environ.get("FABRIC_DATAAGENT_ID", "").strip()
        self.dataagent_name: str = os.environ.get(
            "FABRIC_DATAAGENT_NAME", "Customer360Agent"
        ).strip()

        if not self.workspace_id:
            raise ValueError("FABRIC_WORKSPACE_ID environment variable is required.")
        if not self.dataagent_id:
            raise ValueError("FABRIC_DATAAGENT_ID environment variable is required.")

        self._credential = DefaultAzureCredential()

        # Token cache
        self._token: Optional[str] = None
        self._token_expires_on: float = 0.0

        # Per-user conversation IDs (server-side history via conversationId)
        # {user_id: conversationId}
        self._conversation_ids: Dict[str, str] = {}

        # Per-user conversation history sent in each request payload
        # {user_id: [{"role": "user"|"assistant", "content": "..."}]}
        self._histories: Dict[str, List[Dict[str, str]]] = {}

        logger.info(
            "FabricClient initialized  workspace=%s  agent_id=%s  agent_name=%s",
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
        logger.debug("Fabric token refreshed (expires_on=%s)", token_resp.expires_on)
        return self._token

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

    # ─── Endpoints ──────────────────────────────────────────────────────────

    def _primary_url(self) -> str:
        """Fabric Data Agent native query endpoint (preferred)."""
        return (
            f"{FABRIC_BASE_URL}/workspaces/{self.workspace_id}"
            f"/dataAgents/{self.dataagent_id}/query"
        )

    def _openai_compat_url(self) -> str:
        """
        OpenAI-compatible chat completion endpoint.
        'aiskills' is the legacy item-type path; 'dataAgents' is the new path.
        Try both so the client works regardless of how the agent was created.
        """
        return (
            f"{FABRIC_BASE_URL}/workspaces/{self.workspace_id}"
            f"/aiskills/{self.dataagent_id}/aiassistant/openai"
        )

    # ─── Auto-discovery ──────────────────────────────────────────────────────

    def _discover_agent_id(self) -> Optional[str]:
        """
        Search the workspace for a Data Agent whose displayName matches
        self.dataagent_name.  Called when the stored ID returns 404.

        Tries:
          1. GET /workspaces/{id}/dataAgents        (Data Agent API)
          2. GET /workspaces/{id}/items?type=DataAgent  (Items API fallback)

        Returns the discovered agent ID, or None if not found.
        """
        target = self.dataagent_name.lower()

        for path, key in [
            (f"/workspaces/{self.workspace_id}/dataAgents", "value"),
            (f"/workspaces/{self.workspace_id}/items?type=DataAgent", "value"),
        ]:
            try:
                resp = requests.get(
                    f"{FABRIC_BASE_URL}{path}",
                    headers=self._headers(),
                    timeout=30,
                )
                if resp.ok:
                    items = resp.json().get(key, []) or []
                    for item in items:
                        if item.get("displayName", "").lower() == target:
                            discovered: str = item["id"]
                            logger.info(
                                "Auto-discovery via %s: found '%s' (ID: %s, was: %s)",
                                path, self.dataagent_name, discovered, self.dataagent_id,
                            )
                            return discovered
                    logger.warning(
                        "Auto-discovery via %s: agent '%s' not found (%d items listed). "
                        "Ensure the App Service Managed Identity is a Fabric workspace member.",
                        path, self.dataagent_name, len(items),
                    )
                else:
                    logger.warning(
                        "Auto-discovery GET %s returned HTTP %d: %s",
                        path, resp.status_code, resp.text[:200],
                    )
            except Exception as exc:
                logger.warning("Auto-discovery %s failed (non-fatal): %s", path, exc)

        return None

    # ─── Helper: call primary endpoint ──────────────────────────────────────

    def _call_primary(
        self, message: str, conversation_id: Optional[str],
        history: Optional[List[Dict[str, str]]] = None,
    ) -> Optional[requests.Response]:
        """POST to the native Data Agent /query endpoint.

        Returns the response, or None if a network/connection error occurs.
        """
        payload: Dict[str, Any] = {"userMessage": message}
        if conversation_id:
            payload["conversationId"] = conversation_id
        if history:
            payload["history"] = history

        logger.debug(
            "POST %s  conversationId=%s  message=%.80s",
            self._primary_url(), conversation_id, message,
        )
        try:
            return requests.post(
                self._primary_url(),
                headers=self._headers(),
                json=payload,
                timeout=120,
            )
        except requests.exceptions.RequestException as exc:
            logger.warning("Primary endpoint request failed: %s", exc)
            return None

    # ─── Helper: call OpenAI-compatible fallback endpoint ───────────────────

    def _call_openai_compat(
        self, message: str, conversation_id: Optional[str]
    ) -> Optional[requests.Response]:
        """
        POST to the OpenAI-compatible /aiskills/.../aiassistant/openai endpoint.
        Returns the response, or None if the endpoint is not found (404).
        """
        # Build messages: if we have a conversationId we can't reconstruct history,
        # so just send the current question (the server holds context).
        payload: Dict[str, Any] = {
            "messages": [{"role": "user", "content": message}],
        }
        url = self._openai_compat_url()
        logger.debug("POST (openai-compat fallback) %s", url)
        try:
            resp = requests.post(
                url,
                headers=self._headers(),
                json=payload,
                timeout=120,
            )
            return resp
        except Exception as exc:
            logger.warning("OpenAI-compat fallback exception: %s", exc)
            return None

    # ─── Extract answer from various response shapes ─────────────────────────

    @staticmethod
    def _extract_answer(data: Dict[str, Any]) -> str:
        """
        Pull the text answer from the Fabric Data Agent response.

        Known response shapes:
          Native endpoint:
            {"response": "...", "conversationId": "...", "type": "answer"}
          OpenAI-compatible endpoint:
            {"choices": [{"message": {"role": "assistant", "content": "..."}}]}
          Legacy / preview variations:
            {"answer": "..."}, {"message": "..."}, {"text": "..."}
        """
        # Native endpoint field
        native = data.get("response")
        if native:
            return str(native)

        # OpenAI chat-completion format
        choices = data.get("choices") or []
        if choices:
            msg = choices[0].get("message", {})
            content = msg.get("content") or ""
            if content:
                return str(content)

        # Legacy / other preview field names
        for key in ("answer", "message", "text", "output", "result"):
            val = data.get(key)
            if val and isinstance(val, str):
                return val

        return ""

    # ─── Public chat method ──────────────────────────────────────────────────

    def chat(self, user_id: str, message: str) -> Dict[str, Any]:
        """
        Send *message* from *user_id* to the Fabric Data Agent and return::

            {
                "answer":    str,
                "timestamp": str,    # ISO 8601 UTC
                "metadata":  dict
            }

        Raises RuntimeError if the Fabric API returns a non-2xx status that
        cannot be recovered from.
        """
        conversation_id = self._conversation_ids.get(user_id)
        history = self._histories.get(user_id, [])

        logger.info(
            "Fabric Data Agent query  user=%s  agent=%s  workspace=%s  "
            "conversationId=%s  message=%.80s",
            user_id, self.dataagent_id, self.workspace_id,
            conversation_id, message,
        )

        resp: Optional[requests.Response] = None
        used_endpoint = "primary"

        # ── Pre-flight: verify agent state via GET ────────────────────────────────
        # Log the agent state before querying so failures are easier to diagnose.
        # A Fabric Data Agent in Draft state returns 404 on /query even when the
        # workspace Managed Identity is a Contributor.
        try:
            agent_check = requests.get(
                f"{FABRIC_BASE_URL}/workspaces/{self.workspace_id}"
                f"/dataAgents/{self.dataagent_id}",
                headers=self._headers(),
                timeout=20,
            )
            if agent_check.ok:
                agent_data = agent_check.json()
                raw_state = (
                    agent_data.get("state")
                    or agent_data.get("status")
                    or agent_data.get("publishState")
                    or "unknown"
                )
                logger.info(
                    "Agent pre-flight: '%s' state=%s",
                    self.dataagent_id, raw_state,
                )
                # If agent is in Draft state, fail fast with a clear,
                # actionable error instead of burning through retry cycles
                # that will all return 404.
                if str(raw_state).lower() in ("draft", "unpublished", "inactive"):
                    raise AgentNotReadyError(
                        f"Fabric Data Agent '{self.dataagent_name}' is in "
                        f"'{raw_state}' state and cannot answer queries yet.\n"
                        f"To fix: open the Fabric portal, open the agent, "
                        f"and click 'Publish':\n"
                        f"  https://app.fabric.microsoft.com/groups/"
                        f"{self.workspace_id}\n"
                        f"After publishing, retry your question.\n"
                        f"For diagnostics visit the /api/debug endpoint on "
                        f"the backend.",
                        workspace_id=self.workspace_id,
                        agent_id=self.dataagent_id,
                    )
            else:
                logger.warning(
                    "Agent pre-flight GET returned HTTP %d: %s",
                    agent_check.status_code, agent_check.text[:200],
                )
        except AgentNotReadyError:
            raise
        except Exception as pre_exc:
            logger.debug("Agent pre-flight check failed (non-fatal): %s", pre_exc)

        # ── Attempt 1: primary endpoint with current agent ID ────────────────
        for attempt in range(1, _QUERY_MAX_RETRIES + 1):
            resp = self._call_primary(message, conversation_id, history)

            if resp is None:
                logger.warning(
                    "Primary endpoint returned no response on attempt %d/%d — "
                    "network error or connection refused.",
                    attempt, _QUERY_MAX_RETRIES,
                )
                if attempt < _QUERY_MAX_RETRIES:
                    time.sleep(_QUERY_RETRY_WAIT)
                    continue
                break

            if resp.ok:
                break

            # Clear cached token on auth errors
            if resp.status_code in (401, 403):
                self._token = None
                logger.warning(
                    "Auth error %d on Fabric API — token cleared. "
                    "Ensure the App Service Managed Identity is added to the "
                    "Fabric workspace (Contributor role). "
                    "workspace=%s  agent=%s",
                    resp.status_code, self.workspace_id, self.dataagent_id,
                )

            if resp.status_code == 404:
                try:
                    err_code = resp.json().get("errorCode", "")
                except Exception:
                    err_code = ""

                if err_code == "EntityNotFound":
                    if attempt < _QUERY_MAX_RETRIES:
                        logger.warning(
                            "EntityNotFound on attempt %d/%d — agent may be warming up. "
                            "Retrying in %ds.  agent_id=%s",
                            attempt, _QUERY_MAX_RETRIES, _QUERY_RETRY_WAIT,
                            self.dataagent_id,
                        )
                        time.sleep(_QUERY_RETRY_WAIT)
                        continue

                    # Exhausted warm-up retries — try auto-discovery
                    logger.warning(
                        "Stored agent ID '%s' returned 404 after %d retries. "
                        "Attempting auto-discovery by name '%s'...",
                        self.dataagent_id, _QUERY_MAX_RETRIES, self.dataagent_name,
                    )
                    discovered = self._discover_agent_id()
                    if discovered and discovered != self.dataagent_id:
                        logger.info(
                            "Auto-discovery succeeded: new agent ID = %s", discovered
                        )
                        self.dataagent_id = discovered
                        # Reset conversationId — new agent won't know old context
                        self._conversation_ids.pop(user_id, None)
                        conversation_id = None
                        resp = self._call_primary(message, conversation_id, history)
                        if resp is not None and resp.ok:
                            break
                        if resp is not None:
                            logger.error(
                                "Discovered agent ID %s also returned %d: %s",
                                discovered, resp.status_code, resp.text[:300],
                            )

            # Non-recoverable on this attempt — break and try fallback
            break

        # ── Attempt 2: OpenAI-compatible fallback endpoint ───────────────────
        if not resp or not resp.ok:
            logger.info(
                "Primary endpoint failed (HTTP %s) — trying OpenAI-compat fallback",
                resp.status_code if resp else "N/A",
            )
            fallback = self._call_openai_compat(message, conversation_id)
            if fallback and fallback.ok:
                resp = fallback
                used_endpoint = "openai-compat"
            elif fallback:
                logger.warning(
                    "OpenAI-compat fallback also failed HTTP %d: %s",
                    fallback.status_code, fallback.text[:300],
                )

        # ── Raise if still not ok ─────────────────────────────────────────────
        if not resp or not resp.ok:
            if resp is None:
                raise RuntimeError(
                    "Fabric Data Agent returned no HTTP response from primary and fallback endpoints. "
                    "Likely network timeout/connection issue between the web app and "
                    "Fabric API. Please check network connectivity and endpoint availability. "
                    f"(primary={self._primary_url()}, fallback={self._openai_compat_url()})"
                )
            status = resp.status_code
            detail = resp.text[:400]

            # Build a helpful, actionable error message
            if status == 404:
                raise AgentNotReadyError(
                    f"Fabric Data Agent returned HTTP 404 (EntityNotFound). "
                    f"The agent '{self.dataagent_name}' is not reachable.\n"
                    f"Two most common causes:\n"
                    f"  1. Agent is in DRAFT state (not Published). Open the Fabric portal, "
                    f"open the agent, and click 'Publish':\n"
                    f"     https://app.fabric.microsoft.com/groups/{self.workspace_id}\n"
                    f"  2. The App Service Managed Identity is not a workspace member. "
                    f"Re-run the deploy workflow to add it, or add manually:\n"
                    f"     Fabric portal -> Workspace -> Manage access -> Add as Contributor.\n"
                    f"Visit the /api/debug endpoint on the backend for diagnostics.",
                    workspace_id=self.workspace_id,
                    agent_id=self.dataagent_id,
                )
            if status in (401, 403):
                raise RuntimeError(
                    f"Fabric Data Agent returned HTTP {status} (auth error). "
                    f"The App Service Managed Identity does not have access. "
                    f"Ensure it is a Fabric workspace Contributor. "
                    f"(workspace={self.workspace_id})"
                )
            raise RuntimeError(
                f"Fabric Data Agent returned HTTP {status}: {detail}"
            )

        # ── Parse response ────────────────────────────────────────────────────
        data = resp.json()
        logger.debug(
            "Fabric API response  endpoint=%s  keys=%s", used_endpoint, list(data.keys())
        )

        answer = self._extract_answer(data)

        # Update conversationId for next turn (only from native endpoint)
        if used_endpoint == "primary":
            new_conv_id = data.get("conversationId") or data.get("sessionId")
            if new_conv_id:
                self._conversation_ids[user_id] = str(new_conv_id)
            elif user_id in self._conversation_ids:
                # Keep existing conversationId if API didn't return a new one
                pass

        # Update per-user history for next turn
        history_from_response = data.get("history")
        if history_from_response is not None and isinstance(history_from_response, list):
            self._histories[user_id] = history_from_response
        else:
            current_history = self._histories.get(user_id, [])
            self._histories[user_id] = current_history + [
                {"role": "user", "content": message},
                {"role": "assistant", "content": answer},
            ]

        timestamp = datetime.now(tz=timezone.utc).isoformat()

        return {
            "answer": answer or "No response received.",
            "timestamp": timestamp,
            "metadata": {
                "workspace_id": self.workspace_id,
                "dataagent_id": self.dataagent_id,
                "source": "fabric_data_agent",
                "endpoint": used_endpoint,
            },
        }

    def reset_conversation(self, user_id: str) -> None:
        """Clear the stored conversationId and history so the next message starts fresh."""
        self._conversation_ids.pop(user_id, None)
        self._histories.pop(user_id, None)
        logger.info("Conversation reset for user %s", user_id)
