"""
FoundryClient
─────────────
Wraps Azure AI Foundry Agents API for the Customer360 chat backend.

Authentication:
  Uses DefaultAzureCredential (Managed Identity on App Service, or
  CLI/env credentials locally).  No manual token management needed.

Thread management:
  A per-user thread is kept in memory so follow-up questions maintain
  context within a single app-service instance.  For production you'd
  persist thread IDs in a cache / database.

Environment variables required:
  AZURE_AI_FOUNDRY_PROJECT_ENDPOINT  – AI Foundry project endpoint
                                       e.g. https://<project>.api.azureml.ms
  AZURE_AI_FOUNDRY_AGENT_ID          – Agent ID (asst_…)
  FABRIC_CONNECTION_ID               – (optional) Fabric connection ID used
                                       when creating the agent in AI Foundry
"""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger("customer360-foundry")

# ── SDK import with graceful fallback ────────────────────────────────────────
try:
    from azure.ai.projects import AIProjectClient
    from azure.ai.projects.models import MessageTextContent
    from azure.identity import DefaultAzureCredential
    _SDK_AVAILABLE = True
except ImportError:
    _SDK_AVAILABLE = False
    logger.warning(
        "azure-ai-projects SDK not installed. "
        "Will attempt direct REST fallback."
    )

# ── REST fallback imports ─────────────────────────────────────────────────────
import requests


class FoundryClient:
    """
    Chat client for Azure AI Foundry Agents with Fabric Data Agent tool.

    Priority:
      1. azure-ai-projects SDK  (recommended, handles auth + polling)
      2. Direct REST calls      (fallback if SDK unavailable)
    """

    _FOUNDRY_SCOPE = "https://ml.azure.com/.default"
    _POLL_INTERVAL = 2      # seconds between run-status polls
    _POLL_TIMEOUT  = 120    # seconds max wait for a run to complete

    def __init__(self) -> None:
        self.project_endpoint: str = os.environ.get(
            "AZURE_AI_FOUNDRY_PROJECT_ENDPOINT", ""
        ).rstrip("/")
        self.agent_id: str = os.environ.get("AZURE_AI_FOUNDRY_AGENT_ID", "")
        self.fabric_connection_id: str = os.environ.get("FABRIC_CONNECTION_ID", "")
        self.api_version: str = os.environ.get(
            "AZURE_AI_FOUNDRY_API_VERSION", "2025-05-15-preview"
        )

        if not self.project_endpoint:
            raise ValueError(
                "AZURE_AI_FOUNDRY_PROJECT_ENDPOINT environment variable is required."
            )
        if not self.agent_id:
            raise ValueError(
                "AZURE_AI_FOUNDRY_AGENT_ID environment variable is required."
            )

        # Per-user thread cache: {user_id: thread_id}
        self._threads: Dict[str, str] = {}

        if _SDK_AVAILABLE:
            self._credential = DefaultAzureCredential()
            self._sdk_client = AIProjectClient(
                endpoint=self.project_endpoint,
                credential=self._credential,
            )
            logger.info("FoundryClient initialized (azure-ai-projects SDK)")
        else:
            self._credential = None
            self._sdk_client = None
            logger.info("FoundryClient initialized (REST fallback)")

    # ─── Public chat method ──────────────────────────────────────────────────

    def chat(self, user_id: str, message: str) -> Dict[str, Any]:
        """
        Send *message* from *user_id* to the AI Foundry agent and return
        the assistant reply dict::

            {
                "answer": str,
                "timestamp": str | None,  # ISO 8601
                "metadata": dict
            }
        """
        if _SDK_AVAILABLE and self._sdk_client is not None:
            return self._chat_sdk(user_id, message)
        return self._chat_rest(user_id, message)

    # ─── SDK path ────────────────────────────────────────────────────────────

    def _chat_sdk(self, user_id: str, message: str) -> Dict[str, Any]:
        client = self._sdk_client
        agents = client.agents

        # Get or create thread for this user
        thread_id = self._threads.get(user_id)
        if not thread_id:
            thread = agents.create_thread()
            thread_id = thread.id
            self._threads[user_id] = thread_id
            logger.debug("Created new thread %s for user %s", thread_id, user_id)
        else:
            logger.debug("Reusing thread %s for user %s", thread_id, user_id)

        # Add user message
        agents.create_message(
            thread_id=thread_id,
            role="user",
            content=message,
        )

        # Create and process run (polls until terminal state)
        run = agents.create_and_process_run(
            thread_id=thread_id,
            agent_id=self.agent_id,
        )

        if run.status != "completed":
            # If failed, discard thread so next message starts fresh
            if run.status in ("failed", "cancelled", "expired"):
                self._threads.pop(user_id, None)
            error_detail = getattr(run, "last_error", None)
            raise RuntimeError(
                f"Agent run ended with status '{run.status}'. "
                f"Detail: {error_detail}"
            )

        # Retrieve messages (most-recent first)
        messages = agents.list_messages(thread_id=thread_id)
        answer = ""
        created_at: Optional[int] = None

        for msg in messages.data:
            if msg.role == "assistant":
                for block in msg.content or []:
                    if isinstance(block, MessageTextContent):
                        answer = block.text.value or ""
                    elif hasattr(block, "text") and hasattr(block.text, "value"):
                        answer = block.text.value or ""
                    elif isinstance(block, dict):
                        answer = block.get("text", {}).get("value", "")
                if answer:
                    created_at = getattr(msg, "created_at", None)
                    break  # use the latest assistant message

        timestamp = None
        if created_at is not None:
            timestamp = datetime.fromtimestamp(created_at, tz=timezone.utc).isoformat()

        return {
            "answer": answer or "No response received.",
            "timestamp": timestamp,
            "metadata": {
                "agent_id": self.agent_id,
                "thread_id": thread_id,
                "run_id": run.id,
                "run_status": run.status,
            },
        }

    # ─── REST fallback path ──────────────────────────────────────────────────

    def _get_rest_token(self) -> str:
        """Acquire token for Azure ML / AI Foundry REST API."""
        if self._credential is None:
            from azure.identity import DefaultAzureCredential
            self._credential = DefaultAzureCredential()
        return self._credential.get_token(self._FOUNDRY_SCOPE).token

    def _rest_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_rest_token()}",
            "Content-Type": "application/json",
        }

    def _rest_url(self, path: str) -> str:
        return (
            f"{self.project_endpoint}/openai{path}"
            f"?api-version={self.api_version}"
        )

    def _chat_rest(self, user_id: str, message: str) -> Dict[str, Any]:
        """Direct REST implementation of the Agents Thread/Run pattern."""
        headers = self._rest_headers()

        # Get or create thread
        thread_id = self._threads.get(user_id)
        if not thread_id:
            resp = requests.post(
                self._rest_url("/threads"),
                headers=headers,
                json={},
                timeout=30,
            )
            resp.raise_for_status()
            thread_id = resp.json()["id"]
            self._threads[user_id] = thread_id
            logger.debug("REST: created thread %s for user %s", thread_id, user_id)

        # Add user message
        resp = requests.post(
            self._rest_url(f"/threads/{thread_id}/messages"),
            headers=headers,
            json={"role": "user", "content": message},
            timeout=30,
        )
        resp.raise_for_status()

        # Create run
        run_payload: Dict[str, Any] = {"assistant_id": self.agent_id}
        if self.fabric_connection_id:
            run_payload["tools"] = [
                {
                    "type": "microsoft_fabric_dataagent",
                    "connection_id": self.fabric_connection_id,
                }
            ]
        resp = requests.post(
            self._rest_url(f"/threads/{thread_id}/runs"),
            headers=headers,
            json=run_payload,
            timeout=60,
        )
        resp.raise_for_status()
        run = resp.json()
        run_id = run["id"]

        # Poll for completion
        deadline = time.time() + self._POLL_TIMEOUT
        while time.time() < deadline:
            time.sleep(self._POLL_INTERVAL)
            resp = requests.get(
                self._rest_url(f"/threads/{thread_id}/runs/{run_id}"),
                headers=self._rest_headers(),  # refresh token each poll
                timeout=30,
            )
            resp.raise_for_status()
            run = resp.json()
            status = run.get("status", "")
            if status == "completed":
                break
            if status in ("failed", "cancelled", "expired"):
                self._threads.pop(user_id, None)
                raise RuntimeError(
                    f"Agent run ended with status '{status}'. "
                    f"Error: {run.get('last_error')}"
                )

        # Retrieve messages
        resp = requests.get(
            self._rest_url(f"/threads/{thread_id}/messages"),
            headers=self._rest_headers(),
            timeout=30,
        )
        resp.raise_for_status()
        messages = resp.json().get("data", [])

        answer = ""
        timestamp = None
        for msg in messages:
            if msg.get("role") == "assistant":
                for block in msg.get("content", []):
                    if block.get("type") == "text":
                        answer = block.get("text", {}).get("value", "")
                if answer:
                    raw_ts = msg.get("created_at")
                    if raw_ts:
                        timestamp = datetime.fromtimestamp(
                            raw_ts, tz=timezone.utc
                        ).isoformat()
                    break

        return {
            "answer": answer or "No response received.",
            "timestamp": timestamp,
            "metadata": {
                "agent_id": self.agent_id,
                "thread_id": thread_id,
                "run_id": run_id,
                "run_status": run.get("status"),
            },
        }
