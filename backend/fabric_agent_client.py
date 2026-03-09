"""
FabricAgentClient — OpenAI Assistants API client for Fabric Data Agents
────────────────────────────────────────────────────────────────────────
Server-side adaptation of Microsoft's ``fabric_data_agent_client``
(https://github.com/microsoft/fabric_data_agent_client) for use inside
a FastAPI backend with ``DefaultAzureCredential`` (Managed Identity).

Instead of InteractiveBrowserCredential (which opens a browser and is
unsuitable for headless servers), this module uses the same
DefaultAzureCredential the rest of the backend already relies on.

Key capabilities beyond the existing ``fabric_client.py`` REST wrapper:
  • OpenAI Assistants API (threads → messages → runs → steps)
  • Named / persistent conversation threads
  • Detailed run-step introspection (SQL queries, data previews)
  • Draft-vs-Production agent comparison

Environment variables (same as fabric_client.py):
  FABRIC_WORKSPACE_ID   — Fabric workspace GUID
  FABRIC_DATAAGENT_ID   — Fabric Data Agent item GUID
  FABRIC_DATAAGENT_NAME — display name (default: Customer360Agent)
"""

import json
import logging
import os
import re
import time
import uuid
import warnings
from typing import Any, Dict, List, Optional

from azure.identity import DefaultAzureCredential
from openai import OpenAI

# Suppress OpenAI Assistants API deprecation warnings
# (Fabric Data Agents don't support the newer Responses API yet)
warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    message=r".*Assistants API is deprecated.*",
)

logger = logging.getLogger("customer360-agent-client")

FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
FABRIC_BASE_URL = "https://api.fabric.microsoft.com"

# Seconds before token expiry to pro-actively refresh
_TOKEN_REFRESH_MARGIN = 300  # 5 minutes


class FabricAgentClient:
    """OpenAI-Assistants-API client for Fabric Data Agents.

    This client wraps the Data-Agent's OpenAI-compatible endpoint::

        {FABRIC_BASE_URL}/v1/workspaces/{workspace_id}
            /dataAgents/{agent_id}/aiassistant/openai

    using the ``openai`` Python SDK (Assistants → threads, messages,
    runs, steps).
    """

    def __init__(
        self,
        workspace_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        agent_name: Optional[str] = None,
    ) -> None:
        self.workspace_id = (
            workspace_id or os.environ.get("FABRIC_WORKSPACE_ID", "").strip()
        )
        self.agent_id = (
            agent_id or os.environ.get("FABRIC_DATAAGENT_ID", "").strip()
        )
        self.agent_name = (
            agent_name
            or os.environ.get("FABRIC_DATAAGENT_NAME", "Customer360Agent").strip()
        )

        if not self.workspace_id:
            raise ValueError("workspace_id / FABRIC_WORKSPACE_ID is required")
        if not self.agent_id:
            raise ValueError("agent_id / FABRIC_DATAAGENT_ID is required")

        self._credential = DefaultAzureCredential()
        self._token: Optional[Any] = None  # azure.core.credentials.AccessToken
        self._token_expires_on: float = 0.0

        # Build the base URL for the OpenAI-compatible endpoint
        self._data_agent_url = (
            f"{FABRIC_BASE_URL}/v1/workspaces/{self.workspace_id}"
            f"/dataAgents/{self.agent_id}/aiassistant/openai"
        )

        logger.info(
            "FabricAgentClient initialised  workspace=%s  agent=%s  url=%s",
            self.workspace_id,
            self.agent_id,
            self._data_agent_url,
        )

    # ── Authentication ────────────────────────────────────────────────────────

    def _refresh_token(self) -> None:
        """Acquire / refresh a Fabric bearer token via Managed Identity."""
        self._token = self._credential.get_token(FABRIC_SCOPE)
        self._token_expires_on = float(self._token.expires_on)
        logger.debug("Token refreshed, expires_on=%s", self._token.expires_on)

    def _get_valid_token(self) -> str:
        """Return a valid bearer token string, refreshing if needed."""
        now = time.time()
        if self._token and now < self._token_expires_on - _TOKEN_REFRESH_MARGIN:
            return self._token.token
        self._refresh_token()
        return self._token.token  # type: ignore[union-attr]

    # ── OpenAI client factory ─────────────────────────────────────────────────

    def _get_openai_client(self) -> OpenAI:
        """Create an ``OpenAI`` client pointing at the Fabric Data Agent."""
        token = self._get_valid_token()
        return OpenAI(
            api_key="",  # not used — bearer token auth
            base_url=self._data_agent_url,
            default_query={"api-version": "2024-05-01-preview"},
            default_headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "ActivityId": str(uuid.uuid4()),
            },
        )

    # ── Thread management ─────────────────────────────────────────────────────

    def get_or_create_thread(
        self,
        thread_name: Optional[str] = None,
    ) -> Dict[str, str]:
        """Get an existing thread by name, or create a new one.

        Returns ``{"id": "<thread-id>", "name": "<thread-name>"}``.
        """
        import requests as _requests

        if thread_name is None:
            thread_name = f"external-client-thread-{uuid.uuid4()}"

        base_url = self._data_agent_url
        if "aiskills" in base_url:
            base_url = (
                base_url.replace("aiskills", "dataagents")
                .removesuffix("/openai")
                .replace("/aiassistant", "/__private/aiassistant")
            )
        else:
            base_url = base_url.removesuffix("/openai").replace(
                "/aiassistant", "/__private/aiassistant"
            )

        url = f'{base_url}/threads/fabric?tag="{thread_name}"'
        headers = {
            "Authorization": f"Bearer {self._get_valid_token()}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "ActivityId": str(uuid.uuid4()),
        }

        resp = _requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        thread = resp.json()
        thread["name"] = thread_name
        return thread

    # ── Core: ask ─────────────────────────────────────────────────────────────

    def ask(
        self,
        question: str,
        timeout: int = 120,
        thread_name: Optional[str] = None,
    ) -> str:
        """Ask a question, return the plain-text answer."""
        if not question.strip():
            raise ValueError("Question cannot be empty")

        client = self._get_openai_client()
        assistant = client.beta.assistants.create(model="not used")
        thread = self.get_or_create_thread(thread_name=thread_name)

        client.beta.threads.messages.create(
            thread_id=thread["id"], role="user", content=question
        )
        run = client.beta.threads.runs.create(
            thread_id=thread["id"], assistant_id=assistant.id
        )

        start = time.time()
        while run.status in ("queued", "in_progress"):
            if time.time() - start > timeout:
                logger.warning("ask() timed out after %ds", timeout)
                break
            time.sleep(2)
            run = client.beta.threads.runs.retrieve(
                thread_id=thread["id"], run_id=run.id
            )

        messages = client.beta.threads.messages.list(
            thread_id=thread["id"], order="asc"
        )

        responses: List[str] = []
        for msg in messages.data:
            if msg.role == "assistant":
                try:
                    content = msg.content[0]
                    if hasattr(content, "text"):
                        text_content = getattr(content, "text", None)
                        if text_content is not None and hasattr(text_content, "value"):
                            responses.append(text_content.value)
                        elif text_content is not None:
                            responses.append(str(text_content))
                        else:
                            responses.append(str(content))
                    else:
                        responses.append(str(content))
                except (IndexError, AttributeError):
                    responses.append(str(msg.content))

        # Cleanup
        try:
            client.beta.threads.delete(thread_id=thread["id"])
        except Exception:
            pass

        return "\n".join(responses) if responses else "No response received."

    # ── Core: get_run_details ─────────────────────────────────────────────────

    def get_run_details(
        self,
        question: str,
        timeout: int = 120,
        thread_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Ask a question, return detailed run info (steps, SQL, data previews)."""
        client = self._get_openai_client()
        assistant = client.beta.assistants.create(model="not used")
        thread = self.get_or_create_thread(thread_name=thread_name)

        client.beta.threads.messages.create(
            thread_id=thread["id"], role="user", content=question
        )
        run = client.beta.threads.runs.create(
            thread_id=thread["id"], assistant_id=assistant.id
        )

        start = time.time()
        while run.status in ("queued", "in_progress"):
            if time.time() - start > timeout:
                break
            time.sleep(2)
            run = client.beta.threads.runs.retrieve(
                thread_id=thread["id"], run_id=run.id
            )

        steps = client.beta.threads.runs.steps.list(
            thread_id=thread["id"], run_id=run.id
        )
        messages = client.beta.threads.messages.list(
            thread_id=thread["id"], order="asc"
        )

        # Extract SQL queries from steps
        sql_analysis = self._extract_sql_from_steps(steps)

        # Also pull data from the assistant's final text
        messages_data = messages.model_dump()
        assistant_msgs = [
            m for m in messages_data.get("data", []) if m.get("role") == "assistant"
        ]
        answer_text = ""
        if assistant_msgs:
            content = assistant_msgs[-1].get("content", [])
            if content:
                c0 = content[0]
                if isinstance(c0, dict) and "text" in c0:
                    txt = c0["text"]
                    answer_text = (
                        txt["value"]
                        if isinstance(txt, dict) and "value" in txt
                        else str(txt)
                    )
                else:
                    answer_text = str(c0)

            if answer_text:
                text_preview = self._extract_data_from_text(answer_text)
                if text_preview and sql_analysis["queries"]:
                    if not sql_analysis["data_previews"] or not any(
                        sql_analysis["data_previews"]
                    ):
                        sql_analysis["data_previews"] = [text_preview]
                    if (
                        not sql_analysis["data_retrieval_query"]
                        and sql_analysis["queries"]
                    ):
                        sql_analysis["data_retrieval_query"] = sql_analysis["queries"][
                            0
                        ]

        # Cleanup
        try:
            client.beta.threads.delete(thread_id=thread["id"])
        except Exception:
            pass

        result: Dict[str, Any] = {
            "question": question,
            "answer": answer_text,
            "run_status": run.status,
            "run_steps": steps.model_dump(),
            "messages": messages_data,
            "timestamp": time.time(),
            "thread": {"id": thread["id"], "name": thread.get("name", "")},
        }

        if sql_analysis["queries"]:
            result["sql_queries"] = sql_analysis["queries"]
            result["sql_data_previews"] = sql_analysis["data_previews"]
            result["data_retrieval_query"] = sql_analysis["data_retrieval_query"]

        return result

    # ── Core: get_raw_run_response ────────────────────────────────────────────

    def get_raw_run_response(
        self,
        question: str,
        timeout: int = 120,
        thread_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Ask a question, return the full raw response dict."""
        if not question.strip():
            raise ValueError("Question cannot be empty")

        client = self._get_openai_client()
        assistant = client.beta.assistants.create(model="not used")
        thread = self.get_or_create_thread(thread_name=thread_name)

        client.beta.threads.messages.create(
            thread_id=thread["id"], role="user", content=question
        )
        run = client.beta.threads.runs.create(
            thread_id=thread["id"], assistant_id=assistant.id
        )

        start = time.time()
        while run.status in ("queued", "in_progress"):
            if time.time() - start > timeout:
                break
            time.sleep(2)
            run = client.beta.threads.runs.retrieve(
                thread_id=thread["id"], run_id=run.id
            )

        steps = client.beta.threads.runs.steps.list(
            thread_id=thread["id"], run_id=run.id
        )
        messages = client.beta.threads.messages.list(
            thread_id=thread["id"], order="desc"
        )

        # Cleanup
        try:
            client.beta.threads.delete(thread_id=thread["id"])
        except Exception:
            pass

        return {
            "question": question,
            "run": run.model_dump(),
            "steps": steps.model_dump(),
            "messages": messages.model_dump(),
            "timestamp": time.time(),
            "timeout": timeout,
            "success": run.status == "completed",
            "thread": {"id": thread["id"], "name": thread.get("name", "")},
        }

    # ── Draft vs Production comparison ────────────────────────────────────────

    def compare_draft_vs_production(
        self,
        question: str,
        draft_agent_id: str,
        production_agent_id: str,
        timeout: int = 120,
    ) -> Dict[str, Any]:
        """Query both a draft and production agent with the same question and
        return a side-by-side comparison.

        Parameters
        ----------
        question : str
            The question to send to both agents.
        draft_agent_id : str
            The Fabric Data Agent item GUID for the *draft* agent.
        production_agent_id : str
            The Fabric Data Agent item GUID for the *production* agent.
        timeout : int
            Per-agent query timeout in seconds (default 120).

        Returns
        -------
        dict
            ``{"question", "draft", "production", "match", "timestamp"}``
        """
        saved_id = self.agent_id

        results: Dict[str, Any] = {"question": question, "timestamp": time.time()}

        for label, aid in [
            ("draft", draft_agent_id),
            ("production", production_agent_id),
        ]:
            self.agent_id = aid
            # Rebuild the URL for the swapped agent
            self._data_agent_url = (
                f"{FABRIC_BASE_URL}/v1/workspaces/{self.workspace_id}"
                f"/dataAgents/{aid}/aiassistant/openai"
            )
            try:
                details = self.get_run_details(question, timeout=timeout)
                results[label] = {
                    "answer": details.get("answer", ""),
                    "run_status": details.get("run_status", ""),
                    "sql_queries": details.get("sql_queries", []),
                    "data_previews": details.get("sql_data_previews", []),
                    "error": None,
                }
            except Exception as exc:
                logger.warning("compare %s agent %s failed: %s", label, aid, exc)
                results[label] = {
                    "answer": "",
                    "run_status": "failed",
                    "sql_queries": [],
                    "data_previews": [],
                    "error": str(exc),
                }

        # Restore the original agent
        self.agent_id = saved_id
        self._data_agent_url = (
            f"{FABRIC_BASE_URL}/v1/workspaces/{self.workspace_id}"
            f"/dataAgents/{saved_id}/aiassistant/openai"
        )

        draft_answer = results.get("draft", {}).get("answer", "")
        prod_answer = results.get("production", {}).get("answer", "")
        results["match"] = draft_answer.strip() == prod_answer.strip()
        return results

    # ── SQL extraction helpers ────────────────────────────────────────────────

    def _extract_sql_from_steps(self, steps: Any) -> Dict[str, Any]:
        """Extract SQL queries and data previews from run steps."""
        queries: List[str] = []
        previews: List[List[str]] = []
        retrieval_query: Optional[str] = None

        try:
            for step in steps.data:
                if not hasattr(step, "step_details") or not step.step_details:
                    continue
                sd = step.step_details
                if not hasattr(sd, "tool_calls") or not sd.tool_calls:
                    continue

                for tc in sd.tool_calls:
                    sql_from_args = self._sql_from_function_args(tc)
                    sql_from_out = self._sql_from_output(tc)
                    found = sql_from_args + sql_from_out
                    queries.extend(found)

                    data_preview = self._data_from_output(tc)
                    previews.append(data_preview)

                    if data_preview and found:
                        retrieval_query = found[-1]
        except Exception as exc:
            logger.warning("SQL extraction error: %s", exc)

        unique = list(dict.fromkeys(queries))
        return {
            "queries": unique,
            "data_previews": previews,
            "data_retrieval_query": retrieval_query,
        }

    @staticmethod
    def _sql_from_function_args(tool_call: Any) -> List[str]:
        results: List[str] = []
        try:
            if not hasattr(tool_call, "function") or not tool_call.function:
                return results
            args_str = getattr(tool_call.function, "arguments", "")
            if not args_str:
                return results
            args = json.loads(args_str)
            if not isinstance(args, dict):
                return results
            for key in ("sql", "query", "sql_query", "statement", "command", "code"):
                val = args.get(key)
                if val and len(str(val).strip()) > 10:
                    results.append(str(val).strip())
        except (json.JSONDecodeError, AttributeError):
            pass
        return results

    @staticmethod
    def _sql_from_output(tool_call: Any) -> List[str]:
        results: List[str] = []
        try:
            output = getattr(tool_call, "output", None)
            if not output:
                return results
            output_str = str(output)
            try:
                data = json.loads(output_str)
                if isinstance(data, dict):
                    for key in (
                        "sql",
                        "query",
                        "sql_query",
                        "statement",
                        "generated_code",
                    ):
                        val = data.get(key)
                        if val and len(str(val).strip()) > 10:
                            results.append(str(val).strip())
            except json.JSONDecodeError:
                pass

            # Regex fallback
            if any(
                kw in output_str.upper()
                for kw in ("SELECT", "INSERT", "UPDATE", "DELETE")
            ):
                patterns = [
                    r"(SELECT\s+.*?FROM\s+.*?)(?=\s*[;}\"\'\n]|\s*$)",
                ]
                for pat in patterns:
                    for m in re.findall(pat, output_str, re.IGNORECASE | re.DOTALL):
                        clean = re.sub(r"\s+", " ", m.strip())
                        if len(clean) > 10:
                            results.append(clean)
        except Exception:
            pass
        return results

    @staticmethod
    def _data_from_output(tool_call: Any) -> List[str]:
        """Extract structured data preview from tool-call output."""
        lines: List[str] = []
        try:
            output = getattr(tool_call, "output", None)
            if not output:
                return lines
            data = json.loads(str(output))
            if isinstance(data, list) and data and isinstance(data[0], dict):
                headers = list(data[0].keys())
                lines.append("| " + " | ".join(headers) + " |")
                lines.append("|" + "---|" * len(headers))
                for row in data[:10]:
                    vals = [str(row.get(h, "")) for h in headers]
                    lines.append("| " + " | ".join(vals) + " |")
        except (json.JSONDecodeError, Exception):
            pass
        return lines

    @staticmethod
    def _extract_data_from_text(text: str) -> List[str]:
        """Pull a markdown table or numbered-list data from the text response."""
        # Markdown table extraction
        table_lines: List[str] = []
        in_table = False
        for line in text.split("\n"):
            stripped = line.strip()
            if "|" in stripped and (
                "---" in stripped or stripped.count("-") > 3
            ):
                table_lines.append(line)
                in_table = True
            elif "|" in stripped and in_table:
                table_lines.append(line)
            elif in_table and stripped == "":
                table_lines.append(line)
            elif in_table and "|" not in stripped and stripped:
                break

        while table_lines and not table_lines[-1].strip():
            table_lines.pop()
        if len(table_lines) >= 2:
            return ["\n".join(table_lines)]

        # Fallback: numbered lists
        numbered = re.findall(r"^\d+\.\s+(.+)", text, re.MULTILINE)
        if numbered:
            return [f"Row {i + 1}: {r}" for i, r in enumerate(numbered)]
        return []
