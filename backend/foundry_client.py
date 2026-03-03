import os
from typing import Dict, Any
import requests
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

class FoundryClient:
    def __init__(self) -> None:
        self.project_endpoint = os.environ.get("AZURE_AI_FOUNDRY_PROJECT_ENDPOINT", "")
        self.api_version = os.environ.get("AZURE_AI_FOUNDRY_API_VERSION", "2025-05-15-preview")
        self.agent_id = os.environ.get("AZURE_AI_FOUNDRY_AGENT_ID", "")
        self.fabric_connection_id = os.environ.get("FABRIC_CONNECTION_ID", "")

        keyvault_url = os.environ.get("KEYVAULT_URL", "")
        if keyvault_url:
            credential = DefaultAzureCredential()
            self.kv_client = SecretClient(vault_url=keyvault_url, credential=credential)
        else:
            self.kv_client = None

    def _get_agent_token(self) -> str:
        if not self.kv_client:
            return os.environ.get("AGENT_TOKEN", "")
        try:
            secret = self.kv_client.get_secret("AGENT-TOKEN")
            return secret.value
        except Exception as ex:
            raise RuntimeError(f"Failed to get agent token from Key Vault: {ex}")

    def chat(self, user_id: str, message: str) -> Dict[str, Any]:
        if not self.project_endpoint or not self.agent_id:
            raise ValueError("Azure AI Foundry configuration missing")

        token = self._get_agent_token()
        if not token:
            raise ValueError("Agent token not found")

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        url = f"{self.project_endpoint}/agents/{self.agent_id}:chat"
        params = {"api-version": self.api_version}

        payload = {
            "messages": [{"role": "user", "content": message, "user": user_id}],
            "tools": [{"type": "microsoft.fabric", "connection_id": self.fabric_connection_id}],
            "tool_choice": "auto"
        }

        resp = requests.post(url, headers=headers, params=params, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        answer = ""
        if "choices" in data and len(data["choices"]) > 0:
            answer = data["choices"][0].get("message", {}).get("content", "")
        elif "answer" in data:
            answer = data["answer"]

        return {
            "answer": answer,
            "timestamp": data.get("created"),
            "metadata": {"agent_id": self.agent_id, "model": data.get("model")}
        }
