#!/usr/bin/env python3
"""
Post-Deployment Smoke Test
───────────────────────────
Quick end-to-end check that the deployed Customer360 app is healthy
and the chat API is responding.

Usage:
    python scripts/smoke_test.py \
        --backend  https://app-cust360-backend-dev.azurewebsites.net \
        --frontend https://app-cust360-frontend-dev.azurewebsites.net \
        [--chat-message "Top 5 customers by LifetimeValue"] \
        [--timeout 30]

Exit codes:
    0  – all checks passed
    1  – one or more checks failed
"""

import argparse
import json
import sys
import time
from typing import List, Tuple

import requests

PASS = "✅"
FAIL = "❌"
WARN = "⚠️ "


def _check(name: str, fn, results: List[Tuple[bool, str]]) -> bool:
    """Run a check, print result, append to results list."""
    try:
        ok, detail = fn()
    except Exception as exc:
        ok, detail = False, f"Exception: {exc}"
    symbol = PASS if ok else FAIL
    print(f"  {symbol}  {name}: {detail}")
    results.append((ok, name))
    return ok


# ─── Individual checks ───────────────────────────────────────────────────────

def check_backend_health(backend_url: str, timeout: int) -> Tuple[bool, str]:
    url = f"{backend_url.rstrip('/')}/health"
    resp = requests.get(url, timeout=timeout)
    if resp.status_code == 200:
        data = resp.json()
        return True, f"HTTP 200 – {data}"
    return False, f"HTTP {resp.status_code} – {resp.text[:100]}"


def check_backend_root(backend_url: str, timeout: int) -> Tuple[bool, str]:
    url = f"{backend_url.rstrip('/')}/"
    resp = requests.get(url, timeout=timeout)
    if resp.status_code == 200:
        data = resp.json()
        service = data.get("service", "?")
        return True, f"service={service}"
    return False, f"HTTP {resp.status_code}"


def check_frontend_reachable(frontend_url: str, timeout: int) -> Tuple[bool, str]:
    resp = requests.get(frontend_url, timeout=timeout)
    if resp.status_code == 200:
        content_type = resp.headers.get("Content-Type", "")
        return True, f"HTTP 200  Content-Type: {content_type}"
    return False, f"HTTP {resp.status_code}"


def check_chat_api(
    backend_url: str,
    message: str,
    timeout: int,
) -> Tuple[bool, str]:
    url = f"{backend_url.rstrip('/')}/api/chat"
    resp = requests.post(
        url,
        json={"message": message, "userId": "smoke-test"},
        timeout=timeout,
    )
    if resp.status_code == 200:
        data = resp.json()
        answer = data.get("answer", "")
        preview = answer[:120] + ("…" if len(answer) > 120 else "")
        return True, f'Got answer ({len(answer)} chars): "{preview}"'
    if resp.status_code == 503:
        return (
            False,
            "503 – Fabric Data Agent not configured yet. "
            "Set FABRIC_WORKSPACE_ID + FABRIC_DATAAGENT_ID "
            "in App Service settings.",
        )
    return False, f"HTTP {resp.status_code} – {resp.text[:200]}"


def check_cors_headers(backend_url: str, timeout: int) -> Tuple[bool, str]:
    url = f"{backend_url.rstrip('/')}/health"
    resp = requests.options(
        url,
        headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "GET",
        },
        timeout=timeout,
    )
    cors = resp.headers.get("Access-Control-Allow-Origin", "")
    if cors:
        return True, f"CORS origin header: {cors}"
    return False, "No Access-Control-Allow-Origin header found"


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Customer360 post-deployment smoke test"
    )
    parser.add_argument(
        "--backend",
        required=True,
        help="Backend App Service URL (e.g. https://app-cust360-backend-dev.azurewebsites.net)",
    )
    parser.add_argument(
        "--frontend",
        required=False,
        default="",
        help="Frontend App Service URL (optional)",
    )
    parser.add_argument(
        "--chat-message",
        default="Top 5 customers by LifetimeValue",
        help="Sample query to send to /api/chat",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP request timeout in seconds (default 30)",
    )
    parser.add_argument(
        "--wait-for-warmup",
        type=int,
        default=0,
        help="Seconds to wait before starting tests (App Service cold-start)",
    )
    args = parser.parse_args()

    if args.wait_for_warmup > 0:
        print(f"⏳ Waiting {args.wait_for_warmup}s for App Service warm-up...")
        time.sleep(args.wait_for_warmup)

    print("=" * 62)
    print("🔍 Customer360 Smoke Test")
    print("=" * 62)
    print(f"  Backend  : {args.backend}")
    if args.frontend:
        print(f"  Frontend : {args.frontend}")
    print()

    results: List[Tuple[bool, str]] = []

    # ── Backend checks ────────────────────────────────────────────────────
    print("── Backend ─────────────────────────────────────────────────")
    _check(
        "GET /health",
        lambda: check_backend_health(args.backend, args.timeout),
        results,
    )
    _check(
        "GET / (root info)",
        lambda: check_backend_root(args.backend, args.timeout),
        results,
    )
    _check(
        "CORS headers",
        lambda: check_cors_headers(args.backend, args.timeout),
        results,
    )

    # ── Chat API check ────────────────────────────────────────────────────
    print()
    print("── Chat API ─────────────────────────────────────────────────")
    _check(
        f'POST /api/chat  "{args.chat_message}"',
        lambda: check_chat_api(args.backend, args.chat_message, args.timeout),
        results,
    )

    # ── Frontend check ────────────────────────────────────────────────────
    if args.frontend:
        print()
        print("── Frontend ─────────────────────────────────────────────────")
        _check(
            "GET / (index.html)",
            lambda: check_frontend_reachable(args.frontend, args.timeout),
            results,
        )

    # ── Summary ───────────────────────────────────────────────────────────
    passed = sum(1 for ok, _ in results if ok)
    failed = len(results) - passed

    print()
    print("=" * 62)
    print(f"  Results: {passed} passed  /  {failed} failed  (total {len(results)})")
    print("=" * 62)

    if failed > 0:
        print("\nFailed checks:")
        for ok, name in results:
            if not ok:
                print(f"  {FAIL}  {name}")
        sys.exit(1)
    else:
        print(f"\n{PASS}  All checks passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()
