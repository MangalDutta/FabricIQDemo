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
    retries: int = 3,
    retry_wait: int = 15,
) -> Tuple[bool, str]:
    url = f"{backend_url.rstrip('/')}/api/chat"
    last_detail = ""
    for attempt in range(1, retries + 1):
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
            # Try to get the actual detail from the response body
            try:
                detail = resp.json().get("detail", "")
                if isinstance(detail, dict):
                    detail = detail.get("message", str(detail))
            except Exception:
                detail = resp.text[:200]
            last_detail = f"503 – {detail or 'Backend not ready (Fabric agent may not be configured yet)'}"
        else:
            last_detail = f"HTTP {resp.status_code} – {resp.text[:200]}"
        # Retry on 503 (agent warming up) — give the Fabric Data Agent time
        # to become fully queryable after deployment/publish.
        if attempt < retries and resp.status_code in (503, 502, 504):
            print(f"    ↻ attempt {attempt}/{retries} failed ({resp.status_code}), retrying in {retry_wait}s…")
            time.sleep(retry_wait)
    return False, last_detail


def check_cors_headers(backend_url: str, frontend_url: str, timeout: int) -> Tuple[bool, str]:
    """
    Test CORS by sending an OPTIONS preflight with the frontend's actual origin.
    Using http://localhost:5173 would always fail when the backend is configured
    to only allow the real frontend URL.
    """
    # Use the real frontend URL as Origin (matches the CORS_ALLOWED_ORIGINS setting).
    # Fall back to localhost only if no frontend URL was supplied.
    origin = frontend_url.rstrip("/") if frontend_url else "http://localhost:5173"

    url = f"{backend_url.rstrip('/')}/health"
    resp = requests.options(
        url,
        headers={
            "Origin": origin,
            "Access-Control-Request-Method": "GET",
        },
        timeout=timeout,
    )
    cors = resp.headers.get("Access-Control-Allow-Origin", "")
    if cors:
        return True, f"CORS origin header: {cors}  (tested origin: {origin})"
    return False, (
        f"No Access-Control-Allow-Origin header found  "
        f"(tested origin: {origin}, HTTP {resp.status_code})"
    )


def wait_for_backend(backend_url: str, timeout: int, max_wait: int) -> bool:
    """
    Poll /health until it returns 200 or max_wait seconds have elapsed.
    Returns True if the backend became healthy within the time limit.
    Replaces a blind time.sleep() that often finishes before the App Service
    container has actually restarted with updated env vars.
    """
    url = f"{backend_url.rstrip('/')}/health"
    deadline = time.time() + max_wait
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.status_code == 200:
                elapsed = int(deadline - max_wait - time.time() + max_wait)
                print(f"  Backend healthy after ~{attempt * 5}s (attempt {attempt})")
                return True
        except Exception:
            pass
        remaining = int(deadline - time.time())
        if remaining > 0:
            print(f"  Not ready yet — retrying ({remaining}s remaining)…")
            time.sleep(5)
    return False


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
        help=(
            "Maximum seconds to poll /health waiting for App Service warm-up "
            "(default 0 = no wait).  Replaces a blind sleep — polling stops "
            "as soon as the backend responds 200."
        ),
    )
    args = parser.parse_args()

    if args.wait_for_warmup > 0:
        print(f"⏳ Waiting up to {args.wait_for_warmup}s for App Service warm-up (polling /health)…")
        ready = wait_for_backend(args.backend, timeout=10, max_wait=args.wait_for_warmup)
        if not ready:
            print(f"  {WARN} Backend did not respond within {args.wait_for_warmup}s — running checks anyway")

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
        lambda: check_cors_headers(args.backend, args.frontend, args.timeout),
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
