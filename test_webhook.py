#!/usr/bin/env python3
"""
Test script for the AM artifact webhook.

Sends a simulated GitHub workflow_run event to the webhook app.

Usage examples:

  # Test signature rejection (no secret):
  python test_webhook.py --url http://localhost:5000

  # Test with a real workflow run ID and commit SHA:
  python test_webhook.py \\
    --url http://localhost:5000 \\
    --secret YOUR_WEBHOOK_SECRET \\
    --run-id 12345678 \\
    --head-sha abc123def456...

  # Dry run: print the request without sending:
  python test_webhook.py --dry-run --secret mysecret --run-id 123
"""

import argparse
import hashlib
import hmac
import json
import sys

try:
    import requests
except ImportError:
    sys.exit("requests is required: pip install requests")


def build_workflow_run_payload(run_id: int, head_sha: str) -> dict:
    return {
        "action": "completed",
        "workflow_run": {
            "id": run_id,
            "name": "Build master branch",
            "head_sha": head_sha,
            "conclusion": "success",
            "status": "completed",
        },
    }


def sign_payload(body: bytes, secret: str) -> str:
    sig = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return f"sha256={sig}"


def main():
    parser = argparse.ArgumentParser(
        description="Test the AM artifact webhook endpoint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--url",
        default="http://localhost:5000",
        help="Base URL of the webhook app (default: http://localhost:5000)",
    )
    parser.add_argument(
        "--secret",
        default=None,
        help="Webhook secret for HMAC-SHA256 signing (omit to test auth rejection)",
    )
    parser.add_argument(
        "--run-id",
        type=int,
        default=99999999,
        help="GitHub Actions workflow run ID (default: 99999999)",
    )
    parser.add_argument(
        "--head-sha",
        default="0000000000000000000000000000000000000000",
        help="Git commit SHA for the run",
    )
    parser.add_argument(
        "--event",
        default="workflow_run",
        help="X-GitHub-Event header value (default: workflow_run)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the request details without sending",
    )
    args = parser.parse_args()

    endpoint = args.url.rstrip("/") + "/webhook"
    payload = build_workflow_run_payload(args.run_id, args.head_sha)
    body = json.dumps(payload).encode()
    headers = {
        "Content-Type": "application/json",
        "X-GitHub-Event": args.event,
    }
    if args.secret:
        headers["X-Hub-Signature-256"] = sign_payload(body, args.secret)

    print("=== Request ===")
    print(f"POST {endpoint}")
    for k, v in headers.items():
        display = v[:10] + "****" if k == "X-Hub-Signature-256" else v
        print(f"  {k}: {display}")
    print(f"  Body: {json.dumps(payload, indent=2)}")

    if args.dry_run:
        print("\n[dry-run] Not sending.")
        return

    print("\n=== Response ===")
    try:
        resp = requests.post(endpoint, data=body, headers=headers, timeout=15)
        print(f"  Status: {resp.status_code}")
        try:
            print(f"  Body:   {json.dumps(resp.json(), indent=2)}")
        except Exception:
            print(f"  Body:   {resp.text}")

        if resp.status_code == 202:
            print("\n✓ Accepted — processing started in background on server.")
            print("  Check server logs to confirm artifact download/symstore success.")
        elif resp.status_code == 401:
            print("\n✗ Auth rejected (expected if testing without a secret).")
        else:
            print(f"\n✗ Unexpected status {resp.status_code}.")
            sys.exit(1)

    except requests.exceptions.ConnectionError:
        print(f"  Connection refused — is the app running at {args.url}?")
        sys.exit(1)
    except requests.exceptions.Timeout:
        print("  Request timed out.")
        sys.exit(1)


if __name__ == "__main__":
    main()
