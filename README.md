# GitHub Artifact Webhook Server

A FastAPI-based server that listens for GitHub `workflow_run` events and automatically
processes PDB symbol files into a Microsoft-compatible symbol store.

When a CI workflow completes successfully, GitHub POSTs a `workflow_run` event here.
The server finds the `pdbs` artifact for the run, downloads it using a configured PAT,
extracts the PDBs into the symbol store, and upserts a build record into the downloads DB.

An hourly APScheduler job reconciles any builds that were missed (e.g. while the server
was down).

## Setup

1. Install [uv](https://docs.astral.sh/uv/), then install dependencies:
```bash
uv sync
```

2. Configure the application by editing `config.yaml` (see [Configuration](#configuration)).

3. Register a GitHub webhook on your repo:
   - **Payload URL**: your server's `/webhook` endpoint
   - **Content type**: `application/json`
   - **Events**: Workflow runs
   - **Secret**: the value you set in `github.webhook_secret`

## Symbol Store Structure

```
/data/symbols/
  └── sourcemod/      # repo.product_name (must be pre-created by an admin)
        └── ...       # Microsoft symstore layout
```

The product subdirectory must exist before the server starts processing builds.
The server will not create it automatically.

## Configuration

The server is configured via `config.yaml`. Any value can be overridden with an
environment variable using the `SECTION__KEY` pattern (double underscore):

```yaml
api:
  host: "0.0.0.0"    # env: API__HOST
  port: 5000         # env: API__PORT

storage:
  symbol_store_base_path: "/data/symbols"  # env: STORAGE__SYMBOL_STORE_BASE_PATH

github:
  webhook_secret: "..."   # Required; env: GITHUB__WEBHOOK_SECRET
  token: "ghp_..."        # PAT for downloading Actions artifacts; env: GITHUB__TOKEN
  retry_attempts: 3
  retry_delay: 5

database:
  host: "db-host"
  port: 3306
  user: "sm_commit_update"
  password: "..."         # env: DATABASE__PASSWORD
  name: "sourcemod"

repo:
  owner: "alliedmodders"
  name: "sourcemod"
  product_name: "sourcemod"   # Subdirectory under symbol_store_base_path

log:
  level: "INFO"   # env: LOG__LEVEL
```

`storage` is required. `database` and `repo` are optional — omitting them disables
DB upsert and reconciliation.

## Running the Server

```bash
uv run python app.py
```

Or directly via uvicorn:

```bash
uv run uvicorn app:app --host 0.0.0.0 --port 5000
```

## API

### GET /health

Returns `{"status": "ok"}`. Used for monitoring and deployment verification.

### POST /webhook

Receives GitHub webhook events. All requests must include a valid
`X-Hub-Signature-256` header (HMAC-SHA256 of the body using `github.webhook_secret`).

**Handled events:**

- `workflow_run` (`action: completed`, `conclusion: success`): downloads the `pdbs`
  artifact for the run and processes it into the symbol store.
- `ping`: returns `{"message": "pong", ...}`.

All other events return `202` with an "unhandled event" message.

## Security

- **Authentication**: HMAC-SHA256 signature on every request (`X-Hub-Signature-256`).
  The server returns `500` at startup if `webhook_secret` is not configured.
- **Path traversal**: product names are validated (alphanumeric, hyphens, underscores only);
  all resolved paths are checked to remain within their base directories.
- **Zip slip**: ZIP entries are validated before extraction.
- **Temp files**: created with mode `0o700` and deleted after use.

## Local Development and Testing

Use `test_webhook.py` to send a simulated `workflow_run` event:

```bash
# Test signature rejection (no secret):
python test_webhook.py --url http://localhost:5000

# Send a real run ID and commit SHA:
python test_webhook.py \
  --url http://localhost:5000 \
  --secret YOUR_WEBHOOK_SECRET \
  --run-id 12345678 \
  --head-sha abc123...

# Dry run (print request without sending):
python test_webhook.py --dry-run --secret mysecret --run-id 123
```

## Logging

The application logs to stdout. Log level is configured via `config.yaml` (`log.level`)
or the `LOG__LEVEL` environment variable.
