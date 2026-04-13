import re
import os
import hmac
import hashlib
import json
import logging
import threading
import tempfile
import zipfile
import shutil
from contextlib import asynccontextmanager
from pathlib import Path

import requests
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, Request, Header, HTTPException, Depends
from pydantic import BaseModel, Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)
from symstore import Store

from releases import GitHubReleasesClient
from reconciler import reconcile, upsert_from_release


class ApiSettings(BaseModel):
    host: str = "0.0.0.0"
    port: int = 5000


class StorageSettings(BaseModel):
    symbol_store_base_path: str
    # If set, release build archives (Windows zip, Linux tar.gz) are downloaded
    # from GitHub Releases and stored at {build_drop_base_path}/{version}/{filename}
    # as a local backup mirror alongside legacy FTP builds.
    build_drop_base_path: str | None = None


class GithubSettings(BaseModel):
    webhook_secret: str | None = None
    # Default token for authenticating GitHub artifact downloads. Can be overridden
    # per-request via the github_token field in the webhook payload.
    token: str | None = None
    retry_attempts: int = 3
    retry_delay: int = 5


class DatabaseSettings(BaseModel):
    host: str = "localhost"
    port: int = 3306
    user: str
    password: str
    name: str = "sourcemod"
    commit_log_table: str = "sm_commit_log"


class RepoSettings(BaseModel):
    owner: str
    name: str
    product_name: str = "sourcemod"
    # Path of the workflow file that produces PDB symbols.
    # Only workflow_run events for this path will be processed.
    workflow_path: str = ".github/workflows/build-release.yml"
    # Maps version prefix ("1.13") → sm_commit_log branch value ("master").
    # Add a new entry whenever a new stable branch is cut.
    version_branches: dict[str, str] = {
        "1.10": "1.10-dev",
        "1.11": "1.11-dev",
        "1.12": "1.12-dev",
        "1.13": "master",
    }
    # Reconciler will not retry incomplete releases older than this many days.
    # Prevents stale broken releases from blocking pagination indefinitely.
    # Set to null to disable.
    reconcile_max_age_days: int | None = 90
    # When set, only release assets whose name contains this string are
    # matched for the windows_url / linux_url columns.  Useful for
    # multi-package releases (e.g. set to "base" to match only the base
    # package archive).
    asset_match_filter: str | None = None

    @property
    def full_name(self) -> str:
        return f"{self.owner}/{self.name}"


class LogSettings(BaseModel):
    level: str = "INFO"


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(
        yaml_file="config.yaml",
        env_nested_delimiter="__",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (init_settings, env_settings, YamlConfigSettingsSource(settings_cls))

    api: ApiSettings = Field(default_factory=ApiSettings)
    storage: StorageSettings | None = None
    github: GithubSettings = Field(default_factory=GithubSettings)
    database: DatabaseSettings | None = None
    repo: RepoSettings | None = None
    log: LogSettings = Field(default_factory=LogSettings)


config = AppConfig()  # ty: ignore[missing-argument]

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logging.getLogger().setLevel(config.log.level)

# Shared GitHub releases client — only instantiated when repo is configured.
_releases_client: GitHubReleasesClient | None = None
if config.repo:
    _releases_client = GitHubReleasesClient(
        repo=config.repo.full_name,
        token=config.github.token,
    )

# Serializes symstore writes to prevent transaction ID corruption on concurrent runs.
_storage_lock = threading.Lock()


def _run_reconcile():
    if not config.database or not config.repo or not _releases_client:
        return
    try:
        reconcile(
            _releases_client,
            config.database,
            config.repo.version_branches,
            drop_base_path=config.storage.build_drop_base_path
            if config.storage
            else None,
            process_symbols_fn=_process_pdb_artifact_for_sha
            if config.storage
            else None,
            download_fn=lambda url, path: download_file(url, path),
            product_name=config.repo.product_name,
            max_age_days=config.repo.reconcile_max_age_days,
            commit_log_table=config.database.commit_log_table,
            asset_match_filter=config.repo.asset_match_filter,
        )
    except Exception:
        logger.exception("Scheduled reconciliation failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler: BackgroundScheduler | None = None
    if config.database and config.repo:
        scheduler = BackgroundScheduler()
        scheduler.add_job(
            _run_reconcile,
            "interval",
            hours=1,
            id="reconcile",
            max_instances=1,
            coalesce=True,
        )
        scheduler.start()
        logger.info("Reconciliation scheduler started (interval: 1 hour)")
        # Run an initial reconciliation in a background thread so startup is non-blocking.
        threading.Thread(
            target=_run_reconcile, daemon=True, name="reconcile-startup"
        ).start()
    else:
        logger.info("database/repo not configured; DB reconciliation disabled")
    yield
    if scheduler:
        scheduler.shutdown(wait=False)


app = FastAPI(lifespan=lifespan)


async def verify_github_signature(request: Request):
    """Dependency to verify GitHub webhook HMAC-SHA256 signature."""
    webhook_secret = config.github.webhook_secret
    if not webhook_secret:
        raise HTTPException(status_code=500, detail="Webhook secret not configured")

    signature = request.headers.get("x-hub-signature-256")
    if not signature:
        raise HTTPException(
            status_code=401, detail="Missing X-Hub-Signature-256 header"
        )

    try:
        body = await request.body()
        expected = (
            "sha256="
            + hmac.new(webhook_secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
        )
        if not hmac.compare_digest(signature, expected):
            raise HTTPException(status_code=401, detail="Invalid signature")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Signature verification failed: {e}")
        raise HTTPException(status_code=401, detail="Signature verification failed")


def download_file(url: str, target_path: Path, headers: dict | None = None) -> Path:
    """Download a file from a URL to a target path."""
    attempts = config.github.retry_attempts
    for attempt in range(attempts):
        try:
            response = requests.get(url, stream=True, headers=headers or {})
            response.raise_for_status()

            target_path.parent.mkdir(parents=True, exist_ok=True)

            with open(target_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return target_path
        except Exception as e:
            logger.error(f"Download attempt {attempt + 1} failed: {e}")
            if attempt == attempts - 1:
                raise
    raise RuntimeError("download_file called with retry_attempts=0")


def _process_symbols_only(
    symbols_url: str,
    safe_version: str,
    product_name: str,
    auth_headers: dict,
) -> None:
    """Download PDB zip, extract, and commit to symstore. Serialized via _storage_lock."""
    base_symbols_path = config.storage.symbol_store_base_path

    product_symbols_path = Path(base_symbols_path).resolve()

    if not product_symbols_path.is_dir():
        raise ValueError(
            f"Symbol store directory does not exist: {product_symbols_path}. "
            "The directory must be created by an administrator."
        )

    temp_dir = Path(tempfile.mkdtemp(prefix="symbols_"))
    temp_dir.chmod(0o700)

    try:
        with tempfile.NamedTemporaryFile(
            dir=temp_dir, suffix=".zip", delete=False
        ) as temp_file:
            temp_path = Path(temp_file.name)
        download_file(symbols_url, temp_path, auth_headers)
        logger.info(f"Symbols downloaded to temporary file: {temp_path}")

        extract_dir = temp_dir / "extracted"
        extract_dir.mkdir(mode=0o700)

        with zipfile.ZipFile(temp_path, "r") as zip_ref:
            # Check for zip slip vulnerability
            for zip_entry in zip_ref.namelist():
                target = (extract_dir / zip_entry).resolve()
                if not target.is_relative_to(extract_dir.resolve()):
                    raise ValueError(f"Zip slip detected: {zip_entry}")
            zip_ref.extractall(extract_dir)

        with _storage_lock:
            store = Store(str(product_symbols_path))
            transaction = store.new_transaction(
                product_name, safe_version, comment=safe_version
            )
            for file_path in extract_dir.rglob("*"):
                if file_path.is_file():
                    entry = transaction.new_entry(str(file_path))
                    transaction.add_entry(entry)

            for entry in transaction.entries:
                logger.info(
                    "Symbol entry: %s/%s (source: %s)",
                    entry.file_name,
                    entry.file_hash,
                    entry.source_file,
                )

            logger.debug(
                "Publishing %d symbol %s for %s",
                len(transaction.entries),
                "entry" if len(transaction.entries) == 1 else "entries",
                safe_version,
            )

            # Publish entries serially so exceptions surface with full tracebacks.
            # The symstore library uses ThreadPoolExecutor.map() without consuming
            # the result iterator, which silently swallows exceptions from publish().
            for entry in transaction.entries:
                entry.publish()

            store.commit(transaction)

            missing = [e for e in transaction.entries if not e.exists()]
            if missing:
                raise RuntimeError(
                    f"Symbol store commit completed but {len(missing)} entr"
                    f"{'y' if len(missing) == 1 else 'ies'} missing on disk: "
                    + ", ".join(f"{e.file_name}/{e.file_hash}" for e in missing)
                )
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    logger.info("Symbols processed and stored in %s", product_symbols_path)


def _process_pdb_artifact_for_sha(sha: str, product_name: str) -> None:
    """
    For reconciler use: find the completed workflow run for a commit SHA,
    download the PDB artifact, and process into symstore.
    Artifacts expire after ~90 days; logs a warning and returns if unavailable.
    """
    if not _releases_client or not config.repo or not config.storage:
        return

    run = _releases_client.find_workflow_run_for_commit(sha, config.repo.workflow_path)
    if not run:
        logger.info(
            "No completed workflow run found for commit %s; symbols skipped", sha[:12]
        )
        return

    run_id = run["id"]
    try:
        artifacts = _releases_client.list_run_artifacts(run_id)
    except Exception:
        logger.warning(
            "Could not list artifacts for run %s (may have expired); symbols skipped",
            run_id,
            exc_info=True,
        )
        return

    pdb_artifact = next((a for a in artifacts if a["name"] == "pdbs"), None)
    if not pdb_artifact:
        logger.info(
            "No 'pdbs' artifact for run %s (may have expired); symbols skipped", run_id
        )
        return

    artifact_id = pdb_artifact["id"]
    symbols_url = f"https://api.github.com/repos/{config.repo.full_name}/actions/artifacts/{artifact_id}/zip"
    token = config.github.token
    auth_headers = {"Authorization": f"Bearer {token}"} if token else {}
    safe_version = re.sub(r"[^\w.-]", "_", sha[:12])

    try:
        _process_symbols_only(symbols_url, safe_version, product_name, auth_headers)
    except Exception:
        logger.warning(
            "Symbol processing failed for commit %s (artifact may have expired)",
            sha[:12],
            exc_info=True,
        )


def process_artifacts(
    symbols_url,
    build_version,
    product_name,
    github_token=None,
):
    """Process PDB symbols into the symbol store and upsert the DB build record."""
    try:
        safe_version = re.sub(r"[^\w.-]", "_", build_version)

        token = github_token or config.github.token
        auth_headers = {"Authorization": f"Bearer {token}"} if token else {}

        # Download and process symbol files
        if symbols_url and config.storage:
            _process_symbols_only(symbols_url, safe_version, product_name, auth_headers)

        # Fetch the GitHub Release once; used for both DB upsert and build drop.
        release = None
        needs_release = (config.database and config.repo) or (
            config.storage and config.storage.build_drop_base_path and config.repo
        )
        if needs_release and _releases_client:
            try:
                release = _releases_client.get_release_for_tag(build_version)
            except Exception:
                logger.exception(
                    "Could not fetch release for tag %s (non-fatal)", build_version
                )

        # Update the downloads database for this build version.
        # Fetch the GitHub Release for the tag and upsert whatever asset URLs are
        # available now; the hourly reconciler will fill in any that aren't ready yet.
        if release and config.database and config.repo and _releases_client:
            try:
                upsert_from_release(
                    release,
                    _releases_client,
                    config.database,
                    config.repo.version_branches,
                    commit_log_table=config.database.commit_log_table,
                    asset_match_filter=config.repo.asset_match_filter,
                )
            except Exception:
                logger.exception(
                    "DB upsert failed for build %s (non-fatal)", build_version
                )

        # Download build archives to the local drop directory as a backup mirror.
        if release and config.storage and config.storage.build_drop_base_path:
            try:
                version_prefix = ".".join(build_version.split(".")[:2])
                drop_dir = Path(config.storage.build_drop_base_path) / version_prefix
                drop_dir.mkdir(parents=True, exist_ok=True)

                for asset in release.get("assets", []):
                    name = asset["name"]
                    if not re.search(
                        r"-(windows\.zip|linux\.tar\.gz)$", name, re.IGNORECASE
                    ):
                        continue
                    target = drop_dir / name
                    if target.exists() and target.stat().st_size >= asset["size"]:
                        continue
                    tmp_fd, tmp_str = tempfile.mkstemp(dir=drop_dir, suffix=".tmp")
                    os.close(tmp_fd)
                    tmp_path = Path(tmp_str)
                    try:
                        download_file(asset["browser_download_url"], tmp_path)
                        tmp_path.rename(target)
                        logger.info(f"Build archive saved to {target}")
                    except Exception:
                        tmp_path.unlink(missing_ok=True)
                        raise
            except Exception:
                logger.exception("Build drop failed for %s (non-fatal)", build_version)

    except Exception as e:
        logger.error(f"Error processing artifacts: {e}")
        raise


def process_workflow_run(run_id: int, head_sha: str) -> None:
    """Triggered by a workflow_run completed event. Downloads the PDB artifact and processes it."""
    if not config.repo or not _releases_client:
        logger.error("Repo not configured; cannot process workflow run %s", run_id)
        return

    try:
        artifacts = _releases_client.list_run_artifacts(run_id)
        pdb_artifact = next((a for a in artifacts if a["name"] == "pdbs"), None)
        if not pdb_artifact:
            logger.warning("No 'pdbs' artifact found for run %s", run_id)
            return

        artifact_id = pdb_artifact["id"]
        symbols_url = f"https://api.github.com/repos/{config.repo.full_name}/actions/artifacts/{artifact_id}/zip"

        release = _releases_client.get_release_for_commit(head_sha)
        if not release:
            logger.warning(
                "No release found for commit %s (run %s); DB upsert skipped",
                head_sha,
                run_id,
            )
            # Still process symbols even if we can't find the release for DB.
            build_version = head_sha[:12]
        else:
            build_version = release["tag_name"]

        logger.info(
            "Processing workflow run %s: build_version=%s artifact_id=%s",
            run_id,
            build_version,
            artifact_id,
        )
        process_artifacts(
            symbols_url=symbols_url,
            build_version=build_version,
            product_name=config.repo.product_name,
        )
    except Exception:
        logger.exception("Failed to process workflow run %s", run_id)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/webhook", status_code=202)
async def webhook(
    request: Request,
    x_github_event: str = Header(default=""),
    _sig: None = Depends(verify_github_signature),
):
    body = await request.body()
    payload = json.loads(body)

    if x_github_event == "ping":
        return {"message": "pong", "zen": payload.get("zen", "")}

    if x_github_event == "workflow_run":
        if payload.get("action") != "completed":
            return {"message": "ignored: not completed"}
        run = payload["workflow_run"]

        if config.repo and run.get("path") != config.repo.workflow_path:
            logger.debug(
                "Ignoring workflow run %s: path=%r (expected %r)",
                run.get("id"),
                run.get("path"),
                config.repo.workflow_path,
            )
            return {"message": "ignored: wrong workflow"}

        conclusion = run.get("conclusion")
        if conclusion != "success":
            logger.info(
                "Ignoring workflow run %s: conclusion=%s", run.get("id"), conclusion
            )
            return {"message": f"ignored: conclusion={conclusion}"}

        run_id = run["id"]
        head_sha = run["head_sha"]
        thread = threading.Thread(
            target=process_workflow_run,
            args=(run_id, head_sha),
            daemon=True,
        )
        thread.start()
        return {"message": "Processing started"}

    logger.debug("Unhandled GitHub event: %s", x_github_event)
    return {"message": f"unhandled event: {x_github_event}"}


if __name__ == "__main__":
    uvicorn.run(app, host=config.api.host, port=config.api.port)
