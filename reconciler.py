import re
import os
import time
import logging
import tempfile
from pathlib import Path
from typing import Callable

from db import get_connection, get_known_builds, update_build_urls, upsert_build
from releases import GitHubReleasesClient

logger = logging.getLogger(__name__)


def upsert_from_release(
    release: dict,
    client: GitHubReleasesClient,
    db_config,
    version_branches: dict[str, str],
    commit_log_table: str = "sm_commit_log",
    asset_match_filter: str | None = None,
) -> bool:
    """
    Resolve and upsert a single GitHub release to the DB.

    Called immediately from the webhook handler after a CI build completes.
    Returns True if the build was written, False if skipped.
    """
    tag = release["tag_name"]
    parsed = client.parse_tag(tag)
    if not parsed:
        return False

    version_prefix, build_num = parsed
    branch = version_branches.get(version_prefix)
    if not branch:
        logger.warning(
            "Unknown version prefix '%s' for tag %s, skipping DB update",
            version_prefix,
            tag,
        )
        return False

    try:
        sha = client.resolve_tag_to_commit_sha(tag)
        message = client.get_commit_message(sha)
    except Exception:
        logger.exception("Could not resolve tag %s for DB update", tag)
        return False

    timestamp = client.release_timestamp(release)
    windows_url, linux_url = client.parse_release_assets(
        release, asset_filter=asset_match_filter
    )

    with get_connection(db_config) as conn:
        upsert_build(
            conn,
            branch=branch,
            sha=sha,
            build_num=build_num,
            timestamp=timestamp,
            message=message,
            windows_url=windows_url,
            linux_url=linux_url,
            table_name=commit_log_table,
        )

    logger.info("DB updated for build %d (branch %s, tag %s)", build_num, branch, tag)
    return True


def _archives_complete(release: dict, drop_base_path: str) -> bool:
    """Return True if all release archives exist locally with their expected sizes."""
    version_prefix = ".".join(release["tag_name"].split(".")[:2])
    drop_dir = Path(drop_base_path) / version_prefix
    for asset in release.get("assets", []):
        name = asset["name"]
        if not re.search(r"-(windows\.zip|linux\.tar\.gz)$", name, re.IGNORECASE):
            continue
        target = drop_dir / name
        if not target.exists() or target.stat().st_size < asset["size"]:
            return False
    return True


def _download_missing_archives(
    release: dict,
    drop_base_path: str,
    download_fn: Callable,
) -> None:
    """Download any release archives that are absent or smaller than expected."""
    version_prefix = ".".join(release["tag_name"].split(".")[:2])
    drop_dir = Path(drop_base_path) / version_prefix
    drop_dir.mkdir(parents=True, exist_ok=True)

    for asset in release.get("assets", []):
        name = asset["name"]
        if not re.search(r"-(windows\.zip|linux\.tar\.gz)$", name, re.IGNORECASE):
            continue
        target = drop_dir / name
        if target.exists() and target.stat().st_size >= asset["size"]:
            continue
        logger.info("Downloading missing/incomplete archive: %s", name)
        tmp_fd, tmp_str = tempfile.mkstemp(dir=drop_dir, suffix=".tmp")
        os.close(tmp_fd)
        tmp_path = Path(tmp_str)
        try:
            download_fn(asset["browser_download_url"], tmp_path)
            tmp_path.rename(target)
            logger.info("Archive saved: %s", target)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            logger.warning("Failed to download archive %s", name, exc_info=True)


def reconcile(
    client: GitHubReleasesClient,
    db_config,
    version_branches: dict[str, str],
    drop_base_path: str | None = None,
    process_symbols_fn: Callable | None = None,
    download_fn: Callable | None = None,
    product_name: str | None = None,
    max_age_days: int | None = 90,
    commit_log_table: str = "sm_commit_log",
    asset_match_filter: str | None = None,
) -> int:
    """
    Fetch GitHub releases and reconcile DB records, build archives, and symbols.

    For each release not yet in the DB:
      - Upserts the DB record
      - Attempts to download the PDB artifact into symstore (may be expired)

    For every release encountered (new or known):
      - Downloads any missing or undersized build archives if drop_base_path is set
      - Back-fills any NULL URL columns if the release now has matching assets

    Stops paginating once an entire page of releases is both fully in the DB
    and has complete local archives (if drop_base_path is configured).

    Releases older than max_age_days that are already in the DB are abandoned
    rather than retried, preventing stale broken releases from blocking early-exit.
    Set max_age_days=None to disable the age limit.

    Returns the number of newly DB-inserted builds.
    """
    with get_connection(db_config) as conn:
        known = get_known_builds(conn, table_name=commit_log_table)

    new_count = 0

    for page_releases in client.iter_release_pages():
        all_done = True

        for release in page_releases:
            tag = release["tag_name"]
            parsed = client.parse_tag(tag)
            if not parsed:
                continue

            version_prefix, build_num = parsed
            branch = version_branches.get(version_prefix)
            if not branch:
                continue

            branch_known = known.get(branch, {})
            is_new = build_num not in branch_known

            # Check if build is missing any URLs
            needs_url_update = False
            if not is_new:
                build_info = branch_known[build_num]
                needs_url_update = (
                    build_info["windows_url"] is None or build_info["linux_url"] is None
                )

            archives_done = not drop_base_path or _archives_complete(
                release, drop_base_path
            )

            if not is_new and not needs_url_update and archives_done:
                continue

            if max_age_days is not None and not is_new:
                age_days = (time.time() - client.release_timestamp(release)) / 86400
                if age_days > max_age_days:
                    logger.debug(
                        "Skipping stale release %s (%.0f days old, limit %d)",
                        tag,
                        age_days,
                        max_age_days,
                    )
                    continue

            all_done = False

            if needs_url_update:
                windows_url, linux_url = client.parse_release_assets(
                    release, asset_filter=asset_match_filter
                )
                if windows_url or linux_url:
                    with get_connection(db_config) as conn:
                        update_build_urls(
                            conn,
                            branch=branch,
                            build_num=build_num,
                            windows_url=windows_url,
                            linux_url=linux_url,
                            table_name=commit_log_table,
                        )
                    known.setdefault(branch, {})[build_num] = {
                        "windows_url": windows_url
                        or branch_known[build_num]["windows_url"],
                        "linux_url": linux_url or branch_known[build_num]["linux_url"],
                    }
                    logger.info(
                        "Updated missing URLs for build %d (branch %s)",
                        build_num,
                        branch,
                    )

            if is_new:
                try:
                    sha = client.resolve_tag_to_commit_sha(tag)
                    message = client.get_commit_message(sha)
                except Exception:
                    logger.warning(
                        "Could not resolve tag %s, skipping", tag, exc_info=True
                    )
                    continue

                timestamp = client.release_timestamp(release)
                windows_url, linux_url = client.parse_release_assets(
                    release, asset_filter=asset_match_filter
                )

                with get_connection(db_config) as conn:
                    upsert_build(
                        conn,
                        branch=branch,
                        sha=sha,
                        build_num=build_num,
                        timestamp=timestamp,
                        message=message,
                        windows_url=windows_url,
                        linux_url=linux_url,
                        table_name=commit_log_table,
                    )

                known.setdefault(branch, {})[build_num] = {
                    "windows_url": windows_url,
                    "linux_url": linux_url,
                }
                new_count += 1
                logger.info(
                    "Processed build %d for branch %s (tag %s)", build_num, branch, tag
                )

                # Attempt PDB artifact download for newly discovered builds.
                # Artifacts expire after ~90 days; failures are non-fatal.
                if process_symbols_fn and product_name:
                    try:
                        process_symbols_fn(sha, product_name)
                    except Exception:
                        logger.warning(
                            "Symbol processing failed for build %d (non-fatal)",
                            build_num,
                            exc_info=True,
                        )

            if not archives_done and drop_base_path and download_fn:
                try:
                    _download_missing_archives(release, drop_base_path, download_fn)
                except Exception:
                    logger.warning(
                        "Archive download failed for tag %s (non-fatal)",
                        tag,
                        exc_info=True,
                    )

        if all_done:
            break

    logger.info("Reconciliation complete: %d new build(s) processed", new_count)
    return new_count
