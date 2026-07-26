import logging
import os
import re
import tempfile
import time
from collections.abc import Callable
from pathlib import Path

from db import get_connection, get_known_builds, update_build_urls, upsert_build
from releases import GitHubReleasesClient

logger = logging.getLogger(__name__)


def _archive_platform(name: str) -> tuple[str | None, str]:
    """Split a build-archive filename into (platform, stem).

    Returns ('windows'|'linux', filename-without-platform-suffix), or (None, '')
    if the name isn't a build archive. E.g.
    'amxmodx-1.10.0-git1234-base-linux.tar.gz' → ('linux', 'amxmodx-1.10.0-git1234-base').
    """
    low = name.lower()
    if low.endswith("-windows.zip"):
        return "windows", name[: -len("-windows.zip")]
    if low.endswith("-linux.tar.gz"):
        return "linux", name[: -len("-linux.tar.gz")]
    return None, ""


def latest_pointer_map(release: dict, product_name: str) -> dict[str, str]:
    """Map pointer-filename → archive-filename for every build archive in a release.

    One pointer per archive. The pointer name is
    `<product>-latest-[<package>-]<platform>`.  The <package> token is whatever
    distinguishes sibling archives: the leading tokens shared by every archive
    stem are the product+version prefix, and the remainder is the package.  So a
    multi-package release (amxmodx base/cstrike/dod) yields one pointer each
    (amxmodx-latest-base-linux, ...), while a single-archive product (mmsource,
    sourcemod) omits the package entirely (mmsource-latest-linux).
    """
    # (filename, platform, stem tokens) for each build archive.
    archives: list[tuple[str, str, list[str]]] = []
    for asset in release.get("assets", []):
        name = asset["name"]
        platform, stem = _archive_platform(name)
        if platform is None:
            continue
        archives.append((name, platform, stem.split("-")))
    if not archives:
        return {}

    # Count the leading stem tokens common to every archive (product + version);
    # zip() stops at the shortest token list automatically.
    shared = 0
    for column in zip(*(toks for _, _, toks in archives)):
        if len(set(column)) == 1:
            shared += 1
        else:
            break

    pointers: dict[str, str] = {}
    for name, platform, toks in archives:
        package = "-".join(toks[shared:])
        suffix = f"{package}-{platform}" if package else platform
        pointers[f"{product_name}-latest-{suffix}"] = name
    return pointers


def write_latest_pointer(drop_dir: Path, pointer_name: str, filename: str) -> None:
    """(Re)write one latest-pointer file (mode 0644) holding only `filename` (no newline)."""
    pointer = drop_dir / pointer_name
    pointer.write_text(filename)
    pointer.chmod(0o644)


def write_latest_pointers(drop_dir: Path, product_name: str, release: dict) -> None:
    """(Re)write a latest-pointer beside every build archive of `release` present in drop_dir."""
    for pointer_name, filename in latest_pointer_map(release, product_name).items():
        if (drop_dir / filename).exists():
            write_latest_pointer(drop_dir, pointer_name, filename)


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
            target.chmod(0o644)  # heal archives written before the 0644 fix
            continue
        logger.info("Downloading missing/incomplete archive: %s", name)
        tmp_fd, tmp_str = tempfile.mkstemp(dir=drop_dir, suffix=".tmp")
        os.close(tmp_fd)
        tmp_path = Path(tmp_str)
        try:
            download_fn(asset["browser_download_url"], tmp_path)
            tmp_path.rename(target)
            target.chmod(0o644)  # mkstemp creates 0600; downloads must be 0644
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

    # Newest archive filename per (version_prefix, pointer_name). Releases are
    # iterated newest-first, so the first entry seen for each key is the latest;
    # used after the loop to (re)write the latest-pointer files.
    latest_archives: dict[tuple[str, str], str] = {}

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

            if drop_base_path and product_name:
                for pname, fname in latest_pointer_map(release, product_name).items():
                    latest_archives.setdefault((version_prefix, pname), fname)

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

    # (Re)write each latest-pointer to the newest build that is present locally.
    if drop_base_path:
        for (version_prefix, pname), fname in latest_archives.items():
            drop_dir = Path(drop_base_path) / version_prefix
            if (drop_dir / fname).exists():
                write_latest_pointer(drop_dir, pname, fname)

    logger.info("Reconciliation complete: %d new build(s) processed", new_count)
    return new_count
