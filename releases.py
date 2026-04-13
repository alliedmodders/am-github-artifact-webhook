import re
import logging
from datetime import datetime
from typing import Any, Generator

import requests

logger = logging.getLogger(__name__)

# Tag format: {major}.{minor}.{patch}.{build}  e.g. "1.13.0.7301"
_TAG_RE = re.compile(r"^(\d+\.\d+)\.\d+\.(\d+)$")


class GitHubReleasesClient:
    def __init__(self, repo: str, token: str | None = None):
        """
        Args:
            repo: Full repo name, e.g. "alliedmodders/sourcemod".
            token: Optional GitHub PAT for higher rate limits.
        """
        self.repo = repo
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": "AM-downloads-updater/2.0",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )
        if token:
            self._session.headers["Authorization"] = f"token {token}"

    def _get(self, path: str) -> Any:
        url = f"https://api.github.com{path}"
        response = self._session.get(url, timeout=15)
        remaining = int(response.headers.get("X-RateLimit-Remaining", 9999))
        if remaining < 5:
            raise RuntimeError("GitHub API rate limit nearly exhausted")
        response.raise_for_status()
        return response.json()

    def resolve_tag_to_commit_sha(self, tag: str) -> str:
        """
        Resolve an annotated tag to its underlying commit SHA.

        GitHub release tags are annotated objects, not direct commit refs.
        Two API calls are required:
          1. GET /git/ref/tags/{tag}  → tag object SHA
          2. GET /git/tags/{sha}      → commit SHA
        """
        ref = self._get(f"/repos/{self.repo}/git/ref/tags/{tag}")
        sha = ref["object"]["sha"]
        if ref["object"]["type"] == "tag":
            tag_obj = self._get(f"/repos/{self.repo}/git/tags/{sha}")
            sha = tag_obj["object"]["sha"]
        return sha

    def get_commit_message(self, sha: str) -> str:
        """Return the first line of a commit message."""
        commit = self._get(f"/repos/{self.repo}/commits/{sha}")
        full = commit.get("commit", {}).get("message", "")
        return full.split("\n")[0].strip()

    def get_release_for_tag(self, tag: str) -> dict:
        """Fetch a single release by tag name."""
        return self._get(f"/repos/{self.repo}/releases/tags/{tag}")

    def iter_release_pages(self, per_page: int = 100) -> Generator[list, None, None]:
        """Paginate through all releases newest-first, yielding one page at a time."""
        page = 1
        while True:
            releases = self._get(
                f"/repos/{self.repo}/releases?per_page={per_page}&page={page}"
            )
            if not releases:
                break
            yield releases
            if len(releases) < per_page:
                break
            page += 1

    def list_run_artifacts(self, run_id: int) -> list[dict]:
        """List all artifacts for a workflow run."""
        data = self._get(f"/repos/{self.repo}/actions/runs/{run_id}/artifacts")
        return data.get("artifacts", [])

    def find_workflow_run_for_commit(self, sha: str, workflow_path: str) -> dict | None:
        """
        Find a successfully completed workflow run for a commit SHA and workflow file.
        Returns the most recent matching run, or None if not found.
        """
        data = self._get(
            f"/repos/{self.repo}/actions/runs"
            f"?head_sha={sha}&status=completed&event=push"
        )
        for run in data.get("workflow_runs", []):
            if run.get("path") == workflow_path and run.get("conclusion") == "success":
                return run
        return None

    def get_release_for_commit(self, sha: str, max_pages: int = 3) -> dict | None:
        """
        Find the release whose target_commitish matches the given commit SHA.

        Scans recent releases (newest-first) up to max_pages pages.  The release
        will almost always be on page 1 since it was just created by the CI run.
        """
        for i, page in enumerate(self.iter_release_pages(per_page=10)):
            for release in page:
                if release.get("target_commitish") == sha:
                    return release
            if i + 1 >= max_pages:
                break
        return None

    @staticmethod
    def parse_tag(tag: str) -> tuple[str, int] | None:
        """Parse '1.13.0.7301' → ('1.13', 7301), or None if not a valid SM tag."""
        m = _TAG_RE.match(tag)
        if not m:
            return None
        return m.group(1), int(m.group(2))

    @staticmethod
    def parse_release_assets(
        release: dict, asset_filter: str | None = None
    ) -> tuple[str | None, str | None]:
        """Extract (windows_url, linux_url) from release assets.

        If asset_filter is set, only assets whose name contains the filter
        string are considered (e.g. "base" to match only the base package
        in a multi-package release).
        """
        windows_url = linux_url = None
        for asset in release.get("assets", []):
            name = asset["name"]
            if asset_filter and asset_filter not in name:
                continue
            if re.search(r"-windows\.zip$", name, re.IGNORECASE):
                windows_url = asset["browser_download_url"]
            elif re.search(r"-linux\.tar\.gz$", name, re.IGNORECASE):
                linux_url = asset["browser_download_url"]
        return windows_url, linux_url

    @staticmethod
    def release_timestamp(release: dict) -> int:
        """Parse published_at to a Unix timestamp."""
        published = release.get("published_at", "")
        dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
        return int(dt.timestamp())
