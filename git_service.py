"""
Git service: read, compare, and commit files in a configured Git repository.

Supports:
  - Cloning / pulling a remote Git repository to a local working directory
  - Reading DAG (.py) and SQL (.sql) files directly from the local clone
  - Comparing any two text versions (git vs GCS, git vs rendered Airflow SQL, etc.)
    and producing a unified diff
  - Writing an optimised file back to the repo and committing (+ pushing) the result

All configuration is driven by .env variables (see settings.py):
  GIT_REPO_URL          : HTTPS or SSH URL of the repository
  GIT_BRANCH            : Branch to work on (default: main)
  GIT_LOCAL_PATH        : Local clone directory  (default: /tmp/dbconnect_git_repo)
  GIT_TOKEN             : Personal Access Token for HTTPS auth (omit for SSH key auth)
  GIT_DAG_PATH          : Subdirectory inside the repo that holds DAG files (default: dags/)
  GIT_SQL_PATH          : Subdirectory inside the repo that holds SQL files (default: sql/)
  GIT_COMMIT_USER_NAME  : Committer display name  (default: MyAgent)
  GIT_COMMIT_USER_EMAIL : Committer e-mail        (default: dbconnect@example.com)
"""
import difflib
import logging
import re
from pathlib import Path
from typing import Any

import settings

log = logging.getLogger(__name__)

_repo = None  # module-level cache; reset by _reset_repo() when needed


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _reset_repo() -> None:
    global _repo
    _repo = None


def _build_clone_url() -> str:
    """Inject a PAT token into an HTTPS URL when GIT_TOKEN is set."""
    url = settings.GIT_REPO_URL
    if settings.GIT_TOKEN and url.startswith("https://"):
        url = url.replace("https://", f"https://{settings.GIT_TOKEN}@", 1)
    return url


def _get_repo():
    """
    Return a git.Repo object for the configured repository.

    Strategy:
      1. Return the cached instance if it is still valid (after a pull).
      2. Open an existing local clone at GIT_LOCAL_PATH and pull.
      3. Clone fresh from GIT_REPO_URL if no local clone exists.

    Raises RuntimeError when neither GIT_REPO_URL nor a valid local clone is available.
    """
    global _repo
    try:
        from git import Repo, InvalidGitRepositoryError  # type: ignore
    except ImportError:
        raise RuntimeError(
            "gitpython is not installed. "
            "Run: pip install 'gitpython>=3.1' (or update requirements.txt and reinstall)."
        )

    if not settings.GIT_LOCAL_PATH:
        raise RuntimeError("GIT_LOCAL_PATH is not configured. Set it in your .env file.")

    local_path = Path(settings.GIT_LOCAL_PATH)

    # Re-use cached repo — pull latest changes first
    if _repo is not None:
        try:
            if settings.GIT_BRANCH and _repo.active_branch.name != settings.GIT_BRANCH:
                _repo.git.checkout(settings.GIT_BRANCH)
            if _repo.remotes:
                _repo.remotes.origin.pull(settings.GIT_BRANCH or _repo.active_branch.name)
        except Exception as exc:
            log.warning("Git pull failed (using cached state): %s", exc)
        return _repo

    # Open existing local clone
    if local_path.exists() and (local_path / ".git").exists():
        try:
            repo = Repo(str(local_path))
            if settings.GIT_BRANCH and repo.active_branch.name != settings.GIT_BRANCH:
                repo.git.checkout(settings.GIT_BRANCH)
            if repo.remotes:
                try:
                    repo.remotes.origin.pull(settings.GIT_BRANCH or repo.active_branch.name)
                except Exception as exc:
                    log.warning("Git pull failed: %s", exc)
            _repo = repo
            log.info("Opened existing git repo at %s (branch: %s)", local_path, repo.active_branch.name)
            return _repo
        except (InvalidGitRepositoryError, Exception) as exc:
            log.warning("Could not open existing repo at %s: %s — will reclone", local_path, exc)
            _repo = None

    # Fresh clone
    if not settings.GIT_REPO_URL:
        raise RuntimeError(
            "GIT_REPO_URL is not configured and no local clone exists at GIT_LOCAL_PATH. "
            "Set GIT_REPO_URL in your .env file."
        )

    log.info("Cloning %s → %s (branch: %s)", settings.GIT_REPO_URL, local_path, settings.GIT_BRANCH or "default")
    local_path.mkdir(parents=True, exist_ok=True)

    clone_kwargs: dict[str, Any] = {}
    if settings.GIT_BRANCH:
        clone_kwargs["branch"] = settings.GIT_BRANCH

    _repo = Repo.clone_from(_build_clone_url(), str(local_path), **clone_kwargs)
    log.info("Clone complete.")
    return _repo


def _normalize_name(name: str) -> str:
    """Reduce any naming convention to lowercase_snake for fuzzy matching."""
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _score_match(target: str, path: str) -> int:
    """Score how closely a file path matches a target name (0 = no match, 4 = exact)."""
    stem = _normalize_name(re.sub(r"\.(py|sql)$", "", Path(path).name, flags=re.IGNORECASE))
    if stem == target:
        return 4
    if stem.startswith(target) or target.startswith(stem):
        return 3
    if target in stem or stem in target:
        return 2
    if set(target.split("_")) & set(stem.split("_")):
        return 1
    return 0


def _unified_diff(text_a: str, text_b: str, label_a: str, label_b: str) -> str:
    """Return a unified-diff string, or a sentinel when the contents are identical."""
    lines_a = text_a.splitlines(keepends=True)
    lines_b = text_b.splitlines(keepends=True)
    diff = list(difflib.unified_diff(lines_a, lines_b, fromfile=label_a, tofile=label_b))
    return "".join(diff) if diff else "(files are identical)"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_git_status() -> dict[str, Any]:
    """
    Verify git connectivity and return repository metadata.

    Returns branch name, latest commit info, remote URL, and the configured
    DAG / SQL subdirectory paths.  Use this as the first step to confirm that
    the git integration is working before calling any other git tool.
    """
    if not settings.GIT_REPO_URL and not settings.GIT_LOCAL_PATH:
        return {
            "error": (
                "Git is not configured. "
                "Set GIT_REPO_URL and GIT_LOCAL_PATH in your .env file."
            )
        }
    try:
        repo = _get_repo()
        commit = repo.head.commit
        return {
            "status": "ok",
            "local_path": settings.GIT_LOCAL_PATH,
            "remote_url": settings.GIT_REPO_URL,
            "branch": repo.active_branch.name,
            "latest_commit": {
                "sha": commit.hexsha[:12],
                "message": commit.message.strip(),
                "author": str(commit.author),
                "date": commit.committed_datetime.isoformat(),
            },
            "dag_path": settings.GIT_DAG_PATH,
            "sql_path": settings.GIT_SQL_PATH,
        }
    except Exception as exc:
        return {"error": str(exc)}


def list_git_files(path_prefix: str = "") -> dict[str, Any]:
    """
    List files in the git repo under an optional subdirectory prefix.

    Args:
        path_prefix: relative path within the repo root (e.g. "dags/" or "sql/").
                     Leave empty to list all tracked files.
    """
    try:
        repo = _get_repo()
        root = Path(settings.GIT_LOCAL_PATH)
        search_root = root / path_prefix if path_prefix else root

        if not search_root.exists():
            return {
                "error": f"Path not found in git repo: {path_prefix!r}",
                "hint": "Check GIT_DAG_PATH / GIT_SQL_PATH in your .env file.",
            }

        files = []
        for item in sorted(search_root.rglob("*")):
            if item.is_file() and ".git" not in item.parts:
                files.append(str(item.relative_to(root)))

        return {
            "path_prefix": path_prefix or "(repo root)",
            "branch": repo.active_branch.name,
            "files": files,
            "count": len(files),
        }
    except Exception as exc:
        return {"error": str(exc)}


def read_git_file(file_path: str) -> dict[str, Any]:
    """
    Read a file from the local git clone by its repo-relative path.

    Args:
        file_path: path relative to the repo root (e.g. "dags/my_dag.py").
                   Obtain paths from list_git_files(), find_dag_in_git(), or
                   find_sql_in_git().
    """
    try:
        repo = _get_repo()
        abs_path = Path(settings.GIT_LOCAL_PATH) / file_path
        if not abs_path.exists():
            return {
                "error": f"File not found in git repo: {file_path}",
                "hint": "Call list_git_files() to browse available paths.",
            }
        content = abs_path.read_text(encoding="utf-8", errors="replace")
        return {
            "file_path": file_path,
            "branch": repo.active_branch.name,
            "content": content,
            "citation": f"[Git: {repo.active_branch.name}/{file_path}]",
        }
    except Exception as exc:
        return {"error": str(exc)}


def find_dag_in_git(dag_name: str) -> dict[str, Any]:
    """
    Search the configured git DAG directory for a Python file matching dag_name.

    Performs fuzzy stem matching (exact → prefix → substring → shared token).
    Returns the best-matching file with its full content ready for comparison
    or optimization.

    Args:
        dag_name: DAG id, partial file name, or table name to search for.
    """
    try:
        listing = list_git_files(settings.GIT_DAG_PATH)
        if "error" in listing:
            return listing

        py_files = [f for f in listing.get("files", []) if f.endswith(".py")]
        if not py_files:
            return {
                "error": f"No Python (.py) files found under {settings.GIT_DAG_PATH!r} in git.",
                "hint": "Verify GIT_DAG_PATH in your .env file.",
            }

        target = _normalize_name(dag_name)
        ranked = sorted(py_files, key=lambda p: _score_match(target, p), reverse=True)
        best_score = _score_match(target, ranked[0]) if ranked else 0

        if best_score == 0:
            return {
                "error": f"No DAG file matching '{dag_name}' found in git.",
                "available_files": ranked[:10],
                "hint": "Call list_git_files(dag_path) to browse all DAG files.",
            }

        result = read_git_file(ranked[0])
        result["dag_name"] = dag_name
        result["match_score"] = best_score
        result["other_candidates"] = [p for p in ranked[1:5] if _score_match(target, p) > 0]
        return result
    except Exception as exc:
        return {"error": str(exc)}


def find_sql_in_git(table_name: str) -> dict[str, Any]:
    """
    Search the configured git SQL directory for a .sql file matching table_name.

    Args:
        table_name: table name, partial file name, or DAG-style identifier to match.
    """
    try:
        listing = list_git_files(settings.GIT_SQL_PATH)
        if "error" in listing:
            return listing

        sql_files = [f for f in listing.get("files", []) if f.lower().endswith(".sql")]
        if not sql_files:
            return {
                "error": f"No SQL (.sql) files found under {settings.GIT_SQL_PATH!r} in git.",
                "hint": "Verify GIT_SQL_PATH in your .env file.",
            }

        target = _normalize_name(table_name)
        ranked = sorted(sql_files, key=lambda p: _score_match(target, p), reverse=True)
        best_score = _score_match(target, ranked[0]) if ranked else 0

        if best_score == 0:
            return {
                "error": f"No SQL file matching '{table_name}' found in git.",
                "available_files": ranked[:10],
                "hint": "Call list_git_files(sql_path) to browse all SQL files.",
            }

        result = read_git_file(ranked[0])
        result["table_name"] = table_name
        result["match_score"] = best_score
        result["other_candidates"] = [p for p in ranked[1:5] if _score_match(target, p) > 0]
        return result
    except Exception as exc:
        return {"error": str(exc)}


def compare_files(
    content_a: str,
    content_b: str,
    label_a: str = "version_a",
    label_b: str = "version_b",
) -> dict[str, Any]:
    """
    Compare two text contents and return a structured diff report.

    Typical usage patterns:
      - Git source vs GCS file  (label_a="git:branch/path", label_b="gcs:gs://…")
      - Git source vs rendered Airflow SQL  (label_b="airflow:dag/task/run")
      - Original SQL vs optimised SQL  (label_a="original", label_b="optimised")

    Args:
        content_a: first text (the reference / "before" version)
        content_b: second text (the candidate / "after" version)
        label_a:   human-readable label for content_a used in the diff header
        label_b:   human-readable label for content_b used in the diff header
    """
    diff = _unified_diff(content_a, content_b, label_a, label_b)
    identical = diff == "(files are identical)"

    diff_lines = diff.splitlines() if not identical else []
    added = sum(1 for l in diff_lines if l.startswith("+") and not l.startswith("+++"))
    removed = sum(1 for l in diff_lines if l.startswith("-") and not l.startswith("---"))

    return {
        "label_a": label_a,
        "label_b": label_b,
        "identical": identical,
        "line_count_a": len(content_a.splitlines()),
        "line_count_b": len(content_b.splitlines()),
        "lines_added": added,
        "lines_removed": removed,
        "diff": diff,
    }


def commit_file_to_git(
    file_path: str,
    content: str,
    commit_message: str,
) -> dict[str, Any]:
    """
    Write content to a file in the local git repo and create a commit.

    The file is written at *file_path* relative to the repo root (e.g.
    "dags/optimised_dag.py" or "sql/orders.sql").  After the commit a push is
    attempted automatically if a remote is configured; push failures are
    surfaced as a warning rather than an error so the local commit is never lost.

    Args:
        file_path:      repo-relative path to write (parent directories are
                        created automatically if they do not exist).
        content:        new file content (UTF-8).
        commit_message: descriptive message for the git commit.
    """
    try:
        repo = _get_repo()
        abs_path = Path(settings.GIT_LOCAL_PATH) / file_path
        abs_path.parent.mkdir(parents=True, exist_ok=True)
        abs_path.write_text(content, encoding="utf-8")

        # Set committer identity from .env (non-persistent — scoped to this write)
        with repo.config_writer() as cw:
            cw.set_value("user", "name", settings.GIT_COMMIT_USER_NAME)
            cw.set_value("user", "email", settings.GIT_COMMIT_USER_EMAIL)

        repo.index.add([str(abs_path)])
        commit = repo.index.commit(commit_message)

        result: dict[str, Any] = {
            "file_path": file_path,
            "branch": repo.active_branch.name,
            "commit_sha": commit.hexsha[:12],
            "commit_message": commit_message,
            "author": f"{settings.GIT_COMMIT_USER_NAME} <{settings.GIT_COMMIT_USER_EMAIL}>",
            "status": "committed",
        }

        # Attempt push
        if repo.remotes:
            try:
                repo.remotes.origin.push(repo.active_branch.name)
                result["push_status"] = "pushed"
                result["remote_url"] = settings.GIT_REPO_URL
            except Exception as push_exc:
                result["push_status"] = f"push_failed: {push_exc}"
                result["push_hint"] = (
                    "The commit was created locally. "
                    "Run `git push` manually or check GIT_TOKEN / SSH key access."
                )
        else:
            result["push_status"] = "no_remote_configured"

        return result
    except Exception as exc:
        log.error("commit_file_to_git(%s): %s", file_path, exc)
        return {"error": str(exc)}
