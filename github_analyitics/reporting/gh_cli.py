from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional


class GhCliNotFound(RuntimeError):
    pass


class GhCliError(RuntimeError):
    pass


def _env_flag(name: str) -> bool:
    return (os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "on"}


def _gh_timeout_seconds() -> Optional[float]:
    value = (os.getenv("GITHUB_ANALYTICS_GH_TIMEOUT_SECONDS") or "").strip()
    if not value:
        return None
    try:
        seconds = float(value)
        if seconds <= 0:
            return None
        return seconds
    except Exception:
        return None


def ensure_gh_available() -> str:
    gh = shutil.which("gh")
    if not gh:
        raise GhCliNotFound(
            "GitHub CLI (gh) not found in PATH. Install it from https://cli.github.com/ and run `gh auth login`."
        )
    return gh


def run_gh(args: List[str], *, cwd: Optional[str] = None, env: Optional[Dict[str, str]] = None) -> str:
    gh = ensure_gh_available()
    cmd = [gh] + args

    timeout = _gh_timeout_seconds()
    verbose = _env_flag("GITHUB_ANALYTICS_VERBOSE") or _env_flag("GITHUB_ANALYTICS_DEBUG")

    if verbose:
        pretty = " ".join(cmd)
        if timeout:
            print(f"[gh] -> {pretty} (timeout={timeout}s)")
        else:
            print(f"[gh] -> {pretty}")
        start = time.perf_counter()

    try:
        proc = subprocess.run(
            cmd,
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        pretty = " ".join(cmd)
        raise GhCliError(
            "Timed out while running GitHub CLI command. "
            f"Command: {pretty}. "
            "Tip: increase timeout via GITHUB_ANALYTICS_GH_TIMEOUT_SECONDS or use --skip-commit-stats/--skip-file-modifications."
        )

    if verbose:
        elapsed = time.perf_counter() - start
        print(f"[gh] <- exit={proc.returncode} ({elapsed:.2f}s)")
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        msg = stderr or stdout or f"gh exited with code {proc.returncode}"
        raise GhCliError(msg)
    return (proc.stdout or "").strip()


def gh_api_json(
    path: str,
    *,
    method: str = "GET",
    params: Optional[Dict[str, str]] = None,
    paginate: bool = False,
) -> Any:
    """Call `gh api` and parse JSON.

    Args:
        path: REST path like `repos/OWNER/REPO/issues` or `/user`.
        method: HTTP method.
        params: Query parameters (strings).
        paginate: Use `--paginate`.
    """
    if not path.startswith("/"):
        path = "/" + path

    args: List[str] = ["api", path, "-X", method]
    if paginate:
        args.append("--paginate")

    if params:
        for k, v in params.items():
            if v is None:
                continue
            args.extend(["-f", f"{k}={v}"])

    out = run_gh(args)
    if not out:
        return None
    return json.loads(out)


def gh_auth_login() -> str:
    return run_gh(["api", "user", "-q", ".login"])
