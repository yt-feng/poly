"""Keep the daily-equity collector running with a bounded, acknowledged handoff.

All callers must hold the same Actions concurrency lock.  This helper does not
retry GitHub requests: an ambiguous dispatch must be inspected by the next
watchdog through the complete active-run inventory.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
import re
import subprocess
from typing import Callable
from urllib.parse import quote


ACTIVE = {"requested", "waiting", "pending", "queued", "in_progress"}
WORKFLOW = "equity-daily.yml"
WORKFLOW_PATH = f".github/workflows/{WORKFLOW}"
PRODUCTION_EVENTS = {"workflow_dispatch", "schedule", "push"}
RESTART_LIMIT = 3
RESTART_WINDOW = timedelta(minutes=15)
TERMINAL_CONCLUSIONS = {
    "success", "failure", "neutral", "cancelled", "skipped", "timed_out",
    "action_required", "startup_failure", "stale",
}


def gh_call(args: list[str]) -> str:
    """Run one GitHub CLI request, with no network or dispatch retries."""
    try:
        result = subprocess.run(
            ["gh", *args], check=True, capture_output=True, text=True, timeout=40,
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"GitHub request failed: {exc.stderr.strip()}") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("GitHub request timed out; request was not retried") from exc
    return result.stdout


def _positive_id(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"Invalid {label}; refusing a blind dispatch")
    return value


def _json(call: Callable[[list[str]], str], args: list[str]) -> object:
    try:
        return json.loads(call(args))
    except (json.JSONDecodeError, TypeError) as exc:
        raise ValueError("Invalid GitHub JSON; refusing a blind dispatch") from exc


def _object(value: object, label: str) -> dict:
    if not isinstance(value, dict):
        raise ValueError(f"Invalid {label}; refusing a blind dispatch")
    return value


def _pages(call: Callable[[list[str]], str], endpoint: str, key: str) -> list[dict]:
    """Validate gh's slurped pagination, including coverage and duplicate IDs.

    A changing inventory can invalidate a page count.  Stopping is preferable to
    assuming the missing records contain no collector; the next check can take
    a fresh snapshot.  This also detects GitHub's 1,000-run search-result cap.
    """
    pages = _json(call, ["api", endpoint, "--paginate", "--slurp"])
    if not isinstance(pages, list) or not pages:
        raise ValueError("Invalid paginated inventory; refusing a blind dispatch")
    records: list[dict] = []
    expected = None
    seen: set[int] = set()
    for index, raw_page in enumerate(pages):
        page = _object(raw_page, "inventory page")
        count = page.get("total_count")
        rows = page.get(key)
        if type(count) is not int or count < 0 or not isinstance(rows, list):
            raise ValueError("Invalid inventory count or records; refusing a blind dispatch")
        if expected is None:
            expected = count
        if count != expected or len(rows) > 100 or (index < len(pages) - 1 and len(rows) != 100):
            raise ValueError("Incomplete or changing pagination; refusing a blind dispatch")
        for raw_row in rows:
            row = _object(raw_row, "inventory record")
            if key == "workflow_runs":
                run_id = _positive_id(row.get("id"), "run ID")
                if run_id in seen:
                    raise ValueError("Duplicate run in pagination; refusing a blind dispatch")
                seen.add(run_id)
            records.append(row)
    if len(records) != expected:
        raise ValueError("Incomplete pagination; refusing a blind dispatch")
    return records


def _created_at(run: dict) -> datetime:
    value = run.get("created_at")
    if not isinstance(value, str):
        raise ValueError("Missing run creation time; refusing a blind dispatch")
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Invalid run creation time; refusing a blind dispatch") from exc
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError("Run creation time lacks a timezone; refusing a blind dispatch")
    return stamp


def _identity(run: dict, repo: str, workflow_id: int) -> None:
    _positive_id(run.get("id"), "run ID")
    _positive_id(run.get("workflow_id"), "run workflow ID")
    repository = run.get("repository")
    path = run.get("path")
    if (
        not isinstance(repository, dict)
        or repository.get("full_name") != repo
        or not isinstance(path, str)
        or path.split("@", 1)[0] != WORKFLOW_PATH
        or run.get("workflow_id") != workflow_id
        or run.get("head_branch") != "main"
    ):
        raise RuntimeError("Run identity does not match the expected repository, workflow and main branch")
    status = run.get("status")
    event = run.get("event")
    if not isinstance(status, str) or status not in ACTIVE | {"completed"} or not isinstance(event, str) or not event:
        raise ValueError("Invalid workflow run status or event; refusing a blind dispatch")
    _created_at(run)


def ensure_capture(
    repo: str, *, current_run_id: int | None = None, now: datetime | None = None,
    call: Callable[[list[str]], str] = gh_call,
) -> dict:
    """Inspect all active runs, dispatch at most once, and verify its run ID."""
    if os.environ.get("EQUITY_CAPTURE_PAUSED", "").strip().lower() == "true":
        return {"action": "paused"}
    if not isinstance(repo, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo):
        raise ValueError("GH_REPO must be an owner/repository name")
    if current_run_id is not None:
        current_run_id = _positive_id(current_run_id, "current run ID")
    now = now or datetime.now(timezone.utc)
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("Current time must include a timezone")
    workflow_endpoint = f"repos/{repo}/actions/workflows/{WORKFLOW}"
    workflow = _object(_json(call, ["api", workflow_endpoint]), "workflow response")
    workflow_id = _positive_id(workflow.get("id"), "workflow ID")
    if workflow.get("path") != WORKFLOW_PATH:
        raise RuntimeError("Target workflow has an unexpected path")
    state = workflow.get("state")
    if not isinstance(state, str):
        raise ValueError("Unknown target workflow state; refusing a blind dispatch")
    if state in {"disabled_manually", "disabled_inactivity", "disabled_fork", "deleted"}:
        return {"action": "workflow_disabled", "state": state}
    if state != "active":
        raise ValueError("Unknown target workflow state; refusing a blind dispatch")

    active_runs: dict[int, dict] = {}
    # A single recent-runs page can hide an older running collector.  Ask for
    # every active state independently and follow every page in each response.
    for status in sorted(ACTIVE):
        endpoint = f"{workflow_endpoint}/runs?branch=main&status={status}&per_page=100"
        for run in _pages(call, endpoint, "workflow_runs"):
            _identity(run, repo, workflow_id)
            if run["event"] not in PRODUCTION_EVENTS or run["status"] not in ACTIVE:
                continue
            previous = active_runs.get(run["id"])
            if previous is not None and previous["status"] != run["status"]:
                raise RuntimeError("Active run changed during inventory; refusing a blind dispatch")
            active_runs[run["id"]] = run

    def capture_ended(run_id: int) -> bool:
        run = _object(_json(call, ["api", f"repos/{repo}/actions/runs/{run_id}"]), "run response")
        _identity(run, repo, workflow_id)
        if run["id"] != run_id or run["event"] not in PRODUCTION_EVENTS:
            raise RuntimeError("Capture run response has a mismatched identity")
        jobs = _pages(call, f"repos/{repo}/actions/runs/{run_id}/jobs?filter=latest&per_page=100", "jobs")
        for job in jobs:
            status = job.get("status")
            if not isinstance(job.get("name"), str) or not isinstance(status, str) or status not in ACTIVE | {"completed"}:
                raise ValueError("Invalid job inventory; refusing a blind dispatch")
        captures = [job for job in jobs if job["name"] == "capture"]
        if len(captures) > 1:
            raise ValueError("Ambiguous capture jobs; refusing a blind dispatch")
        if not captures or captures[0]["status"] != "completed":
            return False
        conclusion = captures[0].get("conclusion")
        if not isinstance(conclusion, str) or conclusion not in TERMINAL_CONCLUSIONS:
            raise ValueError("Completed capture has no terminal conclusion; refusing a blind dispatch")
        return True

    draining: list[int] = []
    if current_run_id is not None:
        current = active_runs.get(current_run_id)
        if current is None:
            raise RuntimeError("Current run is absent from the active inventory; refusing to ignore an unknown run")
        if current["status"] != "in_progress" or not capture_ended(current_run_id):
            raise RuntimeError("Current capture job is not terminal; refusing an overlapping successor")
        draining.append(current_run_id)
    active: list[int] = []
    for run_id, run in sorted(active_runs.items()):
        if run_id == current_run_id:
            continue
        # A watchdog can replace a pending handoff under the shared lock.  The
        # ended collector must then be recognized as draining by that watchdog.
        if run["status"] == "in_progress" and capture_ended(run_id):
            draining.append(run_id)
        else:
            active.append(run_id)
    if active:
        return {"action": "already_active", "runs": active, "draining_runs": draining}

    cutoff = now - RESTART_WINDOW
    since = quote(">=" + cutoff.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"), safe="")
    recent_runs = _pages(
        call, f"{workflow_endpoint}/runs?branch=main&created={since}&per_page=100", "workflow_runs",
    )
    recent: set[int] = set()
    for run in recent_runs:
        _identity(run, repo, workflow_id)
        if run["event"] in PRODUCTION_EVENTS and _created_at(run) >= cutoff:
            recent.add(run["id"])
    if len(recent) >= RESTART_LIMIT:
        raise RuntimeError(f"{RESTART_LIMIT} captures started within 15 minutes; refusing a restart loop")

    # No retry: a response lost after acceptance must not create a second run.
    result = _object(_json(call, [
        "api", f"{workflow_endpoint}/dispatches", "--method", "POST",
        "-H", "X-GitHub-Api-Version: 2026-03-10", "-f", "ref=main",
        "-F", "return_run_details=true",
    ]), "dispatch acknowledgement")
    successor_id = _positive_id(result.get("workflow_run_id"), "acknowledged successor ID")
    if successor_id == current_run_id or successor_id in active_runs or successor_id in recent:
        raise RuntimeError("Dispatch did not acknowledge a distinct successor run")
    successor = _object(_json(call, ["api", f"repos/{repo}/actions/runs/{successor_id}"]), "successor response")
    _identity(successor, repo, workflow_id)
    if successor["id"] != successor_id or successor["status"] not in ACTIVE or successor["event"] != "workflow_dispatch":
        raise RuntimeError("Acknowledged successor is not the expected active dispatch; inspect it before retrying")
    url = successor.get("html_url")
    if not isinstance(url, str) or url != f"https://github.com/{repo}/actions/runs/{successor_id}":
        raise RuntimeError("Acknowledged successor has an unexpected URL")
    return {
        "action": "dispatched", "predecessor_run_id": current_run_id,
        "draining_runs": draining, "successor_run_id": successor_id,
        "successor_url": url,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--current-run-id", type=int)
    args = parser.parse_args()
    code = 0
    try:
        result = ensure_capture(os.environ.get("GH_REPO", ""), current_run_id=args.current_run_id)
    except (RuntimeError, ValueError, OSError) as exc:
        result = {"action": "error", "error": str(exc)}
        code = 1
    rendered = json.dumps(result, sort_keys=True)
    print(rendered)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a", encoding="utf-8") as handle:
            handle.write(f"\n### Equity capture continuity\n\n```json\n{rendered}\n```\n")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
