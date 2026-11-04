#!/usr/bin/env bash
# Publish captured chunks while tolerating a bounded GitHub service interruption.
set -euo pipefail

for attempt in 1 2 3 4 5; do
  if git fetch origin main; then
    if git merge-base --is-ancestor HEAD origin/main; then
      echo "Captured commit is already present on origin/main."
      exit 0
    fi
    if ! git rebase origin/main; then
      git rebase --abort || true
      echo "Cannot rebase captured data; preserving it for artifact recovery." >&2
      exit 1
    fi
    if git push origin HEAD:main; then
      exit 0
    fi
    # A failed response can follow a successful write. Inspect before retrying.
    if git fetch origin main && git merge-base --is-ancestor HEAD origin/main; then
      echo "Verified captured commit on origin/main after the push response failed."
      exit 0
    fi
  fi
  if [ "$attempt" -eq 5 ]; then
    echo "Publish failed after 5 attempts; preserving captured data for artifact recovery." >&2
    exit 1
  fi
  delay=$((15 * (2 ** (attempt - 1))))
  echo "GitHub publish attempt ${attempt}/5 did not complete; retrying in ${delay}s."
  sleep "$delay"
done
