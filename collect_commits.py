#!/usr/bin/env python3
"""Agent 1: Collect commits from local posthog git repo."""

import json
import subprocess
import sys
import os

REPO_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "posthog")
OUTPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "raw-data", "commits.json")
SINCE = "2025-12-02"
UNTIL = "2026-03-02"

def collect_commits():
    result = subprocess.run(
        [
            "git", "log", "--all",
            f"--since={SINCE}", f"--until={UNTIL}",
            "--pretty=tformat:COMMIT_START%n%H|%an|%ae|%aI|%s|%P",
            "--numstat",
        ],
        cwd=REPO_DIR,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"git log failed: {result.stderr}", file=sys.stderr)
        sys.exit(1)

    commits = []
    current = None

    for line in result.stdout.split("\n"):
        if line.startswith("COMMIT_START"):
            if current:
                current["total_additions"] = sum(f["additions"] for f in current["files"])
                current["total_deletions"] = sum(f["deletions"] for f in current["files"])
                commits.append(current)
            current = None
            continue

        if current is None and "|" in line:
            parts = line.split("|", 5)
            if len(parts) == 6:
                parent_hashes = parts[5].strip().split() if parts[5].strip() else []
                current = {
                    "hash": parts[0],
                    "author_name": parts[1],
                    "author_email": parts[2],
                    "timestamp": parts[3],
                    "subject": parts[4],
                    "parent_hashes": parent_hashes,
                    "is_merge": len(parent_hashes) > 1,
                    "files": [],
                }
            continue

        if current and line.strip() and "\t" in line:
            parts = line.split("\t", 2)
            if len(parts) == 3:
                adds = int(parts[0]) if parts[0] != "-" else 0
                dels = int(parts[1]) if parts[1] != "-" else 0
                current["files"].append({
                    "filename": parts[2],
                    "additions": adds,
                    "deletions": dels,
                })

    if current:
        current["total_additions"] = sum(f["additions"] for f in current["files"])
        current["total_deletions"] = sum(f["deletions"] for f in current["files"])
        commits.append(current)

    with open(OUTPUT, "w") as f:
        json.dump(commits, f)

    print(f"Wrote {len(commits)} commits to {OUTPUT}")
    unique_authors = len(set(c["author_email"] for c in commits))
    print(f"Unique authors: {unique_authors}")

if __name__ == "__main__":
    collect_commits()
