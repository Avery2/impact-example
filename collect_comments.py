#!/usr/bin/env python3
"""Agent 4: Collect issue comments via REST API. Depends on issues.json from Agent 3."""

import json
import subprocess
import sys
import os
import time

ISSUES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "raw-data", "issues.json")
OUTPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "raw-data", "issue_comments.json")

def truncate(text, max_len=200):
    if not text:
        return ""
    return text[:max_len] + ("..." if len(text) > max_len else "")

def fetch_comments(issue_number):
    result = subprocess.run(
        [
            "gh", "api",
            f"repos/PostHog/posthog/issues/{issue_number}/comments?per_page=100",
            "--paginate",
        ],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"  Failed to fetch comments for #{issue_number}: {result.stderr}", file=sys.stderr)
        return []

    # --paginate for array endpoints concatenates JSON arrays
    text = result.stdout.strip()
    if not text:
        return []

    comments_raw = json.loads(text)
    if not isinstance(comments_raw, list):
        comments_raw = [comments_raw]

    return [
        {
            "issue_number": issue_number,
            "author": c["user"]["login"] if c.get("user") else None,
            "created_at": c["created_at"],
            "body_preview": truncate(c.get("body", "")),
            "body_length": len(c.get("body", "")),
        }
        for c in comments_raw
    ]

def collect_comments():
    with open(ISSUES_FILE) as f:
        issues = json.load(f)

    issues_with_comments = [i for i in issues if i["comments_count"] > 0]
    print(f"Fetching comments for {len(issues_with_comments)} issues (out of {len(issues)} total)...")

    all_comments = []
    for idx, issue in enumerate(issues_with_comments):
        if idx % 50 == 0 and idx > 0:
            print(f"  Progress: {idx}/{len(issues_with_comments)}", flush=True)
        comments = fetch_comments(issue["number"])
        all_comments.extend(comments)

    with open(OUTPUT, "w") as f:
        json.dump(all_comments, f)

    print(f"Wrote {len(all_comments)} comments to {OUTPUT}")

if __name__ == "__main__":
    collect_comments()
