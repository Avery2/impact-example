#!/usr/bin/env python3
"""Agent 3: Collect closed issues via GitHub Search API."""

import json
import subprocess
import sys
import os

OUTPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "raw-data", "issues.json")

def truncate(text, max_len=200):
    if not text:
        return ""
    return text[:max_len] + ("..." if len(text) > max_len else "")

def collect_issues():
    result = subprocess.run(
        [
            "gh", "api",
            "search/issues?q=repo:PostHog/posthog+is:issue+is:closed+closed:2025-12-02..2026-03-02&per_page=100&sort=updated&order=desc",
            "--paginate",
        ],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"API call failed: {result.stderr}", file=sys.stderr)
        sys.exit(1)

    # --paginate concatenates JSON objects; parse them
    all_issues = []
    decoder = json.JSONDecoder()
    text = result.stdout.strip()
    pos = 0
    while pos < len(text):
        while pos < len(text) and text[pos] in " \n\r\t":
            pos += 1
        if pos >= len(text):
            break
        obj, end = decoder.raw_decode(text, pos)
        pos = end
        for item in obj.get("items", []):
            issue = {
                "number": item["number"],
                "title": item["title"],
                "creator": item["user"]["login"] if item.get("user") else None,
                "created_at": item["created_at"],
                "closed_at": item["closed_at"],
                "state_reason": item.get("state_reason"),
                "comments_count": item.get("comments", 0),
                "labels": [l["name"] for l in item.get("labels", [])],
                "reactions": {
                    "+1": item.get("reactions", {}).get("+1", 0),
                    "-1": item.get("reactions", {}).get("-1", 0),
                    "heart": item.get("reactions", {}).get("heart", 0),
                    "rocket": item.get("reactions", {}).get("rocket", 0),
                    "total_count": item.get("reactions", {}).get("total_count", 0),
                },
                "body_preview": truncate(item.get("body", "")),
            }
            all_issues.append(issue)

    with open(OUTPUT, "w") as f:
        json.dump(all_issues, f)

    print(f"Wrote {len(all_issues)} issues to {OUTPUT}")

if __name__ == "__main__":
    collect_issues()
