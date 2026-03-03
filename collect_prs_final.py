#!/usr/bin/env python3
"""PR collection - final version. Handles GitHub Search pagination bug
where hasNextPage stays true past actual results, causing infinite loops.
Deduplicates by PR number and stops when duplicates appear."""

import json
import subprocess
import sys
import os
import time
from datetime import date, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT = os.path.join(BASE_DIR, "raw-data", "prs.json")

def generate_chunks(start_date, end_date, chunk_days=10):
    chunks = []
    current = start_date
    while current < end_date:
        chunk_end = min(current + timedelta(days=chunk_days), end_date)
        chunks.append((current.isoformat(), chunk_end.isoformat()))
        current = chunk_end
    return chunks

DATE_CHUNKS = generate_chunks(date(2025, 12, 2), date(2026, 3, 2), chunk_days=10)

QUERY_TEMPLATE = """
query($cursor: String) {{
  search(query: "repo:PostHog/posthog is:pr is:merged merged:{start}..{end}", type: ISSUE, first: 100, after: $cursor) {{
    issueCount
    pageInfo {{ hasNextPage endCursor }}
    nodes {{
      ... on PullRequest {{
        number title additions deletions changedFiles
        createdAt mergedAt
        author {{ login }}
        mergedBy {{ login }}
        labels(first: 10) {{ nodes {{ name }} }}
        body
        reviewDecision
        reviews(first: 10) {{
          nodes {{ state author {{ login }} submittedAt body }}
        }}
        files(first: 50) {{
          nodes {{ path additions deletions changeType }}
        }}
        comments {{ totalCount }}
        reviewThreads {{ totalCount }}
      }}
    }}
  }}
}}
"""

def truncate(text, max_len=200):
    if not text:
        return ""
    return text[:max_len] + ("..." if len(text) > max_len else "")

def parse_node(node):
    if not node or "number" not in node:
        return None
    return {
        "number": node["number"],
        "title": node["title"],
        "author": node["author"]["login"] if node.get("author") else None,
        "merged_by": node["mergedBy"]["login"] if node.get("mergedBy") else None,
        "created_at": node["createdAt"],
        "merged_at": node["mergedAt"],
        "additions": node["additions"],
        "deletions": node["deletions"],
        "changed_files": node["changedFiles"],
        "labels": [l["name"] for l in (node.get("labels", {}).get("nodes") or [])],
        "body_preview": truncate(node.get("body", "")),
        "review_decision": node.get("reviewDecision"),
        "comments_count": node.get("comments", {}).get("totalCount", 0),
        "review_threads_count": node.get("reviewThreads", {}).get("totalCount", 0),
        "reviews": [
            {
                "state": r["state"],
                "author": r["author"]["login"] if r.get("author") else None,
                "submitted_at": r["submittedAt"],
                "body_preview": truncate(r.get("body", "")),
            }
            for r in (node.get("reviews", {}).get("nodes") or [])
        ],
        "files": [
            {
                "path": f["path"],
                "additions": f["additions"],
                "deletions": f["deletions"],
                "change_type": f.get("changeType"),
            }
            for f in (node.get("files", {}).get("nodes") or [])
        ],
    }

def fetch_chunk(start, end):
    query = QUERY_TEMPLATE.format(start=start, end=end)
    chunk_prs = {}  # keyed by PR number for dedup
    cursor = None

    for page in range(1, 12):  # max 11 pages = 1100 results (safety cap)
        variables = json.dumps({"cursor": cursor})
        success = False
        for attempt in range(3):
            result = subprocess.run(
                ["gh", "api", "graphql", "-f", f"query={query}", "-f", f"variables={variables}"],
                capture_output=True, text=True,
            )
            if result.returncode == 0:
                success = True
                break
            if attempt < 2:
                time.sleep(15 * (attempt + 1))
        if not success:
            print(f"  [{start}..{end}] GAVE UP on page {page}", flush=True)
            break

        data = json.loads(result.stdout)
        search = data["data"]["search"]
        total_expected = search.get("issueCount", 0)

        new_count = 0
        for node in search["nodes"]:
            pr = parse_node(node)
            if pr and pr["number"] not in chunk_prs:
                chunk_prs[pr["number"]] = pr
                new_count += 1

        # Stop if we got no new PRs (pagination is cycling)
        if new_count == 0:
            print(f"  [{start}..{end}] page {page}: no new PRs, stopping (got {len(chunk_prs)}/{total_expected})", flush=True)
            break

        print(f"  [{start}..{end}] page {page}: +{new_count} new, {len(chunk_prs)}/{total_expected} total", flush=True)

        if not search["pageInfo"]["hasNextPage"] or len(chunk_prs) >= total_expected:
            break
        cursor = search["pageInfo"]["endCursor"]
        time.sleep(0.3)

    return list(chunk_prs.values())

def main():
    print(f"Collecting PRs in {len(DATE_CHUNKS)} sequential chunks (10-day windows)...", flush=True)
    all_prs = {}

    for i, (start, end) in enumerate(DATE_CHUNKS):
        prs = fetch_chunk(start, end)
        for pr in prs:
            all_prs[pr["number"]] = pr
        print(f"  Chunk {i+1}/{len(DATE_CHUNKS)} done: {len(prs)} PRs, {len(all_prs)} total unique", flush=True)

    result = sorted(all_prs.values(), key=lambda p: p["merged_at"])

    with open(OUTPUT, "w") as f:
        json.dump(result, f)

    print(f"\nWrote {len(result)} unique PRs to {OUTPUT}", flush=True)

if __name__ == "__main__":
    main()
