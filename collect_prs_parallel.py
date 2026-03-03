#!/usr/bin/env python3
"""Parallel PR collection: splits date range into chunks, runs concurrently."""

import json
import subprocess
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT = os.path.join(BASE_DIR, "raw-data", "prs.json")
PROGRESS_FILE = os.path.join(BASE_DIR, "raw-data", ".prs_progress.json")

# Split 90 days into 6 chunks of ~15 days
DATE_CHUNKS = [
    ("2025-12-02", "2025-12-17"),
    ("2025-12-17", "2026-01-01"),
    ("2026-01-01", "2026-01-16"),
    ("2026-01-16", "2026-01-31"),
    ("2026-01-31", "2026-02-15"),
    ("2026-02-15", "2026-03-02"),
]

QUERY_TEMPLATE = """
query($cursor: String) {{
  search(query: "repo:PostHog/posthog is:pr is:merged merged:{start}..{end}", type: ISSUE, first: 100, after: $cursor) {{
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
    chunk_prs = []
    cursor = None
    page = 0

    while True:
        page += 1
        variables = json.dumps({"cursor": cursor})
        for attempt in range(3):
            result = subprocess.run(
                ["gh", "api", "graphql", "-f", f"query={query}", "-f", f"variables={variables}"],
                capture_output=True, text=True,
            )
            if result.returncode == 0:
                break
            print(f"  [{start}..{end}] page {page} attempt {attempt+1} failed: {result.stderr.strip()}", flush=True)
            if attempt < 2:
                time.sleep(10 * (attempt + 1))
        else:
            print(f"  [{start}..{end}] GIVING UP on page {page} after 3 attempts", flush=True)
            return chunk_prs

        data = json.loads(result.stdout)
        search = data["data"]["search"]
        for node in search["nodes"]:
            pr = parse_node(node)
            if pr:
                chunk_prs.append(pr)

        print(f"  [{start}..{end}] page {page}: {len(chunk_prs)} PRs so far", flush=True)

        if not search["pageInfo"]["hasNextPage"]:
            break
        cursor = search["pageInfo"]["endCursor"]

    return chunk_prs

def main():
    print(f"Collecting PRs in {len(DATE_CHUNKS)} parallel chunks...", flush=True)
    all_prs = []

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_chunk, s, e): (s, e) for s, e in DATE_CHUNKS}
        for future in as_completed(futures):
            s, e = futures[future]
            try:
                prs = future.result()
                all_prs.extend(prs)
                print(f"Chunk {s}..{e}: {len(prs)} PRs", flush=True)
            except Exception as exc:
                print(f"Chunk {s}..{e} failed: {exc}", flush=True)

    # Deduplicate by PR number (chunks share boundary dates)
    seen = set()
    deduped = []
    for pr in all_prs:
        if pr["number"] not in seen:
            seen.add(pr["number"])
            deduped.append(pr)

    deduped.sort(key=lambda p: p["merged_at"])

    with open(OUTPUT, "w") as f:
        json.dump(deduped, f)

    # Clean up progress file from old sequential run
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)

    print(f"Wrote {len(deduped)} PRs to {OUTPUT}", flush=True)

if __name__ == "__main__":
    main()
