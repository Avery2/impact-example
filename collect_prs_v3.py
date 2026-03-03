#!/usr/bin/env python3
"""PR collection v3: small date chunks to stay within GitHub Search 1000-result limit."""

import json
import subprocess
import sys
import os
import time
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT = os.path.join(BASE_DIR, "raw-data", "prs.json")

# Generate 5-day chunks across 90 days
def generate_chunks(start_date, end_date, chunk_days=5):
    chunks = []
    current = start_date
    while current < end_date:
        chunk_end = min(current + timedelta(days=chunk_days), end_date)
        chunks.append((current.isoformat(), chunk_end.isoformat()))
        current = chunk_end
    return chunks

DATE_CHUNKS = generate_chunks(date(2025, 12, 2), date(2026, 3, 2), chunk_days=5)

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
    max_pages = 10  # GitHub Search returns max 1000 results

    for page in range(1, max_pages + 1):
        variables = json.dumps({"cursor": cursor})
        for attempt in range(3):
            result = subprocess.run(
                ["gh", "api", "graphql", "-f", f"query={query}", "-f", f"variables={variables}"],
                capture_output=True, text=True,
            )
            if result.returncode == 0:
                break
            if attempt < 2:
                time.sleep(15 * (attempt + 1))
        else:
            print(f"  [{start}..{end}] GAVE UP on page {page}", flush=True)
            return chunk_prs

        data = json.loads(result.stdout)
        search = data["data"]["search"]
        for node in search["nodes"]:
            pr = parse_node(node)
            if pr:
                chunk_prs.append(pr)

        if not search["pageInfo"]["hasNextPage"]:
            break
        cursor = search["pageInfo"]["endCursor"]

    return chunk_prs

def main():
    print(f"Collecting PRs in {len(DATE_CHUNKS)} chunks (5-day windows, 2 workers)...", flush=True)
    all_prs = []

    # Use only 2 workers to avoid rate limits
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(fetch_chunk, s, e): (s, e) for s, e in DATE_CHUNKS}
        for future in as_completed(futures):
            s, e = futures[future]
            try:
                prs = future.result()
                all_prs.extend(prs)
                print(f"  {s}..{e}: {len(prs)} PRs (total so far: {len(all_prs)})", flush=True)
            except Exception as exc:
                print(f"  {s}..{e} FAILED: {exc}", flush=True)

    # Deduplicate by PR number
    seen = set()
    deduped = []
    for pr in all_prs:
        if pr["number"] not in seen:
            seen.add(pr["number"])
            deduped.append(pr)

    deduped.sort(key=lambda p: p["merged_at"])

    with open(OUTPUT, "w") as f:
        json.dump(deduped, f)

    print(f"\nWrote {len(deduped)} unique PRs to {OUTPUT}", flush=True)

if __name__ == "__main__":
    main()
