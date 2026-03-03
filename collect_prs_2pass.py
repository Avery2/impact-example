#!/usr/bin/env python3
"""PR collection - 2-pass approach:
Pass 1: Lightweight search to get all PR numbers
Pass 2: Fetch full details in batches of 20"""

import json
import subprocess
import sys
import os
import time
from datetime import date, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT = os.path.join(BASE_DIR, "raw-data", "prs.json")

SEARCH_QUERY = 'query($cursor: String) {{ search(query: "repo:PostHog/posthog is:pr is:merged merged:{start}..{end}", type: ISSUE, first: 100, after: $cursor) {{ issueCount pageInfo {{ hasNextPage endCursor }} nodes {{ ... on PullRequest {{ number }} }} }} }}'

def truncate(text, max_len=200):
    if not text:
        return ""
    return text[:max_len] + ("..." if len(text) > max_len else "")

def parse_pr(node):
    if not node:
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

def gh_graphql(query, cursor=None, max_retries=3):
    """Run a GraphQL query with proper cursor variable passing."""
    cmd = ["gh", "api", "graphql", "-f", f"query={query}"]
    if cursor:
        cmd.extend(["-F", f"cursor={cursor}"])
    for attempt in range(max_retries):
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            return json.loads(result.stdout)
        if attempt < max_retries - 1:
            wait = 15 * (attempt + 1)
            print(f"    Retry in {wait}s: {result.stderr.strip()[:100]}", flush=True)
            time.sleep(wait)
    return None

def build_batch_query(numbers):
    parts = []
    for i, num in enumerate(numbers):
        parts.append(f'pr{i}: repository(owner: "PostHog", name: "posthog") {{ pullRequest(number: {num}) {{ number title additions deletions changedFiles createdAt mergedAt author {{ login }} mergedBy {{ login }} labels(first: 10) {{ nodes {{ name }} }} body reviewDecision reviews(first: 10) {{ nodes {{ state author {{ login }} submittedAt body }} }} files(first: 50) {{ nodes {{ path additions deletions changeType }} }} comments {{ totalCount }} reviewThreads {{ totalCount }} }} }}')
    return "query {\n" + "\n".join(parts) + "\n}"

def pass1_collect_numbers():
    chunks = []
    current = date(2025, 12, 2)
    end_date = date(2026, 3, 2)
    while current < end_date:
        chunk_end = min(current + timedelta(days=10), end_date)
        chunks.append((current.isoformat(), chunk_end.isoformat()))
        current = chunk_end

    all_numbers = set()

    for i, (start, end) in enumerate(chunks):
        cursor = None
        chunk_nums = set()
        query = SEARCH_QUERY.format(start=start, end=end)

        for page in range(1, 12):
            data = gh_graphql(query, cursor=cursor)
            if not data:
                print(f"  [{start}..{end}] page {page} FAILED", flush=True)
                break

            search = data["data"]["search"]
            expected = search.get("issueCount", 0)

            prev_size = len(chunk_nums)
            for node in search["nodes"]:
                if node and "number" in node:
                    chunk_nums.add(node["number"])

            if len(chunk_nums) == prev_size:
                break

            if not search["pageInfo"]["hasNextPage"] or len(chunk_nums) >= expected:
                break
            cursor = search["pageInfo"]["endCursor"]
            time.sleep(0.2)

        all_numbers |= chunk_nums
        print(f"  Chunk {i+1}/{len(chunks)}: {start}..{end} -> {len(chunk_nums)} PRs (expected ~{expected})", flush=True)

    return sorted(all_numbers)

def pass2_fetch_details(numbers):
    batch_size = 20
    all_prs = []

    for i in range(0, len(numbers), batch_size):
        batch = numbers[i:i+batch_size]
        query = build_batch_query(batch)

        data = gh_graphql(query)
        if not data:
            print(f"  Batch {i//batch_size + 1} FAILED, skipping {len(batch)} PRs", flush=True)
            continue

        for j in range(len(batch)):
            repo_data = data.get("data", {}).get(f"pr{j}", {})
            pr_data = repo_data.get("pullRequest") if repo_data else None
            if pr_data:
                pr = parse_pr(pr_data)
                if pr:
                    all_prs.append(pr)

        done = min(i + batch_size, len(numbers))
        if (i // batch_size + 1) % 5 == 0 or done == len(numbers):
            print(f"  Fetched {len(all_prs)}/{len(numbers)} PR details...", flush=True)

        time.sleep(0.3)

    return all_prs

def main():
    print("=== Pass 1: Collecting PR numbers ===", flush=True)
    numbers = pass1_collect_numbers()
    print(f"\nFound {len(numbers)} unique PR numbers\n", flush=True)

    print("=== Pass 2: Fetching PR details ===", flush=True)
    prs = pass2_fetch_details(numbers)
    prs.sort(key=lambda p: p["merged_at"])

    with open(OUTPUT, "w") as f:
        json.dump(prs, f)

    print(f"\nWrote {len(prs)} PRs to {OUTPUT}", flush=True)

if __name__ == "__main__":
    main()
