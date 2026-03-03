#!/usr/bin/env python3
"""PR collection - 2-pass approach:
Pass 1: Lightweight search to get all PR numbers (pagination works)
Pass 2: Fetch full details per PR using direct node query"""

import json
import subprocess
import sys
import os
import time
from datetime import date, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT = os.path.join(BASE_DIR, "raw-data", "prs.json")

SEARCH_QUERY = """
query($cursor: String) {{
  search(query: "repo:PostHog/posthog is:pr is:merged merged:{start}..{end}", type: ISSUE, first: 100, after: $cursor) {{
    issueCount
    pageInfo {{ hasNextPage endCursor }}
    nodes {{ ... on PullRequest {{ number }} }}
  }}
}}
"""

# Batch query: fetch 25 PRs at once using aliases
def build_batch_query(numbers):
    parts = []
    for i, num in enumerate(numbers):
        parts.append(f"""
    pr{i}: repository(owner: "PostHog", name: "posthog") {{
      pullRequest(number: {num}) {{
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
    }}""")
    return "query {\n" + "\n".join(parts) + "\n}"

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

def gh_graphql(query, max_retries=3):
    for attempt in range(max_retries):
        result = subprocess.run(
            ["gh", "api", "graphql", "-f", f"query={query}"],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
        if attempt < max_retries - 1:
            time.sleep(10 * (attempt + 1))
    return None

def pass1_collect_numbers():
    """Lightweight search to get all PR numbers."""
    chunks = []
    current = date(2025, 12, 2)
    end = date(2026, 3, 2)
    while current < end:
        chunk_end = min(current + timedelta(days=10), end)
        chunks.append((current.isoformat(), chunk_end.isoformat()))
        current = chunk_end

    all_numbers = set()

    for i, (start, end_str) in enumerate(chunks):
        cursor = None
        chunk_nums = set()

        for page in range(1, 12):
            q = SEARCH_QUERY.format(start=start, end=end_str)
            if cursor:
                q = q.replace("$cursor", f'"{cursor}"')
            else:
                q = q.replace("$cursor", "null")

            # Use variables properly
            variables = json.dumps({"cursor": cursor})
            result = subprocess.run(
                ["gh", "api", "graphql",
                 "-f", f"query={SEARCH_QUERY.format(start=start, end=end_str)}",
                 "-f", f"variables={variables}"],
                capture_output=True, text=True,
            )
            if result.returncode != 0:
                time.sleep(15)
                result = subprocess.run(
                    ["gh", "api", "graphql",
                     "-f", f"query={SEARCH_QUERY.format(start=start, end=end_str)}",
                     "-f", f"variables={variables}"],
                    capture_output=True, text=True,
                )
                if result.returncode != 0:
                    print(f"  [{start}..{end_str}] page {page} FAILED", flush=True)
                    break

            data = json.loads(result.stdout)
            search = data["data"]["search"]

            new_nums = set()
            for node in search["nodes"]:
                if node and "number" in node:
                    new_nums.add(node["number"])

            prev_size = len(chunk_nums)
            chunk_nums |= new_nums

            if len(chunk_nums) == prev_size:
                break  # No new results, pagination cycling

            if not search["pageInfo"]["hasNextPage"]:
                break
            cursor = search["pageInfo"]["endCursor"]
            time.sleep(0.2)

        all_numbers |= chunk_nums
        expected = search.get("issueCount", "?")
        print(f"  Chunk {i+1}: {start}..{end_str} -> {len(chunk_nums)} PRs (expected ~{expected})", flush=True)

    return sorted(all_numbers)

def pass2_fetch_details(numbers):
    """Fetch full PR details in batches of 20."""
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
            key = f"pr{j}"
            repo_data = data.get("data", {}).get(key, {})
            pr_data = repo_data.get("pullRequest") if repo_data else None
            if pr_data:
                pr = parse_pr(pr_data)
                if pr:
                    all_prs.append(pr)

        if (i // batch_size + 1) % 10 == 0:
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
