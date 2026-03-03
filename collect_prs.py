#!/usr/bin/env python3
"""Agent 2: Collect merged PRs with reviews and files via GraphQL."""

import json
import subprocess
import sys
import os
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT = os.path.join(BASE_DIR, "raw-data", "prs.json")
PROGRESS_FILE = os.path.join(BASE_DIR, "raw-data", ".prs_progress.json")

QUERY = """
query($cursor: String) {
  search(query: "repo:PostHog/posthog is:pr is:merged merged:2025-12-02..2026-03-02", type: ISSUE, first: 100, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    nodes {
      ... on PullRequest {
        number
        title
        additions
        deletions
        changedFiles
        createdAt
        mergedAt
        author { login }
        mergedBy { login }
        labels(first: 10) { nodes { name } }
        body
        reviewDecision
        reviews(first: 10) {
          nodes { state author { login } submittedAt body }
        }
        files(first: 50) {
          nodes { path additions deletions changeType }
        }
        comments { totalCount }
        reviewThreads { totalCount }
      }
    }
  }
}
"""

def truncate(text, max_len=200):
    if not text:
        return ""
    return text[:max_len] + ("..." if len(text) > max_len else "")

def run_query(cursor=None, max_retries=3):
    variables = json.dumps({"cursor": cursor})
    for attempt in range(max_retries):
        result = subprocess.run(
            ["gh", "api", "graphql", "-f", f"query={QUERY}", "-f", f"variables={variables}"],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
        print(f"  Attempt {attempt+1} failed: {result.stderr.strip()}", file=sys.stderr, flush=True)
        if attempt < max_retries - 1:
            wait = 10 * (attempt + 1)
            print(f"  Retrying in {wait}s...", flush=True)
            time.sleep(wait)
    print(f"GraphQL query failed after {max_retries} attempts", file=sys.stderr)
    return None

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

def save_progress(prs, cursor, page):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"prs": prs, "cursor": cursor, "page": page}, f)

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            data = json.load(f)
        print(f"Resuming from page {data['page']} with {len(data['prs'])} PRs already collected", flush=True)
        return data["prs"], data["cursor"], data["page"]
    return [], None, 0

def collect_prs():
    all_prs, cursor, page = load_progress()

    while True:
        page += 1
        print(f"Fetching page {page}...", flush=True)
        data = run_query(cursor)

        if data is None:
            print(f"Saving progress at page {page-1} with {len(all_prs)} PRs", flush=True)
            save_progress(all_prs, cursor, page - 1)
            sys.exit(1)

        search = data["data"]["search"]
        for node in search["nodes"]:
            pr = parse_node(node)
            if pr:
                all_prs.append(pr)

        if not search["pageInfo"]["hasNextPage"]:
            break
        cursor = search["pageInfo"]["endCursor"]

        if page % 10 == 0:
            save_progress(all_prs, cursor, page)

    with open(OUTPUT, "w") as f:
        json.dump(all_prs, f)

    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)

    print(f"Wrote {len(all_prs)} PRs to {OUTPUT}")

if __name__ == "__main__":
    collect_prs()
