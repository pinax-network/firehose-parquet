#!/usr/bin/env python3
"""Read-only GitHub source check for #507: did any near-firehose-indexer version
populate Block.state_changes? Prints the matching lines of src/codec/mod.rs at
every commit that touched it (needs an authenticated `gh`)."""
import base64
import json
import subprocess

commits = json.loads(subprocess.run(
    ["gh", "api", "repos/streamingfast/near-firehose-indexer/commits?path=src/codec/mod.rs&per_page=100"],
    capture_output=True, text=True, check=True).stdout)
for commit in commits:
    sha = commit["sha"]
    content = json.loads(subprocess.run(
        ["gh", "api", f"repos/streamingfast/near-firehose-indexer/contents/src/codec/mod.rs?ref={sha}"],
        capture_output=True, text=True, check=True).stdout)["content"]
    text = base64.b64decode(content).decode()
    lines = [line.strip() for line in text.splitlines() if "state_changes" in line or "StateChange" in line]
    print(sha[:12], commit["commit"]["author"]["date"][:10], lines[:4])
