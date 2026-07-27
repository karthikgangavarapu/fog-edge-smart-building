"""
CI assertion: did the end-to-end run actually put data in the backend?

Kept as a file rather than an inline heredoc in the workflow, because an
indented heredoc terminator inside a YAML block scalar silently never closes
and the step fails with a confusing parser error.
"""
import json
import sys
import urllib.request

URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000/api/metrics"

metrics = json.load(urllib.request.urlopen(URL, timeout=10))
print(json.dumps(metrics, indent=2))

assert metrics["batches"] > 0, "no batches were persisted"
assert metrics["aggregates"] > 0, "no aggregates were persisted"
assert metrics["rejected"] == 0, "the backend rejected messages"
print("pipeline assertions passed")
