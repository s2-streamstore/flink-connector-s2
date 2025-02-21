import json
import sys

kv: dict[str, list[str]] = dict()

for line in sys.stdin:
  parsed: dict = json.loads(line)
  headers = dict(parsed["headers"])
  if headers["@action"] == "u":
    kv[json.loads(headers["@key"])["item"]] = json.loads(parsed["body"])

for (k, v) in sorted(kv.items()):
  print(json.dumps((k, v)))
