# test_get.py
import requests

BASE = "http://127.0.0.1:5000/api/storage/v2"
HEADERS = {"Authorization": "Bearer dev-placeholder", "data-partition-id": "opendes"}

params = {"ids": "osdu:unit--Meter:1", "includeDeleted": "false"}
resp = requests.get(f"{BASE}/records", headers=HEADERS, params=params)
print(resp.status_code, resp.json())
