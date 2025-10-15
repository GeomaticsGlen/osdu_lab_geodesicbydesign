# test_patch_single.py
import requests, json

BASE = "http://127.0.0.1:5000/api/storage/v2"
HEADERS = {"Authorization": "Bearer dev-placeholder", "data-partition-id": "opendes", "Content-Type": "application/json"}

record_id = "osdu:unit--Meter:1"
patch = {"data": {"Description": "Updated description"}}

resp = requests.patch(f"{BASE}/records/{record_id}", headers=HEADERS, data=json.dumps(patch))
print(resp.status_code, resp.json())
