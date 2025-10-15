# test_patch_bulk.py
import requests, json

BASE = "http://127.0.0.1:5000/api/storage/v2"
HEADERS = {"Authorization": "Bearer dev-placeholder", "data-partition-id": "opendes", "Content-Type": "application/json"}

patches = [
    {"id": "osdu:unit--Meter:1", "data": {"Description": "Bulk patched meter"}},
    {"id": "osdu:unit--Foot:1", "data": {"Description": "Bulk patched foot"}}
]

resp = requests.post(f"{BASE}/records:patch", headers=HEADERS, data=json.dumps({"records": patches}))
print(resp.status_code, resp.json())
