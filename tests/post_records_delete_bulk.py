# test_delete_bulk.py
import requests, json

BASE = "http://127.0.0.1:5000/api/storage/v2"
HEADERS = {"Authorization": "Bearer dev-placeholder", "data-partition-id": "opendes", "Content-Type": "application/json"}

ids = ["osdu:unit--Foot:1", "osdu:unit--Meter:1"]
resp = requests.post(f"{BASE}/records:delete", headers=HEADERS, data=json.dumps({"ids": ids}))
print(resp.status_code, resp.json())
