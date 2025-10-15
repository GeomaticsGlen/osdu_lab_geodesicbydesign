# test_delete_single.py
import requests

BASE = "http://127.0.0.1:5000/api/storage/v2"
HEADERS = {"Authorization": "Bearer dev-placeholder", "data-partition-id": "opendes"}

record_id = "osdu:unit--Meter:1"
resp = requests.delete(f"{BASE}/records/{record_id}", headers=HEADERS)
print(resp.status_code, resp.json())
