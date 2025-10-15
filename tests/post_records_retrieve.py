# test_retrieve.py
import requests, json

BASE = "http://127.0.0.1:5000/api/storage/v2"
HEADERS = {"Authorization": "Bearer dev-placeholder", "data-partition-id": "opendes", "Content-Type": "application/json"}

ids = ["osdu:unit--Meter:1", "osdu:unit--Foot:1"]
resp = requests.post(f"{BASE}/records:retrieve?includeDeleted=false&latest=true",
                     headers=HEADERS, data=json.dumps({"records": ids}))
print(resp.status_code, resp.json())
