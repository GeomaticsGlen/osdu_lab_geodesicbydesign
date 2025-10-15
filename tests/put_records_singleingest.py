# test_put.py
import requests, json

BASE = "http://127.0.0.1:5000/api/storage/v2"
HEADERS = {
    "Authorization": "Bearer dev-placeholder",
    "data-partition-id": "opendes",
    "Content-Type": "application/json"
}

record = {
    "id": "osdu:unit--Meter:1",
    "kind": "osdu:wks:reference-data--UnitOfMeasure:1.0.0",
    "acl": {"owners": ["data.default.owners@opendes"], "viewers": ["data.default.viewers@opendes"]},
    "legal": {"legaltags": ["opendes-public-usa"], "otherRelevantDataCountries": ["US"], "status": "compliant"},
    "data": {"Code": "m", "Name": "meter", "Description": "SI base unit of length"}
}

resp = requests.put(f"{BASE}/records", headers=HEADERS, data=json.dumps([record]))
print(resp.status_code, resp.json())
