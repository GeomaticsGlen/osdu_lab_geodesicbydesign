# test_batch.py
import requests, json

BASE = "http://127.0.0.1:5000/api/storage/v2"
HEADERS = {"Authorization": "Bearer dev-placeholder", "data-partition-id": "opendes", "Content-Type": "application/json"}

records = [
    {
        "id": "osdu:unit--Foot:1",
        "kind": "osdu:wks:reference-data--UnitOfMeasure:1.0.0",
        "acl": {"owners": ["data.default.owners@opendes"], "viewers": ["data.default.viewers@opendes"]},
        "legal": {"legaltags": ["opendes-public-usa"], "otherRelevantDataCountries": ["US"], "status": "compliant"},
        "data": {"Code": "ft", "Name": "foot", "Description": "Imperial unit of length"}
    }
]

resp = requests.post(f"{BASE}/records:batch", headers=HEADERS, data=json.dumps({"records": records}))
print(resp.status_code, resp.json())
