import requests
import json

# Environment
BASE_URL = "http://127.0.0.1:5000"
PARTITION = "opendes"
TOKEN = "dev-placeholder"  # your Flask stub ignores auth, but header still required

# A single ReferenceValue record
record = {
    "id": "osdu:reference-data--UnitOfMeasure:Meter",
    "kind": "osdu:reference-data--UnitOfMeasure:1.0.0",
    "acl": {
        "owners": ["data.default.owners@opendes.contoso.com"],
        "viewers": ["data.default.viewers@opendes.contoso.com"]
    },
    "legal": {
        "legaltags": ["opendes-public-usa"],
        "otherRelevantDataCountries": ["US"],
        "status": "compliant"
    },
    "data": {
        "Code": "m",
        "Name": "meter",
        "Description": "SI base unit of length"
    }
}

# Option 1: PUT /records (expects a list of records)
url_put = f"{BASE_URL}/api/storage/v2/records"
resp_put = requests.put(
    url_put,
    headers={
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
        "data-partition-id": PARTITION
    },
    data=json.dumps([record])  # wrap in list
)
print("PUT response:", resp_put.status_code, resp_put.json())

# Option 2: POST /records:batch (expects {"records": [...]})
url_batch = f"{BASE_URL}/api/storage/v2/records:batch"
resp_batch = requests.post(
    url_batch,
    headers={
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
        "data-partition-id": PARTITION
    },
    data=json.dumps({"records": [record]})
)
print("BATCH response:", resp_batch.status_code, resp_batch.json())
