import requests
import json
from datetime import datetime, timedelta

# Endpoint
url = "http://localhost:5000/api/storage/v2/records:batch"

# Headers
headers = {
    "Content-Type": "application/json",
    "data-partition-id": "opendes"
}

# Generate 20 dummy Well records
base_date = datetime(2021, 1, 1)
records = []

for i in range(2, 22 + 1):  # example-002 to example-022
    record_id = f"osdu:master-data--Well:example-{i:03d}:1"
    spud_date = (base_date + timedelta(days=i - 2)).strftime("%Y-%m-%dT00:00:00Z")
    record = {
        "id": record_id,
        "kind": "wks:Well:1.4.0",
        "legal": {
            "legaltags": ["opendes-public-usa"],
            "otherRelevantDataCountries": ["US"]
        },
        "acl": {
            "viewers": ["data.default.viewer@opendes"],
            "owners": ["data.default.owner@opendes"]
        },
        "data": {
            "name": f"Well example-{i:03d}",
            "spudDate": spud_date
        }
    }
    records.append(record)

# Payload
payload = {
    "records": records
}

# Send POST request
response = requests.post(url, headers=headers, data=json.dumps(payload))

# Output result
print("Status:", response.status_code)
print(json.dumps(response.json(), indent=2))
