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

# Generate 60 Wellbore records (3 per Well)
base_date = datetime(2021, 2, 1)
records = []

for well_num in range(2, 23):  # example-002 to example-022
    well_id = f"osdu:master-data--Well:example-{well_num:03d}:1"
    for bore_num in range(1, 4):  # 3 wellbores per well
        wellbore_id = f"example-{well_num:03d}-bore-{bore_num}"
        record_id = f"osdu:master-data--Wellbore:{wellbore_id}:1"
        spud_date = (base_date + timedelta(days=(well_num - 2)*3 + bore_num - 1)).strftime("%Y-%m-%dT00:00:00Z")
        record = {
            "id": record_id,
            "kind": "wks:Wellbore:1.5.1",
            "legal": {
                "legaltags": ["opendes-public-usa"],
                "otherRelevantDataCountries": ["US"]
            },
            "acl": {
                "viewers": ["data.default.viewer@opendes"],
                "owners": ["data.default.owner@opendes"]
            },
            "data": {
                "name": f"Wellbore {wellbore_id}",
                "wellID": well_id,
                "wellboreID": wellbore_id,
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
