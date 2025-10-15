import json
import requests

# Storage Service endpoint (your app.py)
STORAGE_URL = "http://localhost:5000/api/storage/v2/records"

# Well record to ingest
record = {
    "id": "osdu:master-data--Well:example-001:1",
    "kind": "wks:Well:1.4.0",
    "legal": {
        "legaltags": ["opendes-public-usa"],
        "otherRelevantDataCountries": ["US"]
    },
    "acl": {
        "viewers": ["data.default.viewers@opendes.example.com"],
        "owners": ["data.default.owners@opendes.example.com"]
    },
    "data": {
        "aliasNames": [
            {
                "name": "Example Well",
                "namingSystem": "Internal"
            }
        ],
        "spatialLocation": {
            "geometry": {
                "type": "Point",
                "coordinates": [-100.0, 40.0]
            }
        },
        "wellboreIDs": ["osdu:master-data--Wellbore:example-456:1"],
        "countryID": "osdu:reference-data--Country:US:1",
        "statusID": "osdu:reference-data--WellStatus:active:1",
        "purposeID": "osdu:reference-data--WellPurpose:exploration:1",
        "typeID": "osdu:reference-data--WellType:oil:1"
    }
}

# Send PUT request
response = requests.put(STORAGE_URL, json=[record])

# Print response
print(f"Status: {response.status_code}")
print(json.dumps(response.json(), indent=2))
